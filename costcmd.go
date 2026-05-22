package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
)

var (
	reCostTicket = regexp.MustCompile(`^T-(\d+)-`)
	reCostRole   = regexp.MustCompile(`-([a-z]+)-ws-`)
)

// costMain implements `overseer cost [--ticket N] <root>`: it re-derives the
// dollar cost of past runs from the saved run logs (run-usage is also persisted
// live, but this works on any root's logs after the fact).
func costMain(argv []string) {
	fs := flag.NewFlagSet("cost", flag.ExitOnError)
	ticket := fs.Int("ticket", -1, "restrict to ticket N (default: whole project)")
	Throw(fs.Parse(argv))

	root := fs.Arg(0)

	if root == "" {
		ThrowFmt("usage: overseer cost [--ticket N] <root>")
	}

	reportCost(root, *ticket)
}

type costAgg struct {
	runs int
	usd  float64
}

func reportCost(root string, onlyTicket int) {
	entries := Throw2(os.ReadDir(runsDir(root)))

	byRole := map[string]*costAgg{}
	byModel := map[string]*costAgg{}

	var total float64
	var runs, unpriced int

	for _, e := range entries {
		name := e.Name()

		tm := reCostTicket.FindStringSubmatch(name)
		rm := reCostRole.FindStringSubmatch(name)

		if e.IsDir() || tm == nil || rm == nil {
			continue
		}

		if tn, _ := strconv.Atoi(tm[1]); onlyTicket >= 0 && tn != onlyTicket {
			continue
		}

		usd, model, ok := runCostFromLog(filepath.Join(runsDir(root), name))
		runs++

		if !ok {
			unpriced++

			continue
		}

		total += usd
		costAdd(byRole, rm[1], usd)
		costAdd(byModel, model, usd)
	}

	scope := "whole project"

	if onlyTicket >= 0 {
		scope = fmt.Sprintf("T-%d", onlyTicket)
	}

	fmt.Printf("cost — %s  (%s)\n", scope, root)
	printCostAgg("by role", byRole)
	printCostAgg("by model", byModel)
	fmt.Printf("\nTOTAL: $%.2f over %d runs (%d unpriced)\n", total, runs, unpriced)
}

func costAdd(m map[string]*costAgg, key string, usd float64) {
	a := m[key]

	if a == nil {
		a = &costAgg{}
		m[key] = a
	}

	a.runs++
	a.usd += usd
}

func printCostAgg(title string, m map[string]*costAgg) {
	keys := make([]string, 0, len(m))

	for k := range m {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool { return m[keys[i]].usd > m[keys[j]].usd })

	fmt.Printf("\n%s:\n", title)

	for _, k := range keys {
		fmt.Printf("  %-22s %3d runs  $%8.4f\n", k, m[k].runs, m[k].usd)
	}
}

// runCostFromLog re-derives one run's cost from its saved jsonl: token tally via
// the harness's own AccumulateUsage, model from the claude stream (or codex's
// configured model — it omits it from the stream), priced through the embedded
// table. ok=false when there's no usage or the model isn't priceable.
func runCostFromLog(path string) (usd float64, model string, ok bool) {
	f := Throw2(os.Open(path))
	defer f.Close()

	var h Harness
	var usage RunUsage

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 16<<20)

	for sc.Scan() {
		var o struct {
			T    string         `json:"t"`
			Args []string       `json:"args"`
			Ev   map[string]any `json:"ev"`
		}

		if json.Unmarshal(sc.Bytes(), &o) != nil {
			continue
		}

		switch o.T {
		case "start":
			h = harnessFromStartArgs(o.Args)
		case "harness":
			if h == nil || o.Ev == nil {
				continue
			}

			h.AccumulateUsage(o.Ev, &usage)

			if m := claudeStreamModel(o.Ev); m != "" {
				model = m
			}
		}
	}

	if h == nil || usage.tokens() == 0 {
		return 0, model, false
	}

	if _, codex := h.(*Codex); codex && model == "" {
		model = codexConfiguredModel()
	}

	usd, ok = usdForModel(model, usage)

	return usd, model, ok
}

// harnessFromStartArgs picks the harness out of a run's recorded exec argv: the
// jail wrapper puts the harness binary right after "--"; --no-jail runs have it
// at argv[0].
func harnessFromStartArgs(args []string) Harness {
	bin := ""

	for i, a := range args {
		if a == "--" && i+1 < len(args) {
			bin = args[i+1]

			break
		}
	}

	if bin == "" && len(args) > 0 {
		bin = args[0]
	}

	if bin == "" {
		return nil
	}

	var h Harness
	Try(func() { h = SelectHarness(bin) })

	return h
}

func claudeStreamModel(ev map[string]any) string {
	if t, _ := ev["type"].(string); t != "assistant" {
		return ""
	}

	msg, _ := ev["message"].(map[string]any)

	if msg == nil {
		return ""
	}

	m, _ := msg["model"].(string)

	return m
}
