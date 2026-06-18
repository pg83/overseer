package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func runsDir(orchRoot string) string {
	return filepath.Join(orchRoot, "runs")
}

// writeGateRun records the fast-merge gate's ./acceptance invocation as a run jsonl,
// so the synthetic merger shows up in RUNS_DIR alongside agent merger runs (same
// start / finish schema readers expect — PRIOR_RUNS, cost tooling, audits). The gate
// skips the agent, so without this the merge decision would leave no per-run trail.
// Best-effort: the caller wraps it so a logging failure never undoes a merge.
func writeGateRun(orchRoot string, ticket int, ws string, args []string, output string, code int, verdict AgentVerdict, detail string) {
	Throw(os.MkdirAll(runsDir(orchRoot), 0755))

	runID := fmt.Sprintf("T-%d-%s-%s-%s", ticket, time.Now().UTC().Format("20060102-150405.000000000"), RoleMerger, ws)
	f := Throw2(os.Create(filepath.Join(runsDir(orchRoot), runID+".jsonl")))
	defer f.Close()

	write := func(payload map[string]any) {
		payload["ts"] = time.Now().UTC().Format(time.RFC3339Nano)
		b := Throw2(json.Marshal(payload))
		Throw2(f.Write(append(b, '\n')))
	}

	write(map[string]any{"t": "start", "role": string(RoleMerger), "ticket": ticket, "ws": ws, "args": args, "gate": true})
	write(map[string]any{"t": "acceptance", "exit": code, "output": output})
	write(map[string]any{"t": "finish", "verdict": string(verdict), "detail": detail})
}

// Per-run state lives in a single jsonl file written by runAgentInner — one event per line,
// fields tagged by `t`. Schema in agent.go (search "writeEvent"). All readers below consume
// only this stream; nothing else writes to or reads from per-run state.
//
// priorRunsForTicket aggregates assistant text + final verdict from prior runs of a ticket
// for inclusion in the next prompt's PRIOR_RUNS section. Past runs may have been driven
// by different harnesses (per-role config), so the offline assistantText extractor is
// generic over the formats both backends emit.
func priorRunsForTicket(orchRoot string, ticketN int) string {
	entries, err := os.ReadDir(runsDir(orchRoot))

	if err != nil {
		return ""
	}

	prefix := fmt.Sprintf("T-%d-", ticketN)
	suffix := ".jsonl"

	var matched []string

	for _, e := range entries {
		if strings.HasPrefix(e.Name(), prefix) && strings.HasSuffix(e.Name(), suffix) {
			matched = append(matched, e.Name())
		}
	}

	sort.Strings(matched)

	var sb strings.Builder

	for _, n := range matched {
		path := filepath.Join(runsDir(orchRoot), n)
		summary := summarizeRunJsonl(path)

		if summary == "" {
			continue
		}

		fmt.Fprintf(&sb, "\n--- %s ---\nLOG_FILE: %s\n%s", n, path, summary)
	}

	return sb.String()
}

// summarizeRunJsonl returns only the final verdict from a run. Content is
// omitted — message events are already in TICKET_CHAT via messages.txt;
// full reasoning is available by reading LOG_FILE directly.
func summarizeRunJsonl(path string) string {
	f, err := os.Open(path)

	if err != nil {
		return ""
	}

	defer f.Close()

	var verdict, detail string

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 16<<20)

	for scanner.Scan() {
		var line map[string]any

		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			continue
		}

		if t, _ := line["t"].(string); t == "finish" {
			verdict, _ = line["verdict"].(string)
			detail, _ = line["detail"].(string)
		}
	}

	return fmt.Sprintf("VERDICT: %s: %s\n", verdict, detail)
}

// outputPriming is appended at the very end of every agent stdin. Research on
// weak models (glm-4 family etc.) shows the last instruction before the
// assistant turn has the strongest pull on the first emitted tokens — output
// priming ("your reply begins with `{`") biases the model toward starting
// generation with a JSON object instead of prose narration. The role-specific
// rules already live in prompts/*.txt and common.txt; this is the universal
// last-line nudge.
const outputPriming = "\n\nYour reply contains only JSON-line events. Each non-blank line is one JSON object: {\"type\": \"...\", ...}. Begin your reply with `{`."
