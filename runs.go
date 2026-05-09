package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func runsDir(orchRoot string) string {
	return filepath.Join(orchRoot, "runs")
}

// Per-run state lives in a single jsonl file written by runAgentInner — one event per line,
// fields tagged by `t`. Schema in agent.go (search "writeEvent"). All readers below consume
// only this stream; nothing else writes to or reads from per-run state.
//
// priorRunsForTicket aggregates assistant text + final verdict from prior runs of a ticket
// for inclusion in the next prompt's PRIOR_RUNS section.
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

// summarizeRunJsonl scans a run's jsonl and returns the assistant's accumulated text plus
// the final verdict line. Replanner/operator can grep the file directly for richer detail.
func summarizeRunJsonl(path string) string {
	f, err := os.Open(path)

	if err != nil {
		return ""
	}

	defer f.Close()

	var sb strings.Builder

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 16<<20)

	for scanner.Scan() {
		var line map[string]any

		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			continue
		}

		t, _ := line["t"].(string)

		switch t {
		case "harness":
			ev, _ := line["ev"].(map[string]any)

			if ev == nil {
				continue
			}

			if txt := harnessAssistantText(ev); txt != "" {
				sb.WriteString(txt)

				if !strings.HasSuffix(txt, "\n") {
					sb.WriteByte('\n')
				}
			}
		case "finish":
			verdict, _ := line["verdict"].(string)
			detail, _ := line["detail"].(string)
			fmt.Fprintf(&sb, "VERDICT: %s: %s\n", verdict, detail)
		}
	}

	return sb.String()
}

// harnessAssistantText extracts the assistant's text from one harness event, regardless of
// backend. Claude (stream-json) emits `type:"result"` with a `result` string at the end;
// opencode emits `type:"text"` with `part.text` per chunk.
func harnessAssistantText(ev map[string]any) string {
	typ, _ := ev["type"].(string)

	switch typ {
	case "result":
		if txt, _ := ev["result"].(string); txt != "" {
			return txt
		}
	case "text":
		if part, ok := ev["part"].(map[string]any); ok {
			if txt, _ := part["text"].(string); txt != "" {
				return txt
			}
		}
	}

	return ""
}

func concatPromptInput(prompt, input string) string {
	if prompt == "" {
		return input
	}

	return prompt + "\n\n" + input
}

