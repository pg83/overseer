package main

import (
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

func dumpAgentRun(orchRoot string, role AgentRole, ticket int, ws, stdin string, res AgentResult) {
	Throw(os.MkdirAll(runsDir(orchRoot), 0755))

	name := fmt.Sprintf("T-%d-%s-%s-%s.log",
		ticket,
		time.Now().UTC().Format("20060102-150405.000000000"),
		role,
		ws)
	path := filepath.Join(runsDir(orchRoot), name)

	var sb strings.Builder
	sb.WriteString("=== INPUT ===\n")
	sb.WriteString(stdin)
	sb.WriteString("\n=== STREAM ===\n")
	sb.WriteString(res.RawStream)
	sb.WriteString("\n=== STDOUT ===\n")
	sb.WriteString(res.Stdout)
	sb.WriteString("\n=== STDERR ===\n")
	sb.WriteString(res.Stderr)
	fmt.Fprintf(&sb, "\n=== META ===\nrole=%s ticket=%d ws=%s verdict=%s\ndetail=%s\n",
		res.Role, res.Ticket, res.Workspace, res.Verdict, res.Detail)

	Throw(os.WriteFile(path, []byte(sb.String()), 0644))
}

func priorRunsForTicket(orchRoot string, ticketN int) string {
	entries, err := os.ReadDir(runsDir(orchRoot))

	if err != nil {
		return ""
	}

	prefix := fmt.Sprintf("T-%d-", ticketN)

	var matched []string

	for _, e := range entries {
		if strings.HasPrefix(e.Name(), prefix) {
			matched = append(matched, e.Name())
		}
	}

	sort.Strings(matched)

	var sb strings.Builder

	for _, n := range matched {
		path := filepath.Join(runsDir(orchRoot), n)

		data, err := os.ReadFile(path)

		if err != nil {
			continue
		}

		fmt.Fprintf(&sb, "\n--- %s ---\n", n)
		sb.Write(data)
	}

	return sb.String()
}

func concatPromptInput(prompt, input string) string {
	if prompt == "" {
		return input
	}

	return prompt + "\n\n" + input
}

func extractPlan(stdout string) string {
	lines := strings.Split(stdout, "\n")

	start := -1
	end := len(lines)

	for i, l := range lines {
		t := strings.TrimSpace(l)

		if start < 0 && t == "PLAN:" {
			start = i + 1

			continue
		}

		if start >= 0 && strings.HasPrefix(t, "VERDICT:") {
			end = i

			break
		}
	}

	if start < 0 {
		return ""
	}

	return strings.TrimSpace(strings.Join(lines[start:end], "\n"))
}

func trimAtVerdict(body string) string {
	lines := strings.Split(body, "\n")

	for i, l := range lines {
		if strings.HasPrefix(strings.TrimSpace(l), "VERDICT:") {
			return strings.TrimSpace(strings.Join(lines[:i], "\n"))
		}
	}

	return strings.TrimSpace(body)
}
