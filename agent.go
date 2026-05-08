package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

const claudeBin = "claude"

var (
	reReplan       = regexp.MustCompile(`(?m)^REPLAN:\s*(.+)$`)
	reVerdict      = regexp.MustCompile(`(?m)^VERDICT:\s*([A-Z_]+)(?::\s*(.+))?$`)
	reTargetHash   = regexp.MustCompile(`(?m)^REBASE_TARGET:\s*([0-9a-f]+)$`)
	reCancelTicket = regexp.MustCompile(`(?m)^CANCEL:\s*(\d+)$`)
)

func runAgent(ctx context.Context, role AgentRole, ticket int, ws, stdin string) AgentResult {
	cmd := exec.CommandContext(ctx, claudeBin, "-p", "--dangerously-skip-permissions")

	if ws != "" {
		cmd.Dir = ws
	}

	cmd.Stdin = strings.NewReader(stdin)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()

	res := AgentResult{
		Role:      role,
		Ticket:    ticket,
		Workspace: ws,
		Stdout:    stdout.String(),
		Stderr:    stderr.String(),
	}

	if err != nil {
		res.Verdict = VerdictCrashed
		res.Detail = fmt.Sprintf("exit_err=%v stderr=%s", err, stderr.String())

		return res
	}

	parseAgentOutput(&res)

	return res
}

func parseAgentOutput(res *AgentResult) {
	for _, m := range reReplan.FindAllStringSubmatch(res.Stdout, -1) {
		res.ReplanLines = append(res.ReplanLines, strings.TrimSpace(m[1]))
	}

	if m := reVerdict.FindStringSubmatch(res.Stdout); m != nil {
		res.Verdict = AgentVerdict(m[1])

		if len(m) > 2 {
			res.Detail = strings.TrimSpace(m[2])
		}

		return
	}

	res.Verdict = VerdictNoAction
}

func ExtractCancelTickets(text string) []int {
	var out []int

	for _, m := range reCancelTicket.FindAllStringSubmatch(text, -1) {
		n, err := strconv.Atoi(m[1])

		if err == nil {
			out = append(out, n)
		}
	}

	return out
}

func ExtractRebaseTarget(text string) string {
	if m := reTargetHash.FindStringSubmatch(text); m != nil {
		return m[1]
	}

	return ""
}

func loadPrompt(orchRoot string, role AgentRole) string {
	path := filepath.Join(orchRoot, "prompts", string(role)+".txt")
	data, err := os.ReadFile(path)

	if err != nil {
		return defaultPrompt(role)
	}

	return string(data)
}

func defaultPrompt(role AgentRole) string {
	return "You are the " + string(role) + " agent in an orchestrated coding system. Output VERDICT: <code>: <detail> as the final line. Use REPLAN: <text> to enqueue replanner."
}
