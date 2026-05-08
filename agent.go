package main

import (
	"bufio"
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

//go:embed prompts/*.txt
var embeddedPrompts embed.FS

var (
	reReplan       = regexp.MustCompile(`(?m)^REPLAN:\s*(.+)$`)
	reVerdict      = regexp.MustCompile(`(?m)^VERDICT:\s*([A-Z_]+)(?::\s*(.+))?$`)
	reTargetHash   = regexp.MustCompile(`(?m)^REBASE_TARGET:\s*([0-9a-f]+)$`)
	reCancelTicket = regexp.MustCompile(`(?m)^CANCEL:\s*(\d+)$`)
	reMessage      = regexp.MustCompile(`(?m)^MESSAGE:\s*(.+)$`)
)

func jailRWPaths(home string) []string {
	return []string{
		"/tmp",
		filepath.Join(home, ".claude"),
		filepath.Join(home, ".claude.json"),
		filepath.Join(home, ".cache"),
		filepath.Join(home, "go"),
	}
}

func (o *Orchestrator) runAgent(ctx context.Context, role AgentRole, ticket int, wsID, stdin string) AgentResult {
	o.AgentSem <- struct{}{}
	defer func() { <-o.AgentSem }()

	wsAbs := ""
	tmpdir := ""

	if wsID != "" {
		wsAbs = wsPath(o.Root, wsID)
		tmpdir = filepath.Join(wsAbs, ".tmp")
		Throw(os.MkdirAll(tmpdir, 0755))
	}

	home := os.Getenv("HOME")

	if home == "" {
		ThrowFmt("HOME env var is empty")
	}

	args := []string{}

	if wsAbs != "" {
		args = append(args, "--rw="+wsAbs)
	}

	if tmpdir != "" {
		args = append(args, "--rw="+tmpdir)
	}

	for _, p := range jailRWPaths(home) {
		args = append(args, "--rw="+p)
	}

	args = append(args, "--", o.ClaudeBin,
		"-p",
		"--output-format", "stream-json",
		"--verbose",
		"--dangerously-skip-permissions")

	cmd := exec.CommandContext(ctx, o.JailBin, args...)

	if wsAbs != "" {
		cmd.Dir = wsAbs
	}

	cmd.Stdin = strings.NewReader(stdin)
	cmd.Env = append(os.Environ(), "TMPDIR="+tmpdir)

	stdoutPipe := Throw2(cmd.StdoutPipe())

	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf

	Throw(cmd.Start())

	var rawLines, finalText strings.Builder

	scanner := bufio.NewScanner(stdoutPipe)
	scanner.Buffer(make([]byte, 1<<20), 16<<20)

	for scanner.Scan() {
		line := scanner.Bytes()
		rawLines.Write(line)
		rawLines.WriteByte('\n')

		var ev map[string]any

		if err := json.Unmarshal(line, &ev); err != nil {
			continue
		}

		traceAgentEvent(role, ticket, ev)

		if t, _ := ev["type"].(string); t == "result" {
			if txt, _ := ev["result"].(string); txt != "" {
				finalText.WriteString(txt)
			}
		}
	}

	err := cmd.Wait()

	res := AgentResult{
		Role:      role,
		Ticket:    ticket,
		Workspace: wsID,
		Stdout:    finalText.String(),
		Stderr:    stderrBuf.String(),
		RawStream: rawLines.String(),
	}

	if err != nil {
		var ee *exec.ExitError

		if errors.As(err, &ee) {
			res.Verdict = VerdictCrashed
			res.Detail = fmt.Sprintf("exit=%d stderr=%s", ee.ExitCode(), stderrBuf.String())

			return res
		}

		res.Verdict = VerdictCrashed
		res.Detail = fmt.Sprintf("wait: %v", err)

		return res
	}

	parseAgentOutput(&res)

	for _, m := range res.Messages {
		uiTicket("💬", role, ticket, "MESSAGE", m)
	}

	return res
}

func traceAgentEvent(role AgentRole, ticket int, ev map[string]any) {
	typ, _ := ev["type"].(string)

	if typ != "assistant" {
		return
	}

	msg, _ := ev["message"].(map[string]any)

	if msg == nil {
		return
	}

	content, _ := msg["content"].([]any)

	for _, c := range content {
		block, _ := c.(map[string]any)

		if block == nil {
			continue
		}

		btyp, _ := block["type"].(string)

		if btyp != "tool_use" {
			continue
		}

		name, _ := block["name"].(string)
		input, _ := block["input"].(map[string]any)
		ui("·", role, ticket, name, summarizeToolInput(name, input))
	}
}

func summarizeToolInput(toolName string, input map[string]any) string {
	if input == nil {
		return ""
	}

	switch toolName {
	case "Read", "Edit", "Write", "NotebookEdit":
		if p, ok := input["file_path"].(string); ok {
			return p
		}
	case "Bash":
		if c, ok := input["command"].(string); ok {
			return strings.ReplaceAll(c, "\n", " ⏎ ")
		}
	case "Grep", "Glob":
		if p, ok := input["pattern"].(string); ok {
			return p
		}
	}

	return ""
}

func parseAgentOutput(res *AgentResult) {
	for _, m := range reReplan.FindAllStringSubmatch(res.Stdout, -1) {
		res.ReplanLines = append(res.ReplanLines, strings.TrimSpace(m[1]))
	}

	for _, m := range reMessage.FindAllStringSubmatch(res.Stdout, -1) {
		res.Messages = append(res.Messages, strings.TrimSpace(m[1]))
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
	base := loadRolePrompt(orchRoot, role)
	common := loadCommonPrompt(orchRoot)

	if common == "" {
		return base
	}

	return base + "\n\n" + common
}

func loadRolePrompt(orchRoot string, role AgentRole) string {
	path := filepath.Join(orchRoot, "prompts", string(role)+".txt")

	if data, err := os.ReadFile(path); err == nil {
		return string(data)
	}

	if data, err := embeddedPrompts.ReadFile("prompts/" + string(role) + ".txt"); err == nil {
		return string(data)
	}

	return defaultPrompt(role)
}

func loadCommonPrompt(orchRoot string) string {
	path := filepath.Join(orchRoot, "prompts", "common.txt")

	if data, err := os.ReadFile(path); err == nil {
		return string(data)
	}

	if data, err := embeddedPrompts.ReadFile("prompts/common.txt"); err == nil {
		return string(data)
	}

	return ""
}

func defaultPrompt(role AgentRole) string {
	return "You are the " + string(role) + " agent in an orchestrated coding system. Output VERDICT: <code>: <detail> as the final line. Use REPLAN: <text> to enqueue replanner."
}
