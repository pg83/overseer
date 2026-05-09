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
	"strings"
	"time"
)

//go:embed prompts/*.txt
var embeddedPrompts embed.FS

// modelForRole resolves the model to use for a given role. Precedence:
// per-role override → group (think/work) → global default → empty (harness picks).
func (o *Orchestrator) modelForRole(role AgentRole) string {
	if m := o.Models[string(role)]; m != "" {
		return m
	}

	switch role {
	case RoleTasker, RoleReplanner, RoleOverseer:
		if m := o.Models["think"]; m != "" {
			return m
		}
	case RoleDigger, RoleReviewer:
		if m := o.Models["work"]; m != "" {
			return m
		}
	}

	return o.Models["default"]
}

func jailRWPaths(home string, backend Backend) []string {
	common := []string{
		"/tmp",
		filepath.Join(home, ".cache"),
		filepath.Join(home, "go"),
	}

	switch backend {
	case BackendClaude:
		return append(common,
			filepath.Join(home, ".claude"),
			filepath.Join(home, ".claude.json"),
		)
	case BackendOpencode:
		return append(common,
			filepath.Join(home, ".config", "opencode"),
			filepath.Join(home, ".local", "share", "opencode"),
			filepath.Join(home, ".ya"),
		)
	}

	return common
}

// runAgent is the only entry-point consumers use. It guarantees:
//
//   - On success — returns an AgentResult whose Events came from the agent's own output
//     (or empty Events if the run was cancelled mid-flight via ctx; consumers' STALE
//     checks drop those).
//   - On retryable transport failures (network glitch, rate limit, API quota) — retries
//     internally with exponential backoff; consumer never sees the failure.
//   - On any other failure — hard-stops the orchestrator process via fatal(). The user's
//     invariant: agent-side failures never leak back as some "crashed" verdict; either
//     they're transient and we paper over them, or they're real bugs and we surface them
//     by killing the process.
//
// runAgentOnce communicates failures by Throw'ing an *agentFault. Cancellation by the
// orchestrator (ctx.Done) is NOT a fault — runAgentOnce returns the partial result and
// we propagate it up so the consumer's STALE check can drop it.
func (o *Orchestrator) runAgent(ctx context.Context, role AgentRole, ticket int, wsID, stdin string) AgentResult {
	backoff := 5 * time.Second
	maxBackoff := 60 * time.Second

	for attempt := 1; ; attempt++ {
		var res AgentResult

		exc := Try(func() {
			res = o.runAgentOnce(ctx, role, ticket, wsID, stdin)
		})

		if exc == nil {
			return res
		}

		fault, ok := exc.AsError().(*agentFault)

		if !ok {
			o.fatal(fmt.Sprintf("runAgent panic [%s T-%d ws=%s attempt=%d]: %s",
				role, ticket, wsID, attempt, exc.Error()))
		}

		retryable, why := classifyAgentFault(fault)

		if !retryable {
			o.fatal(fmt.Sprintf("agent failed non-retryably [%s T-%d ws=%s attempt=%d]: %s",
				role, ticket, wsID, attempt, why))
		}

		uiTicket("⏳", role, ticket, "RETRY",
			fmt.Sprintf("attempt=%d backoff=%s reason=%s", attempt, backoff, why))

		select {
		case <-ctx.Done():
			return AgentResult{Role: role, Ticket: ticket, Workspace: wsID}
		case <-time.After(backoff):
		}

		if backoff < maxBackoff {
			backoff *= 2

			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// agentFault is what runAgentOnce Throws when the harness invocation fails for a real
// reason (non-zero exit, harness stream-level error, etc.). Carries enough signal for
// classifyAgentFault to decide retryable vs. fatal.
type agentFault struct {
	exitCode int
	stderr   string
	stdout   string
}

func (f *agentFault) Error() string {
	return fmt.Sprintf("agent fault: exit=%d stderr=%s", f.exitCode, truncate(f.stderr, 200))
}

// classifyAgentFault decides whether a one-attempt harness failure is worth retrying.
// Whitelist of well-known transient signatures (rate limits, quota, common network
// glitches) — everything else is treated as a hard stop on the principle that mystery
// failures are likely real bugs in our config or code, not transient flakes.
//
// Grow these lists as production reveals new transient modes; keep the default strict
// so we surface bugs rather than mask them.
func classifyAgentFault(f *agentFault) (retryable bool, reason string) {
	haystack := strings.ToLower(f.stderr + "\n" + f.stdout)

	rateLimit := []string{
		"rate limit",
		"rate_limit",
		"429 too many requests",
		"too many requests",
		"quota exceeded",
		"credit balance",
		"insufficient credit",
		"out of credits",
		"monthly limit",
		"usage limit",
	}

	network := []string{
		"connection refused",
		"connection reset",
		"i/o timeout",
		"deadline exceeded",
		"no route to host",
		"temporary failure in name resolution",
		"name resolution failed",
		"tls handshake timeout",
		"503 service unavailable",
		"502 bad gateway",
		"504 gateway timeout",
	}

	for _, p := range rateLimit {
		if strings.Contains(haystack, p) {
			return true, "rate-limit/quota: " + p
		}
	}

	for _, p := range network {
		if strings.Contains(haystack, p) {
			return true, "transient network: " + p
		}
	}

	return false, fmt.Sprintf("exit=%d stderr=%q", f.exitCode, truncate(f.stderr, 200))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}

	return s[:n] + "..."
}

// fatal hard-stops the orchestrator. The user's invariant: any agent-harness failure
// the classifier doesn't recognize as retryable is almost certainly a config or
// programming bug we want to surface immediately, not loop on. Logs the reason,
// signals shutdown to other goroutines, and exits the process (defers don't run).
func (o *Orchestrator) fatal(reason string) {
	uiSys("💀", "FATAL", reason)
	fmt.Fprintf(os.Stderr, "FATAL: %s\n", reason)

	if o.StopCancel != nil {
		o.StopCancel()
	}

	os.Exit(1)
}

// runAgentOnce executes one harness invocation. On success returns the AgentResult
// (Events come from the agent's own output). On orchestrator-driven cancellation
// (ctx.Done) returns the partial result for the caller to propagate. On a real harness
// failure Throws an *agentFault — the caller (runAgent) classifies and either retries
// or hard-stops the process. Never returns a synthesized "crashed" verdict event.
func (o *Orchestrator) runAgentOnce(ctx context.Context, role AgentRole, ticket int, wsID, stdin string) AgentResult {
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

	rwArgs := []string{}

	if wsAbs != "" {
		rwArgs = append(rwArgs, "--rw="+wsAbs)
	}

	if tmpdir != "" {
		rwArgs = append(rwArgs, "--rw="+tmpdir)
	}

	for _, p := range jailRWPaths(home, o.Backend) {
		rwArgs = append(rwArgs, "--rw="+p)
	}

	model := o.modelForRole(role)

	if model == "" && o.Backend == BackendClaude && role == RoleDigger {
		model = "sonnet"
	}

	var cmd *exec.Cmd
	var parseLine func(map[string]any, *strings.Builder, *streamErr, AgentRole, int)

	switch o.Backend {
	case BackendClaude:
		cmd = buildClaudeCmd(ctx, o.JailBin, o.Harness, model, rwArgs, stdin)
		parseLine = parseClaudeStreamLine
	case BackendOpencode:
		cmd = buildOpencodeCmd(ctx, o.JailBin, o.Harness, model, wsAbs, rwArgs, stdin)
		parseLine = parseOpencodeStreamLine
	default:
		ThrowFmt("unknown backend %q", o.Backend)
	}

	if wsAbs != "" {
		cmd.Dir = wsAbs
	}

	cmd.Env = append(os.Environ(), "TMPDIR="+tmpdir)

	argsCopy := append([]string{}, cmd.Args...)

	uiTicket("🔧", role, ticket, "EXEC", strings.Join(argsCopy, " "))

	// Single jsonl writer for the whole run; no other persistence path. All readers
	// (priorRunsForTicket, replanner mining, operator) consume only this file.
	Throw(os.MkdirAll(runsDir(o.Root), 0755))
	runID := fmt.Sprintf("T-%d-%s-%s-%s",
		ticket, time.Now().UTC().Format("20060102-150405.000000000"), role, wsID)
	jsonlPath := filepath.Join(runsDir(o.Root), runID+".jsonl")
	jf := Throw2(os.Create(jsonlPath))
	defer jf.Close()

	writeEvent := func(payload map[string]any) {
		payload["ts"] = time.Now().UTC().Format(time.RFC3339Nano)
		b := Throw2(json.Marshal(payload))
		Throw2(jf.Write(append(b, '\n')))
	}

	writeEvent(map[string]any{
		"t": "start", "role": string(role), "ticket": ticket, "ws": wsID, "args": argsCopy,
	})
	writeEvent(map[string]any{"t": "stdin", "data": stdin})

	res := AgentResult{
		Role:      role,
		Ticket:    ticket,
		Workspace: wsID,
		Args:      argsCopy,
		Stdin:     stdin,
	}

	defer func() {
		v, d := lastVerdict(res.Events)
		writeEvent(map[string]any{
			"t": "finish", "verdict": string(v), "detail": d,
		})
	}()

	stdoutPipe := Throw2(cmd.StdoutPipe())

	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf

	Throw(cmd.Start())

	var finalText strings.Builder
	var rawStream bytes.Buffer
	var streamFault streamErr

	scanner := bufio.NewScanner(stdoutPipe)
	scanner.Buffer(make([]byte, 1<<20), 16<<20)

	for scanner.Scan() {
		line := scanner.Bytes()

		rawStream.Write(line)
		rawStream.WriteByte('\n')

		var ev map[string]any

		if err := json.Unmarshal(line, &ev); err != nil {
			continue
		}

		writeEvent(map[string]any{"t": "harness", "ev": ev})

		parseLine(ev, &finalText, &streamFault, role, ticket)
	}

	err := cmd.Wait()

	if stderrBuf.Len() > 0 {
		writeEvent(map[string]any{"t": "stderr", "data": stderrBuf.String()})
	}

	res.Stdout = finalText.String()
	res.RawStream = rawStream.String()
	res.Events = parseEvents(res.Stdout)

	if err != nil {
		// ctx.Err() != nil means we (the orchestrator) killed the harness — replanner
		// cancel op or shutdown. Not a fault; return the partial result and let the
		// consumer's STALE check drop it.
		if ctx.Err() != nil {
			return res
		}

		fault := &agentFault{
			stderr:   stderrBuf.String(),
			stdout:   res.Stdout,
			exitCode: -1,
		}

		var ee *exec.ExitError

		if errors.As(err, &ee) {
			fault.exitCode = ee.ExitCode()
		} else {
			fault.stderr = fmt.Sprintf("%s\nwait error: %v", fault.stderr, err)
		}

		Throw(fault)
	}

	if streamFault.set {
		Throw(&agentFault{
			stderr: "stream error: " + streamFault.msg,
			stdout: res.Stdout,
		})
	}

	for _, ev := range res.Events {
		if t, _ := ev["type"].(string); t != "message" {
			continue
		}

		text := messageText(ev)

		if text == "" {
			continue
		}

		uiTicket("💬", role, ticket, "MESSAGE", text)
		appendMessage(o.Root, role, ticket, text)
	}

	return res
}

type streamErr struct {
	set bool
	msg string
}

func (s *streamErr) record(msg string) {
	if !s.set {
		s.set = true
		s.msg = msg
	}
}

func wrapJail(jail string, rwArgs []string, harness string, harnessArgs ...string) (string, []string) {
	if jail == "" {
		return harness, harnessArgs
	}

	args := append([]string{}, rwArgs...)
	args = append(args, "--", harness)
	args = append(args, harnessArgs...)

	return jail, args
}

func buildClaudeCmd(ctx context.Context, jail, harness, model string, rwArgs []string, stdin string) *exec.Cmd {
	harnessArgs := []string{
		"-p",
		"--output-format", "stream-json",
		"--verbose",
		"--dangerously-skip-permissions",
	}

	if model != "" {
		harnessArgs = append(harnessArgs, "--model", model)
	}

	bin, args := wrapJail(jail, rwArgs, harness, harnessArgs...)

	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Stdin = strings.NewReader(stdin)

	return cmd
}

func buildOpencodeCmd(ctx context.Context, jail, harness, model, wsAbs string, rwArgs []string, prompt string) *exec.Cmd {
	harnessArgs := []string{"run",
		"--format", "json",
		"--dangerously-skip-permissions"}

	if model != "" {
		harnessArgs = append(harnessArgs, "-m", model)
	}

	if wsAbs != "" {
		harnessArgs = append(harnessArgs, "--dir", wsAbs)
	}

	bin, args := wrapJail(jail, rwArgs, harness, harnessArgs...)

	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Stdin = strings.NewReader(prompt)

	return cmd
}

func parseClaudeStreamLine(ev map[string]any, finalText *strings.Builder, fault *streamErr, role AgentRole, ticket int) {
	traceClaudeAssistant(finalText, role, ticket, ev)

	if t, _ := ev["type"].(string); t == "result" {
		if txt, _ := ev["result"].(string); txt != "" {
			finalText.WriteString(txt)
		}
	}
}

func parseOpencodeStreamLine(ev map[string]any, finalText *strings.Builder, fault *streamErr, role AgentRole, ticket int) {
	typ, _ := ev["type"].(string)

	switch typ {
	case "text":
		part, _ := ev["part"].(map[string]any)

		if part == nil {
			return
		}

		txt, _ := part["text"].(string)

		if txt == "" {
			return
		}

		finalText.WriteString(txt)
	case "tool_use":
		traceOpencodeToolUse(role, ticket, ev)
	case "error":
		fault.record(extractOpencodeErrorMsg(ev))
	}
}

func extractOpencodeErrorMsg(ev map[string]any) string {
	e, _ := ev["error"].(map[string]any)

	if e == nil {
		return "unknown"
	}

	if data, ok := e["data"].(map[string]any); ok {
		if msg, _ := data["message"].(string); msg != "" {
			return msg
		}
	}

	if name, _ := e["name"].(string); name != "" {
		return name
	}

	return "unknown"
}

func traceClaudeAssistant(finalText *strings.Builder, role AgentRole, ticket int, ev map[string]any) {
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

		switch btyp {
		case "tool_use":
			name, _ := block["name"].(string)
			input, _ := block["input"].(map[string]any)
			ui("·", role, ticket, name, summarizeToolInput(name, input))
		case "text":
			if txt, _ := block["text"].(string); txt != "" {
				finalText.WriteString(txt)

				if !strings.HasSuffix(txt, "\n") {
					finalText.WriteByte('\n')
				}
			}
		}
	}
}

func traceOpencodeToolUse(role AgentRole, ticket int, ev map[string]any) {
	part, _ := ev["part"].(map[string]any)

	if part == nil {
		return
	}

	tool, _ := part["tool"].(string)

	if tool == "" {
		return
	}

	state, _ := part["state"].(map[string]any)
	input, _ := state["input"].(map[string]any)

	ui("·", role, ticket, tool, summarizeToolInput(tool, input))
}

func summarizeToolInput(toolName string, input map[string]any) string {
	if input == nil {
		return ""
	}

	switch toolName {
	case "Read", "Edit", "Write", "NotebookEdit", "read", "edit", "write":
		for _, k := range []string{"file_path", "filePath", "path"} {
			if p, ok := input[k].(string); ok && p != "" {
				return p
			}
		}
	case "Bash", "bash":
		if c, ok := input["command"].(string); ok {
			return strings.ReplaceAll(c, "\n", " ⏎ ")
		}
	case "Grep", "Glob", "grep", "glob":
		if p, ok := input["pattern"].(string); ok {
			return p
		}
	}

	return ""
}

// parseEvents scans agent stdout line-by-line for embedded JSON events: any line whose
// first character is `{` (column 0) is fed to json.Unmarshal; valid objects accumulate
// in order. Lines that don't parse — prose, agent reasoning, tool traces — are dropped.
// AgentResult holds the resulting slice as-is; per-role consumers walk it and pull the
// event types they care about (see lastVerdict / messageText below + role-specific
// extractors in scheduler.go).
func parseEvents(stdout string) []map[string]any {
	var out []map[string]any

	for _, line := range strings.Split(stdout, "\n") {
		if !strings.HasPrefix(line, "{") {
			continue
		}

		var ev map[string]any

		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			continue
		}

		out = append(out, ev)
	}

	return out
}

// lastVerdict returns the last `verdict` event in the stream — agents sometimes quote
// VERDICT lines mid-thought (back when they were textual) or emit multiple verdicts;
// the protocol says the FINAL verdict is authoritative. Falls back to NO_ACTION when
// the stream contained no verdict event at all.
func lastVerdict(events []map[string]any) (AgentVerdict, string) {
	v := VerdictNoAction
	d := ""

	for _, ev := range events {
		if t, _ := ev["type"].(string); t != "verdict" {
			continue
		}

		vs, _ := ev["verdict"].(string)
		ds, _ := ev["detail"].(string)
		v = AgentVerdict(strings.TrimSpace(vs))
		d = strings.TrimSpace(ds)
	}

	return v, d
}

// messageText extracts the human-readable body from a `message` event. Tolerates
// either `text` (canonical) or `message` (alias) string fields.
func messageText(ev map[string]any) string {
	t, _ := ev["text"].(string)

	if t == "" {
		t, _ = ev["message"].(string)
	}

	return strings.TrimSpace(t)
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
	return "You are the " + string(role) + " agent in an orchestrated coding system. " +
		`Emit your final structured signal as a single-line JSON event at column 0, e.g. ` +
		`{"type":"verdict","verdict":"<code>","detail":"<detail>"}. ` +
		`Use {"type":"replan","reason":"<text>"} to enqueue the replanner.`
}
