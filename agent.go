package main

import (
	"bufio"
	"bytes"
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

// harnessModelForRole resolves the (harness, model) binding for a role. Precedence:
//   1. per-role override   (--<role>-harness)
//   2. group default       (--think-harness | --work-harness)
//   3. global default      (--harness, always set)
//
// Empty Model in the result means "let the harness pick its own default for this role"
// — caller falls back to harness.DefaultModel(role).
func (o *Orchestrator) harnessModelForRole(role AgentRole) HarnessModel {
	if hm, ok := o.Bindings[string(role)]; ok {
		return hm
	}

	switch role {
	case RoleTasker, RoleReplanner, RoleOverseer:
		if hm, ok := o.Bindings["think"]; ok {
			return hm
		}
	case RoleDigger, RoleReviewer, RoleMerger, RoleArbiter:
		if hm, ok := o.Bindings["work"]; ok {
			return hm
		}
	}

	if hm, ok := o.Bindings["default"]; ok {
		return hm
	}

	ThrowFmt("no harness binding for role %q (and no default)", role)

	return HarnessModel{}
}

// resolveModel picks the explicit model from the binding, or falls back to the
// harness's per-role default ("" if the harness itself has no preference).
func (hm HarnessModel) resolveModel(role AgentRole) string {
	if hm.Model != "" {
		return hm.Model
	}

	return hm.Harness.DefaultModel(role)
}

// runAgent is the only entry-point consumers use. It guarantees:
//
//   - The harness always runs to completion. We never kill it from outside.
//   - On retryable transport failures (rate limit, quota, network glitch) — retries
//     with exponential backoff; the consumer never sees the failure.
//   - On any other transport failure — fatal() the orchestrator process.
//   - Agent stdout is parsed permissively as JSON-line (see parseEvents): prose
//     preamble, code fences, and minor brace drift are silently skipped. Role-
//     specific helpers retry when no matching verdict arrives, so garbled output
//     turns into a "no verdict" respawn at that layer rather than here.
//   - On success, surfaces `message` events to the team chat + UI before returning.
//
// `env` is exported as environment variables so prompts can reference inputs as
// `$WORKSPACE`, `$MERGER_WORKTREE`, etc. in bash tool calls. The same values appear
// in the prose `stdin` for context.
func (o *Orchestrator) runAgent(role AgentRole, ticket int, wsID, stdin string, env map[string]string) AgentResult {
	harness := o.harnessModelForRole(role).Harness

	backoff := 5 * time.Second
	maxBackoff := 60 * time.Second

	bumpBackoff := func() {
		time.Sleep(backoff)

		if backoff < maxBackoff {
			backoff *= 2

			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}

	for attempt := 1; ; attempt++ {
		var res AgentResult

		exc := Try(func() {
			res = o.runAgentOnce(role, ticket, wsID, stdin, env)
		})

		if exc != nil {
			fault, ok := exc.AsError().(*agentFault)

			if !ok {
				o.fatal(fmt.Sprintf("runAgent panic [%s T-%d ws=%s attempt=%d]: %s",
					role, ticket, wsID, attempt, exc.Error()))
			}

			retryable, why := harness.ClassifyFault(fault)

			if !retryable {
				o.fatal(fmt.Sprintf("agent failed non-retryably [%s T-%d ws=%s attempt=%d]: %s",
					role, ticket, wsID, attempt, why))
			}

			// Surface any message events accumulated before the fault so they
			// reach messages.txt even though we're about to retry the whole run.
			for _, ev := range parseEvents(fault.stdout) {
				if t, _ := ev["type"].(string); t == "message" {
					if text := messageText(ev); text != "" {
						uiTicket("💬", role, ticket, "MESSAGE", text)
						appendMessage(o.Root, role, ticket, text)
					}
				}
			}

			uiTicket("⏳", role, ticket, "RETRY",
				fmt.Sprintf("attempt=%d backoff=%s reason=%s", attempt, backoff, why))

			bumpBackoff()

			continue
		}

		res.Events = parseEvents(res.Stdout)

		for _, ev := range res.Events {
			t, _ := ev["type"].(string)

			switch t {
			case "message":
				text := messageText(ev)

				if text == "" {
					continue
				}

				uiTicket("💬", role, ticket, "MESSAGE", text)
				appendMessage(o.Root, role, ticket, text)

			case "unparsed":
				text, _ := ev["text"].(string)

				if text == "" {
					continue
				}

				uiTicket("⚠️", role, ticket, "UNPARSED", text)
			}
		}

		return res
	}
}

// agentFault is what runAgentOnce Throws when the harness invocation fails for a real
// reason (non-zero exit, harness stream-level error, ...). Carries enough signal for
// Harness.ClassifyFault to decide retryable vs. fatal.
type agentFault struct {
	exitCode int
	stderr   string
	stdout   string
}

func (f *agentFault) Error() string {
	return fmt.Sprintf("agent fault: exit=%d stderr=%s", f.exitCode, f.stderr)
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
// (Events come from the agent's own output). On a real harness failure Throws an
// *agentFault — the caller (runAgent) classifies and either retries or hard-stops the
// process. Never returns a synthesized "crashed" verdict event. The harness is never
// killed from outside — it always runs to completion, on the user's invariant.
func (o *Orchestrator) runAgentOnce(role AgentRole, ticket int, wsID, stdin string, env map[string]string) AgentResult {
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

	hm := o.harnessModelForRole(role)
	harness := hm.Harness

	for _, p := range harness.JailRWPaths(home) {
		rwArgs = append(rwArgs, "--rw="+p)
	}

	for _, p := range o.ExtraRW {
		rwArgs = append(rwArgs, "--rw="+p)
	}

	model := hm.resolveModel(role)

	bin, args := wrapJail(o.Jail, rwArgs, harness.Bin(), harness.Args(model, wsAbs))

	cmd := exec.Command(bin, args...)
	cmd.Stdin = strings.NewReader(stdin)

	if wsAbs != "" {
		cmd.Dir = wsAbs
	}

	cmd.Env = append(os.Environ(), "TMPDIR="+tmpdir)

	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}

	argsCopy := append([]string{}, cmd.Args...)

	uiTicket("🔧", role, ticket, "EXEC", fmt.Sprintf("%s prompt_len=%d", strings.Join(argsCopy, " "), len(stdin)))

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
		v, d := lastVerdict(parseEvents(res.Stdout))
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

		harness.ParseStreamLine(ev, &finalText, &streamFault, role, ticket)
	}

	err := cmd.Wait()

	if stderrBuf.Len() > 0 {
		writeEvent(map[string]any{"t": "stderr", "data": stderrBuf.String()})
	}

	res.Stdout = finalText.String()
	res.RawStream = rawStream.String()

	if err != nil {
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

// wrapJail composes the final exec invocation. `jail` is the full jail-prefix
// command picked by resolveJail; empty / nil means no wrapper. Three shapes
// cover all modes:
//   nil / []        → harness runs directly (--no-jail).
//   ["X"]           → external jail binary X.
//   ["/proc/self/exe", "jail"] → built-in `overseer jail` subcommand.
// Generic over all backends — only the inner (bin, args) pair varies per Harness.
func wrapJail(jail, rwArgs []string, harnessBin string, harnessArgs []string) (string, []string) {
	if len(jail) == 0 {
		return harnessBin, harnessArgs
	}

	args := append([]string{}, jail[1:]...)
	args = append(args, rwArgs...)
	args = append(args, "--", harnessBin)
	args = append(args, harnessArgs...)

	return jail[0], args
}

// summarizeToolInput is the UI-trace helper shared by both backends — claude uses
// CamelCase tool names (Read / Edit / Bash / Grep / Glob), opencode uses lowercase
// (read / edit / bash / grep / glob), so the switch lists both forms together.
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

// parseEvents extracts JSON event objects from stdout permissively. Tolerates
// reasoning-tag preambles, markdown code fences, prose narration between
// events, pretty-printed multi-line JSON, and the occasional trailing brace
// from glm-style emit drift.
//
// Algorithm: scan the stream looking for top-level balanced `{...}` runs
// (string-aware so braces inside string literals don't confuse the depth
// counter). Each balanced run is Unmarshal'd; if it parses as a JSON object
// with a non-empty `type`, it becomes an event. Everything else (text outside
// braces, runs that don't unmarshal, objects without a `type`) is collected.
//
// The collected leftovers surface at the end as a single synthetic
// `{"type":"unparsed","text":"<joined>"}` event so the UI, run logs, and
// replanner mining still see what the agent emitted. Role-specific helpers
// ignore the synthetic type and respawn naturally when no verdict arrived.
func parseEvents(stdout string) []map[string]any {
	var out []map[string]any
	var unparsed []string

	for _, raw := range scanJSONObjects(stdout, &unparsed) {
		var ev map[string]any

		if err := json.Unmarshal([]byte(raw), &ev); err != nil {
			unparsed = append(unparsed, raw)
			continue
		}

		if t, _ := ev["type"].(string); t == "" {
			unparsed = append(unparsed, raw)
			continue
		}

		out = append(out, ev)
	}

	if len(unparsed) > 0 {
		out = append(out, map[string]any{
			"type": "unparsed",
			"text": strings.Join(unparsed, "\n"),
		})
	}

	return out
}

// scanJSONObjects walks the input collecting top-level balanced `{...}`
// substrings. String literals are tracked so braces inside JSON strings don't
// affect the depth counter. Anything between balanced runs (prose preamble,
// code fences, trailing junk) is appended to leftovers verbatim, trimmed.
//
// An unterminated `{` (depth never returns to zero before EOF) is treated as
// trailing junk and dumped into leftovers.
func scanJSONObjects(s string, leftovers *[]string) []string {
	var objs []string

	i := 0
	n := len(s)

	for i < n {
		j := strings.IndexByte(s[i:], '{')

		if j < 0 {
			if tail := strings.TrimSpace(s[i:]); tail != "" {
				*leftovers = append(*leftovers, tail)
			}

			break
		}

		j += i

		if pre := strings.TrimSpace(s[i:j]); pre != "" {
			*leftovers = append(*leftovers, pre)
		}

		end := matchBrace(s, j)

		if end < 0 {
			// matchBrace ran off the end without balancing — typically the
			// string-state walk fell out of phase (e.g. `{"type":task"...}`
			// with a missing opening quote inverts every subsequent toggle).
			// Find the next `{` and try again; the broken stretch is dropped
			// into leftovers in a single chunk so later valid objects in the
			// same stdout don't get swallowed alongside it.
			nextRel := strings.IndexByte(s[j+1:], '{')

			if nextRel < 0 {
				if tail := strings.TrimSpace(s[j:]); tail != "" {
					*leftovers = append(*leftovers, tail)
				}

				break
			}

			next := j + 1 + nextRel

			if broken := strings.TrimSpace(s[j:next]); broken != "" {
				*leftovers = append(*leftovers, broken)
			}

			i = next

			continue
		}

		objs = append(objs, s[j:end+1])
		i = end + 1
	}

	return objs
}

// matchBrace finds the index of the `}` that closes the `{` at start, with
// JSON-aware string + escape handling. Returns -1 if the input ends before
// depth reaches zero.
func matchBrace(s string, start int) int {
	depth := 0
	inString := false
	escape := false

	for k := start; k < len(s); k++ {
		c := s[k]

		if escape {
			escape = false
			continue
		}

		if inString {
			switch c {
			case '\\':
				escape = true
			case '"':
				inString = false
			}

			continue
		}

		switch c {
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			depth--

			if depth == 0 {
				return k
			}
		}
	}

	return -1
}

// lastVerdict returns the last `verdict` event in the stream — agents sometimes
// emit multiple verdicts; the protocol says the FINAL one is authoritative. Falls
// back to the empty AgentVerdict when no verdict event was emitted; consumers'
// default switch arms handle both cases the same way.
func lastVerdict(events []map[string]any) (AgentVerdict, string) {
	var v AgentVerdict
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

func loadPrompt(role AgentRole) string {
	return loadEmbedded("prompts/"+string(role)+".txt") + "\n\n" + loadEmbedded("prompts/common.txt")
}

func loadEmbedded(path string) string {
	return string(Throw2(embeddedPrompts.ReadFile(path)))
}
