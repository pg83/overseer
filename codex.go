package main

import (
	"path/filepath"
	"strings"
)

// Codex drives OpenAI's `codex` CLI (codex exec --json) as the agent harness.
// Reads the streaming-JSON output where every line is `{"id":..., "msg":{...}}`
// — the inner `msg.type` switches between agent_message_delta, agent_message,
// exec_command_begin/end, task_started, task_complete, etc.
type Codex struct {
	bin string
}

func NewCodex(bin string) *Codex {
	return &Codex{bin: bin}
}

func (c *Codex) Name() string { return "codex" }
func (c *Codex) Bin() string  { return c.bin }

// Args invokes `codex exec` non-interactively with JSON streaming. Prompt is fed
// via stdin (codex exec reads it when no positional prompt is given). --cd pins
// the working dir; --skip-git-repo-check tolerates the workspace being a fresh
// clone with no commits yet.
func (c *Codex) Args(model, wsAbs, _ string) []string {
	args := []string{
		"exec",
		"--json",
		"--skip-git-repo-check",
		"--dangerously-bypass-approvals-and-sandbox",
	}

	if model != "" {
		args = append(args, "--model", model)
	}

	if wsAbs != "" {
		args = append(args, "--cd", wsAbs)
	}

	// TODO: codex generates its own session UUID per `codex exec` and prints it
	// on stdout (`session id: <uuid>`); resumption is via a separate subcommand
	// `codex exec resume <uuid>`. To support cross-run memory: capture the UUID
	// from the stream on first run, store at <orch-root>/sessions/codex/<sessionID>.id,
	// switch the subcommand to `exec resume <uuid>` on subsequent runs. Not
	// implemented yet — every run is fresh.
	return args
}

func (c *Codex) JailRWPaths(home string) []string {
	return []string{
		"/tmp",
		filepath.Join(home, ".cache"),
		filepath.Join(home, "go"),
		filepath.Join(home, ".codex"),
	}
}

func (c *Codex) DefaultModel(_ AgentRole) string { return "" }

func (c *Codex) ParseStreamLine(ev map[string]any, finalText *strings.Builder, fault *streamErr, role AgentRole, ticket int) {
	msg, _ := ev["msg"].(map[string]any)

	if msg == nil {
		return
	}

	switch t, _ := msg["type"].(string); t {
	case "agent_message":
		// Final assistant text for this turn. Codex sends one of these per turn.
		if txt, _ := msg["message"].(string); txt != "" {
			finalText.WriteString(txt)

			if !strings.HasSuffix(txt, "\n") {
				finalText.WriteByte('\n')
			}
		}
	case "exec_command_begin":
		// Surface shell commands as tool traces. Codex inlines argv as a string
		// array in `command`; collapse for one-line UI.
		argv, _ := msg["command"].([]any)
		var parts []string

		for _, a := range argv {
			if s, _ := a.(string); s != "" {
				parts = append(parts, s)
			}
		}

		ui("·", role, ticket, "exec", strings.Join(parts, " "))
	case "patch_apply_begin":
		// File-edit tool trace.
		path, _ := msg["path"].(string)
		ui("·", role, ticket, "patch", path)
	case "error":
		fault.record(c.extractErrorMsg(msg))
	}
}

// ClassifyFault: OpenAI-side transient signatures plus the shared network set.
// Grow the list as we observe new codex-cli error messages.
func (c *Codex) ClassifyFault(f *agentFault) (bool, string) {
	if ok, reason := classifyTransientNetworkFault(f.stderr, f.stdout); ok {
		return true, reason
	}

	haystack := strings.ToLower(f.stderr + "\n" + f.stdout)

	for _, p := range []string{
		"rate limit",
		"rate_limit",
		"429 too many requests",
		"too many requests",
		"insufficient_quota",
		"quota exceeded",
		"server is overloaded",
		"server_overloaded",
		"the engine is currently overloaded",
	} {
		if strings.Contains(haystack, p) {
			return true, "openai rate-limit/quota: " + p
		}
	}

	return faultUnknown(f)
}

// extractErrorMsg pulls a human-readable error message out of a codex `error`
// event. Codex's error shape isn't fully stable across versions; check the
// common fields and fall back to a generic marker.
func (c *Codex) extractErrorMsg(msg map[string]any) string {
	if s, _ := msg["message"].(string); s != "" {
		return s
	}

	if e, _ := msg["error"].(map[string]any); e != nil {
		if s, _ := e["message"].(string); s != "" {
			return s
		}

		if s, _ := e["type"].(string); s != "" {
			return s
		}
	}

	return "unknown"
}
