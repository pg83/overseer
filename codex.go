package main

import (
	"path/filepath"
	"strings"
)

// Codex drives OpenAI's `codex` CLI (codex 0.130+, `codex exec --json`) as
// the agent harness. The --json output is a ThreadEvent stream tagged by `type`:
// thread.started (carries thread_id, our session id), turn.started,
// item.{started,updated,completed} (with an inner item.type discriminating
// agent_message / reasoning / command_execution / file_change / mcp_tool_call /
// ...), turn.completed / turn.failed.
type Codex struct {
	bin string
}

func NewCodex(bin string) *Codex {
	return &Codex{bin: bin}
}

func (c *Codex) Name() string { return "codex" }
func (c *Codex) Bin() string  { return c.bin }

// Args invokes `codex exec` non-interactively with JSON streaming. Prompt is
// fed via stdin (codex exec reads it when no positional prompt is given). --cd
// pins the working dir; --skip-git-repo-check tolerates the workspace being a
// fresh clone with no commits yet. No --dangerously-bypass-approvals-and-sandbox:
// it triggers codex's bypass path that tries to write to ~/.codex even when
// that dir is on a read-only mount, surfacing as a confusing EROFS at start.
// Defaults work fine for plan-style read/grep workloads.
func (c *Codex) Args(model, wsAbs string) []string {
	args := []string{
		"exec",
		"--json",
		"--skip-git-repo-check",
	}

	if model != "" {
		args = append(args, "--model", model)
	}

	if wsAbs != "" {
		args = append(args, "--cd", wsAbs)
	}

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
	t, _ := ev["type"].(string)

	switch t {
	case "item.completed":
		item, _ := ev["item"].(map[string]any)

		if item == nil {
			return
		}

		switch itype, _ := item["type"].(string); itype {
		case "agent_message":
			if txt, _ := item["text"].(string); txt != "" {
				finalText.WriteString(txt)

				if !strings.HasSuffix(txt, "\n") {
					finalText.WriteByte('\n')
				}
			}
		case "command_execution":
			cmd, _ := item["command"].(string)
			ui("·", role, ticket, "exec", strings.ReplaceAll(cmd, "\n", " ⏎ "))
		case "file_change":
			ui("·", role, ticket, "patch", c.firstChangedPath(item))
		case "mcp_tool_call":
			server, _ := item["server"].(string)
			tool, _ := item["tool"].(string)
			ui("·", role, ticket, "mcp", server+"/"+tool)
		case "error":
			msg, _ := item["message"].(string)
			fault.record(msg)
		}
	case "turn.failed":
		errObj, _ := ev["error"].(map[string]any)
		msg := "unknown"

		if errObj != nil {
			if m, _ := errObj["message"].(string); m != "" {
				msg = m
			}
		}

		fault.record(msg)
	case "error":
		fault.record(c.extractErrorMsg(ev))
	}
}

// firstChangedPath grabs a representative path from a file_change item's
// `changes: [{path,...}]` array for the one-line UI trace. Falls back to a
// "<file change>" placeholder if the array is empty or shaped unexpectedly.
func (c *Codex) firstChangedPath(item map[string]any) string {
	changes, _ := item["changes"].([]any)

	for _, ch := range changes {
		m, _ := ch.(map[string]any)

		if m == nil {
			continue
		}

		if p, _ := m["path"].(string); p != "" {
			return p
		}
	}

	return "<file change>"
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

// SupportsSession: codex 0.130 resumes via the positional sub-subcommand
// `codex exec resume <session-id>`. On a fresh turn we use plain `codex exec`
// and capture the session id emitted in the `thread.started` event (top-level
// `thread_id`) for the next turn.
func (c *Codex) SupportsSession() bool { return true }

func (c *Codex) SessionArgs(model, wsAbs, sessionID string) []string {
	if sessionID == "" {
		return c.Args(model, wsAbs)
	}

	// `codex exec resume` is a sub-subcommand with its OWN narrow flag set —
	// the global flags flattened onto `exec` (--cd, --model, ...) are NOT
	// propagated through. Runtime usage line is literally:
	//   codex exec resume --json --skip-git-repo-check <SESSION_ID> [PROMPT]
	// Anything else trips clap "unexpected argument" before stdin is even
	// read. cwd is still pinned via cmd.Dir; model is locked to whatever was
	// chosen on the first turn (sessions are model-bound).
	return []string{
		"exec",
		"resume", sessionID,
		"--json",
		"--skip-git-repo-check",
	}
}

// ParseSessionID extracts thread_id from the `thread.started` event — codex's
// thread id is its session id (the same value `codex exec resume <id>` takes).
// Emitted once at the very start of each run.
func (c *Codex) ParseSessionID(ev map[string]any) string {
	if t, _ := ev["type"].(string); t == "thread.started" {
		if tid, _ := ev["thread_id"].(string); tid != "" {
			return tid
		}
	}

	return ""
}

// LiveTextChunk surfaces text from completed (and updated, future-proof for
// when codex starts emitting streaming updates) agent_message and reasoning
// items. codex 0.130's jsonl event processor only fires item.completed for
// these today — there's no token-stream — so live display is granularity of
// "one logical message" rather than per-token.
func (c *Codex) LiveTextChunk(ev map[string]any) string {
	t, _ := ev["type"].(string)

	if t != "item.completed" && t != "item.updated" {
		return ""
	}

	item, _ := ev["item"].(map[string]any)

	if item == nil {
		return ""
	}

	switch itype, _ := item["type"].(string); itype {
	case "agent_message", "reasoning":
		if txt, _ := item["text"].(string); txt != "" {
			return txt + "\n"
		}
	}

	return ""
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
