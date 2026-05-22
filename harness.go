package main

import (
	"fmt"
	"path/filepath"
	"strings"
)

// Harness abstracts a CLI agent backend (claude-code, opencode, ...) that the
// orchestrator drives via a child process. Implementations supply the binary path,
// command-line shape, jail RW path set, JSON-stream parser, error classifier — every
// per-CLI bit lives behind this interface; the orchestrator stays generic.
type Harness interface {
	// Name is "claude" / "opencode" — used in self-block + diagnostics.
	Name() string

	// Bin is the absolute path to the harness binary.
	Bin() string

	// Args builds the harness CLI args (sans the binary path itself). model=""
	// means "harness picks its own default". wsAbs is the workspace path;
	// harnesses that need an explicit --dir flag use it.
	Args(model, wsAbs string) []string

	// JailRWPaths is the set of filesystem paths (config dirs, caches, ...) the
	// harness needs read-write inside the jail to function. The workspace and tmpdir
	// are added by the orchestrator on top — this list is the harness's own.
	JailRWPaths(home string) []string

	// DefaultModel is the harness's preferred default for a given role when no
	// --model / --<role>-model / --<group>-model was set. "" means "harness picks".
	DefaultModel(role AgentRole) string

	// ParseStreamLine consumes one JSONL event from the harness's stdout, accumulating
	// final assistant text, recording stream-level errors, and emitting UI tool
	// traces along the way.
	ParseStreamLine(ev map[string]any, finalText *strings.Builder, fault *streamErr, role AgentRole, ticket int)

	// ClassifyFault decides whether a one-attempt failure of THIS harness is worth
	// retrying. Different CLIs surface transient errors with different vocabulary
	// (HTTP codes vs cloud-provider words vs Russian quasi-opencode messages); the
	// classifier owns that knowledge.
	ClassifyFault(f *agentFault) (retryable bool, reason string)

	// AccumulateUsage folds one stream event's token/cost figures into u,
	// normalized to fresh-input / cache / output (+ USD when the harness reports
	// one). Harnesses that don't surface usage leave u untouched.
	AccumulateUsage(ev map[string]any, u *RunUsage)

	// SupportsSession reports whether this harness can resume a multi-turn dialog.
	// The `overseer plan` handler refuses to bind a non-supporting harness as PUPA
	// or LUPA. Orchestrator main loop ignores this — each agent run there is its
	// own fresh context, no resume needed.
	SupportsSession() bool

	// SessionArgs returns CLI args for either a fresh session (sessionID="") or a
	// resumed one (sessionID!=""). Only called when SupportsSession() returns true.
	// Each harness encapsulates whether it uses --session-id, --resume, or other
	// flag conventions.
	SessionArgs(model, wsAbs, sessionID string) []string

	// ParseSessionID extracts the session id from one stream event, "" if this event
	// doesn't carry one. Called on every event of the first turn until a non-empty
	// id is found; subsequent turns reuse it for resume.
	ParseSessionID(ev map[string]any) string

	// LiveTextChunk returns the human-readable text fragment carried by this stream
	// event — assistant text deltas, reasoning deltas — or "" if the event has no
	// text content (control events, tool calls, etc.). Used by `overseer plan` to
	// stream Pupa/Lupa output to stderr while the harness is still running, so cold
	// thinking phases don't look like a hang. Orchestrator main loop ignores it.
	LiveTextChunk(ev map[string]any) string
}

// SelectHarness picks the implementation by basename of the harness path —
// must contain one of "claude" / "opencode" / "codex" / "gemini". Anything else
// is a config bug.
func SelectHarness(harnessAbs string) Harness {
	base := strings.ToLower(filepath.Base(harnessAbs))

	switch {
	case strings.Contains(base, "opencode"):
		return NewOpencode(harnessAbs)
	case strings.Contains(base, "claude"):
		return NewClaude(harnessAbs)
	case strings.Contains(base, "codex"):
		return NewCodex(harnessAbs)
	case strings.Contains(base, "gemini"):
		return NewGemini(harnessAbs)
	}

	ThrowFmt("--harness %q: basename must contain one of claude/opencode/codex/gemini", harnessAbs)

	return nil
}

// classifyTransientNetworkFault matches the universal HTTP / TCP transient patterns
// shared by any cloud-backed CLI. Each Harness.ClassifyFault calls this first, then
// falls back to its own backend-specific patterns.
func classifyTransientNetworkFault(stderr, stdout string) (retryable bool, reason string) {
	haystack := strings.ToLower(stderr + "\n" + stdout)

	for _, p := range []string{
		"connection refused",
		"connection reset",
		"i/o timeout",
		"deadline exceeded",
		"no route to host",
		"temporary failure in name resolution",
		"name resolution failed",
		"tls handshake timeout",
		"tls handshake eof",
		"stream disconnected",
		"disconnected before completion",
		"reconnecting",
		"503 service unavailable",
		"502 bad gateway",
		"504 gateway timeout",
	} {
		if strings.Contains(haystack, p) {
			return true, "transient network: " + p
		}
	}

	return false, ""
}

// faultUnknown is the default "we don't know how to classify this" answer — caller
// turns this into a hard-stop. Each harness composes it as the final fall-through.
func faultUnknown(f *agentFault) (bool, string) {
	return false, fmt.Sprintf("exit=%d stderr=%q", f.exitCode, f.stderr)
}

