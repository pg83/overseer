package main

import (
	"crypto/sha1"
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

	// Args builds the harness CLI args (sans the binary path itself).
	//
	// model="" means "harness picks its own default". wsAbs is the workspace path;
	// harnesses that need an explicit --dir flag use it. sessionID is the
	// orchestrator-issued name for cross-run memory — replanner/overseer get a
	// stable per-root ID, work roles get a per-ticket ID. Each harness translates
	// it to whatever its CLI supports (e.g. UUID for claude, mapping file for
	// codex/opencode/gemini); empty sessionID means "no resumption, fresh run".
	Args(model, wsAbs, sessionID string) []string

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

// orchestratorSessionNS is the UUID5 namespace for orchestrator-issued session names.
// Any 16 bytes work; this is arbitrary but stable so deterministic UUIDs from session
// names don't drift across orchestrator versions. Bytes spell "OverseerSessionN".
var orchestratorSessionNS = []byte{
	0x4f, 0x76, 0x65, 0x72, 0x73, 0x65, 0x65, 0x72,
	0x53, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x4e,
}

// sessionUUID5 hashes an orchestrator-side session name into a deterministic RFC 4122
// version-5 UUID. Used by harnesses (currently just claude) whose CLI accepts a
// custom UUID at create-time so the same ID resumes the same session across runs.
func sessionUUID5(name string) string {
	h := sha1.New()
	h.Write(orchestratorSessionNS)
	h.Write([]byte(name))
	sum := h.Sum(nil)[:16]
	sum[6] = (sum[6] & 0x0f) | 0x50
	sum[8] = (sum[8] & 0x3f) | 0x80

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		sum[0:4], sum[4:6], sum[6:8], sum[8:10], sum[10:16])
}
