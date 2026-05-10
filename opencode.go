package main

import (
	"path/filepath"
	"strings"
)

// Opencode drives the opencode CLI (sst/opencode + Yandex's quasi_opencode fork) as
// the agent harness. Reads its `--format json` stdout: `type:"text"` chunks for
// final assistant text, `type:"tool_use"` for tool traces, `type:"error"` for
// stream-level failures.
type Opencode struct {
	bin string
}

func NewOpencode(bin string) *Opencode {
	return &Opencode{bin: bin}
}

func (o *Opencode) Name() string { return "opencode" }
func (o *Opencode) Bin() string  { return o.bin }

func (o *Opencode) Args(model, wsAbs, _ string) []string {
	args := []string{
		"run",
		"--format", "json",
		"--dangerously-skip-permissions",
	}

	if model != "" {
		args = append(args, "-m", model)
	}

	if wsAbs != "" {
		args = append(args, "--dir", wsAbs)
	}

	// TODO: opencode CLI's --session flag only accepts its own ses_xxx IDs (no
	// custom IDs at create). To support cross-run memory: maintain a mapping
	// file <orch-root>/sessions/opencode/<sessionID>.id that records the harness
	// ID after first run (parsed from stream events), then pass --session on
	// subsequent runs. Not implemented yet — every run is fresh.
	return args
}

func (o *Opencode) JailRWPaths(home string) []string {
	return []string{
		"/tmp",
		filepath.Join(home, ".cache"),
		filepath.Join(home, "go"),
		filepath.Join(home, ".config", "opencode"),
		filepath.Join(home, ".local", "share", "opencode"),
		filepath.Join(home, ".ya"),
	}
}

func (o *Opencode) DefaultModel(_ AgentRole) string { return "" }

func (o *Opencode) ParseStreamLine(ev map[string]any, finalText *strings.Builder, fault *streamErr, role AgentRole, ticket int) {
	switch t, _ := ev["type"].(string); t {
	case "text":
		part, _ := ev["part"].(map[string]any)

		if part == nil {
			return
		}

		txt, _ := part["text"].(string)

		if txt == "" {
			return
		}

		// Opencode streams logical chunks (one sentence / one JSON object per
		// event) without trailing newlines. Without inserting a newline we'd
		// concatenate prose and the agent's verdict line into one long string,
		// the JSON wouldn't be at column 0, and parseEvents would miss it.
		finalText.WriteString(txt)

		if !strings.HasSuffix(txt, "\n") {
			finalText.WriteByte('\n')
		}
	case "tool_use":
		o.traceToolUse(role, ticket, ev)
	case "error":
		fault.record(o.extractErrorMsg(ev))
	}
}

// ClassifyFault: opencode/quasi_opencode-side transient signatures plus the shared
// network set. Grow the rate-limit list as we observe new patterns from this CLI.
func (o *Opencode) ClassifyFault(f *agentFault) (bool, string) {
	if ok, reason := classifyTransientNetworkFault(f.stderr, f.stdout); ok {
		return true, reason
	}

	haystack := strings.ToLower(f.stderr + "\n" + f.stdout)

	for _, p := range []string{
		"rate limit",
		"too many requests",
		"quota exceeded",
		"server overloaded",
	} {
		if strings.Contains(haystack, p) {
			return true, "opencode rate-limit/quota: " + p
		}
	}

	return faultUnknown(f)
}

func (o *Opencode) traceToolUse(role AgentRole, ticket int, ev map[string]any) {
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

func (o *Opencode) extractErrorMsg(ev map[string]any) string {
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
