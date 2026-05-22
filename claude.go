package main

import (
	"path/filepath"
	"strings"
)

// Claude drives the claude-code CLI as the agent harness. Reads its stream-json
// output (`type:"assistant"` blocks for tool traces, `type:"result"` for the final
// concatenated text), runs through the Anthropic API.
type Claude struct {
	bin string
}

func NewClaude(bin string) *Claude {
	return &Claude{bin: bin}
}

func (c *Claude) Name() string { return "claude" }
func (c *Claude) Bin() string  { return c.bin }

func (c *Claude) Args(model, _ string) []string {
	args := []string{
		"-p",
		"--output-format", "stream-json",
		"--verbose",
		"--dangerously-skip-permissions",
	}

	if model != "" {
		args = append(args, "--model", model)
	}

	return args
}

func (c *Claude) JailRWPaths(home string) []string {
	return []string{
		filepath.Join(home, ".cache"),
		filepath.Join(home, "go"),
		filepath.Join(home, ".claude"),
		filepath.Join(home, ".claude.json"),
	}
}

// DefaultModel: claude-code's own default for a digger is the same big model as for
// the planner roles, which wastes capacity on bulk implementation work — pin diggers
// to sonnet so the cheaper model handles the high-volume role.
func (c *Claude) DefaultModel(role AgentRole) string {
	if role == RoleDigger {
		return "sonnet"
	}

	return ""
}

func (c *Claude) ParseStreamLine(ev map[string]any, finalText *strings.Builder, _ *streamErr, role AgentRole, ticket int) {
	c.traceAssistant(finalText, role, ticket, ev)

	if t, _ := ev["type"].(string); t == "result" {
		if txt, _ := ev["result"].(string); txt != "" {
			finalText.WriteString(txt)
		}
	}
}

// AccumulateUsage reads the single `result` event, which carries the run's
// cumulative token usage. claude's input_tokens excludes cache (cache is reported
// separately), so map it straight to fresh Input.
func (c *Claude) AccumulateUsage(ev map[string]any, u *RunUsage) {
	if t, _ := ev["type"].(string); t != "result" {
		return
	}

	usage, _ := ev["usage"].(map[string]any)

	if usage == nil {
		return
	}

	u.Input += jsonInt(usage["input_tokens"])
	u.Output += jsonInt(usage["output_tokens"])
	u.Cache += jsonInt(usage["cache_read_input_tokens"]) + jsonInt(usage["cache_creation_input_tokens"])
}

// CostUSD maps claude-code's model aliases (the bare names DefaultModel hands out)
// to current price-table keys, then prices the tokens.
func (c *Claude) CostUSD(model string, u RunUsage) float64 {
	switch model {
	case "sonnet":
		model = "claude-sonnet-4-5"
	case "opus":
		model = "claude-opus-4-7"
	case "haiku":
		model = "claude-haiku-4-5"
	}

	usd, _ := usdForModel(model, u)

	return usd
}

// ClassifyFault: Anthropic-side transient signatures plus the shared network set.
// Grow the rate-limit / quota list as we observe new claude-code error signatures.
func (c *Claude) ClassifyFault(f *agentFault) (bool, string) {
	if ok, reason := classifyTransientNetworkFault(f.stderr, f.stdout); ok {
		return true, reason
	}

	haystack := strings.ToLower(f.stderr + "\n" + f.stdout)

	for _, p := range []string{
		"rate limit",
		"rate_limit",
		"429 too many requests",
		"too many requests",
		"credit balance",
		"insufficient credit",
		"out of credits",
		"monthly limit",
		"usage limit",
		"overloaded_error",
	} {
		if strings.Contains(haystack, p) {
			return true, "anthropic rate-limit/quota: " + p
		}
	}

	return faultUnknown(f)
}

// SupportsSession: claude-code resumes a conversation via --resume <session-id>.
// The session id is emitted on the very first stream event (system/init).
func (c *Claude) SupportsSession() bool { return true }

// SessionArgs is the Args() shape with --resume <id> appended when continuing.
// On the first turn (sessionID==""), the call is plain — claude assigns and
// emits the id we then capture via ParseSessionID for subsequent turns.
func (c *Claude) SessionArgs(model, _, sessionID string) []string {
	args := []string{
		"-p",
		"--output-format", "stream-json",
		"--verbose",
		"--dangerously-skip-permissions",
	}

	if model != "" {
		args = append(args, "--model", model)
	}

	if sessionID != "" {
		args = append(args, "--resume", sessionID)
	}

	return args
}

// ParseSessionID looks for a top-level session_id on any event. claude-code
// emits {"type":"system","subtype":"init","session_id":"...",...} as the first
// stream line and also includes it on the final `result` event — first non-empty
// hit wins.
func (c *Claude) ParseSessionID(ev map[string]any) string {
	if sid, _ := ev["session_id"].(string); sid != "" {
		return sid
	}

	return ""
}

// LiveTextChunk pulls visible text out of an `assistant` event — claude-code
// emits these incrementally with content[]={text|tool_use|...} blocks; we
// concat the text blocks for live display. The final `result` event carries
// the same prose concatenated, so plan does NOT also surface it here (would
// duplicate everything).
func (c *Claude) LiveTextChunk(ev map[string]any) string {
	if t, _ := ev["type"].(string); t != "assistant" {
		return ""
	}

	msg, _ := ev["message"].(map[string]any)

	if msg == nil {
		return ""
	}

	content, _ := msg["content"].([]any)

	var sb strings.Builder

	for _, c := range content {
		block, _ := c.(map[string]any)

		if block == nil {
			continue
		}

		if btyp, _ := block["type"].(string); btyp == "text" {
			if txt, _ := block["text"].(string); txt != "" {
				sb.WriteString(txt)
			}
		}
	}

	return sb.String()
}

// traceAssistant pulls tool_use blocks out of a stream-json `assistant` event
// for UI traces. Text blocks are intentionally NOT accumulated into finalText —
// the final `result` event already carries the full concatenated text, and
// double-collecting duplicates every embedded JSON event downstream.
func (c *Claude) traceAssistant(_ *strings.Builder, role AgentRole, ticket int, ev map[string]any) {
	if t, _ := ev["type"].(string); t != "assistant" {
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

		if btyp, _ := block["type"].(string); btyp == "tool_use" {
			name, _ := block["name"].(string)
			input, _ := block["input"].(map[string]any)
			ui("·", role, ticket, name, summarizeToolInput(name, input))
		}
	}
}
