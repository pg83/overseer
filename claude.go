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

func (c *Claude) Args(model, _, sessionID string) []string {
	args := []string{
		"-p",
		"--output-format", "stream-json",
		"--verbose",
		"--dangerously-skip-permissions",
	}

	if model != "" {
		args = append(args, "--model", model)
	}

	// Claude is the only backend whose CLI accepts a custom UUID at create-time
	// (--session-id in -p mode). Subsequent runs with the same UUID append to the
	// same .jsonl transcript, giving us cross-run memory. Sessions are stored
	// per-cwd under ~/.claude/projects/<encoded-cwd>/, so resumption works only
	// when the working directory is stable across runs (which it is for digger /
	// reviewer / merger on a given ticket — they reuse the same workspace dir).
	if sessionID != "" {
		args = append(args, "--session-id", sessionUUID5(sessionID))
	}

	return args
}

func (c *Claude) JailRWPaths(home string) []string {
	return []string{
		"/tmp",
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

// traceAssistant pulls tool_use and text blocks out of a stream-json `assistant`
// event — tool blocks become UI traces, text blocks accumulate into finalText so
// embedded JSON events can be parsed downstream.
func (c *Claude) traceAssistant(finalText *strings.Builder, role AgentRole, ticket int, ev map[string]any) {
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
