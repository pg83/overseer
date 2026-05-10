package main

import (
	"path/filepath"
	"strings"
)

// Gemini drives Google's `gemini` CLI (gemini-cli) as the agent harness. The CLI
// prints incremental text to stdout and emits structured tool-call events when
// `--output-format json` is set. Final assistant text is the concatenation of
// `text` events; tool invocations come through as `tool_call` / `function_call`.
type Gemini struct {
	bin string
}

func NewGemini(bin string) *Gemini {
	return &Gemini{bin: bin}
}

func (g *Gemini) Name() string { return "gemini" }
func (g *Gemini) Bin() string  { return g.bin }

// Args invokes the gemini CLI in non-interactive mode. Prompt is fed via stdin
// (`-p -` reads stdin). --yolo bypasses the per-tool confirmation prompt; the
// orchestrator's jail is the only sandbox we trust.
func (g *Gemini) Args(model, wsAbs, _ string) []string {
	args := []string{
		"-p", "-",
		"--output-format", "json",
		"--yolo",
	}

	if model != "" {
		args = append(args, "--model", model)
	}

	if wsAbs != "" {
		args = append(args, "--include-directories", wsAbs)
	}

	// TODO: gemini-cli supports `--resume <uuid>` for existing sessions but
	// generates its own UUID at create-time (no custom-ID-at-create). Mapping-
	// file approach (capture UUID first run, store under <orch-root>/sessions/
	// gemini/<sessionID>.id, --resume on subsequent) — not implemented yet.
	return args
}

func (g *Gemini) JailRWPaths(home string) []string {
	return []string{
		"/tmp",
		filepath.Join(home, ".cache"),
		filepath.Join(home, "go"),
		filepath.Join(home, ".gemini"),
		filepath.Join(home, ".config", "gemini"),
	}
}

func (g *Gemini) DefaultModel(_ AgentRole) string { return "" }

func (g *Gemini) ParseStreamLine(ev map[string]any, finalText *strings.Builder, fault *streamErr, role AgentRole, ticket int) {
	switch t, _ := ev["type"].(string); t {
	case "text":
		// Streaming assistant text chunk. Add a trailing newline if missing —
		// otherwise consecutive chunks glue together and JSON-event lines lose
		// their column-0 anchor (see opencode.go for the same fix).
		if txt, _ := ev["text"].(string); txt != "" {
			finalText.WriteString(txt)

			if !strings.HasSuffix(txt, "\n") {
				finalText.WriteByte('\n')
			}
		}
	case "content":
		// Some gemini-cli versions emit a `content` event with the full message.
		if txt, _ := ev["text"].(string); txt != "" {
			finalText.WriteString(txt)

			if !strings.HasSuffix(txt, "\n") {
				finalText.WriteByte('\n')
			}
		}
	case "tool_call", "function_call":
		g.traceToolCall(role, ticket, ev)
	case "error":
		fault.record(g.extractErrorMsg(ev))
	}
}

// ClassifyFault: Google-side transient signatures plus the shared network set.
// Gemini surfaces quota / safety / overload errors with distinctive wording —
// grow the list as production reveals new patterns.
func (g *Gemini) ClassifyFault(f *agentFault) (bool, string) {
	if ok, reason := classifyTransientNetworkFault(f.stderr, f.stdout); ok {
		return true, reason
	}

	haystack := strings.ToLower(f.stderr + "\n" + f.stdout)

	for _, p := range []string{
		"rate limit",
		"resource_exhausted",
		"resource has been exhausted",
		"429 too many requests",
		"too many requests",
		"quota exceeded",
		"quota_exceeded",
		"deadline_exceeded",
		"unavailable",
		"the model is overloaded",
		"server overloaded",
	} {
		if strings.Contains(haystack, p) {
			return true, "google rate-limit/quota: " + p
		}
	}

	return faultUnknown(f)
}

func (g *Gemini) traceToolCall(role AgentRole, ticket int, ev map[string]any) {
	name, _ := ev["name"].(string)

	if name == "" {
		// Some shapes wrap the call in a sub-object.
		if call, _ := ev["call"].(map[string]any); call != nil {
			name, _ = call["name"].(string)
		}
	}

	if name == "" {
		return
	}

	args, _ := ev["args"].(map[string]any)

	if args == nil {
		args, _ = ev["arguments"].(map[string]any)
	}

	if args == nil {
		if call, _ := ev["call"].(map[string]any); call != nil {
			args, _ = call["args"].(map[string]any)
		}
	}

	ui("·", role, ticket, name, summarizeToolInput(name, args))
}

func (g *Gemini) extractErrorMsg(ev map[string]any) string {
	if s, _ := ev["message"].(string); s != "" {
		return s
	}

	if e, _ := ev["error"].(map[string]any); e != nil {
		if s, _ := e["message"].(string); s != "" {
			return s
		}

		if s, _ := e["status"].(string); s != "" {
			return s
		}
	}

	return "unknown"
}
