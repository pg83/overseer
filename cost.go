package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"sync/atomic"
)

// RunUsage is one agent run's token tally, normalized across harnesses: Input is
// fresh (non-cached) input tokens, Cache is cache read+write, Output is output
// incl. reasoning. USD is the dollar cost the harness synthesizes from these
// tokens via the embedded price table — never read from the API stream (we may
// be on a subscription where the stream carries no cost), and locked in per run
// so the project total stays stable even if the model later changes.
type RunUsage struct {
	Input  int
	Cache  int
	Output int
	USD    float64
}

func (u RunUsage) tokens() int { return u.Input + u.Cache + u.Output }

// model_prices_and_context_window.json is LiteLLM's maintained per-model price
// table (refresh by re-downloading it from BerriAI/litellm). Keyed by model id;
// costs are per-token.
//
//go:embed model_prices_and_context_window.json
var pricesJSON []byte

type modelPrice struct {
	In        float64 `json:"input_cost_per_token"`
	Out       float64 `json:"output_cost_per_token"`
	CacheRead float64 `json:"cache_read_input_token_cost"`
}

// Best-effort: a malformed table degrades to "no synthesized cost" (empty
// column) rather than crashing the orchestrator over a non-critical feature.
var priceTable = func() map[string]modelPrice {
	var m map[string]modelPrice
	_ = json.Unmarshal(pricesJSON, &m)

	return m
}()

// usdForModel synthesizes a run's dollar cost from its tokens under model key,
// pricing cache at the cache-read rate. ok=false when the model isn't in the
// table, so a harness can try an alias before giving up.
func usdForModel(key string, u RunUsage) (float64, bool) {
	p, ok := priceTable[key]

	if !ok {
		return 0, false
	}

	return float64(u.Input)*p.In + float64(u.Cache)*p.CacheRead + float64(u.Output)*p.Out, true
}

// meter is the process-wide running total shown in the UI cost column. Lock-free:
// pool workers add concurrently while the coordinator never touches it, so a plain
// atomic suffices (no coordinator-state invariant involved). Holds synthesized USD
// as micro-dollars — each run's cost was priced with its own model, so the sum is
// model-stable; it is display state, not persisted.
var meter costMeter

type costMeter struct {
	microUSD atomic.Int64
}

func (m *costMeter) add(u RunUsage) {
	m.microUSD.Add(int64(u.USD * 1e6))
}

func (m *costMeter) totalUSD() float64 {
	return float64(m.microUSD.Load()) / 1e6
}

// column renders the cumulative total for the fixed-width UI column; "" before
// the first priced run (blank column).
func (m *costMeter) column() string {
	if usd := float64(m.microUSD.Load()) / 1e6; usd > 0 {
		return fmt.Sprintf("$%.2f", usd)
	}

	return ""
}
