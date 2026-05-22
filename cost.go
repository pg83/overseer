package main

import (
	"fmt"
	"sync/atomic"
)

// RunUsage is one agent run's token/cost tally, normalized across harnesses:
// Input is fresh (non-cached) input tokens, Cache is cache read+write, Output is
// output incl. reasoning. USD is the dollar cost when the harness reports one
// (claude-code's total_cost_usd); codex reports tokens only, so USD stays 0.
type RunUsage struct {
	Input  int
	Cache  int
	Output int
	USD    float64
}

func (u RunUsage) tokens() int { return u.Input + u.Cache + u.Output }

func (u RunUsage) any() bool { return u.tokens() != 0 || u.USD != 0 }

// meter is the process-wide running total, shown in every UI line's cost column.
// Lock-free: pool workers add to it concurrently while the coordinator never
// touches it, so plain atomics suffice (no coordinator-state invariant involved).
var meter costMeter

type costMeter struct {
	tokens   atomic.Int64
	microUSD atomic.Int64
}

func (m *costMeter) add(u RunUsage) {
	m.tokens.Add(int64(u.tokens()))
	m.microUSD.Add(int64(u.USD * 1e6))
}

// column renders the cumulative total for the fixed-width UI column: dollars when
// any harness reported a cost, otherwise humanized token count. "" before the
// first run (blank column).
func (m *costMeter) column() string {
	if usd := float64(m.microUSD.Load()) / 1e6; usd > 0 {
		return fmt.Sprintf("$%.2f", usd)
	}

	return humanTokens(int(m.tokens.Load()))
}

func humanTokens(n int) string {
	switch {
	case n <= 0:
		return ""
	case n < 1000:
		return fmt.Sprintf("%d", n)
	case n < 1_000_000:
		return fmt.Sprintf("%.0fk", float64(n)/1e3)
	case n < 1_000_000_000:
		return fmt.Sprintf("%.2fM", float64(n)/1e6)
	default:
		return fmt.Sprintf("%.2fB", float64(n)/1e9)
	}
}
