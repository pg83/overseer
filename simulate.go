package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// simulate, set by `run --sim`, replaces real agent runs with synthesized
// AgentResults and stubs every real side effect (workspace clones, trunk git, plan
// reads). It exists to drive the coordinator state machine through huge, fast runs
// and watch for stuck tickets — no harness, no tokens, no repo writes. Set once at
// startup and never mutated, so all goroutines read it lock-free.
var simulate bool

// simMaxTickets caps how many tickets the simulated lead will ever create
// (0 = unbounded). A finite cap lets a sim run wind down — the backlog drains and
// stays drained — so the stop path (end_project -> GOALS_ACHIEVED) gets exercised.
var simMaxTickets int

// simTicketN hands out fresh ticket numbers for the simulated lead. Seeded once
// (lazily) above any pre-existing N; only the serial lead pool touches it.
var simTicketN atomic.Int64

// simSpec parses the --sim flag. Bare --sim (or --sim=0) enables an open-ended sim;
// --sim=N enables one capped at N tickets. IsBoolFlag lets `--sim` work without a value.
type simSpec struct{}

func (simSpec) String() string { return "" }

func (simSpec) IsBoolFlag() bool { return true }

func (simSpec) Set(v string) error {
	simulate = true

	if v == "" || v == "true" {
		simMaxTickets = 0

		return nil
	}

	n, err := strconv.Atoi(v)

	if err != nil {
		return err
	}

	simMaxTickets = n

	return nil
}

// simulatedRun fabricates one agent result: a random think-time, always at least one
// message (for tracking), then role-appropriate events the coordinator consumes
// exactly as if a real harness had produced them. The lead thinks markedly longer
// than the worker roles — true in reality (a big planning model, minutes), and it widens
// the window in which a ticket it planned against advances under it, so the optimistic
// staleness guard actually gets exercised.
func (o *Orchestrator) simulatedRun(role AgentRole, ticket int, wsID, stdin string) AgentResult {
	think := time.Duration(rand.Int63n(int64(2 * time.Second)))

	if role == RoleLead {
		think += 2 * time.Second
	}

	time.Sleep(think)

	res := AgentResult{Role: role, Ticket: ticket, Workspace: wsID, Stdin: stdin, Usage: simUsage(), Events: simEvents(role, ticket, stdin)}

	for _, ev := range res.Events {
		if t, _ := ev["type"].(string); t == "message" {
			if text := messageText(ev); text != "" {
				o.noteMessage(role, ticket, text)
			}
		}
	}

	o.reportUsage(role, ticket, res.Usage)

	return res
}

// simEvents builds the synthetic event stream for one role: always a message, then a
// weighted verdict (or a plan / replan / task-op batch for the roles that don't emit
// verdicts), shaped so the verdict mix exercises every branch of the state machine.
func simEvents(role AgentRole, ticket int, stdin string) []map[string]any {
	msg := map[string]any{"type": "message", "text": fmt.Sprintf("[sim] %s on T-%d", role, ticket)}

	switch role {
	case RoleDigger:
		return []map[string]any{msg, simVerdict("READY", 0.80, "CANT_DO", 0.15, "ALGEDONIC", 0.05)}
	case RoleReviewer:
		return []map[string]any{msg, simVerdict("APPROVE", 0.70, "REWORK", 0.25, "DISCARD", 0.05)}
	case RoleMerger:
		return []map[string]any{msg, simVerdict("MERGED", 0.85, "MERGE_FAIL", 0.15)}
	case RoleArbiter:
		return []map[string]any{msg, simVerdict("CONTINUE", 0.70, "ESCALATE", 0.30)}
	case RoleTasker:
		if rand.Float64() < 0.90 {
			return []map[string]any{msg, {"type": "plan", "body": fmt.Sprintf("[sim] plan for T-%d", ticket)}}
		}

		// Can't-plan: a replan event is what lets jobTasker stop waiting for a plan;
		// onTasker then reads the absent plan as NO_PLAN and routes to the arbiter.
		return []map[string]any{msg, {"type": "replan", "reason": fmt.Sprintf("[sim] cannot plan T-%d", ticket)}}
	case RoleLead:
		// Handles every subagent context (start/end/algedonic/replan) the same way:
		// top the backlog up toward the target, which keeps a sim run churning
		// open-endedly (end_project never declares GOALS_ACHIEVED in sim).
		return append([]map[string]any{msg}, simReplanOps(stdin)...)
	}

	return []map[string]any{msg}
}

// simUsage fabricates a plausible per-run token tally so a sim run exercises the cost
// meter (real harnesses report this; sim has to make it up). Synthetic USD, not real.
func simUsage() RunUsage {
	in := 2000 + rand.Intn(8000)
	out := 500 + rand.Intn(2500)
	cache := rand.Intn(40000)

	return RunUsage{
		Input:  in,
		Cache:  cache,
		Output: out,
		USD:    float64(in)*3e-6 + float64(out)*15e-6 + float64(cache)*0.3e-6,
	}
}

// simVerdict picks one verdict from (name, weight) pairs whose weights sum to ~1.
func simVerdict(pairs ...any) map[string]any {
	r, acc := rand.Float64(), 0.0
	pick := ""

	for i := 0; i+1 < len(pairs); i += 2 {
		v, _ := pairs[i].(string)
		p, _ := pairs[i+1].(float64)
		acc += p
		pick = v

		if r < acc {
			break
		}
	}

	return map[string]any{"type": "verdict", "verdict": pick, "detail": "[sim] " + pick}
}

// simReplanOps tops the open-ticket count back up toward a target, so a sim run keeps
// a steady, bounded backlog churning through the pipeline. A fresh code ticket
// occasionally depends on a plan ticket created in the same batch, to exercise the
// plan -> PLANNED -> dependent-unblocks path. It also occasionally mutates an existing
// open ticket (cancel / update / replace) so those state-machine branches — and the
// optimistic-concurrency staleness guard, which fires when the chosen ticket advanced
// during the pass's think-time — get exercised, not just `new`. With --sim=N it stops
// creating once the project hits N tickets; when the backlog then drains in end_project
// it declares GOALS_ACHIEVED, exercising the run's stop path.
func simReplanOps(stdin string) []map[string]any {
	const target = 12

	simTicketN.CompareAndSwap(0, int64(parseMaxTicketN(stdin)))

	need := target - simOpenCount(stdin)

	if need > 8 {
		need = 8
	}

	if simMaxTickets > 0 {
		if budget := simMaxTickets - int(simTicketN.Load()); budget < need {
			need = budget
		}
	}

	if need <= 0 {
		// Backlog drained with the ticket cap exhausted → the simulated project is
		// done; declare it so the stop path (GOALS_ACHIEVED → StopCancel) runs.
		if simMaxTickets > 0 && simSubagent(stdin) == "end_project" {
			return []map[string]any{{"type": "verdict", "verdict": "GOALS_ACHIEVED", "detail": fmt.Sprintf("[sim] %d-ticket cap reached, backlog drained", simMaxTickets)}}
		}

		return nil
	}

	var ops []map[string]any

	// Occasionally mutate an existing open ticket. The target comes from this pass's
	// snapshot; by apply time the inner loop may have advanced it, so this is what drives
	// the staleness-guard path (gen mismatch → whole batch deferred) in addition to the
	// cancel / update / replace op handlers themselves.
	if open := simOpenTickets(stdin); len(open) >= 4 && rand.Float64() < 0.35 {
		v := open[rand.Intn(len(open))]

		switch r := rand.Float64(); {
		case r < 0.5:
			ops = append(ops, map[string]any{"type": "task", "op": "cancel", "n": v, "reason": "[sim] cancel for coverage"})
		case r < 0.8:
			ops = append(ops, map[string]any{"type": "task", "op": "update", "n": v, "deps": []int{}})
		default:
			ops = append(ops, map[string]any{"type": "task", "op": "replace", "from": v, "to": open[rand.Intn(len(open))]})
		}
	}

	lastPlan := 0

	for i := 0; i < need; i++ {
		n := int(simTicketN.Add(1))

		switch {
		case rand.Float64() < 0.2:
			ops = append(ops, simTaskOp(n, "plan", nil))
			lastPlan = n
		case lastPlan != 0 && rand.Float64() < 0.5:
			ops = append(ops, simTaskOp(n, "code", []int{lastPlan}))
		default:
			ops = append(ops, simTaskOp(n, "code", nil))
		}
	}

	return ops
}

func simTaskOp(n int, kind string, deps []int) map[string]any {
	op := map[string]any{
		"type":        "task",
		"op":          "new",
		"n":           n,
		"ticket_type": kind,
		"descr":       fmt.Sprintf("[sim] %s ticket T-%d", kind, n),
	}

	if len(deps) > 0 {
		op["deps"] = deps
	}

	return op
}

func parseMaxTicketN(stdin string) int {
	return simIntAfter(stdin, "MAX_TICKET_N:")
}

// simSubagent extracts the lead's subagent context from the rendered prompt — the
// backticked value in lead.txt's `SUBAGENT = `<ctx>`` heading. "" if absent.
func simSubagent(stdin string) string {
	const key = "SUBAGENT = `"
	i := strings.Index(stdin, key)

	if i < 0 {
		return ""
	}

	rest := stdin[i+len(key):]

	if j := strings.IndexByte(rest, '`'); j >= 0 {
		return rest[:j]
	}

	return ""
}

func simIntAfter(s, key string) int {
	i := strings.Index(s, key)

	if i < 0 {
		return 0
	}

	var n int
	fmt.Sscanf(s[i+len(key):], "%d", &n)

	return n
}

// simOpenCount counts the open tickets the snapshot carries, by counting ticket
// objects in its OPEN_TICKETS section (each renders exactly one "descr").
func simOpenCount(stdin string) int {
	a := strings.Index(stdin, "OPEN_TICKETS")

	if a < 0 {
		return 0
	}

	seg := stdin[a:]

	if b := strings.Index(seg, "CLOSED_DEPS"); b >= 0 {
		seg = seg[:b]
	}

	return strings.Count(seg, "\"descr\"")
}

// simOpenTickets pulls the open-ticket numbers out of the snapshot's OPEN_TICKETS
// section (each ticket's pretty JSON carries one `"n": N` line), so the sim lead
// can target a real existing ticket for a cancel / update / replace op.
func simOpenTickets(stdin string) []int {
	a := strings.Index(stdin, "OPEN_TICKETS")

	if a < 0 {
		return nil
	}

	seg := stdin[a:]

	if b := strings.Index(seg, "CLOSED_DEPS"); b >= 0 {
		seg = seg[:b]
	}

	var out []int

	for _, line := range strings.Split(seg, "\n") {
		if line = strings.TrimSpace(line); strings.HasPrefix(line, "\"n\":") {
			out = append(out, simIntAfter(line, "\"n\":"))
		}
	}

	return out
}
