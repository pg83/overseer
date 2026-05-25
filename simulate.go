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

// simMaxTickets caps how many tickets the simulated replanner will ever create
// (0 = unbounded). A finite cap lets a sim run wind down — the backlog drains and
// stays drained — so the stop path (end_project -> GOALS_ACHIEVED) gets exercised.
var simMaxTickets int

// simTicketN hands out fresh ticket numbers for the simulated replanner. Seeded once
// (lazily) above any pre-existing N; only the serial replanner pool touches it.
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
// exactly as if a real harness had produced them.
func (o *Orchestrator) simulatedRun(role AgentRole, ticket int, wsID, stdin string) AgentResult {
	time.Sleep(time.Duration(rand.Int63n(int64(2 * time.Second))))

	res := AgentResult{Role: role, Ticket: ticket, Workspace: wsID, Events: simEvents(role, ticket, stdin)}

	for _, ev := range res.Events {
		if t, _ := ev["type"].(string); t == "message" {
			if text := messageText(ev); text != "" {
				o.noteMessage(role, ticket, text)
			}
		}
	}

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
	case RoleReplanner:
		// Handles every subagent context (start/end/algedonic/replan) the same way:
		// top the backlog up toward the target, which keeps a sim run churning
		// open-endedly (end_project never declares GOALS_ACHIEVED in sim).
		return append([]map[string]any{msg}, simReplanOps(stdin)...)
	}

	return []map[string]any{msg}
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
// plan -> PLANNED -> dependent-unblocks path. With --sim=N it stops creating once the
// project hits N tickets; when the backlog then drains in end_project it declares
// GOALS_ACHIEVED, exercising the run's stop path.
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
		if simMaxTickets > 0 && simStrAfter(stdin, "SUBAGENT:") == "end_project" {
			return []map[string]any{{"type": "verdict", "verdict": "GOALS_ACHIEVED", "detail": fmt.Sprintf("[sim] %d-ticket cap reached, backlog drained", simMaxTickets)}}
		}

		return nil
	}

	var ops []map[string]any
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

// simStrAfter returns the whitespace-delimited token following key (e.g. the SUBAGENT
// value the coordinator wrote into the replanner input), "" if key is absent.
func simStrAfter(s, key string) string {
	i := strings.Index(s, key)

	if i < 0 {
		return ""
	}

	rest := strings.TrimSpace(s[i+len(key):])

	if j := strings.IndexAny(rest, " \t\r\n"); j >= 0 {
		rest = rest[:j]
	}

	return rest
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
