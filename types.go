package main

import (
	"context"
	"sync"
)

type State string

const (
	StateOpen   State = "OPEN"
	StateClosed State = "CLOSED"
)

type CloseReason string

const (
	// CloseMerged means the ticket's work landed in trunk via fast-forward.
	// Dependents of a MERGED ticket can rely on its work being present.
	CloseMerged CloseReason = "MERGED"

	// CloseDiscarded means the ticket won't ship — replanner cancelled it,
	// reviewer rejected it, digger gave up, tasker couldn't plan, or any
	// similar abandonment. Single state for "closed without merging" since
	// every consumer treats them identically (history-only, not a "todo").
	CloseDiscarded CloseReason = "DISCARDED"
)

type TicketEvent struct {
	Ts     string `json:"ts"`
	Kind   string `json:"kind"`
	Detail string `json:"detail,omitempty"`
}

type Ticket struct {
	N           int           `json:"n"`
	State       State         `json:"state"`
	Descr       string        `json:"descr"`
	Prio        int           `json:"prio"`
	Deps        []int         `json:"deps,omitempty"`
	Workspaces  []string      `json:"workspaces,omitempty"`
	CloseReason CloseReason   `json:"close_reason,omitempty"`
	Events      []TicketEvent `json:"events,omitempty"`

	// BounceCount counts cycle-back events on this ticket: each REVIEWER_REWORK,
	// MERGE_FAIL, MERGE_FF_FAIL increments it. Read by the bounce throttle that
	// fires the replanner only every Nth iteration so we don't overwhelm it on
	// every loop pass; also visible to the replanner itself in CURRENT_TASKS so
	// it can see "this ticket has bounced 30 times" and act accordingly.
	BounceCount int `json:"bounce_count,omitempty"`

	// In-memory only; never persisted.
	// Set true when the ticket enters the work pipeline (tasker / digger / reviewer / merger),
	// cleared only on terminal close. Source of truth for scheduleReady's "is this ticket busy".
	InProgress bool `json:"-"`
}

type AgentRole string

const (
	RoleTasker    AgentRole = "tasker"
	RoleDigger    AgentRole = "digger"
	RoleReviewer  AgentRole = "reviewer"
	RoleMerger    AgentRole = "merger"
	RoleReplanner AgentRole = "replanner"
	RoleOverseer  AgentRole = "overseer"

	// Arbiter is the cycle-internal escalation gate. When a digger →
	// reviewer / merger cycle hits a disagreement (REWORK / DISCARD /
	// MERGE_FAIL), the arbiter decides: keep iterating in the cycle, or
	// escalate to the full replanner. Replanner-lite, called on every
	// disagreement instead of bouncing the full replanner only every N.
	RoleArbiter AgentRole = "arbiter"
)


type AgentVerdict string

const (
	VerdictReady           AgentVerdict = "READY"
	VerdictCantDo          AgentVerdict = "CANT_DO"
	VerdictApprove         AgentVerdict = "APPROVE"
	VerdictRework          AgentVerdict = "REWORK"
	VerdictDiscard         AgentVerdict = "DISCARD"
	VerdictMerged        AgentVerdict = "MERGED"
	VerdictMergeFail     AgentVerdict = "MERGE_FAIL"
	VerdictGoalsAchieved AgentVerdict = "GOALS_ACHIEVED"

	// Arbiter verdicts.
	VerdictContinue AgentVerdict = "CONTINUE"
	VerdictEscalate AgentVerdict = "ESCALATE"
)

// AgentResult is pure transport: routing identity (Role/Ticket/Workspace), raw I/O
// (Args/Stdin/Stdout/RawStream), and the parsed JSON events the agent emitted on
// stdout. No role-specific fields — consumers walk Events and pull what they care
// about (verdict, plan body, set_tasks, cancel, ...) themselves. See scheduler.go
// for the per-role extractors and agent.go for parseEvents / lastVerdict helpers.
type AgentResult struct {
	Role      AgentRole
	Ticket    int
	Workspace string

	Args      []string
	Stdin     string
	Stdout    string
	RawStream string

	Events []map[string]any
}

type ReplanRequest struct {
	Source AgentRole
	Ticket int
	Reason string
}

type MergeRequest struct {
	Ticket    int
	Workspace string
	History   string
}

// ArbiterRequest is what the cycle hands to the arbiter when a disagreement
// surfaces (reviewer REWORK / DISCARD, merger MERGE_FAIL / FF_FAIL). The arbiter
// reads ticket history + workspace state + the trigger detail and decides
// CONTINUE (spawn next digger iteration) or ESCALATE (queue full replanner).
type ArbiterRequest struct {
	Ticket       int
	Workspace    string       // digger's branch workspace to continue on
	Source       AgentRole    // who triggered: reviewer or merger
	Trigger      AgentVerdict // REWORK / DISCARD / MERGE_FAIL
	Detail       string       // what the trigger source said
	RebaseTarget string       // for MERGE_FAIL: trunk head to rebase onto
	MergeOut     string       // for MERGE_FAIL: merge command output to surface in digger input
}

type OverseerRequest struct {
	Reason string
}

// HarnessModel pairs a Harness implementation with the model name to drive it. Empty
// Model means "let Harness.DefaultModel pick for this role".
type HarnessModel struct {
	Harness Harness
	Model   string
}

type Orchestrator struct {
	Root      string
	Trunk     string
	GoalsHash string
	JailBin   string

	// Bindings is the role → (harness, model) resolution table. Lookup precedence
	// (in harnessModelForRole):
	//   1. Bindings[<role-name>]   e.g. "tasker", "digger", "reviewer", ...
	//   2. Bindings["think"]       for tasker / replanner / overseer
	//   3. Bindings["work"]        for digger / reviewer
	//   4. Bindings["default"]     from --harness
	// "default" is required; the rest are optional overrides.
	Bindings map[string]HarnessModel

	Mu      sync.Mutex
	Tickets []Ticket

	AgentSem chan struct{}

	QReplanner chan ReplanRequest
	QMerger    chan MergeRequest
	QOverseer  chan OverseerRequest
	QArbiter   chan ArbiterRequest

	AgentDone chan AgentResult

	Wakeup chan struct{}

	StopCtx    context.Context
	StopCancel context.CancelFunc
	Stopped    chan struct{}
}
