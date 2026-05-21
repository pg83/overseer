package main

import "context"

// Phase is the persisted pipeline position of a ticket — the work it needs next.
// It is the single source of truth for ticket state (written to the event log),
// replacing the old OPEN/CLOSED + ephemeral Stage. The coordinator advances it on
// every agent verdict. PhaseMerged / PhaseDiscarded are terminal.
type Phase string

const (
	PhasePlan      Phase = "PLAN"      // needs a tasker (no plan yet)
	PhaseImplement Phase = "IMPLEMENT" // needs a digger
	PhaseReview    Phase = "REVIEW"    // needs a reviewer
	PhaseMerge     Phase = "MERGE"     // needs the merger
	PhaseArbitrate Phase = "ARBITRATE" // needs the arbiter (a disagreement surfaced)
	PhaseEscalate  Phase = "ESCALATE"  // needs the replanner (arbiter escalated)
	PhaseMerged    Phase = "MERGED"    // terminal: work landed in trunk
	PhaseDiscarded Phase = "DISCARDED" // terminal: dropped
)

func (p Phase) Terminal() bool {
	return p == PhaseMerged || p == PhaseDiscarded
}

// roleForPhase maps a non-terminal phase to the agent role that advances it. The
// coordinator's dispatch loop routes every STOPPED ticket to roleForPhase(phase).
func roleForPhase(p Phase) AgentRole {
	switch p {
	case PhasePlan:
		return RoleTasker
	case PhaseImplement:
		return RoleDigger
	case PhaseReview:
		return RoleReviewer
	case PhaseMerge:
		return RoleMerger
	case PhaseArbitrate:
		return RoleArbiter
	case PhaseEscalate:
		return RoleReplanner
	}

	return ""
}

// Shadow is the coordinator's in-memory scheduling state for a ticket — orthogonal
// to Phase. Only the coordinate goroutine touches it, so there is no lock.
// ShadowStopped = not in flight, eligible for dispatch; ShadowScheduled = handed to
// a role pool, an agent is (or will be) working it. A ticket goes Scheduled on
// dispatch and back to Stopped when its result returns.
type Shadow string

const (
	ShadowStopped   Shadow = "STOPPED"
	ShadowScheduled Shadow = "SCHEDULED"
)

type TicketEvent struct {
	Ts     string `json:"ts"`
	Kind   string `json:"kind"`
	Detail string `json:"detail,omitempty"`
}

type Ticket struct {
	N          int           `json:"n"`
	Phase      Phase         `json:"phase"`
	Descr      string        `json:"descr"`
	Prio       int           `json:"prio"`
	Deps       []int         `json:"deps,omitempty"`
	Workspaces []string      `json:"workspaces,omitempty"`
	Events     []TicketEvent `json:"events,omitempty"`
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
	// escalate to the full replanner.
	RoleArbiter AgentRole = "arbiter"
)

type AgentVerdict string

const (
	VerdictReady         AgentVerdict = "READY"
	VerdictCantDo        AgentVerdict = "CANT_DO"
	VerdictApprove       AgentVerdict = "APPROVE"
	VerdictRework        AgentVerdict = "REWORK"
	VerdictDiscard       AgentVerdict = "DISCARD"
	VerdictMerged        AgentVerdict = "MERGED"
	VerdictMergeFail     AgentVerdict = "MERGE_FAIL"
	VerdictGoalsAchieved AgentVerdict = "GOALS_ACHIEVED"

	// VerdictNoPlan is a synthetic trigger raised when a tasker fails to produce a
	// plan. The tasker doesn't emit it (it just emits no `plan` event); the
	// coordinator categorizes that absence as NO_PLAN for the arbiter's input.
	VerdictNoPlan AgentVerdict = "NO_PLAN"

	// Arbiter verdicts.
	VerdictContinue AgentVerdict = "CONTINUE"
	VerdictEscalate AgentVerdict = "ESCALATE"
)

// AgentResult is pure transport: routing identity (Role/Ticket/Workspace), raw I/O
// (Args/Stdin/Stdout/RawStream), and the parsed JSON events the agent emitted on
// stdout. A pool worker fills it and sends it to the coordinator, which walks
// Events and pulls what it cares about (verdict, plan body, task ops, ...) via the
// extractors in agent.go / coordinator.go. Workers never touch ticket state.
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

// ReplanReason is one nudge to the replanner — either an escalated ticket or a
// global signal (overseer guidance, post-merge fallout, GOALS.md change). The
// coordinator accumulates these and batches them into a single replanner Job.
type ReplanReason struct {
	Source AgentRole
	Ticket int
	Reason string
}

// Job is a unit of work the coordinator hands to a role pool. The coordinator is
// the only writer; a pool worker reads it, runs the harness, and replies with an
// AgentResult on Orchestrator.Events. One struct for all roles — each reads the
// fields it needs. Ticket is a snapshot; the coordinator owns the live copy.
type Job struct {
	Role   AgentRole
	Ticket Ticket

	// WS is the workspace the worker must use; if NewWS is set the worker creates a
	// fresh clone instead and reports it back in the result.
	WS    string
	NewWS bool

	// Arbiter context (PhaseArbitrate).
	Trigger      AgentVerdict
	Detail       string
	RebaseTarget string
	MergeOut     string

	// Replanner context (PhaseEscalate tickets + global nudges, batched).
	Reasons []ReplanReason

	// Overseer context.
	OverseerReason string

	// Snapshot is CURRENT_TASKS, rendered by the coordinator (which owns o.Tickets)
	// at dispatch time, for the replanner / overseer — so a worker never reads
	// coordinator state.
	Snapshot string
}

// HarnessModel pairs a Harness implementation with the model name to drive it. Empty
// Model means "let Harness.DefaultModel pick for this role".
type HarnessModel struct {
	Harness Harness
	Model   string
}

// arbCtx is the trigger context the coordinator remembers for a ticket parked in
// PhaseArbitrate / PhaseImplement-after-merge-fail, so it can build the right Job.
// Reconstructed from the phase event on replay (rebaseTarget / mergeOut are live
// only — a restart loses them and the digger simply rebases without the diff text).
type arbCtx struct {
	trigger      AgentVerdict
	detail       string
	rebaseTarget string
	mergeOut     string
}

// Orchestrator is the coordinator: a single goroutine owns all of the fields below
// (Tickets, shadow, branchWS, arb, nudges, the busy flags) — no mutex, because
// nothing else touches them. Role pools read Jobs from o.jobs[role] and reply on
// o.Events; that is the only cross-goroutine communication.
type Orchestrator struct {
	Root      string
	Trunk     string
	GoalsHash string
	Jail      []string
	ExtraRW   []string

	// Bindings is the role → (harness, model) resolution table — see
	// harnessModelForRole. "default" required; the rest optional overrides.
	Bindings map[string]HarnessModel

	// Coordinator-owned state (no lock).
	Tickets       []Ticket
	shadow        map[int]Shadow
	branchWS      map[int]string
	arb           map[int]arbCtx
	nudges        []ReplanReason
	replanOwned   []int
	replannerBusy bool
	overseerBusy  bool

	jobs   map[AgentRole]chan Job
	Events chan AgentResult

	StopCtx    context.Context
	StopCancel context.CancelFunc
	Stopped    chan struct{}
}

// poolSizes is the fixed number of workers per role — total harness concurrency is
// their sum (no shared semaphore). merger / replanner / overseer are serial; tune
// digger to control implementation parallelism.
var poolSizes = map[AgentRole]int{
	RoleTasker:    2,
	RoleDigger:    4,
	RoleReviewer:  2,
	RoleArbiter:   2,
	RoleMerger:    1,
	RoleReplanner: 1,
	RoleOverseer:  1,
}
