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
	CloseMerged    CloseReason = "MERGED"
	CloseDiscarded CloseReason = "DISCARDED"
	CloseCancelled CloseReason = "CANCELLED"
)

type Ticket struct {
	N           int
	State       State
	Descr       string
	Prio        int
	Deps        []int
	Workspaces  []string
	CloseReason CloseReason

	// In-memory only; never persisted to TASKS.md.
	// Set true when the ticket enters the work pipeline (tasker / digger / reviewer / merger),
	// cleared only on terminal close. Source of truth for scheduleReady's "is this ticket busy".
	InProgress bool
}

type Backend string

const (
	BackendClaude   Backend = "claude"
	BackendOpencode Backend = "opencode"
)

type AgentRole string

const (
	RoleTasker    AgentRole = "tasker"
	RoleDigger    AgentRole = "digger"
	RoleReviewer  AgentRole = "reviewer"
	RoleMerger    AgentRole = "merger"
	RoleReplanner AgentRole = "replanner"
	RoleOverseer  AgentRole = "overseer"
)

type AgentRun struct {
	Role      AgentRole
	Ticket    int
	Workspace string
	Cancel    context.CancelFunc
	Done      chan AgentResult
}

type AgentVerdict string

const (
	VerdictReady           AgentVerdict = "READY"
	VerdictCantDo          AgentVerdict = "CANT_DO"
	VerdictApprove         AgentVerdict = "APPROVE"
	VerdictRework          AgentVerdict = "REWORK"
	VerdictDiscard         AgentVerdict = "DISCARD"
	VerdictMerged          AgentVerdict = "MERGED"
	VerdictMergeFail       AgentVerdict = "MERGE_FAIL"
	VerdictPlanWritten     AgentVerdict = "PLAN_WRITTEN"
	VerdictGoalsAchieved   AgentVerdict = "GOALS_ACHIEVED"
	VerdictNoAction        AgentVerdict = "NO_ACTION"
	VerdictReplanApplied   AgentVerdict = "REPLAN_APPLIED"
	VerdictReplanInvalid   AgentVerdict = "REPLAN_INVALID"
	VerdictCrashed         AgentVerdict = "CRASHED"
)

type AgentResult struct {
	Role        AgentRole
	Ticket      int
	Workspace   string
	Verdict     AgentVerdict
	Detail      string
	ReplanLines []string
	Messages    []string
	Args        []string
	Stdout      string
	Stderr      string
	RawStream   string
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

type OverseerRequest struct {
	Reason string
}

type Orchestrator struct {
	Root      string
	Trunk     string
	GoalsHash string
	Harness   string
	Backend   Backend
	JailBin   string

	// Models is the model-resolution table. Lookup precedence (in modelForRole):
	//   1. Models[<role-name>]      e.g. "tasker", "digger", "reviewer", ...
	//   2. Models["think"]          for overseer / replanner / tasker
	//   3. Models["work"]           for digger / reviewer
	//   4. Models["default"]        from --model
	// Empty string at every level falls through to backend's own default.
	Models map[string]string

	Mu       sync.Mutex
	Tickets  []Ticket
	Inflight map[int]*AgentRun

	AgentSem chan struct{}

	QReplanner chan ReplanRequest
	QMerger    chan MergeRequest
	QOverseer  chan OverseerRequest

	AgentDone chan AgentResult

	Wakeup chan struct{}

	StopCtx    context.Context
	StopCancel context.CancelFunc
	Stopped    chan struct{}
}
