package main

import (
	"strings"
	"testing"
)

func TestArbiterWorkspaceFromTaskerNoPlan(t *testing.T) {
	o := testOrchestratorForArbiter(t, Ticket{N: 12, Phase: PhasePlan, Descr: "t", Prio: 1})

	o.onTasker(AgentResult{
		Role:      RoleTasker,
		Ticket:    12,
		Workspace: "ws-tasker",
	})

	assertArbiterJobWorkspace(t, o, 12, "ws-tasker")
}

func TestPlanTicketTaskerSuccessRoutesToArbiter(t *testing.T) {
	o := testOrchestratorForArbiter(t, Ticket{N: 13, Type: TicketTypePlan, Phase: PhasePlan, Descr: "plan", Prio: 1})

	o.onTasker(AgentResult{
		Role:      RoleTasker,
		Ticket:    13,
		Workspace: "ws-tasker",
		Events: []map[string]any{
			{"type": "plan", "body": "research artifact"},
		},
	})

	got, ok := o.findTicket(13)
	if !ok {
		t.Fatalf("ticket not found")
	}

	if got.Phase != PhaseArbitrate {
		t.Fatalf("phase = %s, want %s", got.Phase, PhaseArbitrate)
	}

	if o.arb[13].trigger != VerdictPlanWritten {
		t.Fatalf("trigger = %s, want %s", o.arb[13].trigger, VerdictPlanWritten)
	}

	assertArbiterJobWorkspace(t, o, 13, "ws-tasker")
}

func TestArbiterWorkspaceFromDiggerCantDo(t *testing.T) {
	o := testOrchestratorForArbiter(t, Ticket{N: 1, Phase: PhaseImplement, Descr: "t", Prio: 1})

	o.onDigger(AgentResult{
		Role:      RoleDigger,
		Ticket:    1,
		Workspace: "ws-digger",
		Events: []map[string]any{
			{"type": "verdict", "verdict": string(VerdictCantDo), "detail": "blocked"},
		},
	})

	assertArbiterJobWorkspace(t, o, 1, "ws-digger")
}

func TestArbiterWorkspaceFromReviewerRework(t *testing.T) {
	o := testOrchestratorForArbiter(t, Ticket{N: 2, Phase: PhaseReview, Descr: "t", Prio: 1})
	o.branchWS[2] = "ws-digger"

	o.onReviewer(AgentResult{
		Role:      RoleReviewer,
		Ticket:    2,
		Workspace: "ws-digger",
		Events: []map[string]any{
			{"type": "verdict", "verdict": string(VerdictRework), "detail": "fix it"},
		},
	})

	assertArbiterJobWorkspace(t, o, 2, "ws-digger")
}

func TestArbiterWorkspaceFromReviewerDiscard(t *testing.T) {
	o := testOrchestratorForArbiter(t, Ticket{N: 3, Phase: PhaseReview, Descr: "t", Prio: 1})
	o.branchWS[3] = "ws-digger"

	o.onReviewer(AgentResult{
		Role:      RoleReviewer,
		Ticket:    3,
		Workspace: "ws-digger",
		Events: []map[string]any{
			{"type": "verdict", "verdict": string(VerdictDiscard), "detail": "wrong ticket"},
		},
	})

	assertArbiterJobWorkspace(t, o, 3, "ws-digger")
}

func TestArbiterWorkspaceFromMergerMergeFailUsesBranchWorkspace(t *testing.T) {
	o := testOrchestratorForArbiter(t, Ticket{N: 4, Phase: PhaseMerge, Descr: "t", Prio: 1})
	o.branchWS[4] = "ws-digger"

	o.onMerger(AgentResult{
		Role:      RoleMerger,
		Ticket:    4,
		Workspace: "ws-merger",
		Events: []map[string]any{
			{"type": "verdict", "verdict": string(VerdictMergeFail), "detail": "tests regressed"},
		},
	})

	assertArbiterJobWorkspace(t, o, 4, "ws-digger")
}

func TestArbiterFallsBackToFreshWorkspaceWhenNoWorkspaceKnown(t *testing.T) {
	o := testOrchestratorForArbiter(t, Ticket{N: 5, Phase: PhaseArbitrate, Descr: "t", Prio: 1})
	o.arb[5] = arbCtx{trigger: VerdictNoPlan, detail: "missing workspace"}

	got, ok := o.findTicket(5)
	if !ok {
		t.Fatalf("ticket not found")
	}

	job := o.buildJob(got, RoleArbiter)
	if !job.NewWS {
		t.Fatalf("arbiter job should request fresh workspace when no workspace is known")
	}

	if job.WS != "" {
		t.Fatalf("arbiter job ws = %q, want empty when NewWS is set", job.WS)
	}
}

func TestArbiterContinueAfterPlanWrittenReturnsToPlan(t *testing.T) {
	o := testOrchestratorForArbiter(t, Ticket{N: 6, Type: TicketTypePlan, Phase: PhaseArbitrate, Descr: "plan", Prio: 1})
	o.arb[6] = arbCtx{trigger: VerdictPlanWritten, detail: "tasker produced plan", workspace: "ws-tasker"}

	o.onArbiter(AgentResult{
		Role:   RoleArbiter,
		Ticket: 6,
		Events: []map[string]any{
			{"type": "verdict", "verdict": string(VerdictContinue), "detail": "revise the plan"},
		},
	})

	got, ok := o.findTicket(6)
	if !ok {
		t.Fatalf("ticket not found")
	}

	if got.Phase != PhasePlan {
		t.Fatalf("phase = %s, want %s", got.Phase, PhasePlan)
	}
}

func TestAfterTerminalTriggersOverseerOnlyAtZeroOpen(t *testing.T) {
	o := testOrchestratorForArbiter(t,
		Ticket{N: 1, Phase: PhaseMerged, Descr: "done", Prio: 1},
		Ticket{N: 2, Phase: PhasePlan, Descr: "open", Prio: 1},
	)
	o.jobs = map[AgentRole]chan Job{RoleOverseer: make(chan Job, 1)}

	o.afterTerminal(1, "MERGED")

	if o.overseerBusy {
		t.Fatalf("overseer should not start while an open ticket remains")
	}

	o.Tickets[1].Phase = PhaseMerged
	o.afterTerminal(2, "MERGED")

	if !o.overseerBusy {
		t.Fatalf("overseer should start when open tickets reach zero")
	}
}

func TestApplyReplannerOpsCancelThenNewDoesNotTriggerOverseerMidBatch(t *testing.T) {
	o := testOrchestratorForArbiter(t, Ticket{N: 1, Phase: PhasePlan, Descr: "old", Prio: 1})
	o.jobs = map[AgentRole]chan Job{RoleOverseer: make(chan Job, 1)}

	ops := []map[string]any{
		{"type": "task", "op": "cancel", "n": 1, "reason": "replace"},
		{"type": "task", "op": "new", "n": 2, "ticket_type": string(TicketTypeCode), "descr": "new", "prio": 1, "deps": []any{}},
	}

	o.applyReplannerOps(AgentResult{}, ops)

	if o.overseerBusy {
		t.Fatalf("overseer should not start when replanner batch leaves open tickets")
	}
}

func TestApplyReplannerOpsCancelLastTicketTriggersOverseer(t *testing.T) {
	o := testOrchestratorForArbiter(t, Ticket{N: 1, Phase: PhasePlan, Descr: "old", Prio: 1})
	o.jobs = map[AgentRole]chan Job{RoleOverseer: make(chan Job, 1)}

	ops := []map[string]any{
		{"type": "task", "op": "cancel", "n": 1, "reason": "done"},
	}

	o.applyReplannerOps(AgentResult{}, ops)

	if !o.overseerBusy {
		t.Fatalf("overseer should start when replanner batch leaves zero open tickets")
	}
}

func TestLegacyCreateDefaultsToCodeImplement(t *testing.T) {
	got := applyLogEvent(nil, LogEvent{
		"k":     "create",
		"n":     1,
		"descr": "legacy",
		"prio":  1,
		"deps":  []any{},
	})

	if got[0].Type != TicketTypeCode {
		t.Fatalf("type = %q, want %q", got[0].Type, TicketTypeCode)
	}

	if got[0].Phase != PhaseImplement {
		t.Fatalf("phase = %s, want %s", got[0].Phase, PhaseImplement)
	}
}

func TestLegacyPlanPhaseNormalizesToImplementForCodeTicket(t *testing.T) {
	tickets := applyLogEvent(nil, LogEvent{
		"k":     "create",
		"n":     1,
		"descr": "legacy",
		"prio":  1,
		"deps":  []any{},
	})

	got := applyLogEvent(tickets, LogEvent{
		"k":     "phase",
		"n":     1,
		"phase": string(PhasePlan),
	})

	if got[0].Phase != PhaseImplement {
		t.Fatalf("phase = %s, want %s", got[0].Phase, PhaseImplement)
	}
}

func TestApplyLogEventCreatePreservesDepsFromNativeIntSlice(t *testing.T) {
	got := applyLogEvent(nil, LogEvent{
		"k":     "create",
		"n":     15,
		"type":  "code",
		"descr": "deps",
		"prio":  1,
		"deps":  []int{3, 7},
	})

	if len(got[0].Deps) != 2 || got[0].Deps[0] != 3 || got[0].Deps[1] != 7 {
		t.Fatalf("deps = %#v, want []int{3,7}", got[0].Deps)
	}
}

func TestApplyLogEventUpdatePreservesDepsFromNativeIntSlice(t *testing.T) {
	tickets := applyLogEvent(nil, LogEvent{
		"k":     "create",
		"n":     15,
		"type":  "code",
		"descr": "deps",
		"prio":  1,
		"deps":  []any{},
	})

	got := applyLogEvent(tickets, LogEvent{
		"k":    "update",
		"n":    15,
		"deps": []int{3},
	})

	if len(got[0].Deps) != 1 || got[0].Deps[0] != 3 {
		t.Fatalf("deps = %#v, want []int{3}", got[0].Deps)
	}
}

func TestApplyTaskOpUpdateDepsAllowed(t *testing.T) {
	got := applyTaskOp([]Ticket{{N: 1, Type: TicketTypeCode, Phase: PhaseImplement, Descr: "x", Prio: 1}}, map[string]any{
		"op":   "update",
		"n":    1,
		"deps": []any{3, 5},
	})

	if len(got[0].Deps) != 2 || got[0].Deps[0] != 3 || got[0].Deps[1] != 5 {
		t.Fatalf("deps = %#v, want []int{3,5}", got[0].Deps)
	}
}

func TestApplyTaskOpUpdatePrioAllowed(t *testing.T) {
	got := applyTaskOp([]Ticket{{N: 1, Type: TicketTypeCode, Phase: PhaseImplement, Descr: "x", Prio: 1}}, map[string]any{
		"op":   "update",
		"n":    1,
		"prio": 7,
	})

	if got[0].Prio != 7 {
		t.Fatalf("prio = %d, want 7", got[0].Prio)
	}
}

func TestApplyTaskOpUpdateDepsAndPrioAllowed(t *testing.T) {
	got := applyTaskOp([]Ticket{{N: 1, Type: TicketTypeCode, Phase: PhaseImplement, Descr: "x", Prio: 1}}, map[string]any{
		"op":   "update",
		"n":    1,
		"prio": 9,
		"deps": []any{2},
	})

	if got[0].Prio != 9 {
		t.Fatalf("prio = %d, want 9", got[0].Prio)
	}

	if len(got[0].Deps) != 1 || got[0].Deps[0] != 2 {
		t.Fatalf("deps = %#v, want []int{2}", got[0].Deps)
	}
}

func TestApplyTaskOpUpdateNonDepsRejected(t *testing.T) {
	exc := Try(func() {
		_ = applyTaskOp([]Ticket{{N: 1, Type: TicketTypeCode, Phase: PhaseImplement, Descr: "x", Prio: 1}}, map[string]any{
			"op":    "update",
			"n":     1,
			"descr": "y",
		})
	})

	if exc == nil {
		t.Fatalf("expected op=update to be rejected")
	}
}

func TestApplyTaskOpReplaceRewritesOpenTicketDeps(t *testing.T) {
	got := applyTaskOp([]Ticket{
		{N: 1, Type: TicketTypeCode, Phase: PhaseImplement, Descr: "a", Prio: 1, Deps: []int{3, 7}},
		{N: 2, Type: TicketTypeCode, Phase: PhaseMerged, Descr: "b", Prio: 1, Deps: []int{3, 8}},
		{N: 3, Type: TicketTypeCode, Phase: PhaseImplement, Descr: "c", Prio: 1},
		{N: 9, Type: TicketTypeCode, Phase: PhaseImplement, Descr: "d", Prio: 1},
	}, map[string]any{
		"op":   "replace",
		"from": 3,
		"to":   9,
	})

	if len(got[0].Deps) != 2 || got[0].Deps[0] != 9 || got[0].Deps[1] != 7 {
		t.Fatalf("open deps = %#v, want []int{9,7}", got[0].Deps)
	}

	if len(got[1].Deps) != 2 || got[1].Deps[0] != 3 || got[1].Deps[1] != 8 {
		t.Fatalf("terminal deps should stay unchanged, got %#v", got[1].Deps)
	}
}

func TestApplyTaskOpReplaceDedupesDeps(t *testing.T) {
	got := applyTaskOp([]Ticket{
		{N: 1, Type: TicketTypeCode, Phase: PhaseImplement, Descr: "a", Prio: 1, Deps: []int{3, 9}},
		{N: 3, Type: TicketTypeCode, Phase: PhaseImplement, Descr: "from", Prio: 1},
		{N: 9, Type: TicketTypeCode, Phase: PhaseImplement, Descr: "to", Prio: 1},
	}, map[string]any{
		"op":   "replace",
		"from": 3,
		"to":   9,
	})

	if len(got[0].Deps) != 1 || got[0].Deps[0] != 9 {
		t.Fatalf("deps = %#v, want []int{9}", got[0].Deps)
	}
}

func testOrchestratorForArbiter(t *testing.T, tickets ...Ticket) *Orchestrator {
	t.Helper()

	root := t.TempDir()
	trunk := t.TempDir()

	return &Orchestrator{
		Root:        root,
		Trunk:       trunk,
		Tickets:     tickets,
		shadow:      map[int]Shadow{},
		branchWS:    map[int]string{},
		arb:         map[int]arbCtx{},
		jobs:        map[AgentRole]chan Job{},
		Bindings:    map[string]HarnessModel{"default": {Harness: testHarness{}}},
		Events:      make(chan AgentResult, 1),
		nudges:      nil,
		replanOwned: nil,
	}
}

type testHarness struct{}

func (testHarness) Name() string { return "test" }
func (testHarness) Bin() string  { return "/bin/true" }
func (testHarness) Args(model, wsAbs string) []string {
	return nil
}
func (testHarness) JailRWPaths(home string) []string { return nil }
func (testHarness) DefaultModel(role AgentRole) string {
	return ""
}
func (testHarness) ParseStreamLine(ev map[string]any, finalText *strings.Builder, fault *streamErr, role AgentRole, ticket int) {
}
func (testHarness) ClassifyFault(f *agentFault) (bool, string) { return false, "" }
func (testHarness) SupportsSession() bool                      { return false }
func (testHarness) SessionArgs(model, wsAbs, sessionID string) []string {
	return nil
}
func (testHarness) ParseSessionID(ev map[string]any) string { return "" }
func (testHarness) LiveTextChunk(ev map[string]any) string  { return "" }

func assertArbiterJobWorkspace(t *testing.T, o *Orchestrator, ticketN int, want string) {
	t.Helper()

	got, ok := o.findTicket(ticketN)
	if !ok {
		t.Fatalf("ticket %d not found", ticketN)
	}

	if got.Phase != PhaseArbitrate {
		t.Fatalf("ticket %d phase = %s, want %s", ticketN, got.Phase, PhaseArbitrate)
	}

	if o.arb[ticketN].workspace != want {
		t.Fatalf("ticket %d arb workspace = %q, want %q", ticketN, o.arb[ticketN].workspace, want)
	}

	job := o.buildJob(got, RoleArbiter)
	if job.NewWS {
		t.Fatalf("ticket %d arbiter job unexpectedly requested fresh workspace", ticketN)
	}

	if job.WS != want {
		t.Fatalf("ticket %d arbiter job ws = %q, want %q", ticketN, job.WS, want)
	}
}
