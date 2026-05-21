package main

import "testing"

func TestArbiterWorkspaceFromTaskerNoPlan(t *testing.T) {
	o := testOrchestratorForArbiter(t, Ticket{N: 12, Phase: PhasePlan, Descr: "t", Prio: 1})

	o.onTasker(AgentResult{
		Role:      RoleTasker,
		Ticket:    12,
		Workspace: "ws-tasker",
	})

	assertArbiterJobWorkspace(t, o, 12, "ws-tasker")
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

func testOrchestratorForArbiter(t *testing.T, ticket Ticket) *Orchestrator {
	t.Helper()

	root := t.TempDir()
	trunk := t.TempDir()

	return &Orchestrator{
		Root:     root,
		Trunk:    trunk,
		Tickets:  []Ticket{ticket},
		shadow:   map[int]Shadow{},
		branchWS: map[int]string{},
		arb:      map[int]arbCtx{},
	}
}

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
