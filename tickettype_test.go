package main

import (
	"strings"
	"testing"
)

func TestPlanTicketTaskerSuccessTransitionsToPlanned(t *testing.T) {
	o := testOrchestratorForArbiter(t, Ticket{N: 1, Type: TicketTypePlan, Phase: PhasePlan, Descr: "plan", Prio: 1})
	o.jobs[RoleOverseer] = make(chan Job, 1)

	o.onTasker(AgentResult{
		Role:      RoleTasker,
		Ticket:    1,
		Workspace: "ws-tasker",
		Events: []map[string]any{
			{"type": "plan", "body": "do X"},
		},
	})

	got, _ := o.findTicket(1)

	if got.Phase != PhasePlanned {
		t.Fatalf("phase = %s, want %s", got.Phase, PhasePlanned)
	}

	if !planExists(o.Root, 1) {
		t.Fatalf("expected plan file for T-1")
	}
}

func TestLegacyTaskerSuccessTransitionsToImplement(t *testing.T) {
	o := testOrchestratorForArbiter(t, Ticket{N: 1, Phase: PhasePlan, Descr: "legacy", Prio: 1})

	o.onTasker(AgentResult{
		Role:      RoleTasker,
		Ticket:    1,
		Workspace: "ws-tasker",
		Events: []map[string]any{
			{"type": "plan", "body": "do X"},
		},
	})

	got, _ := o.findTicket(1)

	if got.Phase != PhaseImplement {
		t.Fatalf("phase = %s, want %s", got.Phase, PhaseImplement)
	}
}

func TestCodeTicketEscalateResumesAtImplement(t *testing.T) {
	o := testOrchestratorForArbiter(t, Ticket{N: 1, Type: TicketTypeCode, Phase: PhaseEscalate, Descr: "code", Prio: 1})
	o.replannerBusy = true
	o.replanOwned = []int{1}

	o.onReplanner(AgentResult{Role: RoleReplanner})

	got, _ := o.findTicket(1)

	if got.Phase != PhaseImplement {
		t.Fatalf("phase = %s, want %s", got.Phase, PhaseImplement)
	}
}

func TestPlanTicketEscalateResumesAtPlan(t *testing.T) {
	o := testOrchestratorForArbiter(t, Ticket{N: 1, Type: TicketTypePlan, Phase: PhaseEscalate, Descr: "plan", Prio: 1})
	o.replannerBusy = true
	o.replanOwned = []int{1}

	o.onReplanner(AgentResult{Role: RoleReplanner})

	got, _ := o.findTicket(1)

	if got.Phase != PhasePlan {
		t.Fatalf("phase = %s, want %s", got.Phase, PhasePlan)
	}
}

func TestApplyTaskOpNewRequiresTicketType(t *testing.T) {
	exc := Try(func() {
		_ = applyTaskOp(nil, map[string]any{
			"op":    "new",
			"n":     1,
			"descr": "x",
			"prio":  1,
			"deps":  []any{},
		})
	})

	if exc == nil {
		t.Fatalf("expected missing ticket_type to fail")
	}
}

func TestApplyTaskOpNewCodeStartsAtImplement(t *testing.T) {
	got := applyTaskOp(nil, map[string]any{
		"op":          "new",
		"n":           1,
		"ticket_type": "code",
		"descr":       "x",
		"prio":        1,
		"deps":        []any{},
	})

	if got[0].Type != TicketTypeCode {
		t.Fatalf("type = %q, want %q", got[0].Type, TicketTypeCode)
	}

	if got[0].Phase != PhaseImplement {
		t.Fatalf("phase = %s, want %s", got[0].Phase, PhaseImplement)
	}
}

func TestApplyLogEventCreateCodeStartsAtImplement(t *testing.T) {
	got := applyLogEvent(nil, LogEvent{
		"k":     "create",
		"n":     1,
		"type":  "code",
		"descr": "x",
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

func TestBuildAgentInputIncludesDependencyPlansForCodeTicket(t *testing.T) {
	o := testOrchestratorForArbiter(t, Ticket{N: 9, Type: TicketTypeCode, Phase: PhaseImplement, Descr: "code", Prio: 1, Deps: []int{7}})
	writePlan(o.Root, 7, "dep plan")

	input := o.buildAgentInput(RoleDigger, o.Tickets[0], "/tmp/ws")

	if !strings.Contains(input, "DEPENDENCY_PLANS") {
		t.Fatalf("missing dependency plans block")
	}

	if !strings.Contains(input, "T-7:\n") {
		t.Fatalf("missing dependency plan label")
	}

	if !strings.Contains(input, "dep plan") {
		t.Fatalf("missing dependency plan body")
	}
}
