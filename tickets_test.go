package main

import (
	"bytes"
	"os"
	"testing"
)

func TestRenderOpenTicketsIncludesHistoryPlanAndChat(t *testing.T) {
	root := t.TempDir()

	writePlan(root, 7, "research plan")
	Throw(os.WriteFile(messagesLogPath(root), []byte("2026-01-01T00:00:00Z\ttasker\tT-7\tfirst note\n"), 0644))

	tickets := []Ticket{
		{
			N:     7,
			Type:  TicketTypePlan,
			Phase: PhasePlan,
			Descr: "investigate",
			Prio:  8,
			Deps:  []int{3},
			Workspaces: []string{
				"ws-1",
			},
			Events: []TicketEvent{
				{Ts: "2026-01-01T00:00:00Z", Kind: "TASK_NEW", Detail: "created"},
			},
		},
		{
			N:     8,
			Type:  TicketTypeCode,
			Phase: PhaseMerged,
			Descr: "closed",
			Prio:  1,
		},
	}

	var out bytes.Buffer
	renderOpenTickets(root, tickets, &out)
	got := out.String()

	assertContains(t, got, "T-7\n")
	assertContains(t, got, "type: plan\n")
	assertContains(t, got, "phase: PLAN\n")
	assertContains(t, got, "deps: [3]\n")
	assertContains(t, got, "path: "+ticketPlanPath(root, 7)+"\n")
	assertContains(t, got, "research plan\n")
	assertContains(t, got, "TASK_NEW")
	assertContains(t, got, "first note")

	if bytes.Contains(out.Bytes(), []byte("T-8\n")) {
		t.Fatalf("closed ticket should not be rendered:\n%s", got)
	}
}

func TestRenderOpenTicketsEmpty(t *testing.T) {
	var out bytes.Buffer

	renderOpenTickets(t.TempDir(), nil, &out)

	if got := out.String(); got != "(no open tickets)\n" {
		t.Fatalf("unexpected output: %q", got)
	}
}

func assertContains(t *testing.T, haystack, needle string) {
	t.Helper()

	if !bytes.Contains([]byte(haystack), []byte(needle)) {
		t.Fatalf("missing %q in output:\n%s", needle, haystack)
	}
}
