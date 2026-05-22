package main

import (
	"fmt"
	"os"
)

// taskSnap is one ticket's DB state for the TUI tasks tab — pushed by the
// coordinator (the owner of ticket state) so the TUI never reads o.Tickets across
// goroutines.
type taskSnap struct {
	n        int
	descr    string
	phase    Phase
	inFlight bool // dispatched to a pool right now (shadow SCHEDULED)
}

// uiSink consumes uiEvents (and, for the TUI, ticket-DB snapshots). logSink prints
// the classic colored line and ignores snapshots; tuiSink folds both into the
// live model.
type uiSink interface {
	emit(uiEvent)
	tasks([]taskSnap)
}

type logSink struct{}

func (logSink) tasks([]taskSnap) {}

func (logSink) emit(e uiEvent) {
	fmt.Fprintf(os.Stderr, "%s%s%s  %s%7s%s  %s  %s%-5s%s  %s%-10s%s  %s%s%s  %s\n",
		cDim, e.ts.Format("15:04:05"), cReset,
		cGray, e.cost, cReset,
		e.emoji,
		cBold, ticketLabel(e.ticket), cReset,
		roleColor(e.role), roleName(e.role), cReset,
		cBold, e.kind, cReset,
		e.msg)
}

func roleName(r AgentRole) string {
	if r == "" {
		return "system"
	}

	return string(r)
}

func ticketLabel(ticket int) string {
	if ticket < 0 {
		return "—"
	}

	return fmt.Sprintf("T-%d", ticket)
}
