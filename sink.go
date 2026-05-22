package main

import (
	"fmt"
	"os"
)

// uiSink consumes uiEvents. logSink prints the same colored line the orchestrator
// always has; tuiSink (tui.go) folds them into the live TUI model.
type uiSink interface {
	emit(uiEvent)
}

type logSink struct{}

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
