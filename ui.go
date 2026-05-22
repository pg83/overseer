package main

import (
	"time"
)

const (
	cReset   = "\x1b[0m"
	cBold    = "\x1b[1m"
	cDim     = "\x1b[2m"
	cRed     = "\x1b[91m"
	cGreen   = "\x1b[92m"
	cYellow  = "\x1b[93m"
	cBlue    = "\x1b[94m"
	cMagenta = "\x1b[95m"
	cCyan    = "\x1b[96m"
	cGray    = "\x1b[90m"
)

func roleColor(r AgentRole) string {
	switch r {
	case RoleTasker:
		return cBlue
	case RoleDigger:
		return cYellow
	case RoleReviewer:
		return cMagenta
	case RoleMerger:
		return cCyan
	case RoleReplanner:
		return cGreen
	case RoleOverseer:
		return cRed
	}

	return cGray
}

func roleEmoji(r AgentRole) string {
	switch r {
	case RoleTasker:
		return "🧠"
	case RoleDigger:
		return "⛏️ "
	case RoleReviewer:
		return "👁️ "
	case RoleMerger:
		return "🔀"
	case RoleReplanner:
		return "🧮"
	case RoleOverseer:
		return "🦉"
	}

	return "📦"
}

// uiEvent is one line of orchestrator output, captured structurally so both sinks
// (log → stderr, tui → in-memory model) render it their own way. cost is a
// snapshot of the running cost column at emit time.
type uiEvent struct {
	ts     time.Time
	cost   string
	emoji  string
	role   AgentRole
	ticket int
	kind   string
	msg    string
}

// uiOut is the active sink. Default is the line logger (unchanged behavior);
// `run --ui=tui` swaps in the TUI sink at startup.
var uiOut uiSink = logSink{}

// uiCleanup, when set (TUI mode), tears down the alternate screen and restores
// stdio. fatal() calls it before exiting so a hard stop doesn't leave the
// terminal in raw mode.
var uiCleanup func()

// uiTasksWanted gates the coordinator's per-dispatch ticket-DB snapshot — only
// the TUI consumes it, so log mode skips the work.
var uiTasksWanted bool

func ui(emoji string, role AgentRole, ticket int, kind, msg string) {
	uiOut.emit(uiEvent{
		ts:     time.Now(),
		cost:   meter.column(),
		emoji:  emoji,
		role:   role,
		ticket: ticket,
		kind:   kind,
		msg:    msg,
	})
}

func uiSys(emoji, kind, msg string) {
	ui(emoji, AgentRole(""), -1, kind, msg)
}

func uiTicket(emoji string, role AgentRole, ticket int, kind, msg string) {
	ui(emoji, role, ticket, kind, msg)
}
