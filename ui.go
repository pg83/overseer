package main

import (
	"fmt"
	"os"
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

func ui(emoji string, role AgentRole, ticket int, kind, msg string) {
	ts := time.Now().Format("15:04:05")

	rname := string(role)

	if rname == "" {
		rname = "system"
	}

	tstr := "—"

	if ticket >= 0 {
		tstr = fmt.Sprintf("T-%d", ticket)
	}

	color := roleColor(role)

	fmt.Fprintf(os.Stderr, "%s%s%s  %s  %s%-5s%s  %s%-10s%s  %s%s%s  %s\n",
		cDim, ts, cReset,
		emoji,
		cBold, tstr, cReset,
		color, rname, cReset,
		cBold, kind, cReset,
		msg)
}

func uiSys(emoji, kind, msg string) {
	ui(emoji, AgentRole(""), -1, kind, msg)
}

func uiTicket(emoji string, role AgentRole, ticket int, kind, msg string) {
	ui(emoji, role, ticket, kind, msg)
}
