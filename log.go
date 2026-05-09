package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

func ticketDir(orchRoot string, n int) string {
	return filepath.Join(orchRoot, "tickets", fmt.Sprintf("T-%d", n))
}

func ticketLogPath(orchRoot string, n int) string {
	return filepath.Join(ticketDir(orchRoot, n), "log.md")
}

func ticketPlanPath(orchRoot string, n int) string {
	return filepath.Join(ticketDir(orchRoot, n), "plan.md")
}

func appendTicketLog(orchRoot string, n int, event, detail string) {
	appendTicketLogTs(orchRoot, n, time.Now().UTC().Format(time.RFC3339Nano), event, detail)
}

func appendTicketLogTs(orchRoot string, n int, ts, event, detail string) {
	dir := ticketDir(orchRoot, n)
	Throw(os.MkdirAll(dir, 0755))

	f := Throw2(os.OpenFile(ticketLogPath(orchRoot, n), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644))
	defer f.Close()

	line := fmt.Sprintf("%s %s %s\n", ts, event, detail)
	Throw2(f.WriteString(line))
}

func planExists(orchRoot string, n int) bool {
	_, err := os.Stat(ticketPlanPath(orchRoot, n))

	return err == nil
}

func writePlan(orchRoot string, n int, content string) {
	dir := ticketDir(orchRoot, n)
	Throw(os.MkdirAll(dir, 0755))

	Throw(os.WriteFile(ticketPlanPath(orchRoot, n), []byte(content), 0644))
}

func messagesLogPath(orchRoot string) string {
	return filepath.Join(orchRoot, "messages.txt")
}

var messagesLogMu sync.Mutex

// appendMessage appends one MESSAGE line to <root>/messages.txt — the team's shared chat
// across all roles and tickets. Format: "<ts>\t<role>\t<ticket>\t<msg>\n", no truncation.
// Mutex-protected so concurrent agents don't interleave large messages.
func appendMessage(orchRoot string, role AgentRole, ticket int, msg string) {
	messagesLogMu.Lock()
	defer messagesLogMu.Unlock()

	f := Throw2(os.OpenFile(messagesLogPath(orchRoot), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644))
	defer f.Close()

	line := fmt.Sprintf("%s\t%s\tT-%d\t%s\n",
		time.Now().UTC().Format(time.RFC3339Nano), role, ticket, msg)
	Throw2(f.WriteString(line))
}
