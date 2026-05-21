package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

type messageLogEntry struct {
	root string
	line string
}

// appendMessage appends one MESSAGE line to <root>/messages.txt — the team's shared chat
// across all roles and tickets. Format: "<ts>\t<role>\t<ticket>\t<msg>\n", no truncation.
// A single writer goroutine serializes appends so concurrent agents don't interleave.
// Returns the exact line written so the coordinator can reuse it for replanner context.
func appendMessage(orchRoot string, role AgentRole, ticket int, msg string) string {
	line := fmt.Sprintf("%s\t%s\tT-%d\t%s\n",
		time.Now().UTC().Format(time.RFC3339Nano), role, ticket, msg)

	messagesLogCh <- messageLogEntry{root: orchRoot, line: line}

	return line
}

var messagesLogCh = make(chan messageLogEntry, 1000)

func init() {
	go messagesLogWriter()
}

func messagesLogWriter() {
	for entry := range messagesLogCh {
		f, err := os.OpenFile(messagesLogPath(entry.root), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

		if err != nil {
			fmt.Fprintf(os.Stderr, "messages log open: %v\n", err)

			continue
		}

		if _, err := f.WriteString(entry.line); err != nil {
			fmt.Fprintf(os.Stderr, "messages log write: %v\n", err)
		}

		if err := f.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "messages log close: %v\n", err)
		}
	}
}

// ticketMessages reads the team chat and returns the lines that mention the
// given ticket — used to inject a per-ticket "what teammates said" digest into
// every spawn's prompt (alongside LOG of state transitions and PRIOR_RUNS).
// Returns "" if the chat doesn't exist or no lines match.
func ticketMessages(orchRoot string, ticketN int) string {
	f, err := os.Open(messagesLogPath(orchRoot))

	if err != nil {
		return ""
	}

	defer f.Close()

	needle := fmt.Sprintf("\tT-%d\t", ticketN)

	var sb strings.Builder

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 16<<20)

	for scanner.Scan() {
		line := scanner.Text()

		if strings.Contains(line, needle) {
			sb.WriteString(line)
			sb.WriteByte('\n')
		}
	}

	return sb.String()
}
