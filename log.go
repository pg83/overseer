package main

import (
	"fmt"
	"os"
	"path/filepath"
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
	dir := ticketDir(orchRoot, n)
	Throw(os.MkdirAll(dir, 0755))

	f := Throw2(os.OpenFile(ticketLogPath(orchRoot, n), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644))
	defer f.Close()

	line := fmt.Sprintf("%s %s %s\n", time.Now().UTC().Format(time.RFC3339Nano), event, detail)
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
