package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const tasksFile = "TASKS.md"

func ParseTasks(data string) []Ticket {
	var tickets []Ticket

	for _, block := range splitBlocks(data) {
		t := parseBlock(block)
		tickets = append(tickets, t)
	}

	return tickets
}

func splitBlocks(data string) []string {
	var blocks []string
	var cur []string

	for _, line := range strings.Split(data, "\n") {
		if strings.TrimSpace(line) == "---" {
			if hasContent(cur) {
				blocks = append(blocks, strings.Join(cur, "\n"))
			}

			cur = nil

			continue
		}

		cur = append(cur, line)
	}

	if hasContent(cur) {
		blocks = append(blocks, strings.Join(cur, "\n"))
	}

	return blocks
}

func hasContent(lines []string) bool {
	for _, l := range lines {
		if strings.TrimSpace(l) != "" {
			return true
		}
	}

	return false
}

func parseBlock(block string) Ticket {
	t := Ticket{}

	for _, line := range strings.Split(block, "\n") {
		line = strings.TrimRight(line, "\r")

		if strings.TrimSpace(line) == "" {
			continue
		}

		idx := strings.Index(line, ":")

		if idx <= 0 {
			ThrowFmt("malformed K:V line: %q", line)
		}

		key := strings.TrimSpace(line[:idx])
		val := strings.TrimSpace(line[idx+1:])

		applyField(&t, key, val)
	}

	return t
}

func applyField(t *Ticket, key, val string) {
	switch key {
	case "N":
		t.N = Throw2(strconv.Atoi(val))
	case "STATE":
		t.State = State(val)
	case "DESCR":
		t.Descr = val
	case "PRIO":
		t.Prio = Throw2(strconv.Atoi(val))
	case "DEPS":
		t.Deps = parseIntList(val)
	case "WORKSPACES":
		t.Workspaces = parseStringList(val)
	case "CLOSE_REASON":
		t.CloseReason = CloseReason(val)
	default:
		ThrowFmt("unknown field %q", key)
	}
}

func parseIntList(s string) []int {
	if strings.TrimSpace(s) == "" {
		return nil
	}

	var out []int

	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)

		if p == "" {
			continue
		}

		out = append(out, Throw2(strconv.Atoi(p)))
	}

	return out
}

func parseStringList(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}

	var out []string

	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)

		if p == "" {
			continue
		}

		out = append(out, p)
	}

	return out
}

func SerializeTasks(tickets []Ticket) string {
	sorted := make([]Ticket, len(tickets))
	copy(sorted, tickets)

	sort.Slice(sorted, func(a, b int) bool {
		return sorted[a].N < sorted[b].N
	})

	var sb strings.Builder

	for i, t := range sorted {
		if i > 0 {
			sb.WriteString("---\n")
		}

		writeTicket(&sb, t)
	}

	if len(sorted) > 0 {
		sb.WriteString("---\n")
	}

	return sb.String()
}

func writeTicket(sb *strings.Builder, t Ticket) {
	fmt.Fprintf(sb, "N: %d\n", t.N)
	fmt.Fprintf(sb, "STATE: %s\n", t.State)
	fmt.Fprintf(sb, "DESCR: %s\n", t.Descr)
	fmt.Fprintf(sb, "PRIO: %d\n", t.Prio)

	if len(t.Deps) > 0 {
		fmt.Fprintf(sb, "DEPS: %s\n", joinInts(t.Deps))
	}

	if len(t.Workspaces) > 0 {
		fmt.Fprintf(sb, "WORKSPACES: %s\n", strings.Join(t.Workspaces, ", "))
	}

	if t.CloseReason != "" {
		fmt.Fprintf(sb, "CLOSE_REASON: %s\n", t.CloseReason)
	}
}

func joinInts(xs []int) string {
	parts := make([]string, len(xs))

	for i, x := range xs {
		parts[i] = strconv.Itoa(x)
	}

	return strings.Join(parts, ", ")
}

func ValidateTasks(tickets []Ticket) {
	seen := map[int]bool{}
	known := map[int]bool{}

	for _, t := range tickets {
		known[t.N] = true
	}

	for _, t := range tickets {
		if seen[t.N] {
			ThrowFmt("duplicate N: %d", t.N)
		}

		seen[t.N] = true

		if t.State != StateOpen && t.State != StateClosed {
			ThrowFmt("ticket %d: invalid STATE %q", t.N, t.State)
		}

		if t.Prio < 1 || t.Prio > 10 {
			ThrowFmt("ticket %d: PRIO %d out of [1,10]", t.N, t.Prio)
		}

		if strings.TrimSpace(t.Descr) == "" {
			ThrowFmt("ticket %d: empty DESCR", t.N)
		}

		if strings.ContainsAny(t.Descr, "\n\r") {
			ThrowFmt("ticket %d: DESCR has newline", t.N)
		}

		if t.State == StateClosed {
			switch t.CloseReason {
			case CloseMerged, CloseDiscarded, CloseCancelled:
			default:
				ThrowFmt("ticket %d: STATE=CLOSED requires valid CLOSE_REASON, got %q", t.N, t.CloseReason)
			}
		} else if t.CloseReason != "" {
			ThrowFmt("ticket %d: STATE=OPEN must not have CLOSE_REASON", t.N)
		}

		for _, d := range t.Deps {
			if !known[d] {
				ThrowFmt("ticket %d: DEPS references missing ticket %d", t.N, d)
			}

			if d == t.N {
				ThrowFmt("ticket %d: DEPS contains self", t.N)
			}
		}
	}

	checkNoCycles(tickets)
}

func checkNoCycles(tickets []Ticket) {
	g := map[int][]int{}

	for _, t := range tickets {
		g[t.N] = t.Deps
	}

	color := map[int]int{}

	var visit func(int)

	visit = func(n int) {
		if color[n] == 2 {
			return
		}

		if color[n] == 1 {
			ThrowFmt("DEPS cycle detected at ticket %d", n)
		}

		color[n] = 1

		for _, d := range g[n] {
			visit(d)
		}

		color[n] = 2
	}

	for _, t := range tickets {
		visit(t.N)
	}
}

func LoadTasks(root string) []Ticket {
	path := filepath.Join(root, tasksFile)

	data, err := os.ReadFile(path)

	if os.IsNotExist(err) {
		return nil
	}

	Throw(err)

	tickets := ParseTasks(string(data))
	ValidateTasks(tickets)

	return tickets
}

func SaveTasks(root string, tickets []Ticket) {
	ValidateTasks(tickets)

	path := filepath.Join(root, tasksFile)
	tmp := path + ".tmp"

	Throw(os.WriteFile(tmp, []byte(SerializeTasks(tickets)), 0644))
	Throw(os.Rename(tmp, path))
}
