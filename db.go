package main

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const tasksFile = "tasks.jsonl"

func tasksDBPath(orchRoot string) string {
	return filepath.Join(orchRoot, tasksFile)
}

func LoadTasks(root string) []Ticket {
	path := tasksDBPath(root)

	f, err := os.Open(path)

	if os.IsNotExist(err) {
		return nil
	}

	Throw(err)

	defer f.Close()

	var tickets []Ticket

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 64<<20)

	for scanner.Scan() {
		line := scanner.Bytes()

		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}

		var t Ticket
		Throw(json.Unmarshal(line, &t))
		tickets = append(tickets, t)
	}

	Throw(scanner.Err())

	ValidateTasks(tickets)

	return tickets
}

func SaveTasks(root string, tickets []Ticket) {
	ValidateTasks(tickets)

	sorted := make([]Ticket, len(tickets))
	copy(sorted, tickets)
	sort.Slice(sorted, func(a, b int) bool {
		return sorted[a].N < sorted[b].N
	})

	var sb strings.Builder

	for _, t := range sorted {
		b := Throw2(json.Marshal(t))
		sb.Write(b)
		sb.WriteByte('\n')
	}

	path := tasksDBPath(root)
	tmp := path + ".tmp"
	Throw(os.WriteFile(tmp, []byte(sb.String()), 0644))
	Throw(os.Rename(tmp, path))
}

// SerializeTasks renders the in-memory tickets as JSONL — same format the replanner
// receives in CURRENT_TASKS. The replanner returns its replacement task list inside a
// `set_tasks` JSON event whose `tickets` field is a parsed array (see applyAgentEvent).
func SerializeTasks(tickets []Ticket) string {
	sorted := make([]Ticket, len(tickets))
	copy(sorted, tickets)
	sort.Slice(sorted, func(a, b int) bool {
		return sorted[a].N < sorted[b].N
	})

	var sb strings.Builder

	for _, t := range sorted {
		b := Throw2(json.Marshal(t))
		sb.Write(b)
		sb.WriteByte('\n')
	}

	return sb.String()
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

// recordEventLocked appends a TicketEvent to ticket n in-memory, persists tasks.jsonl,
// and mirrors the same event into the per-ticket log.md (human-readable). Caller must
// hold o.Mu. Use o.recordEvent for code paths that don't already hold the lock.
func (o *Orchestrator) recordEventLocked(n int, kind, detail string) {
	ts := time.Now().UTC().Format(time.RFC3339Nano)

	for i := range o.Tickets {
		if o.Tickets[i].N == n {
			o.Tickets[i].Events = append(o.Tickets[i].Events, TicketEvent{
				Ts: ts, Kind: kind, Detail: detail,
			})
		}
	}

	SaveTasks(o.Root, o.Tickets)
	appendTicketLogTs(o.Root, n, ts, kind, detail)
}

func (o *Orchestrator) recordEvent(n int, kind, detail string) {
	o.Mu.Lock()
	defer o.Mu.Unlock()

	o.recordEventLocked(n, kind, detail)
}
