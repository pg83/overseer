package main

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const eventsLogFile = "tasks.events.jsonl"

func eventsLogPath(orchRoot string) string {
	return filepath.Join(orchRoot, eventsLogFile)
}

// tasksDBPath retains the legacy name (some prompts and replanner inputs reference
// it as `TASKS_DB`). It now points at the events log, which IS the database.
func tasksDBPath(orchRoot string) string {
	return eventsLogPath(orchRoot)
}

// LogEvent is one append-only record describing a state transition. Persisted as
// one JSON object per line in tasks.events.jsonl. Required fields:
//
//	ts: RFC3339Nano timestamp
//	k:  kind discriminator — "create" | "update" | "close" | "event" | "ws"
//	n:  ticket number
//
// Kind-specific fields:
//
//	create: descr (string), prio (int), deps ([]int)
//	update: descr? (string), prio? (int), deps? ([]int) — only present fields change
//	close:  reason ("MERGED" | "DISCARDED")
//	event:  kind (string), detail (string) — append to ticket.Events
//	ws:     ws (string) — appends to ticket.Workspaces
type LogEvent = map[string]any

// LoadTasks rebuilds the in-memory ticket list by replaying tasks.events.jsonl.
// Missing log = fresh orchestrator (returns nil). On legacy tasks.jsonl we migrate:
// convert each ticket to a series of events, write the new log, return the result.
func LoadTasks(root string) []Ticket {
	path := eventsLogPath(root)

	f, err := os.Open(path)

	if os.IsNotExist(err) {
		return migrateLegacyFormat(root)
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

		var ev LogEvent
		Throw(json.Unmarshal(line, &ev))

		tickets = applyLogEvent(tickets, ev)
	}

	Throw(scanner.Err())

	return tickets
}

// migrateLegacyFormat reads the old tasks.jsonl snapshot (if any), converts each
// ticket into a series of log events, and writes them as the new event log. The
// legacy file is left in place — operator can rm after verifying.
func migrateLegacyFormat(root string) []Ticket {
	oldPath := filepath.Join(root, "tasks.jsonl")

	f, err := os.Open(oldPath)

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

		if t.CloseReason == "CANCELLED" {
			t.CloseReason = CloseDiscarded
		}

		tickets = append(tickets, t)
	}

	Throw(scanner.Err())

	out := Throw2(os.Create(eventsLogPath(root)))
	defer out.Close()

	for _, t := range tickets {
		emitLegacyTicketAsEvents(out, t)
	}

	return tickets
}

// emitLegacyTicketAsEvents writes a synthetic event sequence reproducing one
// legacy ticket's state. Used only at one-shot migration time.
func emitLegacyTicketAsEvents(w io.Writer, t Ticket) {
	now := time.Now().UTC().Format(time.RFC3339Nano)

	write := func(ev LogEvent) {
		if _, ok := ev["ts"]; !ok {
			ev["ts"] = now
		}

		b := Throw2(json.Marshal(ev))
		Throw2(w.Write(append(b, '\n')))
	}

	write(LogEvent{"k": "create", "n": t.N, "descr": t.Descr, "prio": t.Prio, "deps": t.Deps})

	for _, ws := range t.Workspaces {
		write(LogEvent{"k": "ws", "n": t.N, "ws": ws})
	}

	for _, e := range t.Events {
		write(LogEvent{"ts": e.Ts, "k": "event", "n": t.N, "kind": e.Kind, "detail": e.Detail})
	}

	if t.State == StateClosed {
		write(LogEvent{"k": "close", "n": t.N, "reason": string(t.CloseReason)})
	}
}

// applyLogEvent applies one event to a ticket list, returning the updated list.
// Tolerant: unknown ticket N or unknown kind is a no-op (allows safe replay even
// of partially-written or future-format logs). Closes are idempotent.
func applyLogEvent(tickets []Ticket, ev LogEvent) []Ticket {
	n := jsonInt(ev["n"])

	if n <= 0 {
		return tickets
	}

	idx := -1

	for i, t := range tickets {
		if t.N == n {
			idx = i

			break
		}
	}

	switch kind, _ := ev["k"].(string); kind {
	case "create":
		if idx >= 0 {
			return tickets
		}

		descr, _ := ev["descr"].(string)

		return append(tickets, Ticket{
			N:     n,
			State: StateOpen,
			Descr: descr,
			Prio:  jsonInt(ev["prio"]),
			Deps:  jsonIntArray(ev["deps"]),
		})
	case "update":
		if idx < 0 {
			return tickets
		}

		if d, ok := ev["descr"].(string); ok {
			tickets[idx].Descr = d
		}

		if _, ok := ev["prio"]; ok {
			tickets[idx].Prio = jsonInt(ev["prio"])
		}

		if _, ok := ev["deps"]; ok {
			tickets[idx].Deps = jsonIntArray(ev["deps"])
		}

		return tickets
	case "close":
		if idx < 0 {
			return tickets
		}

		if tickets[idx].State == StateClosed {
			tickets[idx].InProgress = false

			return tickets
		}

		reason, _ := ev["reason"].(string)

		if reason == "CANCELLED" {
			reason = string(CloseDiscarded)
		}

		tickets[idx].State = StateClosed
		tickets[idx].CloseReason = CloseReason(reason)
		tickets[idx].InProgress = false

		return tickets
	case "event":
		if idx < 0 {
			return tickets
		}

		ts, _ := ev["ts"].(string)
		ek, _ := ev["kind"].(string)
		det, _ := ev["detail"].(string)

		tickets[idx].Events = append(tickets[idx].Events, TicketEvent{Ts: ts, Kind: ek, Detail: det})

		return tickets
	case "ws":
		if idx < 0 {
			return tickets
		}

		ws, _ := ev["ws"].(string)

		if ws != "" {
			tickets[idx].Workspaces = append(tickets[idx].Workspaces, ws)
		}

		return tickets
	}

	return tickets
}

// appendLogEventLocked persists one event to the log and applies it to o.Tickets.
// Caller must hold o.Mu. Sets ts if not present. This is THE write path for state
// — every mutation goes through here.
func (o *Orchestrator) appendLogEventLocked(ev LogEvent) {
	if _, ok := ev["ts"]; !ok {
		ev["ts"] = time.Now().UTC().Format(time.RFC3339Nano)
	}

	b := Throw2(json.Marshal(ev))

	f := Throw2(os.OpenFile(eventsLogPath(o.Root), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644))
	defer f.Close()

	Throw2(f.Write(append(b, '\n')))

	o.Tickets = applyLogEvent(o.Tickets, ev)
}

// SerializeTasks renders the in-memory tickets as JSONL — replanner's CURRENT_TASKS
// view. Same format as the legacy tasks.jsonl snapshot; one ticket per line, sorted
// by N. The events log is the source of truth, this is just a projection.
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

// ValidateTasks checks structural invariants on a candidate ticket list. Run only
// on replanner-batch sandboxes before committing — the events log is already-
// applied state and trusted by construction. Other write paths (record-event,
// bounce, ws, close-MERGED) can't violate these invariants.
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
			case CloseMerged, CloseDiscarded:
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

	// An OPEN ticket cannot depend on a DISCARDED prerequisite — whoever cancels
	// a ticket is responsible for cleaning up its dependents in the same batch
	// (update their deps to remove the dropped N, or cascade-cancel them).
	byN := map[int]Ticket{}

	for _, t := range tickets {
		byN[t.N] = t
	}

	for _, t := range tickets {
		if t.State != StateOpen {
			continue
		}

		for _, d := range t.Deps {
			dep := byN[d]

			if dep.State == StateClosed && dep.CloseReason == CloseDiscarded {
				ThrowFmt("ticket %d (OPEN) depends on T-%d which is DISCARDED — drop the dep or cancel ticket %d too",
					t.N, d, t.N)
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

// recordEventLocked appends a state-transition record to the events log (and
// mirrors it into the per-ticket human-readable log.md). Caller must hold o.Mu.
func (o *Orchestrator) recordEventLocked(n int, kind, detail string) {
	ts := time.Now().UTC().Format(time.RFC3339Nano)

	o.appendLogEventLocked(LogEvent{
		"ts":     ts,
		"k":      "event",
		"n":      n,
		"kind":   kind,
		"detail": detail,
	})

	appendTicketLogTs(o.Root, n, ts, kind, detail)
}

func (o *Orchestrator) recordEvent(n int, kind, detail string) {
	o.Mu.Lock()
	defer o.Mu.Unlock()

	o.recordEventLocked(n, kind, detail)
}

