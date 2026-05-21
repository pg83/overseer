package main

import (
	"bufio"
	"encoding/json"
	"fmt"
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
// it as `TASKS_DB`). It points at the events log, which IS the database.
func tasksDBPath(orchRoot string) string {
	return eventsLogPath(orchRoot)
}

// LogEvent is one append-only record describing a state transition. Persisted as
// one JSON object per line in tasks.events.jsonl. Required fields:
//
//	ts: RFC3339Nano timestamp
//	k:  kind discriminator — "create" | "phase" | "update" | "event" | "ws"
//	n:  ticket number
//
// Kind-specific fields:
//
//	create: type (TicketType), descr (string), prio (int), deps ([]int) — plan
//	        tickets start at PhasePlan, code tickets start at PhaseImplement
//	phase:  phase (Phase) — the new pipeline position; terminal phases close the ticket
//	update: descr? (string), prio? (int), deps? ([]int) — only present fields change
//	event:  kind (string), detail (string) — appended to ticket.Events (history)
//	ws:     ws (string) — appended to ticket.Workspaces
//
// Legacy logs may carry "close" (reason MERGED/DISCARDED) — replayed into a phase.
type LogEvent = map[string]any

// LoadTasks rebuilds the in-memory ticket list by replaying tasks.events.jsonl.
// Missing log = fresh orchestrator (returns nil). A legacy tasks.jsonl snapshot is
// migrated on first run.
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

	out := Throw2(os.Create(eventsLogPath(root)))
	defer out.Close()

	var tickets []Ticket

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 64<<20)

	for scanner.Scan() {
		line := scanner.Bytes()

		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}

		var legacy struct {
			N           int           `json:"n"`
			State       string        `json:"state"`
			Descr       string        `json:"descr"`
			Prio        int           `json:"prio"`
			Deps        []int         `json:"deps"`
			Workspaces  []string      `json:"workspaces"`
			CloseReason string        `json:"close_reason"`
			Events      []TicketEvent `json:"events"`
		}
		Throw(json.Unmarshal(line, &legacy))

		phase := PhasePlan

		if legacy.State == "CLOSED" {
			phase = PhaseDiscarded

			if legacy.CloseReason == "MERGED" {
				phase = PhaseMerged
			}
		}

		emitLegacyTicketAsEvents(out, legacy.N, legacy.Descr, legacy.Prio, legacy.Deps, legacy.Workspaces, legacy.Events, phase)

		tickets = applyLogEvent(tickets, LogEvent{"k": "create", "n": legacy.N, "descr": legacy.Descr, "prio": legacy.Prio, "deps": legacy.Deps})
		tickets = applyLogEvent(tickets, LogEvent{"k": "phase", "n": legacy.N, "phase": string(phase)})
	}

	Throw(scanner.Err())

	return tickets
}

// emitLegacyTicketAsEvents writes a synthetic event sequence reproducing one
// legacy ticket's state. Used only at one-shot migration time.
func emitLegacyTicketAsEvents(w io.Writer, n int, descr string, prio int, deps []int, workspaces []string, events []TicketEvent, phase Phase) {
	now := time.Now().UTC().Format(time.RFC3339Nano)

	write := func(ev LogEvent) {
		if _, ok := ev["ts"]; !ok {
			ev["ts"] = now
		}

		b := Throw2(json.Marshal(ev))
		Throw2(w.Write(append(b, '\n')))
	}

	write(LogEvent{"k": "create", "n": n, "type": string(TicketTypeCode), "descr": descr, "prio": prio, "deps": deps})

	for _, ws := range workspaces {
		write(LogEvent{"k": "ws", "n": n, "ws": ws})
	}

	for _, e := range events {
		write(LogEvent{"ts": e.Ts, "k": "event", "n": n, "kind": e.Kind, "detail": e.Detail})
	}

	write(LogEvent{"k": "phase", "n": n, "phase": string(phase)})
}

// applyLogEvent applies one event to a ticket list, returning the updated list.
// Tolerant: unknown ticket N or unknown kind is a no-op (safe replay of partial or
// future-format logs).
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
		ticketType := replayTicketType(jsonTicketType(ev["type"]))
		phase := newTicketPhase(ticketType)

		return append(tickets, Ticket{
			N:     n,
			Type:  ticketType,
			Phase: phase,
			Descr: descr,
			Prio:  jsonInt(ev["prio"]),
			Deps:  jsonIntArray(ev["deps"]),
		})
	case "phase":
		if idx < 0 {
			return tickets
		}

		p, _ := ev["phase"].(string)

		if p != "" {
			phase := Phase(p)

			if tickets[idx].Type == TicketTypeCode && phase == PhasePlan {
				phase = PhaseImplement
			}

			tickets[idx].Phase = phase
		}

		return tickets
	case "close":
		// Legacy: map an old close record onto a terminal phase.
		if idx < 0 {
			return tickets
		}

		reason, _ := ev["reason"].(string)
		tickets[idx].Phase = PhaseDiscarded

		if reason == "MERGED" {
			tickets[idx].Phase = PhaseMerged
		}

		return tickets
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

// appendLog persists one event to the log and applies it to o.Tickets. Called only
// by the coordinate goroutine (single-threaded — no lock). THE write path for
// state: every mutation goes through here.
func (o *Orchestrator) appendLog(ev LogEvent) {
	if _, ok := ev["ts"]; !ok {
		ev["ts"] = time.Now().UTC().Format(time.RFC3339Nano)
	}

	b := Throw2(json.Marshal(ev))

	f := Throw2(os.OpenFile(eventsLogPath(o.Root), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644))
	defer f.Close()

	Throw2(f.Write(append(b, '\n')))

	o.Tickets = applyLogEvent(o.Tickets, ev)
}

// setPhase persists a phase transition and mirrors a human-readable line into the
// ticket's log.md. The single way the coordinator advances a ticket.
func (o *Orchestrator) setPhase(n int, p Phase, detail string) {
	ts := time.Now().UTC().Format(time.RFC3339Nano)

	o.appendLog(LogEvent{"ts": ts, "k": "phase", "n": n, "phase": string(p)})

	appendTicketLogTs(o.Root, n, ts, "PHASE:"+string(p), detail)
}

// recordEvent appends a history record to the events log and to log.md. Called only
// by the coordinate goroutine.
func (o *Orchestrator) recordEvent(n int, kind, detail string) {
	ts := time.Now().UTC().Format(time.RFC3339Nano)

	o.appendLog(LogEvent{"ts": ts, "k": "event", "n": n, "kind": kind, "detail": detail})

	appendTicketLogTs(o.Root, n, ts, kind, detail)
}

// SerializeTasks renders the in-memory tickets for the replanner's CURRENT_TASKS
// input. OPEN_TICKETS = every non-terminal ticket (pretty JSON incl. phase);
// CLOSED_DEPS = compact JSON blocks for terminal tickets that are direct deps of a
// non-terminal one. Sorted by N.
func SerializeTasks(tickets []Ticket) string {
	sorted := make([]Ticket, len(tickets))
	copy(sorted, tickets)
	sort.Slice(sorted, func(a, b int) bool {
		return sorted[a].N < sorted[b].N
	})

	directDeps := map[int]bool{}

	for _, t := range sorted {
		if !t.Phase.Terminal() {
			for _, dep := range t.Deps {
				directDeps[dep] = true
			}
		}
	}

	var open, closedDeps strings.Builder

	for _, t := range sorted {
		if !t.Phase.Terminal() {
			b := Throw2(json.MarshalIndent(t, "", "  "))
			open.Write(b)
			open.WriteString("\n\n")
		} else if directDeps[t.N] {
			b := Throw2(json.MarshalIndent(map[string]any{
				"n":     t.N,
				"phase": t.Phase,
				"descr": t.Descr,
			}, "", "  "))
			closedDeps.Write(b)
			closedDeps.WriteString("\n\n")
		}
	}

	maxN := 0

	for _, t := range sorted {
		if t.N > maxN {
			maxN = t.N
		}
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "MAX_TICKET_N: %d\n\n", maxN)
	sb.WriteString("OPEN_TICKETS (non-terminal — replanner may cancel/update these):\n")

	if open.Len() == 0 {
		sb.WriteString("(none)\n")
	} else {
		sb.WriteString(open.String())
	}

	sb.WriteString("\nCLOSED_DEPS (terminal deps of open tickets — immutable, do not cancel/update):\n")

	if closedDeps.Len() == 0 {
		sb.WriteString("(none)\n")
	} else {
		sb.WriteString(closedDeps.String())
	}

	sb.WriteString("\nFull ticket history: see TASKS_DB path in input header.\n")

	return sb.String()
}

// ValidateTasks checks structural invariants on a candidate ticket list. Run on the
// replanner-batch sandbox before committing.
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

		if !validPhase(t.Phase) {
			ThrowFmt("ticket %d: invalid PHASE %q", t.N, t.Phase)
		}

		if t.Type != "" && !validTicketType(t.Type) {
			ThrowFmt("ticket %d: invalid TYPE %q", t.N, t.Type)
		}

		switch t.Type {
		case TicketTypePlan:
			switch t.Phase {
			case PhasePlan, PhaseArbitrate, PhaseEscalate, PhasePlanned, PhaseDiscarded:
			default:
				ThrowFmt("ticket %d: plan ticket cannot be in phase %q", t.N, t.Phase)
			}
		case TicketTypeCode:
			if t.Phase == PhasePlanned {
				ThrowFmt("ticket %d: code ticket cannot be in phase %q", t.N, t.Phase)
			}
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

		for _, d := range t.Deps {
			if !known[d] {
				ThrowFmt("ticket %d: DEPS references missing ticket %d", t.N, d)
			}

			if d == t.N {
				ThrowFmt("ticket %d: DEPS contains self", t.N)
			}
		}
	}

	// A non-terminal ticket cannot depend on a DISCARDED prerequisite — whoever
	// cancels a ticket cleans up its dependents in the same batch.
	byN := map[int]Ticket{}

	for _, t := range tickets {
		byN[t.N] = t
	}

	for _, t := range tickets {
		if t.Phase.Terminal() {
			continue
		}

		for _, d := range t.Deps {
			if byN[d].Phase == PhaseDiscarded {
				ThrowFmt("ticket %d depends on T-%d which is DISCARDED — drop the dep or cancel ticket %d too",
					t.N, d, t.N)
			}
		}
	}

	checkNoCycles(tickets)
}

func validPhase(p Phase) bool {
	switch p {
	case PhasePlan, PhaseImplement, PhaseReview, PhaseMerge, PhaseArbitrate, PhaseEscalate, PhasePlanned, PhaseMerged, PhaseDiscarded:
		return true
	}

	return false
}

func jsonTicketType(v any) TicketType {
	s, _ := v.(string)
	t := TicketType(strings.TrimSpace(s))

	if validTicketType(t) {
		return t
	}

	return ""
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

// jsonInt accepts JSON numbers (float64 after Unmarshal-into-any), bare ints, or
// string forms ("42", "T-42") and returns the int. 0 on failure — callers check sign.
func jsonInt(v any) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	case string:
		s := strings.TrimPrefix(strings.TrimSpace(x), "T-")

		var n int

		if _, err := fmt.Sscanf(s, "%d", &n); err == nil {
			return n
		}
	}

	return 0
}

func jsonIntArray(v any) []int {
	switch x := v.(type) {
	case []int:
		out := make([]int, len(x))
		copy(out, x)

		return out
	case []any:
		var out []int

		for _, item := range x {
			out = append(out, jsonInt(item))
		}

		return out
	}

	return nil
}
