package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

const mergerQueueThrottle = 4

// Generic event extractors used by more than one role handler. Role-specific extractors
// (plan body, set_tasks, cancel) live close to their single consumer further down so
// each consumer's knowledge of the event vocabulary stays minimal.

// eventReplans pulls every `replan` event's `reason` field (in emission order) out of
// the agent's event stream. Used by the role handlers that fan replan nudges into the
// replanner queue.
func eventReplans(events []map[string]any) []string {
	var out []string

	for _, ev := range events {
		if t, _ := ev["type"].(string); t != "replan" {
			continue
		}

		r, _ := ev["reason"].(string)
		r = strings.TrimSpace(r)

		if r != "" {
			out = append(out, r)
		}
	}

	return out
}

func NewOrchestrator(root, trunk string, bindings map[string]HarnessModel, jailBin string) *Orchestrator {
	ctx, cancel := context.WithCancel(context.Background())

	o := &Orchestrator{
		Root:       root,
		Trunk:      trunk,
		Bindings:   bindings,
		JailBin:    jailBin,
		AgentSem:   make(chan struct{}, 6),
		QReplanner: make(chan ReplanRequest, 256),
		QMerger:    make(chan MergeRequest, 256),
		QOverseer:  make(chan OverseerRequest, 64),
		AgentDone:  make(chan AgentResult, 64),
		Wakeup:     make(chan struct{}, 1),
		StopCtx:    ctx,
		StopCancel: cancel,
		Stopped:    make(chan struct{}),
	}

	o.Tickets = LoadTasks(root)
	o.GoalsHash = readGoalsHash(trunk)

	o.QOverseer <- OverseerRequest{Reason: "boot: re-evaluate goals and seed plan if needed"}
	uiSys("📥", "→Q_overseer", "boot")

	return o
}

func (o *Orchestrator) Run() {
	defer close(o.Stopped)

	go o.replannerLoop()
	go o.mergerLoop()
	go o.overseerLoop()

	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()

	for {
		select {
		case <-o.StopCtx.Done():
			return
		case <-tick.C:
		case <-o.Wakeup:
		case res := <-o.AgentDone:
			o.safe("HANDLE", func() { o.handleAgentResult(res) })
		}

		o.safe("SCHEDULE", o.scheduleReady)
	}
}

// safe wraps a panic-prone operation so an `*Exception` Throw inside doesn't kill the
// orchestrator goroutine. Surfaced to UI for visibility — never silently swallowed.
func (o *Orchestrator) safe(name string, f func()) {
	Try(f).Catch(func(e *Exception) {
		uiSys("💥", name+"_PANIC", e.Error())
	})
}

func (o *Orchestrator) signalWake() {
	select {
	case o.Wakeup <- struct{}{}:
	default:
	}
}

func (o *Orchestrator) openCountLocked() int {
	n := 0

	for _, t := range o.Tickets {
		if t.State == StateOpen {
			n++
		}
	}

	return n
}

func (o *Orchestrator) scheduleReady() {
	o.Mu.Lock()
	candidates := o.readyCandidatesLocked()
	o.Mu.Unlock()

	for _, t := range candidates {
		if len(o.QMerger) > mergerQueueThrottle {
			break
		}

		o.Mu.Lock()

		cur, ok := o.findTicketLocked(t.N)

		if !ok || cur.InProgress || cur.State != StateOpen {
			o.Mu.Unlock()

			continue
		}

		o.startAgentForTicketLocked(cur)

		o.Mu.Unlock()
	}
}

func (o *Orchestrator) readyCandidatesLocked() []Ticket {
	closedMerged := map[int]bool{}

	for _, t := range o.Tickets {
		if t.State == StateClosed && t.CloseReason == CloseMerged {
			closedMerged[t.N] = true
		}
	}

	var ready []Ticket

	for _, t := range o.Tickets {
		if t.State != StateOpen {
			continue
		}

		if t.InProgress {
			continue
		}

		ok := true

		for _, d := range t.Deps {
			if !closedMerged[d] {
				ok = false

				break
			}
		}

		if ok {
			ready = append(ready, t)
		}
	}

	sort.Slice(ready, func(a, b int) bool {
		if ready[a].Prio != ready[b].Prio {
			return ready[a].Prio > ready[b].Prio
		}

		return ready[a].N < ready[b].N
	})

	return ready
}

func (o *Orchestrator) startAgentForTicketLocked(t Ticket) {
	role := RoleDigger

	if !planExists(o.Root, t.N) {
		role = RoleTasker
	}

	wsID := NewWorkspace(o.Root, o.Trunk)

	o.setInProgressLocked(t.N, true)
	o.appendWorkspaceLocked(t.N, wsID)

	wsAbs := wsPath(o.Root, wsID)

	o.recordEventLocked(t.N, "AGENT_START", fmt.Sprintf("role=%s ws=%s", role, wsID))
	uiTicket("🚀", role, t.N, "START", "ws="+wsID)

	input := o.buildAgentInput(role, t.N, wsAbs)
	prompt := loadPrompt(role)
	stdin := concatPromptInput(prompt, input)

	go func() {
		res := o.runAgent(role, t.N, wsID, stdin)
		o.AgentDone <- res
	}()
}

// agentSelfBlock identifies the agent to itself: which role it is, which harness+model
// is driving it. Goes at the top of every agent's input so the agent can make self-aware
// decisions (e.g. "I'm a small model, keep edits cheap"). Roles can be bound to different
// harness:model combinations via --<role>-harness, so this lookup is per-role.
func (o *Orchestrator) agentSelfBlock(role AgentRole) string {
	hm := o.harnessModelForRole(role)
	model := hm.resolveModel(role)

	if model == "" {
		model = "(harness default)"
	}

	return fmt.Sprintf("ROLE: %s\nMODEL: %s\nHARNESS: %s\nMESSAGES_LOG: %s\n",
		role, model, hm.Harness.Name(), messagesLogPath(o.Root))
}

func (o *Orchestrator) buildAgentInput(role AgentRole, ticketN int, wsAbs string) string {
	var sb strings.Builder

	sb.WriteString(o.agentSelfBlock(role))

	t, _ := o.findTicketLocked(ticketN)
	fmt.Fprintf(&sb, "TICKET: %d\nDESCR: %s\nPRIO: %d\nDEPS: %v\n", t.N, t.Descr, t.Prio, t.Deps)
	fmt.Fprintf(&sb, "WORKSPACE: %s\n", wsAbs)
	fmt.Fprintf(&sb, "TRUNK_PATH: %s\n", o.Trunk)
	fmt.Fprintf(&sb, "TRUNK_HASH: %s\n", CurrentTrunkHash(o.Trunk))

	if planExists(o.Root, ticketN) {
		data, err := os.ReadFile(ticketPlanPath(o.Root, ticketN))

		if err == nil {
			fmt.Fprintf(&sb, "\nPLAN:\n%s\n", string(data))
		}
	}

	if data, err := os.ReadFile(ticketLogPath(o.Root, ticketN)); err == nil {
		fmt.Fprintf(&sb, "\nLOG:\n%s\n", string(data))
	}

	if prior := priorRunsForTicket(o.Root, ticketN); prior != "" {
		fmt.Fprintf(&sb, "\nPRIOR_RUNS (compact summaries — Read each LOG_FILE for the full reasoning stream if you need tool-by-tool detail):\n%s\n", prior)
	}

	return sb.String()
}

func (o *Orchestrator) findTicketLocked(n int) (Ticket, bool) {
	for _, t := range o.Tickets {
		if t.N == n {
			return t, true
		}
	}

	return Ticket{}, false
}

func (o *Orchestrator) appendWorkspaceLocked(n int, ws string) {
	for i := range o.Tickets {
		if o.Tickets[i].N == n {
			o.Tickets[i].Workspaces = append(o.Tickets[i].Workspaces, ws)
		}
	}

	SaveTasks(o.Root, o.Tickets)
}

func (o *Orchestrator) setInProgressLocked(n int, v bool) {
	for i := range o.Tickets {
		if o.Tickets[i].N == n {
			o.Tickets[i].InProgress = v
		}
	}
}

func (o *Orchestrator) handleAgentResult(res AgentResult) {
	o.Mu.Lock()

	// If the ticket was closed while the agent was still running (replanner cancel op
	// closed it; the agent finished anyway and its result landed here), drop the result
	// and stop the pipeline at this transition. No follow-up spawn, no state rewrite —
	// also clear InProgress in case it survived the close path.
	if t, ok := o.findTicketLocked(res.Ticket); ok && t.State == StateClosed {
		o.setInProgressLocked(res.Ticket, false)
		o.Mu.Unlock()
		v, _ := lastVerdict(res.Events)
		uiTicket("👻", res.Role, res.Ticket, "STALE", fmt.Sprintf("ticket already %s, dropping %s result", t.CloseReason, v))

		return
	}

	for _, line := range eventReplans(res.Events) {
		select {
		case o.QReplanner <- ReplanRequest{Source: res.Role, Ticket: res.Ticket, Reason: line}:
		default:
		}
	}

	switch res.Role {
	case RoleTasker:
		o.handleTaskerResultLocked(res)
	case RoleDigger:
		o.handleDiggerResultLocked(res)
	case RoleReviewer:
		o.handleReviewerResultLocked(res)
	}

	o.Mu.Unlock()

	o.signalWake()
}

// taskerPlanBody scans for the tasker's `plan` event and returns its `body` field.
// Tasker-specific — only this handler cares about plan events.
func taskerPlanBody(events []map[string]any) string {
	body := ""

	for _, ev := range events {
		if t, _ := ev["type"].(string); t != "plan" {
			continue
		}

		b, _ := ev["body"].(string)
		body = b
	}

	return body
}

func (o *Orchestrator) handleTaskerResultLocked(res AgentResult) {
	verdict, detail := lastVerdict(res.Events)
	plan := strings.TrimSpace(taskerPlanBody(res.Events))

	if plan == "" {
		reason := fmt.Sprintf("tasker produced no plan: verdict=%s detail=%s", verdict, detail)
		o.recordEventLocked(res.Ticket, "TASKER_NO_PLAN", reason)
		uiTicket("💀", RoleTasker, res.Ticket, "NO_PLAN", reason)
		o.closeTicketLocked(res.Ticket, CloseDiscarded)
		uiTicket("🪦", "", res.Ticket, "DISCARDED", "no plan from tasker")

		select {
		case o.QReplanner <- ReplanRequest{Source: RoleTasker, Ticket: res.Ticket, Reason: reason}:
			uiTicket("📥", RoleTasker, res.Ticket, "→Q_replanner", reason)
		default:
		}

		return
	}

	writePlan(o.Root, res.Ticket, plan)
	o.recordEventLocked(res.Ticket, "PLAN_WRITTEN", "ws="+res.Workspace+" summary="+detail)
	uiTicket("📝", RoleTasker, res.Ticket, "PLAN_WRITTEN", detail)

	// Tasker done; ticket stays InProgress=true. scheduleReady won't pick it again,
	// so spawn digger explicitly with a fresh workspace.
	if t, ok := o.findTicketLocked(res.Ticket); ok {
		o.startAgentForTicketLocked(t)
	}
}

func (o *Orchestrator) handleDiggerResultLocked(res AgentResult) {
	verdict, detail := lastVerdict(res.Events)

	switch verdict {
	case VerdictReady:
		// Structural sanity check: harness defaults often skip git commit unless explicitly
		// asked. If the digger emitted READY but the branch has zero commits ahead of base,
		// the work isn't actually visible to anyone — bounce back as REWORK with a concrete
		// reason so the next digger iteration commits.
		ahead := WorkspaceCommitsAhead(wsPath(o.Root, res.Workspace))

		if ahead == 0 {
			reason := fmt.Sprintf("READY claimed but %s has zero commits ahead of base — work was never committed; rerunning", res.Workspace)
			o.recordEventLocked(res.Ticket, "DIGGER_NO_COMMIT", reason)
			uiTicket("⚠️", RoleDigger, res.Ticket, "NO_COMMIT", reason)
			o.spawnDiggerSameWorkspaceLocked(res.Ticket, res.Workspace)

			return
		}

		o.recordEventLocked(res.Ticket, "DIGGER_READY", "ws="+res.Workspace+" summary="+detail)
		uiTicket("✅", RoleDigger, res.Ticket, "READY", detail)
		o.spawnReviewerLocked(res.Ticket, res.Workspace)
	case VerdictCantDo:
		reason := "digger can't do: " + detail
		o.recordEventLocked(res.Ticket, "DIGGER_CANT_DO", detail)
		uiTicket("🛑", RoleDigger, res.Ticket, "CANT_DO", detail)
		o.closeTicketLocked(res.Ticket, CloseDiscarded)
		uiTicket("🪦", "", res.Ticket, "DISCARDED", "digger gave up")

		select {
		case o.QReplanner <- ReplanRequest{Source: res.Role, Ticket: res.Ticket, Reason: reason}:
			uiTicket("📥", RoleDigger, res.Ticket, "→Q_replanner", reason)
		default:
		}
	default:
		reason := fmt.Sprintf("digger returned unclear verdict=%s detail=%s", verdict, detail)
		o.recordEventLocked(res.Ticket, "DIGGER_UNCLEAR", fmt.Sprintf("verdict=%s detail=%s", verdict, detail))
		uiTicket("❓", RoleDigger, res.Ticket, "UNCLEAR", fmt.Sprintf("verdict=%s", verdict))
		o.closeTicketLocked(res.Ticket, CloseDiscarded)
		uiTicket("🪦", "", res.Ticket, "DISCARDED", "digger unclear")

		select {
		case o.QReplanner <- ReplanRequest{Source: res.Role, Ticket: res.Ticket, Reason: reason}:
			uiTicket("📥", RoleDigger, res.Ticket, "→Q_replanner", reason)
		default:
		}
	}
}

func (o *Orchestrator) handleReviewerResultLocked(res AgentResult) {
	verdict, detail := lastVerdict(res.Events)

	switch verdict {
	case VerdictApprove:
		o.recordEventLocked(res.Ticket, "REVIEWER_APPROVE", detail)
		uiTicket("👍", RoleReviewer, res.Ticket, "APPROVE", detail)

		select {
		case o.QMerger <- MergeRequest{Ticket: res.Ticket, Workspace: res.Workspace}:
			uiTicket("📥", RoleReviewer, res.Ticket, "→Q_merger", "ws="+res.Workspace)
		default:
		}
	case VerdictRework:
		o.recordEventLocked(res.Ticket, "REVIEWER_REWORK", detail)
		uiTicket("🔁", RoleReviewer, res.Ticket, "REWORK", detail)

		select {
		case o.QReplanner <- ReplanRequest{Source: RoleReviewer, Ticket: res.Ticket, Reason: fmt.Sprintf("REWORK on T-%d: %s", res.Ticket, detail)}:
		default:
		}

		o.spawnDiggerSameWorkspaceLocked(res.Ticket, res.Workspace)
	case VerdictDiscard:
		o.recordEventLocked(res.Ticket, "REVIEWER_DISCARD", detail)
		uiTicket("👎", RoleReviewer, res.Ticket, "DISCARD", detail)
		o.closeTicketLocked(res.Ticket, CloseDiscarded)
		uiTicket("🪦", "", res.Ticket, "DISCARDED", "reviewer rejected")

		select {
		case o.QReplanner <- ReplanRequest{Source: res.Role, Ticket: res.Ticket, Reason: "reviewer discarded: " + detail}:
			uiTicket("📥", RoleReviewer, res.Ticket, "→Q_replanner", detail)
		default:
		}
	default:
		reason := fmt.Sprintf("reviewer unclear verdict=%s detail=%s", verdict, detail)
		o.recordEventLocked(res.Ticket, "REVIEWER_UNCLEAR", reason)
		uiTicket("❓", RoleReviewer, res.Ticket, "UNCLEAR", fmt.Sprintf("verdict=%s detail=%s", verdict, detail))
		o.closeTicketLocked(res.Ticket, CloseDiscarded)
		uiTicket("🪦", "", res.Ticket, "DISCARDED", "reviewer "+string(verdict))

		select {
		case o.QReplanner <- ReplanRequest{Source: res.Role, Ticket: res.Ticket, Reason: reason}:
			uiTicket("📥", RoleReviewer, res.Ticket, "→Q_replanner", reason)
		default:
		}
	}
}

func (o *Orchestrator) closeTicketLocked(n int, reason CloseReason) {
	for i := range o.Tickets {
		if o.Tickets[i].N != n {
			continue
		}

		// Idempotent: if the ticket was already CLOSED (e.g. via replanner cancel op →
		// CANCELLED), keep the original CLOSE_REASON. A late-arriving result from a
		// goroutine that was already cancelled must not rewrite history (CANCELLED →
		// DISCARDED).
		if o.Tickets[i].State == StateClosed {
			o.Tickets[i].InProgress = false

			continue
		}

		o.Tickets[i].State = StateClosed
		o.Tickets[i].CloseReason = reason
		o.Tickets[i].InProgress = false
	}

	SaveTasks(o.Root, o.Tickets)

	if o.openCountLocked() <= 2 {
		select {
		case o.QOverseer <- OverseerRequest{Reason: fmt.Sprintf("low-open after T-%d closed (%s)", n, reason)}:
			uiSys("📥", "→Q_overseer", fmt.Sprintf("after T-%d %s", n, reason))
		default:
		}
	}
}

func (o *Orchestrator) spawnReviewerLocked(ticketN int, ws string) {
	uiTicket("🚀", RoleReviewer, ticketN, "START", "ws="+ws)

	wsAbs := wsPath(o.Root, ws)
	input := o.buildAgentInput(RoleReviewer, ticketN, wsAbs)
	prompt := loadPrompt(RoleReviewer)
	stdin := concatPromptInput(prompt, input)

	go func() {
		res := o.runAgent(RoleReviewer, ticketN, ws, stdin)
		o.AgentDone <- res
	}()
}

func (o *Orchestrator) spawnDiggerSameWorkspaceLocked(ticketN int, ws string) {
	uiTicket("🚀", RoleDigger, ticketN, "START", "ws="+ws)

	wsAbs := wsPath(o.Root, ws)
	input := o.buildAgentInput(RoleDigger, ticketN, wsAbs) +
		fmt.Sprintf("PREV_WORKSPACE: %s\n", wsAbs)
	prompt := loadPrompt(RoleDigger)
	stdin := concatPromptInput(prompt, input)

	go func() {
		res := o.runAgent(RoleDigger, ticketN, ws, stdin)
		o.AgentDone <- res
	}()
}

func (o *Orchestrator) replannerLoop() {
	for {
		select {
		case <-o.StopCtx.Done():
			return
		case req := <-o.QReplanner:
			o.safe("REPLANNER", func() { o.runReplanner(req) })
		}
	}
}

func (o *Orchestrator) runReplanner(req ReplanRequest) {
	uiTicket("🚀", RoleReplanner, req.Ticket, "START", "reason="+req.Reason)

	wsID := NewWorkspace(o.Root, o.Trunk)

	o.Mu.Lock()
	currentTasks := SerializeTasks(o.Tickets)
	o.Mu.Unlock()

	input := o.agentSelfBlock(RoleReplanner) +
		fmt.Sprintf("REASON_FOR_REPLAN: %s\nSOURCE_AGENT: %s\nSOURCE_TICKET: %d\nRUNS_DIR: %s\nTASKS_DB: %s\n\nCURRENT_TASKS:\n%s\n",
			req.Reason, req.Source, req.Ticket, runsDir(o.Root), tasksDBPath(o.Root), currentTasks)

	prompt := loadPrompt(RoleReplanner)
	stdin := concatPromptInput(prompt, input)

	res := o.runAgent(RoleReplanner, req.Ticket, wsID, stdin)

	ops := replannerTaskOps(res.Events)

	if len(ops) == 0 {
		verdict, detail := lastVerdict(res.Events)
		uiTicket("💤", RoleReplanner, req.Ticket, "NO_ACTION", fmt.Sprintf("verdict=%s detail=%s", verdict, detail))

		return
	}

	o.applyReplannerOps(res, req, ops)
}

// replannerTaskOps pulls every `task` event out of the replanner's stream, in emission
// order. Replanner-private vocabulary: each event has an `op` field of "new" / "update"
// / "cancel" plus per-op fields (n, descr, prio, deps, reason). No other role emits
// task events.
func replannerTaskOps(events []map[string]any) []map[string]any {
	var out []map[string]any

	for _, ev := range events {
		if t, _ := ev["type"].(string); t == "task" {
			out = append(out, ev)
		}
	}

	return out
}

// applyTaskOp applies one `task` event to a ticket-list sandbox. Mutates entries in
// place (`update`, `cancel`) or grows the slice (`new`); throws on any schema violation.
// Apply ops in emission order — later ops see the cumulative effect of earlier ones, so
// `new T-7` then `update T-7` is valid; `cancel T-3` then `update T-3` fails because
// T-3 is now CLOSED. Caller (applyReplannerOps) sandboxes a copy before invoking this
// so a partial apply followed by a thrown error doesn't leave the live state corrupt.
func applyTaskOp(tickets []Ticket, ev map[string]any) []Ticket {
	op, _ := ev["op"].(string)
	n := jsonInt(ev["n"])

	if n <= 0 {
		ThrowFmt("task op %q: missing or invalid n", op)
	}

	idx := -1

	for i, t := range tickets {
		if t.N == n {
			idx = i

			break
		}
	}

	switch op {
	case "new":
		if idx >= 0 {
			ThrowFmt("op=new ticket %d: N already exists", n)
		}

		descr, _ := ev["descr"].(string)
		prio := jsonInt(ev["prio"])
		deps := jsonIntArray(ev["deps"])

		return append(tickets, Ticket{
			N:     n,
			State: StateOpen,
			Descr: descr,
			Prio:  prio,
			Deps:  deps,
		})

	case "update":
		if idx < 0 {
			ThrowFmt("op=update ticket %d: not found", n)
		}

		if tickets[idx].State != StateOpen {
			ThrowFmt("op=update ticket %d: not OPEN (state=%s)", n, tickets[idx].State)
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

	case "cancel":
		if idx < 0 {
			ThrowFmt("op=cancel ticket %d: not found", n)
		}

		if tickets[idx].State != StateOpen {
			ThrowFmt("op=cancel ticket %d: not OPEN (state=%s)", n, tickets[idx].State)
		}

		tickets[idx].State = StateClosed
		tickets[idx].CloseReason = CloseCancelled
		tickets[idx].InProgress = false

		return tickets
	}

	ThrowFmt("unknown task op %q (expected new/update/cancel)", op)

	return tickets
}

// jsonInt accepts JSON numbers (float64 after Unmarshal-into-any), bare ints, or string
// forms ("42", "T-42") and returns the int. 0 on failure — callers check sign.
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
	arr, _ := v.([]any)

	var out []int

	for _, x := range arr {
		out = append(out, jsonInt(x))
	}

	return out
}

// applyReplannerOps sandboxes the ops, validates the resulting list against the global
// schema (no cycles, valid deps, etc.), then commits in one shot — swapping o.Tickets,
// killing in-flight goroutines for every `cancel` op, and recording per-ticket events.
// On any apply or validation error nothing is committed; the replanner is bounced with
// its original output as feedback so it can fix and retry.
func (o *Orchestrator) applyReplannerOps(res AgentResult, req ReplanRequest, ops []map[string]any) {
	o.Mu.Lock()

	sandbox := make([]Ticket, len(o.Tickets))
	copy(sandbox, o.Tickets)

	exc := Try(func() {
		for _, ev := range ops {
			sandbox = applyTaskOp(sandbox, ev)
		}

		ValidateTasks(sandbox)
	})

	if exc != nil {
		o.Mu.Unlock()

		o.recordEvent(req.Ticket, "REPLAN_REJECTED", exc.Error())
		uiTicket("❌", RoleReplanner, req.Ticket, "REJECTED", exc.Error())

		feedback := fmt.Sprintf("previous replanner output invalid: %s\n\nREJECTED_OUTPUT:\n%s",
			exc.Error(), res.Stdout)

		select {
		case o.QReplanner <- ReplanRequest{Source: RoleReplanner, Ticket: req.Ticket, Reason: feedback}:
			uiTicket("📥", RoleReplanner, req.Ticket, "→Q_replanner", "retry after reject")
		default:
		}

		return
	}

	// Validation passed — swap state, then apply the ops' side-effects (kill in-flight
	// goroutines on cancel, record per-ticket events). recordEventLocked saves
	// tasks.jsonl on each call which covers the final write too.
	o.Tickets = sandbox

	canceledAny := false

	for _, ev := range ops {
		op, _ := ev["op"].(string)
		n := jsonInt(ev["n"])
		reason, _ := ev["reason"].(string)

		switch op {
		case "new":
			descr, _ := ev["descr"].(string)
			o.recordEventLocked(n, "TASK_NEW", "by=replanner descr="+descr)
			uiTicket("🆕", RoleReplanner, n, "NEW", descr)
		case "update":
			var changes []string

			if d, ok := ev["descr"].(string); ok {
				changes = append(changes, "descr="+d)
			}

			if _, ok := ev["prio"]; ok {
				changes = append(changes, fmt.Sprintf("prio=%d", jsonInt(ev["prio"])))
			}

			if _, ok := ev["deps"]; ok {
				changes = append(changes, fmt.Sprintf("deps=%v", jsonIntArray(ev["deps"])))
			}

			summary := strings.Join(changes, " ")
			o.recordEventLocked(n, "TASK_UPDATE", "by=replanner "+summary)
			uiTicket("✏️", RoleReplanner, n, "UPDATE", summary)
		case "cancel":
			canceledAny = true

			// We never kill the agent goroutine — it runs to completion and its result
			// is dropped by handleAgentResult's STALE check. Closing the ticket here
			// (already done by applyTaskOp) is the only state change we need.

			detail := "by=replanner"

			if reason != "" {
				detail += " reason=" + reason
			}

			o.recordEventLocked(n, "CANCELLED", detail)
			uiTicket("🛑", RoleReplanner, n, "CANCELLED", reason)
		}
	}

	// Only fire an overseer nudge when cancel ops actually dropped open tickets — new /
	// update ops can't reduce open count.
	if canceledAny && o.openCountLocked() <= 2 {
		select {
		case o.QOverseer <- OverseerRequest{Reason: fmt.Sprintf("low-open after replanner batch (req T-%d)", req.Ticket)}:
			uiSys("📥", "→Q_overseer", "after replanner cancels")
		default:
		}
	}

	o.Mu.Unlock()

	o.recordEvent(req.Ticket, "REPLAN_APPLIED", fmt.Sprintf("ops=%d", len(ops)))
	uiTicket("✨", RoleReplanner, req.Ticket, "APPLIED", fmt.Sprintf("%d ops", len(ops)))
	o.signalWake()
}

func (o *Orchestrator) mergerLoop() {
	for {
		select {
		case <-o.StopCtx.Done():
			return
		case req := <-o.QMerger:
			o.safe("MERGER", func() { o.runMerger(req) })
		}
	}
}

func (o *Orchestrator) runMerger(req MergeRequest) {
	o.Mu.Lock()
	t, ok := o.findTicketLocked(req.Ticket)

	if !ok || t.State == StateClosed {
		o.setInProgressLocked(req.Ticket, false)
		o.Mu.Unlock()
		uiTicket("👻", RoleMerger, req.Ticket, "STALE", fmt.Sprintf("ticket %s before merger picked it up; skipping", t.CloseReason))

		return
	}

	o.Mu.Unlock()

	uiTicket("🚀", RoleMerger, req.Ticket, "START", "ws="+req.Workspace)

	trunkHead := CurrentTrunkHash(o.Trunk)
	newGoals := readGoalsHash(o.Trunk)
	mergerWS := NewWorkspace(o.Root, o.Trunk)

	o.Mu.Lock()
	prevGoals := o.GoalsHash
	o.GoalsHash = newGoals
	o.Mu.Unlock()

	if prevGoals != "" && newGoals != prevGoals {
		select {
		case o.QReplanner <- ReplanRequest{Source: RoleMerger, Reason: "GOALS.md changed locally in trunk"}:
		default:
		}
	}

	diggerBranch := "ovs/" + req.Workspace
	diggerWSAbs := wsPath(o.Root, req.Workspace)
	mergerWSAbs := wsPath(o.Root, mergerWS)

	input := o.agentSelfBlock(RoleMerger) +
		fmt.Sprintf("TICKET: %d\nDIGGER_BRANCH: %s\nDIGGER_WORKTREE: %s\nMERGER_WORKTREE: %s\nTRUNK_HEAD: %s\n",
			req.Ticket, diggerBranch, diggerWSAbs, mergerWSAbs, trunkHead)
	prompt := loadPrompt(RoleMerger)
	stdin := concatPromptInput(prompt, input)

	res := o.runAgent(RoleMerger, req.Ticket, mergerWS, stdin)

	for _, line := range eventReplans(res.Events) {
		select {
		case o.QReplanner <- ReplanRequest{Source: RoleMerger, Ticket: req.Ticket, Reason: line}:
		default:
		}
	}

	verdict, detail := lastVerdict(res.Events)

	switch verdict {
	case VerdictMerged:
		FetchBranch(o.Trunk, mergerWSAbs, "ovs/"+mergerWS)
		ok, out := FfMergeBranch(o.Trunk, "ovs/"+mergerWS)
		newHead := CurrentTrunkHash(o.Trunk)

		if !ok {
			o.recordEvent(req.Ticket, "MERGE_FF_FAIL", out)
			uiTicket("⚠️", RoleMerger, req.Ticket, "FF_FAIL", out)
			o.spawnDiggerWithRebase(req.Ticket, req.Workspace, newHead, out)

			return
		}

		o.recordEvent(req.Ticket, "MERGED", "ws="+req.Workspace+" merger_ws="+mergerWS+" head="+newHead)
		uiTicket("✅", RoleMerger, req.Ticket, "MERGED", "head="+newHead[:8])

		o.Mu.Lock()
		o.closeTicketLocked(req.Ticket, CloseMerged)
		o.Mu.Unlock()

		uiTicket("🏁", "", req.Ticket, "CLOSED", "MERGED")

		select {
		case o.QReplanner <- ReplanRequest{Source: RoleMerger, Ticket: req.Ticket, Reason: "merged"}:
			uiTicket("📥", RoleMerger, req.Ticket, "→Q_replanner", "post-merge")
		default:
		}

		o.signalWake()

	case VerdictMergeFail:
		o.recordEvent(req.Ticket, "MERGE_FAIL", detail)
		uiTicket("❌", RoleMerger, req.Ticket, "FAIL", detail)

		select {
		case o.QReplanner <- ReplanRequest{Source: RoleMerger, Ticket: req.Ticket, Reason: fmt.Sprintf("MERGE_FAIL on T-%d: %s", req.Ticket, detail)}:
		default:
		}

		head := CurrentTrunkHash(o.Trunk)

		o.spawnDiggerWithRebase(req.Ticket, req.Workspace, head, detail)

	default:
		o.recordEvent(req.Ticket, "MERGE_UNCLEAR", fmt.Sprintf("verdict=%s detail=%s", verdict, detail))
		uiTicket("❓", RoleMerger, req.Ticket, "UNCLEAR", fmt.Sprintf("verdict=%s", verdict))

		o.Mu.Lock()
		o.setInProgressLocked(req.Ticket, false)
		o.Mu.Unlock()

		o.signalWake()
	}
}

func (o *Orchestrator) spawnDiggerWithRebase(ticketN int, ws, target, mergeOut string) {
	o.Mu.Lock()

	short := target

	if len(short) > 8 {
		short = short[:8]
	}

	uiTicket("🚀", RoleDigger, ticketN, "START", "ws="+ws+" rebase→"+short)

	wsAbs := wsPath(o.Root, ws)
	input := o.buildAgentInput(RoleDigger, ticketN, wsAbs) +
		fmt.Sprintf("PREV_WORKSPACE: %s\n", wsAbs) +
		"\nMERGE_FAIL_OUTPUT:\n" + mergeOut + "\nREBASE_TARGET: " + target + "\n"
	prompt := loadPrompt(RoleDigger)
	stdin := concatPromptInput(prompt, input)

	o.Mu.Unlock()

	go func() {
		res := o.runAgent(RoleDigger, ticketN, ws, stdin)
		o.AgentDone <- res
	}()
}

func (o *Orchestrator) overseerLoop() {
	for {
		select {
		case <-o.StopCtx.Done():
			return
		case req := <-o.QOverseer:
			o.safe("OVERSEER", func() { o.runOverseer(req) })
		}
	}
}

func (o *Orchestrator) runOverseer(req OverseerRequest) {
	uiSys("🚀", "OVERSEER_START", "reason="+req.Reason)

	wsID := NewWorkspace(o.Root, o.Trunk)
	

	o.Mu.Lock()
	currentTasks := SerializeTasks(o.Tickets)
	o.Mu.Unlock()

	input := o.agentSelfBlock(RoleOverseer) +
		fmt.Sprintf("REASON: %s\n\nCURRENT_TASKS:\n%s\n", req.Reason, currentTasks)
	prompt := loadPrompt(RoleOverseer)
	stdin := concatPromptInput(prompt, input)

	res := o.runAgent(RoleOverseer, 0, wsID, stdin)

	verdict, _ := lastVerdict(res.Events)
	replans := eventReplans(res.Events)

	switch verdict {
	case VerdictGoalsAchieved:
		uiSys("🎯", "GOALS_ACHIEVED", "stopping orchestrator")
		o.writeReport()
		o.StopCancel()
	default:
		uiSys("🦉", "OVERSEER_DONE", fmt.Sprintf("verdict=%s replans=%d", verdict, len(replans)))

		for _, line := range replans {
			select {
			case o.QReplanner <- ReplanRequest{Source: RoleOverseer, Ticket: 0, Reason: line}:
				uiSys("📥", "→Q_replanner", "from overseer: "+line)
			default:
			}
		}
	}
}

func (o *Orchestrator) writeReport() {
	o.Mu.Lock()
	defer o.Mu.Unlock()

	var sb strings.Builder
	sb.WriteString("# Overseer Report\n\n")

	for _, t := range o.Tickets {
		fmt.Fprintf(&sb, "- T-%d [%s] %s — %s\n", t.N, t.State, t.CloseReason, t.Descr)
	}

	_ = os.WriteFile(o.Root+"/REPORT.md", []byte(sb.String()), 0644)
}
