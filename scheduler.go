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

func NewOrchestrator(root, trunk, harness string, backend Backend, models map[string]string, jailBin string) *Orchestrator {
	ctx, cancel := context.WithCancel(context.Background())

	o := &Orchestrator{
		Root:       root,
		Trunk:      trunk,
		Harness:    harness,
		Backend:    backend,
		Models:     models,
		JailBin:    jailBin,
		Inflight:   map[int]*AgentRun{},
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

	ctx, cancel := context.WithCancel(o.StopCtx)
	run := &AgentRun{
		Role:      role,
		Ticket:    t.N,
		Workspace: wsID,
		Cancel:    cancel,
	}
	o.Inflight[t.N] = run

	o.recordEventLocked(t.N, "AGENT_START", fmt.Sprintf("role=%s ws=%s", role, wsID))
	uiTicket("🚀", role, t.N, "START", "ws="+wsID)

	input := o.buildAgentInput(role, t.N, wsAbs)
	prompt := loadPrompt(o.Root, role)
	stdin := concatPromptInput(prompt, input)

	go func() {
		defer cancel()
		res := o.runAgent(ctx, role, t.N, wsID, stdin)
		o.AgentDone <- res
	}()
}

// agentSelfBlock identifies the agent to itself: which role it is, which model is driving
// it, which harness backend. Goes at the top of every agent's input so the agent can use
// it for self-aware decisions (e.g. "I'm running on a small model, keep edits cheap").
func (o *Orchestrator) agentSelfBlock(role AgentRole) string {
	model := o.modelForRole(role)

	if model == "" {
		model = "(harness default)"
	}

	return fmt.Sprintf("ROLE: %s\nMODEL: %s\nHARNESS: %s\nMESSAGES_LOG: %s\n",
		role, model, o.Backend, messagesLogPath(o.Root))
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

	delete(o.Inflight, res.Ticket)

	// If the ticket was closed while the agent was running (cancelTicket or replanner
	// DISCARD via TASKS_NEW), drop the result and stop the pipeline at this transition.
	// No follow-up spawn (reviewer/merger/digger), no state rewrite — also clear the
	// InProgress flag (still set if replanner closed via TASKS_NEW which preserves it).
	if t, ok := o.findTicketLocked(res.Ticket); ok && t.State == StateClosed {
		o.setInProgressLocked(res.Ticket, false)
		o.Mu.Unlock()
		uiTicket("👻", res.Role, res.Ticket, "STALE", fmt.Sprintf("ticket already %s, dropping %s result", t.CloseReason, res.Verdict))

		return
	}

	for _, line := range res.ReplanLines {
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

func (o *Orchestrator) handleTaskerResultLocked(res AgentResult) {
	plan := extractPlan(res.Stdout)

	if plan == "" {
		reason := fmt.Sprintf("tasker produced no plan: verdict=%s detail=%s", res.Verdict, res.Detail)
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
	o.recordEventLocked(res.Ticket, "PLAN_WRITTEN", "ws="+res.Workspace+" summary="+res.Detail)
	uiTicket("📝", RoleTasker, res.Ticket, "PLAN_WRITTEN", res.Detail)

	// Tasker done; ticket stays InProgress=true. scheduleReady won't pick it again,
	// so spawn digger explicitly with a fresh workspace.
	if t, ok := o.findTicketLocked(res.Ticket); ok {
		o.startAgentForTicketLocked(t)
	}
}

func (o *Orchestrator) handleDiggerResultLocked(res AgentResult) {
	switch res.Verdict {
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

		o.recordEventLocked(res.Ticket, "DIGGER_READY", "ws="+res.Workspace+" summary="+res.Detail)
		uiTicket("✅", RoleDigger, res.Ticket, "READY", res.Detail)
		o.spawnReviewerLocked(res.Ticket, res.Workspace)
	case VerdictCantDo:
		reason := "digger can't do: " + res.Detail
		o.recordEventLocked(res.Ticket, "DIGGER_CANT_DO", res.Detail)
		uiTicket("🛑", RoleDigger, res.Ticket, "CANT_DO", res.Detail)
		o.closeTicketLocked(res.Ticket, CloseDiscarded)
		uiTicket("🪦", "", res.Ticket, "DISCARDED", "digger gave up")

		select {
		case o.QReplanner <- ReplanRequest{Source: res.Role, Ticket: res.Ticket, Reason: reason}:
			uiTicket("📥", RoleDigger, res.Ticket, "→Q_replanner", reason)
		default:
		}
	case VerdictCrashed:
		reason := "digger crashed: " + res.Detail
		o.recordEventLocked(res.Ticket, "DIGGER_CRASHED", res.Detail)
		uiTicket("💥", RoleDigger, res.Ticket, "CRASHED", res.Detail)
		o.closeTicketLocked(res.Ticket, CloseDiscarded)
		uiTicket("🪦", "", res.Ticket, "DISCARDED", "digger crashed")

		select {
		case o.QReplanner <- ReplanRequest{Source: res.Role, Ticket: res.Ticket, Reason: reason}:
			uiTicket("📥", RoleDigger, res.Ticket, "→Q_replanner", reason)
		default:
		}
	default:
		reason := fmt.Sprintf("digger returned unclear verdict=%s detail=%s", res.Verdict, res.Detail)
		o.recordEventLocked(res.Ticket, "DIGGER_UNCLEAR", fmt.Sprintf("verdict=%s detail=%s", res.Verdict, res.Detail))
		uiTicket("❓", RoleDigger, res.Ticket, "UNCLEAR", fmt.Sprintf("verdict=%s", res.Verdict))
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
	switch res.Verdict {
	case VerdictApprove:
		o.recordEventLocked(res.Ticket, "REVIEWER_APPROVE", res.Detail)
		uiTicket("👍", RoleReviewer, res.Ticket, "APPROVE", res.Detail)

		select {
		case o.QMerger <- MergeRequest{Ticket: res.Ticket, Workspace: res.Workspace}:
			uiTicket("📥", RoleReviewer, res.Ticket, "→Q_merger", "ws="+res.Workspace)
		default:
		}
	case VerdictRework:
		o.recordEventLocked(res.Ticket, "REVIEWER_REWORK", res.Detail)
		uiTicket("🔁", RoleReviewer, res.Ticket, "REWORK", res.Detail)

		select {
		case o.QReplanner <- ReplanRequest{Source: RoleReviewer, Ticket: res.Ticket, Reason: fmt.Sprintf("REWORK on T-%d: %s", res.Ticket, res.Detail)}:
		default:
		}

		o.spawnDiggerSameWorkspaceLocked(res.Ticket, res.Workspace)
	case VerdictDiscard:
		o.recordEventLocked(res.Ticket, "REVIEWER_DISCARD", res.Detail)
		uiTicket("👎", RoleReviewer, res.Ticket, "DISCARD", res.Detail)
		o.closeTicketLocked(res.Ticket, CloseDiscarded)
		uiTicket("🪦", "", res.Ticket, "DISCARDED", "reviewer rejected")

		select {
		case o.QReplanner <- ReplanRequest{Source: res.Role, Ticket: res.Ticket, Reason: "reviewer discarded: " + res.Detail}:
			uiTicket("📥", RoleReviewer, res.Ticket, "→Q_replanner", res.Detail)
		default:
		}
	default:
		reason := fmt.Sprintf("reviewer unclear verdict=%s detail=%s", res.Verdict, res.Detail)
		o.recordEventLocked(res.Ticket, "REVIEWER_UNCLEAR", reason)
		uiTicket("❓", RoleReviewer, res.Ticket, "UNCLEAR", fmt.Sprintf("verdict=%s detail=%s", res.Verdict, res.Detail))
		o.closeTicketLocked(res.Ticket, CloseDiscarded)
		uiTicket("🪦", "", res.Ticket, "DISCARDED", "reviewer "+string(res.Verdict))

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

		// Idempotent: if the ticket was already CLOSED (e.g. via cancelTicket → CANCELLED),
		// keep the original CLOSE_REASON. A late-arriving result from a goroutine that was
		// already cancelled must not rewrite history (CANCELLED → DISCARDED).
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
	ctx, cancel := context.WithCancel(o.StopCtx)
	run := &AgentRun{
		Role:      RoleReviewer,
		Ticket:    ticketN,
		Workspace: ws,
		Cancel:    cancel,
	}
	o.Inflight[ticketN] = run

	uiTicket("🚀", RoleReviewer, ticketN, "START", "ws="+ws)

	wsAbs := wsPath(o.Root, ws)
	input := o.buildAgentInput(RoleReviewer, ticketN, wsAbs)
	prompt := loadPrompt(o.Root, RoleReviewer)
	stdin := concatPromptInput(prompt, input)

	go func() {
		defer cancel()
		res := o.runAgent(ctx, RoleReviewer, ticketN, ws, stdin)
		o.AgentDone <- res
	}()
}

func (o *Orchestrator) spawnDiggerSameWorkspaceLocked(ticketN int, ws string) {
	ctx, cancel := context.WithCancel(o.StopCtx)
	run := &AgentRun{
		Role:      RoleDigger,
		Ticket:    ticketN,
		Workspace: ws,
		Cancel:    cancel,
	}
	o.Inflight[ticketN] = run

	uiTicket("🚀", RoleDigger, ticketN, "START", "ws="+ws)

	wsAbs := wsPath(o.Root, ws)
	input := o.buildAgentInput(RoleDigger, ticketN, wsAbs) +
		fmt.Sprintf("PREV_WORKSPACE: %s\n", wsAbs)
	prompt := loadPrompt(o.Root, RoleDigger)
	stdin := concatPromptInput(prompt, input)

	go func() {
		defer cancel()
		res := o.runAgent(ctx, RoleDigger, ticketN, ws, stdin)
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

	prompt := loadPrompt(o.Root, RoleReplanner)
	stdin := concatPromptInput(prompt, input)

	ctx, cancel := context.WithCancel(o.StopCtx)
	defer cancel()

	res := o.runAgent(ctx, RoleReplanner, req.Ticket, wsID, stdin)

	cancels := ExtractCancelTickets(res.Stdout)

	for _, n := range cancels {
		o.cancelTicket(n)
	}

	hasTasksNew := strings.Contains(res.Stdout, "TASKS_NEW:")

	if hasTasksNew {
		o.tryApplyReplanOutput(res, req)
	}

	if !hasTasksNew && len(cancels) == 0 {
		uiTicket("💤", RoleReplanner, req.Ticket, "NO_ACTION", fmt.Sprintf("verdict=%s detail=%s", res.Verdict, res.Detail))
	}
}

func (o *Orchestrator) cancelTicket(n int) {
	o.Mu.Lock()
	defer o.Mu.Unlock()

	if run, ok := o.Inflight[n]; ok {
		run.Cancel()
		delete(o.Inflight, n)
	}

	o.closeTicketLocked(n, CloseCancelled)
	o.recordEventLocked(n, "CANCELLED", "by=replanner")
	uiTicket("🛑", RoleReplanner, n, "CANCELLED", "by replanner")
	o.signalWake()
}

func (o *Orchestrator) tryApplyReplanOutput(res AgentResult, req ReplanRequest) {
	idx := strings.Index(res.Stdout, "TASKS_NEW:")

	if idx < 0 {
		return
	}

	body := trimAtVerdict(res.Stdout[idx+len("TASKS_NEW:"):])

	exc := Try(func() {
		newTickets := ParseTasks(body)
		ValidateTasks(newTickets)

		o.Mu.Lock()
		defer o.Mu.Unlock()

		validateReplanTransition(o.Tickets, newTickets)

		// Preserve orchestrator-owned fields across replan: events are append-only history
		// the orchestrator writes (replanner is told not to include them, and even if they
		// do we always restore the authoritative copy from prior); InProgress is in-memory.
		priorEvents := map[int][]TicketEvent{}
		priorWS := map[int][]string{}
		inProgress := map[int]bool{}

		for _, t := range o.Tickets {
			priorEvents[t.N] = t.Events
			priorWS[t.N] = t.Workspaces

			if t.InProgress {
				inProgress[t.N] = true
			}
		}

		for i := range newTickets {
			n := newTickets[i].N
			newTickets[i].Events = priorEvents[n]
			newTickets[i].Workspaces = priorWS[n]
			newTickets[i].InProgress = inProgress[n]
		}

		o.Tickets = newTickets
		SaveTasks(o.Root, o.Tickets)
	})

	if exc != nil {
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

	o.recordEvent(req.Ticket, "REPLAN_APPLIED", "by=replanner")
	uiTicket("✨", RoleReplanner, req.Ticket, "APPLIED", "TASKS.md updated")
	o.signalWake()
}

// validateReplanTransition enforces that replanner can only change ticket DESCR/DEPS/PRIO
// or close OPEN tickets as DISCARDED/CANCELLED — never MERGED (only the merger can mint a
// MERGED state after actually fast-forwarding into trunk), and never resurrect or delete
// already-CLOSED tickets (history is immutable). Without this check the replanner has been
// observed to mass-mark live OPEN tickets as CLOSED+MERGED, faking work that never landed.
func validateReplanTransition(prior, next []Ticket) {
	priorByN := map[int]Ticket{}

	for _, t := range prior {
		priorByN[t.N] = t
	}

	seen := map[int]bool{}

	for _, t := range next {
		seen[t.N] = true

		p, was := priorByN[t.N]

		if !was {
			if t.State != StateOpen {
				ThrowFmt("ticket %d: new tickets must be STATE=OPEN, got %s", t.N, t.State)
			}

			continue
		}

		if p.State == StateClosed {
			if t.State != StateClosed || t.CloseReason != p.CloseReason {
				ThrowFmt("ticket %d: cannot modify CLOSED history (was STATE=%s/CLOSE_REASON=%s, attempted STATE=%s/CLOSE_REASON=%s)",
					t.N, p.State, p.CloseReason, t.State, t.CloseReason)
			}

			continue
		}

		if t.State == StateClosed && t.CloseReason == CloseMerged {
			ThrowFmt("ticket %d: replanner cannot set CLOSE_REASON=MERGED — only the merger may mint MERGED after a real fast-forward into trunk", t.N)
		}
	}

	for _, p := range prior {
		if !seen[p.N] {
			ThrowFmt("ticket %d: missing from new TASKS — replanner must preserve every prior ticket (CLOSED verbatim, OPEN modifiable)", p.N)
		}
	}
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
	prompt := loadPrompt(o.Root, RoleMerger)
	stdin := concatPromptInput(prompt, input)

	ctx, cancel := context.WithCancel(o.StopCtx)
	defer cancel()

	res := o.runAgent(ctx, RoleMerger, req.Ticket, mergerWS, stdin)

	for _, line := range res.ReplanLines {
		select {
		case o.QReplanner <- ReplanRequest{Source: RoleMerger, Ticket: req.Ticket, Reason: line}:
		default:
		}
	}

	switch res.Verdict {
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

	case VerdictMergeFail, VerdictCrashed:
		o.recordEvent(req.Ticket, "MERGE_FAIL", res.Detail)
		uiTicket("❌", RoleMerger, req.Ticket, "FAIL", res.Detail)

		select {
		case o.QReplanner <- ReplanRequest{Source: RoleMerger, Ticket: req.Ticket, Reason: fmt.Sprintf("MERGE_FAIL on T-%d: %s", req.Ticket, res.Detail)}:
		default:
		}

		head := CurrentTrunkHash(o.Trunk)

		o.spawnDiggerWithRebase(req.Ticket, req.Workspace, head, res.Detail)

	default:
		o.recordEvent(req.Ticket, "MERGE_UNCLEAR", fmt.Sprintf("verdict=%s detail=%s", res.Verdict, res.Detail))
		uiTicket("❓", RoleMerger, req.Ticket, "UNCLEAR", fmt.Sprintf("verdict=%s", res.Verdict))

		o.Mu.Lock()
		o.setInProgressLocked(req.Ticket, false)
		o.Mu.Unlock()

		o.signalWake()
	}
}

func (o *Orchestrator) spawnDiggerWithRebase(ticketN int, ws, target, mergeOut string) {
	o.Mu.Lock()

	ctx, cancel := context.WithCancel(o.StopCtx)
	run := &AgentRun{
		Role:      RoleDigger,
		Ticket:    ticketN,
		Workspace: ws,
		Cancel:    cancel,
	}
	o.Inflight[ticketN] = run

	short := target

	if len(short) > 8 {
		short = short[:8]
	}

	uiTicket("🚀", RoleDigger, ticketN, "START", "ws="+ws+" rebase→"+short)

	wsAbs := wsPath(o.Root, ws)
	input := o.buildAgentInput(RoleDigger, ticketN, wsAbs) +
		fmt.Sprintf("PREV_WORKSPACE: %s\n", wsAbs) +
		"\nMERGE_FAIL_OUTPUT:\n" + mergeOut + "\nREBASE_TARGET: " + target + "\n"
	prompt := loadPrompt(o.Root, RoleDigger)
	stdin := concatPromptInput(prompt, input)

	o.Mu.Unlock()

	go func() {
		defer cancel()
		res := o.runAgent(ctx, RoleDigger, ticketN, ws, stdin)
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
	prompt := loadPrompt(o.Root, RoleOverseer)
	stdin := concatPromptInput(prompt, input)

	ctx, cancel := context.WithCancel(o.StopCtx)
	defer cancel()

	res := o.runAgent(ctx, RoleOverseer, 0, wsID, stdin)

	switch res.Verdict {
	case VerdictGoalsAchieved:
		uiSys("🎯", "GOALS_ACHIEVED", "stopping orchestrator")
		o.writeReport()
		o.StopCancel()
	default:
		uiSys("🦉", "OVERSEER_DONE", fmt.Sprintf("verdict=%s replans=%d", res.Verdict, len(res.ReplanLines)))

		for _, line := range res.ReplanLines {
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
