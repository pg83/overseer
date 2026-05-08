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

func NewOrchestrator(root, trunk, harness string, backend Backend, model, jailBin string) *Orchestrator {
	ctx, cancel := context.WithCancel(context.Background())

	o := &Orchestrator{
		Root:       root,
		Trunk:      trunk,
		Harness:    harness,
		Backend:    backend,
		Model:      model,
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
			o.handleAgentResult(res)
		}

		o.scheduleReady()
	}
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

		if _, busy := o.Inflight[t.N]; busy {
			o.Mu.Unlock()

			continue
		}

		o.startAgentForTicketLocked(t)

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

		if _, busy := o.Inflight[t.N]; busy {
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

	o.TrunkMu.Lock()
	wsID := NewWorkspace(o.Root, o.Trunk)
	o.TrunkMu.Unlock()

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

	appendTicketLog(o.Root, t.N, "AGENT_START", fmt.Sprintf("role=%s ws=%s", role, wsID))
	uiTicket("🚀", role, t.N, "START", "ws="+wsID)

	input := o.buildAgentInput(role, t.N, wsAbs)
	prompt := loadPrompt(o.Root, role)
	stdin := concatPromptInput(prompt, input)

	go func() {
		res := o.runAgent(ctx, role, t.N, wsID, stdin)
		dumpAgentRun(o.Root, role, t.N, wsID, stdin, res)
		o.AgentDone <- res
	}()
}

func (o *Orchestrator) buildAgentInput(role AgentRole, ticketN int, wsAbs string) string {
	var sb strings.Builder

	t, _ := o.findTicketLocked(ticketN)
	fmt.Fprintf(&sb, "TICKET: %d\nDESCR: %s\nPRIO: %d\nDEPS: %v\n", t.N, t.Descr, t.Prio, t.Deps)
	fmt.Fprintf(&sb, "WORKSPACE: %s\n", wsAbs)
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

func (o *Orchestrator) handleAgentResult(res AgentResult) {
	o.Mu.Lock()

	delete(o.Inflight, res.Ticket)

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
		appendTicketLog(o.Root, res.Ticket, "TASKER_NO_PLAN", reason)
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
	appendTicketLog(o.Root, res.Ticket, "PLAN_WRITTEN", "ws="+res.Workspace+" summary="+res.Detail)
	uiTicket("📝", RoleTasker, res.Ticket, "PLAN_WRITTEN", res.Detail)
}

func (o *Orchestrator) handleDiggerResultLocked(res AgentResult) {
	switch res.Verdict {
	case VerdictReady:
		appendTicketLog(o.Root, res.Ticket, "DIGGER_READY", "ws="+res.Workspace+" summary="+res.Detail)
		uiTicket("✅", RoleDigger, res.Ticket, "READY", res.Detail)
		o.spawnReviewerLocked(res.Ticket, res.Workspace)
	case VerdictCantDo:
		reason := "digger can't do: " + res.Detail
		appendTicketLog(o.Root, res.Ticket, "DIGGER_CANT_DO", res.Detail)
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
		appendTicketLog(o.Root, res.Ticket, "DIGGER_CRASHED", res.Detail)
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
		appendTicketLog(o.Root, res.Ticket, "DIGGER_UNCLEAR", fmt.Sprintf("verdict=%s detail=%s", res.Verdict, res.Detail))
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
		appendTicketLog(o.Root, res.Ticket, "REVIEWER_APPROVE", res.Detail)
		uiTicket("👍", RoleReviewer, res.Ticket, "APPROVE", res.Detail)

		select {
		case o.QMerger <- MergeRequest{Ticket: res.Ticket, Workspace: res.Workspace}:
			uiTicket("📥", RoleReviewer, res.Ticket, "→Q_merger", "ws="+res.Workspace)
		default:
		}
	case VerdictRework:
		appendTicketLog(o.Root, res.Ticket, "REVIEWER_REWORK", res.Detail)
		uiTicket("🔁", RoleReviewer, res.Ticket, "REWORK", res.Detail)
		o.spawnDiggerSameWorkspaceLocked(res.Ticket, res.Workspace)
	case VerdictDiscard:
		appendTicketLog(o.Root, res.Ticket, "REVIEWER_DISCARD", res.Detail)
		uiTicket("👎", RoleReviewer, res.Ticket, "DISCARD", res.Detail)
		o.closeTicketLocked(res.Ticket, CloseDiscarded)
		uiTicket("🪦", "", res.Ticket, "DISCARDED", "reviewer rejected")

		select {
		case o.QReplanner <- ReplanRequest{Source: res.Role, Ticket: res.Ticket, Reason: "reviewer discarded: " + res.Detail}:
			uiTicket("📥", RoleReviewer, res.Ticket, "→Q_replanner", res.Detail)
		default:
		}
	}
}

func (o *Orchestrator) closeTicketLocked(n int, reason CloseReason) {
	for i := range o.Tickets {
		if o.Tickets[i].N == n {
			o.Tickets[i].State = StateClosed
			o.Tickets[i].CloseReason = reason
		}
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

	wsAbs := wsPath(o.Root, ws)
	input := o.buildAgentInput(RoleReviewer, ticketN, wsAbs)
	prompt := loadPrompt(o.Root, RoleReviewer)
	stdin := concatPromptInput(prompt, input)

	go func() {
		res := o.runAgent(ctx, RoleReviewer, ticketN, ws, stdin)
		dumpAgentRun(o.Root, RoleReviewer, ticketN, ws, stdin, res)
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

	wsAbs := wsPath(o.Root, ws)
	input := o.buildAgentInput(RoleDigger, ticketN, wsAbs)
	prompt := loadPrompt(o.Root, RoleDigger)
	stdin := concatPromptInput(prompt, input)

	go func() {
		res := o.runAgent(ctx, RoleDigger, ticketN, ws, stdin)
		dumpAgentRun(o.Root, RoleDigger, ticketN, ws, stdin, res)
		o.AgentDone <- res
	}()
}

func (o *Orchestrator) replannerLoop() {
	for {
		select {
		case <-o.StopCtx.Done():
			return
		case req := <-o.QReplanner:
			o.runReplanner(req)
		}
	}
}

func (o *Orchestrator) runReplanner(req ReplanRequest) {
	uiTicket("🚀", RoleReplanner, req.Ticket, "START", "reason="+req.Reason)

	o.TrunkMu.Lock()
	wsID := NewWorkspace(o.Root, o.Trunk)
	o.TrunkMu.Unlock()

	o.Mu.Lock()
	currentTasks := SerializeTasks(o.Tickets)
	o.Mu.Unlock()

	input := fmt.Sprintf("REASON_FOR_REPLAN: %s\nSOURCE_AGENT: %s\nSOURCE_TICKET: %d\nRUNS_DIR: %s\n\nCURRENT_TASKS:\n%s\n",
		req.Reason, req.Source, req.Ticket, runsDir(o.Root), currentTasks)

	prompt := loadPrompt(o.Root, RoleReplanner)
	stdin := concatPromptInput(prompt, input)

	ctx, cancel := context.WithCancel(o.StopCtx)
	defer cancel()

	res := o.runAgent(ctx, RoleReplanner, req.Ticket, wsID, stdin)
	dumpAgentRun(o.Root, RoleReplanner, req.Ticket, wsID, stdin, res)

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
	appendTicketLog(o.Root, n, "CANCELLED", "by=replanner")
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

		o.Tickets = newTickets
		SaveTasks(o.Root, o.Tickets)
	})

	if exc != nil {
		appendTicketLog(o.Root, req.Ticket, "REPLAN_REJECTED", exc.Error())
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

	appendTicketLog(o.Root, req.Ticket, "REPLAN_APPLIED", "by=replanner")
	uiTicket("✨", RoleReplanner, req.Ticket, "APPLIED", "TASKS.md updated")
	o.signalWake()
}

func (o *Orchestrator) mergerLoop() {
	for {
		select {
		case <-o.StopCtx.Done():
			return
		case req := <-o.QMerger:
			o.runMerger(req)
		}
	}
}

func (o *Orchestrator) runMerger(req MergeRequest) {
	uiTicket("🚀", RoleMerger, req.Ticket, "START", "ws="+req.Workspace)

	o.TrunkMu.Lock()
	prevGoals := o.GoalsHash
	TrunkPull(o.Trunk)
	postPullHash := CurrentTrunkHash(o.Trunk)
	newGoals := readGoalsHash(o.Trunk)
	mergerWS := NewWorkspace(o.Root, o.Trunk)
	o.TrunkMu.Unlock()

	if prevGoals != "" && newGoals != prevGoals {
		o.GoalsHash = newGoals

		select {
		case o.QReplanner <- ReplanRequest{Source: RoleMerger, Reason: "GOALS.md changed in trunk pull"}:
		default:
		}
	}

	diggerBranch := "ovs/" + req.Workspace
	diggerWSAbs := wsPath(o.Root, req.Workspace)
	mergerWSAbs := wsPath(o.Root, mergerWS)

	input := fmt.Sprintf("TICKET: %d\nDIGGER_BRANCH: %s\nDIGGER_WORKTREE: %s\nMERGER_WORKTREE: %s\nTRUNK_HEAD: %s\n",
		req.Ticket, diggerBranch, diggerWSAbs, mergerWSAbs, postPullHash)
	prompt := loadPrompt(o.Root, RoleMerger)
	stdin := concatPromptInput(prompt, input)

	ctx, cancel := context.WithCancel(o.StopCtx)
	defer cancel()

	res := o.runAgent(ctx, RoleMerger, req.Ticket, mergerWS, stdin)
	dumpAgentRun(o.Root, RoleMerger, req.Ticket, mergerWS, stdin, res)

	for _, line := range res.ReplanLines {
		select {
		case o.QReplanner <- ReplanRequest{Source: RoleMerger, Ticket: req.Ticket, Reason: line}:
		default:
		}
	}

	switch res.Verdict {
	case VerdictMerged:
		o.TrunkMu.Lock()
		FetchBranch(o.Trunk, mergerWSAbs, "ovs/"+mergerWS)
		ok, out := FfMergeBranch(o.Trunk, "ovs/"+mergerWS)
		newHead := CurrentTrunkHash(o.Trunk)
		o.TrunkMu.Unlock()

		if !ok {
			appendTicketLog(o.Root, req.Ticket, "MERGE_FF_FAIL", out)
			uiTicket("⚠️", RoleMerger, req.Ticket, "FF_FAIL", out)
			o.spawnDiggerWithRebase(req.Ticket, req.Workspace, newHead, out)

			return
		}

		appendTicketLog(o.Root, req.Ticket, "MERGED", "ws="+req.Workspace+" merger_ws="+mergerWS+" head="+newHead)
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
		appendTicketLog(o.Root, req.Ticket, "MERGE_FAIL", res.Detail)
		uiTicket("❌", RoleMerger, req.Ticket, "FAIL", res.Detail)

		o.TrunkMu.Lock()
		head := CurrentTrunkHash(o.Trunk)
		o.TrunkMu.Unlock()

		o.spawnDiggerWithRebase(req.Ticket, req.Workspace, head, res.Detail)

	default:
		appendTicketLog(o.Root, req.Ticket, "MERGE_UNCLEAR", fmt.Sprintf("verdict=%s detail=%s", res.Verdict, res.Detail))
		uiTicket("❓", RoleMerger, req.Ticket, "UNCLEAR", fmt.Sprintf("verdict=%s", res.Verdict))
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

	wsAbs := wsPath(o.Root, ws)
	input := o.buildAgentInput(RoleDigger, ticketN, wsAbs) +
		"\nMERGE_FAIL_OUTPUT:\n" + mergeOut + "\nREBASE_TARGET: " + target + "\n"
	prompt := loadPrompt(o.Root, RoleDigger)
	stdin := concatPromptInput(prompt, input)

	o.Mu.Unlock()

	go func() {
		res := o.runAgent(ctx, RoleDigger, ticketN, ws, stdin)
		dumpAgentRun(o.Root, RoleDigger, ticketN, ws, stdin, res)
		o.AgentDone <- res
	}()
}

func (o *Orchestrator) overseerLoop() {
	for {
		select {
		case <-o.StopCtx.Done():
			return
		case req := <-o.QOverseer:
			o.runOverseer(req)
		}
	}
}

func (o *Orchestrator) runOverseer(req OverseerRequest) {
	uiSys("🚀", "OVERSEER_START", "reason="+req.Reason)

	o.TrunkMu.Lock()
	wsID := NewWorkspace(o.Root, o.Trunk)
	o.TrunkMu.Unlock()

	o.Mu.Lock()
	currentTasks := SerializeTasks(o.Tickets)
	o.Mu.Unlock()

	input := fmt.Sprintf("REASON: %s\n\nCURRENT_TASKS:\n%s\n", req.Reason, currentTasks)
	prompt := loadPrompt(o.Root, RoleOverseer)
	stdin := concatPromptInput(prompt, input)

	ctx, cancel := context.WithCancel(o.StopCtx)
	defer cancel()

	res := o.runAgent(ctx, RoleOverseer, 0, wsID, stdin)
	dumpAgentRun(o.Root, RoleOverseer, 0, wsID, stdin, res)

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
