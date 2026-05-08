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

func NewOrchestrator(root, trunk string) *Orchestrator {
	ctx, cancel := context.WithCancel(context.Background())

	o := &Orchestrator{
		Root:       root,
		Trunk:      trunk,
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

	if len(o.Tickets) == 0 {
		o.bootstrapT0()
	}

	return o
}

func (o *Orchestrator) bootstrapT0() {
	t0 := Ticket{
		N:     0,
		State: StateOpen,
		Descr: "BOOTSTRAP: read GOALS.md, draft acceptance criteria, define test strategy, write CLAUDE.md, propose initial task breakdown",
		Prio:  10,
	}
	o.Tickets = []Ticket{t0}
	SaveTasks(o.Root, o.Tickets)
	appendTicketLog(o.Root, 0, "CREATED", "by=orchestrator role=bootstrap")
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

		if o.shouldFireOverseer() {
			select {
			case o.QOverseer <- OverseerRequest{Reason: "idle/low-open"}:
			default:
			}
		}
	}
}

func (o *Orchestrator) signalWake() {
	select {
	case o.Wakeup <- struct{}{}:
	default:
	}
}

func (o *Orchestrator) shouldFireOverseer() bool {
	o.Mu.Lock()
	defer o.Mu.Unlock()

	open := o.openCountLocked()

	if open <= 2 && len(o.Inflight) == 0 {
		return true
	}

	return false
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

		select {
		case o.AgentSem <- struct{}{}:
		default:
			return
		}

		o.Mu.Lock()

		if _, busy := o.Inflight[t.N]; busy {
			o.Mu.Unlock()
			<-o.AgentSem

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
	wsID := ""

	if !planExists(o.Root, t.N) {
		role = RoleTasker
		wsID = NewWorkspace(o.Root, o.Trunk)
		o.appendWorkspaceLocked(t.N, wsID)
	} else {
		wsID = NewWorkspace(o.Root, o.Trunk)
		o.appendWorkspaceLocked(t.N, wsID)
	}

	wsAbs := wsPath(o.Root, wsID)

	ctx, cancel := context.WithCancel(o.StopCtx)
	run := &AgentRun{
		Role:      role,
		Ticket:    t.N,
		Workspace: wsID,
		Cancel:    cancel,
		Done:      make(chan AgentResult, 1),
	}

	o.Inflight[t.N] = run

	appendTicketLog(o.Root, t.N, "AGENT_START", fmt.Sprintf("role=%s ws=%s", role, wsID))

	go func() {
		defer func() {
			<-o.AgentSem
		}()

		input := o.buildAgentInput(role, t.N, wsAbs)
		prompt := loadPrompt(o.Root, role)

		res := runAgent(ctx, role, t.N, wsAbs, prompt, input)
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
			fmt.Fprintf(&sb, "PLAN:\n%s\n", string(data))
		}
	}

	data, err := os.ReadFile(ticketLogPath(o.Root, ticketN))

	if err == nil {
		fmt.Fprintf(&sb, "LOG:\n%s\n", string(data))
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
	if res.Verdict == VerdictPlanWritten {
		writePlan(o.Root, res.Ticket, res.Detail)
		appendTicketLog(o.Root, res.Ticket, "PLAN_WRITTEN", "ws="+res.Workspace)

		return
	}

	appendTicketLog(o.Root, res.Ticket, "TASKER_NO_PLAN", fmt.Sprintf("verdict=%s detail=%s", res.Verdict, res.Detail))
}

func (o *Orchestrator) handleDiggerResultLocked(res AgentResult) {
	switch res.Verdict {
	case VerdictReady:
		appendTicketLog(o.Root, res.Ticket, "DIGGER_READY", "ws="+res.Workspace)
		o.spawnReviewerLocked(res.Ticket, res.Workspace)
	case VerdictCantDo:
		appendTicketLog(o.Root, res.Ticket, "DIGGER_CANT_DO", res.Detail)
		MarkWorkspaceReadOnly(o.Root, res.Workspace)
	case VerdictCrashed:
		appendTicketLog(o.Root, res.Ticket, "DIGGER_CRASHED", res.Detail)

		select {
		case o.QReplanner <- ReplanRequest{Source: res.Role, Ticket: res.Ticket, Reason: "digger crashed: " + res.Detail}:
		default:
		}
	default:
		appendTicketLog(o.Root, res.Ticket, "DIGGER_UNCLEAR", fmt.Sprintf("verdict=%s detail=%s", res.Verdict, res.Detail))
	}
}

func (o *Orchestrator) handleReviewerResultLocked(res AgentResult) {
	switch res.Verdict {
	case VerdictApprove:
		appendTicketLog(o.Root, res.Ticket, "REVIEWER_APPROVE", res.Detail)

		select {
		case o.QMerger <- MergeRequest{Ticket: res.Ticket, Workspace: res.Workspace}:
		default:
		}
	case VerdictRework:
		appendTicketLog(o.Root, res.Ticket, "REVIEWER_REWORK", res.Detail)
		o.spawnDiggerSameWorkspaceLocked(res.Ticket, res.Workspace)
	case VerdictDiscard:
		appendTicketLog(o.Root, res.Ticket, "REVIEWER_DISCARD", res.Detail)
		o.closeTicketLocked(res.Ticket, CloseDiscarded)
		MarkWorkspaceReadOnly(o.Root, res.Workspace)

		select {
		case o.QReplanner <- ReplanRequest{Source: res.Role, Ticket: res.Ticket, Reason: "reviewer discarded: " + res.Detail}:
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
}

func (o *Orchestrator) spawnReviewerLocked(ticketN int, ws string) {
	select {
	case o.AgentSem <- struct{}{}:
	default:
		appendTicketLog(o.Root, ticketN, "REVIEWER_DEFERRED", "sem-full")

		return
	}

	ctx, cancel := context.WithCancel(o.StopCtx)
	run := &AgentRun{
		Role:      RoleReviewer,
		Ticket:    ticketN,
		Workspace: ws,
		Cancel:    cancel,
		Done:      make(chan AgentResult, 1),
	}
	o.Inflight[ticketN] = run

	wsAbs := wsPath(o.Root, ws)
	input := o.buildAgentInput(RoleReviewer, ticketN, wsAbs)
	prompt := loadPrompt(o.Root, RoleReviewer)

	go func() {
		defer func() { <-o.AgentSem }()

		res := runAgent(ctx, RoleReviewer, ticketN, wsAbs, prompt, input)
		o.AgentDone <- res
	}()
}

func (o *Orchestrator) spawnDiggerSameWorkspaceLocked(ticketN int, ws string) {
	select {
	case o.AgentSem <- struct{}{}:
	default:
		appendTicketLog(o.Root, ticketN, "DIGGER_REWORK_DEFERRED", "sem-full")

		return
	}

	ctx, cancel := context.WithCancel(o.StopCtx)
	run := &AgentRun{
		Role:      RoleDigger,
		Ticket:    ticketN,
		Workspace: ws,
		Cancel:    cancel,
		Done:      make(chan AgentResult, 1),
	}
	o.Inflight[ticketN] = run

	wsAbs := wsPath(o.Root, ws)
	input := o.buildAgentInput(RoleDigger, ticketN, wsAbs)
	prompt := loadPrompt(o.Root, RoleDigger)

	go func() {
		defer func() { <-o.AgentSem }()

		res := runAgent(ctx, RoleDigger, ticketN, wsAbs, prompt, input)
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
	o.AgentSem <- struct{}{}
	defer func() { <-o.AgentSem }()

	wsID := NewWorkspace(o.Root, o.Trunk)
	wsAbs := wsPath(o.Root, wsID)

	o.Mu.Lock()
	currentTasks := SerializeTasks(o.Tickets)
	o.Mu.Unlock()

	input := fmt.Sprintf("REASON_FOR_REPLAN: %s\nSOURCE_AGENT: %s\nSOURCE_TICKET: %d\n\nCURRENT_TASKS:\n%s\n",
		req.Reason, req.Source, req.Ticket, currentTasks)

	prompt := loadPrompt(o.Root, RoleReplanner)
	ctx, cancel := context.WithCancel(o.StopCtx)
	defer cancel()

	res := runAgent(ctx, RoleReplanner, 0, wsAbs, prompt, input)

	for _, n := range ExtractCancelTickets(res.Stdout) {
		o.cancelTicket(n)
	}

	if res.Verdict == VerdictReplanApplied || strings.Contains(res.Stdout, "TASKS_NEW:") {
		o.tryApplyReplanOutput(res, req)
	}

	MarkWorkspaceReadOnly(o.Root, wsID)
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
	o.signalWake()
}

func (o *Orchestrator) tryApplyReplanOutput(res AgentResult, req ReplanRequest) {
	idx := strings.Index(res.Stdout, "TASKS_NEW:")

	if idx < 0 {
		return
	}

	body := strings.TrimSpace(res.Stdout[idx+len("TASKS_NEW:"):])

	exc := Try(func() {
		newTickets := ParseTasks(body)
		ValidateTasks(newTickets)

		o.Mu.Lock()
		defer o.Mu.Unlock()

		o.Tickets = newTickets
		SaveTasks(o.Root, o.Tickets)
	})

	if exc != nil {
		fmt.Fprintln(os.Stderr, "replanner output rejected:", exc.Error())
		appendTicketLog(o.Root, req.Ticket, "REPLAN_REJECTED", exc.Error())

		select {
		case o.QReplanner <- ReplanRequest{Source: RoleReplanner, Ticket: req.Ticket, Reason: "previous replanner output invalid: " + exc.Error()}:
		default:
		}

		return
	}

	appendTicketLog(o.Root, req.Ticket, "REPLAN_APPLIED", "by=replanner")
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
	o.AgentSem <- struct{}{}
	defer func() { <-o.AgentSem }()

	prevHash := o.GoalsHash
	newHash := TrunkPull(o.Trunk)

	if prevHash != "" && newHash != "" && prevHash != newHash {
		o.GoalsHash = newHash

		select {
		case o.QReplanner <- ReplanRequest{Source: RoleMerger, Ticket: 0, Reason: "GOALS.md changed in trunk"}:
		default:
		}
	}

	ok, out := MergeWorkspace(o.Trunk, req.Workspace, o.Root)

	if ok {
		appendTicketLog(o.Root, req.Ticket, "MERGED", "ws="+req.Workspace)

		o.Mu.Lock()
		o.closeTicketLocked(req.Ticket, CloseMerged)
		o.Mu.Unlock()

		MarkWorkspaceReadOnly(o.Root, req.Workspace)

		select {
		case o.QReplanner <- ReplanRequest{Source: RoleMerger, Ticket: req.Ticket, Reason: "merged"}:
		default:
		}

		o.signalWake()

		return
	}

	appendTicketLog(o.Root, req.Ticket, "MERGE_FAIL", out)
	o.spawnDiggerWithRebase(req.Ticket, req.Workspace, CurrentTrunkHash(o.Trunk), out)
}

func (o *Orchestrator) spawnDiggerWithRebase(ticketN int, ws, target, mergeOut string) {
	o.AgentSem <- struct{}{}

	o.Mu.Lock()
	defer o.Mu.Unlock()

	ctx, cancel := context.WithCancel(o.StopCtx)
	run := &AgentRun{
		Role:      RoleDigger,
		Ticket:    ticketN,
		Workspace: ws,
		Cancel:    cancel,
		Done:      make(chan AgentResult, 1),
	}
	o.Inflight[ticketN] = run

	wsAbs := wsPath(o.Root, ws)
	input := o.buildAgentInput(RoleDigger, ticketN, wsAbs) + "\nMERGE_FAIL_OUTPUT:\n" + mergeOut + "\nREBASE_TARGET: " + target + "\n"
	prompt := loadPrompt(o.Root, RoleDigger)

	go func() {
		defer func() { <-o.AgentSem }()

		res := runAgent(ctx, RoleDigger, ticketN, wsAbs, prompt, input)
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
	o.AgentSem <- struct{}{}
	defer func() { <-o.AgentSem }()

	wsID := NewWorkspace(o.Root, o.Trunk)
	wsAbs := wsPath(o.Root, wsID)

	o.Mu.Lock()
	currentTasks := SerializeTasks(o.Tickets)
	o.Mu.Unlock()

	input := fmt.Sprintf("REASON: %s\n\nCURRENT_TASKS:\n%s\n", req.Reason, currentTasks)
	prompt := loadPrompt(o.Root, RoleOverseer)

	ctx, cancel := context.WithCancel(o.StopCtx)
	defer cancel()

	res := runAgent(ctx, RoleOverseer, 0, wsAbs, prompt, input)
	MarkWorkspaceReadOnly(o.Root, wsID)

	switch res.Verdict {
	case VerdictGoalsAchieved:
		fmt.Fprintln(os.Stderr, "OVERSEER: GOALS_ACHIEVED — stopping")
		o.writeReport()
		o.StopCancel()
	default:
		for _, line := range res.ReplanLines {
			select {
			case o.QReplanner <- ReplanRequest{Source: RoleOverseer, Ticket: 0, Reason: line}:
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
