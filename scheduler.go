package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

// The coordinator is the heart of the orchestrator: a single goroutine (Run) owns
// all ticket state — the persisted Phase, the in-memory shadow scheduling state,
// branchWS, arbiter context, the pending replan nudges. Nothing else touches that
// state, so there are no locks. Role pools (pool.go) read Jobs the coordinator
// hands them and reply with an AgentResult on o.Events; that is the only
// cross-goroutine communication. The loop is: read an event → fold it into the DB
// (phase transition, written to the log) and the shadow → dispatch every STOPPED
// ticket to the role its phase calls for.

func NewOrchestrator(root, trunk string, bindings map[string]HarnessModel, jail, extraRW []string) *Orchestrator {
	ctx, cancel := context.WithCancel(context.Background())

	o := &Orchestrator{
		Root:       root,
		Trunk:      trunk,
		Bindings:   bindings,
		Jail:       jail,
		ExtraRW:    extraRW,
		shadow:     map[int]Shadow{},
		branchWS:   map[int]string{},
		arb:        map[int]arbCtx{},
		jobs:       map[AgentRole]chan Job{},
		Events:     make(chan AgentResult, 1000),
		StopCtx:    ctx,
		StopCancel: cancel,
		Stopped:    make(chan struct{}),
	}

	for role := range poolSizes {
		o.jobs[role] = make(chan Job, 1000)
	}

	o.Tickets = LoadTasks(root)
	o.GoalsHash = readGoalsHash(trunk)

	// Restart resume: every non-terminal ticket starts STOPPED so dispatch re-routes
	// it; branchWS comes from the last recorded workspace so REVIEW / MERGE / rework
	// resume in place rather than from scratch.
	for _, t := range o.Tickets {
		if !t.Phase.Terminal() {
			o.shadow[t.N] = ShadowStopped
		}

		if len(t.Workspaces) > 0 {
			o.branchWS[t.N] = t.Workspaces[len(t.Workspaces)-1]
		}
	}

	return o
}

func (o *Orchestrator) Run() {
	defer close(o.Stopped)

	o.startPools()

	o.bootDirectives()

	o.dispatch()

	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()

	for {
		select {
		case <-o.StopCtx.Done():
			return
		case <-tick.C:
			o.safe("GOALS", o.checkGoals)
			o.safe("DISPATCH", o.dispatch)
		case res := <-o.Events:
			o.safe("HANDLE", func() { o.handleResult(res) })
			o.safe("DISPATCH", o.dispatch)
		}
	}
}

// bootDirectives kicks off exactly one agent at startup. Operator flags take
// precedence: --replan queues a mandatory operator nudge with mandatory framing.
// Then the boot depends on the task DB — an empty DB runs the replanner in its
// start_project context to read GOALS.md and seed direction; an existing plan gets
// one routine replanner pass to re-evaluate it against the goals before work resumes.
func (o *Orchestrator) bootDirectives() {
	if o.bootReplan != "" {
		o.nudges = append(o.nudges, ReplanReason{
			Source: RoleOperator,
			Reason: operatorDirective(o.bootReplan),
		})
		uiSys("📥", "OPERATOR_REPLAN", o.bootReplan)
	}

	switch {
	case len(o.Tickets) == 0:
		// Empty DB — a brand-new project. Seed it from GOALS.md.
		o.wantReplan("start_project")
		uiSys("🚀", "START_PROJECT", "boot: empty task DB — read GOALS.md and seed the plan")
	case o.nonTerminalCount() == 0:
		// Tickets exist but all terminal — check whether the goals are met.
		o.wantReplan("end_project")
		uiSys("🏁", "END_PROJECT", "boot: all tickets terminal — verify goals / seed remaining work")
	case o.bootReplan == "":
		o.nudges = append(o.nudges, ReplanReason{
			Source: RoleOperator,
			Reason: "boot: re-evaluate the open plan against GOALS.md and current state before resuming work",
		})
		uiSys("📥", "BOOT_REPLAN", "re-evaluate open plan")
	}
}

// operatorDirective wraps a human-supplied directive so the agent's prompt marks it as
// a mandatory, non-stale instruction from the operator.
func operatorDirective(text string) string {
	return "OPERATOR DIRECTIVE (mandatory — a direct instruction from the human operator; act on it this pass, never dismiss it as stale or already-handled): " + text
}

// safe runs a coordinator step so a Throw inside it surfaces to the UI instead of
// killing the coordinate goroutine.
func (o *Orchestrator) safe(name string, f func()) {
	Try(f).Catch(func(e *Exception) {
		uiSys("💥", name+"_PANIC", e.Error())
	})
}

func (o *Orchestrator) findTicket(n int) (Ticket, bool) {
	for _, t := range o.Tickets {
		if t.N == n {
			return t, true
		}
	}

	return Ticket{}, false
}

func (o *Orchestrator) ticketWorkspaceHint(n int) string {
	if c := o.arb[n]; c.workspace != "" {
		return c.workspace
	}

	if ws := o.branchWS[n]; ws != "" {
		return ws
	}

	if t, ok := o.findTicket(n); ok && len(t.Workspaces) > 0 {
		return t.Workspaces[len(t.Workspaces)-1]
	}

	return ""
}

func (o *Orchestrator) nonTerminalCount() int {
	n := 0

	for _, t := range o.Tickets {
		if !t.Phase.Terminal() {
			n++
		}
	}

	return n
}

// hasOpenDependent reports whether any non-terminal ticket depends on n — used to
// tell whether a freshly-completed plan ticket already has implementation work
// queued against it (deps model) or needs the replanner to operationalize it.
func (o *Orchestrator) hasOpenDependent(n int) bool {
	for _, t := range o.Tickets {
		if t.Phase.Terminal() {
			continue
		}

		for _, d := range t.Deps {
			if d == n {
				return true
			}
		}
	}

	return false
}

// depsSatisfied reports whether every dependency of t has reached a terminal phase
// (its work landed, or it was dropped — the replanner cleans dropped-prereq cases).
func (o *Orchestrator) depsSatisfied(t Ticket) bool {
	for _, d := range t.Deps {
		dt, ok := o.findTicket(d)

		if !ok || !dt.Phase.Terminal() {
			return false
		}
	}

	return true
}

// checkGoals folds a local GOALS.md change into a replan nudge — the operator may
// edit goals between runs / mid-run.
func (o *Orchestrator) checkGoals() {
	h := readGoalsHash(o.Trunk)

	if o.GoalsHash != "" && h != o.GoalsHash {
		o.nudges = append(o.nudges, ReplanReason{Source: RoleOperator, Reason: "GOALS.md changed in trunk"})
		uiSys("📥", "GOALS_CHANGED", "queued replan nudge")
	}

	o.GoalsHash = h
}

// dispatch routes every STOPPED non-terminal ticket to the pool its phase calls
// for, in ticket-number order, then batches the replanner. Pure function of the
// coordinator-owned state; pool sizes (not a shared semaphore) cap concurrency.
func (o *Orchestrator) dispatch() {
	ready := append([]Ticket{}, o.Tickets...)
	sort.Slice(ready, func(a, b int) bool {
		return ready[a].N < ready[b].N
	})

	for _, t := range ready {
		if t.Phase.Terminal() || o.shadow[t.N] == ShadowScheduled {
			continue
		}

		role := roleForPhase(t.Phase)

		if role == "" || role == RoleReplanner {
			continue
		}

		// Serialize merges through the coordinator: dispatch the next merge only
		// after onMerger has ff-merged the previous one (mergerBusy cleared there).
		// Otherwise the merger worker would clone trunk for the next job in parallel
		// with the coordinator's ff-merge of the last one, capturing a pre-landing
		// trunk and then failing to fast-forward.
		if role == RoleMerger && o.mergerBusy {
			continue
		}

		if (t.Phase == PhasePlan || t.Phase == PhaseImplement) && !o.depsSatisfied(t) {
			continue
		}

		o.shadow[t.N] = ShadowScheduled

		if role == RoleMerger {
			o.mergerBusy = true
		}

		o.jobs[role] <- o.buildJob(t, role)
		uiTicket("📤", role, t.N, "DISPATCH", string(t.Phase))
	}

	o.dispatchReplanner()
	o.publishTasks()
}

// publishTasks pushes a ticket-DB snapshot to the TUI (coordinator-owned state →
// safe to read here). No-op in log mode. Sent at boot too, since dispatch runs
// before the main loop.
func (o *Orchestrator) publishTasks() {
	if !uiTasksWanted {
		return
	}

	snap := make([]taskSnap, 0, len(o.Tickets))

	for _, t := range o.Tickets {
		snap = append(snap, taskSnap{
			n:        t.N,
			descr:    t.Descr,
			phase:    t.Phase,
			inFlight: o.shadow[t.N] == ShadowScheduled,
		})
	}

	uiOut.tasks(snap)
}

// dispatchReplanner hands the (serial) replanner pool one batched Job covering every
// STOPPED ESCALATE ticket plus all pending global nudges. Only the coordinator writes
// the replanner queue; replannerBusy keeps one in flight at a time. The Job carries a
// subagent context (start_project / end_project / algedonic / replan) that selects the
// replanner's prompt — the most urgent pending one wins (see wantReplan). A special
// context dispatches a pass even with no nudges or escalations (e.g. end_project must
// run to check the goals when the queue has drained).
func (o *Orchestrator) dispatchReplanner() {
	if o.replannerBusy {
		return
	}

	var escalate []int

	for _, t := range o.Tickets {
		if t.Phase == PhaseEscalate && o.shadow[t.N] == ShadowStopped {
			escalate = append(escalate, t.N)
		}
	}

	if o.replanCtx == "" && len(escalate) == 0 && len(o.nudges) == 0 {
		return
	}

	subagent := o.replanCtx

	if subagent == "" {
		subagent = "replan"
	}

	reasons := append([]ReplanReason{}, o.nudges...)
	chatLog := append([]string{}, o.replanChat...)

	for _, n := range escalate {
		t, _ := o.findTicket(n)
		reasons = append(reasons, ReplanReason{
			Source:    RoleArbiter,
			Ticket:    n,
			Workspace: o.ticketWorkspaceHint(n),
			Reason:    "escalated ticket — re-scope it or cancel it: " + t.Descr,
		})
		o.shadow[n] = ShadowScheduled
	}

	o.nudges = nil
	o.replanChat = nil
	o.replanCtx = ""
	o.replanOwned = escalate
	o.replanPlans = o.plannedPlanTickets()
	o.replannerBusy = true

	uiSys("📤", "REPLANNER", fmt.Sprintf("%s — %d reason(s), %d escalated, %d plan(s)", subagent, len(reasons), len(escalate), len(o.replanPlans)))
	o.jobs[RoleReplanner] <- Job{Role: RoleReplanner, NewWS: true, Params: map[string]string{"Subagent": subagent, "Plans": o.closedPlansText()}, Reasons: reasons, ChatLog: chatLog, Snapshot: SerializeTasks(o.Tickets)}
}

// buildJob assembles the Job for a ticket's phase: which workspace the worker uses
// (fresh clone vs the digger branch) plus role-specific context.
func (o *Orchestrator) buildJob(t Ticket, role AgentRole) Job {
	j := Job{Role: role, Ticket: t, Params: o.promptParams(role, t)}

	switch role {
	case RoleTasker:
		j.NewWS = true
	case RoleDigger:
		if ws := o.branchWS[t.N]; ws != "" {
			j.WS = ws
		} else {
			j.NewWS = true
		}

		c := o.arb[t.N]
		j.Trigger, j.Detail, j.RebaseTarget, j.MergeOut = c.trigger, c.detail, c.rebaseTarget, c.mergeOut
	case RoleReviewer:
		j.WS = o.branchWS[t.N]
	case RoleArbiter:
		c := o.arb[t.N]

		switch {
		case c.workspace != "":
			j.WS = c.workspace
		case o.branchWS[t.N] != "":
			j.WS = o.branchWS[t.N]
		default:
			j.NewWS = true
		}

		j.Trigger, j.Detail, j.RebaseTarget, j.MergeOut = c.trigger, c.detail, c.rebaseTarget, c.mergeOut
	case RoleMerger:
		j.WS = o.branchWS[t.N]
	}

	return j
}

// promptParams assembles the prompt template context for a ticket-phase role. The
// implementing roles get the text of the plan tickets their ticket depends on, via the
// template (`{{.Plans}}`) rather than threaded through buildAgentInput.
func (o *Orchestrator) promptParams(role AgentRole, t Ticket) map[string]string {
	p := map[string]string{}

	switch role {
	case RoleDigger, RoleReviewer:
		p["Plans"] = dependencyPlans(o.Root, t.Deps)
	}

	return p
}

// plannedPlanTickets lists the plan tickets sitting in PLANNED — written but not yet
// read by the replanner. After a replanner pass consumes them they flip to CONSUMED
// and drop out, so each plan is shown to the replanner exactly once.
func (o *Orchestrator) plannedPlanTickets() []int {
	var ns []int

	for _, t := range o.Tickets {
		if t.Phase == PhasePlanned {
			ns = append(ns, t.N)
		}
	}

	return ns
}

// closedPlansText concatenates the plan.md of every PLANNED (not-yet-consumed) plan
// ticket, so the replanner can build on prior research instead of re-deriving it.
func (o *Orchestrator) closedPlansText() string {
	var sb strings.Builder

	for _, n := range o.plannedPlanTickets() {
		t, _ := o.findTicket(n)

		if data, err := os.ReadFile(ticketPlanPath(o.Root, n)); err == nil {
			fmt.Fprintf(&sb, "T-%d (%s):\n%s\n\n", n, t.Descr, string(data))
		}
	}

	return strings.TrimSpace(sb.String())
}

// replanCtxRank orders the replanner's wake-up contexts by urgency so the most
// important one wins when several pend before the next dispatch.
func replanCtxRank(c string) int {
	switch c {
	case "algedonic":
		return 3
	case "start_project":
		return 2
	case "end_project":
		return 1
	}

	return 0
}

// wantReplan records that the replanner should run, keeping the most urgent pending
// context; dispatchReplanner turns it into one Job. Replaces the former overseer
// trigger — the replanner now does the overseer's job under that context.
func (o *Orchestrator) wantReplan(ctx string) {
	if replanCtxRank(ctx) >= replanCtxRank(o.replanCtx) {
		o.replanCtx = ctx
	}
}

// handleResult folds one worker result into the state machine: collect any replan
// nudges, then advance the ticket's phase / shadow per role+verdict.
func (o *Orchestrator) handleResult(res AgentResult) {
	if res.Kind == "chat" {
		if line := strings.TrimRight(res.ChatLine, "\n"); line != "" {
			o.replanChat = append(o.replanChat, line)
		}

		return
	}

	n := res.Ticket

	o.recordUsage(res)

	for _, line := range eventReplans(res.Events) {
		o.nudges = append(o.nudges, ReplanReason{Source: res.Role, Ticket: n, Workspace: res.Workspace, Reason: line})
	}

	// Record the exact prompt this run received into the ticket log, so it can be
	// audited that everything needed (plans, deps, history) actually reached the agent.
	if res.Stdin != "" {
		appendTicketPrompt(o.Root, n, res.Role, res.Stdin)
	}

	// Per-ticket result for an already-terminal ticket (replanner cancelled it
	// mid-flight): drop it, just clear the shadow. A merger landing here never
	// reaches onMerger, so free the merge slot explicitly or it deadlocks.
	if res.Role != RoleReplanner {
		if t, ok := o.findTicket(n); ok && t.Phase.Terminal() {
			if res.Role == RoleMerger {
				o.mergerBusy = false
			}

			o.shadow[n] = ShadowStopped
			uiTicket("👻", res.Role, n, "STALE", "ticket "+string(t.Phase))

			return
		}
	}

	switch res.Role {
	case RoleTasker:
		o.shadow[n] = ShadowStopped
		o.onTasker(res)
	case RoleDigger:
		o.shadow[n] = ShadowStopped
		o.onDigger(res)
	case RoleReviewer:
		o.shadow[n] = ShadowStopped
		o.onReviewer(res)
	case RoleMerger:
		o.shadow[n] = ShadowStopped
		o.onMerger(res)
	case RoleArbiter:
		o.shadow[n] = ShadowStopped
		o.onArbiter(res)
	case RoleReplanner:
		o.onReplanner(res)
	}
}

func (o *Orchestrator) onTasker(res AgentResult) {
	n := res.Ticket
	plan := strings.TrimSpace(taskerPlanContent(res.Events))

	if plan == "" {
		o.arb[n] = arbCtx{trigger: VerdictNoPlan, detail: "tasker produced no plan", workspace: res.Workspace}
		o.recordEvent(n, "TASKER_NO_PLAN", "no plan")
		o.setPhase(n, PhaseArbitrate, "tasker NO_PLAN")
		uiTicket("💀", RoleTasker, n, "NO_PLAN", "")

		return
	}

	t, ok := o.findTicket(n)

	if !ok {
		ThrowFmt("onTasker: ticket %d not found", n)
	}

	writePlan(o.Root, n, plan)
	o.recordEvent(n, "PLAN_WRITTEN", "ws="+res.Workspace)
	uiTicket("📝", RoleTasker, n, "PLAN_WRITTEN", "")

	if t.Type == TicketTypePlan {
		// A plan ticket is research: it terminates as PLANNED, and its plan.md is read
		// by dependents via DEPENDENCY_PLANS. No arbiter relay, no discard. If nothing
		// depends on it yet (the replanner couldn't break the work down until the plan
		// existed), nudge the replanner to operationalize it into implementation tickets.
		o.setPhase(n, PhasePlanned, "research plan complete")
		uiTicket("📐", RoleTasker, n, "PLANNED", "")

		if !o.hasOpenDependent(n) {
			o.nudges = append(o.nudges, ReplanReason{
				Source: RoleTasker,
				Ticket: n,
				Reason: fmt.Sprintf("T-%d produced its research plan (plan.md; full text in its tasker run under RUNS_DIR). Operationalize it: create the implementation ticket(s) it calls for and depend them on T-%d. T-%d is already complete (PLANNED) — do not cancel it.", n, n, n),
			})
		}

		return
	}

	o.setPhase(n, PhaseImplement, "plan written")
}

func (o *Orchestrator) onDigger(res AgentResult) {
	n := res.Ticket

	// First dig records its branch workspace; the rebase context (if this was a
	// merge-fail pass) has been consumed by the Job already, so drop it.
	if o.branchWS[n] == "" && res.Workspace != "" {
		o.branchWS[n] = res.Workspace
		o.appendLog(LogEvent{"k": "ws", "n": n, "ws": res.Workspace})
	}

	delete(o.arb, n)

	verdict, detail := lastVerdict(res.Events)

	if verdict == VerdictReady && WorkspaceCommitsAhead(wsPath(o.Root, res.Workspace)) == 0 {
		verdict = VerdictCantDo
		detail = "READY claimed but zero commits ahead of base — work was never committed"
	}

	switch verdict {
	case VerdictReady:
		o.recordEvent(n, "DIGGER_READY", detail)
		o.setPhase(n, PhaseReview, detail)
		uiTicket("✅", RoleDigger, n, "READY", detail)
	case VerdictCantDo:
		o.arb[n] = arbCtx{trigger: VerdictCantDo, detail: detail, workspace: res.Workspace}
		o.recordEvent(n, "DIGGER_CANT_DO", detail)
		o.setPhase(n, PhaseArbitrate, detail)
		uiTicket("🛑", RoleDigger, n, "CANT_DO", detail)
	case VerdictAlgedonic:
		// Emergency cord: escalate the ticket and wake the replanner in its algedonic
		// context (bypassing review / merge / arbiter). The escalated ticket and the
		// digger's cry are both in the next replanner batch, so one pass does the full
		// analysis and the re-scope.
		o.recordEvent(n, "ALGEDONIC", detail)
		o.setPhase(n, PhaseEscalate, detail)
		uiTicket("🚨", RoleDigger, n, "ALGEDONIC", detail)
		o.nudges = append(o.nudges, ReplanReason{Source: RoleDigger, Ticket: n, Workspace: res.Workspace, Reason: algedonicReason(n, detail)})
		o.wantReplan("algedonic")
	}
}

func algedonicReason(n int, detail string) string {
	return fmt.Sprintf("ALGEDONIC — the digger on T-%d pulled the emergency cord: %q. Drop the routine pass and do a FULL root-cause analysis (this ticket and its run history, GOALS.md, the surrounding plan and deps), then re-scope decisively to unblock it.", n, detail)
}

func (o *Orchestrator) onReviewer(res AgentResult) {
	n := res.Ticket
	verdict, detail := lastVerdict(res.Events)

	switch verdict {
	case VerdictApprove:
		o.recordEvent(n, "REVIEWER_APPROVE", detail)
		o.setPhase(n, PhaseMerge, detail)
		uiTicket("👍", RoleReviewer, n, "APPROVE", detail)
	case VerdictRework:
		o.arb[n] = arbCtx{trigger: VerdictRework, detail: detail, workspace: res.Workspace}
		o.recordEvent(n, "REVIEWER_REWORK", detail)
		o.setPhase(n, PhaseArbitrate, detail)
		uiTicket("🔁", RoleReviewer, n, "REWORK", detail)
	case VerdictDiscard:
		o.arb[n] = arbCtx{trigger: VerdictDiscard, detail: detail, workspace: res.Workspace}
		o.recordEvent(n, "REVIEWER_DISCARD", detail)
		o.setPhase(n, PhaseArbitrate, detail)
		uiTicket("👎", RoleReviewer, n, "DISCARD", detail)
	}
}

func (o *Orchestrator) onMerger(res AgentResult) {
	o.mergerBusy = false

	n := res.Ticket
	verdict, detail := lastVerdict(res.Events)

	if verdict == VerdictMerged {
		mergerWS := res.Workspace
		FetchBranch(o.Trunk, wsPath(o.Root, mergerWS), "ovs/"+mergerWS)
		ok, out := FfMergeBranch(o.Trunk, "ovs/"+mergerWS)
		newHead := CurrentTrunkHash(o.Trunk)

		if !ok {
			o.arb[n] = arbCtx{
				trigger:      VerdictMergeFail,
				detail:       "ff-merge failed: " + out,
				rebaseTarget: newHead,
				mergeOut:     out,
				workspace:    o.branchWS[n],
			}
			o.recordEvent(n, "MERGE_FF_FAIL", out)
			o.setPhase(n, PhaseArbitrate, "ff-merge failed")
			uiTicket("⚠️", RoleMerger, n, "FF_FAIL", out)

			return
		}

		o.recordEvent(n, "MERGED", "merger_ws="+mergerWS+" head="+newHead)
		o.setPhase(n, PhaseMerged, "head="+newHead)
		delete(o.arb, n)
		delete(o.branchWS, n)
		uiTicket("✅", RoleMerger, n, "MERGED", "head="+shortHash(newHead))
		o.afterTerminal(n, "MERGED", mergerWS)

		return
	}

	head := CurrentTrunkHash(o.Trunk)
	o.arb[n] = arbCtx{
		trigger:      VerdictMergeFail,
		detail:       detail,
		rebaseTarget: head,
		mergeOut:     detail,
		workspace:    o.branchWS[n],
	}
	o.recordEvent(n, "MERGE_FAIL", detail)
	o.setPhase(n, PhaseArbitrate, detail)
	uiTicket("❌", RoleMerger, n, "FAIL", detail)
}

func (o *Orchestrator) onArbiter(res AgentResult) {
	n := res.Ticket
	verdict, detail := lastVerdict(res.Events)
	c := o.arb[n]

	switch verdict {
	case VerdictContinue:
		o.recordEvent(n, "ARBITER_CONTINUE", detail)
		uiTicket("➡️", RoleArbiter, n, "CONTINUE", detail)

		// A tasker no-plan ticket loops back to planning; everything else routes into
		// implementation. The MERGE_FAIL rebase context stays in o.arb so the next
		// digger Job rebases; other triggers drop it (the digger reads the
		// reviewer/CANT_DO feedback from log.md).
		if c.trigger == VerdictNoPlan {
			delete(o.arb, n)
			o.setPhase(n, PhasePlan, "arbiter continue (re-plan)")

			return
		}

		if c.trigger != VerdictMergeFail {
			delete(o.arb, n)
		}

		o.setPhase(n, PhaseImplement, "arbiter continue (re-implement)")
	case VerdictEscalate:
		delete(o.arb, n)
		o.recordEvent(n, "ARBITER_ESCALATE", detail)
		o.setPhase(n, PhaseEscalate, detail)
		uiTicket("⤴️", RoleArbiter, n, "ESCALATE", detail)
	}
}

func (o *Orchestrator) onReplanner(res AgentResult) {
	o.replannerBusy = false

	// GOALS_ACHIEVED ends the run — the replanner now owns this top-level call (it was
	// the overseer's). Reached mainly from the end_project context.
	if verdict, detail := lastVerdict(res.Events); verdict == VerdictGoalsAchieved {
		uiSys("🎯", "GOALS_ACHIEVED", detail)
		o.writeReport()
		o.StopCancel()

		return
	}

	ops := replannerTaskOps(res.Events)

	if len(ops) > 0 {
		o.applyReplannerOps(res, ops)
	} else {
		uiSys("💤", "REPLANNER_NO_OPS", "no task events")
	}

	// Release the escalated tickets this pass owned: a re-scope (update) leaves them
	// in PhaseEscalate, so move them back to the entry phase their type uses;
	// cancelled ones are already terminal. Then clear their shadow so dispatch can
	// pick them up.
	for _, n := range o.replanOwned {
		if t, ok := o.findTicket(n); ok && t.Phase == PhaseEscalate {
			o.setPhase(n, resumePhaseAfterReplan(t), "replanner pass — resume by ticket type")
		}

		o.shadow[n] = ShadowStopped
	}

	o.replanOwned = nil

	// The plans this pass was shown are now read & processed — flip them PLANNED →
	// CONSUMED so the next pass isn't re-fed the same research. Their plan.md stays on
	// disk for dependents (dependencyPlans reads the file regardless of phase).
	for _, n := range o.replanPlans {
		if t, ok := o.findTicket(n); ok && t.Phase == PhasePlanned {
			o.recordEvent(n, "CONSUMED", "read & processed by replanner")
			o.setPhase(n, PhaseConsumed, "consumed by replanner")
		}
	}

	o.replanPlans = nil
}

// afterTerminal fires the post-terminal bookkeeping: a fallout replan nudge plus an
// end_project replanner pass (check the goals) when the open queue reaches zero.
func (o *Orchestrator) afterTerminal(n int, reason, workspace string) {
	o.nudges = append(o.nudges, ReplanReason{
		Source:    RoleMerger,
		Ticket:    n,
		Workspace: workspace,
		Reason:    fmt.Sprintf("T-%d %s — scan for fallout / unblocked work", n, reason),
	})

	if o.nonTerminalCount() == 0 {
		o.wantReplan("end_project")
	}
}

// applyReplannerOps validates the batch on a sandbox, then commits each op to the
// log: new → a fresh PLAN ticket, update → field changes, cancel → DISCARDED. On a
// schema violation the whole batch is rejected and re-queued as a feedback nudge.
func (o *Orchestrator) applyReplannerOps(res AgentResult, ops []map[string]any) {
	sandbox := make([]Ticket, len(o.Tickets))
	copy(sandbox, o.Tickets)

	exc := Try(func() {
		for _, ev := range ops {
			sandbox = applyTaskOp(sandbox, ev)
		}

		ValidateTasks(sandbox)
	})

	if exc != nil {
		uiSys("❌", "REPLAN_REJECTED", exc.Error())
		o.nudges = append(o.nudges, ReplanReason{
			Source: RoleReplanner,
			Reason: fmt.Sprintf("previous replanner output invalid: %s\n\nREJECTED_OUTPUT:\n%s", exc.Error(), res.Stdout),
		})

		return
	}

	canceledAny := false

	for _, ev := range ops {
		op, _ := ev["op"].(string)
		n := jsonInt(ev["n"])

		switch op {
		case "new":
			descr, _ := ev["descr"].(string)
			deps := jsonIntArray(ev["deps"])
			ticketType := jsonTicketType(ev["ticket_type"])

			o.appendLog(LogEvent{"k": "create", "n": n, "type": string(ticketType), "descr": descr, "deps": deps})
			o.recordEvent(n, "TASK_NEW", "by=replanner descr="+descr)
			uiTicket("🆕", RoleReplanner, n, "NEW", descr)
		case "update":
			change := LogEvent{"k": "update", "n": n}
			summaryParts := []string{}

			if _, ok := ev["deps"]; ok {
				deps := jsonIntArray(ev["deps"])
				change["deps"] = deps
				summaryParts = append(summaryParts, fmt.Sprintf("deps=%v", deps))
			}

			summary := strings.Join(summaryParts, " ")
			o.appendLog(change)
			o.recordEvent(n, "TASK_UPDATE", "by=replanner "+summary)
			uiTicket("✏️", RoleReplanner, n, "UPDATE", summary)
		case "cancel":
			reason, _ := ev["reason"].(string)
			o.setPhase(n, PhaseDiscarded, "by=replanner reason="+reason)
			o.recordEvent(n, "DISCARDED", "by=replanner reason="+reason)
			uiTicket("🛑", RoleReplanner, n, "DISCARDED", reason)
			o.nudges = append(o.nudges, ReplanReason{
				Source:    RoleMerger,
				Ticket:    n,
				Workspace: o.ticketWorkspaceHint(n),
				Reason:    fmt.Sprintf("T-%d %s — scan for fallout / unblocked work", n, "DISCARDED"),
			})
			canceledAny = true
		case "replace":
			from := jsonInt(ev["from"])
			to := jsonInt(ev["to"])

			for _, t := range o.Tickets {
				if t.Phase.Terminal() {
					continue
				}

				deps, changed := replaceDepRefs(t.Deps, from, to)

				if !changed {
					continue
				}

				o.appendLog(LogEvent{"k": "update", "n": t.N, "deps": deps})
				o.recordEvent(t.N, "TASK_UPDATE", fmt.Sprintf("by=replanner replace=%d->%d deps=%v", from, to, deps))
				uiTicket("✏️", RoleReplanner, t.N, "UPDATE", fmt.Sprintf("replace %d->%d deps=%v", from, to, deps))
			}
		}
	}

	if canceledAny && o.nonTerminalCount() == 0 {
		o.wantReplan("end_project")
	}
}

func (o *Orchestrator) writeReport() {
	var sb strings.Builder
	sb.WriteString("# Project Report\n\n")

	for _, t := range o.Tickets {
		fmt.Fprintf(&sb, "- T-%d [%s] %s\n", t.N, t.Phase, t.Descr)
	}

	_ = os.WriteFile(o.Root+"/REPORT.md", []byte(sb.String()), 0644)
}

func shortHash(h string) string {
	if len(h) > 8 {
		return h[:8]
	}

	return h
}
