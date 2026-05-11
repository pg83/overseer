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
		QReplanner: make(chan ReplanRequest, 1000),
		QMerger:    make(chan MergeRequest, 1000),
		QOverseer:  make(chan OverseerRequest, 1000),
		QArbiter:   make(chan ArbiterRequest, 1000),
		AgentDone:  make(chan AgentResult, 1000),
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
	go o.arbiterLoop()

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
	// Any CLOSED ticket counts as a satisfied dep — MERGED for the canonical
	// "work landed" path, DISCARDED only ever appears here as a transient until
	// the next replanner pass cleans up dependents (ValidateTasks rejects
	// OPEN→DISCARDED at write time, so this is just defensive).
	closed := map[int]bool{}

	for _, t := range o.Tickets {
		if t.State == StateClosed {
			closed[t.N] = true
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
			if !closed[d] {
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
	if planExists(o.Root, t.N) {
		o.spawnDiggerFreshLocked(t.N)

		return
	}

	o.spawnTaskerLocked(t.N)
}

// runAgentDigger drives a digger invocation, retrying until the agent emits a
// recognized verdict (READY or CANT_DO). On each retry the stdin is rebuilt so
// PRIOR_RUNS includes the just-completed failed attempt — otherwise every retry
// within the same spawn sees a stale snapshot that predates the digger's own
// runs, causing the model to start from scratch instead of continuing.
//
// extraInput is appended after buildAgentInput on every iteration — use it for
// per-call context like MERGE_FAIL_OUTPUT / REBASE_TARGET that doesn't change
// between retries but is specific to this spawn (e.g. merge-fail digger pass).
func (o *Orchestrator) runAgentDigger(ticketN int, ws, extraInput string, env map[string]string) AgentResult {
	wsAbs := wsPath(o.Root, ws)
	prompt := loadPrompt(RoleDigger)

	buildStdin := func() string {
		input := o.buildAgentInput(RoleDigger, ticketN, wsAbs) +
			fmt.Sprintf("PREV_WORKSPACE: %s\n", wsAbs) +
			extraInput
		return concatPromptInput(prompt, input)
	}

	for {
		res := o.runAgent(RoleDigger, ticketN, ws, buildStdin(), env)
		v, _ := lastVerdict(res.Events)

		if v == VerdictReady || v == VerdictCantDo {
			return res
		}

		uiTicket("🔄", RoleDigger, ticketN, "RESPAWN", fmt.Sprintf("verdict=%q — retrying", v))
	}
}

// runAgentReviewer drives a reviewer invocation, retrying until it emits APPROVE,
// REWORK, or DISCARD.
func (o *Orchestrator) runAgentReviewer(ticketN int, ws, stdin string, env map[string]string) AgentResult {
	for {
		res := o.runAgent(RoleReviewer, ticketN, ws, stdin, env)
		v, _ := lastVerdict(res.Events)

		if v == VerdictApprove || v == VerdictRework || v == VerdictDiscard {
			return res
		}

		uiTicket("🔄", RoleReviewer, ticketN, "RESPAWN", fmt.Sprintf("verdict=%q — retrying", v))
	}
}

// runAgentMerger drives a merger invocation, retrying until it emits MERGED or
// MERGE_FAIL.
func (o *Orchestrator) runAgentMerger(ticketN int, ws, stdin string, env map[string]string) AgentResult {
	for {
		res := o.runAgent(RoleMerger, ticketN, ws, stdin, env)
		v, _ := lastVerdict(res.Events)

		if v == VerdictMerged || v == VerdictMergeFail {
			return res
		}

		uiTicket("🔄", RoleMerger, ticketN, "RESPAWN", fmt.Sprintf("verdict=%q — retrying", v))
	}
}

// runAgentArbiter drives an arbiter invocation, retrying until it emits CONTINUE
// or ESCALATE.
func (o *Orchestrator) runAgentArbiter(ticketN int, ws, stdin string, env map[string]string) AgentResult {
	for {
		res := o.runAgent(RoleArbiter, ticketN, ws, stdin, env)
		v, _ := lastVerdict(res.Events)

		if v == VerdictContinue || v == VerdictEscalate {
			return res
		}

		uiTicket("🔄", RoleArbiter, ticketN, "RESPAWN", fmt.Sprintf("verdict=%q — retrying", v))
	}
}

// runAgentTasker drives a tasker invocation, retrying until the agent either
// writes a plan file and emits {"type":"plan","path":"..."} or emits a replan
// event signalling the ticket needs re-scoping. If the model produced malformed
// JSON (detectable via the unparsed bucket) without a plan or replan, respawn —
// it tried but failed to emit structured output.
func (o *Orchestrator) runAgentTasker(ticketN int, wsID, stdin string, env map[string]string) AgentResult {
	for {
		res := o.runAgent(RoleTasker, ticketN, wsID, stdin, env)

		if taskerPlanContent(res.Events) != "" {
			return res
		}

		if hasReplanEvent(res.Events) {
			return res
		}

		if hasJSONInUnparsed(res.Events) {
			uiTicket("🔄", RoleTasker, ticketN, "RESPAWN", "unparsed JSON, no plan — retrying")
			continue
		}

		return res
	}
}

// runAgentReplanner drives a replanner invocation, retrying when the model
// produced malformed JSON without emitting any task ops. If there are ops (or
// genuinely none needed), return immediately — the caller decides what to do.
func (o *Orchestrator) runAgentReplanner(ticketN int, wsID, stdin string, env map[string]string) AgentResult {
	for {
		res := o.runAgent(RoleReplanner, ticketN, wsID, stdin, env)

		if len(replannerTaskOps(res.Events)) > 0 {
			return res
		}

		if hasJSONInUnparsed(res.Events) {
			uiTicket("🔄", RoleReplanner, ticketN, "RESPAWN", "unparsed JSON, no task ops — retrying")
			continue
		}

		return res
	}
}

// runAgentOverseer drives an overseer invocation, retrying until the agent
// either certifies goals are met (GOALS_ACHIEVED verdict) or emits at least
// one replan event. Malformed JSON output (JSON-in-unparsed with no useful
// events) also triggers a retry.
func (o *Orchestrator) runAgentOverseer(wsID, stdin string, env map[string]string) AgentResult {
	for {
		res := o.runAgent(RoleOverseer, 0, wsID, stdin, env)
		v, _ := lastVerdict(res.Events)
		replans := eventReplans(res.Events)

		if v == VerdictGoalsAchieved || len(replans) > 0 {
			return res
		}

		if hasJSONInUnparsed(res.Events) {
			uiSys("🔄", "OVERSEER_RESPAWN", "unparsed JSON, no decision — retrying")
			continue
		}

		uiSys("🔄", "OVERSEER_RESPAWN",
			fmt.Sprintf("verdict=%q replans=0 — neither GOALS_ACHIEVED nor replan emitted", v))
	}
}

// hasJSONInUnparsed reports whether the synthetic unparsed event (if any)
// contains JSON fragments — a signal the model tried to emit structured output
// but produced malformed lines.
func hasJSONInUnparsed(events []map[string]any) bool {
	for _, ev := range events {
		if t, _ := ev["type"].(string); t == "unparsed" {
			if text, _ := ev["text"].(string); strings.Contains(text, `{"`) {
				return true
			}
		}
	}
	return false
}

// hasReplanEvent reports whether any replan event was emitted.
func hasReplanEvent(events []map[string]any) bool {
	for _, ev := range events {
		if t, _ := ev["type"].(string); t == "replan" {
			return true
		}
	}
	return false
}

// ticketEnv returns the common environment-variable map every ticket-bound role
// (tasker / digger / reviewer) gets. Prompts reference `$WORKSPACE`, `$TRUNK_PATH`,
// etc. in bash tool calls; we export the values so the agent's shell actually
// resolves them. Same values still appear in the prose input header for context.
func (o *Orchestrator) ticketEnv(ticketN int, wsAbs string) map[string]string {
	return map[string]string{
		"WORKSPACE":  wsAbs,
		"TICKET":     fmt.Sprintf("%d", ticketN),
		"TRUNK_PATH": o.Trunk,
		"TRUNK_HASH": CurrentTrunkHash(o.Trunk),
	}
}

func (o *Orchestrator) spawnTaskerLocked(ticketN int) {
	wsID := NewWorkspace(o.Root, o.Trunk)

	o.setInProgressLocked(ticketN, true)
	o.appendWorkspaceLocked(ticketN, wsID)

	wsAbs := wsPath(o.Root, wsID)

	o.recordEventLocked(ticketN, "AGENT_START", fmt.Sprintf("role=%s ws=%s", RoleTasker, wsID))
	uiTicket("🚀", RoleTasker, ticketN, "START", "ws="+wsID)

	input := o.buildAgentInput(RoleTasker, ticketN, wsAbs)
	prompt := loadPrompt(RoleTasker)
	stdin := concatPromptInput(prompt, input)
	env := o.ticketEnv(ticketN, wsAbs)

	go func() {
		res := o.runAgentTasker(ticketN, wsID, stdin, env)
		o.AgentDone <- res
	}()
}

func (o *Orchestrator) spawnDiggerFreshLocked(ticketN int) {
	wsID := NewWorkspace(o.Root, o.Trunk)

	o.setInProgressLocked(ticketN, true)
	o.appendWorkspaceLocked(ticketN, wsID)

	o.recordEventLocked(ticketN, "AGENT_START", fmt.Sprintf("role=%s ws=%s", RoleDigger, wsID))
	uiTicket("🚀", RoleDigger, ticketN, "START", "ws="+wsID)

	env := o.ticketEnv(ticketN, wsPath(o.Root, wsID))

	go func() {
		res := o.runAgentDigger(ticketN, wsID, "", env)
		o.AgentDone <- res
	}()
}

// agentSelfBlock identifies the agent to itself: which role it is, which harness+model
// is driving it. Goes at the top of every agent's input so the agent can make self-aware
// decisions (e.g. "I'm a small model, keep edits cheap"). Roles can be bound to different
// harness:model combinations via --<role>-harness, so this lookup is per-role. The
// "cwd vs workspace" rule is explained once in common.txt — no need to repeat per-input.
func (o *Orchestrator) agentSelfBlock(role AgentRole, ticket int) string {
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

	sb.WriteString(o.agentSelfBlock(role, ticketN))

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
		fmt.Fprintf(&sb, "\nLOG (state transitions for this ticket, append-only):\n%s\n", string(data))
	}

	if msgs := ticketMessages(o.Root, ticketN); msgs != "" {
		fmt.Fprintf(&sb, "\nTICKET_CHAT (lines from MESSAGES_LOG mentioning this ticket — what teammates noticed across prior agent runs):\n%s\n", msgs)
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
	o.appendLogEventLocked(LogEvent{"k": "ws", "n": n, "ws": ws})
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
		o.QReplanner <- ReplanRequest{Source: res.Role, Ticket: res.Ticket, Reason: line}
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

// taskerPlanContent scans for the tasker's `plan` event and returns the plan
// body. The event carries a `path` field pointing to a file the tasker wrote
// (avoids embedding multi-line markdown inside a JSON string, which breaks
// weak models). Falls back to `body` for backwards compat with old runs.
func taskerPlanContent(events []map[string]any) string {
	content := ""

	for _, ev := range events {
		if t, _ := ev["type"].(string); t != "plan" {
			continue
		}

		if p, _ := ev["path"].(string); p != "" {
			if data, err := os.ReadFile(p); err == nil {
				content = string(data)
			}

			continue
		}

		if b, _ := ev["body"].(string); b != "" {
			content = b
		}
	}

	return content
}

func (o *Orchestrator) handleTaskerResultLocked(res AgentResult) {
	plan := strings.TrimSpace(taskerPlanContent(res.Events))

	if plan == "" {
		reason := "tasker produced no plan"
		o.recordEventLocked(res.Ticket, "TASKER_NO_PLAN", reason)
		uiTicket("💀", RoleTasker, res.Ticket, "NO_PLAN", reason)

		o.QArbiter <- ArbiterRequest{
			Ticket:    res.Ticket,
			Workspace: res.Workspace,
			Source:    RoleTasker,
			Trigger:   VerdictNoPlan,
			Detail:    reason,
		}
		uiTicket("📥", RoleTasker, res.Ticket, "→Q_arbiter", reason)

		return
	}

	writePlan(o.Root, res.Ticket, plan)
	o.recordEventLocked(res.Ticket, "PLAN_WRITTEN", "ws="+res.Workspace)
	uiTicket("📝", RoleTasker, res.Ticket, "PLAN_WRITTEN", "ws="+res.Workspace)

	// Tasker done; ticket stays InProgress=true. scheduleReady won't pick it again,
	// so spawn digger explicitly with a fresh workspace.
	if t, ok := o.findTicketLocked(res.Ticket); ok {
		o.startAgentForTicketLocked(t)
	}
}

func (o *Orchestrator) handleDiggerResultLocked(res AgentResult) {
	verdict, detail := lastVerdict(res.Events)

	// Structural sanity check: harness defaults often skip git commit unless
	// explicitly asked. READY with zero commits ahead of base means the work
	// isn't visible — semantically the same as CANT_DO. Override and route
	// through the same arbiter path.
	if verdict == VerdictReady && WorkspaceCommitsAhead(wsPath(o.Root, res.Workspace)) == 0 {
		verdict = VerdictCantDo
		detail = fmt.Sprintf("READY claimed but %s has zero commits ahead of base — work was never committed", res.Workspace)
	}

	switch verdict {
	case VerdictReady:
		o.recordEventLocked(res.Ticket, "DIGGER_READY", "ws="+res.Workspace+" summary="+detail)
		uiTicket("✅", RoleDigger, res.Ticket, "READY", detail)
		o.spawnReviewerLocked(res.Ticket, res.Workspace)
	case VerdictCantDo:
		o.recordEventLocked(res.Ticket, "DIGGER_CANT_DO", detail)
		uiTicket("🛑", RoleDigger, res.Ticket, "CANT_DO", detail)

		o.QArbiter <- ArbiterRequest{
			Ticket:    res.Ticket,
			Workspace: res.Workspace,
			Source:    RoleDigger,
			Trigger:   VerdictCantDo,
			Detail:    detail,
		}
		uiTicket("📥", RoleDigger, res.Ticket, "→Q_arbiter", detail)
	}
}

func (o *Orchestrator) handleReviewerResultLocked(res AgentResult) {
	verdict, detail := lastVerdict(res.Events)

	switch verdict {
	case VerdictApprove:
		o.recordEventLocked(res.Ticket, "REVIEWER_APPROVE", detail)
		uiTicket("👍", RoleReviewer, res.Ticket, "APPROVE", detail)

		o.QMerger <- MergeRequest{Ticket: res.Ticket, Workspace: res.Workspace}
		uiTicket("📥", RoleReviewer, res.Ticket, "→Q_merger", "ws="+res.Workspace)
	case VerdictRework:
		o.recordEventLocked(res.Ticket, "REVIEWER_REWORK", detail)
		uiTicket("🔁", RoleReviewer, res.Ticket, "REWORK", detail)

		o.QArbiter <- ArbiterRequest{
			Ticket:    res.Ticket,
			Workspace: res.Workspace,
			Source:    RoleReviewer,
			Trigger:   VerdictRework,
			Detail:    detail,
		}
		uiTicket("📥", RoleReviewer, res.Ticket, "→Q_arbiter", detail)
	case VerdictDiscard:
		o.recordEventLocked(res.Ticket, "REVIEWER_DISCARD", detail)
		uiTicket("👎", RoleReviewer, res.Ticket, "DISCARD", detail)

		o.QArbiter <- ArbiterRequest{
			Ticket:    res.Ticket,
			Workspace: res.Workspace,
			Source:    RoleReviewer,
			Trigger:   VerdictDiscard,
			Detail:    detail,
		}
		uiTicket("📥", RoleReviewer, res.Ticket, "→Q_arbiter", detail)
	}
}

func (o *Orchestrator) closeTicketLocked(n int, reason CloseReason) {
	// Idempotent: applyLogEvent on a "close" event for an already-CLOSED ticket
	// preserves the original close_reason and just clears InProgress.
	o.appendLogEventLocked(LogEvent{"k": "close", "n": n, "reason": string(reason)})

	if o.openCountLocked() <= 2 {
		o.QOverseer <- OverseerRequest{Reason: fmt.Sprintf("low-open after T-%d closed (%s)", n, reason)}
		uiSys("📥", "→Q_overseer", fmt.Sprintf("after T-%d %s", n, reason))
	}
}

func (o *Orchestrator) spawnReviewerLocked(ticketN int, ws string) {
	uiTicket("🚀", RoleReviewer, ticketN, "START", "ws="+ws)

	wsAbs := wsPath(o.Root, ws)
	input := o.buildAgentInput(RoleReviewer, ticketN, wsAbs)
	prompt := loadPrompt(RoleReviewer)
	stdin := concatPromptInput(prompt, input)
	env := o.ticketEnv(ticketN, wsAbs)

	go func() {
		res := o.runAgentReviewer(ticketN, ws, stdin, env)
		o.AgentDone <- res
	}()
}

func (o *Orchestrator) spawnDiggerSameWorkspaceLocked(ticketN int, ws string) {
	o.recordEvent(ticketN, "AGENT_START", fmt.Sprintf("role=%s ws=%s", RoleDigger, ws))
	uiTicket("🚀", RoleDigger, ticketN, "START", "ws="+ws)

	wsAbs := wsPath(o.Root, ws)
	env := o.ticketEnv(ticketN, wsAbs)
	env["PREV_WORKSPACE"] = wsAbs

	go func() {
		res := o.runAgentDigger(ticketN, ws, "", env)
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

	input := o.agentSelfBlock(RoleReplanner, 0) +
		fmt.Sprintf("REASON_FOR_REPLAN: %s\nSOURCE_AGENT: %s\nSOURCE_TICKET: %d\nRUNS_DIR: %s\nTASKS_DB: %s\n\n%s",
			req.Reason, req.Source, req.Ticket, runsDir(o.Root), tasksDBPath(o.Root), currentTasks)

	prompt := loadPrompt(RoleReplanner)
	stdin := concatPromptInput(prompt, input)

	env := map[string]string{
		"REASON_FOR_REPLAN": req.Reason,
		"SOURCE_AGENT":      string(req.Source),
		"SOURCE_TICKET":     fmt.Sprintf("%d", req.Ticket),
		"RUNS_DIR":          runsDir(o.Root),
		"TASKS_DB":          tasksDBPath(o.Root),
	}

	res := o.runAgentReplanner(req.Ticket, wsID, stdin, env)

	ops := replannerTaskOps(res.Events)

	if len(ops) == 0 {
		uiTicket("💤", RoleReplanner, req.Ticket, "NO_OPS", "replanner emitted no task events")

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
		tickets[idx].CloseReason = CloseDiscarded
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

// applyReplannerOps sandboxes the ops, validates the resulting list against the
// schema (no cycles, valid deps, no orphan DISCARDED-deps), and on success
// emits one log event per op. Each op is its own atomic event in the log; the
// pre-emit validation guarantees the cumulative state is consistent. On
// validation failure the replanner is bounced with its output as feedback.
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

		o.QReplanner <- ReplanRequest{Source: RoleReplanner, Ticket: req.Ticket, Reason: feedback}
		uiTicket("📥", RoleReplanner, req.Ticket, "→Q_replanner", "retry after reject")

		return
	}

	// Validation passed — emit one log event per op. applyLogEvent inside
	// appendLogEventLocked maintains o.Tickets as we go.
	canceledAny := false

	for _, ev := range ops {
		op, _ := ev["op"].(string)
		n := jsonInt(ev["n"])
		reason, _ := ev["reason"].(string)

		switch op {
		case "new":
			descr, _ := ev["descr"].(string)
			prio := jsonInt(ev["prio"])
			deps := jsonIntArray(ev["deps"])

			o.appendLogEventLocked(LogEvent{
				"k": "create", "n": n, "descr": descr, "prio": prio, "deps": deps,
			})
			o.recordEventLocked(n, "TASK_NEW", "by=replanner descr="+descr)
			uiTicket("🆕", RoleReplanner, n, "NEW", descr)
		case "update":
			change := LogEvent{"k": "update", "n": n}
			var changes []string

			if d, ok := ev["descr"].(string); ok {
				change["descr"] = d
				changes = append(changes, "descr="+d)
			}

			if _, ok := ev["prio"]; ok {
				p := jsonInt(ev["prio"])
				change["prio"] = p
				changes = append(changes, fmt.Sprintf("prio=%d", p))
			}

			if _, ok := ev["deps"]; ok {
				d := jsonIntArray(ev["deps"])
				change["deps"] = d
				changes = append(changes, fmt.Sprintf("deps=%v", d))
			}

			o.appendLogEventLocked(change)

			summary := strings.Join(changes, " ")
			o.recordEventLocked(n, "TASK_UPDATE", "by=replanner "+summary)
			uiTicket("✏️", RoleReplanner, n, "UPDATE", summary)
		case "cancel":
			canceledAny = true

			o.appendLogEventLocked(LogEvent{"k": "close", "n": n, "reason": string(CloseDiscarded)})

			detail := "by=replanner"

			if reason != "" {
				detail += " reason=" + reason
			}

			o.recordEventLocked(n, "DISCARDED", detail)
			uiTicket("🛑", RoleReplanner, n, "DISCARDED", reason)
		}
	}

	// Only fire an overseer nudge when cancel ops actually dropped open tickets — new /
	// update ops can't reduce open count.
	if canceledAny && o.openCountLocked() <= 2 {
		o.QOverseer <- OverseerRequest{Reason: fmt.Sprintf("low-open after replanner batch (req T-%d)", req.Ticket)}
		uiSys("📥", "→Q_overseer", "after replanner cancels")
	}

	o.Mu.Unlock()

	o.recordEvent(req.Ticket, "REPLAN_APPLIED", fmt.Sprintf("ops=%d", len(ops)))
	uiTicket("✨", RoleReplanner, req.Ticket, "APPLIED", fmt.Sprintf("%d ops", len(ops)))
	o.signalWake()
}

func (o *Orchestrator) arbiterLoop() {
	for {
		select {
		case <-o.StopCtx.Done():
			return
		case req := <-o.QArbiter:
			o.safe("ARBITER", func() { o.runArbiter(req) })
		}
	}
}

// runArbiter is the cycle-internal escalation gate. Called on every disagreement
// inside the digger → reviewer → merger cycle (REWORK / DISCARD / MERGE_FAIL),
// it decides CONTINUE (spawn next digger iteration) or ESCALATE (queue full
// replanner). Replaces the bounce-count trigger that used to ping the replanner
// every Nth iteration regardless of context.
func (o *Orchestrator) runArbiter(req ArbiterRequest) {
	uiTicket("🚀", RoleArbiter, req.Ticket, "START",
		fmt.Sprintf("trigger=%s/%s ws=%s", req.Source, req.Trigger, req.Workspace))

	wsAbs := wsPath(o.Root, req.Workspace)

	input := o.buildAgentInput(RoleArbiter, req.Ticket, wsAbs) +
		fmt.Sprintf("\nTRIGGER_ROLE: %s\nTRIGGER_VERDICT: %s\nTRIGGER_DETAIL: %s\n",
			req.Source, req.Trigger, req.Detail)

	prompt := loadPrompt(RoleArbiter)
	stdin := concatPromptInput(prompt, input)

	env := o.ticketEnv(req.Ticket, wsAbs)
	env["TRIGGER_ROLE"] = string(req.Source)
	env["TRIGGER_VERDICT"] = string(req.Trigger)
	env["TRIGGER_DETAIL"] = req.Detail

	res := o.runAgentArbiter(req.Ticket, req.Workspace, stdin, env)
	verdict, detail := lastVerdict(res.Events)

	switch verdict {
	case VerdictContinue:
		o.recordEvent(req.Ticket, "ARBITER_CONTINUE", detail)
		uiTicket("➡️", RoleArbiter, req.Ticket, "CONTINUE", detail)

		// Dispatch back into the cycle based on which role failed and how.
		switch req.Source {
		case RoleTasker:
			// Tasker NO_PLAN — try planning again. Same session, fresh ws.
			o.Mu.Lock()
			o.spawnTaskerLocked(req.Ticket)
			o.Mu.Unlock()
		case RoleDigger:
			// Digger CANT_DO — same workspace, prior session preserved.
			o.spawnDiggerSameWorkspaceLocked(req.Ticket, req.Workspace)
		case RoleReviewer:
			// REWORK or DISCARD — digger gets the trigger detail as feedback.
			o.spawnDiggerSameWorkspaceLocked(req.Ticket, req.Workspace)
		case RoleMerger:
			// MERGE_FAIL or FF_FAIL — digger rebases onto current trunk head.
			o.spawnDiggerWithRebase(req.Ticket, req.Workspace, req.RebaseTarget, req.MergeOut)
		}
	case VerdictEscalate:
		o.recordEvent(req.Ticket, "ARBITER_ESCALATE", detail)
		uiTicket("⤴️", RoleArbiter, req.Ticket, "ESCALATE", detail)

		o.QReplanner <- ReplanRequest{
			Source: RoleArbiter,
			Ticket: req.Ticket,
			Reason: fmt.Sprintf("arbiter escalated from %s/%s: %s — original trigger detail: %s",
				req.Source, req.Trigger, detail, req.Detail),
		}
		uiTicket("📥", RoleArbiter, req.Ticket, "→Q_replanner", detail)
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
		o.QReplanner <- ReplanRequest{Source: RoleMerger, Reason: "GOALS.md changed locally in trunk"}
	}

	diggerBranch := "ovs/" + req.Workspace
	diggerWSAbs := wsPath(o.Root, req.Workspace)
	mergerWSAbs := wsPath(o.Root, mergerWS)

	o.Mu.Lock()
	input := o.buildAgentInput(RoleMerger, req.Ticket, mergerWSAbs) +
		fmt.Sprintf("\nDIGGER_BRANCH: %s\nDIGGER_WORKTREE: %s\nMERGER_WORKTREE: %s\nTRUNK_HEAD: %s\n",
			diggerBranch, diggerWSAbs, mergerWSAbs, trunkHead)
	o.Mu.Unlock()

	prompt := loadPrompt(RoleMerger)
	stdin := concatPromptInput(prompt, input)

	env := map[string]string{
		"TICKET":          fmt.Sprintf("%d", req.Ticket),
		"DIGGER_BRANCH":   diggerBranch,
		"DIGGER_WORKTREE": diggerWSAbs,
		"MERGER_WORKTREE": mergerWSAbs,
		"TRUNK_HEAD":      trunkHead,
	}

	res := o.runAgentMerger(req.Ticket, mergerWS, stdin, env)

	for _, line := range eventReplans(res.Events) {
		o.QReplanner <- ReplanRequest{Source: RoleMerger, Ticket: req.Ticket, Reason: line}
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

			o.QArbiter <- ArbiterRequest{
				Ticket:       req.Ticket,
				Workspace:    req.Workspace,
				Source:       RoleMerger,
				Trigger:      VerdictMergeFail,
				Detail:       "ff-merge into trunk failed: " + out,
				RebaseTarget: newHead,
				MergeOut:     out,
			}
			uiTicket("📥", RoleMerger, req.Ticket, "→Q_arbiter", "ff_fail")

			return
		}

		o.recordEvent(req.Ticket, "MERGED", "ws="+req.Workspace+" merger_ws="+mergerWS+" head="+newHead)
		uiTicket("✅", RoleMerger, req.Ticket, "MERGED", "head="+newHead[:8])

		o.Mu.Lock()
		o.closeTicketLocked(req.Ticket, CloseMerged)
		o.Mu.Unlock()

		uiTicket("🏁", "", req.Ticket, "CLOSED", "MERGED")

		o.QReplanner <- ReplanRequest{Source: RoleMerger, Ticket: req.Ticket, Reason: "merged"}
		uiTicket("📥", RoleMerger, req.Ticket, "→Q_replanner", "post-merge")

		o.signalWake()

	case VerdictMergeFail:
		o.recordEvent(req.Ticket, "MERGE_FAIL", detail)
		uiTicket("❌", RoleMerger, req.Ticket, "FAIL", detail)

		head := CurrentTrunkHash(o.Trunk)

		o.QArbiter <- ArbiterRequest{
			Ticket:       req.Ticket,
			Workspace:    req.Workspace,
			Source:       RoleMerger,
			Trigger:      VerdictMergeFail,
			Detail:       detail,
			RebaseTarget: head,
			MergeOut:     detail,
		}
		uiTicket("📥", RoleMerger, req.Ticket, "→Q_arbiter", detail)
	}
}

func (o *Orchestrator) spawnDiggerWithRebase(ticketN int, ws, target, mergeOut string) {
	o.Mu.Lock()

	short := target

	if len(short) > 8 {
		short = short[:8]
	}

	o.recordEventLocked(ticketN, "AGENT_START",
		fmt.Sprintf("role=%s ws=%s rebase=%s", RoleDigger, ws, short))
	uiTicket("🚀", RoleDigger, ticketN, "START", "ws="+ws+" rebase→"+short)

	wsAbs := wsPath(o.Root, ws)
	// MERGE_FAIL_OUTPUT is passed as extraInput so it stays in stdin on every
	// retry iteration alongside fresh PRIOR_RUNS from buildAgentInput.
	extraInput := "\nMERGE_FAIL_OUTPUT:\n" + mergeOut + "\nREBASE_TARGET: " + target + "\n"
	env := o.ticketEnv(ticketN, wsAbs)
	env["PREV_WORKSPACE"] = wsAbs
	env["REBASE_TARGET"] = target

	o.Mu.Unlock()

	go func() {
		res := o.runAgentDigger(ticketN, ws, extraInput, env)
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

	input := o.agentSelfBlock(RoleOverseer, 0) +
		fmt.Sprintf("REASON: %s\n\nCURRENT_TASKS:\n%s\n", req.Reason, currentTasks)
	prompt := loadPrompt(RoleOverseer)
	stdin := concatPromptInput(prompt, input)

	env := map[string]string{
		"REASON":     req.Reason,
		"TRUNK_PATH": o.Trunk,
		"TRUNK_HASH": CurrentTrunkHash(o.Trunk),
		"TASKS_DB":   tasksDBPath(o.Root),
		"RUNS_DIR":   runsDir(o.Root),
	}

	res := o.runAgentOverseer(wsID, stdin, env)

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
			o.QReplanner <- ReplanRequest{Source: RoleOverseer, Ticket: 0, Reason: line}
			uiSys("📥", "→Q_replanner", "from overseer: "+line)
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
