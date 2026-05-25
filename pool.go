package main

import (
	"fmt"
	"os"
	"strings"
)

// Pools are the only place a harness is invoked. Each role gets a fixed number of
// worker goroutines (poolSizes) that read Jobs the coordinator hands them, run the
// harness to a recognized verdict, and reply with an AgentResult on o.Events. A
// worker never touches ticket state — everything it needs is in the Job (a ticket
// snapshot, the workspace, role-specific context). Total harness concurrency is the
// sum of pool sizes; merger / replanner are size 1 (serial).

func (o *Orchestrator) startPools() {
	for role, size := range poolSizes {
		for i := 0; i < size; i++ {
			go o.poolWorker(role)
		}
	}
}

func (o *Orchestrator) poolWorker(role AgentRole) {
	for {
		select {
		case <-o.StopCtx.Done():
			return
		case job := <-o.jobs[role]:
			var res AgentResult

			exc := Try(func() { res = o.runJob(job) })

			if exc != nil {
				// A worker-side failure (e.g. git clone) must still report back, or
				// the ticket would stay SCHEDULED forever. An empty result makes the
				// coordinator reset the shadow and re-dispatch.
				uiTicket("💥", role, job.Ticket.N, "WORKER_PANIC", exc.Error())
				res = AgentResult{Role: role, Ticket: job.Ticket.N}
			}

			select {
			case o.Events <- res:
			case <-o.StopCtx.Done():
				return
			}
		}
	}
}

func (o *Orchestrator) runJob(job Job) AgentResult {
	switch job.Role {
	case RoleTasker:
		return o.jobTasker(job)
	case RoleDigger:
		return o.jobDigger(job)
	case RoleReviewer:
		return o.jobReviewer(job)
	case RoleMerger:
		return o.jobMerger(job)
	case RoleArbiter:
		return o.jobArbiter(job)
	case RoleReplanner:
		return o.jobReplanner(job)
	}

	ThrowFmt("runJob: unknown role %q", job.Role)

	return AgentResult{}
}

// workspaceFor resolves the workspace a Job runs in: a fresh git clone when NewWS is
// set, otherwise the workspace the coordinator picked (the digger branch).
func (o *Orchestrator) workspaceFor(job Job) string {
	if job.NewWS {
		return NewWorkspace(o.Root, o.Trunk)
	}

	return job.WS
}

func (o *Orchestrator) jobTasker(job Job) AgentResult {
	ws := o.workspaceFor(job)
	wsAbs := wsPath(o.Root, ws)
	env := o.ticketEnv(job.Ticket.N, wsAbs)
	stdin := concatPromptInput(loadPrompt(o.Trunk, RoleTasker, PromptData{}), o.buildAgentInput(RoleTasker, job.Ticket, wsAbs))

	for {
		res := o.runAgent(RoleTasker, job.Ticket.N, ws, stdin, env)

		if taskerPlanContent(res.Events) != "" || hasReplanEvent(res.Events) {
			res.Workspace = ws

			return res
		}

		uiTicket("🔄", RoleTasker, job.Ticket.N, "RESPAWN", "no plan event")
	}
}

func (o *Orchestrator) jobDigger(job Job) AgentResult {
	ws := o.workspaceFor(job)
	wsAbs := wsPath(o.Root, ws)
	prompt := loadPrompt(o.Trunk, RoleDigger, PromptData{})

	env := o.ticketEnv(job.Ticket.N, wsAbs)
	env["PREV_WORKSPACE"] = wsAbs

	extra := fmt.Sprintf("PREV_WORKSPACE: %s\n", wsAbs)

	if job.MergeOut != "" {
		extra += "\nMERGE_FAIL_OUTPUT:\n" + job.MergeOut + "\nREBASE_TARGET: " + job.RebaseTarget + "\n"
		env["REBASE_TARGET"] = job.RebaseTarget
	}

	// Rebuilt per attempt so PRIOR_RUNS includes the just-failed try.
	build := func() string {
		return concatPromptInput(prompt, o.buildAgentInput(RoleDigger, job.Ticket, wsAbs)+extra)
	}

	for {
		res := o.runAgent(RoleDigger, job.Ticket.N, ws, build(), env)
		v, _ := lastVerdict(res.Events)

		if v == VerdictReady || v == VerdictCantDo || v == VerdictAlgedonic {
			res.Workspace = ws

			return res
		}

		uiTicket("🔄", RoleDigger, job.Ticket.N, "RESPAWN", fmt.Sprintf("verdict=%q", v))
	}
}

func (o *Orchestrator) jobReviewer(job Job) AgentResult {
	ws := job.WS
	wsAbs := wsPath(o.Root, ws)
	env := o.ticketEnv(job.Ticket.N, wsAbs)
	stdin := concatPromptInput(loadPrompt(o.Trunk, RoleReviewer, PromptData{}), o.buildAgentInput(RoleReviewer, job.Ticket, wsAbs))

	for {
		res := o.runAgent(RoleReviewer, job.Ticket.N, ws, stdin, env)
		v, _ := lastVerdict(res.Events)

		if v == VerdictApprove || v == VerdictRework || v == VerdictDiscard {
			res.Workspace = ws

			return res
		}

		uiTicket("🔄", RoleReviewer, job.Ticket.N, "RESPAWN", fmt.Sprintf("verdict=%q", v))
	}
}

func (o *Orchestrator) jobArbiter(job Job) AgentResult {
	ws := job.WS
	wsAbs := wsPath(o.Root, ws)

	env := o.ticketEnv(job.Ticket.N, wsAbs)
	env["TRIGGER_ROLE"] = string(sourceForTrigger(job.Trigger))
	env["TRIGGER_VERDICT"] = string(job.Trigger)
	env["TRIGGER_DETAIL"] = job.Detail

	input := o.buildAgentInput(RoleArbiter, job.Ticket, wsAbs) +
		fmt.Sprintf("\nTRIGGER_ROLE: %s\nTRIGGER_VERDICT: %s\nTRIGGER_DETAIL: %s\n",
			sourceForTrigger(job.Trigger), job.Trigger, job.Detail)
	stdin := concatPromptInput(loadPrompt(o.Trunk, RoleArbiter, PromptData{}), input)

	for {
		res := o.runAgent(RoleArbiter, job.Ticket.N, ws, stdin, env)
		v, _ := lastVerdict(res.Events)

		if v == VerdictContinue || v == VerdictEscalate {
			res.Workspace = ws

			return res
		}

		uiTicket("🔄", RoleArbiter, job.Ticket.N, "RESPAWN", fmt.Sprintf("verdict=%q", v))
	}
}

func (o *Orchestrator) jobMerger(job Job) AgentResult {
	diggerWS := job.WS
	mergerWS := NewWorkspace(o.Root, o.Trunk)
	mergerWSAbs := wsPath(o.Root, mergerWS)
	diggerWSAbs := wsPath(o.Root, diggerWS)
	diggerBranch := "ovs/" + diggerWS
	trunkHead := CurrentTrunkHash(o.Trunk)

	env := map[string]string{
		"TICKET":          fmt.Sprintf("%d", job.Ticket.N),
		"DIGGER_BRANCH":   diggerBranch,
		"DIGGER_WORKTREE": diggerWSAbs,
		"MERGER_WORKTREE": mergerWSAbs,
		"TRUNK_HEAD":      trunkHead,
	}

	input := o.buildAgentInput(RoleMerger, job.Ticket, mergerWSAbs) +
		fmt.Sprintf("\nDIGGER_BRANCH: %s\nDIGGER_WORKTREE: %s\nMERGER_WORKTREE: %s\nTRUNK_HEAD: %s\n",
			diggerBranch, diggerWSAbs, mergerWSAbs, trunkHead)
	stdin := concatPromptInput(loadPrompt(o.Trunk, RoleMerger, PromptData{}), input)

	for {
		res := o.runAgent(RoleMerger, job.Ticket.N, mergerWS, stdin, env)
		v, _ := lastVerdict(res.Events)

		if v == VerdictMerged || v == VerdictMergeFail {
			// Report the merger workspace — the coordinator ff-merges its branch into
			// trunk (the single place trunk is written).
			res.Workspace = mergerWS

			return res
		}

		uiTicket("🔄", RoleMerger, job.Ticket.N, "RESPAWN", fmt.Sprintf("verdict=%q", v))
	}
}

func (o *Orchestrator) jobReplanner(job Job) AgentResult {
	ws := o.workspaceFor(job)
	triggers := formatReplanTriggers(job.Reasons)
	chat := strings.Join(job.ChatLog, "\n")

	input := o.agentSelfBlock(RoleReplanner, 0) +
		fmt.Sprintf("SUBAGENT: %s\n\nREPLAN_TRIGGERS (every nudge accumulated since the last replanner run — address them together as one batch; some may be duplicates or already handled, check TASKS_DB before acting):\n%s\nREPLAN_CHAT (all team-chat lines accumulated since the previous replanner run):\n%s\nRUNS_DIR: %s\nTASKS_DB: %s\n\n%s",
			job.Subagent, triggers, chat, runsDir(o.Root), tasksDBPath(o.Root), job.Snapshot)
	stdin := concatPromptInput(loadPrompt(o.Trunk, RoleReplanner, PromptData{Subagent: job.Subagent}), input)

	env := map[string]string{
		"REPLAN_TRIGGERS": triggers,
		"REPLAN_CHAT":     chat,
		"RUNS_DIR":        runsDir(o.Root),
		"TASKS_DB":        tasksDBPath(o.Root),
	}

	for {
		res := o.runAgent(RoleReplanner, 0, ws, stdin, env)

		if !hasJSONInUnparsed(res.Events) {
			res.Workspace = ws

			return res
		}

		uiSys("🔄", "REPLANNER_RESPAWN", "unparsed JSON — retrying for clean output")
	}
}

// sourceForTrigger maps an arbiter trigger verdict back to the role that raised it,
// for the TRIGGER_ROLE field the arbiter prompt expects.
func sourceForTrigger(t AgentVerdict) AgentRole {
	switch t {
	case VerdictNoPlan:
		return RoleTasker
	case VerdictCantDo:
		return RoleDigger
	case VerdictRework, VerdictDiscard:
		return RoleReviewer
	case VerdictMergeFail:
		return RoleMerger
	}

	return ""
}

// agentSelfBlock identifies the agent to itself: role, harness, model, chat-log path.
// Reads only immutable bindings, so it is safe to call from a pool worker.
func (o *Orchestrator) agentSelfBlock(role AgentRole, ticket int) string {
	hm := o.harnessModelForRole(role)
	model := hm.resolveModel(role)

	if model == "" {
		model = "(harness default)"
	}

	return fmt.Sprintf("ROLE: %s\nMODEL: %s\nHARNESS: %s\nMESSAGES_LOG: %s\n",
		role, model, hm.Harness.Name(), messagesLogPath(o.Root))
}

// ticketEnv is the common env every ticket-bound role gets — prompts reference these
// as $WORKSPACE / $TRUNK_PATH etc. in bash tool calls.
func (o *Orchestrator) ticketEnv(ticketN int, wsAbs string) map[string]string {
	return map[string]string{
		"WORKSPACE":  wsAbs,
		"TICKET":     fmt.Sprintf("%d", ticketN),
		"TRUNK_PATH": o.Trunk,
		"TRUNK_HASH": CurrentTrunkHash(o.Trunk),
	}
}

// buildAgentInput renders the prose input header for a ticket-bound role from the
// Job's ticket snapshot plus on-disk context (plan / log / chat / prior runs). Reads
// no coordinator state — keyed only by the snapshot and files.
func (o *Orchestrator) buildAgentInput(role AgentRole, t Ticket, wsAbs string) string {
	var sb strings.Builder

	sb.WriteString(o.agentSelfBlock(role, t.N))
	fmt.Fprintf(&sb, "TICKET: %d\nDESCR: %s\nDEPS: %v\n", t.N, t.Descr, t.Deps)
	fmt.Fprintf(&sb, "WORKSPACE: %s\n", wsAbs)
	fmt.Fprintf(&sb, "TRUNK_PATH: %s\n", o.Trunk)
	fmt.Fprintf(&sb, "TRUNK_HASH: %s\n", CurrentTrunkHash(o.Trunk))

	if planExists(o.Root, t.N) {
		if data, err := os.ReadFile(ticketPlanPath(o.Root, t.N)); err == nil {
			fmt.Fprintf(&sb, "\nPLAN:\n%s\n", string(data))
		}
	}

	if t.Type != TicketTypePlan {
		if plans := dependencyPlans(o.Root, t.Deps); plans != "" {
			fmt.Fprintf(&sb, "\nDEPENDENCY_PLANS (from deps with plan.md):\n%s\n", plans)
		}
	}

	if data, err := os.ReadFile(ticketLogPath(o.Root, t.N)); err == nil {
		fmt.Fprintf(&sb, "\nLOG (phase transitions for this ticket, append-only):\n%s\n", string(data))
	}

	if msgs := ticketMessages(o.Root, t.N); msgs != "" {
		fmt.Fprintf(&sb, "\nTICKET_CHAT (lines from MESSAGES_LOG mentioning this ticket):\n%s\n", msgs)
	}

	if prior := priorRunsForTicket(o.Root, t.N); prior != "" {
		fmt.Fprintf(&sb, "\nPRIOR_RUNS (compact summaries — Read each LOG_FILE for the full reasoning stream):\n%s\n", prior)
	}

	return sb.String()
}

func dependencyPlans(orchRoot string, deps []int) string {
	var sb strings.Builder

	for _, dep := range deps {
		data, err := os.ReadFile(ticketPlanPath(orchRoot, dep))

		if err != nil {
			continue
		}

		fmt.Fprintf(&sb, "T-%d:\n%s\n", dep, string(data))
	}

	return strings.TrimSpace(sb.String())
}

func replaceDepRefs(deps []int, from, to int) ([]int, bool) {
	if from == to {
		return deps, false
	}

	changed := false
	seen := map[int]bool{}
	var out []int

	for _, dep := range deps {
		if dep == from {
			dep = to
			changed = true
		}

		if seen[dep] {
			changed = true
			continue
		}

		seen[dep] = true
		out = append(out, dep)
	}

	return out, changed
}

// formatReplanTriggers renders the batch of nudges a replanner Job carries — one
// numbered line per trigger (source role, ticket, optional workspace, reason).
func formatReplanTriggers(reasons []ReplanReason) string {
	var sb strings.Builder

	for i, r := range reasons {
		if r.Workspace != "" {
			fmt.Fprintf(&sb, "%d. source=%s ticket=T-%d workspace=%s: %s\n", i+1, r.Source, r.Ticket, r.Workspace, r.Reason)
		} else {
			fmt.Fprintf(&sb, "%d. source=%s ticket=T-%d: %s\n", i+1, r.Source, r.Ticket, r.Reason)
		}
	}

	return sb.String()
}

// eventReplans pulls every `replan` event's reason out of an agent's event stream,
// in emission order.
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

// taskerPlanContent scans for the tasker's `plan` event and returns the plan body.
// The event carries a `path` to a file the tasker wrote (avoids embedding multi-line
// markdown in a JSON string); falls back to `body`.
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

// replannerTaskOps pulls every `task` event out of the replanner's stream, in
// emission order. No other role emits task events.
func replannerTaskOps(events []map[string]any) []map[string]any {
	var out []map[string]any

	for _, ev := range events {
		if t, _ := ev["type"].(string); t == "task" {
			out = append(out, ev)
		}
	}

	return out
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

// hasJSONInUnparsed reports whether the synthetic unparsed event contains JSON
// fragments — a signal the model tried to emit structured output but malformed it.
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

// applyTaskOp applies one replanner `task` event to a ticket-list sandbox, throwing
// on any schema violation. Ops apply in emission order; the caller validates the
// cumulative result before committing.
func applyTaskOp(tickets []Ticket, ev map[string]any) []Ticket {
	op, _ := ev["op"].(string)
	n := 0

	if op != "replace" {
		n = jsonInt(ev["n"])

		if n <= 0 {
			ThrowFmt("task op %q: missing or invalid n", op)
		}
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

		for _, t := range tickets {
			if n <= t.N {
				ThrowFmt("op=new ticket %d: N must be greater than all existing tickets (max=%d)", n, t.N)
			}
		}

		descr, _ := ev["descr"].(string)
		ticketType := jsonTicketType(ev["ticket_type"])

		if ticketType == "" {
			ThrowFmt("op=new ticket %d: ticket_type must be one of %q or %q", n, TicketTypePlan, TicketTypeCode)
		}

		return append(tickets, Ticket{
			N:     n,
			Type:  ticketType,
			Phase: newTicketPhase(ticketType),
			Descr: descr,
			Deps:  jsonIntArray(ev["deps"]),
		})
	case "update":
		if idx < 0 {
			ThrowFmt("op=update ticket %d: not found", n)
		}

		if tickets[idx].Phase.Terminal() {
			ThrowFmt("op=update ticket %d: terminal (%s)", n, tickets[idx].Phase)
		}

		if _, ok := ev["ticket_type"]; ok {
			ThrowFmt("op=update ticket %d: ticket_type is immutable", n)
		}

		if _, ok := ev["descr"]; ok {
			ThrowFmt("op=update ticket %d: only deps may be updated", n)
		}

		if _, ok := ev["deps"]; !ok {
			ThrowFmt("op=update ticket %d: update requires deps", n)
		}

		tickets[idx].Deps = jsonIntArray(ev["deps"])

		return tickets
	case "cancel":
		if idx < 0 {
			ThrowFmt("op=cancel ticket %d: not found", n)
		}

		if tickets[idx].Phase.Terminal() {
			ThrowFmt("op=cancel ticket %d: terminal (%s)", n, tickets[idx].Phase)
		}

		tickets[idx].Phase = PhaseDiscarded

		return tickets
	case "replace":
		from := jsonInt(ev["from"])
		to := jsonInt(ev["to"])

		if from <= 0 || to <= 0 {
			ThrowFmt("op=replace: from/to must be valid ticket numbers")
		}

		known := map[int]bool{}

		for _, t := range tickets {
			known[t.N] = true
		}

		if !known[from] {
			ThrowFmt("op=replace: from ticket %d not found", from)
		}

		if !known[to] {
			ThrowFmt("op=replace: to ticket %d not found", to)
		}

		for i := range tickets {
			if tickets[i].Phase.Terminal() {
				continue
			}

			deps, changed := replaceDepRefs(tickets[i].Deps, from, to)

			if changed {
				tickets[i].Deps = deps
			}
		}

		return tickets
	}

	ThrowFmt("unknown task op %q (expected new/update/cancel/replace)", op)

	return tickets
}
