package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// Pools are the only place a harness is invoked. Each role gets a fixed number of
// worker goroutines (poolSizes) that read Jobs the coordinator hands them, run the
// harness to a recognized verdict, and reply with an AgentResult on o.Events. A
// worker never touches ticket state — everything it needs is in the Job (a ticket
// snapshot, the workspace, role-specific context). Total harness concurrency is the
// sum of pool sizes; merger / lead are size 1 (serial).

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
	case RoleLead:
		return o.jobLead(job)
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
	p := o.ticketParams(RoleTasker, job, wsPath(o.Root, ws))
	stdin, env := loadPrompt(o.Trunk, RoleTasker, p), envFrom(p)

	for {
		res := o.runAgent(RoleTasker, job.Ticket.N, ws, stdin, env)

		if o.StopCtx.Err() != nil {
			return res
		}

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

	for {
		// Rebuilt per attempt so PRIOR_RUNS includes the just-failed try.
		p := o.ticketParams(RoleDigger, job, wsAbs)

		// PREV_WORKSPACE drives the digger's "this is a continuation" block: set it only
		// when we reused an existing branch workspace (a real prior attempt), not on a
		// fresh first-dispatch clone, so {{if .PREV_WORKSPACE}} means what it says.
		if !job.NewWS {
			p["PREV_WORKSPACE"] = wsAbs
		}

		res := o.runAgent(RoleDigger, job.Ticket.N, ws, loadPrompt(o.Trunk, RoleDigger, p), envFrom(p))

		if o.StopCtx.Err() != nil {
			return res
		}

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
	p := o.ticketParams(RoleReviewer, job, wsPath(o.Root, ws))
	stdin, env := loadPrompt(o.Trunk, RoleReviewer, p), envFrom(p)

	for {
		res := o.runAgent(RoleReviewer, job.Ticket.N, ws, stdin, env)

		if o.StopCtx.Err() != nil {
			return res
		}

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
	p := o.ticketParams(RoleArbiter, job, wsPath(o.Root, ws))
	stdin, env := loadPrompt(o.Trunk, RoleArbiter, p), envFrom(p)

	for {
		res := o.runAgent(RoleArbiter, job.Ticket.N, ws, stdin, env)

		if o.StopCtx.Err() != nil {
			return res
		}

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

	diggerBranch := "ovs/" + diggerWS
	diggerWorktree := wsPath(o.Root, diggerWS)
	mergerWorktree := wsPath(o.Root, mergerWS)

	// When the trunk ships ./acceptance, IT decides the merge — the LLM merger agent
	// is never run. Only a missing script (or --sim) falls through to the agent below.
	if res, ok := o.mergerAcceptance(job.Ticket.N, mergerWS, mergerWorktree, diggerBranch, diggerWorktree); ok {
		return res
	}

	p := o.ticketParams(RoleMerger, job, mergerWorktree)
	p["DIGGER_BRANCH"] = diggerBranch
	p["DIGGER_WORKTREE"] = diggerWorktree
	p["MERGER_WORKTREE"] = mergerWorktree
	p["TRUNK_HEAD"] = p["TRUNK_HASH"]

	stdin, env := loadPrompt(o.Trunk, RoleMerger, p), envFrom(p)

	for {
		res := o.runAgent(RoleMerger, job.Ticket.N, mergerWS, stdin, env)

		if o.StopCtx.Err() != nil {
			return res
		}

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

func (o *Orchestrator) jobLead(job Job) AgentResult {
	ws := o.workspaceFor(job)

	p := o.selfParams(RoleLead)
	p["Subagent"] = job.Params["Subagent"]
	p["LeadPlans"] = job.Params["LeadPlans"]
	p["REPLAN_TRIGGERS"] = formatReplanTriggers(job.Reasons)
	p["REPLAN_CHAT"] = strings.Join(job.ChatLog, "\n")
	p["RUNS_DIR"] = runsDir(o.Root)
	p["TASKS_DB"] = tasksDBPath(o.Root)
	p["SNAPSHOT"] = job.Snapshot

	stdin, env := loadPrompt(o.Trunk, RoleLead, p), envFrom(p)

	for {
		res := o.runAgent(RoleLead, 0, ws, stdin, env)

		if o.StopCtx.Err() != nil {
			return res
		}

		if !hasJSONInUnparsed(res.Events) {
			res.Workspace = ws

			return res
		}

		uiSys("🔄", "LEAD_RESPAWN", "unparsed JSON — retrying for clean output")
	}
}

// mergerAcceptance IS the merger whenever the trunk ships an executable ./acceptance:
// it decides every merge deterministically and the LLM merger agent is never run.
// The merger is serial (pool size 1) and the slowest stage, so a script verdict takes
// the agent out of the loop entirely. It merges the digger's branch into a fresh trunk
// clone, then runs `./acceptance <trunk> <merged-tree>` from the trunk, streaming the
// script's output to the UI line by line as it appears (the run can be long):
//
//   - clean merge + exit 0 → MERGED. The merge commit already sits on mergerWS's
//     branch, so the coordinator ff-merges it into trunk exactly as it would an
//     agent's result.
//   - exit != 0            → MERGE_FAIL. Acceptance rejected the merged tree; that IS
//     the verdict — no second opinion from an agent.
//   - merge conflict       → MERGE_FAIL. The branch does not apply to current trunk.
//
// MERGE_FAIL routes through the arbiter exactly like an agent merge-fail (the digger
// rebases onto current trunk and retries with the acceptance output as feedback). The
// NEW tree handed to acceptance is the MERGED worktree (current trunk + the digger's
// branch) — the exact landing candidate — not the digger's possibly-stale standalone
// workspace. Returns ok=false ONLY when there is no ./acceptance (or under --sim); the
// caller then runs the LLM merger agent.
func (o *Orchestrator) mergerAcceptance(ticket int, mergerWS, mergerWorktree, diggerBranch, diggerWorktree string) (AgentResult, bool) {
	if simulate || !hasAcceptanceGate(o.Trunk) {
		return AgentResult{}, false
	}

	uiTicket("🚦", RoleMerger, ticket, "GATE", "found "+acceptancePath(o.Trunk)+" — merging "+diggerBranch)

	if !MergeBranchInto(mergerWorktree, diggerWorktree, diggerBranch) {
		detail := "digger branch " + diggerBranch + " does not merge cleanly into current trunk (conflict)"
		uiTicket("❌", RoleMerger, ticket, "MERGE_FAIL", detail)

		return mergerVerdict(ticket, mergerWS, VerdictMergeFail, detail), true
	}

	acceptArgs := []string{acceptancePath(o.Trunk), o.Trunk, mergerWorktree}
	uiTicket("🔧", RoleMerger, ticket, "ACCEPTANCE", strings.Join(acceptArgs, " "))

	out, code := o.runAcceptance(ticket, mergerWorktree)
	o.noteMessage(RoleMerger, ticket, mergerGateMessage(code, out))

	verdict, detail := VerdictMerged, "acceptance exit=0 — landed"
	emoji, kind := "✅", "MERGED"

	if code != 0 {
		verdict = VerdictMergeFail
		detail = fmt.Sprintf("acceptance exit=%d — merged tree rejected:\n%s", code, tailLines(out, 40))
		emoji, kind = "❌", "MERGE_FAIL"
	}

	Try(func() {
		writeGateRun(o.Root, ticket, mergerWS, acceptArgs, out, code, verdict, detail)
	}).Catch(func(e *Exception) {
		uiTicket("⚠️", RoleMerger, ticket, "ACCEPTANCE_LOG", e.Error())
	})

	uiTicket(emoji, RoleMerger, ticket, kind, fmt.Sprintf("acceptance exit=%d", code))

	return mergerVerdict(ticket, mergerWS, verdict, detail), true
}

// mergerVerdict synthesizes the merger's result for the coordinator: a single verdict
// event (MERGED → onMerger ff-merges mergerWS's branch into trunk; MERGE_FAIL → onMerger
// routes it to the arbiter with the detail as the digger's rebase feedback).
func mergerVerdict(ticket int, ws string, v AgentVerdict, detail string) AgentResult {
	return AgentResult{
		Role:      RoleMerger,
		Ticket:    ticket,
		Workspace: ws,
		Events:    []map[string]any{{"type": "verdict", "verdict": string(v), "detail": detail}},
	}
}

// runAcceptance runs the trunk's ./acceptance from the trunk, passing the old (trunk)
// and new (merged-tree) paths, streaming its merged stdout+stderr to the UI one line
// at a time as it is produced — the script can run for many minutes, and a single
// buffered dump at the end looks like a hang. Returns the full captured output and the
// exit code (-1 if it couldn't be launched). EOF on the read end (hence return) only
// happens once acceptance and every child it spawned have closed the write end.
func (o *Orchestrator) runAcceptance(ticket int, mergedTree string) (string, int) {
	cmd := exec.Command(acceptancePath(o.Trunk), o.Trunk, mergedTree)
	cmd.Dir = o.Trunk

	pr, pw := Throw3(os.Pipe())
	cmd.Stdout = pw
	cmd.Stderr = pw

	Throw(cmd.Start())
	pw.Close() // parent drops its write end; pr sees EOF when the child tree closes it

	var buf strings.Builder

	scanner := bufio.NewScanner(pr)
	scanner.Buffer(make([]byte, 1<<20), 16<<20)

	for scanner.Scan() {
		line := scanner.Text()
		buf.WriteString(line)
		buf.WriteByte('\n')
		ui("·", RoleMerger, ticket, "acceptance", line)
	}

	pr.Close()
	err := cmd.Wait()

	if err == nil {
		return buf.String(), 0
	}

	if ee, ok := err.(*exec.ExitError); ok {
		return buf.String(), ee.ExitCode()
	}

	return buf.String() + err.Error() + "\n", -1
}

// tailLines returns the last n lines of s (all of it when shorter), for embedding a
// readable failure excerpt in a MERGE_FAIL detail without dragging the whole log in.
func tailLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")

	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}

	return strings.Join(lines, "\n")
}

// mergerGateMessage renders the one team-chat line for an acceptance run: its
// verdict, exit code, and output collapsed to a single line (messages.txt keeps
// one entry per line, so embedded newlines become ⏎).
func mergerGateMessage(code int, out string) string {
	verdict := "rejected"

	if code == 0 {
		verdict = "accepted"
	}

	body := strings.TrimSpace(out)

	if body == "" {
		body = "(no output)"
	}

	body = strings.ReplaceAll(body, "\n", " ⏎ ")

	return fmt.Sprintf("acceptance gate %s (exit %d): %s", verdict, code, body)
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

// selfParams is the agent's self-identification, present for every role.
func (o *Orchestrator) selfParams(role AgentRole) map[string]string {
	hm := o.harnessModelForRole(role)
	model := hm.resolveModel(role)

	if model == "" {
		model = "(harness default)"
	}

	return map[string]string{
		"ROLE":         string(role),
		"MODEL":        model,
		"HARNESS":      hm.Harness.Name(),
		"MESSAGES_LOG": messagesLogPath(o.Root),
	}
}

// ticketParams assembles the prompt + env params for a ticket-bound role: the self
// block, the ticket snapshot, on-disk context (plan / log / chat / prior runs), and the
// coordinator's per-dispatch params (Plans, trigger, merge-fail) merged last. One map
// feeds both the template (loadPrompt) and the env (envFrom), so they can't drift — and
// TRUNK_HASH is read once here, not separately for env and input. The re-ingested LOG
// is stripped of the bulky PROMPT blocks the coordinator records, so prompts don't feed
// back on themselves.
func (o *Orchestrator) ticketParams(role AgentRole, job Job, wsAbs string) map[string]string {
	t := job.Ticket
	p := o.selfParams(role)
	p["TICKET"] = fmt.Sprintf("%d", t.N)
	p["DESCR"] = t.Descr
	p["DEPS"] = fmt.Sprintf("%v", t.Deps)
	p["WORKSPACE"] = wsAbs
	p["TRUNK_PATH"] = o.Trunk
	p["TRUNK_HASH"] = CurrentTrunkHash(o.Trunk)

	if planExists(o.Root, t.N) {
		if data, err := os.ReadFile(ticketPlanPath(o.Root, t.N)); err == nil {
			p["PLAN"] = string(data)
		}
	}

	if log := ticketLogForContext(o.Root, t.N); log != "" {
		p["LOG"] = log
	}

	if msgs := ticketMessages(o.Root, t.N); msgs != "" {
		p["TICKET_CHAT"] = msgs
	}

	if prior := priorRunsForTicket(o.Root, t.N); prior != "" {
		p["PRIOR_RUNS"] = prior
	}

	for k, v := range job.Params {
		p[k] = v
	}

	return p
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

// formatReplanTriggers renders the batch of nudges a lead Job carries — one
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

// leadTaskOps pulls every `task` event out of the lead's stream, in
// emission order. No other role emits task events.
func leadTaskOps(events []map[string]any) []map[string]any {
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

// opAffectedTickets returns the existing tickets whose prior state a lead op
// depends on — the ones whose generation must still match the planning snapshot. `new`
// only creates (its number didn't exist in the snapshot), so it affects nothing existing.
func opAffectedTickets(ev map[string]any) []int {
	switch op, _ := ev["op"].(string); op {
	case "cancel", "update":
		return []int{jsonInt(ev["n"])}
	case "replace":
		return []int{jsonInt(ev["from"]), jsonInt(ev["to"])}
	}

	return nil
}

// applyTaskOp applies one lead `task` event to a ticket-list sandbox, throwing
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
