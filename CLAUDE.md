# overseer — context for Claude

Go orchestrator that drives a Claude Code based multi-agent system to satisfy a free-form `GOALS.md` in a git repository. See `README.md` for the design.

## Style

Same as gorn — see `STYLE.md` (copied verbatim). `Throw`/`Try` error flow, blank lines around control blocks, no `if err != nil { return err }` pass-through.

Layout is flat — all `.go` files in repo root.

## Conventions

- Git author: `claude <claude@users.noreply.github.com>`. English commit messages.
- Single-line comments only.
- Workspaces never deleted — RO after work in them ends.

## `run` architecture (coordinator + pools)

A single coordinate goroutine (`scheduler.go`) owns ALL ticket state — no locks. Role pools (`pool.go`) are stateless harness workers: they read a `Job` the coordinator hands them on `o.jobs[role]`, run the harness to a recognized verdict, and reply with an `AgentResult` on `o.Events`. Agents never touch ticket state; they only message the coordinator.

- **Persisted ticket state is `Phase`** (`PLAN`/`IMPLEMENT`/`REVIEW`/`MERGE`/`ARBITRATE`/`ESCALATE` + terminal `PLANNED`/`CONSUMED`/`MERGED`/`DISCARDED`) — written to `tasks.events.jsonl` as `phase` events, the single source of truth. The coordinator advances Phase on each verdict. A restart replays phases and resumes mid-pipeline. A plan ticket goes `PLANNED` (plan.md written) → `CONSUMED` (the lead read it once); dependents still read its plan.md off disk regardless of phase.
- **`shadow` (in-memory, coordinator-only)** = `STOPPED` (dispatchable) / `SCHEDULED` (handed to a pool). The dispatch loop routes every STOPPED non-terminal ticket to `roleForPhase(phase)` and marks it SCHEDULED; the result returns it to STOPPED.
- **Lead** is the single planning + steering authority (there is no overseer). The coordinator feeds it STOPPED `ESCALATE` tickets + accumulated global nudges (post-merge fallout, GOALS.md change), batched into one Job, and tags the pass with a **subagent context** (`wantReplan`, highest urgency wins): `start_project` (empty DB → seed), `end_project` (open queue drained → judge goals, maybe `GOALS_ACHIEVED`), `algedonic` (digger emergency cord → full root-cause re-scope), or `replan` (routine). The context selects the prompt via `text/template` (`renderPrompt`, generic `map[string]string` params — add a key to extend a prompt, no signature threading). Completed plans reach the lead (`{{.Plans}}` = all `PLANNED` plan.md) and the digger/reviewer (`{{.Plans}}` = their dep plans); a plan the lead is shown flips `PLANNED` → `CONSUMED` so it isn't re-fed. After a pass it returns owned ESCALATE tickets to their entry phase. Every agent run's full prompt is appended to its ticket `log.md` (fenced, stripped on re-ingest) for auditing what reached the agent.
- **Concurrency = fixed per-role pool sizes** (`poolSizes` in `types.go`); no shared semaphore. `merger`/`lead` are size 1 (serial); tune `digger` for implementation parallelism. Trunk is written only by the coordinator (the merger ff-merge).
- **Simulator** (`simulate.go`, `run --sim` / `--sim=N`): a global flag swaps real harness runs for synthesized weighted verdicts (and stubs workspaces/trunk git) to stress the state machine with no tokens. `--sim=N` caps the project at N tickets so the run winds down and exercises the stop path.
