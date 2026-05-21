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

- **Persisted ticket state is `Phase`** (`PLAN`/`IMPLEMENT`/`REVIEW`/`MERGE`/`ARBITRATE`/`ESCALATE` + terminal `MERGED`/`DISCARDED`) — written to `tasks.events.jsonl` as `phase` events, the single source of truth. The coordinator advances Phase on each verdict. A restart replays phases and resumes mid-pipeline.
- **`shadow` (in-memory, coordinator-only)** = `STOPPED` (dispatchable) / `SCHEDULED` (handed to a pool). The dispatch loop routes every STOPPED non-terminal ticket to `roleForPhase(phase)` and marks it SCHEDULED; the result returns it to STOPPED.
- **Replanner** is fed by the coordinator only: STOPPED `ESCALATE` tickets + accumulated global nudges (overseer guidance, post-merge fallout, GOALS.md change), batched into one Job. After a pass it returns owned ESCALATE tickets to `PLAN`. **Overseer** is global/serial, triggered on boot + low open-count.
- **Concurrency = fixed per-role pool sizes** (`poolSizes` in `types.go`); no shared semaphore. `merger`/`replanner`/`overseer` are size 1 (serial); tune `digger` for implementation parallelism. Trunk is written only by the coordinator (the merger ff-merge).
