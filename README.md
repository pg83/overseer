# overseer

Orchestrator for a Claude-Code-based multi-agent coding system. Given a git repo with a `GOALS.md`, runs roles (replanner, tasker, digger, reviewer, merger, overseer) until the goals are met.

## Build

```sh
go build ./...
```

## Run

```sh
overseer --root /path/to/state --trunk /path/to/git/repo
```

`--root` holds:
- `TASKS.md` — ticket DB
- `tickets/T-<N>/{plan.md,log.md}` — per-ticket artifacts
- `workspaces/<ws-id>/` — git worktrees (RO after use)
- `prompts/<role>.txt` — agent prompts (defaults baked in)
- `REPORT.md` — written on `GOALS_ACHIEVED`

`--trunk` is the working tree being modified. Branches `ovs/<ws-id>` are created for each agent attempt.

## Roles

- **replanner** (1 per repo) — owns `TASKS.md`. Receives REPLAN signals; outputs new task DB or CANCEL directives. Output validated by orchestrator before applying.
- **tasker** (1 per ticket) — extends a ticket into a `plan.md` after research.
- **digger** (1 per ticket) — implements the plan in a workspace.
- **reviewer** (1 per ticket) — reviews digger's output in same workspace; can APPROVE / REWORK / DISCARD.
- **merger** (1 per repo) — serially merges approved workspaces into trunk; falls back to digger on failure.
- **overseer** (1 per repo) — checks goal completion when queue is small.

## Ticket schema

See `prompts/replanner.txt`. Required: `N STATE DESCR PRIO`. Optional: `DEPS WORKSPACES CLOSE_REASON`. Tickets separated by `---`.

## Communication

Out-of-band signal `REPLAN: <text>` in any agent's stdout enqueues the replanner. `CANCEL: <N>` from replanner cancels a live ticket. Final line of any agent output is `VERDICT: <code>[: <detail>]`.

## Termination

Overseer's `VERDICT: GOALS_ACHIEVED` writes `REPORT.md` and exits.

## State

Ephemeral. On restart: load `TASKS.md`, re-spawn for every OPEN ticket whose deps are satisfied, with fresh workspaces. Old workspaces stay as RO archives.
