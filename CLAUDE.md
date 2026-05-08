# overseer — context for Claude

Go orchestrator that drives a Claude Code based multi-agent system to satisfy a free-form `GOALS.md` in a git repository. See `README.md` for the design.

## Style

Same as gorn — see `STYLE.md` (copied verbatim). `Throw`/`Try` error flow, blank lines around control blocks, no `if err != nil { return err }` pass-through.

Layout is flat — all `.go` files in repo root.

## Conventions

- Git author: `claude <claude@users.noreply.github.com>`. English commit messages.
- Single-line comments only.
- Single-writer for `TASKS.md` (the orchestrator main loop).
- Workspaces never deleted — RO after work in them ends.
- Ticket states on disk: only `OPEN` / `CLOSED`. Per-ticket in-flight progress is ephemeral memory.
- All `claude -p` invocations share one global semaphore (size 6). Merger queue throttle (>4) blocks new TASKER/DIGGER spawns but not in-flight reworks.
