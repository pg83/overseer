# overseer

Orchestrator for Claude-Code-style multi-agent coding sessions. One binary, two subcommands:

- `overseer run` — full orchestrator. Given a git repo with a `GOALS.md`, runs roles (replanner, tasker, digger, reviewer, merger, overseer, arbiter) until the goals are met.
- `overseer plan` — two-agent debate (Pupa ↔ Lupa) over a question on stdin. Pupa proposes, Lupa critiques, they iterate until Lupa accepts.

## Build

```sh
go build ./...
```

## `overseer run` — full orchestrator

```sh
overseer run --root /path/to/state --trunk /path/to/git/repo --harness <bin>[:<model>] [overrides...]
```

`--root` holds:
- `tasks.events.jsonl` — append-only ticket event log (this IS the database; legacy `tasks.jsonl` is migrated on first run)
- `tickets/T-<N>/{plan.md,log.md}` — per-ticket artifacts
- `workspaces/<ws-id>/` — local clones of trunk (RO after use, never deleted)
- `runs/T-<N>-<ts>-<role>-<ws>.jsonl` — per-agent-run streams (start/stdin/harness/stderr/finish)
- `messages.txt` — team-wide chat (every `{"type":"message"}` event from every role)
- `REPORT.md` — written on `GOALS_ACHIEVED`

`--trunk` is the git working tree being modified. The orchestrator creates branches `ovs/<ws-id>` per workspace and ff-merges approved work back into trunk. **No other operation writes to trunk** — see `workspace.go::FfMergeBranch` for why `git pull` is avoided.

### Harness binding flags

The default harness is required (`--harness`). Per-role and per-group overrides layer on top. Resolution precedence (highest wins):

1. `--<role>-harness` — `--tasker-harness`, `--digger-harness`, `--reviewer-harness`, `--merger-harness`, `--replanner-harness`, `--overseer-harness`, `--arbiter-harness`
2. `--think-harness` (covers tasker / replanner / overseer) or `--work-harness` (covers digger / reviewer / merger / arbiter)
3. `--harness` (the default)

Each flag takes `<bin>` or `<bin>:<model>`. The binary path is PATH-resolved; the harness implementation is picked by basename (must contain `claude`, `opencode`, `codex`, or `gemini`).

### Jail (`--jail-bin`)

Optional sandboxing binary. When set, the orchestrator wraps every harness invocation as `<jail> --rw=<ws> --rw=<ws>/.tmp [--rw=...harness-specific-paths] -- <harness> <args>`. Each `Harness.JailRWPaths(home)` declares its own minimum RW set (`~/.claude`, `~/.codex`, etc.); the workspace and tmpdir are appended automatically.

If `--jail-bin` is empty, the harness runs directly — no sandbox.

### Roles

| Role | Count | Job | Verdict |
|------|-------|-----|---------|
| **tasker** | 1 per ticket | Research codebase, write `plan.md` for a digger | emits `{"type":"plan","path":"..."}` |
| **digger** | 1 per ticket | Implement plan, squash + rebase onto trunk | `READY` / `CANT_DO` |
| **reviewer** | 1 per ticket | Find what's broken in digger's output | `APPROVE` / `REWORK` / `DISCARD` |
| **merger** | 1 globally, serial | Test-baseline → `git merge --no-ff` → re-test → ff-merge into trunk | `MERGED` / `MERGE_FAIL` |
| **replanner** | 1 per repo | Rewrite the ticket DB on REPLAN nudges. Owns `task` ops (`new` / `update` / `cancel`) | emits `task` events; orchestrator validates and applies |
| **overseer** | 1 per repo | Check goal completion when queue is small | `GOALS_ACHIEVED` (terminates run) or replan nudges |
| **arbiter** | per disagreement | Cycle-internal escalation gate (REWORK / DISCARD / MERGE_FAIL / NO_PLAN) | `CONTINUE` (respawn same role) / `ESCALATE` (queue replanner) |

### Concurrency

- One global semaphore size 6 for all harness invocations (`AgentSem` in `scheduler.go`).
- Merger queue is serial; new tasker / digger spawns are throttled when `len(QMerger) > 4`.
- Replanner / merger / overseer / arbiter each have their own queue and a dedicated loop goroutine.

### Tickets

```json
{"n": 12, "state": "OPEN", "descr": "...", "prio": 7, "deps": [3, 8]}
```

- `state ∈ {OPEN, CLOSED}` on disk; `InProgress` is in-memory only.
- `CloseReason ∈ {MERGED, DISCARDED}`. `MERGED` is the "work landed" path; `DISCARDED` is every other terminal close (cancelled, repeatedly bounced, etc.).
- `prio ∈ [1, 10]`. Ready-queue sorted by `prio DESC, n ASC`.
- An OPEN ticket may not depend on a DISCARDED prerequisite — `ValidateTasks` enforces this on every replanner-batch.

### Communication protocol

Every agent's stdout is parsed as a JSON-line event stream by `parseEvents` in `agent.go` — tolerant of prose preamble, code fences, and pretty-printed multi-line JSON via a string-aware brace matcher. Universal events:

```json
{"type": "verdict", "verdict": "READY", "detail": "..."}
{"type": "message", "text": "team chat line"}
{"type": "replan", "reason": "why the task DB needs adjustment"}
```

The **last** `verdict` event is authoritative. `message` events post to `messages.txt`. `replan` events queue the replanner.

### Termination

`overseer run` exits when the overseer agent emits `GOALS_ACHIEVED` — it writes `REPORT.md` and cancels the run context. SIGINT/SIGTERM also cause clean shutdown.

## `overseer plan` — Pupa & Lupa

A synchronous two-agent debate. Read a question from stdin; iterate Pupa (solver) ↔ Lupa (critic) until Lupa accepts.

```sh
echo "should we switch the queue prefix from /gorn/queue_v3/ to /gorn/queue_v4/ to add the X field?" \
    | overseer plan \
        --pupa-harness /usr/local/bin/claude:opus \
        --lupa-harness /usr/local/bin/codex:gpt-5 \
        --out plan.md \
        --max-rounds 10
```

### Protocol

Pupa's reply ends with one JSON line:

```json
{"plan_num": N}
```

…where `N` is an integer Pupa picks to label this version of the plan. If Pupa isn't proposing (asking Lupa for clarification, partial thinking) — no marker, just text.

Lupa either accepts with:

```json
{"accept_plan": N}
```

…or writes critique that Pupa receives on the next turn. Once accept arrives, the dialog ends.

No validation on `N` — `accept_plan` from Lupa terminates the loop with whatever N was supplied. If `N` matches a plan Pupa emitted earlier, the text of that plan is what `--out` captures; otherwise the most recent Pupa text is used.

### Sessions

Each harness keeps a single session for the whole dialog — Pupa sees the original question once, then only Lupa's critiques; Lupa sees the original question + first Pupa reply once, then only subsequent Pupa replies. Internally:

- First turn: `<harness>` invoked plain; orchestrator captures `session_id` from the stream.
- Subsequent turns: `<harness>` invoked with the captured id (`--resume <id>` for claude; `exec resume <id>` for codex).

`SupportsSession()` reports per-harness:

| Harness | Sessions |
|---------|----------|
| claude  | ✓ (via `--resume <uuid>`) |
| codex   | ✓ (via `codex exec resume <id>`) |
| opencode | ✗ (stub; flag set not wired) |
| gemini  | ✗ (stub; non-interactive resume not wired) |

`overseer plan` refuses to bind a non-supporting harness as Pupa or Lupa.

### Flags

| Flag | Required | Default | Meaning |
|------|----------|---------|---------|
| `--pupa-harness <bin>[:<model>]` | yes | — | Solver harness |
| `--lupa-harness <bin>[:<model>]` | yes | — | Critic harness |
| `--jail-bin <bin>` | no | (none) | Same jail wrapper as `run`; `--rw=<cwd>` + per-harness paths |
| `--out <path>` | no | (none) | Write final Pupa text (no marker line) here |
| `--max-rounds <n>` | no | `0` | Cap at N (Pupa + Lupa) rounds; `0` = no cap |

### Workspace

The current working directory (`$PWD`) is the workspace — both agents see it through `cmd.Dir` and the harness-specific cwd flag (`--cd` / `--dir` / `--include-directories`). They can `Read`, `Bash`, `Grep`, etc. against whatever files are around. No clone, no branch.

### Output

- **stderr** — live progress. After each completed turn, a block of the form:
  ```
  ============================ PUPA #1 ============================
  <full text including the {"plan_num":N} marker>
  ```
  Tool-trace events emitted by the harness during a turn flow to stderr inline (claude `tool_use` blocks → `Read foo.go`, codex `exec_command_begin` → `exec ls -la`, etc.).
- **stdout** — empty.
- **`--out` file** — final Pupa text, marker line stripped. Trailing newline appended.

### Retry & faults

Each turn classifies non-zero harness exits through `Harness.ClassifyFault`. Anthropic rate limits, OpenAI quota messages, generic transient HTTP/TCP patterns → retry with exponential backoff (5s → 60s). Unclassified faults → exit 1 with the harness stderr surfaced.

Session id is captured per harness on the first successful turn; retries before that first capture re-run plain (no `--resume`), so a transient on the very first call doesn't leave the agent stuck on a half-created session.

## Layout & style

Flat: all `.go` files in the repo root. No `internal/`, no `cmd/`, no `pkg/`.

`Throw`/`Try` for error handling (`throw.go`); no `if err != nil { return err }` pass-through. Blank lines around control blocks. JSON-only config — never YAML. See [`STYLE.md`](STYLE.md) for the full rules.

## See also

- [`CLAUDE.md`](CLAUDE.md) — brief context for Claude Code sessions.
- [`STYLE.md`](STYLE.md) — code style and error-handling rules (shared with `gorn`).
