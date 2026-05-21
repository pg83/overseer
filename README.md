# overseer

A small Go framework for describing multi-agent dialogs over heterogeneous CLI agent harnesses (claude-code, codex, opencode, gemini), with a built-in user+mount namespace sandbox so every agent invocation runs read-only against the host except for an explicit allow-list.

The framework itself is one file (`harness.go`) declaring the `Harness` interface. Each backend implements it in one file (`claude.go`, `codex.go`, `opencode.go`, `gemini.go`). Each *orchestrator* — a recipe for how agents talk to each other — is another file that consumes `Harness` generically. Two orchestrators ship today, plus the sandbox primitive:

- `overseer run` — full code-modification orchestrator. A single coordinator goroutine drives a git working tree toward `GOALS.md` using seven roles (replanner / tasker / digger / reviewer / merger / overseer / arbiter), each served by a fixed-size pool of harness workers.
- `overseer plan` — synchronous two-agent debate. PUPA (solver) and LUPA (critic) iterate over a free-form question on stdin until LUPA accepts. Output: prose plan / forecast / analysis / code — whatever the question called for.
- `overseer jail` — the sandbox itself. Used implicitly by `run` and `plan`; also callable standalone like `overseer jail --rw=/tmp -- <cmd>`.

Adding a new orchestrator = write another `.go` against `Harness`. Adding a new backend = implement `Harness` in one more file; `SelectHarness` in `harness.go` picks the impl by basename of the binary.

## Build

```sh
go build ./...
```

Self-contained binary. Prompts in `prompts/*.txt` are baked in via `embed.FS`; no runtime files outside the working directory.

## Core concepts

### The `Harness` interface (`harness.go`)

Each backend declares:

| Method | What it returns |
|--------|-----------------|
| `Name()`, `Bin()` | identity + absolute binary path |
| `Args(model, wsAbs)` | CLI args for a fresh single-shot invocation |
| `SessionArgs(model, wsAbs, sessionID)` | CLI args for a resumed dialog (when supported) |
| `SupportsSession()` | does the backend have multi-turn resume? |
| `ParseSessionID(ev)` | extract session id from one stream event |
| `JailRWPaths(home)` | filesystem paths this backend needs RW (config dirs, caches) |
| `DefaultModel(role)` | per-role model preference, `""` = backend default |
| `ParseStreamLine(ev, finalText, fault, role, ticket)` | JSONL stream parser — accumulate text, record stream errors, emit UI tool traces |
| `LiveTextChunk(ev)` | text fragment for live stderr streaming (used by `plan` so cold thinking doesn't look like a hang) |
| `ClassifyFault(f)` | decide whether a failed invocation is retryable (rate limit / transient HTTP) or fatal |

### Backends shipped

| Backend | Bin basename match | Sessions | Notes |
|---------|--------------------|----------|-------|
| claude-code | contains `claude` | ✓ (`--resume <uuid>`) | stream-json output |
| codex | contains `codex` | ✓ (`codex exec resume <id>`) | 0.130+ ThreadEvent schema |
| opencode | contains `opencode` | ✗ | session-resume flags not wired yet |
| gemini | contains `gemini` | ✗ | non-interactive resume not yet provided by gemini-cli |

`overseer plan` refuses to bind a non-session backend as PUPA or LUPA. `overseer run` doesn't care — each agent run there is its own fresh context.

### Jail (`jail.go`)

Built-in Linux user+mount namespace sandbox. Default for both `run` and `plan`. Three modes (resolved by `main.go::resolveJail`):

- **default** — `--jail-bin` not set: orchestrator wraps every harness invocation in `<self> jail …` (the same binary recurses into the `jail` subcommand).
- **`--jail-bin /path/to/external`** — use an external sandbox binary with the same CLI shape (`<bin> [--rw=PATH]... -- CMD [ARGS...]`). PATH-resolved.
- **`--no-jail`** — direct exec, no sandbox. Trusted environments only.

The built-in implements two re-execs (`top → --__stage=mount → --__stage=drop`) using `SysProcAttr.Cloneflags = CLONE_NEWUSER|CLONE_NEWNS`, because the Go runtime is multi-threaded almost from `main()` and per-thread `unshare(2)` would only affect the calling thread. The kernel applies the cloneflags atomically at `clone(2)` time, so the child is born in fresh namespaces with one thread. The drop stage ends in `syscall.Exec(...)` so the harness inherits the PID and stdio directly — no wrapper Go process between init and the agent.

The RW set is composed at call time:

- `run`: workspace → workspace's `.tmp` → host `$TMPDIR` (if set) → `Harness.JailRWPaths(HOME)` → `--rw` flags
- `plan`: `cwd` → `Harness.JailRWPaths(HOME)` → host `$TMPDIR` (if set) → `--rw` flags

Everything else is bind-mounted read-only.

---

## `overseer run` — full orchestrator

A small-software-team emulator. You point it at a git working tree and a free-form `GOALS.md`, and it runs a continuous loop of *plan → implement → review → merge → verify* across a queue of tickets — using LLM agents in each role — until the goals are met. The orchestrator itself doesn't write code or call the API: a single **coordinator** owns all ticket state and routes work; stateless **pool workers** run the role-shaped agent subprocesses and report JSON-event verdicts back.

The mental model is a stripped-down engineering team:

| Agent role | Human analogue | What it does |
|------------|----------------|--------------|
| **overseer** | product owner / QA lead | Reads `GOALS.md`, decides whether the project is done. When done → emits `GOALS_ACHIEVED`, run terminates and writes `REPORT.md`. Otherwise nudges the replanner. |
| **replanner** | planning lead | Owns the ticket database. Reads goals + ticket history + recent agent runs, emits `task` operations (`new` / `update` / `replace` / `cancel`) that the orchestrator validates and applies. Invoked on every `replan` nudge from any role. |
| **tasker** | senior engineer writing specs | Gets a ticket in phase `PLAN`, researches the codebase, writes `plan.md` for it — the work order for the digger. |
| **digger** | implementer | Reads `plan.md`, makes the changes in a per-ticket workspace, squashes + rebases onto trunk. Reports `READY` (work done) or `CANT_DO` (can't, here's why). |
| **reviewer** | code reviewer | Independently audits the digger's branch. Reports `APPROVE`, `REWORK` (needs revision), or `DISCARD` (kill the ticket). |
| **merger** | release engineer | Serial (pool size 1). Runs tests on trunk (baseline), `git merge --no-ff` the digger's branch into a scratch worktree, re-runs tests. Reports `MERGED` or `MERGE_FAIL`; on `MERGED` the **coordinator** ff-merges that scratch branch into real trunk (the one place trunk is written). |
| **arbiter** | tech lead breaking ties | Invoked on every disagreement (`REWORK` / `DISCARD` / `MERGE_FAIL` / `NO_PLAN`). Decides `CONTINUE` (keep iterating with the same role) or `ESCALATE` (kick to the replanner). |

### Architecture: one coordinator, role pools

A single **coordinator** goroutine (`scheduler.go`) owns *all* ticket state — there are no locks. It reads agent results off one channel and, per result, (1) appends the phase transition to `tasks.events.jsonl`, (2) updates its in-memory ticket view, (3) updates an in-memory **shadow** scheduling map, then (4) dispatches every dispatchable ticket to the pool its phase calls for. Stateless **pool workers** (`pool.go`, fixed size per role) are the only place a harness is invoked; they read a `Job`, run the agent to a recognized verdict, and reply on the coordinator's channel. Agents never touch ticket state — they only message the coordinator.

Two state axes:

- **`Phase`** (persisted) — what work a ticket needs next: `PLAN` → tasker, `IMPLEMENT` → digger, `REVIEW` → reviewer, `MERGE` → merger, `ARBITRATE` → arbiter, `ESCALATE` → replanner, plus terminal `MERGED` / `DISCARDED`. Written to the event log as `phase` events; a restart replays them and resumes mid-pipeline.
- **`shadow`** (in-memory, coordinator-only) — `STOPPED` (dispatchable) or `SCHEDULED` (handed to a pool). Dispatch routes every `STOPPED` non-terminal ticket to `roleForPhase(phase)` and flips it to `SCHEDULED`; the returning result flips it back to `STOPPED`. That two-line rule *is* the scheduler.

### Pipeline: from a goal to a merged commit

The loop is **continuous and parallel**, not sequential. Multiple tickets are in flight at once (bounded by the per-role pool sizes); the pipeline below is one ticket's path through the system — really a walk through its `Phase` values, driven by the coordinator.

```
                                            GOALS.md  (operator's input)
                                                │
                                                ▼
                                  ┌──────────  overseer  ──────────┐
                                  │  not done — kick replanner     │
                                  │  done     — emit GOALS_ACHIEVED│
                                  └────────────┬───────────────────┘
                                               │
                                               ▼
                                            replanner
                                       (rewrites ticket DB
                                        with new/update/replace/cancel
                                        operations)
                                               │
                                               ▼
                                   ┌─ STOPPED tickets, deps satisfied ┐
                                   │   (dispatched by phase, sorted   │
                                   │    by ticket number n ASC)       │
                                   └────────────┬─────────────────────┘
                                                │  coordinator dispatches by phase
                                                ▼
                              ┌─ phase PLAN ────┐    ┌─ phase IMPLEMENT ┐
                              │      tasker      │    │   (plan exists)  │
                              │  writes plan.md  │    └──────────────────┘
                              └────────┬─────────┘            │
                                       │ {"type":"plan",...}  │
                                       └──────────┬───────────┘
                                                  ▼
                                              digger
                                       (implements plan in
                                         workspaces/<ws-id>/,
                                         branch ovs/<ws-id>)
                                                  │
                                       READY / CANT_DO
                                                  │
                              ┌───────────────────┴─────────────┐
                              │ CANT_DO                         │ READY
                              ▼                                 ▼
                          arbiter                            reviewer
                          (REWORK/                       APPROVE/REWORK/DISCARD
                           DISCARD                            │
                           input)                ┌────────────┼──────────────┐
                                                 │ APPROVE   │ REWORK       │ DISCARD
                                                 ▼            ▼              ▼
                                               merger      arbiter        arbiter
                                          (test, merge,    │               │
                                          re-test, ff)     ▼               ▼
                                                 │      CONTINUE       CONTINUE
                                       MERGED / MERGE_FAIL  ▶ next         ▶ next
                                                 │           digger        digger
                                       ┌─────────┴──────────┐ iter         iter (or
                                       │ MERGED             │             ESCALATE)
                                       ▼                    │
                                  phase MERGED              │ MERGE_FAIL
                                  (terminal; coordinator    ▼
                                   ff-merges into trunk)  arbiter ──▶ CONTINUE digger
                                       │                          (with rebase target +
                                       │                           merge-fail output)
                                       │                     or ESCALATE replanner
                                       ▼
                              overseer re-evaluates
                              ("open queue small? are we done?")
```

Step by step, on one ticket:

1. **Boot.** The coordinator loads `tasks.events.jsonl` (the ticket DB), marks every non-terminal ticket `STOPPED`, and triggers one overseer evaluation: «re-evaluate goals and seed plan if needed».
2. **Overseer evaluates.** Reads `GOALS.md`, ticket DB, recent agent runs. If goals aren't met → emits one or more `replan` events with reasons (the coordinator accumulates them as nudges). If met → `GOALS_ACHIEVED`, the run writes `REPORT.md` and shuts down.
3. **Replanner reshapes the DB.** The coordinator hands the (serial) replanner one batched job covering all `ESCALATE` tickets + pending nudges. It emits `task` events; the coordinator validates them (no non-terminal→DISCARDED deps, no cycles), appends to `tasks.events.jsonl`, updates in-memory tickets. New tickets start in phase `PLAN`.
4. **Dispatch picks the ticket up.** Every `STOPPED` non-terminal ticket whose deps are all terminal is routed to `roleForPhase(phase)`, sorted by ticket number `n ASC`, and flipped to `SCHEDULED`. A fresh ticket is phase `PLAN` → tasker.
5. **Tasker writes the spec.** A tasker worker researches the codebase in a fresh workspace, writes `tickets/T-<N>/plan.md`, emits `{"type":"plan","path":"..."}`. The coordinator advances the ticket to phase `IMPLEMENT`.
6. **Digger implements.** A digger worker works in `workspaces/<ws-id>/` (a clone of trunk on branch `ovs/<ws-id>`, recorded as the ticket's branch workspace). On `READY` (clean rebase-able branch ahead of trunk) → phase `REVIEW`. On `CANT_DO` → phase `ARBITRATE`.
7. **Reviewer audits.** A reviewer worker independently audits the same branch workspace. `APPROVE` → phase `MERGE`. `REWORK` / `DISCARD` → phase `ARBITRATE`.
8. **Merger lands the change.** A single merger worker (pool size 1) runs the project's tests on trunk as a baseline; if baseline is red it declines (no landing into a broken trunk). Then `git merge --no-ff` into a scratch worktree, re-runs tests. On green → `MERGED`, and **the coordinator** ff-merges the scratch branch into real trunk (the only writer of trunk); ticket → terminal `MERGED`. On red / conflict, or a failed ff-merge → phase `ARBITRATE` with `RebaseTarget` (new trunk HEAD) + `MergeOut` so the next digger pass can rebase.
9. **Arbiter resolves disagreements.** Runs for every `ARBITRATE` ticket — trigger was REWORK, DISCARD, MERGE_FAIL, CANT_DO, or NO_PLAN. `CONTINUE` → back to `IMPLEMENT` (or `PLAN` for NO_PLAN); `ESCALATE` → phase `ESCALATE`, which the coordinator feeds to the replanner. After a replanner pass, an escalated ticket returns to `PLAN`.
10. **Overseer re-evaluates** on boot and whenever the open (non-terminal) count drops to ≤ 2, asking "are the goals met now?". When yes → terminates the run.

Any agent can emit `{"type":"replan","reason":"..."}` mid-run to signal «the ticket DB itself looks wrong, not just this one task» — the coordinator collects these as nudges and folds them into the next replanner batch, without short-circuiting the current role's verdict.

### Workspaces

Each ticket gets a dedicated workspace under `<root>/workspaces/<ws-id>/` — a fresh local clone of `--trunk` on a branch named `ovs/<ws-id>`. Workspaces are **never deleted**: once work in them ends (MERGED or DISCARDED), they go read-only and stay there as forensic record. Successive digger iterations on the same ticket reuse the same workspace, accumulating commits that get squashed before merge.

`--trunk` itself is written in exactly one place: the coordinator's ff-merge of a green merger scratch branch (`scheduler.go::onMerger` → `workspace.go::FfMergeBranch`). Single-writer by construction — the coordinator is one goroutine. No other operation touches trunk; see `FfMergeBranch` for why even `git pull` is avoided.

### State layout under `--root`

| Path | Role | Notes |
|------|------|-------|
| `tasks.events.jsonl` | ticket database | Append-only event log (`create` / `phase` / `update` / `event` / `ws`). `phase` events are the persisted ticket state. Legacy `tasks.jsonl` is migrated on first run. |
| `tickets/T-<N>/plan.md` | per-ticket spec | Written by tasker, consumed by digger. |
| `tickets/T-<N>/log.md` | per-ticket history | Human-readable trace of phase transitions. |
| `workspaces/<ws-id>/` | per-ticket git clone | Branch `ovs/<ws-id>`. RO after terminal close. |
| `runs/T-<N>-<ts>-<role>-<ws>.jsonl` | per-invocation stream | start / stdin / harness events / stderr / finish. The forensic record. |
| `messages.txt` | team chat | Every `{"type":"message","text":...}` from any agent, appended in real time. Readable by humans and replayed in the next agent's PRIOR_RUNS context. |
| `REPORT.md` | done marker | Written by overseer on `GOALS_ACHIEVED`. |

### Communication protocol

Every agent's stdout is parsed as a JSON-line event stream by `parseEvents` in `agent.go` — tolerant of prose preamble, code fences, and pretty-printed multi-line JSON via a string-aware brace matcher. Universal events:

```json
{"type": "verdict", "verdict": "READY", "detail": "..."}
{"type": "message", "text": "team chat line — visible to other roles"}
{"type": "replan", "reason": "why the task DB needs adjustment"}
```

Role-specific events on top: tasker emits `{"type":"plan","path":"..."}`; replanner emits `{"type":"task","op":"new",...}` / `{"type":"task","op":"update","deps":[...]}` / `{"type":"task","op":"replace","from":...,"to":...}` / `{"type":"task","op":"cancel",...}`.

The **last** `verdict` event is authoritative — agents sometimes emit multiple. `message` events post to `messages.txt` and surface in the UI. `replan` events become coordinator nudges regardless of which role they came from.

### Concurrency

- **Fixed per-role worker pools** (`poolSizes` in `types.go`) cap parallelism — total harness concurrency is their sum, *not* a shared semaphore. Defaults: digger 4, tasker / reviewer / arbiter 2, merger / replanner / overseer 1. Tune `digger` to control implementation parallelism (and, transitively, how fast the merge queue fills).
- **Merger / replanner / overseer are serial** (pool size 1). The merger because tests + the trunk ff-merge must not race; the replanner because it rewrites the shared ticket DB; the overseer because it's a single global decision.
- **One coordinator, no locks.** All ticket state (`Phase`, `shadow`, branch workspaces, arbiter context, pending nudges) is owned by the single `scheduler.go` coordinate goroutine. Pools communicate with it only by channel (`Job` in, `AgentResult` out), so there is nothing to lock.

### Tickets

```json
{"n": 12, "phase": "REVIEW", "descr": "...", "deps": [3, 8]}
```

- **`Phase`** is the persisted state: `PLAN` / `IMPLEMENT` / `REVIEW` / `MERGE` / `ARBITRATE` / `ESCALATE`, plus terminal `MERGED` (work landed) and `DISCARDED` (every other terminal close — cancelled by replanner, repeatedly bounced, reviewer-killed, etc.). The in-memory `shadow` (`STOPPED` / `SCHEDULED`) is the only ephemeral state and never persisted.
- Dispatch order: ticket number `n ASC`.
- A non-terminal ticket may not depend on a `DISCARDED` prerequisite — `ValidateTasks` enforces this on every replanner batch. A dependency is "satisfied" once the dep reaches a terminal phase.

### Example invocation

Minimum — one harness for everything, built-in jail, defaults:

```sh
overseer run \
    --root  /var/run/overseer/proj-x \
    --trunk /home/me/proj-x \
    --harness /usr/local/bin/claude:opus
```

With per-role specialization (thinking roles on codex, work roles on cheaper claude, merger on a bigger model, extra RW path inside the jail):

```sh
overseer run \
    --root  /var/run/overseer/proj-x \
    --trunk /home/me/proj-x \
    --harness          /usr/local/bin/claude \
    --think-harness    /usr/local/bin/codex:gpt-5 \
    --work-harness     /usr/local/bin/claude:sonnet \
    --merger-harness   /usr/local/bin/claude:opus \
    --rw=/etc/ssl/certs
```

The orchestrator runs in the foreground, streaming a structured log to stderr (BOOT / EXEC / verdict transitions / inter-role chat) until either `GOALS_ACHIEVED` arrives or you send SIGINT / SIGTERM. To resume after an interrupt, run the same command again with the same `--root` — ticket state and workspaces persist; the run boots, overseer re-evaluates, and the pipeline picks up where it left off.

### Harness binding flags

The default harness is required (`--harness`). Per-role and per-group overrides layer on top. Resolution precedence (highest wins) — see `agent.go::harnessModelForRole`:

1. `--<role>-harness` — `--tasker-harness`, `--digger-harness`, `--reviewer-harness`, `--merger-harness`, `--replanner-harness`, `--overseer-harness`, `--arbiter-harness`
2. `--think-harness` (covers tasker / replanner / overseer) or `--work-harness` (covers digger / reviewer / merger / arbiter)
3. `--harness` (the default)

Each flag takes `<bin>` or `<bin>:<model>`. The binary path is PATH-resolved; the harness implementation is picked by basename (must contain `claude`, `opencode`, `codex`, or `gemini`).

### Sandbox flags

| Flag | Meaning |
|------|---------|
| `--jail-bin <bin>` | External jail binary, PATH-resolved. Same CLI shape as built-in. |
| `--no-jail` | Run harnesses directly, no sandbox. Trusted environments only. |
| `--rw <PATH>` | Extra path to bind read-write inside the jail. Repeatable. Stacks on top of workspace / harness defaults / per-task TMPDIR. Ignored with `--no-jail`. |

If neither `--jail-bin` nor `--no-jail` is given, the built-in `overseer jail` is used (`os.Executable() + " jail"`).

### Termination

`overseer run` exits when the overseer agent emits `GOALS_ACHIEVED` — it writes `REPORT.md` and cancels the run context. `SIGINT` / `SIGTERM` also cause clean shutdown. Workspaces and tickets persist across restarts; re-running with the same `--root` resumes the pipeline.

---

## `overseer plan` — Pupa & Lupa

A synchronous two-agent debate. Read a question from stdin; iterate PUPA (solver) ↔ LUPA (critic) until LUPA accepts. The deliverable is whatever the question called for: a plan, a forecast, an analysis, a recommendation, a written answer, a piece of code. Not constrained to "plan" shape.

### Example

```sh
echo "should we switch the queue prefix from /gorn/queue_v3/ to /gorn/queue_v4/ to add the X field?" \
    | overseer plan \
        --pupa-harness /usr/local/bin/claude:opus \
        --lupa-harness /usr/local/bin/codex:gpt-5 \
        --out plan.md \
        --max-rounds 10
```

Or with content in a file:

```sh
overseer plan \
    --pupa-harness claude:opus \
    --lupa-harness codex:gpt-5 \
    --out result.md \
    < REQ.md
```

### Protocol

PUPA's reply ends with one JSON line when it has a concrete proposal to make:

```json
{"result_num": N}
```

`N` is an integer PUPA picks to label this version of the result. Bumps on each revision. If PUPA is **not yet proposing** — asking LUPA clarifying questions, narrowing scope, agreeing on the shape of the deliverable — no marker, just text. The first few rounds are usually framing-alignment.

LUPA either accepts:

```json
{"accept_result": N}
```

…or writes critique that PUPA receives on the next turn. Once accept arrives, the dialog ends. While PUPA hasn't emitted a `result_num` yet, LUPA behaves as a collaborator — answers framing questions, pushes back on scope, suggests deliverable shape. Once a concrete `result_num` arrives, LUPA flips to critic mode: "this is broken until I prove otherwise."

No validation on `N` — `accept_result` from LUPA terminates the loop with whatever N was supplied. If `N` matches a result PUPA emitted earlier, the text of that result is what `--out` captures; otherwise the most recent PUPA text is used (and a warning is printed to stderr).

### Sessions

Each harness keeps a single session for the whole dialog — PUPA sees the original question once, then only LUPA's critiques; LUPA sees the original question + first PUPA reply once, then only subsequent PUPA replies. Internally:

- First turn: harness invoked plain; orchestrator captures `session_id` from the stream.
- Subsequent turns: harness invoked with the captured id (`--resume <id>` for claude; `exec resume <id>` for codex).

`SupportsSession()` is required on both PUPA and LUPA — `plan` refuses to bind opencode or gemini (yet) as either.

### Live output

`overseer plan` streams the harness's text and reasoning deltas to stderr while the harness is still running (`Harness.LiveTextChunk`). Without that, a cold-start with jail + wirez + harness boot can be many silent seconds, looking exactly like a hang. Harness stderr is teed to your stderr as well, so wirez / jail / network errors show up immediately.

Per-turn boundary on stderr:

```
============================ PUPA #1  (claude:opus) ============================
<live text streaming as the harness produces it>
{"result_num": 1}
```

### Flags

| Flag | Required | Default | Meaning |
|------|----------|---------|---------|
| `--pupa-harness <bin>[:<model>]` | yes | — | Solver harness |
| `--lupa-harness <bin>[:<model>]` | yes | — | Critic harness |
| `--jail-bin <bin>` | no | use built-in | External jail binary |
| `--no-jail` | no | false | Skip the sandbox (trusted env only) |
| `--rw <PATH>` | no | — | Extra RW path inside the jail. Repeatable. |
| `--out <path>` | no | — | Write final PUPA text (no marker line) here |
| `--max-rounds <n>` | no | `0` | Cap at N (PUPA + LUPA) rounds; `0` = no cap |

### Workspace

The current working directory (`$PWD`) is the workspace — both agents see it through `cmd.Dir` and the harness-specific cwd flag (`--cd` / `--dir` / `--include-directories`). They can `Read`, `Bash`, `Grep`, etc. against whatever files are around. No clone, no branch.

### Output

- **stderr** — live progress (turn headers, harness deltas, retry notices, framing trace).
- **stdout** — empty.
- **`--out` file** — final PUPA text, marker line stripped, trailing newline appended.

### Retry & faults

Each turn classifies non-zero harness exits through `Harness.ClassifyFault`. Anthropic rate limits, OpenAI quota messages, generic transient HTTP/TCP patterns → retry with exponential backoff (5s → 60s). Unclassified faults → exit 1 with the harness stderr surfaced.

Session id is captured per harness on the first successful turn; retries before that first capture re-run plain (no `--resume`), so a transient on the very first call doesn't leave the agent stuck on a half-created session.

Empty-output guard: if `cmd.Wait()` is happy but neither `finalText` nor any live-stream chunk appeared, `plan` synthesizes a fault with whatever stderr was captured. This catches wrapper layers (subreaper / wirez / jail / shell) that swallow exit codes and would otherwise loop on empty prompts.

---

## `overseer jail` — sandbox primitive

The built-in sandbox is a normal subcommand and can be called standalone:

```sh
overseer jail --rw=/tmp --rw=$HOME/.cache -- /usr/bin/python3 - <<'PY'
import os
open("/etc/passwd", "a")   # → PermissionError: Read-only file system
open("/tmp/scratch", "w")  # → OK
print("uid =", os.getuid())
PY
```

CLI shape: `overseer jail [--rw PATH | --rw=PATH]... -- CMD [ARGS...]`. Everything not in `--rw` (and not in the skip-fstype list — `proc`, `sysfs`, `cgroup`, `cgroup2`, `devpts`, `mqueue`, `debugfs`, `tracefs`, `bpf`, `securityfs`, `pstore`, `fusectl`, `configfs`) is remounted read-only inside the new mount namespace.

Internals: see `jail.go`. Two re-execs (`top → --__stage=mount → --__stage=drop`); the inner user-namespace nesting puts the harness back at its original host uid/gid rather than at root-in-ns. The final stage uses `syscall.Exec(...)` so the user command inherits PID and stdio without an extra Go wrapper process.

---

## Layout & style

Flat: all `.go` files in the repo root. No `internal/`, no `cmd/`, no `pkg/`.

`Throw`/`Try` for error handling (`throw.go`); no `if err != nil { return err }` pass-through. Blank lines around control blocks. JSON-only config — never YAML. See [`STYLE.md`](STYLE.md) for the full rules.

## See also

- [`CLAUDE.md`](CLAUDE.md) — brief context for Claude Code sessions.
- [`STYLE.md`](STYLE.md) — code style and error-handling rules (shared with `gorn`).
- `jail/jail.c` (in the sibling `jail/` repo) — the reference C implementation that `jail.go` ports.
