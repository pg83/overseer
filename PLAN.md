# PLAN

Date: 2026-05-21

## Context

This note summarizes a comparison between `overseer` and modern agentic software-engineering systems and frameworks. The goal is not to copy hype patterns, but to identify what `overseer` already does well and which improvements would produce the highest engineering return.

High-level conclusion: `overseer` is already closer to recent "serious" async SWE-agent architectures than to shallow "spawn more agents" demos. The next gains are less about adding more roles and more about better search/localization, better observability, and tighter control over cost and retries.

## What `overseer` already does well

### 1. Strong orchestration model

- A single coordinator owns ticket state, scheduling state, and transitions.
- Ticket progress is a real state machine, not an implicit chat workflow.
- State is recoverable from an append-only event log.

This is stronger than many handoff-oriented SDK examples, which often show routing between agents but do not impose a durable, replayable orchestration model.

### 2. Good async execution shape

- Tickets run in isolated workspaces.
- Trunk has a single writer.
- Merge is serialized and test-gated.
- Review and merge are distinct from implementation.

This is aligned with recent async SWE-agent ideas such as centralized delegation plus isolated workspaces and controlled branch integration.

### 3. Pragmatic role split

The current split is useful:

- `tasker` for scoped planning
- `digger` for implementation
- `reviewer` for independent checking
- `merger` for integration verification
- `arbiter` for disagreement handling
- `replanner` for task-database reshaping
- `overseer` for goal completion

This is better than theatrical "virtual company" role inflation. The roles here map to actual control points in the engineering loop.

### 4. Safety and containment

- Built-in jail with read-only mounts by default
- Explicit read-write allowlist
- Serial merger
- Baseline-vs-merged test comparison

Many agent systems mention sandboxing; fewer make it part of the default runtime model this explicitly.

### 5. Good forensic trail

- `tasks.events.jsonl`
- `runs/*.jsonl`
- `messages.txt`
- persistent workspaces

This is valuable for debugging, replay, audits, and future evaluation work.

## What looks weaker than it should be

### 1. Search/localization is underpowered as a first-class concept

Modern SWE-agent performance depends heavily on how well the system narrows the code/search space before implementation. `overseer` currently has planning and execution roles, but no dedicated retrieval/localization stage or tool-specific orchestration for difficult repository search problems.

Implication: on larger repositories, role quality may be bottlenecked more by context acquisition than by reasoning.

### 2. Observability exists, but evaluation does not

`overseer` stores excellent raw logs, but it does not yet appear to have a first-class layer for:

- comparing orchestrator versions
- tracking cost/latency by role
- counting failure modes
- measuring rework and merge-fail rates
- analyzing prompt or policy changes across runs

Raw JSONL is a strong base, but not the same thing as an evaluation system.

### 3. Budget control is weak

Recent work suggests dynamic turn control and early termination can significantly reduce cost without much quality loss. `overseer` still relies on repeated loops and role respawns without a more explicit budget policy per ticket, per role, or per failure pattern.

### 4. Context shaping is too coarse

`buildAgentInput()` currently includes several broad context blocks:

- plan
- log
- ticket chat
- prior runs

This is useful, but increasingly expensive and noisy as histories grow. Modern systems tend to move toward filtered or structured handoff payloads, not just richer prompts.

### 5. Conflict-aware scheduling is limited

`deps` help with hard prerequisites, but they do not represent "likely edit collision" or "hotspot contention". In practice, many multi-agent failures come from parallel work touching adjacent shared files rather than formal dependency violations.

### 6. No simplified fast path

Not every task needs the full team loop. Some recent systems and papers show that simpler pipelines can be more efficient for localized bug-fix work:

- localization
- repair
- validation

`overseer` would likely benefit from a cheaper path for narrow, low-ambiguity tickets.

## What modern systems suggest

### 1. Better agent-computer interfaces matter a lot

SWE-agent and AutoCodeRover both point toward the same lesson: tool/interface quality and code-search quality matter as much as model capability.

For `overseer`, this suggests:

- dedicated repo search support
- structured localization outputs
- stronger file/range targeting before implementation

### 2. Async coding agents benefit from strict integration discipline

Recent async SWE-agent designs emphasize:

- centralized control
- isolated branches/workspaces
- staged merge validation

`overseer` already does this well. This is one of the strongest parts of the current design.

### 3. Observability and tracing are becoming baseline expectations

OpenAI Agents, LangGraph/LangSmith, and similar stacks increasingly treat tracing and evaluation as part of the product, not an afterthought. `overseer` already has enough raw data to support this, but it has not yet been surfaced as a dedicated subsystem.

### 4. More agents is not automatically better

Frameworks like MetaGPT popularized role-rich systems, but the more recent direction is more sober:

- fewer roles
- stronger interfaces
- clearer control flow
- more measurable loops

This favors `overseer`'s current architecture, but also argues against adding more decorative agent types unless they solve a concrete bottleneck.

## Recommended improvements, in priority order

### 1. Build an evaluations and observability layer on top of existing logs

Add derived metrics and analysis tooling over:

- `tasks.events.jsonl`
- `runs/*.jsonl`
- `messages.txt`

Track at least:

- per-role latency
- per-role retry rate
- `REWORK` frequency
- `MERGE_FAIL` frequency
- replanner invalid-output rate
- reopened/escalated ticket rate
- tickets completed per run

This is likely the highest-leverage next step because it improves every future change.

### 2. Add a first-class localization/search stage

Before `tasker` or before `digger`, introduce a lightweight search/localization pass that outputs:

- candidate files
- ranked evidence
- probable symbols/functions
- relevant tests

This would reduce wasted prompt budget and improve quality on large repositories.

### 3. Add explicit budgets and stop conditions

Per ticket or per role, introduce policies such as:

- max tasker respawns
- max digger/reviewer cycles before mandatory replanner review
- max retry budget by transport fault class
- early stop when confidence drops and no new evidence appears

This should reduce loop waste materially.

### 4. Make context handoff more structured and selective

Replace "always include broad history blocks" with a more deliberate model:

- compact summaries by default
- expandable artifacts on demand
- structured failure payloads for arbiter/replanner
- targeted prior-run excerpts instead of full generic summaries

### 5. Add conflict-aware scheduling

Extend scheduling beyond `deps` with a soft contention model:

- likely hotspot files
- recently touched shared modules
- ticket similarity or overlapping paths

Then avoid dispatching likely-conflicting implementation tickets in parallel.

### 6. Add a cheap fast path for narrow tasks

For well-localized, low-risk tickets, consider a reduced pipeline such as:

- localize
- implement
- review
- merge

without always requiring the full tasker/replanner overhead.

### 7. Add policy gates for high-risk surfaces

Introduce stricter controls for:

- CI/workflow files
- destructive repository-wide edits
- secret-adjacent files
- sandbox expansion requests
- dependency or install scripts

These can remain opt-in, but they should be first-class policy concepts.

## Bottom line

`overseer` is already strong where many frameworks are weak:

- durable orchestration
- workspace isolation
- controlled trunk writes
- real review/merge gates
- solid forensic logging

The next meaningful improvements are not "more multi-agent". They are:

1. observability and evaluation
2. stronger search/localization
3. better cost and retry control
4. better scheduling under contention

If those are done well, `overseer` should become materially better without becoming more theatrical or harder to reason about.

## Sources

- Anthropic Claude Code subagents: https://code.claude.com/docs/en/sub-agents
- Anthropic Claude Code common workflows: https://code.claude.com/docs/en/common-workflows
- OpenAI Agents handoffs: https://openai.github.io/openai-agents-js/guides/handoffs/
- OpenAI Agents tracing: https://openai.github.io/openai-agents-python/tracing/
- LangGraph multi-agent handoffs: https://docs.langchain.com/oss/python/langchain/multi-agent/handoffs
- LangGraph custom workflows: https://docs.langchain.com/oss/python/langchain/multi-agent/custom-workflow
- LangSmith observability concepts: https://docs.langchain.com/langsmith/observability-concepts
- LangSmith evaluation: https://docs.langchain.com/langsmith/evaluation
- GitHub Copilot cloud agent: https://docs.github.com/en/copilot/concepts/agents/cloud-agent/about-cloud-agent
- GitHub review of Copilot output: https://docs.github.com/en/copilot/how-tos/copilot-on-github/use-copilot-agents/review-copilot-output
- OpenHands paper: https://openreview.net/forum?id=OJd3ayDDoF
- OpenHands agents docs: https://docs.openhands.dev/openhands/usage/agents
- SWE-agent: https://arxiv.org/abs/2405.15793
- AutoCodeRover: https://arxiv.org/abs/2404.05427
- Agentless: https://arxiv.org/abs/2407.01489
- SWE-Search: https://arxiv.org/abs/2410.20285
- CAID / async SWE agents: https://arxiv.org/abs/2603.21489
- Turn control for coding agents: https://arxiv.org/abs/2510.16786
- Early termination for coding agents: https://arxiv.org/abs/2601.05777
- MetaGPT: https://arxiv.org/abs/2308.00352
- MetaGPT repository: https://github.com/FoundationAgents/MetaGPT
