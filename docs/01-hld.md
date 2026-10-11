# 01 — High-level design

## 1. What aftman is

Aftman is a supervisor process for coding agents. It has two concurrent loops:

```text
CODING AGENT LOOP                        AFTMAN LOOP
reason                                     watch
  │                                          │
  ▼                                          ▼
tool                                       assess
  │                                          │
  ▼                                          ▼
observe ────────── factory events ────────► Jev
  │                                          │
  ▼                                          ▼
edit                                       decide (Rust policy)
  │                                          │
  ▼                                          ▼
test ◄──────────── intervention ────────── intervene
  │
  └── continue
```

The coding agent's loop is untouched. Aftman does not choose the agent's tools, files, or
strategy, and does not stop the agent in order to think. Worker output and lifecycle events flow
into a debounced observation loop that runs while the subprocess is still active.

The supervisory loop has one job: turn heterogeneous, bounded evidence into a small number of
narrow, uncertain judgments, then apply deterministic control on top of them.

## 2. Why a decision model and not another coding agent

Aftman's supervisory questions are narrow and repetitive: *is this worker stuck?*, *is this
requirement satisfied?*, *do we now need independent verification?* A general coding agent can
answer those, but it does so by generating text, one question at a time, at generative-model
latency and cost.

Jev is a **System One** model: structured state and typed questions go in; structured answers and
calibrated probabilities come out. It exposes three primitives:

- **Noul** — probability that a yes/no statement is true (`0.0`–`1.0`).
- **Choice** — one selected category plus a probability for every category, plus confidence.
- **Score** — an expected position on an ordered rubric, plus per-level probabilities.

Two properties make it fit:

1. **Typed answers, not parsed prose.** `worker_stuck = 0.88` is a probability, not a label we
   hope to extract from a sentence.
2. **Independent parallel questions.** All active checks share one compact observation and go out
   in a single request. Adding a safety dimension does not extend a token-by-token answer.

Being calibrated still does not make it right. Aftman validates every field, handles timeouts and
malformed answers, persists every score, and lets code decide whether an action is permitted.

## 3. Goals

- Supervise any of `pi`, `agy`, `claude` (or a generic command) with one runtime.
- Keep supervision cheap and fast enough to run *during* a job, not just after it.
- Make every decision auditable and every run replayable offline.
- Work standalone, inside [herdr](https://github.com/thruwire/herdr), and through a local web UI
  over the network or a tailnet, without herdr or a browser being required.
- Never hand a coding agent the supervisor's credentials.

## 4. Non-goals

- **Not a coding agent.** Aftman does not edit code. It supervises something that does.
- **Not a model trainer or an evaluator.** No calibration claims are made or measured here.
- **Not an orchestrator of many parallel workers.** v1 runs one coding worker at a time, by
  design: the supervisory questions all assume a single active worker. Multiple workers are
  modeled as *history*, not as concurrency.
- **Not a sandbox.** Aftman does not confine a worker's filesystem or network access. Choosing an
  agent, a permission mode, and a sandbox is the operator's job, and the docs say so plainly.
- **Not a replacement for human review.** `escalate` is a first-class outcome, not a failure.

## 5. Components

```text
                    ┌──────────────────────────────────────────────┐
                    │              aftman-daemon                   │
                    │                                              │
  Job ─────────────►│  FactoryRuntime                              │
                    │    ├─ event bus  (tokio broadcast)           │
                    │    ├─ worker task ──────► Worker (trait)     │
                    │    │                        ├─ pi            │
                    │    │                        ├─ agy           │
                    │    │                        ├─ claude        │
                    │    │                        ├─ generic       │
                    │    │                        └─ herdr-pane    │
                    │    ├─ watcher task                           │
                    │    │    └─ ObservationBuilder                │
                    │    │         ├─ git evidence                 │
                    │    │         ├─ untracked excerpts           │
                    │    │         ├─ output tails / test summary  │
                    │    │         └─ command evidence providers   │
                    │    ├─ ResponsibilityRegistry                 │
                    │    ├─ DecisionModel (trait) ──► aftman-jev   │
                    │    ├─ EnsembleResolver  (see doc 06)         │
                    │    └─ FactoryPolicy                          │
                    │                                              │
                    │  RunStore:  events.jsonl + state.json        │
                    │  ProtocolServer: unix socket + HTTP/SSE      │
                    └──────────────────────────────────────────────┘
                             ▲                 ▲              ▲
                             │ NDJSON          │ HTTP/SSE     │ HTTP/SSE
                        aftman CLI          web UI        herdr bridge
                         / TUI            (browser)     (optional client)
```

Component responsibilities:

| Component | Owns | Never does |
| --- | --- | --- |
| `FactoryRuntime` | Run lifecycle, tasks, event queue, timeouts | Interpret evidence |
| `Worker` impls | One agent subprocess/​pane, bounded output, steering, termination | Read the run store |
| `ObservationBuilder` | Bounded, deterministic evidence assembly | Call a model |
| `ResponsibilityRegistry` | Active responsibilities and their flattened checks | Decide |
| `DecisionModel` | One Jev request, validated typed answers | Decide |
| `EnsembleResolver` | Resolving an uncertain or high-stakes answer | Override hard invariants |
| `FactoryPolicy` | Directive arbitration under hard invariants | Call a model |
| `RunStore` | Append-only events, atomic state snapshots | Mutate history |
| `ProtocolServer` | Client fan-out, transport, auth | Hold run state |

## 6. Trust boundaries

Three boundaries matter, and each has a doc:

1. **Supervisor ↔ worker.** The worker is untrusted with credentials *and* with evidence. It can
   write anything into its own output; the observation treats output as data to be judged, never
   as fact. Environment scrubbing is in [08-security.md](08-security.md).
2. **Evidence ↔ model.** Everything that reaches Jev is bounded and is a *selection* made by
   aftman. Repository prose (including `AGENTS.md` and untracked file contents) can contain text
   addressed at a model; see the injection discussion in [06-ensemble.md §10](06-ensemble.md).
3. **Network ↔ daemon.** The daemon's socket is the real API. The HTTP surface is a second
   transport in front of the same protocol, loopback-only by default, with optional bearer auth
   when bound more widely. See [07-surfaces.md](07-surfaces.md).

## 7. Surfaces

One protocol, three transports, zero required surfaces:

| Surface | Transport | Use |
| --- | --- | --- |
| `aftman` CLI + TUI | Unix socket, NDJSON | Standalone use, scripts, CI, watching a run |
| Web UI | HTTP + SSE | Browsing history, live timeline, starting a job, remote/tailnet access |
| herdr | Subprocess (`herdr` CLI) + optional client | Watching workers in panes, human-grade steering, notifications |

The daemon auto-starts on first CLI use and is the single owner of run state. A one-shot
`--no-daemon` mode embeds the runtime in the CLI process for scripts and tests.

Details in [07-surfaces.md](07-surfaces.md).

## 8. Data flow of one cycle

1. A worker event enters the runtime's event queue.
2. The watcher drains adjacent events and enforces the minimum assessment interval.
3. On worker completion, command evidence providers referenced by active checks run first, and
   their bounded results are persisted.
4. Git state, diffs, untracked excerpts, output tails, repository instructions, prior decisions,
   counters, and evidence are assembled into one `Observation`.
5. `ResponsibilityRegistry` groups active checks by the evidence providers they select; each group
   becomes one Jev request.
6. Answers are validated and normalized into one `Assessment` grouped by responsibility.
7. Responsibilities propose directives from their check results.
8. `EnsembleResolver` may request an independent opinion for uncertain or high-stakes inputs
   (doc 06) and records the resolution.
9. `FactoryPolicy` records every proposal and selects exactly one permitted directive.
10. The runtime executes it: steer the live turn, interrupt, spawn a verifier, stop, or finish.
11. The result, the resolution, and the action are persisted **before** observation resumes.

Aftman-generated events do not feed back into the queue, so the observer cannot trigger itself.

## 9. What aftman watches

Each observation is compact and bounded:

- the original job and current factory status;
- active worker summary, recent worker history, output tails, exit status, elapsed time;
- one worker's steering and attempt history;
- `git status`, a bounded diff, and changed file names;
- bounded excerpts of untracked text files (new tests and sources never appear in a diff);
- bounded repository-root `AGENTS.override.md` / `AGENTS.md` instructions, transient only;
- parsed test summaries from worker output and from command evidence;
- recent persisted events;
- the prior assessment and the prior intervention;
- attempt/failure counts and elapsed factory time.

Defaults: 20,000 characters of diff, 12,000 characters per captured output tail, 30 recent events,
10 workers of history, 3 untracked files capped at 4,096 bytes total. All of these are typed
config fields, and the request budget can shrink them further per call.

## 10. What aftman does not promise

- The probabilities are not verified to be calibrated *in this system*.
- The checks are only as good as their instructions and thresholds.
- A supervisor cannot make a bad agent good. It can stop a bad run early and say why.
- Model scores are not deterministic; they are never used as test assertions.

## 11. Related documents

- Runtime detail and types: [02-lld.md](02-lld.md)
- The model contract: [03-jev.md](03-jev.md)
- Worker backends and steering: [04-workers.md](04-workers.md)
- Checks, routing, and policy: [05-responsibilities.md](05-responsibilities.md)
- Honest uncertainty: [06-ensemble.md](06-ensemble.md)
- CLI, herdr, web: [07-surfaces.md](07-surfaces.md)
