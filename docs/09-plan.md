# 09 — Plan

Build order, exit criteria, and what is deliberately deferred.

The order is chosen so that **nothing real is required to run anything**: M0 works with a
simulated worker and a simulated model, M1 adds a real Jev call, and only then does a real coding
agent appear. Every milestone ends with the full offline suite green.

## M0 — Skeleton, core types, offline run

*Deliverable:* `aftman run --repo . --job "..." --worker simulated` completes with no network and
no API key, streaming events to the terminal.

| Work | Crate |
| --- | --- |
| Workspace, CI (fmt, clippy `-D warnings`, test, audit), `.gitignore` | — |
| `RunId`, `WorkerId`, `EventSeq`, `Clock`, `Job`, `RunConfig`, `FactoryStatus` + transition table | `aftman-core` |
| `Observation`, `BoundedLog`, `state_for` with byte-exact snapshot tests | `aftman-core` |
| `EventEnvelope` + all event kinds; `FileRunStore` with atomic snapshot and torn-log recovery | `aftman-store` / `aftman-core` |
| `Worker` trait, `SimulatedWorker` | `aftman-workers` |
| `ScriptedModel`, `SimulatedModel` | `aftman-core::testing` |
| Minimal `FactoryRuntime` + `aftman run --no-daemon` | `aftman-daemon`, `aftman-cli` |
| `ThreadRng`-free determinism: injected clock, no `Date::now` in domain code | — |

**Exit criteria:** `cargo test --workspace` passes with an empty environment; a run produces a
well-formed `events.jsonl` and `state.json`; the transition table rejects an illegal transition;
a truncated final line in `events.jsonl` is recovered.

## M1 — Real Jev

*Deliverable:* `aftman run --repo . --job "..."` makes a real System One call and logs usage.

At this point the observation is **minimal** (job, status, one worker record) and the question set is
a single hardcoded check, because responsibilities, routing, and the full observation arrive in M2.
The purpose of M1 is to prove the wire contract, the retry and budget behaviour, and the credential
path — not to supervise well yet. An earlier revision promised "real System One calls" from a run
whose observation and check registry did not exist until the following milestone.

| Work | Crate |
| --- | --- |
| `DecisionModel` trait, `Question`/`AnswerValue`, `ModelError` | `aftman-core` |
| Minimal `ObservationBuilder` (job, status, worker record) + one hardcoded check set | `aftman-core` |
| `SystemOneClient`: reqwest, retry with `retry-after-ms`/`retry-after`, timeout, error extraction | `aftman-jev` |
| Wire types mirroring the documented request/response, tolerant of additive change | `aftman-jev` |
| Request budget: pair/request caps, hysteresis targets, deterministic compaction plan | `aftman-jev` |
| Usage log + `aftman usage` | `aftman-jev`, `aftman-cli` |
| `aftman doctor` with a key-presence check and an opt-in live one-question probe | `aftman-cli` |
| `wiremock` fixtures: happy path, 429, 500, timeout, missing answer, unknown answer type, NaN | `aftman-jev` |

**Exit criteria:** every error row in [03-jev.md §4](03-jev.md) has a test; a run against the real
API with `AFTMAN_LIVE=1` succeeds; a 429 with `retry-after-ms: 100` retries within tolerance; the
budget module provably refuses to send a request missing the job.

## M2 — Responsibilities, policy, and a real worker

*Deliverable:* `aftman run --worker pi|agy|claude` supervises a real agent and can steer, stop, or
finish.

| Work | Crate |
| --- | --- |
| The six bundled TOML definitions, ported with identical ids and thresholds | `aftman-core` |
| TOML schema + validation, `deny_unknown_fields`, `include_str!` + user-dir override | `aftman-core` |
| Router: global + conditional, one request, thresholds, `JOB_ROUTED` event | `aftman-core` |
| Built-in responsibility rule logic: completion (joint `Finish` precondition), verification, worker health (with steering grace), instructions drift, human escalation | `aftman-core` |
| `FactoryPolicy::arbitrate` — pure, total order, every suppression recorded | `aftman-core` |
| `ObservationBuilder`: git, untracked excerpts, test-summary parser, sensitive-file skipping | `aftman-core` |
| Command evidence providers, `run_when` scheduling | `aftman-core` |
| `pi` (`--mode json`; `--mode rpc` for steering), `agy` (`--print="…" --output-format stream-json`), and `claude` (`-p --permission-mode … --permission-prompts none`) backends | `aftman-workers` |
| Process-group spawn, bounded streaming, interrupt→grace→kill | `aftman-workers` |
| Watcher trigger architecture (activity channel, lifecycle channel, idle ticker) + unchanged-observation digest skip | `aftman-daemon` |
| `worker_environment` scrubbing + a test that asserts no `TYPESAFE_*` survives | `aftman-workers` |
| Ensemble **S0**: types, events, no-op resolver | `aftman-core` |

**Exit criteria:** the `Finish` precondition has a truth-table test; priority ordering is pinned
pairwise and asserted to be a total order with no ties; a fake worker script that prints
`3 passed` and exits 0 produces the expected directive; `permitted == false` suppresses a rule
whose value is far from the threshold **in both comparison directions**; an unresolved active check
suppresses `Finish` even when `Finish`'s own direct conjuncts are satisfied; `Stop` leaves the run in
`Stopped` and a following `Retry` returns it to `Running` (so the stop-then-retry path is reachable);
a silent worker consumes no assessment budget across many idle ticks; the scrub test passes. A real
agent run against the live API is a gated `AFTMAN_LIVE=1` smoke test and is **not** an exit
criterion, because exit criteria must be checkable by `cargo test --workspace` with no network and
no credentials (`AGENTS.md` hard rule 4).

## M3 — Daemon, protocol, CLI, TUI

*Deliverable:* `aftman serve` + `aftman watch` with a second client attaching mid-run without
losing events.

| Work | Crate |
| --- | --- |
| `ProtocolServer`: NDJSON framing, request/response, subscriptions, bounded per-client channels | `aftman-daemon` |
| Unix socket, `0600`, stale-socket handling, pid lock, double-fork, `stop`/`status` | `aftman-daemon` |
| Auto-start on first client command | `aftman-cli` |
| Full CLI surface incl. `ls`, `show`, `logs`, `decisions`, `steer`, `cancel`, `answer`, `purge` | `aftman-cli` |
| TUI: decision timeline, worker tail, assessment panel, steer and answer keybindings | `aftman-cli` |
| Reconnect/resume from `seq`; test that a dropped client loses nothing | `aftman-daemon` |
| Backpressure test: a stalled subscriber cannot stall a run | `aftman-daemon` |

**Exit criteria:** two clients observe the same run; a client killed mid-run resumes with no gap and
no duplicate; `aftman run --no-daemon` still works; daemon restart marks in-flight runs
unambiguously (resumed or `Failed`, never zombie).

## M4 — Web UI

*Deliverable:* a browser at `127.0.0.1:8787` shows live runs, decisions, diffs, and answers
escalations.

| Work | Crate |
| --- | --- |
| axum router mirroring the protocol; SSE with `Last-Event-ID` | `aftman-web` |
| Embedded assets via `rust-embed`; server-rendered pages, no Node in the runtime path | `aftman-web` |
| Pages: runs, run detail (timeline + assessment + diff), decisions, evidence, new job, usage, config | `aftman-web` |
| Auth: `loopback` \| `token` (+ cookie login), refuse non-loopback with `auth = "none"` | `aftman-web` |
| `NeedsHuman` visually distinct from `Failed`; escalation form | `aftman-web` |
| Tailscale section in the README: `tailscale serve --bg 8787` | docs |

**Exit criteria:** SSE reconnect during a run produces no gaps; binding `0.0.0.0` without a token
fails to start; every check value is visible with its threshold and margin; an escalation can be
answered from the browser and the run continues.

## M5 — Ensemble S1 and S2

*Deliverable:* uncertainty near a threshold triggers a deterministic evidence refresh; a
framing-consistency check catches a framing-sensitive answer.

| Work | Crate |
| --- | --- |
| Trigger computation: `NearThreshold`, `LowConfidence`, `ClaimOnlyEvidence`, `OutcomeRelevant`, `HighStakes`, `Irreversible`, `RepeatContest` — with `OutcomeRelevant` computed by re-running pure policy | `aftman-core` |
| **S1 — R0**: evidence refresh, re-assessment, `supersedes` linking, refresh cap | `aftman-core` |
| **S2 — R2**: negated question, framing tolerance, consistency verdict | `aftman-core` |
| `permitted` gating on a direction-agnostic measurement, contest suppression, and the in-band → R0/escalate path | `aftman-core` |
| The two invariant property tests (no fusion; no opinion-driven finish) | `aftman-core` |
| Replay from `RecordedOpinionSource` | `aftman-core::testing` |

**Exit criteria:** a check in band, or one whose rule would not fire, requests zero opinions; a fake passing test run
converts a `0.71` into a decisive value; no input can produce an opinion-driven `Finish`; repeated
refresh is capped and visible.

## M6 — herdr

*Deliverable:* `aftman run --worker herdr:pane --kind claude` runs a worker in a pane, steers it,
and escalates a `blocked` agent.

| Work | Crate |
| --- | --- |
| `herdr:pane` backend: split → `agent start` → `agent prompt --wait` → poll → read | `aftman-workers::herdr` |
| `blocked` → `EscalationRaised`; `unknown` ≠ done; `done` ≠ finished | `aftman-workers::herdr` |
| Level 1: `notification show` on escalation and terminal transitions, rate-limited | `aftman-daemon` |
| Refuse to close/steal panes aftman did not create; require `HERDR_ENV=1`; fall back cleanly | `aftman-workers::herdr` |
| `interactive` permission mode gated to this surface | `aftman-workers` |
| Records a fake `herdr` executable on `PATH` for the offline suite | tests |
| Live smoke test against a real herdr session (gated) | tests |

**Exit criteria:** the offline suite never invokes real herdr; with `HERDR_ENV` unset, no herdr
process is executed; a simulated `blocked` transition produces an escalation and resumes on
answer; the pane id is recorded as `artifact_ref` and deep-linkable.

## M7 — Ensemble S3 and S4

*Deliverable:* a cross-model judge can contest a check, and a human can answer an escalation from
all three surfaces.

| Work | Crate |
| --- | --- |
| **S3 — R1**: judge in schema mode, tool-less, state fenced as data, verdict parsing | `aftman-core`, `aftman-workers` |
| Same-family warning at startup; record judge model ids | `aftman-core` |
| `state_digest` / `prompt_hash`; the judge never sees Jev's value or prior opinions | `aftman-core` |
| **S4 — R3**: escalation channels (CLI prompt, TUI, web form, herdr notification) | all surfaces |
| `NeedsHuman` as a terminal state after `escalation_timeout`; exit code 3 | `aftman-core`, `aftman-cli` |
| Adversarial injection test | `aftman-core` |

**Exit criteria:** a contest suppresses `Finish` and cannot stop work on its own; no judge receives
Jev's value; an injected instruction in the state cannot produce an opinion-driven action; a
`NeedsHuman` run is distinguishable from `Failed` in every surface.

## M8 — Backend hardening and verifier missions

All three agent backends are implemented in M2, because M6 (`--kind claude`) and M7 (an `agy` judge)
both depend on them and an earlier sequencing put the backends after their own consumers. M8 is the
work that turns "the command line launches" into "the output is understood":

*Deliverable:* every backend parses recorded real output, and the verifier mission is constrained.

| Work | Crate |
| --- | --- |
| Record real CLI output for each backend into `tests/fixtures/`; parse the recording, not a guess | `aftman-workers` |
| `agy` hardening: `--print-timeout`, stream-json event coverage | `aftman-workers` |
| `claude` hardening: stream-json schema, `--json-schema` verifier output, `--resume` continuity | `aftman-workers` |
| Verifier mission for all three, using `--json-schema` → `EvidenceResult` | `aftman-workers` |
| Verifier-mutation detection (diff the tree before/after) | `aftman-core` |
| Per-backend permission mapping (§8.1) implemented and tested, including the `pi` refusal path | `aftman-workers` |
| Update the §1 matrix with verified versions; `doctor --probe` gating for unverified backends | docs, `aftman-cli` |

**Exit criteria:** each backend parses its recorded fixture; a backend whose stream schema is not
fixture-verified stays gated behind `doctor --probe`; `mode = "sandboxed"` with `--worker pi` fails
at startup with guidance rather than silently degrading; a verifier that modifies the repo is
recorded as inconclusive, not as a pass.

## Deferred, with reasons

| Deferred | Why |
| --- | --- |
| Multi-worker concurrency | Every check assumes one active worker. Needs a different observation and policy model, not a flag. |
| Attached-session supervision (native hooks for an agent aftman did not start) | A second runtime with a different state model. Valuable, but not needed for the standalone/herdr/web story and it doubles the surface area. |
| Extension marketplace / remote plugin loading | A remote code-loading path inside the daemon conflicts with §4 of the security doc. Local definition files already cover the customization need. |
| herdr Level 3 (a herdr integration entry / sidebar plugin) | Depends on herdr internals and adds a maintenance coupling. Levels 0–2 already cover standalone, herdr, and web. |
| Automatic model routing across providers | Provider selection is one base URL by design. Auto-failover between *billing accounts* is a finance decision, not an architecture one. |
| A learned ensemble weighting | Requires a labeled dataset and a joint error model. Doc 06 rejects it for cause. |
| Windows support | Process groups, Unix sockets, and signal semantics all need a second implementation. Linux/macOS only in v1. |
| Non-git repositories | Git evidence is the observation's backbone. A fallback provider is possible but the checks would need rewriting. |

## Risk register

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Jev latency too high to supervise in-flight work | The architecture's core hypothesis fails | Debounce interval, explicit timeouts, `Decisive`-only opinions, and a documented measurement in M1 rather than a marketing claim |
| A model change alters answer shapes | Silent wrong supervision | Missing requested answers are fatal; unknown types ignored; recorded fixtures; `model` persisted as reported |
| Prompt injection through repository content | Bad judgments | Jev receives state + questions, not instructions; judges are tool-less; opinions cannot finish |
| Agent CLI flags drift | Backends break | Fixture recordings + `doctor --probe`; flags centralised per backend |
| Shadow-mode disagreement between upstream and aftman expectations | Operator confusion | Ported ids and thresholds verbatim; deviations listed in doc 05 |
| Scope creep into orchestration | Misses the actual point | Non-goals in doc 01 §4, deferred list above, and the security doc's explicit limits |

## Definition of done for v1

1. `cargo test --workspace` passes with no network and no credentials.
2. `aftman run --repo <real repo> --job <real job> --worker pi` finishes or stops correctly against
   the live API, with `events.jsonl` explaining every decision.
3. The same run is watchable from the CLI, from a browser, and — with `--worker herdr:pane` — from
   a herdr pane.
4. `aftman decisions <run-id>` answers "why did it do that?" without re-running anything.
5. No path exists from a model output, an opinion, or a UI action to `Finish`, other than the
   `Finish` precondition in doc 05 §6.2.
6. `aftman doctor` is clean on a fresh machine after setting one environment variable.
