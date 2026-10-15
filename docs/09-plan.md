# 09 — Plan

Build order, exit criteria, and what is deliberately deferred.

The order is chosen so that **nothing real is required to run anything**: M0 works with a
simulated worker and a simulated model, M1 adds a real Jev call, and only then does a real coding
agent appear. Every milestone ends with the full offline suite green.

## Progress at a glance

| Milestone | Status | Commits |
| --- | --- | --- |
| M0 — skeleton, core types, offline run | **complete** | `aaf9cc9` |
| M1 — real Jev | **complete** | `b826530` |
| M2 — responsibilities, policy, a real worker | **complete** | `d3730f5`, `6d4b2a9`, `450b1a6` |
| M3 — daemon, protocol, CLI, TUI | **complete** | `cdb5b0c`, `eab59ac`, `d673a67`, `76810fe`, `160ebdc`, `d74327f`, `1169b5c` |
| M4 — web UI | not started | — |
| M5 — ensemble S1 and S2 | not started | — |
| M6 — herdr | not started | — |
| M7 — ensemble S3 and S4 | not started | — |
| M8 — backend hardening and verifier missions | in progress; in-process repository observation built | — |

As of the M3 commits: **920 tests passing**, `cargo clippy --all-targets --all-features -- -D
warnings` clean, `cargo fmt --check` clean, and the whole suite green with an empty environment.
(394 as of M2.)

### Where the record lives

Each milestone section below carries a `**Status**` paragraph saying what actually landed, which
exit criteria were met, and — where the code diverged from this plan or from `docs/` — why. The
per-milestone test count is stated in that paragraph or in its commit message.

The authoritative record of *why* a change was made is the commit message, which states what was
built, what was found, and any scope decision taken rather than assumed, including the ones where
the plan turned out to be wrong:

```bash
git log --oneline          # the milestone commits, in order
git show <sha>             # the reasoning behind one of them
```

Milestone findings that changed `docs/` as well as code are noted in place:

- the watcher must drain worker completion independently of the trigger that woke it
  ([02-lld.md §7.1](02-lld.md));
- `Condition` must not derive `serde` ([02-lld.md](02-lld.md), and the type's own documentation);
- every writer of a run's events appends to the store before it emits, in `seq` order — the
  precondition the hub's gap-free resume rests on ([02-lld.md §11](02-lld.md));
- hard rule 3 covers the supervisor's own subprocesses, `git` included; every spawn site is listed
  with the environment it gets ([08-security.md §2.2](08-security.md)), and the supervisor's
  `git` is hardened against repository-controlled configuration
  ([08-security.md §3.2](08-security.md)).

## M0 — Skeleton, core types, offline run

**Status: complete.** `cargo test --workspace` is green (160 tests) with an empty environment, clippy
is clean under `-D warnings`, and `aftman demo` reaches `finished` with no network, no credentials,
and no agent binary, leaving the target repository untouched. No design change was needed: every bug
found during M0 was a deviation of the code from these documents, which is the intended direction.

Two details earned a place in the design from this milestone: the completion channel must be drained
independently of the trigger that woke the loop (`docs/02-lld.md` §7.1), and the content digest must
exclude *every* per-cycle counter, including the assessment sequence inside the prior decision.

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

**Status: complete.** `crates/aftman-jev` implements the System One contract against
`docs/03-jev.md`: request and response shapes, retry with `Retry-After` honouring, the
request budget, and usage accounting. `aftman doctor` reports the resolved provider and
`--probe` makes one billable liveness call; `aftman usage` aggregates the token log.

Two design decisions worth recording, both forced by hard rule 4 rather than by
convenience:

- **The HTTP boundary is a trait, not `reqwest`.** A `FixtureTransport` that answers from
  memory keeps the retry, timeout, and error-mapping tests free of sockets, rather than
  binding a loopback port and calling that offline. The real `ReqwestTransport` is used
  by the binary and by the `AFTMAN_LIVE=1` tests.
- **A configured provider is used by default; the simulated model must be asked for.**
  A supervised run that quietly fell back to a simulation would be a lie about what
  happened, so `run` refuses to start without a provider unless `--simulate-model` is
  passed.

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

**Status: complete.** What landed:

- `aftman-workers::subprocess` — one process-group spawn path, bounded streaming,
  interrupt-then-kill with a grace period, and a scrubbed environment. Termination
  reaches grandchildren, which is the reason for the process group.
- `aftman-workers::backends` — `pi` (one-shot and steerable RPC), `agy`, `claude`, and
  a quoted generic template, as pure functions from options to argv, so command
  construction is tested against the documented lines without any agent installed.
- Posture validation at **startup**: `pi` cannot express `sandboxed` because it has no
  sandbox flag and no tool-permission flag, so it is refused before run state exists
  rather than launching something unpinned.

Also landed from this milestone: **declarative definitions**
(`aftman-core::definitions`) with the six bundled `.toml` files compiled in via
`include_str!`, a validating schema (`deny_unknown_fields`, so a typo fails loudly),
and a config-directory overlay that replaces a bundled definition of the same name.
The `when` grammar from `docs/05` §2.1 is implemented in `aftman-core::condition`,
including the rule that mixing `and` with `or` at one level without parentheses is a
parse error.

One scope decision, stated rather than assumed: **TOML owns checks, Rust owns rule
logic.** A definition declares ids, instructions, thresholds, evidence selection, and
(for operator-authored responsibilities) `[[rules]]` over check results. It cannot
express `verifier_completed`, the remaining steer budget, or the attempt count, because
those are runtime facts rather than check results — so the bundled responsibilities keep
their logic in code and a condition may only read checks their own definition declares.
Pretending otherwise would produce rules that silently mean something weaker than they
read.

The **router** (`aftman-core::routing`) evaluates every conditional responsibility in one
request against the job text and nothing else, activates each one that clears its own
threshold, and records every probability and threshold in `JOB_ROUTED` so "why didn't the
documentation check run?" has an answer. A model failure degrades to the globals and says
so. `quality.documentation` is the first bundled responsibility to go through this path,
and it turned out to have no runtime-aware logic at all — so its rule moved into TOML,
which is exactly what the declarative form is for.

**Command evidence providers** (`aftman-core::evidence` for configuration and scheduling,
`aftman-workers::evidence` for execution) run an operator-configured `argv` with a literal
argument list, a scrubbed environment, an own process group, bounded output, and a hard
timeout. A non-zero exit is a *measurement*, not a failure: a test command that exits 1
has still run. `run_when = "before_finish"` is deferred correctly — when `Finish` is
selected and those providers have not run, they run and the run is judged again rather
than finishing on a claim.

Two invariants came out of building this, both runtime facts rather than judgements:

- **`Finish` requires that a worker has run at least once** (`Invariant::NoWorkAttempted`).
  Without it, a model that answers optimistically on the first cycle — before any work
  exists — ends a job that never started.
- **An empty proposal set with no active worker escalates** rather than returning
  `Continue`. Otherwise a correctly-refused `Finish` spins on the idle ticker until the
  overall timeout, which is the same class of bug as the M0 deadlock by a different route.

Two layering corrections came out of this milestone and are worth keeping: backend
launch options must **not** live in `RunConfig` (that would make `aftman-core` depend
on `aftman-workers`), and the mission is run-time data, so a backend builds its argv
per run rather than at construction — the same worker instance serves the coding
worker and the verifier.

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

**Status: complete.** What landed, in the order it was built:

- **Event hub and wire protocol** (`aftman-daemon::hub`, `::protocol`). One fan-out shared by
  the socket server and, in M4, SSE. Resume is gap-free by construction: a subscriber is
  registered and the replay ring snapshotted in one critical section, the on-disk log is read
  only afterwards, and the overlap is dropped by `seq`. That depends on the runtime appending to
  the store before it emits, which is now a stated precondition of every writer.
- **Run control plane** (`aftman-daemon::control`, `FactoryRuntime::start` / `drive`). Cancel,
  human steer and escalation answers on a live run. `NeedsHuman` is a quiet wait when a human
  channel is attached and ends the run when one is not.
- **The daemon** (`manager`, `server`, `lifecycle`). `RunManager` has one method per protocol
  method and no socket knowledge; building a run is injected through `RuntimeFactory`, so the
  daemon crate still cannot name the provider. Restart recovery marks an undriven, unfinished
  run `Failed` with reason `daemon restarted`.
- **The CLI as a client** (`aftman-cli` is now a library with a thin `main`), with one factory
  shared by the daemon and `--no-daemon` so the two cannot drift.
- **The TUI** (`aftman watch`): a pure fold over the event stream, a pure view, pure input
  handling, and a driver generic over the backend, so all of it is tested without a terminal.

Exit criteria, each checked by driving the real binary as well as by the offline suite: two
clients observe the same run; a killed client resumes with no gap and no duplicate; a stalled
subscriber is dropped without stalling the run; `aftman run --no-daemon` works for every outcome
including an escalation; and after `kill -9` of the daemon the run is `Failed` exactly once with
its orphaned worker ended. The first pass of that check found `--no-daemon` hanging on an
escalation and the first command after a daemon crash failing; both were fixed and re-checked.

How it was built: each stage was implemented, then reviewed adversarially by a separate reviewer
that was given the code and the contract but not the implementer's reasoning, then fixed; the
integrated milestone was reviewed once more against these criteria, and the fixes from that review
were themselves verified. Not everything those reviews listed was reached. Known unverified
ground: recovery with many orphans at once, very large untracked trees, a state directory on
read-only or network storage, panics inside TUI tasks, and any terminal emulator other than tmux.

Scope decisions, stated rather than assumed:

- **No double fork.** `fork` is `unsafe` and `unsafe` is forbidden workspace-wide. The CLI spawns
  `serve --detached`, and the child calls `setsid()` and `chdir("/")` before any thread exists.
  The outcome the plan wanted — closing the terminal does not kill a running job — holds.
- **Resume after a daemon restart is not implemented.** The plan allowed "resumed or `Failed`";
  M3 does `Failed`. Resuming needs the worker's process to be adoptable, which is M6 territory.
- **A human `Stop` ends the run as `Cancelled`, not `Stopped`.** `Stopped` is non-terminal and
  `core.completion:retry-stopped` would restart work a person had just stopped.
- **The daemon keeps the environment it was started with.** Workers used to inherit it, which is
  why a `GITHUB_TOKEN` exported in the shell that started the daemon reached every later run.
  **Decided after this milestone: forward an allowlist per run**, and see the follow-ups below.
- **One daemon per state directory**, and a per-run ownership claim (`runs/<id>/run.lock`) held
  by whichever process drives the run. Without the claim, a daemon auto-started by `aftman ls`
  would mark a live `aftman run --no-daemon` in another terminal as `Failed`.
- **An orphaned worker is signalled only when it can be positively identified** (pid, start time,
  process group and boot id all match the record). Otherwise recovery says `unverified` and does
  not signal. The non-Linux identity path uses `ps` and is not exercised by any test.

Findings that changed code written in earlier milestones, because the daemon made them reachable
or the reviews tripped over them:

- **Hard rule 3 was violated by the supervisor's own `git`** (M0 code). The observation ran
  `git status` / `git diff` with the daemon's environment, so a worker that set `core.fsmonitor`
  in `.git/config` had its script run with the provider key. Supervisor git is now scrubbed, so
  no git child sees a supervisor secret, and hardened against code-executing configuration in
  every config scope ([08-security.md §3.2](08-security.md)). The first version of the hardening
  scanned only repository-local config and was bypassed through worktree-scoped config; a
  verification pass over the fix caught that before the milestone closed.
- **Observation assembly blocked the run loop** with no timeout, though the LLD said it ran in
  `spawn_blocking` under 8 s. It now does what the document says.
- **`WorkerOutput` was specified in the LLD and never emitted.** It is now, once per cycle.
- **Policy's overall-timeout invariant never fired**: it read `counters.elapsed_secs`, which
  nothing set.
- **`FileRunStore::append` glued a new event onto a torn final line**, losing the event and
  corrupting the log mid-file.
- **The loopback check in `aftman-jev` took URL userinfo for the host**, so
  `http://localhost@evil.example` was allowed as a cleartext base URL.

Left open, deliberately visible:

- The model-facing observation still sends empty `output_head` / `output_tail` for the active
  worker (`aftman-core::observation`, since M2). Filling them changes what is sent to a billable
  model and the byte-exact snapshot tests, so it was not folded into this milestone.
  **Decided: fill them as designed.** Under way — see the follow-ups below.
- ~~The supervisor's `git` cannot be fully isolated from a worker that edits `.git/` *while an
  observation is running*.~~ **Resolved in M8**: the observation is now computed in-process with
  `gix` and an empty filter-driver list, so no configuration the repository writes can select a
  program at all ([08-security.md §3.2](08-security.md)).
- Recovery binds the socket only after orphans are reaped, so the first command after a daemon
  crash waits about one termination grace and says why.
- An all-runs subscriber gets no marker when a run is abandoned; a run-scoped one does.
- The TUI does not show the diff ([07-surfaces.md §8](07-surfaces.md) says so); that arrives
  with the web UI's diff view in M4.

**920 tests**, `cargo clippy --all-targets --all-features -- -D warnings` clean, `cargo fmt
--check` clean, and the suite green with an empty environment.

### M3 follow-ups — the three decisions, and what came of them

Each is a separate behaviour change.

1. **The run can see the worker's output.** *(Built — see the commit.)* It judged "is the worker
   stuck" without seeing anything the agent printed; only parsed test counts reached it. Now the
   observation carries a bounded head and tail of the most recent worker's log
   (`WorkerOutputExcerpt`), with terminal control sequences stripped
   ([ansi.rs](../crates/aftman-core/src/ansi.rs)) because they cost tokens and mean nothing to a
   model, and with `dropped_bytes` reported so a clipped log is never presented as a whole one.
   Three things were found on the way:
   - **Budget ordering.** `fit` checked the protected-evidence set *before* truncating oversized
     pairs, so a chatty worker's 50 KiB tail counted in full and the request was **refused** —
     supervision would stop rather than shrink a field it is required to keep. Truncation now runs
     first, and the check runs on the bounded result. Refusing is still the outcome when the
     required evidence genuinely cannot fit.
   - **The digest now includes output**, which is a change to the M0 skip. Recorded in
     02-lld §7.1 with why it does not weaken the silent-worker property.
   - **The tail is prose, not JSON.** `SubprocessWorker` now routes stdout through
     `StreamDecoder` for the three structured backends, so the bounded tail spends its bytes on the
     agent's words and tool names instead of envelope keys. Unstructured backends pass through
     untouched. Covered by two tests that drive a real pipe: one plays the recorded `pi` stream back
     through `cat`, the other checks a `Generic` worker's output is not altered.
   - **The decoded summary reaches the model** (issue #2). `WorkerRecord::activity` carries the
     decoded `StreamActivity`, `pump` publishes it as the stream arrives so a loop is visible
     *before* the worker finishes, and the observation projects it as `worker_activity`. The
     tool-call list is capped at 256 with the true count kept separate: persisting an unbounded
     list is the same class of bug as an uncapped output tail, and the count is the signal a cap
     would otherwise hide. The counts are in the content digest; a repeat of one tool with
     identical results changes the count and nothing else.
   - **Cost, measured:** a request with the default excerpts (4 000-byte head, 12 000-byte tail)
     serialises to 16 241 bytes against 241 without — the caps, as expected. No live run was made;
     that is the only way to get a token number and it is billable.
2. **The daemon contributes only a baseline; the run forwards an allowlist.** *(Built — see the
   commit.)* `RunStartParams.env` carries the client's chosen names, bounded and validated, with a
   supervisor name refused at both the protocol layer and the environment layer. Three things
   changed beyond adding the field:
   - `worker_environment()` no longer passes the daemon's environment through. It builds from the
     daemon's **baseline** (`HOME`, `PATH`, `SHELL`, `TERM`, `TMPDIR`, locale, `XDG_*`) plus the
     run's allowlist. Without this the symptom the task exists to fix — a variable exported in the
     daemon's shell reaching every later worker — would have survived, because the field is
     additive and the inheritance was the leak.
   - `supervisor_environment()` was split out for the supervisor's own `git`, which is not an agent
     and does need the daemon's environment. One helper serving both policies is how a worker came
     to inherit the shell. (M8 removed it again when the supervisor's `git` itself was removed: it
     had no other caller.)
   - `SSH_AUTH_SOCK` is in the client's default allowlist rather than the baseline: it is a live
     signing capability, so the operator grants it rather than the daemon handing it out for
     having sat in the right shell.
3. **Supervisor `git` isolation.** *(Decided: accept at M3; **built in M8**.)* The residual — a
   worker inserting a filter between the supervisor's config scan and its `git diff` — was
   detected and the cycle's git evidence discarded with a note. The in-process-git investigation
   (issue #1) has since been implemented: see [M8](#m8--backend-hardening-and-verifier-missions).

### Known gaps carried from M3

Found in the M3 reviews. None is decided and none is urgent; they are written down so they are not
rediscovered. Each is a candidate issue rather than a plan item.

- Recovery binds the socket only after orphaned workers are reaped, so the first command after a
  daemon crash waits about one termination grace (≈5 s) and says why.
- An all-runs subscriber gets no marker when a run is abandoned; a run-scoped one does.
- `Follow`'s liveness check (`check_driven`) is not strictly resumable across a dropped `next()`.
- The TUI's decision list is not scrollable (it refuses to act below a minimum size instead); the
  launch posture shows in the header only on terminals ≈140 columns wide (an `auto-approve` badge
  always shows).
- `EventKind::Unknown` and `ErrorCode::Unknown` lose the original string on re-serialisation.
- `aftman decisions` prints f32 values widened to f64 (`margin=0.15000000596046448`); cosmetic —
  `round4` in `runtime.rs` exists for exactly this.
- Unverified by any review: recovery with many orphans at once, very large untracked trees, a state
  directory on read-only or network storage, panics inside TUI tasks, terminals other than tmux,
  and the non-Linux (`ps`-based) orphan identity path.
- The CLI e2e suite takes 25–35 s because of the stubborn-worker tests.

*Deliverable:* `aftman serve` + `aftman watch` with a second client attaching mid-run without
losing events.

| Work | Crate |
| --- | --- |
| `ProtocolServer`: NDJSON framing, request/response, subscriptions, bounded per-client channels | `aftman-daemon` |
| Unix socket, `0600`, stale-socket handling, pid lock, detach (spawn + `setsid`, not a double fork — see Status), `stop`/`status` | `aftman-daemon`, `aftman-cli` |
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
| ~~First recordings: `pi/one-shot`, `agy/print`, `claude/print`~~ — **done**, with the decoder wired into the stdout pipe; a *failed* run per backend and a `pi --mode rpc` session remain | `aftman-workers` |
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

### M8 — in-process repository observation (built)

The M3 review left one residual, and the M3 follow-up decided to accept it: a worker editing
`.git/config` *while an observation ran* could insert a clean/smudge/process filter after the
supervisor's config scan and before its `git diff`, and git has no switch that disables the filter
family. The cycle's evidence was discarded afterwards, but the program had already run.

**What was built.** Every repository read in `aftman-daemon` now goes through `gix`, replacing
`obs::git_evidence`, `untracked_excerpts`, `config_scan` and the whole `git` CLI plumbing: status
as two walks (HEAD-vs-index and index-vs-worktree), a worktree-vs-index unified
diff, a git-shaped `--stat`, `HEAD`, branch, and the untracked excerpts. The filter and diff
driver lists are built empty and the plumbing is the same `gix_status` entry point
`Repository::index_worktree_status` itself calls, with `Mode::ToGit`; `prepare_diff_command` and
`prepare_binary_to_text_cmd` are never called. There is **no CLI fallback**: if gix cannot open or
observe the repository, the cycle gets no git evidence and the existing `observation_note`.

**What it cost.** `gix 0.89` with a deliberately narrow feature set (`status`, `revision`, `sha1`,
`interrupt`) — the investigation's probe added roughly 120–140 crates to the graph. In exchange
`aftman-daemon` spawns no subprocess for observation at all. The fidelity work was the larger
part: git's `XY path` status columns, C-style path quoting, `diff --git` / `index` / `---` /
`+++` / `@@` headers and hunk bodies, and the `--stat` layout all had to be re-synthesised from
structured data. A module of ~700 lines replaced ~450 lines of CLI plumbing and config scanning.

**What it found.**
- **gix's high-level API is not safe by default.** `Repository::status()` and
  `diff_resource_cache()` build their pipeline from the repository's configuration and *do* run a
  clean filter; the empty-driver plumbing is the only safe shape. The investigation's demo proves
  both halves, and the `a_planted_*` tests here prove the plumbing.
- **The concurrency window is closed twice over.** The empty driver list means a filter name
  selects nothing; on top of that, gix snapshots configuration at open time, so a mid-observation
  insertion is not even seen. `a_filter_inserted_mid_observation_is_never_run` covers the second.
- **A huge file is the interrupt gap.** The built-in compare hashes a whole file with a throwaway
  interrupt flag, so one very large same-size file can outlive the deadline. A custom
  `CompareBlobs` refuses to hash above 32 MiB (reporting the file as changed) and a worktree file
  above 8 MiB is diffed as binary rather than read in full.
- **An in-process reader can be blocked where a killed child could not.** gix opens
  `.gitattributes`, `.gitignore`, `core.attributesFile` and `info/attributes` with a plain
  `File::open`, so a FIFO planted at one of those paths hung the blocking thread forever — the
  probe that exposed this is `a_fifo_attributes_file_cannot_block_the_observation`. Fixed by
  reading attributes and ignores from the index, overriding the two configured files to
  `/dev/null`, and refusing a non-regular `info/attributes` or `info/exclude` with a note. This is
  the general lesson: without a killable process boundary, every path gix will open has to be
  either in the worktree we already type-checked or eliminated by configuration.
- **`--stat` is more intricate than it looks.** The number column widens to fit `Bin` when any
  binary file is present, a mode-only change prints as `0`, and the insertions/deletions summary
  clauses are conditional; the tests pin all three against real git output.
- **Dead code was removed rather than left as a tempting path.** The supervisor environment helper
  (`supervisor_environment()` / `build_supervisor_environment`) existed only for the git child and
  is gone, so no unused helper invites a subprocess back.

**Scope decisions.** No rename tracking (a rename renders as a deletion plus an addition), no
submodule status, no SHA-256 support without a further feature (it fails safely with a note),
no intent-to-add diff, no `\ No newline at end of file` marker, and `changed_files` holds raw paths.
Each is named in [08-security.md §3.2](08-security.md).

### Bounding the in-process reader

The rewrite above closes code execution but cannot make an in-process read interruptible, so a FIFO
planted at a file gix opens blocks its thread for good. Two mitigations, both in the same change
that introduced the exposure:

- **Refuse the predictable paths.** `.git/index`, `HEAD`, `packed-refs`, `shallow`,
  `objects/info/alternates`, the `refs` tree and `info/attributes`/`info/exclude` are checked for
  being regular files before gix opens them. Git never writes them as anything else.
- **Bound the rest per run.** `.git/config` is read during discovery and loose objects wherever the
  diff needs them, so neither can be refused up front. Each run gets four observation slots, the
  permit is held by the blocking task rather than the awaiting future, and a refusal is a note
  rather than a queue. Per run, so one hostile repository degrades only its own supervision.

A thread that is blocked is also *not* released at exit, which matters because the runtime waits for
blocking tasks on shutdown — already handled, since the CLI calls `shutdown_timeout` before exiting.

## Dependency policy

Two decisions, both made because a check that can go red without a code change teaches
people to ignore the check column.

- **`cargo audit` gates on a pinned snapshot.** `advisories/` is a submodule pinned to one
  commit of the RustSec database, and the `audit` job runs it with `--no-fetch`, so the
  result is a function of this repository and nothing else. Refreshing is a deliberate act
  (`scripts/refresh-advisories.sh`) whose commit records which advisories arrived and what
  was done about them — a refresh nobody acts on is just a moved pin.
- **Freshness comes from the schedule, not from drift.** A weekly `audit-live` job audits
  against the live database and is deliberately *not* a gate: it runs only on the schedule,
  so it cannot block a pull request, and a failure there means "refresh the snapshot and fix
  what it finds" rather than "this change is broken".

The arrangement exists because the check is not a function of the repository: a rustls
advisory published on 2026-10-14 turned `main` red with no change here, and it was luck that
an unrelated pull request happened to be open to notice it. The snapshot is what makes a red
gate mean something; the schedule is what stops the snapshot going quietly stale.

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
