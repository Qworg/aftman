# 02 — Low-level design

This is the implementation contract. Types here are normative: if code disagrees, fix the code or
fix this document in the same change.

## 1. Workspace layout

```text
aftman/
├─ Cargo.toml                    # [workspace]
├─ AGENTS.md
├─ docs/
├─ crates/
│  ├─ aftman-core/               # domain: types, responsibilities, policy, observation, ensemble
│  ├─ aftman-jev/                # System One HTTP client; the ONLY crate that knows Jev
│  ├─ aftman-workers/            # Worker trait + backends (incl. aftman-workers::herdr)
│  ├─ aftman-store/              # events.jsonl, state.json, run index
│  ├─ aftman-daemon/             # FactoryRuntime, protocol server, transports
│  ├─ aftman-web/                # axum router + embedded assets (used by the daemon)
│  └─ aftman-cli/                # `aftman` binary: clap, client, TUI
└─ xtask/                        # optional build automation
```

Dependency direction is strictly downward. `aftman-core` depends on nothing in the workspace.

```text
aftman-cli ──► aftman-daemon ──► aftman-web
                   │  │
                   │  ├──► aftman-store
                   │  ├──► aftman-workers ──► aftman-core
                   │  └──► aftman-jev ──────► aftman-core
                   └──► aftman-core
```

One binary: `aftman`. `aftman serve` runs the daemon; every other subcommand is a client of it
(or of the embedded runtime under `--no-daemon`).

## 2. Core identifiers

```rust
pub struct RunId(Uuid);          // UUIDv7, time-ordered
pub struct WorkerId(Uuid);       // UUIDv7
pub struct AttemptId(Uuid);
pub struct EventSeq(u64);        // monotonic per run, starts at 1
pub struct AssessmentSeq(u64);   // monotonic per run
```

Time is never read from the system clock inside the domain. `Clock` is injected:

```rust
pub trait Clock: Send + Sync {
    fn now(&self) -> OffsetDateTime;          // wall clock, for stamps
    fn instant(&self) -> tokio::time::Instant; // monotonic, for elapsed/budget
}
```

`SystemClock` in the binary; `TestClock` in tests so debounce and timeout logic is deterministic.

## 3. Core domain types

### 3.1 Job and run

```rust
pub struct Job {
    pub prompt: String,                 // free-form: ticket, spec, bug report, request
    pub repo: PathBuf,                  // absolute, validated to be a directory
    pub source: JobSource,              // Cli | Web | File(PathBuf) | Daemon
}

pub struct RunConfig {                  // resolved for one run; env/config merged
    pub worker: WorkerKind,
    pub worker_model: Option<String>,
    pub verify: VerifyPolicy,           // Never | WhenAsked | Always
    pub max_workers: u32,               // default 3 (1 coder + 1 verifier + 1 retry)
    pub max_attempts: u32,              // default 2
    pub overall_timeout: Duration,      // default 2h
    pub worker_timeout: Duration,       // default 45m
    pub min_assessment_interval: Duration, // debounce, default 5s
    pub idle_poll_interval: Duration,   // silent-worker ticker, default 30s; see §7.1
    pub skip_unchanged_assessments: bool,  // default true; see §7.1
    pub max_assessments: u32,           // default 400
    pub steering_enabled: bool,
    pub max_steers_per_worker: u32,     // default 1
}

pub enum FactoryStatus {
    Queued, Routing, Running, Assessing, Stopped, Verifying,
    NeedsHuman, Finished, Failed, Cancelled,
}
```

`FactoryStatus` transitions are enforced by a single `fn transition(&mut self, to: FactoryStatus)`
with an explicit legal-transition table; an illegal transition is a bug that panics in debug and
is persisted as an `ERROR` event in release.

### 3.2 Workers

```rust
pub enum WorkerKind { Pi, Agy, Claude, Generic, HerdrPane, Simulated }
pub enum TaskKind { Code, Verify }
pub enum WorkerStatus { Pending, Running, Completed, Failed, TimedOut, Stopped }

pub struct WorkerRecord {
    pub id: WorkerId,
    pub kind: WorkerKind,
    pub task: TaskKind,
    pub mission: String,                // the prompt actually sent
    pub parent: Option<WorkerId>,       // verifier/retry lineage
    pub status: WorkerStatus,
    pub exit: Option<WorkerExit>,       // code, signal, cause
    pub started_at: Option<OffsetDateTime>,
    pub ended_at: Option<OffsetDateTime>,
    pub output: BoundedLog,             // ring buffer, 12k chars default, both streams
    pub steers: Vec<SteerRecord>,
    pub artifact_ref: Option<String>,   // herdr pane id / agent name, for the surface
}
```

`BoundedLog` is a fixed-capacity ring of `(Stream, String)` chunks plus a total-bytes counter. It
keeps both the head and the tail: the head shows what the worker was asked to do, the tail shows
where it ended up. Both are capped.

### 3.3 Observation

```rust
pub struct Observation {
    pub run_id: RunId,
    pub status: FactoryStatus,
    pub job: JobSummary,                 // prompt (capped), repo, elapsed
    pub active: Option<WorkerSummary>,
    pub history: Vec<WorkerSummary>,     // capped at config.workers_history
    pub git: GitEvidence,
    pub untracked: Vec<UntrackedExcerpt>,
    pub instructions: Option<RepoInstructions>,
    pub tests: Vec<TestResult>,
    pub events: Vec<EventSummary>,       // capped at config.events_history
    pub prior: Option<PriorDecision>,
    pub counters: Counters,
    pub evidence: BTreeMap<String, EvidenceResult>,
}

impl Observation {
    /// The JSON `state` object handed to Jev, filtered to the providers
    /// selected by `providers`. Deterministic: same observation + providers
    /// ⇒ byte-identical output.
    pub fn state_for(&self, providers: &[EvidenceId]) -> serde_json::Value;
}
```

`state_for` is the *only* place evidence becomes model input, which is what makes the request
budget and the injection surface auditable in one place.

`RepoInstructions` is read fresh on every observation and is **never persisted** — only a drift
observation about it is. This mirrors the upstream decision and keeps repository prose out of the
run log.

### 3.4 Assessment

```rust
pub struct Assessment {
    pub seq: AssessmentSeq,
    pub at: OffsetDateTime,
    pub model: String,                       // as reported by the provider
    pub by_responsibility: BTreeMap<ResponsibilityId, BTreeMap<CheckId, AnswerValue>>,
    pub usage: Usage,
    pub latency: Duration,
    pub request_ids: Vec<String>,
}

pub enum AnswerValue {
    Noul(f32),                                             // 0.0..=1.0
    Choice { choice: String, probabilities: BTreeMap<String, f32>, confidence: f32 },
    Score  { score: f32, level: Option<u32>, confidence: f32, probabilities: BTreeMap<u32, f32> },
}
```

Validation rules (a violation is a `ModelError`, never a silent clamp):

- every requested check key is present in the response;
- a `Noul` value is finite and within `0.0..=1.0` (tolerate tiny float overshoot, then clamp);
- a `Choice` selection is one of the requested criteria and the probabilities sum to ≈1;
- a `Score` is within the requested rubric range;
- unknown answer `type`s are ignored, not fatal (forward compatibility);
- unknown answer *keys* are ignored; missing *requested* keys are fatal.

**Score answer keys arrive as JSON strings.** The provider sends
`"probabilities": {"0": 0.03, "1": 0.11, …}` and `"legend": {"0": "None", …}` with *string*
object keys. `serde_json` will not deserialize those into `BTreeMap<u32, _>`; it fails with
`invalid type: string "0", expected u32`. The wire type therefore keeps
`BTreeMap<String, f32>`, and `aftman-jev` converts to `BTreeMap<u32, f32>` in the domain type,
returning `ModelError::InvalidAnswer` on a non-numeric key. `level` is **derived** (the argmax of
`probabilities`), never sent by the provider, and is `None` when the keys do not parse.

### 3.5 Remaining domain types

The types above are the ones a reader needs to follow a cycle. These are the rest of the surface an
implementer must have, given compactly. All are `serde` types, and all collections that can reach
policy or a snapshot are ordered (`BTreeMap`, `Vec`) so that replay is deterministic — **no
`HashMap` appears in any type that reaches policy or a state snapshot.**

```rust
// ---- identity newtypes -------------------------------------------------
pub struct ResponsibilityId(String);  // "core.completion"
pub struct CheckId(String);           // "implementation_complete"
pub struct CheckKey(String);          // "core.completion__implementation_complete"
pub struct EvidenceId(String);        // "git_diff", "command:tests"
pub struct RuleId(String);            // "core.worker-health:stuck"
pub struct EscalationId(Uuid);

// ---- observation carriers ---------------------------------------------
pub struct WorkerSummary { pub id: WorkerId, pub kind: WorkerKind, pub task: TaskKind,
    pub status: WorkerStatus, pub mission_excerpt: String, pub elapsed: Duration,
    pub exit: Option<WorkerExit>, pub steer_count: u32 }
pub struct WorkerExit { pub code: Option<i32>, pub signal: Option<i32>, pub cause: ExitCause }
pub enum   ExitCause { Completed, Failed, TimedOut, Terminated, LaunchFailed }
pub struct GitEvidence { pub status_short: String, pub diff_excerpt: String,
    pub diff_stat: String, pub changed_files: Vec<String>, pub truncated: bool,
    pub head_sha: Option<String>, pub branch: Option<String> }
pub struct UntrackedExcerpt { pub path: String, pub bytes: usize, pub content: String,
    pub truncated: bool }
pub struct RepoInstructions { pub file: String, pub excerpt: String, pub truncated: bool }
pub struct TestResult { pub source: TestSource, pub passed: u32, pub failed: u32,
    pub errored: u32, pub skipped: u32, pub status: TestStatus, pub raw: String }
pub enum   TestSource { WorkerOutput, Command(EvidenceId) }
pub enum   TestStatus { Passed, Failed, Errored, Mixed, Unknown }
pub struct EventSummary { pub seq: EventSeq, pub at: OffsetDateTime, pub kind: EventKind,
    pub summary: String }
pub struct PriorDecision { pub assessment_seq: AssessmentSeq, pub directive: Directive,
    pub reason: String }
pub struct Counters { pub workers_run: u32, pub attempts: u32, pub failures: u32,
    pub verifiers_run: u32, pub verifier_completed: bool, pub steers_sent: u32,
    pub assessments: u32, pub elapsed: Duration, pub consecutive_model_failures: u32 }
pub struct EvidenceResult { pub provider: EvidenceId, pub argv: Vec<String>,
    pub exit_code: Option<i32>, pub duration: Duration, pub output_excerpt: String,
    pub truncated: bool, pub observed_at: OffsetDateTime }

// ---- responsibilities and policy --------------------------------------
pub enum   Stakes { Low, High }
pub enum   SteerKind { Nudge, Refocus, Warn, Ask }
pub struct Proposal { pub responsibility: ResponsibilityId, pub rule: RuleId,
    pub directive: Directive, pub priority: u16, pub stakes: Stakes }
pub struct Suppression { pub responsibility: ResponsibilityId, pub rule: RuleId,
    pub reason: SuppressionReason }
pub enum   SuppressionReason { NotPermitted, Invariant(Invariant), HigherPriority,
    CapabilityUnavailable }
/// The hard invariants of doc 05 §6.1, named so that a suppression is
/// self-describing in the event log. Distinct from `CheckSuppression`
/// (doc 06 §5), which is about measurement quality, not proposal arbitration.
pub enum   Invariant { OverallTimeout, AttemptsExhausted, WorkerLimit, VerificationAlreadyRun,
    AssessmentBudget, ModelUnavailable, UnresolvedUncertainty,
    // No worker has ever run, so there is nothing to declare complete.
    NoWorkAttempted }
pub struct Routing { pub always: bool, pub instructions: Option<String>, pub threshold: f32 }

// ---- store ------------------------------------------------------------
pub struct RunHeader { pub id: RunId, pub created_at: OffsetDateTime, pub job: Job,
    pub config: RunConfig, pub aftman_version: String,
    pub worker_versions: BTreeMap<String, String>, pub granted_key_names: Vec<String> }
pub struct RunState { pub header: RunHeader, pub status: FactoryStatus,
    pub last_seq: EventSeq, pub counters: Counters, pub workers: Vec<WorkerRecord>,
    pub checks: BTreeMap<CheckKey, ResolvedCheck>, pub last_directive: Option<Directive>,
    pub escalations: Vec<Escalation>,
    /// Milliseconds spent in `NeedsHuman` with a control channel attached (§7.1). Subtracted
    /// from elapsed time wherever `overall_timeout` is evaluated. `#[serde(default)]`; lives
    /// here and not in `Counters`, which is serialized into the model-facing observation.
    pub human_wait_ms: u64 }
pub struct RunSummary { pub id: RunId, pub status: FactoryStatus, pub created_at: OffsetDateTime,
    pub elapsed_secs: u64, pub job_excerpt: String, pub repo: PathBuf,
    pub worker: WorkerKind, pub last_directive: Option<String> /* the directive's name */ }
pub struct RunFilter { pub status: Option<Vec<FactoryStatus>>, pub repo: Option<PathBuf>,
    pub limit: Option<usize>, pub cursor: Option<RunId> }
pub struct Escalation { pub id: EscalationId, pub kind: EscalationKind,
    pub raised_at: OffsetDateTime,
    /// `None` when the escalation is not about one check — budget exhaustion and
    /// model unavailability have no check key.
    pub check: Option<CheckKey>,
    pub ask: String, pub jev_p: Option<f32>, pub opinions: Vec<Opinion>,
    pub answered: Option<(OffsetDateTime, HumanDecision, String)> }
pub enum   EscalationKind { Check, AssessmentBudget, ModelUnavailable, WorkerBlocked }

// ---- model layer ------------------------------------------------------
pub enum   Purpose { Assessment, Routing }
pub struct Usage { pub input_tokens: Option<u64>, pub output_tokens: Option<u64> }
pub struct ModelInfo { pub name: String, pub description: String, pub release_date: String }
pub enum   Question { Noul   { instructions: Json },
    Choice { instructions: Json, criteria: BTreeMap<String, Json> },
    Score  { instructions: Json, criteria: Vec<Json> } }
pub struct Budget { pub pair_bytes: usize, pub request_bytes: usize,
    pub pair_target_bytes: usize, pub request_target_bytes: usize }

// ---- worker runtime ---------------------------------------------------
pub struct WorkerEnv(Vec<(String, String)>);   // built only by worker_environment()
pub struct SteerRecord { pub at: OffsetDateTime, pub message_excerpt: String,
    pub outcome: SteerOutcome, pub queue_depth: Option<u32>,
    /// `#[serde(default)]` = `Supervisor`, so older snapshots load.
    pub source: SteerSource }
/// Only `Supervisor` steers count against `max_steers_per_worker`: `WorkerRecord::steer_count()`
/// (what policy reads) excludes `Human` ones, and a human steer does not increment
/// `counters.steers_sent`. An operator's guidance must not use up the supervisor's budget.
pub enum   SteerSource { Supervisor, Human }
pub enum   WorkerOutcome { Completed, Failed { code: Option<i32> }, TimedOut,
    Terminated(TerminateReason), StreamError(String) }
pub struct WorkerEventSink { /* sends WorkerSignal; see §7.1 */ }
```

A property worth asserting in a test: every type reachable from `PolicyInput` and from `RunState`
contains only ordered collections, so `serde_json::to_vec` is byte-stable and snapshot comparisons
are meaningful.

Four types are **defined once, in [06-ensemble.md §5](06-ensemble.md)**, and referenced here rather
than redefined, because a duplicate definition of the same name in two documents is how the
`SuppressionReason` collision nearly happened: `Rung`, `Opinion`, `CheckSuppression`,
`ResolvedCheck`, and `HumanDecision`.

## 4. Responsibilities

```rust
pub struct Responsibility {
    pub id: ResponsibilityId,            // "core.completion"
    pub title: String,
    pub checks: Vec<Check>,
    pub rules: Vec<Rule>,
    pub routing: Routing,
}

pub struct Check {
    pub id: CheckId,                     // "implementation_complete"
    pub key: CheckKey,                   // wire name; always "<responsibility_id>__<check_id>"
    pub question: Question,              // Noul | Choice | Score, with instructions
    pub evidence: Vec<EvidenceId>,       // empty ⇒ default provider set
    pub threshold: Option<f32>,
}

pub struct Rule {
    pub id: RuleId,                      // unique; REQUIRED for every rule, including built-ins
    pub when: Condition,                 // boolean expression over check results
    pub propose: Directive,
    pub stakes: Stakes,                  // Low | High  (High opts into the ensemble path)
}

/// A parsed `when` expression. Grammar in doc 05 §2.1; implementation in
/// `aftman-core::condition`.
///
/// **Deliberately not `serde`-derived.** A condition is parsed from a rule's `when`
/// string and evaluated; it never crosses a boundary, so it needs no representation on
/// one. Deriving it matters more than it looks: the type is recursive, and serde's
/// internally-tagged form makes the compiler evaluate a trait bound it cannot resolve
/// (`overflow evaluating the requirement`). Raising `recursion_limit` does not fix that,
/// because the cycle is real rather than merely deep — which is a useful thing to
/// recognise before spending an afternoon on it.
pub enum Condition {
    Comparison { key: CheckKey, op: Comparator, value: f32 },
    Not(Box<Condition>),
    Conjunction(Vec<Condition>),
    Disjunction(Vec<Condition>),
}
pub enum Comparator { GreaterOrEqual, Greater, LessOrEqual, Less }

/// The single directive vocabulary. Every document and the code use these
/// names; `docs/05-responsibilities.md` priorities are keyed by them.
pub enum Directive {
    Continue,
    StartWorker { reason: String },
    Steer       { kind: SteerKind, reason: String },   // Nudge | Refocus | Warn | Ask
    Retry       { reason: String },
    Verify      { reason: String },                    // spawn an independent verifier
    Stop        { reason: String },
    Finish      { reason: String },
    Escalate    { reason: String, ask: String },       // escalate to a human
}
```

**Why the wire key is `__` and not `.`.** Responsibility ids contain dots
(`core.completion`), so a dot-joined key such as `core.completion.implementation_complete` cannot
be split back into its parts unambiguously. The double-underscore separator is also what the
upstream project uses, so a recorded fixture or a hand-written Jev probe stays valid.

Responsibilities are loaded from bundled TOML (compiled in via `include_str!`) and merged with a
user config directory. The TOML schema is defined in
[05-responsibilities.md](05-responsibilities.md).

## 5. Policy

Policy is pure: `fn arbitrate(input: PolicyInput) -> PolicyDecision`. Same input, same decision —
no clock, no randomness, no I/O. That property is what makes runs replayable.

```rust
pub struct PolicyInput<'a> {
    pub status: FactoryStatus,
    pub counters: &'a Counters,          // attempts, failures, verifiers run, elapsed
    pub config: &'a RunConfig,
    pub assessment: &'a Assessment,
    /// Every check, with Jev's unmodified value and whether policy may act on
    /// it. See docs/06-ensemble.md §5 — policy reads `permitted`, never a
    /// fabricated number.
    pub checks: &'a BTreeMap<CheckKey, ResolvedCheck>,
    /// Materialised by the runtime so this function needs no clock. See below.
    pub now: OffsetDateTime,
    pub active_worker: Option<&'a WorkerRecord>,
}
```

**Where the impurity lives.** An earlier revision claimed `arbitrate()` was pure while the
steer-versus-stop decision (doc 05 §6.5) depended on `within_grace`, which depends on the current
time and `worker.last_steered_at` — neither of which was in `PolicyInput`. Purity is restored by
splitting the pipeline in two, with all impurity confined to the first step:

```text
runtime (impure: clock, I/O, git)
  └─► PolicyInput          fully materialised, including `now`
        ├─► responsibilities::propose(&PolicyInput) -> Vec<Proposal>   pure
        └─► policy::arbitrate(&PolicyInput, &[Proposal]) -> PolicyDecision   pure
```

`Proposal` therefore remains `&[Proposal]` only because the runtime passes it; proposal *generation*
is also pure, so replay re-derives the same proposals from the same `PolicyInput`. `now` is
injected from the `Clock`, and on replay it is reconstructed from the event log's timestamps.

```rust
pub struct PolicyDecision {
    pub selected: Directive,
    pub considered: Vec<Proposal>,       // every proposal, with its rule id
    pub suppressed: Vec<Suppression>,    // proposals rejected by an invariant, with the reason
}
```

Arbitration is two-phase:

1. **Hard invariants** (`Guardrails`). Non-negotiable, independent of any score:
   - overall timeout exceeded ⇒ `Stop`;
   - `attempts >= max_attempts` ⇒ no `Retry`;
   - `verifiers_run > 0` ⇒ `Verify` is suppressed;
   - **`permitted` gates every rule, in both directions.** A rule fires only when
     `permitted(check) && comparator(p, t)`. A proposal whose check has `permitted == false` is
     suppressed with `SuppressionReason::NotPermitted`, no matter how far its value is from the
     threshold. `permitted` is `decisive && !contested` and is **not** `p >= t`, so a `<` rule
     behaves exactly like a `>=` rule (doc 06 §5.2). Nothing but Jev's own decisive value or an R0
     evidence refresh can set it.
   - `Finish` requires all of: `ready_to_finish` and `requirements_satisfied` and
     `tests_sufficient` above threshold **and permitted** (`permitted == true`, so each value is
     decisively outside the uncertainty band), **and**
     `(verification_completed or (needs_verification < threshold and permitted))`, **and** no
     active worker, **and** no active check is unresolved (next bullet);
   - **Any unresolved uncertainty suppresses `Finish`.** If *any* active check has a resolution of
     `UnresolvedInBand`, `SuppressedByContest`, or `Unresolved`, `Finish` is suppressed with
     `SuppressionReason::Invariant(Invariant::UnresolvedUncertainty)`.

     This closes a hole that is easy to miss. Priority arbitration inhibits a low-priority
     directive with a higher-priority rule — `quality.documentation:insufficient` (750) beats
     `core.completion:thresholds-satisfied` (700). But if that inhibitor's check is *in band*, the
     inhibitor is not permitted to fire, so the inhibition silently vanishes and `Finish` proceeds
     without its own precondition ever having looked at documentation. **Uncertainty about a
     blocker must block, not permit.**

     Stating it once, over "any active check", rather than as an extra `Finish` conjunct per
     responsibility, is deliberate: a responsibility added later cannot reopen the hole, because it
     does not have to remember to register itself as a blocker. With `ensemble.enabled = false`
     (the default) no check can ever be `UnresolvedInBand`, so this is a no-op at upstream parity.
   - `max_assessments` exhausted ⇒ `Stop` (or `Escalate` if verification is pending).

2. **Priority** among surviving proposals. The exact numeric table is authoritative and lives in
   one place: [05-responsibilities.md §6.3](05-responsibilities.md). It is a *total order*
   (priority desc → stakes desc → responsibility id → rule id), so arbitration is never ambiguous
   and is reproducible from the log alone.

   The ordering is priority-based rather than a hand-rolled conservative ladder, because the
   conservatism belongs in the guards above it: `Finish` is only ever *proposable* once every
   completion conjunct holds, and an unverified state cannot satisfy the precondition. Putting
   `Finish` at a high priority without that precondition would be the unsafe design; putting it
   at a low priority with a weak precondition would be the dishonest one.

Every proposal and every suppression is recorded in the `DIRECTIVE_SELECTED` event, so "why
didn't it stop?" is always answerable from the log.

## 6. State machine

```text
                ┌─────────┐
                │ Queued  │
                └────┬────┘
                     │ route(job)
                     ▼
                ┌─────────┐
                │ Routing │──────────────► Failed   (routing model error, bad config)
                └────┬────┘
                     │ JOB_ROUTED
                     ▼
   ┌────────────►┌─────────┐  activity / ticker  ┌───────────┐
   │             │ Running │────────────────────►│ Assessing │
   │             └────┬────┘                     └─────┬─────┘
   │                  │                                │ directive
   │  Continue/Steer  │                                ├──► Continue/Steer ──► Running
   └──────────────────┴────────────────────────────────┤
                                                       ├──► Verify    ──► Verifying ──► Running
                                                       ├──► Retry     ──► Running (new attempt)
                                                       ├──► Stop      ──► Stopped
                                                       ├──► Finish    ──► Finished
                                                       └──► Escalate  ──► NeedsHuman
                                                                              │ operator replies
                                                                              ▼
                                                                            Running

   ┌─────────┐  Retry (core.completion:retry-stopped)   ┌─────────┐
   │ Stopped │─────────────────────────────────────────►│ Running │
   └────┬────┘                                          └─────────┘
        ├──► Finish  ──► Finished
        ├──► Escalate ──► NeedsHuman
        └──► Cancel  ──► Cancelled
```

Terminal states: `Finished`, `Failed`, `Cancelled`. `NeedsHuman` is a wait state. A timeout of its
own (default: none; `--escalation-timeout` optional) is **M7 and not implemented**: today a parked run
waits for an answer, a cancel, or for every control handle to be dropped.

**`NeedsHuman` is a wait only when something can answer.** With a control channel attached
(`FactoryRuntime::drive(started, Some(control))`, §7.1) the run parks: no assessment cycles, no model
calls, no `AssessmentSkippedUnchanged` events, and the task sleeps on its channels rather than
polling. It leaves `NeedsHuman` only by an answer (`Accept`/`Recheck`/`ClaimComplete`/`Guidance` →
`Running`; `Stop` → `Cancelled`) or a cancel. A worker that completes meanwhile is still folded
into state. If every control handle is dropped while parked, the run ends as it does with no
channel at all (the `--no-daemon` path): `NeedsHuman` ends the run with the question recorded.
Time spent parked is accumulated in `RunState.human_wait_ms` and **excluded from
`overall_timeout`**, wherever it is evaluated (the post-cycle check, `time_driven_check_due`,
the observation's `elapsed_secs` that policy reads). `worker_timeout` is the backend's own and
is unaffected.

**Runtime API.** `FactoryRuntime::start(job) -> StartedRun` validates the repository and persists the
run (store header, `RunCreated`, a first snapshot) before any supervision, so a caller knows the
`RunId` and the run is addressable; a bad repository fails here, before any run state exists.
`drive(started, Option<ControlReceiver>) -> RunOutcome` then supervises it. `run(job)` is
`start` + `drive(.., None)`. A `RunControl` (`steer`, `cancel`, `answer`; cheap to clone) is created
with `RunControl::channel()`.

**No worker outlives its run.** Whenever `drive` returns, and whenever a cancel or stop reply
is sent, no worker process of the run is alive or can still come alive. Every path that ends a worker
(cancel, human `Stop`, `Directive::Stop`, supersede on `Retry`, every handle dropped while parked, the
no-channel `NeedsHuman` end, `Finished`/`Failed` with a worker live, and error returns) goes through one
`stop_worker`: it keeps the worker task's `JoinHandle`, calls `Worker::terminate` and **re-issues it
every 50 ms until the task reports** (a terminate that arrives before the backend has launched is a
no-op, so a single call can be missed), aborts the task after 30 s (backends kill their process group
when dropped), and only then flushes the final output and records the end. A new worker is never
started before the old one is reaped. A completion already in the channel wins over a termination.
`max_assessments` is a hard ceiling: once spent the model is not asked again (policy gets an empty
check set and its budget invariant escalates; `BudgetExceeded` is emitted), however the run was
resumed. A human steer is debounced like activity: it marks a cycle pending for the next
`min_assessment_interval` boundary instead of forcing one.

**Cancel is legal from every non-terminal state**, including `NeedsHuman` and `Stopped`. It
terminates the active worker with `TerminateReason::Cancelled`, emits `WorkerTerminated`
(`cause: "cancelled"`, `reason`), and `RunFinished` carries the same `reason`.

**`Stopped` is a state, not a terminal one.** `Stop` terminates the active worker; it does not end
the run. The factory sits in `Stopped` with no active worker, and on the next cycle
`core.completion:retry-stopped` may propose `Retry` at priority 800, which returns it to `Running`.
An earlier revision routed `Stop` straight to `Failed | Cancelled`, which made `Retry` — and the
whole stop-then-retry recovery path of [05-responsibilities.md §6.3](05-responsibilities.md) —
unreachable. A run reaches `Failed` only from an exhausted attempt budget or an unrecoverable
error, and `Cancelled` only from an explicit cancel or daemon shutdown.

## 7. Events

Append-only NDJSON, one envelope per line, in `events.jsonl`.

```rust
pub struct EventEnvelope {
    pub seq: EventSeq,
    pub run_id: RunId,
    pub at: OffsetDateTime,
    pub kind: EventKind,
    pub payload: serde_json::Value,      // schema per kind, versioned
}

pub enum EventKind {
    RunCreated, JobRouted, WorkerStarted, WorkerOutput, WorkerCompleted, WorkerSteered,
    WorkerTerminated, EvidenceCollected, AssessmentRequested, AssessmentCompleted,
    AssessmentSkippedUnchanged, DirectiveProposed, DirectiveSelected, EnsembleTriggered,
    OpinionRequested, OpinionRecorded, EnsembleResolved, StateChanged, EscalationRaised,
    EscalationAnswered, ModelError, RateLimited, BudgetExceeded, RunFinished, Error,
    Notification,
}
```

Variant notes, because three of these were previously cited in prose without existing here:

- `RunCreated` — payload `{repo, launch?}`. `launch` is the opaque, bounded
  `RuntimeDeps::launch` map (how the worker was launched: `permissions`, `external_sandbox`,
  `steering`); see §11. Core never reads it.
- `WorkerTerminated` — payload `{worker_id, cause, reason, ...}`. `cause: "orphaned"` (written by
  restart recovery for a worker its dead supervisor left behind) adds `pgid`, `result`
  (`killed` | `already_gone` | `unverified`) and `detail`.
- `JobRouted` — the routing decision, with each conditional responsibility's probability and
  threshold. Replaces the earlier `RESPONSIBILITIES_ACTIVATED` label
  ([05-responsibilities.md §5](05-responsibilities.md)).
- `AssessmentSkippedUnchanged` — a cycle that the content digest found unchanged (`§7.1`). Recorded
  so "why did supervision not react?" is answerable.
- `ModelError`, `RateLimited`, `BudgetExceeded` — distinct kinds rather than payloads inside `Error`,
  because a rate limit and a malformed answer are different operational events and the UI shows
  them differently. `Error` remains for internal faults only.

Payloads added for the control plane (every payload also carries `schema_version`):

| Kind | Fields |
| --- | --- |
| `WorkerOutput` | `worker_id`, `text` (only the bytes that arrived since the previous flush, at most `WORKER_OUTPUT_EVENT_CAP_BYTES` = 8192, newest kept, cut on a character boundary), `bytes` (how many new bytes arrived), `truncated` (`bytes` > `text` length), `exit` (the worker's final flush). Never emitted empty, so **the end of a worker is signalled by `WorkerCompleted` / `WorkerTerminated`, not by the presence of an `exit: true` output event** (a worker whose last bytes were already flushed has none). Every path that ends a worker (completion, cancel, `Stop`, supersede, run end) reaps it and then flushes what remains. Deliberately left out of the event summaries fed to the model: summaries carry no payload text, and output must not be echoed into the model-facing history through the log. |
| `WorkerSteered` | `source` (`supervisor` \| `human`), `worker_id`, `outcome`, `accepted`, and for a human steer `excerpt` (≤ 200 chars). |
| `WorkerTerminated` | Uniform: `worker_id`, `cause` (`stopped` for a `Stop` directive, `cancelled`, `superseded` for a `Retry`, `run_ended` when the run ended with a worker still live), `reason`. A worker that had already completed when its stop was seen is recorded by `WorkerCompleted` instead. |
| `EscalationRaised` | `escalation_id` (what `run.answer` needs), `kind`, `reason`, `ask`. |
| `EscalationAnswered` | `escalation_id`, `surface`, `decision` (the serialized `HumanDecision`), `note` (≤ 2000 chars). |
| `Notification` (`code: "guidance_not_delivered"`) | A `Guidance` answer with no active worker to receive it; recorded, not an error. |
| `RunFinished` | `status`, counts, and `reason` when something stated one (a cancel's reason, `"stopped by operator"`). |

Rules:

- events are *immutable*; corrections are new events;
- `WorkerOutput` is **not** written per line. Output is buffered and flushed as one event per
  observation cycle (plus one on worker exit), and only when new bytes arrived. This keeps the log proportional to decisions
  rather than to agent verbosity;
- aftman-generated events never enter the assessment queue;
- every event carries a `schema_version` inside `payload`.

### 7.1 What wakes the watcher

An earlier revision said a cycle begins when "a worker event enters the queue", while also saying
`WorkerOutput` is only flushed *at* a cycle. That is circular, and would have meant the supervisor
slept until the worker exited — defeating the entire premise of supervising work in flight.

The watcher selects over four sources, and persisted events are an *output* of a cycle, not its
input:

| Source | Purpose |
| --- | --- |
| **Activity channel** (`tokio::mpsc`, unpersisted) | Signalled by each stdout/stderr chunk as `BoundedLog` absorbs it. Cheap, lossy, coalesced. |
| **Lifecycle channel** | `WorkerStarted`, `WorkerCompleted`, `WorkerSteered`, `WorkerTerminated`. Completion forces an immediate cycle. |
| **Idle ticker** | `idle_poll_interval` (default 30s), deliberately **not** `min_assessment_interval`. Guarantees forward progress for a silent worker without burning the assessment budget. |
| **Control channel** | Bounded `tokio::mpsc` of `steer` / `cancel` / `answer` requests from a `RunControl` handle, each carrying a `oneshot` reply. Optional: absent for `run()` / `--no-daemon`. |

The control channel is **drained without blocking at the top of every iteration**, for the same
reason as the completion channel (below), and is also an arm of the wait `select!`, so an idle run
reacts immediately. A request is applied between cycles, never inside one (a model call in flight
is not interrupted), and its reply is sent only after the effect is applied **and persisted**. A
request to a run that has ended gets `ControlError::RunEnded` (the loop dropped the reply slot), never
a hang. A human steer invalidates the observation digest and requests a cycle: it "does not skip
the assessment loop" ([07-surfaces.md §10](07-surfaces.md)).

`ControlError` is what `RunManager` maps to the §11 codes (`manager::control_error`):
`WrongState(msg)` → `wrong_state`; `RunEnded` → `wrong_state` ("the run has ended");
`UnknownEscalation` → `wrong_state` (the run exists; the escalation is unknown or already answered);
`Invalid(msg)` → `invalid_request`; `Internal(msg)` → `internal`. `unknown_run` is decided before
the run is asked, from the store: an id it has never seen. A backend that
refuses or cannot take a human steer is a successful reply carrying `Rejected` / `Unsupported`
(the protocol layer turns `Unsupported` into `steering_unsupported` if it wants a hard error).

Debounce rules:

- activity signals are coalesced within `min_assessment_interval` (default 5s);
- **the completion channel is drained without blocking at the top of every iteration,**
  independently of which trigger fired. Observing a worker's completion only inside the wait
  branch is a deadlock: if a cycle outlasts the idle interval — which a real model call easily does
  — `idle_due` is true on every iteration, the wait branch is never reached, the completion is never
  seen, and the runtime supervises a worker that no longer exists. Implemented in M0; the bug was
  found by an integration test whose idle interval was shorter than its cycle;
- **an idle tick only runs a cycle if there is something to decide.** The condition is
  `content_changed || time_driven_check_due`, not `content_changed` alone:
  - `content_changed` compares an **`observation_content_digest`**: BLAKE3 over the canonical
    serialization of the observation with every volatile scalar removed — `elapsed` durations,
    `*_at` timestamps, and `EventSeq`. This is **not** the ensemble's `state_digest`, which must
    cover the exact bytes a judge saw and therefore keeps them
    ([06-ensemble.md §8](06-ensemble.md)). An earlier revision hashed `state_for(...)` directly;
    because that serializes `elapsed`, the digest changed on every tick, making the skip dead code
    and letting a silent worker burn the whole assessment budget anyway;
  - `time_driven_check_due` is true when a time-based boundary will trip within the next tick:
    `overall_timeout`, `worker_timeout`, the steering grace window, or (M7, not built) an escalation timeout.
    Without this, the skip could suppress the one cycle that would notice a timeout;
  - a skipped cycle records `AssessmentSkippedUnchanged` and consumes no assessment budget;
- while parked in `NeedsHuman` there is no cycle to flush on, so `WorkerOutput` is flushed on the
  **idle interval** instead (at most once per `idle_poll_interval`). This is the one stated exception
  to "the log is proportional to decisions": a chatty worker under a parked run costs one event per
  idle interval, not one per debounce interval;
- `WorkerOutput` is flushed only when new bytes actually arrived, so an idle tick never emits an
  empty output event;
- a worker-completion event always forces an immediate cycle, regardless of the interval, after
  command evidence providers have run;
- a cycle in progress is never interrupted; the next signal starts the next cycle;
- the ticker alone is enough to make progress, so a worker that prints nothing still gets
  supervised.

The activity channel carries no payload and is never persisted. This is what keeps the log
proportional to *decisions* (`WorkerOutput` once per cycle) while still reacting to output within
milliseconds.

`state.json` is a full snapshot, written by atomic rename (`state.json.tmp` → `fsync` →
`rename`), containing the last `seq` it includes. On startup, state is loaded from `state.json`
and validated against the tail of `events.jsonl`; a mismatch with no trailing events means
corruption and aftman refuses to continue rather than guessing.

## 8. Run store

```text
$XDG_STATE_HOME/aftman/            (default ~/.local/state/aftman)
├─ runs/
│  └─ <run-id>/
│     ├─ meta.json                 # job, repo, config at start (redacted)
│     ├─ state.json                # latest snapshot, atomic
│     ├─ events.jsonl              # append-only
│     ├─ run.lock                  # advisory flock held by the process driving the run
│     ├─ processes.json            # worker processes launched and not yet reaped (docs/04 §11)
│     └─ artifacts/                # diffs captured at decision points, evidence blobs
├─ index.sqlite                    # optional: run list queries when the dir grows
└─ jev-usage.jsonl                 # one line per model request
```

`RunStore` API:

```rust
#[async_trait]
pub trait RunStore: Send + Sync {
    async fn create(&self, run: &RunHeader) -> Result<()>;
    async fn append(&self, event: &EventEnvelope) -> Result<()>;
    async fn snapshot(&self, state: &RunState) -> Result<()>;
    async fn load(&self, id: RunId) -> Result<RunState>;
    async fn events(&self, id: RunId, from: EventSeq) -> Result<Vec<EventEnvelope>>;
    /// A page: up to `limit` events at or after `from`, and whether more follow. The file
    /// store answers from a ranged read that parses only the page (see below).
    async fn events_page(&self, id: RunId, from: EventSeq, limit: usize) -> Result<EventsPage>;
    async fn list(&self, filter: &RunFilter) -> Result<Vec<RunSummary>>;
    async fn read_log(&self, id: RunId) -> Result<EventLogRead>;   // reports a torn final line

    /// Delete one run's data completely (`aftman purge`). No soft delete.
    async fn delete(&self, id: RunId) -> Result<()>;
    /// Every run id held, including runs `list` skips because they cannot be loaded.
    async fn ids(&self) -> Result<Vec<RunId>>;
    /// Exclusive ownership of an existing run; `Ok(None)` when someone else holds it,
    /// `NotFound` when it does not exist (never creates anything).
    async fn claim(&self, id: RunId) -> Result<Option<RunClaim>>;
    /// Ownership of a run that does not exist yet, before `create` (creates the directory).
    async fn claim_new(&self, id: RunId) -> Result<Option<RunClaim>>;
    /// Hints that avoid reading whole logs (defaults read everything):
    async fn snapshot_status(&self, id: RunId) -> Result<FactoryStatus>;
    async fn ends_with_run_finished(&self, id: RunId) -> Result<bool>;
    /// Record / clear the process a worker launched, so recovery can end it (docs/04 §11).
    async fn set_worker_process(&self, run: RunId, worker: WorkerId, p: Option<ProcessIdentity>)
        -> Result<()>;
    async fn worker_processes(&self, run: RunId) -> Result<Vec<(WorkerId, ProcessIdentity)>>;
}
```

**Ranged paging.** `events_page` does not parse the log outside the page. Event `seq` N is the
Nth event line of a well-formed log, so the lines before the page are skipped by counting (blank
lines are not events, as in `parse_event_log`) and the first line of the page must carry
`seq == from` — which, because `append` writes strictly increasing `seq`s, proves what was
skipped. If that check fails, or a line is too long to buffer (8 MiB), the store falls back to the
full parse. Memory per call is the page (reading stops after `limit + 1` events); a malformed line
inside the page is corruption unless it is the final line (a torn append); corruption in the
skipped region is not looked for. `run.events` uses it, so paging a long run costs a page per call.

**Run ownership.** Exactly one process may write to a run's log. The process that drives a run
holds `claim(id)` for the run's lifetime: an exclusive, non-blocking `flock` on `run.lock` in the
run directory (`MemoryRunStore`: in-process bookkeeping). The kernel releases it if the process
dies, so there is no stale-lock cleanup. `FactoryRuntime::start` takes `claim_new` *before*
`create`, and the `StartedRun` carries it until `drive` returns (or the value is dropped), so a
`--no-daemon` run and a daemon-driven run are protected identically, and there is no moment at
which a run directory is visible but unowned. Everything that would write to a run it does not
drive (daemon restart recovery, `run.purge`) takes the claim first and skips the run if it
cannot; that is how a daemon never marks `Failed` a run that is alive in another terminal's
`--no-daemon`. Only `claim_new` creates (the run directory, `0700`); `claim` on a run that does
not exist, or that was deleted while the lock was being taken (the inode is re-checked after
locking), is `NotFound`. A failed `start` deletes what it created, so no `Queued` run is left
that nobody drives.

**Deleting.** `delete` renames the run directory to a `runs/.deleted-*` tombstone (after which
`claim` and `ids` cannot see the run) and then removes the tombstone, so two concurrent purges
cannot both own, or both half-delete, a run. Directories aftman creates (`<root>`, `runs/`,
`runs/<id>/`) are `0700`; a pre-existing root is not chmodded.

**Writes are serialized per run, on blocking threads.** `append` and `snapshot` each run as one
`spawn_blocking` closure holding a per-run mutex, so a write already in flight when its caller
is aborted still finishes *before* the next writer starts. Two guards cover the rest: an
`append` whose `seq` is not greater than the log's last fails with `SeqConflict` (the daemon
re-reads and continues), and a `snapshot` older than the stored one is dropped.

**Torn tails.** `append` repairs the end of a log before writing: a last byte that is not `\n`
is either a complete final line (a newline is added) or a fragment from an interrupted append
(it is truncated, with a warning), so a crash mid-append never turns into corruption in the
middle of the log.

`FileRunStore` (JSONL, the default):
- replays the last N events on load to rebuild in-memory state;
- uses `index.sqlite` only for `list` when more than 500 runs exist, and can be deleted at any
  time without losing data.

`MemoryRunStore` for tests.

## 9. Worker protocol

```rust
#[async_trait]
pub trait Worker: Send + Sync {
    fn kind(&self) -> WorkerKind;
    fn supports_steering(&self) -> bool;
    fn artifact_ref(&self) -> Option<String> { None }

    async fn run(&self, ctx: WorkerCtx, emit: WorkerEventSink) -> Result<WorkerOutcome>;
    async fn steer(&self, message: &str) -> Result<SteerOutcome>;
    async fn terminate(&self, reason: TerminateReason) -> Result<()>;
}

pub struct WorkerCtx {
    pub id: WorkerId,
    pub repo: PathBuf,
    pub mission: String,
    pub timeout: Duration,
    pub model: Option<String>,
    pub allowed_tools: Option<Vec<String>>,
    pub env: WorkerEnv,                  // scrubbed; see §10
}

pub enum SteerOutcome { Delivered, Queued, Rejected(&'static str), Unsupported }
pub enum TerminateReason { Timeout, Cancelled, Stopped, Superseded }
```

Requirements for every backend:

- the child runs in its **own process group** (`process_group(0)` on Unix) so termination reaches
  grandchildren;
- stdout and stderr are read concurrently and feed `BoundedLog`;
- termination is **interrupt, then grace, then kill**: `SIGINT`, wait `terminate_grace` (default
  5s), `SIGKILL` the group;
- launch failure is surfaced as `WorkerStatus::Failed` with the resolved argv and the scrubbed env
  *keys* (never values) in the event;
- `supports_steering` is honest. A backend that cannot accept mid-turn input returns
  `SteerOutcome::Unsupported`, and policy degrades to stop/retry.

Backend specifics, commands, and the steering matrix are in [04-workers.md](04-workers.md).

## 10. Worker environment

```rust
pub fn worker_environment(extra: &[(String, String)]) -> Vec<(String, String)>;
```

- inherits the daemon's environment;
- **removes** every key matching `TYPESAFE_*` and `AFTMAN_*`;
- removes `OPENROUTER_API_KEY`, `ANTHROPIC_API_KEY`, `OPENAI_API_KEY` unless the run config
  explicitly grants them to the worker (off by default — a coding agent normally authenticates
  itself through its own CLI);
- applies `extra` last.

## 11. Daemon protocol

One message schema, two transports. NDJSON over a Unix socket; the same messages over HTTP. The
schema is `aftman_daemon::protocol` (types and pure encode/decode only); event fan-out is
`aftman_daemon::hub`. Neither knows about sockets.

Request/response (NDJSON, one JSON object per line, `id` echoes). `error` is `null` on success;
on failure `result` is omitted:

```jsonc
{"id":1,"method":"run.start","params":{"repo":"/abs/path","job":"...","worker":"pi"}}
{"id":1,"result":{"run_id":"0193..."},"error":null}
```

Event push (server -> client, no `id`):

```jsonc
{"event":{"seq":42,"run_id":"0193...","kind":"DIRECTIVE_SELECTED","at":"...","payload":{...}}}
```

Out-of-band error push (server -> client, no `id`; also the reply to a line whose `id` could not
be parsed). A slow subscriber is dropped with this, after receiving a correct gap-free prefix:

```jsonc
{"error":{"code":"subscriber_lagged","message":"...resume from seq 9","retryable":true,
          "data":{"run_id":"0193...","resume_from":9}}}
```

`params` must be a JSON object (or `null`/absent for "none"); an array or scalar is
`invalid_request`. A client must tolerate an error `code` it does not know (it decodes as
`unknown`). The `decision` of `run.answer` is decoded strictly (unknown fields are rejected;
guidance text must be non-empty). The optional `surface` of `run.answer` names the surface the
answer comes from and is recorded in `ESCALATION_ANSWERED`; it must be one of `cli`, `tui`, `web`,
`herdr` (anything else is `invalid_request`), and absent means the transport's own default
(`cli` on the Unix socket).

Methods (every params struct rejects unknown fields; a method with no params accepts `{}`,
`null`, or no `params`):

| Method | Params | Result |
| --- | --- | --- |
| `daemon.status` | none | `{version, uptime_secs, pid, runs_total, runs_active}` |
| `daemon.stop` | none | `{ok}` (graceful shutdown) |
| `run.start` | `repo, job` + optional `worker, cmd, permissions, effort, steer, external_sandbox, model, verify, ensemble, simulate_model` (the flags of `aftman run`) | `{run_id}` |
| `run.list` | `status?[], limit?, cursor?` | `{runs: RunSummary[], next_cursor}` |
| `run.get` | `run_id` | `RunState` |
| `run.events` | `run_id, from_seq?, limit?` | `{events: EventEnvelope[], has_more}` |
| `subscription.open` | `run_id?, from_seq?` | `{run_id, from_seq, replaced}`, then streamed `event` pushes |
| `subscription.close` | none | `{closed}` |
| `run.steer` | `run_id, message` | `SteerOutcome` |
| `run.cancel` | `run_id` | `{ok}` |
| `run.answer` | `run_id, escalation_id, decision, note?, surface?` | `{ok}` |
| `run.purge` | exactly one of `run_id` or `all: true` | `{purged}` |
| `responsibility.list` | none | `{responsibilities: [{id, description, checks[]}]}` |
| `jev.usage` | `since?` (RFC 3339), `run_id?` | aggregated token usage (opaque to the protocol crate) |

Additive fields (all `#[serde(default)]`, so mixed versions decode): `subscription.open` also
returns **`live`**, whether *this daemon is driving the run*, so events will keep arriving. It is
false for a run that has ended, one that is alive in another process (`aftman run --no-daemon`),
or one that is non-terminal but orphaned; the history is still served, but a client that wants to
follow such a run polls `run.events`. It is always true for an all-runs stream. `daemon.status`
also returns **`state_dir`** (the directory the daemon serves, so a client can notice its own
`--data-dir` differs), **`started_at`** (RFC 3339, from the injected clock) and **`info`** (free-form
strings from the composition root, at most 32 entries, keys at most 64 bytes, values at most 256).
`run.start` requires an absolute `repo` (`invalid_repo` otherwise): a relative path would be
resolved against the daemon's working directory, not the client's.

**`RUN_CREATED` payload.** `{repo, launch?}`. `launch` is an opaque, bounded map (at most 32
entries, keys 64 chars, values 256) that the composition root supplies in `RuntimeDeps::launch`:
how the worker was launched, for example `permissions`, `external_sandbox`, `steering`. It is a
description for the record, never an input to a decision, and core knows nothing about launch flags;
it exists so an `auto-approve` run is distinguishable from a sandboxed one in the log.

`decision` is `aftman_core::assessment::HumanDecision` (`{"decision":"accept"}`,
`{"decision":"guidance","text":"..."}`, ...). `run.purge` backs `aftman purge <run-id>` and
`aftman purge --all` ([08-security.md](08-security.md)). List-style results are objects, not bare
arrays, so a cursor or flag can be added without a breaking change.

**Subscriptions.** `subscription.open` with `run_id` delivers every event of that run with
`seq >= from_seq` (default: the first) exactly once, in order, whether the run is live, finished,
or has not emitted yet. Without `run_id` it is a live-only stream of all runs; `from_seq` without
`run_id` is `invalid_request` (sequence numbers are per run). **One subscription per
connection:** event pushes carry no subscription id, so overlapping subscriptions would be
indistinguishable duplicates. A second `subscription.open` replaces the first
(`replaced: true`); a client that wants two streams opens two connections.

**Backpressure.** Each subscriber has a bounded queue (`HubConfig::subscriber_capacity`, default
1,024). `emit` never blocks. If the queue is full the subscriber is removed, it still receives
what was already queued, and then gets the `subscriber_lagged` push. For a single-run subscription
`data.resume_from` is the next `seq` it has not seen and the client reconnects and resumes from
there; for an all-runs subscription (live-only, no replay) `data.run_id` and `data.resume_from`
are both `null` and the client simply re-subscribes.

**Abandoned runs.** If the daemon stops driving a run and cannot record its end (the store keeps
failing: a read-only file, a full disk, the directory removed underneath), every subscriber
scoped to that run is sent one id-less error push and its stream ends:

```jsonc
{"error":{"code":"run_abandoned","retryable":false,
          "message":"the daemon stopped driving run 0193... and could not record its end: <reason>",
          "data":{"run_id":"0193...","reason":"<reason>"}}}
```

It follows the events that were already queued, nothing is sent after it on that subscription,
and the connection stays open (like `subscriber_lagged`). It is **not retryable**: the run's record
is unfinished until a daemon restart repairs it, so a client should report an error rather than
wait. A subscriber that opens later (`subscription.open` for that run) receives the history and
then the same push; the hub remembers the last 256 abandoned runs, until the run is forgotten
(`run.purge`). All-runs streams are not ended by it. The reason is at most 512 bytes. The same
applies at shutdown to a run task that cannot be stopped: the daemon records `Failed` itself (it
holds the claim), and abandons the run only if that fails too.

**Replay.** The hub keeps a bounded ring (`HubConfig::replay_capacity`, default 10,000 events
across all runs, oldest evicted first) and falls back to `RunStore::events` when the ring does not
reach back to `from_seq`. Correctness depends on the runtime appending to the store *before* it
emits, and emitting one run's events in `seq` order; the argument is in the `hub` module docs.
If the ring does not cover `from_seq` and the store does not have the run, the subscribe fails
with `unknown_run` rather than returning a silent gap. `EventHub::forget(run_id)` (used by
`run.purge`) drops a run's ring contents and cleanly ends its run-scoped subscribers. Shutdown
(`close_all`, or dropping the last hub handle) is sticky: streams end cleanly and later
subscribes fail. See §15 for the cost of the store fallback.

Errors are structured (`data` is optional and omitted when absent):

```jsonc
{"id":7,"error":{"code":"invalid_repo","message":"not a directory: /x","retryable":false}}
```

Codes: `invalid_repo`, `invalid_config`, `unknown_run`, `wrong_state`, `steering_unsupported`,
`model_unavailable`, `rate_limited`, `internal`, plus `invalid_request` (malformed JSON, bad
envelope, unknown or missing params, oversize field), `unknown_method`, and the two out-of-band
push codes `subscriber_lagged` and `run_abandoned`.
A request that fails to decode still gets a response echoing its `id` when one could be parsed;
otherwise the reply is an id-less error push.

**Bounds** (named constants in `protocol`; exceeding one is `invalid_request`):

| Constant | Value |
| --- | --- |
| `MAX_REQUEST_LINE_BYTES` | 1 MiB |
| `MAX_JOB_BYTES` | 256 KiB |
| `MAX_STEER_BYTES` | 16 KiB |
| `MAX_ANSWER_BYTES` (note, guidance text) | 16 KiB |
| `MAX_FIELD_BYTES` (worker, model, cmd, repo, ...) | 4 KiB |
| `run.events` `limit` | default 500, max 5,000 (larger is clamped, 0 is an error) |
| `run.list` `limit` | default 50, max 500 (larger is clamped, 0 is an error) |

Framing: one message is exactly one line terminated by a newline; JSON string escaping guarantees
a payload containing newlines stays on one line.

**Method semantics** (`aftman_daemon::manager::RunManager`, one method each; transports add
framing only):

- `run.start` validates, asks the injected `RuntimeFactory` to build a `FactoryRuntime` and `Job`
  (the factory can answer `invalid_repo`, `invalid_config`, `model_unavailable`), calls
  `FactoryRuntime::start` (so the run is persisted and its id known), spawns `drive`, and replies
  with the id. More than `ManagerConfig::max_live_runs` (default 16) live runs is `rate_limited`
  (retryable). The work runs in its own task, so a client that hangs up, or a panic in the
  factory, cannot strand a half-started run or leak a slot. A run that is not live is never left
  non-terminal: if `drive` errors, panics, or is aborted, the manager's supervisor records
  `StateChanged` + `RunFinished` (`status: failed`, reason `run failed: ...`, `run task
  panicked`, `run task aborted`) and a snapshot, and workers still recorded `Running` are closed
  out first (a worker is aborted by `Drop` when its run task ends by any route). Recording is
  retried with backoff a bounded number of times; if the store keeps failing it is logged loudly
  and the next start's recovery is the last resort, so the guarantee is "never left non-terminal
  while the store works, and repaired at the next start otherwise". Events that reached the log
  but not the hub are replayed to the hub before the manager appends its own, so subscribers
  never see a gap.
- `run.steer`, `run.cancel`, `run.answer` act only on a run that is live in this daemon. An id
  the store has never seen is `unknown_run`; a known run that is not live, or is in the wrong state
  (no active worker, no open escalation), is `wrong_state`; a malformed message is
  `invalid_request`. A steer to a backend with no input channel is `steering_unsupported`; a
  backend that *rejects* the message is a normal result (`{"outcome":"rejected",...}`). The surface
  recorded with an answer comes from the transport; the socket server records `"cli"`.
- `run.purge` deletes ended runs (`all: true` skips live ones and reports how many it deleted;
  one run failing does not stop the rest). A run that is live here, or whose ownership claim
  another process holds, is refused with `wrong_state`. A run whose `RUN_FINISHED` has been
  emitted (or whose snapshot is already terminal) is over: purge waits, bounded, for its task to
  return instead of refusing. The claim is held across the delete.
- **`RUN_FINISHED` means over.** The runtime writes the final event as append, then final
  snapshot, then emit, so a subscriber that reacts to `RUN_FINISHED` with `run.get` sees the
  terminal status; `runs_active` no longer counts the run. Every manager method validates its
  own parameters (a direct caller such as the HTTP transport cannot bypass a bound).
- `daemon.status`: `runs_total` counts every run directory, `runs_active` the runs this process
  is supervising; `uptime_secs` is monotonic.

**Socket server** (`aftman_daemon::server`). Per connection: a bounded line reader (never more than
`MAX_REQUEST_LINE_BYTES` of an unfinished line is buffered; a longer line gets an `invalid_request`
push and the connection is closed), one task per in-flight request (at most
`ServerConfig::max_in_flight`, 32), the subscription forwarder, and a single writer task fed by a
bounded queue (`outbound_queue`, 256) with a per-write timeout (`write_timeout`, 5 s). Responses
echo `id` and may arrive out of order; `subscription.open`/`close` and `daemon.stop` are handled
in request order. A client that stops reading fills its kernel buffer, then its queue, then its hub
channel (it is marked lagged; the run never waits), and the write timeout then closes the
connection. Over `max_connections` (64) a new connection receives a `rate_limited` push and is
closed. A connection with no subscription and no request in flight is closed after
`idle_timeout` (60 s), so idle clients cannot occupy every slot and lock out `aftman stop`.
`max_in_flight` bounds the requests *executing* on one connection (the reader stops reading it
until one finishes); it does not bound the total across connections (`max_connections` times
it) or the responses queued for a slow reader (that is `outbound_queue` and `write_timeout`).
A client that half-closes its write side (`nc -N`, `socat`) still receives its in-flight
responses and keeps an open subscription until it, or the writer, ends. `daemon.stop` replies
`{"ok":true}` and then starts the graceful shutdown in [07-surfaces.md section 1](07-surfaces.md).

HTTP mirror (used by the web UI and any remote client):

```text
GET    /v1/status
POST   /v1/runs
GET    /v1/runs
GET    /v1/runs/{id}
GET    /v1/runs/{id}/events          # SSE; supports Last-Event-ID for resume
POST   /v1/runs/{id}/steer
POST   /v1/runs/{id}/cancel
POST   /v1/runs/{id}/answer
GET    /v1/responsibilities
GET    /v1/usage
GET    /                              # embedded UI
```

SSE event ids are `EventSeq` values, so a browser that reconnects resumes exactly.

## 12. Configuration

Precedence: CLI flag > environment > project config > user config > bundled default.

```toml
# ~/.config/aftman/config.toml

[jev]
model = "jev-latest"
base_url = "https://api.typesafe.ai"     # or https://openrouter.ai/api
timeout_seconds = 10
max_retries = 2
max_assessments_per_run = 400

[jev.budget]
pair_bytes = 20000          # one evidence-value pair
request_bytes = 120000      # the whole state object
pair_target_bytes = 12000
request_target_bytes = 60000

[obs]
diff_chars = 20000
tail_chars = 12000
events_history = 30
workers_history = 10
untracked_files = 3
untracked_bytes = 4096

[runtime]
# The debounce interval and the idle poll interval are separate on purpose: a
# single 5s ticker would let a silent worker exhaust the assessment budget well
# inside its own timeout. See §7.1.
min_assessment_interval_ms = 5000
idle_poll_interval_ms = 30000
# Skip the model call when the observation digest is unchanged.
skip_unchanged_assessments = true

[workers]
default = "pi"
terminate_grace_ms = 5000

[ensemble]
enabled = false             # see docs/06-ensemble.md; false ⇒ exact upstream thresholds
uncertainty_margin = 0.15
confidence_floor = 0.60
framing_tolerance = 0.15
max_opinions_per_run = 20
max_evidence_refreshes_per_check = 3
opinion_timeout_seconds = 120
per_check_cooldown_cycles = 10
consecutive_contests_before_human = 2
rungs = ["refresh_evidence", "reframe_same_model", "independent_judge", "human"]

[ensemble.judge]
backend = "agy"
model = "..."
tools = "none"              # enforced; not configurable in v1

[herdr]
enabled = "auto"            # auto | always | never

[web]
enabled = true
bind = "127.0.0.1:8787"
auth = "loopback"           # loopback | token | none
# Required when auth = "token". Canonicalised, symlink-resolved prefixes that
# POST /v1/runs may use as `repo`. Without this, a token holder could point a
# run at any directory the daemon user can read. Defaults to $HOME when
# auth = "loopback".
allowed_repo_roots = ["/home/me/Development"]
```

**There is no `[policy]` threshold block.** Thresholds are owned by the responsibility definitions
(doc 05), which are the single source of truth for `min_threshold`, so that changing a check and
changing its threshold is one edit in one file. An earlier revision listed a second set of
thresholds here (`finish_threshold = 0.85`, `stuck_threshold = 0.70`, …) that contradicted the
definitions and would have been either dead config or a silent override. `FactoryConfig` carries no
semantic thresholds at all.

`TYPESAFE_BASE_URL` must be `https://…` **or** a loopback address (`http://127.0.0.1:…`,
`http://localhost:…`). A cleartext remote base URL would send the supervisor key over the network in
the clear, so it is rejected at startup rather than warned about.

Environment variables use the `AFTMAN_` prefix with underscores
(`AFTMAN_JEV_MODEL`, `AFTMAN_WEB_BIND`, …), except the Jev credentials, which keep their
upstream names for compatibility: `TYPESAFE_API_KEY`, `TYPESAFE_BASE_URL`.

Config is parsed with `deny_unknown_fields` so a typo is an error, not a silent default.

**Status (M3): the file above is the design; no `config.toml` is read yet.** What exists today:
CLI flags and their environment variables (`AFTMAN_DATA_DIR`, `AFTMAN_SOCKET`, the Jev
credentials `TYPESAFE_*`), the bundled responsibility definitions and evidence plan (TOML,
parsed with `deny_unknown_fields`), and compiled defaults for the sections above: `[obs]` is
`ObsCaps::default()`, `[runtime]` is `RunConfig::default()`, `[jev.budget]` is the budget
module's constants. Every key in the block is **designed but not yet read**, including
`[workers]`, `[ensemble]`, `[herdr]` and `[web]` (the web surface is M4). Do not rely on a key
here having an effect until the milestone that builds it lands.

## 13. Logging and observability

- `tracing` with a `--log-level` flag; JSON logs when not attached to a TTY.
- Never log: `TYPESAFE_API_KEY`, `Authorization`, cookie values, or full untracked file contents.
- Log the `x-typesafe-request-id` of every Jev request at `info`; it is also persisted in the
  assessment event, which makes a bad answer reportable.
- `aftman doctor` checks: config parse, API key presence *and* a live one-question probe (opt-in),
  worker binary availability and version, git presence, herdr presence, socket permissions.

## 14. Testing strategy

| Layer | How it is tested |
| --- | --- |
| Policy | Pure table-driven tests: input cases → expected directive, including every suppressed proposal |
| Responsibilities | TOML parse + rule evaluation unit tests; snapshot of the flattened question set |
| Observation | Fixture repositories (tiny git repos built in a tempdir) → byte-identical `state_for` snapshots |
| Jev client | `wiremock` HTTP fixture: happy path, 429 with `retry-after-ms`, 500, timeout, missing answer, extra unknown answer type, malformed float |
| Workers | Fake executables (shell scripts) that emit known stdout/stderr and exit codes; assert argv, env scrubbing, streaming, graceful termination |
| Runtime | `tokio::time::pause` + `SimulatedWorker` + `ScriptedDecisionModel`; assert event sequence and directives |
| Ensemble | Scripted opinions → deterministic resolutions, including the "disagreement escalates" cases |
| Store | Crash-consistency: truncate `events.jsonl` mid-line and assert recovery behaviour |
| Protocol | In-process client against a real socket; HTTP mirror against the same handler |
| End-to-end | One offline scenario per surface: CLI run, daemon run with two clients, web SSE reconnect |
| Live (gated) | `#[ignore]` tests behind `AFTMAN_LIVE=1` for a real Jev request and a real worker binary. Never in CI |

The default `cargo test --workspace` must pass with **no environment variables set** and **no
network access**.

## 15. Performance and limits

- One Jev request per evidence group per cycle; the debounce interval defaults to 5s, so a
  one-hour job makes at most ~720 requests if every cycle fires, typically far fewer.
- Request size is bounded by the budget module, so cost is bounded by *cycles*, not by repository
  size.
- Observation assembly (git + filesystem) never blocks the run loop and is bounded by one 8 s
  deadline (`obs::OBSERVATION_TIMEOUT`). Git runs as a child process (killed, with its process
  group, on timeout; output capped at 256 KiB per command); file reads happen on blocking threads,
  only for regular files opened `O_NONBLOCK | O_NOFOLLOW` (a FIFO or symlink named `AGENTS.md`, or
  among the untracked files, is skipped), with a byte cap enforced while reading. On timeout the
  repository evidence of that cycle is omitted and `evidence["observation_note"]` says so; the
  cycle still runs and a cancel is answered. Every git is hardened as described in
  [08-security.md section 3.2](08-security.md).
- The daemon holds at most one `WorkerRecord::output` ring per run; ended runs drop their logs to
  disk and keep only summaries in memory. Default in-memory budget: 50 MB, configurable.

- **Known limit, event replay from disk.** When a subscriber's `from_seq` is older than the
  in-memory ring, `RunStore::events` returns the whole remaining log as one `Vec`. Memory per such
  subscriber is proportional to the remainder of the run's log, and N simultaneous deep resumes
  cost N copies. The read is off the hub lock, so a run is never delayed by it. Bounding this
  needs a paged `RunStore::events`; deferred.

## 16. Extension points (deliberately small)

| Trait | Implement to |
| --- | --- |
| `Worker` | Add a coding agent backend |
| `DecisionModel` | Swap or wrap the supervisory model |
| `SecondOpinion` | Add an ensemble opinion source (doc 06) |
| `EvidenceProvider` | Add a command-based evidence collector (doc 05) |
| `RunStore` | Change persistence |

Anything not on that list is not pluggable in v1. In particular there is no plugin loader, no
remote extension registry, and no scripting layer inside the daemon.
