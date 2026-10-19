//! The run loop.
//!
//! Two loops run concurrently, and the design point is that the second one does not
//! stop the first:
//!
//! - the **worker** runs as an independent task, streaming output into a shared
//!   bounded log and signalling as it goes;
//! - the **watcher** wakes on activity, on worker completion, or on an idle tick,
//!   assembles a bounded observation, asks the model, arbitrates, and applies
//!   exactly one directive.
//!
//! Persisted events are an *output* of a cycle, never its input. An earlier design
//! said a cycle begins when a worker event enters the queue while also saying output
//! events are only flushed at a cycle — which is circular, and would have meant the
//! supervisor slept until the worker exited. See `docs/02-lld.md` §7.1.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot};

use aftman_core::activity::StreamActivity;
use aftman_core::assessment::{HumanDecision, ResolvedCheck, Rung};
use aftman_core::clock::Clock;
use aftman_core::definitions::DefinitionSet;
use aftman_core::directive::{Directive, Invariant, Proposal, SuppressionReason};
use aftman_core::error::{ModelError, TransitionError};
use aftman_core::event::{EventEnvelope, EventKind};
use aftman_core::evidence::{EvidenceOutcome, EvidencePlan, RunWhen};
use aftman_core::ids::{CheckKey, EscalationId, EventSeq, RunId, WorkerId};
use aftman_core::job::{FactoryStatus, Job, RunConfig, TaskKind, WorkerStatus};
use aftman_core::log::BoundedLog;
use aftman_core::model::{DecisionModel, Purpose, Question};
use aftman_core::observation::{EventSummary, Observation};
use aftman_core::policy::{PolicyInput, arbitrate};
use aftman_core::responsibility::ResponsibilityRegistry;
use aftman_core::routing;
use aftman_core::run::{AnsweredEscalation, Escalation, EscalationKind, RunHeader, RunState};
use aftman_core::worker::{
    SteerOutcome, SteerRecord, SteerSource, TerminateReason, Worker, WorkerCtx, WorkerError,
    WorkerEventSink, WorkerOutcome, WorkerRecord, WorkerSignal,
};
use aftman_store::{RunClaim, RunStore, StoreError};

use crate::control::{ControlError, ControlMessage, ControlReceiver};
use crate::obs::ObservationBuilder;
use crate::sink::EventSink;

/// Everything the runtime needs. Assembled by the caller so that tests can supply
/// doubles for any part.
pub struct RuntimeDeps {
    pub config: RunConfig,
    pub store: Arc<dyn RunStore>,
    pub model: Arc<dyn DecisionModel>,
    pub worker: Arc<dyn Worker>,
    pub registry: ResponsibilityRegistry,
    /// The declarations routing and check construction read.
    pub definitions: DefinitionSet,
    /// Command evidence providers.
    pub evidence: EvidencePlan,
    /// Scratch space for a provider that runs with `cwd = "temp"`.
    pub evidence_temp_dir: PathBuf,
    pub clock: Arc<dyn Clock>,
    pub sink: Arc<dyn EventSink>,
    /// Whether the uncertainty band holds a rule. Off by default, which restores
    /// plain threshold behaviour exactly.
    pub ensemble_enabled: bool,
    pub uncertainty_margin: f32,
    pub steering_grace: Duration,
    /// The request budget the provider was configured with.
    ///
    /// Passed to every `ask` rather than defaulted: `AFTMAN_JEV_*_BUDGET_BYTES` were parsed
    /// and shown by `doctor`, but both call sites here passed `Budget::default()`, so an
    /// operator who set one got a budget they had not asked for — and a cap lowered *below*
    /// the fixed target could never be met, so every request would be refused.
    pub jev_budget: aftman_core::model::Budget,
    /// How the worker is launched, as the composition root describes it: opaque
    /// key/value pairs such as `permissions`, `external_sandbox`, `steering`. Written
    /// into the `RUN_CREATED` payload under `launch`, so a run launched with approvals
    /// off is distinguishable in the record from a sandboxed one. Core stays ignorant
    /// of launch flags; this is a description, never an input to a decision. Bounded
    /// by [`MAX_LAUNCH_ENTRIES`], [`MAX_LAUNCH_KEY_CHARS`] and [`MAX_LAUNCH_VALUE_CHARS`]
    /// (excess is dropped or truncated).
    pub launch: BTreeMap<String, String>,
    /// Environment this run forwards to its worker and to its command evidence.
    ///
    /// Per run, not per daemon, because the daemon keeps whichever environment started
    /// it: without this, a worker would use the `PATH` and virtualenv of a shell the
    /// operator may not even have open. The client chooses the allowlist; this is the
    /// result of that choice, and it has already been checked for supervisor names.
    ///
    /// **Values must never be recorded.** Names are written to `RUN_CREATED` under
    /// `launch`; values reach a subprocess and nothing else.
    pub forwarded_env: BTreeMap<String, String>,
}

/// Most `launch` entries recorded in `RUN_CREATED`.
pub const MAX_LAUNCH_ENTRIES: usize = 32;
/// Longest `launch` key, in characters.
pub const MAX_LAUNCH_KEY_CHARS: usize = 64;
/// Longest `launch` value, in characters.
pub const MAX_LAUNCH_VALUE_CHARS: usize = 256;

/// The `launch` map as it is recorded: bounded.
fn bounded_launch(launch: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    launch
        .iter()
        .take(MAX_LAUNCH_ENTRIES)
        .map(|(k, v)| {
            (
                k.chars().take(MAX_LAUNCH_KEY_CHARS).collect(),
                v.chars().take(MAX_LAUNCH_VALUE_CHARS).collect(),
            )
        })
        .collect()
}

/// Records a worker's process in the run directory through the store, so recovery can
/// end it if this process dies before reaping it.
struct StoreLedger {
    store: Arc<dyn RunStore>,
    run_id: RunId,
    worker_id: WorkerId,
}

impl std::fmt::Debug for StoreLedger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreLedger")
            .field("run_id", &self.run_id)
            .field("worker_id", &self.worker_id)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl aftman_core::worker::ProcessLedger for StoreLedger {
    async fn record(&self, identity: aftman_core::worker::ProcessIdentity) {
        if let Err(error) = self
            .store
            .set_worker_process(self.run_id, self.worker_id, Some(identity))
            .await
        {
            tracing::warn!(run_id = %self.run_id, %error, "could not record the worker's process");
        }
    }

    async fn clear(&self) {
        if let Err(error) = self
            .store
            .set_worker_process(self.run_id, self.worker_id, None)
            .await
        {
            tracing::debug!(run_id = %self.run_id, %error, "could not clear the worker's process record");
        }
    }
}

/// Errors that stop a run for a reason operability can act on.
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("repository {0} is not a directory")]
    BadRepo(String),

    #[error("storage error: {0}")]
    Store(#[from] StoreError),

    /// Another process holds this run's ownership claim. Cannot happen for a
    /// fresh run id; reported rather than ignored because it would mean two
    /// writers on one log.
    #[error("run {0} is claimed by another process")]
    Claimed(RunId),

    #[error("model error: {0}")]
    Model(#[from] ModelError),

    #[error("worker error: {0}")]
    Worker(#[from] WorkerError),

    #[error("illegal status transition: {0}")]
    Transition(#[from] TransitionError),

    #[error("responsibility configuration: {0}")]
    Responsibility(String),
}

/// How a run ended.
#[derive(Debug, Clone)]
pub struct RunOutcome {
    pub run_id: RunId,
    pub status: FactoryStatus,
    pub directive: Directive,
    /// Model calls actually made. Skipped cycles are not counted.
    pub assessments: u32,
    /// Cycles skipped because the observation content was unchanged.
    pub skipped: u32,
    pub counters: aftman_core::observation::Counters,
    /// Why the run ended, when something said so: a cancel's reason, or
    /// `"stopped by operator"`. `None` for a run that simply finished or failed.
    pub reason: Option<String>,
}

/// A run that has been persisted but is not yet being supervised.
///
/// Returned by [`FactoryRuntime::start`]; consumed by [`FactoryRuntime::drive`].
pub struct StartedRun {
    job: Job,
    state: RunState,
    /// The run's ownership claim (`RunStore::claim`), held until `drive` returns
    /// and the value is dropped, or until the caller drops an undriven run. It is
    /// what keeps a restarting daemon, or a purge, in another process from
    /// treating a live run as an orphan.
    _claim: RunClaim,
}

impl StartedRun {
    #[must_use]
    pub fn run_id(&self) -> RunId {
        self.state.header.id
    }

    #[must_use]
    pub fn job(&self) -> &Job {
        &self.job
    }

    /// The state as persisted by `start`.
    #[must_use]
    pub fn state(&self) -> &RunState {
        &self.state
    }
}

/// Longest steer message a human may send, in characters.
pub const MAX_STEER_CHARS: usize = 8_000;
/// Longest note stored with an answer, in characters.
pub const MAX_NOTE_CHARS: usize = 2_000;
/// Longest cancel reason stored, in characters.
const MAX_REASON_CHARS: usize = 500;
/// The most worker output one `WorkerOutput` event carries, in bytes. Anything
/// beyond is dropped from the front and flagged with `truncated`.
pub const WORKER_OUTPUT_EVENT_CAP_BYTES: usize = 8_192;

/// The run loop's mutable bookkeeping, gathered so that control handlers can adjust
/// it (invalidate the digest, request a cycle) without a dozen `&mut` parameters.
struct Loop {
    worker: Option<WorkerHandle>,
    last_directive: Option<Directive>,
    assessments: u32,
    skipped: u32,
    next_assessment_at: tokio::time::Instant,
    next_idle_at: tokio::time::Instant,
    /// Earliest next `WorkerOutput` flush while parked in `NeedsHuman`, where no
    /// cycle exists to flush on.
    next_flush_at: tokio::time::Instant,
    // Start with a cycle pending so the first assessment happens immediately
    // rather than after one idle interval.
    activity_pending: bool,
    last_digest: Option<String>,
    evidence_outcomes: Vec<EvidenceOutcome>,
    before_finish_ran: bool,
    /// Wall-clock time the current human wait began.
    wait_started: Option<OffsetDateTime>,
    /// A worker completed while the run waited for a human, so its
    /// `WorkerCompleted` evidence providers have not run yet.
    completed_while_waiting: bool,
    finish_reason: Option<String>,
}

impl Loop {
    fn new(now: tokio::time::Instant, config: &RunConfig) -> Self {
        Self {
            worker: None,
            last_directive: None,
            assessments: 0,
            skipped: 0,
            next_assessment_at: now,
            next_idle_at: now + config.idle_poll_interval,
            next_flush_at: now,
            activity_pending: true,
            last_digest: None,
            evidence_outcomes: Vec::new(),
            before_finish_ran: false,
            wait_started: None,
            completed_while_waiting: false,
            finish_reason: None,
        }
    }

    /// Ask for a fresh assessment as soon as the loop can run one, even if the
    /// observation looks unchanged.
    /// Mark a cycle pending without bypassing the debounce window: the cycle runs
    /// at `next_assessment_at`, which already sits one `min_assessment_interval`
    /// after the previous cycle. Used for a human steer, so a burst of steers costs
    /// one assessment, not one each.
    fn request_assessment_debounced(&mut self) {
        self.last_digest = None;
        self.activity_pending = true;
    }

    fn request_assessment(&mut self, now: tokio::time::Instant) {
        self.last_digest = None;
        self.activity_pending = true;
        self.next_assessment_at = now;
    }
}

/// Wait for the next control message; pends forever if no channel is attached.
/// `None` means the channel closed.
async fn recv_control(control: &mut Option<ControlReceiver>) -> Option<ControlMessage> {
    match control {
        Some(receiver) => receiver.recv().await,
        None => std::future::pending().await,
    }
}

fn has_unflushed(handle: &WorkerHandle) -> bool {
    handle
        .output
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .has_unflushed()
}

/// Send a reply, and surface a fatal runtime error after telling the caller.
fn settle<T>(
    reply: oneshot::Sender<Result<T, ControlError>>,
    result: Result<Result<T, ControlError>, RuntimeError>,
) -> Result<(), RuntimeError> {
    match result {
        Ok(answer) => {
            // A caller that stopped waiting is not an error.
            let _ = reply.send(answer);
            Ok(())
        }
        Err(error) => {
            let _ = reply.send(Err(ControlError::Internal(error.to_string())));
            Err(error)
        }
    }
}

/// Record a provider outcome, replacing any earlier one from the same provider: the
/// observation carries the latest result per provider, not every result ever, so a stale
/// failure does not shadow a later pass and the vector is bounded by the number of
/// providers.
fn keep_latest(outcomes: &mut Vec<EvidenceOutcome>, outcome: EvidenceOutcome) {
    outcomes.retain(|existing| existing.provider != outcome.provider);
    outcomes.push(outcome);
}

/// A prefix of `text` of at most `max` characters.
fn truncate_chars(text: &str, max: usize) -> String {
    text.chars().take(max).collect()
}

/// A live worker task.
struct WorkerHandle {
    id: WorkerId,
    outcome: Option<WorkerOutcome>,

    signals: mpsc::UnboundedReceiver<WorkerSignal>,
    done: mpsc::UnboundedReceiver<Result<WorkerOutcome, WorkerError>>,
    /// The live log the worker streams into. Kept here so the runtime can copy it
    /// into the worker record before every observation — otherwise a running
    /// worker's tail would always read as empty, and the supervisor would be
    /// judging a worker it cannot see.
    output: Arc<Mutex<BoundedLog>>,
    /// The decoded summary the backend publishes as it reads its stream. Kept here for the
    /// same reason as `output`: the runtime copies it into the worker record, so a worker
    /// that is looping can be seen to be looping before it finishes.
    activity: Arc<Mutex<Option<StreamActivity>>>,
    /// The worker's task, kept so a stop can reap or abort it.
    task: tokio::task::JoinHandle<()>,
}

impl Drop for WorkerHandle {
    /// No way of ending a run task (return, error, panic, abort) may leave its
    /// worker running. A dropped `JoinHandle` only *detaches* the task, so abort it
    /// explicitly: the backends kill their process group when their `run` future is
    /// dropped. A normal stop has already reaped the task, and aborting a finished
    /// task is a no-op.
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Why a worker is being ended; decides the termination reason and the
/// `WorkerTerminated` payload.
enum StopWhy<'a> {
    Cancelled(&'a str),
    Stopped(&'a str),
    Superseded,
    RunEnded(&'a str),
}

impl StopWhy<'_> {
    fn terminate_reason(&self) -> TerminateReason {
        match self {
            Self::Cancelled(_) => TerminateReason::Cancelled,
            Self::Stopped(_) | Self::RunEnded(_) => TerminateReason::Stopped,
            Self::Superseded => TerminateReason::Superseded,
        }
    }

    fn cause(&self) -> &'static str {
        match self {
            Self::Cancelled(_) => "cancelled",
            Self::Stopped(_) => "stopped",
            Self::Superseded => "superseded",
            Self::RunEnded(_) => "run_ended",
        }
    }

    fn reason(&self) -> &str {
        match self {
            Self::Cancelled(r) | Self::Stopped(r) | Self::RunEnded(r) => r,
            Self::Superseded => "superseded",
        }
    }
}

/// How often `terminate` is re-issued to a worker that has not yet reported.
const REAP_REISSUE: Duration = Duration::from_millis(50);
/// The longest a stop waits for a worker task before aborting it. Must exceed a
/// backend's own interrupt-then-kill grace (5 s by default).
const REAP_TIMEOUT: Duration = Duration::from_secs(30);

/// The outcome recorded for a worker task that ended without reporting (a panic).
fn lost_worker_outcome() -> WorkerOutcome {
    WorkerOutcome::StreamError {
        message: "worker task ended without reporting a result".to_owned(),
    }
}

/// The run loop.
pub struct FactoryRuntime {
    deps: RuntimeDeps,
    builder: ObservationBuilder,
}

impl FactoryRuntime {
    /// Change the limit on assembling the repository evidence of one observation
    /// (default 8 s). For tests.
    #[must_use]
    pub fn with_observation_timeout(mut self, timeout: Duration) -> Self {
        self.builder.timeout = timeout;
        self
    }

    #[must_use]
    pub fn new(deps: RuntimeDeps) -> Self {
        let builder = ObservationBuilder::for_worker(deps.config.worker);
        Self { deps, builder }
    }

    /// Run one job to a terminal state, with no control channel attached.
    ///
    /// This is the `--no-daemon` path: equivalent to [`FactoryRuntime::start`]
    /// followed by [`FactoryRuntime::drive`] with `None`, so `NeedsHuman` ends the
    /// run rather than waiting for an answer nobody can give.
    ///
    /// # Errors
    /// Returns an error only for conditions that make the run impossible to start or
    /// continue: a missing repository, a store failure, or a worker that cannot be
    /// launched. A model failure is *not* an error here — it degrades to a
    /// conservative directive, because a supervisor that dies when its model is
    /// briefly unavailable is worse than one that says so.
    pub async fn run(&self, job: Job) -> Result<RunOutcome, RuntimeError> {
        let started = self.start(job).await?;
        self.drive(started, None).await
    }

    /// Validate the job and persist a new run, without supervising it yet.
    ///
    /// On `Ok`, the run id is known, the store header exists, and `RunCreated` has
    /// been emitted: a caller can answer "which run?" and safely list or subscribe to
    /// it before [`FactoryRuntime::drive`] begins. A bad repository fails here, before
    /// any run state exists.
    ///
    /// # Errors
    /// [`RuntimeError::BadRepo`] or a store failure.
    pub async fn start(&self, job: Job) -> Result<StartedRun, RuntimeError> {
        self.start_with_id(job, RunId::new()).await
    }

    /// [`start`](Self::start) with the caller choosing the id.
    ///
    /// A caller that owns a hub needs this: `RUN_CREATED` is pushed to subscribers before
    /// the first snapshot, so a failure after that point must be able to remove the run from
    /// the hub as well as the store — and the hub is keyed by id.
    pub async fn start_with_id(&self, job: Job, run_id: RunId) -> Result<StartedRun, RuntimeError> {
        if !job.repo.is_dir() {
            return Err(RuntimeError::BadRepo(job.repo.display().to_string()));
        }

        // Claim before the run exists, so there is no moment at which a run
        // directory is visible to another process and unowned.
        let Some(claim) = self.deps.store.claim_new(run_id).await? else {
            return Err(RuntimeError::Claimed(run_id));
        };
        let header = RunHeader {
            id: run_id,
            created_at: self.now(),
            job: job.clone(),
            config: self.deps.config.clone(),
            aftman_version: env!("CARGO_PKG_VERSION").to_owned(),
            worker_versions: BTreeMap::new(),
            granted_key_names: Vec::new(),
        };
        // From here on a failure must not leave a half-made run that nobody drives
        // (it would sit `Queued` until the next restart): delete it. We hold its
        // claim, so nobody else can be using it.
        let result = async {
            self.deps.store.create(&header).await?;
            let mut state = RunState::new(header, FactoryStatus::Queued);
            self.emit(&mut state, EventKind::RunCreated, {
                let mut payload = json!({ "repo": job.repo.display().to_string() });
                let mut launch = bounded_launch(&self.deps.launch);
                // Names only. A value here would put a forwarded secret in the run log.
                if !self.deps.forwarded_env.is_empty() {
                    let names: Vec<String> = self.deps.forwarded_env.keys().cloned().collect();
                    launch.insert("forwarded_env".to_owned(), names.join(","));
                }
                if !launch.is_empty() {
                    payload["launch"] = json!(launch);
                }
                payload
            })
            .await?;
            // A snapshot now, so the run is loadable (`run.get`) while it is still
            // routing.
            self.snapshot(&mut state).await?;
            Ok::<_, RuntimeError>(state)
        }
        .await;
        let state = match result {
            Ok(state) => state,
            Err(error) => {
                if let Err(cleanup) = self.deps.store.delete(run_id).await {
                    tracing::warn!(%run_id, %cleanup, "could not remove a run that failed to start");
                }
                return Err(error);
            }
        };
        Ok(StartedRun {
            job,
            state,
            _claim: claim,
        })
    }

    /// Supervise a started run to the end.
    ///
    /// With `control` attached, the run can be steered, cancelled, and answered while
    /// it runs, and `NeedsHuman` becomes a *wait* state: no cycles, no model calls,
    /// until an answer or a cancel arrives. If every [`RunControl`] handle is dropped
    /// while waiting, the run ends the way [`FactoryRuntime::run`] ends it. Without
    /// `control`, `NeedsHuman` ends the run.
    ///
    /// Control messages are observed at the top of every loop iteration and in the
    /// wait `select!`; a cycle in progress is never interrupted, and a reply is sent
    /// only after its effect is applied and persisted. Routing is a model call and
    /// counts as a cycle in progress.
    ///
    /// # Errors
    /// As [`FactoryRuntime::run`].
    pub async fn drive(
        &self,
        started: StartedRun,
        control: Option<ControlReceiver>,
    ) -> Result<RunOutcome, RuntimeError> {
        let StartedRun {
            job,
            mut state,
            _claim,
        } = started;
        let mut ls = Loop::new(self.instant(), &self.deps.config);
        let result = self.drive_inner(job, &mut state, &mut ls, control).await;
        if result.is_err() {
            // An error return must not leave a worker running against the repository.
            if let Some(handle) = ls.worker.take() {
                let _ = self
                    .stop_worker(&mut state, handle, StopWhy::RunEnded("run failed"))
                    .await;
                let _ = self.snapshot(&mut state).await;
            }
        }
        result
    }

    async fn drive_inner(
        &self,
        job: Job,
        state: &mut RunState,
        ls: &mut Loop,
        mut control: Option<ControlReceiver>,
    ) -> Result<RunOutcome, RuntimeError> {
        let run_id = state.header.id;

        self.transition(state, FactoryStatus::Routing).await?;

        // Routing sees the job text and nothing else, and decides which *conditional*
        // responsibilities participate. It never decides what happens: a routing mistake
        // costs a dimension of supervision, not an action.
        let routing_outcome = routing::route(
            self.deps.model.as_ref(),
            &aftman_core::model::AskContext::for_run(
                Purpose::Routing,
                run_id,
                aftman_core::ids::AssessmentSeq(0),
            ),
            &self.deps.definitions,
            &job.prompt,
            &self.deps.jev_budget,
        )
        .await;

        let mut routing_payload = json!({
            "activated": routing_outcome.activated.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "degraded": routing_outcome.degraded,
            // Rounded because an f32 widened to f64 prints as `0.699999988079071`, and
            // these numbers are read off the log by people.
            "conditional": routing_outcome.decisions.iter().map(|decision| json!({
                "responsibility": decision.responsibility.to_string(),
                "probability": decision.probability.map(round4),
                "threshold": round4(decision.threshold),
                "activated": decision.activated,
            })).collect::<Vec<_>>(),
        });
        if let Some(model) = &routing_outcome.model {
            routing_payload["model"] = json!(model);
        }
        self.emit(state, EventKind::JobRouted, routing_payload)
            .await?;

        // The registry is built from what routing activated, so a conditional
        // responsibility that was not activated has no checks to ask.
        let registry = ResponsibilityRegistry::from_definitions(
            &self.deps.definitions,
            &routing_outcome.activated,
            self.deps.steering_grace.as_secs(),
            1,
        )
        .map_err(RuntimeError::Responsibility)?;
        self.transition(state, FactoryStatus::Running).await?;

        loop {
            // ---- control ------------------------------------------------------
            //
            // Drained without blocking at the top of every iteration, for the same
            // reason the completion channel is (LLD §7.1): a cancel observed only in
            // the wait branch would never be seen by a run whose cycles outlast the
            // idle interval. It is never observed mid-cycle.
            self.drain_control(state, &job, ls, &mut control).await?;

            if let Some(finish) = self.terminal_status(
                state,
                control.as_ref().is_some_and(ControlReceiver::has_human),
            ) {
                // No exit path may leave a worker of this run alive, and no human wait
                // may be left open: close both before the run is reported over.
                self.end_human_wait(state, ls);
                if let Some(handle) = ls.worker.take() {
                    self.stop_worker(state, handle, StopWhy::RunEnded(finish.as_str()))
                        .await?;
                }
                let outcome = RunOutcome {
                    run_id,
                    status: finish,
                    directive: ls.last_directive.take().unwrap_or(Directive::Continue),
                    assessments: ls.assessments,
                    skipped: ls.skipped,
                    counters: state.counters.clone(),
                    reason: ls.finish_reason.clone(),
                };
                // Build the payload before taking the mutable borrow.
                let mut payload = json!({
                    "status": finish.as_str(),
                    "assessments": ls.assessments,
                    "assessments_skipped": ls.skipped,
                    "workers_run": state.counters.workers_run,
                });
                if let Some(reason) = &ls.finish_reason {
                    payload["reason"] = json!(reason);
                }
                self.emit_final(state, payload).await?;
                return Ok(outcome);
            }

            // ---- observe completions and activity without blocking -----------
            //
            // A worker completing must be observed regardless of which trigger woke
            // the loop. Draining this only inside the wait branch below is a
            // deadlock waiting to happen: if a cycle outlasts the idle interval —
            // which a real model call easily does — `idle_due` is true every
            // iteration, the wait branch is never reached, and the completion is
            // never seen. The run then supervises a worker that no longer exists.
            if let Some(handle) = ls.worker.as_mut() {
                if handle.outcome.is_none() {
                    match handle.done.try_recv() {
                        Ok(outcome) => handle.outcome = Some(outcome?),
                        // The task ended without reporting (it panicked). Record a
                        // failure rather than supervising a worker that is gone.
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            handle.outcome = Some(lost_worker_outcome());
                        }
                        Err(mpsc::error::TryRecvError::Empty) => {}
                    }
                }
                while handle.signals.try_recv().is_ok() {
                    ls.activity_pending = true;
                    ls.next_assessment_at =
                        self.instant() + self.deps.config.min_assessment_interval;
                }
            }

            // ---- waiting for a human ------------------------------------------
            //
            // With a control channel attached, `NeedsHuman` is a wait state: no
            // cycles, no model calls, no skipped-cycle events. Only a control message,
            // a worker finishing, or buffered output to flush wakes the loop.
            if state.status == FactoryStatus::NeedsHuman {
                ls.wait_started.get_or_insert_with(|| self.now());
                self.wait_for_human(state, &job, ls, &mut control).await?;
                continue;
            }

            // ---- wait for a reason to think ---------------------------------
            let idle_due = self.instant() >= ls.next_idle_at;
            let activity_due = ls.activity_pending && self.instant() >= ls.next_assessment_at;
            let worker_finished = ls.worker.as_ref().is_some_and(|w| w.outcome.is_some());
            if !(idle_due || activity_due || worker_finished) {
                let deadline = if ls.activity_pending {
                    ls.next_assessment_at
                } else {
                    ls.next_idle_at
                };
                let wake = tokio::time::sleep_until(deadline);
                tokio::pin!(wake);
                // `Some(None)` is a closed channel, `Some(Some(_))` a message.
                let mut message: Option<Option<ControlMessage>> = None;
                match ls.worker.as_mut() {
                    Some(handle) => {
                        tokio::select! {
                            biased;
                            msg = recv_control(&mut control) => { message = Some(msg); }
                            outcome = handle.done.recv() => {
                                handle.outcome = Some(match outcome {
                                    Some(outcome) => outcome?,
                                    None => lost_worker_outcome(),
                                });
                            }
                            Some(_signal) = handle.signals.recv() => {
                                ls.activity_pending = true;
                                ls.next_assessment_at = self.instant() + self.deps.config.min_assessment_interval;
                            }
                            () = &mut wake => {}
                        }
                    }
                    None => {
                        tokio::select! {
                            biased;
                            msg = recv_control(&mut control) => { message = Some(msg); }
                            () = &mut wake => {}
                        }
                    }
                }
                match message {
                    Some(Some(message)) => {
                        self.handle_control(state, &job, ls, message).await?;
                    }
                    Some(None) => control = None,
                    None => {}
                }
                continue;
            }
            if idle_due {
                ls.next_idle_at = self.instant() + self.deps.config.idle_poll_interval;
            }
            // Coalesce: this cycle consumes the pending activity.
            ls.activity_pending = false;
            ls.next_assessment_at = self.instant() + self.deps.config.min_assessment_interval;

            let forced = worker_finished;

            // Fold a finished worker into state before observing, and **drop the
            // handle**. Leaving a finished handle in place is a deadlock: `apply`
            // would see a worker and decline to start a new one, and
            // `worker_finished` would never become true again, so the run would
            // spin on the idle ticker forever making no progress.
            let mut just_finished = false;
            if let Some(handle) = ls.worker.as_mut()
                && let Some(outcome) = handle.outcome.take()
            {
                self.finish_worker(state, handle.id, &outcome, handle)
                    .await?;
                state.counters.failures += u32::from(!outcome.is_success());
                just_finished = true;
            }
            if just_finished {
                ls.worker = None;
                // Run the evidence providers that are due now, *before* the cycle that
                // judges the worker. This is what turns the agent's "3 passed" into a
                // measurement rather than a claim.
                self.collect_evidence(
                    state,
                    &job,
                    RunWhen::WorkerCompleted,
                    &mut ls.evidence_outcomes,
                )
                .await?;
            }
            // A still-running worker's live output must reach the observation, and
            // be flushed to the log as one `WorkerOutput` event for this cycle (only
            // if new bytes arrived). Flush first: the record's copy of the log is
            // taken after, so it carries no stale unflushed count.
            if let Some(handle) = ls.worker.as_ref() {
                self.flush_output(state, handle, false).await?;
                self.sync_live(state, handle);
            }

            // ---- gather evidence --------------------------------------------
            //
            // The observation's `active` slot is the **most recent** worker, running
            // or not: the model is asked "is this done?", and it cannot answer that
            // if the worker that just finished has vanished from the evidence. Policy
            // uses a different notion — `state.active_worker()`, running only — because
            // "may I steer it" and "may I start another" are about the live process.
            let event_summaries = self.recent_event_summaries(run_id, state.last_seq).await;
            let most_recent = state.workers.last().cloned();
            let observation = self
                .builder
                .build(
                    state,
                    &job,
                    most_recent.as_ref(),
                    event_summaries,
                    &ls.evidence_outcomes,
                    self.now(),
                )
                .await;

            // ---- skip an unchanged observation ------------------------------
            let digest = observation.content_digest();
            if self.deps.config.skip_unchanged_assessments
                && !forced
                && ls.last_digest.as_deref() == Some(digest.as_str())
                && !self.time_driven_check_due(state)
            {
                ls.skipped = state.counters.assessments_skipped + 1;
                state.counters.assessments_skipped = ls.skipped;
                self.emit(
                    state,
                    EventKind::AssessmentSkippedUnchanged,
                    json!({ "digest": digest }),
                )
                .await?;
                continue;
            }
            ls.last_digest = Some(digest);

            // A judgement is formed in `Assessing`, not in `Running`. The state
            // machine encodes this deliberately: `Running -> Finished` is not a legal
            // transition, because a run must pass through the state where the
            // question was asked. `apply` returns it to `Running` afterwards.
            if state.status == FactoryStatus::Running {
                self.transition(state, FactoryStatus::Assessing).await?;
            }

            // ---- ask the model ----------------------------------------------
            let questions = self.questions(&registry);
            let keys: Vec<(CheckKey, Option<f32>)> = registry
                .checks()
                .into_iter()
                .map(|c| (c.key, c.threshold))
                .collect();
            let state_json = observation.state_for(&aftman_core::observation::default_evidence());

            self.emit(
                state,
                EventKind::AssessmentRequested,
                json!({ "questions": questions.keys().map(ToString::to_string).collect::<Vec<_>>() }),
            )
            .await?;

            // `max_assessments` is a hard ceiling: once it is spent the model is not
            // asked again, however the run got here. Policy is handed an empty check
            // set, exactly as on a model failure, and its budget invariant escalates.
            let budget_spent = ls.assessments >= self.deps.config.max_assessments;
            if budget_spent {
                self.emit(
                    state,
                    EventKind::BudgetExceeded,
                    json!({ "max_assessments": self.deps.config.max_assessments }),
                )
                .await?;
            }
            let (checks, model_name, usage) = if budget_spent {
                (
                    BTreeMap::new(),
                    "budget_exhausted".to_owned(),
                    usage_default(),
                )
            } else {
                match self
                    .deps
                    .model
                    .ask(
                        &aftman_core::model::AskContext::for_run(
                            Purpose::Assessment,
                            run_id,
                            aftman_core::ids::AssessmentSeq(ls.assessments.into()),
                        ),
                        &state_json,
                        &questions,
                        &self.deps.jev_budget,
                    )
                    .await
                {
                    Ok(answers) => {
                        state.counters.consecutive_model_failures = 0;
                        let resolved = aftman_core::model::resolve_checks(
                            &answers,
                            &keys,
                            self.deps.uncertainty_margin,
                            self.deps.ensemble_enabled,
                        );
                        (resolved, answers.model, answers.usage)
                    }
                    Err(error) => {
                        state.counters.consecutive_model_failures += 1;
                        // A rate limit is a different operational event from a malformed
                        // answer, and the UI shows them differently, so they are distinct
                        // event kinds rather than one `Error` with a code inside.
                        let kind = if matches!(error, ModelError::RateLimited { .. }) {
                            EventKind::RateLimited
                        } else {
                            EventKind::ModelError
                        };
                        self.emit(
                            state,
                            kind,
                            json!({
                                "code": error.code(),
                                "message": error.to_string(),
                                "retryable": error.is_retryable(),
                            }),
                        )
                        .await?;
                        // No guesses: an empty check set can only produce conservative
                        // directives.
                        (BTreeMap::new(), "unavailable".to_owned(), usage_default())
                    }
                }
            };

            if !budget_spent {
                ls.assessments += 1;
            }
            state.counters.assessments = ls.assessments;

            // ---- record the assessment --------------------------------------
            self.emit(
                state,
                EventKind::AssessmentCompleted,
                json!({
                    "assessment_seq": ls.assessments,
                    "model": model_name,
                    "input_tokens": usage.input_tokens,
                    "output_tokens": usage.output_tokens,
                    "checks": checks.iter().map(|(k, v)| {
                        json!({
                            "key": k.to_string(),
                            "jev_p": v.jev_p,
                            "threshold": v.threshold,
                            "decisive": v.decisive,
                            "contested": v.contested,
                            "permitted": v.permitted,
                            "unresolved": v.is_unresolved(),
                        })
                    }).collect::<Vec<_>>(),
                }),
            )
            .await?;

            // ---- arbitrate ---------------------------------------------------
            // Policy reads `state.counters`, so the elapsed figure its timeout invariant
            // compares must be current there too (supervised time: human waits excluded).
            state.counters.elapsed_secs = state.supervised_elapsed(self.now()).as_secs();
            let proposals = {
                let input = PolicyInput {
                    status: state.status,
                    counters: &state.counters,
                    config: &self.deps.config,
                    checks: &checks,
                    observation: &observation,
                    now: self.now(),
                    active_worker: state.active_worker(),
                    last_directive: ls.last_directive.as_ref(),
                    ensemble_enabled: self.deps.ensemble_enabled,
                    steering_grace: self.deps.steering_grace,
                };
                registry.propose(&input)
            };

            let decision = {
                let input = PolicyInput {
                    status: state.status,
                    counters: &state.counters,
                    config: &self.deps.config,
                    checks: &checks,
                    observation: &observation,
                    now: self.now(),
                    active_worker: state.active_worker(),
                    last_directive: ls.last_directive.as_ref(),
                    ensemble_enabled: self.deps.ensemble_enabled,
                    steering_grace: self.deps.steering_grace,
                };
                arbitrate(&input, &proposals)
            };

            // S0: the ensemble is types-and-plumbing only. Every check keeps its
            // baseline resolution, and the invariant list is still reported so the
            // log shows what the band would have done.
            self.emit(
                state,
                EventKind::EnsembleResolved,
                json!({
                    "enabled": self.deps.ensemble_enabled,
                    "margin": self.deps.uncertainty_margin,
                    "unresolved": checks.iter().filter(|(_, v)| v.is_unresolved()).map(|(k, _)| k.to_string()).collect::<Vec<_>>(),
                    "rungs": Vec::<Rung>::new(),
                }),
            )
            .await?;

            for proposal in &decision.considered {
                self.emit(
                    state,
                    EventKind::DirectiveProposed,
                    json!({
                        "responsibility": proposal.responsibility.to_string(),
                        "rule": proposal.rule.to_string(),
                        "directive": proposal.directive.name(),
                        "priority": proposal.priority,
                    }),
                )
                .await?;
            }
            self.emit(
                state,
                EventKind::DirectiveSelected,
                json!({
                    "directive": decision.selected.name(),
                    "reason": decision.selected.reason().unwrap_or_default(),
                    "invariants": decision.invariants.iter().map(|i| format!("{i:?}")).collect::<Vec<_>>(),
                    "suppressed": decision.suppressed.iter().map(|s| json!({
                        "rule": s.rule.to_string(),
                        "reason": match &s.reason {
                            SuppressionReason::NotPermitted => "not_permitted".to_owned(),
                            SuppressionReason::Invariant(i) => format!("invariant:{i:?}"),
                            SuppressionReason::HigherPriority => "higher_priority".to_owned(),
                            SuppressionReason::CapabilityUnavailable => "capability_unavailable".to_owned(),
                        },
                    })).collect::<Vec<_>>(),
                }),
            )
            .await?;

            state.checks = checks;
            state.counters.assessments = ls.assessments;
            state.last_directive = Some(decision.selected.clone());

            // ---- apply -------------------------------------------------------
            //
            // A `Finish` selected on evidence that has not been refreshed is exactly the
            // case `run_when = "before_finish"` exists for. Run those providers and judge
            // again, rather than finishing on a claim.
            let selected = decision.selected.clone();
            if matches!(selected, Directive::Finish { .. })
                && !ls.before_finish_ran
                && !self.deps.evidence.due(RunWhen::BeforeFinish).is_empty()
            {
                ls.before_finish_ran = true;
                self.collect_evidence(
                    state,
                    &job,
                    RunWhen::BeforeFinish,
                    &mut ls.evidence_outcomes,
                )
                .await?;
                state.last_directive = Some(selected.clone());
                ls.last_directive = Some(selected);
                // The observation has changed, so the digest must not suppress the
                // re-assessment.
                ls.last_digest = None;
                ls.next_assessment_at = self.instant();
                ls.activity_pending = true;
                self.snapshot(state).await?;
                continue;
            }

            ls.worker = self
                .apply(state, &job, selected, ls.worker.take(), &observation)
                .await?;
            // A directive that neither ended the run nor parked it leaves the run
            // supervising, so return to `Running`.
            if state.status == FactoryStatus::Assessing {
                self.transition(state, FactoryStatus::Running).await?;
            }
            ls.last_directive = Some(decision.selected.clone());
            if state.status == FactoryStatus::NeedsHuman {
                // Time from here until an answer does not count against the overall
                // timeout (only meaningful with a control channel attached).
                ls.wait_started.get_or_insert_with(|| self.now());
            }
            self.snapshot(state).await?;

            if !state.status.is_terminal() && self.timeout_exceeded(state) {
                self.transition(state, FactoryStatus::Failed).await?;
            }
        }
    }

    // ---- helpers ----------------------------------------------------------

    fn now(&self) -> OffsetDateTime {
        self.deps.clock.now()
    }

    fn instant(&self) -> tokio::time::Instant {
        self.deps.clock.instant()
    }

    /// Whether `overall_timeout` has passed. Time spent waiting for a human is not
    /// counted (`RunState::human_wait_ms`).
    fn timeout_exceeded(&self, state: &RunState) -> bool {
        state.supervised_elapsed(self.now()) >= self.deps.config.overall_timeout
    }

    /// Whether a time-based decision will trip within the next tick.
    ///
    /// The content digest deliberately ignores elapsed time, so a timeout
    /// approaching is invisible to it. Without this check the digest skip could
    /// suppress the one cycle that would notice the timeout.
    ///
    /// The boundaries: `overall_timeout`; the active worker's `worker_timeout`; and the end
    /// of the steering-grace window after an accepted steer (when the suppressed stop
    /// checks come back). The escalation timeout is M7 and not built.
    fn time_driven_check_due(&self, state: &RunState) -> bool {
        let now = self.now();
        let tick = self.deps.config.idle_poll_interval;
        let elapsed = state.supervised_elapsed(now);
        if self.deps.config.overall_timeout.saturating_sub(elapsed) <= tick {
            return true;
        }
        let since = |at: OffsetDateTime| -> Duration {
            // A wall clock that stepped backwards reads as "just now", not as a panic.
            Duration::try_from(now - at).unwrap_or(Duration::ZERO)
        };
        let Some(worker) = state.active_worker() else {
            return false;
        };
        if let Some(started) = worker.started_at {
            let remaining = self
                .deps
                .config
                .worker_timeout
                .saturating_sub(since(started));
            if remaining <= tick {
                return true;
            }
        }
        if let Some(steered) = worker.last_steered_at {
            let age = since(steered);
            let grace = self.deps.steering_grace;
            // Inside the window and about to leave it.
            if age < grace && grace - age <= tick {
                return true;
            }
        }
        false
    }

    /// A terminal status, or `None` to keep supervising.
    ///
    /// Without a channel a human can answer through, `NeedsHuman` ends the run: nobody
    /// can answer, so the run stops with the question recorded rather than proceeding
    /// on a guess. With one attached it is a wait state and the run keeps going. (A
    /// cancel-only channel, `RunControl::channel_without_human`, is not one.)
    fn terminal_status(&self, state: &RunState, has_control: bool) -> Option<FactoryStatus> {
        if state.status.is_terminal() {
            return Some(state.status);
        }
        if state.status == FactoryStatus::NeedsHuman && !has_control {
            return Some(FactoryStatus::NeedsHuman);
        }
        None
    }

    // ---- control plane ------------------------------------------------------

    /// Apply every control message already queued, without waiting for more.
    async fn drain_control(
        &self,
        state: &mut RunState,
        job: &Job,
        ls: &mut Loop,
        control: &mut Option<ControlReceiver>,
    ) -> Result<(), RuntimeError> {
        loop {
            let Some(receiver) = control.as_mut() else {
                return Ok(());
            };
            match receiver.try_recv() {
                Ok(message) => self.handle_control(state, job, ls, message).await?,
                Err(mpsc::error::TryRecvError::Empty) => return Ok(()),
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    *control = None;
                    return Ok(());
                }
            }
        }
    }

    /// Park in `NeedsHuman` until something happens, doing only bookkeeping.
    ///
    /// Makes no model call and starts no cycle. A worker that completes is folded
    /// into state (its completion must not be lost), and buffered worker output is
    /// flushed at most once per `min_assessment_interval`. With nothing to do the
    /// task is asleep on the channels, not polling.
    async fn wait_for_human(
        &self,
        state: &mut RunState,
        job: &Job,
        ls: &mut Loop,
        control: &mut Option<ControlReceiver>,
    ) -> Result<(), RuntimeError> {
        // Named rather than inlined into the chain below: as a chain clause the borrow of
        // `ls.worker` would be extended over the body, and the `take()` would not borrow-check.
        let finished = ls.worker.as_ref().is_some_and(|w| w.outcome.is_some());
        if finished && let Some(mut handle) = ls.worker.take() {
            if let Some(outcome) = handle.outcome.take() {
                self.finish_worker(state, handle.id, &outcome, &handle)
                    .await?;
                state.counters.failures += u32::from(!outcome.is_success());
                ls.completed_while_waiting = true;
                self.snapshot(state).await?;
            }
            return Ok(());
        }

        let unflushed = ls.worker.as_ref().is_some_and(has_unflushed);
        if unflushed && self.instant() >= ls.next_flush_at {
            if let Some(handle) = ls.worker.as_ref() {
                self.flush_output(state, handle, false).await?;
            }
            // While parked there are no decisions to be proportional to, so output
            // is flushed on the (coarse) idle interval, not the debounce interval.
            ls.next_flush_at = self.instant() + self.deps.config.idle_poll_interval;
            return Ok(());
        }
        let flush_at = unflushed.then_some(ls.next_flush_at);
        let timer = async move {
            match flush_at {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending().await,
            }
        };
        tokio::pin!(timer);

        enum Wake {
            Control(Option<ControlMessage>),
            Done(Option<Result<WorkerOutcome, WorkerError>>),
            Other,
        }
        let wake = match ls.worker.as_mut() {
            Some(handle) => {
                tokio::select! {
                    biased;
                    message = recv_control(control) => Wake::Control(message),
                    done = handle.done.recv() => Wake::Done(done),
                    Some(_signal) = handle.signals.recv() => Wake::Other,
                    () = &mut timer => Wake::Other,
                }
            }
            None => {
                tokio::select! {
                    biased;
                    message = recv_control(control) => Wake::Control(message),
                    () = &mut timer => Wake::Other,
                }
            }
        };
        match wake {
            Wake::Control(Some(message)) => self.handle_control(state, job, ls, message).await,
            Wake::Control(None) => {
                // Every handle is gone: nobody can answer, so end as `run()` would.
                *control = None;
                Ok(())
            }
            Wake::Done(result) => {
                if let Some(handle) = ls.worker.as_mut() {
                    handle.outcome = Some(match result {
                        Some(result) => result?,
                        // The worker task vanished without reporting. Treat it as a
                        // stream failure rather than waiting on it forever.
                        None => lost_worker_outcome(),
                    });
                }
                Ok(())
            }
            Wake::Other => Ok(()),
        }
    }

    /// Apply one control message and send its reply.
    ///
    /// The reply goes out only after the effect is applied and persisted. A storage
    /// failure is reported to the caller as `Internal` *and* ends the run, as it
    /// would anywhere else in the loop.
    async fn handle_control(
        &self,
        state: &mut RunState,
        job: &Job,
        ls: &mut Loop,
        message: ControlMessage,
    ) -> Result<(), RuntimeError> {
        match message {
            ControlMessage::Steer { message, reply } => {
                let result = self.control_steer(state, ls, &message).await;
                settle(reply, result)
            }
            ControlMessage::Cancel { reason, reply } => {
                let result = self.control_cancel(state, ls, &reason).await;
                settle(reply, result)
            }
            ControlMessage::Answer {
                escalation_id,
                decision,
                note,
                surface,
                reply,
            } => {
                let result = self
                    .control_answer(state, job, ls, escalation_id, decision, &note, &surface)
                    .await;
                settle(reply, result)
            }
        }
    }

    async fn control_steer(
        &self,
        state: &mut RunState,
        ls: &mut Loop,
        message: &str,
    ) -> Result<Result<SteerOutcome, ControlError>, RuntimeError> {
        if state.status.is_terminal() {
            return Ok(Err(ControlError::RunEnded));
        }
        if message.trim().is_empty() {
            return Ok(Err(ControlError::Invalid(
                "steer message is empty".to_owned(),
            )));
        }
        if message.chars().count() > MAX_STEER_CHARS {
            return Ok(Err(ControlError::Invalid(format!(
                "steer message is longer than {MAX_STEER_CHARS} characters"
            ))));
        }
        let Some(outcome) = self.deliver_human_steer(state, ls, message).await? else {
            return Ok(Err(ControlError::WrongState(format!(
                "the run has no active worker to steer (status: {})",
                state.status
            ))));
        };
        // "Does not skip the assessment loop" (docs/07 §10): a steer is new
        // information, so the next cycle must not be suppressed as unchanged.
        ls.request_assessment_debounced();
        self.snapshot(state).await?;
        Ok(Ok(outcome))
    }

    async fn control_cancel(
        &self,
        state: &mut RunState,
        ls: &mut Loop,
        reason: &str,
    ) -> Result<Result<(), ControlError>, RuntimeError> {
        if state.status.is_terminal() {
            return Ok(Err(ControlError::RunEnded));
        }
        let reason = if reason.trim().is_empty() {
            "cancelled by operator".to_owned()
        } else {
            truncate_chars(reason, MAX_REASON_CHARS)
        };
        self.cancel_run(state, ls, &reason).await?;
        Ok(Ok(()))
    }

    #[allow(clippy::too_many_arguments)]
    async fn control_answer(
        &self,
        state: &mut RunState,
        job: &Job,
        ls: &mut Loop,
        escalation_id: EscalationId,
        mut decision: HumanDecision,
        note: &str,
        surface: &str,
    ) -> Result<Result<(), ControlError>, RuntimeError> {
        if state.status.is_terminal() {
            return Ok(Err(ControlError::RunEnded));
        }
        let Some(index) = state
            .escalations
            .iter()
            .position(|e| e.id == escalation_id && e.answered.is_none())
        else {
            return Ok(Err(ControlError::UnknownEscalation(escalation_id)));
        };
        if state.status != FactoryStatus::NeedsHuman {
            return Ok(Err(ControlError::WrongState(format!(
                "the run is not waiting for a human (status: {})",
                state.status
            ))));
        }
        if let HumanDecision::Guidance { text } = &mut decision {
            if text.trim().is_empty() {
                return Ok(Err(ControlError::Invalid(
                    "guidance text is empty".to_owned(),
                )));
            }
            if text.chars().count() > MAX_STEER_CHARS {
                return Ok(Err(ControlError::Invalid(format!(
                    "guidance is longer than {MAX_STEER_CHARS} characters"
                ))));
            }
        }
        let note = truncate_chars(note, MAX_NOTE_CHARS);
        let surface = truncate_chars(surface, 32);

        state.escalations[index].answered = Some(AnsweredEscalation {
            at: self.now(),
            surface: surface.clone(),
            decision: decision.clone(),
            note: note.clone(),
        });
        self.emit(
            state,
            EventKind::EscalationAnswered,
            json!({
                "escalation_id": escalation_id.to_string(),
                "surface": surface,
                "decision": serde_json::to_value(&decision).unwrap_or(serde_json::Value::Null),
                "note": note,
            }),
        )
        .await?;

        // docs/06-ensemble.md "R3 — Human". An answer is an authorization, never a
        // directive: the only thing a person can cause directly is a stop. Nothing
        // below can produce `Finish`; `ClaimComplete` only buys a re-measurement, and
        // the next cycle's policy decision stands on the evidence.
        match decision {
            HumanDecision::Stop => {
                self.cancel_run(state, ls, "stopped by operator").await?;
            }
            HumanDecision::Accept => self.resume(state, job, ls, false).await?,
            HumanDecision::Recheck | HumanDecision::ClaimComplete => {
                self.resume(state, job, ls, true).await?;
            }
            HumanDecision::Guidance { text } => {
                if self.deliver_human_steer(state, ls, &text).await?.is_none() {
                    self.emit(
                        state,
                        EventKind::Notification,
                        json!({
                            "code": "guidance_not_delivered",
                            "message": "guidance was recorded but there is no active worker to receive it",
                            "escalation_id": escalation_id.to_string(),
                        }),
                    )
                    .await?;
                }
                self.resume(state, job, ls, false).await?;
            }
        }
        self.snapshot(state).await?;
        Ok(Ok(()))
    }

    /// Leave `NeedsHuman` for `Running` and arrange an immediate re-assessment.
    ///
    /// With `refresh`, re-run the command evidence providers first (the M2
    /// mechanism; the full R0 rung is M5). `BeforeFinish` providers are not run here:
    /// they stay behind the gate in front of `Finish`, which a refresh re-arms.
    async fn resume(
        &self,
        state: &mut RunState,
        job: &Job,
        ls: &mut Loop,
        refresh: bool,
    ) -> Result<(), RuntimeError> {
        self.end_human_wait(state, ls);
        self.transition(state, FactoryStatus::Running).await?;
        if refresh {
            for trigger in [RunWhen::WorkerCompleted, RunWhen::OnDemand] {
                self.collect_evidence(state, job, trigger, &mut ls.evidence_outcomes)
                    .await?;
            }
            ls.before_finish_ran = false;
            ls.completed_while_waiting = false;
        } else if ls.completed_while_waiting {
            // The worker finished while nobody was looking; its evidence is still due.
            self.collect_evidence(
                state,
                job,
                RunWhen::WorkerCompleted,
                &mut ls.evidence_outcomes,
            )
            .await?;
            ls.completed_while_waiting = false;
        }
        let now = self.instant();
        ls.next_idle_at = now + self.deps.config.idle_poll_interval;
        ls.request_assessment(now);
        Ok(())
    }

    /// Add the time since the current human wait began to the run's account.
    fn end_human_wait(&self, state: &mut RunState, ls: &mut Loop) {
        if let Some(started) = ls.wait_started.take() {
            let waited = (self.now() - started)
                .whole_milliseconds()
                .clamp(0, i128::from(u64::MAX));
            state.human_wait_ms = state
                .human_wait_ms
                .saturating_add(u64::try_from(waited).unwrap_or(0));
        }
    }

    /// End the run as `Cancelled`: terminate the active worker through the normal
    /// path, record it, and transition. Legal from every non-terminal state.
    async fn cancel_run(
        &self,
        state: &mut RunState,
        ls: &mut Loop,
        reason: &str,
    ) -> Result<(), RuntimeError> {
        self.end_human_wait(state, ls);
        if let Some(handle) = ls.worker.take() {
            self.stop_worker(state, handle, StopWhy::Cancelled(reason))
                .await?;
        }
        self.transition(state, FactoryStatus::Cancelled).await?;
        ls.finish_reason = Some(reason.to_owned());
        // Snapshotted here on purpose: a cancel must be observable as soon as it is accepted,
        // before the drive loop reaches `emit_final`. That means the terminal status can be
        // visible a moment before `RUN_FINISHED` is appended, which is why a follower does not
        // end on the status alone — see `follow::Follow`.
        self.snapshot(state).await?;
        Ok(())
    }

    /// Fold a worker's reported outcome into state.
    async fn fold_finished(
        &self,
        state: &mut RunState,
        handle: &WorkerHandle,
        outcome: &WorkerOutcome,
    ) -> Result<(), RuntimeError> {
        self.finish_worker(state, handle.id, outcome, handle)
            .await?;
        state.counters.failures += u32::from(!outcome.is_success());
        Ok(())
    }

    /// End a worker and **reap** it: when this returns, the worker's task has
    /// ended and (for a subprocess) its process group has been signalled to death,
    /// so no process of this run is alive or can still come alive.
    ///
    /// A completion already in the channel wins over a termination: a worker that
    /// finished before the stop was seen is recorded as completed, not as killed.
    /// The final output is synced and flushed after the reap, so the last bytes
    /// the pumps delivered are not lost.
    async fn stop_worker(
        &self,
        state: &mut RunState,
        mut handle: WorkerHandle,
        why: StopWhy<'_>,
    ) -> Result<(), RuntimeError> {
        if handle.outcome.is_none() {
            match handle.done.try_recv() {
                Ok(result) => handle.outcome = Some(result?),
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    handle.outcome = Some(lost_worker_outcome());
                }
                Err(mpsc::error::TryRecvError::Empty) => {}
            }
        }
        if let Some(outcome) = handle.outcome.take() {
            // It had already finished on its own; there is nothing to terminate.
            return self.fold_finished(state, &handle, &outcome).await;
        }

        self.terminate_and_reap(&mut handle, why.terminate_reason())
            .await;

        self.flush_output(state, &handle, true).await?;
        let snapshot = handle
            .output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let ended_at = self.now();
        let activity = handle
            .activity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(record) = state.workers.iter_mut().find(|w| w.id == handle.id) {
            record.status = WorkerStatus::Stopped;
            record.exit = Some(aftman_core::job::WorkerExit::terminated());
            record.ended_at = Some(ended_at);
            record.output = snapshot;
            // A stopped worker's activity is still evidence: a run that was interrupted
            // mid-loop should say so rather than look like a run that did nothing.
            record.activity = activity;
        }
        self.emit(
            state,
            EventKind::WorkerTerminated,
            json!({
                "worker_id": handle.id.to_string(),
                "cause": why.cause(),
                "reason": why.reason(),
            }),
        )
        .await?;
        Ok(())
    }

    /// Terminate a worker, re-issuing until its task reports.
    ///
    /// `Worker::terminate` only affects a worker that has already launched. A task
    /// that was spawned but has not yet reached `Worker::run` would otherwise be
    /// missed, and would launch after the run was reported over. So the call is
    /// repeated until the task reports, and the task is aborted if it does not within
    /// `REAP_TIMEOUT` (backends kill their process group when dropped).
    async fn terminate_and_reap(&self, handle: &mut WorkerHandle, reason: TerminateReason) {
        let worker = Arc::clone(&self.deps.worker);
        let id = handle.id;
        let reissue = async {
            loop {
                if let Err(error) = worker.terminate(reason).await {
                    tracing::warn!(%error, worker = %id, "terminate failed; will retry");
                }
                tokio::time::sleep(REAP_REISSUE).await;
            }
        };
        let done = &mut handle.done;
        let reaped = tokio::time::timeout(REAP_TIMEOUT, async {
            tokio::select! {
                _ = done.recv() => {}
                _ = reissue => {}
            }
        })
        .await
        .is_ok();
        if !reaped {
            tracing::warn!(worker = %id, "worker did not stop in time; aborting its task");
        }
        handle.task.abort();
        let _ = (&mut handle.task).await;
    }

    /// Deliver a human steer to the active worker and record it.
    ///
    /// `None` when there is no active worker. A refusal or an unsupported backend is
    /// an outcome, not an error. The supervisor's steer budget is untouched: the
    /// record carries `SteerSource::Human` and `steers_sent` is not incremented.
    async fn deliver_human_steer(
        &self,
        state: &mut RunState,
        ls: &Loop,
        message: &str,
    ) -> Result<Option<SteerOutcome>, RuntimeError> {
        let Some(worker_id) = ls
            .worker
            .as_ref()
            .filter(|h| h.outcome.is_none())
            .map(|h| h.id)
        else {
            return Ok(None);
        };
        let outcome = match self.deps.worker.steer(message).await {
            Ok(outcome) => outcome,
            Err(WorkerError::SteeringUnsupported) => SteerOutcome::Unsupported,
            Err(error) => SteerOutcome::Rejected {
                reason: truncate_chars(&error.to_string(), 200),
            },
        };
        let at = self.now();
        let excerpt = truncate_chars(message, 200);
        if let Some(record) = state.workers.iter_mut().find(|w| w.id == worker_id) {
            // The grace window protects a worker that was actually interrupted.
            if outcome.is_accepted() {
                record.last_steered_at = Some(at);
            }
            record.steers.push(SteerRecord {
                at,
                message_excerpt: excerpt.clone(),
                outcome: outcome.clone(),
                queue_depth: None,
                source: SteerSource::Human,
            });
        }
        self.emit(
            state,
            EventKind::WorkerSteered,
            json!({
                "source": SteerSource::Human.as_str(),
                "worker_id": worker_id.to_string(),
                "outcome": format!("{outcome:?}"),
                "accepted": outcome.is_accepted(),
                "excerpt": excerpt,
            }),
        )
        .await?;
        Ok(Some(outcome))
    }

    /// Flush the worker's new output as one `WorkerOutput` event.
    ///
    /// Carries only the text that arrived since the previous flush, capped at
    /// [`WORKER_OUTPUT_EVENT_CAP_BYTES`], and is a no-op when nothing arrived — an
    /// idle tick must never emit an empty event. `exit` marks the last flush of a
    /// worker.
    async fn flush_output(
        &self,
        state: &mut RunState,
        handle: &WorkerHandle,
        exit: bool,
    ) -> Result<(), RuntimeError> {
        let (text, new_bytes, truncated) = {
            let mut log = handle
                .output
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let new_bytes = log.take_unflushed();
            if new_bytes == 0 {
                return Ok(());
            }
            let wanted = usize::try_from(new_bytes)
                .unwrap_or(usize::MAX)
                .min(WORKER_OUTPUT_EVENT_CAP_BYTES);
            // The tail holds the newest bytes, so the last `wanted` bytes of it are
            // the new text (less whatever the log itself had to drop).
            let text = log.tail(wanted).to_owned();
            let truncated = (text.len() as u64) < new_bytes;
            (text, new_bytes, truncated)
        };
        self.emit(
            state,
            EventKind::WorkerOutput,
            json!({
                "worker_id": handle.id.to_string(),
                "text": text,
                "bytes": new_bytes,
                "truncated": truncated,
                "exit": exit,
            }),
        )
        .await?;
        Ok(())
    }

    fn questions(&self, registry: &ResponsibilityRegistry) -> BTreeMap<CheckKey, Question> {
        registry
            .checks()
            .into_iter()
            .map(|c| (c.key, c.question))
            .collect()
    }

    async fn transition(
        &self,
        state: &mut RunState,
        to: FactoryStatus,
    ) -> Result<(), RuntimeError> {
        let from = state.status;
        state.status = from.transition(to)?;
        self.emit(
            state,
            EventKind::StateChanged,
            json!({ "from": from.as_str(), "to": to.as_str() }),
        )
        .await
        .map(|_| ())
    }

    async fn snapshot(&self, state: &mut RunState) -> Result<(), RuntimeError> {
        self.deps.store.snapshot(state).await?;
        Ok(())
    }

    /// The last event of a run. Ordering matters to every observer: append to the
    /// log, then write the final snapshot, and only then tell the sink, so a
    /// subscriber that reacts to `RUN_FINISHED` by calling `run.get` always sees the
    /// terminal status.
    async fn emit_final(
        &self,
        state: &mut RunState,
        payload: serde_json::Value,
    ) -> Result<(), RuntimeError> {
        let seq = state.last_seq.next();
        let event = EventEnvelope::new(
            seq,
            state.header.id,
            self.now(),
            EventKind::RunFinished,
            payload,
        );
        self.deps.store.append(&event).await?;
        state.last_seq = seq;
        // Snapshot *after* the append, never before: the snapshot is how a reader learns the
        // run is over, so a terminal status must never be visible before the event that says
        // so. A follower that ends on the status would otherwise never deliver `RUN_FINISHED`.
        self.deps.store.snapshot(state).await?;
        self.deps.sink.emit(&event);
        Ok(())
    }

    async fn emit(
        &self,
        state: &mut RunState,
        kind: EventKind,
        payload: serde_json::Value,
    ) -> Result<EventEnvelope, RuntimeError> {
        let seq = state.last_seq.next();
        let event = EventEnvelope::new(seq, state.header.id, self.now(), kind, payload);
        self.deps.store.append(&event).await?;
        state.last_seq = seq;
        self.deps.sink.emit(&event);
        Ok(event)
    }

    /// The most recent events, for the model-facing history.
    ///
    /// Reads a bounded *tail*, not the whole log. This runs every cycle, so a full read is
    /// O(n) per cycle and O(n²) over a run — with the entire log, `WORKER_OUTPUT` payloads
    /// included, in memory each time. The window is several times `events_history` because
    /// `WorkerOutput` events are filtered out below and must not crowd out the rest.
    async fn recent_event_summaries(&self, run_id: RunId, last_seq: EventSeq) -> Vec<EventSummary> {
        /// How far back to read. Larger than `ObsCaps::events_history` (30) so that
        /// filtering out worker output still leaves a full history.
        const WINDOW: u64 = 256;
        let from = EventSeq(last_seq.0.saturating_sub(WINDOW).max(1));
        match self.deps.store.events(run_id, from).await {
            // `WorkerOutput` is left out of the model-facing history: summaries carry
            // no payload text anyway, and worker output must not be echoed into what
            // reaches the model through the event log (hard rule 5: every field that
            // reaches a model is capped, and output has its own path).
            Ok(events) => events
                .into_iter()
                .filter(|e| e.kind != EventKind::WorkerOutput)
                .map(|e| EventSummary {
                    seq: e.seq,
                    kind: e.kind.as_str().to_owned(),
                    summary: e.payload["summary"]
                        .as_str()
                        .map_or_else(|| e.summary(), ToOwned::to_owned),
                })
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Copy a live worker's streamed output into its record.
    ///
    /// The worker writes into a shared bounded log; the record is what the
    /// observation reads. Without this sync, a running worker's tail would always be
    /// empty.
    /// Copy what the worker is producing *now* into its record.
    ///
    /// Both halves matter while the worker runs: the log tail is how a model sees what the
    /// agent is saying, and the activity summary is how it sees what the agent is *doing*.
    /// A worker that has called the same tool two hundred times looks busy in the log and
    /// looks looping here, and the second is the one the decision turns on.
    fn sync_live(&self, state: &mut RunState, handle: &WorkerHandle) {
        let snapshot = handle
            .output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let activity = handle
            .activity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(record) = state.workers.iter_mut().find(|w| w.id == handle.id) {
            record.output = snapshot;
            record.activity = activity;
        }
    }

    async fn finish_worker(
        &self,
        state: &mut RunState,
        id: WorkerId,
        outcome: &WorkerOutcome,
        handle: &WorkerHandle,
    ) -> Result<(), RuntimeError> {
        // The worker's last words, flushed before its completion is recorded.
        self.flush_output(state, handle, true).await?;
        let ended_at = self.now();
        let snapshot = handle
            .output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let activity = handle
            .activity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(record) = state.workers.iter_mut().find(|w| w.id == id) {
            record.status = outcome.status();
            record.exit = outcome.exit();
            record.ended_at = Some(ended_at);
            // The final output must survive the worker's exit; otherwise the last
            // thing a worker did before finishing is invisible.
            record.output = snapshot;
            // Same for what it did: the summary is what says whether the work that just
            // finished was eight tool calls or the same one eight hundred times.
            record.activity = activity;
        }
        self.emit(
            state,
            EventKind::WorkerCompleted,
            json!({
                "worker_id": id.to_string(),
                "outcome": format!("{outcome:?}"),
                "success": outcome.is_success(),
            }),
        )
        .await?;
        Ok(())
    }

    /// Run the providers due for one trigger, recording each outcome.
    ///
    /// Every provider produces an event whether it succeeded, failed, or could not be
    /// launched: a check that expects a measurement and silently gets nothing is how a
    /// run finishes on a claim.
    async fn collect_evidence(
        &self,
        state: &mut RunState,
        job: &Job,
        trigger: RunWhen,
        outcomes: &mut Vec<EvidenceOutcome>,
    ) -> Result<(), RuntimeError> {
        let due: Vec<aftman_core::evidence::CommandProvider> = self
            .deps
            .evidence
            .due(trigger)
            .into_iter()
            .cloned()
            .collect();
        for provider in due {
            // Evidence runs with the same forwarded environment as the worker, so a
            // test command finds the same `python` or `node` the agent did. Without
            // this a provider would run with the daemon's `PATH` and could fail on a
            // toolchain the worker had no trouble with.
            let forwarded: Vec<(String, String)> = self
                .deps
                .forwarded_env
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect();
            let outcome = aftman_workers::run_provider(
                &provider,
                &job.repo,
                &self.deps.evidence_temp_dir,
                &forwarded,
                self.now(),
            )
            .await;
            let payload = json!({
                "provider": outcome.provider.to_string(),
                "argv": outcome.argv,
                "exit_code": outcome.exit_code,
                "success": outcome.success,
                "duration_ms": outcome.duration.as_millis() as u64,
                "truncated": outcome.truncated,
                "launch_error": outcome.launch_error,
                "summary": outcome.summary(),
            });
            keep_latest(outcomes, outcome);
            self.emit(state, EventKind::EvidenceCollected, payload)
                .await?;
        }
        Ok(())
    }

    /// Spawn a worker as an independent task.
    #[allow(clippy::too_many_arguments)]
    async fn start_worker(
        &self,
        state: &mut RunState,
        job: &Job,
        task: TaskKind,
        mission: String,
        parent: Option<WorkerId>,
    ) -> Result<WorkerHandle, RuntimeError> {
        let id = WorkerId::new();
        let capacity = self.deps.config.obs_tail_chars.max(4_096);
        let output = Arc::new(Mutex::new(BoundedLog::new(capacity, capacity)));
        let activity: Arc<Mutex<Option<StreamActivity>>> = Arc::new(Mutex::new(None));

        let mut record = WorkerRecord::new(
            id,
            self.deps.config.worker,
            task,
            mission.clone(),
            BoundedLog::new(capacity, capacity),
            self.deps.worker.supports_steering(),
        );
        record.parent = parent;
        record.status = WorkerStatus::Running;
        record.started_at = Some(self.now());
        record.artifact_ref = self.deps.worker.artifact_ref();
        state.workers.push(record);
        state.counters.workers_run += 1;
        if task == TaskKind::Code {
            state.counters.attempts += 1;
        } else {
            state.counters.verifiers_run += 1;
        }

        self.emit(
            state,
            EventKind::WorkerStarted,
            json!({
                "worker_id": id.to_string(),
                "kind": self.deps.config.worker.as_str(),
                "task": format!("{task:?}"),
                "mission_excerpt": mission.chars().take(200).collect::<String>(),
                "supports_steering": self.deps.worker.supports_steering(),
            }),
        )
        .await?;

        let (signal_tx, signal_rx) = mpsc::unbounded_channel();
        let (done_tx, done_rx) = mpsc::unbounded_channel();
        let sink = WorkerEventSink::new(signal_tx);

        let ctx = WorkerCtx {
            id,
            kind: self.deps.config.worker,
            task,
            repo: job.repo.clone(),
            mission,
            model: self.deps.config.worker_model.clone(),
            timeout: self.deps.config.worker_timeout,
            // The client's allowlisted environment, so the worker finds the toolchain the
            // operator is actually using. `worker_environment` scrubs it again.
            env: self
                .deps
                .forwarded_env
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            output: Arc::clone(&output),
            activity: Arc::clone(&activity),
            ledger: Some(Arc::new(StoreLedger {
                store: Arc::clone(&self.deps.store),
                run_id: state.header.id,
                worker_id: id,
            })),
        };

        let worker = Arc::clone(&self.deps.worker);
        let task = tokio::spawn(async move {
            let outcome = worker.run(ctx, sink).await;
            let _ = done_tx.send(outcome);
        });

        Ok(WorkerHandle {
            id,
            outcome: None,
            signals: signal_rx,
            done: done_rx,
            output,
            activity,
            task,
        })
    }

    /// Apply one directive, returning the (possibly new) worker handle.
    async fn apply(
        &self,
        state: &mut RunState,
        job: &Job,
        directive: Directive,
        worker: Option<WorkerHandle>,
        observation: &Observation,
    ) -> Result<Option<WorkerHandle>, RuntimeError> {
        match directive {
            Directive::Continue => Ok(worker),

            Directive::StartWorker { .. } => {
                if worker.is_some() {
                    return Ok(worker);
                }
                let mission = code_mission(job, observation);
                Ok(Some(
                    self.start_worker(state, job, TaskKind::Code, mission, None)
                        .await?,
                ))
            }

            Directive::Retry { .. } => {
                // A retry supersedes whatever was running.
                if let Some(old) = worker {
                    self.stop_worker(state, old, StopWhy::Superseded).await?;
                }
                let mission = code_mission(job, observation);
                Ok(Some(
                    self.start_worker(state, job, TaskKind::Code, mission, None)
                        .await?,
                ))
            }

            Directive::Verify { .. } => {
                // A verifier judges a *settled* tree, so whatever was running is superseded —
                // exactly as `Retry` does it. Dropping the handle instead ran
                // `WorkerHandle::drop`, which aborts the task: no `WORKER_TERMINATED`, the
                // record left at `Running` for good (`active_worker()` only looks at the last
                // record, so nothing ever closes it), and the process killed by the backend's
                // drop but never reaped or cleared from the ledger.
                if let Some(old) = worker {
                    self.stop_worker(state, old, StopWhy::Superseded).await?;
                }
                let mission = verify_mission(job);
                Ok(Some(
                    self.start_worker(state, job, TaskKind::Verify, mission, None)
                        .await?,
                ))
            }

            Directive::Steer { reason, .. } => {
                let Some(handle) = worker.as_ref() else {
                    return Ok(worker);
                };
                let message = reason.clone();
                // A failed delivery is an outcome, not a reason to drop the handle (and
                // with it the only way to reap the worker).
                let outcome = match self.deps.worker.steer(&message).await {
                    Ok(outcome) => outcome,
                    Err(WorkerError::SteeringUnsupported) => SteerOutcome::Unsupported,
                    Err(error) => SteerOutcome::Rejected {
                        reason: truncate_chars(&error.to_string(), 200),
                    },
                };
                if let Some(record) = state.workers.iter_mut().find(|w| w.id == handle.id) {
                    record.last_steered_at = Some(self.now());
                    record.steers.push(SteerRecord {
                        at: self.now(),
                        message_excerpt: message.chars().take(200).collect(),
                        outcome: outcome.clone(),
                        queue_depth: None,
                        source: SteerSource::Supervisor,
                    });
                }
                if outcome.is_accepted() {
                    state.counters.steers_sent += 1;
                }
                self.emit(
                    state,
                    EventKind::WorkerSteered,
                    json!({
                        "source": SteerSource::Supervisor.as_str(),
                        "worker_id": handle.id.to_string(),
                        "outcome": format!("{outcome:?}"),
                        "accepted": outcome.is_accepted(),
                    }),
                )
                .await?;
                Ok(worker)
            }

            Directive::Stop { ref reason } => {
                if let Some(handle) = worker {
                    self.stop_worker(state, handle, StopWhy::Stopped(reason))
                        .await?;
                }
                self.transition(state, FactoryStatus::Stopped).await?;
                // The handle is gone: the worker is stopped and reaped, and a later
                // `Retry` starts a fresh one.
                Ok(None)
            }

            Directive::Finish { .. } => {
                self.transition(state, FactoryStatus::Finished).await?;
                Ok(worker)
            }

            Directive::Escalate { reason, ask } => {
                let escalation_id = EscalationId::new();
                let kind = escalation_kind(&reason);
                let escalation = Escalation {
                    id: escalation_id,
                    kind,
                    raised_at: self.now(),
                    check: None,
                    ask: ask.clone(),
                    jev_p: None,
                    opinions: Vec::new(),
                    answered: None,
                };
                state.escalations.push(escalation);
                self.emit(
                    state,
                    EventKind::EscalationRaised,
                    json!({
                        // The id a surface needs to answer with (`run.answer`).
                        "escalation_id": escalation_id.to_string(),
                        "kind": kind,
                        "reason": reason,
                        "ask": ask,
                    }),
                )
                .await?;
                self.transition(state, FactoryStatus::NeedsHuman).await?;
                Ok(worker)
            }
        }
    }
}

/// Round a probability to four decimals, for log readability.
fn round4(value: f32) -> f64 {
    (f64::from(value) * 10_000.0).round() / 10_000.0
}

fn escalation_kind(reason: &str) -> EscalationKind {
    if reason.contains("model_unavailable") {
        EscalationKind::ModelUnavailable
    } else if reason.contains("budget") {
        EscalationKind::AssessmentBudget
    } else {
        EscalationKind::Check
    }
}

fn usage_default() -> aftman_core::model::Usage {
    aftman_core::model::Usage::default()
}

/// The mission sent to a coding worker.
///
/// Assembled deterministically and stored verbatim, so a decision can always be
/// traced to the exact text the worker received.
#[must_use]
pub fn code_mission(job: &Job, observation: &Observation) -> String {
    let tests = if observation.tests.is_empty() {
        String::new()
    } else {
        format!(
            "\nPrevious test evidence (a claim from earlier output, not verified by the supervisor): {}\n",
            observation
                .tests
                .iter()
                .map(|t| t.raw.as_str())
                .collect::<Vec<_>>()
                .join("; ")
        )
    };
    format!(
        "You are working in the repository at {}.\n\
         All commands run there. Do not ask for confirmation; complete the work and run the tests.\n\n\
         ## Job\n{}\n{}\n\
         ## Worker contract\n\
         You are being supervised. Report progress through normal tool use and output.\n\
         Do not claim a test passed unless you ran it in this session.\n",
        job.repo.display(),
        job.prompt,
        tests
    )
}

/// The mission sent to an independent verifier.
///
/// The verifier is told explicitly not to trust the coding agent's reports, and not
/// to modify anything. A verifier that mutates the repository makes its own result
/// unusable.
#[must_use]
pub fn verify_mission(job: &Job) -> String {
    format!(
        "You are an independent verifier. You did not write this code and must not trust\n\
         the previous agent's reports.\n\n\
         Repository: {}\n\
         Original job: {}\n\n\
         1. Inspect the working tree and the change (git status, git diff).\n\
         2. Determine what the job actually required.\n\
         3. Run the repository's own checks. Record the exact commands and their results.\n\
         4. Try to falsify the claim that the job is complete.\n\
         5. Report PASS or FAIL, the exact evidence, and any requirement you could not evaluate.\n\n\
         Do not modify the repository. Do not fix anything. Report only.\n",
        job.repo.display(),
        job.prompt
    )
}

/// Whether a resolved check is permitted, for a caller that does not want to reach
/// into the whole record.
#[must_use]
pub fn is_permitted(check: &ResolvedCheck) -> bool {
    check.permitted
}

/// The invariants a decision reported, for a surface.
#[must_use]
pub fn reported_invariants(invariants: &[Invariant]) -> Vec<String> {
    invariants.iter().map(|i| format!("{i:?}")).collect()
}

/// A proposal list rendered for a log line.
#[must_use]
pub fn render_proposals(proposals: &[Proposal]) -> Vec<String> {
    proposals
        .iter()
        .map(|p| format!("{}:{} @{}", p.responsibility, p.rule, p.priority))
        .collect()
}

/// Steering outcomes that count against a worker's steer budget.
#[must_use]
pub fn steer_was_accepted(outcome: &SteerOutcome) -> bool {
    outcome.is_accepted()
}

/// Convenience for building a runtime with a simulation-friendly default set.
#[must_use]
pub fn default_margin() -> f32 {
    0.15
}

#[cfg(test)]
mod tests {
    use aftman_core::evidence::EvidenceOutcome;
    use aftman_core::ids::EvidenceId;

    fn outcome(provider: &str, success: bool) -> EvidenceOutcome {
        EvidenceOutcome {
            provider: EvidenceId::from(provider),
            argv: vec!["t".into()],
            cwd: PathBuf::from("/r"),
            exit_code: Some(i32::from(!success)),
            success,
            duration: std::time::Duration::ZERO,
            output_excerpt: String::new(),
            truncated: false,
            at: time::OffsetDateTime::UNIX_EPOCH,
            launch_error: None,
        }
    }

    #[test]
    fn only_the_latest_outcome_per_provider_is_kept() {
        let mut outcomes = Vec::new();
        super::keep_latest(&mut outcomes, outcome("tests", false));
        super::keep_latest(&mut outcomes, outcome("lint", true));
        for _ in 0..100 {
            super::keep_latest(&mut outcomes, outcome("tests", true));
        }
        assert_eq!(outcomes.len(), 2, "bounded by the number of providers");
        let tests = outcomes
            .iter()
            .find(|o| o.provider == EvidenceId::from("tests"))
            .expect("tests");
        assert!(
            tests.success,
            "the stale failure no longer shadows the later pass"
        );
    }

    #[test]
    fn the_recorded_launch_posture_is_bounded() {
        let mut launch = std::collections::BTreeMap::new();
        for i in 0..100 {
            launch.insert(format!("k{i:03}"), "v".repeat(1_000));
        }
        launch.insert("k".repeat(500), "x".into());
        let bounded = super::bounded_launch(&launch);
        assert_eq!(bounded.len(), super::MAX_LAUNCH_ENTRIES);
        assert!(
            bounded
                .iter()
                .all(|(k, v)| k.chars().count() <= super::MAX_LAUNCH_KEY_CHARS
                    && v.chars().count() <= super::MAX_LAUNCH_VALUE_CHARS)
        );
    }
    use super::{code_mission, verify_mission};
    use aftman_core::job::{Job, JobSource, JobSummary};
    use aftman_core::observation::{
        Counters, GitEvidence, Observation, TestResult, TestSource, TestStatus,
    };
    use std::path::PathBuf;

    fn job() -> Job {
        Job {
            prompt: "Add request retries with backoff".into(),
            repo: PathBuf::from("/repo"),
            source: JobSource::Cli,
        }
    }

    fn observation() -> Observation {
        Observation {
            run_id: aftman_core::ids::RunId::new(),
            status: aftman_core::job::FactoryStatus::Assessing,
            job: JobSummary {
                prompt_excerpt: "add retries".into(),
                prompt_truncated: false,
                repo: PathBuf::from("/repo"),
                elapsed_secs: 0,
            },
            active: None,
            history: Vec::new(),
            git: GitEvidence::default(),
            untracked: Vec::new(),
            instructions: None,
            tests: Vec::new(),
            events: Vec::new(),
            prior: None,
            counters: Counters::default(),
            evidence: Default::default(),
            activity: None,
            observation_note: None,
            output: None,
        }
    }

    #[test]
    fn the_code_mission_states_the_repo_and_the_job() {
        let mission = code_mission(&job(), &observation());
        assert!(mission.contains("/repo"));
        assert!(mission.contains("Add request retries with backoff"));
        assert!(
            mission.contains("Do not claim a test passed unless you ran it"),
            "the worker contract must be explicit"
        );
    }

    #[test]
    fn previous_test_evidence_is_labelled_as_a_claim() {
        let mut obs = observation();
        obs.tests.push(TestResult {
            source: TestSource::WorkerOutput,
            passed: 3,
            failed: 0,
            errored: 0,
            skipped: 0,
            status: TestStatus::Passed,
            raw: "3 passed".into(),
        });
        let mission = code_mission(&job(), &obs);
        assert!(mission.contains("3 passed"));
        assert!(
            mission.contains("not verified by the supervisor"),
            "a claim must never be presented as a fact"
        );
    }

    #[test]
    fn the_verify_mission_forbids_mutation_and_trust() {
        let mission = verify_mission(&job());
        assert!(mission.contains("must not trust"));
        assert!(mission.contains("Do not modify the repository"));
        assert!(mission.contains("Report only"));
    }
}
