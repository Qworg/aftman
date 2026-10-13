//! The run manager: one method per protocol method, no sockets and no JSON.
//!
//! [`RunManager`] owns the store, the [`EventHub`], and the table of live runs.
//! Transports (the Unix-socket [`crate::server::ProtocolServer`] today, HTTP in M4)
//! decode a request, call one method here, and encode the typed result or the
//! [`ProtocolError`]. Every method validates its own parameters, so a transport that
//! builds a request some other way cannot bypass a bound. Nothing in this module
//! interprets a probability: starting, steering, cancelling, and answering are
//! requests handed to the run loop, and deciding is policy's job (`docs/01-hld.md`,
//! hard rule 1).
//!
//! # Building a run is injected
//!
//! This crate cannot construct the real decision model or agent backends (hard
//! rules 2 and 4), so [`RuntimeFactory`] turns validated `run.start` params into a
//! [`FactoryRuntime`] and a [`Job`]. The CLI supplies the real implementation; tests
//! supply one built from simulated parts. The manager hands the factory a sink that
//! feeds the hub.
//!
//! # No zombies
//!
//! A run that is not in the live table must not be non-terminal in the store. Three
//! mechanisms enforce it: the supervisor task of every run marks it `Failed` when
//! `drive` errors, panics, or is aborted; graceful shutdown cancels every run (and
//! aborts, then fails, the ones that do not stop in time); and restart recovery
//! fails whatever an earlier process left behind (`docs/07-surfaces.md` section 9).
//! If even recording the failure keeps failing (the disk is gone) it is retried a
//! bounded number of times and logged loudly; recovery at the next start is the last
//! resort.
//!
//! # What subscribers may rely on
//!
//! - `RUN_FINISHED` is the last event of a run, and by the time a subscriber sees it
//!   the final snapshot is written (`run.get` shows the terminal status) and the
//!   manager treats the run as over (`run.purge` waits for its task rather than
//!   refusing; `runs_active` no longer counts it).
//! - Whatever the manager appends on a run's behalf (`fail_run`) first replays any
//!   logged-but-never-emitted events through the hub, so the stream stays contiguous.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::sync::Notify;
use tokio::task::{AbortHandle, JoinHandle, JoinSet};

use aftman_core::clock::Clock;
use aftman_core::error::TransitionError;
use aftman_core::event::{EventEnvelope, EventKind};
use aftman_core::ids::{EventSeq, RunId};
use aftman_core::job::{FactoryStatus, Job, WorkerExit, WorkerStatus};
use aftman_core::run::{RunFilter, RunState};
use aftman_core::worker::SteerOutcome;
use aftman_store::{RunStore, StoreError};
use aftman_workers::process::{ORPHAN_GRACE, OrphanOutcome, end_orphan};

use crate::control::{ControlError, RunControl};
use crate::hub::{EventHub, SubscribeError, Subscription};
use crate::protocol::{
    DEFAULT_EVENTS_LIMIT, DEFAULT_RUN_LIST_LIMIT, DaemonStatusResult, ErrorCode, JevUsageParams,
    MAX_EVENTS_LIMIT, MAX_RUN_LIST_LIMIT, OkResult, ProtocolError, Request,
    ResponsibilityListResult, RunAnswerParams, RunEventsParams, RunEventsResult, RunGetResult,
    RunIdParams, RunListParams, RunListResult, RunPurgeParams, RunPurgeResult, RunStartParams,
    RunStartResult, RunSteerParams, RunSteerResult, SubscriptionOpenParams, SubscriptionOpenResult,
    effective_limit,
};
use crate::runtime::{FactoryRuntime, RunOutcome, RuntimeError};
use crate::sink::EventSink;

/// Most runs the daemon will supervise at once. Over it, `run.start` is
/// `rate_limited` (retryable).
pub const DEFAULT_MAX_LIVE_RUNS: usize = 16;

/// How long graceful shutdown waits for cancelled runs to persist, before the
/// stragglers are aborted and recorded `Failed`.
pub const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_secs(60);

/// Extra time, after aborting stragglers, for their supervisors to record the failure.
const ABORT_SETTLE: Duration = Duration::from_secs(5);

/// How long `run.purge` waits for a run whose final event has been emitted to finish
/// returning from `drive` before it reports the run live.
const FINISH_WAIT: Duration = Duration::from_secs(5);

/// Attempts at recording a failed run before giving up until the next restart.
const FAIL_ATTEMPTS: u32 = 5;
/// First backoff between attempts; doubles each time.
const FAIL_BACKOFF: Duration = Duration::from_millis(100);
/// Re-reads after losing a race with a write that was still in flight.
const CONFLICT_RETRIES: u32 = 5;

/// The reason recorded for a run an earlier daemon process left unfinished.
pub const RESTART_REASON: &str = "daemon restarted";

/// The reason recorded when shutdown cancels a live run.
pub const SHUTDOWN_REASON: &str = "daemon shutdown";

/// Manager tuning. Every bound is explicit (hard rule 5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManagerConfig {
    pub max_live_runs: usize,
    pub shutdown_grace: Duration,
}

impl Default for ManagerConfig {
    fn default() -> Self {
        Self {
            max_live_runs: DEFAULT_MAX_LIVE_RUNS,
            shutdown_grace: DEFAULT_SHUTDOWN_GRACE,
        }
    }
}

/// What the manager gives a [`RuntimeFactory`] to build a run with. The runtime
/// must use exactly these: the store (so the manager can see the run), the sink
/// (so subscribers see its events), and the clock (so tests control time).
#[derive(Clone)]
pub struct RuntimeContext {
    pub store: Arc<dyn RunStore>,
    pub sink: Arc<dyn EventSink>,
    pub clock: Arc<dyn Clock>,
}

/// A runtime ready to start, and the job it will run.
pub struct BuiltRun {
    pub runtime: Arc<FactoryRuntime>,
    pub job: Job,
}

/// Builds runs, and answers the two questions that need the real model stack.
///
/// Errors are protocol errors so the factory can say `invalid_repo`,
/// `invalid_config`, or `model_unavailable` directly.
#[async_trait]
pub trait RuntimeFactory: Send + Sync {
    /// Turn validated params into a runtime and a job.
    async fn build(
        &self,
        params: &RunStartParams,
        context: RuntimeContext,
    ) -> Result<BuiltRun, ProtocolError>;

    /// `responsibility.list`.
    async fn responsibilities(&self) -> Result<ResponsibilityListResult, ProtocolError>;

    /// `jev.usage`: opaque to this crate.
    async fn usage(&self, params: &JevUsageParams) -> Result<Value, ProtocolError>;
}

// ---------------------------------------------------------------------------
// The sink between the runtime and the hub
// ---------------------------------------------------------------------------

#[derive(Default)]
struct SinkState {
    /// Runs whose `RUN_FINISHED` has been emitted.
    finishing: HashSet<RunId>,
    /// Highest `seq` emitted to the hub, per run.
    emitted: HashMap<RunId, u64>,
}

/// Wraps the hub for the runs of this process. It remembers the last `seq` it
/// forwarded per run, so [`fail_run`] can bridge a gap (an event that reached the
/// log but never the hub), and which runs have emitted `RUN_FINISHED`, so the
/// manager can treat them as over.
pub struct ManagedSink {
    hub: EventHub,
    state: Mutex<SinkState>,
}

impl ManagedSink {
    #[must_use]
    pub fn new(hub: EventHub) -> Self {
        Self {
            hub,
            state: Mutex::new(SinkState::default()),
        }
    }

    fn state(&self) -> MutexGuard<'_, SinkState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[must_use]
    pub fn hub(&self) -> &EventHub {
        &self.hub
    }

    fn is_finishing(&self, id: RunId) -> bool {
        self.state().finishing.contains(&id)
    }

    fn release(&self, id: RunId) {
        let mut state = self.state();
        state.finishing.remove(&id);
        state.emitted.remove(&id);
    }
}

/// Where [`fail_run`] publishes what it appends.
///
/// Everything it appends goes to the store first and then to [`EventSink::emit`]. A sink
/// that tracks what it has already forwarded (the daemon's [`ManagedSink`]) also gets the
/// chance to bridge a gap before new events are published.
pub trait FailSink: EventSink {
    /// Called with the run's log before anything new is appended. A sink that has been
    /// forwarding this run's events and missed some replays them here, so subscribers
    /// see a contiguous stream. The default does nothing.
    fn catch_up(&self, _id: RunId, _log: &[EventEnvelope]) {}
}

impl FailSink for ManagedSink {
    /// Forward events that are in the log but were never emitted for a run this
    /// process has been emitting for. A run with no history here (an old run seen
    /// by recovery) needs nothing: no subscriber can have seen a prefix of it.
    fn catch_up(&self, id: RunId, log: &[EventEnvelope]) {
        let Some(last) = self.state().emitted.get(&id).copied() else {
            return;
        };
        for event in log.iter().filter(|e| e.seq.get() > last) {
            self.emit(event);
        }
    }
}

/// An arbitrary sink, with no gap bridging: for the embedded (`--no-daemon`) path.
pub struct PlainSink<'a>(pub &'a dyn EventSink);

impl EventSink for PlainSink<'_> {
    fn emit(&self, event: &EventEnvelope) {
        self.0.emit(event);
    }
}

impl FailSink for PlainSink<'_> {}

impl EventSink for ManagedSink {
    fn emit(&self, event: &EventEnvelope) {
        {
            let mut state = self.state();
            let seq = state.emitted.entry(event.run_id).or_default();
            *seq = (*seq).max(event.seq.get());
            // Before the hub hears it, so a subscriber reacting to RUN_FINISHED
            // already finds the manager treating the run as over.
            if event.kind == EventKind::RunFinished {
                state.finishing.insert(event.run_id);
            }
        }
        self.hub.emit(event);
    }
}

// ---------------------------------------------------------------------------
// fail_run
// ---------------------------------------------------------------------------

/// Why recovery or failure-marking could not touch a run.
#[derive(Debug, thiserror::Error)]
pub enum FailRunError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Transition(#[from] TransitionError),
}

/// What [`fail_run`] found and did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailOutcome {
    /// The run is now `Failed`, with events and a snapshot.
    Failed,
    /// The log already has a `RunFinished` and the snapshot agrees: nothing to do.
    AlreadyFinished,
    /// The log was already over but the files disagreed (a snapshot trailing the
    /// log, a terminal status with no `RunFinished`); they were reconciled from the
    /// log.
    Repaired,
    /// Someone else (another process, or this one) is driving the run.
    Claimed,
    /// The directory held no run (only a lock file); it was removed.
    Removed,
    /// The run was deleted while we looked.
    Gone,
}

struct Emitter<'a> {
    store: &'a dyn RunStore,
    sink: &'a dyn FailSink,
    clock: &'a dyn Clock,
}

impl Emitter<'_> {
    /// Append, then publish: nothing is announced that is not durable.
    async fn emit(
        &self,
        state: &mut RunState,
        kind: EventKind,
        payload: Value,
    ) -> Result<(), StoreError> {
        let seq = state.last_seq.next();
        let event = EventEnvelope::new(seq, state.header.id, self.clock.now(), kind, payload);
        self.store.append(&event).await?;
        state.last_seq = seq;
        self.sink.emit(&event);
        Ok(())
    }

    /// `RUN_FINISHED`: append, final snapshot, then publish.
    async fn emit_final(&self, state: &mut RunState, payload: Value) -> Result<(), StoreError> {
        let seq = state.last_seq.next();
        let event = EventEnvelope::new(
            seq,
            state.header.id,
            self.clock.now(),
            EventKind::RunFinished,
            payload,
        );
        self.store.append(&event).await?;
        state.last_seq = seq;
        self.store.snapshot(state).await?;
        self.sink.emit(&event);
        Ok(())
    }
}

/// Mark workers still recorded `Running` as stopped; returns their ids. The task
/// that ran them is gone (or never will be seen again), and its `Drop` has aborted
/// the worker.
fn close_workers(
    state: &mut RunState,
    now: time::OffsetDateTime,
) -> Vec<aftman_core::ids::WorkerId> {
    let mut closed = Vec::new();
    for record in &mut state.workers {
        if record.status == WorkerStatus::Running {
            record.status = WorkerStatus::Stopped;
            record.exit = Some(WorkerExit::terminated());
            record.ended_at = Some(now);
            closed.push(record.id);
        }
    }
    closed
}

/// Make a run that nobody is driving consistent and over.
///
/// Takes the run's ownership claim first (so it never touches a run somebody is
/// driving), then reads the log, which is the source of truth: the snapshot may trail
/// it, so the status a run was really in is the last `STATE_CHANGED` in the log.
///
/// - Non-terminal: marked `Failed` with `reason`, via `STATE_CHANGED` and
///   `RUN_FINISHED` continuing the run's `seq`, then a snapshot. Workers still
///   recorded `Running` are closed out first.
/// - Already over in the log (a terminal status, or a `RUN_FINISHED`) but the files
///   disagree: the missing `RUN_FINISHED` is appended and the snapshot rewritten from
///   the log ([`FailOutcome::Repaired`]).
/// - A directory with no log at all: removed.
///
/// Events in the log that never reached the hub are replayed to it first, so the
/// subscribers' stream stays contiguous.
///
/// # Errors
/// A store failure (including a corrupt log), or an illegal transition.
pub async fn fail_run(
    store: &dyn RunStore,
    sink: &dyn FailSink,
    clock: &dyn Clock,
    id: RunId,
    reason: &str,
) -> Result<FailOutcome, FailRunError> {
    fail_run_detailed(store, sink, clock, id, reason)
        .await
        .map(|report| report.outcome)
}

/// [`fail_run`] for a caller that has just a store, a clock and an [`EventSink`]: the
/// embedded `aftman run --no-daemon` path, when `drive` returned an error and left the
/// run non-terminal. Same guarantees (the log's status wins, worker records closed out,
/// contiguous `seq`, an orphaned worker process ended if it can be identified); it only
/// lacks the daemon's bridging of events that reached the log but not the sink, because
/// a plain sink keeps no such record.
///
/// # Errors
/// As [`fail_run`].
pub async fn mark_run_failed(
    store: &dyn RunStore,
    sink: &dyn EventSink,
    clock: &dyn Clock,
    id: RunId,
    reason: &str,
) -> Result<FailOutcome, FailRunError> {
    fail_run(store, &PlainSink(sink), clock, id, reason).await
}

/// A worker process found recorded for a run nobody is driving, and what became of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanReport {
    pub worker: aftman_core::ids::WorkerId,
    pub pgid: i32,
    pub outcome: OrphanOutcome,
}

/// [`fail_run`]'s outcome plus the orphaned worker processes it dealt with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailReport {
    pub outcome: FailOutcome,
    pub orphans: Vec<OrphanReport>,
}

/// [`fail_run`], reporting orphaned workers too.
///
/// With the run's claim held (so nothing is driving it), any worker process recorded in
/// its directory is ended if, and only if, it can be positively identified as that worker
/// ([`aftman_workers::process::end_orphan`]); otherwise it is left alone and reported as
/// [`OrphanOutcome::Unverified`]. The records and the workers' mission files are removed.
/// Orphans are named in the `WORKER_TERMINATED` events of a run that is being failed.
///
/// # Errors
/// As [`fail_run`].
pub async fn fail_run_detailed(
    store: &dyn RunStore,
    sink: &dyn FailSink,
    clock: &dyn Clock,
    id: RunId,
    reason: &str,
) -> Result<FailReport, FailRunError> {
    let _claim = match store.claim(id).await {
        Ok(Some(claim)) => claim,
        Ok(None) => {
            return Ok(FailReport {
                outcome: FailOutcome::Claimed,
                orphans: Vec::new(),
            });
        }
        Err(StoreError::NotFound(_)) => {
            return Ok(FailReport {
                outcome: FailOutcome::Gone,
                orphans: Vec::new(),
            });
        }
        Err(error) => return Err(error.into()),
    };
    let orphans = reap_orphans(store, id).await;
    // A write from a task that was aborted may still be in flight; the store
    // serializes it and refuses a duplicate `seq`, so re-read and go on.
    let mut attempts = 0;
    loop {
        match fail_attempt(store, sink, clock, id, reason, &orphans).await {
            Err(FailRunError::Store(StoreError::SeqConflict { .. }))
                if attempts < CONFLICT_RETRIES =>
            {
                attempts += 1;
            }
            Ok(outcome) => return Ok(FailReport { outcome, orphans }),
            Err(error) => return Err(error),
        }
    }
}

/// End the worker processes recorded for `id`, and forget them. The caller holds the
/// run's claim.
async fn reap_orphans(store: &dyn RunStore, id: RunId) -> Vec<OrphanReport> {
    let recorded = store.worker_processes(id).await.unwrap_or_default();
    let mut reports = Vec::new();
    for (worker, identity) in recorded {
        let outcome = end_orphan(&identity, ORPHAN_GRACE).await;
        match &outcome {
            OrphanOutcome::Killed => {
                tracing::warn!(run_id = %id, %worker, pgid = identity.pgid, "ended a worker orphaned by a dead supervisor");
            }
            OrphanOutcome::AlreadyGone => {}
            OrphanOutcome::Unverified(why) => {
                tracing::error!(run_id = %id, %worker, pgid = identity.pgid, why, "a worker may still be running, but could not be identified; NOT signalled");
            }
        }
        if let Some(file) = &identity.mission_file {
            let _ = tokio::fs::remove_file(file).await;
        }
        let _ = store.set_worker_process(id, worker, None).await;
        reports.push(OrphanReport {
            worker,
            pgid: identity.pgid,
            outcome,
        });
    }
    reports
}

async fn fail_attempt(
    store: &dyn RunStore,
    sink: &dyn FailSink,
    clock: &dyn Clock,
    id: RunId,
    reason: &str,
    orphans: &[OrphanReport],
) -> Result<FailOutcome, FailRunError> {
    let log = match store.read_log(id).await {
        Ok(log) => log,
        Err(StoreError::NotFound(_)) => {
            // No event log: this is not a run, just the leftovers of one that never
            // got started. We hold its claim, so nobody is creating it.
            return match store.delete(id).await {
                Ok(()) => Ok(FailOutcome::Removed),
                Err(StoreError::NotFound(_)) => Ok(FailOutcome::Gone),
                Err(error) => Err(error.into()),
            };
        }
        Err(error) => return Err(error.into()),
    };
    sink.catch_up(id, &log.events);

    let mut state = store.load(id).await?;
    let (snapshot_status, snapshot_seq) = (state.status, state.last_seq);
    let finished = log.events.iter().any(|e| e.kind == EventKind::RunFinished);
    // The log is the truth; the snapshot is a cache that may trail it.
    let logged_status = log
        .events
        .iter()
        .rev()
        .find(|e| e.kind == EventKind::StateChanged)
        .and_then(|e| e.payload.get("to"))
        .and_then(|to| serde_json::from_value::<FactoryStatus>(to.clone()).ok());
    let status = logged_status.unwrap_or(snapshot_status);
    state.last_seq = log.last_seq();
    let closed = close_workers(&mut state, clock.now());
    let emitter = Emitter { store, sink, clock };

    if finished || status.is_terminal() {
        state.status = status;
        if !finished {
            // Terminal in the log but the run never said so.
            emitter
                .emit_final(
                    &mut state,
                    json!({
                        "status": status.as_str(),
                        "reason": "recovered: the run ended without recording RUN_FINISHED",
                    }),
                )
                .await?;
            return Ok(FailOutcome::Repaired);
        }
        let consistent =
            snapshot_status == status && snapshot_seq == state.last_seq && closed.is_empty();
        if consistent {
            return Ok(FailOutcome::AlreadyFinished);
        }
        store.snapshot(&state).await?;
        return Ok(FailOutcome::Repaired);
    }

    let to = status.transition(FactoryStatus::Failed)?;
    state.status = to;
    // Every worker still recorded Running, plus every orphaned process found, gets one
    // WORKER_TERMINATED event: `orphaned` (with what became of the process) if a process
    // was left behind, `run_aborted` otherwise.
    let mut announced: Vec<aftman_core::ids::WorkerId> = closed.clone();
    for orphan in orphans {
        if !announced.contains(&orphan.worker) {
            announced.push(orphan.worker);
        }
    }
    for worker in &announced {
        let payload = match orphans.iter().find(|o| o.worker == *worker) {
            Some(orphan) => {
                let (result, detail) = match &orphan.outcome {
                    OrphanOutcome::Killed => ("killed", String::new()),
                    OrphanOutcome::AlreadyGone => ("already_gone", String::new()),
                    OrphanOutcome::Unverified(why) => ("unverified", why.clone()),
                };
                json!({
                    "worker_id": worker.to_string(),
                    "cause": "orphaned",
                    "reason": reason,
                    "pgid": orphan.pgid,
                    "result": result,
                    "detail": detail,
                })
            }
            None => json!({
                "worker_id": worker.to_string(),
                "cause": "run_aborted",
                "reason": reason,
            }),
        };
        emitter
            .emit(&mut state, EventKind::WorkerTerminated, payload)
            .await?;
    }
    emitter
        .emit(
            &mut state,
            EventKind::StateChanged,
            json!({ "from": status.as_str(), "to": to.as_str(), "reason": reason }),
        )
        .await?;
    emitter
        .emit_final(
            &mut state,
            json!({ "status": to.as_str(), "reason": reason }),
        )
        .await?;
    Ok(FailOutcome::Failed)
}

// ---------------------------------------------------------------------------
// The manager
// ---------------------------------------------------------------------------

/// The result of restart recovery.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    /// Runs marked `Failed` with reason `daemon restarted`.
    pub failed: Vec<RunId>,
    /// Runs left alone because they were already over and consistent.
    pub untouched: Vec<RunId>,
    /// Runs already over whose files disagreed, reconciled from the log.
    pub repaired: Vec<RunId>,
    /// Run directories holding no run at all (only a lock file), removed.
    pub removed: Vec<RunId>,
    /// Runs left alone because another process holds their claim.
    pub claimed_elsewhere: Vec<RunId>,
    /// Runs that could not be read or updated. Logged; never fatal.
    pub unreadable: Vec<(RunId, String)>,
    /// Worker processes found still recorded for a run recovery handled: ended,
    /// already gone, or **left alone because they could not be identified** (see
    /// [`OrphanOutcome`]).
    pub orphans: Vec<(RunId, OrphanReport)>,
}

struct LiveRun {
    control: RunControl,
    /// The supervisor. Taken (once) by shutdown.
    task: Option<JoinHandle<()>>,
    /// The `drive` task, so shutdown can abort a straggler.
    driver: AbortHandle,
}

#[derive(Default)]
struct Live {
    runs: HashMap<RunId, LiveRun>,
    /// Slots promised to `run.start` calls that are still working.
    reserved: usize,
}

/// A slot for one `run.start`, released however the call ends (including a panic in
/// the factory).
struct Reservation {
    manager: Arc<RunManager>,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.manager.live().reserved -= 1;
        self.manager.changed.notify_waiters();
    }
}

/// Most entries of [`DaemonInfo::info`] reported by `daemon.status`.
pub const MAX_INFO_ENTRIES: usize = 32;
/// Longest info key, in bytes.
pub const MAX_INFO_KEY_BYTES: usize = 64;
/// Longest info value, in bytes.
pub const MAX_INFO_VALUE_BYTES: usize = 256;

/// What the composition root tells `daemon.status` about this daemon.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DaemonInfo {
    /// The state directory this daemon serves, so a client can notice that its own
    /// `--data-dir` differs.
    pub state_dir: Option<std::path::PathBuf>,
    /// Free-form facts (for example whether a model provider is configured). Bounded by
    /// [`MAX_INFO_ENTRIES`], [`MAX_INFO_KEY_BYTES`] and [`MAX_INFO_VALUE_BYTES`]: excess
    /// entries are dropped and long strings truncated when the info is set.
    pub info: std::collections::BTreeMap<String, String>,
}

fn truncate_bytes(text: &str, max: usize) -> String {
    let mut out = String::new();
    for c in text.chars() {
        if out.len() + c.len_utf8() > max {
            break;
        }
        out.push(c);
    }
    out
}

impl DaemonInfo {
    fn bounded(self) -> Self {
        Self {
            state_dir: self.state_dir,
            info: self
                .info
                .into_iter()
                .take(MAX_INFO_ENTRIES)
                .map(|(k, v)| {
                    (
                        truncate_bytes(&k, MAX_INFO_KEY_BYTES),
                        truncate_bytes(&v, MAX_INFO_VALUE_BYTES),
                    )
                })
                .collect(),
        }
    }
}

/// See the module documentation.
pub struct RunManager {
    info: Mutex<DaemonInfo>,
    started_at: time::OffsetDateTime,
    store: Arc<dyn RunStore>,
    sink: Arc<ManagedSink>,
    factory: Arc<dyn RuntimeFactory>,
    clock: Arc<dyn Clock>,
    config: ManagerConfig,
    started: tokio::time::Instant,
    live: Mutex<Live>,
    /// Shutdown has begun: no new `run.start`.
    shutting_down: AtomicBool,
    /// Shutdown is over: the hub is closed and nothing can be announced.
    closed: AtomicBool,
    /// Pulsed when a run leaves the live table or a reservation is released.
    changed: Notify,
}

fn store_error(error: &StoreError) -> ProtocolError {
    match error {
        StoreError::NotFound(id) => {
            ProtocolError::new(ErrorCode::UnknownRun, format!("unknown run {id}"))
        }
        other => ProtocolError::new(ErrorCode::Internal, format!("storage error: {other}")),
    }
}

fn control_error(error: ControlError) -> ProtocolError {
    match error {
        ControlError::RunEnded => ProtocolError::new(ErrorCode::WrongState, "the run has ended"),
        ControlError::WrongState(message) => ProtocolError::new(ErrorCode::WrongState, message),
        ControlError::UnknownEscalation(id) => ProtocolError::new(
            ErrorCode::WrongState,
            format!("escalation {id} is unknown or was already answered"),
        ),
        ControlError::Invalid(message) => ProtocolError::invalid(message),
        ControlError::Internal(message) => ProtocolError::new(ErrorCode::Internal, message),
    }
}

fn runtime_error(error: &RuntimeError) -> ProtocolError {
    match error {
        RuntimeError::BadRepo(path) => ProtocolError::new(
            ErrorCode::InvalidRepo,
            format!("repository {path} is not a directory"),
        ),
        other => ProtocolError::new(ErrorCode::Internal, other.to_string()),
    }
}

enum Purge {
    Purged,
    Live,
    Gone,
}

impl RunManager {
    /// A manager over `store`. `hub` must be built over the same store.
    #[must_use]
    pub fn new(
        store: Arc<dyn RunStore>,
        hub: EventHub,
        factory: Arc<dyn RuntimeFactory>,
        clock: Arc<dyn Clock>,
        config: ManagerConfig,
    ) -> Arc<Self> {
        let started = clock.instant();
        let started_at = clock.now();
        Arc::new(Self {
            info: Mutex::new(DaemonInfo::default()),
            started_at,
            store,
            sink: Arc::new(ManagedSink::new(hub)),
            factory,
            clock,
            config,
            started,
            live: Mutex::new(Live::default()),
            shutting_down: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            changed: Notify::new(),
        })
    }

    fn live(&self) -> MutexGuard<'_, Live> {
        self.live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Set what `daemon.status` reports about this daemon (bounded; see [`DaemonInfo`]).
    pub fn set_info(&self, info: DaemonInfo) {
        *self
            .info
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = info.bounded();
    }

    /// Whether this daemon is driving `run_id` right now, so that a subscription to it
    /// will keep receiving events. False for a run that has ended, that is alive in
    /// another process (a `--no-daemon` run), or that is non-terminal but orphaned.
    #[must_use]
    pub fn is_live(&self, run_id: RunId) -> bool {
        self.live().runs.contains_key(&run_id) && !self.sink.is_finishing(run_id)
    }

    /// The hub this manager publishes through.
    #[must_use]
    pub fn hub(&self) -> &EventHub {
        self.sink.hub()
    }

    /// Whether shutdown has begun.
    #[must_use]
    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::SeqCst)
    }

    /// Runs currently in the live table (including one whose final event has been
    /// emitted but whose task has not yet returned).
    #[must_use]
    pub fn live_run_count(&self) -> usize {
        self.live().runs.len()
    }

    // ---- recovery -----------------------------------------------------------

    /// Restart recovery (`docs/07-surfaces.md` section 9). Call before serving.
    ///
    /// A run whose snapshot is terminal *and* whose log ends with `RUN_FINISHED` (a
    /// read of the last few KiB, not the whole log) is over and is not read further.
    /// Every
    /// other run is handed to [`fail_run`] with reason [`RESTART_REASON`]: failed if
    /// nobody is driving it, reconciled if its log is already over, removed if it is
    /// an empty shell, skipped if another process holds its claim. Resuming is not
    /// implemented in M3. A run that cannot be read is logged and skipped: a damaged
    /// run directory must not stop the daemon from starting.
    pub async fn recover(&self) -> RecoveryReport {
        let mut report = RecoveryReport::default();
        let ids = match self.store.ids().await {
            Ok(ids) => ids,
            Err(error) => {
                tracing::error!(%error, "recovery could not enumerate runs");
                return report;
            }
        };
        for id in ids {
            if self
                .store
                .snapshot_status(id)
                .await
                .is_ok_and(FactoryStatus::is_terminal)
                && self.store.ends_with_run_finished(id).await.unwrap_or(false)
            {
                report.untouched.push(id);
                continue;
            }
            let detailed = fail_run_detailed(
                self.store.as_ref(),
                self.sink.as_ref(),
                self.clock.as_ref(),
                id,
                RESTART_REASON,
            )
            .await;
            self.sink.release(id);
            let outcome = detailed.map(|found| {
                report
                    .orphans
                    .extend(found.orphans.into_iter().map(|orphan| (id, orphan)));
                found.outcome
            });
            match outcome {
                Ok(FailOutcome::Failed) => {
                    tracing::warn!(run_id = %id, "marked an unfinished run Failed: {RESTART_REASON}");
                    report.failed.push(id);
                }
                Ok(FailOutcome::AlreadyFinished) => report.untouched.push(id),
                Ok(FailOutcome::Repaired) => {
                    tracing::warn!(run_id = %id, "reconciled a run's snapshot with its log");
                    report.repaired.push(id);
                }
                Ok(FailOutcome::Removed) => {
                    tracing::warn!(run_id = %id, "removed an empty run directory");
                    report.removed.push(id);
                }
                Ok(FailOutcome::Claimed) => report.claimed_elsewhere.push(id),
                Ok(FailOutcome::Gone) => {}
                Err(error) => {
                    tracing::error!(run_id = %id, %error, "recovery skipped an unreadable run");
                    report.unreadable.push((id, error.to_string()));
                }
            }
        }
        report
    }

    // ---- daemon.status ------------------------------------------------------

    /// `daemon.status`. `runs_active` counts runs still being supervised; a run
    /// whose final event has been emitted is over.
    ///
    /// # Errors
    /// `internal` when the store cannot be listed.
    pub async fn status(&self) -> Result<DaemonStatusResult, ProtocolError> {
        let total = self.store.ids().await.map_err(|e| store_error(&e))?.len();
        let info = self
            .info
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let active = self
            .live()
            .runs
            .keys()
            .filter(|id| !self.sink.is_finishing(**id))
            .count();
        Ok(DaemonStatusResult {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            uptime_secs: self
                .clock
                .instant()
                .saturating_duration_since(self.started)
                .as_secs(),
            pid: std::process::id(),
            runs_total: total as u64,
            runs_active: active as u64,
            state_dir: info.state_dir,
            started_at: self
                .started_at
                .format(&time::format_description::well_known::Rfc3339)
                .ok(),
            info: info.info,
        })
    }

    // ---- run.start ----------------------------------------------------------

    fn reserve(self: &Arc<Self>) -> Result<Reservation, ProtocolError> {
        let mut live = self.live();
        // Checked under the lock, so shutdown (which sets the flag, then reads
        // `reserved` under the same lock) either sees this reservation or refuses it.
        if self.is_shutting_down() {
            return Err(ProtocolError::new(
                ErrorCode::WrongState,
                "the daemon is shutting down",
            ));
        }
        let used = live.runs.len() + live.reserved;
        if used >= self.config.max_live_runs {
            return Err(ProtocolError::new(
                ErrorCode::RateLimited,
                format!(
                    "{used} runs are already live; the limit is {}",
                    self.config.max_live_runs
                ),
            ));
        }
        live.reserved += 1;
        Ok(Reservation {
            manager: Arc::clone(self),
        })
    }

    /// `run.start`: validate, build, persist (so the id is known), spawn, reply.
    ///
    /// The work runs in its own task, so dropping the caller's future (a client that
    /// hangs up, an HTTP request that is cancelled) cannot strand a half-started
    /// run, and a panic in the factory is an `internal` error rather than a leaked
    /// slot.
    ///
    /// # Errors
    /// `invalid_request`/`invalid_repo`/`invalid_config`/`model_unavailable` from
    /// validation and the factory, `rate_limited` over the live-run bound,
    /// `wrong_state` during shutdown, `internal` otherwise.
    pub async fn run_start(
        self: &Arc<Self>,
        params: RunStartParams,
    ) -> Result<RunStartResult, ProtocolError> {
        Request::RunStart(params.clone()).validate()?;
        let reservation = self.reserve()?;
        let me = Arc::clone(self);
        let work = tokio::spawn(async move {
            let _reservation = reservation;
            me.start_reserved(&params).await
        });
        work.await.unwrap_or_else(|join| {
            tracing::error!(%join, "run.start failed unexpectedly");
            Err(ProtocolError::new(
                ErrorCode::Internal,
                "run.start failed unexpectedly",
            ))
        })
    }

    async fn start_reserved(
        self: &Arc<Self>,
        params: &RunStartParams,
    ) -> Result<RunStartResult, ProtocolError> {
        let built = self
            .factory
            .build(
                params,
                RuntimeContext {
                    store: Arc::clone(&self.store),
                    sink: Arc::clone(&self.sink) as Arc<dyn EventSink>,
                    clock: Arc::clone(&self.clock),
                },
            )
            .await?;
        let runtime = built.runtime;
        // On failure the runtime has already removed whatever it created.
        let started = runtime
            .start(built.job)
            .await
            .map_err(|e| runtime_error(&e))?;
        let run_id = started.run_id();
        let (control, receiver) = RunControl::channel();

        let mut live = self.live();
        if self.closed.load(Ordering::SeqCst) {
            // Shutdown finished while the factory was still working (it outlasted
            // the shutdown wait). Nothing can be announced any more; record the run
            // terminal rather than leave it.
            drop(started);
            drop(live);
            self.fail_after_the_fact(run_id, SHUTDOWN_REASON).await;
            return Err(ProtocolError::new(
                ErrorCode::WrongState,
                "the daemon shut down while the run was being started",
            ));
        }
        let driver = tokio::spawn(async move { runtime.drive(started, Some(receiver)).await });
        let abort = driver.abort_handle();
        let me = Arc::clone(self);
        let task = tokio::spawn(async move { me.supervise(run_id, driver).await });
        live.runs.insert(
            run_id,
            LiveRun {
                control,
                task: Some(task),
                driver: abort,
            },
        );
        Ok(RunStartResult { run_id })
    }

    /// Wait for one run's `drive` task, then make sure the run is not left
    /// non-terminal, and take it out of the live table.
    async fn supervise(
        self: Arc<Self>,
        run_id: RunId,
        driver: JoinHandle<Result<RunOutcome, RuntimeError>>,
    ) {
        let failure = match driver.await {
            Ok(Ok(outcome)) => {
                tracing::info!(%run_id, status = %outcome.status.as_str(), "run ended");
                None
            }
            Ok(Err(error)) => Some(format!("run failed: {error}")),
            Err(join) if join.is_panic() => Some("run task panicked".to_owned()),
            Err(_) => Some("run task aborted".to_owned()),
        };
        if let Some(reason) = failure {
            tracing::error!(%run_id, %reason, "run ended abnormally");
            self.fail_after_the_fact(run_id, &reason).await;
        }
        self.live().runs.remove(&run_id);
        self.sink.release(run_id);
        self.changed.notify_waiters();
    }

    /// [`fail_run`] for a run this process was driving. The claim is released when
    /// the `drive` future ends (normally, by error, by panic, or by abort), before
    /// this runs. Retried with backoff, since the usual cause of failure is a store
    /// that is struggling; if it never works, the next restart's recovery is the last
    /// resort and this says so loudly.
    async fn fail_after_the_fact(&self, run_id: RunId, reason: &str) {
        let mut backoff = FAIL_BACKOFF;
        for attempt in 1..=FAIL_ATTEMPTS {
            match fail_run(
                self.store.as_ref(),
                self.sink.as_ref(),
                self.clock.as_ref(),
                run_id,
                reason,
            )
            .await
            {
                Ok(outcome) => {
                    tracing::debug!(%run_id, ?outcome, "recorded an abnormal run end");
                    return;
                }
                Err(error) => {
                    tracing::error!(%run_id, %error, attempt, "could not record an abnormal run end");
                    if attempt < FAIL_ATTEMPTS {
                        tokio::time::sleep(backoff).await;
                        backoff *= 2;
                    }
                }
            }
        }
        tracing::error!(
            %run_id,
            "GIVING UP recording a run as failed; it stays non-terminal until the next daemon start recovers it"
        );
    }

    // ---- reads --------------------------------------------------------------

    /// `run.list`.
    ///
    /// # Errors
    /// `invalid_request` for a zero limit, `internal` on a store failure.
    pub async fn run_list(&self, params: RunListParams) -> Result<RunListResult, ProtocolError> {
        Request::RunList(params.clone()).validate()?;
        let limit = effective_limit(params.limit, DEFAULT_RUN_LIST_LIMIT, MAX_RUN_LIST_LIMIT)?;
        let filter = RunFilter {
            status: params.status,
            repo: None,
            // One extra row says whether another page exists.
            limit: Some(limit + 1),
            cursor: params.cursor,
        };
        let mut runs = self
            .store
            .list(&filter)
            .await
            .map_err(|e| store_error(&e))?;
        let more = runs.len() > limit;
        runs.truncate(limit);
        let next_cursor = if more {
            runs.last().map(|r| r.id)
        } else {
            None
        };
        Ok(RunListResult { runs, next_cursor })
    }

    /// `run.get`.
    ///
    /// # Errors
    /// `unknown_run`, or `internal` for an unreadable run.
    pub async fn run_get(&self, params: RunIdParams) -> Result<RunGetResult, ProtocolError> {
        self.store
            .load(params.run_id)
            .await
            .map_err(|e| store_error(&e))
    }

    /// `run.events`: one page of the log.
    ///
    /// # Errors
    /// `invalid_request` for a zero limit, `unknown_run`, `internal`.
    pub async fn run_events(
        &self,
        params: RunEventsParams,
    ) -> Result<RunEventsResult, ProtocolError> {
        Request::RunEvents(params.clone()).validate()?;
        let limit = effective_limit(params.limit, DEFAULT_EVENTS_LIMIT, MAX_EVENTS_LIMIT)?;
        let from = params.from_seq.unwrap_or(EventSeq::FIRST);
        let mut events = self
            .store
            .events(params.run_id, from)
            .await
            .map_err(|e| store_error(&e))?;
        let has_more = events.len() > limit;
        events.truncate(limit);
        Ok(RunEventsResult { events, has_more })
    }

    /// `responsibility.list`.
    ///
    /// # Errors
    /// Whatever the factory reports.
    pub async fn responsibilities(&self) -> Result<ResponsibilityListResult, ProtocolError> {
        self.factory.responsibilities().await
    }

    /// `jev.usage`.
    ///
    /// # Errors
    /// `invalid_request`, or whatever the factory reports.
    pub async fn jev_usage(&self, params: JevUsageParams) -> Result<Value, ProtocolError> {
        Request::JevUsage(params.clone()).validate()?;
        self.factory.usage(&params).await
    }

    // ---- subscriptions ------------------------------------------------------

    /// Open an event subscription for a transport to forward.
    ///
    /// # Errors
    /// `unknown_run`, `invalid_request`, `wrong_state` (shutting down), `internal`.
    pub async fn subscribe(
        &self,
        params: &SubscriptionOpenParams,
    ) -> Result<Subscription, ProtocolError> {
        Request::SubscriptionOpen(params.clone()).validate()?;
        self.hub()
            .subscribe(params.run_id, params.from_seq)
            .await
            .map_err(|error| match error {
                SubscribeError::UnknownRun(id) => {
                    ProtocolError::new(ErrorCode::UnknownRun, format!("unknown run {id}"))
                }
                SubscribeError::FromSeqWithoutRun => ProtocolError::invalid(error.to_string()),
                SubscribeError::ShuttingDown => {
                    ProtocolError::new(ErrorCode::WrongState, error.to_string())
                }
                SubscribeError::Store(other) => store_error(&other),
            })
    }

    /// The `subscription.open` result for `params`.
    ///
    /// `live` says whether this daemon is driving the run (always true for an all-runs
    /// stream); when it is false a client that wants to follow the run must poll
    /// `run.events`, because this daemon's hub will never see further events.
    #[must_use]
    pub fn subscription_result(
        &self,
        params: &SubscriptionOpenParams,
        replaced: bool,
    ) -> SubscriptionOpenResult {
        SubscriptionOpenResult {
            live: params.run_id.is_none_or(|id| self.is_live(id)),
            run_id: params.run_id,
            from_seq: params
                .run_id
                .map(|_| EventSeq(params.from_seq.map_or(1, EventSeq::get).max(1))),
            replaced,
        }
    }

    // ---- control ------------------------------------------------------------

    /// The control handle of a live run, or the right error for why there is none:
    /// `unknown_run` for an id the store has never seen, `wrong_state` otherwise.
    async fn control_for(&self, run_id: RunId) -> Result<RunControl, ProtocolError> {
        if let Some(live) = self.live().runs.get(&run_id) {
            return Ok(live.control.clone());
        }
        let state = self.store.load(run_id).await.map_err(|e| store_error(&e))?;
        Err(ProtocolError::new(
            ErrorCode::WrongState,
            format!(
                "run {run_id} is not live (status {})",
                state.status.as_str()
            ),
        ))
    }

    /// `run.steer`.
    ///
    /// # Errors
    /// `unknown_run`, `wrong_state` (not live, or no active worker),
    /// `steering_unsupported` (the backend has no live input channel),
    /// `invalid_request`, `internal`. A backend that *rejects* the message is an
    /// `Ok` result carrying the rejection.
    pub async fn run_steer(&self, params: RunSteerParams) -> Result<RunSteerResult, ProtocolError> {
        Request::RunSteer(params.clone()).validate()?;
        let control = self.control_for(params.run_id).await?;
        match control.steer(params.message).await {
            Ok(SteerOutcome::Unsupported) => Err(ProtocolError::new(
                ErrorCode::SteeringUnsupported,
                "this worker has no live input channel",
            )),
            Ok(outcome) => Ok(outcome),
            Err(error) => Err(control_error(error)),
        }
    }

    /// `run.cancel`.
    ///
    /// # Errors
    /// `unknown_run`, `wrong_state` (not live), `internal`.
    pub async fn run_cancel(&self, params: RunIdParams) -> Result<OkResult, ProtocolError> {
        let control = self.control_for(params.run_id).await?;
        control
            .cancel("cancelled by operator")
            .await
            .map(|()| OkResult::OK)
            .map_err(control_error)
    }

    /// `run.answer`. `surface` is the transport's name for itself (`"cli"` for the
    /// socket server) and is recorded with the answer.
    ///
    /// # Errors
    /// `unknown_run`, `wrong_state` (not live, not waiting, or the escalation is
    /// unknown or already answered), `invalid_request`, `internal`.
    pub async fn run_answer(
        &self,
        params: RunAnswerParams,
        surface: &str,
    ) -> Result<OkResult, ProtocolError> {
        Request::RunAnswer(params.clone()).validate()?;
        let control = self.control_for(params.run_id).await?;
        control
            .answer(params.escalation_id, params.decision, params.note, surface)
            .await
            .map(|()| OkResult::OK)
            .map_err(control_error)
    }

    // ---- purge --------------------------------------------------------------

    /// `run.purge`: delete one ended run, or every ended run.
    ///
    /// A run that is live here, or claimed by another process, is refused with
    /// `wrong_state` (for `all`, skipped). A run whose final event has been emitted
    /// is over: purge waits (bounded) for its task to finish rather than refusing.
    /// The claim is held across the delete. For `all`, a failure on one run is
    /// logged and the rest still go; the count is of runs actually deleted.
    ///
    /// # Errors
    /// `invalid_request` unless exactly one of `run_id` / `all` is given,
    /// `unknown_run`, `wrong_state`, `internal`.
    pub async fn run_purge(&self, params: RunPurgeParams) -> Result<RunPurgeResult, ProtocolError> {
        Request::RunPurge(params.clone()).validate()?;
        if let Some(run_id) = params.run_id {
            return match self.purge_one(run_id).await? {
                Purge::Purged => Ok(RunPurgeResult { purged: 1 }),
                Purge::Gone => Err(ProtocolError::new(
                    ErrorCode::UnknownRun,
                    format!("unknown run {run_id}"),
                )),
                Purge::Live => Err(ProtocolError::new(
                    ErrorCode::WrongState,
                    format!("run {run_id} is live; cancel it before purging"),
                )),
            };
        }
        let mut purged = 0;
        for run_id in self.store.ids().await.map_err(|e| store_error(&e))? {
            match self.purge_one(run_id).await {
                Ok(Purge::Purged) => purged += 1,
                Ok(Purge::Live | Purge::Gone) => {}
                Err(error) => tracing::warn!(%run_id, %error, "could not purge a run"),
            }
        }
        Ok(RunPurgeResult { purged })
    }

    /// Whether `run_id` is still being supervised, after giving a run that has
    /// announced its end (its final event was emitted, or its snapshot already says
    /// terminal) a moment to return from `drive`.
    async fn still_live(&self, run_id: RunId) -> bool {
        let deadline = tokio::time::Instant::now() + FINISH_WAIT;
        loop {
            let changed = self.changed.notified();
            if !self.live().runs.contains_key(&run_id) {
                return false;
            }
            let over = self.sink.is_finishing(run_id)
                || self
                    .store
                    .snapshot_status(run_id)
                    .await
                    .is_ok_and(FactoryStatus::is_terminal);
            if !over {
                return true;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return self.live().runs.contains_key(&run_id);
            }
        }
    }

    async fn purge_one(&self, run_id: RunId) -> Result<Purge, ProtocolError> {
        if self.still_live(run_id).await {
            return Ok(Purge::Live);
        }
        let claim = match self.store.claim(run_id).await {
            Ok(Some(claim)) => claim,
            Ok(None) => return Ok(Purge::Live),
            Err(StoreError::NotFound(_)) => return Ok(Purge::Gone),
            Err(error) => return Err(store_error(&error)),
        };
        let outcome = match self.store.delete(run_id).await {
            Ok(()) => Purge::Purged,
            // Deleted by someone between claiming and now.
            Err(StoreError::NotFound(_)) => Purge::Gone,
            Err(error) => return Err(store_error(&error)),
        };
        drop(claim);
        self.hub().forget(run_id);
        Ok(outcome)
    }

    // ---- shutdown -----------------------------------------------------------

    /// Graceful shutdown (`docs/07-surfaces.md` section 1): refuse new runs, let
    /// `run.start` calls already in flight land, cancel every live run with reason
    /// [`SHUTDOWN_REASON`], wait (bounded by `shutdown_grace`) for each to be
    /// persisted `Cancelled`, then close every subscription.
    ///
    /// A run that does not finish within the grace period is aborted (its worker
    /// with it) and recorded `Failed`, so even then nothing is left non-terminal.
    /// Idempotent.
    pub async fn shutdown(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
        let deadline = tokio::time::Instant::now() + self.config.shutdown_grace;

        // A start that began before the flag is allowed to finish, and is then
        // cancelled like any other run. Bounded: a factory that never returns must
        // not hold the daemon up.
        loop {
            let changed = self.changed.notified();
            if self.live().reserved == 0 {
                break;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                tracing::error!("a run.start outlasted the shutdown wait");
                break;
            }
        }

        loop {
            let (controls, mut tasks) = {
                let mut live = self.live();
                let mut controls = Vec::new();
                let mut tasks = JoinSet::new();
                for (id, run) in &mut live.runs {
                    if let Some(task) = run.task.take() {
                        controls.push((*id, run.control.clone()));
                        tasks.spawn(async move {
                            let _ = task.await;
                        });
                    }
                }
                (controls, tasks)
            };
            if controls.is_empty() {
                break;
            }
            let mut cancels = JoinSet::new();
            for (id, control) in controls {
                cancels.spawn(async move {
                    // `RunEnded` just means it finished by itself first.
                    if let Err(error) = control.cancel(SHUTDOWN_REASON).await {
                        tracing::debug!(run_id = %id, %error, "shutdown cancel");
                    }
                });
            }
            let all_done = async {
                while cancels.join_next().await.is_some() {}
                while tasks.join_next().await.is_some() {}
            };
            if tokio::time::timeout_at(deadline, all_done).await.is_err() {
                tracing::error!("runs did not stop within the shutdown grace; aborting them");
                for run in self.live().runs.values() {
                    run.driver.abort();
                }
                // Supervisors notice the abort and record the failure.
                let settle = async { while tasks.join_next().await.is_some() {} };
                let _ = tokio::time::timeout(ABORT_SETTLE, settle).await;
                break;
            }
        }
        self.closed.store(true, Ordering::SeqCst);
        self.hub().close_all();
    }
}
