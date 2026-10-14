//! The control plane: steering, cancelling, and answering a run while it runs.
//!
//! Offline and deterministic. Most tests run under `start_paused`, so idle intervals,
//! human waits, and timeouts are virtual time rather than sleeps, and the injected
//! clock follows tokio's clock so wall-clock stamps move with it. The one family of
//! tests that spawns a real process (`echo`, as a command evidence provider) runs in
//! real time, because a paused clock auto-advances while a child process is pending.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use time::OffsetDateTime;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use aftman_core::assessment::{AnswerValue, HumanDecision};
use aftman_core::clock::Clock;
use aftman_core::error::ModelError;
use aftman_core::event::{EventEnvelope, EventKind};
use aftman_core::evidence::EvidencePlan;
use aftman_core::ids::{CheckKey, EscalationId, RunId};
use aftman_core::job::{FactoryStatus, Job, JobSource, RunConfig, WorkerKind, WorkerStatus};
use aftman_core::log::{BoundedLog, Stream};
use aftman_core::model::{
    AskContext, Budget, DecisionModel, ModelAnswers, Purpose, Question, Usage,
};
use aftman_core::responsibility::ResponsibilityRegistry;
use aftman_core::run::RunFilter;
use aftman_core::testing::SimulatedModel;
use aftman_core::worker::{
    SteerOutcome, SteerSource, TerminateReason, Worker, WorkerCtx, WorkerError, WorkerEventSink,
    WorkerOutcome,
};
use aftman_daemon::{
    ControlError, FactoryRuntime, RunControl, RunOutcome, RuntimeDeps, RuntimeError, VecSink,
    runtime::WORKER_OUTPUT_EVENT_CAP_BYTES,
};
use aftman_store::{FileRunStore, RunStore};
use aftman_workers::SimulatedWorker;

type TestResult = Result<(), Box<dyn std::error::Error>>;

// ---- doubles ----------------------------------------------------------------

/// Wall time that follows tokio's (possibly paused) clock.
struct TokioClock {
    base: OffsetDateTime,
    start: tokio::time::Instant,
}

impl TokioClock {
    fn new() -> Self {
        Self {
            base: OffsetDateTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000),
            start: tokio::time::Instant::now(),
        }
    }
}

impl Clock for TokioClock {
    fn now(&self) -> OffsetDateTime {
        self.base + (tokio::time::Instant::now() - self.start)
    }

    fn instant(&self) -> tokio::time::Instant {
        tokio::time::Instant::now()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    /// Not done, nothing wrong, no one needed: a worker that is running is left alone.
    Working,
    /// Asks for a human.
    Escalate,
    /// The worker looks stuck, so policy stops it.
    Stuck,
}

type Gate = (Arc<Notify>, Arc<Notify>);

/// A model whose answers a test can change while the run is live.
struct DialModel {
    mode: Mutex<Mode>,
    calls: AtomicUsize,
    states: Mutex<Vec<Value>>,
    /// `(entered, release)`: the next `ask` signals `entered` and waits for `release`.
    gate: Mutex<Option<Gate>>,
}

impl DialModel {
    /// Make the next model call block until the returned `release` is notified.
    fn arm_gate(&self) -> Gate {
        let gate: Gate = (Arc::new(Notify::new()), Arc::new(Notify::new()));
        *self.gate.lock().unwrap_or_else(|e| e.into_inner()) = Some(gate.clone());
        gate
    }

    fn new(mode: Mode) -> Self {
        Self {
            mode: Mutex::new(mode),
            calls: AtomicUsize::new(0),
            states: Mutex::new(Vec::new()),
            gate: Mutex::new(None),
        }
    }

    fn set(&self, mode: Mode) {
        *self.mode.lock().unwrap_or_else(|e| e.into_inner()) = mode;
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn states(&self) -> Vec<Value> {
        self.states
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

#[async_trait]
impl DecisionModel for DialModel {
    async fn ask(
        &self,
        context: &AskContext,
        state: &Value,
        questions: &BTreeMap<CheckKey, Question>,
        _budget: &Budget,
    ) -> Result<ModelAnswers, ModelError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.states
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(state.clone());
        // Routing is not a cycle: only an assessment call waits at the gate.
        let gate = if context.purpose == Purpose::Assessment {
            self.gate.lock().unwrap_or_else(|e| e.into_inner()).take()
        } else {
            None
        };
        if let Some((entered, release)) = gate {
            entered.notify_one();
            release.notified().await;
        }
        let mode = *self.mode.lock().unwrap_or_else(|e| e.into_inner());
        let needs_human = if mode == Mode::Escalate { 0.99 } else { 0.01 };
        let stuck = if mode == Mode::Stuck { 0.99 } else { 0.02 };
        let table: [(&str, f32); 10] = [
            ("core.completion__ready_to_finish", 0.10),
            ("core.completion__implementation_complete", 0.10),
            ("core.completion__requirements_satisfied", 0.10),
            ("core.verification__tests_sufficient", 0.10),
            ("core.verification__needs_verification", 0.10),
            ("core.worker-health__worker_stuck", stuck),
            ("core.worker-health__work_off_track", 0.02),
            ("core.human-escalation__needs_human", needs_human),
            ("repository.instructions__agents_md_drift", 0.01),
            ("core.worker-health__meaningful_progress", 0.90),
        ];
        let mut answers = BTreeMap::new();
        for key in questions.keys() {
            if let Some((_, p)) = table.iter().find(|(name, _)| CheckKey::from(*name) == *key) {
                answers.insert(key.clone(), AnswerValue::Noul(*p));
            }
        }
        Ok(ModelAnswers {
            model: "dial".into(),
            usage: Usage {
                input_tokens: Some(1),
                output_tokens: Some(1),
            },
            answers,
            request_id: None,
            attempts: 1,
        })
    }
}

/// A worker that runs until the test says otherwise, and can be made to talk.
struct ScriptWorker {
    steer_outcome: SteerOutcome,
    steered: Mutex<Vec<String>>,
    terminated: Mutex<Vec<TerminateReason>>,
    finish: Notify,
    kill: Notify,
    live: Mutex<Option<(Arc<Mutex<BoundedLog>>, WorkerEventSink)>>,
    starts: AtomicUsize,
    /// Workers currently inside `run`.
    active: AtomicUsize,
    /// `run` panics as soon as it starts.
    panic_on_start: bool,
    /// `run` panics when told to complete.
    panic_on_finish: bool,
}

struct ActiveGuard<'a>(&'a AtomicUsize);

impl Drop for ActiveGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl ScriptWorker {
    fn new(steer_outcome: SteerOutcome) -> Self {
        Self {
            steer_outcome,
            steered: Mutex::new(Vec::new()),
            terminated: Mutex::new(Vec::new()),
            finish: Notify::new(),
            kill: Notify::new(),
            live: Mutex::new(None),
            starts: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            panic_on_start: false,
            panic_on_finish: false,
        }
    }

    fn alive(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }

    /// Print something, as the agent would.
    fn say(&self, text: &str) {
        let live = self.live.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((log, sink)) = live.as_ref() {
            log.lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(Stream::Stdout, text);
            sink.activity();
        }
    }

    /// Let the agent complete successfully.
    fn complete(&self) {
        self.finish.notify_one();
    }

    fn steered(&self) -> Vec<String> {
        self.steered
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn terminations(&self) -> Vec<TerminateReason> {
        self.terminated
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

#[async_trait]
impl Worker for ScriptWorker {
    fn kind(&self) -> WorkerKind {
        WorkerKind::Simulated
    }

    fn supports_steering(&self) -> bool {
        self.steer_outcome.is_accepted()
    }

    async fn run(
        &self,
        ctx: WorkerCtx,
        sink: WorkerEventSink,
    ) -> Result<WorkerOutcome, WorkerError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        self.active.fetch_add(1, Ordering::SeqCst);
        let _alive = ActiveGuard(&self.active);
        *self.live.lock().unwrap_or_else(|e| e.into_inner()) = Some((ctx.output.clone(), sink));
        assert!(!self.panic_on_start, "the worker panicked on start");
        tokio::select! {
            () = self.finish.notified() => {
                assert!(!self.panic_on_finish, "the worker panicked on finish");
                Ok(WorkerOutcome::Completed)
            }
            () = self.kill.notified() => Ok(WorkerOutcome::Terminated {
                reason: TerminateReason::Cancelled,
            }),
        }
    }

    async fn steer(&self, message: &str) -> Result<SteerOutcome, WorkerError> {
        if self.steer_outcome.is_accepted() {
            self.steered
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(message.to_owned());
        }
        Ok(self.steer_outcome.clone())
    }

    async fn terminate(&self, reason: TerminateReason) -> Result<(), WorkerError> {
        self.terminated
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(reason);
        self.kill.notify_one();
        Ok(())
    }
}

// ---- fixture ----------------------------------------------------------------

struct Options {
    mode: Mode,
    steer: SteerOutcome,
    evidence: EvidencePlan,
    tweak: Box<dyn Fn(&mut RunConfig)>,
    /// Block the first model call (see `Fixture::gate`).
    gated: bool,
    panic_on_start: bool,
    panic_on_finish: bool,
    /// A cancel-only channel, like the embedded `--no-daemon` run's.
    no_human: bool,
}

impl Options {
    fn new(mode: Mode) -> Self {
        Self {
            mode,
            steer: SteerOutcome::Queued,
            evidence: EvidencePlan::default(),
            tweak: Box::new(|_| {}),
            gated: false,
            panic_on_start: false,
            panic_on_finish: false,
            no_human: false,
        }
    }

    fn no_human(mut self) -> Self {
        self.no_human = true;
        self
    }

    fn gated(mut self) -> Self {
        self.gated = true;
        self
    }

    fn steer(mut self, outcome: SteerOutcome) -> Self {
        self.steer = outcome;
        self
    }

    fn tweak(mut self, tweak: impl Fn(&mut RunConfig) + 'static) -> Self {
        self.tweak = Box::new(tweak);
        self
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    store: Arc<dyn RunStore>,
    sink: Arc<VecSink>,
    model: Arc<DialModel>,
    worker: Arc<ScriptWorker>,
    run_id: RunId,
    control: RunControl,
    task: JoinHandle<Result<RunOutcome, RuntimeError>>,
    /// `(entered, release)` of the armed model gate, when `Options::gated`.
    gate: Option<Gate>,
}

fn base_config() -> RunConfig {
    RunConfig {
        worker: WorkerKind::Simulated,
        min_assessment_interval: Duration::from_secs(1),
        idle_poll_interval: Duration::from_secs(30),
        overall_timeout: Duration::from_secs(100 * 3600),
        worker_timeout: Duration::from_secs(100 * 3600),
        // Every idle tick asks the model, so a run can reach `NeedsHuman` on a tick.
        skip_unchanged_assessments: false,
        max_assessments: 10_000,
        ..RunConfig::default()
    }
}

fn job(repo: &std::path::Path) -> Job {
    Job {
        prompt: "Add request retries with backoff, and cover them with tests.".into(),
        repo: repo.to_path_buf(),
        source: JobSource::Cli,
    }
}

fn runtime_for(
    dir: &tempfile::TempDir,
    store: Arc<dyn RunStore>,
    sink: Arc<VecSink>,
    config: RunConfig,
    model: Arc<dyn DecisionModel>,
    worker: Arc<dyn Worker>,
    evidence: EvidencePlan,
) -> Arc<FactoryRuntime> {
    let temp = dir.path().join("evidence-tmp");
    let _ = std::fs::create_dir_all(&temp);
    Arc::new(FactoryRuntime::new(RuntimeDeps {
        config,
        store,
        model,
        worker,
        registry: ResponsibilityRegistry::builtin(),
        definitions: aftman_core::definitions::DefinitionSet::bundled().unwrap_or_default(),
        evidence,
        evidence_temp_dir: temp,
        clock: Arc::new(TokioClock::new()),
        sink: sink as Arc<dyn aftman_daemon::EventSink>,
        ensemble_enabled: false,
        uncertainty_margin: 0.15,
        steering_grace: Duration::from_secs(30),
        launch: Default::default(),

        forwarded_env: Default::default(),
    }))
}

async fn launch(options: Options) -> Result<Fixture, Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let repo: PathBuf = dir.path().join("repo");
    std::fs::create_dir_all(&repo)?;
    let store: Arc<dyn RunStore> = Arc::new(FileRunStore::new(dir.path().join("state")));
    let sink = Arc::new(VecSink::new());
    let model = Arc::new(DialModel::new(options.mode));
    let mut script = ScriptWorker::new(options.steer.clone());
    script.panic_on_start = options.panic_on_start;
    script.panic_on_finish = options.panic_on_finish;
    let worker = Arc::new(script);
    let gate = options.gated.then(|| model.arm_gate());
    let mut config = base_config();
    (options.tweak)(&mut config);

    let rt = runtime_for(
        &dir,
        Arc::clone(&store),
        Arc::clone(&sink),
        config,
        Arc::clone(&model) as Arc<dyn DecisionModel>,
        Arc::clone(&worker) as Arc<dyn Worker>,
        options.evidence,
    );
    let started = rt.start(job(&repo)).await?;
    let run_id = started.run_id();
    let (control, receiver) = if options.no_human {
        RunControl::channel_without_human()
    } else {
        RunControl::channel()
    };
    let task = tokio::spawn({
        let rt = Arc::clone(&rt);
        async move { rt.drive(started, Some(receiver)).await }
    });
    Ok(Fixture {
        _dir: dir,
        store,
        sink,
        model,
        worker,
        run_id,
        control,
        task,
        gate,
    })
}

/// Poll until `condition` holds. Event-driven: the condition is about what the run
/// has done, never about how long it took.
async fn until(what: &str, mut condition: impl FnMut() -> bool) {
    for _ in 0..200_000 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("gave up waiting for {what}");
}

impl Fixture {
    fn of_kind(&self, kind: EventKind) -> Vec<EventEnvelope> {
        self.sink.of_kind(kind)
    }

    fn count(&self, kind: EventKind) -> usize {
        self.of_kind(kind).len()
    }

    async fn worker_running(&self) {
        until("a worker to start", || {
            self.worker.starts.load(Ordering::SeqCst) > 0
                && self.worker.live.lock().is_ok_and(|l| l.is_some())
        })
        .await;
    }

    /// Wait for the run to be parked in `NeedsHuman`; returns the open escalation.
    async fn needs_human(&self, nth: usize) -> Result<EscalationId, Box<dyn std::error::Error>> {
        until("an escalation", || {
            self.count(EventKind::EscalationRaised) >= nth
                && self
                    .of_kind(EventKind::StateChanged)
                    .last()
                    .is_some_and(|e| e.payload["to"] == "needs_human")
        })
        .await;
        let raised = self.of_kind(EventKind::EscalationRaised);
        let id = raised[nth - 1].payload["escalation_id"]
            .as_str()
            .ok_or("EscalationRaised must carry escalation_id")?;
        Ok(id.parse()?)
    }

    async fn answer(&self, id: EscalationId, decision: HumanDecision) -> Result<(), ControlError> {
        self.control.answer(id, decision, "because", "test").await
    }

    async fn finish(&mut self) -> Result<RunOutcome, Box<dyn std::error::Error>> {
        Ok((&mut self.task).await??)
    }

    async fn cancel_and_join(&mut self) -> Result<RunOutcome, Box<dyn std::error::Error>> {
        self.control.cancel("test done").await?;
        self.finish().await
    }
}

fn state_changes(fx: &Fixture) -> Vec<String> {
    fx.of_kind(EventKind::StateChanged)
        .iter()
        .map(|e| e.payload["to"].as_str().unwrap_or("?").to_owned())
        .collect()
}

// ---- start / drive ------------------------------------------------------------

#[tokio::test]
async fn start_persists_the_run_before_supervision_begins() -> TestResult {
    let dir = tempfile::tempdir()?;
    let repo = dir.path().join("repo");
    std::fs::create_dir_all(&repo)?;
    let store: Arc<dyn RunStore> = Arc::new(FileRunStore::new(dir.path().join("state")));
    let sink = Arc::new(VecSink::new());
    let rt = runtime_for(
        &dir,
        Arc::clone(&store),
        Arc::clone(&sink),
        base_config(),
        Arc::new(SimulatedModel::new()),
        Arc::new(SimulatedWorker::new()),
        EvidencePlan::default(),
    );

    let started = rt.start(job(&repo)).await?;
    // Nothing is supervising yet, but the run is already addressable.
    let state = store.load(started.run_id()).await?;
    assert_eq!(state.status, FactoryStatus::Queued);
    assert_eq!(sink.of_kind(EventKind::RunCreated).len(), 1);
    assert_eq!(sink.len(), 1, "no supervision event may precede drive()");

    let outcome = rt.drive(started, None).await?;
    assert_eq!(outcome.status, FactoryStatus::Finished);
    Ok(())
}

#[tokio::test]
async fn a_bad_repo_fails_before_any_run_state_exists() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store: Arc<dyn RunStore> = Arc::new(FileRunStore::new(dir.path().join("state")));
    let sink = Arc::new(VecSink::new());
    let rt = runtime_for(
        &dir,
        Arc::clone(&store),
        Arc::clone(&sink),
        base_config(),
        Arc::new(SimulatedModel::new()),
        Arc::new(SimulatedWorker::new()),
        EvidencePlan::default(),
    );
    let missing = Job {
        prompt: "x".into(),
        repo: PathBuf::from("/definitely/not/here"),
        source: JobSource::Cli,
    };
    let error = rt.start(missing).await.err().ok_or("must fail")?;
    assert!(matches!(error, RuntimeError::BadRepo(_)));
    assert!(sink.is_empty());
    assert!(store.list(&RunFilter::default()).await?.is_empty());
    Ok(())
}

// ---- cancel -----------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn cancel_terminates_the_worker_and_records_the_reason() -> TestResult {
    let mut fx = launch(Options::new(Mode::Working)).await?;
    fx.worker_running().await;

    fx.control.cancel("operator cancel").await?;
    // The reply is sent after the effect is persisted: the snapshot already agrees.
    let state = fx.store.load(fx.run_id).await?;
    assert_eq!(state.status, FactoryStatus::Cancelled);
    assert_eq!(state.workers[0].status, WorkerStatus::Stopped);
    assert!(state.workers[0].ended_at.is_some());

    let run_id = fx.run_id;
    let store = Arc::clone(&fx.store);
    let worker = Arc::clone(&fx.worker);
    let terminated = fx.of_kind(EventKind::WorkerTerminated);
    assert_eq!(terminated.len(), 1);
    assert_eq!(terminated[0].payload["cause"], "cancelled");
    assert_eq!(terminated[0].payload["reason"], "operator cancel");

    let sink = Arc::clone(&fx.sink);
    let outcome = fx.finish().await?;
    assert_eq!(outcome.status, FactoryStatus::Cancelled);
    assert_eq!(outcome.reason.as_deref(), Some("operator cancel"));
    assert_eq!(worker.terminations(), vec![TerminateReason::Cancelled]);

    let finished = sink.of_kind(EventKind::RunFinished);
    assert_eq!(finished.len(), 1);
    assert_eq!(finished[0].payload["status"], "cancelled");
    assert_eq!(finished[0].payload["reason"], "operator cancel");

    let log = store.read_log(run_id).await?;
    let state = store.load(run_id).await?;
    assert_eq!(state.last_seq, log.last_seq());
    assert_eq!(state.status, FactoryStatus::Cancelled);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cancel_works_while_waiting_for_a_human() -> TestResult {
    let mut fx = launch(Options::new(Mode::Working)).await?;
    fx.worker_running().await;
    until("a first assessment", || fx.model.calls() > 0).await;
    fx.model.set(Mode::Escalate);
    fx.needs_human(1).await?;
    tokio::time::sleep(Duration::from_secs(100)).await;

    fx.control.cancel("daemon shutdown").await?;
    // The wait interval is closed by the cancel and accounted.
    let state = fx.store.load(fx.run_id).await?;
    assert!(state.human_wait_ms >= 100_000, "{}", state.human_wait_ms);
    let worker = Arc::clone(&fx.worker);
    let outcome = fx.finish().await?;
    assert_eq!(outcome.status, FactoryStatus::Cancelled);
    assert_eq!(outcome.reason.as_deref(), Some("daemon shutdown"));
    assert_eq!(worker.terminations(), vec![TerminateReason::Cancelled]);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cancel_works_with_no_worker_in_needs_human() -> TestResult {
    let mut fx = launch(Options::new(Mode::Escalate)).await?;
    fx.needs_human(1).await?;
    assert_eq!(fx.worker.starts.load(Ordering::SeqCst), 0);
    let outcome = fx.cancel_and_join().await?;
    assert_eq!(outcome.status, FactoryStatus::Cancelled);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_cancel_only_channel_still_ends_a_needs_human_run() -> TestResult {
    // The embedded `--no-daemon` run has a control channel (so a signal can cancel it)
    // but nothing that could answer: NeedsHuman must end it, question recorded, worker
    // reaped, RUN_FINISHED written.
    let mut fx = launch(Options::new(Mode::Escalate).no_human()).await?;
    fx.needs_human(1).await?;
    let outcome = fx.finish().await?;
    assert_eq!(outcome.status, FactoryStatus::NeedsHuman);
    assert_eq!(fx.count(EventKind::RunFinished), 1);
    assert_eq!(
        fx.of_kind(EventKind::RunFinished)[0].payload["status"],
        "needs_human"
    );
    let state = fx.store.load(fx.run_id).await?;
    assert!(state.has_open_escalation(), "the question is recorded");
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_cancel_only_channel_still_cancels_before_the_escalation() -> TestResult {
    let mut fx = launch(Options::new(Mode::Working).no_human()).await?;
    fx.worker_running().await;
    let outcome = fx.cancel_and_join().await?;
    assert_eq!(outcome.status, FactoryStatus::Cancelled);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cancel_after_the_run_ended_is_a_clean_error() -> TestResult {
    let mut fx = launch(Options::new(Mode::Working)).await?;
    fx.worker_running().await;
    fx.control.cancel("first").await?;
    let control = fx.control.clone();
    fx.finish().await?;

    // The loop is gone: a second cancel must neither panic nor hang.
    assert_eq!(control.cancel("second").await, Err(ControlError::RunEnded));
    assert_eq!(control.steer("late").await, Err(ControlError::RunEnded));
    Ok(())
}

#[tokio::test]
async fn control_on_a_run_that_finished_by_itself_is_run_ended() -> TestResult {
    let dir = tempfile::tempdir()?;
    let repo = dir.path().join("repo");
    std::fs::create_dir_all(&repo)?;
    let rt = runtime_for(
        &dir,
        Arc::new(FileRunStore::new(dir.path().join("state"))),
        Arc::new(VecSink::new()),
        RunConfig {
            min_assessment_interval: Duration::from_millis(1),
            idle_poll_interval: Duration::from_millis(10),
            ..base_config()
        },
        Arc::new(SimulatedModel::new()),
        Arc::new(SimulatedWorker::new()),
        EvidencePlan::default(),
    );
    let started = rt.start(job(&repo)).await?;
    let (control, receiver) = RunControl::channel();
    let outcome = rt.drive(started, Some(receiver)).await?;
    assert_eq!(outcome.status, FactoryStatus::Finished);
    assert_eq!(control.cancel("late").await, Err(ControlError::RunEnded));
    Ok(())
}

// ---- steer ------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn a_human_steer_is_recorded_as_human_and_costs_the_supervisor_nothing() -> TestResult {
    let mut fx = launch(Options::new(Mode::Working).tweak(|c| {
        // Only output or a steer can start a cycle, and an unchanged observation is
        // skipped: so a following assessment is proof the steer asked for one.
        c.idle_poll_interval = Duration::from_secs(1_000_000);
        c.skip_unchanged_assessments = true;
    }))
    .await?;
    fx.worker_running().await;
    until("the first assessment", || fx.model.calls() > 0).await;
    let calls_before = fx.model.calls();

    let outcome = fx.control.steer("please also handle 429s").await?;
    assert_eq!(outcome, SteerOutcome::Queued);
    assert_eq!(fx.worker.steered(), vec!["please also handle 429s"]);

    let state = fx.store.load(fx.run_id).await?;
    let record = &state.workers[0];
    assert_eq!(record.steers.len(), 1);
    assert_eq!(record.steers[0].source, SteerSource::Human);
    assert!(record.last_steered_at.is_some(), "the grace window applies");
    assert_eq!(
        record.steer_count(),
        0,
        "the supervisor's own budget is untouched"
    );
    assert_eq!(record.human_steer_count(), 1);
    assert_eq!(state.counters.steers_sent, 0);

    let steered = fx.of_kind(EventKind::WorkerSteered);
    assert_eq!(steered.len(), 1);
    assert_eq!(steered[0].payload["source"], "human");
    assert_eq!(steered[0].payload["excerpt"], "please also handle 429s");

    // A human steer does not skip the assessment loop.
    until("an assessment after the steer", || {
        fx.model.calls() > calls_before
    })
    .await;
    fx.cancel_and_join().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_refused_steer_is_an_outcome_not_an_error() -> TestResult {
    let mut fx = launch(Options::new(Mode::Working).steer(SteerOutcome::Unsupported)).await?;
    fx.worker_running().await;

    let outcome = fx.control.steer("anything").await?;
    assert_eq!(outcome, SteerOutcome::Unsupported);

    let state = fx.store.load(fx.run_id).await?;
    assert_eq!(state.workers[0].steers.len(), 1);
    assert_eq!(
        state.workers[0].steers[0].outcome,
        SteerOutcome::Unsupported
    );
    assert!(
        state.workers[0].last_steered_at.is_none(),
        "a steer nothing accepted starts no grace window"
    );
    assert_eq!(state.counters.steers_sent, 0);
    fx.cancel_and_join().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn steering_with_no_active_worker_is_a_wrong_state_error() -> TestResult {
    let mut fx = launch(Options::new(Mode::Escalate)).await?;
    fx.needs_human(1).await?;

    let error = fx.control.steer("hello?").await.err().ok_or("must fail")?;
    assert!(matches!(error, ControlError::WrongState(_)), "{error:?}");
    assert!(error.to_string().contains("no active worker"));
    assert_eq!(fx.count(EventKind::WorkerSteered), 0);

    let empty = fx.control.steer("   ").await.err().ok_or("must fail")?;
    assert!(matches!(empty, ControlError::Invalid(_)), "{empty:?}");
    fx.cancel_and_join().await?;
    Ok(())
}

// ---- waiting for a human ------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn needs_human_is_a_quiet_wait_not_a_loop() -> TestResult {
    // Under paused time a spinning task stops virtual time, so a tokio timeout would
    // never fire: this watchdog counts real time on its own thread.
    let _watchdog = Watchdog::new(60);
    let mut fx = launch(Options::new(Mode::Working)).await?;
    fx.worker_running().await;
    until("a first assessment", || fx.model.calls() > 0).await;
    fx.model.set(Mode::Escalate);
    fx.needs_human(1).await?;

    let calls = fx.model.calls();
    let events = fx.sink.len();
    // Several hundred idle intervals. A busy loop would never let virtual time pass.
    tokio::time::sleep(Duration::from_secs(10 * 3600)).await;
    assert_eq!(fx.model.calls(), calls, "no model call while waiting");
    assert_eq!(fx.sink.len(), events, "nothing is emitted while waiting");
    assert_eq!(fx.count(EventKind::AssessmentSkippedUnchanged), 0);
    assert!(!fx.task.is_finished());
    fx.cancel_and_join().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_worker_that_completes_while_waiting_is_not_lost() -> TestResult {
    let mut fx = launch(Options::new(Mode::Working)).await?;
    fx.worker_running().await;
    until("a first assessment", || fx.model.calls() > 0).await;
    fx.model.set(Mode::Escalate);
    fx.needs_human(1).await?;
    let calls = fx.model.calls();

    fx.worker.complete();
    until("the completion to be recorded", || {
        fx.count(EventKind::WorkerCompleted) == 1
    })
    .await;
    let state = fx.store.load(fx.run_id).await?;
    assert_eq!(state.status, FactoryStatus::NeedsHuman);
    assert_eq!(state.workers[0].status, WorkerStatus::Completed);
    assert_eq!(fx.model.calls(), calls, "still no model call");

    tokio::time::sleep(Duration::from_secs(3600)).await;
    assert_eq!(fx.model.calls(), calls);
    fx.cancel_and_join().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn dropping_every_handle_while_waiting_ends_the_run_as_run_does() -> TestResult {
    let fx = launch(Options::new(Mode::Escalate)).await?;
    fx.needs_human(1).await?;
    let task = fx.task;
    drop(fx.control);
    let outcome = tokio::time::timeout(Duration::from_secs(3600), task).await???;
    assert_eq!(outcome.status, FactoryStatus::NeedsHuman);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn time_spent_waiting_for_a_human_does_not_count_against_the_overall_timeout() -> TestResult {
    let mut fx = launch(Options::new(Mode::Working).tweak(|c| {
        c.overall_timeout = Duration::from_secs(600);
    }))
    .await?;
    fx.worker_running().await;
    until("a first assessment", || fx.model.calls() > 0).await;
    fx.model.set(Mode::Escalate);
    let id = fx.needs_human(1).await?;

    // Two virtual hours with nobody home: twelve times the whole budget.
    tokio::time::sleep(Duration::from_secs(2 * 3600)).await;
    fx.model.set(Mode::Working);
    fx.answer(id, HumanDecision::Accept).await?;

    let calls = fx.model.calls();
    until("supervision to resume", || fx.model.calls() >= calls + 3).await;
    assert!(!fx.task.is_finished(), "the run must still be alive");
    let state = fx.store.load(fx.run_id).await?;
    assert!(!state.status.is_terminal(), "{:?}", state.status);
    assert!(
        state.human_wait_ms >= 2 * 3600 * 1000,
        "the wait is accounted: {}",
        state.human_wait_ms
    );
    for selected in fx.of_kind(EventKind::DirectiveSelected) {
        let reason = selected.payload["reason"].as_str().unwrap_or_default();
        assert!(
            !reason.contains("timeout"),
            "a human wait must not trip the timeout: {reason}"
        );
    }
    assert_eq!(fx.count(EventKind::WorkerTerminated), 0);
    fx.cancel_and_join().await?;
    Ok(())
}

// ---- answering ----------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn accept_resumes_supervision() -> TestResult {
    let mut fx = launch(Options::new(Mode::Working)).await?;
    fx.worker_running().await;
    until("a first assessment", || fx.model.calls() > 0).await;
    fx.model.set(Mode::Escalate);
    let id = fx.needs_human(1).await?;
    fx.model.set(Mode::Working);

    fx.control
        .answer(id, HumanDecision::Accept, "carry on", "tui")
        .await?;
    let answered = fx.of_kind(EventKind::EscalationAnswered);
    assert_eq!(answered.len(), 1);
    assert_eq!(answered[0].payload["surface"], "tui");
    assert_eq!(answered[0].payload["decision"]["decision"], "accept");

    let state = fx.store.load(fx.run_id).await?;
    let recorded = state.escalations[0]
        .answered
        .as_ref()
        .ok_or("the answer must be recorded")?;
    assert_eq!(recorded.surface, "tui");
    assert_eq!(recorded.note, "carry on");
    assert_eq!(recorded.decision, HumanDecision::Accept);

    let calls = fx.model.calls();
    until("assessment to resume", || fx.model.calls() > calls).await;
    let changes = state_changes(&fx);
    let parked = changes
        .iter()
        .rposition(|s| s == "needs_human")
        .ok_or("the run parked")?;
    assert!(
        changes[parked + 1..].iter().any(|s| s == "running"),
        "supervision resumed: {changes:?}"
    );
    fx.cancel_and_join().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn an_answer_is_bounded() -> TestResult {
    let mut fx = launch(Options::new(Mode::Escalate)).await?;
    let id = fx.needs_human(1).await?;
    fx.model.set(Mode::Working);
    let note = "n".repeat(50_000);
    fx.control
        .answer(
            id,
            HumanDecision::Accept,
            note,
            "a-very-long-surface-name-indeed-yes-it-is",
        )
        .await?;
    let state = fx.store.load(fx.run_id).await?;
    let recorded = state.escalations[0].answered.as_ref().ok_or("recorded")?;
    assert!(recorded.note.chars().count() <= 2_000);
    assert!(recorded.surface.chars().count() <= 32);
    fx.cancel_and_join().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn stop_ends_the_run_as_cancelled_and_nothing_restarts_it() -> TestResult {
    let mut fx = launch(Options::new(Mode::Working)).await?;
    fx.worker_running().await;
    until("a first assessment", || fx.model.calls() > 0).await;
    fx.model.set(Mode::Escalate);
    let id = fx.needs_human(1).await?;

    fx.answer(id, HumanDecision::Stop).await?;
    let state = fx.store.load(fx.run_id).await?;
    assert_eq!(state.status, FactoryStatus::Cancelled);

    let worker = Arc::clone(&fx.worker);
    let sink = Arc::clone(&fx.sink);
    let outcome = fx.finish().await?;
    assert_eq!(outcome.status, FactoryStatus::Cancelled);
    assert_eq!(outcome.reason.as_deref(), Some("stopped by operator"));
    assert_eq!(worker.terminations(), vec![TerminateReason::Cancelled]);
    assert_eq!(worker.starts.load(Ordering::SeqCst), 1, "no retry");

    // Never routed through `Stopped`, which `retry-stopped` would restart.
    let changes: Vec<String> = sink
        .of_kind(EventKind::StateChanged)
        .iter()
        .map(|e| e.payload["to"].as_str().unwrap_or("?").to_owned())
        .collect();
    assert!(!changes.iter().any(|s| s == "stopped"), "{changes:?}");
    assert_eq!(changes.last().map(String::as_str), Some("cancelled"));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn claiming_complete_never_finishes_the_run_by_itself() -> TestResult {
    // The model still says "not done" after the operator claims it is. Hard rule 1:
    // only policy, on evidence, may finish a run.
    let mut fx = launch(Options::new(Mode::Working)).await?;
    fx.worker_running().await;
    until("a first assessment", || fx.model.calls() > 0).await;
    fx.model.set(Mode::Escalate);
    let id = fx.needs_human(1).await?;
    fx.model.set(Mode::Working);

    fx.answer(id, HumanDecision::ClaimComplete).await?;
    let calls = fx.model.calls();
    until("re-assessment after the claim", || {
        fx.model.calls() >= calls + 3
    })
    .await;

    assert!(!fx.task.is_finished(), "the claim must not end the run");
    let state = fx.store.load(fx.run_id).await?;
    assert_ne!(state.status, FactoryStatus::Finished);
    for selected in fx.of_kind(EventKind::DirectiveSelected) {
        assert_ne!(selected.payload["directive"], "finish");
    }
    assert!(
        !state_changes(&fx).iter().any(|s| s == "finished"),
        "{:?}",
        state_changes(&fx)
    );
    fx.cancel_and_join().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn recheck_resumes_and_reassesses_immediately() -> TestResult {
    let mut fx = launch(Options::new(Mode::Working).tweak(|c| {
        c.idle_poll_interval = Duration::from_secs(1_000_000);
        c.skip_unchanged_assessments = true;
    }))
    .await?;
    fx.worker_running().await;
    until("a first assessment", || fx.model.calls() > 0).await;
    fx.model.set(Mode::Escalate);
    // With skipping on and no idle tick, only output starts a cycle.
    fx.worker.say("working\n");
    let id = fx.needs_human(1).await?;
    fx.model.set(Mode::Working);

    let calls = fx.model.calls();
    fx.answer(id, HumanDecision::Recheck).await?;
    until("an immediate re-assessment", || fx.model.calls() > calls).await;
    assert!(!fx.task.is_finished());
    fx.cancel_and_join().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn guidance_resumes_and_steers_the_active_worker() -> TestResult {
    let mut fx = launch(Options::new(Mode::Working)).await?;
    fx.worker_running().await;
    until("a first assessment", || fx.model.calls() > 0).await;
    fx.model.set(Mode::Escalate);
    let id = fx.needs_human(1).await?;
    fx.model.set(Mode::Working);

    fx.answer(
        id,
        HumanDecision::Guidance {
            text: "use the retry crate".into(),
        },
    )
    .await?;
    assert_eq!(fx.worker.steered(), vec!["use the retry crate"]);
    let state = fx.store.load(fx.run_id).await?;
    assert_eq!(state.workers[0].steers[0].source, SteerSource::Human);
    assert_eq!(state.workers[0].steer_count(), 0);
    assert_eq!(state.counters.steers_sent, 0);
    let steered = fx.of_kind(EventKind::WorkerSteered);
    assert_eq!(steered[0].payload["source"], "human");
    assert_ne!(state.status, FactoryStatus::NeedsHuman);
    fx.cancel_and_join().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn guidance_with_no_worker_is_recorded_not_an_error() -> TestResult {
    let mut fx = launch(Options::new(Mode::Escalate)).await?;
    let id = fx.needs_human(1).await?;
    fx.model.set(Mode::Working);

    fx.answer(
        id,
        HumanDecision::Guidance {
            text: "try again".into(),
        },
    )
    .await?;
    assert!(
        fx.of_kind(EventKind::Notification)
            .iter()
            .any(|e| e.payload["code"] == "guidance_not_delivered")
    );
    assert!(fx.worker.steered().is_empty());
    fx.cancel_and_join().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn unknown_and_already_answered_escalations_are_errors() -> TestResult {
    let mut fx = launch(Options::new(Mode::Escalate)).await?;
    let id = fx.needs_human(1).await?;

    let stranger = EscalationId::new();
    assert_eq!(
        fx.answer(stranger, HumanDecision::Accept).await,
        Err(ControlError::UnknownEscalation(stranger))
    );
    assert_eq!(fx.count(EventKind::EscalationAnswered), 0);

    fx.model.set(Mode::Working);
    fx.answer(id, HumanDecision::Accept).await?;
    assert_eq!(
        fx.answer(id, HumanDecision::Accept).await,
        Err(ControlError::UnknownEscalation(id)),
        "an answer is final"
    );
    assert_eq!(fx.count(EventKind::EscalationAnswered), 1);
    fx.cancel_and_join().await?;
    Ok(())
}

// ---- WorkerOutput -------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn worker_output_is_flushed_once_per_cycle_with_only_new_text() -> TestResult {
    let mut fx = launch(Options::new(Mode::Working).tweak(|c| {
        c.idle_poll_interval = Duration::from_secs(3600);
        c.skip_unchanged_assessments = true;
    }))
    .await?;
    fx.worker_running().await;
    until("a first assessment", || fx.model.calls() > 0).await;
    assert_eq!(
        fx.count(EventKind::WorkerOutput),
        0,
        "no output yet, so no event"
    );

    fx.worker.say("alpha\n");
    until("the first flush", || fx.count(EventKind::WorkerOutput) == 1).await;
    let first = &fx.of_kind(EventKind::WorkerOutput)[0];
    assert_eq!(first.payload["text"], "alpha\n");
    assert_eq!(first.payload["bytes"], 6);
    assert_eq!(first.payload["truncated"], false);
    assert_eq!(first.payload["exit"], false);

    // Many idle ticks, no new bytes: no empty event.
    tokio::time::sleep(Duration::from_secs(10 * 3600)).await;
    assert_eq!(fx.count(EventKind::WorkerOutput), 1);

    fx.worker.say("beta\n");
    until("the second flush", || {
        fx.count(EventKind::WorkerOutput) == 2
    })
    .await;
    let second = &fx.of_kind(EventKind::WorkerOutput)[1];
    assert_eq!(second.payload["text"], "beta\n", "only the new text");

    // A flood is bounded, and says so.
    fx.worker.say(&"x".repeat(20_000));
    until("the third flush", || fx.count(EventKind::WorkerOutput) == 3).await;
    let third = &fx.of_kind(EventKind::WorkerOutput)[2];
    assert_eq!(third.payload["bytes"], 20_000);
    assert_eq!(third.payload["truncated"], true);
    let text = third.payload["text"].as_str().ok_or("text")?;
    assert!(text.len() <= WORKER_OUTPUT_EVENT_CAP_BYTES);

    // One more on exit, carrying what was printed since the last cycle.
    fx.worker.say("goodbye\n");
    fx.worker.complete();
    until("the exit flush", || fx.count(EventKind::WorkerOutput) == 4).await;
    let last = &fx.of_kind(EventKind::WorkerOutput)[3];
    assert!(
        last.payload["text"]
            .as_str()
            .ok_or("text")?
            .ends_with("goodbye\n")
    );

    for event in fx.of_kind(EventKind::WorkerOutput) {
        assert!(
            !event.payload["text"].as_str().ok_or("text")?.is_empty(),
            "an empty WorkerOutput must never be emitted"
        );
    }

    // The observation carries the tail itself; the output events are not repeated
    // into its event history.
    for state in fx.model.states() {
        assert!(
            !state.to_string().contains("WORKER_OUTPUT"),
            "output events must stay out of the model-facing history"
        );
    }
    fx.cancel_and_join().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn worker_output_keeps_flowing_while_waiting_for_a_human() -> TestResult {
    let mut fx = launch(Options::new(Mode::Working)).await?;
    fx.worker_running().await;
    until("a first assessment", || fx.model.calls() > 0).await;
    fx.model.set(Mode::Escalate);
    fx.needs_human(1).await?;
    let calls = fx.model.calls();

    fx.worker.say("still going\n");
    until("a flush while waiting", || {
        fx.count(EventKind::WorkerOutput) == 1
    })
    .await;
    assert_eq!(
        fx.of_kind(EventKind::WorkerOutput)[0].payload["text"],
        "still going\n"
    );
    assert_eq!(fx.model.calls(), calls);
    fx.cancel_and_join().await?;
    Ok(())
}

// ---- real process evidence ------------------------------------------------------

#[tokio::test]
async fn recheck_and_claim_complete_rerun_the_command_providers() -> TestResult {
    let evidence = EvidencePlan::from_toml(
        "[[evidence.commands]]\nid = \"probe\"\nargv = [\"echo\", \"ok\"]\nrun_when = \"on_demand\"\n",
        "test",
    )?;
    let mut options = Options::new(Mode::Working).tweak(|c| {
        c.min_assessment_interval = Duration::from_millis(2);
        c.idle_poll_interval = Duration::from_millis(10);
    });
    options.evidence = evidence;
    let mut fx = launch(options).await?;
    fx.worker_running().await;
    until("a first assessment", || fx.model.calls() > 0).await;
    assert_eq!(fx.count(EventKind::EvidenceCollected), 0);

    fx.model.set(Mode::Escalate);
    let first = fx.needs_human(1).await?;
    fx.answer(first, HumanDecision::Recheck).await?;
    assert_eq!(
        fx.count(EventKind::EvidenceCollected),
        1,
        "recheck re-measures"
    );

    // The model asks again; the operator now claims completion.
    let second = fx.needs_human(2).await?;
    fx.model.set(Mode::Working);
    fx.answer(second, HumanDecision::ClaimComplete).await?;
    assert_eq!(fx.count(EventKind::EvidenceCollected), 2, "so does a claim");
    for event in fx.of_kind(EventKind::EvidenceCollected) {
        assert_eq!(event.payload["success"], true);
    }

    let calls = fx.model.calls();
    until("re-assessment", || fx.model.calls() >= calls + 2).await;
    assert!(!fx.task.is_finished(), "a claim never finishes a run");
    fx.cancel_and_join().await?;
    Ok(())
}

// ==== review round: reaping, flushing, ceilings, debounce, panics ===============

use aftman_core::testing::SimulatedModel as Sim;
use aftman_store::MemoryRunStore;
use aftman_workers::SteerProtocol;
use aftman_workers::subprocess::{CommandSpec, SubprocessWorker};

/// Aborts the whole test process if the test is still running after `secs` of real
/// time. A busy loop under paused time stops virtual time, so no tokio timeout can
/// catch it; a plain thread can.
struct Watchdog(Arc<std::sync::atomic::AtomicBool>);

impl Watchdog {
    fn new(secs: u64) -> Self {
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = Arc::clone(&done);
        std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(secs);
            while std::time::Instant::now() < deadline {
                if flag.load(Ordering::SeqCst) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            eprintln!("watchdog: the test is hung (a busy loop under paused time?)");
            std::process::exit(101);
        });
        Self(done)
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// Give a spawned control call time to reach the channel.
async fn let_it_queue() {
    tokio::time::sleep(Duration::from_millis(20)).await;
}

fn escalation_id(sink: &VecSink, nth: usize) -> Result<EscalationId, Box<dyn std::error::Error>> {
    let raised = sink.of_kind(EventKind::EscalationRaised);
    let id = raised
        .get(nth - 1)
        .and_then(|e| e.payload["escalation_id"].as_str())
        .ok_or("no such escalation")?;
    Ok(id.parse()?)
}

/// Whether any process on this machine has `token` in its command line.
fn process_with(token: &str) -> bool {
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return false;
    };
    entries.flatten().any(|entry| {
        std::fs::read(entry.path().join("cmdline"))
            .map(|bytes| {
                String::from_utf8_lossy(&bytes)
                    .replace('\0', " ")
                    .contains(token)
            })
            .unwrap_or(false)
    })
}

#[tokio::test(start_paused = true)]
async fn a_cancel_queued_during_the_model_call_is_applied_before_the_next_cycle() -> TestResult {
    let mut fx = launch(Options::new(Mode::Working).gated()).await?;
    let (entered, release) = fx.gate.clone().ok_or("gated")?;
    entered.notified().await;
    // The model call of the first cycle is in flight: the run is `assessing`.
    assert_eq!(
        state_changes(&fx).last().map(String::as_str),
        Some("assessing")
    );

    let control = fx.control.clone();
    let cancel = tokio::spawn(async move { control.cancel("queued mid-call").await });
    let_it_queue().await;
    assert!(
        !cancel.is_finished(),
        "a cycle in progress is not interrupted"
    );
    release.notify_one();
    cancel.await??;

    // The cycle finished (it started a worker), then the cancel was applied before
    // any second cycle, and that worker is gone.
    assert_eq!(fx.worker.alive(), 0, "no worker outlives a cancelled run");
    assert_eq!(fx.model.calls(), 2, "routing plus exactly one assessment");
    let state = fx.store.load(fx.run_id).await?;
    assert_eq!(state.status, FactoryStatus::Cancelled);
    assert_eq!(state.workers.len(), 1);
    assert_eq!(state.workers[0].status, WorkerStatus::Stopped);
    assert!(!fx.worker.terminations().is_empty());
    let outcome = fx.finish().await?;
    assert_eq!(outcome.reason.as_deref(), Some("queued mid-call"));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_steer_queued_during_the_model_call_is_applied_after_the_cycle() -> TestResult {
    let mut fx = launch(Options::new(Mode::Working)).await?;
    fx.worker_running().await;
    until("a first assessment", || fx.model.calls() > 1).await;
    let (entered, release) = fx.model.arm_gate();
    // The next cycle (an idle tick) blocks in the model.
    entered.notified().await;

    let control = fx.control.clone();
    let steer = tokio::spawn(async move { control.steer("mid-call guidance").await });
    let_it_queue().await;
    assert!(!steer.is_finished(), "the steer waits for the cycle to end");
    assert_eq!(fx.count(EventKind::WorkerSteered), 0);
    release.notify_one();
    assert_eq!(steer.await??, SteerOutcome::Queued);

    let completed = fx.of_kind(EventKind::AssessmentCompleted);
    let steered = fx.of_kind(EventKind::WorkerSteered);
    assert!(
        completed.iter().filter(|e| e.seq < steered[0].seq).count() >= 2,
        "the in-flight cycle completed before the steer was applied"
    );
    fx.cancel_and_join().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_queued_cancel_beats_nothing_but_a_completion_already_reported() -> TestResult {
    let mut fx = launch(Options::new(Mode::Working)).await?;
    fx.worker_running().await;
    until("a first assessment", || fx.model.calls() > 1).await;
    let (entered, release) = fx.model.arm_gate();
    entered.notified().await;

    // The worker finishes while the model is thinking, and a cancel is queued.
    fx.worker.complete();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let control = fx.control.clone();
    let cancel = tokio::spawn(async move { control.cancel("too late").await });
    let_it_queue().await;
    release.notify_one();
    cancel.await??;

    assert_eq!(
        fx.count(EventKind::WorkerCompleted),
        1,
        "recorded as completed"
    );
    assert_eq!(fx.count(EventKind::WorkerTerminated), 0);
    assert!(
        fx.worker.terminations().is_empty(),
        "nothing was terminated"
    );
    let outcome = fx.finish().await?;
    assert_eq!(outcome.status, FactoryStatus::Cancelled);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cancel_from_stopped_ends_the_run_cancelled() -> TestResult {
    let mut fx = launch(Options::new(Mode::Working)).await?;
    fx.worker_running().await;
    until("a first assessment", || fx.model.calls() > 1).await;
    fx.model.set(Mode::Stuck);
    until("the worker to be stopped", || {
        state_changes(&fx).last().map(String::as_str) == Some("stopped")
    })
    .await;

    fx.control.cancel("while stopped").await?;
    let outcome = fx.finish().await?;
    assert_eq!(outcome.status, FactoryStatus::Cancelled);
    assert_eq!(fx.worker.alive(), 0);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_simulated_worker_does_not_outlive_a_queued_cancel() -> TestResult {
    let dir = tempfile::tempdir()?;
    let repo = dir.path().join("repo");
    std::fs::create_dir_all(&repo)?;
    let store: Arc<dyn RunStore> = Arc::new(MemoryRunStore::new());
    let sink = Arc::new(VecSink::new());
    let model = Arc::new(DialModel::new(Mode::Working));
    let (entered, release) = model.arm_gate();
    let rt = runtime_for(
        &dir,
        Arc::clone(&store),
        Arc::clone(&sink),
        base_config(),
        Arc::clone(&model) as Arc<dyn DecisionModel>,
        Arc::new(SimulatedWorker::new()),
        EvidencePlan::default(),
    );
    let started = rt.start(job(&repo)).await?;
    let run_id = started.run_id();
    let (control, receiver) = RunControl::channel();
    let task = tokio::spawn({
        let rt = Arc::clone(&rt);
        async move { rt.drive(started, Some(receiver)).await }
    });
    entered.notified().await;
    let handle = tokio::spawn({
        let control = control.clone();
        async move { control.cancel("queued").await }
    });
    let_it_queue().await;
    release.notify_one();
    handle.await??;

    let outcome = task.await??;
    assert_eq!(outcome.status, FactoryStatus::Cancelled);
    let state = store.load(run_id).await?;
    assert_eq!(state.workers[0].status, WorkerStatus::Stopped);
    let before = state.workers[0].output.total_bytes();
    tokio::time::sleep(Duration::from_secs(5)).await;
    let after = store.load(run_id).await?.workers[0].output.total_bytes();
    assert_eq!(before, after, "the worker wrote nothing after the cancel");
    let events = sink.events();
    assert_eq!(
        events.last().map(|e| e.kind),
        Some(EventKind::RunFinished),
        "nothing is emitted after the run is over"
    );
    Ok(())
}

/// The reviewer's scenario with a real subprocess: a cancel queued during the model
/// call of the cycle that starts the worker. The terminate it triggers used to be a
/// no-op (the process was not launched yet), and the worker then ran to completion in
/// the repository of a run already reported `Cancelled`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancel_queued_before_launch_never_lets_a_real_process_run() -> TestResult {
    for round in 0..5 {
        let dir = tempfile::tempdir()?;
        let repo = dir.path().join("repo");
        std::fs::create_dir_all(&repo)?;
        let marker = repo.join("worker-ran");
        let token = format!("aftman-reap-{}-{round}", std::process::id());
        let script = format!("sleep 0.4; touch {} # {token}", marker.display());
        let spec = CommandSpec {
            program: "sh".into(),
            args: vec!["-c".into(), script],
            env: Vec::new(),
            steer: SteerProtocol::None,
            working_dir_is_repo: true,
        };
        let worker = Arc::new(
            SubprocessWorker::new(WorkerKind::Simulated, spec)
                .with_terminate_grace(Duration::from_millis(300)),
        );
        let store: Arc<dyn RunStore> = Arc::new(MemoryRunStore::new());
        let sink = Arc::new(VecSink::new());
        let model = Arc::new(DialModel::new(Mode::Working));
        let (entered, release) = model.arm_gate();
        let rt = runtime_for(
            &dir,
            Arc::clone(&store),
            Arc::clone(&sink),
            base_config(),
            Arc::clone(&model) as Arc<dyn DecisionModel>,
            worker,
            EvidencePlan::default(),
        );
        let started = rt.start(job(&repo)).await?;
        let (control, receiver) = RunControl::channel();
        let task = tokio::spawn({
            let rt = Arc::clone(&rt);
            async move { rt.drive(started, Some(receiver)).await }
        });
        entered.notified().await;
        let cancel = tokio::spawn({
            let control = control.clone();
            async move { control.cancel("queued").await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        release.notify_one();
        cancel.await??;

        // The reply is the guarantee: from here nothing of this run is alive, and
        // nothing can come alive.
        assert!(!process_with(&token), "round {round}: the worker is alive");
        let outcome = task.await??;
        assert_eq!(outcome.status, FactoryStatus::Cancelled);
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert!(
            !marker.exists(),
            "round {round}: the worker ran after the cancel"
        );
        assert!(
            !process_with(&token),
            "round {round}: the worker came alive"
        );
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn dropping_every_handle_while_parked_stops_the_worker_and_closes_the_wait() -> TestResult {
    let fx = launch(Options::new(Mode::Working)).await?;
    fx.worker_running().await;
    until("a first assessment", || fx.model.calls() > 1).await;
    fx.model.set(Mode::Escalate);
    fx.needs_human(1).await?;
    tokio::time::sleep(Duration::from_secs(100)).await;

    let task = fx.task;
    drop(fx.control);
    let outcome = tokio::time::timeout(Duration::from_secs(3600), task).await???;
    assert_eq!(outcome.status, FactoryStatus::NeedsHuman);
    assert_eq!(fx.worker.alive(), 0, "the run ended, so its worker did");
    assert!(fx.worker.terminations().contains(&TerminateReason::Stopped));
    let state = fx.store.load(fx.run_id).await?;
    assert_eq!(state.workers[0].status, WorkerStatus::Stopped);
    assert!(state.human_wait_ms >= 100_000, "{}", state.human_wait_ms);
    let terminated = fx.sink.of_kind(EventKind::WorkerTerminated);
    assert_eq!(terminated[0].payload["cause"], "run_ended");
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn dropping_every_handle_while_running_keeps_supervising() -> TestResult {
    let fx = launch(Options::new(Mode::Working)).await?;
    fx.worker_running().await;
    until("a first assessment", || fx.model.calls() > 1).await;
    drop(fx.control);
    let calls = fx.model.calls();
    tokio::time::sleep(Duration::from_secs(300)).await;
    assert!(!fx.task.is_finished(), "an unattended run carries on");
    assert!(fx.model.calls() > calls, "and is still supervised");
    fx.task.abort();
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn the_last_output_is_flushed_when_a_cancel_ends_the_worker() -> TestResult {
    let mut fx = launch(Options::new(Mode::Working)).await?;
    fx.worker_running().await;
    until("a first assessment", || fx.model.calls() > 1).await;
    fx.worker.say("last words\n");
    fx.control.cancel("bye").await?;

    let outputs = fx.of_kind(EventKind::WorkerOutput);
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].payload["text"], "last words\n");
    assert_eq!(outputs[0].payload["exit"], true);
    let terminated = fx.of_kind(EventKind::WorkerTerminated);
    assert!(outputs[0].seq < terminated[0].seq, "flushed before the end");
    let state = fx.store.load(fx.run_id).await?;
    assert!(state.workers[0].output.tail(100).ends_with("last words\n"));
    fx.finish().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn the_last_output_is_flushed_when_policy_stops_the_worker() -> TestResult {
    let mut fx = launch(Options::new(Mode::Working).tweak(|c| {
        c.overall_timeout = Duration::from_secs(100);
    }))
    .await?;
    fx.worker_running().await;
    until("a first assessment", || fx.model.calls() > 1).await;
    // Arm the gate while the run is young; the tick that blocks on it is after the
    // timeout, so its cycle ends in `Stop`.
    tokio::time::sleep(Duration::from_secs(105)).await;
    let (entered, release) = fx.model.arm_gate();
    entered.notified().await;
    fx.worker.say("typed during the model call\n");
    release.notify_one();

    let outcome = fx.finish().await?;
    assert_eq!(outcome.status, FactoryStatus::Failed, "timeout ends it");
    assert_eq!(fx.worker.alive(), 0);
    let terminated = fx.of_kind(EventKind::WorkerTerminated);
    assert_eq!(terminated[0].payload["cause"], "stopped");
    assert!(
        terminated[0].payload["reason"]
            .as_str()
            .is_some_and(|r| r.contains("timeout"))
    );
    assert!(terminated[0].payload["worker_id"].is_string());
    let outputs = fx.of_kind(EventKind::WorkerOutput);
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].payload["text"], "typed during the model call\n");
    assert_eq!(outputs[0].payload["exit"], true);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn multi_byte_output_is_cut_on_a_character_boundary() -> TestResult {
    let mut fx = launch(Options::new(Mode::Working)).await?;
    fx.worker_running().await;
    until("a first assessment", || fx.model.calls() > 1).await;
    // 10 000 bytes of two-byte characters: the 8192-byte cut must not split one.
    fx.worker.say(&"é".repeat(5_000));
    until("a flush", || fx.count(EventKind::WorkerOutput) == 1).await;
    let event = &fx.of_kind(EventKind::WorkerOutput)[0];
    let text = event.payload["text"].as_str().ok_or("text")?;
    assert!(text.len() <= WORKER_OUTPUT_EVENT_CAP_BYTES);
    assert!(text.chars().all(|c| c == 'é'), "no character was split");
    assert_eq!(event.payload["truncated"], true);
    assert_eq!(event.payload["bytes"], 10_000);
    fx.cancel_and_join().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn parked_output_is_flushed_on_the_idle_interval_not_the_debounce() -> TestResult {
    let mut fx = launch(Options::new(Mode::Working)).await?;
    fx.worker_running().await;
    until("a first assessment", || fx.model.calls() > 1).await;
    fx.model.set(Mode::Escalate);
    fx.needs_human(1).await?;
    let before = fx.count(EventKind::WorkerOutput);
    // A chatty worker for 600 virtual seconds with nobody deciding anything.
    for _ in 0..600 {
        fx.worker.say("chatter\n");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let flushes = fx.count(EventKind::WorkerOutput) - before;
    assert!(
        flushes <= 600 / 30 + 1,
        "{flushes} output events while parked: the log must stay proportional to decisions"
    );
    assert!(flushes >= 1);
    fx.cancel_and_join().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_burst_of_human_steers_costs_one_assessment() -> TestResult {
    let mut fx = launch(Options::new(Mode::Working).tweak(|c| {
        c.min_assessment_interval = Duration::from_secs(60);
        c.idle_poll_interval = Duration::from_secs(1_000_000);
        c.skip_unchanged_assessments = true;
    }))
    .await?;
    fx.worker_running().await;
    until("a first assessment", || fx.model.calls() > 1).await;
    let calls = fx.model.calls();
    for n in 0..10 {
        fx.control.steer(format!("nudge {n}")).await?;
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(
        fx.model.calls(),
        calls,
        "the debounce window holds while steers arrive"
    );
    tokio::time::sleep(Duration::from_secs(120)).await;
    assert_eq!(fx.model.calls(), calls + 1, "one assessment for all ten");
    fx.cancel_and_join().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn max_assessments_is_a_hard_ceiling() -> TestResult {
    let mut fx = launch(Options::new(Mode::Escalate).tweak(|c| {
        c.max_assessments = 2;
    }))
    .await?;
    for nth in 1..=5 {
        let id = fx.needs_human(nth).await?;
        fx.answer(id, HumanDecision::Accept).await?;
    }
    fx.needs_human(6).await?;
    let state = fx.store.load(fx.run_id).await?;
    assert_eq!(state.counters.assessments, 2);
    assert_eq!(
        fx.model.calls(),
        2 + 1,
        "two assessments plus the routing call, however many answers resumed it"
    );
    assert!(fx.count(EventKind::BudgetExceeded) >= 1);
    let last = fx
        .of_kind(EventKind::EscalationRaised)
        .pop()
        .ok_or("raised")?;
    let ask = last.payload["ask"].as_str().ok_or("ask")?;
    assert!(
        ask.contains("assessment budget (2 model calls) is spent"),
        "{ask}"
    );
    fx.cancel_and_join().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_worker_that_panics_is_recorded_as_failed() -> TestResult {
    let mut options = Options::new(Mode::Working);
    options.panic_on_start = true;
    let mut fx = launch(options).await?;
    until("the failure to be noticed", || {
        fx.count(EventKind::WorkerCompleted) >= 1
    })
    .await;
    let completed = &fx.of_kind(EventKind::WorkerCompleted)[0];
    assert_eq!(completed.payload["success"], false);
    let state = fx.store.load(fx.run_id).await?;
    assert_eq!(state.workers[0].status, WorkerStatus::Failed);
    fx.cancel_and_join().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_worker_that_panics_while_parked_is_recorded_too() -> TestResult {
    let mut options = Options::new(Mode::Working);
    options.panic_on_finish = true;
    let mut fx = launch(options).await?;
    fx.worker_running().await;
    until("a first assessment", || fx.model.calls() > 1).await;
    fx.model.set(Mode::Escalate);
    fx.needs_human(1).await?;
    let calls = fx.model.calls();

    fx.worker.complete();
    until("the loss to be recorded", || {
        fx.count(EventKind::WorkerCompleted) == 1
    })
    .await;
    assert_eq!(
        fx.of_kind(EventKind::WorkerCompleted)[0].payload["success"],
        false
    );
    assert_eq!(fx.model.calls(), calls);
    assert!(!fx.task.is_finished());
    fx.cancel_and_join().await?;
    Ok(())
}

/// A clock that can be pushed forward from outside, on top of tokio's.
struct JumpClock {
    inner: TokioClock,
    jump_secs: Arc<AtomicUsize>,
}

impl Clock for JumpClock {
    fn now(&self) -> OffsetDateTime {
        self.inner.now() + Duration::from_secs(self.jump_secs.load(Ordering::SeqCst) as u64)
    }

    fn instant(&self) -> tokio::time::Instant {
        self.inner.instant()
    }
}

/// Moves the clock an hour the moment the run reports `finished`.
struct JumpOnFinish {
    jump_secs: Arc<AtomicUsize>,
}

impl aftman_daemon::EventSink for JumpOnFinish {
    fn emit(&self, event: &EventEnvelope) {
        if event.kind == EventKind::StateChanged && event.payload["to"] == "finished" {
            self.jump_secs.store(3_600, Ordering::SeqCst);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_finish_that_lands_after_the_timeout_is_not_failed() -> TestResult {
    // The run is `Finished` when the post-cycle timeout check looks at a clock that has
    // since passed `overall_timeout`; `Finished -> Failed` is not a transition, and
    // used to kill the run task with an error.
    let dir = tempfile::tempdir()?;
    let repo = dir.path().join("repo");
    std::fs::create_dir_all(&repo)?;
    let jump = Arc::new(AtomicUsize::new(0));
    let rt = FactoryRuntime::new(RuntimeDeps {
        config: RunConfig {
            overall_timeout: Duration::from_secs(600),
            min_assessment_interval: Duration::from_millis(10),
            idle_poll_interval: Duration::from_secs(1),
            ..base_config()
        },
        store: Arc::new(MemoryRunStore::new()),
        model: Arc::new(SimulatedModel::new()),
        worker: Arc::new(SimulatedWorker::new()),
        registry: ResponsibilityRegistry::builtin(),
        definitions: aftman_core::definitions::DefinitionSet::bundled().unwrap_or_default(),
        evidence: EvidencePlan::default(),
        evidence_temp_dir: dir.path().join("evidence-tmp"),
        clock: Arc::new(JumpClock {
            inner: TokioClock::new(),
            jump_secs: Arc::clone(&jump),
        }),
        sink: Arc::new(JumpOnFinish { jump_secs: jump }),
        ensemble_enabled: false,
        uncertainty_margin: 0.15,
        steering_grace: Duration::from_secs(30),
        launch: Default::default(),

        forwarded_env: Default::default(),
    });
    let outcome = rt.run(job(&repo)).await?;
    assert_eq!(outcome.status, FactoryStatus::Finished);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn the_timeout_check_ignores_human_wait_when_deciding_whether_to_skip() -> TestResult {
    // With the skip on, a cycle whose observation is unchanged is not asked. Whether a
    // timeout is "about to trip" must use supervised time: measured on the wall it
    // would be true forever after a long human wait, and nothing would ever be skipped.
    let mut fx = launch(Options::new(Mode::Working).tweak(|c| {
        c.overall_timeout = Duration::from_secs(600);
        c.skip_unchanged_assessments = true;
    }))
    .await?;
    fx.worker_running().await;
    until("a first assessment", || fx.model.calls() > 1).await;
    fx.model.set(Mode::Escalate);
    fx.control.steer("nudge").await?; // changes the digest, so the model is asked
    let id = fx.needs_human(1).await?;
    tokio::time::sleep(Duration::from_secs(2 * 3600)).await;
    fx.model.set(Mode::Working);
    fx.answer(id, HumanDecision::Accept).await?;

    tokio::time::sleep(Duration::from_secs(300)).await;
    assert!(
        fx.count(EventKind::AssessmentSkippedUnchanged) >= 3,
        "idle ticks with nothing new must be skipped, got {}",
        fx.count(EventKind::AssessmentSkippedUnchanged)
    );
    fx.cancel_and_join().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn guidance_longer_than_a_steer_may_be_is_rejected_not_truncated() -> TestResult {
    let mut fx = launch(Options::new(Mode::Escalate)).await?;
    let id = fx.needs_human(1).await?;
    let error = fx
        .answer(
            id,
            HumanDecision::Guidance {
                text: "g".repeat(8_001),
            },
        )
        .await
        .err()
        .ok_or("must be rejected")?;
    assert!(matches!(error, ControlError::Invalid(_)), "{error:?}");
    // Still waiting, still open, nothing recorded.
    assert_eq!(fx.count(EventKind::EscalationAnswered), 0);
    let state = fx.store.load(fx.run_id).await?;
    assert!(state.escalations[0].answered.is_none());
    fx.model.set(Mode::Working);
    fx.answer(
        id,
        HumanDecision::Guidance {
            text: "g".repeat(8_000),
        },
    )
    .await?;
    fx.cancel_and_join().await?;
    Ok(())
}

// ---- real-process evidence, continued -------------------------------------------

fn echo_provider(when: &str) -> Result<EvidencePlan, Box<dyn std::error::Error>> {
    Ok(EvidencePlan::from_toml(
        &format!(
            "[[evidence.commands]]\nid = \"probe\"\nargv = [\"echo\", \"ok\"]\nrun_when = \"{when}\"\n"
        ),
        "test",
    )?)
}

#[tokio::test]
async fn a_worker_that_finished_while_parked_gets_its_evidence_on_resume() -> TestResult {
    let mut options = Options::new(Mode::Working).tweak(|c| {
        c.min_assessment_interval = Duration::from_millis(2);
        c.idle_poll_interval = Duration::from_millis(10);
    });
    options.evidence = echo_provider("worker_completed")?;
    let mut fx = launch(options).await?;
    fx.worker_running().await;
    until("a first assessment", || fx.model.calls() > 1).await;
    fx.model.set(Mode::Escalate);
    let id = fx.needs_human(1).await?;

    fx.worker.complete();
    until("the completion", || {
        fx.count(EventKind::WorkerCompleted) == 1
    })
    .await;
    assert_eq!(
        fx.count(EventKind::EvidenceCollected),
        0,
        "no providers run while parked"
    );
    fx.model.set(Mode::Working);
    fx.answer(id, HumanDecision::Accept).await?;
    assert_eq!(
        fx.count(EventKind::EvidenceCollected),
        1,
        "its WorkerCompleted evidence is collected on resume"
    );
    fx.cancel_and_join().await?;
    Ok(())
}

/// Simulated model until the first `before_finish` evidence exists, then it asks
/// for a human, until the test lets it go back.
struct HybridModel {
    sim: Sim,
    dial: DialModel,
    sink: Arc<VecSink>,
    armed: std::sync::atomic::AtomicBool,
    escalating: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl DecisionModel for HybridModel {
    async fn ask(
        &self,
        context: &AskContext,
        state: &Value,
        questions: &BTreeMap<CheckKey, Question>,
        budget: &Budget,
    ) -> Result<ModelAnswers, ModelError> {
        if !self.sink.of_kind(EventKind::EvidenceCollected).is_empty()
            && self.armed.swap(false, Ordering::SeqCst)
        {
            self.escalating.store(true, Ordering::SeqCst);
        }
        if self.escalating.load(Ordering::SeqCst) {
            self.dial.ask(context, state, questions, budget).await
        } else {
            self.sim.ask(context, state, questions, budget).await
        }
    }
}

#[tokio::test]
async fn a_recheck_re_arms_the_before_finish_gate() -> TestResult {
    let dir = tempfile::tempdir()?;
    let repo = dir.path().join("repo");
    std::fs::create_dir_all(&repo)?;
    let sink = Arc::new(VecSink::new());
    let model = Arc::new(HybridModel {
        sim: Sim::new(),
        dial: DialModel::new(Mode::Escalate),
        sink: Arc::clone(&sink),
        armed: std::sync::atomic::AtomicBool::new(true),
        escalating: std::sync::atomic::AtomicBool::new(false),
    });
    let rt = runtime_for(
        &dir,
        Arc::new(MemoryRunStore::new()),
        Arc::clone(&sink),
        RunConfig {
            min_assessment_interval: Duration::from_millis(2),
            idle_poll_interval: Duration::from_millis(10),
            ..base_config()
        },
        Arc::clone(&model) as Arc<dyn DecisionModel>,
        Arc::new(SimulatedWorker::new()),
        echo_provider("before_finish")?,
    );
    let started = rt.start(job(&repo)).await?;
    let (control, receiver) = RunControl::channel();
    let task = tokio::spawn({
        let rt = Arc::clone(&rt);
        async move { rt.drive(started, Some(receiver)).await }
    });

    // The first Finish ran the gate (one measurement), then the model asked for a human.
    until("the escalation", || {
        sink.of_kind(EventKind::EscalationRaised).len() == 1
            && sink
                .of_kind(EventKind::StateChanged)
                .last()
                .is_some_and(|e| e.payload["to"] == "needs_human")
    })
    .await;
    assert_eq!(sink.of_kind(EventKind::EvidenceCollected).len(), 1);

    model.escalating.store(false, Ordering::SeqCst);
    control
        .answer(escalation_id(&sink, 1)?, HumanDecision::Recheck, "", "test")
        .await?;
    let outcome = tokio::time::timeout(Duration::from_secs(30), task).await???;
    assert_eq!(outcome.status, FactoryStatus::Finished);
    assert_eq!(
        sink.of_kind(EventKind::EvidenceCollected).len(),
        2,
        "the recheck re-armed the gate, so Finish measured again before it took effect"
    );
    Ok(())
}

// ---- time-driven checks beyond the overall timeout ---------------------------------

#[tokio::test(start_paused = true)]
async fn a_worker_timeout_about_to_trip_forces_a_cycle_even_when_nothing_changed() -> TestResult {
    let mut fx = launch(Options::new(Mode::Working).tweak(|c| {
        c.skip_unchanged_assessments = true;
        c.idle_poll_interval = Duration::from_secs(5);
        c.worker_timeout = Duration::from_secs(60);
    }))
    .await?;
    fx.worker_running().await;
    tokio::time::sleep(Duration::from_secs(40)).await;
    let before = fx.count(EventKind::AssessmentRequested);
    assert!(before >= 1);
    tokio::time::sleep(Duration::from_secs(10)).await; // 50 s: 10 s left, more than a tick
    assert_eq!(
        fx.count(EventKind::AssessmentRequested),
        before,
        "nothing changed and no boundary is near, so the cycle is skipped"
    );
    tokio::time::sleep(Duration::from_secs(12)).await; // 62 s: crossed the 55 s mark
    assert!(
        fx.count(EventKind::AssessmentRequested) > before,
        "within one tick of the worker timeout, an unchanged observation is still assessed"
    );
    fx.cancel_and_join().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn the_end_of_the_steering_grace_window_forces_a_cycle() -> TestResult {
    let mut fx = launch(Options::new(Mode::Working).tweak(|c| {
        c.skip_unchanged_assessments = true;
        c.idle_poll_interval = Duration::from_secs(5);
    }))
    .await?;
    fx.worker_running().await;
    fx.control.steer("try the other approach").await?;
    tokio::time::sleep(Duration::from_secs(12)).await;
    let before = fx.count(EventKind::AssessmentRequested);
    tokio::time::sleep(Duration::from_secs(8)).await; // 20 s into a 30 s window
    assert_eq!(
        fx.count(EventKind::AssessmentRequested),
        before,
        "inside the window, far from its end: skipped"
    );
    tokio::time::sleep(Duration::from_secs(14)).await; // 34 s: the window ended at 30 s
    assert!(
        fx.count(EventKind::AssessmentRequested) > before,
        "the suppressed checks come back when the window closes, so a cycle must run"
    );
    fx.cancel_and_join().await?;
    Ok(())
}
