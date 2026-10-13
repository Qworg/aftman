//! Shared doubles and a raw-NDJSON client for the daemon tests.
//!
//! Everything is offline: no network, no real agent, no environment variables. The
//! client speaks the wire format directly over `tokio::net::UnixStream` (docs/07
//! section 2: the protocol must be testable without a client library), and every
//! await on the socket goes through a timeout so a regression fails rather than
//! hangs.
#![allow(dead_code)]

use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;

use aftman_core::assessment::AnswerValue;
use aftman_core::clock::SystemClock;
use aftman_core::error::ModelError;
use aftman_core::event::{EventEnvelope, EventKind};
use aftman_core::evidence::EvidencePlan;
use aftman_core::ids::{CheckKey, EventSeq, RunId};
use aftman_core::job::{Job, JobSource, RunConfig, WorkerKind};
use aftman_core::log::Stream;
use aftman_core::model::{
    AskContext, Budget, DecisionModel, ModelAnswers, Purpose, Question, Usage,
};
use aftman_core::responsibility::ResponsibilityRegistry;
use aftman_core::run::{RunFilter, RunHeader, RunState, RunSummary};
use aftman_core::testing::SimulatedModel;
use aftman_core::worker::{
    SteerOutcome, TerminateReason, Worker, WorkerCtx, WorkerError, WorkerEventSink, WorkerOutcome,
};
use aftman_daemon::protocol::{
    ErrorCode, JevUsageParams, ProtocolError, ResponsibilityInfo, ResponsibilityListResult,
    RunStartParams,
};
use aftman_daemon::{
    BuiltRun, Daemon, DaemonConfig, DaemonPaths, FactoryRuntime, RecoveryReport, RunManager,
    RuntimeContext, RuntimeDeps, RuntimeFactory,
};
use aftman_store::{EventLogRead, FileRunStore, RunClaim, RunStore, StoreError};
use aftman_workers::SimulatedWorker;

pub type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Longest any single wait in these tests may take.
pub const PATIENCE: Duration = Duration::from_secs(20);

/// A temp directory with a path short enough for a Unix socket, whatever `TMPDIR` is.
pub fn short_tempdir() -> Result<tempfile::TempDir, std::io::Error> {
    tempfile::Builder::new().prefix("aftd").tempdir_in("/tmp")
}

/// Poll `condition` until it holds. Event-driven in spirit: the condition is about
/// what the system did, and the deadline only turns a hang into a failure.
pub async fn wait_until<F, Fut>(what: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + PATIENCE;
    loop {
        if condition().await {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// ---------------------------------------------------------------------------
// Doubles
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Mode {
    Working,
    Escalate,
}

pub type Gate = (Arc<Notify>, Arc<Notify>);

/// A model whose answers a test can change while the run is live.
pub struct DialModel {
    mode: Mutex<Mode>,
    gate: Mutex<Option<Gate>>,
    pub calls: AtomicUsize,
}

impl DialModel {
    pub fn new(mode: Mode) -> Self {
        Self {
            mode: Mutex::new(mode),
            gate: Mutex::new(None),
            calls: AtomicUsize::new(0),
        }
    }

    pub fn set(&self, mode: Mode) {
        *self.mode.lock().unwrap_or_else(|e| e.into_inner()) = mode;
    }

    /// Make the next assessment call block until the returned `release` fires.
    pub fn arm_gate(&self) -> Gate {
        let gate: Gate = (Arc::new(Notify::new()), Arc::new(Notify::new()));
        *self.gate.lock().unwrap_or_else(|e| e.into_inner()) = Some(gate.clone());
        gate
    }
}

#[async_trait]
impl DecisionModel for DialModel {
    async fn ask(
        &self,
        context: &AskContext,
        _state: &Value,
        questions: &BTreeMap<CheckKey, Question>,
        _budget: &Budget,
    ) -> Result<ModelAnswers, ModelError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
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
        let table: [(&str, f32); 10] = [
            ("core.completion__ready_to_finish", 0.10),
            ("core.completion__implementation_complete", 0.10),
            ("core.completion__requirements_satisfied", 0.10),
            ("core.verification__tests_sufficient", 0.10),
            ("core.verification__needs_verification", 0.10),
            ("core.worker-health__worker_stuck", 0.02),
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

/// A model that panics on the first *assessment* after a worker has started
/// (routing and earlier assessments work), so the panic happens with a worker up.
pub struct PanicLateModel(pub Arc<DialModel>, pub Arc<ScriptWorker>);

#[async_trait]
impl DecisionModel for PanicLateModel {
    async fn ask(
        &self,
        context: &AskContext,
        state: &Value,
        questions: &BTreeMap<CheckKey, Question>,
        budget: &Budget,
    ) -> Result<ModelAnswers, ModelError> {
        assert!(
            context.purpose != Purpose::Assessment || self.1.starts.load(Ordering::SeqCst) == 0,
            "the model panicked mid-run (intentionally, in a test)"
        );
        self.0.ask(context, state, questions, budget).await
    }
}

/// A model that panics, to prove a panicking run is not left non-terminal.
pub struct PanicModel;

#[async_trait]
impl DecisionModel for PanicModel {
    async fn ask(
        &self,
        _context: &AskContext,
        _state: &Value,
        _questions: &BTreeMap<CheckKey, Question>,
        _budget: &Budget,
    ) -> Result<ModelAnswers, ModelError> {
        panic!("the model panicked (intentionally, in a test)");
    }
}

/// A model that blocks its *thread* (not the task) inside an assessment, ignoring
/// cancellation entirely, until released: a run task that cannot be aborted.
pub struct BlockingModel {
    pub dial: Arc<DialModel>,
    pub entered: Arc<std::sync::atomic::AtomicBool>,
    pub release: Arc<std::sync::atomic::AtomicBool>,
}

#[async_trait]
impl DecisionModel for BlockingModel {
    async fn ask(
        &self,
        context: &AskContext,
        state: &Value,
        questions: &BTreeMap<CheckKey, Question>,
        budget: &Budget,
    ) -> Result<ModelAnswers, ModelError> {
        if context.purpose == Purpose::Assessment {
            self.entered.store(true, Ordering::SeqCst);
            while !self.release.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        self.dial.ask(context, state, questions, budget).await
    }
}

/// A worker that runs until the test says otherwise, and can be made to talk.
pub struct ScriptWorker {
    steer_outcome: SteerOutcome,
    steered: Mutex<Vec<String>>,
    terminated: Mutex<Vec<TerminateReason>>,
    finish: Notify,
    kill: Notify,
    live: Mutex<Option<(Arc<Mutex<aftman_core::log::BoundedLog>>, WorkerEventSink)>>,
    pub starts: AtomicUsize,
    active: AtomicUsize,
}

struct ActiveGuard<'a>(&'a AtomicUsize);

impl Drop for ActiveGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl ScriptWorker {
    pub fn new(steer_outcome: SteerOutcome) -> Self {
        Self {
            steer_outcome,
            steered: Mutex::new(Vec::new()),
            terminated: Mutex::new(Vec::new()),
            finish: Notify::new(),
            kill: Notify::new(),
            live: Mutex::new(None),
            starts: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
        }
    }

    /// Workers currently inside `run`.
    pub fn alive(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }

    pub fn is_running(&self) -> bool {
        self.live.lock().is_ok_and(|l| l.is_some()) && self.alive() > 0
    }

    /// Print something, as the agent would.
    pub fn say(&self, text: &str) {
        let live = self.live.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((log, sink)) = live.as_ref() {
            log.lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(Stream::Stdout, text);
            sink.activity();
        }
    }

    pub fn complete(&self) {
        self.finish.notify_one();
    }

    pub fn steered(&self) -> Vec<String> {
        self.steered
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn terminations(&self) -> Vec<TerminateReason> {
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
        tokio::select! {
            () = self.finish.notified() => Ok(WorkerOutcome::Completed),
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

/// Wraps a store and fails exactly one `append`: the `fail_at`th (1-based). With
/// `land`, the event is written first and the error reported afterwards (a crash
/// between the write and the acknowledgement): it is in the log but the runtime
/// never emitted it.
pub struct FailingStore {
    inner: Arc<dyn RunStore>,
    appends: AtomicUsize,
    fail_at: usize,
    land: bool,
}

impl FailingStore {
    pub fn new(inner: Arc<dyn RunStore>, fail_at: usize) -> Self {
        Self {
            inner,
            appends: AtomicUsize::new(0),
            fail_at,
            land: false,
        }
    }

    pub fn landing(inner: Arc<dyn RunStore>, fail_at: usize) -> Self {
        Self {
            land: true,
            ..Self::new(inner, fail_at)
        }
    }
}

#[async_trait]
impl RunStore for FailingStore {
    async fn create(&self, header: &RunHeader) -> Result<(), StoreError> {
        self.inner.create(header).await
    }
    async fn append(&self, event: &EventEnvelope) -> Result<(), StoreError> {
        if self.appends.fetch_add(1, Ordering::SeqCst) + 1 == self.fail_at {
            if self.land {
                self.inner.append(event).await?;
            }
            return Err(StoreError::Io {
                path: "events.jsonl".into(),
                source: std::io::Error::other("disk on fire (injected)"),
            });
        }
        self.inner.append(event).await
    }
    async fn snapshot(&self, state: &RunState) -> Result<(), StoreError> {
        self.inner.snapshot(state).await
    }
    async fn load(&self, id: RunId) -> Result<RunState, StoreError> {
        self.inner.load(id).await
    }
    async fn events(&self, id: RunId, from: EventSeq) -> Result<Vec<EventEnvelope>, StoreError> {
        self.inner.events(id, from).await
    }
    async fn list(&self, filter: &RunFilter) -> Result<Vec<RunSummary>, StoreError> {
        self.inner.list(filter).await
    }
    async fn read_log(&self, id: RunId) -> Result<EventLogRead, StoreError> {
        self.inner.read_log(id).await
    }
    async fn delete(&self, id: RunId) -> Result<(), StoreError> {
        self.inner.delete(id).await
    }
    async fn ids(&self) -> Result<Vec<RunId>, StoreError> {
        self.inner.ids().await
    }
    async fn claim(&self, id: RunId) -> Result<Option<RunClaim>, StoreError> {
        self.inner.claim(id).await
    }
    async fn claim_new(&self, id: RunId) -> Result<Option<RunClaim>, StoreError> {
        self.inner.claim_new(id).await
    }
}

// ---------------------------------------------------------------------------
// The factory
// ---------------------------------------------------------------------------

pub fn fast_config() -> RunConfig {
    RunConfig {
        worker: WorkerKind::Simulated,
        min_assessment_interval: Duration::from_millis(3),
        idle_poll_interval: Duration::from_millis(20),
        overall_timeout: Duration::from_secs(600),
        worker_timeout: Duration::from_secs(600),
        skip_unchanged_assessments: false,
        max_assessments: 1_000_000,
        ..RunConfig::default()
    }
}

/// Builds runs from simulated parts. `run.start` params steer what is built:
///
/// - `worker = "simulated"`: `SimulatedWorker` and `SimulatedModel` (finishes alone);
///   `"nosteer"`: a scripted worker that cannot be steered; otherwise a scripted
///   worker that accepts steers.
/// - `model = "escalate"`: the model asks for a human; `"down"`: the factory
///   refuses with `model_unavailable`.
/// - `cmd = "bad"`: `invalid_config`.
/// - `verify = "fail-append"`: the runtime's store fails its fifth append;
///   `"land-then-fail"`: its fifth append is written and then reported failed;
///   `"fail-create"`: its first append (`RUN_CREATED`) fails;
///   `"panic-model"`: the model panics in routing; `"panic-late"`: on its first
///   assessment, with a worker running; `"panic-factory"`: the factory itself panics;
///   `"slow-build"`: the factory takes 300 ms; `"block-model"`: the model blocks a thread
///   in its first assessment until `block_release` is set.
pub struct TestFactory {
    pub workers: Mutex<Vec<Arc<ScriptWorker>>>,
    pub models: Mutex<Vec<Arc<DialModel>>>,
    /// Set by a `block-model` run once its model is blocking a thread.
    pub block_entered: Arc<std::sync::atomic::AtomicBool>,
    /// Set by the test to let a `block-model` run's thread go.
    pub block_release: Arc<std::sync::atomic::AtomicBool>,
    scratch: PathBuf,
}

impl TestFactory {
    pub fn new(scratch: &Path) -> Arc<Self> {
        Arc::new(Self {
            workers: Mutex::new(Vec::new()),
            models: Mutex::new(Vec::new()),
            block_entered: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            block_release: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            scratch: scratch.to_owned(),
        })
    }

    pub fn last_worker(&self) -> Arc<ScriptWorker> {
        self.workers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .last()
            .cloned()
            .expect("a scripted worker was built")
    }

    pub fn last_model(&self) -> Arc<DialModel> {
        self.models
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .last()
            .cloned()
            .expect("a dial model was built")
    }
}

#[async_trait]
impl RuntimeFactory for TestFactory {
    async fn build(
        &self,
        params: &RunStartParams,
        context: RuntimeContext,
    ) -> Result<BuiltRun, ProtocolError> {
        if params.cmd.as_deref() == Some("bad") {
            return Err(ProtocolError::new(
                ErrorCode::InvalidConfig,
                "cmd must contain {repo}",
            ));
        }
        if params.model.as_deref() == Some("down") {
            return Err(ProtocolError::new(
                ErrorCode::ModelUnavailable,
                "the decision model is unreachable",
            ));
        }
        assert!(
            params.verify.as_deref() != Some("panic-factory"),
            "the factory panicked (intentionally, in a test)"
        );
        if params.verify.as_deref() == Some("slow-build") {
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        let store: Arc<dyn RunStore> = match params.verify.as_deref() {
            Some("fail-append") => Arc::new(FailingStore::new(context.store.clone(), 5)),
            Some("land-then-fail") => Arc::new(FailingStore::landing(context.store.clone(), 5)),
            Some("fail-create") => Arc::new(FailingStore::new(context.store.clone(), 1)),
            _ => context.store.clone(),
        };

        let (model, worker): (Arc<dyn DecisionModel>, Arc<dyn Worker>) =
            if params.worker.as_deref() == Some("simulated") {
                (
                    Arc::new(SimulatedModel::new()),
                    Arc::new(SimulatedWorker::new()),
                )
            } else {
                let steer = if params.worker.as_deref() == Some("nosteer") {
                    SteerOutcome::Unsupported
                } else {
                    SteerOutcome::Queued
                };
                let script = Arc::new(ScriptWorker::new(steer));
                let mode = if params.model.as_deref() == Some("escalate") {
                    Mode::Escalate
                } else {
                    Mode::Working
                };
                let dial = Arc::new(DialModel::new(mode));
                self.workers
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(script.clone());
                self.models
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(dial.clone());
                let model: Arc<dyn DecisionModel> = match params.verify.as_deref() {
                    Some("panic-model") => Arc::new(PanicModel),
                    Some("panic-late") => Arc::new(PanicLateModel(dial, script.clone())),
                    Some("block-model") => Arc::new(BlockingModel {
                        dial,
                        entered: self.block_entered.clone(),
                        release: self.block_release.clone(),
                    }),
                    _ => dial,
                };
                (model, script)
            };

        let temp = self.scratch.join("evidence-tmp");
        std::fs::create_dir_all(&temp)
            .map_err(|e| ProtocolError::new(ErrorCode::Internal, e.to_string()))?;
        let runtime = FactoryRuntime::new(RuntimeDeps {
            config: fast_config(),
            store,
            model,
            worker,
            registry: ResponsibilityRegistry::builtin(),
            definitions: aftman_core::definitions::DefinitionSet::bundled().unwrap_or_default(),
            evidence: EvidencePlan::default(),
            evidence_temp_dir: temp,
            clock: context.clock,
            sink: context.sink,
            ensemble_enabled: false,
            uncertainty_margin: 0.15,
            steering_grace: Duration::from_secs(30),
            launch: [
                (
                    "permissions".to_owned(),
                    params
                        .permissions
                        .clone()
                        .unwrap_or_else(|| "sandboxed".to_owned()),
                ),
                (
                    "external_sandbox".to_owned(),
                    params.external_sandbox.to_string(),
                ),
                ("steering".to_owned(), params.steer.to_string()),
            ]
            .into(),
        });
        Ok(BuiltRun {
            runtime: Arc::new(runtime),
            job: Job {
                prompt: params.job.clone(),
                repo: params.repo.clone(),
                source: JobSource::Cli,
            },
        })
    }

    async fn responsibilities(&self) -> Result<ResponsibilityListResult, ProtocolError> {
        Ok(ResponsibilityListResult {
            responsibilities: vec![ResponsibilityInfo {
                id: "core.completion".into(),
                description: "Is the job done?".into(),
                checks: vec!["ready_to_finish".into()],
            }],
        })
    }

    async fn usage(&self, params: &JevUsageParams) -> Result<Value, ProtocolError> {
        Ok(json!({ "requests": 0, "since": params.since }))
    }
}

// ---------------------------------------------------------------------------
// The harness: an in-process daemon on a socket in a temp directory
// ---------------------------------------------------------------------------

pub struct Harness {
    pub dir: tempfile::TempDir,
    pub repo: PathBuf,
    pub state: PathBuf,
    pub paths: DaemonPaths,
    pub store: Arc<FileRunStore>,
    pub factory: Arc<TestFactory>,
    pub manager: Arc<RunManager>,
    pub task: Option<JoinHandle<()>>,
    pub recovery: RecoveryReport,
    stop: Option<oneshot::Sender<()>>,
}

/// Remove a test directory even if a run's background write is still landing in it.
///
/// A test can end with a run still going: its task is dropped with the runtime, but a
/// snapshot already handed to the blocking pool finishes afterwards and recreates files
/// in a directory `TempDir` has just deleted (or makes the delete fail "not empty"),
/// leaving `/tmp/aft*` behind. Blocking here keeps the async tasks from writing any
/// more, lets in-flight blocking work land, and retries until the tree is gone.
pub fn remove_dir_for_certain(path: &Path) {
    use std::io::ErrorKind;
    use std::time::{Duration as Wait, Instant as Clock};
    let deadline = Clock::now() + Wait::from_secs(5);
    loop {
        match std::fs::remove_dir_all(path) {
            Ok(()) => return,
            Err(error) if error.kind() == ErrorKind::NotFound => return,
            Err(_) if Clock::now() < deadline => std::thread::sleep(Wait::from_millis(20)),
            Err(_) => return,
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
        remove_dir_for_certain(self.dir.path());
    }
}

impl Harness {
    pub async fn start() -> Result<Self, Box<dyn std::error::Error>> {
        Self::start_with(|_| {}).await
    }

    pub async fn start_with(
        tweak: impl FnOnce(&mut DaemonConfig),
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let dir = short_tempdir()?;
        let repo = dir.path().join("repo");
        std::fs::create_dir_all(&repo)?;
        let state = dir.path().join("state");
        Self::start_in(dir, repo, state, tweak).await
    }

    /// Start a daemon over an existing state directory (restart tests).
    pub async fn start_in(
        dir: tempfile::TempDir,
        repo: PathBuf,
        state: PathBuf,
        tweak: impl FnOnce(&mut DaemonConfig),
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let paths = DaemonPaths::in_dir(&dir.path().join("run"));
        let store = Arc::new(FileRunStore::new(&state));
        let factory = TestFactory::new(dir.path());
        let mut config = DaemonConfig::new(paths.clone());
        config.server.write_timeout = Duration::from_millis(500);
        config.server.drain_timeout = Duration::from_secs(5);
        config.manager.shutdown_grace = Duration::from_secs(10);
        config.state_dir = Some(state.clone());
        tweak(&mut config);
        let daemon = Daemon::start(
            config,
            store.clone(),
            factory.clone(),
            Arc::new(SystemClock),
        )
        .await?;
        let manager = daemon.manager().clone();
        let recovery = daemon.recovery().clone();
        let (stop, stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            daemon
                .serve(async move {
                    let _ = stopped.await;
                })
                .await;
        });
        Ok(Self {
            dir,
            repo,
            state,
            paths,
            store,
            factory,
            manager,
            task: Some(task),
            recovery,
            stop: Some(stop),
        })
    }

    pub async fn client(&self) -> Result<Client, Box<dyn std::error::Error>> {
        Client::connect(&self.paths.socket).await
    }

    /// `run.start` with a scripted worker.
    pub async fn start_run(
        &self,
        client: &mut Client,
        tweak: impl FnOnce(&mut Value),
    ) -> Result<RunId, Box<dyn std::error::Error>> {
        let mut params = json!({ "repo": self.repo, "job": "Add retries with backoff." });
        tweak(&mut params);
        let result = client.result("run.start", params).await?;
        Ok(result["run_id"].as_str().ok_or("run_id")?.parse()?)
    }

    /// Ask the serve loop to stop through the shutdown future, and wait for it.
    pub async fn shutdown_via_future(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        self.join().await;
    }

    pub async fn join(&mut self) {
        if let Some(task) = self.task.take() {
            tokio::time::timeout(PATIENCE, task)
                .await
                .expect("the daemon did not stop in time")
                .expect("the daemon task panicked");
        }
    }

    pub async fn status_of(&self, id: RunId) -> Result<String, Box<dyn std::error::Error>> {
        Ok(self.store.load(id).await?.status.as_str().to_owned())
    }

    pub async fn wait_status(&self, id: RunId, want: &str) {
        wait_until(&format!("run {id} to be {want}"), || async {
            self.status_of(id).await.is_ok_and(|s| s == want)
        })
        .await;
    }

    pub async fn wait_worker_running(&self) {
        wait_until("a worker to start", || async {
            self.factory
                .workers
                .lock()
                .is_ok_and(|w| w.last().is_some_and(|w| w.is_running()))
        })
        .await;
    }
}

// ---------------------------------------------------------------------------
// The raw NDJSON client
// ---------------------------------------------------------------------------

pub struct Client {
    reader: BufReader<OwnedReadHalf>,
    pub writer: OwnedWriteHalf,
    next_id: u64,
    /// Event pushes seen while waiting for something else.
    pub events: VecDeque<Value>,
    /// Id-less error pushes seen while waiting for something else.
    pub pushes: VecDeque<Value>,
    /// Responses to other pipelined requests, seen while waiting for one.
    pub other_responses: Vec<Value>,
}

impl Client {
    pub async fn connect(socket: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let stream = tokio::time::timeout(PATIENCE, UnixStream::connect(socket)).await??;
        Ok(Self::from_stream(stream))
    }

    pub fn from_stream(stream: UnixStream) -> Self {
        let (read, writer) = stream.into_split();
        Self {
            reader: BufReader::new(read),
            writer,
            next_id: 1,
            events: VecDeque::new(),
            pushes: VecDeque::new(),
            other_responses: Vec::new(),
        }
    }

    pub async fn send_raw(&mut self, text: &str) -> Result<(), Box<dyn std::error::Error>> {
        tokio::time::timeout(PATIENCE, self.writer.write_all(text.as_bytes())).await??;
        Ok(())
    }

    pub async fn send_bytes(&mut self, bytes: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        tokio::time::timeout(PATIENCE, self.writer.write_all(bytes)).await??;
        Ok(())
    }

    /// Send a request without waiting; returns its id.
    pub async fn send(
        &mut self,
        method: &str,
        params: Value,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let id = self.next_id;
        self.next_id += 1;
        let line = json!({ "id": id, "method": method, "params": params });
        self.send_raw(&format!("{line}\n")).await?;
        Ok(id)
    }

    /// The next line from the server, or `None` at end of stream.
    pub async fn read_json(&mut self) -> Result<Option<Value>, Box<dyn std::error::Error>> {
        let mut line = String::new();
        let n = tokio::time::timeout(PATIENCE, self.reader.read_line(&mut line))
            .await
            .map_err(|_| "timed out waiting for the server")??;
        if n == 0 {
            return Ok(None);
        }
        Ok(Some(serde_json::from_str(line.trim_end())?))
    }

    /// Read until the response to `id`, buffering everything else seen on the way
    /// (event pushes, error pushes, responses to other pipelined requests).
    pub async fn response_for(&mut self, id: u64) -> Result<Value, Box<dyn std::error::Error>> {
        if let Some(at) = self
            .other_responses
            .iter()
            .position(|r| r.get("id").and_then(Value::as_u64) == Some(id))
        {
            return Ok(self.other_responses.remove(at));
        }
        loop {
            let message = self
                .read_json()
                .await?
                .ok_or("connection closed while waiting for a response")?;
            if message.get("event").is_some() {
                self.events.push_back(message);
            } else if message.get("id").and_then(Value::as_u64) == Some(id) {
                return Ok(message);
            } else if message.get("id").is_some() {
                self.other_responses.push(message);
            } else {
                self.pushes.push_back(message);
            }
        }
    }

    pub async fn call(
        &mut self,
        method: &str,
        params: Value,
    ) -> Result<Value, Box<dyn std::error::Error>> {
        let id = self.send(method, params).await?;
        self.response_for(id).await
    }

    /// The `result` of a call that must succeed.
    pub async fn result(
        &mut self,
        method: &str,
        params: Value,
    ) -> Result<Value, Box<dyn std::error::Error>> {
        let response = self.call(method, params).await?;
        if !response["error"].is_null() {
            return Err(format!("{method} failed: {}", response["error"]).into());
        }
        Ok(response["result"].clone())
    }

    /// The `error` of a call that must fail.
    pub async fn error(
        &mut self,
        method: &str,
        params: Value,
    ) -> Result<Value, Box<dyn std::error::Error>> {
        let response = self.call(method, params).await?;
        if response["error"].is_null() {
            return Err(format!("{method} unexpectedly succeeded: {response}").into());
        }
        Ok(response["error"].clone())
    }

    pub async fn error_code(
        &mut self,
        method: &str,
        params: Value,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let error = self.error(method, params).await?;
        Ok(error["code"].as_str().unwrap_or("").to_owned())
    }

    /// The next event push, `None` at end of stream. An id-less error push is
    /// returned as an `Err`.
    pub async fn next_event(&mut self) -> Result<Option<Value>, Box<dyn std::error::Error>> {
        if let Some(event) = self.events.pop_front() {
            return Ok(Some(event["event"].clone()));
        }
        match self.read_json().await? {
            None => Ok(None),
            Some(message) if message.get("event").is_some() => Ok(Some(message["event"].clone())),
            Some(other) => Err(format!("unexpected message: {other}").into()),
        }
    }

    /// Collect events until a `RUN_FINISHED` for `run` (inclusive).
    pub async fn events_until_finished(
        &mut self,
        run: RunId,
    ) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
        let mut seen = Vec::new();
        loop {
            let event = self
                .next_event()
                .await?
                .ok_or("stream ended before RUN_FINISHED")?;
            let done = event["kind"] == "RUN_FINISHED" && event["run_id"] == json!(run);
            seen.push(event);
            if done {
                return Ok(seen);
            }
        }
    }

    /// Read events until `count` have been seen.
    pub async fn take_events(
        &mut self,
        count: usize,
    ) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
        let mut seen = Vec::new();
        while seen.len() < count {
            seen.push(self.next_event().await?.ok_or("stream ended early")?);
        }
        Ok(seen)
    }
}

pub fn seqs(events: &[Value]) -> Vec<u64> {
    events
        .iter()
        .map(|e| e["seq"].as_u64().unwrap_or(0))
        .collect()
}

/// The store's log as wire JSON, for byte-for-byte comparison with pushes.
pub async fn log_as_json(
    store: &dyn RunStore,
    run: RunId,
) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
    Ok(store
        .events(run, EventSeq::FIRST)
        .await?
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<_, _>>()?)
}

pub fn kinds(events: &[Value]) -> Vec<String> {
    events
        .iter()
        .map(|e| e["kind"].as_str().unwrap_or("?").to_owned())
        .collect()
}

pub fn event_count(events: &[EventEnvelope], kind: EventKind) -> usize {
    events.iter().filter(|e| e.kind == kind).count()
}
