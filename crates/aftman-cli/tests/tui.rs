//! `aftman watch`, end to end: the real driver against an in-process daemon, on a
//! `TestBackend`, with scripted keys.
//!
//! Offline: no network, no real agent, no credentials, no environment variables. Runs
//! come from the real `CliFactory` (simulated worker and model; `sh -c 'sleep ..'` and
//! `true` behind the `generic` backend) or, where a test needs a worker that stays
//! alive and can be steered, from a small factory in this file that wraps it.
//!
//! Every wait has a deadline, and none relies on a fixed sleep for correctness: a test
//! polls the screen (or the daemon) until the thing it wants appears.

use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use ratatui::Terminal;
use ratatui::backend::{Backend, ClearType, TestBackend, WindowSize};
use ratatui::buffer::Cell;
use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::layout::{Position, Size};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::UnixListener;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;

use aftman_cli::client::{Client, ClientConfig, ClientError};
use aftman_cli::error::exit;
use aftman_cli::factory::{CliFactory, FactoryInputs, Mode};
use aftman_cli::tui::driver::{How, TermEvent, WatchError, WatchOptions, WatchOutcome, run_watch};
use aftman_cli::tui::state::WatchState;
use aftman_core::clock::SystemClock;
use aftman_core::definitions::DefinitionSet;
use aftman_core::event::{EventEnvelope, EventKind};
use aftman_core::evidence::EvidencePlan;
use aftman_core::ids::{EventSeq, RunId};
use aftman_core::job::{FactoryStatus, Job, JobSource, RunConfig, WorkerKind};
use aftman_core::log::Stream;
use aftman_core::responsibility::ResponsibilityRegistry;
use aftman_core::testing::SimulatedModel;
use aftman_core::worker::{
    SteerOutcome, TerminateReason, Worker, WorkerCtx, WorkerError, WorkerEventSink, WorkerOutcome,
};
use aftman_daemon::protocol::{
    ErrorCode, JevUsageParams, ProtocolError, Request, Response, ResponsibilityListResult,
    RunStartParams, ServerMessage, SubscriptionOpenResult, decode_request, encode_line,
};
use aftman_daemon::{
    BuiltRun, Daemon, DaemonConfig, DaemonPaths, FactoryRuntime, RuntimeContext, RuntimeDeps,
    RuntimeFactory, default_margin,
};
use aftman_store::FileRunStore;

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// The longest any wait in this file lasts.
const PATIENCE: Duration = Duration::from_secs(30);

/// The job text that makes [`TestFactory`] build a parked, steerable worker.
const PARK: &str = "PARK";

async fn within<T>(what: &str, future: impl Future<Output = T>) -> T {
    match tokio::time::timeout(PATIENCE, future).await {
        Ok(value) => value,
        Err(_) => panic!("timed out waiting for {what}"),
    }
}

fn short_tempdir() -> Result<tempfile::TempDir, io::Error> {
    tempfile::Builder::new().prefix("aft").tempdir_in("/tmp")
}

// ===========================================================================
// A worker that stays alive, takes steering, and prints hostile output
// ===========================================================================

#[derive(Default)]
struct Parked {
    steered: Mutex<Vec<String>>,
    stop: AtomicBool,
    wake: Notify,
}

impl Parked {
    fn steered(&self) -> Vec<String> {
        self.steered.lock().map(|s| s.clone()).unwrap_or_default()
    }
}

struct ParkedWorker(Arc<Parked>);

#[async_trait]
impl Worker for ParkedWorker {
    fn kind(&self) -> WorkerKind {
        WorkerKind::Simulated
    }

    fn supports_steering(&self) -> bool {
        true
    }

    async fn run(
        &self,
        ctx: WorkerCtx,
        sink: WorkerEventSink,
    ) -> Result<WorkerOutcome, WorkerError> {
        self.0.stop.store(false, Ordering::SeqCst);
        {
            let mut output = ctx
                .output
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            output.push(Stream::Stdout, "parked and waiting for guidance\n");
            // What a hostile repository could make a worker print.
            output.push(
                Stream::Stdout,
                "\u{1b}]0;pwned\u{7}\u{1b}[2J\u{1b}[31mscary-looking\u{1b}[0m\u{202e} text\rall tests passed\n",
            );
        }
        sink.activity();
        loop {
            if self.0.stop.load(Ordering::SeqCst) {
                return Ok(WorkerOutcome::Terminated {
                    reason: TerminateReason::Stopped,
                });
            }
            self.0.wake.notified().await;
        }
    }

    async fn steer(&self, message: &str) -> Result<SteerOutcome, WorkerError> {
        if let Ok(mut steered) = self.0.steered.lock() {
            steered.push(message.to_owned());
        }
        Ok(SteerOutcome::Delivered)
    }

    async fn terminate(&self, _reason: TerminateReason) -> Result<(), WorkerError> {
        self.0.stop.store(true, Ordering::SeqCst);
        self.0.wake.notify_one();
        Ok(())
    }
}

/// The real factory, except for the job text [`PARK`].
struct TestFactory {
    inner: CliFactory,
    parked: Arc<Parked>,
    scratch: PathBuf,
}

#[async_trait]
impl RuntimeFactory for TestFactory {
    async fn build(
        &self,
        params: &RunStartParams,
        context: RuntimeContext,
    ) -> Result<BuiltRun, ProtocolError> {
        if params.job != PARK {
            return self.inner.build(params, context).await;
        }
        let internal = |m: String| ProtocolError::new(ErrorCode::Internal, m);
        let definitions = DefinitionSet::bundled().map_err(|e| internal(format!("{e}")))?;
        std::fs::create_dir_all(&self.scratch).map_err(|e| internal(format!("{e}")))?;
        let runtime = FactoryRuntime::new(RuntimeDeps {
            config: RunConfig {
                worker: WorkerKind::Simulated,
                ..RunConfig::default()
            },
            store: context.store,
            model: Arc::new(SimulatedModel::new()),
            worker: Arc::new(ParkedWorker(Arc::clone(&self.parked))),
            registry: ResponsibilityRegistry::builtin(),
            definitions,
            evidence: EvidencePlan::default(),
            evidence_temp_dir: self.scratch.clone(),
            clock: context.clock,
            sink: context.sink,
            ensemble_enabled: false,
            uncertainty_margin: default_margin(),
            steering_grace: Duration::from_secs(30),
            launch: [
                ("permissions", "auto-approve"),
                ("external_sandbox", "true"),
                ("steering", "available"),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect(),
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
        self.inner.responsibilities().await
    }

    async fn usage(&self, params: &JevUsageParams) -> Result<Value, ProtocolError> {
        self.inner.usage(params).await
    }
}

// ===========================================================================
// An in-process daemon
// ===========================================================================

struct Harness {
    _dir: tempfile::TempDir,
    paths: DaemonPaths,
    repo: PathBuf,
    parked: Arc<Parked>,
    task: Option<JoinHandle<()>>,
    stop: Option<oneshot::Sender<()>>,
}

impl Harness {
    async fn start() -> Result<Self, Box<dyn std::error::Error>> {
        let dir = short_tempdir()?;
        let repo = dir.path().join("repo");
        std::fs::create_dir_all(&repo)?;
        let state = dir.path().join("state");
        let paths = DaemonPaths::in_dir(&dir.path().join("run"));
        let store = Arc::new(FileRunStore::new(&state));
        let parked = Arc::new(Parked::default());
        let factory = TestFactory {
            inner: CliFactory::new(&state, FactoryInputs::empty(), Mode::Daemon),
            parked: Arc::clone(&parked),
            scratch: dir.path().join("scratch"),
        };
        let daemon = Daemon::start(
            DaemonConfig::new(paths.clone()),
            store,
            Arc::new(factory),
            Arc::new(SystemClock),
        )
        .await?;
        let (stop, stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            daemon
                .serve(async move {
                    let _ = stopped.await;
                })
                .await;
        });
        Ok(Self {
            _dir: dir,
            paths,
            repo,
            parked,
            task: Some(task),
            stop: Some(stop),
        })
    }

    async fn client(&self) -> Result<Client, ClientError> {
        Client::connect(&self.paths.socket).await
    }

    fn simulated(&self) -> RunStartParams {
        RunStartParams {
            worker: Some("simulated".into()),
            simulate_model: true,
            ..RunStartParams::new(&self.repo, "Add retries with backoff.")
        }
    }

    fn parked_run(&self) -> RunStartParams {
        RunStartParams {
            worker: Some("simulated".into()),
            simulate_model: true,
            ..RunStartParams::new(&self.repo, PARK)
        }
    }

    /// A worker that sleeps (no input channel), behind the generic backend.
    fn sleeper(&self, unique: &str) -> RunStartParams {
        RunStartParams {
            worker: Some("generic".into()),
            cmd: Some(format!("sh -c 'sleep {unique}' {{repo}}")),
            external_sandbox: true,
            simulate_model: true,
            ..RunStartParams::new(&self.repo, "wait")
        }
    }

    /// A worker that exits at once without doing the job: ends in an escalation.
    fn escalating(&self) -> RunStartParams {
        RunStartParams {
            worker: Some("generic".into()),
            cmd: Some("true {repo}".into()),
            external_sandbox: true,
            simulate_model: true,
            ..RunStartParams::new(&self.repo, "do the impossible")
        }
    }

    fn options(&self) -> WatchOptions {
        let mut options = WatchOptions::new(&self.paths.socket, Arc::new(SystemClock));
        options.frame_interval = Duration::from_millis(10);
        options.client = ClientConfig {
            connect_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_secs(20),
            reconnect_attempts: 3,
            reconnect_backoff: Duration::from_millis(10),
            reconnect_max_backoff: Duration::from_millis(40),
        };
        options
    }

    /// Stop whatever is still live, so no run outlives its test.
    async fn cancel_all(&self) {
        let Ok(mut client) = self.client().await else {
            return;
        };
        let Ok(runs) = client.all_runs(None, 100).await else {
            return;
        };
        for run in runs {
            if !run.status.is_terminal() {
                let _ = client.run_cancel(run.id).await;
            }
        }
    }

    async fn shutdown(&mut self) {
        self.cancel_all().await;
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = within("the daemon to stop", task).await;
        }
    }
}

async fn wait_for_status(
    client: &mut Client,
    run: RunId,
    want: FactoryStatus,
) -> Result<(), Box<dyn std::error::Error>> {
    within(&format!("run {run} to be {want:?}"), async {
        loop {
            if client.run_get(run).await?.status == want {
                return Ok::<(), ClientError>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    Ok(())
}

// ===========================================================================
// A TestBackend two parties can look at
// ===========================================================================

/// `TestBackend` behind a lock: the driver owns the `Terminal`, the test reads the
/// screen it drew while the driver is still running.
#[derive(Clone)]
struct SharedBackend(Arc<Mutex<TestBackend>>);

impl SharedBackend {
    fn new(width: u16, height: u16) -> Self {
        Self(Arc::new(Mutex::new(TestBackend::new(width, height))))
    }

    fn with<T>(&self, f: impl FnOnce(&mut TestBackend) -> T) -> T {
        let mut guard = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        f(&mut guard)
    }

    fn screen(&self) -> String {
        self.with(|backend| {
            let buffer = backend.buffer();
            let area = buffer.area;
            let mut out = String::new();
            for y in 0..area.height {
                let mut skip = 0;
                for x in 0..area.width {
                    let symbol = buffer[(x, y)].symbol();
                    if skip > 0 {
                        skip -= 1;
                        continue;
                    }
                    skip = unicode_width::UnicodeWidthStr::width(symbol).saturating_sub(1);
                    out.push_str(symbol);
                }
                out.push('\n');
            }
            out
        })
    }
}

impl Backend for SharedBackend {
    fn draw<'a, I>(&mut self, content: I) -> io::Result<()>
    where
        I: Iterator<Item = (u16, u16, &'a Cell)>,
    {
        self.with(|b| b.draw(content))
    }

    fn append_lines(&mut self, n: u16) -> io::Result<()> {
        self.with(|b| b.append_lines(n))
    }

    fn hide_cursor(&mut self) -> io::Result<()> {
        self.with(Backend::hide_cursor)
    }

    fn show_cursor(&mut self) -> io::Result<()> {
        self.with(Backend::show_cursor)
    }

    fn get_cursor_position(&mut self) -> io::Result<Position> {
        self.with(Backend::get_cursor_position)
    }

    fn set_cursor_position<P: Into<Position>>(&mut self, position: P) -> io::Result<()> {
        let position = position.into();
        self.with(|b| b.set_cursor_position(position))
    }

    fn clear(&mut self) -> io::Result<()> {
        self.with(Backend::clear)
    }

    fn clear_region(&mut self, clear_type: ClearType) -> io::Result<()> {
        self.with(|b| b.clear_region(clear_type))
    }

    fn size(&self) -> io::Result<Size> {
        self.with(|b| b.size())
    }

    fn window_size(&mut self) -> io::Result<WindowSize> {
        self.with(Backend::window_size)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.with(Backend::flush)
    }
}

// ===========================================================================
// A watcher: the real driver plus scripted keys
// ===========================================================================

struct Watcher {
    backend: SharedBackend,
    keys: mpsc::Sender<TermEvent>,
    task: JoinHandle<Result<WatchOutcome, WatchError>>,
}

fn key(code: KeyCode) -> TermEvent {
    TermEvent::Key(KeyEvent::new(code, KeyModifiers::NONE))
}

impl Watcher {
    fn spawn(h: &Harness, run: RunId, width: u16, height: u16) -> Result<Self, io::Error> {
        Self::spawn_with(h.options(), run, width, height)
    }

    fn spawn_with(
        options: WatchOptions,
        run: RunId,
        width: u16,
        height: u16,
    ) -> Result<Self, io::Error> {
        let backend = SharedBackend::new(width, height);
        let mut terminal = Terminal::new(backend.clone())?;
        let (keys, mut input) = mpsc::channel(64);
        let task =
            tokio::spawn(async move { run_watch(&mut terminal, &mut input, run, &options).await });
        Ok(Self {
            backend,
            keys,
            task,
        })
    }

    fn screen(&self) -> String {
        self.backend.screen()
    }

    /// Wait until the screen satisfies `ready`; fail with the last screen otherwise.
    async fn until(&self, what: &str, ready: impl Fn(&str) -> bool) -> String {
        let deadline = tokio::time::Instant::now() + PATIENCE;
        loop {
            let screen = self.screen();
            if ready(&screen) {
                return screen;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {what}; the screen is:\n{screen}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn contains(&self, needle: &str) -> String {
        self.until(&format!("{needle:?} on screen"), |s| s.contains(needle))
            .await
    }

    async fn press(&self, code: KeyCode) {
        self.keys
            .send(key(code))
            .await
            .expect("the watcher is alive");
    }

    async fn ch(&self, c: char) {
        self.press(KeyCode::Char(c)).await;
    }

    async fn type_text(&self, text: &str) {
        for c in text.chars() {
            self.ch(c).await;
        }
    }

    /// Quit and collect the outcome.
    async fn quit(self) -> WatchOutcome {
        self.press(KeyCode::Char('q')).await;
        self.outcome().await
    }

    async fn outcome(self) -> WatchOutcome {
        match within("the watcher to end", self.task).await {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(error)) => panic!("the watcher failed: {error}"),
            Err(error) => panic!("the watcher panicked: {error}"),
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[tokio::test]
async fn a_run_is_watched_to_completion_and_the_final_screen_stays_up() -> TestResult {
    let mut h = Harness::start().await?;
    let mut client = h.client().await?;
    let run = client.run_start(h.simulated()).await?;

    let watcher = Watcher::spawn(&h, run, 120, 40)?;
    let screen = watcher
        .until("the run to finish", |s| s.contains("run ended"))
        .await;
    for needle in [
        "FINISHED",
        "1 Decisions",
        "2 Assessment",
        "3 Worker output",
        "finish",
        "test result: ok. 3 passed",
        "decisive",
        "margin",
        "worker: simulated",
        "Add retries with backoff.",
        "q quit",
    ] {
        assert!(screen.contains(needle), "missing {needle:?} in:\n{screen}");
    }
    assert!(!screen.contains("NEEDS HUMAN"));

    // It is still up, still responding: scroll, help, close help.
    watcher.ch('2').await;
    watcher.ch('?').await;
    watcher.contains("Keys").await;
    watcher.ch('x').await;
    watcher
        .until("help to close", |s| !s.contains("Keys"))
        .await;

    let outcome = watcher.quit().await;
    assert!(matches!(outcome.how, How::Quit));
    assert_eq!(outcome.exit_code(), exit::FINISHED);

    // One code path: what the watcher folded live is what a replay of the log gives.
    let mut expected = WatchState::new();
    expected.set_header(&client.run_get(run).await?);
    expected.apply_all(&client.all_events(run, EventSeq::FIRST).await?);
    assert_eq!(outcome.state, expected);
    assert!(outcome.state.is_finished());
    h.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn the_screen_is_correct_at_80x24_too() -> TestResult {
    let mut h = Harness::start().await?;
    let mut client = h.client().await?;
    let run = client.run_start(h.simulated()).await?;
    let watcher = Watcher::spawn(&h, run, 80, 24)?;
    let screen = watcher
        .until("the run to finish", |s| s.contains("run ended"))
        .await;
    for needle in ["FINISHED", "finish", "test result: ok. 3 passed", "margin"] {
        assert!(screen.contains(needle), "missing {needle:?} in:\n{screen}");
    }
    let _ = watcher.quit().await;
    h.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn a_second_watcher_attached_mid_run_converges_to_the_same_state() -> TestResult {
    let mut h = Harness::start().await?;
    let mut client = h.client().await?;
    let run = client.run_start(h.parked_run()).await?;

    let first = Watcher::spawn(&h, run, 120, 40)?;
    first.contains("parked and waiting").await;
    // Let a few assessment cycles happen before the second one attaches.
    first
        .until("an assessment", |s| {
            s.contains("assessments 1") || s.contains("assessments 2")
        })
        .await;

    let second = Watcher::spawn(&h, run, 120, 40)?;
    second.contains("parked and waiting").await;

    // Both are live; end the run from outside and let both see it end.
    client.run_cancel(run).await?;
    first
        .until("run ended (first)", |s| s.contains("run ended"))
        .await;
    second
        .until("run ended (second)", |s| s.contains("run ended"))
        .await;

    let a = first.quit().await;
    let b = second.quit().await;
    assert_eq!(a.state, b.state, "two watchers saw the same run");
    assert!(a.state.last_seq > 5);
    assert_eq!(a.exit_code(), exit::FAILED, "cancelled");
    assert_eq!(a.state.status_label(), "cancelled");
    h.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn steer_keystrokes_reach_the_worker_and_the_footer_reports_the_outcome() -> TestResult {
    let mut h = Harness::start().await?;
    let mut client = h.client().await?;
    let run = client.run_start(h.parked_run()).await?;

    let watcher = Watcher::spawn(&h, run, 120, 40)?;
    watcher.contains("parked and waiting").await;
    watcher.ch('s').await;
    watcher.contains("Steer the worker").await;
    watcher
        .type_text("move the retry into http.rs, héllo")
        .await;
    watcher.press(KeyCode::Enter).await;

    let screen = watcher.contains("steer delivered to the worker").await;
    assert!(!screen.contains("Steer the worker"), "the box closed");
    assert_eq!(
        h.parked.steered(),
        vec!["move the retry into http.rs, héllo"]
    );
    // The daemon's event for it shows in the timeline and the counter.
    watcher.contains("steer (human)").await;
    watcher
        .until("the steer counter", |s| s.contains("steers 1"))
        .await;

    // Quitting does not cancel the run.
    let outcome = watcher.quit().await;
    assert_eq!(outcome.exit_code(), exit::DETACHED);
    assert!(matches!(outcome.how, How::Quit));
    let state = client.run_get(run).await?;
    assert!(
        !state.status.is_terminal(),
        "the run is still live: {:?}",
        state.status
    );
    h.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn a_worker_with_no_input_channel_says_so_in_the_footer_and_the_tui_stays_up() -> TestResult {
    let mut h = Harness::start().await?;
    let mut client = h.client().await?;
    let run = client.run_start(h.sleeper("31337")).await?;
    wait_for_status(&mut client, run, FactoryStatus::Running).await?;

    let watcher = Watcher::spawn(&h, run, 120, 40)?;
    watcher.contains("RUNNING").await;
    watcher.ch('s').await;
    watcher.type_text("go left").await;
    watcher.press(KeyCode::Enter).await;
    let screen = watcher.contains("steering_unsupported").await;
    assert!(screen.contains("RUNNING"), "still watching:\n{screen}");

    // The failed message is not lost: it is back in the editor next time.
    watcher.ch('s').await;
    watcher.contains("go left").await;
    watcher.press(KeyCode::Esc).await;
    watcher
        .until("the box to close", |s| !s.contains("Steer the worker"))
        .await;

    let outcome = watcher.quit().await;
    assert_eq!(outcome.exit_code(), exit::DETACHED);
    h.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn cancel_asks_first_and_then_ends_the_run_as_cancelled() -> TestResult {
    let mut h = Harness::start().await?;
    let mut client = h.client().await?;
    let run = client.run_start(h.sleeper("31338")).await?;
    wait_for_status(&mut client, run, FactoryStatus::Running).await?;

    let watcher = Watcher::spawn(&h, run, 100, 30)?;
    watcher.contains("RUNNING").await;

    // Declined: nothing happens.
    watcher.ch('c').await;
    watcher.contains("Cancel this run?").await;
    watcher.press(KeyCode::Esc).await;
    watcher
        .until("the prompt to close", |s| !s.contains("Cancel this run?"))
        .await;
    assert!(!client.run_get(run).await?.status.is_terminal());

    // Confirmed.
    watcher.ch('c').await;
    watcher.contains("Cancel this run?").await;
    watcher.type_text("cancel").await;
    watcher.press(KeyCode::Enter).await;
    let screen = watcher
        .until("the run to end", |s| {
            s.contains("CANCELLED") && s.contains("run ended")
        })
        .await;
    assert!(!screen.contains("s steer"), "no steering once it ended");

    // Cancelling again is refused by the daemon and reported, not fatal.
    watcher.ch('c').await;
    watcher
        .until("a refusal", |s| s.contains("nothing to cancel"))
        .await;

    let outcome = watcher.quit().await;
    assert_eq!(outcome.exit_code(), exit::FAILED);
    assert_eq!(outcome.state.status_label(), "cancelled");
    h.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn an_escalation_is_answered_from_the_tui_and_the_run_continues() -> TestResult {
    let mut h = Harness::start().await?;
    let mut client = h.client().await?;
    let run = client.run_start(h.escalating()).await?;
    wait_for_status(&mut client, run, FactoryStatus::NeedsHuman).await?;
    let open = client
        .run_get(run)
        .await?
        .open_escalations()
        .next()
        .map(|e| e.id.to_string())
        .ok_or("an open escalation")?;

    let watcher = Watcher::spawn(&h, run, 120, 40)?;
    let screen = watcher.contains("NEEDS A HUMAN").await;
    assert!(
        screen.contains(&open),
        "the banner names the escalation:\n{screen}"
    );
    assert!(screen.contains("NEEDS HUMAN"), "and the badge says so");
    assert!(!screen.contains("FAILED"));

    // a -> pick "recheck" -> no note.
    watcher.ch('a').await;
    let popup = watcher.contains("Answer the escalation").await;
    assert!(popup.contains("claim-complete") && popup.contains("guidance"));
    watcher.ch('2').await;
    watcher.contains("Look again").await;
    watcher.press(KeyCode::Enter).await;
    watcher.contains("Note for 'recheck'").await;
    watcher.type_text("worth another look").await;
    watcher.press(KeyCode::Enter).await;

    watcher.contains("answered the escalation: recheck").await;
    watcher.contains(&format!("answered {open}: recheck")).await;
    // The run moved on: a later event than the answer exists, and the log has it.
    let events = within("the answer to be logged", async {
        loop {
            let events = client.all_events(run, EventSeq::FIRST).await?;
            if events
                .iter()
                .any(|e| e.kind == EventKind::EscalationAnswered)
            {
                return Ok::<Vec<EventEnvelope>, ClientError>(events);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    let answered = events
        .iter()
        .find(|e| e.kind == EventKind::EscalationAnswered)
        .ok_or("the answer event")?;
    assert_eq!(answered.payload["decision"]["decision"], "recheck");
    assert_eq!(answered.payload["note"], "worth another look");
    assert!(
        events.last().map(|e| e.seq) > Some(answered.seq),
        "the run continued after the answer"
    );

    let _ = watcher.quit().await;
    h.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn answering_stop_asks_for_confirmation_and_ends_the_run() -> TestResult {
    let mut h = Harness::start().await?;
    let mut client = h.client().await?;
    let run = client.run_start(h.escalating()).await?;
    wait_for_status(&mut client, run, FactoryStatus::NeedsHuman).await?;

    let watcher = Watcher::spawn(&h, run, 100, 30)?;
    watcher.contains("NEEDS A HUMAN").await;

    // Declined at the confirmation: still waiting for a human.
    watcher.ch('a').await;
    watcher.ch('3').await;
    watcher.press(KeyCode::Enter).await;
    watcher.press(KeyCode::Enter).await; // no note
    watcher.contains("Answer: stop?").await;
    watcher.press(KeyCode::Esc).await;
    watcher
        .until("the prompt to close", |s| !s.contains("Answer: stop?"))
        .await;
    assert_eq!(client.run_get(run).await?.status, FactoryStatus::NeedsHuman);

    // Confirmed.
    watcher.ch('a').await;
    watcher.ch('3').await;
    watcher.press(KeyCode::Enter).await;
    watcher.type_text("not worth it").await;
    watcher.press(KeyCode::Enter).await;
    watcher.contains("Answer: stop?").await;
    watcher.type_text("stop").await;
    watcher.press(KeyCode::Enter).await;
    watcher
        .until("the run to end", |s| s.contains("run ended"))
        .await;

    let outcome = watcher.quit().await;
    assert_eq!(outcome.exit_code(), exit::FAILED);
    assert!(outcome.state.is_finished());
    h.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn an_answer_that_lost_the_race_is_refused_by_the_daemon_and_shown() -> TestResult {
    let mut h = Harness::start().await?;
    let mut client = h.client().await?;
    let run = client.run_start(h.escalating()).await?;
    wait_for_status(&mut client, run, FactoryStatus::NeedsHuman).await?;
    let open = client
        .run_get(run)
        .await?
        .open_escalations()
        .next()
        .map(|e| e.id)
        .ok_or("an open escalation")?;

    let watcher = Watcher::spawn(&h, run, 100, 30)?;
    watcher.contains("NEEDS A HUMAN").await;
    // The popup is open on this screen...
    watcher.ch('a').await;
    watcher.contains("Answer the escalation").await;
    // ...when somebody else (the CLI, say) answers first.
    client
        .run_answer(
            run,
            open,
            aftman_core::assessment::HumanDecision::Stop,
            "elsewhere",
        )
        .await?;
    watcher
        .until("the run to end", |s| s.contains("run ended"))
        .await;
    // Choosing accept now is refused by the daemon; the screen says so and carries on.
    watcher.ch('1').await;
    watcher.press(KeyCode::Enter).await;
    watcher.press(KeyCode::Enter).await;
    let screen = watcher.contains("wrong_state").await;
    assert!(screen.contains("run ended"), "{screen}");
    let outcome = watcher.quit().await;
    assert_eq!(outcome.exit_code(), exit::FAILED);
    h.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn quitting_leaves_the_run_running_in_the_daemon() -> TestResult {
    let mut h = Harness::start().await?;
    let mut client = h.client().await?;
    let run = client.run_start(h.parked_run()).await?;
    let watcher = Watcher::spawn(&h, run, 100, 30)?;
    watcher.contains("parked and waiting").await;

    watcher.press(KeyCode::Esc).await;
    let outcome = watcher.outcome().await;
    assert!(matches!(outcome.how, How::Quit));
    assert_eq!(outcome.exit_code(), exit::DETACHED);

    let state = client.run_get(run).await?;
    assert!(!state.status.is_terminal());

    // A new watcher picks it up where it is, with the whole history.
    let again = Watcher::spawn(&h, run, 100, 30)?;
    again.contains("parked and waiting").await;
    let ctrl_c = TermEvent::Key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL));
    again.keys.send(ctrl_c).await?;
    assert_eq!(again.outcome().await.exit_code(), exit::DETACHED);
    assert!(!client.run_get(run).await?.status.is_terminal());
    h.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn quitting_while_a_human_is_needed_exits_three() -> TestResult {
    let mut h = Harness::start().await?;
    let mut client = h.client().await?;
    let run = client.run_start(h.escalating()).await?;
    wait_for_status(&mut client, run, FactoryStatus::NeedsHuman).await?;
    let watcher = Watcher::spawn(&h, run, 100, 30)?;
    watcher.contains("NEEDS A HUMAN").await;
    let outcome = watcher.quit().await;
    assert_eq!(outcome.exit_code(), exit::NEEDS_HUMAN);
    assert_eq!(client.run_get(run).await?.status, FactoryStatus::NeedsHuman);
    h.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn hostile_worker_output_never_reaches_the_screen() -> TestResult {
    let mut h = Harness::start().await?;
    let mut client = h.client().await?;
    let run = client.run_start(h.parked_run()).await?;
    let watcher = Watcher::spawn(&h, run, 120, 40)?;
    let screen = watcher.contains("scary-looking").await;
    assert!(
        screen.contains("AUTO-APPROVE"),
        "the launch posture shows:\n{screen}"
    );
    assert!(!screen.contains("pwned"));
    assert!(
        screen.contains("all tests passed"),
        "the CR trick shows both parts"
    );
    assert!(
        screen.chars().all(|c| c == '\n' || !c.is_control()),
        "a control character reached the screen"
    );
    let _ = watcher.quit().await;
    h.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn an_unknown_run_is_an_error_before_anything_is_drawn() -> TestResult {
    let mut h = Harness::start().await?;
    let result = {
        let backend = SharedBackend::new(80, 24);
        let mut terminal = Terminal::new(backend)?;
        let (_keys, mut input) = mpsc::channel(4);
        run_watch(&mut terminal, &mut input, RunId::new(), &h.options()).await
    };
    match result {
        Err(WatchError::Client(error)) => {
            assert_eq!(
                error.protocol().map(|e| e.code),
                Some(ErrorCode::UnknownRun)
            );
        }
        other => panic!("expected unknown_run, got {other:?}"),
    }
    h.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn a_closed_input_source_quits_cleanly() -> TestResult {
    let mut h = Harness::start().await?;
    let mut client = h.client().await?;
    let run = client.run_start(h.parked_run()).await?;
    let watcher = Watcher::spawn(&h, run, 100, 30)?;
    watcher.contains("parked and waiting").await;
    watcher.keys.send(TermEvent::Closed).await?;
    let outcome = watcher.outcome().await;
    assert!(matches!(outcome.how, How::Quit));
    h.shutdown().await;
    Ok(())
}

/// A server that acknowledges one subscription, says nothing, then disappears for good.
async fn vanishing_daemon(
    socket: &Path,
    run_id: RunId,
) -> Result<JoinHandle<()>, Box<dyn std::error::Error>> {
    let listener = UnixListener::bind(socket)?;
    let socket = socket.to_owned();
    Ok(tokio::spawn(async move {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let (read, mut write) = stream.into_split();
        let mut lines = BufReader::new(read).lines();
        // Header requests may arrive on their own connections; this is the first one.
        if let Ok(Some(line)) = lines.next_line().await {
            if let Ok(envelope) = decode_request(&line) {
                if let Request::SubscriptionOpen(_) = envelope.request {
                    let result = SubscriptionOpenResult {
                        run_id: Some(run_id),
                        from_seq: Some(EventSeq(1)),
                        replaced: false,
                        live: true,
                    };
                    if let Ok(response) = Response::ok(envelope.id, &result) {
                        if let Ok(line) = encode_line(&ServerMessage::Response(response)) {
                            let _ = write.write_all(line.as_bytes()).await;
                        }
                    }
                }
            }
        }
        // Gone: the connection, the listener and the socket file.
        drop(write);
        drop(lines);
        drop(listener);
        let _ = std::fs::remove_file(&socket);
    }))
}

#[tokio::test]
async fn a_daemon_that_goes_away_shows_an_error_and_ends_cleanly() -> TestResult {
    let dir = short_tempdir()?;
    let socket = dir.path().join("gone.sock");
    let run = RunId::new();
    let server = vanishing_daemon(&socket, run).await?;

    let mut options = WatchOptions::new(&socket, Arc::new(SystemClock));
    options.frame_interval = Duration::from_millis(10);
    options.client = ClientConfig {
        connect_timeout: Duration::from_millis(300),
        request_timeout: Duration::from_millis(300),
        reconnect_attempts: 3,
        reconnect_backoff: Duration::from_millis(10),
        reconnect_max_backoff: Duration::from_millis(20),
    };
    let watcher = Watcher::spawn_with(options, run, 100, 30)?;
    // No `q` is sent: the watch ends by itself, and says why on its last frame.
    let screen_before_end = watcher.backend.clone();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !watcher.task.is_finished() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the watcher did not end; the screen is:\n{}",
            screen_before_end.screen()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let outcome = watcher.outcome().await;
    let screen = screen_before_end.screen();
    assert!(screen.contains("lost the daemon"), "{screen}");
    assert!(
        screen.contains("NO DAEMON"),
        "the screen says it is stale:\n{screen}"
    );
    match &outcome.how {
        How::Disconnected(error) => {
            assert!(
                error.is_transient() || matches!(error, ClientError::Reconnect { .. }),
                "{error}"
            );
        }
        How::Quit => panic!("expected a disconnect"),
    }
    assert_eq!(outcome.exit_code(), exit::FAILED);
    server.abort();
    Ok(())
}

#[tokio::test]
async fn the_screen_keeps_updating_while_an_action_is_in_flight() -> TestResult {
    // A cancel of a sleeping worker takes a moment (the process is stopped and
    // reaped); while it does, the footer says what is happening and keys still work.
    let mut h = Harness::start().await?;
    let mut client = h.client().await?;
    let run = client.run_start(h.sleeper("31339")).await?;
    wait_for_status(&mut client, run, FactoryStatus::Running).await?;
    let watcher = Watcher::spawn(&h, run, 100, 30)?;
    watcher.contains("RUNNING").await;
    watcher.ch('c').await;
    watcher.type_text("cancel").await;
    watcher.press(KeyCode::Enter).await;
    // Whether or not the daemon is quick, help must open at once and close again.
    watcher.ch('?').await;
    watcher.contains("Keys").await;
    watcher.ch('x').await;
    watcher
        .until("the run to end", |s| s.contains("run ended"))
        .await;
    let outcome = watcher.quit().await;
    assert_eq!(outcome.state.status_label(), "cancelled");
    h.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn json_events_with_new_fields_do_not_disturb_the_watcher() -> TestResult {
    // A daemon newer than this TUI adds fields to payloads; the fold ignores them.
    let mut state = WatchState::new();
    let run = RunId::new();
    for (seq, kind, payload) in [
        (
            1,
            EventKind::RunCreated,
            json!({"repo": "/r", "brand_new_field": [1, 2, 3]}),
        ),
        (
            2,
            EventKind::StateChanged,
            json!({"from": "queued", "to": "routing", "extra": {"a": null}}),
        ),
        (
            3,
            EventKind::DirectiveSelected,
            json!({"directive": "continue", "future": true}),
        ),
    ] {
        state.apply(&EventEnvelope::new(
            EventSeq(seq),
            run,
            time::OffsetDateTime::now_utc(),
            kind,
            payload,
        ));
    }
    assert_eq!(state.last_seq, 3);
    assert_eq!(state.status_label(), "routing");
    Ok(())
}
