//! The M4 exit criteria, against a real daemon, a real socket, and real HTTP.
//!
//! Offline throughout (`AGENTS.md` hard rule 4): a simulated worker, a scripted model, no
//! network, no credentials, no agent binary. The client is raw HTTP over `TcpStream` rather
//! than a library, so the test exercises the bytes a browser and `curl` would send — and so
//! the SSE test can cut the connection at an exact sequence number.
//!
//! The four criteria ([09-plan.md](../../../docs/09-plan.md) M4):
//!
//! 1. an SSE reconnect during a run produces no gaps;
//! 2. binding `0.0.0.0` without a token fails to start;
//! 3. every check value is visible with its threshold and margin;
//! 4. an escalation can be answered from the browser and the run continues.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use aftman_core::assessment::AnswerValue;
use aftman_core::clock::SystemClock;
use aftman_core::definitions::DefinitionSet;
use aftman_core::error::ModelError;
use aftman_core::evidence::EvidencePlan;
use aftman_core::ids::CheckKey;
use aftman_core::job::{Job, JobSource, RunConfig, WorkerKind};
use aftman_core::model::{AskContext, Budget, DecisionModel, ModelAnswers, Question, Usage};
use aftman_core::responsibility::ResponsibilityRegistry;
use aftman_core::testing::SimulatedModel;
use aftman_core::worker::{
    SteerOutcome, TerminateReason, Worker, WorkerCtx, WorkerError, WorkerEventSink, WorkerOutcome,
};
use aftman_daemon::protocol::{
    ErrorCode, JevUsageParams, ProtocolError, ResponsibilityInfo, ResponsibilityListResult,
    RunStartParams, SubscriptionCloseResult,
};
use aftman_daemon::{
    BuiltRun, EventHub, FactoryRuntime, HubConfig, ManagerConfig, RunManager, RuntimeContext,
    RuntimeDeps, RuntimeFactory,
};
use aftman_store::{FileRunStore, RunStore};
use aftman_web::{AuthMode, Web, WebConfig};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const PATIENCE: Duration = Duration::from_secs(20);

// ---------------------------------------------------------------------------
// Doubles
// ---------------------------------------------------------------------------

/// A model that is always uncertain: every value lands inside the margin, so policy can
/// neither finish nor stop the run and has to ask a person.
struct UncertainModel;

#[async_trait]
impl DecisionModel for UncertainModel {
    async fn ask(
        &self,
        _context: &AskContext,
        _state: &Value,
        questions: &BTreeMap<CheckKey, Question>,
        _budget: &Budget,
    ) -> Result<ModelAnswers, ModelError> {
        let mut answers = BTreeMap::new();
        for (key, question) in questions {
            // A value exactly at the threshold is the most uncertain reading there is.
            let value = match question {
                Question::Noul { .. } => AnswerValue::Noul(0.5),
                Question::Choice { criteria, .. } => {
                    let chosen = criteria.keys().next().cloned().unwrap_or_default();
                    AnswerValue::Choice {
                        choice: chosen.clone(),
                        probabilities: criteria.keys().map(|name| (name.clone(), 0.5)).collect(),
                        confidence: 0.5,
                    }
                }
                Question::Score { criteria, .. } => AnswerValue::Score {
                    score: 0.5,
                    level: if criteria.is_empty() { None } else { Some(0) },
                    confidence: 0.5,
                    probabilities: criteria
                        .iter()
                        .enumerate()
                        .map(|(index, _)| (u32::try_from(index).unwrap_or(0), 0.5))
                        .collect(),
                },
            };
            answers.insert(key.clone(), value);
        }
        Ok(ModelAnswers {
            model: "uncertain-test".to_owned(),
            usage: Usage::default(),
            answers,
            request_id: None,
            attempts: 1,
        })
    }
}

/// A worker that emits output over time, so a run lasts long enough for a test to cut an SSE
/// connection in the middle of it.
struct ChattyWorker {
    chunks: usize,
    delay: Duration,
}

#[async_trait]
impl Worker for ChattyWorker {
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
        for index in 0..self.chunks {
            {
                let mut output = ctx
                    .output
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                output.push(
                    aftman_core::log::Stream::Stdout,
                    &format!("chunk {index} of the worker's output\n"),
                );
            }
            sink.activity();
            tokio::time::sleep(self.delay).await;
        }
        Ok(WorkerOutcome::Completed)
    }

    async fn steer(&self, _message: &str) -> Result<SteerOutcome, WorkerError> {
        Ok(SteerOutcome::Queued)
    }

    async fn terminate(&self, _reason: TerminateReason) -> Result<(), WorkerError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// A daemon with the web surface in front of it
// ---------------------------------------------------------------------------

struct Factory {
    scratch: PathBuf,
}

#[async_trait]
impl RuntimeFactory for Factory {
    async fn build(
        &self,
        params: &RunStartParams,
        context: RuntimeContext,
    ) -> Result<BuiltRun, ProtocolError> {
        // `model = "escalate"` builds an uncertain model and a chatty worker; anything else
        // builds the simulated pair that finishes on its own.
        let (model, worker, config): (Arc<dyn DecisionModel>, Arc<dyn Worker>, RunConfig) =
            if params.model.as_deref() == Some("escalate") {
                (
                    Arc::new(UncertainModel),
                    Arc::new(ChattyWorker {
                        chunks: 6,
                        delay: Duration::from_millis(40),
                    }),
                    RunConfig {
                        min_assessment_interval: Duration::from_millis(5),
                        idle_poll_interval: Duration::from_millis(15),
                        ..RunConfig::default()
                    },
                )
            } else {
                (
                    Arc::new(SimulatedModel::new()),
                    Arc::new(aftman_workers::SimulatedWorker::new()),
                    RunConfig {
                        worker: WorkerKind::Simulated,
                        min_assessment_interval: Duration::from_millis(1),
                        idle_poll_interval: Duration::from_millis(10),
                        ..RunConfig::default()
                    },
                )
            };
        let runtime = FactoryRuntime::new(RuntimeDeps {
            config,
            store: Arc::clone(&context.store),
            model,
            worker,
            registry: ResponsibilityRegistry::builtin(),
            definitions: definitions(),
            evidence: EvidencePlan::default(),
            evidence_temp_dir: self.scratch.join("evidence"),
            clock: Arc::new(SystemClock),
            sink: Arc::clone(&context.sink),
            ensemble_enabled: false,
            uncertainty_margin: aftman_core::assessment::DEFAULT_UNCERTAINTY_MARGIN,
            steering_grace: Duration::from_secs(5),
            launch: Default::default(),
            forwarded_env: Default::default(),
            jev_budget: Default::default(),
        });
        Ok(BuiltRun {
            runtime: Arc::new(runtime),
            job: Job {
                prompt: format!("a job for {} (test)", params.repo.display()),
                repo: params.repo.clone(),
                source: JobSource::Cli,
            },
        })
    }

    async fn responsibilities(&self) -> Result<ResponsibilityListResult, ProtocolError> {
        Ok(ResponsibilityListResult {
            responsibilities: vec![ResponsibilityInfo {
                id: "core".to_owned(),
                description: "the always-on core responsibilities".to_owned(),
                checks: Vec::new(),
            }],
        })
    }

    async fn usage(&self, _params: &JevUsageParams) -> Result<Value, ProtocolError> {
        Ok(json!({ "requests": 0, "input_tokens": 0, "output_tokens": 0 }))
    }
}

fn definitions() -> DefinitionSet {
    DefinitionSet::bundled().expect("the bundled definitions are compiled in and valid")
}

struct Harness {
    dir: tempfile::TempDir,
    repo: PathBuf,
    manager: Arc<RunManager>,
    addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(task) = self.task.take() {
            task.abort();
        }
        // A run can still be landing a snapshot in a blocking task. Retry like the daemon
        // tests do, so a leftover `/tmp` directory is not the failure a later run trips on.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            match std::fs::remove_dir_all(self.dir.path()) {
                Ok(()) => break,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                Err(_) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20));
                }
                Err(_) => break,
            }
        }
    }
}

impl Harness {
    /// Start a daemon with the web surface bound to an ephemeral loopback port.
    async fn start(
        auth: AuthMode,
        token: Option<&str>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let dir = tempfile::Builder::new()
            .prefix("aftweb")
            .tempdir_in("/tmp")?;
        let repo = dir.path().join("repo");
        std::fs::create_dir_all(repo.join(".git"))?;
        std::fs::write(repo.join("README.md"), "# test\n")?;
        let store: Arc<dyn RunStore> = Arc::new(FileRunStore::new(dir.path().join("state")));
        let hub = EventHub::new(Arc::clone(&store), HubConfig::default());
        let factory: Arc<dyn RuntimeFactory> = Arc::new(Factory {
            scratch: dir.path().join("scratch"),
        });
        let manager = RunManager::new(
            Arc::clone(&store),
            hub,
            factory,
            Arc::new(SystemClock),
            ManagerConfig::default(),
        );

        let home = dir.path().to_path_buf();
        let config = WebConfig::from_parts(
            aftman_web::config::WebParts {
                bind: "127.0.0.1:0".parse()?,
                auth,
                token: token.map(ToOwned::to_owned),
                allowed_repo_roots: vec![home.clone()],
                ..aftman_web::config::WebParts::default()
            },
            Some(&home),
        )?;
        // Port 0 in the configuration is what makes the bind ephemeral; the listener reports
        // the real port.
        let listener = tokio::net::TcpListener::bind(config.bind()).await?;
        let addr = listener.local_addr()?;
        let web = Arc::new(Web::prepare(config)?);
        let shutdown = Arc::new(AtomicBool::new(false));

        let router_web = Arc::clone(&web);
        let router_manager = Arc::clone(&manager);
        let flag = Arc::clone(&shutdown);
        let task = tokio::spawn(async move {
            let app = router_web.router(router_manager);
            let _ = axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async move {
                while !flag.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await;
        });

        Ok(Self {
            dir,
            repo,
            manager,
            addr,
            shutdown,
            task: Some(task),
        })
    }

    async fn default_harness() -> Result<Self, Box<dyn std::error::Error>> {
        Self::start(AuthMode::Loopback, None).await
    }

    async fn start_run(
        &self,
        model: Option<&str>,
    ) -> Result<aftman_core::ids::RunId, Box<dyn std::error::Error>> {
        let mut params = RunStartParams::new(self.repo.clone(), "do the thing");
        params.model = model.map(ToOwned::to_owned);
        let started = self.manager.run_start(params).await?;
        Ok(started.run_id)
    }

    /// Wait until the run's status satisfies `predicate`.
    async fn wait_status(
        &self,
        run: aftman_core::ids::RunId,
        what: &str,
        predicate: impl Fn(aftman_core::job::FactoryStatus) -> bool,
    ) -> Result<aftman_core::job::FactoryStatus, Box<dyn std::error::Error>> {
        let deadline = tokio::time::Instant::now() + PATIENCE;
        loop {
            let state = self
                .manager
                .run_get(aftman_daemon::protocol::RunIdParams { run_id: run })
                .await?;
            if predicate(state.status) {
                return Ok(state.status);
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {what}; status stayed {}",
                state.status.as_str()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn shutdown_manager(&self) {
        self.manager.shutdown().await;
    }
}

// ---------------------------------------------------------------------------
// A raw HTTP client
// ---------------------------------------------------------------------------

struct Response {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
}

impl Response {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

async fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<&str>,
) -> Result<Response, Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect(addr).await?;
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nAccept: text/html\r\n"
    );
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    if let Some(body) = body {
        request.push_str("Content-Type: application/x-www-form-urlencoded\r\n");
        request.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    request.push_str("\r\n");
    if let Some(body) = body {
        request.push_str(body);
    }
    stream.write_all(request.as_bytes()).await?;
    stream.flush().await?;

    let mut reader = BufReader::new(stream);
    let mut status_line = String::new();
    reader.read_line(&mut status_line).await?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or(0);
    let mut headers_out = Vec::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).await? == 0 {
            break;
        }
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            headers_out.push((name.trim().to_owned(), value.trim().to_owned()));
        }
    }
    let mut body = String::new();
    let mut chunk = String::new();
    while reader.read_line(&mut chunk).await? > 0 {
        body.push_str(&chunk);
        chunk.clear();
    }
    // `Connection: close` means a plain body, not chunked framing we have to unframe.
    Ok(Response {
        status,
        headers: headers_out,
        body,
    })
}

/// One SSE event as the wire carries it.
#[derive(Debug, Clone)]
struct SseEvent {
    id: Option<u64>,
    kind: String,
    data: Value,
}

/// Read SSE events for at most `want` events or `budget`, then drop the connection.
///
/// Dropping the stream is the point: it is what a browser does when a laptop sleeps, and the
/// test then reconnects with `Last-Event-ID` exactly as the browser would.
async fn read_sse(
    addr: SocketAddr,
    run: &str,
    last_event_id: Option<u64>,
    want: usize,
    budget: Duration,
) -> Result<Vec<SseEvent>, Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect(addr).await?;
    let mut request = format!(
        "GET /v1/runs/{run}/events HTTP/1.1\r\nHost: {addr}\r\n\
         Accept: text/event-stream\r\nConnection: close\r\n"
    );
    if let Some(id) = last_event_id {
        request.push_str(&format!("Last-Event-ID: {id}\r\n"));
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).await?;
    stream.flush().await?;

    let mut reader = BufReader::new(stream);
    let mut status_line = String::new();
    reader.read_line(&mut status_line).await?;
    assert!(
        status_line.contains(" 200 "),
        "SSE must be accepted, got {status_line:?}"
    );
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).await? == 0 {
            break;
        }
        if line == "\r\n" {
            break;
        }
    }

    let mut events = Vec::new();
    let mut current = SseEvent {
        id: None,
        kind: String::new(),
        data: Value::Null,
    };
    // A keep-alive is a comment line (`: ...`) followed by a blank line, so an empty frame
    // is not an event. Without this the keep-alive arrives as a `null` data value.
    let mut has_data = false;
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        if events.len() >= want {
            break;
        }
        let mut line = String::new();
        let read = tokio::time::timeout_at(deadline, reader.read_line(&mut line)).await;
        let Ok(read) = read else {
            break; // budget spent: return what we have
        };
        if read? == 0 {
            break;
        }
        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            // End of one frame. A frame with no `data:` line is a keep-alive, not an event.
            if !has_data {
                current = SseEvent {
                    id: None,
                    kind: String::new(),
                    data: Value::Null,
                };
                continue;
            }
            if let Ok(data) = serde_json::from_str::<Value>(&current.data_str()) {
                current.data = data;
                if current.kind.is_empty() {
                    current.kind = "message".to_owned();
                }
                let lagged = current.kind == "lagged";
                events.push(current.clone());
                current = SseEvent {
                    id: None,
                    kind: String::new(),
                    data: Value::Null,
                };
                has_data = false;
                if lagged {
                    break;
                }
            }
            continue;
        }
        if let Some(rest) = line.strip_prefix("id: ") {
            current.id = rest.trim().parse().ok();
        } else if let Some(rest) = line.strip_prefix("event: ") {
            current.kind = rest.trim().to_owned();
        } else if let Some(rest) = line.strip_prefix("data: ") {
            current.data = Value::String(rest.to_owned());
            has_data = true;
        }
    }
    Ok(events)
}

impl SseEvent {
    fn data_str(&self) -> String {
        match &self.data {
            Value::String(text) => text.clone(),
            other => other.to_string(),
        }
    }
}

fn form(fields: &[(&str, &str)]) -> String {
    fields
        .iter()
        .map(|(key, value)| format!("{}={}", key, urlencode(value)))
        .collect::<Vec<_>>()
        .join("&")
}

fn urlencode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(char::from(*byte));
            }
            b' ' => out.push('+'),
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Exit criterion 2: binding 0.0.0.0 without a token fails to start
// ---------------------------------------------------------------------------

#[tokio::test]
async fn binding_a_non_loopback_address_without_a_token_fails_to_start() -> TestResult {
    let home = Path::new("/home/tester");
    let error = WebConfig::from_parts(
        aftman_web::config::WebParts {
            bind: "0.0.0.0:8787".parse()?,
            allowed_repo_roots: vec![home.to_path_buf()],
            ..aftman_web::config::WebParts::default()
        },
        Some(home),
    )
    .expect_err("a LAN bind with the default auth must be refused, not warned about");
    assert!(
        error.to_string().contains("web.auth = \"token\""),
        "the message must say how to fix it: {error}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Exit criterion 1: an SSE reconnect during a run produces no gaps
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_sse_reconnect_mid_run_resumes_with_no_gap_and_no_duplicate() -> TestResult {
    let harness = Harness::default_harness().await?;
    let run = harness.start_run(Some("escalate")).await?;
    let run_text = run.to_string();

    // Read the first few events, then drop the connection as a sleeping laptop would.
    let first = read_sse(harness.addr, &run_text, None, 4, Duration::from_secs(10)).await?;
    assert!(
        !first.is_empty(),
        "the run must produce events for the test to cut"
    );
    let last_seen = first
        .iter()
        .filter_map(|event| event.id)
        .next_back()
        .expect("every log event carries a sequence id");

    // Reconnect from the last id we saw, exactly as the browser's EventSource would.
    let second = read_sse(
        harness.addr,
        &run_text,
        Some(last_seen),
        40,
        Duration::from_secs(10),
    )
    .await?;

    let mut seen: Vec<u64> = first.iter().filter_map(|event| event.id).collect();
    seen.extend(second.iter().filter_map(|event| event.id));

    // The claim: the union is a contiguous run of sequence numbers with no gap and no
    // repeat. A gap here is the failure the whole resume design exists to prevent.
    for pair in seen.windows(2) {
        assert_eq!(
            pair[1],
            pair[0] + 1,
            "sequence numbers must be contiguous across the reconnect: {seen:?}"
        );
    }
    // ...and the reconnect resumed exactly after what we had already seen, rather than
    // replaying from the start.
    if let Some(first_of_second) = second.iter().filter_map(|event| event.id).next() {
        assert_eq!(
            first_of_second,
            last_seen + 1,
            "the resume must start at the first unseen event"
        );
    }
    // The event bodies are the log's, not placeholders.
    for event in &second {
        assert!(
            event.data.get("kind").is_some() && event.data.get("seq").is_some(),
            "an SSE frame must carry kind and seq: {}",
            event.data
        );
    }
    harness.shutdown_manager().await;
    Ok(())
}

// ---------------------------------------------------------------------------
// Exit criterion 3: every check value is visible with its threshold and margin
// ---------------------------------------------------------------------------

#[tokio::test]
async fn every_check_value_shows_its_threshold_and_margin() -> TestResult {
    let harness = Harness::default_harness().await?;
    let run = harness.start_run(None).await?;
    harness
        .wait_status(run, "an assessment", |status| {
            status == aftman_core::job::FactoryStatus::Finished
        })
        .await?;

    // Read the truth from the store, then require the page to show it. Comparing against
    // the store rather than against hard-coded numbers is what makes this a test of the
    // page instead of a test of the fixtures.
    let state = harness
        .manager
        .run_get(aftman_daemon::protocol::RunIdParams { run_id: run })
        .await?;
    assert!(
        !state.checks.is_empty(),
        "the run must have assessed checks"
    );

    let page = request(harness.addr, "GET", &format!("/runs/{run}"), &[], None).await?;
    assert_eq!(page.status, 200, "the run page must render: {}", page.body);

    for check in state.checks.values() {
        let key = check.check.as_str();
        assert!(
            page.body.contains(&escape_html(key)),
            "the page must show the check {key}"
        );
        // The margin is the exit criterion: a value without the margin it was judged
        // against cannot be read.
        let margin = format!("{:.3}", check.margin);
        assert!(
            page.body.contains(&margin),
            "the page must show the margin {margin} for {key}"
        );
        if let Some(threshold) = check.threshold {
            let expected = format!("{threshold:.3}");
            assert!(
                page.body.contains(&expected),
                "the page must show the threshold {expected} for {key}"
            );
        }
        if let (Some(p), Some(t)) = (check.jev_p, check.threshold) {
            let distance = format!("{:.3}", (p - t).abs());
            assert!(
                page.body.contains(&distance),
                "the page must show |p-t| = {distance} for {key}"
            );
        }
    }
    // The margin in the page footer is the one the checks were resolved against, not the
    // daemon's current setting.
    let expected_margin = format!(
        "{:.3}",
        state.checks.values().next().expect("non-empty").margin
    );
    let decisions = request(
        harness.addr,
        "GET",
        &format!("/runs/{run}/decisions"),
        &[],
        None,
    )
    .await?;
    assert!(
        decisions.body.contains(&expected_margin),
        "the decisions page must state the margin in force ({expected_margin})"
    );

    // A page is untrusted output; a worker's text must not arrive as markup.
    assert!(
        !page.body.contains("<script>alert"),
        "no page may echo unescaped worker output"
    );
    harness.shutdown_manager().await;
    Ok(())
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#x27;")
}

// ---------------------------------------------------------------------------
// Exit criterion 4: an escalation can be answered from the browser, and the run continues
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_escalation_answered_from_the_browser_lets_the_run_continue() -> TestResult {
    let harness = Harness::default_harness().await?;
    let run = harness.start_run(Some("escalate")).await?;

    let needs_human = harness
        .wait_status(run, "an escalation", |status| {
            status == aftman_core::job::FactoryStatus::NeedsHuman
        })
        .await?;
    assert_eq!(needs_human, aftman_core::job::FactoryStatus::NeedsHuman);

    let state = harness
        .manager
        .run_get(aftman_daemon::protocol::RunIdParams { run_id: run })
        .await?;
    let escalation = state
        .escalations
        .iter()
        .find(|escalation| escalation.answered.is_none())
        .expect("a run in NeedsHuman has an open escalation")
        .id
        .to_string();

    // The escalation is visible on the page as an answerable form.
    let page = request(harness.addr, "GET", &format!("/runs/{run}"), &[], None).await?;
    assert_eq!(page.status, 200);
    assert!(
        page.body.contains("Needs a human"),
        "an open escalation must be prominent"
    );
    assert!(
        page.body.contains(&escape_html(&escalation)),
        "the form must carry the escalation id"
    );
    assert!(
        page.body.contains(&format!("/runs/{run}/answer")),
        "the form must post to the answer endpoint"
    );

    // Answer it the way the form does.
    let body = form(&[
        ("escalation_id", &escalation),
        ("decision", "accept"),
        ("note", "checked by hand"),
    ]);
    let answered = request(
        harness.addr,
        "POST",
        &format!("/runs/{run}/answer"),
        &[("Content-Type", "application/x-www-form-urlencoded")],
        Some(&body),
    )
    .await?;
    // A form post redirects back to the run on success.
    assert!(
        (300..400).contains(&answered.status),
        "the answer must redirect, got {} {}",
        answered.status,
        answered.body
    );

    // ...and the run continues: it leaves NeedsHuman on its own.
    let after = harness
        .wait_status(run, "the run to leave NeedsHuman", |status| {
            status != aftman_core::job::FactoryStatus::NeedsHuman
        })
        .await?;
    assert_ne!(after, aftman_core::job::FactoryStatus::NeedsHuman);

    // The answer is recorded, with the surface that gave it.
    let state = harness
        .manager
        .run_get(aftman_daemon::protocol::RunIdParams { run_id: run })
        .await?;
    let recorded = state
        .escalations
        .iter()
        .find(|candidate| candidate.id.to_string() == escalation)
        .expect("the escalation is still on the run")
        .answered
        .as_ref()
        .expect("the escalation is answered");
    assert_eq!(recorded.surface, "web", "the answer came from the browser");
    harness.shutdown_manager().await;
    Ok(())
}

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_token_surface_refuses_an_unauthenticated_client_and_accepts_the_token() -> TestResult {
    let harness = Harness::start(AuthMode::Token, Some("correct-horse")).await?;
    let run = harness.start_run(None).await?;

    let denied = request(harness.addr, "GET", &format!("/v1/runs/{run}"), &[], None).await?;
    assert_eq!(denied.status, 401, "no token must be 401");
    assert!(
        denied
            .header("www-authenticate")
            .is_some_and(|value| value.contains("Bearer")),
        "a 401 must name the scheme"
    );

    let allowed = request(
        harness.addr,
        "GET",
        &format!("/v1/runs/{run}"),
        &[("Authorization", "Bearer correct-horse")],
        None,
    )
    .await?;
    assert_eq!(allowed.status, 200, "the right token must be accepted");
    let parsed: Value = serde_json::from_str(&allowed.body)?;
    assert_eq!(
        parsed["header"]["id"].as_str().unwrap_or_default(),
        run.to_string()
    );

    // A browser logging in with `?token=` gets a cookie instead.
    let login = request(harness.addr, "GET", "/?token=correct-horse", &[], None).await?;
    assert!(
        (300..400).contains(&login.status),
        "the login flow must redirect"
    );
    let cookie = login.header("set-cookie").expect("a cookie is set");
    assert!(cookie.contains("HttpOnly"), "{cookie}");
    assert!(cookie.contains("SameSite=Strict"), "{cookie}");

    // ...and the cookie authenticates a page.
    let page = request(harness.addr, "GET", "/", &[("Cookie", cookie)], None).await?;
    assert_eq!(page.status, 200, "the cookie must authenticate the page");
    harness.shutdown_manager().await;
    Ok(())
}

#[tokio::test]
async fn a_dns_rebinding_host_header_is_refused() -> TestResult {
    // The peer is loopback in a rebinding attack too, so the Host header is the guard.
    let harness = Harness::default_harness().await?;
    let mut stream = TcpStream::connect(harness.addr).await?;
    let request = "GET /v1/status HTTP/1.1\r\nHost: evil.example:8787\r\nConnection: close\r\n\r\n";
    let request = request.to_owned();
    stream.write_all(request.as_bytes()).await?;
    let mut reader = BufReader::new(stream);
    let mut status_line = String::new();
    reader.read_line(&mut status_line).await?;
    assert!(
        status_line.contains(" 421 "),
        "an unknown Host must be refused, got {status_line:?}"
    );
    harness.shutdown_manager().await;
    Ok(())
}

// ---------------------------------------------------------------------------
// The repository allow-list
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_repo_outside_the_allowed_roots_is_refused_before_any_worker_starts() -> TestResult {
    let harness = Harness::default_harness().await?;
    // A directory that exists and is a git work tree, but is not under the configured root.
    let outside = tempfile::Builder::new()
        .prefix("aftout")
        .tempdir_in("/tmp")?;
    std::fs::create_dir_all(outside.path().join(".git"))?;

    let body = json!({
        "repo": outside.path().display().to_string(),
        "job": "read something you should not",
    })
    .to_string();
    let refused = request(
        harness.addr,
        "POST",
        "/v1/runs",
        &[("Content-Type", "application/json")],
        Some(&body),
    )
    .await?;
    assert_eq!(
        refused.status, 400,
        "a repo outside every root must be refused: {}",
        refused.body
    );
    let parsed: Value = serde_json::from_str(&refused.body)?;
    assert_eq!(parsed["error"]["code"], "invalid_repo");
    assert!(
        parsed["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("allowed_repo_roots"),
        "the refusal must name the setting: {parsed}"
    );
    // Nothing was started.
    let list = harness
        .manager
        .run_list(aftman_daemon::protocol::RunListParams::default())
        .await?;
    assert!(list.runs.is_empty(), "no run may be created");
    harness.shutdown_manager().await;
    Ok(())
}

// ---------------------------------------------------------------------------
// The API mirrors the protocol rather than reimplementing it
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_json_api_reports_the_protocols_own_error_codes() -> TestResult {
    let harness = Harness::default_harness().await?;
    let missing = request(
        harness.addr,
        "GET",
        "/v1/runs/01a1581c-67b4-71a7-a05b-9ceaa3f89627",
        &[],
        None,
    )
    .await?;
    assert_eq!(missing.status, 404);
    let parsed: Value = serde_json::from_str(&missing.body)?;
    assert_eq!(
        parsed["error"]["code"], "unknown_run",
        "the manager's own code must reach the client unchanged: {parsed}"
    );
    assert_eq!(
        parsed["error"]["retryable"], false,
        "retryability comes from the protocol, not the transport"
    );

    // A status the daemon does not know is a 400 rather than an empty list.
    let bad = request(harness.addr, "GET", "/v1/runs?status=nonsense", &[], None).await?;
    assert_eq!(bad.status, 400);
    harness.shutdown_manager().await;
    Ok(())
}

#[tokio::test]
async fn the_web_config_page_reports_the_posture_without_the_token() -> TestResult {
    let harness = Harness::start(AuthMode::Token, Some("s3cret-token")).await?;
    let described = request(
        harness.addr,
        "GET",
        "/v1/web",
        &[("Authorization", "Bearer s3cret-token")],
        None,
    )
    .await?;
    assert_eq!(described.status, 200);
    assert!(
        !described.body.contains("s3cret-token"),
        "the token must never be rendered: {}",
        described.body
    );
    let parsed: Value = serde_json::from_str(&described.body)?;
    assert_eq!(parsed["auth"], "token");
    assert_eq!(parsed["token"], "configured");
    harness.shutdown_manager().await;
    Ok(())
}

// ---------------------------------------------------------------------------
// Unused-import guard
// ---------------------------------------------------------------------------

/// `SubscriptionCloseResult` and `ErrorCode` are part of the protocol surface a web client
/// needs; referencing them here keeps the import honest without exercising the socket path.
const _: fn() = || {
    let _ = std::any::TypeId::of::<SubscriptionCloseResult>();
    let _ = ErrorCode::UnknownRun;
    let _ = Web::token_was_generated;
};
