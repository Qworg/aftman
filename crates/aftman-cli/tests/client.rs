//! The typed client against an in-process daemon (the real `CliFactory`, simulated
//! parts) and against scripted fake servers.
//!
//! Offline: no network, no real agent, no credentials, no environment variables. The
//! only subprocesses are `sh` and `true`, standing in for a worker that sleeps or
//! exits, behind the `generic` backend.

use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use aftman_cli::client::{Client, ClientConfig, ClientError, Follow};
use aftman_cli::factory::{CliFactory, FactoryInputs, Mode};
use aftman_core::assessment::HumanDecision;
use aftman_core::clock::SystemClock;
use aftman_core::event::{EventEnvelope, EventKind};
use aftman_core::ids::{EscalationId, EventSeq, RunId};
use aftman_core::job::FactoryStatus;
use aftman_daemon::protocol::{
    ErrorCode, JevUsageParams, ProtocolError, Request, Response, RunEventsParams, RunListParams,
    RunPurgeParams, RunStartParams, ServerMessage, SubscriptionOpenParams, SubscriptionOpenResult,
    decode_request, encode_line,
};
use aftman_daemon::{Daemon, DaemonConfig, DaemonPaths};
use aftman_store::{FileRunStore, RunStore};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const PATIENCE: Duration = Duration::from_secs(20);

fn short_tempdir() -> Result<tempfile::TempDir, std::io::Error> {
    tempfile::Builder::new().prefix("aft").tempdir_in("/tmp")
}

fn fast() -> ClientConfig {
    ClientConfig {
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_millis(400),
        reconnect_attempts: 4,
        reconnect_backoff: Duration::from_millis(10),
        reconnect_max_backoff: Duration::from_millis(40),
    }
}

/// Every wait in these tests has a deadline.
async fn within<T>(what: &str, future: impl Future<Output = T>) -> T {
    match tokio::time::timeout(PATIENCE, future).await {
        Ok(value) => value,
        Err(_) => panic!("timed out waiting for {what}"),
    }
}

// ===========================================================================
// An in-process daemon built by the real factory
// ===========================================================================

struct Daemonized {
    _dir: tempfile::TempDir,
    paths: DaemonPaths,
    repo: PathBuf,
    store: Arc<FileRunStore>,
    task: Option<JoinHandle<()>>,
    stop: Option<oneshot::Sender<()>>,
}

impl Daemonized {
    async fn start() -> Result<Self, Box<dyn std::error::Error>> {
        let dir = short_tempdir()?;
        let repo = dir.path().join("repo");
        std::fs::create_dir_all(&repo)?;
        let state = dir.path().join("state");
        let paths = DaemonPaths::in_dir(&dir.path().join("run"));
        let store = Arc::new(FileRunStore::new(&state));
        let mut config = DaemonConfig::new(paths.clone());
        config.state_dir = Some(state.clone());
        let daemon = Daemon::start(
            config,
            store.clone(),
            Arc::new(CliFactory::new(
                &state,
                FactoryInputs::empty(),
                Mode::Daemon,
            )),
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
            store,
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

    /// A worker that sleeps until cancelled, behind the generic backend.
    fn sleeper(&self, unique: &str) -> RunStartParams {
        RunStartParams {
            worker: Some("generic".into()),
            cmd: Some(format!("sh -c 'sleep {unique}' {{repo}}")),
            external_sandbox: true,
            simulate_model: true,
            ..RunStartParams::new(&self.repo, "wait")
        }
    }

    /// A worker that exits at once without doing the job, which ends in an escalation.
    fn escalating(&self) -> RunStartParams {
        RunStartParams {
            worker: Some("generic".into()),
            cmd: Some("true {repo}".into()),
            external_sandbox: true,
            simulate_model: true,
            ..RunStartParams::new(&self.repo, "do the impossible")
        }
    }

    async fn follow(&self, run: RunId) -> Result<Follow, ClientError> {
        Follow::open(
            &self.paths.socket,
            run,
            EventSeq::FIRST,
            ClientConfig::default(),
        )
        .await
    }

    async fn drain(&self, run: RunId) -> Result<Vec<EventEnvelope>, Box<dyn std::error::Error>> {
        let mut follow = self.follow(run).await?;
        let mut events = Vec::new();
        within("the run to finish", async {
            while let Some(event) = follow.next().await? {
                events.push(event);
            }
            Ok::<(), ClientError>(())
        })
        .await?;
        Ok(events)
    }

    async fn shutdown(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = within("the daemon to stop", task).await;
        }
    }
}

fn seqs(events: &[EventEnvelope]) -> Vec<u64> {
    events.iter().map(|e| e.seq.get()).collect()
}

fn finished_status(events: &[EventEnvelope]) -> Option<String> {
    events
        .iter()
        .find(|e| e.kind == EventKind::RunFinished)
        .and_then(|e| e.payload["status"].as_str().map(ToOwned::to_owned))
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
// Every method
// ===========================================================================

#[tokio::test]
async fn every_method_returns_its_typed_result() -> TestResult {
    let mut d = Daemonized::start().await?;
    let mut client = d.client().await?;

    let status = client.status().await?;
    assert_eq!(status.runs_total, 0);
    assert_eq!(status.pid, std::process::id());

    // run.start + the follow stream
    let run = client.run_start(d.simulated()).await?;
    let events = d.drain(run).await?;
    let log = seqs(&events);
    assert_eq!(log, (1..=log.len() as u64).collect::<Vec<_>>());
    assert_eq!(finished_status(&events).as_deref(), Some("finished"));

    // run.get
    let state = client.run_get(run).await?;
    assert_eq!(state.header.id, run);

    // run.events, paged
    let first = client
        .run_events(RunEventsParams {
            run_id: run,
            from_seq: None,
            limit: Some(3),
        })
        .await?;
    assert_eq!(first.events.len(), 3);
    assert!(first.has_more);
    let all = client.all_events(run, EventSeq::FIRST).await?;
    assert_eq!(all, events, "paging gives back exactly the stream");

    // run.list, filtered, paged
    let finished = client
        .run_list(RunListParams {
            status: Some(vec![FactoryStatus::Finished]),
            ..RunListParams::default()
        })
        .await?;
    assert_eq!(finished.runs.len(), 1);
    let second = client.run_start(d.simulated()).await?;
    d.drain(second).await?;
    let page = client
        .run_list(RunListParams {
            limit: Some(1),
            ..RunListParams::default()
        })
        .await?;
    assert_eq!(page.runs.len(), 1);
    assert_eq!(page.runs[0].id, second, "newest first");
    assert_eq!(page.next_cursor, Some(second));
    assert_eq!(client.all_runs(None, 10).await?.len(), 2);

    // responsibility.list, jev.usage
    let responsibilities = client.responsibilities().await?;
    assert!(
        responsibilities
            .responsibilities
            .iter()
            .any(|r| r.id == "core.completion")
    );
    let usage = client.jev_usage(JevUsageParams::default()).await?;
    assert_eq!(usage["totals"]["requests"], 0);

    // The control methods refuse a run that has ended, with typed errors.
    let steer = client.run_steer(run, "go").await.unwrap_err();
    assert_eq!(
        steer.protocol().map(|e| e.code),
        Some(ErrorCode::WrongState)
    );
    let cancel = client.run_cancel(run).await.unwrap_err();
    assert_eq!(
        cancel.protocol().map(|e| e.code),
        Some(ErrorCode::WrongState)
    );
    let answer = client
        .run_answer(run, EscalationId::new(), HumanDecision::Accept, "")
        .await
        .unwrap_err();
    assert_eq!(
        answer.protocol().map(|e| e.code),
        Some(ErrorCode::WrongState)
    );
    let unknown = client.run_get(RunId::new()).await.unwrap_err();
    assert_eq!(
        unknown.protocol().map(|e| e.code),
        Some(ErrorCode::UnknownRun)
    );
    assert!(!unknown.protocol().is_some_and(|e| e.retryable));

    // A bad repository is invalid_repo, from the real factory.
    let bad = client
        .run_start(RunStartParams {
            worker: Some("simulated".into()),
            simulate_model: true,
            ..RunStartParams::new(d.repo.join("missing"), "x")
        })
        .await
        .unwrap_err();
    assert_eq!(bad.protocol().map(|e| e.code), Some(ErrorCode::InvalidRepo));
    let bad = client
        .run_start(RunStartParams {
            worker: Some("gpt-9".into()),
            ..d.simulated()
        })
        .await
        .unwrap_err();
    assert_eq!(
        bad.protocol().map(|e| e.code),
        Some(ErrorCode::InvalidConfig)
    );

    // subscription.open / subscription.close
    let opened = client
        .subscription_open(SubscriptionOpenParams {
            run_id: Some(run),
            from_seq: Some(EventSeq::FIRST),
        })
        .await?;
    assert_eq!(opened.run_id, Some(run));
    assert!(client.subscription_close().await?);
    assert!(!client.subscription_close().await?);

    // run.purge, one and all. `RUN_FINISHED` is announced a moment before the daemon
    // drops the run from its live table, so wait for idle first.
    within("the daemon to go idle", async {
        loop {
            if client.status().await?.runs_active == 0 {
                return Ok::<(), ClientError>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert_eq!(
        client
            .run_purge(RunPurgeParams {
                run_id: Some(run),
                all: false
            })
            .await?,
        1
    );
    assert_eq!(
        client
            .run_get(run)
            .await
            .unwrap_err()
            .protocol()
            .map(|e| e.code),
        Some(ErrorCode::UnknownRun)
    );
    assert_eq!(
        client
            .run_purge(RunPurgeParams {
                run_id: None,
                all: true
            })
            .await?,
        1
    );
    assert!(d.store.ids().await?.is_empty());

    // daemon.stop
    client.stop().await?;
    d.shutdown().await;
    assert!(!d.paths.socket.exists());
    Ok(())
}

#[tokio::test]
async fn a_worker_without_a_live_input_channel_reports_it_and_cancel_ends_the_run() -> TestResult {
    let mut d = Daemonized::start().await?;
    let mut client = d.client().await?;
    let run = client.run_start(d.sleeper("600.201")).await?;
    wait_for_status(&mut client, run, FactoryStatus::Running).await?;

    let steer = client.run_steer(run, "left").await.unwrap_err();
    assert_eq!(
        steer.protocol().map(|e| e.code),
        Some(ErrorCode::SteeringUnsupported),
        "{steer}"
    );

    let mut follow = d.follow(run).await?;
    client.run_cancel(run).await?;
    let mut last = None;
    within("RUN_FINISHED", async {
        while let Some(event) = follow.next().await? {
            last = Some(event);
        }
        Ok::<(), ClientError>(())
    })
    .await?;
    let last = last.ok_or("no events")?;
    assert_eq!(last.kind, EventKind::RunFinished);
    assert_eq!(last.payload["status"], "cancelled");
    d.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn an_escalation_waits_for_an_answer_and_a_stop_answer_ends_the_run() -> TestResult {
    let mut d = Daemonized::start().await?;
    let mut client = d.client().await?;
    let run = client.run_start(d.escalating()).await?;
    wait_for_status(&mut client, run, FactoryStatus::NeedsHuman).await?;

    let state = client.run_get(run).await?;
    let open = state
        .open_escalations()
        .next()
        .ok_or("an open escalation")?;
    let mut follow = d.follow(run).await?;
    client
        .run_answer(run, open.id, HumanDecision::Stop, "not worth it")
        .await?;
    let mut events = Vec::new();
    within("the run to end", async {
        while let Some(event) = follow.next().await? {
            events.push(event);
        }
        Ok::<(), ClientError>(())
    })
    .await?;
    assert!(
        events
            .iter()
            .any(|e| e.kind == EventKind::EscalationAnswered)
    );
    assert_ne!(finished_status(&events).as_deref(), Some("needs_human"));

    // The same escalation cannot be answered twice.
    let again = client
        .run_answer(run, open.id, HumanDecision::Accept, "")
        .await
        .unwrap_err();
    assert_eq!(
        again.protocol().map(|e| e.code),
        Some(ErrorCode::WrongState)
    );
    d.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn following_a_finished_run_replays_it_and_ends() -> TestResult {
    let mut d = Daemonized::start().await?;
    let mut client = d.client().await?;
    let run = client.run_start(d.simulated()).await?;
    let live = d.drain(run).await?;
    let replay = d.drain(run).await?;
    assert_eq!(live, replay);

    // From the middle: exactly the suffix.
    let mut follow =
        Follow::open(&d.paths.socket, run, EventSeq(5), ClientConfig::default()).await?;
    let first = follow.next().await?.ok_or("an event")?;
    assert_eq!(first.seq, EventSeq(5));
    assert_eq!(follow.next_seq(), EventSeq(6));
    d.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn an_unknown_run_is_refused_when_following() -> TestResult {
    let mut d = Daemonized::start().await?;
    let error = d.follow(RunId::new()).await.err().ok_or("an error")?;
    assert_eq!(
        error.protocol().map(|e| e.code),
        Some(ErrorCode::UnknownRun)
    );
    d.shutdown().await;
    Ok(())
}

// ===========================================================================
// A scripted fake server, for faults a real daemon will not produce on demand
// ===========================================================================

enum Step {
    /// Read a `subscription.open`, check its `from_seq`, and acknowledge it.
    ExpectOpen(u64),
    /// Read a `subscription.open` and answer it with this error.
    RejectOpen(ErrorCode),
    /// Push these event sequence numbers; the last is `RUN_FINISHED` when asked.
    Events(Vec<u64>, bool),
    /// The slow-subscriber push.
    Lagged,
    /// Drop the connection.
    Close,
    /// Say nothing, ever.
    Hang,
}

impl Step {
    fn events(range: RangeInclusive<u64>) -> Self {
        Self::Events(range.collect(), false)
    }
    fn finishing(range: RangeInclusive<u64>) -> Self {
        Self::Events(range.collect(), true)
    }
}

struct Fake {
    _dir: tempfile::TempDir,
    socket: PathBuf,
    run_id: RunId,
    opens: Arc<Mutex<Vec<u64>>>,
    failures: Arc<Mutex<Vec<String>>>,
    task: JoinHandle<()>,
}

impl Fake {
    fn start(scripts: Vec<Vec<Step>>) -> Result<Self, Box<dyn std::error::Error>> {
        let dir = short_tempdir()?;
        let socket = dir.path().join("f.sock");
        let listener = UnixListener::bind(&socket)?;
        let run_id = RunId::new();
        let opens = Arc::new(Mutex::new(Vec::new()));
        let failures = Arc::new(Mutex::new(Vec::new()));
        let (o, f) = (opens.clone(), failures.clone());
        let task = tokio::spawn(async move {
            let mut scripts = scripts.into_iter();
            let mut held: Vec<JoinHandle<()>> = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                let Some(script) = scripts.next() else {
                    drop(stream);
                    continue;
                };
                held.push(tokio::spawn(handle(
                    stream,
                    script,
                    run_id,
                    o.clone(),
                    f.clone(),
                )));
            }
        });
        Ok(Self {
            _dir: dir,
            socket,
            run_id,
            opens,
            failures,
            task,
        })
    }

    fn opens(&self) -> Vec<u64> {
        self.opens.lock().map(|o| o.clone()).unwrap_or_default()
    }

    fn assert_clean(&self) {
        let failures = self.failures.lock().map(|f| f.clone()).unwrap_or_default();
        assert!(failures.is_empty(), "fake server saw: {failures:?}");
    }

    async fn follow(&self, config: ClientConfig) -> Result<Follow, ClientError> {
        Follow::open(&self.socket, self.run_id, EventSeq::FIRST, config).await
    }
}

impl Drop for Fake {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn handle(
    stream: UnixStream,
    steps: Vec<Step>,
    run_id: RunId,
    opens: Arc<Mutex<Vec<u64>>>,
    failures: Arc<Mutex<Vec<String>>>,
) {
    let fail = |message: String| {
        if let Ok(mut f) = failures.lock() {
            f.push(message);
        }
    };
    let (read, mut write) = stream.into_split();
    let mut lines = BufReader::new(read).lines();
    for step in steps {
        let sent = match step {
            Step::ExpectOpen(want) => {
                let Ok(Some(line)) = lines.next_line().await else {
                    fail("connection closed before subscription.open".into());
                    return;
                };
                let Ok(envelope) = decode_request(&line) else {
                    fail(format!("undecodable request {line}"));
                    return;
                };
                let Request::SubscriptionOpen(p) = envelope.request else {
                    fail(format!("expected subscription.open, got {line}"));
                    return;
                };
                let from = p.from_seq.map_or(0, EventSeq::get);
                if let Ok(mut o) = opens.lock() {
                    o.push(from);
                }
                if from != want || p.run_id != Some(run_id) {
                    fail(format!("open from {from}, wanted {want}"));
                }
                let result = SubscriptionOpenResult {
                    run_id: Some(run_id),
                    from_seq: Some(EventSeq(from)),
                    replaced: false,
                    live: true,
                };
                match Response::ok(envelope.id, &result) {
                    Ok(response) => send(&mut write, &ServerMessage::Response(response)).await,
                    Err(_) => false,
                }
            }
            Step::RejectOpen(code) => {
                let Ok(Some(line)) = lines.next_line().await else {
                    return;
                };
                let Ok(envelope) = decode_request(&line) else {
                    return;
                };
                let response = Response::err(envelope.id, ProtocolError::new(code, "refused"));
                send(&mut write, &ServerMessage::Response(response)).await
            }
            Step::Events(list, finish) => {
                let mut ok = true;
                let last = list.last().copied();
                for seq in list {
                    let kind = if finish && Some(seq) == last {
                        EventKind::RunFinished
                    } else {
                        EventKind::WorkerOutput
                    };
                    let payload = if kind == EventKind::RunFinished {
                        json!({ "status": "finished" })
                    } else {
                        json!({ "text": format!("line {seq}\n") })
                    };
                    let event = EventEnvelope::new(
                        EventSeq(seq),
                        run_id,
                        time::OffsetDateTime::now_utc(),
                        kind,
                        payload,
                    );
                    ok &= send(&mut write, &ServerMessage::event(event)).await;
                }
                ok
            }
            Step::Lagged => send(&mut write, &ServerMessage::lagged(Some(run_id), None)).await,
            Step::Close => return,
            Step::Hang => {
                std::future::pending::<()>().await;
                true
            }
        };
        if !sent {
            return;
        }
    }
    // Script done: keep the connection open until the client goes away.
    let _ = lines.next_line().await;
}

async fn send(write: &mut tokio::net::unix::OwnedWriteHalf, message: &ServerMessage) -> bool {
    match encode_line(message) {
        Ok(line) => write.write_all(line.as_bytes()).await.is_ok(),
        Err(_) => false,
    }
}

async fn collect(follow: &mut Follow) -> Result<Vec<EventEnvelope>, ClientError> {
    let mut out = Vec::new();
    within("the stream to end", async {
        while let Some(event) = follow.next().await? {
            out.push(event);
        }
        Ok::<(), ClientError>(())
    })
    .await?;
    Ok(out)
}

#[tokio::test]
async fn a_lagged_push_reopens_from_the_next_unseen_seq() -> TestResult {
    let fake = Fake::start(vec![
        vec![Step::ExpectOpen(1), Step::events(1..=3), Step::Lagged],
        vec![Step::ExpectOpen(4), Step::finishing(4..=6)],
    ])?;
    let mut follow = fake.follow(fast()).await?;
    let events = collect(&mut follow).await?;
    assert_eq!(seqs(&events), [1, 2, 3, 4, 5, 6], "no gap, no duplicate");
    assert_eq!(follow.stats().lagged, 1);
    assert_eq!(
        follow.stats().reconnects,
        0,
        "a lag is not a lost connection"
    );
    assert_eq!(fake.opens(), [1, 4]);
    fake.assert_clean();
    Ok(())
}

#[tokio::test]
async fn a_killed_connection_is_restored_and_resumed_without_a_gap() -> TestResult {
    let fake = Fake::start(vec![
        vec![Step::ExpectOpen(1), Step::events(1..=2), Step::Close],
        vec![Step::ExpectOpen(3), Step::finishing(3..=5)],
    ])?;
    let mut follow = fake.follow(fast()).await?;
    let events = collect(&mut follow).await?;
    assert_eq!(seqs(&events), [1, 2, 3, 4, 5]);
    assert_eq!(follow.stats().reconnects, 1);
    assert_eq!(fake.opens(), [1, 3]);
    assert!(follow.is_finished());
    assert!(follow.next().await?.is_none(), "the stream stays ended");
    fake.assert_clean();
    Ok(())
}

#[tokio::test]
async fn a_failing_reconnect_is_retried_with_backoff_then_succeeds() -> TestResult {
    let fake = Fake::start(vec![
        vec![Step::ExpectOpen(1), Step::events(1..=2), Step::Close],
        // The first re-establish attempt is dropped before it can be answered.
        vec![Step::Close],
        vec![Step::ExpectOpen(3), Step::finishing(3..=3)],
    ])?;
    let mut follow = fake.follow(fast()).await?;
    let events = collect(&mut follow).await?;
    assert_eq!(seqs(&events), [1, 2, 3]);
    fake.assert_clean();
    Ok(())
}

#[tokio::test]
async fn giving_up_is_an_error_not_a_hang() -> TestResult {
    let fake = Fake::start(vec![vec![
        Step::ExpectOpen(1),
        Step::events(1..=1),
        Step::Close,
    ]])?;
    let mut follow = fake.follow(fast()).await?;
    assert_eq!(follow.next().await?.map(|e| e.seq.get()), Some(1));
    // The server accepts no further connection scripts: every reconnect is dropped.
    let error = within("the client to give up", follow.next())
        .await
        .err()
        .ok_or("an error")?;
    assert!(
        matches!(error, ClientError::Reconnect { attempts: 4, .. }),
        "{error}"
    );
    // The stream is dead afterwards.
    assert!(follow.next().await.is_err());
    Ok(())
}

#[tokio::test]
async fn dropping_the_future_does_not_reset_the_reconnect_budget() -> TestResult {
    // A UI polls `next()` inside a `select!` with a tick, so the future is dropped over
    // and over. A daemon that stays away must still be reported, not retried forever.
    let fake = Fake::start(vec![vec![
        Step::ExpectOpen(1),
        Step::events(1..=1),
        Step::Close,
    ]])?;
    let mut follow = fake.follow(fast()).await?;
    assert_eq!(follow.next().await?.map(|e| e.seq.get()), Some(1));
    let started = tokio::time::Instant::now();
    let error = loop {
        match tokio::time::timeout(Duration::from_millis(3), follow.next()).await {
            Ok(Err(error)) => break error,
            Ok(Ok(event)) => panic!("unexpected event {event:?}"),
            Err(_) => assert!(
                started.elapsed() < PATIENCE,
                "the budget was reset by dropping the future"
            ),
        }
    };
    assert!(matches!(error, ClientError::Reconnect { .. }), "{error}");
    Ok(())
}

#[tokio::test]
async fn a_run_nobody_drives_is_followed_by_polling_its_log() -> TestResult {
    // An embedded run (like `aftman run --no-daemon`) holds the run's claim in another
    // "process": the daemon is not driving it, so it would never push an event.
    let mut d = Daemonized::start().await?;
    let factory = CliFactory::new(
        d._dir.path().join("state"),
        FactoryInputs::empty(),
        Mode::Embedded,
    );
    let sink = Arc::new(aftman_daemon::VecSink::default());
    let prepared = factory
        .prepare(
            &d.sleeper("600.203"),
            aftman_daemon::RuntimeContext {
                store: d.store.clone(),
                sink: sink.clone(),
                clock: Arc::new(SystemClock),
            },
        )
        .await?;
    let runtime = prepared.built.runtime;
    let started = runtime.start(prepared.built.job).await?;
    let run = started.run_id();
    let (control, receiver) = aftman_daemon::RunControl::channel();
    let driving = tokio::spawn({
        let runtime = runtime.clone();
        async move { runtime.drive(started, Some(receiver)).await }
    });

    let mut follow = d.follow(run).await?;
    assert!(follow.is_polling(), "the daemon does not drive this run");
    let mut seen = Vec::new();
    for _ in 0..4 {
        seen.push(within("an event", follow.next()).await?.ok_or("an event")?);
    }
    control.cancel("test over").await?;
    let mut rest = Vec::new();
    within("the polled stream to end", async {
        while let Some(event) = follow.next().await? {
            rest.push(event);
        }
        Ok::<(), ClientError>(())
    })
    .await?;
    seen.extend(rest);
    let _ = within("drive", driving).await;
    assert_eq!(
        seen,
        d.store.read_log(run).await?.events,
        "the whole log, once"
    );
    assert_eq!(seen.last().map(|e| e.kind), Some(EventKind::RunFinished));
    d.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn a_gap_is_an_error_never_papered_over() -> TestResult {
    let fake = Fake::start(vec![vec![
        Step::ExpectOpen(1),
        Step::Events(vec![1, 2, 4], false),
    ]])?;
    let mut follow = fake.follow(fast()).await?;
    assert_eq!(follow.next().await?.map(|e| e.seq.get()), Some(1));
    assert_eq!(follow.next().await?.map(|e| e.seq.get()), Some(2));
    let error = follow.next().await.err().ok_or("an error")?;
    assert!(
        matches!(
            error,
            ClientError::Sequence {
                expected: 3,
                got: 4
            }
        ),
        "{error}"
    );
    assert!(
        follow.next().await.is_err(),
        "no further events after a violation"
    );
    Ok(())
}

#[tokio::test]
async fn a_duplicate_is_an_error_never_papered_over() -> TestResult {
    let fake = Fake::start(vec![vec![
        Step::ExpectOpen(1),
        Step::Events(vec![1, 2, 2], false),
    ]])?;
    let mut follow = fake.follow(fast()).await?;
    follow.next().await?;
    follow.next().await?;
    let error = follow.next().await.err().ok_or("an error")?;
    assert!(
        matches!(
            error,
            ClientError::Sequence {
                expected: 3,
                got: 2
            }
        ),
        "{error}"
    );
    Ok(())
}

#[tokio::test]
async fn a_refused_subscription_is_not_retried() -> TestResult {
    let fake = Fake::start(vec![vec![Step::RejectOpen(ErrorCode::UnknownRun)]])?;
    let error = fake.follow(fast()).await.err().ok_or("an error")?;
    assert_eq!(
        error.protocol().map(|e| e.code),
        Some(ErrorCode::UnknownRun)
    );
    Ok(())
}

#[tokio::test]
async fn a_wedged_daemon_is_a_timeout_not_a_hang() -> TestResult {
    let fake = Fake::start(vec![vec![Step::Hang], vec![Step::Hang]])?;
    let started = tokio::time::Instant::now();
    let mut client = Client::connect_with(&fake.socket, fast()).await?;
    let error = client.status().await.err().ok_or("an error")?;
    assert!(
        matches!(
            error,
            ClientError::Timeout {
                method: "daemon.status",
                ..
            }
        ),
        "{error}"
    );
    assert!(started.elapsed() < Duration::from_secs(5));
    // After a timeout the connection is not trusted with another request.
    let again = client.status().await.err().ok_or("an error")?;
    assert!(matches!(again, ClientError::Poisoned(_)), "{again}");

    // The same bound applies to opening a stream.
    let error = fake.follow(fast()).await.err().ok_or("an error")?;
    assert!(matches!(error, ClientError::Timeout { .. }), "{error}");
    Ok(())
}

#[tokio::test]
async fn no_daemon_is_reported_as_not_running() -> TestResult {
    let dir = short_tempdir()?;
    let socket = dir.path().join("none.sock");
    let error = Client::connect(&socket).await.err().ok_or("an error")?;
    assert!(error.is_not_running(), "{error}");
    let error = Follow::open(&socket, RunId::new(), EventSeq::FIRST, fast())
        .await
        .err()
        .ok_or("an error")?;
    assert!(error.is_not_running(), "{error}");
    Ok(())
}

#[tokio::test]
async fn a_stream_survives_the_real_daemon_restarting_the_connection_underneath() -> TestResult {
    // A real daemon, a real run, and a client whose connection is cut by hand. The
    // run keeps going; the follow stream reconnects and must lose nothing.
    let mut d = Daemonized::start().await?;
    let mut client = d.client().await?;
    let run = client.run_start(d.sleeper("600.202")).await?;
    wait_for_status(&mut client, run, FactoryStatus::Running).await?;

    // Pass the follow stream through a relay that we can cut.
    let relay_dir = short_tempdir()?;
    let relay_socket = relay_dir.path().join("r.sock");
    let relay = UnixListener::bind(&relay_socket)?;
    let upstream = d.paths.socket.clone();
    let cuts: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::new(Mutex::new(Vec::new()));
    let relay_cuts = cuts.clone();
    let relay_task = tokio::spawn(async move {
        while let Ok((mut inbound, _)) = relay.accept().await {
            let upstream = upstream.clone();
            let pump = tokio::spawn(async move {
                if let Ok(mut outbound) = UnixStream::connect(&upstream).await {
                    let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                }
            });
            if let Ok(mut c) = relay_cuts.lock() {
                c.push(pump);
            }
        }
    });

    let mut follow =
        Follow::open(&relay_socket, run, EventSeq::FIRST, ClientConfig::default()).await?;
    let mut events = Vec::new();
    for _ in 0..6 {
        events.push(within("an event", follow.next()).await?.ok_or("an event")?);
    }
    // Cut every relayed connection mid-stream.
    if let Ok(mut c) = cuts.lock() {
        for pump in c.drain(..) {
            pump.abort();
        }
    }
    client.run_cancel(run).await?;
    events.extend(collect(&mut follow).await?);

    let stored = client.all_events(run, EventSeq::FIRST).await?;
    assert_eq!(events, stored, "the cut cost nothing");
    assert!(follow.stats().reconnects >= 1);
    relay_task.abort();
    d.shutdown().await;
    Ok(())
}
