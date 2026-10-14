//! Behaviour pinned by the review of the first daemon commit. Each test names the
//! property; each would fail if that property were broken.

mod common;

use std::os::unix::fs::PermissionsExt as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde_json::json;
use tokio::io::AsyncWriteExt as _;

use aftman_core::clock::SystemClock;
use aftman_core::event::EventKind;
use aftman_core::job::WorkerStatus;
use aftman_daemon::protocol::{ErrorCode, RunPurgeParams, RunSteerParams};
use aftman_daemon::{Daemon, DaemonConfig, DaemonPaths};
use aftman_store::{FileRunStore, RunStore};
use common::{
    Client, Harness, PATIENCE, TestFactory, TestResult, event_count, log_as_json, seqs, wait_until,
};

fn contiguous(n: usize) -> Vec<u64> {
    (1..=n as u64).collect()
}

fn finished_reason(log: &aftman_store::EventLogRead) -> Option<String> {
    log.events
        .iter()
        .find(|e| e.kind == EventKind::RunFinished)
        .and_then(|e| e.payload["reason"].as_str().map(str::to_owned))
}

// ---- item 1: no way of ending a run task leaves its worker alive ------------

#[tokio::test]
async fn a_run_that_panics_after_its_worker_started_leaves_no_worker_behind() -> TestResult {
    let h = Harness::start().await?;
    let mut c = h.client().await?;
    let run = h
        .start_run(&mut c, |p| p["verify"] = json!("panic-late"))
        .await?;
    h.wait_status(run, "failed").await;

    let worker = h.factory.last_worker();
    assert!(
        worker.starts.load(Ordering::SeqCst) >= 1,
        "the worker really was up when the model panicked"
    );
    wait_until("the worker to be gone", || async { worker.alive() == 0 }).await;

    let state = h.store.load(run).await?;
    assert!(!state.workers.is_empty());
    assert!(
        state
            .workers
            .iter()
            .all(|w| w.status != WorkerStatus::Running),
        "no worker record may stay Running: {:?}",
        state.workers.iter().map(|w| w.status).collect::<Vec<_>>()
    );
    assert_eq!(
        finished_reason(&h.store.read_log(run).await?).as_deref(),
        Some("run task panicked")
    );
    Ok(())
}

#[tokio::test]
async fn a_run_aborted_at_the_end_of_the_shutdown_grace_leaves_no_worker_behind() -> TestResult {
    let mut h =
        Harness::start_with(|c| c.manager.shutdown_grace = Duration::from_millis(400)).await?;
    let mut c = h.client().await?;
    let run = h.start_run(&mut c, |_| {}).await?;
    h.wait_worker_running().await;
    let worker = h.factory.last_worker();
    // Hold the model mid-cycle so the cancel can never be observed.
    let (entered, _never_released) = h.factory.last_model().arm_gate();
    tokio::time::timeout(PATIENCE, entered.notified())
        .await
        .map_err(|_| "the model call never started")?;

    h.shutdown_via_future().await;

    assert_eq!(
        h.status_of(run).await?,
        "failed",
        "aborted stragglers are Failed, not left"
    );
    assert_eq!(
        finished_reason(&h.store.read_log(run).await?).as_deref(),
        Some("run task aborted")
    );
    wait_until("the aborted run's worker to be gone", || async {
        worker.alive() == 0
    })
    .await;
    let state = h.store.load(run).await?;
    assert!(
        state
            .workers
            .iter()
            .all(|w| w.status != WorkerStatus::Running)
    );
    assert!(!h.paths.socket.exists());
    Ok(())
}

// ---- RUN_FINISHED means the run is over --------------------------------------

#[tokio::test]
async fn a_subscriber_reacting_to_run_finished_sees_a_terminal_run_it_can_purge() -> TestResult {
    let h = Harness::start().await?;
    let mut watcher = h.client().await?;
    watcher.result("subscription.open", json!({})).await?;
    let mut a = h.client().await?;

    for round in 0..6 {
        // Alternate between a run that finishes by itself and one that is cancelled.
        let finishing = round % 2 == 0;
        let run = h
            .start_run(&mut a, |p| {
                if finishing {
                    p["worker"] = json!("simulated");
                }
            })
            .await?;
        if !finishing {
            h.wait_worker_running().await;
            a.result("run.cancel", json!({ "run_id": run })).await?;
        }
        // React the instant RUN_FINISHED arrives.
        loop {
            let event = watcher.next_event().await?.ok_or("stream ended")?;
            if event["kind"] == "RUN_FINISHED" && event["run_id"] == json!(run) {
                break;
            }
        }
        let got = watcher.result("run.get", json!({ "run_id": run })).await?;
        let status = got["status"].as_str().unwrap_or("");
        assert!(
            matches!(status, "finished" | "cancelled"),
            "round {round}: run.get right after RUN_FINISHED said {status:?}"
        );
        let status = watcher.result("daemon.status", json!({})).await?;
        assert_eq!(status["runs_active"], 0, "round {round}: the run is over");
        let purged = watcher
            .result("run.purge", json!({ "run_id": run }))
            .await?;
        assert_eq!(purged, json!({ "purged": 1 }), "round {round}");
    }
    Ok(())
}

// ---- hub continuity -----------------------------------------------------------

#[tokio::test]
async fn subscribers_see_no_gap_when_an_event_reached_the_log_but_not_the_hub() -> TestResult {
    let h = Harness::start().await?;
    let mut all = h.client().await?;
    all.result("subscription.open", json!({})).await?;
    let mut a = h.client().await?;
    // The fifth append lands in the log and then reports failure, so the runtime
    // never emits it; the manager then appends further events on the run's behalf.
    let run = h
        .start_run(&mut a, |p| p["verify"] = json!("land-then-fail"))
        .await?;
    h.wait_status(run, "failed").await;

    let log = h.store.read_log(run).await?;
    let log_seqs: Vec<u64> = log.events.iter().map(|e| e.seq.get()).collect();
    assert_eq!(log_seqs, contiguous(log.events.len()));
    assert!(
        log_seqs.len() > 5,
        "events were appended after the lost emit"
    );

    let live = all.events_until_finished(run).await?;
    assert_eq!(seqs(&live), log_seqs, "the live all-runs stream has no gap");

    let mut late = h.client().await?;
    late.result("subscription.open", json!({ "run_id": run, "from_seq": 1 }))
        .await?;
    let replay = late.events_until_finished(run).await?;
    assert_eq!(
        replay,
        log_as_json(&*h.store, run).await?,
        "nor does a late subscriber"
    );
    Ok(())
}

// ---- run.start failures --------------------------------------------------------

#[tokio::test]
async fn a_run_start_that_fails_after_create_leaves_nothing_queued() -> TestResult {
    let h = Harness::start().await?;
    let mut c = h.client().await?;
    let error = c
        .error(
            "run.start",
            json!({ "repo": h.repo, "job": "x", "verify": "fail-create" }),
        )
        .await?;
    assert_eq!(error["code"], "internal");
    assert!(
        h.store.ids().await?.is_empty(),
        "no half-made run directory"
    );
    assert_eq!(c.result("run.list", json!({})).await?["runs"], json!([]));
    assert_eq!(c.result("daemon.status", json!({})).await?["runs_total"], 0);
    Ok(())
}

#[tokio::test]
async fn a_panicking_factory_does_not_leak_a_run_slot() -> TestResult {
    let h = Harness::start_with(|c| c.manager.max_live_runs = 2).await?;
    let mut c = h.client().await?;
    for _ in 0..3 {
        assert_eq!(
            c.error_code(
                "run.start",
                json!({ "repo": h.repo, "job": "x", "verify": "panic-factory" })
            )
            .await?,
            "internal"
        );
    }
    // Both slots are still usable.
    h.start_run(&mut c, |_| {}).await?;
    h.start_run(&mut c, |_| {}).await?;
    // And the direct caller (M4's HTTP) is just as safe.
    let direct = h
        .manager
        .run_start(aftman_daemon::protocol::RunStartParams::new(&h.repo, "x"))
        .await;
    assert_eq!(direct.err().map(|e| e.code), Some(ErrorCode::RateLimited));
    Ok(())
}

#[tokio::test]
async fn a_run_start_straddling_shutdown_becomes_a_run_that_shutdown_cancels() -> TestResult {
    let mut h = Harness::start().await?;
    let mut watcher = h.client().await?;
    watcher.result("subscription.open", json!({})).await?;
    let mut a = h.client().await?;
    let mut stopper = h.client().await?;

    // The factory takes 300 ms; the stop arrives while it is still building.
    let start = a
        .send(
            "run.start",
            json!({ "repo": h.repo, "job": "x", "verify": "slow-build" }),
        )
        .await?;
    tokio::time::sleep(Duration::from_millis(60)).await;
    stopper.result("daemon.stop", json!({})).await?;

    let response = a.response_for(start).await?;
    assert!(
        response["error"].is_null(),
        "accepted before the stop: {response}"
    );
    let run: aftman_core::ids::RunId = response["result"]["run_id"]
        .as_str()
        .ok_or("run_id")?
        .parse()?;
    h.join().await;

    assert_eq!(h.status_of(run).await?, "cancelled");
    let log = h.store.read_log(run).await?;
    assert_eq!(finished_reason(&log).as_deref(), Some("daemon shutdown"));
    let seen = watcher.events_until_finished(run).await?;
    assert_eq!(
        seqs(&seen),
        contiguous(log.events.len()),
        "a subscriber saw all of it"
    );
    Ok(())
}

// ---- purge ---------------------------------------------------------------------

#[tokio::test]
async fn concurrent_purges_count_every_run_exactly_once_and_never_fail() -> TestResult {
    let h = Harness::start_with(|c| c.manager.max_live_runs = 64).await?;
    let mut a = h.client().await?;
    let mut ids = Vec::new();
    for _ in 0..30 {
        ids.push(
            h.start_run(&mut a, |p| p["worker"] = json!("simulated"))
                .await?,
        );
    }
    for id in &ids {
        h.wait_status(*id, "finished").await;
    }
    wait_until("the runs to leave the live table", || async {
        h.manager.live_run_count() == 0
    })
    .await;

    let (mut b, mut c) = (h.client().await?, h.client().await?);
    let (first, second) = tokio::join!(
        b.result("run.purge", json!({ "all": true })),
        c.result("run.purge", json!({ "all": true }))
    );
    let total =
        first?["purged"].as_u64().ok_or("purged")? + second?["purged"].as_u64().ok_or("purged")?;
    assert_eq!(total, 30, "each run is deleted, and counted, exactly once");
    assert!(h.store.ids().await?.is_empty());
    // No tombstone directories left behind.
    let leftovers = std::fs::read_dir(h.state.join("runs"))?.count();
    assert_eq!(leftovers, 0);
    Ok(())
}

#[tokio::test]
async fn purge_forgets_the_run_in_the_hub() -> TestResult {
    let h = Harness::start().await?;
    let mut c = h.client().await?;
    let run = h
        .start_run(&mut c, |p| p["worker"] = json!("simulated"))
        .await?;
    h.wait_status(run, "finished").await;
    let mut s = h.client().await?;
    s.result("subscription.open", json!({ "run_id": run, "from_seq": 1 }))
        .await?;
    assert!(h.manager.hub().ring_events_held() > 0);
    assert_eq!(h.manager.hub().subscriber_count(), 1);

    c.result("run.purge", json!({ "run_id": run })).await?;
    assert_eq!(
        h.manager.hub().ring_events_held(),
        0,
        "the replay ring forgot it"
    );
    assert_eq!(
        h.manager.hub().subscriber_count(),
        0,
        "its subscribers were ended"
    );
    Ok(())
}

#[tokio::test]
async fn the_manager_validates_its_own_parameters() -> TestResult {
    let h = Harness::start().await?;
    let mut c = h.client().await?;
    let run = h
        .start_run(&mut c, |p| p["worker"] = json!("simulated"))
        .await?;
    h.wait_status(run, "finished").await;

    // Neither run_id nor all: must not purge everything.
    let error = h
        .manager
        .run_purge(RunPurgeParams {
            run_id: None,
            all: false,
        })
        .await
        .err()
        .ok_or("must be refused")?;
    assert_eq!(error.code, ErrorCode::InvalidRequest);
    assert!(h.store.load(run).await.is_ok(), "nothing was deleted");

    let error = h
        .manager
        .run_steer(RunSteerParams {
            run_id: run,
            message: "  ".into(),
        })
        .await
        .err()
        .ok_or("must be refused")?;
    assert_eq!(error.code, ErrorCode::InvalidRequest);
    Ok(())
}

// ---- recovery: states the in-process kill cannot produce -------------------------

#[tokio::test]
async fn recovery_repairs_the_files_of_runs_that_are_already_over() -> TestResult {
    use aftman_core::job::FactoryStatus;
    use aftman_core::run::RunState;

    let dir = common::short_tempdir()?;
    let repo = dir.path().join("repo");
    std::fs::create_dir_all(&repo)?;
    let state_dir = dir.path().join("state");
    let store = Arc::new(FileRunStore::new(&state_dir));
    let factory = TestFactory::new(dir.path());

    // Three finished runs made without a daemon.
    let mut ids = Vec::new();
    for _ in 0..3 {
        let built = aftman_daemon::RuntimeFactory::build(
            &*factory,
            &serde_json::from_value(json!({ "repo": repo, "job": "x", "worker": "simulated" }))?,
            aftman_daemon::RuntimeContext {
                store: store.clone(),
                sink: Arc::new(aftman_daemon::NullSink),
                clock: Arc::new(SystemClock),
            },
        )
        .await?;
        let outcome = built.runtime.run(built.job).await?;
        assert_eq!(outcome.status, FactoryStatus::Finished);
        ids.push(outcome.run_id);
    }
    let [trailing, unlogged, mid_log] = [ids[0], ids[1], ids[2]];

    let rewrite_snapshot = |id, status: FactoryStatus, last_seq: u64| -> TestResult {
        let path = store.paths(id).state;
        let mut state: RunState = serde_json::from_slice(&std::fs::read(&path)?)?;
        state.status = status;
        state.last_seq = aftman_core::ids::EventSeq(last_seq);
        std::fs::write(path, serde_json::to_vec_pretty(&state)?)?;
        Ok(())
    };

    // (1) The log is finished; the snapshot is stale and says Running.
    rewrite_snapshot(trailing, FactoryStatus::Running, 3)?;

    // (2) The log ends with a terminal StateChanged but RUN_FINISHED never made it.
    let log_path = store.paths(unlogged).events;
    let text = std::fs::read_to_string(&log_path)?;
    let mut lines: Vec<&str> = text.lines().collect();
    lines.pop().ok_or("log")?;
    std::fs::write(&log_path, lines.join("\n") + "\n")?;
    rewrite_snapshot(unlogged, FactoryStatus::Running, 3)?;

    // (3) The snapshot's *status* disagrees with the log's last STATE_CHANGED while
    // the run is genuinely not over: the log decides. Cut the log back to a point
    // where the run was Running, with a snapshot claiming Queued.
    let log_path = store.paths(mid_log).events;
    let text = std::fs::read_to_string(&log_path)?;
    let mut keep = Vec::new();
    let mut last_running = 0;
    for (i, line) in text.lines().enumerate() {
        let event: serde_json::Value = serde_json::from_str(line)?;
        if event["kind"] == "STATE_CHANGED" && event["payload"]["to"] == "assessing" {
            last_running = i;
        }
        keep.push(line);
    }
    assert!(last_running > 0, "the run assessed at least once");
    keep.truncate(last_running + 1);
    std::fs::write(&log_path, keep.join("\n") + "\n")?;
    rewrite_snapshot(mid_log, FactoryStatus::Queued, 1)?;

    let h = Harness::start_in(dir, repo, state_dir, |_| {}).await?;
    let mut c = h.client().await?;

    // (1) reconciled from the log.
    assert!(h.recovery.repaired.contains(&trailing), "{:?}", h.recovery);
    let got = c.result("run.get", json!({ "run_id": trailing })).await?;
    assert_eq!(got["status"], "finished");
    assert_eq!(
        got["last_seq"].as_u64(),
        Some(h.store.read_log(trailing).await?.last_seq().get())
    );
    let running = c
        .result("run.list", json!({ "status": ["running"] }))
        .await?;
    assert!(
        !running["runs"].to_string().contains(&trailing.to_string()),
        "no longer listed as running"
    );
    assert_eq!(
        c.error_code("run.cancel", json!({ "run_id": trailing }))
            .await?,
        "wrong_state"
    );

    // (2) the missing RUN_FINISHED was appended, once, with the log's status.
    assert!(h.recovery.repaired.contains(&unlogged), "{:?}", h.recovery);
    let log = h.store.read_log(unlogged).await?;
    assert_eq!(event_count(&log.events, EventKind::RunFinished), 1);
    let last = log.events.last().ok_or("events")?;
    assert_eq!(last.kind, EventKind::RunFinished);
    assert_eq!(last.payload["status"], "finished");
    assert_eq!(h.status_of(unlogged).await?, "finished");
    let mut waiter = h.client().await?;
    waiter
        .result(
            "subscription.open",
            json!({ "run_id": unlogged, "from_seq": 1 }),
        )
        .await?;
    waiter.events_until_finished(unlogged).await?; // would hang forever otherwise

    // (3) failed *from the status the log says*, not the snapshot's.
    assert!(h.recovery.failed.contains(&mid_log), "{:?}", h.recovery);
    let log = h.store.read_log(mid_log).await?;
    let change = &log.events[log.events.len() - 2];
    assert_eq!(change.kind, EventKind::StateChanged);
    assert_eq!(change.payload["from"], "assessing");
    assert_eq!(change.payload["to"], "failed");
    Ok(())
}

#[tokio::test]
async fn recovery_cuts_a_torn_tail_and_removes_lock_only_directories() -> TestResult {
    let dir = common::short_tempdir()?;
    let repo = dir.path().join("repo");
    std::fs::create_dir_all(&repo)?;
    let state_dir = dir.path().join("state");
    let store = Arc::new(FileRunStore::new(&state_dir));
    let factory = TestFactory::new(dir.path());

    // A run that was started and then its process died mid-append.
    let built = aftman_daemon::RuntimeFactory::build(
        &*factory,
        &serde_json::from_value(json!({ "repo": repo, "job": "x" }))?,
        aftman_daemon::RuntimeContext {
            store: store.clone(),
            sink: Arc::new(aftman_daemon::NullSink),
            clock: Arc::new(SystemClock),
        },
    )
    .await?;
    let started = built.runtime.start(built.job).await?;
    let torn = started.run_id();
    drop(started);
    {
        use std::io::Write as _;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(store.paths(torn).events)?;
        f.write_all(b"{\"seq\":2,\"run_id\":\"01")?;
    }

    // A directory that holds nothing but a lock file.
    let shell = aftman_core::ids::RunId::new();
    let claim = store.claim_new(shell).await?;
    drop(claim);
    assert_eq!(store.ids().await?.len(), 2);

    let h = Harness::start_in(dir, repo, state_dir, |_| {}).await?;
    assert_eq!(h.recovery.failed, vec![torn]);
    assert_eq!(h.recovery.removed, vec![shell]);
    assert!(
        h.recovery.unreadable.is_empty(),
        "{:?}",
        h.recovery.unreadable
    );

    let log = h.store.read_log(torn).await?;
    assert!(log.torn_lines.is_empty(), "the fragment was cut, not kept");
    let all: Vec<u64> = log.events.iter().map(|e| e.seq.get()).collect();
    assert_eq!(all, contiguous(log.events.len()));
    assert_eq!(event_count(&log.events, EventKind::RunFinished), 1);

    // Surfaces agree about what exists.
    let mut c = h.client().await?;
    assert_eq!(c.result("daemon.status", json!({})).await?["runs_total"], 1);
    assert_eq!(
        c.result("run.list", json!({})).await?["runs"]
            .as_array()
            .map(Vec::len),
        Some(1)
    );
    assert!(!h.store.paths(shell).dir.exists());
    Ok(())
}

// ---- connections -------------------------------------------------------------------

#[tokio::test]
async fn half_closing_the_write_side_still_gets_responses_and_the_stream() -> TestResult {
    // `printf '...' | nc -N -U sock`, in a test.
    let h = Harness::start().await?;
    let mut a = h.client().await?;
    let mut nc = h.client().await?;
    nc.send_raw(concat!(
        "{\"id\":1,\"method\":\"subscription.open\",\"params\":{}}\n",
        "{\"id\":2,\"method\":\"daemon.status\"}\n"
    ))
    .await?;
    nc.writer.shutdown().await?; // EOF on the server's read side

    for id in [1, 2] {
        let response = nc.response_for(id).await?;
        assert!(response["error"].is_null(), "{response}");
    }
    // The subscription outlives the half-close.
    let run = h.start_run(&mut a, |_| {}).await?;
    let event = nc
        .next_event()
        .await?
        .ok_or("the stream ended after the half-close")?;
    assert_eq!(event["run_id"], json!(run));
    Ok(())
}

#[tokio::test]
async fn a_half_closed_connection_with_nothing_owed_is_closed() -> TestResult {
    let h = Harness::start().await?;
    let mut c = h.client().await?;
    c.send_raw("{\"id\":1,\"method\":\"daemon.status\"}\n")
        .await?;
    c.writer.shutdown().await?;
    assert!(c.response_for(1).await?["error"].is_null());
    assert!(c.read_json().await?.is_none(), "then the server closes");
    Ok(())
}

#[tokio::test]
async fn idle_connections_are_reaped_but_subscribed_ones_are_not() -> TestResult {
    let h = Harness::start_with(|c| c.server.idle_timeout = Duration::from_millis(300)).await?;
    let mut idle = h.client().await?;
    let mut subscribed = h.client().await?;
    subscribed.result("subscription.open", json!({})).await?;

    assert!(idle.read_json().await?.is_none(), "closed for idleness");
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert!(
        subscribed.result("daemon.status", json!({})).await?["pid"].is_u64(),
        "a connection with a subscription is not idle"
    );
    // With the subscription gone it becomes reapable.
    subscribed.result("subscription.close", json!({})).await?;
    assert!(subscribed.read_json().await?.is_none());
    Ok(())
}

#[tokio::test]
async fn max_in_flight_stops_a_connection_reading_further_requests() -> TestResult {
    let h = Harness::start_with(|c| c.server.max_in_flight = 2).await?;
    let mut a = h.client().await?;
    let run = h.start_run(&mut a, |_| {}).await?;
    h.wait_worker_running().await;
    let (entered, release) = h.factory.last_model().arm_gate();
    tokio::time::timeout(PATIENCE, entered.notified())
        .await
        .map_err(|_| "the model call never started")?;

    let mut x = h.client().await?;
    // Two cancels wait on the held model call and occupy both slots.
    let first = x.send("run.cancel", json!({ "run_id": run })).await?;
    let second = x.send("run.cancel", json!({ "run_id": run })).await?;
    let status = x.send("daemon.status", json!({})).await?;
    let early = tokio::time::timeout(Duration::from_millis(400), x.response_for(status)).await;
    assert!(
        early.is_err(),
        "a third request must not be read while two are in flight"
    );

    release.notify_one();
    for id in [first, second, status] {
        x.response_for(id).await?;
    }
    Ok(())
}

#[tokio::test]
async fn shutdown_gives_a_slow_subscriber_everything_it_was_owed() -> TestResult {
    let mut h = Harness::start_with(|c| {
        c.server.outbound_queue = 4;
        c.server.write_timeout = Duration::from_secs(10);
        c.hub.subscriber_capacity = 65_536;
    })
    .await?;
    let mut a = h.client().await?;
    let run = h.start_run(&mut a, |_| {}).await?;
    h.wait_worker_running().await;

    let mut slow = h.client().await?;
    slow.result("subscription.open", json!({ "run_id": run, "from_seq": 1 }))
        .await?;
    // Far more output than a socket buffer holds, which the client does not read yet.
    let worker = h.factory.last_worker();
    let big = "y".repeat(8_000);
    for _ in 0..300 {
        worker.say(&big);
        tokio::time::sleep(Duration::from_millis(4)).await;
    }

    let mut stopper = h.client().await?;
    stopper.result("daemon.stop", json!({})).await?;
    let reader = tokio::spawn(async move {
        // Start reading only once shutdown is already under way.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let mut events = Vec::new();
        while let Ok(Some(message)) = slow.read_json().await {
            if message.get("event").is_some() {
                events.push(message["event"].clone());
            }
        }
        events
    });
    h.join().await;
    let received = tokio::time::timeout(PATIENCE, reader).await??;

    let log = log_as_json(&*h.store, run).await?;
    assert_eq!(
        received.len(),
        log.len(),
        "every logged event, including the end"
    );
    assert_eq!(
        received.last().map(|e| e["kind"].clone()),
        Some(json!("RUN_FINISHED"))
    );
    Ok(())
}

// ---- lifecycle ---------------------------------------------------------------------

#[tokio::test]
async fn the_socket_never_exists_with_a_wider_mode() -> TestResult {
    let dir = common::short_tempdir()?;
    // A directory the user chose, and which is readable by others: the staging area
    // is what keeps the socket private until it is in place.
    let run_dir = dir.path().join("shared");
    std::fs::create_dir_all(&run_dir)?;
    std::fs::set_permissions(&run_dir, std::fs::Permissions::from_mode(0o755))?;
    let paths = DaemonPaths {
        socket: run_dir.join("aftman.sock"),
        pid_file: run_dir.join("aftman.pid"),
        dir_policy: aftman_daemon::lifecycle::DirPolicy::UserChosen,
    };
    let stop = Arc::new(AtomicBool::new(false));
    let wide = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let samples = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let watcher = {
        let (socket, run_dir) = (paths.socket.clone(), run_dir.clone());
        let (stop, wide, samples) = (stop.clone(), wide.clone(), samples.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                // Anything socket-like that appears anywhere in the directory.
                if let Ok(entries) = std::fs::read_dir(&run_dir) {
                    for entry in entries.flatten() {
                        if let Ok(meta) = entry.metadata()
                            && std::os::unix::fs::FileTypeExt::is_socket(&meta.file_type())
                        {
                            samples.fetch_add(1, Ordering::SeqCst);
                            if meta.permissions().mode() & 0o077 != 0 {
                                wide.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                    }
                }
                let _ = std::fs::metadata(&socket);
            }
        })
    };
    for _ in 0..25 {
        let store: Arc<dyn RunStore> = Arc::new(FileRunStore::new(dir.path().join("state")));
        let daemon = Daemon::start(
            DaemonConfig::new(paths.clone()),
            store,
            TestFactory::new(dir.path()),
            Arc::new(SystemClock),
        )
        .await?;
        daemon.serve(async {}).await;
    }
    stop.store(true, Ordering::SeqCst);
    watcher.join().map_err(|_| "watcher panicked")?;
    assert!(
        samples.load(Ordering::SeqCst) > 0,
        "the watcher did observe sockets"
    );
    assert_eq!(
        wide.load(Ordering::SeqCst),
        0,
        "a socket was visible with a wider mode"
    );
    Ok(())
}

#[tokio::test]
async fn paths_at_the_length_boundary_are_accepted_or_refused_consistently() -> TestResult {
    use aftman_daemon::lifecycle::max_socket_path_bytes;
    let dir = common::short_tempdir()?;
    let base = dir.path().to_path_buf();
    let max = max_socket_path_bytes();
    let name_for = |total: usize| {
        let prefix = base.as_os_str().len() + 1;
        "s".repeat(total - prefix)
    };

    // Exactly at the limit: validates, and really binds.
    let at_limit = DaemonPaths {
        socket: base.join(name_for(max)),
        pid_file: base.join("limit.pid"),
        dir_policy: aftman_daemon::lifecycle::DirPolicy::UserChosen,
    };
    assert_eq!(at_limit.socket.as_os_str().len(), max);
    at_limit.validate()?;
    let store: Arc<dyn RunStore> = Arc::new(FileRunStore::new(base.join("state")));
    let daemon = Daemon::start(
        DaemonConfig::new(at_limit.clone()),
        store.clone(),
        TestFactory::new(&base),
        Arc::new(SystemClock),
    )
    .await?;
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let serving = tokio::spawn(daemon.serve(async move {
        let _ = stopped.await;
    }));
    let mut c = Client::connect(&at_limit.socket).await?;
    assert!(c.result("daemon.status", json!({})).await?["pid"].is_u64());
    let _ = stop.send(());
    serving.await?;

    // One byte more: refused up front with the clear error, never a bind failure.
    let over = DaemonPaths {
        socket: base.join(name_for(max + 1)),
        pid_file: base.join("over.pid"),
        dir_policy: aftman_daemon::lifecycle::DirPolicy::UserChosen,
    };
    assert!(matches!(
        over.validate(),
        Err(aftman_daemon::LifecycleError::SocketPathTooLong { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn shutdown_removes_only_the_socket_and_pid_file_it_created() -> TestResult {
    let mut h = Harness::start().await?;
    // While this daemon runs, something else takes over both paths (a newer daemon
    // started with different pid-file semantics, or the user).
    let newer_socket = h.paths.dir().join("newer.sock");
    let newer = std::os::unix::net::UnixListener::bind(&newer_socket)?;
    std::fs::rename(&newer_socket, &h.paths.socket)?;
    let newer_pid = h.paths.dir().join("newer.pid");
    std::fs::write(&newer_pid, "424242\n")?;
    std::fs::rename(&newer_pid, &h.paths.pid_file)?;

    h.shutdown_via_future().await;

    assert!(
        h.paths.socket.exists(),
        "the newer socket is not ours to remove"
    );
    assert_eq!(std::fs::read_to_string(&h.paths.pid_file)?.trim(), "424242");
    drop(newer);
    Ok(())
}

#[tokio::test]
async fn the_directory_policy_follows_the_socket_not_the_pid_file() -> TestResult {
    let paths = aftman_daemon::resolve_paths(&aftman_daemon::PathInputs {
        xdg_runtime_dir: Some("/run/user/1000".into()),
        pid_override: Some("/tmp/elsewhere/aftman.pid".into()),
        ..Default::default()
    })?;
    assert_eq!(
        paths.dir_policy,
        aftman_daemon::lifecycle::DirPolicy::Managed
    );
    assert_eq!(
        paths.socket,
        std::path::PathBuf::from("/run/user/1000/aftman/aftman.sock")
    );

    // And it is enforced: a wide default directory is tightened even with the pid
    // file elsewhere.
    let dir = common::short_tempdir()?;
    let sock_dir = dir.path().join("sock");
    std::fs::create_dir_all(&sock_dir)?;
    std::fs::set_permissions(&sock_dir, std::fs::Permissions::from_mode(0o777))?;
    let moved = DaemonPaths {
        pid_file: dir.path().join("pids").join("aftman.pid"),
        ..DaemonPaths::in_dir(&sock_dir)
    };
    let store: Arc<dyn RunStore> = Arc::new(FileRunStore::new(dir.path().join("state")));
    let daemon = Daemon::start(
        DaemonConfig::new(moved),
        store,
        TestFactory::new(dir.path()),
        Arc::new(SystemClock),
    )
    .await?;
    assert_eq!(
        std::fs::metadata(&sock_dir)?.permissions().mode() & 0o777,
        0o700
    );
    drop(daemon);
    Ok(())
}
