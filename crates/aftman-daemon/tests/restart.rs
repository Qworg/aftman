//! Restart recovery and run ownership.
//!
//! The milestone criterion: "daemon restart marks in-flight runs unambiguously
//! (resumed or `Failed`, never zombie)". M3 does not resume, so: `Failed`.

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use aftman_core::clock::SystemClock;
use aftman_core::event::EventKind;
use aftman_core::ids::RunId;
use aftman_core::job::FactoryStatus;
use aftman_daemon::protocol::RunStartParams;
use aftman_daemon::{Daemon, DaemonConfig, DaemonPaths, NullSink, RuntimeContext, RuntimeFactory};
use aftman_store::{FileRunStore, MemoryRunStore, RunStore};
use common::{
    Harness, PATIENCE, TestFactory, TestResult, event_count, log_as_json, seqs, wait_until,
};

fn params(repo: &std::path::Path, extra: serde_json::Value) -> RunStartParams {
    let mut value = json!({ "repo": repo, "job": "Add retries with backoff." });
    for (k, v) in extra.as_object().into_iter().flatten() {
        value[k] = v.clone();
    }
    serde_json::from_value(value).expect("valid params")
}

fn context(store: Arc<dyn RunStore>) -> RuntimeContext {
    RuntimeContext {
        store,
        sink: Arc::new(NullSink),
        clock: Arc::new(SystemClock),
    }
}

async fn log_len(store: &dyn RunStore, id: RunId) -> usize {
    store.read_log(id).await.map_or(0, |l| l.events.len())
}

#[tokio::test]
async fn restart_fails_orphans_and_leaves_everything_else_alone() -> TestResult {
    let dir = common::short_tempdir()?;
    let repo = dir.path().join("repo");
    std::fs::create_dir_all(&repo)?;
    let state = dir.path().join("state");
    let store = Arc::new(FileRunStore::new(&state));
    let dyn_store: Arc<dyn RunStore> = store.clone();
    let factory = TestFactory::new(dir.path());

    // ---- runs made without any daemon, as `aftman run --no-daemon` would -------
    // A run that needed a human and ended there: it HAS a RunFinished.
    let built = factory
        .build(
            &params(&repo, json!({ "model": "escalate" })),
            context(dyn_store.clone()),
        )
        .await?;
    let outcome = built.runtime.run(built.job).await?;
    assert_eq!(outcome.status, FactoryStatus::NeedsHuman);
    let needs_human = outcome.run_id;

    // A run that is alive in "another process": started, claimed, never finished.
    let built = factory
        .build(&params(&repo, json!({})), context(dyn_store.clone()))
        .await?;
    let foreign_started = built.runtime.start(built.job).await?;
    let foreign = foreign_started.run_id();

    // ---- daemon A, in its own runtime so it can be killed outright ------------
    let paths = DaemonPaths::in_dir(&dir.path().join("run"));
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let ready = rt.spawn({
        let (store, factory, paths) = (dyn_store.clone(), factory.clone(), paths.clone());
        async move {
            let mut config = DaemonConfig::new(paths);
            config.server.write_timeout = Duration::from_millis(500);
            match Daemon::start(config, store, factory, Arc::new(SystemClock)).await {
                Ok(daemon) => {
                    tokio::spawn(daemon.serve(std::future::pending()));
                    Ok(())
                }
                Err(error) => Err(error.to_string()),
            }
        }
    });
    ready.await??;

    let mut client = common::Client::connect(&paths.socket).await?;
    let start = |extra: serde_json::Value| {
        let mut p = json!({ "repo": repo, "job": "Add retries with backoff." });
        for (k, v) in extra.as_object().into_iter().flatten() {
            p[k] = v.clone();
        }
        p
    };
    let finished: RunId = client
        .result("run.start", start(json!({ "worker": "simulated" })))
        .await?["run_id"]
        .as_str()
        .ok_or("run_id")?
        .parse()?;
    wait_until("the simulated run to finish", || async {
        store
            .load(finished)
            .await
            .is_ok_and(|s| s.status == FactoryStatus::Finished)
    })
    .await;
    let running: RunId = client.result("run.start", start(json!({}))).await?["run_id"]
        .as_str()
        .ok_or("run_id")?
        .parse()?;
    wait_until("a worker to run", || async {
        factory
            .workers
            .lock()
            .is_ok_and(|w| w.last().is_some_and(|w| w.is_running()))
    })
    .await;
    let waiting: RunId = client
        .result("run.start", start(json!({ "model": "escalate" })))
        .await?["run_id"]
        .as_str()
        .ok_or("run_id")?
        .parse()?;
    wait_until("the escalation", || async {
        store
            .load(waiting)
            .await
            .is_ok_and(|s| s.status == FactoryStatus::NeedsHuman)
    })
    .await;

    // Damaged directories: nothing about them may stop the next daemon starting.
    let wreck = RunId::new();
    let torn_middle = RunId::new();
    std::fs::create_dir_all(store.paths(wreck).dir)?;
    std::fs::write(store.paths(wreck).meta, "{ nonsense")?;
    std::fs::write(store.paths(wreck).state, "{ nonsense")?;
    std::fs::write(store.paths(wreck).events, "garbage\nmore garbage\n")?;
    {
        // A valid run whose log is corrupt in the middle.
        use aftman_core::job::{Job, JobSource, RunConfig};
        let header = aftman_core::run::RunHeader {
            id: torn_middle,
            created_at: time::OffsetDateTime::UNIX_EPOCH,
            job: Job {
                prompt: "x".into(),
                repo: repo.clone(),
                source: JobSource::Cli,
            },
            config: RunConfig::default(),
            aftman_version: "0".into(),
            worker_versions: Default::default(),
            granted_key_names: Vec::new(),
        };
        store.create(&header).await?;
        std::fs::write(
            store.paths(torn_middle).events,
            "not json\n{\"also\":\"bad\"}\n",
        )?;
    }

    let untouched_before = (
        log_len(&*store, finished).await,
        log_len(&*store, needs_human).await,
        log_len(&*store, foreign).await,
    );

    // ---- kill daemon A: no graceful shutdown, tasks just stop -----------------
    rt.shutdown_background();
    for run in [running, waiting] {
        // As on process death, the OS-level claim goes away with the owner.
        wait_until("the dead daemon's claim to be released", || async {
            store.claim(run).await.is_ok_and(|c| c.is_some())
        })
        .await;
    }
    assert!(
        paths.socket.exists(),
        "a killed daemon leaves its socket behind"
    );

    // ---- daemon B over the same store ------------------------------------------
    let h = Harness::start_in(dir, repo.clone(), state, |_| {}).await?;
    let report = &h.recovery;
    let mut failed = report.failed.clone();
    failed.sort();
    let mut expected = vec![running, waiting];
    expected.sort();
    assert_eq!(failed, expected, "exactly the two orphans were failed");
    assert_eq!(report.claimed_elsewhere, vec![foreign]);
    assert!(
        report.unreadable.iter().any(|(id, _)| *id == torn_middle),
        "{:?}",
        report.unreadable
    );
    assert!(report.untouched.contains(&finished));
    assert!(report.untouched.contains(&needs_human));

    let mut client = h.client().await?;
    for orphan in [running, waiting] {
        let log = h.store.read_log(orphan).await?;
        assert_eq!(
            event_count(&log.events, EventKind::RunFinished),
            1,
            "{orphan}"
        );
        let all_seqs: Vec<u64> = log.events.iter().map(|e| e.seq.get()).collect();
        assert_eq!(all_seqs, (1..=log.events.len() as u64).collect::<Vec<_>>());
        let last = log.events.last().ok_or("events")?;
        assert_eq!(last.kind, EventKind::RunFinished);
        assert_eq!(last.payload["status"], "failed");
        assert_eq!(last.payload["reason"], "daemon restarted");
        let before = &log.events[log.events.len() - 2];
        assert_eq!(before.kind, EventKind::StateChanged);
        assert_eq!(before.payload["to"], "failed");

        // run.get agrees with the log.
        let got = client
            .result("run.get", json!({ "run_id": orphan }))
            .await?;
        assert_eq!(got["status"], "failed");
        assert_eq!(got["last_seq"].as_u64(), Some(log.last_seq().get()));
    }

    // Everything else is exactly as it was.
    assert_eq!(h.status_of(finished).await?, "finished");
    assert_eq!(h.status_of(needs_human).await?, "needs_human");
    assert_eq!(h.status_of(foreign).await?, "queued");
    assert_eq!(
        (
            log_len(&*h.store, finished).await,
            log_len(&*h.store, needs_human).await,
            log_len(&*h.store, foreign).await,
        ),
        untouched_before
    );
    assert_eq!(
        event_count(
            &h.store.read_log(needs_human).await?.events,
            EventKind::RunFinished
        ),
        1
    );
    assert_eq!(
        event_count(
            &h.store.read_log(foreign).await?.events,
            EventKind::RunFinished
        ),
        0,
        "a run driven elsewhere gets no RunFinished from us"
    );

    // run.list agrees with run.get for every run it shows; damaged runs are skipped.
    let listed = client.result("run.list", json!({})).await?;
    let runs = listed["runs"].as_array().ok_or("runs")?;
    assert_eq!(
        runs.len(),
        5,
        "three untouched and two failed; damaged runs are skipped"
    );
    for summary in listed["runs"].as_array().ok_or("runs")? {
        let id = summary["id"].clone();
        let got = client.result("run.get", json!({ "run_id": id })).await?;
        assert_eq!(summary["status"], got["status"]);
    }
    let failed_list = client
        .result("run.list", json!({ "status": ["failed"] }))
        .await?;
    assert_eq!(failed_list["runs"].as_array().map(Vec::len), Some(2));
    assert_eq!(
        client.result("daemon.status", json!({})).await?["runs_active"],
        0
    );

    // A late subscriber gets the whole history, recovery events included.
    let mut watcher = h.client().await?;
    watcher
        .result(
            "subscription.open",
            json!({ "run_id": running, "from_seq": 1 }),
        )
        .await?;
    let replay = watcher.events_until_finished(running).await?;
    assert_eq!(replay, log_as_json(&*h.store, running).await?);
    assert_eq!(
        seqs(&replay).last().copied(),
        Some(h.store.read_log(running).await?.last_seq().get())
    );

    // The foreign run is protected from purge too, until its owner lets go.
    assert_eq!(
        client
            .error_code("run.purge", json!({ "run_id": foreign }))
            .await?,
        "wrong_state"
    );
    drop(foreign_started);
    assert_eq!(
        client
            .result("run.purge", json!({ "run_id": foreign }))
            .await?,
        json!({ "purged": 1 })
    );
    Ok(())
}

#[tokio::test]
async fn a_second_recovery_is_a_no_op() -> TestResult {
    // Recovery must be idempotent: the Failed runs now have a RunFinished.
    let mut h = Harness::start().await?;
    let mut c = h.client().await?;
    let run = h.start_run(&mut c, |_| {}).await?;
    h.wait_worker_running().await;
    // Another daemon over the same state can't run alongside; use the manager
    // directly after stopping the first one gracefully, which leaves Cancelled.
    h.shutdown_via_future().await;
    assert_eq!(h.status_of(run).await?, "cancelled");

    let report = h.manager.recover().await;
    assert!(report.failed.is_empty(), "{report:?}");
    assert_eq!(report.untouched, vec![run]);
    Ok(())
}

#[tokio::test]
async fn a_no_daemon_run_holds_its_claim_for_its_whole_life() -> TestResult {
    // Both stores must enforce it, since `--no-daemon` can use either.
    for memory in [false, true] {
        let dir = common::short_tempdir()?;
        let repo = dir.path().join("repo");
        std::fs::create_dir_all(&repo)?;
        let store: Arc<dyn RunStore> = if memory {
            Arc::new(MemoryRunStore::new())
        } else {
            Arc::new(FileRunStore::new(dir.path().join("state")))
        };
        let factory = TestFactory::new(dir.path());
        let built = factory
            .build(
                &params(&repo, json!({ "model": "escalate" })),
                context(store.clone()),
            )
            .await?;
        // Hold the first assessment so the run is observably mid-flight.
        let (entered, release) = factory.last_model().arm_gate();

        let runtime = built.runtime;
        let job = built.job;
        let run = tokio::spawn(async move { runtime.run(job).await });
        tokio::time::timeout(PATIENCE, entered.notified())
            .await
            .map_err(|_| "the run never reached its first assessment")?;

        let ids = store.ids().await?;
        assert_eq!(ids.len(), 1);
        assert!(
            store.claim(ids[0]).await?.is_none(),
            "memory={memory}: a running run is claimed, so nobody else may write to it"
        );

        release.notify_one();
        let outcome = tokio::time::timeout(PATIENCE, run).await???;
        assert_eq!(outcome.status, FactoryStatus::NeedsHuman);
        assert!(
            store.claim(ids[0]).await?.is_some(),
            "memory={memory}: released when the run ends"
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_started_but_undriven_run_is_claimed_until_dropped() -> TestResult {
    let dir = common::short_tempdir()?;
    let repo: PathBuf = dir.path().join("repo");
    std::fs::create_dir_all(&repo)?;
    let store: Arc<dyn RunStore> = Arc::new(FileRunStore::new(dir.path().join("state")));
    let factory = TestFactory::new(dir.path());
    let built = factory
        .build(&params(&repo, json!({})), context(store.clone()))
        .await?;
    let started = built.runtime.start(built.job).await?;
    let id = started.run_id();
    assert!(store.claim(id).await?.is_none());
    drop(started);
    assert!(store.claim(id).await?.is_some());
    Ok(())
}
