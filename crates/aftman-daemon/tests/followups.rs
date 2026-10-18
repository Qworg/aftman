//! Behaviour added after the review of the CLI stage: one daemon per state directory,
//! a presence probe, `live` subscriptions, status info, recorded launch posture, the
//! embedded failure-marking entry point, and ending workers orphaned by a dead supervisor.

mod common;

use std::os::unix::process::CommandExt as _;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use aftman_core::clock::SystemClock;
use aftman_core::event::EventKind;
use aftman_core::evidence::EvidencePlan;
use aftman_core::ids::{RunId, WorkerId};
use aftman_core::job::{FactoryStatus, Job, JobSource, WorkerKind};
use aftman_core::responsibility::ResponsibilityRegistry;
use aftman_daemon::lifecycle::{self, DaemonPresence};
use aftman_daemon::protocol::RunStartParams;
use aftman_daemon::{
    Daemon, DaemonConfig, DaemonPaths, FactoryRuntime, LifecycleError, NullSink, RunControl,
    RuntimeContext, RuntimeDeps, RuntimeFactory, VecSink, mark_run_failed, probe, probe_state_dir,
};
use aftman_store::{FileRunStore, RunStore};
use aftman_workers::process::{OrphanOutcome, identify};
use aftman_workers::{CommandSpec, SteerProtocol, SubprocessWorker};
use common::{
    DialModel, Harness, Mode, PATIENCE, TestFactory, TestResult, event_count, short_tempdir,
    wait_until,
};

fn context(store: Arc<dyn RunStore>) -> RuntimeContext {
    RuntimeContext {
        store,
        sink: Arc::new(NullSink),
        clock: Arc::new(SystemClock),
    }
}

async fn started_run(
    factory: &Arc<TestFactory>,
    store: &Arc<dyn RunStore>,
    repo: &std::path::Path,
    extra: serde_json::Value,
) -> Result<(aftman_daemon::runtime::StartedRun, Arc<FactoryRuntime>), Box<dyn std::error::Error>> {
    let mut params = json!({ "repo": repo, "job": "do it" });
    for (k, v) in extra.as_object().into_iter().flatten() {
        params[k] = v.clone();
    }
    let params: RunStartParams = serde_json::from_value(params)?;
    let built = factory.build(&params, context(store.clone())).await?;
    let started = built.runtime.start(built.job).await?;
    Ok((started, built.runtime))
}

// ---- 3: one daemon per state directory ---------------------------------------------

#[tokio::test]
async fn only_one_daemon_may_serve_a_state_directory() -> TestResult {
    let mut h = Harness::start().await?;
    let other = short_tempdir()?;
    let mut config = DaemonConfig::new(DaemonPaths::in_dir(&other.path().join("run")));
    config.state_dir = Some(h.state.clone());
    let store: Arc<dyn RunStore> = Arc::new(FileRunStore::new(&h.state));
    let start = |config: DaemonConfig| {
        Daemon::start(
            config,
            store.clone(),
            TestFactory::new(other.path()),
            Arc::new(SystemClock),
        )
    };

    // A different socket and pid file, the same state directory.
    match start(config.clone()).await.err().ok_or("must be refused")? {
        LifecycleError::StateDirInUse { pid, state_dir } => {
            assert_eq!(pid, std::process::id());
            assert_eq!(state_dir, h.state);
        }
        other => panic!("wrong error: {other}"),
    }
    assert_eq!(probe_state_dir(&h.state).await, Some(std::process::id()));

    h.shutdown_via_future().await;
    assert!(
        !h.state.join("daemon.lock").exists(),
        "released on shutdown"
    );
    assert_eq!(probe_state_dir(&h.state).await, None);
    assert!(start(config).await.is_ok(), "free again");

    // The probe is non-destructive: it creates nothing.
    let ghost = other.path().join("ghost");
    assert_eq!(probe_state_dir(&ghost).await, None);
    assert!(!ghost.exists());
    Ok(())
}

// ---- 4: the presence probe ---------------------------------------------------------

#[tokio::test]
async fn the_probe_tells_not_running_from_running_from_busy() -> TestResult {
    let dir = short_tempdir()?;
    let paths = DaemonPaths::in_dir(&dir.path().join("run"));
    assert_eq!(probe(&paths).await, DaemonPresence::NotRunning);
    assert!(!paths.dir().exists(), "probing creates nothing");

    // Lock held, nothing listening: starting up (or shutting down).
    let held = lifecycle::lock(&paths).await?;
    assert_eq!(
        probe(&paths).await,
        DaemonPresence::Busy {
            pid: std::process::id()
        }
    );
    // And the probe disturbed nothing: the holder still holds it.
    assert!(matches!(
        lifecycle::lock(&paths).await.err(),
        Some(LifecycleError::AlreadyRunning { .. })
    ));
    drop(held);
    assert_eq!(
        probe(&paths).await,
        DaemonPresence::NotRunning,
        "stale files only"
    );

    // A serving daemon.
    let mut h = Harness::start().await?;
    assert_eq!(
        probe(&h.paths).await,
        DaemonPresence::Running {
            pid: std::process::id()
        }
    );
    h.shutdown_via_future().await;
    assert_eq!(probe(&h.paths).await, DaemonPresence::NotRunning);
    Ok(())
}

#[tokio::test]
async fn a_daemon_that_is_shutting_down_probes_as_busy_not_as_gone() -> TestResult {
    let mut h = Harness::start_with(|c| c.manager.shutdown_grace = Duration::from_secs(3)).await?;
    let mut c = h.client().await?;
    let _run = h.start_run(&mut c, |_| {}).await?;
    h.wait_worker_running().await;
    // Hold the model mid-cycle so shutdown has to wait out its grace.
    let (entered, release) = h.factory.last_model().arm_gate();
    tokio::time::timeout(PATIENCE, entered.notified())
        .await
        .map_err(|_| "no model call")?;

    c.result("daemon.stop", json!({})).await?;
    let paths = h.paths.clone();
    wait_until("the daemon to report Busy", || async {
        matches!(probe(&paths).await, DaemonPresence::Busy { pid } if pid == std::process::id())
    })
    .await;
    release.notify_one();
    h.join().await;
    assert_eq!(probe(&h.paths).await, DaemonPresence::NotRunning);
    Ok(())
}

// ---- 5: subscription.open says whether the daemon drives the run -------------------

#[tokio::test]
async fn subscription_open_says_whether_this_daemon_is_driving_the_run() -> TestResult {
    let h = Harness::start().await?;
    let mut c = h.client().await?;
    let live_run = h.start_run(&mut c, |_| {}).await?;
    h.wait_worker_running().await;

    let mut s = h.client().await?;
    let opened = s
        .result("subscription.open", json!({ "run_id": live_run }))
        .await?;
    assert_eq!(opened["live"], true);
    let all = s.result("subscription.open", json!({})).await?;
    assert_eq!(all["live"], true, "an all-runs stream is live");

    // A run alive in another process: started and claimed here by someone else.
    let store: Arc<dyn RunStore> = h.store.clone();
    let (foreign, _rt) = started_run(&h.factory, &store, &h.repo, json!({})).await?;
    let opened = s
        .result(
            "subscription.open",
            json!({ "run_id": foreign.run_id(), "from_seq": 1 }),
        )
        .await?;
    assert_eq!(opened["live"], false, "not driven here");
    assert_eq!(opened["from_seq"], 1, "but the history is still served");

    // An ended run.
    c.result("run.cancel", json!({ "run_id": live_run }))
        .await?;
    wait_until("the run to leave", || async {
        !h.manager.is_live(live_run)
    })
    .await;
    let opened = s
        .result("subscription.open", json!({ "run_id": live_run }))
        .await?;
    assert_eq!(opened["live"], false);
    Ok(())
}

// ---- 6: daemon.status additions -----------------------------------------------------

#[tokio::test]
async fn status_reports_the_state_directory_start_time_and_bounded_info() -> TestResult {
    let h = Harness::start_with(|c| {
        c.info.insert("a-provider".into(), "yes".into());
        for i in 0..40 {
            c.info.insert(format!("b-key-{i:02}"), "v".into());
        }
        c.info.insert("a-long".into(), "x".repeat(1_000));
    })
    .await?;
    let mut c = h.client().await?;
    let status = c.result("daemon.status", json!({})).await?;
    assert_eq!(status["state_dir"], json!(h.state));
    let started = status["started_at"].as_str().ok_or("started_at")?;
    time::OffsetDateTime::parse(started, &time::format_description::well_known::Rfc3339)?;
    let info = status["info"].as_object().ok_or("info")?;
    assert_eq!(info["a-provider"], "yes");
    assert!(info.len() <= 32, "bounded: {}", info.len());
    assert!(
        info.values()
            .all(|v| v.as_str().is_some_and(|v| v.len() <= 256))
    );
    Ok(())
}

// ---- 7: the launch posture is recorded ----------------------------------------------

#[tokio::test]
async fn the_launch_posture_is_recorded_in_run_created() -> TestResult {
    let h = Harness::start().await?;
    let mut c = h.client().await?;
    let run = h
        .start_run(&mut c, |p| {
            p["permissions"] = json!("auto-approve");
            p["external_sandbox"] = json!(true);
        })
        .await?;
    let page = c
        .result("run.events", json!({ "run_id": run, "limit": 1 }))
        .await?;
    let created = &page["events"][0];
    assert_eq!(created["kind"], "RUN_CREATED");
    assert_eq!(created["payload"]["launch"]["permissions"], "auto-approve");
    assert_eq!(created["payload"]["launch"]["external_sandbox"], "true");

    // A default run says it is sandboxed.
    let other = h.start_run(&mut c, |_| {}).await?;
    let page = c
        .result("run.events", json!({ "run_id": other, "limit": 1 }))
        .await?;
    assert_eq!(
        page["events"][0]["payload"]["launch"]["permissions"],
        "sandboxed"
    );
    Ok(())
}

// ---- 8: failure marking for the embedded path ---------------------------------------

#[tokio::test]
async fn an_embedded_run_whose_drive_failed_can_be_marked_failed_with_just_a_store() -> TestResult {
    let dir = short_tempdir()?;
    let repo = dir.path().join("repo");
    std::fs::create_dir_all(&repo)?;
    let store: Arc<dyn RunStore> = Arc::new(FileRunStore::new(dir.path().join("state")));
    let factory = TestFactory::new(dir.path());
    let (started, _runtime) = started_run(&factory, &store, &repo, json!({})).await?;
    let id = started.run_id();
    drop(started); // what is left when `drive` returned Err and nothing else happened

    let sink = VecSink::new();
    let outcome = mark_run_failed(
        store.as_ref(),
        &sink,
        &SystemClock,
        id,
        "the worker could not be launched",
    )
    .await?;
    assert_eq!(outcome, aftman_daemon::FailOutcome::Failed);
    assert_eq!(store.load(id).await?.status, FactoryStatus::Failed);
    let log = store.read_log(id).await?;
    let seqs: Vec<u64> = log.events.iter().map(|e| e.seq.get()).collect();
    assert_eq!(seqs, vec![1, 2, 3]);
    assert_eq!(event_count(&log.events, EventKind::RunFinished), 1);
    assert_eq!(
        sink.events()
            .iter()
            .map(|e| e.seq.get())
            .collect::<Vec<_>>(),
        vec![2, 3],
        "what it appended reached the sink, in order"
    );
    // Idempotent.
    assert_eq!(
        mark_run_failed(store.as_ref(), &sink, &SystemClock, id, "again").await?,
        aftman_daemon::FailOutcome::AlreadyFinished
    );
    Ok(())
}

// ---- 9: workers orphaned by a dead supervisor ----------------------------------------

fn spawn_sleeper() -> (std::process::Child, i32) {
    let child = std::process::Command::new("/bin/sh")
        .args(["-c", "sleep 120"])
        .process_group(0)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn");
    let pgid = i32::try_from(child.id()).expect("pid");
    (child, pgid)
}

#[tokio::test]
async fn recovery_ends_an_orphaned_worker_it_can_identify_and_spares_one_it_cannot() -> TestResult {
    let dir = short_tempdir()?;
    let repo = dir.path().join("repo");
    std::fs::create_dir_all(&repo)?;
    let state = dir.path().join("state");
    let store: Arc<dyn RunStore> = Arc::new(FileRunStore::new(&state));
    let factory = TestFactory::new(dir.path());

    // Two runs whose supervisor died (their claims are gone).
    let (a, _) = started_run(&factory, &store, &repo, json!({})).await?;
    let (b, _) = started_run(&factory, &store, &repo, json!({})).await?;
    let (run_a, run_b) = (a.run_id(), b.run_id());
    drop((a, b));

    // Run A: its worker really is still running, and its record is true.
    let (child_a, pgid_a) = spawn_sleeper();
    let mission = dir.path().join("mission-a.md");
    std::fs::write(&mission, "SECRET JOB TEXT")?;
    let worker_a = WorkerId::new();
    store
        .set_worker_process(
            run_a,
            worker_a,
            Some(identify(pgid_a, Some(mission.clone()))),
        )
        .await?;
    let reaper = std::thread::spawn(move || {
        let mut child = child_a;
        child.wait()
    });

    // Run B: the recorded pid now belongs to an unrelated process (its start time
    // does not match what was recorded).
    let (mut unrelated, pgid_b) = spawn_sleeper();
    let mut recycled = identify(pgid_b, None);
    recycled.start_token = Some("1".to_owned());
    let worker_b = WorkerId::new();
    store
        .set_worker_process(run_b, worker_b, Some(recycled))
        .await?;

    let h = Harness::start_in(dir, repo, state, |_| {}).await?;

    // A: identified and ended.
    let outcome_a = h
        .recovery
        .orphans
        .iter()
        .find(|(run, o)| *run == run_a && o.worker == worker_a)
        .map(|(_, o)| o.outcome.clone());
    assert_eq!(
        outcome_a,
        Some(OrphanOutcome::Killed),
        "{:?}",
        h.recovery.orphans
    );
    wait_until("the orphan to die", || async { reaper.is_finished() }).await;
    let status = reaper.join().map_err(|_| "reaper")??;
    assert!(!status.success(), "terminated by a signal: {status:?}");
    assert!(!mission.exists(), "its mission file went with it");

    // B: not signalled, and the report says why.
    let outcome_b = h
        .recovery
        .orphans
        .iter()
        .find(|(run, _)| *run == run_b)
        .map(|(_, o)| o.outcome.clone());
    assert!(
        matches!(outcome_b, Some(OrphanOutcome::Unverified(_))),
        "{:?}",
        h.recovery.orphans
    );
    assert!(
        unrelated.try_wait()?.is_none(),
        "the unrelated process was left alone"
    );

    // Both runs are Failed, with the orphan named in the log, and nothing is left recorded.
    for (run, result) in [(run_a, "killed"), (run_b, "unverified")] {
        assert_eq!(h.status_of(run).await?, "failed");
        let log = h.store.read_log(run).await?;
        let terminated = log
            .events
            .iter()
            .find(|e| e.kind == EventKind::WorkerTerminated)
            .ok_or("WORKER_TERMINATED")?;
        assert_eq!(terminated.payload["cause"], "orphaned");
        assert_eq!(terminated.payload["result"], result);
        assert!(h.store.worker_processes(run).await?.is_empty());
    }
    // The whole group: `sh -c` may have forked the sleep.
    let _ = nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(-pgid_b),
        nix::sys::signal::Signal::SIGKILL,
    );
    let _ = unrelated.wait();
    Ok(())
}

/// A worker group that ignores the polite interrupt, so ending it costs the full grace.
fn spawn_stubborn() -> (std::process::Child, i32) {
    let child = std::process::Command::new("/bin/sh")
        .args(["-c", "trap '' INT; while :; do sleep 1; done"])
        .process_group(0)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn");
    let pgid = i32::try_from(child.id()).expect("pid");
    (child, pgid)
}

#[tokio::test]
async fn recovery_reaps_many_stubborn_orphans_in_about_one_grace_period() -> TestResult {
    const ORPHANS: usize = 4;
    let dir = short_tempdir()?;
    let repo = dir.path().join("repo");
    std::fs::create_dir_all(&repo)?;
    let state = dir.path().join("state");
    let store: Arc<dyn RunStore> = Arc::new(FileRunStore::new(&state));
    let factory = TestFactory::new(dir.path());

    let mut runs = Vec::new();
    let mut children = Vec::new();
    for _ in 0..ORPHANS {
        let (started, _) = started_run(&factory, &store, &repo, json!({})).await?;
        let run = started.run_id();
        drop(started);
        let (child, pgid) = spawn_stubborn();
        store
            .set_worker_process(run, WorkerId::new(), Some(identify(pgid, None)))
            .await?;
        runs.push(run);
        children.push(child);
    }
    let reapers: Vec<_> = children
        .into_iter()
        .map(|mut c| std::thread::spawn(move || c.wait()))
        .collect();

    // One after another this would be ORPHANS x ORPHAN_GRACE (20 s).
    let began = std::time::Instant::now();
    let h = Harness::start_in(dir, repo, state, |_| {}).await?;
    let took = began.elapsed();
    assert!(
        took < Duration::from_secs(12),
        "recovery took {took:?}; orphans must be reaped concurrently"
    );
    assert_eq!(h.recovery.orphans.len(), ORPHANS);
    for (_, orphan) in &h.recovery.orphans {
        assert_eq!(orphan.outcome, OrphanOutcome::Killed);
    }
    // Every run is Failed, its orphan named in a contiguous log, ending in RUN_FINISHED.
    for run in runs {
        assert_eq!(h.status_of(run).await?, "failed");
        let log = h.store.read_log(run).await?;
        let seqs: Vec<u64> = log.events.iter().map(|e| e.seq.get()).collect();
        assert_eq!(seqs, (1..=seqs.len() as u64).collect::<Vec<_>>());
        assert_eq!(
            log.events.last().map(|e| e.kind),
            Some(EventKind::RunFinished)
        );
        assert!(
            log.events
                .iter()
                .any(|e| e.kind == EventKind::WorkerTerminated && e.payload["result"] == "killed")
        );
    }
    for reaper in reapers {
        wait_until("the orphan to die", || async { reaper.is_finished() }).await;
    }
    Ok(())
}

fn subprocess_runtime(
    dir: &std::path::Path,
    store: Arc<dyn RunStore>,
    script: &str,
) -> Arc<FactoryRuntime> {
    let temp = dir.join("evidence-tmp");
    let _ = std::fs::create_dir_all(&temp);
    Arc::new(FactoryRuntime::new(RuntimeDeps {
        config: common::fast_config(),
        store,
        model: Arc::new(DialModel::new(Mode::Working)),
        worker: Arc::new(SubprocessWorker::new(
            WorkerKind::Generic,
            CommandSpec {
                program: "/bin/sh".to_owned(),
                args: vec!["-c".to_owned(), script.to_owned()],
                env: Vec::new(),
                steer: SteerProtocol::None,
                working_dir_is_repo: true,
            },
        )),
        registry: ResponsibilityRegistry::builtin(),
        definitions: aftman_core::definitions::DefinitionSet::bundled().unwrap_or_default(),
        evidence: EvidencePlan::default(),
        evidence_temp_dir: temp,
        clock: Arc::new(SystemClock),
        sink: Arc::new(NullSink),
        ensemble_enabled: false,
        uncertainty_margin: 0.15,
        steering_grace: Duration::from_secs(30),
        launch: Default::default(),

        forwarded_env: Default::default(),
        jev_budget: Default::default(),
    }))
}

#[tokio::test]
async fn the_runtime_records_a_workers_process_and_clears_it_when_reaped() -> TestResult {
    let dir = short_tempdir()?;
    let repo = dir.path().join("repo");
    std::fs::create_dir_all(&repo)?;
    let store: Arc<dyn RunStore> = Arc::new(FileRunStore::new(dir.path().join("state")));
    let runtime = subprocess_runtime(dir.path(), store.clone(), "sleep 0.6");
    let started = runtime
        .start(Job {
            prompt: "x".into(),
            repo,
            source: JobSource::Cli,
        })
        .await?;
    let id = started.run_id();
    let (control, receiver) = RunControl::channel();
    let task = tokio::spawn({
        let runtime = runtime.clone();
        async move { runtime.drive(started, Some(receiver)).await }
    });
    wait_until("the process to be recorded", || async {
        store
            .worker_processes(id)
            .await
            .is_ok_and(|p| !p.is_empty())
    })
    .await;
    wait_until(
        "the record to be cleared when the worker is reaped",
        || async { store.worker_processes(id).await.is_ok_and(|p| p.is_empty()) },
    )
    .await;
    control.cancel("done").await?;
    task.await??;
    Ok(())
}

#[tokio::test]
async fn a_worker_whose_run_task_is_aborted_dies_and_its_stale_record_is_cleaned() -> TestResult {
    let dir = short_tempdir()?;
    let repo = dir.path().join("repo");
    std::fs::create_dir_all(&repo)?;
    let store: Arc<dyn RunStore> = Arc::new(FileRunStore::new(dir.path().join("state")));
    let runtime = subprocess_runtime(dir.path(), store.clone(), "sleep 120");
    let started = runtime
        .start(Job {
            prompt: "x".into(),
            repo,
            source: JobSource::Cli,
        })
        .await?;
    let id = started.run_id();
    let (_control, receiver) = RunControl::channel();
    let task = tokio::spawn({
        let runtime = runtime.clone();
        async move { runtime.drive(started, Some(receiver)).await }
    });
    wait_until("the process to be recorded", || async {
        store
            .worker_processes(id)
            .await
            .is_ok_and(|p| !p.is_empty())
    })
    .await;
    let recorded = store.worker_processes(id).await?;
    let pgid = recorded[0].1.pgid;
    let alive = |pgid: i32| {
        !matches!(
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(-pgid), None),
            Err(nix::errno::Errno::ESRCH)
        )
    };
    assert!(alive(pgid));

    // The supervisor dies: its guards kill the group, but cannot clear the record.
    task.abort();
    let _ = task.await;
    wait_until("the group to be killed with its supervisor", || async {
        !alive(pgid)
    })
    .await;
    assert!(
        !store.worker_processes(id).await?.is_empty(),
        "the record is stale"
    );

    let report = aftman_daemon::fail_run_detailed(
        store.as_ref(),
        &aftman_daemon::manager::PlainSink(&NullSink),
        &SystemClock,
        id,
        "the run task was aborted",
    )
    .await?;
    assert_eq!(report.orphans.len(), 1);
    assert_eq!(report.orphans[0].outcome, OrphanOutcome::AlreadyGone);
    assert!(store.worker_processes(id).await?.is_empty());
    assert_eq!(store.load(id).await?.status, FactoryStatus::Failed);
    Ok(())
}

// ---- 10: nothing is created on the way out --------------------------------------------

#[tokio::test]
async fn shutdown_creates_nothing_when_the_data_directory_has_been_removed() -> TestResult {
    let mut h = Harness::start().await?;
    let mut c = h.client().await?;
    let _run = h.start_run(&mut c, |_| {}).await?;
    h.wait_worker_running().await;
    std::fs::remove_dir_all(&h.state)?;

    h.shutdown_via_future().await;
    assert!(
        !h.state.exists(),
        "shutdown recreated {}: {:?}",
        h.state.display(),
        std::fs::read_dir(&h.state).map(|d| d.count())
    );
    Ok(())
}

// ---- 1: relative repo paths ------------------------------------------------------------

#[tokio::test]
async fn a_relative_repo_path_is_refused_because_the_daemon_cwd_is_not_the_clients() -> TestResult {
    let h = Harness::start().await?;
    let mut c = h.client().await?;
    assert_eq!(
        c.error_code("run.start", json!({ "repo": "relative/repo", "job": "x" }))
            .await?,
        "invalid_repo"
    );
    let _ = RunId::new();
    Ok(())
}
