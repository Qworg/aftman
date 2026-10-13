//! When the daemon stops driving a run and cannot record its end, followers are told
//! explicitly; and the shutdown sequence leaves no run live, uncancelled, or `running`.

mod common;

use std::os::unix::fs::PermissionsExt as _;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use serde_json::{Value, json};

use aftman_core::event::EventKind;
use aftman_core::ids::RunId;
use aftman_daemon::manager::cap_reason;
use aftman_daemon::protocol::{ErrorCode, ServerMessage};
use aftman_daemon::{EventHub, HubConfig};
use aftman_store::{MemoryRunStore, RunStore};
use common::{Client, Harness, TestResult, wait_until};

fn is_root() -> bool {
    nix::unistd::geteuid().is_root()
}

/// Read until an id-less error push; events before it are returned too.
async fn until_push(
    client: &mut Client,
) -> Result<(Vec<Value>, Value), Box<dyn std::error::Error>> {
    let mut events = Vec::new();
    loop {
        let message = client
            .read_json()
            .await?
            .ok_or("stream closed before the push")?;
        if message.get("event").is_some() {
            events.push(message["event"].clone());
        } else if message.get("error").is_some() && message.get("id").is_none() {
            return Ok((events, message["error"].clone()));
        }
    }
}

/// Nothing more arrives for a short while.
async fn silent(client: &mut Client) -> bool {
    tokio::time::timeout(Duration::from_millis(700), client.read_json())
        .await
        .is_err()
}

#[tokio::test]
async fn followers_are_told_when_the_end_of_a_run_cannot_be_recorded() -> TestResult {
    if is_root() {
        return Ok(()); // chmod does not stop root
    }
    let mut h = Harness::start().await?;
    let mut a = h.client().await?;
    let run = h.start_run(&mut a, |_| {}).await?;
    h.wait_worker_running().await;
    let mut follower = h.client().await?;
    follower
        .result("subscription.open", json!({ "run_id": run, "from_seq": 1 }))
        .await?;
    let mut all_runs = h.client().await?;
    all_runs.result("subscription.open", json!({})).await?;

    // The log can no longer be appended to: the run dies on its next event, and
    // recording that fails too.
    let log = h.store.paths(run).events;
    std::fs::set_permissions(&log, std::fs::Permissions::from_mode(0o444))?;

    let (events, push) = until_push(&mut follower).await?;
    assert!(!events.is_empty(), "it got the history first");
    assert_eq!(push["code"], "run_abandoned", "{push}");
    assert_eq!(push["retryable"], false);
    assert_eq!(push["data"]["run_id"], json!(run));
    assert!(
        push["data"]["reason"]
            .as_str()
            .is_some_and(|r| !r.is_empty())
    );
    assert!(silent(&mut follower).await, "and nothing follows it");

    wait_until("the run to leave the live table", || async {
        h.manager.live_run_count() == 0
    })
    .await;
    assert_eq!(
        a.result("daemon.status", json!({})).await?["runs_active"],
        0
    );

    // A follower that arrives later is told the same, after the history.
    let mut late = h.client().await?;
    late.result("subscription.open", json!({ "run_id": run, "from_seq": 1 }))
        .await?;
    let (history, push) = until_push(&mut late).await?;
    assert!(!history.is_empty());
    assert_eq!(push["code"], "run_abandoned");

    // An all-runs stream is not ended by one run's abandonment.
    assert!(
        a.result("daemon.status", json!({})).await.is_ok(),
        "the daemon is fine"
    );
    drop(all_runs);

    std::fs::set_permissions(&log, std::fs::Permissions::from_mode(0o600))?;
    h.shutdown_via_future().await;
    Ok(())
}

#[tokio::test]
async fn followers_are_told_when_the_run_directory_is_removed_underneath() -> TestResult {
    let mut h = Harness::start().await?;
    let mut a = h.client().await?;
    let run = h.start_run(&mut a, |_| {}).await?;
    h.wait_worker_running().await;
    let mut follower = h.client().await?;
    follower
        .result("subscription.open", json!({ "run_id": run, "from_seq": 1 }))
        .await?;

    std::fs::remove_dir_all(h.store.paths(run).dir)?;

    let (_, push) = until_push(&mut follower).await?;
    assert_eq!(push["code"], "run_abandoned", "{push}");
    assert!(
        push["data"]["reason"]
            .as_str()
            .is_some_and(|r| r.contains("removed")),
        "{push}"
    );
    h.shutdown_via_future().await;
    Ok(())
}

#[tokio::test]
async fn the_hub_ends_a_runs_subscribers_and_remembers_for_late_ones() -> TestResult {
    let store = Arc::new(MemoryRunStore::new());
    let run = RunId::new();
    let other = RunId::new();
    for id in [run, other] {
        store
            .create(&aftman_core::run::RunHeader {
                id,
                created_at: time::OffsetDateTime::UNIX_EPOCH,
                job: aftman_core::job::Job {
                    prompt: "x".into(),
                    repo: "/r".into(),
                    source: aftman_core::job::JobSource::Cli,
                },
                config: aftman_core::job::RunConfig::default(),
                aftman_version: "0".into(),
                worker_versions: Default::default(),
                granted_key_names: Vec::new(),
            })
            .await?;
    }
    let hub = EventHub::new(store, HubConfig::default());
    let mut scoped = hub.subscribe(Some(run), None).await?;
    let mut elsewhere = hub.subscribe(Some(other), None).await?;
    let everything = hub.subscribe(None, None).await?;

    hub.abandon(run, "the disk is on fire");
    assert!(scoped.next().await.is_none(), "the stream ends");
    let gone = scoped.abandoned().expect("told why");
    assert_eq!(gone.run_id, run);
    assert_eq!(gone.reason, "the disk is on fire");
    assert!(elsewhere.abandoned().is_none());
    assert!(everything.abandoned().is_none());
    assert_eq!(hub.subscriber_count(), 2, "other subscribers are untouched");

    // Late.
    let mut late = hub.subscribe(Some(run), None).await?;
    assert!(late.next().await.is_none());
    assert_eq!(
        late.abandoned().map(|a| a.reason),
        Some("the disk is on fire".into())
    );

    // Forgetting a run forgets the abandonment.
    hub.forget(run);
    let fresh = hub.subscribe(Some(run), None).await;
    assert!(fresh.is_ok_and(|s| s.abandoned().is_none()));

    // The reason is bounded.
    hub.abandon(other, &"x".repeat(10_000));
    assert!(elsewhere.next().await.is_none());
    assert!(elsewhere.abandoned().is_some_and(|a| a.reason.len() <= 512));
    Ok(())
}

#[test]
fn the_abandoned_push_has_a_stable_documented_shape() -> TestResult {
    let run = RunId::new();
    let line = aftman_daemon::protocol::encode_line(&ServerMessage::abandoned(run, "why"))?;
    let value: Value = serde_json::from_str(line.trim())?;
    assert!(value.get("id").is_none());
    assert_eq!(value["error"]["code"], "run_abandoned");
    assert_eq!(value["error"]["retryable"], false);
    assert_eq!(
        value["error"]["data"],
        json!({ "run_id": run, "reason": "why" })
    );
    assert!(
        value["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("why"))
    );
    let back = ServerMessage::decode(line.trim())?;
    match back {
        ServerMessage::Error(push) => assert_eq!(push.error.code, ErrorCode::RunAbandoned),
        other => panic!("{other:?}"),
    }
    Ok(())
}

#[test]
fn every_reason_that_reaches_an_event_is_capped_on_a_character_boundary() {
    assert_eq!(cap_reason("short"), "short");
    let long = "é".repeat(2_000);
    let capped = cap_reason(&long);
    assert!(capped.len() <= aftman_daemon::manager::MAX_REASON_BYTES);
    assert!(capped.ends_with("..."));
    assert!(capped.is_char_boundary(capped.len() - 3));
}

// ---- shutdown is airtight ----------------------------------------------------------------

#[tokio::test]
async fn a_factory_slower_than_the_grace_does_not_turn_graceful_cancels_into_aborts() -> TestResult
{
    let mut h =
        Harness::start_with(|c| c.manager.shutdown_grace = Duration::from_millis(150)).await?;
    let mut a = h.client().await?;
    let steady = h.start_run(&mut a, |_| {}).await?;
    h.wait_worker_running().await;
    let mut stopper = h.client().await?;

    // A start whose factory takes 300 ms, i.e. twice the grace.
    let slow = a
        .send(
            "run.start",
            json!({ "repo": h.repo, "job": "x", "verify": "slow-build" }),
        )
        .await?;
    tokio::time::sleep(Duration::from_millis(30)).await;
    stopper.result("daemon.stop", json!({})).await?;
    let response = a.response_for(slow).await?;
    h.join().await;

    // The run that was live was cancelled gracefully, not aborted.
    assert_eq!(h.status_of(steady).await?, "cancelled");
    let log = h.store.read_log(steady).await?;
    let finished = log
        .events
        .iter()
        .find(|e| e.kind == EventKind::RunFinished)
        .ok_or("RunFinished")?;
    assert_eq!(finished.payload["reason"], "daemon shutdown");

    // The late start was refused and its run recorded; nothing is live or `running`.
    assert_eq!(response["error"]["code"], "wrong_state", "{response}");
    for id in h.store.ids().await? {
        assert!(
            h.status_of(id)
                .await
                .is_ok_and(|s| s == "cancelled" || s == "failed"),
            "run {id} was left non-terminal"
        );
    }
    assert_eq!(h.manager.live_run_count(), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_task_that_cannot_be_aborted_is_still_recorded_failed_at_shutdown() -> TestResult {
    let mut h =
        Harness::start_with(|c| c.manager.shutdown_grace = Duration::from_millis(300)).await?;
    let mut a = h.client().await?;
    let run = h
        .start_run(&mut a, |p| p["verify"] = json!("block-model"))
        .await?;
    wait_until("the model to block its thread", || async {
        h.factory.block_entered.load(Ordering::SeqCst)
    })
    .await;

    let started = tokio::time::Instant::now();
    h.shutdown_via_future().await;
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "shutdown is bounded"
    );

    // The task never yielded, so it could not record anything itself; the daemon did.
    assert_eq!(h.status_of(run).await?, "failed");
    let log = h.store.read_log(run).await?;
    assert_eq!(
        log.events
            .iter()
            .filter(|e| e.kind == EventKind::RunFinished)
            .count(),
        1
    );
    let reason = log
        .events
        .iter()
        .find(|e| e.kind == EventKind::RunFinished)
        .and_then(|e| e.payload["reason"].as_str().map(str::to_owned))
        .unwrap_or_default();
    assert!(reason.contains("could not be stopped"), "{reason}");

    // Let the blocked thread go; it must not corrupt the log it can no longer write to.
    h.factory.block_release.store(true, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let after = h.store.read_log(run).await?;
    let seqs: Vec<u64> = after.events.iter().map(|e| e.seq.get()).collect();
    assert_eq!(seqs, (1..=after.events.len() as u64).collect::<Vec<_>>());
    Ok(())
}
