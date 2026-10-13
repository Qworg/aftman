//! The daemon over a real Unix socket, spoken to with raw NDJSON.
//!
//! The first block pins the milestone's exit criteria (ordering and completeness
//! across subscribers, resume without gaps, a stalled subscriber, graceful
//! shutdown, no run left non-terminal); the second exercises every method and the
//! framing rules. Offline, no environment variables.

mod common;

use std::time::Duration;

use serde_json::{Value, json};

use aftman_core::event::EventKind;
use aftman_core::ids::RunId;
use aftman_core::job::FactoryStatus;
use aftman_core::run::RunFilter;
use aftman_daemon::protocol::MAX_REQUEST_LINE_BYTES;
use aftman_store::RunStore;
use common::{Harness, PATIENCE, TestResult, event_count, kinds, log_as_json, seqs, wait_until};

fn contiguous(n: usize) -> Vec<u64> {
    (1..=n as u64).collect()
}

// ===========================================================================
// Exit criteria
// ===========================================================================

#[tokio::test]
async fn two_subscribers_observe_identical_complete_ordered_sequences() -> TestResult {
    let h = Harness::start().await?;
    let mut a = h.client().await?;
    let run = h.start_run(&mut a, |_| {}).await?;

    let mut b = h.client().await?;
    let mut c = h.client().await?;
    for client in [&mut b, &mut c] {
        let opened = client
            .result("subscription.open", json!({ "run_id": run, "from_seq": 1 }))
            .await?;
        assert_eq!(opened["from_seq"], 1);
        assert_eq!(opened["replaced"], false);
    }

    h.wait_worker_running().await;
    let worker = h.factory.last_worker();
    for i in 0..6 {
        worker.say(&format!("progress {i}\n"));
        tokio::time::sleep(Duration::from_millis(12)).await;
    }
    a.result(
        "run.steer",
        json!({ "run_id": run, "message": "keep going" }),
    )
    .await?;
    a.result("run.cancel", json!({ "run_id": run })).await?;

    let (from_b, from_c) = tokio::join!(b.events_until_finished(run), c.events_until_finished(run));
    let (from_b, from_c) = (from_b?, from_c?);

    let log = log_as_json(&*h.store, run).await?;
    assert!(log.len() > 10, "the run produced a real history");
    assert_eq!(from_b, log, "subscriber B saw exactly the stored log");
    assert_eq!(from_c, log, "subscriber C saw exactly the stored log");
    assert_eq!(seqs(&from_b), contiguous(log.len()), "no gap, no duplicate");
    Ok(())
}

#[tokio::test]
async fn a_reconnecting_client_resumes_with_no_gap_and_no_duplicate() -> TestResult {
    let h = Harness::start().await?;
    let mut a = h.client().await?;
    let run = h.start_run(&mut a, |_| {}).await?;
    h.wait_worker_running().await;

    let mut b = h.client().await?;
    b.result("subscription.open", json!({ "run_id": run, "from_seq": 1 }))
        .await?;
    let worker = h.factory.last_worker();
    for i in 0..3 {
        worker.say(&format!("before {i}\n"));
        tokio::time::sleep(Duration::from_millis(12)).await;
    }
    let first = b.take_events(4).await?;
    let last_seen = *seqs(&first).last().ok_or("no events")?;
    drop(b); // the connection drops mid-run

    // The run keeps going while nobody is subscribed.
    for i in 0..5 {
        worker.say(&format!("while away {i}\n"));
        tokio::time::sleep(Duration::from_millis(12)).await;
    }
    wait_until("the run to log more events", || async {
        h.store
            .read_log(run)
            .await
            .is_ok_and(|l| l.last_seq().get() >= last_seen + 4)
    })
    .await;

    let mut c = h.client().await?;
    let opened = c
        .result(
            "subscription.open",
            json!({ "run_id": run, "from_seq": last_seen + 1 }),
        )
        .await?;
    assert_eq!(opened["from_seq"], last_seen + 1);
    a.result("run.cancel", json!({ "run_id": run })).await?;
    let rest = c.events_until_finished(run).await?;

    let mut combined = first;
    combined.extend(rest);
    let log = log_as_json(&*h.store, run).await?;
    assert_eq!(combined, log, "prefix + resumed suffix is exactly the log");
    assert_eq!(seqs(&combined), contiguous(log.len()));
    Ok(())
}

#[tokio::test]
async fn a_subscriber_that_never_reads_cannot_stall_a_run() -> TestResult {
    let h = Harness::start_with(|c| {
        c.server.outbound_queue = 4;
        c.server.write_timeout = Duration::from_millis(300);
        c.hub.subscriber_capacity = 256;
    })
    .await?;
    let mut a = h.client().await?;
    let run = h.start_run(&mut a, |_| {}).await?;
    h.wait_worker_running().await;

    // A well-behaved client, reading all the while.
    let mut good = h.client().await?;
    good.result("subscription.open", json!({ "run_id": run, "from_seq": 1 }))
        .await?;
    let reader = tokio::spawn(async move {
        good.events_until_finished(run)
            .await
            .map_err(|e| e.to_string())
    });

    // A client that subscribes and then never reads a byte.
    let mut stalled = h.client().await?;
    stalled
        .result("subscription.open", json!({ "run_id": run, "from_seq": 1 }))
        .await?;
    assert_eq!(h.manager.hub().subscriber_count(), 2);

    // Produce far more output than the socket buffers and queues can hold.
    let worker = h.factory.last_worker();
    let big = "x".repeat(8_000);
    // Keep producing until the stalled client has been cut off, however large the
    // kernel's socket buffers happen to be on this machine; the deadline only turns a
    // hang into a failure.
    let deadline = tokio::time::Instant::now() + PATIENCE;
    while h.manager.hub().subscriber_count() != 1 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the stalled subscriber was never cut off"
        );
        worker.say(&big);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    wait_until("the stalled subscriber to be cut off", || async {
        h.manager.hub().subscriber_count() == 1
    })
    .await;

    // The run is still fully responsive: a cancel completes promptly.
    tokio::time::timeout(
        Duration::from_secs(10),
        a.result("run.cancel", json!({ "run_id": run })),
    )
    .await
    .map_err(|_| "the run stalled behind a subscriber that never reads")??;
    h.wait_status(run, "cancelled").await;

    // The good client received everything.
    let received = tokio::time::timeout(PATIENCE, reader).await???;
    let log = log_as_json(&*h.store, run).await?;
    assert_eq!(received, log);

    // The stalled client holds a correct, gap-free prefix and then the stream ends
    // (a lagged push, or the connection simply closed).
    let mut seen = Vec::new();
    let ended_cleanly;
    loop {
        match stalled.read_json().await {
            Ok(Some(message)) if message.get("event").is_some() => {
                seen.push(message["event"].clone());
            }
            Ok(Some(push)) => {
                assert_eq!(push["error"]["code"], "subscriber_lagged", "{push}");
                assert_eq!(push["error"]["retryable"], true);
            }
            Ok(None) => {
                ended_cleanly = true;
                break;
            }
            Err(error) => {
                assert!(!error.to_string().contains("timed out"), "{error}");
                ended_cleanly = true; // reset by the server
                break;
            }
        }
    }
    assert!(ended_cleanly, "the stalled connection must end");
    assert_eq!(seqs(&seen), contiguous(seen.len()), "a gap-free prefix");
    assert!(seen.len() < log.len(), "it was cut off before the end");
    Ok(())
}

async fn graceful_shutdown_cancels_runs(via_stop_request: bool) -> TestResult {
    let mut h = Harness::start().await?;
    let mut a = h.client().await?;
    let live = h.start_run(&mut a, |_| {}).await?;
    h.wait_worker_running().await;
    let live_worker = h.factory.last_worker();
    let waiting = h
        .start_run(&mut a, |p| p["model"] = json!("escalate"))
        .await?;
    h.wait_status(waiting, "needs_human").await;

    let mut watcher = h.client().await?;
    watcher.result("subscription.open", json!({})).await?;

    if via_stop_request {
        let reply = a.result("daemon.stop", json!({})).await?;
        assert_eq!(reply, json!({ "ok": true }));
        h.join().await;
    } else {
        h.shutdown_via_future().await;
    }

    for run in [live, waiting] {
        let state = h.store.load(run).await?;
        assert_eq!(state.status, FactoryStatus::Cancelled, "run {run}");
        let log = h.store.read_log(run).await?;
        let finished: Vec<_> = log
            .events
            .iter()
            .filter(|e| e.kind == EventKind::RunFinished)
            .collect();
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].payload["status"], "cancelled");
        assert_eq!(finished[0].payload["reason"], "daemon shutdown");
    }
    assert_eq!(live_worker.alive(), 0, "the worker was terminated");
    assert!(
        !live_worker.terminations().is_empty(),
        "through the normal termination path"
    );

    assert!(!h.paths.socket.exists(), "socket file removed");
    assert!(!h.paths.pid_file.exists(), "pid file removed");
    assert!(
        tokio::net::UnixStream::connect(&h.paths.socket)
            .await
            .is_err()
    );

    // The watcher was sent the end of the story and then closed.
    let mut finished_seen = 0;
    while let Ok(Some(message)) = watcher.read_json().await {
        if message["event"]["kind"] == "RUN_FINISHED" {
            finished_seen += 1;
        }
    }
    assert_eq!(finished_seen, 2, "both RUN_FINISHED events were delivered");
    Ok(())
}

#[tokio::test]
async fn daemon_stop_cancels_live_runs_and_cleans_up() -> TestResult {
    graceful_shutdown_cancels_runs(true).await
}

#[tokio::test]
async fn the_shutdown_future_cancels_live_runs_and_cleans_up() -> TestResult {
    graceful_shutdown_cancels_runs(false).await
}

#[tokio::test]
async fn a_run_whose_drive_fails_is_recorded_failed_not_left_running() -> TestResult {
    let h = Harness::start().await?;
    let mut a = h.client().await?;
    let mut watcher = h.client().await?;
    watcher.result("subscription.open", json!({})).await?;

    let run = h
        .start_run(&mut a, |p| p["verify"] = json!("fail-append"))
        .await?;
    h.wait_status(run, "failed").await;

    let log = h.store.read_log(run).await?;
    assert_eq!(event_count(&log.events, EventKind::RunFinished), 1);
    let finished = log
        .events
        .iter()
        .find(|e| e.kind == EventKind::RunFinished)
        .ok_or("RunFinished")?;
    assert_eq!(finished.payload["status"], "failed");
    assert!(
        finished.payload["reason"]
            .as_str()
            .is_some_and(|r| r.starts_with("run failed")),
        "{}",
        finished.payload
    );
    let seqs: Vec<u64> = log.events.iter().map(|e| e.seq.get()).collect();
    assert_eq!(seqs, contiguous(log.events.len()), "seq continues, no gap");

    // The failure was announced to subscribers, and the run left the live table.
    let announced = watcher.events_until_finished(run).await?;
    assert_eq!(
        kinds(&announced).last().map(String::as_str),
        Some("RUN_FINISHED")
    );
    wait_until("the run to leave the live table", || async {
        h.manager.live_run_count() == 0
    })
    .await;
    let status = a.result("daemon.status", json!({})).await?;
    assert_eq!(status["runs_active"], 0);
    Ok(())
}

#[tokio::test]
async fn a_run_that_panics_is_recorded_failed_not_left_running() -> TestResult {
    let h = Harness::start().await?;
    let mut a = h.client().await?;
    let run = h
        .start_run(&mut a, |p| p["verify"] = json!("panic-model"))
        .await?;
    h.wait_status(run, "failed").await;

    let log = h.store.read_log(run).await?;
    assert_eq!(event_count(&log.events, EventKind::RunFinished), 1);
    let finished = log
        .events
        .iter()
        .find(|e| e.kind == EventKind::RunFinished)
        .ok_or("RunFinished")?;
    assert_eq!(finished.payload["reason"], "run task panicked");
    wait_until("the run to leave the live table", || async {
        h.manager.live_run_count() == 0
    })
    .await;
    // The daemon is unharmed.
    assert_eq!(
        a.result("daemon.status", json!({})).await?["runs_active"],
        0
    );
    Ok(())
}

// ===========================================================================
// Protocol behaviour
// ===========================================================================

#[tokio::test]
async fn status_reports_the_daemon_and_counts_runs() -> TestResult {
    let h = Harness::start().await?;
    let mut c = h.client().await?;
    let status = c.result("daemon.status", json!({})).await?;
    assert_eq!(status["pid"], std::process::id());
    assert_eq!(status["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(status["runs_total"], 0);
    assert_eq!(status["runs_active"], 0);
    assert!(status["uptime_secs"].is_u64());

    let _run = h.start_run(&mut c, |_| {}).await?;
    let status = c.result("daemon.status", json!({})).await?;
    assert_eq!(status["runs_total"], 1);
    assert_eq!(status["runs_active"], 1);
    Ok(())
}

#[tokio::test]
async fn bad_input_gets_a_structured_error_and_the_connection_survives() -> TestResult {
    let h = Harness::start().await?;
    let mut c = h.client().await?;

    // Malformed JSON: no id to echo, so an id-less error push.
    c.send_raw("this is not json\n").await?;
    let push = c.read_json().await?.ok_or("closed")?;
    assert!(push.get("id").is_none());
    assert_eq!(push["error"]["code"], "invalid_request");
    assert_eq!(push["error"]["retryable"], false);

    // Not an object, missing id, invalid UTF-8.
    c.send_raw("[1,2,3]\n").await?;
    assert_eq!(
        c.read_json().await?.ok_or("closed")?["error"]["code"],
        "invalid_request"
    );
    c.send_raw("{\"method\":\"daemon.status\"}\n").await?;
    assert_eq!(
        c.read_json().await?.ok_or("closed")?["error"]["code"],
        "invalid_request"
    );
    c.send_bytes(b"\xff\xfe\xfd\n").await?;
    assert_eq!(
        c.read_json().await?.ok_or("closed")?["error"]["code"],
        "invalid_request"
    );

    // Unknown method echoes the id.
    let response = c.call("run.explode", json!({})).await?;
    assert_eq!(response["error"]["code"], "unknown_method");
    assert!(response["result"].is_null());

    // Positional params are refused, with the id echoed.
    c.send_raw("{\"id\":77,\"method\":\"run.get\",\"params\":[\"x\"]}\n")
        .await?;
    let response = c.response_for(77).await?;
    assert_eq!(response["error"]["code"], "invalid_request");

    // Unknown field, wrong type, unknown envelope field.
    assert_eq!(
        c.error_code("run.get", json!({ "run_id": RunId::new(), "extra": 1 }))
            .await?,
        "invalid_request"
    );
    assert_eq!(
        c.error_code("run.get", json!({ "run_id": 12 })).await?,
        "invalid_request"
    );
    c.send_raw("{\"id\":78,\"method\":\"daemon.status\",\"bogus\":1}\n")
        .await?;
    assert_eq!(
        c.response_for(78).await?["error"]["code"],
        "invalid_request"
    );

    // An unknown run.
    assert_eq!(
        c.error_code("run.get", json!({ "run_id": RunId::new() }))
            .await?,
        "unknown_run"
    );

    // Blank lines are ignored and CRLF is accepted.
    c.send_raw("\n\r\n{\"id\":90,\"method\":\"daemon.status\"}\r\n")
        .await?;
    assert!(c.response_for(90).await?["error"].is_null());

    // After all of that, the connection still works.
    assert!(c.result("daemon.status", json!({})).await?["pid"].is_u64());
    Ok(())
}

#[tokio::test]
async fn an_oversize_line_is_refused_and_the_connection_closed() -> TestResult {
    let h = Harness::start().await?;
    let mut c = h.client().await?;
    // An endless line with no newline: the server must stop reading at the bound.
    let flood = "a".repeat(MAX_REQUEST_LINE_BYTES + 4096);
    let _ = c.send_raw(&flood).await; // the server may hang up before we finish

    let push = c.read_json().await?.ok_or("closed without saying why")?;
    assert_eq!(push["error"]["code"], "invalid_request", "{push}");
    assert!(
        push["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("exceeds"))
    );
    // Then the connection ends (a clean EOF, or a reset).
    match c.read_json().await {
        Ok(None) | Err(_) => {}
        Ok(Some(extra)) => return Err(format!("unexpected message {extra}").into()),
    }

    // Other clients are unaffected.
    let mut other = h.client().await?;
    assert!(other.result("daemon.status", json!({})).await?["pid"].is_u64());
    Ok(())
}

#[tokio::test]
async fn run_start_validates_and_reports_factory_errors() -> TestResult {
    let h = Harness::start_with(|c| c.manager.max_live_runs = 2).await?;
    let mut c = h.client().await?;
    let base = |extra: Value| {
        let mut p = json!({ "repo": h.repo, "job": "do the thing" });
        for (k, v) in extra.as_object().into_iter().flatten() {
            p[k] = v.clone();
        }
        p
    };

    assert_eq!(
        c.error_code(
            "run.start",
            json!({ "repo": "/definitely/not/a/repo", "job": "x" })
        )
        .await?,
        "invalid_repo"
    );
    assert_eq!(
        c.error_code("run.start", base(json!({ "cmd": "bad" })))
            .await?,
        "invalid_config"
    );
    let down = c
        .error("run.start", base(json!({ "model": "down" })))
        .await?;
    assert_eq!(down["code"], "model_unavailable");
    assert_eq!(down["retryable"], true);
    assert_eq!(
        c.error_code("run.start", base(json!({ "job": "   " })))
            .await?,
        "invalid_request"
    );
    assert_eq!(
        c.error_code("run.start", json!({ "repo": h.repo })).await?,
        "invalid_request",
        "job is required"
    );
    assert_eq!(
        h.store.ids().await?.len(),
        0,
        "failed starts leave no run behind"
    );

    // The live-run bound.
    let first = h.start_run(&mut c, |_| {}).await?;
    let _second = h.start_run(&mut c, |_| {}).await?;
    let limited = c.error("run.start", base(json!({}))).await?;
    assert_eq!(limited["code"], "rate_limited");
    assert_eq!(limited["retryable"], true);

    // A freed slot is usable again.
    c.result("run.cancel", json!({ "run_id": first })).await?;
    wait_until("the slot to free", || async {
        h.manager.live_run_count() == 1
    })
    .await;
    h.start_run(&mut c, |_| {}).await?;
    Ok(())
}

#[tokio::test]
async fn a_simulated_run_goes_from_start_to_finished_over_the_socket() -> TestResult {
    let h = Harness::start().await?;
    let mut c = h.client().await?;
    let mut sub = h.client().await?;
    sub.result("subscription.open", json!({})).await?;
    let run = h
        .start_run(&mut c, |p| p["worker"] = json!("simulated"))
        .await?;

    let events = sub.events_until_finished(run).await?;
    let finished = events.last().ok_or("no events")?;
    assert_eq!(finished["payload"]["status"], "finished");
    h.wait_status(run, "finished").await;

    let state = c.result("run.get", json!({ "run_id": run })).await?;
    assert_eq!(state["status"], "finished");
    assert_eq!(state["header"]["id"], json!(run));
    Ok(())
}

#[tokio::test]
async fn list_and_events_page_correctly() -> TestResult {
    let h = Harness::start().await?;
    let mut c = h.client().await?;
    let mut ids = Vec::new();
    for _ in 0..3 {
        let run = h
            .start_run(&mut c, |p| p["worker"] = json!("simulated"))
            .await?;
        h.wait_status(run, "finished").await;
        ids.push(run);
        // Distinct millisecond timestamps, so ids sort in creation order.
        tokio::time::sleep(Duration::from_millis(3)).await;
    }

    // run.list: newest first, paged by cursor.
    let all = c.result("run.list", json!({})).await?;
    let listed: Vec<String> = all["runs"]
        .as_array()
        .ok_or("runs")?
        .iter()
        .map(|r| r["id"].as_str().unwrap_or("").to_owned())
        .collect();
    let mut expected: Vec<String> = ids.iter().map(ToString::to_string).collect();
    expected.reverse();
    assert_eq!(listed, expected);
    assert!(all["next_cursor"].is_null());

    let page1 = c.result("run.list", json!({ "limit": 2 })).await?;
    assert_eq!(page1["runs"].as_array().map(Vec::len), Some(2));
    let cursor = page1["next_cursor"].clone();
    assert!(cursor.is_string());
    let page2 = c
        .result("run.list", json!({ "limit": 2, "cursor": cursor }))
        .await?;
    assert_eq!(page2["runs"].as_array().map(Vec::len), Some(1));
    assert!(page2["next_cursor"].is_null());
    assert_eq!(page2["runs"][0]["id"], json!(ids[0]));

    let finished = c
        .result("run.list", json!({ "status": ["finished"] }))
        .await?;
    assert_eq!(finished["runs"].as_array().map(Vec::len), Some(3));
    let running = c
        .result("run.list", json!({ "status": ["running"] }))
        .await?;
    assert_eq!(running["runs"].as_array().map(Vec::len), Some(0));
    assert_eq!(
        c.error_code("run.list", json!({ "limit": 0 })).await?,
        "invalid_request"
    );
    // An oversized limit is clamped, not refused.
    c.result("run.list", json!({ "limit": 1_000_000 })).await?;

    // run.events: pages concatenate to the log.
    let run = ids[0];
    let log = log_as_json(&*h.store, run).await?;
    let mut collected = Vec::new();
    let mut from = 1;
    loop {
        let page = c
            .result(
                "run.events",
                json!({ "run_id": run, "from_seq": from, "limit": 4 }),
            )
            .await?;
        let events = page["events"].as_array().ok_or("events")?.clone();
        assert!(events.len() <= 4);
        if let Some(last) = events.last() {
            from = last["seq"].as_u64().ok_or("seq")? + 1;
        }
        collected.extend(events);
        if page["has_more"] == false {
            break;
        }
    }
    assert_eq!(collected, log);
    let beyond = c
        .result("run.events", json!({ "run_id": run, "from_seq": 100_000 }))
        .await?;
    assert_eq!(beyond, json!({ "events": [], "has_more": false }));
    assert_eq!(
        c.error_code("run.events", json!({ "run_id": run, "limit": 0 }))
            .await?,
        "invalid_request"
    );
    assert_eq!(
        c.error_code("run.events", json!({ "run_id": RunId::new() }))
            .await?,
        "unknown_run"
    );
    Ok(())
}

#[tokio::test]
async fn steer_reports_delivery_unsupported_and_no_worker() -> TestResult {
    let h = Harness::start().await?;
    let mut c = h.client().await?;

    // Accepted.
    let run = h.start_run(&mut c, |_| {}).await?;
    h.wait_worker_running().await;
    let worker = h.factory.last_worker();
    let outcome = c
        .result(
            "run.steer",
            json!({ "run_id": run, "message": "use the retry helper" }),
        )
        .await?;
    assert_eq!(outcome, json!({ "outcome": "queued" }));
    assert_eq!(worker.steered(), vec!["use the retry helper".to_owned()]);
    assert_eq!(
        c.error_code("run.steer", json!({ "run_id": run, "message": "  " }))
            .await?,
        "invalid_request"
    );

    // A backend with no live input channel.
    let mute = h
        .start_run(&mut c, |p| p["worker"] = json!("nosteer"))
        .await?;
    h.wait_worker_running().await;
    assert_eq!(
        c.error_code("run.steer", json!({ "run_id": mute, "message": "hello" }))
            .await?,
        "steering_unsupported"
    );

    // A live run with no active worker (waiting for a human).
    let waiting = h
        .start_run(&mut c, |p| p["model"] = json!("escalate"))
        .await?;
    h.wait_status(waiting, "needs_human").await;
    let error = c
        .error(
            "run.steer",
            json!({ "run_id": waiting, "message": "hello?" }),
        )
        .await?;
    assert_eq!(error["code"], "wrong_state");
    assert!(
        error["message"]
            .as_str()
            .is_some_and(|m| m.contains("no active worker"))
    );

    // Unknown run, and an ended run.
    assert_eq!(
        c.error_code(
            "run.steer",
            json!({ "run_id": RunId::new(), "message": "x" })
        )
        .await?,
        "unknown_run"
    );
    c.result("run.cancel", json!({ "run_id": run })).await?;
    wait_until("the run to leave the live table", || async {
        h.status_of(run).await.is_ok_and(|s| s == "cancelled")
    })
    .await;
    assert_eq!(
        c.error_code("run.steer", json!({ "run_id": run, "message": "late" }))
            .await?,
        "wrong_state"
    );
    Ok(())
}

#[tokio::test]
async fn cancel_ends_a_run_once() -> TestResult {
    let h = Harness::start().await?;
    let mut c = h.client().await?;
    let run = h.start_run(&mut c, |_| {}).await?;
    h.wait_worker_running().await;

    let ok = c.result("run.cancel", json!({ "run_id": run })).await?;
    assert_eq!(ok, json!({ "ok": true }));
    h.wait_status(run, "cancelled").await;
    wait_until("the worker to be reaped", || async {
        h.factory.last_worker().alive() == 0
    })
    .await;

    assert_eq!(
        c.error_code("run.cancel", json!({ "run_id": run })).await?,
        "wrong_state",
        "a second cancel is a state error, not a second effect"
    );
    assert_eq!(
        c.error_code("run.cancel", json!({ "run_id": RunId::new() }))
            .await?,
        "unknown_run"
    );
    let log = h.store.read_log(run).await?;
    assert_eq!(event_count(&log.events, EventKind::RunFinished), 1);
    Ok(())
}

#[tokio::test]
async fn an_escalation_can_be_answered_over_the_socket_and_the_run_continues() -> TestResult {
    let h = Harness::start().await?;
    let mut c = h.client().await?;
    let mut s = h.client().await?;
    let run = h
        .start_run(&mut c, |p| p["model"] = json!("escalate"))
        .await?;
    s.result("subscription.open", json!({ "run_id": run, "from_seq": 1 }))
        .await?;

    // Wait for the escalation to be raised, and take its id from the event.
    let escalation = loop {
        let event = s.next_event().await?.ok_or("stream ended")?;
        if event["kind"] == "ESCALATION_RAISED" {
            break event["payload"]["escalation_id"]
                .as_str()
                .ok_or("escalation_id")?
                .to_owned();
        }
    };
    h.wait_status(run, "needs_human").await;

    // Wrong id, malformed decision.
    let stranger = uuid::Uuid::now_v7().to_string();
    assert_eq!(
        c.error_code(
            "run.answer",
            json!({ "run_id": run, "escalation_id": stranger, "decision": { "decision": "stop" } })
        )
        .await?,
        "wrong_state"
    );
    assert_eq!(
        c.error_code(
            "run.answer",
            json!({ "run_id": run, "escalation_id": escalation, "decision": { "decision": "bogus" } })
        )
        .await?,
        "invalid_request"
    );
    assert_eq!(
        c.error_code(
            "run.answer",
            json!({ "run_id": RunId::new(), "escalation_id": escalation, "decision": { "decision": "stop" } })
        )
        .await?,
        "unknown_run"
    );

    // The model now thinks all is well; the human says carry on with guidance.
    h.factory.last_model().set(common::Mode::Working);
    let ok = c
        .result(
            "run.answer",
            json!({
                "run_id": run,
                "escalation_id": escalation,
                "decision": { "decision": "guidance", "text": "prefer the existing retry helper" },
                "note": "looked fine to me"
            }),
        )
        .await?;
    assert_eq!(ok, json!({ "ok": true }));

    // The answer is recorded with the socket surface's name, and the run goes on.
    let answered = loop {
        let event = s.next_event().await?.ok_or("stream ended")?;
        if event["kind"] == "ESCALATION_ANSWERED" {
            break event;
        }
    };
    assert_eq!(answered["payload"]["surface"], "cli");
    wait_until("the run to resume", || async {
        h.status_of(run).await.is_ok_and(|s| s != "needs_human")
    })
    .await;
    assert_eq!(
        h.manager.live_run_count(),
        1,
        "still live: the run continues"
    );

    // Answering again is refused: the escalation is closed.
    assert_eq!(
        c.error_code(
            "run.answer",
            json!({ "run_id": run, "escalation_id": escalation, "decision": { "decision": "stop" } })
        )
        .await?,
        "wrong_state"
    );
    c.result("run.cancel", json!({ "run_id": run })).await?;
    h.wait_status(run, "cancelled").await;
    Ok(())
}

#[tokio::test]
async fn purge_deletes_ended_runs_and_refuses_live_ones() -> TestResult {
    let h = Harness::start().await?;
    let mut c = h.client().await?;

    let ended = h
        .start_run(&mut c, |p| p["worker"] = json!("simulated"))
        .await?;
    h.wait_status(ended, "finished").await;
    let cancelled = h.start_run(&mut c, |_| {}).await?;
    h.wait_worker_running().await;
    c.result("run.cancel", json!({ "run_id": cancelled }))
        .await?;
    h.wait_status(cancelled, "cancelled").await;
    let live = h.start_run(&mut c, |_| {}).await?;
    h.wait_worker_running().await;

    // A live run is refused and untouched.
    assert_eq!(
        c.error_code("run.purge", json!({ "run_id": live })).await?,
        "wrong_state"
    );
    assert!(h.store.load(live).await.is_ok());
    assert_eq!(
        c.error_code("run.purge", json!({ "run_id": RunId::new() }))
            .await?,
        "unknown_run"
    );
    // Exactly one of run_id / all.
    assert_eq!(
        c.error_code("run.purge", json!({})).await?,
        "invalid_request"
    );
    assert_eq!(
        c.error_code("run.purge", json!({ "run_id": ended, "all": true }))
            .await?,
        "invalid_request"
    );

    // One ended run: gone from disk, from get, and from the list.
    let dir = h.store.paths(ended).dir;
    assert!(dir.exists());
    assert_eq!(
        c.result("run.purge", json!({ "run_id": ended })).await?,
        json!({ "purged": 1 })
    );
    assert!(!dir.exists());
    assert_eq!(
        c.error_code("run.get", json!({ "run_id": ended })).await?,
        "unknown_run"
    );

    // All ended runs: the live one survives.
    assert_eq!(
        c.result("run.purge", json!({ "all": true })).await?,
        json!({ "purged": 1 })
    );
    let remaining = h.store.list(&RunFilter::default()).await?;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].id, live);
    Ok(())
}

#[tokio::test]
async fn responsibility_list_and_jev_usage_come_from_the_factory() -> TestResult {
    let h = Harness::start().await?;
    let mut c = h.client().await?;
    let list = c.result("responsibility.list", json!({})).await?;
    assert_eq!(list["responsibilities"][0]["id"], "core.completion");
    let usage = c
        .result("jev.usage", json!({ "since": "2026-01-01T00:00:00Z" }))
        .await?;
    assert_eq!(usage["requests"], 0);
    assert_eq!(usage["since"], "2026-01-01T00:00:00Z");
    assert_eq!(
        c.error_code("jev.usage", json!({ "bogus": true })).await?,
        "invalid_request"
    );
    Ok(())
}

#[tokio::test]
async fn subscriptions_open_replace_and_close() -> TestResult {
    let h = Harness::start().await?;
    let mut a = h.client().await?;
    let first = h.start_run(&mut a, |_| {}).await?;
    h.wait_worker_running().await;
    let first_worker = h.factory.last_worker();
    let second = h.start_run(&mut a, |_| {}).await?;
    h.wait_worker_running().await;
    let second_worker = h.factory.last_worker();

    let mut s = h.client().await?;
    assert_eq!(
        s.error_code("subscription.open", json!({ "run_id": RunId::new() }))
            .await?,
        "unknown_run"
    );
    assert_eq!(
        s.error_code("subscription.open", json!({ "from_seq": 3 }))
            .await?,
        "invalid_request"
    );

    let opened = s
        .result(
            "subscription.open",
            json!({ "run_id": first, "from_seq": 1 }),
        )
        .await?;
    assert_eq!(
        opened,
        json!({ "run_id": first, "from_seq": 1, "replaced": false, "live": true })
    );

    // A second open replaces the first: only the new run's events arrive.
    let replaced = s
        .result("subscription.open", json!({ "run_id": second }))
        .await?;
    assert_eq!(
        replaced,
        json!({ "run_id": second, "from_seq": 1, "replaced": true, "live": true })
    );
    // Whatever the old stream delivered was queued before the reply; from here on
    // only the replacement may speak.
    s.events.clear();
    // The old stream's backlog is not delivered after the reply.
    first_worker.say("noise for the first run\n");
    second_worker.say("noise for the second run\n");
    let batch = s.take_events(5).await?;
    assert!(
        batch.iter().all(|e| e["run_id"] == json!(second)),
        "only the replacement subscription delivers: {batch:?}"
    );

    // Close; closing again says there was nothing to close.
    assert_eq!(
        s.result("subscription.close", json!({})).await?,
        json!({ "closed": true })
    );
    s.events.clear();
    assert_eq!(
        s.result("subscription.close", json!({})).await?,
        json!({ "closed": false })
    );
    assert_eq!(h.manager.hub().subscriber_count(), 0);

    // A live-only stream of every run.
    let all = s.result("subscription.open", json!({})).await?;
    assert_eq!(
        all,
        json!({ "run_id": null, "from_seq": null, "replaced": false, "live": true })
    );
    let live = s.take_events(2).await?;
    assert!(
        live.iter()
            .all(|e| e["run_id"] == json!(first) || e["run_id"] == json!(second)),
        "{live:?}"
    );

    a.result("run.cancel", json!({ "run_id": first })).await?;
    a.result("run.cancel", json!({ "run_id": second })).await?;
    Ok(())
}

#[tokio::test]
async fn requests_pipeline_and_a_slow_one_does_not_block_event_pushes() -> TestResult {
    let h = Harness::start().await?;
    let mut x = h.client().await?;

    // Pipelining: three requests in one write, three answers by id.
    let burst = concat!(
        "{\"id\":1,\"method\":\"daemon.status\"}\n",
        "{\"id\":2,\"method\":\"responsibility.list\"}\n",
        "{\"id\":3,\"method\":\"run.list\"}\n",
    );
    x.send_raw(burst).await?;
    for id in 1..=3u64 {
        let response = x.response_for(id).await?;
        assert!(response["error"].is_null(), "{response}");
    }

    // A run whose model call we can hold: a cancel then has to wait for the cycle.
    let mut a = h.client().await?;
    let slow = h.start_run(&mut a, |_| {}).await?;
    h.wait_worker_running().await;
    let (entered, release) = h.factory.last_model().arm_gate();
    tokio::time::timeout(PATIENCE, entered.notified())
        .await
        .map_err(|_| "the model call never started")?;

    x.result("subscription.open", json!({})).await?;
    let cancel = x.send("run.cancel", json!({ "run_id": slow })).await?;
    let status = x.send("daemon.status", json!({})).await?;

    // A later request on the same connection is answered while the cancel waits.
    let response = x.response_for(status).await?;
    assert!(response["error"].is_null());
    assert!(
        x.other_responses.is_empty(),
        "the cancel cannot have been answered yet: {:?}",
        x.other_responses
    );

    // And event pushes keep flowing on that connection: another run's events arrive
    // while the cancel is still pending.
    let busy = h.start_run(&mut a, |_| {}).await?;
    h.wait_worker_running().await;
    h.factory.last_worker().say("output from the other run\n");
    let flowing = x.take_events(5).await?;
    assert!(
        flowing.iter().all(|e| e["run_id"] == json!(busy)),
        "{flowing:?}"
    );
    assert!(x.other_responses.is_empty(), "still pending");

    // Release the model; the cancel now completes.
    release.notify_one();
    let response = x.response_for(cancel).await?;
    assert!(response["error"].is_null(), "{response}");
    h.wait_status(slow, "cancelled").await;
    a.result("run.cancel", json!({ "run_id": busy })).await?;
    Ok(())
}

#[tokio::test]
async fn connections_are_bounded() -> TestResult {
    let h = Harness::start_with(|c| c.server.max_connections = 2).await?;
    let mut one = h.client().await?;
    let mut two = h.client().await?;
    for client in [&mut one, &mut two] {
        assert!(client.result("daemon.status", json!({})).await?["pid"].is_u64());
    }

    let mut three = h.client().await?;
    let push = three
        .read_json()
        .await?
        .ok_or("closed without saying why")?;
    assert_eq!(push["error"]["code"], "rate_limited", "{push}");
    assert_eq!(push["error"]["retryable"], true);
    assert!(three.read_json().await?.is_none(), "then it is closed");

    // The admitted clients are unaffected, and a slot frees when one leaves.
    assert!(two.result("daemon.status", json!({})).await?["pid"].is_u64());
    drop(one);
    wait_until("a connection slot to free", || async {
        let Ok(mut c) = h.client().await else {
            return false;
        };
        let Ok(id) = c.send("daemon.status", json!({})).await else {
            return false;
        };
        matches!(c.response_for(id).await, Ok(r) if r["error"].is_null())
    })
    .await;
    Ok(())
}
