//! `events_page` must answer exactly what a full read would, however the log is shaped,
//! and the store's files must be owner-only.

use std::path::PathBuf;

use aftman_core::event::{EventEnvelope, EventKind};
use aftman_core::ids::{EventSeq, RunId};
use aftman_core::job::{Job, JobSource, RunConfig};
use aftman_core::run::RunHeader;
use aftman_store::{FileRunStore, RunStore};
use time::macros::datetime;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn header(id: RunId) -> RunHeader {
    RunHeader {
        id,
        created_at: datetime!(2026-01-01 00:00:00 UTC),
        job: Job {
            prompt: "x".into(),
            repo: PathBuf::from("/repo"),
            source: JobSource::Cli,
        },
        config: RunConfig::default(),
        aftman_version: "0".into(),
        worker_versions: Default::default(),
        granted_key_names: Vec::new(),
    }
}

fn line(id: RunId, seq: u64, kind: &str) -> String {
    // Hand-written so an unknown kind can be included.
    format!(
        "{{\"seq\":{seq},\"run_id\":\"{id}\",\"at\":\"2026-01-01T00:00:00Z\",\"kind\":\"{kind}\",\"payload\":{{\"schema_version\":1}}}}"
    )
}

async fn write_log(store: &FileRunStore, id: RunId, body: &str) -> TestResult {
    store.create(&header(id)).await?;
    tokio::fs::write(store.paths(id).events, body).await?;
    Ok(())
}

/// Every (from, limit) page equals the corresponding slice of the full read.
async fn assert_equivalent(store: &FileRunStore, id: RunId) -> TestResult {
    let all = store.events(id, EventSeq::FIRST).await?;
    let last = all.last().map_or(0, |e| e.seq.get());
    for from in 0..=last + 2 {
        for limit in [1usize, 2, 3, 7, 100] {
            let page = store.events_page(id, EventSeq(from), limit).await?;
            let expect: Vec<_> = all
                .iter()
                .filter(|e| e.seq.get() >= from)
                .cloned()
                .collect();
            let has_more = expect.len() > limit;
            let expect: Vec<_> = expect.into_iter().take(limit).collect();
            assert_eq!(page.events, expect, "from {from} limit {limit}");
            assert_eq!(page.has_more, has_more, "from {from} limit {limit}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn a_page_equals_the_full_read_for_a_plain_log() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = FileRunStore::new(dir.path());
    let id = RunId::new();
    store.create(&header(id)).await?;
    for seq in 1..=12 {
        store
            .append(&EventEnvelope::new(
                EventSeq(seq),
                id,
                datetime!(2026-01-01 00:00:00 UTC),
                EventKind::StateChanged,
                serde_json::json!({ "n": seq }),
            ))
            .await?;
    }
    assert_equivalent(&store, id).await
}

#[tokio::test]
async fn blank_lines_a_torn_tail_and_an_unknown_kind_do_not_change_the_answer() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = FileRunStore::new(dir.path());
    let id = RunId::new();
    let mut body = String::new();
    for seq in 1..=9u64 {
        let kind = if seq == 4 {
            "FROM_THE_FUTURE"
        } else {
            "STATE_CHANGED"
        };
        body.push_str(&line(id, seq, kind));
        body.push('\n');
        if seq % 3 == 0 {
            body.push_str("\n   \n"); // blank lines between events
        }
    }
    body.push_str("{\"seq\":10,\"run_id\":\"01"); // torn final line
    write_log(&store, id, &body).await?;

    let all = store.events(id, EventSeq::FIRST).await?;
    assert_eq!(all.len(), 9);
    assert_eq!(
        all[3].kind,
        EventKind::Unknown,
        "an unknown kind is tolerated"
    );
    assert_equivalent(&store, id).await
}

#[tokio::test]
async fn crlf_lines_and_a_missing_final_newline_are_handled() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = FileRunStore::new(dir.path());
    let id = RunId::new();
    let mut body = String::new();
    for seq in 1..=5u64 {
        body.push_str(&line(id, seq, "STATE_CHANGED"));
        body.push_str(if seq == 5 { "" } else { "\r\n" });
    }
    write_log(&store, id, &body).await?;
    assert_equivalent(&store, id).await
}

#[tokio::test]
async fn a_log_whose_lines_do_not_match_their_seqs_falls_back_to_the_full_read() -> TestResult {
    // Sequence numbers that skip: line 3 carries seq 7. The ranged read cannot trust
    // line == seq, so it must answer what the full read does.
    let dir = tempfile::tempdir()?;
    let store = FileRunStore::new(dir.path());
    let id = RunId::new();
    let mut body = String::new();
    for seq in [1u64, 2, 7, 8, 9] {
        body.push_str(&line(id, seq, "STATE_CHANGED"));
        body.push('\n');
    }
    write_log(&store, id, &body).await?;
    assert_equivalent(&store, id).await
}

#[tokio::test]
async fn corruption_inside_the_page_is_an_error_like_the_full_read() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = FileRunStore::new(dir.path());
    let id = RunId::new();
    let mut body = String::new();
    body.push_str(&line(id, 1, "STATE_CHANGED"));
    body.push('\n');
    body.push_str("this is not json\n");
    body.push_str(&line(id, 3, "STATE_CHANGED"));
    body.push('\n');
    write_log(&store, id, &body).await?;
    assert!(store.events(id, EventSeq::FIRST).await.is_err());
    assert!(store.events_page(id, EventSeq::FIRST, 10).await.is_err());
    Ok(())
}

#[tokio::test]
async fn a_missing_run_is_not_found() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = FileRunStore::new(dir.path());
    assert!(matches!(
        store.events_page(RunId::new(), EventSeq::FIRST, 5).await,
        Err(aftman_store::StoreError::NotFound(_))
    ));
    Ok(())
}

#[tokio::test]
async fn run_files_are_owner_only() -> TestResult {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = tempfile::tempdir()?;
    let store = FileRunStore::new(dir.path().join("state"));
    let id = RunId::new();
    store.create(&header(id)).await?;
    store
        .append(&EventEnvelope::new(
            EventSeq(1),
            id,
            datetime!(2026-01-01 00:00:00 UTC),
            EventKind::RunCreated,
            serde_json::json!({}),
        ))
        .await?;
    let state = store.load(id).await?;
    store.snapshot(&state).await?;
    let _claim = store.claim(id).await?;
    store
        .set_worker_process(
            id,
            aftman_core::ids::WorkerId::new(),
            Some(aftman_core::worker::ProcessIdentity {
                pgid: 99,
                start_token: None,
                boot_id: None,
                mission_file: None,
            }),
        )
        .await?;
    let paths = store.paths(id);
    for file in [
        &paths.meta,
        &paths.state,
        &paths.events,
        &paths.lock,
        &paths.processes,
    ] {
        let mode = std::fs::metadata(file)?.permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "{}", file.display());
    }
    assert_eq!(
        std::fs::metadata(&paths.dir)?.permissions().mode() & 0o777,
        0o700
    );
    Ok(())
}
