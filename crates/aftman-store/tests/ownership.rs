//! Deleting runs, and the ownership claim that keeps two processes off one log.
//! Both stores must behave the same way, so each test runs against both.

use std::path::PathBuf;

use aftman_core::event::{EventEnvelope, EventKind};
use aftman_core::ids::{EventSeq, RunId};
use aftman_core::job::{Job, JobSource, RunConfig};
use aftman_core::run::{RunFilter, RunHeader};
use aftman_store::{FileRunStore, MemoryRunStore, RunStore, StoreError};
use time::macros::datetime;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn header(id: RunId) -> RunHeader {
    RunHeader {
        id,
        created_at: datetime!(2026-01-01 00:00:00 UTC),
        job: Job {
            prompt: "add retries".into(),
            repo: PathBuf::from("/repo"),
            source: JobSource::Cli,
        },
        config: RunConfig::default(),
        aftman_version: "0.1.0".into(),
        worker_versions: Default::default(),
        granted_key_names: Vec::new(),
    }
}

fn event(id: RunId, seq: u64) -> EventEnvelope {
    EventEnvelope::new(
        EventSeq(seq),
        id,
        datetime!(2026-01-01 00:00:00 UTC),
        EventKind::StateChanged,
        serde_json::json!({ "to": "running" }),
    )
}

type StoreCase = (&'static str, Option<tempfile::TempDir>, Box<dyn RunStore>);

/// Each store kind, with the directory (if any) that must outlive it.
fn stores() -> Result<Vec<StoreCase>, std::io::Error> {
    let dir = tempfile::tempdir()?;
    let file = FileRunStore::new(dir.path());
    Ok(vec![
        ("file", Some(dir), Box::new(file)),
        ("memory", None, Box::new(MemoryRunStore::new())),
    ])
}

#[tokio::test]
async fn delete_removes_a_run_completely() -> TestResult {
    for (name, _dir, store) in stores()? {
        let (keep, gone) = (RunId::new(), RunId::new());
        for id in [keep, gone] {
            store.create(&header(id)).await?;
            store.append(&event(id, 1)).await?;
        }
        store.delete(gone).await?;

        assert!(
            matches!(store.load(gone).await, Err(StoreError::NotFound(_))),
            "{name}: load after delete"
        );
        assert!(matches!(
            store.events(gone, EventSeq::FIRST).await,
            Err(StoreError::NotFound(_))
        ));
        assert_eq!(store.ids().await?, vec![keep], "{name}: only the other run");
        assert_eq!(store.list(&RunFilter::default()).await?.len(), 1);
        assert!(
            matches!(store.delete(gone).await, Err(StoreError::NotFound(_))),
            "{name}: deleting twice is NotFound"
        );
        // The other run is untouched.
        assert_eq!(store.events(keep, EventSeq::FIRST).await?.len(), 1);
    }
    Ok(())
}

#[tokio::test]
async fn file_delete_removes_the_directory_from_disk() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = FileRunStore::new(dir.path());
    let id = RunId::new();
    store.create(&header(id)).await?;
    let claim = store.claim(id).await?;
    assert!(store.paths(id).dir.exists());
    store.delete(id).await?;
    assert!(!store.paths(id).dir.exists(), "no soft delete");
    drop(claim);
    Ok(())
}

#[tokio::test]
async fn a_claim_is_exclusive_and_released_on_drop() -> TestResult {
    for (name, _dir, store) in stores()? {
        let id = RunId::new();
        store.create(&header(id)).await?;

        let first = store.claim(id).await?;
        assert!(first.is_some(), "{name}: the first claim wins");
        assert!(
            store.claim(id).await?.is_none(),
            "{name}: a second claim while held is refused"
        );
        // A different run is independent.
        let other = RunId::new();
        store.create(&header(other)).await?;
        assert!(store.claim(other).await?.is_some());

        drop(first);
        assert!(
            store.claim(id).await?.is_some(),
            "{name}: released when dropped"
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_second_store_over_the_same_directory_sees_the_claim() -> TestResult {
    // Stands in for a second process: a separate store value, and (for flock) a
    // separate open file description, exactly as another process would have.
    let dir = tempfile::tempdir()?;
    let mine = FileRunStore::new(dir.path());
    let theirs = FileRunStore::new(dir.path());
    let id = RunId::new();
    mine.create(&header(id)).await?;

    let claim = mine.claim(id).await?.expect("first claim");
    assert!(theirs.claim(id).await?.is_none());
    drop(claim);
    assert!(theirs.claim(id).await?.is_some());
    Ok(())
}

#[tokio::test]
async fn a_claim_can_be_taken_before_the_run_exists() -> TestResult {
    // The runtime claims first, so there is never a visible run without an owner.
    for (name, _dir, store) in stores()? {
        let id = RunId::new();
        let claim = store.claim_new(id).await?;
        assert!(claim.is_some(), "{name}");
        store.create(&header(id)).await?;
        assert!(store.claim(id).await?.is_none(), "{name}: still held");
        store.append(&event(id, 1)).await?;
    }
    Ok(())
}

#[tokio::test]
async fn ids_include_runs_that_cannot_be_loaded() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = FileRunStore::new(dir.path());
    let good = RunId::new();
    let broken = RunId::new();
    store.create(&header(good)).await?;
    store.create(&header(broken)).await?;
    tokio::fs::write(store.paths(broken).state, b"{ not json").await?;

    let mut ids = store.ids().await?;
    ids.sort();
    let mut expected = vec![good, broken];
    expected.sort();
    assert_eq!(ids, expected, "ids sees the damaged run");
    assert_eq!(
        store.list(&RunFilter::default()).await?.len(),
        1,
        "list skips it"
    );
    Ok(())
}

#[tokio::test]
async fn appending_after_a_torn_line_does_not_corrupt_the_log() -> TestResult {
    // A process killed mid-append leaves a fragment with no newline.
    let dir = tempfile::tempdir()?;
    let store = FileRunStore::new(dir.path());
    let id = RunId::new();
    store.create(&header(id)).await?;
    store.append(&event(id, 1)).await?;
    let path = store.paths(id).events;
    let mut text = tokio::fs::read_to_string(&path).await?;
    text.push_str("{\"seq\":2,\"run_id\":\"01");
    tokio::fs::write(&path, text).await?;

    store.append(&event(id, 2)).await?;
    store.append(&event(id, 3)).await?;
    let read = store.read_log(id).await?;
    let seqs: Vec<u64> = read.events.iter().map(|e| e.seq.get()).collect();
    assert_eq!(seqs, vec![1, 2, 3]);
    assert!(read.torn_lines.is_empty());
    Ok(())
}

#[tokio::test]
async fn a_complete_final_line_without_a_newline_is_kept() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = FileRunStore::new(dir.path());
    let id = RunId::new();
    store.create(&header(id)).await?;
    let path = store.paths(id).events;
    tokio::fs::write(&path, serde_json::to_string(&event(id, 1))?).await?;

    store.append(&event(id, 2)).await?;
    let read = store.read_log(id).await?;
    assert_eq!(read.events.len(), 2, "the complete line survives");
    assert!(read.torn_lines.is_empty());
    Ok(())
}

#[tokio::test]
async fn a_claim_on_a_missing_or_deleted_run_creates_nothing() -> TestResult {
    for (name, _dir, store) in stores()? {
        let ghost = RunId::new();
        assert!(
            matches!(store.claim(ghost).await, Err(StoreError::NotFound(_))),
            "{name}: claiming a run that never existed"
        );
        assert!(
            store.ids().await?.is_empty(),
            "{name}: and it created nothing"
        );

        let id = RunId::new();
        store.create(&header(id)).await?;
        let held = store.claim(id).await?.expect("claim");
        store.delete(id).await?;
        drop(held);
        assert!(
            matches!(store.claim(id).await, Err(StoreError::NotFound(_))),
            "{name}: a deleted run cannot be claimed back into existence"
        );
        assert!(store.ids().await?.is_empty(), "{name}");
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_claims_and_deletes_never_double_own_or_error() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = std::sync::Arc::new(FileRunStore::new(dir.path()));
    let mut ids = Vec::new();
    for _ in 0..40 {
        let id = RunId::new();
        store.create(&header(id)).await?;
        ids.push(id);
    }
    // Two purgers race over every run, each claiming then deleting.
    let purger = |store: std::sync::Arc<FileRunStore>, ids: Vec<RunId>| async move {
        let mut deleted = 0;
        for id in ids {
            match store.claim(id).await {
                Ok(Some(_claim)) => match store.delete(id).await {
                    Ok(()) => deleted += 1,
                    Err(StoreError::NotFound(_)) => {}
                    Err(other) => return Err(other),
                },
                Ok(None) | Err(StoreError::NotFound(_)) => {}
                Err(other) => return Err(other),
            }
        }
        Ok::<_, StoreError>(deleted)
    };
    let (a, b) = tokio::join!(
        tokio::spawn(purger(store.clone(), ids.clone())),
        tokio::spawn(purger(store.clone(), ids.iter().rev().copied().collect()))
    );
    assert_eq!(a?? + b??, 40, "each run deleted exactly once, no error");
    assert!(store.ids().await?.is_empty());
    assert_eq!(
        std::fs::read_dir(dir.path().join("runs"))?.count(),
        0,
        "no tombstones"
    );
    Ok(())
}

#[tokio::test]
async fn a_stale_write_cannot_duplicate_a_seq_or_roll_the_snapshot_back() -> TestResult {
    for (name, _dir, store) in stores()? {
        let id = RunId::new();
        store.create(&header(id)).await?;
        store.append(&event(id, 1)).await?;
        store.append(&event(id, 2)).await?;
        let dup = store.append(&event(id, 2)).await;
        assert!(
            matches!(dup, Err(StoreError::SeqConflict { last: 2, .. })),
            "{name}: {dup:?}"
        );
        assert_eq!(store.read_log(id).await?.events.len(), 2);
    }
    // The file store drops a snapshot older than the stored one.
    let dir = tempfile::tempdir()?;
    let store = FileRunStore::new(dir.path());
    let id = RunId::new();
    store.create(&header(id)).await?;
    store.append(&event(id, 1)).await?;
    store.append(&event(id, 2)).await?;
    let mut newer = store.load(id).await?;
    newer.last_seq = EventSeq(2);
    newer.status = aftman_core::job::FactoryStatus::Failed;
    store.snapshot(&newer).await?;
    let mut stale = newer.clone();
    stale.last_seq = EventSeq(1);
    stale.status = aftman_core::job::FactoryStatus::Running;
    store.snapshot(&stale).await?;
    assert_eq!(
        store.load(id).await?.status,
        aftman_core::job::FactoryStatus::Failed,
        "the older snapshot was dropped"
    );
    Ok(())
}

#[tokio::test]
async fn the_file_store_creates_directories_private_but_leaves_existing_ones() -> TestResult {
    use std::os::unix::fs::PermissionsExt as _;
    let mode = |p: &std::path::Path| std::fs::metadata(p).map(|m| m.permissions().mode() & 0o777);

    // A root that does not exist yet: everything aftman creates is 0700.
    let outer = tempfile::tempdir()?;
    let root = outer.path().join("fresh").join("state");
    let store = FileRunStore::new(&root);
    let id = RunId::new();
    store.create(&header(id)).await?;
    assert_eq!(mode(&root)?, 0o700);
    assert_eq!(mode(&root.join("runs"))?, 0o700);
    assert_eq!(mode(&store.paths(id).dir)?, 0o700);

    // A claim is the first creator for a run started by the runtime.
    let claimed = RunId::new();
    let _claim = store.claim_new(claimed).await?;
    assert_eq!(mode(&store.paths(claimed).dir)?, 0o700);

    // A root the user made keeps its mode.
    let theirs = outer.path().join("theirs");
    std::fs::create_dir(&theirs)?;
    std::fs::set_permissions(&theirs, std::fs::Permissions::from_mode(0o755))?;
    FileRunStore::new(&theirs)
        .create(&header(RunId::new()))
        .await?;
    assert_eq!(mode(&theirs)?, 0o755, "not chmodded");
    assert_eq!(
        mode(&theirs.join("runs"))?,
        0o700,
        "but what we create inside is private"
    );
    Ok(())
}

#[tokio::test]
async fn ends_with_run_finished_reads_the_end_of_the_log() -> TestResult {
    for (name, _dir, store) in stores()? {
        let id = RunId::new();
        store.create(&header(id)).await?;
        assert!(
            !store.ends_with_run_finished(id).await?,
            "{name}: empty log"
        );
        store.append(&event(id, 1)).await?;
        assert!(!store.ends_with_run_finished(id).await?, "{name}");
        let finished = EventEnvelope::new(
            EventSeq(2),
            id,
            datetime!(2026-01-01 00:00:00 UTC),
            EventKind::RunFinished,
            serde_json::json!({ "status": "finished" }),
        );
        store.append(&finished).await?;
        assert!(store.ends_with_run_finished(id).await?, "{name}");
        assert!(matches!(
            store.ends_with_run_finished(RunId::new()).await,
            Err(StoreError::NotFound(_))
        ));
    }
    Ok(())
}
