//! The on-disk run store.
//!
//! ```text
//! <root>/
//! ├─ runs/<run-id>/
//! │  ├─ meta.json      header written once at creation
//! │  ├─ state.json     latest snapshot, replaced atomically
//! │  └─ events.jsonl   append-only, one event per line
//! └─ index.sqlite      (M-later) run list queries when the directory grows
//! ```
//!
//! `events.jsonl` is the source of truth and `state.json` is a cache, so every load
//! validates the two against each other. An interrupted append leaves a torn final
//! line, which is recovered; anything else is corruption and is refused.

use std::path::{Path, PathBuf};

use async_trait::async_trait;

use aftman_core::event::EventEnvelope;
use aftman_core::ids::{EventSeq, RunId};
use aftman_core::run::{RunFilter, RunHeader, RunState, RunSummary};

use crate::{EventLogRead, RunStore, StoreError, io_err, parse_event_log, summarize, write_atomic};

/// Paths inside one run directory.
#[derive(Debug, Clone)]
pub struct RunPaths {
    pub dir: PathBuf,
    pub meta: PathBuf,
    pub state: PathBuf,
    pub events: PathBuf,
}

impl RunPaths {
    #[must_use]
    pub fn new(root: &Path, id: RunId) -> Self {
        let dir = root.join("runs").join(id.to_string());
        Self {
            meta: dir.join("meta.json"),
            state: dir.join("state.json"),
            events: dir.join("events.jsonl"),
            dir,
        }
    }
}

/// A run store backed by a directory.
#[derive(Debug, Clone)]
pub struct FileRunStore {
    root: PathBuf,
}

impl FileRunStore {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The default location, `$XDG_STATE_HOME/aftman` (or `~/.local/state/aftman`).
    ///
    /// Returns `None` when no home directory can be determined, which is a
    /// configuration error the caller reports rather than guessing a path.
    #[must_use]
    pub fn default_root() -> Option<PathBuf> {
        if let Some(state) = std::env::var_os("XDG_STATE_HOME") {
            return Some(PathBuf::from(state).join("aftman"));
        }
        std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/state/aftman"))
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    #[must_use]
    pub fn paths(&self, id: RunId) -> RunPaths {
        RunPaths::new(&self.root, id)
    }

    /// Append one line, durably.
    async fn append_line(&self, path: &Path, line: &str) -> Result<(), StoreError> {
        use tokio::io::AsyncWriteExt;
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .map_err(|e| io_err(path.display(), e))?;
        file.write_all(line.as_bytes())
            .await
            .map_err(|e| io_err(path.display(), e))?;
        file.write_all(b"\n")
            .await
            .map_err(|e| io_err(path.display(), e))?;
        // Durable before returning: a crash after a directive is applied but
        // before its event is on disk would leave an unexplainable history.
        file.sync_data()
            .await
            .map_err(|e| io_err(path.display(), e))?;
        Ok(())
    }

    /// Read a file, mapping a missing file to a clean error.
    ///
    /// The caller supplies the run id so that a genuinely absent run reports as
    /// `NotFound(run)` rather than as an I/O failure with a nil id.
    async fn read_run_file(&self, path: &Path, id: RunId) -> Result<Vec<u8>, StoreError> {
        tokio::fs::read(path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StoreError::NotFound(id)
            } else {
                io_err(path.display(), e)
            }
        })
    }
}

#[async_trait]
impl RunStore for FileRunStore {
    async fn create(&self, header: &RunHeader) -> Result<(), StoreError> {
        let paths = self.paths(header.id);
        if paths.meta.exists() {
            return Err(StoreError::AlreadyExists(header.id));
        }
        tokio::fs::create_dir_all(&paths.dir)
            .await
            .map_err(|e| io_err(paths.dir.display(), e))?;

        let encoded =
            serde_json::to_vec_pretty(header).map_err(|e| StoreError::Encode(e.to_string()))?;
        write_atomic(&paths.meta, &encoded).await?;

        // Start with an empty log so that readers never have to treat a missing
        // file as an empty one.
        let empty = RunState::new(header.clone(), aftman_core::job::FactoryStatus::Queued);
        let encoded =
            serde_json::to_vec_pretty(&empty).map_err(|e| StoreError::Encode(e.to_string()))?;
        write_atomic(&paths.state, &encoded).await?;
        write_atomic(&paths.events, b"").await?;
        Ok(())
    }

    async fn append(&self, event: &EventEnvelope) -> Result<(), StoreError> {
        let paths = self.paths(event.run_id);
        if !paths.events.exists() {
            return Err(StoreError::NotFound(event.run_id));
        }
        let line = serde_json::to_string(event).map_err(|e| StoreError::Encode(e.to_string()))?;
        self.append_line(&paths.events, &line).await
    }

    async fn snapshot(&self, state: &RunState) -> Result<(), StoreError> {
        let paths = self.paths(state.header.id);
        if !paths.meta.exists() {
            return Err(StoreError::NotFound(state.header.id));
        }
        let encoded =
            serde_json::to_vec_pretty(state).map_err(|e| StoreError::Encode(e.to_string()))?;
        write_atomic(&paths.state, &encoded).await
    }

    async fn load(&self, id: RunId) -> Result<RunState, StoreError> {
        let paths = self.paths(id);
        if !paths.meta.exists() {
            return Err(StoreError::NotFound(id));
        }
        let state_bytes = self.read_run_file(&paths.state, id).await?;
        let state: RunState =
            serde_json::from_slice(&state_bytes).map_err(|e| StoreError::Encode(e.to_string()))?;

        // Validate against the log. A snapshot may be *behind* the log — that is the
        // normal state between an append and the next snapshot — but it may never be
        // ahead, because that would describe events that do not exist.
        let read = self.read_log(id).await?;
        let log_seq = read.last_seq();
        if state.last_seq > log_seq {
            return Err(StoreError::SnapshotMismatch {
                run_id: id,
                snapshot: state.last_seq.get(),
                log: log_seq.get(),
            });
        }
        Ok(state)
    }

    async fn events(
        &self,
        id: RunId,
        from_seq: EventSeq,
    ) -> Result<Vec<EventEnvelope>, StoreError> {
        let read = self.read_log(id).await?;
        Ok(read
            .events
            .into_iter()
            .filter(|e| e.seq >= from_seq)
            .collect())
    }

    async fn list(&self, filter: &RunFilter) -> Result<Vec<RunSummary>, StoreError> {
        let runs_dir = self.root.join("runs");
        let mut entries = match tokio::fs::read_dir(&runs_dir).await {
            Ok(entries) => entries,
            // No runs directory yet is an empty list, not an error.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(io_err(runs_dir.display(), error)),
        };

        let mut summaries: Vec<RunSummary> = Vec::new();
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| io_err(runs_dir.display(), e))?
        {
            let name = entry.file_name().to_string_lossy().into_owned();
            let Ok(id) = name.parse::<RunId>() else {
                // Not a run directory; skip rather than fail a listing over a stray
                // file someone dropped in the state directory.
                continue;
            };
            // A missing or unreadable snapshot means the run directory is
            // incomplete. Skipping keeps `aftman ls` usable; the run itself will
            // still fail loudly if someone asks for it by id.
            if let Ok(state) = self.load(id).await {
                summaries.push(summarize(&state));
            }
        }

        summaries.retain(|s| filter.matching(s));
        summaries.sort_by_key(|s| core::cmp::Reverse(s.id));
        if let Some(limit) = filter.limit {
            summaries.truncate(limit);
        }
        Ok(summaries)
    }

    async fn read_log(&self, id: RunId) -> Result<EventLogRead, StoreError> {
        let paths = self.paths(id);
        let bytes = match tokio::fs::read(&paths.events).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(StoreError::NotFound(id));
            }
            Err(error) => return Err(io_err(paths.events.display(), error)),
        };
        parse_event_log(id, &bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::FileRunStore;
    use crate::RunStore;
    use aftman_core::event::{EventEnvelope, EventKind};
    use aftman_core::ids::{EventSeq, RunId};
    use aftman_core::job::{FactoryStatus, Job, JobSource, RunConfig};
    use aftman_core::run::{RunFilter, RunHeader};
    use std::path::PathBuf;
    use time::macros::datetime;

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

    fn store() -> Result<(tempfile::TempDir, FileRunStore), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let store = FileRunStore::new(dir.path());
        Ok((dir, store))
    }

    #[tokio::test]
    async fn create_writes_every_file_a_reader_needs() -> Result<(), Box<dyn std::error::Error>> {
        let (_dir, store) = store()?;
        let id = RunId::new();
        store.create(&header(id)).await?;
        let paths = store.paths(id);
        assert!(paths.meta.exists());
        assert!(paths.state.exists());
        assert!(paths.events.exists(), "an empty log beats a missing one");
        Ok(())
    }

    #[tokio::test]
    async fn events_survive_a_reopen_and_arrive_in_order() -> Result<(), Box<dyn std::error::Error>>
    {
        let (dir, store) = store()?;
        let id = RunId::new();
        store.create(&header(id)).await?;
        for seq in 1..=3 {
            store.append(&event(id, seq)).await?;
        }

        // A fresh instance over the same directory, as a restarted daemon would do.
        let reopened = FileRunStore::new(dir.path());
        let events = reopened.events(id, EventSeq::FIRST).await?;
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].seq, EventSeq(1));
        assert_eq!(events[2].seq, EventSeq(3));
        Ok(())
    }

    #[tokio::test]
    async fn a_torn_final_line_is_recovered_from_disk() -> Result<(), Box<dyn std::error::Error>> {
        // The M0 exit criterion. Simulate an interrupted append by truncating the
        // last line mid-write.
        let (dir, store) = store()?;
        let id = RunId::new();
        store.create(&header(id)).await?;
        for seq in 1..=2 {
            store.append(&event(id, seq)).await?;
        }

        let events_path = FileRunStore::new(dir.path()).paths(id).events;
        let mut contents = tokio::fs::read_to_string(&events_path).await?;
        contents.push_str("{\"seq\":3,\"run_id\":\"0193");
        tokio::fs::write(&events_path, contents).await?;

        let reopened = FileRunStore::new(dir.path());
        let read = reopened.read_log(id).await?;
        assert_eq!(read.events.len(), 2, "the complete events survive");
        assert_eq!(read.torn_lines.len(), 1);
        assert_eq!(read.torn_lines[0].line_number, 3);

        // And the store stays usable: appending after a torn line works, because
        // the torn line is the last thing in the file.
        reopened.append(&event(id, 3)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_torn_line_does_not_break_state_loading() -> Result<(), Box<dyn std::error::Error>> {
        let (dir, store) = store()?;
        let id = RunId::new();
        store.create(&header(id)).await?;
        store.append(&event(id, 1)).await?;
        let mut state = store.load(id).await?;
        state.last_seq = EventSeq(1);
        state.status = FactoryStatus::Running;
        store.snapshot(&state).await?;

        let events_path = store.paths(id).events;
        let mut contents = tokio::fs::read_to_string(&events_path).await?;
        contents.push_str("{\"seq\":2,\"run");
        tokio::fs::write(&events_path, contents).await?;

        let reopened = FileRunStore::new(dir.path());
        let loaded = reopened.load(id).await?;
        assert_eq!(loaded.status, FactoryStatus::Running);
        assert_eq!(loaded.last_seq, EventSeq(1));
        Ok(())
    }

    #[tokio::test]
    async fn a_snapshot_ahead_of_the_log_is_refused() -> Result<(), Box<dyn std::error::Error>> {
        let (_dir, store) = store()?;
        let id = RunId::new();
        store.create(&header(id)).await?;
        store.append(&event(id, 1)).await?;

        let mut state = store.load(id).await?;
        state.last_seq = EventSeq(7);
        store.snapshot(&state).await?;

        let error = store.load(id).await.expect_err("snapshot ahead of log");
        assert!(matches!(
            error,
            crate::StoreError::SnapshotMismatch {
                snapshot: 7,
                log: 1,
                ..
            }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn atomic_snapshot_leaves_no_temp_file_behind() -> Result<(), Box<dyn std::error::Error>>
    {
        let (_dir, store) = store()?;
        let id = RunId::new();
        store.create(&header(id)).await?;
        let state = store.load(id).await?;
        store.snapshot(&state).await?;

        let paths = store.paths(id);
        assert!(!paths.state.with_extension("tmp").exists());
        Ok(())
    }

    #[tokio::test]
    async fn list_scans_the_directory_and_ignores_stray_entries()
    -> Result<(), Box<dyn std::error::Error>> {
        let (dir, store) = store()?;
        let first = RunId::new();
        let second = RunId::new();
        store.create(&header(first)).await?;
        store.create(&header(second)).await?;

        // A stray file must not break the listing.
        tokio::fs::write(dir.path().join("runs").join("not-a-run"), b"junk").await?;

        let all = store.list(&RunFilter::default()).await?;
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].id, second, "newest first");
        Ok(())
    }

    #[tokio::test]
    async fn an_absent_runs_directory_lists_as_empty() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let store = FileRunStore::new(dir.path().join("nothing-here"));
        assert!(store.list(&RunFilter::default()).await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn an_unknown_run_is_not_found() -> Result<(), Box<dyn std::error::Error>> {
        let (_dir, store) = store()?;
        let error = store.load(RunId::new()).await.expect_err("unknown run");
        assert!(matches!(error, crate::StoreError::NotFound(_)));
        Ok(())
    }

    #[tokio::test]
    async fn appending_to_an_unknown_run_is_not_found() -> Result<(), Box<dyn std::error::Error>> {
        let (_dir, store) = store()?;
        let id = RunId::new();
        let error = store.append(&event(id, 1)).await.expect_err("unknown run");
        assert!(matches!(error, crate::StoreError::NotFound(_)));
        Ok(())
    }
}
