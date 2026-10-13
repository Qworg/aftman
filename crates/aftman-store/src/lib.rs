//! Append-only run storage.
//!
//! Two invariants make this crate worth its own file:
//!
//! 1. **The event log is the source of truth.** `state.json` is a cache. A snapshot
//!    whose `last_seq` is ahead of the log, or behind it with no trailing events, is
//!    corruption — and this crate refuses to guess rather than resuming from a state
//!    it cannot justify.
//! 2. **A torn write is recoverable, corruption is not.** A half-written *final*
//!    line is an interrupted write and is dropped with a report. A malformed line
//!    anywhere else is real corruption and is an error, because silently skipping
//!    the middle of a history is how a replay quietly produces the wrong answer.
//!
//! See `docs/02-lld.md` §8.

#![forbid(unsafe_code)]

pub mod file;
pub mod memory;

use async_trait::async_trait;

use aftman_core::event::EventEnvelope;
use aftman_core::ids::{EventSeq, RunId};
use aftman_core::job::FactoryStatus;
use aftman_core::run::{RunFilter, RunHeader, RunState, RunSummary};

pub use file::{FileRunStore, RunPaths};
pub use memory::MemoryRunStore;

/// Errors from the store.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("run {0} not found")]
    NotFound(RunId),

    #[error("I/O error at {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("run {run_id} event log is corrupt at line {line}: {detail}")]
    CorruptLog {
        run_id: RunId,
        line: usize,
        detail: String,
    },

    /// The snapshot and the log disagree in a way that cannot be explained by an
    /// interrupted write. Refusing is the only honest response.
    #[error("run {run_id} snapshot/log mismatch: snapshot seq {snapshot}, log seq {log}")]
    SnapshotMismatch {
        run_id: RunId,
        snapshot: u64,
        log: u64,
    },

    #[error("run {0} already exists")]
    AlreadyExists(RunId),

    /// An append whose `seq` is not greater than the log's last. Another writer got
    /// there first (for example a task that was aborted mid-write); the caller
    /// re-reads the log and continues from what is really there.
    #[error("run {run_id}: event seq {seq} does not follow the log's last seq {last}")]
    SeqConflict { run_id: RunId, seq: u64, last: u64 },

    #[error("could not serialize run data: {0}")]
    Encode(String),
}

fn io_err(path: impl std::fmt::Display, source: std::io::Error) -> StoreError {
    StoreError::Io {
        path: path.to_string(),
        source,
    }
}

/// A read of an event log, including what had to be discarded.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EventLogRead {
    pub events: Vec<EventEnvelope>,
    /// Trailing lines that could not be parsed. At most one, and only at the end.
    pub torn_lines: Vec<TornLine>,
}

/// A trailing, unparseable line: an interrupted write, not corruption.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TornLine {
    pub line_number: usize,
    pub bytes: usize,
}

impl EventLogRead {
    /// The highest sequence number present, or `EventSeq(0)` for an empty log.
    #[must_use]
    pub fn last_seq(&self) -> EventSeq {
        self.events.last().map_or(EventSeq(0), |e| e.seq)
    }
}

/// Parse a newline-delimited event log.
///
/// The only tolerated defect is a malformed **final** line, which is what an
/// interrupted append looks like. Anything else is reported as corruption.
pub fn parse_event_log(run_id: RunId, bytes: &[u8]) -> Result<EventLogRead, StoreError> {
    let text = String::from_utf8_lossy(bytes);
    let lines: Vec<&str> = text.split('\n').collect();
    // A trailing newline produces one empty final element; that is well-formed.
    let last_index = lines.len().saturating_sub(1);

    let mut events = Vec::new();
    let mut torn_lines = Vec::new();

    for (index, raw) in lines.iter().enumerate() {
        let line = raw.trim_end_matches('\r');
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<EventEnvelope>(line) {
            Ok(event) => events.push(event),
            Err(error) => {
                if index == last_index {
                    // Interrupted append: the last line was never completed.
                    torn_lines.push(TornLine {
                        line_number: index + 1,
                        bytes: line.len(),
                    });
                } else {
                    return Err(StoreError::CorruptLog {
                        run_id,
                        line: index + 1,
                        detail: error.to_string(),
                    });
                }
            }
        }
    }

    Ok(EventLogRead { events, torn_lines })
}

/// Atomic replacement of a file: write a sibling temp file, flush it, then rename.
///
/// `rename` within a directory is atomic on the platforms aftman targets, so a
/// reader either sees the whole previous snapshot or the whole new one, never a
/// truncated mixture.
pub async fn write_atomic(path: &std::path::Path, bytes: &[u8]) -> Result<(), StoreError> {
    let parent = path
        .parent()
        .ok_or_else(|| io_err(path.display(), std::io::Error::other("path has no parent")))?;
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(|e| io_err(parent.display(), e))?;

    let temp = path.with_extension("tmp");
    {
        use tokio::io::AsyncWriteExt;
        let mut file = tokio::fs::File::create(&temp)
            .await
            .map_err(|e| io_err(temp.display(), e))?;
        file.write_all(bytes)
            .await
            .map_err(|e| io_err(temp.display(), e))?;
        file.flush().await.map_err(|e| io_err(temp.display(), e))?;
        file.sync_all()
            .await
            .map_err(|e| io_err(temp.display(), e))?;
    }
    tokio::fs::rename(&temp, path)
        .await
        .map_err(|e| io_err(path.display(), e))?;

    // Fsync the directory so the rename itself is durable, not just the file
    // contents. Without this a crash can leave the old name pointing at nothing.
    // Not all platforms permit opening a directory; where it fails, the file
    // contents are still durable and only the rename ordering is at risk.
    if let Ok(dir) = std::fs::File::open(parent) {
        let _ = dir.sync_all();
    }
    Ok(())
}

/// Storage for runs.
#[async_trait]
pub trait RunStore: Send + Sync {
    /// Create a run directory and write its header.
    async fn create(&self, header: &RunHeader) -> Result<(), StoreError>;

    /// Append one event. Must be durable before it returns.
    async fn append(&self, event: &EventEnvelope) -> Result<(), StoreError>;

    /// Replace the state snapshot atomically.
    async fn snapshot(&self, state: &RunState) -> Result<(), StoreError>;

    /// Load a run's state, validating it against the log.
    async fn load(&self, id: RunId) -> Result<RunState, StoreError>;

    /// Read events at or after `from_seq`.
    async fn events(&self, id: RunId, from_seq: EventSeq)
    -> Result<Vec<EventEnvelope>, StoreError>;

    /// List runs, newest first.
    async fn list(&self, filter: &RunFilter) -> Result<Vec<RunSummary>, StoreError>;

    /// Read the whole event log, reporting any torn trailing line.
    async fn read_log(&self, id: RunId) -> Result<EventLogRead, StoreError>;

    /// Delete one run's data completely (`aftman purge <run-id>`; no soft delete).
    ///
    /// The caller is responsible for not deleting a run somebody is driving:
    /// take a [`RunStore::claim`] first, as the daemon's `run.purge` does.
    ///
    /// # Errors
    /// [`StoreError::NotFound`] when the run does not exist.
    async fn delete(&self, id: RunId) -> Result<(), StoreError>;

    /// Every run id the store holds, including runs whose data cannot be loaded.
    ///
    /// [`RunStore::list`] skips unreadable runs so that `aftman ls` stays usable;
    /// recovery and purge need to see them too. Order is unspecified.
    async fn ids(&self) -> Result<Vec<RunId>, StoreError>;

    /// The status in a run's snapshot, without validating it against the log.
    /// Cheap (no log read), and only a hint: the snapshot may trail the log.
    async fn snapshot_status(&self, id: RunId) -> Result<FactoryStatus, StoreError> {
        self.load(id).await.map(|state| state.status)
    }

    /// Whether the log's last event is `RUN_FINISHED`. The file store reads only the
    /// end of the file, so a daemon starting over many large finished runs does not
    /// have to read all of their logs.
    async fn ends_with_run_finished(&self, id: RunId) -> Result<bool, StoreError> {
        let log = self.read_log(id).await?;
        Ok(log
            .events
            .last()
            .is_some_and(|e| e.kind == aftman_core::event::EventKind::RunFinished))
    }

    /// Claim the right to write to a run: an exclusive advisory lock held for the
    /// life of the returned [`RunClaim`].
    ///
    /// Exactly one process may drive a run, and two writers appending to one log
    /// corrupt it. The process that drives a run (`FactoryRuntime::start` through
    /// `drive`) holds its claim; daemon restart recovery and `run.purge` take the
    /// claim before touching a run, so a run alive in *another process* sharing the
    /// state directory is never failed or deleted out from under it. The claim is
    /// released when the value is dropped, and by the operating system if the
    /// process dies.
    ///
    /// Returns `Ok(None)` when someone else holds the claim, and
    /// [`StoreError::NotFound`] when the run does not exist (or was deleted while
    /// the claim was being taken). It never creates anything: only
    /// [`RunStore::claim_new`] does, so a claim on a run that is being purged cannot
    /// resurrect it.
    async fn claim(&self, id: RunId) -> Result<Option<RunClaim>, StoreError>;

    /// Record (or, with `None`, clear) the process a worker of this run launched, so
    /// recovery can end it if the supervisor dies. Never creates the run directory.
    /// The default does nothing, for stores that cannot persist it.
    async fn set_worker_process(
        &self,
        run: RunId,
        worker: aftman_core::ids::WorkerId,
        identity: Option<aftman_core::worker::ProcessIdentity>,
    ) -> Result<(), StoreError> {
        let _ = (run, worker, identity);
        Ok(())
    }

    /// The processes recorded and not yet cleared for this run.
    async fn worker_processes(
        &self,
        run: RunId,
    ) -> Result<
        Vec<(
            aftman_core::ids::WorkerId,
            aftman_core::worker::ProcessIdentity,
        )>,
        StoreError,
    > {
        let _ = run;
        Ok(Vec::new())
    }

    /// Claim a run that does not exist yet, before [`RunStore::create`]. This is how
    /// the runtime avoids a window in which a run exists but is unowned. For the file
    /// store it creates the run directory (`0700`).
    async fn claim_new(&self, id: RunId) -> Result<Option<RunClaim>, StoreError>;
}

/// Exclusive ownership of one run, released on drop. See [`RunStore::claim`].
pub struct RunClaim {
    run_id: RunId,
    _guard: Box<dyn Send + Sync>,
}

impl RunClaim {
    /// Wrap whatever keeps the claim alive. Dropping the guard must release it.
    #[must_use]
    pub fn new(run_id: RunId, guard: impl Send + Sync + 'static) -> Self {
        Self {
            run_id,
            _guard: Box::new(guard),
        }
    }

    #[must_use]
    pub fn run_id(&self) -> RunId {
        self.run_id
    }
}

impl std::fmt::Debug for RunClaim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunClaim")
            .field("run_id", &self.run_id)
            .finish_non_exhaustive()
    }
}

/// Build a [`RunSummary`] from state. Shared so both stores report the same shape.
#[must_use]
pub fn summarize(state: &RunState) -> RunSummary {
    RunSummary {
        id: state.header.id,
        status: state.status,
        created_at: state.header.created_at,
        elapsed_secs: state.counters.elapsed_secs,
        job_excerpt: state
            .header
            .job
            .prompt
            .chars()
            .take(200)
            .collect::<String>(),
        repo: state.header.job.repo.clone(),
        worker: state.header.config.worker,
        last_directive: state.last_directive.as_ref().map(|d| d.name().to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::{EventLogRead, TornLine, parse_event_log};
    use aftman_core::event::{EventEnvelope, EventKind};
    use aftman_core::ids::{EventSeq, RunId};
    use time::macros::datetime;

    fn event(seq: u64) -> EventEnvelope {
        EventEnvelope::new(
            EventSeq(seq),
            RunId::new(),
            datetime!(2026-01-01 00:00:00 UTC),
            EventKind::StateChanged,
            serde_json::json!({ "to": "running" }),
        )
    }

    fn log_of(events: &[EventEnvelope]) -> Result<String, serde_json::Error> {
        let mut out = String::new();
        for event in events {
            out.push_str(&serde_json::to_string(event)?);
            out.push('\n');
        }
        Ok(out)
    }

    #[test]
    fn a_well_formed_log_parses_completely() -> Result<(), Box<dyn std::error::Error>> {
        let events = vec![event(1), event(2), event(3)];
        let read = parse_event_log(RunId::new(), log_of(&events)?.as_bytes())?;
        assert_eq!(read.events, events);
        assert!(read.torn_lines.is_empty());
        assert_eq!(read.last_seq(), EventSeq(3));
        Ok(())
    }

    #[test]
    fn a_truncated_final_line_is_recovered() -> Result<(), Box<dyn std::error::Error>> {
        // Exactly what an interrupted append looks like: a half-written line.
        let partial = "{\"seq\":3,\"run_id\":\"0193";
        let mut text = log_of(&[event(1), event(2)])?;
        text.push_str(partial);

        let read = parse_event_log(RunId::new(), text.as_bytes())?;
        assert_eq!(read.events.len(), 2, "the complete events survive");
        assert_eq!(read.last_seq(), EventSeq(2));
        assert_eq!(
            read.torn_lines,
            vec![TornLine {
                line_number: 3,
                bytes: partial.len(),
            }]
        );
        Ok(())
    }

    #[test]
    fn an_empty_log_reads_as_empty() -> Result<(), Box<dyn std::error::Error>> {
        let read = parse_event_log(RunId::new(), b"")?;
        assert_eq!(read, EventLogRead::default());
        assert_eq!(read.last_seq(), EventSeq(0));
        Ok(())
    }

    #[test]
    fn a_missing_trailing_newline_is_not_a_torn_line() -> Result<(), Box<dyn std::error::Error>> {
        let text = serde_json::to_string(&event(1))?;
        let read = parse_event_log(RunId::new(), text.as_bytes())?;
        assert_eq!(read.events.len(), 1);
        assert!(
            read.torn_lines.is_empty(),
            "a complete final line needs no newline"
        );
        Ok(())
    }

    #[test]
    fn corruption_in_the_middle_is_an_error_not_a_skip() -> Result<(), Box<dyn std::error::Error>> {
        // Silently skipping a middle line would make a replay quietly wrong, which
        // is far worse than refusing to start.
        let mut text = log_of(&[event(1), event(2)])?;
        text.push_str("this is not json\n");
        text.push_str(&serde_json::to_string(&event(4))?);
        text.push('\n');

        let error = parse_event_log(RunId::new(), text.as_bytes())
            .expect_err("middle corruption must not be skipped");
        assert!(matches!(
            error,
            super::StoreError::CorruptLog { line: 3, .. }
        ));
        Ok(())
    }

    #[test]
    fn blank_lines_are_ignored() -> Result<(), Box<dyn std::error::Error>> {
        let mut text = log_of(&[event(1)])?;
        text.push('\n');
        text.push('\n');
        text.push_str(&log_of(&[event(2)])?);
        let read = parse_event_log(RunId::new(), text.as_bytes())?;
        assert_eq!(read.events.len(), 2);
        assert!(read.torn_lines.is_empty());
        Ok(())
    }
}
