//! The on-disk run store.
//!
//! ```text
//! <root>/                      (0700 when aftman creates it)
//! ├─ runs/<run-id>/            (0700)
//! │  ├─ meta.json      header written once at creation
//! │  ├─ state.json     latest snapshot, replaced atomically
//! │  ├─ events.jsonl   append-only, one event per line
//! │  └─ run.lock       advisory lock, held by the process driving the run
//! ├─ runs/.deleted-*   tombstones of runs being purged (ignored, then removed)
//! └─ index.sqlite      (M-later) run list queries when the directory grows
//! ```
//!
//! `events.jsonl` is the source of truth and `state.json` is a cache, so every load
//! validates the two against each other. An interrupted append leaves a torn final
//! line, which is recovered; anything else is corruption and is refused.
//!
//! # Writes are serialized per run, on blocking threads
//!
//! `append` and `snapshot` run as one `spawn_blocking` closure that holds a per-run
//! `std` mutex for its whole duration. Dropping (aborting) the calling future does
//! not stop a closure already running, so the lock is held by the *operation*, not by
//! the future: a later writer (the daemon recording a failed run) waits for an
//! in-flight write of an aborted task instead of racing it. Two guards cover what
//! serialization cannot: an `append` whose `seq` is not greater than the log's last is
//! refused (`SeqConflict`), and a `snapshot` older than the stored one is dropped, so
//! a late write from a dead task can neither duplicate a `seq` nor roll the snapshot
//! back.

use std::collections::HashMap;
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;

use aftman_core::event::EventEnvelope;
use aftman_core::ids::{EventSeq, RunId};
use aftman_core::job::FactoryStatus;
use aftman_core::run::{RunFilter, RunHeader, RunState, RunSummary};

use crate::{
    EventLogRead, EventsPage, RunClaim, RunStore, StoreError, io_err, parse_event_log, summarize,
    write_atomic,
};

/// Paths inside one run directory.
#[derive(Debug, Clone)]
pub struct RunPaths {
    pub dir: PathBuf,
    pub meta: PathBuf,
    pub state: PathBuf,
    pub events: PathBuf,
    /// Held under `flock` by the process driving the run (see `RunStore::claim`).
    pub lock: PathBuf,
    /// Processes launched by this run's workers and not yet reaped.
    pub processes: PathBuf,
}

impl RunPaths {
    #[must_use]
    pub fn new(root: &Path, id: RunId) -> Self {
        let dir = root.join("runs").join(id.to_string());
        Self {
            meta: dir.join("meta.json"),
            state: dir.join("state.json"),
            events: dir.join("events.jsonl"),
            lock: dir.join("run.lock"),
            processes: dir.join("processes.json"),
            dir,
        }
    }
}

/// What the store remembers about one run between writes. A cache only: it is
/// validated against the file (length) before it is trusted.
#[derive(Debug, Default)]
struct RunWrites {
    /// `(file length, last seq)` as of this store's last append.
    log: Option<(u64, u64)>,
    /// `last_seq` of the snapshot this store last wrote or read.
    snapshot_seq: Option<u64>,
}

type RunLock = Arc<Mutex<RunWrites>>;

/// A run store backed by a directory.
#[derive(Debug, Clone)]
pub struct FileRunStore {
    root: PathBuf,
    writes: Arc<Mutex<HashMap<RunId, RunLock>>>,
}

/// Create a directory (and any missing parents) `0700`. Existing directories are
/// left exactly as they are.
fn mkdir_private(dir: &Path) -> Result<(), StoreError> {
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
        .map_err(|e| io_err(dir.display(), e))
}

/// The longest log line the ranged read will buffer; a longer one sends it to the
/// full-parse fallback instead of growing memory.
const MAX_PAGE_LINE_BYTES: u64 = 8 * 1024 * 1024;

enum Next {
    Line,
    Eof,
    TooLong,
}

fn is_blank(line: &[u8]) -> bool {
    line.iter().all(u8::is_ascii_whitespace)
}

/// Read the page `[from, from+limit)` of events without parsing anything outside it.
///
/// `seq` N is the Nth event line of a well-formed log, so the `from - 1` event lines before
/// the page are skipped by counting lines (blank lines are not events, exactly as in
/// `parse_event_log`). That assumption is *checked*: the first line of the page must carry
/// `seq == from`, which, because `append` only ever writes strictly increasing `seq`s,
/// proves the skipped lines were `1..from`. If it does not hold, or a line is too long to
/// buffer, this returns `None` and the caller falls back to the full parse. Events are
/// parsed one at a time and reading stops after `limit + 1`, so memory is bounded by the
/// page. A malformed line inside the page is corruption unless it is the last non-blank
/// line (a torn append), the same rule as `parse_event_log`; corruption in the skipped
/// region is not looked for.
fn read_page(
    id: RunId,
    path: &Path,
    from: u64,
    limit: usize,
) -> Result<Option<EventsPage>, StoreError> {
    use std::io::{BufRead as _, BufReader};
    let file = std::fs::File::open(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            StoreError::NotFound(id)
        } else {
            io_err(path.display(), e)
        }
    })?;
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    let mut line_number = 0usize;

    let next_line =
        |reader: &mut BufReader<std::fs::File>, line: &mut Vec<u8>| -> Result<Next, StoreError> {
            line.clear();
            let read = (&mut *reader)
                .take(MAX_PAGE_LINE_BYTES)
                .read_until(b'\n', line)
                .map_err(|e| io_err(path.display(), e))?;
            Ok(if read == 0 {
                Next::Eof
            } else if line.len() as u64 >= MAX_PAGE_LINE_BYTES && line.last() != Some(&b'\n') {
                Next::TooLong
            } else {
                Next::Line
            })
        };

    let skip = from.saturating_sub(1);
    let mut skipped = 0u64;
    // The last event line skipped, kept so that an empty page can still be vouched for.
    let mut last_skipped: Vec<u8> = Vec::new();
    let mut at_eof = false;
    while skipped < skip {
        match next_line(&mut reader, &mut line)? {
            Next::Line => {}
            Next::Eof => {
                at_eof = true;
                break;
            }
            Next::TooLong => return Ok(None),
        }
        line_number += 1;
        if !is_blank(&line) {
            skipped += 1;
            std::mem::swap(&mut line, &mut last_skipped);
        }
    }

    let mut events: Vec<EventEnvelope> = Vec::new();
    let mut first = true;
    loop {
        if at_eof {
            break;
        }
        match next_line(&mut reader, &mut line)? {
            Next::Line => {}
            Next::Eof => break,
            Next::TooLong => return Ok(None),
        }
        line_number += 1;
        let text = String::from_utf8_lossy(&line);
        let text = text.trim_end_matches(['\n', '\r']);
        if text.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<EventEnvelope>(text) {
            Ok(event) => {
                if first {
                    first = false;
                    if skip > 0 && event.seq.get() != from {
                        return Ok(None);
                    }
                }
                events.push(event);
                if events.len() > limit {
                    break;
                }
            }
            Err(error) => {
                // Only the final non-blank line may be torn.
                loop {
                    match next_line(&mut reader, &mut line)? {
                        Next::Line => {
                            if !is_blank(&line) {
                                return Err(StoreError::CorruptLog {
                                    run_id: id,
                                    line: line_number,
                                    detail: error.to_string(),
                                });
                            }
                        }
                        Next::Eof => break,
                        Next::TooLong => return Ok(None),
                    }
                }
                break;
            }
        }
    }
    if events.is_empty() && skipped > 0 {
        // No event in the page to check the line/seq correspondence against, so check
        // the last event before it: it must carry the seq its position implies.
        let text = String::from_utf8_lossy(&last_skipped);
        let vouched = serde_json::from_str::<EventEnvelope>(text.trim())
            .is_ok_and(|e| e.seq.get() == skipped);
        if !vouched {
            return Ok(None);
        }
    }
    events.retain(|e| e.seq.get() >= from);
    let has_more = events.len() > limit;
    events.truncate(limit);
    Ok(Some(EventsPage { events, has_more }))
}

/// The recorded worker processes; unreadable or absent means none.
fn read_processes(
    path: &Path,
) -> std::collections::BTreeMap<String, aftman_core::worker::ProcessIdentity> {
    std::fs::read(path)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default()
}

fn lock_writes(lock: &RunLock) -> MutexGuard<'_, RunWrites> {
    lock.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn join_error(error: tokio::task::JoinError) -> StoreError {
    io_err("blocking store task", std::io::Error::other(error))
}

/// `write_atomic`, blocking: temp file, fsync, rename, fsync the directory.
fn write_atomic_sync(path: &Path, bytes: &[u8]) -> Result<(), StoreError> {
    let parent = path
        .parent()
        .ok_or_else(|| io_err(path.display(), std::io::Error::other("path has no parent")))?;
    let temp = path.with_extension("tmp");
    {
        let mut file = {
            use std::os::unix::fs::OpenOptionsExt as _;
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&temp)
                .map_err(|e| io_err(temp.display(), e))?
        };
        file.write_all(bytes)
            .map_err(|e| io_err(temp.display(), e))?;
        file.sync_all().map_err(|e| io_err(temp.display(), e))?;
    }
    std::fs::rename(&temp, path).map_err(|e| io_err(path.display(), e))?;
    if let Ok(dir) = std::fs::File::open(parent) {
        let _ = dir.sync_all();
    }
    Ok(())
}

/// Make the end of a log safe to append to.
///
/// A process killed mid-append leaves a final line with no newline. Appending
/// straight after it would glue the new event onto the fragment and turn a
/// recoverable torn *final* line into corruption in the *middle* of the log (and
/// lose the new event). So: a log whose last byte is not a newline either has a
/// complete last line (add the newline) or a torn one (cut it off, as
/// `parse_event_log` already treats it as never written).
fn repair_tail(file: &mut std::fs::File, path: &Path) -> Result<(), StoreError> {
    let err = |e| io_err(path.display(), e);
    let len = file.metadata().map_err(err)?.len();
    if len == 0 {
        return Ok(());
    }
    file.seek(SeekFrom::Start(len - 1)).map_err(err)?;
    let mut last = [0u8; 1];
    file.read_exact(&mut last).map_err(err)?;
    if last[0] == b'\n' {
        return Ok(());
    }

    // Slow path, only after an interrupted append.
    file.seek(SeekFrom::Start(0)).map_err(err)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(err)?;
    let cut = bytes.iter().rposition(|b| *b == b'\n').map_or(0, |i| i + 1);
    if serde_json::from_slice::<EventEnvelope>(&bytes[cut..]).is_ok() {
        file.write_all(b"\n").map_err(err)?;
    } else {
        tracing::warn!(
            path = %path.display(),
            dropped_bytes = bytes.len() - cut,
            "dropping a torn final event line before appending"
        );
        file.set_len(cut as u64).map_err(err)?;
    }
    Ok(())
}

/// One append, under the run's write lock. Blocking.
fn append_blocking(
    run: &RunLock,
    id: RunId,
    path: &Path,
    event: &EventEnvelope,
    line: &str,
) -> Result<(), StoreError> {
    let mut writes = lock_writes(run);
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .append(true)
        .open(path)
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StoreError::NotFound(id)
            } else {
                io_err(path.display(), e)
            }
        })?;
    repair_tail(&mut file, path)?;
    let len = file
        .metadata()
        .map_err(|e| io_err(path.display(), e))?
        .len();
    let last = match writes.log {
        Some((cached_len, seq)) if cached_len == len => seq,
        _ => {
            let bytes = std::fs::read(path).map_err(|e| io_err(path.display(), e))?;
            parse_event_log(id, &bytes)?.last_seq().get()
        }
    };
    // Exactly the next one. `<=` accepted a gap, and `read_page`'s fast path and the "no
    // gaps" promise (`docs/02-lld.md` §11) both assume contiguity — after one gap every
    // `events_page` falls back to parsing the whole log, which is the bounded-memory
    // guarantee gone. `SeqConflict` already means "your view of the log is stale", which is
    // exactly what a caller producing a gap has.
    if event.seq.get() != last.saturating_add(1) {
        return Err(StoreError::SeqConflict {
            run_id: id,
            seq: event.seq.get(),
            last,
        });
    }
    let mut record = Vec::with_capacity(line.len() + 1);
    record.extend_from_slice(line.as_bytes());
    record.push(b'\n');
    file.write_all(&record)
        .map_err(|e| io_err(path.display(), e))?;
    // Durable before returning: a crash after a directive is applied but before its
    // event is on disk would leave an unexplainable history.
    file.sync_data().map_err(|e| io_err(path.display(), e))?;
    writes.log = Some((len + record.len() as u64, event.seq.get()));
    Ok(())
}

/// One snapshot, under the run's write lock. Blocking. An older snapshot than the
/// stored one is dropped (see the module documentation).
fn snapshot_blocking(
    run: &RunLock,
    id: RunId,
    paths: &RunPaths,
    state: &RunState,
    bytes: &[u8],
) -> Result<(), StoreError> {
    let mut writes = lock_writes(run);
    if !paths.meta.exists() {
        return Err(StoreError::NotFound(id));
    }
    // The log's last seq, from this store's cache or by reading it once.
    let log_last = match writes.log {
        Some((_, seq)) => seq,
        None => match std::fs::read(&paths.events) {
            Ok(bytes) => parse_event_log(id, &bytes)?.last_seq().get(),
            // A run created but not yet appended to: an empty log.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(io_err(paths.events.display(), error)),
        },
    };
    // A snapshot may be *behind* the log — normal between an append and the next snapshot —
    // but never *ahead*: that describes events that do not exist, and `load` then reports
    // `SnapshotMismatch` for the run's whole life, which is not repairable through the API
    // because a corrective snapshot used to be dropped as stale (below).
    if state.last_seq.get() > log_last {
        return Err(StoreError::SnapshotMismatch {
            run_id: id,
            snapshot: state.last_seq.get(),
            log: log_last,
        });
    }
    let stored = match writes.snapshot_seq {
        Some(seq) => Some(seq),
        None => std::fs::read(&paths.state)
            .ok()
            .and_then(|b| serde_json::from_slice::<RunState>(&b).ok())
            .map(|s| s.last_seq.get()),
    };
    // Dropped only when it is behind a snapshot that is itself valid. A stored snapshot that
    // is *ahead* of the log is the corrupt state this check exists to prevent; a lower one is
    // then the repair, not a regression.
    if stored.is_some_and(|stored| state.last_seq.get() < stored && stored <= log_last) {
        tracing::warn!(
            run_id = %id,
            stale = state.last_seq.get(),
            stored,
            "dropping a snapshot older than the stored one"
        );
        return Ok(());
    }
    write_atomic_sync(&paths.state, bytes)?;
    writes.snapshot_seq = Some(state.last_seq.get());
    Ok(())
}

impl FileRunStore {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            writes: Arc::new(Mutex::new(HashMap::new())),
        }
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

    fn run_lock(&self, id: RunId) -> RunLock {
        let mut map = self
            .writes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(map.entry(id).or_default())
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

    /// Take the run's lock. `create` is true only for a brand-new run.
    async fn claim_inner(&self, id: RunId, create: bool) -> Result<Option<RunClaim>, StoreError> {
        use nix::errno::Errno;
        use nix::fcntl::{Flock, FlockArg};

        let paths = self.paths(id);
        // `flock` itself never blocks here (non-blocking mode), but opening files
        // and creating directories are filesystem calls, so keep them off the
        // async threads.
        tokio::task::spawn_blocking(move || {
            if create {
                mkdir_private(&paths.dir)?;
            } else if !paths.dir.is_dir() {
                return Err(StoreError::NotFound(id));
            }
            let file = match std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .mode(0o600)
                .open(&paths.lock)
            {
                Ok(file) => file,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound && !create => {
                    return Err(StoreError::NotFound(id));
                }
                Err(e) => return Err(io_err(paths.lock.display(), e)),
            };
            let opened = file
                .metadata()
                .map_err(|e| io_err(paths.lock.display(), e))?;
            match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
                Ok(lock) => {
                    // A deleter renames the directory away while holding the lock.
                    // If that happened between our open and our lock we hold a lock
                    // on a file nobody can reach: the run is gone.
                    match std::fs::metadata(&paths.lock) {
                        Ok(on_disk)
                            if on_disk.ino() == opened.ino() && on_disk.dev() == opened.dev() =>
                        {
                            Ok(Some(RunClaim::new(id, lock)))
                        }
                        _ => Err(StoreError::NotFound(id)),
                    }
                }
                Err((_, Errno::EWOULDBLOCK)) => Ok(None),
                Err((_, errno)) => Err(io_err(paths.lock.display(), errno.into())),
            }
        })
        .await
        .map_err(join_error)?
    }
}

#[async_trait]
impl RunStore for FileRunStore {
    async fn create(&self, header: &RunHeader) -> Result<(), StoreError> {
        let paths = self.paths(header.id);
        if paths.meta.exists() {
            return Err(StoreError::AlreadyExists(header.id));
        }
        let dir = paths.dir.clone();
        tokio::task::spawn_blocking(move || mkdir_private(&dir))
            .await
            .map_err(join_error)??;

        let encoded =
            serde_json::to_vec_pretty(header).map_err(|e| StoreError::Encode(e.to_string()))?;
        write_atomic(&paths.meta, &encoded).await?;

        // Start with an empty log so that readers never have to treat a missing
        // file as an empty one.
        let empty = RunState::new(header.clone(), FactoryStatus::Queued);
        let encoded =
            serde_json::to_vec_pretty(&empty).map_err(|e| StoreError::Encode(e.to_string()))?;
        write_atomic(&paths.state, &encoded).await?;
        write_atomic(&paths.events, b"").await?;
        Ok(())
    }

    async fn append(&self, event: &EventEnvelope) -> Result<(), StoreError> {
        let paths = self.paths(event.run_id);
        let line = serde_json::to_string(event).map_err(|e| StoreError::Encode(e.to_string()))?;
        let run = self.run_lock(event.run_id);
        let (id, event) = (event.run_id, event.clone());
        tokio::task::spawn_blocking(move || append_blocking(&run, id, &paths.events, &event, &line))
            .await
            .map_err(join_error)?
    }

    async fn snapshot(&self, state: &RunState) -> Result<(), StoreError> {
        let paths = self.paths(state.header.id);
        let bytes =
            serde_json::to_vec_pretty(state).map_err(|e| StoreError::Encode(e.to_string()))?;
        let run = self.run_lock(state.header.id);
        let (id, state) = (state.header.id, state.clone());
        tokio::task::spawn_blocking(move || snapshot_blocking(&run, id, &paths, &state, &bytes))
            .await
            .map_err(join_error)?
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

    async fn snapshot_status(&self, id: RunId) -> Result<FactoryStatus, StoreError> {
        let paths = self.paths(id);
        let bytes = self.read_run_file(&paths.state, id).await?;
        serde_json::from_slice::<RunState>(&bytes)
            .map(|s| s.status)
            .map_err(|e| StoreError::Encode(e.to_string()))
    }

    async fn events_page(
        &self,
        id: RunId,
        from_seq: EventSeq,
        limit: usize,
    ) -> Result<EventsPage, StoreError> {
        let path = self.paths(id).events;
        let ranged = tokio::task::spawn_blocking({
            let path = path.clone();
            move || read_page(id, &path, from_seq.get(), limit)
        })
        .await
        .map_err(join_error)??;
        match ranged {
            Some(page) => Ok(page),
            // The ranged read could not vouch for itself: do it the slow, certain way.
            None => {
                let mut events = self.events(id, from_seq).await?;
                let has_more = events.len() > limit;
                events.truncate(limit);
                Ok(EventsPage { events, has_more })
            }
        }
    }

    async fn ends_with_run_finished(&self, id: RunId) -> Result<bool, StoreError> {
        let path = self.paths(id).events;
        let tail = tokio::task::spawn_blocking(move || -> Result<Option<Vec<u8>>, StoreError> {
            const WINDOW: u64 = 64 * 1024;
            let mut file = std::fs::File::open(&path).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    StoreError::NotFound(id)
                } else {
                    io_err(path.display(), e)
                }
            })?;
            let len = file
                .metadata()
                .map_err(|e| io_err(path.display(), e))?
                .len();
            let start = len.saturating_sub(WINDOW);
            file.seek(SeekFrom::Start(start))
                .map_err(|e| io_err(path.display(), e))?;
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)
                .map_err(|e| io_err(path.display(), e))?;
            // The last complete line, if the window holds one.
            while bytes.last() == Some(&b'\n') {
                bytes.pop();
            }
            match bytes.iter().rposition(|b| *b == b'\n') {
                Some(i) => Ok(Some(bytes[i + 1..].to_vec())),
                None if start == 0 => Ok(Some(bytes)),
                None => Ok(None), // a line longer than the window
            }
        })
        .await
        .map_err(join_error)??;
        match tail {
            Some(line) if line.is_empty() => Ok(false),
            Some(line) => Ok(serde_json::from_slice::<EventEnvelope>(&line)
                .is_ok_and(|e| e.kind == aftman_core::event::EventKind::RunFinished)),
            None => {
                let log = self.read_log(id).await?;
                Ok(log
                    .events
                    .last()
                    .is_some_and(|e| e.kind == aftman_core::event::EventKind::RunFinished))
            }
        }
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
        let mut summaries: Vec<RunSummary> = Vec::new();
        for id in self.ids().await? {
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

    async fn delete(&self, id: RunId) -> Result<(), StoreError> {
        // Two steps, so that "the run is gone" is one atomic moment: rename the
        // directory out of the namespace (after which `claim` reports NotFound and
        // `ids` cannot see it), then remove the tombstone at leisure.
        let paths = self.paths(id);
        let tomb = self.root.join("runs").join(format!(
            ".deleted-{id}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos())
        ));
        match tokio::fs::rename(&paths.dir, &tomb).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(StoreError::NotFound(id));
            }
            Err(error) => return Err(io_err(paths.dir.display(), error)),
        }
        self.writes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&id);
        if let Err(error) = tokio::fs::remove_dir_all(&tomb).await {
            // The run is deleted as far as every reader is concerned; only disk
            // space is left behind.
            tracing::warn!(path = %tomb.display(), %error, "could not remove a purged run's files");
        }
        Ok(())
    }

    async fn ids(&self) -> Result<Vec<RunId>, StoreError> {
        let runs_dir = self.root.join("runs");
        let mut entries = match tokio::fs::read_dir(&runs_dir).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(io_err(runs_dir.display(), error)),
        };
        let mut ids = Vec::new();
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| io_err(runs_dir.display(), e))?
        {
            let is_dir = entry.file_type().await.is_ok_and(|t| t.is_dir());
            if let (true, Ok(id)) = (is_dir, entry.file_name().to_string_lossy().parse::<RunId>()) {
                ids.push(id);
            }
        }
        Ok(ids)
    }

    async fn set_worker_process(
        &self,
        run: RunId,
        worker: aftman_core::ids::WorkerId,
        identity: Option<aftman_core::worker::ProcessIdentity>,
    ) -> Result<(), StoreError> {
        let paths = self.paths(run);
        let lock = self.run_lock(run);
        tokio::task::spawn_blocking(move || {
            let _guard = lock_writes(&lock);
            if !paths.meta.exists() {
                return Err(StoreError::NotFound(run));
            }
            let mut map = read_processes(&paths.processes);
            match identity {
                Some(identity) => {
                    map.insert(worker.to_string(), identity);
                }
                None => {
                    map.remove(&worker.to_string());
                }
            }
            if map.is_empty() {
                match std::fs::remove_file(&paths.processes) {
                    Ok(()) => Ok(()),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                    Err(e) => Err(io_err(paths.processes.display(), e)),
                }
            } else {
                let bytes = serde_json::to_vec_pretty(&map)
                    .map_err(|e| StoreError::Encode(e.to_string()))?;
                write_atomic_sync(&paths.processes, &bytes)
            }
        })
        .await
        .map_err(join_error)?
    }

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
        let paths = self.paths(run);
        if !paths.dir.is_dir() {
            return Err(StoreError::NotFound(run));
        }
        let map = tokio::task::spawn_blocking(move || read_processes(&paths.processes))
            .await
            .map_err(join_error)?;
        Ok(map
            .into_iter()
            .filter_map(|(worker, identity)| Some((worker.parse().ok()?, identity)))
            .collect())
    }

    async fn claim(&self, id: RunId) -> Result<Option<RunClaim>, StoreError> {
        self.claim_inner(id, false).await
    }

    async fn claim_new(&self, id: RunId) -> Result<Option<RunClaim>, StoreError> {
        self.claim_inner(id, true).await
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

        // And the store stays usable: the torn fragment is cut off before the new
        // event is appended, so the log is whole again rather than corrupt.
        reopened.append(&event(id, 3)).await?;
        let read = reopened.read_log(id).await?;
        assert_eq!(read.events.len(), 3, "the appended event is not lost");
        assert!(read.torn_lines.is_empty());
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
    async fn a_gap_in_the_log_is_refused() -> Result<(), Box<dyn std::error::Error>> {
        // `read_page`'s fast path and the "no gaps" promise (`02-lld.md` §11) assume
        // contiguity; after one gap every `events_page` falls back to parsing the whole log.
        let (_dir, store) = store()?;
        let id = RunId::new();
        store.create(&header(id)).await?;
        store.append(&event(id, 1)).await?;
        for skipped in [3u64, 0] {
            let error = store
                .append(&event(id, skipped))
                .await
                .expect_err("only seq 2 follows seq 1");
            assert!(
                matches!(error, crate::StoreError::SeqConflict { .. }),
                "seq {skipped}: {error:?}"
            );
        }
        // And the next one is still accepted.
        store.append(&event(id, 2)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_snapshot_ahead_of_the_log_is_refused_and_the_run_stays_usable()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_dir, store) = store()?;
        let id = RunId::new();
        store.create(&header(id)).await?;
        store.append(&event(id, 1)).await?;

        let mut state = store.load(id).await?;
        state.last_seq = EventSeq(7);
        // This used to be accepted, after which `load` reported `SnapshotMismatch` for the
        // run's whole life — and a corrective lower snapshot was dropped as stale, so the run
        // could not be repaired through the API. It is refused at the write now.
        let error = store.snapshot(&state).await.expect_err("ahead of log");
        assert!(matches!(
            error,
            crate::StoreError::SnapshotMismatch {
                snapshot: 7,
                log: 1,
                ..
            }
        ));

        // And nothing was written: the legitimate snapshot still loads and still saves.
        let mut valid = store.load(id).await?;
        // Behind the log, which is the normal state between an append and the next snapshot.
        assert_eq!(valid.last_seq, EventSeq(0));
        valid.status = FactoryStatus::Running;
        store.snapshot(&valid).await?;
        assert_eq!(store.load(id).await?.status, FactoryStatus::Running);
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
