//! An in-process run store.
//!
//! Used by tests and by `--no-daemon` runs that do not want to touch the disk. It
//! implements the same validation as the file store — including the snapshot/log
//! agreement check — so that a test which passes here is not passing because the
//! in-memory backend was more forgiving.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;

use aftman_core::event::EventEnvelope;
use aftman_core::ids::{EventSeq, RunId};
use aftman_core::run::{RunFilter, RunHeader, RunState, RunSummary};

use crate::{EventLogRead, RunClaim, RunStore, StoreError, summarize};

#[derive(Debug, Default)]
struct Inner {
    headers: BTreeMap<RunId, RunHeader>,
    events: BTreeMap<RunId, Vec<EventEnvelope>>,
    snapshots: BTreeMap<RunId, RunState>,
    processes:
        BTreeMap<RunId, BTreeMap<aftman_core::ids::WorkerId, aftman_core::worker::ProcessIdentity>>,
}

/// A run store held entirely in memory.
#[derive(Debug, Default)]
pub struct MemoryRunStore {
    inner: Mutex<Inner>,
    /// Runs currently claimed. Shared with each [`RunClaim`] guard so that
    /// dropping the claim releases it.
    claims: Arc<Mutex<BTreeSet<RunId>>>,
}

/// Releases a memory claim on drop.
struct MemoryClaim {
    id: RunId,
    claims: Arc<Mutex<BTreeSet<RunId>>>,
}

impl Drop for MemoryClaim {
    fn drop(&mut self) {
        self.claims
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.id);
    }
}

impl MemoryRunStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Recover a poisoned lock rather than panicking.
    ///
    /// A poisoned lock means a panic occurred while holding it. The inner maps are
    /// plain data with no cross-field invariant that a partial update could break,
    /// so continuing is safe and preferable to aborting a whole test process.
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[async_trait]
impl RunStore for MemoryRunStore {
    async fn create(&self, header: &RunHeader) -> Result<(), StoreError> {
        let mut inner = self.lock();
        if inner.headers.contains_key(&header.id) {
            return Err(StoreError::AlreadyExists(header.id));
        }
        inner.headers.insert(header.id, header.clone());
        inner.events.insert(header.id, Vec::new());
        inner.snapshots.insert(
            header.id,
            RunState::new(header.clone(), aftman_core::job::FactoryStatus::Queued),
        );
        Ok(())
    }

    async fn append(&self, event: &EventEnvelope) -> Result<(), StoreError> {
        let mut inner = self.lock();
        let log = inner
            .events
            .get_mut(&event.run_id)
            .ok_or(StoreError::NotFound(event.run_id))?;
        // Reject a sequence that would make the log ambiguous rather than
        // silently accepting an out-of-order history.
        if let Some(last) = log.last()
            && event.seq <= last.seq
        {
            return Err(StoreError::SeqConflict {
                run_id: event.run_id,
                seq: event.seq.get(),
                last: last.seq.get(),
            });
        }
        log.push(event.clone());
        Ok(())
    }

    async fn snapshot(&self, state: &RunState) -> Result<(), StoreError> {
        let mut inner = self.lock();
        if !inner.headers.contains_key(&state.header.id) {
            return Err(StoreError::NotFound(state.header.id));
        }
        inner.snapshots.insert(state.header.id, state.clone());
        Ok(())
    }

    async fn load(&self, id: RunId) -> Result<RunState, StoreError> {
        let inner = self.lock();
        let state = inner
            .snapshots
            .get(&id)
            .ok_or(StoreError::NotFound(id))?
            .clone();
        let log = inner.events.get(&id).map(Vec::as_slice).unwrap_or(&[]);
        let log_seq = log.last().map_or(EventSeq(0), |e| e.seq);
        // The snapshot must not claim events that do not exist. Being *behind* the
        // log is fine and expected between an append and a snapshot.
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
        let inner = self.lock();
        let log = inner.events.get(&id).ok_or(StoreError::NotFound(id))?;
        Ok(log.iter().filter(|e| e.seq >= from_seq).cloned().collect())
    }

    async fn list(&self, filter: &RunFilter) -> Result<Vec<RunSummary>, StoreError> {
        let inner = self.lock();
        let mut summaries: Vec<RunSummary> = inner
            .snapshots
            .values()
            .map(summarize)
            .filter(|s| filter.matching(s))
            // Newest first.
            .collect();
        summaries.sort_by_key(|s| core::cmp::Reverse(s.id));
        if let Some(limit) = filter.limit {
            summaries.truncate(limit);
        }
        Ok(summaries)
    }

    async fn snapshot_status(
        &self,
        id: RunId,
    ) -> Result<aftman_core::job::FactoryStatus, StoreError> {
        self.lock()
            .snapshots
            .get(&id)
            .map(|s| s.status)
            .ok_or(StoreError::NotFound(id))
    }

    async fn read_log(&self, id: RunId) -> Result<EventLogRead, StoreError> {
        let inner = self.lock();
        let log = inner.events.get(&id).ok_or(StoreError::NotFound(id))?;
        Ok(EventLogRead {
            events: log.clone(),
            torn_lines: Vec::new(),
        })
    }

    async fn delete(&self, id: RunId) -> Result<(), StoreError> {
        let mut inner = self.lock();
        if inner.headers.remove(&id).is_none() {
            return Err(StoreError::NotFound(id));
        }
        inner.events.remove(&id);
        inner.snapshots.remove(&id);
        inner.processes.remove(&id);
        Ok(())
    }

    async fn ids(&self) -> Result<Vec<RunId>, StoreError> {
        Ok(self.lock().headers.keys().copied().collect())
    }

    async fn set_worker_process(
        &self,
        run: RunId,
        worker: aftman_core::ids::WorkerId,
        identity: Option<aftman_core::worker::ProcessIdentity>,
    ) -> Result<(), StoreError> {
        let mut inner = self.lock();
        if !inner.headers.contains_key(&run) {
            return Err(StoreError::NotFound(run));
        }
        let map = inner.processes.entry(run).or_default();
        match identity {
            Some(identity) => {
                map.insert(worker, identity);
            }
            None => {
                map.remove(&worker);
            }
        }
        Ok(())
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
        let inner = self.lock();
        if !inner.headers.contains_key(&run) {
            return Err(StoreError::NotFound(run));
        }
        Ok(inner
            .processes
            .get(&run)
            .map(|m| m.iter().map(|(w, i)| (*w, i.clone())).collect())
            .unwrap_or_default())
    }

    async fn claim(&self, id: RunId) -> Result<Option<RunClaim>, StoreError> {
        if !self.lock().headers.contains_key(&id) {
            return Err(StoreError::NotFound(id));
        }
        self.claim_new(id).await
    }

    async fn claim_new(&self, id: RunId) -> Result<Option<RunClaim>, StoreError> {
        let mut claims = self
            .claims
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !claims.insert(id) {
            return Ok(None);
        }
        Ok(Some(RunClaim::new(
            id,
            MemoryClaim {
                id,
                claims: Arc::clone(&self.claims),
            },
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::MemoryRunStore;
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

    #[tokio::test]
    async fn create_then_load_round_trips() -> Result<(), Box<dyn std::error::Error>> {
        let store = MemoryRunStore::new();
        let id = RunId::new();
        store.create(&header(id)).await?;
        let state = store.load(id).await?;
        assert_eq!(state.header.id, id);
        assert_eq!(state.status, FactoryStatus::Queued);
        Ok(())
    }

    #[tokio::test]
    async fn creating_the_same_run_twice_is_refused() -> Result<(), Box<dyn std::error::Error>> {
        let store = MemoryRunStore::new();
        let id = RunId::new();
        store.create(&header(id)).await?;
        let error = store.create(&header(id)).await.expect_err("duplicate");
        assert!(matches!(error, crate::StoreError::AlreadyExists(_)));
        Ok(())
    }

    #[tokio::test]
    async fn appending_out_of_order_is_refused() -> Result<(), Box<dyn std::error::Error>> {
        let store = MemoryRunStore::new();
        let id = RunId::new();
        store.create(&header(id)).await?;
        store.append(&event(id, 1)).await?;
        let error = store
            .append(&event(id, 1))
            .await
            .expect_err("a repeated sequence must be refused");
        assert!(matches!(error, crate::StoreError::SeqConflict { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn a_snapshot_ahead_of_the_log_is_corruption() -> Result<(), Box<dyn std::error::Error>> {
        let store = MemoryRunStore::new();
        let id = RunId::new();
        store.create(&header(id)).await?;
        store.append(&event(id, 1)).await?;

        let mut state = store.load(id).await?;
        state.last_seq = EventSeq(9);
        store.snapshot(&state).await?;

        let error = store.load(id).await.expect_err("snapshot ahead of log");
        assert!(matches!(
            error,
            crate::StoreError::SnapshotMismatch {
                snapshot: 9,
                log: 1,
                ..
            }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn a_snapshot_behind_the_log_is_allowed() -> Result<(), Box<dyn std::error::Error>> {
        // This is the normal state between an append and the next snapshot; it must
        // not read as corruption.
        let store = MemoryRunStore::new();
        let id = RunId::new();
        store.create(&header(id)).await?;
        store.append(&event(id, 1)).await?;
        store.append(&event(id, 2)).await?;
        let state = store.load(id).await?;
        assert_eq!(state.last_seq, EventSeq(0));
        Ok(())
    }

    #[tokio::test]
    async fn events_can_be_read_from_a_sequence() -> Result<(), Box<dyn std::error::Error>> {
        let store = MemoryRunStore::new();
        let id = RunId::new();
        store.create(&header(id)).await?;
        for seq in 1..=5 {
            store.append(&event(id, seq)).await?;
        }
        let tail = store.events(id, EventSeq(3)).await?;
        assert_eq!(tail.len(), 3);
        assert_eq!(tail[0].seq, EventSeq(3));
        Ok(())
    }

    #[tokio::test]
    async fn list_is_newest_first_and_respects_the_filter() -> Result<(), Box<dyn std::error::Error>>
    {
        let store = MemoryRunStore::new();
        // UUIDv7 is time-ordered, so a later `new()` sorts after an earlier one.
        let first = RunId::new();
        let second = RunId::new();
        store.create(&header(first)).await?;
        store.create(&header(second)).await?;

        let all = store.list(&RunFilter::default()).await?;
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].id, second, "newest first");

        let limited = store
            .list(&RunFilter {
                limit: Some(1),
                ..Default::default()
            })
            .await?;
        assert_eq!(limited.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn an_unknown_run_is_a_clean_not_found() {
        let store = MemoryRunStore::new();
        let error = store.load(RunId::new()).await.expect_err("unknown run");
        assert!(matches!(error, crate::StoreError::NotFound(_)));
    }
}
