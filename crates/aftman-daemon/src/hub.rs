//! Event fan-out with gap-free, duplicate-free resume, and backpressure.
//!
//! [`EventHub`] is an [`EventSink`] that any number of subscribers can follow. It
//! knows nothing about sockets or JSON framing: the Unix-socket server (M3) and the
//! HTTP/SSE server (M4) are designed to share it; neither exists yet.
//!
//! # Why resume is gap-free and duplicate-free
//!
//! Two facts carry the whole argument.
//!
//! 1. **The runtime appends to the store before it calls `emit`**, and emits one
//!    run's events sequentially in `seq` order. So at the moment event `k` is
//!    emitted, events `1..=k` are already durable, and anything the hub has ever
//!    seen is in the store.
//! 2. **`subscribe` registers the live channel and snapshots the ring in one
//!    critical section** (the same lock `emit` takes), and only *afterwards*
//!    reads the store if the ring does not reach back far enough.
//!
//! Let `T0` be that critical section. Every event is either emitted before `T0`
//! or after it. Events emitted after `T0` are pushed into the subscriber's
//! channel, in order. Events emitted before `T0` are, by (1), in the store, and
//! the store read happens after `T0`, so it returns at least all of them (and
//! possibly some that are emitted after `T0` too, or appended but not yet
//! emitted). The ring snapshot taken at `T0` is a contiguous suffix of what was
//! emitted before `T0`, so when it reaches back to `from_seq` it alone is the
//! backlog.
//!
//! The backlog is therefore a contiguous run `from..=b` and the channel yields a
//! contiguous run `c..` with `c <= b + 1`: any event with `seq > b` was appended
//! after the store read, hence emitted after `T0`, hence is in the channel. The
//! subscription drops channel items with `seq < next_expected` — the overlap —
//! so each `seq` is delivered exactly once and in order. Nothing is skipped
//! because the only items ever dropped are ones already delivered.
//!
//! # Known limit: the store fallback is unbounded
//!
//! When the ring does not reach back to `from_seq`, the backlog is read with one
//! `RunStore::events` call, which returns the whole tail as a single `Vec`. Memory
//! per such subscriber is therefore proportional to the run's remaining log
//! (events are payload-bearing JSON, so a long run can be tens of MB), and N
//! simultaneous deep resumes cost N copies. The read happens off the hub lock, so
//! it never delays `emit`. Paging needs a bounded `RunStore::events` and is
//! deferred. While that read is in flight the live channel is already filling; if
//! it overflows the subscriber is dropped as lagged (correct, but the client
//! must resume again).
//!
//! # Shutdown and run removal
//!
//! [`EventHub::close_all`] is sticky: it ends every stream (queued items are still
//! delivered, then `None`, never `Lagged`) and later `subscribe` calls fail with
//! [`SubscribeError::ShuttingDown`]. Dropping the last [`EventHub`] clone does the
//! same, so live subscriptions end instead of pending forever; subscriptions hold
//! only the shared state, not the guard that triggers this, so there is no
//! reference cycle. [`EventHub::forget`] removes one run (for `run.purge`).
//!
//! # Backpressure
//!
//! `emit` never awaits and never blocks on a subscriber: delivery is `try_send`
//! on a bounded channel. If the channel is full the hub removes the subscriber
//! and flags it lagged; everything already queued is still delivered, then the
//! stream yields one `Err(Lagged)` and ends. The prefix a client saw is a correct
//! gap-free prefix, and [`Lagged::resume_from`] says where to resume.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use aftman_core::event::EventEnvelope;
use aftman_core::ids::{EventSeq, RunId};
use aftman_store::{RunStore, StoreError};
use tokio::sync::mpsc;

use crate::sink::EventSink;

/// Default total number of events kept in the in-memory replay ring.
pub const DEFAULT_REPLAY_CAPACITY: usize = 10_000;
/// Default number of undelivered events a subscriber may have queued.
pub const DEFAULT_SUBSCRIBER_CAPACITY: usize = 1_024;
/// Upper bound on `HubConfig::replay_capacity`; larger values are clamped.
pub const MAX_REPLAY_CAPACITY: usize = 1_000_000;
/// Upper bound on `HubConfig::subscriber_capacity`; larger values are clamped
/// (tokio's channel panics on absurd bounds).
pub const MAX_SUBSCRIBER_CAPACITY: usize = 1_048_576;

/// Hub tuning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HubConfig {
    /// Total events kept across all runs. Oldest-first eviction. `0` disables the
    /// ring (every resume is served from the store).
    pub replay_capacity: usize,
    /// Per-subscriber queue bound. Clamped to at least 1.
    pub subscriber_capacity: usize,
}

impl Default for HubConfig {
    fn default() -> Self {
        Self {
            replay_capacity: DEFAULT_REPLAY_CAPACITY,
            subscriber_capacity: DEFAULT_SUBSCRIBER_CAPACITY,
        }
    }
}

/// Why a subscription could not be opened.
#[derive(Debug, thiserror::Error)]
pub enum SubscribeError {
    #[error("from_seq requires a run_id: sequence numbers are per run")]
    FromSeqWithoutRun,
    #[error("unknown run {0}")]
    UnknownRun(RunId),
    #[error("the event hub is shutting down")]
    ShuttingDown,
    #[error("could not read the event log: {0}")]
    Store(StoreError),
}

/// A stream's terminal error: the subscriber fell behind and was dropped.
///
/// Everything delivered before this was a correct, gap-free prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("subscriber lagged and was dropped; resume from seq {resume_from:?}")]
pub struct Lagged {
    pub run_id: Option<RunId>,
    /// The next `seq` the client has not seen, for a single-run subscription.
    /// `None` for an all-runs stream, which is live-only and cannot be resumed.
    pub resume_from: Option<EventSeq>,
}

/// Why a run's subscribers were told the run's story would not be finished.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Abandoned {
    pub run_id: RunId,
    pub reason: String,
}

/// Longest abandonment reason kept, in bytes.
pub const MAX_ABANDON_REASON_BYTES: usize = 512;

/// How many abandoned runs the hub remembers, so a follower that subscribes after the
/// fact is told too. Oldest forgotten first.
const ABANDONED_MEMORY: usize = 256;

type AbandonedSlot = Arc<Mutex<Option<Abandoned>>>;

struct Subscriber {
    run: Option<RunId>,
    tx: mpsc::Sender<Arc<EventEnvelope>>,
    lagged: Arc<AtomicBool>,
    abandoned: AbandonedSlot,
}

#[derive(Default)]
struct State {
    /// Per-run contiguous suffix of recent events.
    ring: HashMap<RunId, VecDeque<Arc<EventEnvelope>>>,
    /// Arrival order, one entry per ring event, for global oldest-first eviction.
    order: VecDeque<RunId>,
    subscribers: HashMap<u64, Subscriber>,
    next_id: u64,
    /// Sticky: set by shutdown, refuses new subscriptions.
    closed: bool,
    /// Runs whose end the daemon could not record, with the reason.
    abandoned: HashMap<RunId, String>,
    abandoned_order: VecDeque<RunId>,
}

struct Shared {
    store: Arc<dyn RunStore>,
    config: HubConfig,
    state: Mutex<State>,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        // Plain data; a panic elsewhere cannot leave a half-applied invariant we
        // rely on beyond what the next emit repairs.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Shared {
    fn shut_down(&self) {
        let dropped = {
            let mut state = self.lock();
            state.closed = true;
            std::mem::take(&mut state.subscribers)
        };
        drop(dropped);
    }
}

/// Shuts the hub down when the last `EventHub` clone goes away. Subscriptions
/// hold `Shared` but not this, so they do not keep the hub alive.
struct ShutdownGuard(Arc<Shared>);

impl Drop for ShutdownGuard {
    fn drop(&mut self) {
        self.0.shut_down();
    }
}

/// Fans events out to subscribers and keeps a bounded replay ring.
#[derive(Clone)]
pub struct EventHub {
    shared: Arc<Shared>,
    _guard: Arc<ShutdownGuard>,
}

impl std::fmt::Debug for EventHub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventHub")
            .field("config", &self.shared.config)
            .finish_non_exhaustive()
    }
}

impl EventHub {
    #[must_use]
    pub fn new(store: Arc<dyn RunStore>, config: HubConfig) -> Self {
        let shared = Arc::new(Shared {
            store,
            config: HubConfig {
                subscriber_capacity: config.subscriber_capacity.clamp(1, MAX_SUBSCRIBER_CAPACITY),
                replay_capacity: config.replay_capacity.min(MAX_REPLAY_CAPACITY),
            },
            state: Mutex::new(State::default()),
        });
        Self {
            _guard: Arc::new(ShutdownGuard(shared.clone())),
            shared,
        }
    }

    /// Open a subscription.
    ///
    /// With `run_id`, yields every event of that run with `seq >= from_seq`
    /// (default: from the first), exactly once and in order. Without, yields a
    /// live-only stream of all runs; `from_seq` is then an error.
    pub async fn subscribe(
        &self,
        run_id: Option<RunId>,
        from_seq: Option<EventSeq>,
    ) -> Result<Subscription, SubscribeError> {
        let Some(run) = run_id else {
            if from_seq.is_some() {
                return Err(SubscribeError::FromSeqWithoutRun);
            }
            return self.register(None, 0);
        };
        let from = from_seq.map_or(1, EventSeq::get).max(1);
        let mut sub = self.register(Some(run), from)?;
        if sub.backlog_covered {
            return Ok(sub);
        }
        // The ring did not reach back to `from`. Registration (above) happened
        // first; this read therefore sees everything emitted before it.
        match self.shared.store.events(run, EventSeq(from)).await {
            Ok(events) => {
                sub.backlog = events.into_iter().map(Arc::new).collect();
                Ok(sub)
            }
            // The ring did not cover `from` and the store has no such run:
            // returning an empty backlog would be a silent gap.
            Err(StoreError::NotFound(id)) => Err(SubscribeError::UnknownRun(id)),
            Err(other) => Err(SubscribeError::Store(other)),
        }
    }

    /// Register a subscriber and snapshot the ring in one critical section.
    fn register(&self, run: Option<RunId>, from: u64) -> Result<Subscription, SubscribeError> {
        let (tx, rx) = mpsc::channel(self.shared.config.subscriber_capacity);
        let lagged = Arc::new(AtomicBool::new(false));
        let abandoned_slot: AbandonedSlot = Arc::new(Mutex::new(None));
        let mut backlog = VecDeque::new();
        let mut covered = run.is_none();
        let id;
        {
            let mut state = self.shared.lock();
            if state.closed {
                return Err(SubscribeError::ShuttingDown);
            }
            id = state.next_id;
            state.next_id += 1;
            if let Some(run) = run
                && let Some(ring) = state.ring.get(&run)
                && ring.front().is_some_and(|e| e.seq.get() <= from)
            {
                covered = true;
                let start = ring.partition_point(|e| e.seq.get() < from);
                backlog.extend(ring.iter().skip(start).cloned());
            }
            let already = run.and_then(|r| {
                state.abandoned.get(&r).map(|reason| Abandoned {
                    run_id: r,
                    reason: reason.clone(),
                })
            });
            if let Some(already) = already {
                // Told at once: the backlog is still delivered, then the stream ends
                // with the abandonment.
                *abandoned_slot
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(already);
                drop(tx);
            } else {
                state.subscribers.insert(
                    id,
                    Subscriber {
                        run,
                        tx,
                        lagged: lagged.clone(),
                        abandoned: abandoned_slot.clone(),
                    },
                );
            }
        }
        let sub = Subscription {
            id,
            shared: self.shared.clone(),
            run,
            rx,
            backlog,
            backlog_covered: covered,
            next_seq: from,
            lagged,
            abandoned: abandoned_slot,
            ended: false,
        };
        Ok(sub)
    }

    /// Number of live subscribers.
    #[must_use]
    pub fn subscriber_count(&self) -> usize {
        self.shared.lock().subscribers.len()
    }

    /// Number of events currently held in the replay ring.
    #[must_use]
    pub fn ring_len(&self) -> usize {
        self.shared.lock().order.len()
    }

    /// Events actually held in memory across all per-run rings (as opposed to
    /// the eviction bookkeeping in [`Self::ring_len`]). Always `<=` the capacity.
    #[must_use]
    pub fn ring_events_held(&self) -> usize {
        self.shared.lock().ring.values().map(VecDeque::len).sum()
    }

    /// Shut down: end every stream cleanly (no `Lagged`) and refuse new
    /// subscriptions with [`SubscribeError::ShuttingDown`]. Sticky.
    pub fn close_all(&self) {
        self.shared.shut_down();
    }

    /// Forget a run entirely: drop its ring contents and end its run-scoped
    /// subscribers cleanly. For `run.purge`. Later subscriptions to the run are
    /// served from the store only (so `UnknownRun` once the store has purged it).
    /// The caller must not emit further events for the run.
    /// The daemon has stopped driving `run` and could not record its end. Every
    /// subscriber scoped to that run (all-runs streams are unaffected) receives what was
    /// already queued and then its stream ends; [`Subscription::abandoned`] says why, so
    /// the transport can tell the client explicitly instead of leaving it waiting. A
    /// subscriber that arrives later is told the same. Remembered for the last
    /// 256 abandoned runs, and until [`EventHub::forget`].
    pub fn abandon(&self, run: RunId, reason: &str) {
        let reason: String = {
            let mut end = reason.len().min(MAX_ABANDON_REASON_BYTES);
            while !reason.is_char_boundary(end) {
                end -= 1;
            }
            reason[..end].to_owned()
        };
        let dropped = {
            let mut state = self.shared.lock();
            if state.abandoned.insert(run, reason.clone()).is_none() {
                state.abandoned_order.push_back(run);
                while state.abandoned_order.len() > ABANDONED_MEMORY {
                    if let Some(old) = state.abandoned_order.pop_front() {
                        state.abandoned.remove(&old);
                    }
                }
            }
            let ids: Vec<u64> = state
                .subscribers
                .iter()
                .filter(|(_, s)| s.run == Some(run))
                .map(|(id, _)| *id)
                .collect();
            let mut dropped = Vec::new();
            for id in ids {
                if let Some(sub) = state.subscribers.remove(&id) {
                    *sub.abandoned
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Abandoned {
                        run_id: run,
                        reason: reason.clone(),
                    });
                    dropped.push(sub);
                }
            }
            dropped
        };
        // Dropped outside the lock: closing the channels wakes the subscribers.
        drop(dropped);
    }

    pub fn forget(&self, run: RunId) {
        let (ring, dropped) = {
            let mut state = self.shared.lock();
            state.abandoned.remove(&run);
            state.abandoned_order.retain(|r| *r != run);
            let ring = state.ring.remove(&run);
            state.order.retain(|r| *r != run);
            let ids: Vec<u64> = state
                .subscribers
                .iter()
                .filter(|(_, s)| s.run == Some(run))
                .map(|(id, _)| *id)
                .collect();
            let dropped: Vec<Subscriber> = ids
                .into_iter()
                .filter_map(|id| state.subscribers.remove(&id))
                .collect();
            (ring, dropped)
        };
        drop(ring);
        drop(dropped);
    }
}

impl EventSink for EventHub {
    fn emit(&self, event: &EventEnvelope) {
        let event = Arc::new(event.clone());
        // Senders removed here are dropped after the lock is released.
        let mut removed: Vec<Subscriber> = Vec::new();
        {
            let mut state = self.shared.lock();
            let cap = self.shared.config.replay_capacity;
            if cap > 0 {
                state
                    .ring
                    .entry(event.run_id)
                    .or_default()
                    .push_back(event.clone());
                state.order.push_back(event.run_id);
                while state.order.len() > cap {
                    let Some(old) = state.order.pop_front() else {
                        break;
                    };
                    if let Some(ring) = state.ring.get_mut(&old) {
                        ring.pop_front();
                        if ring.is_empty() {
                            state.ring.remove(&old);
                        }
                    }
                }
            }
            let mut dead = Vec::new();
            for (id, sub) in &state.subscribers {
                if sub.run.is_some_and(|r| r != event.run_id) {
                    continue;
                }
                match sub.tx.try_send(event.clone()) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        sub.lagged.store(true, Ordering::Release);
                        dead.push(*id);
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => dead.push(*id),
                }
            }
            for id in dead {
                if let Some(sub) = state.subscribers.remove(&id) {
                    removed.push(sub);
                }
            }
        }
        drop(removed);
    }
}

/// One subscriber's view. Dropping it unsubscribes.
pub struct Subscription {
    id: u64,
    shared: Arc<Shared>,
    run: Option<RunId>,
    rx: mpsc::Receiver<Arc<EventEnvelope>>,
    backlog: VecDeque<Arc<EventEnvelope>>,
    backlog_covered: bool,
    /// Next `seq` to deliver (single-run subscriptions only).
    next_seq: u64,
    lagged: Arc<AtomicBool>,
    abandoned: AbandonedSlot,
    ended: bool,
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription")
            .field("id", &self.id)
            .field("run", &self.run)
            .field("next_seq", &self.next_seq)
            .finish_non_exhaustive()
    }
}

impl Subscription {
    /// The run this subscription follows, or `None` for all runs.
    #[must_use]
    pub fn run_id(&self) -> Option<RunId> {
        self.run
    }

    /// The next event, `Some(Err(Lagged))` once if the hub dropped this
    /// subscriber for being too slow, then `None`. Cancel-safe.
    /// Why the stream ended, if it ended because the run was abandoned (see
    /// [`EventHub::abandon`]). Meaningful once [`Subscription::next`] has returned `None`.
    #[must_use]
    pub fn abandoned(&self) -> Option<Abandoned> {
        self.abandoned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub async fn next(&mut self) -> Option<Result<Arc<EventEnvelope>, Lagged>> {
        loop {
            if self.ended {
                return None;
            }
            let item = if let Some(e) = self.backlog.pop_front() {
                e
            } else if let Some(e) = self.rx.recv().await {
                e
            } else {
                self.ended = true;
                if self.lagged.load(Ordering::Acquire) {
                    return Some(Err(Lagged {
                        run_id: self.run,
                        resume_from: self.run.map(|_| EventSeq(self.next_seq)),
                    }));
                }
                return None;
            };
            if self.run.is_some() {
                // The backlog/live overlap: already delivered, drop it.
                if item.seq.get() < self.next_seq {
                    continue;
                }
                self.next_seq = item.seq.get() + 1;
            }
            return Some(Ok(item));
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        let removed = self.shared.lock().subscribers.remove(&self.id);
        drop(removed);
    }
}
