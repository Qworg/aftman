//! Event hub behaviour: fan-out, resume, backpressure. Fully offline.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use aftman_core::event::{EventEnvelope, EventKind};
use aftman_core::ids::{EventSeq, RunId};
use aftman_core::job::{Job, JobSource, RunConfig};
use aftman_core::run::RunHeader;
use aftman_daemon::hub::{HubConfig, Lagged, SubscribeError, Subscription};
use aftman_daemon::{EventHub, EventSink};
use aftman_store::{MemoryRunStore, RunStore};
use time::macros::datetime;

fn header(id: RunId) -> RunHeader {
    RunHeader {
        id,
        created_at: datetime!(2026-01-01 00:00:00 UTC),
        job: Job {
            prompt: "job".into(),
            repo: PathBuf::from("/repo"),
            source: JobSource::Cli,
        },
        config: RunConfig::default(),
        aftman_version: "0.1.0".into(),
        worker_versions: Default::default(),
        granted_key_names: Vec::new(),
    }
}

fn event(run: RunId, seq: u64) -> EventEnvelope {
    EventEnvelope::new(
        EventSeq(seq),
        run,
        datetime!(2026-01-01 00:00:00 UTC),
        EventKind::StateChanged,
        serde_json::json!({ "n": seq }),
    )
}

async fn new_run(store: &MemoryRunStore) -> RunId {
    let id = RunId::new();
    store.create(&header(id)).await.expect("create");
    id
}

/// What the runtime does: append to the store, then emit.
async fn publish(store: &MemoryRunStore, hub: &EventHub, run: RunId, seq: u64) {
    let e = event(run, seq);
    store.append(&e).await.expect("append");
    hub.emit(&e);
}

fn setup(replay: usize, sub_cap: usize) -> (Arc<MemoryRunStore>, EventHub) {
    let store = Arc::new(MemoryRunStore::new());
    let hub = EventHub::new(
        store.clone(),
        HubConfig {
            replay_capacity: replay,
            subscriber_capacity: sub_cap,
        },
    );
    (store, hub)
}

async fn take(sub: &mut Subscription, n: usize) -> Vec<u64> {
    let mut out = Vec::new();
    while out.len() < n {
        let next = tokio::time::timeout(Duration::from_secs(10), sub.next())
            .await
            .expect("stream stalled");
        out.push(next.expect("stream ended").expect("lagged").seq.get());
    }
    out
}

/// `next()` with a hang guard, so a regression fails instead of stalling the suite.
async fn next_item(sub: &mut Subscription) -> Option<Result<Arc<EventEnvelope>, Lagged>> {
    tokio::time::timeout(Duration::from_secs(10), sub.next())
        .await
        .expect("stream stalled")
}

fn seqs(range: std::ops::RangeInclusive<u64>) -> Vec<u64> {
    range.collect()
}

#[tokio::test]
async fn two_subscribers_see_identical_ordered_streams() {
    let (store, hub) = setup(100, 100);
    let run = new_run(&store).await;
    let mut a = hub.subscribe(Some(run), None).await.expect("a");
    let mut b = hub.subscribe(Some(run), None).await.expect("b");
    for s in 1..=20 {
        publish(&store, &hub, run, s).await;
    }
    assert_eq!(take(&mut a, 20).await, seqs(1..=20));
    assert_eq!(take(&mut b, 20).await, seqs(1..=20));
}

#[tokio::test]
async fn resume_mid_run_from_ring_and_from_store() {
    // Ring holds only the last 5 events; seq 8 is in the store only.
    let (store, hub) = setup(5, 100);
    let run = new_run(&store).await;
    for s in 1..=20 {
        publish(&store, &hub, run, s).await;
    }
    assert!(hub.ring_len() <= 5);
    let mut from_store = hub
        .subscribe(Some(run), Some(EventSeq(8)))
        .await
        .expect("s");
    let mut from_ring = hub
        .subscribe(Some(run), Some(EventSeq(18)))
        .await
        .expect("r");
    let mut from_start = hub.subscribe(Some(run), None).await.expect("f");
    publish(&store, &hub, run, 21).await;
    assert_eq!(take(&mut from_store, 14).await, seqs(8..=21));
    assert_eq!(take(&mut from_ring, 4).await, seqs(18..=21));
    assert_eq!(take(&mut from_start, 21).await, seqs(1..=21));
}

#[tokio::test]
async fn from_seq_beyond_the_tip_waits_for_it() {
    let (store, hub) = setup(100, 100);
    let run = new_run(&store).await;
    for s in 1..=3 {
        publish(&store, &hub, run, s).await;
    }
    let mut sub = hub
        .subscribe(Some(run), Some(EventSeq(5)))
        .await
        .expect("sub");
    for s in 4..=6 {
        publish(&store, &hub, run, s).await;
    }
    assert_eq!(take(&mut sub, 2).await, vec![5, 6]);
}

#[tokio::test]
async fn finished_run_replays_from_disk_with_an_empty_ring() {
    let store = Arc::new(MemoryRunStore::new());
    let run = new_run(&store).await;
    for s in 1..=7 {
        store.append(&event(run, s)).await.expect("append");
    }
    // A fresh hub, as after a daemon restart: nothing in the ring.
    let hub = EventHub::new(store.clone(), HubConfig::default());
    let mut sub = hub
        .subscribe(Some(run), Some(EventSeq(3)))
        .await
        .expect("sub");
    assert_eq!(take(&mut sub, 5).await, seqs(3..=7));
}

#[tokio::test]
async fn a_run_with_no_events_yet_streams_from_the_first() {
    let (store, hub) = setup(100, 100);
    let run = new_run(&store).await;
    let mut sub = hub.subscribe(Some(run), None).await.expect("sub");
    publish(&store, &hub, run, 1).await;
    publish(&store, &hub, run, 2).await;
    assert_eq!(take(&mut sub, 2).await, vec![1, 2]);
}

#[tokio::test]
async fn an_unknown_run_is_an_error() {
    let (_store, hub) = setup(100, 100);
    let err = hub.subscribe(Some(RunId::new()), None).await.unwrap_err();
    assert!(matches!(err, SubscribeError::UnknownRun(_)));
    assert_eq!(
        hub.subscriber_count(),
        0,
        "a failed subscribe leaves nothing behind"
    );
}

#[tokio::test]
async fn from_seq_without_a_run_is_rejected() {
    let (_store, hub) = setup(100, 100);
    let err = hub.subscribe(None, Some(EventSeq(3))).await.unwrap_err();
    assert!(matches!(err, SubscribeError::FromSeqWithoutRun));
}

#[tokio::test]
async fn all_runs_subscription_is_live_only() {
    let (store, hub) = setup(100, 100);
    let r1 = new_run(&store).await;
    let r2 = new_run(&store).await;
    publish(&store, &hub, r1, 1).await; // before subscribing: not delivered
    let mut all = hub.subscribe(None, None).await.expect("all");
    publish(&store, &hub, r1, 2).await;
    publish(&store, &hub, r2, 1).await;
    let a = next_item(&mut all).await.expect("one").expect("ok");
    let b = next_item(&mut all).await.expect("two").expect("ok");
    assert_eq!((a.run_id, a.seq.get()), (r1, 2));
    assert_eq!((b.run_id, b.seq.get()), (r2, 1));
}

#[tokio::test]
async fn single_run_subscription_ignores_other_runs() {
    let (store, hub) = setup(100, 100);
    let r1 = new_run(&store).await;
    let r2 = new_run(&store).await;
    let mut sub = hub.subscribe(Some(r1), None).await.expect("sub");
    publish(&store, &hub, r2, 1).await;
    publish(&store, &hub, r1, 1).await;
    assert_eq!(take(&mut sub, 1).await, vec![1]);
}

#[tokio::test]
async fn a_stalled_subscriber_is_dropped_as_lagged_without_affecting_others() {
    let (store, hub) = setup(1000, 8);
    let run = new_run(&store).await;
    let mut slow = hub.subscribe(Some(run), None).await.expect("slow");
    let mut fast = hub.subscribe(Some(run), None).await.expect("fast");
    for s in 1..=30 {
        let e = event(run, s);
        store.append(&e).await.expect("append");
        hub.emit(&e); // sync and non-blocking: returning at all is the property
        // The fast subscriber keeps up.
        assert_eq!(take(&mut fast, 1).await, vec![s]);
    }
    assert_eq!(
        hub.subscriber_count(),
        1,
        "the stalled subscriber was removed"
    );

    // The slow one still gets a correct prefix, then a distinguishable end.
    assert_eq!(take(&mut slow, 8).await, seqs(1..=8));
    let end = next_item(&mut slow).await.expect("terminal item");
    assert_eq!(
        end.unwrap_err(),
        Lagged {
            run_id: Some(run),
            resume_from: Some(EventSeq(9)),
        }
    );
    assert!(next_item(&mut slow).await.is_none());

    // And resuming from the reported point is gap-free.
    let mut resumed = hub
        .subscribe(Some(run), Some(EventSeq(9)))
        .await
        .expect("resume");
    assert_eq!(take(&mut resumed, 22).await, seqs(9..=30));
}

#[tokio::test]
async fn dropping_a_subscription_unsubscribes() {
    let (store, hub) = setup(100, 100);
    let run = new_run(&store).await;
    let a = hub.subscribe(Some(run), None).await.expect("a");
    let b = hub.subscribe(None, None).await.expect("b");
    assert_eq!(hub.subscriber_count(), 2);
    drop(a);
    assert_eq!(hub.subscriber_count(), 1);
    drop(b);
    assert_eq!(hub.subscriber_count(), 0);
    publish(&store, &hub, run, 1).await; // no panic, nothing to deliver to
}

#[tokio::test]
async fn close_all_ends_streams_without_a_lagged_error() {
    let (store, hub) = setup(100, 100);
    let run = new_run(&store).await;
    let mut sub = hub.subscribe(Some(run), None).await.expect("sub");
    hub.close_all();
    assert!(next_item(&mut sub).await.is_none());
    assert_eq!(hub.subscriber_count(), 0);
}

#[tokio::test]
async fn the_ring_is_bounded_across_many_runs_and_old_runs_come_from_disk() {
    let (store, hub) = setup(10, 100);
    let mut runs = Vec::new();
    for _ in 0..5 {
        runs.push(new_run(&store).await);
    }
    for s in 1..=40 {
        for run in &runs {
            publish(&store, &hub, *run, s).await;
            // Events genuinely held in memory, not just the bookkeeping count.
            assert!(hub.ring_events_held() <= 10);
            assert_eq!(hub.ring_events_held(), hub.ring_len());
        }
    }
    assert_eq!(hub.ring_len(), 10);
    assert_eq!(hub.ring_events_held(), 10);
    let mut sub = hub.subscribe(Some(runs[0]), None).await.expect("old run");
    assert_eq!(take(&mut sub, 40).await, seqs(1..=40));
}

#[tokio::test]
async fn a_disabled_ring_serves_everything_from_the_store() {
    let (store, hub) = setup(0, 100);
    let run = new_run(&store).await;
    for s in 1..=5 {
        publish(&store, &hub, run, s).await;
    }
    assert_eq!(hub.ring_len(), 0);
    let mut sub = hub
        .subscribe(Some(run), Some(EventSeq(2)))
        .await
        .expect("sub");
    publish(&store, &hub, run, 6).await;
    assert_eq!(take(&mut sub, 5).await, seqs(2..=6));
}

/// Tiny deterministic generator so the stress test is reproducible.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }
}

/// Subscribers join at random points while a task emits exactly as the runtime
/// does (append, then emit), on a multi-threaded runtime. Every subscriber must
/// see an exactly contiguous range ending at the last event.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscribing_at_random_points_during_emission_is_gap_free_and_duplicate_free() {
    const TOTAL: u64 = 200;
    for iteration in 0..60u64 {
        // Alternate a tiny ring (forces the store path and its race) and a big one.
        let ring = if iteration % 2 == 0 { 8 } else { 10_000 };
        let (store, hub) = setup(ring, 4096);
        let run = new_run(&store).await;

        let emitter = {
            let (store, hub) = (store.clone(), hub.clone());
            tokio::spawn(async move {
                for s in 1..=TOTAL {
                    publish(&store, &hub, run, s).await;
                    if s % 3 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            })
        };

        let mut rng = Lcg(iteration + 1);
        let mut readers = Vec::new();
        for _ in 0..6 {
            let pause = rng.next() % 40;
            let from = match rng.next() % 3 {
                0 => None,
                _ => Some(1 + rng.next() % TOTAL),
            };
            let hub = hub.clone();
            readers.push(tokio::spawn(async move {
                for _ in 0..pause {
                    tokio::task::yield_now().await;
                }
                let mut sub = hub
                    .subscribe(Some(run), from.map(EventSeq))
                    .await
                    .expect("sub");
                let first = from.unwrap_or(1);
                let mut got = Vec::new();
                while got.last().copied() != Some(TOTAL) {
                    let item = tokio::time::timeout(Duration::from_secs(20), sub.next())
                        .await
                        .expect("stalled")
                        .expect("ended early")
                        .expect("lagged");
                    got.push(item.seq.get());
                }
                (first, got)
            }));
        }
        emitter.await.expect("emitter");
        for r in readers {
            let (first, got) = r.await.expect("reader");
            let expected: Vec<u64> = (first..=TOTAL).collect();
            assert_eq!(got, expected, "iteration {iteration}, from {first}");
        }
    }
}

// ---------------------------------------------------------------------------
// Review-fix tests
// ---------------------------------------------------------------------------

use aftman_core::run::{RunFilter, RunState, RunSummary};
use aftman_daemon::hub::SubscribeError as SubErr;
use aftman_store::{EventLogRead, FileRunStore, StoreError};
use tokio::sync::Notify;

/// Wraps a store: can pretend a run is gone, and can hold `events` open after
/// taking its snapshot so tests can emit "during the backlog read".
struct TestStore {
    inner: MemoryRunStore,
    gone: std::sync::Mutex<Vec<RunId>>,
    snapshot_taken: Notify,
    release: Notify,
    gate: std::sync::atomic::AtomicBool,
}

impl TestStore {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: MemoryRunStore::new(),
            gone: std::sync::Mutex::new(Vec::new()),
            snapshot_taken: Notify::new(),
            release: Notify::new(),
            gate: std::sync::atomic::AtomicBool::new(false),
        })
    }
    fn remove(&self, id: RunId) {
        self.gone.lock().expect("lock").push(id);
    }
    fn is_gone(&self, id: RunId) -> bool {
        self.gone.lock().expect("lock").contains(&id)
    }
}

#[async_trait::async_trait]
impl RunStore for TestStore {
    async fn create(&self, h: &RunHeader) -> Result<(), StoreError> {
        self.inner.create(h).await
    }
    async fn append(&self, e: &EventEnvelope) -> Result<(), StoreError> {
        self.inner.append(e).await
    }
    async fn snapshot(&self, s: &RunState) -> Result<(), StoreError> {
        self.inner.snapshot(s).await
    }
    async fn load(&self, id: RunId) -> Result<RunState, StoreError> {
        self.inner.load(id).await
    }
    async fn events(&self, id: RunId, from: EventSeq) -> Result<Vec<EventEnvelope>, StoreError> {
        if self.is_gone(id) {
            return Err(StoreError::NotFound(id));
        }
        let snap = self.inner.events(id, from).await?;
        if self.gate.load(std::sync::atomic::Ordering::SeqCst) {
            self.snapshot_taken.notify_one();
            self.release.notified().await;
        }
        Ok(snap)
    }
    async fn list(&self, f: &RunFilter) -> Result<Vec<RunSummary>, StoreError> {
        self.inner.list(f).await
    }
    async fn read_log(&self, id: RunId) -> Result<EventLogRead, StoreError> {
        self.inner.read_log(id).await
    }
    async fn delete(&self, id: RunId) -> Result<(), StoreError> {
        self.inner.delete(id).await
    }
    async fn ids(&self) -> Result<Vec<RunId>, StoreError> {
        self.inner.ids().await
    }
    async fn claim(&self, id: RunId) -> Result<Option<aftman_store::RunClaim>, StoreError> {
        self.inner.claim(id).await
    }
    async fn claim_new(&self, id: RunId) -> Result<Option<aftman_store::RunClaim>, StoreError> {
        self.inner.claim_new(id).await
    }
}

fn cfg(replay: usize, cap: usize) -> HubConfig {
    HubConfig {
        replay_capacity: replay,
        subscriber_capacity: cap,
    }
}

#[tokio::test]
async fn an_uncovered_run_the_store_no_longer_has_is_an_error_not_a_silent_gap() {
    let store = TestStore::new();
    let hub = EventHub::new(store.clone(), cfg(5, 100));
    let run = RunId::new();
    store.create(&header(run)).await.expect("create");
    for s in 1..=20 {
        let e = event(run, s);
        store.append(&e).await.expect("append");
        hub.emit(&e);
    }
    store.remove(run);
    let err = hub.subscribe(Some(run), None).await.unwrap_err();
    assert!(matches!(err, SubErr::UnknownRun(id) if id == run));
    assert_eq!(hub.subscriber_count(), 0);
    // A request the ring does cover still works without the store.
    let mut ok = hub
        .subscribe(Some(run), Some(EventSeq(18)))
        .await
        .expect("covered");
    assert_eq!(take(&mut ok, 3).await, vec![18, 19, 20]);
}

#[tokio::test]
async fn forget_drops_the_ring_and_ends_run_scoped_subscribers_only() {
    let store = TestStore::new();
    let hub = EventHub::new(store.clone(), cfg(100, 100));
    let (r1, r2) = (RunId::new(), RunId::new());
    for r in [r1, r2] {
        store.create(&header(r)).await.expect("create");
        for s in 1..=3 {
            let e = event(r, s);
            store.append(&e).await.expect("append");
            hub.emit(&e);
        }
    }
    let mut scoped = hub.subscribe(Some(r1), Some(EventSeq(4))).await.expect("s");
    let mut other = hub.subscribe(Some(r2), Some(EventSeq(4))).await.expect("o");
    let mut all = hub.subscribe(None, None).await.expect("a");
    assert_eq!(hub.ring_events_held(), 6);

    hub.forget(r1);
    store.remove(r1);
    assert_eq!(hub.ring_events_held(), 3);
    assert_eq!(hub.ring_len(), 3);
    assert!(next_item(&mut scoped).await.is_none(), "ended cleanly");
    assert_eq!(hub.subscriber_count(), 2);
    // No replay from memory afterwards.
    let err = hub.subscribe(Some(r1), None).await.unwrap_err();
    assert!(matches!(err, SubErr::UnknownRun(_)));

    // The other run and the all-runs stream are untouched.
    let e = event(r2, 4);
    store.append(&e).await.expect("append");
    hub.emit(&e);
    assert_eq!(take(&mut other, 1).await, vec![4]);
    assert_eq!(take(&mut all, 1).await, vec![4]);
}

#[tokio::test]
async fn shutdown_is_sticky_and_ends_streams() {
    let (store, hub) = setup(100, 100);
    let run = new_run(&store).await;
    let mut sub = hub.subscribe(Some(run), None).await.expect("sub");
    let mut all = hub.subscribe(None, None).await.expect("all");
    publish(&store, &hub, run, 1).await;
    hub.close_all();
    // Queued items still arrive, then a clean end (not Lagged).
    assert_eq!(take(&mut sub, 1).await, vec![1]);
    assert!(next_item(&mut sub).await.is_none());
    assert_eq!(take(&mut all, 1).await, vec![1]);
    assert!(next_item(&mut all).await.is_none());
    for r in [Some(run), None] {
        let err = hub.subscribe(r, None).await.unwrap_err();
        assert!(matches!(err, SubErr::ShuttingDown));
    }
    assert_eq!(hub.subscriber_count(), 0);
    publish(&store, &hub, run, 2).await; // emit after shutdown is harmless
}

#[tokio::test]
async fn dropping_the_last_hub_clone_ends_live_subscriptions() {
    let (store, hub) = setup(100, 100);
    let run = new_run(&store).await;
    let mut sub = hub.subscribe(Some(run), None).await.expect("sub");
    let clone = hub.clone();
    drop(hub);
    publish(&store, &clone, run, 1).await;
    assert_eq!(take(&mut sub, 1).await, vec![1], "a clone keeps it alive");
    drop(clone);
    assert!(next_item(&mut sub).await.is_none());
}

#[tokio::test]
async fn absurd_capacities_are_clamped_not_panics() {
    let store = Arc::new(MemoryRunStore::new());
    let hub = EventHub::new(store.clone(), cfg(usize::MAX, usize::MAX));
    let run = new_run(&store).await;
    let mut sub = hub.subscribe(Some(run), None).await.expect("sub");
    publish(&store, &hub, run, 1).await;
    assert_eq!(take(&mut sub, 1).await, vec![1]);
    let zero = EventHub::new(store.clone(), cfg(0, 0));
    let _ = zero.subscribe(None, None).await.expect("sub");
}

#[tokio::test]
async fn file_store_backs_resume_across_a_fresh_hub() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(FileRunStore::new(dir.path()));
    let run = RunId::new();
    store.create(&header(run)).await.expect("create");
    let hub = EventHub::new(store.clone(), cfg(4, 100));
    for s in 1..=12 {
        let e = event(run, s);
        store.append(&e).await.expect("append");
        hub.emit(&e);
    }
    let mut live = hub
        .subscribe(Some(run), Some(EventSeq(2)))
        .await
        .expect("l");
    let e = event(run, 13);
    store.append(&e).await.expect("append");
    hub.emit(&e);
    assert_eq!(take(&mut live, 12).await, seqs(2..=13));
    // "Daemon restart": empty ring, everything from disk.
    let fresh = EventHub::new(store.clone(), cfg(4, 100));
    let mut replay = fresh.subscribe(Some(run), None).await.expect("r");
    assert_eq!(take(&mut replay, 13).await, seqs(1..=13));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn several_runs_emitting_concurrently_stay_independent() {
    const TOTAL: u64 = 150;
    let (store, hub) = setup(40, 4096);
    let mut runs = Vec::new();
    for _ in 0..3 {
        runs.push(new_run(&store).await);
    }
    let mut all = hub.subscribe(None, None).await.expect("all");
    let mut per_run = Vec::new();
    for run in &runs {
        let mut sub = hub.subscribe(Some(*run), None).await.expect("sub");
        per_run.push(tokio::spawn(
            async move { take(&mut sub, TOTAL as usize).await },
        ));
    }
    let mut emitters = Vec::new();
    for run in runs.clone() {
        let (store, hub) = (store.clone(), hub.clone());
        emitters.push(tokio::spawn(async move {
            for s in 1..=TOTAL {
                publish(&store, &hub, run, s).await;
                tokio::task::yield_now().await;
            }
        }));
    }
    for e in emitters {
        e.await.expect("emitter");
    }
    for t in per_run {
        assert_eq!(t.await.expect("reader"), seqs(1..=TOTAL));
    }
    // The all-runs stream saw each run's events in order, interleaved.
    let mut last = std::collections::HashMap::new();
    for _ in 0..(TOTAL as usize * 3) {
        let e = next_item(&mut all).await.expect("item").expect("ok");
        let prev = last.insert(e.run_id, e.seq.get()).unwrap_or(0);
        assert_eq!(e.seq.get(), prev + 1);
    }
    assert!(hub.ring_events_held() <= 40);
}

#[tokio::test]
async fn lag_during_the_backlog_phase_keeps_a_gap_free_prefix() {
    let store = TestStore::new();
    let hub = EventHub::new(store.clone(), cfg(0, 4)); // no ring: store path
    let run = RunId::new();
    store.create(&header(run)).await.expect("create");
    for s in 1..=5 {
        store.append(&event(run, s)).await.expect("append");
    }
    store.gate.store(true, std::sync::atomic::Ordering::SeqCst);
    let h = hub.clone();
    let pending = tokio::spawn(async move { h.subscribe(Some(run), None).await });
    store.snapshot_taken.notified().await;
    // The store snapshot holds 1..=5; these arrive while the read is "in flight".
    for s in 6..=15 {
        let e = event(run, s);
        store.append(&e).await.expect("append");
        hub.emit(&e);
    }
    store.release.notify_one();
    let mut sub = pending.await.expect("join").expect("subscribe");
    // Backlog 1..=5, then the 4 queued live events, then Lagged.
    assert_eq!(take(&mut sub, 9).await, seqs(1..=9));
    let end = next_item(&mut sub).await.expect("terminal");
    assert_eq!(end.unwrap_err().resume_from, Some(EventSeq(10)));
    assert!(next_item(&mut sub).await.is_none());
}

#[tokio::test]
async fn an_all_runs_subscriber_can_lag_and_is_told_it_cannot_resume() {
    let (store, hub) = setup(100, 4);
    let run = new_run(&store).await;
    let mut all = hub.subscribe(None, None).await.expect("all");
    for s in 1..=10 {
        publish(&store, &hub, run, s).await;
    }
    assert_eq!(take(&mut all, 4).await, seqs(1..=4));
    let end = next_item(&mut all).await.expect("terminal").unwrap_err();
    assert_eq!(
        end,
        Lagged {
            run_id: None,
            resume_from: None
        }
    );
    assert!(next_item(&mut all).await.is_none());
    assert_eq!(hub.subscriber_count(), 0);
}

#[tokio::test]
async fn next_is_cancel_safe() {
    let (store, hub) = setup(3, 100);
    let run = new_run(&store).await;
    for s in 1..=10 {
        publish(&store, &hub, run, s).await;
    }
    // Backlog comes from the store; cancel `next` repeatedly, including while
    // events are available, and while idle.
    let mut sub = hub.subscribe(Some(run), None).await.expect("sub");
    let mut got = Vec::new();
    for s in 11..=40u64 {
        for _ in 0..2 {
            tokio::select! {
                biased;
                () = std::future::ready(()) => {}
                _ = sub.next() => unreachable!("ready branch wins"),
            }
        }
        // Idle cancellation: a zero timeout drops the future mid-wait.
        if let Ok(Some(Ok(e))) = tokio::time::timeout(Duration::ZERO, sub.next()).await {
            got.push(e.seq.get());
        }
        publish(&store, &hub, run, s).await;
    }
    while got.len() < 40 {
        got.push(
            next_item(&mut sub)
                .await
                .expect("item")
                .expect("ok")
                .seq
                .get(),
        );
    }
    assert_eq!(got, seqs(1..=40));
}
