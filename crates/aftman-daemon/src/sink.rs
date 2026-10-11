//! Where an emitted event goes.
//!
//! The runtime emits to one sink; the CLI renders it, a test collects it, and the
//! daemon may fan it out to several clients. Keeping this a trait means the runtime
//! never knows which of those it is talking to.

use std::sync::{Arc, Mutex};

use aftman_core::event::EventEnvelope;

/// A destination for persisted events.
///
/// `emit` must not block: it is called from the runtime's assessment path, and a
/// slow surface must not stall a run. A sink that cannot keep up should drop, and
/// a client that needs every event should read it back from the store by sequence
/// number.
pub trait EventSink: Send + Sync {
    fn emit(&self, event: &EventEnvelope);
}

/// Discards everything. Used when a run's events are only wanted on disk.
#[derive(Debug, Default)]
pub struct NullSink;

impl EventSink for NullSink {
    fn emit(&self, _event: &EventEnvelope) {}
}

/// Collects events in memory. For tests.
#[derive(Debug, Default)]
pub struct VecSink {
    events: Mutex<Vec<EventEnvelope>>,
}

impl VecSink {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn events(&self) -> Vec<EventEnvelope> {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Every event of one kind, in order.
    #[must_use]
    pub fn of_kind(&self, kind: aftman_core::event::EventKind) -> Vec<EventEnvelope> {
        self.events()
            .into_iter()
            .filter(|e| e.kind == kind)
            .collect()
    }
}

impl EventSink for VecSink {
    fn emit(&self, event: &EventEnvelope) {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(event.clone());
    }
}

/// Fans one event out to several sinks.
pub struct MultiSink {
    sinks: Vec<Arc<dyn EventSink>>,
}

impl MultiSink {
    #[must_use]
    pub fn new(sinks: Vec<Arc<dyn EventSink>>) -> Self {
        Self { sinks }
    }
}

impl EventSink for MultiSink {
    fn emit(&self, event: &EventEnvelope) {
        for sink in &self.sinks {
            sink.emit(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{EventSink, MultiSink, VecSink};
    use aftman_core::event::{EventEnvelope, EventKind};
    use aftman_core::ids::{EventSeq, RunId};
    use std::sync::Arc;
    use time::macros::datetime;

    fn event(seq: u64, kind: EventKind) -> EventEnvelope {
        EventEnvelope::new(
            EventSeq(seq),
            RunId::new(),
            datetime!(2026-01-01 00:00:00 UTC),
            kind,
            serde_json::json!({}),
        )
    }

    #[test]
    fn vec_sink_collects_in_order() {
        let sink = VecSink::new();
        assert!(sink.is_empty());
        sink.emit(&event(1, EventKind::RunCreated));
        sink.emit(&event(2, EventKind::WorkerStarted));
        assert_eq!(sink.len(), 2);
        assert_eq!(sink.events()[0].seq, EventSeq(1));
        assert_eq!(sink.of_kind(EventKind::WorkerStarted).len(), 1);
    }

    #[test]
    fn multi_sink_fans_out_to_every_destination() {
        let a = Arc::new(VecSink::new());
        let b = Arc::new(VecSink::new());
        let multi = MultiSink::new(vec![a.clone(), b.clone()]);
        multi.emit(&event(1, EventKind::RunCreated));
        assert_eq!(a.len(), 1);
        assert_eq!(b.len(), 1);
    }
}
