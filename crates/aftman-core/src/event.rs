//! The append-only event log's record types.
//!
//! Events are immutable; a correction is a new event. `WorkerOutput` is deliberately
//! *not* written per line — output is buffered and flushed once per assessment cycle
//! (plus once on worker exit), which keeps the log proportional to decisions rather
//! than to agent verbosity.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;

use crate::ids::{EventSeq, RunId};

/// The kind of a persisted event.
///
/// Serialized as `SCREAMING_SNAKE_CASE` to match the names used throughout `docs/`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EventKind {
    RunCreated,
    /// The routing decision: which conditional responsibilities were activated,
    /// with each probability and threshold. Replaced the older
    /// `RESPONSIBILITIES_ACTIVATED` label.
    JobRouted,
    WorkerStarted,
    /// Buffered worker output. At most one per cycle.
    WorkerOutput,
    WorkerCompleted,
    WorkerSteered,
    WorkerTerminated,
    EvidenceCollected,
    AssessmentRequested,
    AssessmentCompleted,
    /// A cycle the content digest found unchanged. Recorded so that "why did
    /// supervision not react?" remains answerable.
    AssessmentSkippedUnchanged,
    DirectiveProposed,
    DirectiveSelected,
    EnsembleTriggered,
    OpinionRequested,
    OpinionRecorded,
    EnsembleResolved,
    StateChanged,
    EscalationRaised,
    EscalationAnswered,
    /// A model call failed. Distinct from `Error`, which is an internal fault.
    ModelError,
    RateLimited,
    BudgetExceeded,
    RunFinished,
    /// An internal fault. Not a model or provider condition.
    Error,
    Notification,
    /// A kind this build does not know: an event written by a newer aftman. It decodes
    /// instead of failing the stream or the log; the original name is not preserved
    /// (serde cannot keep it), so it renders as `UNKNOWN`. Never emitted by this build.
    #[serde(other)]
    Unknown,
}

impl EventKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RunCreated => "RUN_CREATED",
            Self::JobRouted => "JOB_ROUTED",
            Self::WorkerStarted => "WORKER_STARTED",
            Self::WorkerOutput => "WORKER_OUTPUT",
            Self::WorkerCompleted => "WORKER_COMPLETED",
            Self::WorkerSteered => "WORKER_STEERED",
            Self::WorkerTerminated => "WORKER_TERMINATED",
            Self::EvidenceCollected => "EVIDENCE_COLLECTED",
            Self::AssessmentRequested => "ASSESSMENT_REQUESTED",
            Self::AssessmentCompleted => "ASSESSMENT_COMPLETED",
            Self::AssessmentSkippedUnchanged => "ASSESSMENT_SKIPPED_UNCHANGED",
            Self::DirectiveProposed => "DIRECTIVE_PROPOSED",
            Self::DirectiveSelected => "DIRECTIVE_SELECTED",
            Self::EnsembleTriggered => "ENSEMBLE_TRIGGERED",
            Self::OpinionRequested => "OPINION_REQUESTED",
            Self::OpinionRecorded => "OPINION_RECORDED",
            Self::EnsembleResolved => "ENSEMBLE_RESOLVED",
            Self::StateChanged => "STATE_CHANGED",
            Self::EscalationRaised => "ESCALATION_RAISED",
            Self::EscalationAnswered => "ESCALATION_ANSWERED",
            Self::ModelError => "MODEL_ERROR",
            Self::RateLimited => "RATE_LIMITED",
            Self::BudgetExceeded => "BUDGET_EXCEEDED",
            Self::RunFinished => "RUN_FINISHED",
            Self::Error => "ERROR",
            Self::Notification => "NOTIFICATION",
            Self::Unknown => "UNKNOWN",
        }
    }
}

/// The schema version of an event payload. Bump when a payload's shape changes.
pub const EVENT_SCHEMA_VERSION: u32 = 1;

/// One persisted event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub seq: EventSeq,
    pub run_id: RunId,
    #[serde(with = "crate::timefmt")]
    pub at: OffsetDateTime,
    pub kind: EventKind,
    /// Payload shape is per-kind; `schema_version` is always present.
    pub payload: Value,
}

impl EventEnvelope {
    /// Build an envelope, stamping the schema version into the payload.
    ///
    /// A non-object payload is wrapped so that `schema_version` can always be
    /// lifted out by a reader without inspecting the kind first.
    #[must_use]
    pub fn new(
        seq: EventSeq,
        run_id: RunId,
        at: OffsetDateTime,
        kind: EventKind,
        payload: Value,
    ) -> Self {
        let payload = match payload {
            Value::Object(mut map) => {
                map.insert("schema_version".into(), Value::from(EVENT_SCHEMA_VERSION));
                Value::Object(map)
            }
            other => {
                let mut map = serde_json::Map::new();
                map.insert("schema_version".into(), Value::from(EVENT_SCHEMA_VERSION));
                map.insert("value".into(), other);
                Value::Object(map)
            }
        };
        Self {
            seq,
            run_id,
            at,
            kind,
            payload,
        }
    }

    /// One-line human summary, for `aftman show` and the terminal renderer.
    #[must_use]
    pub fn summary(&self) -> String {
        let detail = match self.kind {
            EventKind::WorkerStarted => self
                .payload
                .get("mission_excerpt")
                .and_then(Value::as_str)
                .map(|m| format!(": {}", truncate(m, 60)))
                .unwrap_or_default(),
            EventKind::DirectiveSelected => self
                .payload
                .get("directive")
                .and_then(Value::as_str)
                .map(|d| format!(": {d}"))
                .unwrap_or_default(),
            EventKind::StateChanged => self
                .payload
                .get("to")
                .and_then(Value::as_str)
                .map(|to| format!(": -> {to}"))
                .unwrap_or_default(),
            EventKind::ModelError | EventKind::Error => self
                .payload
                .get("code")
                .and_then(Value::as_str)
                .map(|c| format!(": {c}"))
                .unwrap_or_default(),
            _ => String::new(),
        };
        format!("{}{detail}", self.kind.as_str())
    }
}

fn truncate(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_owned();
    }
    let mut out: String = text.chars().take(max).collect();
    out.push('…');
    out
}

#[cfg(test)]
mod tests {
    use super::{EVENT_SCHEMA_VERSION, EventEnvelope, EventKind};
    use crate::ids::{EventSeq, RunId};
    use serde_json::json;
    use time::macros::datetime;

    #[test]
    fn every_kind_serializes_as_screaming_snake_case() -> Result<(), Box<dyn std::error::Error>> {
        for kind in [
            EventKind::JobRouted,
            EventKind::WorkerStarted,
            EventKind::AssessmentSkippedUnchanged,
            EventKind::EnsembleResolved,
            EventKind::RunFinished,
        ] {
            let encoded = serde_json::to_string(&kind)?;
            assert_eq!(encoded.trim_matches('"'), kind.as_str());
        }
        Ok(())
    }

    #[test]
    fn an_event_kind_from_a_newer_build_decodes_as_unknown()
    -> Result<(), Box<dyn std::error::Error>> {
        let line = r#"{"seq":3,"run_id":"0193a000-0000-7000-8000-000000000001","kind":"BRAND_NEW_THING","at":"2026-01-01T00:00:00Z","payload":{}}"#;
        let event: EventEnvelope = serde_json::from_str(line)?;
        assert_eq!(event.kind, EventKind::Unknown);
        assert_eq!(event.seq.get(), 3);
        Ok(())
    }

    #[test]
    fn schema_version_is_stamped_into_object_payloads() {
        let event = EventEnvelope::new(
            EventSeq::FIRST,
            RunId::new(),
            datetime!(2026-01-01 00:00:00 UTC),
            EventKind::WorkerStarted,
            json!({ "worker_id": "w1" }),
        );
        assert_eq!(
            event.payload["schema_version"],
            json!(EVENT_SCHEMA_VERSION),
            "schema_version must be liftable without knowing the kind"
        );
        assert_eq!(event.payload["worker_id"], json!("w1"));
    }

    #[test]
    fn non_object_payloads_are_wrapped_not_rejected() {
        let event = EventEnvelope::new(
            EventSeq::FIRST,
            RunId::new(),
            datetime!(2026-01-01 00:00:00 UTC),
            EventKind::Notification,
            json!("plain text"),
        );
        assert_eq!(event.payload["value"], json!("plain text"));
        assert_eq!(event.payload["schema_version"], json!(EVENT_SCHEMA_VERSION));
    }

    #[test]
    fn envelopes_round_trip_through_json() -> Result<(), Box<dyn std::error::Error>> {
        let event = EventEnvelope::new(
            EventSeq(7),
            RunId::new(),
            datetime!(2026-01-01 00:00:00 UTC),
            EventKind::DirectiveSelected,
            json!({ "directive": "Finish" }),
        );
        let line = serde_json::to_string(&event)?;
        // One line, always: the log is newline-delimited.
        assert!(!line.contains('\n'));
        assert_eq!(serde_json::from_str::<EventEnvelope>(&line)?, event);
        Ok(())
    }

    #[test]
    fn summary_is_short_and_readable() {
        let event = EventEnvelope::new(
            EventSeq::FIRST,
            RunId::new(),
            datetime!(2026-01-01 00:00:00 UTC),
            EventKind::StateChanged,
            json!({ "to": "running" }),
        );
        assert_eq!(event.summary(), "STATE_CHANGED: -> running");
    }
}
