//! Run-level state: the header, the snapshot, and the shapes the surfaces list.
//!
//! These live in `aftman-core` rather than in the store crate because they are made
//! of domain types. The store persists them; it does not define them.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::assessment::{HumanDecision, Opinion, ResolvedCheck};
use crate::directive::Directive;
use crate::ids::{CheckKey, EscalationId, EventSeq, RunId};
use crate::job::{FactoryStatus, Job, RunConfig, WorkerKind};
use crate::observation::Counters;
use crate::worker::WorkerRecord;

/// Immutable facts about a run, written once at creation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunHeader {
    pub id: RunId,
    #[serde(with = "crate::timefmt")]
    pub created_at: OffsetDateTime,
    pub job: Job,
    pub config: RunConfig,
    pub aftman_version: String,
    /// Resolved worker binary paths and versions, so an outcome is attributable to
    /// a specific agent build.
    pub worker_versions: BTreeMap<String, String>,
    /// Names of provider keys granted to the worker. Names only, never values.
    pub granted_key_names: Vec<String>,
}

/// The current state of a run. Snapshotted atomically; never the source of truth on
/// its own, because [`EventSeq`] ties it to the event log.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunState {
    pub header: RunHeader,
    pub status: FactoryStatus,
    /// The sequence number this snapshot includes. A snapshot whose `last_seq` is
    /// ahead of the log is corruption, not a newer state.
    pub last_seq: EventSeq,
    pub counters: Counters,
    pub workers: Vec<WorkerRecord>,
    pub checks: BTreeMap<CheckKey, ResolvedCheck>,
    pub last_directive: Option<Directive>,
    pub escalations: Vec<Escalation>,
    /// Milliseconds this run has spent waiting in `NeedsHuman` with a human channel
    /// attached. Subtracted from elapsed time wherever `overall_timeout` is
    /// evaluated, so a person who is away does not use up the run's budget. Kept
    /// here and not in `Counters`, which is serialized into the model-facing
    /// observation.
    #[serde(default)]
    pub human_wait_ms: u64,
}

impl RunState {
    #[must_use]
    pub fn new(header: RunHeader, status: FactoryStatus) -> Self {
        Self {
            header,
            status,
            last_seq: EventSeq(0),
            counters: Counters::default(),
            workers: Vec::new(),
            checks: BTreeMap::new(),
            last_directive: None,
            escalations: Vec::new(),
            human_wait_ms: 0,
        }
    }

    /// Time the run has been supervised: wall time since creation, minus time spent
    /// waiting for a human. This is the elapsed time `overall_timeout` is measured
    /// against.
    #[must_use]
    pub fn supervised_elapsed(&self, now: OffsetDateTime) -> std::time::Duration {
        let wall: std::time::Duration = (now - self.header.created_at)
            .try_into()
            .unwrap_or(std::time::Duration::ZERO);
        wall.saturating_sub(std::time::Duration::from_millis(self.human_wait_ms))
    }

    #[must_use]
    pub fn active_worker(&self) -> Option<&WorkerRecord> {
        self.workers.last().filter(|w| w.status.is_running())
    }

    /// Open escalations, oldest first.
    pub fn open_escalations(&self) -> impl Iterator<Item = &Escalation> {
        self.escalations.iter().filter(|e| e.answered.is_none())
    }

    #[must_use]
    pub fn has_open_escalation(&self) -> bool {
        self.open_escalations().next().is_some()
    }
}

/// A raised question awaiting a human.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Escalation {
    pub id: EscalationId,
    pub kind: EscalationKind,
    #[serde(with = "crate::timefmt")]
    pub raised_at: OffsetDateTime,
    /// `None` when the escalation is not about a single check: budget exhaustion
    /// and model unavailability have no check key.
    pub check: Option<CheckKey>,
    pub ask: String,
    pub jev_p: Option<f32>,
    pub opinions: Vec<Opinion>,
    /// Who answered, when, and what they said.
    pub answered: Option<AnsweredEscalation>,
}

/// Why an escalation was raised. Distinguishing these matters: "a person must
/// decide" and "supervision is blind" are very different messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EscalationKind {
    /// A specific check could not be resolved.
    Check,
    /// The assessment budget for the run is exhausted.
    AssessmentBudget,
    /// The model is unreachable or repeatedly failing.
    ModelUnavailable,
    /// A worker is waiting at an approval prompt (herdr surface).
    WorkerBlocked,
}

/// A human's answer to an escalation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AnsweredEscalation {
    #[serde(with = "crate::timefmt")]
    pub at: OffsetDateTime,
    /// Which surface it came from: `cli`, `tui`, `web`, or `herdr`.
    pub surface: String,
    pub decision: HumanDecision,
    pub note: String,
}

/// A row in a run list.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunSummary {
    pub id: RunId,
    pub status: FactoryStatus,
    #[serde(with = "crate::timefmt")]
    pub created_at: OffsetDateTime,
    pub elapsed_secs: u64,
    pub job_excerpt: String,
    pub repo: PathBuf,
    pub worker: WorkerKind,
    pub last_directive: Option<String>,
}

/// Filter for a run list.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunFilter {
    pub status: Option<Vec<FactoryStatus>>,
    pub repo: Option<PathBuf>,
    pub limit: Option<usize>,
    /// Return runs strictly older than this id (the list is time-ordered).
    pub cursor: Option<RunId>,
}

impl RunFilter {
    #[must_use]
    pub fn matching(&self, summary: &RunSummary) -> bool {
        if let Some(statuses) = &self.status
            && !statuses.contains(&summary.status)
        {
            return false;
        }
        if let Some(repo) = &self.repo
            && &summary.repo != repo
        {
            return false;
        }
        if let Some(cursor) = self.cursor
            && summary.id >= cursor
        {
            return false;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::{Escalation, EscalationKind, RunFilter, RunState, RunSummary};
    use crate::assessment::{HumanDecision, ResolvedCheck};
    use crate::ids::{CheckKey, EscalationId, EventSeq, RunId};
    use crate::job::{FactoryStatus, Job, JobSource, RunConfig, WorkerKind};
    use std::path::PathBuf;
    use time::macros::datetime;

    fn header() -> super::RunHeader {
        super::RunHeader {
            id: RunId::new(),
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

    #[test]
    fn a_new_state_starts_before_the_first_event() {
        let state = RunState::new(header(), FactoryStatus::Queued);
        // 0 means "no events yet"; the first real event is 1.
        assert_eq!(state.last_seq, EventSeq(0));
        assert!(state.active_worker().is_none());
        assert!(!state.has_open_escalation());
    }

    #[test]
    fn human_wait_is_subtracted_from_supervised_elapsed() {
        let mut state = RunState::new(header(), FactoryStatus::Running);
        let later = datetime!(2026-01-01 01:00:00 UTC);
        assert_eq!(
            state.supervised_elapsed(later),
            std::time::Duration::from_secs(3600)
        );
        state.human_wait_ms = 2_400_000;
        assert_eq!(
            state.supervised_elapsed(later),
            std::time::Duration::from_secs(1200)
        );
        // More waiting than elapsed (clock skew) saturates rather than underflowing.
        state.human_wait_ms = u64::MAX;
        assert_eq!(state.supervised_elapsed(later), std::time::Duration::ZERO);
    }

    #[test]
    fn a_snapshot_without_human_wait_loads_as_zero() -> Result<(), Box<dyn std::error::Error>> {
        let state = RunState::new(header(), FactoryStatus::Running);
        let mut value = serde_json::to_value(&state)?;
        value
            .as_object_mut()
            .ok_or("an object")?
            .remove("human_wait_ms");
        let loaded: RunState = serde_json::from_value(value)?;
        assert_eq!(loaded.human_wait_ms, 0);
        Ok(())
    }

    #[test]
    fn open_escalations_ignore_answered_ones() {
        let mut state = RunState::new(header(), FactoryStatus::NeedsHuman);
        state.escalations.push(Escalation {
            id: EscalationId::new(),
            kind: EscalationKind::Check,
            raised_at: datetime!(2026-01-01 00:00:00 UTC),
            check: Some(CheckKey::from("core.completion__ready_to_finish")),
            ask: "is it done?".into(),
            jev_p: Some(0.71),
            opinions: Vec::new(),
            answered: None,
        });
        assert!(state.has_open_escalation());

        state.escalations[0].answered = Some(super::AnsweredEscalation {
            at: datetime!(2026-01-01 00:01:00 UTC),
            surface: "cli".into(),
            decision: HumanDecision::Accept,
            note: String::new(),
        });
        assert!(!state.has_open_escalation());
    }

    #[test]
    fn a_budget_escalation_has_no_check_key() {
        let escalation = Escalation {
            id: EscalationId::new(),
            kind: EscalationKind::AssessmentBudget,
            raised_at: datetime!(2026-01-01 00:00:00 UTC),
            check: None,
            ask: "budget exhausted".into(),
            jev_p: None,
            opinions: Vec::new(),
            answered: None,
        };
        assert!(escalation.check.is_none());
    }

    #[test]
    fn run_filter_matches_status_repo_and_cursor() {
        let repo = PathBuf::from("/repo");
        let summary = RunSummary {
            id: RunId::new(),
            status: FactoryStatus::Finished,
            created_at: datetime!(2026-01-01 00:00:00 UTC),
            elapsed_secs: 10,
            job_excerpt: "job".into(),
            repo: repo.clone(),
            worker: WorkerKind::Simulated,
            last_directive: Some("Finish".into()),
        };
        assert!(RunFilter::default().matching(&summary));
        assert!(
            RunFilter {
                status: Some(vec![FactoryStatus::Finished]),
                ..Default::default()
            }
            .matching(&summary)
        );
        assert!(
            !RunFilter {
                status: Some(vec![FactoryStatus::Failed]),
                ..Default::default()
            }
            .matching(&summary)
        );
        assert!(
            !RunFilter {
                repo: Some(PathBuf::from("/other")),
                ..Default::default()
            }
            .matching(&summary)
        );
    }

    #[test]
    fn state_round_trips_through_json() -> Result<(), Box<dyn std::error::Error>> {
        let mut state = RunState::new(header(), FactoryStatus::Running);
        state.last_seq = EventSeq(9);
        let key = CheckKey::from("core.completion__ready_to_finish");
        state.checks.insert(
            key,
            ResolvedCheck::from_measurement(
                CheckKey::from("core.completion__ready_to_finish"),
                Some(0.9),
                Some(0.75),
                0.15,
                true,
            ),
        );
        let json = serde_json::to_string(&state)?;
        assert_eq!(serde_json::from_str::<RunState>(&json)?, state);
        Ok(())
    }
}
