//! Directives: what a responsibility may propose, and how policy records what it
//! did with each proposal.
//!
//! A [`Directive`] is a *proposal* until [`crate::policy::arbitrate`] selects it.
//! The vocabulary is closed and is the same one used in the design docs and in the
//! priority table, so a reader can move between a log line and `docs/05` without a
//! translation step.

use serde::{Deserialize, Serialize};

use crate::ids::{ResponsibilityId, RuleId};

/// How far the worker-health and instruction-drift rules may go.
///
/// `Steer` is only available when the backend has a live input channel; otherwise
/// the same rule produces `Stop`. Policy decides which, based on
/// `worker.supports_steering` and the remaining steer budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SteerKind {
    /// A small correction, keeping the worker on its current approach.
    Nudge,
    /// A larger correction: the worker has drifted from the job.
    Refocus,
    /// A warning that the current approach is failing.
    Warn,
    /// Ask the worker something and use the answer as evidence.
    Ask,
}

/// Whether a rule's outcome is one the ensemble should guard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Stakes {
    Low,
    /// Opts a rule into the second-opinion path when it is about to fire on a
    /// decisive value. See `docs/06-ensemble.md` §3.
    High,
}

/// The single directive vocabulary, used by every responsibility and by policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
// Tag is `directive`, not `kind`, because `Steer` already has a `kind` field.
#[serde(rename_all = "snake_case", tag = "directive")]
pub enum Directive {
    /// Nothing to do; let the worker continue.
    Continue,
    /// Start a coding worker. Also used to ask for more work after a stop.
    StartWorker { reason: String },
    /// Deliver guidance into a running worker.
    Steer { kind: SteerKind, reason: String },
    /// Retry the coding task with a fresh worker.
    Retry { reason: String },
    /// Spawn an independent verifier.
    Verify { reason: String },
    /// Stop the active worker. Parks the run in `Stopped`, which is not terminal.
    Stop { reason: String },
    /// Declare the job complete. Requires the full precondition in `docs/05` §6.2.
    Finish { reason: String },
    /// Escalate to a human. `ask` is the question to put to them.
    Escalate { reason: String, ask: String },
}

impl Directive {
    /// A stable name for logs and the priority table.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Continue => "Continue",
            Self::StartWorker { .. } => "StartWorker",
            Self::Steer { .. } => "Steer",
            Self::Retry { .. } => "Retry",
            Self::Verify { .. } => "Verify",
            Self::Stop { .. } => "Stop",
            Self::Finish { .. } => "Finish",
            Self::Escalate { .. } => "Escalate",
        }
    }

    #[must_use]
    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::Continue => None,
            Self::StartWorker { reason }
            | Self::Retry { reason }
            | Self::Verify { reason }
            | Self::Stop { reason }
            | Self::Finish { reason }
            | Self::Escalate { reason, .. }
            | Self::Steer { reason, .. } => Some(reason),
        }
    }

    /// Whether applying this directive ends supervision permanently.
    ///
    /// Used by the ensemble to decide whether a second opinion is worth buying:
    /// an irreversible action on a decisive value is exactly the case worth a call.
    #[must_use]
    pub const fn is_irreversible(&self) -> bool {
        matches!(self, Self::Finish { .. } | Self::Stop { .. })
    }
}

/// A hard invariant of the policy. These are non-negotiable and are evaluated
/// *before* any priority comparison, because they encode facts a score cannot see.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Invariant {
    OverallTimeout,
    AttemptsExhausted,
    WorkerLimit,
    VerificationAlreadyRun,
    AssessmentBudget,
    ModelUnavailable,
    /// Any active check with an unresolved resolution (`UnresolvedInBand`,
    /// `SuppressedByContest`, or `Unresolved`) suppresses `Finish`.
    ///
    /// This is the mechanism behind "uncertainty about a blocker must block, not
    /// permit". Without it, a high-priority inhibitor rule whose own measurement
    /// landed in the band would silently stop inhibiting, and a lower-priority
    /// `Finish` would proceed on a technicality.
    ///
    /// Stated over *any* active check rather than as one extra `Finish` conjunct
    /// per responsibility, so that a responsibility added later cannot reopen the
    /// hole by forgetting to register itself as a blocker.
    UnresolvedUncertainty,
    /// No worker has ever run, so there is nothing to declare complete.
    ///
    /// A model that answers optimistically on the first cycle — before any work exists —
    /// would otherwise finish a job that never started.
    NoWorkAttempted,
    /// A worker is still running, so finishing now would abandon it mid-turn.
    ///
    /// Distinct from [`Invariant::WorkerLimit`], which is about the *count* of workers. Both
    /// used to be recorded as `WorkerLimit`, so `aftman decisions` answered "why didn't it
    /// finish?" with the wrong reason.
    WorkerStillActive,
    /// `verify = always` and no verifier has completed, so finishing would claim a
    /// verification that never happened.
    ///
    /// [`Invariant::VerificationAlreadyRun`] means the opposite — a verifier *has* run — and
    /// was being used for both.
    VerificationIncomplete,
    /// The repository could not be read this cycle, so "nothing has changed" is not a claim
    /// the supervisor is in a position to make.
    ///
    /// Without this, a run whose repository cannot be observed — a timeout, a refused FIFO, a
    /// repository gix declines — sees empty git evidence, which is byte-identical to a clean
    /// working tree, and can finish on the strength of it. The state now says why
    /// ([`crate::observation::Observation::observation_note`]), but the wrong direction to be
    /// wrong in is concluding "nothing changed" from "nothing could be seen".
    UnobservedRepository,
}

/// Why a proposal was not selected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "reason")]
pub enum SuppressionReason {
    /// The check's measurement was in the band or contested, so policy may not
    /// evaluate its comparator at all. See `docs/06-ensemble.md` §5.2.
    NotPermitted,
    Invariant(Invariant),
    /// A different proposal had a higher priority.
    HigherPriority,
    /// The directive is not executable here: no steerable worker, no steering
    /// budget left, or a backend that cannot accept mid-turn input. The clean
    /// degradation path for non-steerable backends.
    CapabilityUnavailable,
}

/// A directive proposed by a rule, with the priority and stakes needed to order it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Proposal {
    pub responsibility: ResponsibilityId,
    pub rule: RuleId,
    pub directive: Directive,
    /// Higher wins. The authoritative table is `docs/05-responsibilities.md` §6.3.
    pub priority: u16,
    pub stakes: Stakes,
}

impl Proposal {
    #[must_use]
    pub fn new(
        responsibility: impl Into<ResponsibilityId>,
        rule: impl Into<RuleId>,
        directive: Directive,
        priority: u16,
        stakes: Stakes,
    ) -> Self {
        Self {
            responsibility: responsibility.into(),
            rule: rule.into(),
            directive,
            priority,
            stakes,
        }
    }

    /// The total-order key used to pick a winner.
    ///
    /// Priority descending, then stakes (High first), then responsibility id, then
    /// rule id. Because every rule — built-in included — has a unique id, no two
    /// distinct proposals compare equal, which is what makes arbitration
    /// reproducible from the event log.
    #[must_use]
    pub fn order_key(
        &self,
    ) -> (
        core::cmp::Reverse<u16>,
        core::cmp::Reverse<Stakes>,
        &str,
        &str,
    ) {
        (
            core::cmp::Reverse(self.priority),
            core::cmp::Reverse(self.stakes),
            self.responsibility.as_str(),
            self.rule.as_str(),
        )
    }
}

/// A proposal that policy refused, with the reason.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Suppression {
    pub responsibility: ResponsibilityId,
    pub rule: RuleId,
    pub reason: SuppressionReason,
}

/// Canonical priorities. Kept here as constants so the code and the table in
/// `docs/05-responsibilities.md` §6.3 cannot drift apart silently.
pub mod priority {
    pub const ESCALATE_NEEDS_HUMAN: u16 = 1000;
    pub const ESCALATE_BUDGET: u16 = 950;
    pub const STOP_OR_STEER_WORKER_HEALTH: u16 = 900;
    pub const STOP_OR_STEER_INSTRUCTIONS: u16 = 900;
    pub const RETRY_STOPPED: u16 = 800;
    pub const START_WORKER_DOCUMENTATION: u16 = 750;
    pub const FINISH: u16 = 700;
    pub const VERIFY: u16 = 600;
    pub const START_WORKER_COMPLETION: u16 = 500;
    pub const CONTINUE: u16 = 0;
}

#[cfg(test)]
mod tests {
    use super::{Directive, Proposal, Stakes, SteerKind};
    use crate::ids::{ResponsibilityId, RuleId};

    #[test]
    fn proposal_ordering_is_a_total_order() {
        let a = Proposal::new(
            "core.worker-health",
            "core.worker-health:stuck",
            Directive::Stop {
                reason: "stuck".into(),
            },
            900,
            Stakes::High,
        );
        let b = Proposal::new(
            "core.worker-health",
            "core.worker-health:off-track",
            Directive::Stop {
                reason: "off track".into(),
            },
            900,
            Stakes::High,
        );
        // Same responsibility, same priority, same stakes: the rule id must break
        // the tie, and it must do so consistently in both directions.
        assert_ne!(a.order_key(), b.order_key());
        assert_eq!(
            a.order_key().cmp(&b.order_key()),
            a.order_key().cmp(&b.order_key())
        );
        assert_ne!(
            a.order_key().cmp(&b.order_key()),
            core::cmp::Ordering::Equal
        );
    }

    #[test]
    fn higher_priority_sorts_first() {
        let high = Proposal::new(
            "core.human-escalation",
            "core.human-escalation:needs-human",
            Directive::Escalate {
                reason: "x".into(),
                ask: "?".into(),
            },
            1000,
            Stakes::High,
        );
        let low = Proposal::new(
            "core.completion",
            "core.completion:thresholds-satisfied",
            Directive::Finish {
                reason: "done".into(),
            },
            700,
            Stakes::High,
        );
        assert!(high.order_key() < low.order_key());
    }

    #[test]
    fn stakes_break_a_priority_tie() {
        let high = Proposal::new("r", "r:high", Directive::Continue, 800, Stakes::High);
        let low = Proposal::new("r", "r:low", Directive::Continue, 800, Stakes::Low);
        assert!(high.order_key() < low.order_key());
    }

    #[test]
    fn irreversible_directives_are_finish_and_stop_only() {
        assert!(
            Directive::Finish {
                reason: String::new()
            }
            .is_irreversible()
        );
        assert!(
            Directive::Stop {
                reason: String::new()
            }
            .is_irreversible()
        );
        assert!(
            !Directive::Verify {
                reason: String::new()
            }
            .is_irreversible()
        );
        assert!(
            !Directive::Steer {
                kind: SteerKind::Nudge,
                reason: String::new()
            }
            .is_irreversible()
        );
        assert!(!Directive::Continue.is_irreversible());
    }

    #[test]
    fn directive_names_match_the_documented_vocabulary() {
        assert_eq!(Directive::Continue.name(), "Continue");
        assert_eq!(
            Directive::StartWorker {
                reason: String::new()
            }
            .name(),
            "StartWorker"
        );
        assert_eq!(
            Directive::Verify {
                reason: String::new()
            }
            .name(),
            "Verify"
        );
        assert_eq!(
            Directive::Finish {
                reason: String::new()
            }
            .name(),
            "Finish"
        );
        assert_eq!(
            Directive::Escalate {
                reason: String::new(),
                ask: String::new()
            }
            .name(),
            "Escalate"
        );
    }

    #[test]
    fn proposals_round_trip_through_json() -> Result<(), Box<dyn std::error::Error>> {
        let p = Proposal::new(
            ResponsibilityId::new("core.verification"),
            RuleId::new("core.verification:warranted"),
            Directive::Verify {
                reason: "independent verification is warranted".into(),
            },
            600,
            Stakes::High,
        );
        let json = serde_json::to_string(&p)?;
        assert_eq!(serde_json::from_str::<Proposal>(&json)?, p);
        Ok(())
    }
}
