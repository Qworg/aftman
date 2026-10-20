//! Deterministic directive arbitration.
//!
//! [`arbitrate`] is pure: it takes a fully materialised [`PolicyInput`] and returns
//! a [`PolicyDecision`]. It reads no clock, performs no I/O, and contains no
//! randomness. That property is what makes a run replayable from its event log and
//! what makes "why did it do that?" answerable after the fact.
//!
//! It runs in two phases:
//!
//! 1. **Hard invariants.** Facts a score cannot see — timeouts, attempt counts,
//!    worker limits, whether a verifier already ran, and whether any active check
//!    is unresolved. Non-negotiable.
//! 2. **Priority.** The highest-priority surviving proposal wins, with a total
//!    order so the result is never ambiguous.
//!
//! See `docs/05-responsibilities.md` §6.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::assessment::ResolvedCheck;
use crate::directive::{Directive, Invariant, Proposal, Suppression, SuppressionReason, priority};
use crate::ids::CheckKey;
use crate::job::{FactoryStatus, RunConfig};
use crate::observation::{Counters, Observation};
use crate::worker::WorkerRecord;

/// Everything policy is allowed to look at.
///
/// Note what is here: an already-resolved `now` and the active worker record. An
/// earlier revision asserted policy purity while leaving the steering-grace window
/// unreachable, which would have forced an implementer to read the clock inside a
/// function advertised as pure. All impurity is confined to the runtime that
/// materialises this struct.
#[derive(Debug)]
pub struct PolicyInput<'a> {
    pub status: FactoryStatus,
    pub counters: &'a Counters,
    pub config: &'a RunConfig,
    /// Per-check permissions, keyed by wire key.
    pub checks: &'a BTreeMap<CheckKey, ResolvedCheck>,
    pub observation: &'a Observation,
    /// Materialised so `arbitrate` needs no clock.
    pub now: OffsetDateTime,
    pub active_worker: Option<&'a WorkerRecord>,
    /// The previous cycle's selected directive, so a stop-then-retry path can be
    /// recognised without re-deriving it from events.
    pub last_directive: Option<&'a Directive>,
    /// Whether the ensemble is enabled, which is what makes a band hold a rule.
    pub ensemble_enabled: bool,
    pub steering_grace: Duration,
}

impl PolicyInput<'_> {
    /// Whether policy may evaluate a `>=` comparator for this check.
    #[must_use]
    pub fn check_ge(&self, key: &CheckKey, threshold: f32) -> bool {
        self.checks.get(key).is_some_and(|c| c.at_least(threshold))
    }

    /// Whether policy may evaluate a `<` comparator for this check.
    ///
    /// The `<` direction is as legitimate as `>=`, and an in-band or contested
    /// check must fail *both*.
    #[must_use]
    pub fn check_lt(&self, key: &CheckKey, threshold: f32) -> bool {
        self.checks.get(key).is_some_and(|c| c.less_than(threshold))
    }

    /// The permitted value for a check, if any.
    #[must_use]
    pub fn check_value(&self, key: &CheckKey) -> Option<f32> {
        self.checks
            .get(key)
            .and_then(ResolvedCheck::permitted_value)
    }

    /// Whether any active check is holding resolution back.
    ///
    /// This is the predicate behind [`Invariant::UnresolvedUncertainty`]: if any
    /// check is unresolved, `Finish` is suppressed. Without it, a high-priority
    /// inhibitor rule whose own measurement landed in the band would silently stop
    /// inhibiting, and a lower-priority `Finish` would proceed on a technicality.
    #[must_use]
    pub fn any_unresolved(&self) -> bool {
        self.checks.values().any(ResolvedCheck::is_unresolved)
    }

    /// Whether the worker may be steered right now.
    #[must_use]
    pub fn steering_available(&self) -> bool {
        let Some(worker) = self.active_worker else {
            return false;
        };
        self.config.steering_enabled
            && worker.supports_steering
            && worker.steer_count() < self.config.max_steers_per_worker
            && !self.within_steering_grace()
    }

    /// Whether the active worker is inside the post-steer grace window.
    #[must_use]
    pub fn within_steering_grace(&self) -> bool {
        self.steered_within(self.steering_grace)
    }

    /// Whether a steer was delivered more recently than `grace` ago.
    ///
    /// A responsibility may declare its own grace window in its definition, so the
    /// duration is a parameter rather than only run configuration.
    #[must_use]
    pub fn steered_within(&self, grace: Duration) -> bool {
        let Some(worker) = self.active_worker else {
            return false;
        };
        let Some(last) = worker.last_steered_at else {
            return false;
        };
        let since: Duration = (self.now - last).try_into().unwrap_or(Duration::ZERO);
        since < grace
    }

    /// Whether the worker may be steered, given a responsibility's own steer budget.
    #[must_use]
    pub fn steering_available_with_limit(&self, max_steers: u32) -> bool {
        let Some(worker) = self.active_worker else {
            return false;
        };
        self.config.steering_enabled
            && worker.supports_steering
            && worker.steer_count() < max_steers
            && !self.within_steering_grace()
    }

    /// Whether the previous selected directive stopped the worker.
    #[must_use]
    pub fn last_directive_was_stop(&self) -> bool {
        matches!(self.last_directive, Some(Directive::Stop { .. }))
    }

    /// Whether the overall run timeout has expired.
    #[must_use]
    pub fn overall_timeout_exceeded(&self) -> bool {
        Duration::from_secs(self.counters.elapsed_secs) >= self.config.overall_timeout
    }
}

/// What policy decided, and what it rejected on the way.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolicyDecision {
    pub selected: Directive,
    /// Every proposal considered, in priority order.
    pub considered: Vec<Proposal>,
    /// Every proposal refused, with the reason. Makes "why didn't it stop?"
    /// answerable from the log alone.
    pub suppressed: Vec<Suppression>,
    /// Invariants that fired this cycle.
    pub invariants: Vec<Invariant>,
}

impl PolicyDecision {
    fn bare(directive: Directive) -> Self {
        Self {
            selected: directive,
            considered: Vec::new(),
            suppressed: Vec::new(),
            invariants: Vec::new(),
        }
    }
}

/// Select exactly one directive under hard invariants.
///
/// Pure. Same input, same output, always.
#[must_use]
pub fn arbitrate(input: &PolicyInput<'_>, proposals: &[Proposal]) -> PolicyDecision {
    let mut suppressed: Vec<Suppression> = Vec::new();
    let mut invariants: Vec<Invariant> = Vec::new();

    // ---- Phase 1: hard invariants ------------------------------------------
    //
    // These are evaluated before any priority comparison because they encode facts
    // a probability cannot see.

    if input.overall_timeout_exceeded() {
        invariants.push(Invariant::OverallTimeout);
        return PolicyDecision {
            selected: Directive::Stop {
                reason: "overall run timeout exceeded".into(),
            },
            considered: proposals.to_vec(),
            suppressed,
            invariants,
        };
    }

    if input.counters.assessments >= input.config.max_assessments {
        invariants.push(Invariant::AssessmentBudget);
        return PolicyDecision {
            selected: Directive::Escalate {
                reason: "assessment budget exhausted".into(),
                ask: format!(
                    "The assessment budget ({} model calls) is spent, so aftman will not ask the \
                     model again for this run. Stop ends the run. Any other answer lets the \
                     worker continue without model supervision, and this question will be \
                     raised again at the next cycle.",
                    input.config.max_assessments
                ),
            },
            considered: proposals.to_vec(),
            suppressed,
            invariants,
        };
    }

    // A repeated model failure means supervision is blind. This is deliberately a
    // *different* message from "the model thinks a human is needed": conflating
    // them is how a supervisor loses an operator's trust.
    // A request that cannot be compacted is not a model failure. It is the observation being
    // too large to send, which is a fact about the repository and the caps, and it needs its
    // own answer: "reduce the evidence or raise the budget", not "the model is gone".
    if input.counters.consecutive_budget_failures >= 3 {
        invariants.push(Invariant::ObservationTooLarge);
        return PolicyDecision {
            selected: Directive::Escalate {
                reason: "observation_too_large: the request cannot be compacted to fit".into(),
                ask: "The repository evidence does not fit the request budget. Raise \
                      AFTMAN_JEV_REQUEST_BUDGET_BYTES, or reduce what the observation \
                      collects."
                    .into(),
            },
            considered: proposals.to_vec(),
            suppressed,
            invariants,
        };
    }
    if input.counters.consecutive_model_failures >= 3 {
        invariants.push(Invariant::ModelUnavailable);
        return PolicyDecision {
            selected: Directive::Escalate {
                reason: "model_unavailable: supervision is blind".into(),
                ask: "The decision model has failed three cycles in a row.".into(),
            },
            considered: proposals.to_vec(),
            suppressed,
            invariants,
        };
    }

    let workers_exhausted = input.counters.workers_run >= input.config.max_workers;
    if workers_exhausted {
        invariants.push(Invariant::WorkerLimit);
    }
    if input.counters.verifiers_run > 0 {
        invariants.push(Invariant::VerificationAlreadyRun);
    }
    if input.active_worker.is_some() {
        invariants.push(Invariant::WorkerStillActive);
    }
    if input.config.verify == crate::job::VerifyPolicy::Always && !input.counters.verifier_completed
    {
        invariants.push(Invariant::VerificationIncomplete);
    }
    if input.counters.attempts >= input.config.max_attempts {
        invariants.push(Invariant::AttemptsExhausted);
    }
    let uncertainty_blocks_finish = input.any_unresolved();
    if uncertainty_blocks_finish {
        invariants.push(Invariant::UnresolvedUncertainty);
    }
    if input.counters.workers_run == 0 {
        invariants.push(Invariant::NoWorkAttempted);
    }
    if input.observation.observation_note.is_some() {
        invariants.push(Invariant::UnobservedRepository);
    }

    // ---- Phase 2: filter, then take the highest priority -------------------

    let mut surviving: Vec<Proposal> = Vec::new();
    for proposal in proposals {
        if let Some(reason) = refusal(
            input,
            proposal,
            workers_exhausted,
            uncertainty_blocks_finish,
        ) {
            suppressed.push(Suppression {
                responsibility: proposal.responsibility.clone(),
                rule: proposal.rule.clone(),
                reason,
            });
        } else {
            surviving.push(proposal.clone());
        }
    }

    // Priority descending, then stakes, then responsibility id, then rule id.
    surviving.sort_by(|a, b| a.order_key().cmp(&b.order_key()));

    // A proposal that needs a worker, refused *only* because the worker limit is
    // reached, must not fall through to `Continue`. With no worker left to run and
    // no other proposal, `Continue` means "do nothing forever". docs/05 §6.1 says
    // this case escalates, and that is the only honest terminal outcome.
    let worker_limit_hit = suppressed.iter().any(|s| {
        matches!(
            s.reason,
            SuppressionReason::Invariant(Invariant::WorkerLimit)
        )
    });
    // `surviving.is_empty()` matters: without it this generic escalation replaced the
    // selection even when a higher-priority proposal had survived the same cycle, so a
    // specific `ask` from `core.human-escalation` was discarded in favour of
    // "worker budget is exhausted".
    if worker_limit_hit && input.active_worker.is_none() && surviving.is_empty() {
        return PolicyDecision {
            selected: Directive::Escalate {
                reason: "worker limit reached before completion".into(),
                ask: "The worker budget is exhausted and the job does not look complete.".into(),
            },
            considered: proposals.to_vec(),
            suppressed,
            invariants,
        };
    }

    let Some(winner) = surviving.first().cloned() else {
        // Nothing survived. With a worker running that means "carry on"; with no worker
        // there is nothing to carry on *with*, and returning `Continue` would spin on the
        // idle ticker until the run timed out. The suppressions are kept either way:
        // when nothing happens, "why not?" is the question that matters most.
        let selected = if input.active_worker.is_none() {
            Directive::Escalate {
                reason: "nothing can make progress: no proposal survived and no worker is running"
                    .into(),
                ask: "No worker is running and every proposal was refused.".into(),
            }
        } else {
            Directive::Continue
        };
        return PolicyDecision {
            selected,
            considered: proposals.to_vec(),
            suppressed,
            invariants,
        };
    };

    // With no active worker, `Continue` is not a decision — it is a stall. Nothing
    // would ever change, so escalate rather than spin.
    if input.active_worker.is_none() && matches!(winner.directive, Directive::Continue) {
        return PolicyDecision {
            selected: Directive::Escalate {
                reason: "no active worker and nothing left to try".into(),
                ask: "No worker is running and no proposal can make progress.".into(),
            },
            considered: proposals.to_vec(),
            suppressed,
            invariants,
        };
    }

    // Every non-winner is suppressed by priority.
    for proposal in &surviving {
        if proposal.rule != winner.rule {
            suppressed.push(Suppression {
                responsibility: proposal.responsibility.clone(),
                rule: proposal.rule.clone(),
                reason: SuppressionReason::HigherPriority,
            });
        }
    }

    PolicyDecision {
        selected: winner.directive,
        considered: proposals.to_vec(),
        suppressed,
        invariants,
    }
}

/// Whether a proposal is refused by a hard invariant. `None` means it survives.
fn refusal(
    input: &PolicyInput<'_>,
    proposal: &Proposal,
    workers_exhausted: bool,
    uncertainty_blocks_finish: bool,
) -> Option<SuppressionReason> {
    match &proposal.directive {
        // `Finish` is the one irreversible action, so it carries the most guards.
        Directive::Finish { .. } => {
            // Nothing has been attempted, so nothing can be finished. Without this, a
            // model that answers optimistically on the first cycle — before any worker
            // has run — ends a job that never started. This is a runtime fact, not a
            // judgement, so it belongs here rather than in a check.
            if input.counters.workers_run == 0 {
                return Some(SuppressionReason::Invariant(Invariant::NoWorkAttempted));
            }
            // The repository could not be read, so "nothing has changed" is an absence of
            // evidence rather than evidence of absence. Same posture as uncertainty: the one
            // input that cannot be re-derived from anything else in the state has to block
            // rather than quietly read as clean.
            if input.observation.observation_note.is_some() {
                return Some(SuppressionReason::Invariant(
                    Invariant::UnobservedRepository,
                ));
            }
            if uncertainty_blocks_finish {
                return Some(SuppressionReason::Invariant(
                    Invariant::UnresolvedUncertainty,
                ));
            }
            if input.active_worker.is_some() {
                return Some(SuppressionReason::Invariant(Invariant::WorkerStillActive));
            }
            if input.config.verify == crate::job::VerifyPolicy::Always
                && !input.counters.verifier_completed
            {
                return Some(SuppressionReason::Invariant(
                    Invariant::VerificationIncomplete,
                ));
            }
            None
        }
        Directive::Retry { .. } => {
            if workers_exhausted {
                return Some(SuppressionReason::Invariant(Invariant::WorkerLimit));
            }
            if input.counters.attempts >= input.config.max_attempts {
                return Some(SuppressionReason::Invariant(Invariant::AttemptsExhausted));
            }
            None
        }
        Directive::StartWorker { .. } => {
            if workers_exhausted {
                return Some(SuppressionReason::Invariant(Invariant::WorkerLimit));
            }
            None
        }
        Directive::Verify { .. } => {
            // A verifier judges a settled tree. While a code worker is running the tree is
            // still moving, so a verification would measure something that no longer exists
            // by the time it finishes — and the runtime used to abandon that worker rather
            // than stop it. The built-in `core.verification` rule already holds back here;
            // this closes it for a user-defined rule that proposes `verify` outright.
            if input.active_worker.is_some() {
                return Some(SuppressionReason::Invariant(Invariant::WorkerStillActive));
            }
            if input.counters.verifiers_run > 0 {
                return Some(SuppressionReason::Invariant(
                    Invariant::VerificationAlreadyRun,
                ));
            }
            if workers_exhausted {
                return Some(SuppressionReason::Invariant(Invariant::WorkerLimit));
            }
            None
        }
        Directive::Steer { .. } => {
            if input.active_worker.is_none() {
                return Some(SuppressionReason::CapabilityUnavailable);
            }
            if !input.steering_available() {
                // A non-steerable backend, an exhausted steer budget, or an active
                // grace window: degrade rather than silently drop the guidance.
                return Some(SuppressionReason::CapabilityUnavailable);
            }
            None
        }
        Directive::Continue | Directive::Stop { .. } | Directive::Escalate { .. } => None,
    }
}

/// A decision that does nothing, for tests and for the no-op ensemble stage.
#[must_use]
pub fn continue_decision() -> PolicyDecision {
    PolicyDecision::bare(Directive::Continue)
}

/// The documented priority table, as a slice, so the code and `docs/05` §6.3 can be
/// compared in one place.
#[must_use]
pub fn documented_priorities() -> Vec<(&'static str, u16)> {
    vec![
        (
            "core.human-escalation:needs-human",
            priority::ESCALATE_NEEDS_HUMAN,
        ),
        ("runtime:budget-exhausted", priority::ESCALATE_BUDGET),
        (
            "core.worker-health:stuck",
            priority::STOP_OR_STEER_WORKER_HEALTH,
        ),
        (
            "core.worker-health:off-track",
            priority::STOP_OR_STEER_WORKER_HEALTH,
        ),
        (
            "repository.instructions:drift",
            priority::STOP_OR_STEER_INSTRUCTIONS,
        ),
        ("core.completion:retry-stopped", priority::RETRY_STOPPED),
        (
            "quality.documentation:insufficient",
            priority::START_WORKER_DOCUMENTATION,
        ),
        ("core.completion:thresholds-satisfied", priority::FINISH),
        ("core.verification:warranted", priority::VERIFY),
        (
            "core.completion:work-remains",
            priority::START_WORKER_COMPLETION,
        ),
        ("runtime:default", priority::CONTINUE),
    ]
}

#[cfg(test)]
mod tests {
    use super::{PolicyInput, arbitrate};
    use crate::assessment::ResolvedCheck;
    use crate::directive::{Directive, Invariant, Proposal, Stakes, SuppressionReason};
    use crate::ids::{CheckId, CheckKey, ResponsibilityId};
    use crate::job::{FactoryStatus, JobSummary, RunConfig, WorkerKind};
    use crate::observation::{Counters, Observation};
    use crate::responsibility::builtin;
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::time::Duration;
    use time::OffsetDateTime;
    use time::macros::datetime;

    fn key(responsibility: &str, check: &str) -> CheckKey {
        CheckKey::new(&ResponsibilityId::new(responsibility), &CheckId::new(check))
    }

    fn aftman_core_log(capacity: usize) -> crate::log::BoundedLog {
        crate::log::BoundedLog::new(capacity, capacity)
    }

    fn observation() -> Observation {
        Observation {
            run_id: crate::ids::RunId::new(),
            status: FactoryStatus::Assessing,
            job: JobSummary {
                prompt_excerpt: "job".into(),
                prompt_truncated: false,
                repo: PathBuf::from("/repo"),
                elapsed_secs: 0,
            },
            active: None,
            history: Vec::new(),
            git: crate::observation::GitEvidence::default(),
            untracked: Vec::new(),
            instructions: None,
            tests: Vec::new(),
            events: Vec::new(),
            prior: None,
            counters: Counters::default(),
            evidence: BTreeMap::new(),
            output: None,
            activity: None,
            observation_note: None,
        }
    }

    fn checks(entries: &[(&str, &str, f32)]) -> BTreeMap<CheckKey, ResolvedCheck> {
        entries
            .iter()
            .map(|(r, c, p)| {
                let k = key(r, c);
                let rc =
                    ResolvedCheck::from_measurement(k.clone(), Some(*p), Some(0.75), 0.15, true);
                (k, rc)
            })
            .collect()
    }

    fn input<'a>(
        observation: &'a Observation,
        counters: &'a Counters,
        config: &'a RunConfig,
        checks: &'a BTreeMap<CheckKey, ResolvedCheck>,
    ) -> PolicyInput<'a> {
        PolicyInput {
            status: FactoryStatus::Assessing,
            counters,
            config,
            checks,
            observation,
            now: datetime!(2026-01-01 00:00:00 UTC),
            active_worker: None,
            last_directive: None,
            ensemble_enabled: true,
            steering_grace: Duration::from_secs(30),
        }
    }

    fn finish() -> Proposal {
        Proposal::new(
            builtin::COMPLETION,
            "core.completion:thresholds-satisfied",
            Directive::Finish {
                reason: "done".into(),
            },
            700,
            Stakes::High,
        )
    }

    #[test]
    fn no_proposals_with_no_worker_escalates_rather_than_spinning() {
        // With nothing to do and nothing running, `Continue` means "wait forever".
        let o = observation();
        let counters = Counters::default();
        let config = RunConfig::default();
        let checks = BTreeMap::new();
        let decision = arbitrate(&input(&o, &counters, &config, &checks), &[]);
        assert!(
            matches!(decision.selected, Directive::Escalate { .. }),
            "{:?}",
            decision.selected
        );
    }

    #[test]
    fn no_proposals_with_a_running_worker_means_continue() {
        // The complementary case, and the one the default exists for: a worker is
        // running and nothing objects, so let it work.
        let o = observation();
        let counters = Counters {
            workers_run: 1,
            ..Counters::default()
        };
        let config = RunConfig::default();
        let checks = BTreeMap::new();
        let worker = crate::worker::WorkerRecord::new(
            crate::ids::WorkerId::new(),
            WorkerKind::Simulated,
            crate::job::TaskKind::Code,
            "mission",
            aftman_core_log(64),
            true,
        );
        let mut policy_input = input(&o, &counters, &config, &checks);
        policy_input.active_worker = Some(&worker);
        let decision = arbitrate(&policy_input, &[]);
        assert_eq!(decision.selected, Directive::Continue);
    }

    #[test]
    fn an_unresolved_check_suppresses_finish() {
        // The round-3 defect: a high-priority inhibitor whose own measurement is in
        // band stops inhibiting, and Finish proceeds on a technicality. Here the
        // documentation check is in band and Finish must be refused.
        let o = observation();
        let counters = Counters {
            // `Finish` requires that work was attempted, so every test that expects it
            // to be evaluated has to say that a worker ran.
            workers_run: 1,
            verifier_completed: true,
            ..Counters::default()
        };
        let config = RunConfig::default();
        let mut checks = checks(&[
            ("core.completion", "ready_to_finish", 0.95),
            ("core.completion", "requirements_satisfied", 0.95),
            ("core.verification", "tests_sufficient", 0.95),
        ]);
        // In band: |0.70 - 0.75| = 0.05 < 0.15.
        let doc = key("quality.documentation", "documentation_sufficient");
        checks.insert(
            doc.clone(),
            ResolvedCheck::from_measurement(doc, Some(0.70), Some(0.75), 0.15, true),
        );

        let decision = arbitrate(&input(&o, &counters, &config, &checks), &[finish()]);
        assert_ne!(
            decision.selected,
            finish().directive,
            "Finish must be refused"
        );
        assert!(
            decision
                .invariants
                .contains(&Invariant::UnresolvedUncertainty)
        );
        assert!(decision.suppressed.iter().any(|s| matches!(
            s.reason,
            SuppressionReason::Invariant(Invariant::UnresolvedUncertainty)
        )));
    }

    #[test]
    fn finish_is_refused_when_no_work_has_been_attempted() {
        // A model that answers optimistically on the first cycle — before any worker has
        // run — must not end a job that never started.
        let o = observation();
        let counters = Counters {
            workers_run: 0,
            verifier_completed: true,
            ..Counters::default()
        };
        let config = RunConfig::default();
        let checks = checks(&[
            ("core.completion", "ready_to_finish", 0.95),
            ("core.completion", "requirements_satisfied", 0.95),
            ("core.verification", "tests_sufficient", 0.95),
            ("core.verification", "needs_verification", 0.10),
        ]);
        let decision = arbitrate(&input(&o, &counters, &config, &checks), &[finish()]);
        assert_ne!(decision.selected, finish().directive);
        assert!(decision.invariants.contains(&Invariant::NoWorkAttempted));
        assert!(decision.suppressed.iter().any(|s| matches!(
            s.reason,
            SuppressionReason::Invariant(Invariant::NoWorkAttempted)
        )));
    }

    #[test]
    fn finish_is_refused_when_the_repository_could_not_be_observed() {
        // Empty git evidence is byte-identical to a clean tree. When the observation failed,
        // "nothing has changed" is an absence of evidence — and `Finish` is the one
        // irreversible action, so it must not be taken on it. Without this, a run whose
        // repository could not be read (a timeout, a refused FIFO, a directory that is not a
        // repository) would be able to declare complete work nobody could see.
        let mut o = observation();
        o.observation_note = Some("repository observation exceeded 8s".to_owned());
        let counters = Counters {
            workers_run: 1,
            verifier_completed: true,
            ..Counters::default()
        };
        let config = RunConfig::default();
        let checks = checks(&[
            ("core.completion", "ready_to_finish", 0.95),
            ("core.completion", "requirements_satisfied", 0.95),
            ("core.verification", "tests_sufficient", 0.95),
            ("core.verification", "needs_verification", 0.10),
        ]);
        let decision = arbitrate(&input(&o, &counters, &config, &checks), &[finish()]);
        assert_ne!(
            decision.selected,
            finish().directive,
            "Finish must be refused"
        );
        assert!(
            decision
                .invariants
                .contains(&Invariant::UnobservedRepository)
        );
        assert!(decision.suppressed.iter().any(|s| matches!(
            s.reason,
            SuppressionReason::Invariant(Invariant::UnobservedRepository)
        )));
    }

    #[test]
    fn a_fully_resolved_set_lets_finish_through() {
        let o = observation();
        let counters = Counters {
            // `Finish` requires that work was attempted, so every test that expects it
            // to be evaluated has to say that a worker ran.
            workers_run: 1,
            verifier_completed: true,
            ..Counters::default()
        };
        let config = RunConfig::default();
        let checks = checks(&[
            ("core.completion", "ready_to_finish", 0.95),
            ("core.completion", "requirements_satisfied", 0.95),
            ("core.verification", "tests_sufficient", 0.95),
            ("core.verification", "needs_verification", 0.10),
        ]);
        let decision = arbitrate(&input(&o, &counters, &config, &checks), &[finish()]);
        assert_eq!(decision.selected, finish().directive);
        assert!(
            !decision
                .invariants
                .contains(&Invariant::UnresolvedUncertainty)
        );
    }

    #[test]
    fn a_low_priority_proposal_loses_to_a_higher_one() {
        let o = observation();
        let counters = Counters {
            workers_run: 1,
            ..Counters::default()
        };
        let config = RunConfig::default();
        let checks = BTreeMap::new();
        let escalate = Proposal::new(
            builtin::HUMAN_ESCALATION,
            "core.human-escalation:needs-human",
            Directive::Escalate {
                reason: "x".into(),
                ask: "?".into(),
            },
            1000,
            Stakes::High,
        );
        let decision = arbitrate(
            &input(&o, &counters, &config, &checks),
            &[finish(), escalate.clone()],
        );
        assert_eq!(decision.selected, escalate.directive);
        assert!(decision.suppressed.iter().any(|s| s.rule.as_str()
            == "core.completion:thresholds-satisfied"
            && s.reason == SuppressionReason::HigherPriority));
    }

    #[test]
    fn a_steer_is_refused_when_no_worker_is_steerable() {
        let o = observation();
        let counters = Counters::default();
        let config = RunConfig::default();
        let checks = BTreeMap::new();
        let steer = Proposal::new(
            builtin::WORKER_HEALTH,
            "core.worker-health:stuck",
            Directive::Steer {
                kind: crate::directive::SteerKind::Nudge,
                reason: "stuck".into(),
            },
            900,
            Stakes::High,
        );
        // No active worker at all, so steering is not available.
        let decision = arbitrate(&input(&o, &counters, &config, &checks), &[steer]);
        // The steer is refused, and with no worker running there is nothing left to do.
        assert!(!decision.suppressed.is_empty());
        assert!(matches!(decision.selected, Directive::Escalate { .. }));
    }

    #[test]
    fn retry_is_refused_once_attempts_are_exhausted() {
        let o = observation();
        let config = RunConfig::default();
        let counters = Counters {
            attempts: config.max_attempts,
            ..Counters::default()
        };
        let checks = BTreeMap::new();
        let retry = Proposal::new(
            builtin::COMPLETION,
            "core.completion:retry-stopped",
            Directive::Retry {
                reason: "again".into(),
            },
            800,
            Stakes::High,
        );
        let decision = arbitrate(&input(&o, &counters, &config, &checks), &[retry]);
        // The retry is refused, and with no worker running there is nothing to continue
        // with, so the run escalates rather than looping.
        assert!(
            !matches!(decision.selected, Directive::Retry { .. }),
            "retry must not be selected: {:?}",
            decision.selected
        );
        assert!(decision.invariants.contains(&Invariant::AttemptsExhausted));
    }

    #[test]
    fn the_overall_timeout_wins_over_everything() {
        let o = observation();
        let config = RunConfig::default();
        let counters = Counters {
            elapsed_secs: config.overall_timeout.as_secs() + 1,
            ..Counters::default()
        };
        let checks = BTreeMap::new();
        let decision = arbitrate(&input(&o, &counters, &config, &checks), &[finish()]);
        assert!(matches!(decision.selected, Directive::Stop { .. }));
        assert!(decision.invariants.contains(&Invariant::OverallTimeout));
    }

    #[test]
    fn repeated_model_failure_escalates_and_says_supervision_is_blind() {
        let o = observation();
        let config = RunConfig::default();
        let counters = Counters {
            consecutive_model_failures: 3,
            ..Counters::default()
        };
        let checks = BTreeMap::new();
        let decision = arbitrate(&input(&o, &counters, &config, &checks), &[]);
        match decision.selected {
            Directive::Escalate { reason, .. } => {
                assert!(reason.contains("supervision is blind"), "{reason}");
            }
            other => panic!("expected an escalation, got {other:?}"),
        }
        assert!(decision.invariants.contains(&Invariant::ModelUnavailable));
    }

    #[test]
    fn assessment_budget_exhaustion_escalates_rather_than_stopping_blindly() {
        let o = observation();
        let config = RunConfig::default();
        let counters = Counters {
            assessments: config.max_assessments,
            ..Counters::default()
        };
        let checks = BTreeMap::new();
        let decision = arbitrate(&input(&o, &counters, &config, &checks), &[]);
        assert!(matches!(decision.selected, Directive::Escalate { .. }));
        assert!(decision.invariants.contains(&Invariant::AssessmentBudget));
    }

    #[test]
    fn arbitration_is_deterministic() {
        let o = observation();
        let counters = Counters::default();
        let config = RunConfig::default();
        let checks = checks(&[("core.completion", "ready_to_finish", 0.95)]);
        let proposals = vec![finish()];
        let first = arbitrate(&input(&o, &counters, &config, &checks), &proposals);
        for _ in 0..16 {
            let again = arbitrate(&input(&o, &counters, &config, &checks), &proposals);
            assert_eq!(first, again, "policy must be a pure function of its input");
        }
    }

    #[test]
    fn every_documented_priority_appears_exactly_once() {
        let table = super::documented_priorities();
        let mut names: Vec<&str> = table.iter().map(|(name, _)| *name).collect();
        let count = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(
            names.len(),
            count,
            "duplicate rule id in the priority table"
        );
        // Ordering sanity: escalate beats stop beats finish beats continue.
        let get = |needle: &str| {
            table
                .iter()
                .find(|(name, _)| *name == needle)
                .map(|(_, p)| *p)
        };
        assert!(get("core.human-escalation:needs-human") > get("core.worker-health:stuck"));
        assert!(get("core.worker-health:stuck") > get("core.completion:thresholds-satisfied"));
        assert!(get("core.completion:thresholds-satisfied") > get("runtime:default"));
    }

    #[test]
    fn a_less_than_rule_is_not_collateral_damage() {
        // `documentation_sufficient < 0.75` must be reachable when the value is
        // decisively below. Under the pre-round-2 definition (`permitted = p >= t`)
        // every `<` rule was dead.
        let o = observation();
        let counters = Counters::default();
        let config = RunConfig::default();
        let doc = key("quality.documentation", "documentation_sufficient");
        let mut checks = BTreeMap::new();
        checks.insert(
            doc.clone(),
            ResolvedCheck::from_measurement(doc, Some(0.20), Some(0.75), 0.15, true),
        );
        let policy_input = input(&o, &counters, &config, &checks);
        assert!(policy_input.check_lt(
            &key("quality.documentation", "documentation_sufficient"),
            0.75
        ));
        // And the same check must not fire the `>=` direction.
        assert!(!policy_input.check_ge(
            &key("quality.documentation", "documentation_sufficient"),
            0.75
        ));
        let _ = OffsetDateTime::UNIX_EPOCH;
        let _ = WorkerKind::Simulated;
    }

    #[test]
    fn an_in_band_check_fails_both_comparison_directions() {
        let o = observation();
        let counters = Counters::default();
        let config = RunConfig::default();
        let k = key("core.verification", "needs_verification");
        let mut checks = BTreeMap::new();
        checks.insert(
            k.clone(),
            ResolvedCheck::from_measurement(k.clone(), Some(0.60), Some(0.65), 0.15, true),
        );
        let policy_input = input(&o, &counters, &config, &checks);
        assert!(
            !policy_input.check_lt(&k, 0.65),
            "in band must not satisfy `<`"
        );
        assert!(
            !policy_input.check_ge(&k, 0.65),
            "in band must not satisfy `>=`"
        );
        assert!(policy_input.any_unresolved());
    }
}
