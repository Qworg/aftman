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

    // Spent, or too little of it left to ask a whole cycle, which asks one request per
    // evidence selection (`docs/03-jev.md` §5): either way the model is not asked again.
    let budget_spent = input.counters.assessments >= input.config.max_assessments;
    if budget_spent || input.counters.assessment_budget_short {
        invariants.push(Invariant::AssessmentBudget);
        let how = if budget_spent {
            "is spent"
        } else {
            "has too little left to ask a whole cycle"
        };
        return PolicyDecision {
            selected: Directive::Escalate {
                reason: "assessment budget exhausted".into(),
                ask: format!(
                    "The assessment budget ({} model calls) {how}, so aftman will not ask the \
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
    if input.counters.verified_since_code {
        invariants.push(Invariant::VerificationAlreadyRun);
    }
    if input.active_worker.is_some() {
        invariants.push(Invariant::WorkerStillActive);
    }
    if input.config.verify == crate::job::VerifyPolicy::Always && !input.counters.verifier_completed
    {
        invariants.push(Invariant::VerificationIncomplete);
    }
    if input.config.verify == crate::job::VerifyPolicy::Never {
        invariants.push(Invariant::VerificationDisabled);
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
                ask: nothing_survived_ask(input, &suppressed),
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

    // A `Finish` may not win just because a proposal that outranks it, asking for work, was
    // refused by the worker limit (`docs/05-responsibilities.md` §6.3). That rule still says
    // the job is not done — `quality.documentation:insufficient` at 750 over `Finish` at 700 is
    // the inhibition the priority order exists for — and the limit only means aftman may not
    // start the work it asks for. Finishing over it would read "no worker left" as "nothing
    // left to do". So a person is asked, and told which rule still asks for work.
    if matches!(winner.directive, Directive::Finish { .. })
        && let Some(blocked) = refused_work_outranking(proposals, &suppressed, &winner)
    {
        suppressed.push(Suppression {
            responsibility: winner.responsibility.clone(),
            rule: winner.rule.clone(),
            reason: SuppressionReason::Invariant(Invariant::WorkerLimit),
        });
        return PolicyDecision {
            selected: Directive::Escalate {
                reason: format!(
                    "worker limit reached while `{}` still asks for work",
                    blocked.rule
                ),
                ask: worker_limit_ask(input, blocked),
            },
            considered: proposals.to_vec(),
            suppressed,
            invariants,
        };
    }

    PolicyDecision {
        selected: winner.directive,
        considered: proposals.to_vec(),
        suppressed,
        invariants,
    }
}

/// Whether a directive asks for a worker to be started.
const fn starts_work(directive: &Directive) -> bool {
    matches!(
        directive,
        Directive::StartWorker { .. } | Directive::Retry { .. } | Directive::Verify { .. }
    )
}

/// The highest-ranked proposal that asks for work, would have outranked `winner`, and was
/// refused by the worker limit (and by nothing before it).
fn refused_work_outranking<'a>(
    proposals: &'a [Proposal],
    suppressed: &[Suppression],
    winner: &Proposal,
) -> Option<&'a Proposal> {
    proposals
        .iter()
        .filter(|p| starts_work(&p.directive) && p.order_key() < winner.order_key())
        .filter(|p| {
            suppressed.iter().any(|s| {
                s.rule == p.rule
                    && matches!(
                        s.reason,
                        SuppressionReason::Invariant(Invariant::WorkerLimit)
                    )
            })
        })
        .min_by(|a, b| a.order_key().cmp(&b.order_key()))
}

/// The most bytes the worker-limit question may have, like the others policy asks.
pub const WORKER_LIMIT_ASK_CAP_BYTES: usize = 2048;

/// The question when the worker limit refused work a rule still asks for, over a `Finish`.
fn worker_limit_ask(input: &PolicyInput<'_>, blocked: &Proposal) -> String {
    let mut ask = format!(
        "The worker limit ({} workers) is reached, and `{}` still asks for work ({}, priority \
         {}: \"{}\"), so aftman will not finish over it and cannot start that work. Stop ends \
         the run. Any other answer resumes it, and this is asked again while the rule still \
         asks for work.",
        input.config.max_workers,
        blocked.rule,
        blocked.directive.name(),
        blocked.priority,
        blocked.directive.reason().unwrap_or_default(),
    );
    if ask.len() > WORKER_LIMIT_ASK_CAP_BYTES {
        let mut end = WORKER_LIMIT_ASK_CAP_BYTES;
        while !ask.is_char_boundary(end) {
            end -= 1;
        }
        ask.truncate(end);
    }
    ask
}

/// The most unresolved checks the "nothing can make progress" question names one by one.
pub const NAMED_UNRESOLVED_CHECKS: usize = 8;

/// The most bytes that question may have, like an ensemble escalation's
/// (`docs/06-ensemble.md` §6).
pub const NOTHING_SURVIVED_ASK_CAP_BYTES: usize = 2048;

/// The question when nothing can make progress.
///
/// When a `Finish` was refused because an active check is unresolved
/// (`Invariant::UnresolvedUncertainty`), a person cannot answer without knowing which check
/// is holding it — typically one in the band that no rule reads, so nothing else names it.
/// The question lists every unresolved check, its key, Jev's value and its threshold, and why
/// it is held; at most [`NAMED_UNRESOLVED_CHECKS`] of them, then how many more, and never
/// more than [`NOTHING_SURVIVED_ASK_CAP_BYTES`]. The values are Jev's own, unmodified.
fn nothing_survived_ask(input: &PolicyInput<'_>, suppressed: &[Suppression]) -> String {
    const GENERIC: &str = "No worker is running and every proposal was refused.";
    let held_finish = suppressed.iter().any(|s| {
        matches!(
            s.reason,
            SuppressionReason::Invariant(Invariant::UnresolvedUncertainty)
        )
    });
    let unresolved: Vec<(&CheckKey, &ResolvedCheck)> = input
        .checks
        .iter()
        .filter(|(_, check)| check.is_unresolved())
        .collect();
    if !held_finish || unresolved.is_empty() {
        return GENERIC.to_owned();
    }
    let named: Vec<String> = unresolved
        .iter()
        .take(NAMED_UNRESOLVED_CHECKS)
        .map(|(key, check)| {
            let value = check
                .jev_p
                .map_or_else(|| "no value".to_owned(), |p| format!("{p:.2}"));
            let threshold = check
                .threshold
                .map_or_else(|| "no threshold".to_owned(), |t| format!("{t:.2}"));
            let why = match check.suppression {
                Some(crate::assessment::CheckSuppression::InBand) => "in the uncertainty band",
                Some(crate::assessment::CheckSuppression::Contested) => "contested",
                Some(crate::assessment::CheckSuppression::Unresolved) => "unresolved",
                Some(crate::assessment::CheckSuppression::NoThreshold) => "no threshold",
                None => "not usable",
            };
            format!("`{key}` = {value} (threshold {threshold}, {why})")
        })
        .collect();
    let more = unresolved.len().saturating_sub(named.len());
    let mut ask = format!(
        "No worker is running and every proposal was refused. Finish is held because {} \
         unresolved: {}{}. Is the work they describe done?",
        if unresolved.len() == 1 {
            "this check is"
        } else {
            "these checks are"
        },
        named.join("; "),
        if more > 0 {
            format!("; and {more} more")
        } else {
            String::new()
        }
    );
    if ask.len() > NOTHING_SURVIVED_ASK_CAP_BYTES {
        let mut end = NOTHING_SURVIVED_ASK_CAP_BYTES;
        while !ask.is_char_boundary(end) {
            end -= 1;
        }
        ask.truncate(end);
    }
    ask
}

/// The cycle's decision: every responsibility's proposals plus `extra` (the ensemble's
/// escalation), arbitrated, with the one invariant that needs the rules themselves.
///
/// [`arbitrate`] sees proposals, not the rules that made them, so it cannot tell what a
/// directive *rests on*. [`Invariant::UnresolvedDependency`] needs exactly that: a selected
/// `Stop` is refused while any check it rests on is not permitted — a check a rule
/// proposing it reads, or an unpermitted check whose resolution to either side of its
/// threshold would change the kind of directive selected. The refused `Stop` proposals are recorded as
/// suppressed by that invariant and arbitration runs again without them, until what is
/// selected is not a `Stop` or rests only on permitted checks. A `Stop` selected by a hard
/// invariant (the overall timeout) is not a proposal and is never refused; nor is a person's
/// `Stop`, which never passes through here.
///
/// Pure, like [`arbitrate`]. The counterfactuals are plain arbitrations — this invariant is
/// not applied inside them — so it cannot recurse. With the ensemble off it is exactly
/// [`arbitrate`].
#[must_use]
pub fn decide(
    registry: &crate::responsibility::ResponsibilityRegistry,
    input: &PolicyInput<'_>,
    extra: &[Proposal],
) -> PolicyDecision {
    decide_with_holds(registry, input, extra).0
}

/// A `Stop` that [`decide`] refused under [`Invariant::UnresolvedDependency`], and why.
#[derive(Debug, Clone, PartialEq)]
pub struct HeldStop {
    /// The refused directive.
    pub directive: Directive,
    /// The rules whose proposals of it were refused.
    pub rules: Vec<crate::ids::RuleId>,
    /// The checks the rules proposing it read.
    pub reads: Vec<CheckKey>,
    /// The unpermitted checks it rests on: what holds it.
    pub holding: Vec<CheckKey>,
}

/// [`decide`], and every `Stop` it refused, in the order refused. What the ensemble reads to
/// put a refused `Stop` to a person (`docs/06-ensemble.md` §6).
#[must_use]
pub fn decide_with_holds(
    registry: &crate::responsibility::ResponsibilityRegistry,
    input: &PolicyInput<'_>,
    extra: &[Proposal],
) -> (PolicyDecision, Vec<HeldStop>) {
    let mut held: Vec<HeldStop> = Vec::new();
    let mut proposals = registry.propose(input);
    proposals.extend(extra.iter().cloned());
    let mut decision = arbitrate(input, &proposals);
    if !input.ensemble_enabled {
        return (decision, held);
    }
    let mut refused: Vec<Proposal> = Vec::new();
    // Each round refuses at least one Stop proposal, so this ends.
    loop {
        if !matches!(decision.selected, Directive::Stop { .. }) {
            break;
        }
        let stopping: Vec<&Proposal> = proposals
            .iter()
            .filter(|p| p.directive == decision.selected && !refused.contains(p))
            .collect();
        if stopping.is_empty() {
            break;
        }
        let reads: Vec<CheckKey> = stopping
            .iter()
            .flat_map(|p| registry.reads(&p.rule))
            .collect();
        let holding: Vec<CheckKey> = input
            .checks
            .iter()
            .filter(|(key, check)| {
                !check.permitted
                    && check.threshold.is_some()
                    && (reads.contains(key)
                        || resolution_changes(
                            registry,
                            input,
                            extra,
                            key,
                            check,
                            &decision.selected,
                        ))
            })
            .map(|(key, _)| key.clone())
            .collect();
        if holding.is_empty() {
            break;
        }
        held.push(HeldStop {
            directive: decision.selected.clone(),
            rules: stopping.iter().map(|p| p.rule.clone()).collect(),
            reads,
            holding,
        });
        refused.extend(stopping.into_iter().cloned());
        let remaining: Vec<Proposal> = proposals
            .iter()
            .filter(|p| !refused.contains(p))
            .cloned()
            .collect();
        decision = arbitrate(input, &remaining);
    }
    if !refused.is_empty() {
        decision.considered = proposals;
        decision.invariants.push(Invariant::UnresolvedDependency);
        for proposal in &refused {
            decision.suppressed.push(Suppression {
                responsibility: proposal.responsibility.clone(),
                rule: proposal.rule.clone(),
                reason: SuppressionReason::Invariant(Invariant::UnresolvedDependency),
            });
        }
    }
    (decision, held)
}

/// Whether resolving `key` — measuring it decisively, on either side of its threshold —
/// would change the **kind** of directive selected from `selected`'s. A plain arbitration
/// over each side. A change of reason only (`Stop{"stuck"}` for `Stop{"off track"}`, the
/// worker-health runner-up) does not count: the worker is stopped either way. The gate's
/// guard set keeps comparing whole directives, which only buys more opinions.
fn resolution_changes(
    registry: &crate::responsibility::ResponsibilityRegistry,
    input: &PolicyInput<'_>,
    extra: &[Proposal],
    key: &CheckKey,
    check: &ResolvedCheck,
    selected: &Directive,
) -> bool {
    [
        crate::assessment::Side::Above,
        crate::assessment::Side::Below,
    ]
    .into_iter()
    .any(|side| {
        let mut checks = input.checks.clone();
        checks.insert(key.clone(), check.pinned(side));
        let pinned = PolicyInput {
            checks: &checks,
            ..*input
        };
        let mut proposals = registry.propose(&pinned);
        proposals.extend(extra.iter().cloned());
        // Compared by kind: a resolution that only changes which Stop (a runner-up
        // finding's reason) is not something the Stop rests on.
        arbitrate(&pinned, &proposals).selected.name() != selected.name()
    })
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
            // `verify = "never"` is the operator saying no verifier is to run. The built-in
            // rule already proposes none; this holds a user-defined rule that proposes
            // `verify` to the same promise.
            if input.config.verify == crate::job::VerifyPolicy::Never {
                return Some(SuppressionReason::Invariant(
                    Invariant::VerificationDisabled,
                ));
            }
            if input.counters.verified_since_code {
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
        ("core.verification:required", priority::VERIFY),
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
    fn a_finish_does_not_win_because_the_worker_limit_refused_work_that_outranks_it() {
        // `docs/05-responsibilities.md` §6.3. Documentation is decisively insufficient, so its
        // rule asks for a worker at 750, above `Finish` at 700. At the worker limit only that
        // `StartWorker` is refused; the `Finish` used to survive it and win. Policy escalates
        // instead, naming the rule that still asks for work.
        let o = observation();
        let config = RunConfig::default();
        let counters = Counters {
            workers_run: config.max_workers,
            verifier_completed: true,
            ..Counters::default()
        };
        let checks = checks(&[
            ("core.completion", "ready_to_finish", 0.95),
            ("core.completion", "requirements_satisfied", 0.95),
            ("core.verification", "tests_sufficient", 0.95),
        ]);
        let docs = Proposal::new(
            "quality.documentation",
            "quality.documentation:insufficient",
            Directive::StartWorker {
                reason: "the job requires documentation that is not yet complete".into(),
            },
            750,
            Stakes::High,
        );
        let decision = arbitrate(
            &input(&o, &counters, &config, &checks),
            &[docs.clone(), finish()],
        );
        let Directive::Escalate { reason, ask } = &decision.selected else {
            panic!("expected an escalation, got {:?}", decision.selected);
        };
        assert_eq!(
            reason,
            "worker limit reached while `quality.documentation:insufficient` still asks for work"
        );
        assert!(
            ask.contains("`quality.documentation:insufficient`"),
            "{ask}"
        );
        assert!(ask.contains("StartWorker"), "{ask}");
        for rule in [
            "quality.documentation:insufficient",
            "core.completion:thresholds-satisfied",
        ] {
            assert!(
                decision.suppressed.iter().any(|s| s.rule.as_str() == rule
                    && s.reason == SuppressionReason::Invariant(Invariant::WorkerLimit)),
                "{rule}: {:?}",
                decision.suppressed
            );
        }

        // Below the limit the inhibitor simply wins, as before.
        let below = Counters {
            workers_run: config.max_workers - 1,
            ..counters.clone()
        };
        let decision = arbitrate(&input(&o, &below, &config, &checks), &[docs, finish()]);
        assert!(matches!(decision.selected, Directive::StartWorker { .. }));

        // A refused proposal the Finish outranks does not hold it: work-remains (500) is what
        // the completion rule falls back to, never a reason not to finish.
        let remains = Proposal::new(
            builtin::COMPLETION,
            "core.completion:work-remains",
            Directive::StartWorker {
                reason: "meaningful implementation work remains".into(),
            },
            500,
            Stakes::High,
        );
        let decision = arbitrate(
            &input(&o, &counters, &config, &checks),
            &[remains, finish()],
        );
        assert_eq!(decision.selected, finish().directive);

        // Nor does one refused for another reason: a second verifier is refused because one
        // already ran since the last code worker, whatever the limit.
        let verify = Proposal::new(
            "user.check",
            "user.check:verify-again",
            Directive::Verify {
                reason: "again".into(),
            },
            800,
            Stakes::High,
        );
        let ran = Counters {
            verifiers_run: 1,
            verified_since_code: true,
            ..counters.clone()
        };
        let decision = arbitrate(&input(&o, &ran, &config, &checks), &[verify, finish()]);
        assert_eq!(decision.selected, finish().directive);
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
    fn nothing_can_make_progress_names_the_unresolved_checks_holding_finish() {
        // Stage B review SB4. An in-band check that no rule reads holds `Finish` through
        // `UnresolvedUncertainty`, nothing else is proposed, and no worker is running: the
        // generic escalation must say which check, with its value, or a person cannot
        // answer it.
        let o = observation();
        let counters = Counters {
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
        let held = key("user.audit", "licence_clean");
        checks.insert(
            held.clone(),
            ResolvedCheck::from_measurement(held, Some(0.71), Some(0.75), 0.15, true),
        );
        let decision = arbitrate(&input(&o, &counters, &config, &checks), &[finish()]);
        let Directive::Escalate { reason, ask } = &decision.selected else {
            panic!("expected an escalation, got {:?}", decision.selected);
        };
        assert!(reason.starts_with("nothing can make progress"), "{reason}");
        assert!(
            ask.contains(
                "`user.audit__licence_clean` = 0.71 (threshold 0.75, in the uncertainty band)"
            ),
            "{ask}"
        );
        assert!(
            ask.contains("Finish is held because this check is unresolved"),
            "{ask}"
        );
        // The checks that are permitted are not named.
        assert!(!ask.contains("ready_to_finish"), "{ask}");

        // Bounded: many held checks, with long keys, are named up to a count, and the
        // question stays within its cap.
        let mut many = checks.clone();
        for i in 0..40 {
            let k = key(&format!("user.{}", "x".repeat(200)), &format!("c{i:02}"));
            many.insert(
                k.clone(),
                ResolvedCheck::from_measurement(k, Some(0.70), Some(0.75), 0.15, true),
            );
        }
        let decision = arbitrate(&input(&o, &counters, &config, &many), &[finish()]);
        let Directive::Escalate { ask, .. } = &decision.selected else {
            panic!("expected an escalation, got {:?}", decision.selected);
        };
        assert!(
            ask.len() <= super::NOTHING_SURVIVED_ASK_CAP_BYTES,
            "{}",
            ask.len()
        );
        assert!(ask.matches("` = ").count() <= super::NAMED_UNRESOLVED_CHECKS);

        // When Finish was not what uncertainty held, the question is the generic one.
        let decision = arbitrate(&input(&o, &counters, &config, &checks), &[]);
        assert_eq!(
            decision.selected,
            Directive::Escalate {
                reason: "nothing can make progress: no proposal survived and no worker is running"
                    .into(),
                ask: "No worker is running and every proposal was refused.".into(),
            }
        );
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

    /// Finish-ready in every respect, with the model judging no verification needed.
    fn finish_ready_needing_no_verification() -> BTreeMap<CheckKey, ResolvedCheck> {
        checks(&[
            ("core.completion", "implementation_complete", 0.95),
            ("core.completion", "ready_to_finish", 0.95),
            ("core.completion", "requirements_satisfied", 0.95),
            ("core.verification", "tests_sufficient", 0.95),
            ("core.verification", "needs_verification", 0.10),
        ])
    }

    fn with_verify(verify: crate::job::VerifyPolicy) -> RunConfig {
        RunConfig {
            verify,
            ..RunConfig::default()
        }
    }

    #[test]
    fn verify_always_starts_a_verifier_though_the_model_judges_none_needed() {
        // `verify = "always"` refuses every `Finish` until a verifier has completed. The
        // verification rule used to wait for `needs_verification` as well, so a model that
        // said "none needed" left nothing that could make progress: the run escalated
        // "nothing can make progress" after its first worker and could never finish.
        let registry = crate::responsibility::ResponsibilityRegistry::builtin();
        let o = observation();
        let config = with_verify(crate::job::VerifyPolicy::Always);
        let checks = finish_ready_needing_no_verification();

        let before = Counters {
            workers_run: 1,
            ..Counters::default()
        };
        let decision = super::decide(&registry, &input(&o, &before, &config, &checks), &[]);
        assert!(
            matches!(decision.selected, Directive::Verify { .. }),
            "selected {:?}",
            decision.selected
        );
        assert!(
            decision
                .considered
                .iter()
                .any(|p| p.rule.as_str() == "core.verification:required")
        );
        assert!(
            decision
                .invariants
                .contains(&Invariant::VerificationIncomplete)
        );

        // Once it has completed, the same state finishes.
        let after = Counters {
            workers_run: 2,
            verifiers_run: 1,
            verifier_completed: true,
            verified_since_code: true,
            ..Counters::default()
        };
        let decision = super::decide(&registry, &input(&o, &after, &config, &checks), &[]);
        assert!(
            matches!(decision.selected, Directive::Finish { .. }),
            "selected {:?}",
            decision.selected
        );
    }

    #[test]
    fn verify_always_waits_for_implementation_before_requiring_a_verifier() {
        // The required verifier judges finished work, not work in progress: below the
        // implementation threshold it is not proposed, and the run keeps working.
        let registry = crate::responsibility::ResponsibilityRegistry::builtin();
        let o = observation();
        let config = with_verify(crate::job::VerifyPolicy::Always);
        let mut checks = finish_ready_needing_no_verification();
        checks.extend(self::checks(&[(
            "core.completion",
            "implementation_complete",
            0.20,
        )]));
        let counters = Counters {
            workers_run: 1,
            ..Counters::default()
        };
        let decision = super::decide(&registry, &input(&o, &counters, &config, &checks), &[]);
        assert!(
            !decision
                .considered
                .iter()
                .any(|p| matches!(p.directive, Directive::Verify { .. })),
            "considered {:?}",
            decision.considered
        );
    }

    #[test]
    fn verify_when_asked_still_waits_for_the_model_to_judge_a_verifier_needed() {
        // The default is unchanged: no verifier when the model says none is needed.
        let registry = crate::responsibility::ResponsibilityRegistry::builtin();
        let o = observation();
        let config = with_verify(crate::job::VerifyPolicy::WhenAsked);
        let checks = finish_ready_needing_no_verification();
        let counters = Counters {
            workers_run: 1,
            ..Counters::default()
        };
        let decision = super::decide(&registry, &input(&o, &counters, &config, &checks), &[]);
        assert!(
            matches!(decision.selected, Directive::Finish { .. }),
            "selected {:?}",
            decision.selected
        );
    }

    #[test]
    fn verify_never_starts_no_verifier_from_any_rule() {
        // `verify = "never"` was not enforced anywhere: the built-in rule proposed a verifier
        // whenever the model judged one warranted, and so would any user-defined rule.
        let registry = crate::responsibility::ResponsibilityRegistry::builtin();
        let o = observation();
        let config = with_verify(crate::job::VerifyPolicy::Never);
        let checks = checks(&[
            ("core.completion", "implementation_complete", 0.95),
            ("core.completion", "ready_to_finish", 0.20),
            ("core.verification", "needs_verification", 0.95),
        ]);
        let counters = Counters {
            workers_run: 1,
            ..Counters::default()
        };
        let user_rule = Proposal::new(
            "my.checks",
            "my.checks:verify",
            Directive::Verify {
                reason: "a user rule".into(),
            },
            950,
            Stakes::High,
        );
        let decision = super::decide(
            &registry,
            &input(&o, &counters, &config, &checks),
            std::slice::from_ref(&user_rule),
        );
        assert!(
            !matches!(decision.selected, Directive::Verify { .. }),
            "selected {:?}",
            decision.selected
        );
        assert!(
            !decision
                .considered
                .iter()
                .any(|p| p.responsibility.as_str() == builtin::VERIFICATION),
            "the built-in rule proposed a verifier"
        );
        assert!(decision.suppressed.iter().any(|s| s.rule == user_rule.rule
            && s.reason == SuppressionReason::Invariant(Invariant::VerificationDisabled)));
    }
}
