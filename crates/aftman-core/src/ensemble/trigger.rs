//! When the ensemble runs: triggers, computed from the assessment, the proposals and the
//! counters. See `docs/06-ensemble.md` §3.
//!
//! Everything here is pure. The relevance triggers are computed by re-running the same
//! pure proposal and arbitration functions the runtime uses, with one check replaced by
//! a counterfactual. Those counterfactual checks exist only inside this module: they are
//! built, handed to [`crate::policy::arbitrate`], and dropped. None is returned, stored,
//! or logged, so the value they carry can never be mistaken for something Jev said.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::assessment::{
    AnswerValue, Band, CheckSuppression, Resolution, ResolvedCheck, Side, side_of,
};
use crate::directive::{Directive, Proposal, Stakes};
use crate::evidence::{EvidencePlan, RunWhen};
use crate::ids::{AssessmentSeq, CheckKey, EvidenceId};
use crate::observation::Observation;
use crate::policy::{PolicyInput, arbitrate};
use crate::responsibility::Check;

use super::settings::{EnsembleCounters, EnsembleSettings};

/// Why the ensemble looked at a check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnsembleTrigger {
    /// `|p - t| < uncertainty_margin`. Routes to R0, then escalation.
    NearThreshold,
    /// A `Choice`/`Score` answer reported `confidence < confidence_floor`. Routes to R0,
    /// then escalation.
    LowConfidence,
    /// The check saw no command evidence this cycle: what it scored is a claim. Routes to
    /// R0.
    ClaimOnlyEvidence,
    /// Which side of `t` the value is on changes the selected directive. R0 if in band;
    /// R2 (and R1, from S3) if decisive.
    OutcomeRelevant,
    /// A rule that depends on this check is marked `stakes = "high"`. As
    /// `OutcomeRelevant`.
    HighStakes,
    /// The selected directive is `Finish` or `Stop` and rests on this check's side.
    /// Decisive values only; R2 (and R1).
    Irreversible,
    /// The check was held or contested in at least `consecutive_contests_before_human`
    /// consecutive prior cycles. Escalates rather than asking again.
    RepeatContest,
}

impl EnsembleTrigger {
    /// The log spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NearThreshold => "near_threshold",
            Self::LowConfidence => "low_confidence",
            Self::ClaimOnlyEvidence => "claim_only_evidence",
            Self::OutcomeRelevant => "outcome_relevant",
            Self::HighStakes => "high_stakes",
            Self::Irreversible => "irreversible",
            Self::RepeatContest => "repeat_contest",
        }
    }

    /// Whether this trigger may route a *decisive* value to the opinion rungs.
    ///
    /// The others are about the measurement (`NearThreshold`, `LowConfidence`,
    /// `ClaimOnlyEvidence`), which only evidence can fix, or about the loop
    /// (`RepeatContest`), which only a person can break.
    #[must_use]
    pub const fn routes_to_opinions(self) -> bool {
        matches!(
            self,
            Self::OutcomeRelevant | Self::HighStakes | Self::Irreversible
        )
    }
}

/// The selected directive with a check pinned decisively to each side of its threshold.
///
/// Recorded on `EnsembleTriggered` as `decisive_directives`, so the log shows what was
/// at stake. Directive names only: the counterfactual value itself is never recorded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisiveDirectives {
    pub if_above: String,
    pub if_below: String,
}

/// Everything the trigger computation reads. Borrowed, because it is the same state the
/// runtime is about to hand to policy.
pub struct EnsembleContext<'a> {
    pub settings: &'a EnsembleSettings,
    /// The assessment being resolved. A refresh supersedes it; cooldowns count from it.
    pub cycle: AssessmentSeq,
    /// The policy input for this cycle, with `checks` set to the **baseline**
    /// resolution (`ResolvedCheck::from_measurement` for every check).
    pub policy: &'a PolicyInput<'a>,
    /// Proposal generation, for example `|input| registry.propose(input)`. Must be the
    /// same pure function the runtime uses, or the counterfactuals answer a different
    /// question from the one policy will be asked.
    pub propose: &'a dyn Fn(&PolicyInput<'_>) -> Vec<Proposal>,
    /// The active check definitions, for their evidence selections.
    pub definitions: &'a [Check],
    /// The configured command providers.
    pub evidence: &'a EvidencePlan,
    /// This cycle's raw answers, for the confidence of `Choice`/`Score` checks.
    pub answers: &'a BTreeMap<CheckKey, AnswerValue>,
    pub counters: &'a EnsembleCounters,
    /// Checks that can be asked the negated question (R2). Empty until check
    /// definitions carry a negation.
    pub reframe_available: &'a BTreeSet<CheckKey>,
}

/// What the resolver needs to know about one triggered check. Plain data: building it
/// is the only part of the ensemble that reads policy.
/// Its fields are private to the ensemble: a plan is only ever built by
/// [`EnsemblePlan::build`], so a caller cannot hand the resolver a baseline that
/// `ResolvedCheck::from_measurement` did not produce.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CheckPlan {
    pub(super) check: CheckKey,
    /// Jev's own value, resolved with no opinion involved.
    pub(super) baseline: ResolvedCheck,
    /// Never empty for a planned check.
    pub(super) triggers: BTreeSet<EnsembleTrigger>,
    /// A proposal made this cycle would disappear if this check were not permitted.
    ///
    /// This is "a rule is about to act on it": the only case in which an opinion,
    /// which can do nothing but remove permission, could change what is proposed.
    /// Always false for a value that is already held.
    pub(super) rule_would_fire: bool,
    /// Command providers an evidence refresh would run for this check, in configuration
    /// order: those its selection names whose `run_when` is `on_demand` or
    /// `before_finish`.
    pub(super) refresh_providers: Vec<EvidenceId>,
    /// Whether R2 can be asked for this check.
    pub(super) reframe_available: bool,
    pub(super) decisive_directives: DecisiveDirectives,
}

impl CheckPlan {
    #[must_use]
    pub fn check(&self) -> &CheckKey {
        &self.check
    }

    /// Jev's own value, resolved with no opinion involved.
    #[must_use]
    pub fn baseline(&self) -> &ResolvedCheck {
        &self.baseline
    }

    #[must_use]
    pub fn triggers(&self) -> &BTreeSet<EnsembleTrigger> {
        &self.triggers
    }

    /// Whether a proposal made this cycle would disappear if this check were held.
    #[must_use]
    pub fn rule_would_fire(&self) -> bool {
        self.rule_would_fire
    }

    #[must_use]
    pub fn refresh_providers(&self) -> &[EvidenceId] {
        &self.refresh_providers
    }

    #[must_use]
    pub fn reframe_available(&self) -> bool {
        self.reframe_available
    }

    #[must_use]
    pub fn decisive_directives(&self) -> &DecisiveDirectives {
        &self.decisive_directives
    }

    /// A plan for one check, for the ensemble's own tests. The baseline is still derived
    /// by `ResolvedCheck::from_measurement`, never supplied.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(super) fn scripted(
        check: CheckKey,
        p: f32,
        threshold: f32,
        margin: f32,
        triggers: BTreeSet<EnsembleTrigger>,
        rule_would_fire: bool,
        refresh_providers: Vec<EvidenceId>,
        reframe_available: bool,
    ) -> Self {
        let baseline =
            ResolvedCheck::from_measurement(check.clone(), Some(p), Some(threshold), margin, true);
        Self {
            rule_would_fire: rule_would_fire && baseline.permitted,
            check,
            baseline,
            triggers,
            refresh_providers,
            reframe_available,
            decisive_directives: DecisiveDirectives {
                if_above: "Finish".into(),
                if_below: "StartWorker".into(),
            },
        }
    }

    /// Whether a trigger is present.
    #[must_use]
    pub fn has(&self, trigger: EnsembleTrigger) -> bool {
        self.triggers.contains(&trigger)
    }

    /// Whether any trigger routes a decisive value to the opinion rungs.
    #[must_use]
    pub fn wants_opinion(&self) -> bool {
        self.triggers
            .iter()
            .any(|trigger| trigger.routes_to_opinions())
    }
}

/// The ensemble's work for one cycle, decided before any I/O. Built only by
/// [`EnsemblePlan::build`].
#[derive(Debug, Clone, PartialEq)]
pub struct EnsemblePlan {
    pub(super) cycle: AssessmentSeq,
    pub(super) settings: EnsembleSettings,
    /// The counters as they stood before this cycle.
    pub(super) counters: EnsembleCounters,
    /// Every check's baseline resolution, as handed in.
    pub(super) baseline: BTreeMap<CheckKey, ResolvedCheck>,
    /// Checks with at least one trigger, in key order.
    pub(super) triggered: Vec<CheckPlan>,
}

impl EnsemblePlan {
    #[must_use]
    pub fn cycle(&self) -> AssessmentSeq {
        self.cycle
    }

    #[must_use]
    pub fn settings(&self) -> &EnsembleSettings {
        &self.settings
    }

    /// The counters as they stood before this cycle.
    #[must_use]
    pub fn counters(&self) -> &EnsembleCounters {
        &self.counters
    }

    /// Every check's baseline resolution, as handed in.
    #[must_use]
    pub fn baseline(&self) -> &BTreeMap<CheckKey, ResolvedCheck> {
        &self.baseline
    }

    /// Checks with at least one trigger, in key order. Emit `ENSEMBLE_TRIGGERED` for each
    /// with [`CheckPlan::triggered_payload`].
    #[must_use]
    pub fn triggered(&self) -> &[CheckPlan] {
        &self.triggered
    }

    /// A plan from scripted check plans, for the ensemble's own tests.
    #[cfg(test)]
    pub(super) fn scripted(
        cycle: AssessmentSeq,
        settings: EnsembleSettings,
        counters: EnsembleCounters,
        triggered: Vec<CheckPlan>,
    ) -> Self {
        Self {
            cycle,
            settings,
            counters,
            baseline: triggered
                .iter()
                .map(|plan| (plan.check.clone(), plan.baseline.clone()))
                .collect(),
            triggered,
        }
    }

    /// Compute the triggers for every check. Pure.
    ///
    /// With the ensemble disabled, nothing is triggered and the baseline stands.
    #[must_use]
    pub fn build(ctx: &EnsembleContext<'_>) -> Self {
        let baseline = ctx.policy.checks.clone();
        let mut triggered = Vec::new();
        if ctx.settings.enabled {
            let (proposals, selected) = evaluate(ctx, ctx.policy.checks);
            let world = World {
                proposals,
                selected,
            };
            for (key, rc) in &baseline {
                if let Some(plan) = plan_check(ctx, &world, key, rc)
                    && !plan.triggers.is_empty()
                {
                    triggered.push(plan);
                }
            }
        }
        Self {
            cycle: ctx.cycle,
            settings: ctx.settings.clone(),
            counters: ctx.counters.clone(),
            baseline,
            triggered,
        }
    }
}

/// What policy does with the baseline checks.
struct World {
    proposals: Vec<Proposal>,
    selected: Directive,
}

fn plan_check(
    ctx: &EnsembleContext<'_>,
    world: &World,
    key: &CheckKey,
    rc: &ResolvedCheck,
) -> Option<CheckPlan> {
    // A check with no threshold has no band and no side, and the definitions refuse a
    // rule that reads one, so there is nothing to be uncertain about.
    rc.threshold?;

    let selection = evidence_selection(ctx.definitions, key);
    let mut triggers = BTreeSet::new();

    if !rc.decisive && rc.jev_p.is_some() {
        triggers.insert(EnsembleTrigger::NearThreshold);
    }
    // A confidence that is not a number is not a high one.
    if ctx
        .answers
        .get(key)
        .and_then(AnswerValue::confidence)
        .is_some_and(|confidence| confidence.is_nan() || confidence < ctx.settings.confidence_floor)
    {
        triggers.insert(EnsembleTrigger::LowConfidence);
    }
    if !sees_command_evidence(&selection, ctx.policy.observation) {
        triggers.insert(EnsembleTrigger::ClaimOnlyEvidence);
    }

    // The relevance triggers. `permitted` is direction-agnostic, so "forced to fire" and
    // "forced not to fire" cannot be expressed as permitted versus not permitted: a held
    // check also trips `UnresolvedUncertainty`, which would make every check look
    // relevant to `Finish`. Both counterfactuals are therefore permitted and decisive,
    // and differ only in the side of `t` they sit on. Which of them "fires" a given rule
    // depends on the rule's comparator, which is exactly why both are evaluated.
    let (above, above_selected) = with_check(ctx, key, pinned(rc, Side::Above));
    let (below, below_selected) = with_check(ctx, key, pinned(rc, Side::Below));
    let (held, _) = with_check(ctx, key, held(rc));

    if above_selected.name() != below_selected.name() {
        triggers.insert(EnsembleTrigger::OutcomeRelevant);
    }

    // Rules that would fire and depend on this check: for a decisive value, those
    // proposed now; for a held one, those that either decisive side would propose. A
    // proposal that survives the check being held does not depend on it.
    let depends: Vec<&Proposal> = if rc.decisive {
        world.proposals.iter().collect()
    } else {
        above.iter().chain(below.iter()).collect()
    };
    if depends
        .iter()
        .any(|p| p.stakes == Stakes::High && !held.contains(p))
    {
        triggers.insert(EnsembleTrigger::HighStakes);
    }

    let rule_would_fire = rc.permitted && world.proposals.iter().any(|p| !held.contains(p));

    if rc.decisive && world.selected.is_irreversible() {
        let opposite = match rc.side {
            Some(Side::Above) => Some(&below_selected),
            Some(Side::Below) => Some(&above_selected),
            None => None,
        };
        if opposite.is_some_and(|other| other.name() != world.selected.name()) {
            triggers.insert(EnsembleTrigger::Irreversible);
        }
    }

    // In band, the count is of consecutive in-band measurements; decisive, of consecutive
    // contests. Holds of one kind never count toward escalating the other: two in-band
    // cycles are no reason to escalate a value that has since become decisive.
    let counters = ctx.counters.check(key);
    let repeated = if rc.decisive {
        counters.consecutive_contests
    } else {
        counters.consecutive_in_band
    };
    if repeated >= ctx.settings.consecutive_contests_before_human {
        triggers.insert(EnsembleTrigger::RepeatContest);
    }

    Some(CheckPlan {
        check: key.clone(),
        baseline: rc.clone(),
        triggers,
        rule_would_fire,
        refresh_providers: refresh_providers(&selection, ctx.evidence),
        reframe_available: ctx.reframe_available.contains(key),
        decisive_directives: DecisiveDirectives {
            if_above: above_selected.name().to_owned(),
            if_below: below_selected.name().to_owned(),
        },
    })
}

/// Propose and arbitrate over a check set, with everything else as in `ctx.policy`.
fn evaluate(
    ctx: &EnsembleContext<'_>,
    checks: &BTreeMap<CheckKey, ResolvedCheck>,
) -> (Vec<Proposal>, Directive) {
    let base = ctx.policy;
    let input = PolicyInput {
        status: base.status,
        counters: base.counters,
        config: base.config,
        checks,
        observation: base.observation,
        now: base.now,
        active_worker: base.active_worker,
        last_directive: base.last_directive,
        ensemble_enabled: base.ensemble_enabled,
        steering_grace: base.steering_grace,
    };
    let proposals = (ctx.propose)(&input);
    let selected = arbitrate(&input, &proposals).selected;
    (proposals, selected)
}

fn with_check(
    ctx: &EnsembleContext<'_>,
    key: &CheckKey,
    counterfactual: ResolvedCheck,
) -> (Vec<Proposal>, Directive) {
    let mut checks = ctx.policy.checks.clone();
    checks.insert(key.clone(), counterfactual);
    evaluate(ctx, &checks)
}

/// A counterfactual: the check measured decisively on one side.
///
/// The ends of the probability range, so that every comparator on the check resolves
/// the way that side does. Private, and only ever passed to `evaluate`.
fn pinned(rc: &ResolvedCheck, side: Side) -> ResolvedCheck {
    let p = match side {
        Side::Above => 1.0,
        Side::Below => 0.0,
    };
    ResolvedCheck {
        check: rc.check.clone(),
        threshold: rc.threshold,
        margin: rc.margin,
        jev_p: Some(p),
        side: rc.threshold.map(|t| side_of(p, t)),
        band: Band::Decisive,
        decisive: true,
        contested: false,
        permitted: true,
        suppression: None,
        resolution: Resolution::Decisive,
    }
}

/// A counterfactual: the check as it would be if an opinion contested it.
fn held(rc: &ResolvedCheck) -> ResolvedCheck {
    rc.clone().suppress(
        CheckSuppression::Contested,
        Resolution::UnresolvedInBand {
            ask: String::new(),
            rungs: Vec::new(),
        },
    )
}

/// The evidence a check selects: its own list, or the defaults.
fn evidence_selection(definitions: &[Check], key: &CheckKey) -> Vec<String> {
    definitions
        .iter()
        .find(|check| check.key == *key)
        .map_or_else(
            crate::observation::default_evidence,
            crate::responsibility::providers_for,
        )
}

/// Whether the check's selection reached any command evidence this cycle.
///
/// A provider the check names but that has not run (an `on_demand` one, say) does not
/// count: the check did not see it, so what it scored was the worker's claim.
#[must_use]
pub fn sees_command_evidence(selection: &[String], observation: &Observation) -> bool {
    selection.iter().any(|entry| {
        if entry == "command_evidence" {
            !observation.evidence.is_empty()
        } else {
            entry
                .strip_prefix("command:")
                .is_some_and(|id| observation.evidence.contains_key(id))
        }
    })
}

/// The providers an evidence refresh would run for a check: the ones its selection
/// names (or all of them, for `command_evidence`) whose `run_when` is `on_demand` or
/// `before_finish`, in configuration order.
#[must_use]
pub fn refresh_providers(selection: &[String], plan: &EvidencePlan) -> Vec<EvidenceId> {
    let all = selection.iter().any(|entry| entry == "command_evidence");
    plan.providers()
        .iter()
        .filter(|provider| matches!(provider.run_when, RunWhen::OnDemand | RunWhen::BeforeFinish))
        .filter(|provider| {
            all || selection
                .iter()
                .any(|entry| entry.strip_prefix("command:") == Some(provider.id.as_str()))
        })
        .map(|provider| provider.id.clone())
        .collect()
}
