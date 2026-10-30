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

use crate::assessment::{AnswerValue, ResolvedCheck, Side};
use crate::directive::{Directive, Proposal, Stakes};
use crate::evidence::EvidencePlan;
use crate::ids::{AssessmentSeq, CheckKey, EvidenceId};
use crate::policy::PolicyInput;
use crate::responsibility::{Check, ResponsibilityRegistry};

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
    /// The selected directive is `Finish` or `Stop` and rests on this check: a rule
    /// proposing it reads the check, or the check's side changes it. Decisive values
    /// only; R2 (and R1). The irreversible gate uses the same definition.
    Irreversible,
    /// The check was held or contested in at least `consecutive_contests_before_human`
    /// consecutive prior cycles. Escalates rather than asking again.
    RepeatContest,
    /// A person answered an escalation about this check with `Recheck` or
    /// `ClaimComplete`: R0 runs for it this cycle, whatever its band, within the refresh
    /// cap. Never routes to an opinion, and never satisfies anything by itself.
    HumanRecheck,
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
            Self::HumanRecheck => "human_recheck",
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
    /// The policy input for this cycle. Its `checks` must be the baseline the ensemble
    /// derives itself — `ResolvedCheck::from_measurement` over `answers` and `definitions`
    /// with the settings' margin and `enabled` — or `build` refuses.
    pub policy: &'a PolicyInput<'a>,
    /// The active responsibilities: what proposes, and which checks each rule reads. Must
    /// be the registry the runtime arbitrates with, or the counterfactuals answer a
    /// different question from the one policy will be asked. It cannot be compared, so
    /// `resolve` trusts the caller to pass the same one to `build` and `resolve`.
    pub registry: &'a ResponsibilityRegistry,
    /// The active check definitions, for their evidence selections.
    pub definitions: &'a [Check],
    /// The configured command providers.
    pub evidence: &'a EvidencePlan,
    /// This cycle's raw answers, for the confidence of `Choice`/`Score` checks.
    pub answers: &'a BTreeMap<CheckKey, AnswerValue>,
    /// The `state` document this cycle's assessment was actually asked over: what Jev saw
    /// when it gave `answers`. `ClaimOnlyEvidence` is computed from it, never from the
    /// observation, because command output the observation holds but the assessment did
    /// not send is output Jev never saw.
    pub assessed_state: &'a serde_json::Value,
    pub counters: &'a EnsembleCounters,
    /// Checks that can be asked the negated question (R2): see [`reframable_checks`].
    pub reframe_available: &'a BTreeSet<CheckKey>,
    /// Whether a `Finish` selected this cycle will *not* be executed. When `before_finish`
    /// providers exist and have not run, the runtime does not apply a selected `Finish`:
    /// it runs them and re-assesses. Such a `Finish` is not treated as irreversible — not
    /// by the ladder's `Irreversible` trigger, not by the gate — because no opinion bought
    /// for it could protect anything. The re-assessed `Finish` that does execute is
    /// guarded as usual.
    pub finish_is_provisional: bool,
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
    /// The check selects command evidence (`command:<id>` or `command_evidence`) and Jev
    /// was shown none of it this cycle: its value rests on the worker's claim about a
    /// command, and only R0 can show it the output.
    pub(super) unseen_command: bool,
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
            unseen_command: false,
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
    /// A digest of the policy input the plan was built over (`context_fingerprint`), so
    /// `resolve` can refuse a different one.
    pub(super) context: String,
    /// The checks R2 could be asked about, as the plan was built. The gate reads this copy,
    /// and `resolve` refuses a context whose set differs.
    pub(super) reframe_available: BTreeSet<CheckKey>,
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
        mut baseline: BTreeMap<CheckKey, ResolvedCheck>,
        triggered: Vec<CheckPlan>,
        context: String,
        reframe_available: BTreeSet<CheckKey>,
    ) -> Self {
        for plan in &triggered {
            baseline.insert(plan.check.clone(), plan.baseline.clone());
        }
        Self {
            cycle,
            settings,
            counters,
            baseline,
            triggered,
            context,
            reframe_available,
        }
    }

    /// Compute the triggers for every check. Pure.
    ///
    /// The baseline is derived here, from the answers, the definitions and the settings
    /// (`ResolvedCheck::from_measurement` with `settings.enabled` and
    /// `settings.uncertainty_margin`), never taken from the caller. With the ensemble
    /// disabled, nothing is triggered and that baseline stands.
    ///
    /// # Errors
    ///
    /// [`EnsembleError::BaselineMismatch`] if `ctx.policy.checks` is not that baseline: the
    /// caller would otherwise be arbitrating over permissions the ensemble did not grant,
    /// for example a value resolved with a different margin, or with the band switched
    /// off. Refused as an error rather than a debug assertion so that the refusal itself
    /// is testable and holds in release builds.
    pub fn build(ctx: &EnsembleContext<'_>) -> Result<Self, EnsembleError> {
        let baseline = derive_baseline(ctx);
        if let Some(check) = first_difference(&baseline, ctx.policy.checks) {
            return Err(EnsembleError::BaselineMismatch { check });
        }
        let mut triggered = Vec::new();
        if ctx.settings.enabled {
            let (proposals, selected) = evaluate(ctx, &baseline);
            // What is about to act: the rules whose proposal arbitration selects. Not a
            // proposal that is outranked or refused, and not a provisional Finish, which
            // the runtime re-assesses instead of executing.
            let acting: BTreeSet<CheckKey> = proposals
                .iter()
                .filter(|p| p.directive == selected)
                .filter(|p| {
                    !(ctx.finish_is_provisional && matches!(p.directive, Directive::Finish { .. }))
                })
                .flat_map(|p| ctx.registry.reads(&p.rule))
                .collect();
            let irreversible = irreversible_dependencies(ctx, &baseline);
            let high_stakes = high_stakes_dependencies(ctx, &baseline);
            let world = World {
                acting,
                irreversible,
                high_stakes,
            };
            for (key, rc) in &baseline {
                if let Some(plan) = plan_check(ctx, &world, key, rc)
                    && !plan.triggers.is_empty()
                {
                    triggered.push(plan);
                }
            }
        }
        Ok(Self {
            cycle: ctx.cycle,
            settings: ctx.settings.clone(),
            counters: ctx.counters.clone(),
            baseline,
            triggered,
            context: context_fingerprint(ctx),
            reframe_available: ctx.reframe_available.clone(),
        })
    }
}

/// A digest of everything in the context that policy reads besides the checks: the
/// status, counters, configuration, observation, clock, active worker, last directive,
/// steering grace, `ensemble_enabled`, and `finish_is_provisional`. `resolve` compares it
/// with the plan's, so the gate cannot re-arbitrate over a different world from the one
/// the triggers were computed in. The registry is a set of trait objects and cannot be
/// digested: the caller must pass the same one to `build` and `resolve`.
///
/// `Debug` rather than a serializer: every collection involved is ordered, so it is
/// deterministic, and unlike serialization it cannot fail.
pub(super) fn context_fingerprint(ctx: &EnsembleContext<'_>) -> String {
    let p = ctx.policy;
    let text = format!(
        "{:?}|{:?}|{:?}|{:?}|{:?}|{:?}|{:?}|{:?}|{:?}|{:?}",
        p.status,
        p.counters,
        p.config,
        p.observation,
        p.now,
        p.active_worker,
        p.last_directive,
        p.ensemble_enabled,
        p.steering_grace,
        ctx.finish_is_provisional,
    );
    blake3::hash(text.as_bytes()).to_hex().to_string()
}

/// The baseline the ensemble starts from: Jev's own values, resolved with no opinion.
pub(super) fn derive_baseline(ctx: &EnsembleContext<'_>) -> BTreeMap<CheckKey, ResolvedCheck> {
    ctx.definitions
        .iter()
        .map(|check| {
            let p = ctx.answers.get(&check.key).and_then(AnswerValue::as_noul);
            (
                check.key.clone(),
                ResolvedCheck::from_measurement(
                    check.key.clone(),
                    p,
                    check.threshold,
                    ctx.settings.uncertainty_margin,
                    ctx.settings.enabled,
                ),
            )
        })
        .collect()
}

/// The first check on which two check sets disagree, if any.
fn first_difference(
    a: &BTreeMap<CheckKey, ResolvedCheck>,
    b: &BTreeMap<CheckKey, ResolvedCheck>,
) -> Option<CheckKey> {
    a.keys()
        .chain(b.keys())
        .find(|key| a.get(*key) != b.get(*key))
        .cloned()
}

/// Why the ensemble could not plan or resolve a cycle.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum EnsembleError {
    /// The caller's checks are not the baseline the ensemble derives from the answers.
    #[error(
        "the policy input's checks differ from the ensemble's baseline at `{check}`; build \
         them with the same answers, margin and `enabled` as the ensemble settings"
    )]
    BaselineMismatch { check: CheckKey },
    /// `resolve` was handed a context that is not the one the plan was built from: a
    /// different cycle, or answers, definitions or settings that derive a different
    /// baseline.
    #[error("the context does not match the plan: {0}")]
    ContextMismatch(String),
    /// The source could not answer at all.
    #[error(transparent)]
    Source(#[from] super::resolve::SourceError),
}

/// What policy does with the baseline checks.
struct World {
    /// The checks read by the rules whose proposal is selected: what is about to act.
    acting: BTreeSet<CheckKey>,
    /// What the selected directive rests on, if it is irreversible.
    irreversible: BTreeSet<CheckKey>,
    /// What a pending high-stakes directive rests on (`high_stakes_dependencies`).
    high_stakes: BTreeSet<CheckKey>,
}

/// The checks the proposals `wanted` picks out of `checks` rest on. The single definition
/// of "a directive depends on a check", shared by the `Irreversible` trigger, the gate and
/// the `HighStakes` trigger, so that they cannot disagree. The union of
///
/// - the **read set**: every check read by the rule of every wanted proposal — so a
///   directive resting on a disjunction (`a >= t or b >= t`), or proposed by two rules,
///   rests on every check that could have produced it; and
/// - the **counterfactual set**: every decisive, permitted check whose side changes
///   `signature` (computed over the proposals and the selection), each check pinned
///   decisively above and below — so a check that matters because it would let another
///   rule win, or make a wanted proposal appear, is included.
///
/// Empty when nothing is wanted: no pending directive, nothing rests on anything.
fn rests_on<S: PartialEq>(
    ctx: &EnsembleContext<'_>,
    checks: &BTreeMap<CheckKey, ResolvedCheck>,
    wanted: impl Fn(&Proposal, &Directive) -> bool,
    signature: impl Fn(&[Proposal], &Directive) -> S,
) -> BTreeSet<CheckKey> {
    let (proposals, selected) = evaluate(ctx, checks);
    let picked: Vec<&Proposal> = proposals
        .iter()
        .filter(|proposal| wanted(proposal, &selected))
        .collect();
    if picked.is_empty() {
        return BTreeSet::new();
    }
    let mut deps: BTreeSet<CheckKey> = picked
        .iter()
        .flat_map(|proposal| ctx.registry.reads(&proposal.rule))
        .filter(|key| checks.contains_key(key))
        .collect();
    for (key, rc) in checks {
        if rc.threshold.is_some() && rc.decisive && rc.permitted {
            let side = |side: Side| {
                let mut counterfactual = checks.clone();
                counterfactual.insert(key.clone(), pinned(rc, side));
                let (proposals, selected) = evaluate(ctx, &counterfactual);
                signature(&proposals, &selected)
            };
            if side(Side::Above) != side(Side::Below) {
                deps.insert(key.clone());
            }
        }
    }
    deps
}

/// The checks an irreversible selection rests on: empty unless `Finish` or `Stop` is
/// selected over `checks` (and is not a provisional `Finish`); otherwise [`rests_on`] the
/// proposals of the selected directive, the counterfactual compared on the whole selected
/// directive.
///
/// Used by both the ladder's `Irreversible` trigger and the gate, so the two agree.
pub(super) fn irreversible_dependencies(
    ctx: &EnsembleContext<'_>,
    checks: &BTreeMap<CheckKey, ResolvedCheck>,
) -> BTreeSet<CheckKey> {
    rests_on(
        ctx,
        checks,
        |proposal, selected| acts_irreversibly(ctx, selected) && proposal.directive == *selected,
        |_, selected| selected.clone(),
    )
}

/// The checks a **pending** high-stakes directive rests on (`HighStakes`).
///
/// [`rests_on`] the `stakes = "high"` proposals, the counterfactual compared on which
/// high-stakes proposals are made, in these worlds: the baseline; each check held in band
/// pinned decisively above, and below, alone (the others still held); and all of them
/// pinned above together, and below together. The pinned worlds are what make a directive
/// *pending*. Alone, each held worker-health check is one `Stop` away. Together they
/// matter when one is not enough: with two `Finish` conjuncts in band, neither pinned
/// alone produces a `Finish`, so a single-check counterfactual found nothing for either and
/// neither ever escalated — the run started worker after worker instead. Pinned together,
/// the `Finish` appears, and both rest on it. A check with no value is never pinned: there
/// is nothing to pin it to.
fn high_stakes_dependencies(
    ctx: &EnsembleContext<'_>,
    checks: &BTreeMap<CheckKey, ResolvedCheck>,
) -> BTreeSet<CheckKey> {
    let high = |proposal: &Proposal, _: &Directive| proposal.stakes == Stakes::High;
    let signature = |proposals: &[Proposal], _: &Directive| -> BTreeSet<(String, String)> {
        proposals
            .iter()
            .filter(|proposal| proposal.stakes == Stakes::High)
            .map(|proposal| {
                (
                    proposal.rule.to_string(),
                    format!("{:?}", proposal.directive),
                )
            })
            .collect()
    };
    let mut deps = rests_on(ctx, checks, high, signature);
    let held: Vec<&CheckKey> = checks
        .iter()
        .filter(|(_, rc)| rc.threshold.is_some() && !rc.decisive && rc.jev_p.is_some())
        .map(|(key, _)| key)
        .collect();
    if held.is_empty() {
        return deps;
    }
    // Each held check pinned alone (the others still held), then all of them together.
    let mut groups: Vec<Vec<&CheckKey>> = held.iter().map(|key| vec![*key]).collect();
    if held.len() > 1 {
        groups.push(held.clone());
    }
    for group in &groups {
        for side in [Side::Above, Side::Below] {
            let mut world = checks.clone();
            for key in group {
                if let Some(rc) = checks.get(*key) {
                    world.insert((*key).clone(), pinned(rc, side));
                }
            }
            deps.extend(rests_on(ctx, &world, high, signature));
        }
    }
    deps
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
    let saw_command = saw_command_evidence(&selection, ctx.assessed_state);
    if !saw_command {
        triggers.insert(EnsembleTrigger::ClaimOnlyEvidence);
    }
    let unseen_command = !saw_command
        && selection
            .iter()
            .any(|entry| entry == "command_evidence" || entry.starts_with("command:"));

    // The relevance triggers. `permitted` is direction-agnostic, so "forced to fire" and
    // "forced not to fire" cannot be expressed as permitted versus not permitted: a held
    // check also trips `UnresolvedUncertainty`, which would make every check look
    // relevant to `Finish`. Both counterfactuals are therefore permitted and decisive,
    // and differ only in the side of `t` they sit on. Which of them "fires" a given rule
    // depends on the rule's comparator, which is exactly why both are evaluated.
    let (_, above_selected) = with_check(ctx, key, pinned(rc, Side::Above));
    let (_, below_selected) = with_check(ctx, key, pinned(rc, Side::Below));

    if above_selected.name() != below_selected.name() {
        triggers.insert(EnsembleTrigger::OutcomeRelevant);
    }

    // A high-stakes directive, made now or pending once the held checks are measured,
    // rests on this check: the same dependency definition the `Irreversible` trigger and
    // the gate use (`rests_on`).
    if world.high_stakes.contains(key) {
        triggers.insert(EnsembleTrigger::HighStakes);
    }

    // "A rule about to act" is a rule whose proposal arbitration selects, and this check is
    // one it reads (`docs/06-ensemble.md` §3). A proposal that is outranked, or refused by
    // an invariant, is not about to act, and an opinion bought for it protects nothing; nor
    // is a provisional Finish, whose re-assessed, executed Finish is guarded in full. The
    // irreversible gate keeps its wider set (`rests_on`) for the Finish or Stop it guards.
    let rule_would_fire = rc.permitted && world.acting.contains(key);

    if rc.decisive && world.irreversible.contains(key) {
        triggers.insert(EnsembleTrigger::Irreversible);
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
    if counters.recheck {
        triggers.insert(EnsembleTrigger::HumanRecheck);
    }

    Some(CheckPlan {
        check: key.clone(),
        baseline: rc.clone(),
        triggers,
        rule_would_fire,
        refresh_providers: refresh_providers(&selection, ctx.evidence),
        reframe_available: ctx.reframe_available.contains(key),
        unseen_command,
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
    let proposals = ctx.registry.propose(&input);
    // The same decision the runtime makes, invariants that need the rules included.
    let selected = crate::policy::decide(ctx.registry, &input, &[]).selected;
    (proposals, selected)
}

/// Whether `directive`, selected this cycle, would take an irreversible action: `Stop`,
/// or `Finish` unless the runtime will re-assess before executing it.
pub(super) fn acts_irreversibly(ctx: &EnsembleContext<'_>, directive: &Directive) -> bool {
    match directive {
        Directive::Stop { .. } => true,
        Directive::Finish { .. } => !ctx.finish_is_provisional,
        _ => false,
    }
}

/// The selected directive over a check set.
pub(super) fn select(
    ctx: &EnsembleContext<'_>,
    checks: &BTreeMap<CheckKey, ResolvedCheck>,
) -> Directive {
    evaluate(ctx, checks).1
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
    rc.pinned(side)
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

/// Whether Jev saw command output the check selects, in the `state` the cycle's assessment
/// actually sent (its `command_evidence`).
///
/// Not whether the observation holds it: a provider that ran is in the observation, but the
/// runtime asks the cycle's questions over the default selection, which carries no command
/// output (`docs/03-jev.md` §5), so what the check was scored on was the worker's claim. A
/// provider the check names that has not run counts the same way.
#[must_use]
pub fn saw_command_evidence(selection: &[String], assessed_state: &serde_json::Value) -> bool {
    let commands = assessed_state
        .get("command_evidence")
        .and_then(serde_json::Value::as_object);
    selection.iter().any(|entry| {
        if entry == "command_evidence" {
            commands.is_some_and(|commands| !commands.is_empty())
        } else {
            entry
                .strip_prefix("command:")
                .is_some_and(|id| commands.is_some_and(|commands| commands.contains_key(id)))
        }
    })
}

/// Every command-evidence selection that matches no configured provider: a check naming
/// `command:<id>` with no provider `<id>`, or `command_evidence` with no provider at all.
/// With the ensemble on such a check can never be shown its command output — no refresh
/// can run what is not configured — so a run is refused at start
/// (`docs/05-responsibilities.md` §3).
#[must_use]
pub fn unmatched_command_selections(
    checks: &[Check],
    evidence: &EvidencePlan,
) -> Vec<(CheckKey, String)> {
    let mut unmatched = Vec::new();
    for check in checks {
        for entry in &check.evidence {
            let entry = entry.as_str();
            let matched = if entry == "command_evidence" {
                !evidence.providers().is_empty()
            } else if let Some(id) = entry.strip_prefix("command:") {
                evidence.providers().iter().any(|p| p.id.as_str() == id)
            } else {
                true
            };
            if !matched {
                unmatched.push((check.key.clone(), entry.to_owned()));
            }
        }
    }
    unmatched
}

/// The checks R2 can be asked about (`EnsembleContext::reframe_available`): every check
/// that carries an independently written negation and a threshold
/// ([`Check::reframable`]), provided `reframe_same_model` is among the configured rungs.
/// Empty otherwise: a run that has not configured R2 must not plan it. The runtime builds
/// this once per cycle, from the same definitions it asks the questions from.
#[must_use]
pub fn reframable_checks(settings: &EnsembleSettings, checks: &[Check]) -> BTreeSet<CheckKey> {
    if !settings.has(crate::assessment::Rung::ReframeSameModel) {
        return BTreeSet::new();
    }
    checks
        .iter()
        .filter(|check| check.reframable())
        .map(|check| check.key.clone())
        .collect()
}

/// The providers an evidence refresh would run for a check: the ones its selection names
/// (or all of them, for `command_evidence`), in configuration order, **whatever their
/// `run_when`**. `run_when` says when a provider runs by itself; an R0 refresh is an
/// explicit, capped re-measurement through the same scrubbed, bounded path, and a check that
/// selects a `worker_completed` provider must be re-measurable too, or a value resting on
/// output the model was never shown could never be shown it (`docs/06-ensemble.md` §4 R0).
#[must_use]
pub fn refresh_providers(selection: &[String], plan: &EvidencePlan) -> Vec<EvidenceId> {
    let all = selection.iter().any(|entry| entry == "command_evidence");
    plan.providers()
        .iter()
        .filter(|provider| {
            all || selection
                .iter()
                .any(|entry| entry.strip_prefix("command:") == Some(provider.id.as_str()))
        })
        .map(|provider| provider.id.clone())
        .collect()
}
