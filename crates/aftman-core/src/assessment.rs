//! Assessments, and the per-check resolution that policy actually reads.
//!
//! This module is where the ensemble's guarantee is enforced, so it is worth
//! stating precisely:
//!
//! - Jev's number is **never modified**. [`ResolvedCheck::jev_p`] is always exactly
//!   what the model said.
//! - Alongside it sits [`ResolvedCheck::permitted`], a boolean saying whether policy
//!   may evaluate a comparator against that value at all.
//! - `permitted` is `decisive && !contested`. It is **not** `p >= t`. It answers
//!   "is this measurement trustworthy", not "is it above the line" — a rule whose
//!   condition is `< 0.75` is exactly as legitimate as one whose condition is
//!   `>= 0.75`, and conflating the two silently kills every `<` rule.
//! - No code path turns an opinion into `permitted = true`. Only a fresh
//!   measurement (an evidence refresh) can do that. Opinions can only take
//!   permission away.
//!
//! See `docs/06-ensemble.md` §2 and §5.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::ids::{AssessmentSeq, CheckId, CheckKey, EvidenceId, ResponsibilityId};
use crate::model::Usage;

/// Which side of a threshold a value falls on.
///
/// Carried explicitly on a verdict, because a judgement about a *below*-threshold
/// check must never be applied as though it were about an above-threshold one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Above,
    Below,
}

/// The side of `t` that `p` falls on. The threshold itself counts as `Above`.
#[must_use]
pub fn side_of(p: f32, t: f32) -> Side {
    if p >= t { Side::Above } else { Side::Below }
}

/// Whether a measurement is far enough from its threshold to be acted on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Band {
    /// `|p - t| >= margin`, or the ensemble is disabled.
    Decisive,
    /// Inside the uncertainty margin. Held.
    InBand,
}

/// What a second opinion concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Verdict {
    /// Agreement with the model's side. **Changes nothing.**
    Confirm { side: Side },
    /// Disagreement, or unusable evidence. Removes permission.
    Contest { side: Side },
    /// Could not be obtained or parsed.
    Abstain,
}

/// A rung of the second-opinion ladder, cheapest and most reliable first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Rung {
    /// Deterministic: run the configured evidence command and re-ask. Not an
    /// opinion. The only rung that can *raise* permission, because it produces a
    /// fresh measurement rather than a judgement about an old one.
    RefreshEvidence,
    /// Ask the same model the negated question; inconsistency means the original
    /// answer was framing-sensitive.
    ReframeSameModel,
    /// Ask a different model the same question. Records the model id.
    IndependentJudge,
    /// A person.
    Human,
}

/// One recorded second opinion.
///
/// `p_reported` is stored for audit and appears in **no** computation; `verdict` is
/// the only field policy can observe. Retaining the reported number is what lets a
/// reader confirm after the fact that aftman refused to use it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Opinion {
    pub rung: Rung,
    /// Which client answered, for example `agy`.
    pub source: String,
    /// The model id that actually answered, as reported by the client.
    pub model: String,
    pub verdict: Verdict,
    /// Stored, never used in arithmetic.
    pub p_reported: Option<f32>,
    /// A short excerpt of the raw answer, capped by the caller.
    pub excerpt: String,
    /// Hash of the full prompt, so a verdict is attributable to a template revision.
    pub prompt_hash: String,
    /// Digest of the state the opinion was shown, so it can be proven that it saw
    /// what the model saw.
    pub state_digest: String,
    pub latency: Duration,
    pub error: Option<String>,
}

/// Why a check's value may not be used by policy.
///
/// Deliberately a *different* type from [`crate::directive::SuppressionReason`],
/// which is about proposal arbitration. The two must not share a name: one is
/// measurement quality, the other is directive selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckSuppression {
    /// The value sat inside the uncertainty band.
    InBand,
    /// An opinion contested it.
    Contested,
    /// In band and not resolvable by evidence, and no human to ask.
    Unresolved,
    /// No threshold, so no band applies and no rule may reference it.
    NoThreshold,
}

/// What an answer from a human authorizes.
///
/// A human may always **stop** work. A human may never **finish** work by fiat:
/// `ClaimComplete` maps to "refresh the evidence and re-assess", because otherwise
/// a person becomes a path around "only evidence can finish it".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "decision")]
pub enum HumanDecision {
    /// Accept the uncertainty and continue. No directive is enabled.
    Accept,
    /// Look again with fresh evidence.
    Recheck,
    /// Stop the run. A human may always stop work.
    Stop,
    /// "This is complete." Runs an evidence refresh; the resulting measurement
    /// must clear the finish precondition on its own.
    ClaimComplete,
    /// Free-form guidance delivered to the worker.
    Guidance { text: String },
}

/// How a check's value was arrived at.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "resolution")]
pub enum Resolution {
    /// `|p - t| >= margin`. Policy may evaluate the comparator.
    Decisive,
    /// Deterministic evidence produced a fresh value from a real measurement.
    EvidenceRefreshed {
        new_p: f32,
        provider: EvidenceId,
        supersedes: AssessmentSeq,
    },
    /// An opinion contested the value.
    SuppressedByContest { jev_p: f32, dissent: Vec<Opinion> },
    /// In band and not resolvable by evidence.
    UnresolvedInBand { ask: String, rungs: Vec<Rung> },
    /// A human answered.
    HumanDecided { decision: HumanDecision },
}

/// What policy actually reads for one check.
///
/// Jev's number is preserved verbatim in `jev_p`; `permitted` carries the
/// permission separately. There is no operation on this type that raises
/// `permitted` on the basis of an opinion.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolvedCheck {
    pub check: CheckKey,
    /// `None` for a check with no threshold, which therefore has no band.
    pub threshold: Option<f32>,
    /// Exactly what the model said. Never overwritten by an opinion.
    pub jev_p: Option<f32>,
    pub side: Option<Side>,
    pub band: Band,
    /// `|p - t| >= margin`, or always true when the ensemble is disabled.
    pub decisive: bool,
    pub contested: bool,
    /// Whether policy may evaluate **a comparator** against this value.
    ///
    /// `decisive && !contested`. Direction-agnostic by design — see the module docs.
    pub permitted: bool,
    pub suppression: Option<CheckSuppression>,
    pub resolution: Resolution,
}

impl ResolvedCheck {
    /// The baseline: Jev's own value, with no opinion involved.
    ///
    /// This is the **only** place `permitted` can become `true` without a fresh
    /// measurement, and it is a pure function of the value, the threshold, and the
    /// margin — never of which side of the threshold the value is on.
    #[must_use]
    pub fn from_measurement(
        check: CheckKey,
        p: Option<f32>,
        threshold: Option<f32>,
        margin: f32,
        ensemble_enabled: bool,
    ) -> Self {
        let Some(t) = threshold else {
            // No threshold means no band to be uncertain about, so nothing is held.
            return Self {
                check,
                threshold: None,
                jev_p: p,
                side: None,
                band: Band::Decisive,
                decisive: true,
                contested: false,
                permitted: true,
                suppression: None,
                resolution: Resolution::Decisive,
            };
        };

        let (decisive, side) = match p {
            Some(p) => (
                !ensemble_enabled || (p - t).abs() >= margin,
                Some(side_of(p, t)),
            ),
            // No measurement at all: nothing may be inferred from it.
            None => (false, None),
        };

        let band = if decisive {
            Band::Decisive
        } else {
            Band::InBand
        };
        Self {
            check,
            threshold: Some(t),
            jev_p: p,
            side,
            band,
            decisive,
            contested: false,
            permitted: decisive,
            suppression: if decisive {
                None
            } else {
                Some(CheckSuppression::InBand)
            },
            resolution: if decisive {
                Resolution::Decisive
            } else {
                Resolution::UnresolvedInBand {
                    ask: String::new(),
                    rungs: Vec::new(),
                }
            },
        }
    }

    /// Build from a deterministic evidence refresh: a **new measurement**, which is
    /// the only thing that can legitimately move `permitted` upward.
    #[must_use]
    pub fn from_refresh(
        check: CheckKey,
        new_p: f32,
        threshold: Option<f32>,
        margin: f32,
        provider: EvidenceId,
        supersedes: AssessmentSeq,
    ) -> Self {
        let mut refreshed = Self::from_measurement(check, Some(new_p), threshold, margin, true);
        refreshed.resolution = Resolution::EvidenceRefreshed {
            new_p,
            provider,
            supersedes,
        };
        refreshed
    }

    /// Remove permission, recording why. There is no inverse of this method.
    #[must_use]
    pub fn suppress(mut self, reason: CheckSuppression, resolution: Resolution) -> Self {
        self.permitted = false;
        self.contested = matches!(reason, CheckSuppression::Contested);
        self.suppression = Some(reason);
        self.resolution = resolution;
        self
    }

    /// Whether this check is holding `Finish` back.
    ///
    /// True for a thresholded check whose measurement is in band or contested. A
    /// check with no threshold is never unresolved, because there is nothing to be
    /// uncertain about. This is the single predicate behind
    /// [`crate::directive::Invariant::UnresolvedUncertainty`].
    #[must_use]
    pub const fn is_unresolved(&self) -> bool {
        !self.permitted
    }

    /// The value policy may compare, if any.
    #[must_use]
    pub const fn permitted_value(&self) -> Option<f32> {
        if self.permitted { self.jev_p } else { None }
    }

    /// Evaluate a `>=` comparator against this check.
    #[must_use]
    pub fn at_least(&self, t: f32) -> bool {
        self.permitted && self.jev_p.is_some_and(|p| p >= t)
    }

    /// Evaluate a `<` comparator against this check.
    ///
    /// Exists so that the `<` direction is written down once, next to its `>=`
    /// counterpart, rather than being reconstructed (and forgotten) per rule.
    #[must_use]
    pub fn less_than(&self, t: f32) -> bool {
        self.permitted && self.jev_p.is_some_and(|p| p < t)
    }
}

/// One answer from the decision model.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum AnswerValue {
    /// Probability that a yes/no statement is true, in `0.0..=1.0`.
    Noul(f32),
    /// A selected category with its distribution.
    Choice {
        choice: String,
        probabilities: BTreeMap<String, f32>,
        confidence: f32,
    },
    /// An expected position on an ordered rubric.
    Score {
        score: f32,
        /// Derived locally as the argmax of `probabilities`; never sent by the provider.
        level: Option<u32>,
        confidence: f32,
        probabilities: BTreeMap<u32, f32>,
    },
}

impl AnswerValue {
    /// The probability, for a `Noul` answer.
    #[must_use]
    pub const fn as_noul(&self) -> Option<f32> {
        match self {
            Self::Noul(p) => Some(*p),
            Self::Choice { .. } | Self::Score { .. } => None,
        }
    }

    /// Reported confidence, for the primitives that carry one.
    #[must_use]
    pub const fn confidence(&self) -> Option<f32> {
        match self {
            Self::Noul(_) => None,
            Self::Choice { confidence, .. } | Self::Score { confidence, .. } => Some(*confidence),
        }
    }
}

/// One cycle's answers from the model, grouped by responsibility.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Assessment {
    pub seq: AssessmentSeq,
    #[serde(with = "crate::timefmt")]
    pub at: OffsetDateTime,
    /// The model that actually answered, as reported. Aliases resolve, so this can
    /// differ from the requested model, and the reported one is what is persisted.
    pub model: String,
    pub by_responsibility: BTreeMap<ResponsibilityId, BTreeMap<CheckId, AnswerValue>>,
    pub usage: Usage,
    pub latency: Duration,
    pub request_ids: Vec<String>,
}

impl Assessment {
    #[must_use]
    pub fn new(seq: AssessmentSeq, at: OffsetDateTime, model: impl Into<String>) -> Self {
        Self {
            seq,
            at,
            model: model.into(),
            by_responsibility: BTreeMap::new(),
            usage: Usage::default(),
            latency: Duration::ZERO,
            request_ids: Vec::new(),
        }
    }

    /// The probability for one check, if it was a `Noul` answer.
    #[must_use]
    pub fn probability(&self, responsibility: &ResponsibilityId, check: &CheckId) -> Option<f32> {
        self.by_responsibility
            .get(responsibility)
            .and_then(|checks| checks.get(check))
            .and_then(AnswerValue::as_noul)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AnswerValue, Band, CheckSuppression, HumanDecision, Resolution, ResolvedCheck, Side,
        Verdict, side_of,
    };
    use crate::ids::{AssessmentSeq, CheckKey, EvidenceId};

    fn key() -> CheckKey {
        CheckKey::from("core.completion__ready_to_finish")
    }

    const T: f32 = 0.75;
    const M: f32 = 0.15;

    #[test]
    fn side_includes_the_threshold_itself() {
        assert_eq!(side_of(0.80, T), Side::Above);
        assert_eq!(side_of(T, T), Side::Above);
        assert_eq!(side_of(0.74, T), Side::Below);
    }

    #[test]
    fn decisively_above_is_permitted_and_fires() {
        let rc = ResolvedCheck::from_measurement(key(), Some(0.91), Some(T), M, true);
        assert_eq!(rc.band, Band::Decisive);
        assert!(rc.permitted && rc.decisive && !rc.contested);
        assert!(rc.at_least(T));
        assert!(!rc.less_than(T));
        assert!(!rc.is_unresolved());
        // Jev's number is untouched.
        assert_eq!(rc.jev_p, Some(0.91));
    }

    #[test]
    fn decisively_below_is_permitted_but_does_not_fire_above() {
        // The regression that motivated `permitted` being direction-agnostic:
        // a decisively-below value must still be *usable*, so that `<` rules work.
        let rc = ResolvedCheck::from_measurement(key(), Some(0.31), Some(T), M, true);
        assert_eq!(rc.band, Band::Decisive);
        assert!(rc.permitted, "a decisive measurement is usable either way");
        assert!(!rc.at_least(T));
        assert!(rc.less_than(T), "the `<` direction must be reachable");
        assert!(!rc.is_unresolved());
    }

    #[test]
    fn in_band_is_held_in_both_directions() {
        for p in [0.71_f32, 0.79] {
            let rc = ResolvedCheck::from_measurement(key(), Some(p), Some(T), M, true);
            assert_eq!(rc.band, Band::InBand, "p={p}");
            assert!(!rc.permitted, "p={p} must be held");
            assert!(!rc.at_least(T), "p={p}");
            assert!(!rc.less_than(T), "p={p} must not fire a `<` rule either");
            assert!(rc.is_unresolved(), "p={p} blocks Finish");
            assert_eq!(rc.jev_p, Some(p), "the value is still reported verbatim");
        }
    }

    #[test]
    fn a_value_exactly_on_the_threshold_is_in_band() {
        // Being *at* the line is not evidence of being decisively above it.
        let rc = ResolvedCheck::from_measurement(key(), Some(T), Some(T), M, true);
        assert_eq!(rc.band, Band::InBand);
        assert!(!rc.permitted);
    }

    #[test]
    fn disabling_the_ensemble_restores_plain_threshold_behaviour() {
        for p in [0.71_f32, 0.79] {
            let rc = ResolvedCheck::from_measurement(key(), Some(p), Some(T), M, false);
            assert!(rc.permitted, "ensemble off must not gate p={p}");
            assert_eq!(rc.band, Band::Decisive);
            assert_eq!(rc.at_least(T), p >= T);
        }
    }

    #[test]
    fn a_contest_removes_permission_even_from_a_decisive_value() {
        let rc = ResolvedCheck::from_measurement(key(), Some(0.93), Some(T), M, true).suppress(
            CheckSuppression::Contested,
            Resolution::SuppressedByContest {
                jev_p: 0.93,
                dissent: Vec::new(),
            },
        );
        assert!(!rc.permitted);
        assert!(rc.contested);
        assert!(!rc.at_least(T), "a contested value must not fire");
        assert!(!rc.less_than(T));
        assert!(rc.is_unresolved());
        assert_eq!(rc.jev_p, Some(0.93), "the reported value is preserved");
        assert_eq!(rc.permitted_value(), None);
    }

    #[test]
    fn a_refresh_is_the_only_way_permission_returns() {
        let held = ResolvedCheck::from_measurement(key(), Some(0.71), Some(T), M, true);
        assert!(!held.permitted);

        let refreshed = ResolvedCheck::from_refresh(
            key(),
            0.08,
            Some(T),
            M,
            EvidenceId::new("command:tests"),
            AssessmentSeq::FIRST,
        );
        assert!(
            refreshed.permitted,
            "a fresh measurement may restore permission"
        );
        assert!(!refreshed.at_least(T), "but it does not make the rule fire");
        assert!(refreshed.less_than(T));
        assert!(matches!(
            refreshed.resolution,
            Resolution::EvidenceRefreshed { .. }
        ));
    }

    #[test]
    fn missing_measurement_is_held_not_guessed() {
        let rc = ResolvedCheck::from_measurement(key(), None, Some(T), M, true);
        assert!(!rc.permitted);
        assert!(rc.is_unresolved());
        assert_eq!(rc.side, None);
    }

    #[test]
    fn a_check_without_a_threshold_is_never_held() {
        let rc = ResolvedCheck::from_measurement(
            CheckKey::from("core.worker-health__meaningful_progress"),
            Some(0.5),
            None,
            M,
            true,
        );
        assert!(rc.permitted);
        assert!(!rc.is_unresolved());
        assert_eq!(rc.suppression, None);
    }

    #[test]
    fn answer_values_expose_only_what_they_carry() {
        assert_eq!(AnswerValue::Noul(0.9).as_noul(), Some(0.9));
        assert_eq!(AnswerValue::Noul(0.9).confidence(), None);
        let choice = AnswerValue::Choice {
            choice: "verify".into(),
            probabilities: Default::default(),
            confidence: 0.77,
        };
        assert_eq!(choice.as_noul(), None);
        assert_eq!(choice.confidence(), Some(0.77));
    }

    #[test]
    fn verdicts_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        for verdict in [
            Verdict::Confirm { side: Side::Above },
            Verdict::Contest { side: Side::Below },
            Verdict::Abstain,
        ] {
            let json = serde_json::to_string(&verdict)?;
            assert_eq!(serde_json::from_str::<Verdict>(&json)?, verdict);
        }
        Ok(())
    }

    #[test]
    fn human_decisions_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        for decision in [
            HumanDecision::Accept,
            HumanDecision::Recheck,
            HumanDecision::Stop,
            HumanDecision::ClaimComplete,
            HumanDecision::Guidance {
                text: "use staging".into(),
            },
        ] {
            let json = serde_json::to_string(&decision)?;
            assert_eq!(serde_json::from_str::<HumanDecision>(&json)?, decision);
        }
        Ok(())
    }
}
