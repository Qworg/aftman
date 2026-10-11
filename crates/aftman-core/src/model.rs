//! The decision-model boundary.
//!
//! This module defines the *only* way the rest of aftman talks to a supervisory
//! model. Nothing here names a provider, a URL, an SDK type, or an answer wire
//! shape: `aftman-jev` translates those into these types, and every other crate
//! depends on the trait.
//!
//! That isolation is a hard rule (`AGENTS.md` #2). It is also what makes the whole
//! runtime testable offline: `ScriptedModel` and `RecordedModel` implement the same
//! trait with no network at all.

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::assessment::Band;
use crate::assessment::{AnswerValue, ResolvedCheck};
use crate::error::ModelError;
use crate::ids::{AssessmentSeq, CheckKey};

/// Why a model call is being made. Recorded in usage accounting so that routing
/// cost and assessment cost can be told apart.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Purpose {
    /// The per-cycle assessment. The default, because it is what almost every call is.
    #[default]
    Assessment,
    /// Deciding which conditional responsibilities participate.
    Routing,
    /// An independent second opinion. See `docs/06-ensemble.md`.
    Opinion,
}

/// Token usage for one request, when the provider reports it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
}

impl Usage {
    #[must_use]
    pub const fn total(&self) -> Option<u64> {
        match (self.input_tokens, self.output_tokens) {
            (Some(i), Some(o)) => Some(i + o),
            (Some(i), None) => Some(i),
            (None, Some(o)) => Some(o),
            (None, None) => None,
        }
    }
}

/// A question put to the model.
///
/// The three primitives mirror the provider's, because they are genuinely
/// different shapes of question: a probability, a selection, and a position on a
/// rubric. Collapsing them to yes/no would discard information the policy needs to
/// distinguish "definitely not" from "no idea".
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "primitive")]
pub enum Question {
    /// Probability that a yes/no statement is true.
    Noul { instructions: String },
    /// A selection among named criteria.
    Choice {
        instructions: String,
        criteria: BTreeMap<String, String>,
    },
    /// A position on an ordered rubric, from zero.
    Score {
        instructions: String,
        criteria: Vec<String>,
    },
}

impl Question {
    #[must_use]
    pub fn noul(instructions: impl Into<String>) -> Self {
        Self::Noul {
            instructions: instructions.into(),
        }
    }

    #[must_use]
    pub fn choice(
        instructions: impl Into<String>,
        criteria: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        Self::Choice {
            instructions: instructions.into(),
            criteria: criteria
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        }
    }

    #[must_use]
    pub fn score(
        instructions: impl Into<String>,
        criteria: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self::Score {
            instructions: instructions.into(),
            criteria: criteria.into_iter().map(Into::into).collect(),
        }
    }
}

/// A model available to the configured provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelInfo {
    pub name: String,
    pub description: String,
    pub release_date: String,
}

/// A validated set of answers, plus what it cost.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelAnswers {
    /// The model that actually answered, which may differ from the one requested.
    pub model: String,
    pub usage: Usage,
    pub answers: BTreeMap<CheckKey, AnswerValue>,
    pub request_id: Option<String>,
    /// How many transport attempts this took, including retries.
    pub attempts: u32,
}

/// Byte budgets for the evidence sent to a model.
///
/// Two caps, because two different things go wrong: one oversized leaf (a diff, an
/// output tail) and one oversized document made of many small leaves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Budget {
    /// Cap on one leaf pair's serialized size.
    pub pair_bytes: usize,
    /// Cap on the whole `state` object.
    pub request_bytes: usize,
    /// Hysteresis target for a single pair: once compaction starts it continues to
    /// here, so consecutive cycles do not oscillate.
    pub pair_target_bytes: usize,
    /// Hysteresis target for the whole request.
    pub request_target_bytes: usize,
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            pair_bytes: 20_000,
            request_bytes: 120_000,
            pair_target_bytes: 12_000,
            request_target_bytes: 60_000,
        }
    }
}

/// Why a model call is being made, and what it belongs to.
///
/// Carried into every call so that usage accounting can be attributed to a run and a
/// cycle rather than being an unattributable global counter. `run_id` is absent only
/// for calls made outside a run, such as `aftman doctor --probe`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AskContext {
    pub purpose: Purpose,
    pub run_id: Option<crate::ids::RunId>,
    pub assessment_seq: Option<AssessmentSeq>,
}

impl AskContext {
    #[must_use]
    pub const fn new(purpose: Purpose) -> Self {
        Self {
            purpose,
            run_id: None,
            assessment_seq: None,
        }
    }

    #[must_use]
    pub const fn for_run(
        purpose: Purpose,
        run_id: crate::ids::RunId,
        assessment_seq: AssessmentSeq,
    ) -> Self {
        Self {
            purpose,
            run_id: Some(run_id),
            assessment_seq: Some(assessment_seq),
        }
    }
}

/// A supervisory model.
///
/// Implementations are the only place provider specifics live. An implementation
/// must not decide anything: it returns answers, and
/// [`crate::policy::arbitrate`] decides.
#[async_trait]
pub trait DecisionModel: Send + Sync {
    /// Ask one group of questions about one `state` document.
    async fn ask(
        &self,
        context: &AskContext,
        state: &Value,
        questions: &BTreeMap<CheckKey, Question>,
        budget: &Budget,
    ) -> Result<ModelAnswers, ModelError>;

    /// Models available to this provider. Used by `aftman doctor`; may be empty.
    async fn models(&self) -> Result<Vec<ModelInfo>, ModelError> {
        Ok(Vec::new())
    }

    /// Release any pooled resources. Called once at the end of a run.
    async fn close(&self) -> Result<(), ModelError> {
        Ok(())
    }
}

/// Resolve a group of answers into per-check permissions.
///
/// Free rather than a trait method so that [`DecisionModel`] stays object-safe: a
/// generic `impl IntoIterator` parameter would make `Arc<dyn DecisionModel>`
/// impossible, and the runtime needs exactly that.
#[must_use]
pub fn resolve_checks(
    answers: &ModelAnswers,
    keys: &[(CheckKey, Option<f32>)],
    margin: f32,
    ensemble_enabled: bool,
) -> BTreeMap<CheckKey, ResolvedCheck> {
    keys.iter()
        .map(|(key, threshold)| {
            let p = answers.answers.get(key).and_then(AnswerValue::as_noul);
            let resolved = ResolvedCheck::from_measurement(
                key.clone(),
                p,
                *threshold,
                margin,
                ensemble_enabled,
            );
            (key.clone(), resolved)
        })
        .collect()
}

/// Whether a resolved check is decidable enough to be acted on. Convenience for
/// callers that only need the band, not the whole record.
#[must_use]
pub fn is_decisive(check: &ResolvedCheck) -> bool {
    matches!(check.band, Band::Decisive)
}

/// Build a placeholder key set for a routing call.
#[must_use]
pub fn routing_key(responsibility: &str) -> CheckKey {
    CheckKey::from(format!("{responsibility}__routing"))
}

/// Convenience for building an assessment sequence.
#[must_use]
pub const fn first_assessment() -> AssessmentSeq {
    AssessmentSeq::FIRST
}

#[cfg(test)]
mod tests {
    use super::{Budget, Question, Usage};
    use std::collections::BTreeMap;

    #[test]
    fn usage_totals_only_what_was_reported() {
        assert_eq!(Usage::default().total(), None);
        assert_eq!(
            Usage {
                input_tokens: Some(10),
                output_tokens: None
            }
            .total(),
            Some(10)
        );
        assert_eq!(
            Usage {
                input_tokens: Some(10),
                output_tokens: Some(3)
            }
            .total(),
            Some(13)
        );
    }

    #[test]
    fn question_constructors_build_the_documented_shapes() {
        assert_eq!(
            Question::noul("is it done?"),
            Question::Noul {
                instructions: "is it done?".into()
            }
        );
        let choice = Question::choice("which?", [("a", "first"), ("b", "second")]);
        match choice {
            Question::Choice { criteria, .. } => assert_eq!(criteria.len(), 2),
            other => panic!("expected a choice, got {other:?}"),
        }
        let score = Question::score("how much?", ["none", "some"]);
        match score {
            Question::Score { criteria, .. } => assert_eq!(criteria.len(), 2),
            other => panic!("expected a score, got {other:?}"),
        }
    }

    #[test]
    fn budget_defaults_match_the_documented_config() {
        let budget = Budget::default();
        assert_eq!(budget.pair_bytes, 20_000);
        assert_eq!(budget.request_bytes, 120_000);
        // Targets must be below the caps or hysteresis has no room to operate.
        assert!(budget.pair_target_bytes < budget.pair_bytes);
        assert!(budget.request_target_bytes < budget.request_bytes);
    }

    #[test]
    fn questions_round_trip_through_json() -> Result<(), Box<dyn std::error::Error>> {
        let questions = BTreeMap::from([(
            crate::ids::CheckKey::from("core.completion__ready_to_finish"),
            Question::noul("is the job ready to finish?"),
        )]);
        let json = serde_json::to_string(&questions)?;
        assert_eq!(
            serde_json::from_str::<BTreeMap<crate::ids::CheckKey, Question>>(&json)?,
            questions
        );
        Ok(())
    }
}
