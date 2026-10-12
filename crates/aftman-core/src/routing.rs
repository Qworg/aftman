//! Deciding which conditional responsibilities participate.
//!
//! Before the first worker starts, every conditional responsibility is evaluated
//! **together in one request** against the incoming job, and each whose probability
//! clears its own threshold is activated. Global responsibilities skip this entirely.
//!
//! Two properties matter:
//!
//! - **Routing sees the job text, never the repository.** It decides what kind of work
//!   this is, not what the code looks like, so it cannot be swayed by repository content
//!   and does not pay to read it.
//! - **Routing chooses which checks participate, never what happens.** A routing mistake
//!   costs a dimension of supervision; it cannot produce an action. That separation is
//!   why routing is allowed to be a model call at all.
//!
//! See `docs/05-responsibilities.md` §5.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::definitions::DefinitionSet;
use crate::error::ModelError;
use crate::ids::{CheckKey, ResponsibilityId};
use crate::model::{AskContext, Budget, DecisionModel, Question};

/// The suffix that turns a responsibility id into its routing question key.
pub const ROUTING_SUFFIX: &str = "routing";

/// How much of the job routing is shown.
pub const ROUTING_JOB_CAP: usize = 4_000;

/// The routing question key for a responsibility.
#[must_use]
pub fn routing_key(responsibility: &ResponsibilityId) -> CheckKey {
    CheckKey::from(format!("{}__{ROUTING_SUFFIX}", responsibility.as_str()))
}

/// What routing decided about one responsibility.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RoutingDecision {
    pub responsibility: ResponsibilityId,
    pub threshold: f32,
    /// Absent when the model did not answer, which is recorded rather than assumed.
    pub probability: Option<f32>,
    pub activated: bool,
}

/// What routing decided overall.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RoutingOutcome {
    /// Globals first, then activated conditionals, in definition order.
    pub activated: Vec<ResponsibilityId>,
    /// Every conditional responsibility considered, with its probability and threshold,
    /// so "why didn't the documentation check run?" has an answer.
    pub decisions: Vec<RoutingDecision>,
    pub model: Option<String>,
    /// True when the model could not be asked. The globals still run.
    pub degraded: bool,
}

impl RoutingOutcome {
    /// The globals only, for when there is nothing to route.
    #[must_use]
    pub fn globals_only(definitions: &DefinitionSet) -> Self {
        Self {
            activated: definitions
                .enabled()
                .filter(|(_, definition)| definition.routing().always)
                .map(|(id, _)| ResponsibilityId::new(id.as_str()))
                .collect(),
            decisions: Vec::new(),
            model: None,
            degraded: false,
        }
    }

    #[must_use]
    pub fn activated_count(&self) -> usize {
        self.activated.len()
    }

    #[must_use]
    pub fn conditional_count(&self) -> usize {
        self.decisions.len()
    }
}

/// The routing questions for a definition set.
///
/// Only conditionals are asked: a global responsibility does not need a model call to
/// decide it participates, and paying for one would be waste.
#[must_use]
pub fn routing_questions(definitions: &DefinitionSet) -> BTreeMap<CheckKey, Question> {
    definitions
        .enabled()
        .filter_map(|(id, definition)| {
            let routing = definition.routing();
            if routing.always {
                return None;
            }
            Some((
                routing_key(&ResponsibilityId::new(id.as_str())),
                Question::noul(routing.instructions.unwrap_or_default()),
            ))
        })
        .collect()
}

/// The `state` routing is evaluated against: the job, and nothing else.
#[must_use]
pub fn routing_state(job_prompt: &str) -> serde_json::Value {
    let (prompt, truncated) = crate::observation::cap_text(job_prompt, ROUTING_JOB_CAP);
    serde_json::json!({
        "job": { "prompt": prompt, "prompt_truncated": truncated },
        "note": "Routing decides which checks participate. The repository is deliberately \
                 not consulted here.",
    })
}

/// Resolve answers into a routing outcome.
///
/// An unanswered conditional is **not** activated: a responsibility that could not be
/// evaluated does not get to participate, and the reason is recorded.
#[must_use]
pub fn resolve(
    definitions: &DefinitionSet,
    answers: Option<&BTreeMap<CheckKey, crate::assessment::AnswerValue>>,
    model: Option<String>,
    degraded: bool,
) -> RoutingOutcome {
    let mut activated: Vec<ResponsibilityId> = definitions
        .enabled()
        .filter(|(_, definition)| definition.routing().always)
        .map(|(id, _)| ResponsibilityId::new(id.as_str()))
        .collect();
    let mut decisions = Vec::new();

    for (id, definition) in definitions.enabled() {
        let routing = definition.routing();
        if routing.always {
            continue;
        }
        let key = routing_key(&ResponsibilityId::new(id.as_str()));
        let probability = answers
            .and_then(|answers| answers.get(&key))
            .and_then(crate::assessment::AnswerValue::as_noul);
        let is_activated = probability.is_some_and(|p| p >= routing.threshold);
        if is_activated {
            activated.push(ResponsibilityId::new(id.as_str()));
        }
        decisions.push(RoutingDecision {
            responsibility: ResponsibilityId::new(id.as_str()),
            threshold: routing.threshold,
            probability,
            activated: is_activated,
        });
    }

    RoutingOutcome {
        activated,
        decisions,
        model,
        degraded,
    }
}

/// Ask the model which responsibilities participate.
///
/// A model failure is not fatal: the globals still run and the degradation is recorded,
/// because a run with partial supervision is better than no run — provided it says so.
pub async fn route(
    model: &dyn DecisionModel,
    context: &AskContext,
    definitions: &DefinitionSet,
    job_prompt: &str,
    budget: &Budget,
) -> RoutingOutcome {
    let questions = routing_questions(definitions);
    if questions.is_empty() {
        // Nothing conditional: no reason to spend a call.
        return RoutingOutcome::globals_only(definitions);
    }

    match model
        .ask(context, &routing_state(job_prompt), &questions, budget)
        .await
    {
        Ok(answers) => resolve(
            definitions,
            Some(&answers.answers),
            Some(answers.model),
            false,
        ),
        Err(error) => {
            tracing::warn!(
                code = error.code(),
                "routing failed; only the global responsibilities will participate"
            );
            let mut outcome = resolve(definitions, None, None, true);
            outcome.degraded = true;
            outcome
        }
    }
}

/// Whether a routing failure should be surfaced as a distinct error code.
#[must_use]
pub fn is_retryable(error: &ModelError) -> bool {
    error.is_retryable()
}

#[cfg(test)]
mod tests {
    use super::{
        ROUTING_JOB_CAP, RoutingOutcome, resolve, route, routing_key, routing_questions,
        routing_state,
    };
    use crate::assessment::AnswerValue;
    use crate::definitions::DefinitionSet;
    use crate::error::ModelError;
    use crate::ids::{CheckKey, ResponsibilityId};
    use crate::model::{AskContext, Budget, Purpose};
    use crate::testing::ScriptedModel;
    use std::collections::BTreeMap;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn answers(entries: &[(&str, f32)]) -> BTreeMap<CheckKey, AnswerValue> {
        entries
            .iter()
            .map(|(key, p)| (CheckKey::from(*key), AnswerValue::Noul(*p)))
            .collect()
    }

    #[test]
    fn only_conditional_responsibilities_are_asked() -> TestResult {
        let set = DefinitionSet::bundled()?;
        let questions = routing_questions(&set);
        // Exactly one bundled responsibility is conditional: the documentation one.
        assert_eq!(questions.len(), 1);
        assert!(questions.contains_key(&routing_key(&ResponsibilityId::new(
            "quality.documentation"
        ))));
        assert!(!questions.contains_key(&routing_key(&ResponsibilityId::new("core.completion"))));
        Ok(())
    }

    #[test]
    fn the_routing_question_carries_the_declared_instructions() -> TestResult {
        let set = DefinitionSet::bundled()?;
        let questions = routing_questions(&set);
        let question = questions.values().next().ok_or("a routing question")?;
        match question {
            crate::model::Question::Noul { instructions } => {
                assert!(
                    instructions.contains("documentation"),
                    "the declared text must be what the model reads: {instructions}"
                );
            }
            other => panic!("routing is a yes/no question, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn routing_state_carries_the_job_and_not_the_repository() {
        let state = routing_state("Add retries to the HTTP client.");
        assert!(
            state["job"]["prompt"]
                .as_str()
                .is_some_and(|p| p.contains("Add retries"))
        );
        // No repository evidence, by design.
        for absent in [
            "git_diff",
            "changed_files",
            "worker_output",
            "untracked_evidence",
        ] {
            assert!(
                state.get(absent).is_none(),
                "{absent} must not be routed on"
            );
        }
    }

    #[test]
    fn a_long_job_is_capped_before_it_is_routed() {
        let state = routing_state(&"x".repeat(ROUTING_JOB_CAP * 3));
        let prompt = state["job"]["prompt"].as_str().unwrap_or_default();
        assert_eq!(prompt.chars().count(), ROUTING_JOB_CAP);
        assert_eq!(state["job"]["prompt_truncated"], serde_json::json!(true));
    }

    #[test]
    fn a_conditional_above_its_threshold_is_activated() -> TestResult {
        let set = DefinitionSet::bundled()?;
        let outcome = resolve(
            &set,
            Some(&answers(&[("quality.documentation__routing", 0.85)])),
            Some("m".to_owned()),
            false,
        );
        assert!(
            outcome
                .activated
                .iter()
                .any(|id| id.as_str() == "quality.documentation")
        );
        assert_eq!(outcome.conditional_count(), 1);
        assert!(outcome.decisions[0].activated);
        assert_eq!(outcome.decisions[0].probability, Some(0.85));
        assert_eq!(outcome.decisions[0].threshold, 0.70);
        Ok(())
    }

    #[test]
    fn a_conditional_below_its_threshold_is_not_activated() -> TestResult {
        let set = DefinitionSet::bundled()?;
        let outcome = resolve(
            &set,
            Some(&answers(&[("quality.documentation__routing", 0.20)])),
            Some("m".to_owned()),
            false,
        );
        assert!(
            !outcome
                .activated
                .iter()
                .any(|id| id.as_str() == "quality.documentation")
        );
        // The decision is still recorded, so "why didn't it run?" is answerable.
        assert_eq!(outcome.conditional_count(), 1);
        assert!(!outcome.decisions[0].activated);
        Ok(())
    }

    #[test]
    fn the_threshold_boundary_activates() -> TestResult {
        let set = DefinitionSet::bundled()?;
        let outcome = resolve(
            &set,
            Some(&answers(&[("quality.documentation__routing", 0.70)])),
            None,
            false,
        );
        assert!(
            outcome.decisions[0].activated,
            "equal to the threshold clears it"
        );
        Ok(())
    }

    #[test]
    fn globals_are_always_activated() -> TestResult {
        let set = DefinitionSet::bundled()?;
        let outcome = resolve(&set, None, None, true);
        for id in [
            "core.completion",
            "core.verification",
            "core.worker-health",
            "core.human-escalation",
            "repository.instructions",
        ] {
            assert!(
                outcome.activated.iter().any(|a| a.as_str() == id),
                "{id} must participate regardless of routing: {:?}",
                outcome.activated
            );
        }
        assert!(
            !outcome
                .activated
                .iter()
                .any(|a| a.as_str() == "quality.documentation")
        );
        Ok(())
    }

    #[test]
    fn an_unanswered_conditional_is_not_activated_and_says_so() -> TestResult {
        let set = DefinitionSet::bundled()?;
        let outcome = resolve(&set, Some(&BTreeMap::new()), Some("m".to_owned()), false);
        assert!(!outcome.decisions[0].activated);
        assert_eq!(
            outcome.decisions[0].probability, None,
            "an absent answer is recorded as absent, not as zero"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_model_failure_degrades_to_the_globals() -> TestResult {
        let set = DefinitionSet::bundled()?;
        let model = ScriptedModel::new("broken").failing_with(ModelError::Auth);
        let outcome = route(
            &model,
            &AskContext::new(Purpose::Routing),
            &set,
            "Add retries.",
            &Budget::default(),
        )
        .await;
        assert!(outcome.degraded, "the degradation must be visible");
        // The globals still run, because partial supervision beats none.
        assert!(outcome.activated_count() >= 5);
        assert!(
            !outcome
                .activated
                .iter()
                .any(|a| a.as_str() == "quality.documentation")
        );
        Ok(())
    }

    #[tokio::test]
    async fn routing_asks_one_request_and_activatess_what_it_should() -> TestResult {
        let set = DefinitionSet::bundled()?;
        let model = ScriptedModel::new("jev-test").with("quality.documentation__routing", 0.91);
        let outcome = route(
            &model,
            &AskContext::new(Purpose::Routing),
            &set,
            "Document the new retry behaviour in the README.",
            &Budget::default(),
        )
        .await;
        assert_eq!(
            model.call_count(),
            1,
            "one request, not one per responsibility"
        );
        assert!(
            outcome
                .activated
                .iter()
                .any(|a| a.as_str() == "quality.documentation")
        );
        assert_eq!(outcome.model.as_deref(), Some("jev-test"));
        assert!(!outcome.degraded);
        Ok(())
    }

    #[tokio::test]
    async fn routing_is_skipped_entirely_when_nothing_is_conditional() -> TestResult {
        // A definition set with no conditionals must not spend a call deciding nothing.
        let set = DefinitionSet::bundled()?;
        let questions = routing_questions(&set);
        let model = ScriptedModel::new("m").with("quality.documentation__routing", 0.9);
        // With the documentation definition disabled there is nothing to route.
        let mut set_without = DefinitionSet::default();
        for (id, definition) in &set.definitions {
            if id != "quality.documentation" {
                set_without
                    .definitions
                    .insert(id.clone(), definition.clone());
            }
        }
        let outcome = route(
            &model,
            &AskContext::new(Purpose::Routing),
            &set_without,
            "job",
            &Budget::default(),
        )
        .await;
        assert_eq!(questions.len(), 1, "the full set does have one conditional");
        assert_eq!(model.call_count(), 0, "nothing to route, nothing to spend");
        assert_eq!(outcome.activated_count(), 5);
        Ok(())
    }

    #[test]
    fn globals_only_lists_exactly_the_globals() -> TestResult {
        let set = DefinitionSet::bundled()?;
        let outcome = RoutingOutcome::globals_only(&set);
        assert_eq!(outcome.activated_count(), 5);
        assert!(outcome.decisions.is_empty());
        assert!(!outcome.degraded);
        Ok(())
    }
}
