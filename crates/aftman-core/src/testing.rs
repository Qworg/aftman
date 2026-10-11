//! Test doubles for the model boundary.
//!
//! These are what make the runtime testable offline and what make the ensemble
//! replayable. They are compiled into the library rather than hidden behind
//! `#[cfg(test)]` because `aftman demo` and the CLI's offline mode use them too.

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use serde_json::Value;

use crate::assessment::AnswerValue;
use crate::error::ModelError;
use crate::ids::CheckKey;
use crate::model::{Budget, DecisionModel, ModelAnswers, ModelInfo, Purpose, Question, Usage};

/// A model that returns answers from a fixed table.
///
/// Answers are keyed by wire key, so a test can assert exactly what each check
/// saw without going through a provider.
#[derive(Debug, Default)]
pub struct ScriptedModel {
    /// Probability per check key. A key absent here is omitted, which is an error
    /// the caller can test.
    probabilities: BTreeMap<CheckKey, f32>,
    /// If set, every call fails with this error.
    fail_with: Option<ModelError>,
    /// If non-zero, calls fail this many times before succeeding.
    fail_first: u32,
    calls: Mutex<Vec<CallRecord>>,
    model_name: String,
}

/// One recorded call, so a test can assert what was asked.
#[derive(Debug, Clone, PartialEq)]
pub struct CallRecord {
    pub purpose: Purpose,
    pub keys: Vec<CheckKey>,
    pub state: Value,
}

impl ScriptedModel {
    #[must_use]
    pub fn new(model_name: impl Into<String>) -> Self {
        Self {
            model_name: model_name.into(),
            ..Self::default()
        }
    }

    #[must_use]
    pub fn with(mut self, key: &str, probability: f32) -> Self {
        self.probabilities.insert(CheckKey::from(key), probability);
        self
    }

    #[must_use]
    pub fn with_all(mut self, entries: &[(&str, f32)]) -> Self {
        for (key, probability) in entries {
            self.probabilities
                .insert(CheckKey::from(*key), *probability);
        }
        self
    }

    #[must_use]
    pub fn failing_with(mut self, error: ModelError) -> Self {
        self.fail_with = Some(error);
        self
    }

    #[must_use]
    pub fn failing_first(mut self, count: u32) -> Self {
        self.fail_first = count;
        self
    }

    /// Every call made so far.
    #[must_use]
    pub fn calls(&self) -> Vec<CallRecord> {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    #[must_use]
    pub fn call_count(&self) -> usize {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }
}

#[async_trait]
impl DecisionModel for ScriptedModel {
    async fn ask(
        &self,
        purpose: Purpose,
        state: &Value,
        questions: &BTreeMap<CheckKey, Question>,
        _budget: &Budget,
    ) -> Result<ModelAnswers, ModelError> {
        {
            let mut calls = self
                .calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            calls.push(CallRecord {
                purpose,
                keys: questions.keys().cloned().collect(),
                state: state.clone(),
            });
            if self.fail_first > 0 {
                let remaining = calls.len() as u32;
                if remaining <= self.fail_first {
                    return Err(ModelError::Server {
                        status: 503,
                        message: "scripted transient failure".into(),
                    });
                }
            }
        }
        if let Some(error) = &self.fail_with {
            return Err(error.clone());
        }

        let mut answers = BTreeMap::new();
        for key in questions.keys() {
            if let Some(p) = self.probabilities.get(key) {
                answers.insert(key.clone(), AnswerValue::Noul(*p));
            }
        }
        Ok(ModelAnswers {
            model: self.model_name.clone(),
            usage: Usage {
                input_tokens: Some(1),
                output_tokens: Some(1),
            },
            answers,
            request_id: Some("scripted".into()),
            attempts: 1,
        })
    }

    async fn models(&self) -> Result<Vec<ModelInfo>, ModelError> {
        Ok(vec![ModelInfo {
            name: self.model_name.clone(),
            description: "scripted test model".into(),
            release_date: "2026-01-01".into(),
        }])
    }
}

/// A model that derives plausible answers from the observation itself.
///
/// Used by `aftman demo` and by the offline end-to-end test. It is deliberately
/// simple and legible: its job is to make a run take a believable shape, not to be
/// a good supervisor.
///
/// The trajectory is: a first cycle that sees the worker still running and says
/// "not yet", and a later cycle that sees a successful exit and passing tests and
/// says "done".
#[derive(Debug, Default)]
pub struct SimulatedModel {
    model_name: String,
    /// Optional explicit override, for tests that want a fixed first answer.
    overrides: BTreeMap<CheckKey, f32>,
}

impl SimulatedModel {
    #[must_use]
    pub fn new() -> Self {
        Self {
            model_name: "simulated-jev".into(),
            overrides: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn with(mut self, key: &str, probability: f32) -> Self {
        self.overrides.insert(CheckKey::from(key), probability);
        self
    }
}

/// Read a nested number out of the observation `state`.
fn number(state: &Value, path: &[&str]) -> Option<f64> {
    let mut cursor = state;
    for step in path {
        cursor = cursor.get(step)?;
    }
    cursor.as_f64()
}

fn string_at<'a>(state: &'a Value, path: &[&str]) -> Option<&'a str> {
    let mut cursor = state;
    for step in path {
        cursor = cursor.get(step)?;
    }
    cursor.as_str()
}

#[async_trait]
impl DecisionModel for SimulatedModel {
    async fn ask(
        &self,
        _purpose: Purpose,
        state: &Value,
        questions: &BTreeMap<CheckKey, Question>,
        _budget: &Budget,
    ) -> Result<ModelAnswers, ModelError> {
        let worker_done = string_at(state, &["active_worker", "status"]) == Some("completed");
        let exit_ok = number(state, &["active_worker", "exit_code"]) == Some(0.0);
        // Test results are an array of parsed summaries.
        let tests_passed = state
            .get("test_results")
            .and_then(Value::as_array)
            .is_some_and(|results| {
                results.iter().any(|r| {
                    r.get("failed").and_then(Value::as_u64) == Some(0)
                        && r.get("passed").and_then(Value::as_u64).unwrap_or(0) > 0
                })
            });
        let diff_present = state
            .get("git_diff")
            .and_then(|d| d.get("diff"))
            .and_then(Value::as_str)
            .is_some_and(|d| !d.trim().is_empty());

        // A plausible, legible trajectory: an active worker is progressing but not
        // done; a finished worker with passing tests and a diff is complete.
        let (
            implementation,
            requirements,
            ready,
            tests,
            needs_verification,
            stuck,
            off_track,
            human,
        ) = if worker_done && exit_ok && tests_passed && diff_present {
            (0.95_f32, 0.93, 0.94, 0.93, 0.20, 0.02, 0.03, 0.01)
        } else if worker_done && exit_ok {
            (0.80, 0.78, 0.62, 0.55, 0.72, 0.05, 0.06, 0.02)
        } else {
            (0.35, 0.30, 0.10, 0.30, 0.40, 0.10, 0.08, 0.02)
        };

        let table: [(&str, f32); 8] = [
            ("core.completion__implementation_complete", implementation),
            ("core.completion__requirements_satisfied", requirements),
            ("core.completion__ready_to_finish", ready),
            ("core.verification__tests_sufficient", tests),
            ("core.verification__needs_verification", needs_verification),
            ("core.worker-health__worker_stuck", stuck),
            ("core.worker-health__work_off_track", off_track),
            ("core.human-escalation__needs_human", human),
        ];

        let mut answers = BTreeMap::new();
        for key in questions.keys() {
            if let Some(p) = self.overrides.get(key) {
                answers.insert(key.clone(), AnswerValue::Noul(*p));
                continue;
            }
            if let Some((_, p)) = table.iter().find(|(k, _)| *k == key.as_str()) {
                answers.insert(key.clone(), AnswerValue::Noul(*p));
            }
        }
        // `meaningful_progress` is a plausible inverse of stuckness.
        let progress_key = CheckKey::from("core.worker-health__meaningful_progress");
        if questions.contains_key(&progress_key) {
            answers.insert(progress_key, AnswerValue::Noul(1.0 - stuck));
        }
        // `agents_md_drift` answers "no" when there is nothing to compare against,
        // which is what the check's instructions demand.
        let drift_key = CheckKey::from("repository.instructions__agents_md_drift");
        if questions.contains_key(&drift_key) {
            // Present means: the key exists and is not JSON null.
            let present = state
                .get("agents_md_instructions")
                .is_some_and(|v| !v.is_null());
            answers.insert(
                drift_key,
                AnswerValue::Noul(if present { 0.02 } else { 0.0 }),
            );
        }
        // Routing questions are asked per responsibility; answer "yes" so that
        // conditional responsibilities are visible in a demo run.
        for key in questions.keys() {
            if key.as_str().ends_with("__routing") {
                answers.insert(key.clone(), AnswerValue::Noul(0.75));
            }
        }

        Ok(ModelAnswers {
            model: self.model_name.clone(),
            usage: Usage {
                input_tokens: Some(256),
                output_tokens: Some(8),
            },
            answers,
            request_id: Some("simulated".into()),
            attempts: 1,
        })
    }

    async fn models(&self) -> Result<Vec<ModelInfo>, ModelError> {
        Ok(vec![ModelInfo {
            name: self.model_name.clone(),
            description: "offline simulated decision model".into(),
            release_date: "2026-01-01".into(),
        }])
    }
}

#[cfg(test)]
mod tests {
    use super::{ScriptedModel, SimulatedModel};
    use crate::error::ModelError;
    use crate::ids::CheckKey;
    use crate::model::{Budget, DecisionModel, Purpose, Question};
    use std::collections::BTreeMap;

    fn questions() -> BTreeMap<CheckKey, Question> {
        BTreeMap::from([
            (
                CheckKey::from("core.completion__ready_to_finish"),
                Question::noul("ready?"),
            ),
            (
                CheckKey::from("core.worker-health__worker_stuck"),
                Question::noul("stuck?"),
            ),
        ])
    }

    #[tokio::test]
    async fn scripted_model_returns_configured_probabilities()
    -> Result<(), Box<dyn std::error::Error>> {
        let model = ScriptedModel::new("test")
            .with("core.completion__ready_to_finish", 0.91)
            .with("core.worker-health__worker_stuck", 0.02);
        let answers = model
            .ask(
                Purpose::Assessment,
                &serde_json::json!({}),
                &questions(),
                &Budget::default(),
            )
            .await?;
        assert_eq!(
            answers.answers[&CheckKey::from("core.completion__ready_to_finish")].as_noul(),
            Some(0.91)
        );
        assert_eq!(answers.model, "test");
        assert_eq!(model.call_count(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn scripted_model_omits_unconfigured_keys() -> Result<(), Box<dyn std::error::Error>> {
        let model = ScriptedModel::new("test").with("core.completion__ready_to_finish", 0.5);
        let answers = model
            .ask(
                Purpose::Assessment,
                &serde_json::json!({}),
                &questions(),
                &Budget::default(),
            )
            .await?;
        assert!(
            answers
                .answers
                .contains_key(&CheckKey::from("core.completion__ready_to_finish"))
        );
        assert!(
            !answers
                .answers
                .contains_key(&CheckKey::from("core.worker-health__worker_stuck")),
            "an unconfigured key must be absent so the caller can test MissingAnswer"
        );
        Ok(())
    }

    #[tokio::test]
    async fn scripted_model_can_fail_on_demand() {
        let model = ScriptedModel::new("test").failing_with(ModelError::Auth);
        let result = model
            .ask(
                Purpose::Assessment,
                &serde_json::json!({}),
                &questions(),
                &Budget::default(),
            )
            .await;
        assert!(matches!(result, Err(ModelError::Auth)));
        // Usage accounting still happens, so the call is visible even on failure.
        assert_eq!(model.call_count(), 1);
    }

    #[tokio::test]
    async fn scripted_model_can_fail_then_succeed() -> Result<(), Box<dyn std::error::Error>> {
        let model = ScriptedModel::new("test")
            .with("core.completion__ready_to_finish", 0.9)
            .failing_first(2);
        let q = questions();
        assert!(
            model
                .ask(
                    Purpose::Assessment,
                    &serde_json::json!({}),
                    &q,
                    &Budget::default()
                )
                .await
                .is_err()
        );
        assert!(
            model
                .ask(
                    Purpose::Assessment,
                    &serde_json::json!({}),
                    &q,
                    &Budget::default()
                )
                .await
                .is_err()
        );
        assert!(
            model
                .ask(
                    Purpose::Assessment,
                    &serde_json::json!({}),
                    &q,
                    &Budget::default()
                )
                .await
                .is_ok()
        );
        Ok(())
    }

    #[tokio::test]
    async fn scripted_model_records_what_it_was_asked() -> Result<(), Box<dyn std::error::Error>> {
        let model = ScriptedModel::new("test").with("core.completion__ready_to_finish", 0.9);
        let state = serde_json::json!({ "job": { "prompt": "do it" } });
        model
            .ask(
                Purpose::Assessment,
                &state,
                &questions(),
                &Budget::default(),
            )
            .await?;
        let calls = model.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].purpose, Purpose::Assessment);
        assert_eq!(calls[0].keys.len(), 2);
        assert_eq!(calls[0].state["job"]["prompt"], serde_json::json!("do it"));
        Ok(())
    }

    #[tokio::test]
    async fn simulated_model_calls_an_unfinished_worker_not_done()
    -> Result<(), Box<dyn std::error::Error>> {
        let model = SimulatedModel::new();
        let state = serde_json::json!({
            "active_worker": { "status": "running" },
            "git_diff": { "diff": "" },
            "test_results": [],
        });
        let answers = model
            .ask(
                Purpose::Assessment,
                &state,
                &questions(),
                &Budget::default(),
            )
            .await?;
        let ready = answers.answers[&CheckKey::from("core.completion__ready_to_finish")].as_noul();
        assert!(ready.is_some_and(|p| p < 0.5), "got {ready:?}");
        Ok(())
    }

    #[tokio::test]
    async fn simulated_model_calls_a_finished_verified_worker_done()
    -> Result<(), Box<dyn std::error::Error>> {
        let model = SimulatedModel::new();
        let state = serde_json::json!({
            "active_worker": { "status": "completed", "exit_code": 0 },
            "git_diff": { "diff": "+ added retries" },
            "test_results": [{ "passed": 3, "failed": 0 }],
        });
        let answers = model
            .ask(
                Purpose::Assessment,
                &state,
                &questions(),
                &Budget::default(),
            )
            .await?;
        let ready = answers.answers[&CheckKey::from("core.completion__ready_to_finish")].as_noul();
        assert!(ready.is_some_and(|p| p > 0.9), "got {ready:?}");
        Ok(())
    }

    #[tokio::test]
    async fn simulated_model_calls_a_worker_with_claims_but_no_diff_incomplete()
    -> Result<(), Box<dyn std::error::Error>> {
        // This is the "3 passed" with no actual change case the explainer describes.
        let model = SimulatedModel::new();
        let state = serde_json::json!({
            "active_worker": { "status": "completed", "exit_code": 0 },
            "git_diff": { "diff": "   " },
            "test_results": [{ "passed": 3, "failed": 0 }],
        });
        let answers = model
            .ask(
                Purpose::Assessment,
                &state,
                &questions(),
                &Budget::default(),
            )
            .await?;
        let ready = answers.answers[&CheckKey::from("core.completion__ready_to_finish")].as_noul();
        assert!(
            ready.is_some_and(|p| (0.5..0.75).contains(&p)),
            "a claim without a change must not read as finished; got {ready:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn simulated_model_answers_drift_no_when_there_are_no_instructions()
    -> Result<(), Box<dyn std::error::Error>> {
        let model = SimulatedModel::new();
        let mut q = questions();
        q.insert(
            CheckKey::from("repository.instructions__agents_md_drift"),
            Question::noul("drift?"),
        );
        let state = serde_json::json!({ "agents_md_instructions": null });
        let answers = model
            .ask(Purpose::Assessment, &state, &q, &Budget::default())
            .await?;
        assert_eq!(
            answers.answers[&CheckKey::from("repository.instructions__agents_md_drift")].as_noul(),
            Some(0.0),
            "the check demands a decisive no when there is nothing to compare against"
        );
        Ok(())
    }
}
