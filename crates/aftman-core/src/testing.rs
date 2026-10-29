//! Test doubles for the model boundary.
//!
//! These are what make the runtime testable offline and what make the ensemble
//! replayable. They are compiled into the library rather than hidden behind
//! `#[cfg(test)]` because `aftman demo` and the CLI's offline mode use them too.

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use serde_json::Value;

use crate::assessment::{AnswerValue, Rung, Verdict};
use crate::ensemble::{
    EnsembleOutcome, EnsembleResolvedPayload, OpinionKey, OpinionRecordedPayload,
    OpinionRequestedPayload, OpinionSource, ReframeAnswer, ReframeRequest, RefreshOutcome,
    RefreshRequest, SourceError,
};
use crate::error::ModelError;
use crate::event::{EventEnvelope, EventKind};
use crate::ids::{AssessmentSeq, CheckKey};
use crate::model::{
    AskContext, Budget, DecisionModel, ModelAnswers, ModelInfo, Purpose, Question, Usage,
};

/// A model that returns answers from a fixed table.
///
/// Answers are keyed by wire key, so a test can assert exactly what each check
/// saw without going through a provider.
#[derive(Debug, Default)]
pub struct ScriptedModel {
    /// Probability per check key. A key absent here is omitted, which is an error
    /// the caller can test.
    probabilities: BTreeMap<CheckKey, f32>,
    /// The answer to a check's negated question (ensemble R2, `Purpose::Reframe`). A key
    /// absent here is answered with the complement of its probability: a model that is
    /// consistent under reframing.
    negated: BTreeMap<CheckKey, f32>,
    /// If set, every call fails with this error.
    fail_with: Option<ModelError>,
    /// If non-zero, calls fail this many times before succeeding.
    fail_first: u32,
    calls: Mutex<Vec<CallRecord>>,
    model_name: String,
}

/// One recorded call, so a test can assert what was asked: the purpose, the questions
/// exactly as sent, and the state document.
#[derive(Debug, Clone, PartialEq)]
pub struct CallRecord {
    pub context: AskContext,
    pub keys: Vec<CheckKey>,
    pub questions: BTreeMap<CheckKey, Question>,
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

    /// Answer the check's negated question (R2) with `probability`, instead of the
    /// complement of its own answer.
    #[must_use]
    pub fn with_negated(mut self, key: &str, probability: f32) -> Self {
        self.negated.insert(CheckKey::from(key), probability);
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
        context: &AskContext,
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
                context: *context,
                keys: questions.keys().cloned().collect(),
                questions: questions.clone(),
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
            // An R2 reframe asks the check's negation under the same key: a calibrated
            // model answers it with the complement, unless the test scripted otherwise.
            let p = if context.purpose == Purpose::Reframe {
                self.negated
                    .get(key)
                    .copied()
                    .or_else(|| self.probabilities.get(key).map(|p| 1.0 - p))
            } else {
                self.probabilities.get(key).copied()
            };
            if let Some(p) = p {
                answers.insert(key.clone(), AnswerValue::Noul(p));
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
        context: &AskContext,
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
        // An explicit override always wins. Inserting unconditionally here would
        // silently ignore `with("…__routing", p)`, which is exactly how a test asking for
        // "routing says this is not a documentation job" would quietly get the opposite.
        let mut fill = |key: CheckKey, value: f32| {
            answers.entry(key).or_insert(AnswerValue::Noul(value));
        };

        // `meaningful_progress` is a plausible inverse of stuckness.
        let progress_key = CheckKey::from("core.worker-health__meaningful_progress");
        if questions.contains_key(&progress_key) {
            fill(progress_key, 1.0 - stuck);
        }
        // `agents_md_drift` answers "no" when there is nothing to compare against,
        // which is what the check's instructions demand.
        let drift_key = CheckKey::from("repository.instructions__agents_md_drift");
        if questions.contains_key(&drift_key) {
            // Present means: the key exists and is not JSON null.
            let present = state
                .get("agents_md_instructions")
                .is_some_and(|v| !v.is_null());
            fill(drift_key, if present { 0.02 } else { 0.0 });
        }
        // `quality.documentation` is conditional, so routing may activate it. A check
        // whose answer is missing is fatal, so the simulator has to answer it too:
        // documentation is as complete as the change is.
        let docs_key = CheckKey::from("quality.documentation__documentation_sufficient");
        if questions.contains_key(&docs_key) {
            // 0.95 rather than 0.90: against a 0.75 threshold with a 0.15 band, 0.90
            // sits *exactly* on the edge, and f32 makes `0.90 - 0.75` equal
            // `0.14999998`, which reads as in band. A simulator that lands on the
            // boundary is testing float representation rather than the trajectory.
            let score = if worker_done && exit_ok && diff_present {
                0.95
            } else {
                0.40
            };
            fill(docs_key, score);
        }
        // Routing questions are asked per responsibility; answer "yes" so that
        // conditional responsibilities are visible in a demo run.
        for key in questions.keys() {
            if key.as_str().ends_with("__routing") {
                fill(key.clone(), 0.75);
            }
        }

        // An R2 reframe asks each check's negation under the same key. The simulator is
        // consistent under reframing: it answers the complement of what it would say to the
        // question itself.
        if context.purpose == Purpose::Reframe {
            for answer in answers.values_mut() {
                if let AnswerValue::Noul(p) = answer {
                    *p = 1.0 - *p;
                }
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

/// An [`OpinionSource`] that answers from recorded outcomes, and from nothing else.
///
/// Built from a run's events ([`RecordedOpinionSource::from_events`]) or from a
/// resolution ([`RecordedOpinionSource::record`]), it gives back exactly what was
/// recorded, so that replaying a run's [`crate::ensemble::EnsemblePlan`]s against it
/// produces identical resolutions (`docs/06-ensemble.md` §8). Tests also build it by
/// hand, as a scripted source; a test that wants a failed call records one.
///
/// **It never invents an answer.** A request with no recording is a divergence, returned
/// as [`SourceError::NotRecorded`], which aborts the resolution. Verdicts recorded in the
/// log are kept so that [`RecordedOpinionSource::divergences`] can compare them with the
/// ones a replay recomputes. Every request is counted, so a test can assert that nothing
/// was asked.
#[derive(Debug, Default)]
pub struct RecordedOpinionSource {
    refreshes: BTreeMap<(AssessmentSeq, CheckKey), RefreshOutcome>,
    reframes: BTreeMap<OpinionKey, ReframeAnswer>,
    verdicts: BTreeMap<OpinionKey, Verdict>,
    /// A trailing cycle the log started but never resolved. See `from_events`.
    incomplete_cycle: Option<AssessmentSeq>,
    refresh_calls: Mutex<Vec<RefreshRequest>>,
    reframe_calls: Mutex<Vec<ReframeRequest>>,
}

/// Why a log cannot be replayed, or how a replay differed from it.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ReplayError {
    #[error("event {seq} could not be decoded: {message}")]
    Decode { seq: u64, message: String },
    /// `ENSEMBLE_RESOLVED` says an opinion was used, or `OPINION_REQUESTED` says one was
    /// asked, and no `OPINION_RECORDED` records it.
    #[error("the log does not record opinion {0:?}")]
    MissingOpinion(OpinionKey),
    /// `ENSEMBLE_RESOLVED` says a refresh was used and records no outcome for it.
    #[error("the log does not record the refresh of {check} in cycle {cycle}")]
    MissingRefresh {
        cycle: AssessmentSeq,
        check: CheckKey,
    },
    /// The replay computed a different verdict from the recorded answer than the run did.
    #[error("opinion {key:?}: recorded {recorded:?}, replayed {replayed:?}")]
    VerdictMismatch {
        key: OpinionKey,
        recorded: Verdict,
        replayed: Verdict,
    },
    /// The replay produced an opinion the log never recorded.
    #[error("the replay produced opinion {0:?}, which the log does not record")]
    UnrecordedOpinion(OpinionKey),
}

impl RecordedOpinionSource {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Answer a refresh of `check` in `cycle` with `outcome`.
    #[must_use]
    pub fn with_refresh(
        mut self,
        cycle: AssessmentSeq,
        check: impl Into<CheckKey>,
        outcome: RefreshOutcome,
    ) -> Self {
        self.refreshes.insert((cycle, check.into()), outcome);
        self
    }

    /// Answer the opinion `key` with `answer`.
    #[must_use]
    pub fn with_reframe(mut self, key: OpinionKey, answer: ReframeAnswer) -> Self {
        self.reframes.insert(key, answer);
        self
    }

    /// Record everything a resolution asked for and got, verdicts included.
    pub fn record(&mut self, outcome: &EnsembleOutcome) {
        for record in &outcome.records {
            if let Some(refresh) = &record.refresh {
                self.refreshes.insert(
                    (outcome.cycle, record.check.clone()),
                    refresh.outcome.clone(),
                );
            }
            for keyed in &record.opinions {
                self.reframes
                    .insert(keyed.key.clone(), ReframeAnswer::from(&keyed.opinion));
                self.verdicts
                    .insert(keyed.key.clone(), keyed.opinion.verdict);
            }
        }
    }

    /// Build from a run's events: `OPINION_RECORDED` for opinions, and the per-check
    /// `refresh` entries of `ENSEMBLE_RESOLVED` for evidence refreshes.
    ///
    /// # Errors
    ///
    /// Refuses an incomplete log: every opinion that `OPINION_REQUESTED` says was asked,
    /// or that an `ENSEMBLE_RESOLVED` entry lists among its rungs, needs its
    /// `OPINION_RECORDED`; every refresh an entry lists needs its recorded outcome. An
    /// `ENSEMBLE_RESOLVED` written before it carried check entries records nothing and is
    /// skipped.
    ///
    /// One gap is tolerated: requests in a **trailing** cycle that never reached
    /// `ENSEMBLE_RESOLVED` — a run cancelled or crashed mid-opinion. The log replays up to
    /// its last complete cycle, and [`RecordedOpinionSource::incomplete_cycle`] names the
    /// trailing one. A gap in any earlier cycle, or in a resolved cycle, is still refused.
    pub fn from_events(events: &[EventEnvelope]) -> Result<Self, ReplayError> {
        let decode = |event: &EventEnvelope, error: serde_json::Error| ReplayError::Decode {
            seq: event.seq.get(),
            message: error.to_string(),
        };
        let mut source = Self::new();
        let mut required_opinions: Vec<OpinionKey> = Vec::new();
        let mut required_refreshes: Vec<(AssessmentSeq, CheckKey)> = Vec::new();
        let mut resolved_cycles: Vec<AssessmentSeq> = Vec::new();
        let mut last_seen: Option<AssessmentSeq> = None;
        for event in events {
            match event.kind {
                EventKind::OpinionRequested => {
                    let payload: OpinionRequestedPayload =
                        serde_json::from_value(event.payload.clone())
                            .map_err(|e| decode(event, e))?;
                    last_seen = last_seen.max(Some(payload.opinion_id.cycle));
                    required_opinions.push(payload.opinion_id);
                }
                EventKind::OpinionRecorded => {
                    let payload: OpinionRecordedPayload =
                        serde_json::from_value(event.payload.clone())
                            .map_err(|e| decode(event, e))?;
                    source
                        .verdicts
                        .insert(payload.opinion_id.clone(), payload.verdict);
                    source
                        .reframes
                        .insert(payload.opinion_id.clone(), payload.answer());
                }
                EventKind::EnsembleResolved => {
                    let payload: EnsembleResolvedPayload =
                        serde_json::from_value(event.payload.clone())
                            .map_err(|e| decode(event, e))?;
                    let Some(cycle) = payload.cycle else {
                        continue;
                    };
                    resolved_cycles.push(cycle);
                    last_seen = last_seen.max(Some(cycle));
                    for check in payload.checks {
                        for rung in &check.rungs_used {
                            match rung {
                                Rung::RefreshEvidence => {
                                    required_refreshes.push((cycle, check.check.clone()));
                                }
                                Rung::ReframeSameModel | Rung::IndependentJudge => {
                                    required_opinions.push(OpinionKey {
                                        cycle,
                                        check: check.check.clone(),
                                        rung: *rung,
                                    });
                                }
                                Rung::Human => {}
                            }
                        }
                        if let Some(refresh) = check.refresh {
                            source
                                .refreshes
                                .insert((cycle, check.check), refresh.outcome);
                        }
                    }
                }
                _ => {}
            }
        }
        // The only cycle allowed to be missing records: the last one in the log, and only
        // if it was never resolved.
        let last_complete = resolved_cycles.iter().max().copied();
        let trailing = last_seen.filter(|cycle| Some(*cycle) > last_complete);
        for key in required_opinions {
            if !source.reframes.contains_key(&key) {
                if Some(key.cycle) == trailing {
                    source.incomplete_cycle = trailing;
                    continue;
                }
                return Err(ReplayError::MissingOpinion(key));
            }
        }
        for (cycle, check) in required_refreshes {
            if !source.refreshes.contains_key(&(cycle, check.clone())) {
                return Err(ReplayError::MissingRefresh { cycle, check });
            }
        }
        Ok(source)
    }

    /// A trailing cycle the log started but never resolved (a run cancelled or crashed
    /// mid-opinion). A replay should stop before it.
    #[must_use]
    pub const fn incomplete_cycle(&self) -> Option<AssessmentSeq> {
        self.incomplete_cycle
    }

    /// Compare a replayed resolution with the recorded verdicts. Empty means the replay
    /// agrees with the log. An opinion the log does not record is a divergence too.
    ///
    /// Only meaningful for a source built from a recording; a hand-scripted source has no
    /// recorded verdicts and reports every opinion as unrecorded.
    #[must_use]
    pub fn divergences(&self, outcome: &EnsembleOutcome) -> Vec<ReplayError> {
        let mut out = Vec::new();
        for record in &outcome.records {
            for keyed in &record.opinions {
                match self.verdicts.get(&keyed.key) {
                    Some(recorded) if *recorded == keyed.opinion.verdict => {}
                    Some(recorded) => out.push(ReplayError::VerdictMismatch {
                        key: keyed.key.clone(),
                        recorded: *recorded,
                        replayed: keyed.opinion.verdict,
                    }),
                    None => out.push(ReplayError::UnrecordedOpinion(keyed.key.clone())),
                }
            }
        }
        out
    }

    /// Every refresh requested, in order.
    pub fn refresh_calls(&self) -> Vec<RefreshRequest> {
        self.refresh_calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Every opinion requested, in order.
    pub fn reframe_calls(&self) -> Vec<ReframeRequest> {
        self.reframe_calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

#[async_trait]
impl OpinionSource for RecordedOpinionSource {
    async fn refresh_evidence(
        &self,
        request: &RefreshRequest,
    ) -> Result<RefreshOutcome, SourceError> {
        self.refresh_calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(request.clone());
        self.refreshes
            .get(&(request.cycle, request.check.clone()))
            .cloned()
            .ok_or_else(|| {
                SourceError::NotRecorded(format!(
                    "the refresh of {} in cycle {}",
                    request.check, request.cycle
                ))
            })
    }

    async fn reframe_same_model(
        &self,
        request: &ReframeRequest,
    ) -> Result<ReframeAnswer, SourceError> {
        self.reframe_calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(request.clone());
        self.reframes
            .get(&request.opinion)
            .cloned()
            .ok_or_else(|| SourceError::NotRecorded(format!("opinion {:?}", request.opinion)))
    }
}

#[cfg(test)]
mod tests {
    use super::{ScriptedModel, SimulatedModel};
    use crate::error::ModelError;
    use crate::ids::CheckKey;
    use crate::model::{AskContext, Budget, DecisionModel, Purpose, Question};
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
                &AskContext::new(Purpose::Assessment),
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
                &AskContext::new(Purpose::Assessment),
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
                &AskContext::new(Purpose::Assessment),
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
                    &AskContext::new(Purpose::Assessment),
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
                    &AskContext::new(Purpose::Assessment),
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
                    &AskContext::new(Purpose::Assessment),
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
                &AskContext::new(Purpose::Assessment),
                &state,
                &questions(),
                &Budget::default(),
            )
            .await?;
        let calls = model.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].context.purpose, Purpose::Assessment);
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
                &AskContext::new(Purpose::Assessment),
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
                &AskContext::new(Purpose::Assessment),
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
                &AskContext::new(Purpose::Assessment),
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
            .ask(
                &AskContext::new(Purpose::Assessment),
                &state,
                &q,
                &Budget::default(),
            )
            .await?;
        assert_eq!(
            answers.answers[&CheckKey::from("repository.instructions__agents_md_drift")].as_noul(),
            Some(0.0),
            "the check demands a decisive no when there is nothing to compare against"
        );
        Ok(())
    }
}
