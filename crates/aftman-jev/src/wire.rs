//! The System One wire contract.
//!
//! These types mirror `POST /v1/systemone` exactly and are the only place in aftman
//! that names a provider's JSON shape. They are deliberately **tolerant on the way
//! in and strict on the way out**:
//!
//! - responses ignore unknown fields, so the provider can add answer primitives
//!   without breaking us;
//! - an answer whose `type` we do not recognise is dropped with a warning rather
//!   than failing the whole response — the provider adds primitives over time;
//! - a *requested* key with no answer is fatal, because supervising with a missing
//!   dimension silently reads as "no signal" instead of "unknown".
//!
//! See `docs/03-jev.md` §2 and §3.

use std::collections::BTreeMap;

use aftman_core::assessment::AnswerValue;
use aftman_core::error::ModelError;
use aftman_core::ids::CheckKey;
use aftman_core::model::{Question, Usage};
use serde::Serialize;
use serde_json::Value;

/// The assessment endpoint.
pub const SYSTEM_ONE_PATH: &str = "/v1/systemone";
/// The model-listing endpoint.
pub const MODELS_PATH: &str = "/v1/models";

/// A request body.
#[derive(Debug, Clone, Serialize)]
pub struct SystemOneRequest {
    pub state: Value,
    pub model: String,
    pub questions: BTreeMap<String, Value>,
}

/// A response body.
///
/// `answers` is kept as raw [`Value`] per key so that per-answer dispatch can warn
/// and skip an unknown primitive instead of failing the decode of the whole body.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct SystemOneResponse {
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub usage: UsageWire,
    #[serde(default)]
    pub answers: BTreeMap<String, Value>,
}

/// Token counts, when the provider reports them.
#[derive(Debug, Clone, Copy, Default, serde::Deserialize)]
pub struct UsageWire {
    #[serde(default)]
    pub input_tokens: Option<u64>,
    #[serde(default)]
    pub output_tokens: Option<u64>,
}

impl From<UsageWire> for Usage {
    fn from(value: UsageWire) -> Self {
        Self {
            input_tokens: value.input_tokens,
            output_tokens: value.output_tokens,
        }
    }
}

/// The model list.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ListModelsResponse {
    #[serde(default)]
    pub models: Vec<ModelWire>,
}

/// One model's metadata.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ModelWire {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub release_date: String,
}

/// Render a check's instructions for the wire.
///
/// A check may carry a small JSON rubric as its instruction text. Sending it as a
/// structured value rather than a quoted string is what the provider's `instructions`
/// field is for, so an instruction that parses as an object or array is sent as one.
/// Anything else is sent as a plain string.
#[must_use]
pub fn json_content(text: &str) -> Value {
    let trimmed = text.trim_start();
    if (trimmed.starts_with('{') || trimmed.starts_with('['))
        && let Ok(parsed) = serde_json::from_str::<Value>(trimmed)
        && (parsed.is_object() || parsed.is_array())
    {
        return parsed;
    }
    Value::String(text.to_owned())
}

/// Build the wire question map from aftman's questions.
#[must_use]
pub fn questions_to_wire(questions: &BTreeMap<CheckKey, Question>) -> BTreeMap<String, Value> {
    questions
        .iter()
        .map(|(key, question)| {
            let value = match question {
                Question::Noul { instructions } => serde_json::json!({
                    "type": "noul",
                    "instructions": json_content(instructions),
                }),
                Question::Choice {
                    instructions,
                    criteria,
                } => {
                    let criteria: BTreeMap<&str, Value> = criteria
                        .iter()
                        .map(|(name, description)| (name.as_str(), json_content(description)))
                        .collect();
                    serde_json::json!({
                        "type": "choice",
                        "instructions": json_content(instructions),
                        "criteria": criteria,
                    })
                }
                Question::Score {
                    instructions,
                    criteria,
                } => serde_json::json!({
                    "type": "score",
                    "instructions": json_content(instructions),
                    "criteria": criteria.iter().map(|c| json_content(c)).collect::<Vec<_>>(),
                }),
            };
            (key.to_string(), value)
        })
        .collect()
}

/// Parse and validate the answers to a request.
///
/// Every requested key must be present. Unknown answer types and unrequested keys are
/// dropped (the first with a warning, the second silently).
pub fn parse_answers(
    response: &SystemOneResponse,
    questions: &BTreeMap<CheckKey, Question>,
) -> Result<BTreeMap<CheckKey, AnswerValue>, ModelError> {
    let mut parsed = BTreeMap::new();
    for (key, question) in questions {
        let Some(raw) = response.answers.get(key.as_str()) else {
            return Err(ModelError::MissingAnswer(key.to_string()));
        };
        match parse_answer(raw, question) {
            Ok(Some(answer)) => {
                parsed.insert(key.clone(), answer);
            }
            // A key we *asked about*, answered with a type this client does not model, is
            // not an additive change — it is the provider renaming a primitive. Silently
            // dropping it leaves every check without a probability and the run looking
            // healthy while supervision is blind, so it is as fatal as a missing answer.
            // (Answers we did not ask for are still ignored; see `an_unrequested_answer_is_ignored`.)
            Ok(None) => {
                return Err(ModelError::InvalidAnswer(format!(
                    "{key}: the provider returned an answer type this client does not model"
                )));
            }
            Err(detail) => return Err(ModelError::InvalidAnswer(format!("{key}: {detail}"))),
        }
    }
    Ok(parsed)
}

/// Parse one answer value against its question.
///
/// `Ok(None)` means "a type we do not know" — forward compatibility. `Err` means a
/// known type with an unusable value.
fn parse_answer(raw: &Value, question: &Question) -> Result<Option<AnswerValue>, String> {
    let kind = raw.get("type").and_then(Value::as_str).unwrap_or("");
    match kind {
        "noul" => {
            let value = raw
                .get("noul")
                .and_then(Value::as_f64)
                .ok_or_else(|| "noul is missing or not a number".to_owned())?;
            if !value.is_finite() {
                return Err(format!("noul is not finite: {value}"));
            }
            // Tolerate a hair of float overshoot, but not a real out-of-range value.
            let mut value = value;
            if !(-0.001..=1.001).contains(&value) {
                return Err(format!("noul is out of range: {value}"));
            }
            if !(0.0..=1.0).contains(&value) {
                tracing::warn!(value, "clamping a noul just outside 0..=1");
                value = value.clamp(0.0, 1.0);
            }
            if !matches!(question, Question::Noul { .. }) {
                return Err("provider answered a noul for a non-noul question".to_owned());
            }
            Ok(Some(AnswerValue::Noul(value as f32)))
        }
        "choice" => {
            let Question::Choice { criteria, .. } = question else {
                return Err("provider answered a choice for a non-choice question".to_owned());
            };
            let choice = raw
                .get("choice")
                .and_then(Value::as_str)
                .ok_or_else(|| "choice is missing or not a string".to_owned())?
                .to_owned();
            if !criteria.contains_key(&choice) {
                return Err(format!(
                    "selection {choice:?} is not one of the requested criteria"
                ));
            }
            let probabilities = number_map(raw.get("probabilities"), "probabilities")?;
            let confidence = raw
                .get("confidence")
                .and_then(Value::as_f64)
                .unwrap_or_default() as f32;
            Ok(Some(AnswerValue::Choice {
                choice,
                probabilities,
                confidence,
            }))
        }
        "score" => {
            let Question::Score { criteria, .. } = question else {
                return Err("provider answered a score for a non-score question".to_owned());
            };
            let score = raw
                .get("score")
                .and_then(Value::as_f64)
                .ok_or_else(|| "score is missing or not a number".to_owned())?;
            let max = (criteria.len().saturating_sub(1)) as f64;
            if !(0.0..=max).contains(&score) {
                return Err(format!("score {score} is outside the rubric 0..={max}"));
            }
            // Keys arrive as JSON *strings*; `serde_json` will not turn them into
            // `u32` for us, so convert explicitly and reject a non-numeric key.
            let probabilities = indexed_number_map(raw.get("probabilities"))?;
            if probabilities
                .keys()
                .any(|level| *level as usize >= criteria.len())
            {
                return Err(format!(
                    "probabilities has a key outside the rubric 0..={}",
                    criteria.len().saturating_sub(1)
                ));
            }
            // The most probable level, not the highest key present: the provider sends the
            // whole distribution, so `keys().max()` was always the last level whatever the
            // probabilities said. This value is persisted and replayed, so a wrong level is a
            // wrong record rather than only a wrong display. Ties go to the lower level.
            let level = probabilities
                .iter()
                .max_by(|a, b| a.1.total_cmp(b.1).then(b.0.cmp(a.0)))
                .map(|(level, _)| *level);
            let confidence = raw
                .get("confidence")
                .and_then(Value::as_f64)
                .unwrap_or_default() as f32;
            Ok(Some(AnswerValue::Score {
                score: score as f32,
                level,
                confidence,
                probabilities,
            }))
        }
        "" => Err("answer has no type".to_owned()),
        // Forward compatibility: a primitive this client does not model yet.
        _ => Ok(None),
    }
}

fn number_map(raw: Option<&Value>, field: &str) -> Result<BTreeMap<String, f32>, String> {
    let Some(object) = raw.and_then(Value::as_object) else {
        return Ok(BTreeMap::new());
    };
    let mut out = BTreeMap::new();
    for (key, value) in object {
        let number = value
            .as_f64()
            .ok_or_else(|| format!("{field}.{key} is not a number"))?;
        // A probability is a probability: NaN, infinity, and 1e39 (which becomes `inf` as
        // f32) must not reach a check's arithmetic.
        if !number.is_finite() {
            return Err(format!("{field}.{key} is not a finite number"));
        }
        out.insert(key.clone(), number as f32);
    }
    Ok(out)
}

fn indexed_number_map(raw: Option<&Value>) -> Result<BTreeMap<u32, f32>, String> {
    let Some(object) = raw.and_then(Value::as_object) else {
        return Ok(BTreeMap::new());
    };
    let mut out = BTreeMap::new();
    for (key, value) in object {
        let index: u32 = key
            .parse()
            .map_err(|_| format!("probabilities key {key:?} is not a rubric level"))?;
        let number = value
            .as_f64()
            .ok_or_else(|| format!("probabilities.{key} is not a number"))?;
        if !number.is_finite() {
            return Err(format!("probabilities.{key} is not a finite number"));
        }
        out.insert(index, number as f32);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::{SystemOneResponse, UsageWire, json_content, parse_answers, questions_to_wire};
    use aftman_core::assessment::AnswerValue;
    use aftman_core::error::ModelError;
    use aftman_core::ids::CheckKey;
    use aftman_core::model::Question;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn key(name: &str) -> CheckKey {
        CheckKey::from(name)
    }

    fn noul_questions() -> BTreeMap<CheckKey, Question> {
        BTreeMap::from([(
            key("core.completion__ready_to_finish"),
            Question::noul("is it ready?"),
        )])
    }

    fn response(answers: serde_json::Value) -> Result<SystemOneResponse, serde_json::Error> {
        serde_json::from_value(json!({
            "model": "jev-test",
            "usage": { "input_tokens": 10, "output_tokens": 2 },
            "answers": answers,
        }))
    }

    #[test]
    fn a_plain_instruction_is_sent_as_a_string() {
        assert_eq!(json_content("is it done?"), json!("is it done?"));
    }

    #[test]
    fn a_structured_instruction_is_sent_as_structure() {
        // The provider's `instructions` field accepts a JSON object or array, so a
        // check that carries a rubric is not forced into a quoted string.
        let value = json_content(r#"{ "question": "done?", "rubric": ["no", "yes"] }"#);
        assert_eq!(value["question"], json!("done?"));
        assert!(value["rubric"].is_array());
    }

    #[test]
    fn text_that_only_looks_like_json_stays_text() {
        assert_eq!(json_content("{not json at all"), json!("{not json at all"));
        assert_eq!(json_content("  {\"a\":1}"), json!({"a": 1}));
    }

    #[test]
    fn questions_serialize_with_the_documented_shape() {
        let questions = noul_questions();
        let wire = questions_to_wire(&questions);
        let entry = &wire["core.completion__ready_to_finish"];
        assert_eq!(entry["type"], json!("noul"));
        assert_eq!(entry["instructions"], json!("is it ready?"));
    }

    #[test]
    fn a_score_question_carries_its_ordered_rubric() {
        let questions = BTreeMap::from([(
            key("core.worker-health__progress_level"),
            Question::score("how much?", ["none", "some", "most"]),
        )]);
        let wire = questions_to_wire(&questions);
        let entry = &wire["core.worker-health__progress_level"];
        assert_eq!(entry["type"], json!("score"));
        assert_eq!(entry["criteria"], json!(["none", "some", "most"]));
    }

    #[test]
    fn a_valid_noul_parses() -> Result<(), Box<dyn std::error::Error>> {
        let questions = noul_questions();
        let response = response(json!({
            "core.completion__ready_to_finish": { "type": "noul", "noul": 0.91 }
        }))?;
        let answers = parse_answers(&response, &questions)?;
        assert_eq!(
            answers[&key("core.completion__ready_to_finish")].as_noul(),
            Some(0.91)
        );
        Ok(())
    }

    #[test]
    fn a_missing_requested_answer_is_fatal() -> Result<(), Box<dyn std::error::Error>> {
        // Absent must never read as "no signal": that silently loses a dimension.
        let questions = noul_questions();
        let response = response(json!({}))?;
        let error = parse_answers(&response, &questions).expect_err("missing answer");
        assert!(matches!(error, ModelError::MissingAnswer(_)), "{error:?}");
        Ok(())
    }

    #[test]
    fn an_unknown_answer_type_for_a_requested_key_is_fatal()
    -> Result<(), Box<dyn std::error::Error>> {
        // The provider renaming a primitive is a *subtractive* change. Dropping the answer
        // would answer no check and look healthy; extra types the provider adds for questions
        // nobody asked are still ignored, which is what additive tolerance means.
        let questions = BTreeMap::from([
            (
                key("core.completion__ready_to_finish"),
                Question::noul("ready?"),
            ),
            (
                key("core.worker-health__worker_stuck"),
                Question::noul("stuck?"),
            ),
        ]);
        let response = response(json!({
            "core.completion__ready_to_finish": { "type": "noul", "noul": 0.9 },
            "core.worker-health__worker_stuck": { "type": "distribution", "pdf": [0.1, 0.9] }
        }))?;
        let error = parse_answers(&response, &questions).expect_err("an unusable answer is fatal");
        assert!(matches!(error, ModelError::InvalidAnswer(_)), "{error:?}");
        Ok(())
    }

    #[test]
    fn an_unrequested_answer_is_ignored() -> Result<(), Box<dyn std::error::Error>> {
        let questions = noul_questions();
        let response = response(json!({
            "core.completion__ready_to_finish": { "type": "noul", "noul": 0.5 },
            "something__else": { "type": "noul", "noul": 0.5 }
        }))?;
        let answers = parse_answers(&response, &questions)?;
        assert_eq!(answers.len(), 1);
        Ok(())
    }

    #[test]
    fn a_non_numeric_noul_is_an_error() -> Result<(), Box<dyn std::error::Error>> {
        let questions = noul_questions();
        let response = response(json!({
            "core.completion__ready_to_finish": { "type": "noul", "noul": "high" }
        }))?;
        assert!(matches!(
            parse_answers(&response, &questions),
            Err(ModelError::InvalidAnswer(_))
        ));
        Ok(())
    }

    #[test]
    fn an_out_of_range_noul_is_an_error_but_a_slight_overshoot_is_clamped()
    -> Result<(), Box<dyn std::error::Error>> {
        let questions = noul_questions();
        let way_out = response(json!({
            "core.completion__ready_to_finish": { "type": "noul", "noul": 1.8 }
        }))?;
        assert!(matches!(
            parse_answers(&way_out, &questions),
            Err(ModelError::InvalidAnswer(_))
        ));

        let hair_over = response(json!({
            "core.completion__ready_to_finish": { "type": "noul", "noul": 1.0000001 }
        }))?;
        let answers = parse_answers(&hair_over, &questions)?;
        assert_eq!(
            answers[&key("core.completion__ready_to_finish")].as_noul(),
            Some(1.0)
        );
        Ok(())
    }

    #[test]
    fn a_score_parses_its_string_keyed_probabilities_and_derives_the_level()
    -> Result<(), Box<dyn std::error::Error>> {
        // The provider sends `{"0": 0.1, "1": 0.9}`. `serde_json` will not build a
        // `BTreeMap<u32, _>` from string keys, so this conversion is explicit.
        let questions = BTreeMap::from([(
            key("core.worker-health__progress_level"),
            Question::score("how much?", ["none", "some", "most", "all"]),
        )]);
        let response = response(json!({
            "core.worker-health__progress_level": {
                "type": "score",
                "score": 2.4,
                "confidence": 0.81,
                "legend": { "0": "none", "1": "some", "2": "most", "3": "all" },
                "probabilities": { "0": 0.03, "1": 0.11, "2": 0.46, "3": 0.40 }
            }
        }))?;
        let answers = parse_answers(&response, &questions)?;
        match &answers[&key("core.worker-health__progress_level")] {
            AnswerValue::Score {
                probabilities,
                level,
                ..
            } => {
                assert_eq!(probabilities[&2], 0.46);
                // `{2: 0.46, 3: 0.40}` — the argmax is 2. This asserted 3, the highest key
                // present, while its message claimed to be testing the argmax.
                assert_eq!(*level, Some(2), "level is the argmax, derived locally");
            }
            other => panic!("expected a score, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn a_non_numeric_probability_key_is_an_error() -> Result<(), Box<dyn std::error::Error>> {
        let questions = BTreeMap::from([(
            key("core.worker-health__progress_level"),
            Question::score("how much?", ["none", "some"]),
        )]);
        let response = response(json!({
            "core.worker-health__progress_level": {
                "type": "score",
                "score": 1.0,
                "probabilities": { "low": 1.0 }
            }
        }))?;
        assert!(matches!(
            parse_answers(&response, &questions),
            Err(ModelError::InvalidAnswer(_))
        ));
        Ok(())
    }

    #[test]
    fn a_score_outside_its_rubric_is_an_error() -> Result<(), Box<dyn std::error::Error>> {
        let questions = BTreeMap::from([(
            key("core.worker-health__progress_level"),
            Question::score("how much?", ["none", "some"]),
        )]);
        let response = response(json!({
            "core.worker-health__progress_level": { "type": "score", "score": 9.0 }
        }))?;
        assert!(matches!(
            parse_answers(&response, &questions),
            Err(ModelError::InvalidAnswer(_))
        ));
        Ok(())
    }

    #[test]
    fn a_choice_outside_its_criteria_is_an_error() -> Result<(), Box<dyn std::error::Error>> {
        let questions = BTreeMap::from([(
            key("core.verification__next_step"),
            Question::choice("which?", [("continue", "keep going"), ("stop", "halt")]),
        )]);
        let response = response(json!({
            "core.verification__next_step": {
                "type": "choice",
                "choice": "escalate",
                "confidence": 0.9,
                "probabilities": { "escalate": 0.9 }
            }
        }))?;
        assert!(matches!(
            parse_answers(&response, &questions),
            Err(ModelError::InvalidAnswer(_))
        ));
        Ok(())
    }

    #[test]
    fn a_valid_choice_parses() -> Result<(), Box<dyn std::error::Error>> {
        let questions = BTreeMap::from([(
            key("core.verification__next_step"),
            Question::choice("which?", [("continue", "keep going"), ("stop", "halt")]),
        )]);
        let response = response(json!({
            "core.verification__next_step": {
                "type": "choice",
                "choice": "continue",
                "confidence": 0.83,
                "probabilities": { "continue": 0.83, "stop": 0.17 }
            }
        }))?;
        let answers = parse_answers(&response, &questions)?;
        match &answers[&key("core.verification__next_step")] {
            AnswerValue::Choice {
                choice,
                confidence,
                probabilities,
            } => {
                assert_eq!(choice, "continue");
                assert_eq!(*confidence, 0.83);
                assert_eq!(probabilities["stop"], 0.17);
            }
            other => panic!("expected a choice, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn a_guard_rail_answer_does_not_block_logging() -> Result<(), Box<dyn std::error::Error>> {
        // `answers` is raw JSON, so a decoded response still exposes usage even when
        // an individual answer later turns out to be unusable. Usage is recorded
        // before validation precisely so that a malformed reply still shows up in the
        // bill.
        let response: SystemOneResponse = serde_json::from_value(json!({
            "usage": { "input_tokens": 8123, "output_tokens": 96 },
            "answers": { "core.completion__ready_to_finish": { "type": "noul", "noul": "high" } }
        }))?;
        assert_eq!(response.usage.input_tokens, Some(8123));
        assert_eq!(response.usage.output_tokens, Some(96));
        assert!(
            response.model.is_none(),
            "a missing model is tolerated at decode"
        );
        Ok(())
    }

    #[test]
    fn usage_wire_converts_and_totals() {
        let usage: UsageWire = UsageWire {
            input_tokens: Some(10),
            output_tokens: Some(3),
        };
        let usage: aftman_core::model::Usage = usage.into();
        assert_eq!(usage.total(), Some(13));
    }
}
