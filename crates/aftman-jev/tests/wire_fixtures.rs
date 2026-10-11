//! The wire contract, pinned to recorded bytes.
//!
//! `docs/03-jev.md` promises two things about the response shape that are easy to lose
//! in a refactor: unknown answer primitives are ignored rather than fatal, and a
//! requested key that is absent *is* fatal. Both are asserted here against a file
//! rather than an inline literal, so a change to the parser has to face a realistic
//! payload.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use aftman_core::assessment::AnswerValue;
use aftman_core::clock::FixedClock;
use aftman_core::error::ModelError;
use aftman_core::ids::CheckKey;
use aftman_core::model::{AskContext, Budget, DecisionModel, Purpose, Question};
use aftman_jev::transport::{FixtureTransport, HttpResponse, Transport};
use aftman_jev::wire::{SystemOneResponse, parse_answers};
use aftman_jev::{JevSettings, SystemOneClient};

const RECORDED: &str = include_str!("fixtures/systemone_assessment.json");

/// The checks the recorded response actually answers.
fn asked() -> BTreeMap<CheckKey, Question> {
    BTreeMap::from([
        (
            CheckKey::from("core.completion__implementation_complete"),
            Question::noul("implementation complete?"),
        ),
        (
            CheckKey::from("core.completion__ready_to_finish"),
            Question::noul("ready to finish?"),
        ),
        (
            CheckKey::from("core.verification__tests_sufficient"),
            Question::noul("tests sufficient?"),
        ),
        (
            CheckKey::from("core.verification__needs_verification"),
            Question::noul("needs verification?"),
        ),
        (
            CheckKey::from("core.verification__next_step"),
            Question::choice(
                "which step?",
                [("continue", "go on"), ("verify", "check"), ("stop", "halt")],
            ),
        ),
        (
            CheckKey::from("core.worker-health__progress_level"),
            Question::score(
                "how much progress?",
                ["None", "Some", "Most", "Essentially all"],
            ),
        ),
    ])
}

#[test]
fn a_recorded_response_parses_with_its_extra_primitive_ignored()
-> Result<(), Box<dyn std::error::Error>> {
    let response: SystemOneResponse = serde_json::from_str(RECORDED)?;
    assert_eq!(response.model.as_deref(), Some("jev-2026-01-15"));
    assert_eq!(response.usage.input_tokens, Some(8123));

    let answers = parse_answers(&response, &asked())?;
    assert_eq!(answers.len(), 6, "every asked key is answered");

    // The noul values match the recording.
    assert_eq!(
        answers[&CheckKey::from("core.completion__ready_to_finish")].as_noul(),
        Some(0.21)
    );
    assert_eq!(
        answers[&CheckKey::from("core.worker-health__worker_stuck")].as_noul(),
        None,
        "stuckness was not requested here, so it is not in the result"
    );

    // The choice round-trips with its distribution.
    match &answers[&CheckKey::from("core.verification__next_step")] {
        AnswerValue::Choice {
            choice,
            confidence,
            probabilities,
        } => {
            assert_eq!(choice, "verify");
            assert!((confidence - 0.77).abs() < f32::EPSILON);
            assert!((probabilities["stop"] - 0.11).abs() < f32::EPSILON);
        }
        other => panic!("expected a choice, got {other:?}"),
    }

    // The score's string-keyed probabilities became integers, and the level is the
    // argmax. `serde_json` will not do this conversion implicitly.
    match &answers[&CheckKey::from("core.worker-health__progress_level")] {
        AnswerValue::Score {
            score,
            level,
            probabilities,
            ..
        } => {
            assert!((score - 2.4).abs() < 0.001);
            assert_eq!(*level, Some(3));
            assert!((probabilities[&2] - 0.46).abs() < f32::EPSILON);
        }
        other => panic!("expected a score, got {other:?}"),
    }

    // The two entries nobody asked for are simply absent.
    assert!(!answers.contains_key(&CheckKey::from("future__primitive_not_yet_modelled")));
    assert!(!answers.contains_key(&CheckKey::from("nobody__asked_for_this")));
    Ok(())
}

#[test]
fn asking_for_a_key_the_recording_omits_is_fatal() -> Result<(), Box<dyn std::error::Error>> {
    let response: SystemOneResponse = serde_json::from_str(RECORDED)?;
    let mut questions = asked();
    questions.insert(
        CheckKey::from("core.worker-health__worker_stuck"),
        Question::noul("stuck?"),
    );
    let error = parse_answers(&response, &questions).expect_err("absent answer");
    match error {
        ModelError::MissingAnswer(key) => {
            assert_eq!(key, "core.worker-health__worker_stuck");
        }
        other => panic!("expected MissingAnswer, got {other:?}"),
    }
    Ok(())
}

#[tokio::test]
async fn the_recorded_response_drives_a_full_client_call() -> Result<(), Box<dyn std::error::Error>>
{
    let body: serde_json::Value = serde_json::from_str(RECORDED)?;
    let transport = Arc::new(FixtureTransport::always(HttpResponse::json(200, body)));
    let settings = JevSettings {
        api_key: Some("recorded-fixture".to_owned()),
        base_url: "https://api.example.test".to_owned(),
        model: "jev-latest".to_owned(),
        timeout: Duration::from_secs(5),
        ..JevSettings::default()
    };
    let client = SystemOneClient::new(
        settings,
        Arc::clone(&transport) as Arc<dyn Transport>,
        Arc::new(FixedClock::epoch()),
    );

    let state = serde_json::json!({
        "job": { "prompt": "add retries" },
        "factory_status": { "status": "running" },
        "active_worker": { "status": "running" },
        "counters": { "attempts": 1 }
    });
    let answers = client
        .ask(
            &AskContext::new(Purpose::Assessment),
            &state,
            &asked(),
            &Budget::default(),
        )
        .await?;

    assert_eq!(
        answers.model, "jev-2026-01-15",
        "the reported model is persisted"
    );
    assert_eq!(answers.answers.len(), 6);

    // The request carried the double-underscore key form, which is what makes the
    // answer lookup work at all.
    let sent = transport.body(0).ok_or("a recorded body")?;
    assert!(
        sent["questions"]
            .as_object()
            .is_some_and(|q| q.contains_key("core.completion__ready_to_finish")),
        "the wire key must use `__`: {sent}"
    );
    Ok(())
}
