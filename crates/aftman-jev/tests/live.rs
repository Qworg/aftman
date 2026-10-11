//! Live provider tests.
//!
//! Gated behind `AFTMAN_LIVE=1` because they need the network and a billable key, and
//! `AGENTS.md` hard rule 4 says the default suite has neither. Run them deliberately:
//!
//! ```text
//! AFTMAN_LIVE=1 TYPESAFE_API_KEY=... cargo test -p aftman-jev --test live -- --nocapture
//! ```
//!
//! They exist because a mocked transport cannot tell you that the endpoint moved, that
//! a header is rejected, or that a key is wrong. Nothing else in M1 verifies those.

use std::sync::Arc;
use std::time::Duration;

use aftman_core::clock::SystemClock;
use aftman_core::ids::CheckKey;
use aftman_core::model::{AskContext, Budget, DecisionModel, Purpose, Question};
use aftman_jev::{ReqwestTransport, SystemOneClient};

fn live_enabled() -> bool {
    std::env::var("AFTMAN_LIVE").is_ok_and(|value| value == "1" || value == "true")
}

fn settings() -> Result<aftman_jev::JevSettings, Box<dyn std::error::Error>> {
    let env_file = aftman_jev::default_env_file().map(|path| aftman_jev::load_env_file(&path));
    let settings = aftman_jev::resolve(env_file.as_ref())?;
    if !settings.is_configured() {
        return Err("AFTMAN_LIVE is set but no provider key is configured".into());
    }
    Ok(settings)
}

#[tokio::test]
async fn a_real_provider_answers_one_question() -> Result<(), Box<dyn std::error::Error>> {
    if !live_enabled() {
        eprintln!("skipping: set AFTMAN_LIVE=1 to run live provider tests");
        return Ok(());
    }
    let settings = settings()?;

    // A synthetic, obviously-not-real observation. This costs one small request and
    // does not touch a repository.
    let questions = std::collections::BTreeMap::from([(
        CheckKey::from("live_probe__tests_passed"),
        Question::noul("Does this evidence show that the tests passed?"),
    )]);
    let state = serde_json::json!({
        "test_exit_code": 1,
        "test_summary": "FAILED (failures=1)"
    });

    let client = SystemOneClient::new(
        settings.clone(),
        Arc::new(ReqwestTransport::new()),
        Arc::new(SystemClock),
    );
    let answers = client
        .ask(
            &AskContext::new(Purpose::Assessment),
            &state,
            &questions,
            &Budget::default(),
        )
        .await?;

    let probability = answers.answers[&CheckKey::from("live_probe__tests_passed")]
        .as_noul()
        .ok_or("expected a noul answer")?;
    println!(
        "live: model={} p(tests_passed)={probability:.3} tokens={:?}/{:?}",
        answers.model, answers.usage.input_tokens, answers.usage.output_tokens
    );
    // The provider is not deterministic, so this asserts the *shape* of the answer,
    // never its value. A failing-test summary should score low, but a model score is
    // not a test assertion.
    assert!((0.0..=1.0).contains(&probability));
    Ok(())
}

#[tokio::test]
async fn a_real_provider_rejects_a_bad_key() -> Result<(), Box<dyn std::error::Error>> {
    if !live_enabled() {
        eprintln!("skipping: set AFTMAN_LIVE=1 to run live provider tests");
        return Ok(());
    }
    let mut settings = settings()?;
    settings.api_key = Some("definitely-not-a-valid-key".to_owned());
    settings.retry = aftman_jev::RetryPolicy::none();
    settings.timeout = Duration::from_secs(15);

    let client = SystemOneClient::new(
        settings,
        Arc::new(ReqwestTransport::new()),
        Arc::new(SystemClock),
    );
    let questions = std::collections::BTreeMap::from([(
        CheckKey::from("live_probe__anything"),
        Question::noul("does this work?"),
    )]);
    let error = client
        .ask(
            &AskContext::new(Purpose::Assessment),
            &serde_json::json!({ "probe": true }),
            &questions,
            &Budget::default(),
        )
        .await
        .expect_err("a bad key must be rejected");

    println!("live: bad key produced {error:?}");
    assert!(
        matches!(
            error,
            aftman_core::error::ModelError::Auth | aftman_core::error::ModelError::BadRequest(_)
        ),
        "expected an auth rejection, got {error:?}"
    );
    Ok(())
}
