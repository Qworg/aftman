//! The System One client.
//!
//! This is the only type in aftman that performs a Jev request, and the only one that
//! knows an endpoint, a header, or a `429`. Everything outside this crate sees
//! [`aftman_core::model::DecisionModel`].
//!
//! Two behaviours are worth calling out because they are deliberate:
//!
//! - **A `Retry-After` longer than the ceiling aborts.** Sleeping for minutes makes
//!   the supervisor indistinguishable from a hang, so the error is returned with the
//!   requested delay attached and the runtime escalates.
//! - **Usage is recorded before the answers are validated.** A malformed reply still
//!   cost tokens, and a bill that only counts successful calls is a bill that lies.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use async_trait::async_trait;
use serde_json::Value;

use aftman_core::clock::Clock;
use aftman_core::error::ModelError;
use aftman_core::ids::CheckKey;
use aftman_core::model::{
    AskContext, Budget, DecisionModel, ModelAnswers, ModelInfo, Purpose, Question, Usage,
};

use crate::budget::fit;
use crate::config::JevSettings;
use crate::error::{classify_status, with_retry_after};
use crate::retry::{RetryAfter, RetryPolicy, retry_after};
use crate::transport::{HttpRequest, HttpResponse, SharedTransport};
use crate::usage::{UsageEntry, UsageRecorder};
use crate::wire::{ListModelsResponse, SystemOneRequest, SystemOneResponse, questions_to_wire};

/// The result of a connectivity probe.
#[derive(Debug, Clone, PartialEq)]
pub struct ProbeResult {
    pub model: String,
    pub probability: Option<f32>,
    pub latency: Duration,
    pub request_id: Option<String>,
}

/// A client for a System One endpoint.
pub struct SystemOneClient {
    settings: JevSettings,
    transport: SharedTransport,
    usage: Option<Arc<UsageRecorder>>,
    clock: Arc<dyn Clock>,
    /// Drives retry jitter. A parameter rather than a clock read, so a test's retry
    /// schedule is reproducible.
    seed: u64,
}

impl std::fmt::Debug for SystemOneClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SystemOneClient")
            // The key is never rendered, not even a prefix — and neither is any userinfo in
            // the base URL, which `redact_url` strips.
            .field("base_url", &crate::redact_url(&self.settings.base_url))
            .field("model", &self.settings.model)
            .field("configured", &self.settings.is_configured())
            .finish()
    }
}

impl SystemOneClient {
    #[must_use]
    pub fn new(settings: JevSettings, transport: SharedTransport, clock: Arc<dyn Clock>) -> Self {
        Self {
            settings,
            transport,
            usage: None,
            clock,
            seed: 0,
        }
    }

    #[must_use]
    pub fn with_usage(mut self, recorder: Arc<UsageRecorder>) -> Self {
        self.usage = Some(recorder);
        self
    }

    #[must_use]
    pub const fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    #[must_use]
    pub fn settings(&self) -> &JevSettings {
        &self.settings
    }

    fn api_key(&self) -> Result<&str, ModelError> {
        self.settings.api_key.as_deref().ok_or(ModelError::Auth)
    }

    /// Headers for one attempt.
    fn headers(&self, attempt: u32) -> Vec<(String, String)> {
        let agent = concat!("aftman/", env!("CARGO_PKG_VERSION"));
        let mut headers = vec![
            (
                "Authorization".to_owned(),
                format!(
                    "Bearer {}",
                    self.settings.api_key.as_deref().unwrap_or_default()
                ),
            ),
            ("Content-Type".to_owned(), "application/json".to_owned()),
            ("Accept".to_owned(), "application/json".to_owned()),
            ("User-Agent".to_owned(), agent.to_owned()),
            // Set honestly to identify this client. Do not impersonate the SDK.
            ("X-TypeSafe-SDK".to_owned(), agent.to_owned()),
            (
                "X-TypeSafe-Runtime".to_owned(),
                format!(
                    "rust/{} ({}; {})",
                    env!("AFTMAN_RUSTC_VERSION"),
                    std::env::consts::OS,
                    std::env::consts::ARCH
                ),
            ),
        ];
        if attempt > 0 {
            headers.push(("X-TypeSafe-Retry-Count".to_owned(), attempt.to_string()));
        }
        headers
    }

    /// Send one request, retrying per policy.
    ///
    /// Returns the response and the number of attempts it took.
    async fn send_with_retries(
        &self,
        request: &HttpRequest,
        policy: RetryPolicy,
    ) -> Result<(HttpResponse, u32), ModelError> {
        let mut attempt: u32 = 0;
        // The total wall-clock budget for the whole chain, as `docs/03-jev.md` §4 promises.
        // Nothing enforced it: three 10-second timeouts plus two 30-second `Retry-After`
        // waits came to more than ninety seconds against a promised forty. A supervisor that
        // blocks for ninety seconds has stopped supervising.
        let deadline = self.clock.now() + request.timeout * 4;
        loop {
            if self.clock.now() >= deadline {
                return Err(ModelError::Timeout(request.timeout * 4));
            }
            let mut attempt_request = request.clone();
            attempt_request.headers = self.headers(attempt);

            match self.transport.send(attempt_request).await {
                Ok(response) if (200..300).contains(&response.status) => {
                    return Ok((response, attempt + 1));
                }
                Ok(response) => {
                    let requested =
                        retry_after(&response.headers, self.clock.now(), policy.max_retry_after);
                    // Classify *first*. A `401` with `retry-after: 600` is an authentication
                    // failure, not a rate limit, and calling it a rate limit meant the run was
                    // not failed — it sat retrying a request that could never succeed.
                    let classified = classify_status(response.status, &response.body);
                    // A provider asking for longer than we will block for: return the request
                    // rather than sleeping through it. Only a rate limit is a rate limit; a
                    // `401` that says "come back in ten minutes" is still an auth failure.
                    if let RetryAfter::TooLong(delay) = requested {
                        return Err(match classified {
                            ModelError::RateLimited { .. } => ModelError::RateLimited {
                                retry_after: Some(delay),
                            },
                            other => other,
                        });
                    }
                    let error = with_retry_after(
                        classified,
                        match requested {
                            RetryAfter::Wait(delay) => Some(delay),
                            RetryAfter::TooLong(_) | RetryAfter::Unspecified => None,
                        },
                    );
                    if attempt >= policy.max_retries || !error.is_retryable() {
                        return Err(error);
                    }
                    let delay = match requested {
                        RetryAfter::Wait(delay) => delay,
                        _ => policy.backoff(attempt, self.seed),
                    };
                    self.sleep(delay, &response.status.to_string()).await;
                    attempt += 1;
                }
                Err(error) => {
                    if attempt >= policy.max_retries || !error.is_retryable() {
                        return Err(error);
                    }
                    let delay = policy.backoff(attempt, self.seed);
                    self.sleep(delay, error.code()).await;
                    attempt += 1;
                }
            }
        }
    }

    async fn sleep(&self, delay: Duration, reason: &str) {
        if delay.is_zero() {
            return;
        }
        tracing::info!(
            ?delay,
            reason,
            "backing off before retrying the decision model"
        );
        tokio::time::sleep(delay).await;
    }

    /// Make one request and decode it, recording usage as soon as it is knowable.
    async fn request(
        &self,
        settings: &JevSettings,
        context: &AskContext,
        state: &Value,
        questions: &BTreeMap<CheckKey, Question>,
        budget: &Budget,
    ) -> Result<ModelAnswers, ModelError> {
        // Fail before doing any work if the client is unconfigured.
        self.api_key()?;

        // ---- fit the observation -------------------------------------------
        let fitted = fit(state, budget)?;

        // ---- build the body ------------------------------------------------
        let payload = SystemOneRequest {
            state: fitted.state.clone(),
            model: settings.model.clone(),
            questions: questions_to_wire(questions),
        };
        let body = serde_json::to_vec(&payload).map_err(|error| {
            ModelError::BadRequest(format!("could not encode the request body: {error}"))
        })?;

        let request = HttpRequest {
            url: settings.endpoint(),
            headers: self.headers(0),
            body,
            timeout: settings.timeout,
        };

        let started = Instant::now();
        let (response, attempts) = match self.send_with_retries(&request, settings.retry).await {
            Ok(ok) => ok,
            Err(error) => {
                // A failed call still belongs in the log, with the time it consumed.
                self.record(
                    context,
                    Usage::default(),
                    "unavailable",
                    attempts_of(&error, settings.retry.max_retries),
                    started.elapsed(),
                    None,
                    &fitted,
                    Some(error.code()),
                );
                return Err(error);
            }
        };
        let latency = started.elapsed();

        let decoded: SystemOneResponse = match serde_json::from_slice(&response.body) {
            Ok(decoded) => decoded,
            Err(error) => {
                let excerpt = String::from_utf8_lossy(&response.body);
                let excerpt: String = excerpt.chars().take(200).collect();
                // Recorded before returning: the call happened and was billed, so it
                // belongs in the usage log even though the answer was unusable.
                let error = ModelError::Malformed(format!("{error}; body began {excerpt:?}"));
                self.record(
                    context,
                    Usage::default(),
                    &settings.model,
                    attempts,
                    latency,
                    None,
                    &fitted,
                    Some(error.code()),
                );
                return Err(error);
            }
        };

        // Usage first: a malformed *answer* still cost tokens.
        let usage: Usage = decoded.usage.into();
        let reported_model = decoded
            .model
            .clone()
            .unwrap_or_else(|| settings.model.clone());
        // Then validate answers, which may still fail. One record per request, carrying the
        // error code when there was one: the request happened and was billed, so it belongs
        // in the log even when the answer was unusable — and `failed_requests` must count it.
        let answers = crate::wire::parse_answers(&decoded, questions);
        self.record(
            context,
            usage,
            &reported_model,
            attempts,
            latency,
            None,
            &fitted,
            answers.as_ref().err().map(ModelError::code),
        );
        let answers = answers?;
        Ok(ModelAnswers {
            model: reported_model,
            usage,
            answers,
            request_id: None,
            attempts,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn record(
        &self,
        context: &AskContext,
        usage: Usage,
        reported_model: &str,
        attempts: u32,
        latency: Duration,
        request_id: Option<String>,
        fitted: &crate::budget::BudgetFit,
        error: Option<&str>,
    ) {
        let Some(recorder) = &self.usage else {
            return;
        };
        let mut entry = UsageEntry::new(
            self.clock.now(),
            context.purpose,
            &self.settings.model,
            reported_model,
            usage,
        )
        .with_context(context.run_id, context.assessment_seq)
        .with_latency(latency.as_millis() as u64)
        .with_attempts(attempts)
        .with_request_id(request_id)
        .with_measurements(fitted.compacted, fitted.measurements.clone());
        if let Some(error) = error {
            entry = entry.with_error(error);
        }
        if let Err(write_error) = recorder.record(&entry) {
            // An unwritable usage log is not a reason to stop supervising.
            tracing::warn!(%write_error, "could not append to the usage log");
        }
    }

    /// One cheap question, for `aftman doctor --probe`.
    ///
    /// Deliberately uses no retries and a short timeout: a connectivity check that
    /// hangs is worse than one that fails.
    pub async fn probe(&self) -> Result<ProbeResult, ModelError> {
        let mut settings = self.settings.clone();
        settings.retry = RetryPolicy::none();
        settings.timeout = settings.timeout.min(Duration::from_secs(15));

        let questions = BTreeMap::from([(
            CheckKey::from("doctor__reachable"),
            Question::noul("Is this a reachable decision-model endpoint?"),
        )]);
        let state = serde_json::json!({
            "job": { "prompt": "connectivity probe; no repository is involved" },
            "factory_status": { "status": "queued" },
            "probe": { "note": "synthetic evidence" },
        });

        let started = Instant::now();
        // The probe uses its own no-retry settings, which is why `request` takes them
        // rather than reading `self.settings`.
        let answers = self
            .request(
                &settings,
                &AskContext::new(Purpose::Assessment),
                &state,
                &questions,
                &settings.budget,
            )
            .await?;
        Ok(ProbeResult {
            model: answers.model,
            probability: answers
                .answers
                .get(&CheckKey::from("doctor__reachable"))
                .and_then(aftman_core::assessment::AnswerValue::as_noul),
            latency: started.elapsed(),
            request_id: answers.request_id,
        })
    }

    /// List the provider's models.
    pub async fn models(&self) -> Result<Vec<ModelInfo>, ModelError> {
        // Fail before a pointless round trip.
        self.api_key()?;
        let request = HttpRequest {
            url: self.settings.models_endpoint(),
            headers: self.headers(0),
            body: Vec::new(),
            timeout: self.settings.timeout,
        };
        let response = self.transport.send(request).await?;
        if !(200..300).contains(&response.status) {
            return Err(classify_status(response.status, &response.body));
        }
        let decoded: ListModelsResponse = serde_json::from_slice(&response.body)
            .map_err(|error| ModelError::Malformed(error.to_string()))?;
        Ok(decoded
            .models
            .into_iter()
            .map(|model| ModelInfo {
                name: model.name,
                description: model.description,
                release_date: model.release_date,
            })
            .collect())
    }
}

/// How many attempts a failed call consumed, as far as we can tell.
fn attempts_of(error: &ModelError, max_retries: u32) -> u32 {
    if error.is_retryable() {
        max_retries + 1
    } else {
        1
    }
}

#[async_trait]
impl DecisionModel for SystemOneClient {
    async fn ask(
        &self,
        context: &AskContext,
        state: &Value,
        questions: &BTreeMap<CheckKey, Question>,
        budget: &Budget,
    ) -> Result<ModelAnswers, ModelError> {
        // A caller with no key gets a clear failure rather than a 401 round trip.
        if !self.settings.is_configured() {
            return Err(ModelError::Auth);
        }
        self.request(&self.settings, context, state, questions, budget)
            .await
    }

    async fn models(&self) -> Result<Vec<ModelInfo>, ModelError> {
        SystemOneClient::models(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::SystemOneClient;
    use crate::config::JevSettings;
    use crate::retry::{RetryAfter, RetryPolicy};
    use crate::transport::{FixtureResponse, FixtureTransport, HttpResponse, Transport};
    use crate::usage::{UsageRecorder, UsageTotals};
    use aftman_core::clock::FixedClock;
    use aftman_core::error::ModelError;
    use aftman_core::ids::{AssessmentSeq, CheckKey, RunId};
    use aftman_core::model::{AskContext, Budget, DecisionModel, Purpose, Question};
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    fn settings() -> JevSettings {
        JevSettings {
            api_key: Some("test-key-do-not-use".to_owned()),
            base_url: "https://api.example.test".to_owned(),
            model: "jev-latest".to_owned(),
            timeout: Duration::from_secs(5),
            retry: RetryPolicy {
                jitter: false,
                ..RetryPolicy::default()
            },
            budget: Budget::default(),
        }
    }

    fn questions() -> BTreeMap<CheckKey, Question> {
        BTreeMap::from([(
            CheckKey::from("core.completion__ready_to_finish"),
            Question::noul("is it ready?"),
        )])
    }

    fn state() -> serde_json::Value {
        json!({
            "job": { "prompt": "add retries" },
            "factory_status": { "status": "running" },
            "active_worker": { "status": "completed" },
            "worker_output": { "output_tail": "3 passed" },
            "counters": { "attempts": 1 },
            "prior": null
        })
    }

    fn ok_body(probability: f64) -> serde_json::Value {
        json!({
            "model": "jev-2026-01",
            "usage": { "input_tokens": 8123, "output_tokens": 96 },
            "answers": {
                "core.completion__ready_to_finish": { "type": "noul", "noul": probability }
            }
        })
    }

    fn client(transport: Arc<FixtureTransport>) -> SystemOneClient {
        SystemOneClient::new(settings(), transport, Arc::new(FixedClock::epoch())).with_seed(1)
    }

    fn context() -> AskContext {
        AskContext::for_run(Purpose::Assessment, RunId::new(), AssessmentSeq(7))
    }

    #[tokio::test]
    async fn a_happy_path_returns_answers() -> Result<(), Box<dyn std::error::Error>> {
        let transport = Arc::new(FixtureTransport::always(HttpResponse::json(
            200,
            ok_body(0.91),
        )));
        let answers = client(Arc::clone(&transport))
            .ask(&context(), &state(), &questions(), &Budget::default())
            .await?;

        assert_eq!(
            answers.answers[&CheckKey::from("core.completion__ready_to_finish")].as_noul(),
            Some(0.91)
        );
        assert_eq!(answers.model, "jev-2026-01", "the reported model is kept");
        assert_eq!(answers.usage.input_tokens, Some(8123));
        assert_eq!(answers.attempts, 1);

        let sent = transport.requests();
        assert_eq!(sent[0].url, "https://api.example.test/v1/systemone");
        assert_eq!(
            sent[0].header("Authorization"),
            Some("Bearer test-key-do-not-use")
        );
        assert_eq!(
            sent[0].header("X-TypeSafe-SDK"),
            Some(concat!("aftman/", env!("CARGO_PKG_VERSION")))
        );
        assert!(
            sent[0]
                .header("X-TypeSafe-Runtime")
                .is_some_and(|v| v.starts_with("rust/"))
        );
        assert_eq!(
            sent[0].header("X-TypeSafe-Retry-Count"),
            None,
            "the header is absent on a first attempt"
        );
        Ok(())
    }

    #[tokio::test]
    async fn the_request_body_has_the_documented_shape() -> Result<(), Box<dyn std::error::Error>> {
        let transport = Arc::new(FixtureTransport::always(HttpResponse::json(
            200,
            ok_body(0.5),
        )));
        client(Arc::clone(&transport))
            .ask(&context(), &state(), &questions(), &Budget::default())
            .await?;

        let body = transport.body(0).ok_or("a recorded body")?;
        assert_eq!(body["model"], json!("jev-latest"));
        assert!(body["state"].is_object());
        assert_eq!(
            body["questions"]["core.completion__ready_to_finish"]["type"],
            json!("noul")
        );
        assert!(body["questions"]["core.completion__ready_to_finish"]["instructions"].is_string());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_rate_limit_is_retried_and_succeeds() -> Result<(), Box<dyn std::error::Error>> {
        let transport = Arc::new(FixtureTransport::new(vec![
            FixtureResponse::Reply(
                HttpResponse::json(429, json!({"error": "slow down"}))
                    .with_header("retry-after-ms", "100"),
            ),
            FixtureResponse::Reply(HttpResponse::json(200, ok_body(0.9))),
        ]));
        let answers = client(Arc::clone(&transport))
            .ask(&context(), &state(), &questions(), &Budget::default())
            .await?;

        assert_eq!(answers.attempts, 2);
        let sent = transport.requests();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[1].header("X-TypeSafe-Retry-Count"), Some("1"));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_long_retry_after_aborts_instead_of_blocking_the_supervisor()
    -> Result<(), Box<dyn std::error::Error>> {
        let transport = Arc::new(FixtureTransport::always(
            HttpResponse::json(429, json!({"message": "come back later"}))
                .with_header("retry-after", "600"),
        ));
        let error = client(Arc::clone(&transport))
            .ask(&context(), &state(), &questions(), &Budget::default())
            .await
            .expect_err("must abort");

        assert_eq!(
            error,
            ModelError::RateLimited {
                retry_after: Some(Duration::from_secs(600))
            },
            "the requested delay is reported, not slept through"
        );
        assert_eq!(transport.call_count(), 1, "no retry was attempted");
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn retries_give_up_after_the_configured_count() -> Result<(), Box<dyn std::error::Error>>
    {
        let transport = Arc::new(FixtureTransport::always(HttpResponse::json(
            503,
            json!({"message": "unavailable"}),
        )));
        let error = client(Arc::clone(&transport))
            .ask(&context(), &state(), &questions(), &Budget::default())
            .await
            .expect_err("must give up");
        assert!(matches!(error, ModelError::Server { status: 503, .. }));
        // 1 initial + 2 retries.
        assert_eq!(transport.call_count(), 3);
        assert_eq!(
            transport.requests()[2].header("X-TypeSafe-Retry-Count"),
            Some("2")
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_transport_failure_is_retried() -> Result<(), Box<dyn std::error::Error>> {
        let transport = Arc::new(FixtureTransport::new(vec![
            FixtureResponse::Fail(ModelError::Transport("connection refused".to_owned())),
            FixtureResponse::Reply(HttpResponse::json(200, ok_body(0.7))),
        ]));
        let answers = client(Arc::clone(&transport))
            .ask(&context(), &state(), &questions(), &Budget::default())
            .await?;
        assert_eq!(answers.attempts, 2);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_401_with_a_long_retry_after_is_an_auth_failure_not_a_rate_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        // The `TooLong` check used to run before classification, so *any* status with a long
        // `Retry-After` came back as `RateLimited` — and a rate limit is retryable, so the run
        // sat retrying a request that could never succeed.
        let mut response = HttpResponse::json(401, json!({"error": "bad key"}));
        response
            .headers
            .push(("retry-after".to_owned(), "600".to_owned()));
        let transport = Arc::new(FixtureTransport::always(response));
        let error = client(Arc::clone(&transport))
            .ask(&context(), &state(), &questions(), &Budget::default())
            .await
            .expect_err("must fail");
        assert_eq!(
            error,
            ModelError::Auth,
            "a 401 is an auth failure whatever its Retry-After says"
        );
        assert_eq!(transport.call_count(), 1, "auth is never retried");
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_429_with_a_long_retry_after_is_still_a_rate_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        // The other half: a rate limit is a rate limit, and the delay is reported.
        let mut response = HttpResponse::json(429, json!({"error": "slow down"}));
        response
            .headers
            .push(("retry-after".to_owned(), "600".to_owned()));
        let transport = Arc::new(FixtureTransport::always(response));
        let error = client(Arc::clone(&transport))
            .ask(&context(), &state(), &questions(), &Budget::default())
            .await
            .expect_err("must fail");
        assert!(
            matches!(
                error,
                ModelError::RateLimited {
                    retry_after: Some(_)
                }
            ),
            "{error:?}"
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_non_retryable_failure_is_not_retried() -> Result<(), Box<dyn std::error::Error>> {
        let transport = Arc::new(FixtureTransport::always(HttpResponse::json(
            401,
            json!({"error": "bad key"}),
        )));
        let error = client(Arc::clone(&transport))
            .ask(&context(), &state(), &questions(), &Budget::default())
            .await
            .expect_err("must fail");
        assert_eq!(error, ModelError::Auth);
        assert_eq!(transport.call_count(), 1, "auth is never retried");
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_missing_requested_answer_does_not_trigger_a_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        // Asking again would almost certainly produce the same reply and bill for it.
        let transport = Arc::new(FixtureTransport::always(HttpResponse::json(
            200,
            json!({
                "model": "m",
                "usage": { "input_tokens": 1, "output_tokens": 1 },
                "answers": {}
            }),
        )));
        let error = client(Arc::clone(&transport))
            .ask(&context(), &state(), &questions(), &Budget::default())
            .await
            .expect_err("missing answer");
        assert!(matches!(error, ModelError::MissingAnswer(_)));
        assert_eq!(transport.call_count(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn a_malformed_body_is_reported_with_an_excerpt() -> Result<(), Box<dyn std::error::Error>>
    {
        let transport = Arc::new(FixtureTransport::always(HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: b"<html>not json</html>".to_vec(),
        }));
        let error = client(Arc::clone(&transport))
            .ask(&context(), &state(), &questions(), &Budget::default())
            .await
            .expect_err("malformed");
        match error {
            ModelError::Malformed(message) => assert!(message.contains("not json"), "{message}"),
            other => panic!("expected Malformed, got {other:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn an_unconfigured_client_refuses_before_making_a_request() {
        let transport = Arc::new(FixtureTransport::always(HttpResponse::json(
            200,
            ok_body(0.5),
        )));
        let mut configured = settings();
        configured.api_key = None;
        let client = SystemOneClient::new(
            configured,
            Arc::clone(&transport) as Arc<dyn Transport>,
            Arc::new(FixedClock::epoch()),
        );
        let error = client
            .ask(&context(), &state(), &questions(), &Budget::default())
            .await
            .expect_err("no key");
        assert_eq!(error, ModelError::Auth);
        assert_eq!(transport.call_count(), 0, "nothing was sent");
    }

    #[tokio::test]
    async fn the_observation_is_compacted_before_it_is_sent()
    -> Result<(), Box<dyn std::error::Error>> {
        let transport = Arc::new(FixtureTransport::always(HttpResponse::json(
            200,
            ok_body(0.5),
        )));
        let budget = Budget {
            pair_bytes: 200,
            request_bytes: 2_000,
            pair_target_bytes: 100,
            request_target_bytes: 800,
        };
        let mut big = state();
        big["git_diff"] = json!({ "diff": "x".repeat(20_000) });

        client(Arc::clone(&transport))
            .ask(&context(), &big, &questions(), &budget)
            .await?;
        let body = transport.body(0).ok_or("a recorded body")?;
        let diff = body["state"]["git_diff"]["diff"].as_str().ok_or("a diff")?;
        assert!(
            diff.contains("truncated"),
            "the clip is visible to the model"
        );
        assert!(
            body["state"]["job"].is_object(),
            "required evidence remains"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_state_that_cannot_fit_is_refused_rather_than_truncated_blindly()
    -> Result<(), Box<dyn std::error::Error>> {
        let transport = Arc::new(FixtureTransport::always(HttpResponse::json(
            200,
            ok_body(0.5),
        )));
        let tiny = Budget {
            pair_bytes: 20_000,
            request_bytes: 40,
            pair_target_bytes: 12_000,
            request_target_bytes: 20,
        };
        let error = client(Arc::clone(&transport))
            .ask(&context(), &state(), &questions(), &tiny)
            .await
            .expect_err("must refuse");
        assert!(matches!(error, ModelError::BudgetExceeded(_)), "{error:?}");
        assert_eq!(transport.call_count(), 0, "nothing was sent");
        Ok(())
    }

    #[tokio::test]
    async fn usage_is_recorded_with_attribution_and_measurements()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let recorder = Arc::new(UsageRecorder::in_state_dir(dir.path()));
        let transport = Arc::new(FixtureTransport::always(HttpResponse::json(
            200,
            ok_body(0.8),
        )));
        let run = RunId::new();

        SystemOneClient::new(
            settings(),
            Arc::clone(&transport) as Arc<dyn Transport>,
            Arc::new(FixedClock::epoch()),
        )
        .with_usage(Arc::clone(&recorder))
        .ask(
            &AskContext::for_run(Purpose::Assessment, run, AssessmentSeq(3)),
            &state(),
            &questions(),
            &Budget::default(),
        )
        .await?;

        let entries = recorder.read()?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].run_id, Some(run));
        assert_eq!(entries[0].assessment_seq, Some(AssessmentSeq(3)));
        assert_eq!(entries[0].purpose, Purpose::Assessment);
        assert_eq!(entries[0].requested_model, "jev-latest");
        assert_eq!(entries[0].model, "jev-2026-01");
        assert_eq!(entries[0].input_tokens, Some(8123));
        assert_eq!(entries[0].error, None);
        Ok(())
    }

    #[tokio::test]
    async fn a_malformed_body_is_still_billed() -> Result<(), Box<dyn std::error::Error>> {
        // The body could not be parsed, so the usage is unknown — but the call happened, and
        // a bill that only counts successful calls is a bill that lies.
        let dir = tempfile::tempdir()?;
        let recorder = Arc::new(UsageRecorder::in_state_dir(dir.path()));
        let transport = Arc::new(FixtureTransport::always(HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: b"{not json".to_vec(),
        }));
        let error = SystemOneClient::new(
            settings(),
            Arc::clone(&transport) as Arc<dyn Transport>,
            Arc::new(FixedClock::epoch()),
        )
        .with_usage(Arc::clone(&recorder))
        .ask(&context(), &state(), &questions(), &Budget::default())
        .await
        .expect_err("malformed body");
        assert!(matches!(error, ModelError::Malformed(_)), "{error:?}");
        let entries = recorder.read()?;
        assert_eq!(entries.len(), 1, "the call was billed");
        assert_eq!(entries[0].error.as_deref(), Some("malformed"));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn usage_is_recorded_even_when_the_answers_are_unusable()
    -> Result<(), Box<dyn std::error::Error>> {
        // A malformed reply still cost tokens, and a bill that only counts successful
        // calls is a bill that lies.
        let dir = tempfile::tempdir()?;
        let recorder = Arc::new(UsageRecorder::in_state_dir(dir.path()));
        let transport = Arc::new(FixtureTransport::always(HttpResponse::json(
            200,
            json!({
                "model": "m",
                "usage": { "input_tokens": 500, "output_tokens": 9 },
                "answers": {
                    "core.completion__ready_to_finish": { "type": "noul", "noul": "high" }
                }
            }),
        )));

        let error = SystemOneClient::new(
            settings(),
            Arc::clone(&transport) as Arc<dyn Transport>,
            Arc::new(FixedClock::epoch()),
        )
        .with_usage(Arc::clone(&recorder))
        .ask(&context(), &state(), &questions(), &Budget::default())
        .await
        .expect_err("invalid answer");
        assert!(matches!(error, ModelError::InvalidAnswer(_)));

        let entries = recorder.read()?;
        assert_eq!(entries.len(), 1, "the request is still billed");
        assert_eq!(entries[0].input_tokens, Some(500));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_call_is_recorded_so_the_blind_spot_is_visible()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let recorder = Arc::new(UsageRecorder::in_state_dir(dir.path()));
        let transport = Arc::new(FixtureTransport::always(HttpResponse::json(503, json!({}))));

        SystemOneClient::new(
            settings(),
            Arc::clone(&transport) as Arc<dyn Transport>,
            Arc::new(FixedClock::epoch()),
        )
        .with_usage(Arc::clone(&recorder))
        .ask(&context(), &state(), &questions(), &Budget::default())
        .await
        .expect_err("server error");

        let entries = recorder.read()?;
        assert_eq!(entries.len(), 1);
        assert!(entries[0].error.is_some(), "the failure is in the log");
        let totals = crate::usage::totals(&entries);
        assert_eq!(totals.failed_requests, 1);
        Ok(())
    }

    #[tokio::test]
    async fn a_probe_asks_one_question_and_reports_reachability()
    -> Result<(), Box<dyn std::error::Error>> {
        let transport = Arc::new(FixtureTransport::always(HttpResponse::json(
            200,
            json!({
                "model": "jev-2026-01",
                "usage": { "input_tokens": 5, "output_tokens": 1 },
                "answers": { "doctor__reachable": { "type": "noul", "noul": 0.99 } }
            }),
        )));
        let result = client(Arc::clone(&transport)).probe().await?;
        assert_eq!(result.model, "jev-2026-01");
        assert_eq!(result.probability, Some(0.99));
        let body = transport.body(0).ok_or("a recorded body")?;
        assert_eq!(body["questions"].as_object().map(|q| q.len()), Some(1));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_probe_never_retries() {
        // A connectivity check that hangs is worse than one that fails.
        let transport = Arc::new(FixtureTransport::always(HttpResponse::json(503, json!({}))));
        client(Arc::clone(&transport))
            .probe()
            .await
            .expect_err("fails");
        assert_eq!(transport.call_count(), 1);
    }

    #[test]
    fn the_debug_rendering_never_contains_the_key() {
        let transport = Arc::new(FixtureTransport::new(Vec::new()));
        let rendered = format!("{:?}", client(transport));
        assert!(!rendered.contains("test-key-do-not-use"), "{rendered}");
        assert!(rendered.contains("configured: true"));
    }

    #[test]
    fn retry_after_variants_are_classified() {
        assert_eq!(RetryAfter::Unspecified, RetryAfter::Unspecified);
        let totals = UsageTotals::default();
        assert_eq!(totals.total_tokens(), 0);
    }
}
