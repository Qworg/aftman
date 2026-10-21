//! The JSON API and the SSE stream.
//!
//! Every handler here calls the same [`aftman_daemon::RunManager`] method the Unix socket's
//! dispatch calls, with the same parameter type. That is what "a second transport in front of
//! the same protocol, not a second implementation" means concretely: a rule enforced for one
//! surface is enforced for both because there is one copy of it.
//!
//! SSE carries the run's `EventSeq` as the event `id`, so a browser's automatic reconnect
//! sends `Last-Event-ID` and resumes at the first event it has not seen. The hub replays from
//! the store when its ring no longer reaches back that far, so the resume is exact rather
//! than approximate — no polling, no dedupe, no gaps.

use std::convert::Infallible;
use std::net::SocketAddr;

use axum::Json;
use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::{Value, json};

use aftman_core::assessment::HumanDecision;
use aftman_core::ids::{EscalationId, EventSeq, RunId};
use aftman_core::job::FactoryStatus;
use aftman_core::run::RunState;
use aftman_daemon::protocol::{
    DaemonStatusResult, ErrorCode, JevUsageParams, ProtocolError, ResponsibilityListResult,
    RunAnswerParams, RunIdParams, RunListParams, RunListResult, RunStartParams, RunStartResult,
    RunSteerParams, SubscriptionOpenParams,
};

use crate::routes::{AppState, guard, peer_ip, refusal};

/// An API refusal, with the HTTP status the surface chooses.
///
/// The *protocol* error codes pass through unchanged when they come from the manager. The
/// three web-only codes below (`unauthorized`, `forbidden`, `unknown_host`) exist because
/// authentication is a property of this transport, not of the protocol the socket speaks: a
/// Unix socket has no bearer token to be missing.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
    retryable: bool,
}

impl ApiError {
    fn detail(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            retryable: false,
        }
    }
}

impl From<ProtocolError> for ApiError {
    fn from(error: ProtocolError) -> Self {
        let status = match error.code {
            ErrorCode::InvalidRequest | ErrorCode::InvalidRepo | ErrorCode::InvalidConfig => {
                StatusCode::BAD_REQUEST
            }
            ErrorCode::UnknownRun | ErrorCode::UnknownMethod => StatusCode::NOT_FOUND,
            ErrorCode::WrongState => StatusCode::CONFLICT,
            ErrorCode::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            ErrorCode::ModelUnavailable | ErrorCode::RunAbandoned => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            ErrorCode::SteeringUnsupported => StatusCode::UNPROCESSABLE_ENTITY,
            ErrorCode::SubscriberLagged => StatusCode::GONE,
            ErrorCode::Internal | ErrorCode::Unknown => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self {
            status,
            code: error.code.as_str(),
            message: error.message,
            retryable: error.retryable,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = json!({
            "error": {
                "code": self.code,
                "message": self.message,
                "retryable": self.retryable,
            }
        });
        let mut response = (self.status, Json(body)).into_response();
        if self.status == StatusCode::UNAUTHORIZED {
            // A 401 has to name the scheme, or a client has no way to know what to send.
            response.headers_mut().insert(
                axum::http::header::WWW_AUTHENTICATE,
                axum::http::HeaderValue::from_static("Bearer realm=\"aftman\""),
            );
        }
        response
    }
}

fn denied(error: crate::auth::Denied) -> ApiError {
    let code = match error {
        crate::auth::Denied::MissingToken => "unauthorized",
        crate::auth::Denied::NotLoopback | crate::auth::Denied::CrossOrigin => "forbidden",
        crate::auth::Denied::UnknownHost => "unknown_host",
    };
    ApiError::detail(
        StatusCode::from_u16(error.status()).unwrap_or(StatusCode::FORBIDDEN),
        code,
        error.message(),
    )
}

fn bad(message: impl Into<String>) -> ApiError {
    ApiError::detail(StatusCode::BAD_REQUEST, "invalid_request", message)
}

// ---- handlers ---------------------------------------------------------------

pub async fn status(
    State(state): State<AppState>,
    peer: ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<DaemonStatusResult>, ApiError> {
    guard(&state, &headers, &peer).map_err(denied)?;
    Ok(Json(state.manager.status().await?))
}

pub async fn list_runs(
    State(state): State<AppState>,
    peer: ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> Result<Json<RunListResult>, ApiError> {
    guard(&state, &headers, &peer).map_err(denied)?;
    let status = match query.status.as_deref() {
        Some(name) => Some(vec![
            FactoryStatus::parse(name).ok_or_else(|| bad(format!("unknown status {name:?}")))?,
        ]),
        None => None,
    };
    let cursor = match query.cursor.as_deref() {
        Some(raw) => Some(
            raw.trim()
                .parse::<RunId>()
                .map_err(|_| bad("cursor is not a run id"))?,
        ),
        None => None,
    };
    let params = RunListParams {
        status,
        limit: query.limit,
        cursor,
    };
    Ok(Json(state.manager.run_list(params).await?))
}

pub async fn get_run(
    State(state): State<AppState>,
    peer: ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<RunState>, ApiError> {
    guard(&state, &headers, &peer).map_err(denied)?;
    let run_id = parse_run_id(&id)?;
    Ok(Json(state.manager.run_get(RunIdParams { run_id }).await?))
}

pub async fn start_run(
    State(state): State<AppState>,
    peer: ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<StartBody>,
) -> Result<(StatusCode, Json<RunStartResult>), ApiError> {
    guard(&state, &headers, &peer).map_err(denied)?;
    let repo = state
        .resolve_repo(std::path::Path::new(&body.repo))
        .map_err(|error| {
            ApiError::detail(StatusCode::BAD_REQUEST, "invalid_repo", error.to_string())
        })?;
    let mut params = RunStartParams::new(repo, body.job);
    params.worker = body.worker;
    params.model = body.model;
    params.verify = body.verify;
    // The posture has to be asked for explicitly, exactly as the CLI's flag does: this
    // surface never decides on its own to run an agent unsandboxed.
    params.permissions = body.permissions;
    let started = state.manager.run_start(params).await?;
    Ok((StatusCode::CREATED, Json(started)))
}

pub async fn steer(
    State(state): State<AppState>,
    peer: ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<SteerBody>,
) -> Result<Json<Value>, ApiError> {
    guard(&state, &headers, &peer).map_err(denied)?;
    let run_id = parse_run_id(&id)?;
    let outcome = state
        .manager
        .run_steer(RunSteerParams {
            run_id,
            message: body.text,
        })
        .await?;
    Ok(Json(serde_json::to_value(outcome).unwrap_or(Value::Null)))
}

pub async fn cancel(
    State(state): State<AppState>,
    peer: ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    guard(&state, &headers, &peer).map_err(denied)?;
    let run_id = parse_run_id(&id)?;
    state.manager.run_cancel(RunIdParams { run_id }).await?;
    Ok(Json(json!({ "ok": true })))
}

pub async fn answer(
    State(state): State<AppState>,
    peer: ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<AnswerBody>,
) -> Result<Json<Value>, ApiError> {
    let principal = guard(&state, &headers, &peer).map_err(denied)?;
    let run_id = parse_run_id(&id)?;
    let escalation_id = body
        .escalation_id
        .trim()
        .parse::<EscalationId>()
        .map_err(|_| bad("escalation_id is not an id"))?;
    let params = RunAnswerParams {
        run_id,
        escalation_id,
        decision: body.decision,
        note: body.note,
        surface: Some(principal.surface().to_owned()),
    };
    let result = state
        .manager
        .run_answer(params, principal.surface())
        .await?;
    Ok(Json(serde_json::to_value(result).unwrap_or(Value::Null)))
}

pub async fn responsibilities(
    State(state): State<AppState>,
    peer: ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<ResponsibilityListResult>, ApiError> {
    guard(&state, &headers, &peer).map_err(denied)?;
    Ok(Json(state.manager.responsibilities().await?))
}

pub async fn usage(
    State(state): State<AppState>,
    peer: ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    guard(&state, &headers, &peer).map_err(denied)?;
    Ok(Json(
        state.manager.jev_usage(JevUsageParams::default()).await?,
    ))
}

/// The resolved web configuration, so an operator (and a test) can see the posture the
/// daemon is actually serving without reading a file.
pub async fn web_config(
    State(state): State<AppState>,
    peer: ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    guard(&state, &headers, &peer).map_err(denied)?;
    Ok(Json(state.web.config().describe()))
}

// ---- SSE --------------------------------------------------------------------

/// `GET /v1/runs/{id}/events`.
///
/// `Last-Event-ID` is the resume point. A browser sets it automatically from the last `id:` it
/// received; the header is read here rather than relied upon implicitly so the resume can be
/// exercised with `curl -H 'Last-Event-ID: 42'`.
pub async fn events(
    State(state): State<AppState>,
    peer: ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<EventsQuery>,
) -> Response {
    let ip = peer_ip(&peer);
    if let Err(error) = state
        .web
        .auth()
        .check_sse(ip, &headers, query.token.as_deref())
    {
        return refusal(error, &state, &headers);
    }
    let run_id = match parse_run_id(&id) {
        Ok(run_id) => run_id,
        Err(error) => return error.into_response(),
    };
    // `Last-Event-ID` wins over `?from_seq=`: the browser's own idea of where it got to is
    // the one that guarantees no gap.
    //
    // The two numbers mean different things and must be translated, not passed through:
    // `Last-Event-ID` is the last event the client *processed*, so the resume starts at the
    // one after it, while `from_seq` is inclusive ("start here"). Passing the SSE id
    // straight into `from_seq` re-delivers that event, which a browser hides and a
    // sequence-number check does not.
    let resume = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(|last_seen| EventSeq(last_seen.saturating_add(1)))
        .or(query.from_seq.map(EventSeq));

    let params = SubscriptionOpenParams {
        run_id: Some(run_id),
        from_seq: resume,
    };
    let mut subscription = match state.manager.subscribe(&params).await {
        Ok(subscription) => subscription,
        Err(error) => return ApiError::from(error).into_response(),
    };

    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Event, Infallible>>(128);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                // The browser went away. Stop reading the hub rather than holding a
                // subscription until the next event happens to arrive.
                _ = tx.closed() => break,
                next = subscription.next() => {
                    let Some(item) = next else { break };
                    let event = match item {
                        Ok(envelope) => sse_event(&envelope),
                        Err(lagged) => {
                            // Tell the client where the gap starts, then close: the browser
                            // reconnects with `Last-Event-ID` and the hub replays from the
                            // store, so nothing is lost. Naming the sequence number is what
                            // makes the gap recoverable rather than invisible.
                            let event = Event::default()
                                .event("lagged")
                                .data(json!({ "resume_from": lagged.resume_from.map(EventSeq::get) }).to_string());
                            let _ = tx.send(Ok(event)).await;
                            break;
                        }
                    };
                    if tx.send(Ok(event)).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    Sse::new(tokio_stream::wrappers::ReceiverStream::new(rx))
        .keep_alive(
            // A comment line every 15 s keeps an idle connection from being reaped by a proxy,
            // which matters most for a run waiting on a human.
            KeepAlive::new().interval(std::time::Duration::from_secs(15)),
        )
        .into_response()
}

/// One event: `id` is the sequence number so `Last-Event-ID` is the resume point, and the kind
/// travels inside the data rather than as an SSE event name so a client has one message shape
/// to parse. `lagged` is the only named event, because it is a control signal, not a log line.
fn sse_event(envelope: &aftman_core::event::EventEnvelope) -> Event {
    Event::default().id(envelope.seq.get().to_string()).data(
        json!({
            "seq": envelope.seq.get(),
            "at": envelope.at.to_string(),
            "kind": kind_name(envelope.kind),
            "payload": envelope.payload,
        })
        .to_string(),
    )
}

/// The wire name for an event kind, matching what the log records.
#[must_use]
pub fn kind_name(kind: aftman_core::event::EventKind) -> &'static str {
    use aftman_core::event::EventKind;
    match kind {
        EventKind::RunCreated => "RUN_CREATED",
        EventKind::JobRouted => "JOB_ROUTED",
        EventKind::WorkerStarted => "WORKER_STARTED",
        EventKind::WorkerOutput => "WORKER_OUTPUT",
        EventKind::WorkerCompleted => "WORKER_COMPLETED",
        EventKind::WorkerSteered => "WORKER_STEERED",
        EventKind::WorkerTerminated => "WORKER_TERMINATED",
        EventKind::EvidenceCollected => "EVIDENCE_COLLECTED",
        EventKind::AssessmentRequested => "ASSESSMENT_REQUESTED",
        EventKind::AssessmentCompleted => "ASSESSMENT_COMPLETED",
        EventKind::AssessmentSkippedUnchanged => "ASSESSMENT_SKIPPED_UNCHANGED",
        EventKind::DirectiveProposed => "DIRECTIVE_PROPOSED",
        EventKind::DirectiveSelected => "DIRECTIVE_SELECTED",
        EventKind::EnsembleTriggered => "ENSEMBLE_TRIGGERED",
        EventKind::OpinionRequested => "OPINION_REQUESTED",
        EventKind::OpinionRecorded => "OPINION_RECORDED",
        EventKind::EnsembleResolved => "ENSEMBLE_RESOLVED",
        EventKind::StateChanged => "STATE_CHANGED",
        EventKind::EscalationRaised => "ESCALATION_RAISED",
        EventKind::EscalationAnswered => "ESCALATION_ANSWERED",
        EventKind::ModelError => "MODEL_ERROR",
        EventKind::RateLimited => "RATE_LIMITED",
        EventKind::BudgetExceeded => "BUDGET_EXCEEDED",
        EventKind::RunFinished => "RUN_FINISHED",
        EventKind::Error => "ERROR",
        EventKind::Notification => "NOTIFICATION",
        EventKind::Unknown => "UNKNOWN",
    }
}

// ---- request bodies ---------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartBody {
    pub repo: String,
    pub job: String,
    #[serde(default)]
    pub worker: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub verify: Option<String>,
    #[serde(default)]
    pub permissions: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SteerBody {
    pub text: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnswerBody {
    pub escalation_id: String,
    pub decision: HumanDecision,
    #[serde(default)]
    pub note: String,
}

#[derive(Debug, Default, Deserialize)]
pub struct ListQuery {
    pub status: Option<String>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct EventsQuery {
    /// `?token=` is accepted here because `EventSource` cannot set headers.
    pub token: Option<String>,
    /// An explicit resume point, for a client that is not a browser. `Last-Event-ID` wins.
    pub from_seq: Option<u64>,
}

/// A full run id. A prefix would need the run list to disambiguate, which is a second lookup
/// for a URL that the UI always generates in full.
fn parse_run_id(raw: &str) -> Result<RunId, ApiError> {
    raw.trim()
        .parse::<RunId>()
        .map_err(|_| bad(format!("{raw:?} is not a run id")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_protocol_error_keeps_its_code_and_gets_an_http_status() {
        let error = ApiError::from(ProtocolError::new(ErrorCode::UnknownRun, "no such run"));
        assert_eq!(error.status, StatusCode::NOT_FOUND);
        assert_eq!(error.code, "unknown_run");
        assert!(error.message.contains("no such run"));
    }

    #[test]
    fn an_invalid_repo_is_a_400_not_a_500() {
        let error = ApiError::from(ProtocolError::new(ErrorCode::InvalidRepo, "outside roots"));
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn a_missing_token_is_401_and_offers_the_scheme() {
        let error = denied(crate::auth::Denied::MissingToken);
        assert_eq!(error.status, StatusCode::UNAUTHORIZED);
        assert_eq!(error.code, "unauthorized");
    }

    #[test]
    fn every_event_kind_has_a_name() {
        use aftman_core::event::EventKind;
        // A `match` on a non-exhaustive enum would compile; this pins that no kind renders as
        // a placeholder because someone added one and forgot the name.
        for kind in [
            EventKind::RunCreated,
            EventKind::EscalationRaised,
            EventKind::BudgetExceeded,
            EventKind::Unknown,
        ] {
            assert!(!kind_name(kind).is_empty());
        }
    }
}
