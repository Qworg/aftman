//! The daemon wire schema (LLD §11) as serde types.
//!
//! One schema, two transports: NDJSON over the Unix socket (M3), HTTP/SSE (M4).
//! This module is types plus pure encode/decode helpers; it performs no I/O and
//! does not reference any model-provider type (the `jev.usage` result is an
//! opaque [`serde_json::Value`] for that reason).
//!
//! # Subscriptions
//!
//! Event pushes carry no subscription id, so overlapping subscriptions on one
//! connection would be indistinguishable duplicates. The rule is therefore **one
//! subscription per connection**: a second `subscription.open` replaces the
//! first. A client that wants two streams opens two connections.

use std::path::PathBuf;

use aftman_core::assessment::HumanDecision;
use aftman_core::event::EventEnvelope;
use aftman_core::ids::{EscalationId, EventSeq, RunId};
use aftman_core::job::FactoryStatus;
use aftman_core::run::{RunState, RunSummary};
use aftman_core::worker::SteerOutcome;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

// ---------------------------------------------------------------------------
// Bounds (hard rule 5). All are enforced by `Request::validate`, except the line
// length, which `decode_request` enforces before parsing.
// ---------------------------------------------------------------------------

/// Longest accepted request line, in bytes, newline excluded.
pub const MAX_REQUEST_LINE_BYTES: usize = 1024 * 1024;
/// Longest `run.start` job text.
pub const MAX_JOB_BYTES: usize = 256 * 1024;
/// Longest `run.steer` message.
pub const MAX_STEER_BYTES: usize = 16 * 1024;
/// Longest `run.answer` note, and longest guidance text inside a decision.
pub const MAX_ANSWER_BYTES: usize = 16 * 1024;
/// Longest short string field (worker, model, cmd, effort, paths, ...).
pub const MAX_FIELD_BYTES: usize = 4 * 1024;
/// `run.events` page size when `limit` is absent.
pub const DEFAULT_EVENTS_LIMIT: usize = 500;
/// `run.events` largest page; larger requests are clamped.
pub const MAX_EVENTS_LIMIT: usize = 5_000;
/// `run.list` page size when `limit` is absent.
pub const DEFAULT_RUN_LIST_LIMIT: usize = 50;
/// `run.list` largest page; larger requests are clamped.
pub const MAX_RUN_LIST_LIMIT: usize = 500;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// The machine-readable error code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    InvalidRepo,
    InvalidConfig,
    UnknownRun,
    WrongState,
    SteeringUnsupported,
    ModelUnavailable,
    RateLimited,
    Internal,
    /// Malformed JSON, a bad envelope, bad params, or an oversize field.
    InvalidRequest,
    UnknownMethod,
    /// Out-of-band push: this subscriber fell behind and was dropped.
    SubscriberLagged,
    /// A code this build does not know (a newer daemon). Decoding tolerates it;
    /// it re-serializes as `"unknown"`, the original string is not preserved.
    #[serde(other)]
    Unknown,
}

impl ErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidRepo => "invalid_repo",
            Self::InvalidConfig => "invalid_config",
            Self::UnknownRun => "unknown_run",
            Self::WrongState => "wrong_state",
            Self::SteeringUnsupported => "steering_unsupported",
            Self::ModelUnavailable => "model_unavailable",
            Self::RateLimited => "rate_limited",
            Self::Internal => "internal",
            Self::InvalidRequest => "invalid_request",
            Self::UnknownMethod => "unknown_method",
            Self::SubscriberLagged => "subscriber_lagged",
            Self::Unknown => "unknown",
        }
    }

    /// Whether retrying (after a pause, or by resuming) can succeed.
    #[must_use]
    pub const fn default_retryable(self) -> bool {
        matches!(
            self,
            Self::ModelUnavailable | Self::RateLimited | Self::SubscriberLagged
        )
    }
}

/// `{ code, message, retryable }`, plus optional structured `data`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, thiserror::Error)]
#[error("{}: {message}", .code.as_str())]
pub struct ProtocolError {
    pub code: ErrorCode,
    pub message: String,
    pub retryable: bool,
    /// Extra detail (for example the resume point of a lagged push). Omitted
    /// when absent, so the documented three-field shape is the common case.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl ProtocolError {
    #[must_use]
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: code.default_retryable(),
            data: None,
        }
    }

    #[must_use]
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidRequest, message)
    }

    #[must_use]
    pub fn with_data(mut self, data: Value) -> Self {
        self.data = Some(data);
        self
    }

    /// The push sent when a subscriber is dropped for being slow.
    #[must_use]
    pub fn lagged(run_id: Option<RunId>, resume_from: Option<EventSeq>) -> Self {
        let message = match resume_from {
            Some(seq) => {
                format!("subscriber fell behind and was dropped; resume from seq {seq}")
            }
            None => "subscriber fell behind and was dropped; re-subscribe".to_owned(),
        };
        Self::new(ErrorCode::SubscriberLagged, message).with_data(serde_json::json!({
            "run_id": run_id,
            "resume_from": resume_from,
        }))
    }
}

/// A request that could not be turned into a typed [`Request`].
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("{error}")]
pub struct RequestError {
    /// The request id, when one could be parsed. Echoed in the reply.
    pub id: Option<u64>,
    pub error: ProtocolError,
}

impl RequestError {
    /// The message to send back: a response when the id is known, otherwise an
    /// id-less error push.
    #[must_use]
    pub fn into_message(self) -> ServerMessage {
        match self.id {
            Some(id) => ServerMessage::Response(Response::err(id, self.error)),
            None => ServerMessage::Error(ErrorPush { error: self.error }),
        }
    }
}

// ---------------------------------------------------------------------------
// Params and results
// ---------------------------------------------------------------------------

/// Params of a method that takes none. Rejects any field.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmptyParams {}

/// `{"ok": true}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OkResult {
    pub ok: bool,
}

impl OkResult {
    pub const OK: Self = Self { ok: true };
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonStatusResult {
    pub version: String,
    pub uptime_secs: u64,
    pub pid: u32,
    pub runs_total: u64,
    pub runs_active: u64,
    /// The state directory this daemon serves.
    #[serde(default)]
    pub state_dir: Option<PathBuf>,
    /// When the daemon started (RFC 3339, from the injected clock).
    #[serde(default)]
    pub started_at: Option<String>,
    /// Free-form facts supplied by the composition root. Bounded: at most 32 entries,
    /// keys of at most 64 bytes, values of at most 256.
    #[serde(default)]
    pub info: std::collections::BTreeMap<String, String>,
}

/// Everything `aftman run` can express. Only `repo` and `job` are required.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunStartParams {
    pub repo: PathBuf,
    pub job: String,
    #[serde(default)]
    pub worker: Option<String>,
    #[serde(default)]
    pub cmd: Option<String>,
    #[serde(default)]
    pub permissions: Option<String>,
    #[serde(default)]
    pub effort: Option<String>,
    #[serde(default)]
    pub steer: bool,
    #[serde(default)]
    pub external_sandbox: bool,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub verify: Option<String>,
    #[serde(default)]
    pub ensemble: bool,
    #[serde(default)]
    pub simulate_model: bool,
}

impl RunStartParams {
    #[must_use]
    pub fn new(repo: impl Into<PathBuf>, job: impl Into<String>) -> Self {
        Self {
            repo: repo.into(),
            job: job.into(),
            worker: None,
            cmd: None,
            permissions: None,
            effort: None,
            steer: false,
            external_sandbox: false,
            model: None,
            verify: None,
            ensemble: false,
            simulate_model: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunStartResult {
    pub run_id: RunId,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunListParams {
    #[serde(default)]
    pub status: Option<Vec<FactoryStatus>>,
    #[serde(default)]
    pub limit: Option<usize>,
    /// Return runs strictly older than this id.
    #[serde(default)]
    pub cursor: Option<RunId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunListResult {
    pub runs: Vec<RunSummary>,
    /// Pass as `cursor` for the next page; `None` when this was the last.
    pub next_cursor: Option<RunId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunIdParams {
    pub run_id: RunId,
}

pub type RunGetParams = RunIdParams;
pub type RunCancelParams = RunIdParams;
pub type RunGetResult = RunState;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunEventsParams {
    pub run_id: RunId,
    /// Defaults to the first event.
    #[serde(default)]
    pub from_seq: Option<EventSeq>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunEventsResult {
    pub events: Vec<EventEnvelope>,
    /// True when more events exist past this page; continue from the last
    /// returned `seq + 1`.
    pub has_more: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionOpenParams {
    /// Omit for a live-only stream of every run.
    #[serde(default)]
    pub run_id: Option<RunId>,
    /// Requires `run_id`: sequence numbers are per run.
    #[serde(default)]
    pub from_seq: Option<EventSeq>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionOpenResult {
    pub run_id: Option<RunId>,
    /// The `seq` the stream starts at (single-run subscriptions).
    pub from_seq: Option<EventSeq>,
    /// True when this replaced an earlier subscription on the connection.
    pub replaced: bool,
    /// Whether this daemon is driving the run, so events will keep arriving. False for
    /// a run that has ended, is alive in another process (`--no-daemon`), or is
    /// non-terminal but orphaned; then a client follows it by polling `run.events`.
    /// Always true for an all-runs stream. Absent from an older daemon (decodes false).
    #[serde(default)]
    pub live: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionCloseResult {
    /// False when there was no subscription to close.
    pub closed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunSteerParams {
    pub run_id: RunId,
    pub message: String,
}

pub type RunSteerResult = SteerOutcome;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunAnswerParams {
    pub run_id: RunId,
    pub escalation_id: EscalationId,
    #[serde(deserialize_with = "strict_decision")]
    pub decision: HumanDecision,
    #[serde(default)]
    pub note: String,
}

/// `HumanDecision` lives in core and tolerates unknown fields; on the wire a typo
/// must be an error, so decode through this strict mirror and convert.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", tag = "decision", deny_unknown_fields)]
enum WireDecision {
    Accept,
    Recheck,
    Stop,
    ClaimComplete,
    Guidance { text: String },
}

fn strict_decision<'de, D: serde::Deserializer<'de>>(d: D) -> Result<HumanDecision, D::Error> {
    // Buffer first: internally tagged enums cannot see unit-variant extras
    // reliably, so check the key set explicitly as well.
    let value = Value::deserialize(d)?;
    let obj = value
        .as_object()
        .ok_or_else(|| serde::de::Error::custom("decision must be an object"))?;
    let allowed: &[&str] = match obj.get("decision").and_then(Value::as_str) {
        Some("guidance") => &["decision", "text"],
        _ => &["decision"],
    };
    if let Some(extra) = obj.keys().find(|k| !allowed.contains(&k.as_str())) {
        return Err(serde::de::Error::custom(format!(
            "unknown field {extra:?} in decision"
        )));
    }
    let wire: WireDecision = serde_json::from_value(value).map_err(serde::de::Error::custom)?;
    Ok(match wire {
        WireDecision::Accept => HumanDecision::Accept,
        WireDecision::Recheck => HumanDecision::Recheck,
        WireDecision::Stop => HumanDecision::Stop,
        WireDecision::ClaimComplete => HumanDecision::ClaimComplete,
        WireDecision::Guidance { text } => HumanDecision::Guidance { text },
    })
}

/// Delete one run's directory, or (with `all`) clear the store.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunPurgeParams {
    #[serde(default)]
    pub run_id: Option<RunId>,
    #[serde(default)]
    pub all: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunPurgeResult {
    pub purged: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponsibilityInfo {
    pub id: String,
    pub description: String,
    pub checks: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponsibilityListResult {
    pub responsibilities: Vec<ResponsibilityInfo>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JevUsageParams {
    /// RFC 3339 timestamp.
    #[serde(default)]
    pub since: Option<String>,
    #[serde(default)]
    pub run_id: Option<RunId>,
}

/// Opaque here: this crate may not name provider types.
pub type JevUsageResult = Value;

// ---------------------------------------------------------------------------
// Methods and requests
// ---------------------------------------------------------------------------

/// Every method name on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Method {
    DaemonStatus,
    DaemonStop,
    RunStart,
    RunList,
    RunGet,
    RunEvents,
    SubscriptionOpen,
    SubscriptionClose,
    RunSteer,
    RunCancel,
    RunAnswer,
    RunPurge,
    ResponsibilityList,
    JevUsage,
}

impl Method {
    pub const ALL: [Self; 14] = [
        Self::DaemonStatus,
        Self::DaemonStop,
        Self::RunStart,
        Self::RunList,
        Self::RunGet,
        Self::RunEvents,
        Self::SubscriptionOpen,
        Self::SubscriptionClose,
        Self::RunSteer,
        Self::RunCancel,
        Self::RunAnswer,
        Self::RunPurge,
        Self::ResponsibilityList,
        Self::JevUsage,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DaemonStatus => "daemon.status",
            Self::DaemonStop => "daemon.stop",
            Self::RunStart => "run.start",
            Self::RunList => "run.list",
            Self::RunGet => "run.get",
            Self::RunEvents => "run.events",
            Self::SubscriptionOpen => "subscription.open",
            Self::SubscriptionClose => "subscription.close",
            Self::RunSteer => "run.steer",
            Self::RunCancel => "run.cancel",
            Self::RunAnswer => "run.answer",
            Self::RunPurge => "run.purge",
            Self::ResponsibilityList => "responsibility.list",
            Self::JevUsage => "jev.usage",
        }
    }

    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|m| m.as_str() == name)
    }
}

/// A typed request.
#[derive(Debug, Clone, PartialEq)]
pub enum Request {
    DaemonStatus,
    DaemonStop,
    RunStart(RunStartParams),
    RunList(RunListParams),
    RunGet(RunGetParams),
    RunEvents(RunEventsParams),
    SubscriptionOpen(SubscriptionOpenParams),
    SubscriptionClose,
    RunSteer(RunSteerParams),
    RunCancel(RunCancelParams),
    RunAnswer(RunAnswerParams),
    RunPurge(RunPurgeParams),
    ResponsibilityList,
    JevUsage(JevUsageParams),
}

/// The wire shape of a request.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RequestWire {
    pub id: u64,
    pub method: &'static str,
    pub params: Value,
}

fn decode_params<T: serde::de::DeserializeOwned>(params: Value) -> Result<T, ProtocolError> {
    serde_json::from_value(params).map_err(|e| ProtocolError::invalid(format!("bad params: {e}")))
}

fn expect_empty(params: Value) -> Result<(), ProtocolError> {
    decode_params::<EmptyParams>(params).map(|_| ())
}

fn too_long(field: &str, len: usize, max: usize) -> ProtocolError {
    ProtocolError::invalid(format!("{field} is {len} bytes; the limit is {max}"))
}

fn check_len(field: &str, text: &str, max: usize) -> Result<(), ProtocolError> {
    if text.len() > max {
        Err(too_long(field, text.len(), max))
    } else {
        Ok(())
    }
}

fn check_opt(field: &str, text: Option<&String>) -> Result<(), ProtocolError> {
    text.map_or(Ok(()), |t| check_len(field, t, MAX_FIELD_BYTES))
}

/// Resolve a page size: absent gives `default`, zero is an error, larger than
/// `max` is clamped.
pub fn effective_limit(
    limit: Option<usize>,
    default: usize,
    max: usize,
) -> Result<usize, ProtocolError> {
    match limit {
        None => Ok(default),
        Some(0) => Err(ProtocolError::invalid("limit must be at least 1")),
        Some(n) => Ok(n.min(max)),
    }
}

impl Request {
    #[must_use]
    pub const fn method(&self) -> Method {
        match self {
            Self::DaemonStatus => Method::DaemonStatus,
            Self::DaemonStop => Method::DaemonStop,
            Self::RunStart(_) => Method::RunStart,
            Self::RunList(_) => Method::RunList,
            Self::RunGet(_) => Method::RunGet,
            Self::RunEvents(_) => Method::RunEvents,
            Self::SubscriptionOpen(_) => Method::SubscriptionOpen,
            Self::SubscriptionClose => Method::SubscriptionClose,
            Self::RunSteer(_) => Method::RunSteer,
            Self::RunCancel(_) => Method::RunCancel,
            Self::RunAnswer(_) => Method::RunAnswer,
            Self::RunPurge(_) => Method::RunPurge,
            Self::ResponsibilityList => Method::ResponsibilityList,
            Self::JevUsage(_) => Method::JevUsage,
        }
    }

    /// Build the wire form (client side).
    pub fn to_wire(&self, id: u64) -> Result<RequestWire, ProtocolError> {
        let enc = |v: &dyn ErasedSer| v.to_value();
        let params = match self {
            Self::DaemonStatus
            | Self::DaemonStop
            | Self::SubscriptionClose
            | Self::ResponsibilityList => Value::Object(Map::new()),
            Self::RunStart(p) => enc(p)?,
            Self::RunList(p) => enc(p)?,
            Self::RunGet(p) | Self::RunCancel(p) => enc(p)?,
            Self::RunEvents(p) => enc(p)?,
            Self::SubscriptionOpen(p) => enc(p)?,
            Self::RunSteer(p) => enc(p)?,
            Self::RunAnswer(p) => enc(p)?,
            Self::RunPurge(p) => enc(p)?,
            Self::JevUsage(p) => enc(p)?,
        };
        Ok(RequestWire {
            id,
            method: self.method().as_str(),
            params,
        })
    }

    /// Check every bound. Called by [`decode_request`]; servers building a
    /// `Request` some other way (HTTP) call it themselves.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            Self::RunStart(p) => {
                check_len("job", &p.job, MAX_JOB_BYTES)?;
                if p.job.trim().is_empty() {
                    return Err(ProtocolError::invalid("job must not be empty"));
                }
                let repo = p.repo.to_str().ok_or_else(|| {
                    ProtocolError::new(ErrorCode::InvalidRepo, "repo path is not valid UTF-8")
                })?;
                check_len("repo", repo, MAX_FIELD_BYTES)?;
                // Resolved against the daemon's cwd otherwise, which is whatever
                // directory the first client happened to start it from.
                if !p.repo.is_absolute() {
                    return Err(ProtocolError::new(
                        ErrorCode::InvalidRepo,
                        "repo must be an absolute path (the daemon's working directory is not the client's)",
                    ));
                }
                check_opt("worker", p.worker.as_ref())?;
                check_opt("cmd", p.cmd.as_ref())?;
                check_opt("permissions", p.permissions.as_ref())?;
                check_opt("effort", p.effort.as_ref())?;
                check_opt("model", p.model.as_ref())?;
                check_opt("verify", p.verify.as_ref())?;
            }
            Self::RunList(p) => {
                effective_limit(p.limit, DEFAULT_RUN_LIST_LIMIT, MAX_RUN_LIST_LIMIT)?;
            }
            Self::RunEvents(p) => {
                effective_limit(p.limit, DEFAULT_EVENTS_LIMIT, MAX_EVENTS_LIMIT)?;
            }
            Self::SubscriptionOpen(p) => {
                if p.from_seq.is_some() && p.run_id.is_none() {
                    return Err(ProtocolError::invalid(
                        "from_seq requires run_id: sequence numbers are per run",
                    ));
                }
            }
            Self::RunSteer(p) => {
                check_len("message", &p.message, MAX_STEER_BYTES)?;
                if p.message.trim().is_empty() {
                    return Err(ProtocolError::invalid("message must not be empty"));
                }
            }
            Self::RunAnswer(p) => {
                check_len("note", &p.note, MAX_ANSWER_BYTES)?;
                if let HumanDecision::Guidance { text } = &p.decision {
                    check_len("guidance text", text, MAX_ANSWER_BYTES)?;
                    if text.trim().is_empty() {
                        return Err(ProtocolError::invalid("guidance text must not be empty"));
                    }
                }
            }
            Self::RunPurge(p) => {
                if p.run_id.is_some() == p.all {
                    return Err(ProtocolError::invalid(
                        "run.purge needs exactly one of run_id or all: true",
                    ));
                }
            }
            Self::JevUsage(p) => check_opt("since", p.since.as_ref())?,
            Self::DaemonStatus
            | Self::DaemonStop
            | Self::RunGet(_)
            | Self::RunCancel(_)
            | Self::SubscriptionClose
            | Self::ResponsibilityList => {}
        }
        Ok(())
    }
}

/// Lets `to_wire` serialize heterogeneous params through one closure.
trait ErasedSer {
    fn to_value(&self) -> Result<Value, ProtocolError>;
}

impl<T: Serialize> ErasedSer for T {
    fn to_value(&self) -> Result<Value, ProtocolError> {
        serde_json::to_value(self)
            .map_err(|e| ProtocolError::new(ErrorCode::Internal, format!("encode: {e}")))
    }
}

/// A decoded request line.
#[derive(Debug, Clone, PartialEq)]
pub struct Envelope {
    pub id: u64,
    pub request: Request,
}

/// Decode and validate one request line (without its trailing newline).
pub fn decode_request(line: &str) -> Result<Envelope, RequestError> {
    let fail = |id: Option<u64>, error: ProtocolError| RequestError { id, error };
    if line.len() > MAX_REQUEST_LINE_BYTES {
        return Err(fail(
            None,
            too_long("request line", line.len(), MAX_REQUEST_LINE_BYTES),
        ));
    }
    let value: Value = serde_json::from_str(line)
        .map_err(|e| fail(None, ProtocolError::invalid(format!("malformed JSON: {e}"))))?;
    let Value::Object(mut obj) = value else {
        return Err(fail(
            None,
            ProtocolError::invalid("a request must be a JSON object"),
        ));
    };
    let id = obj.get("id").and_then(Value::as_u64);
    let Some(idv) = id else {
        return Err(fail(
            None,
            ProtocolError::invalid("request needs an unsigned integer \"id\""),
        ));
    };
    let id = Some(idv);
    if let Some(extra) = obj
        .keys()
        .find(|k| !matches!(k.as_str(), "id" | "method" | "params"))
    {
        return Err(fail(
            id,
            ProtocolError::invalid(format!("unknown request field {extra:?}")),
        ));
    }
    let Some(method) = obj.get("method").and_then(Value::as_str).map(str::to_owned) else {
        return Err(fail(
            id,
            ProtocolError::invalid("request needs a string \"method\""),
        ));
    };
    let raw_params = obj.remove("params");
    let Some(m) = Method::parse(&method) else {
        return Err(fail(
            id,
            ProtocolError::new(
                ErrorCode::UnknownMethod,
                format!("unknown method {method:?}"),
            ),
        ));
    };
    // Only an object (or null/absent, meaning "no params") is accepted; a
    // positional array would otherwise decode as a struct.
    let params = match raw_params {
        None | Some(Value::Null) => Value::Object(Map::new()),
        Some(p @ Value::Object(_)) => p,
        Some(_) => {
            return Err(fail(
                id,
                ProtocolError::invalid("params must be a JSON object"),
            ));
        }
    };
    let decoded = (|| -> Result<Request, ProtocolError> {
        Ok(match m {
            Method::DaemonStatus => expect_empty(params).map(|()| Request::DaemonStatus)?,
            Method::DaemonStop => expect_empty(params).map(|()| Request::DaemonStop)?,
            Method::SubscriptionClose => {
                expect_empty(params).map(|()| Request::SubscriptionClose)?
            }
            Method::ResponsibilityList => {
                expect_empty(params).map(|()| Request::ResponsibilityList)?
            }
            Method::RunStart => Request::RunStart(decode_params(params)?),
            Method::RunList => Request::RunList(decode_params(params)?),
            Method::RunGet => Request::RunGet(decode_params(params)?),
            Method::RunEvents => Request::RunEvents(decode_params(params)?),
            Method::SubscriptionOpen => Request::SubscriptionOpen(decode_params(params)?),
            Method::RunSteer => Request::RunSteer(decode_params(params)?),
            Method::RunCancel => Request::RunCancel(decode_params(params)?),
            Method::RunAnswer => Request::RunAnswer(decode_params(params)?),
            Method::RunPurge => Request::RunPurge(decode_params(params)?),
            Method::JevUsage => Request::JevUsage(decode_params(params)?),
        })
    })()
    .and_then(|r| r.validate().map(|()| r))
    .map_err(|e| fail(id, e))?;
    Ok(Envelope {
        id: idv,
        request: decoded,
    })
}

// ---------------------------------------------------------------------------
// Server -> client messages
// ---------------------------------------------------------------------------

/// `{ "id", "result", "error" }`. On success `error` is `null`; on failure
/// `result` is omitted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Response {
    pub id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default)]
    pub error: Option<ProtocolError>,
}

impl Response {
    pub fn ok<T: Serialize>(id: u64, result: &T) -> Result<Self, ProtocolError> {
        let value = serde_json::to_value(result)
            .map_err(|e| ProtocolError::new(ErrorCode::Internal, format!("encode: {e}")))?;
        Ok(Self {
            id,
            result: Some(value),
            error: None,
        })
    }

    #[must_use]
    pub fn err(id: u64, error: ProtocolError) -> Self {
        Self {
            id,
            result: None,
            error: Some(error),
        }
    }

    /// Client side: the typed result, or the structured error.
    pub fn into_result<T: serde::de::DeserializeOwned>(self) -> Result<T, ProtocolError> {
        if let Some(error) = self.error {
            return Err(error);
        }
        // Success is `error` being null/absent; a null (or absent) result is a
        // valid unit-like success.
        let value = self.result.unwrap_or(Value::Null);
        serde_json::from_value(value)
            .map_err(|e| ProtocolError::new(ErrorCode::Internal, format!("bad result: {e}")))
    }
}

/// `{ "event": <EventEnvelope> }`, no `id`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventPush {
    pub event: EventEnvelope,
}

/// `{ "error": {...} }`, no `id`: out of band, such as `subscriber_lagged`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrorPush {
    pub error: ProtocolError,
}

/// Anything a server sends.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum ServerMessage {
    Response(Response),
    Event(EventPush),
    Error(ErrorPush),
}

impl ServerMessage {
    /// Classify one server line: `event` key is a push, `id` a response, else an
    /// id-less error.
    pub fn decode(line: &str) -> Result<Self, ProtocolError> {
        let value: Value = serde_json::from_str(line)
            .map_err(|e| ProtocolError::invalid(format!("malformed JSON: {e}")))?;
        let bad = |e: serde_json::Error| ProtocolError::invalid(format!("bad message: {e}"));
        let obj = value
            .as_object()
            .ok_or_else(|| ProtocolError::invalid("a message must be a JSON object"))?;
        if obj.contains_key("event") {
            serde_json::from_value(value).map(Self::Event).map_err(bad)
        } else if obj.contains_key("id") {
            serde_json::from_value(value)
                .map(Self::Response)
                .map_err(bad)
        } else {
            serde_json::from_value(value).map(Self::Error).map_err(bad)
        }
    }

    #[must_use]
    pub fn event(event: EventEnvelope) -> Self {
        Self::Event(EventPush { event })
    }

    #[must_use]
    pub fn lagged(run_id: Option<RunId>, resume_from: Option<EventSeq>) -> Self {
        Self::Error(ErrorPush {
            error: ProtocolError::lagged(run_id, resume_from),
        })
    }
}

// ---------------------------------------------------------------------------
// NDJSON framing
// ---------------------------------------------------------------------------

/// Serialize to exactly one line, terminated by `\n`. JSON string escaping
/// guarantees no raw newline inside; this is asserted rather than assumed.
pub fn encode_line<T: Serialize>(message: &T) -> Result<String, ProtocolError> {
    let mut line = serde_json::to_string(message)
        .map_err(|e| ProtocolError::new(ErrorCode::Internal, format!("encode: {e}")))?;
    if line.contains('\n') {
        return Err(ProtocolError::new(
            ErrorCode::Internal,
            "encoded message contains a raw newline",
        ));
    }
    line.push('\n');
    Ok(line)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aftman_core::event::EventKind;
    use serde_json::json;
    use time::macros::datetime;

    type R = Result<(), Box<dyn std::error::Error>>;

    fn rid() -> RunId {
        RunId::new()
    }

    fn decode(v: &Value) -> Result<Envelope, RequestError> {
        decode_request(&v.to_string())
    }

    #[test]
    fn every_method_round_trips_through_the_wire() -> R {
        let run_id = rid();
        let requests = vec![
            Request::DaemonStatus,
            Request::DaemonStop,
            Request::RunStart(RunStartParams {
                worker: Some("pi".into()),
                steer: true,
                ensemble: true,
                simulate_model: true,
                external_sandbox: true,
                model: Some("m".into()),
                verify: Some("always".into()),
                cmd: Some("x {repo}".into()),
                permissions: Some("sandboxed".into()),
                effort: Some("high".into()),
                ..RunStartParams::new("/r", "do it")
            }),
            Request::RunList(RunListParams {
                status: Some(vec![FactoryStatus::Running]),
                limit: Some(5),
                cursor: Some(run_id),
            }),
            Request::RunGet(RunIdParams { run_id }),
            Request::RunEvents(RunEventsParams {
                run_id,
                from_seq: Some(EventSeq(3)),
                limit: Some(10),
            }),
            Request::SubscriptionOpen(SubscriptionOpenParams {
                run_id: Some(run_id),
                from_seq: Some(EventSeq(9)),
            }),
            Request::SubscriptionClose,
            Request::RunSteer(RunSteerParams {
                run_id,
                message: "go left".into(),
            }),
            Request::RunCancel(RunIdParams { run_id }),
            Request::RunAnswer(RunAnswerParams {
                run_id,
                escalation_id: EscalationId::new(),
                decision: HumanDecision::Guidance { text: "hi".into() },
                note: "n".into(),
            }),
            Request::RunPurge(RunPurgeParams {
                run_id: Some(run_id),
                all: false,
            }),
            Request::RunPurge(RunPurgeParams {
                run_id: None,
                all: true,
            }),
            Request::ResponsibilityList,
            Request::JevUsage(JevUsageParams {
                since: Some("2026-01-01T00:00:00Z".into()),
                run_id: Some(run_id),
            }),
        ];
        let mut seen = std::collections::HashSet::new();
        for (i, req) in requests.into_iter().enumerate() {
            seen.insert(req.method());
            let line = encode_line(&req.to_wire(i as u64)?)?;
            assert!(line.ends_with('\n'));
            let env = decode_request(line.trim_end())?;
            assert_eq!(env.id, i as u64);
            assert_eq!(env.request, req);
        }
        assert_eq!(seen.len(), Method::ALL.len(), "every method covered");
        Ok(())
    }

    #[test]
    fn results_round_trip() -> R {
        let status = DaemonStatusResult {
            version: "0.1.0".into(),
            uptime_secs: 5,
            pid: 7,
            runs_total: 3,
            runs_active: 1,
            state_dir: Some("/state".into()),
            started_at: Some("2026-01-01T00:00:00Z".into()),
            info: [("provider".to_owned(), "yes".to_owned())].into(),
        };
        let r = Response::ok(1, &status)?;
        // An older daemon's reply (no new fields) still decodes.
        let old: DaemonStatusResult = serde_json::from_value(json!({
            "version": "0", "uptime_secs": 1, "pid": 1, "runs_total": 0, "runs_active": 0
        }))?;
        assert_eq!(old.state_dir, None);
        let open: SubscriptionOpenResult =
            serde_json::from_value(json!({ "run_id": null, "from_seq": null, "replaced": false }))?;
        assert!(!open.live);
        let back: DaemonStatusResult = ServerMessage::decode(&encode_line(&r)?)?
            .into_response()?
            .into_result()?;
        assert_eq!(back, status);
        for value in [
            serde_json::to_value(RunStartResult { run_id: rid() })?,
            serde_json::to_value(OkResult::OK)?,
            serde_json::to_value(RunPurgeResult { purged: 2 })?,
            serde_json::to_value(SubscriptionCloseResult { closed: true })?,
            serde_json::to_value(SteerOutcome::Queued)?,
            serde_json::to_value(RunEventsResult {
                events: vec![],
                has_more: false,
            })?,
            serde_json::to_value(ResponsibilityListResult {
                responsibilities: vec![ResponsibilityInfo {
                    id: "core.completion".into(),
                    description: "d".into(),
                    checks: vec!["ready_to_finish".into()],
                }],
            })?,
        ] {
            let line = encode_line(&Response {
                id: 2,
                result: Some(value.clone()),
                error: None,
            })?;
            let ServerMessage::Response(resp) = ServerMessage::decode(&line)? else {
                panic!("not a response");
            };
            assert_eq!(resp.result, Some(value));
        }
        Ok(())
    }

    impl ServerMessage {
        fn into_response(self) -> Result<Response, ProtocolError> {
            match self {
                Self::Response(r) => Ok(r),
                _ => Err(ProtocolError::invalid("not a response")),
            }
        }
    }

    #[test]
    fn documented_examples_parse_and_serialize_back() -> R {
        let req = r#"{"id":1,"method":"run.start","params":{"repo":"/abs/path","job":"...","worker":"pi"}}"#;
        let env = decode_request(req)?;
        assert_eq!(env.id, 1);
        let Request::RunStart(p) = &env.request else {
            panic!("wrong method");
        };
        assert_eq!(p.worker.as_deref(), Some("pi"));
        let wire = serde_json::to_value(env.request.to_wire(1)?)?;
        assert_eq!(wire["method"], "run.start");
        assert_eq!(wire["params"]["repo"], "/abs/path");

        let run_id = "0193a000-0000-7000-8000-000000000001";
        let resp = format!(r#"{{"id":1,"result":{{"run_id":"{run_id}"}},"error":null}}"#);
        let ServerMessage::Response(r) = ServerMessage::decode(&resp)? else {
            panic!("not a response");
        };
        assert_eq!(
            serde_json::from_str::<Value>(&encode_line(&r)?)?,
            serde_json::from_str::<Value>(&resp)?
        );

        let err = r#"{"id":7,"error":{"code":"invalid_repo","message":"not a directory: /x","retryable":false}}"#;
        let ServerMessage::Response(r) = ServerMessage::decode(err)? else {
            panic!("not a response");
        };
        assert_eq!(
            r.error.as_ref().map(|e| e.code),
            Some(ErrorCode::InvalidRepo)
        );
        assert_eq!(
            serde_json::from_str::<Value>(&encode_line(&r)?)?,
            serde_json::from_str::<Value>(err)?
        );

        let ev = json!({"event":{"seq":42,"run_id":run_id,"kind":"DIRECTIVE_SELECTED",
            "at":"2026-01-01T00:00:00Z","payload":{"x":1}}});
        let ServerMessage::Event(push) = ServerMessage::decode(&ev.to_string())? else {
            panic!("not an event");
        };
        assert_eq!(push.event.seq, EventSeq(42));
        assert_eq!(serde_json::to_value(&push)?["event"]["seq"], 42);
        Ok(())
    }

    #[test]
    fn unknown_fields_are_rejected_with_the_id_echoed() {
        let err = decode(&json!({"id":4,"method":"run.get","params":{"run_id":rid(),"runid":1}}))
            .unwrap_err();
        assert_eq!(err.id, Some(4));
        assert_eq!(err.error.code, ErrorCode::InvalidRequest);
        let err = decode(&json!({"id":5,"method":"daemon.status","params":{"x":1}})).unwrap_err();
        assert_eq!(err.error.code, ErrorCode::InvalidRequest);
        let err = decode(&json!({"id":6,"method":"daemon.status","extra":1})).unwrap_err();
        assert_eq!(err.id, Some(6));
        let err = decode(&json!({"id":7,"method":"run.start","params":{"repo":"/r"}})).unwrap_err();
        assert_eq!(err.error.code, ErrorCode::InvalidRequest, "job is required");
    }

    #[test]
    fn unknown_method_echoes_the_id() -> R {
        let err = decode(&json!({"id":9,"method":"run.explode"})).unwrap_err();
        assert_eq!(err.id, Some(9));
        assert_eq!(err.error.code, ErrorCode::UnknownMethod);
        let ServerMessage::Response(r) = err.into_message() else {
            panic!("expected a response");
        };
        assert_eq!(r.id, 9);
        Ok(())
    }

    #[test]
    fn unparseable_requests_become_id_less_error_pushes() {
        for line in [
            "not json",
            "[1]",
            r#"{"method":"run.list"}"#,
            r#"{"id":"x","method":"run.list"}"#,
        ] {
            let err = decode_request(line).unwrap_err();
            assert_eq!(err.id, None, "{line}");
            assert_eq!(err.error.code, ErrorCode::InvalidRequest);
            assert!(matches!(err.into_message(), ServerMessage::Error(_)));
        }
    }

    #[test]
    fn bounds_are_enforced() {
        let huge_line = format!(
            r#"{{"id":1,"method":"daemon.status","pad":"{}"}}"#,
            "a".repeat(MAX_REQUEST_LINE_BYTES)
        );
        assert_eq!(
            decode_request(&huge_line).unwrap_err().error.code,
            ErrorCode::InvalidRequest
        );

        let big_job = json!({"id":1,"method":"run.start","params":{"repo":"/r","job":"j".repeat(MAX_JOB_BYTES + 1)}});
        assert!(decode(&big_job).is_err());
        let ok_job = json!({"id":1,"method":"run.start","params":{"repo":"/r","job":"j".repeat(MAX_JOB_BYTES)}});
        assert!(decode(&ok_job).is_ok());

        let steer = json!({"id":2,"method":"run.steer","params":{"run_id":rid(),"message":"x".repeat(MAX_STEER_BYTES + 1)}});
        assert!(decode(&steer).is_err());
        let answer = json!({"id":3,"method":"run.answer","params":{"run_id":rid(),"escalation_id":EscalationId::new(),
            "decision":{"decision":"accept"},"note":"x".repeat(MAX_ANSWER_BYTES + 1)}});
        assert!(decode(&answer).is_err());
        let guidance = json!({"id":3,"method":"run.answer","params":{"run_id":rid(),"escalation_id":EscalationId::new(),
            "decision":{"decision":"guidance","text":"x".repeat(MAX_ANSWER_BYTES + 1)}}});
        assert!(decode(&guidance).is_err());

        assert_eq!(effective_limit(None, 5, 10).ok(), Some(5));
        assert_eq!(effective_limit(Some(99), 5, 10).ok(), Some(10));
        assert!(effective_limit(Some(0), 5, 10).is_err());
        assert!(decode(&json!({"id":1,"method":"run.list","params":{"limit":0}})).is_err());
    }

    #[test]
    fn subscription_and_purge_rules() {
        let e = decode(&json!({"id":1,"method":"subscription.open","params":{"from_seq":3}}))
            .unwrap_err();
        assert_eq!(e.error.code, ErrorCode::InvalidRequest);
        assert!(decode(&json!({"id":1,"method":"subscription.open"})).is_ok());
        assert!(decode(&json!({"id":1,"method":"run.purge","params":{}})).is_err());
        assert!(
            decode(&json!({"id":1,"method":"run.purge","params":{"all":true,"run_id":rid()}}))
                .is_err()
        );
        assert!(decode(&json!({"id":1,"method":"run.purge","params":{"all":true}})).is_ok());
    }

    #[test]
    fn one_message_is_one_line_even_with_newlines_in_payloads() -> R {
        let steer = Request::RunSteer(RunSteerParams {
            run_id: rid(),
            message: "line one\nline two\r\n\"quoted\"".into(),
        });
        let line = encode_line(&steer.to_wire(1)?)?;
        assert_eq!(line.matches('\n').count(), 1);
        assert!(line.ends_with('\n'));
        assert_eq!(decode_request(line.trim_end_matches('\n'))?.request, steer);

        let event = EventEnvelope::new(
            EventSeq(1),
            rid(),
            datetime!(2026-01-01 00:00:00 UTC),
            EventKind::Notification,
            json!({"text": "a\nb"}),
        );
        let line = encode_line(&ServerMessage::event(event.clone()))?;
        assert_eq!(line.matches('\n').count(), 1);
        let ServerMessage::Event(push) = ServerMessage::decode(&line)? else {
            panic!("not an event");
        };
        assert_eq!(push.event, event);
        Ok(())
    }

    #[test]
    fn lagged_push_has_no_id_and_carries_the_resume_point() -> R {
        let run_id = rid();
        let line = encode_line(&ServerMessage::lagged(Some(run_id), Some(EventSeq(17))))?;
        let value: Value = serde_json::from_str(&line)?;
        assert!(value.get("id").is_none());
        assert_eq!(value["error"]["code"], "subscriber_lagged");
        assert_eq!(value["error"]["retryable"], true);
        assert_eq!(value["error"]["data"]["resume_from"], 17);
        let ServerMessage::Error(push) = ServerMessage::decode(&line)? else {
            panic!("not an error push");
        };
        assert_eq!(push.error.code, ErrorCode::SubscriberLagged);
        Ok(())
    }

    #[test]
    fn params_must_be_an_object() {
        for params in [
            json!(["/r", "do it"]),
            json!([null, true]),
            json!("x"),
            json!(3),
        ] {
            for method in ["run.start", "run.purge", "run.list", "daemon.status"] {
                let e = decode(&json!({"id":1,"method":method,"params":params})).unwrap_err();
                assert_eq!(e.id, Some(1));
                assert_eq!(e.error.code, ErrorCode::InvalidRequest, "{method} {params}");
            }
        }
        assert!(decode(&json!({"id":1,"method":"run.list","params":null})).is_ok());
    }

    #[test]
    fn decisions_are_strict_and_guidance_must_be_non_empty() {
        let answer = |d: Value| {
            decode(&json!({"id":1,"method":"run.answer","params":{
                "run_id": rid(), "escalation_id": EscalationId::new(), "decision": d}}))
        };
        assert!(answer(json!({"decision":"stop"})).is_ok());
        assert!(answer(json!({"decision":"claim_complete"})).is_ok());
        assert!(answer(json!({"decision":"guidance","text":"go"})).is_ok());
        for bad in [
            json!({"decision":"stop","text":"ignored"}),
            json!({"decision":"guidance","text":"go","txt":"typo"}),
            json!({"decision":"guidance"}),
            json!({"decision":"explode"}),
            json!({"decision":"guidance","text":"  "}),
            json!("stop"),
        ] {
            let e = answer(bad.clone()).unwrap_err();
            assert_eq!(e.error.code, ErrorCode::InvalidRequest, "{bad}");
        }
    }

    #[test]
    fn a_null_result_is_a_success() -> R {
        let ServerMessage::Response(r) =
            ServerMessage::decode(r#"{"id":3,"result":null,"error":null}"#)?
        else {
            panic!("not a response");
        };
        assert_eq!(r.into_result::<Option<u8>>()?, None);
        let ServerMessage::Response(r) = ServerMessage::decode(r#"{"id":3,"error":null}"#)? else {
            panic!("not a response");
        };
        r.into_result::<()>()?;
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn a_non_utf8_repo_is_invalid_repo_not_internal() {
        use std::os::unix::ffi::OsStringExt;
        let repo = PathBuf::from(std::ffi::OsString::from_vec(vec![b'/', 0xff, 0xfe]));
        let req = Request::RunStart(RunStartParams::new(repo, "job"));
        assert_eq!(req.validate().unwrap_err().code, ErrorCode::InvalidRepo);
    }

    #[test]
    fn an_unknown_error_code_from_a_newer_daemon_still_decodes() -> R {
        let line = r#"{"id":2,"error":{"code":"quota_exhausted","message":"m","retryable":true}}"#;
        let ServerMessage::Response(r) = ServerMessage::decode(line)? else {
            panic!("not a response");
        };
        let e = r.clone().into_result::<Value>().unwrap_err();
        assert_eq!(e.code, ErrorCode::Unknown);
        assert!(e.retryable);
        assert!(encode_line(&r)?.contains("\"unknown\""));
        let push = r#"{"error":{"code":"brand_new","message":"m","retryable":false}}"#;
        assert!(matches!(
            ServerMessage::decode(push)?,
            ServerMessage::Error(_)
        ));
        Ok(())
    }

    #[test]
    fn error_codes_use_their_documented_names() -> R {
        for code in [
            ErrorCode::InvalidRepo,
            ErrorCode::InvalidConfig,
            ErrorCode::UnknownRun,
            ErrorCode::WrongState,
            ErrorCode::SteeringUnsupported,
            ErrorCode::ModelUnavailable,
            ErrorCode::RateLimited,
            ErrorCode::Internal,
            ErrorCode::InvalidRequest,
            ErrorCode::UnknownMethod,
            ErrorCode::SubscriberLagged,
        ] {
            assert_eq!(serde_json::to_value(code)?, json!(code.as_str()));
        }
        Ok(())
    }
}
