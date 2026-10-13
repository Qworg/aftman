//! A typed NDJSON client for the daemon socket (`docs/02-lld.md` section 11).
//!
//! Two pieces, both meant to be reused by the TUI:
//!
//! - [`Client`] is request/response: one method per protocol method, each returning
//!   the typed result or a [`ClientError`] that carries the daemon's
//!   [`ProtocolError`] (code, message, retryable) unchanged.
//! - [`Follow`] is one run's event stream. It owns a **dedicated connection** (one
//!   subscription per connection is the protocol rule) and recovers by itself: a
//!   `subscriber_lagged` push re-opens the subscription from `last_seen + 1`, a
//!   dropped connection is re-established (bounded attempts, exponential backoff) and
//!   resumed from `last_seen + 1`. It checks `seq` contiguity on every event and
//!   treats a gap or a duplicate as an error, never as something to paper over.
//!
//! Every connect and every request/response exchange has a timeout, so a wedged
//! daemon is an error and not a hang. The event stream itself is deliberately not
//! timed: a run can be silent for as long as a worker thinks.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::de::DeserializeOwned;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::time::timeout;

use aftman_core::assessment::HumanDecision;
use aftman_core::event::EventEnvelope;
use aftman_core::ids::{EscalationId, EventSeq, RunId};
use aftman_core::run::RunState;
use aftman_core::worker::SteerOutcome;
use aftman_daemon::protocol::{
    DaemonStatusResult, JevUsageParams, OkResult, ProtocolError, Request, Response,
    ResponsibilityListResult, RunEventsParams, RunEventsResult, RunIdParams, RunListParams,
    RunListResult, RunPurgeParams, RunPurgeResult, RunStartParams, RunStartResult, RunSteerParams,
    ServerMessage, SubscriptionCloseResult, SubscriptionOpenParams, SubscriptionOpenResult,
    encode_line,
};

/// Longest server line a client will buffer. Generous (a `run.get` of a long run is
/// large) but finite.
pub const MAX_SERVER_LINE_BYTES: usize = 64 * 1024 * 1024;

/// Timeouts and reconnect bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientConfig {
    /// How long one `connect` may take.
    pub connect_timeout: Duration,
    /// How long one request may wait for its response.
    pub request_timeout: Duration,
    /// How many times [`Follow`] tries to re-establish a lost stream before it gives
    /// up and reports the error.
    pub reconnect_attempts: u32,
    /// First backoff between re-establish attempts; doubles up to `reconnect_max_backoff`.
    pub reconnect_backoff: Duration,
    pub reconnect_max_backoff: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(3),
            request_timeout: Duration::from_secs(30),
            reconnect_attempts: 8,
            reconnect_backoff: Duration::from_millis(100),
            reconnect_max_backoff: Duration::from_secs(2),
        }
    }
}

/// Why a client call failed.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// Nothing is listening: the socket is absent or stale.
    #[error("the aftman daemon is not running (no listener at {})", .0.display())]
    NotRunning(PathBuf),

    #[error("cannot connect to {}: {source}", path.display())]
    Connect {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("timed out after {0:?} connecting to the daemon")]
    ConnectTimeout(Duration),

    #[error("the daemon did not answer `{method}` within {after:?}")]
    Timeout {
        method: &'static str,
        after: Duration,
    },

    /// The daemon answered with an error. Code, message and `retryable` intact.
    #[error("{0}")]
    Protocol(ProtocolError),

    #[error("connection to the daemon failed: {0}")]
    Io(#[from] std::io::Error),

    #[error("the daemon closed the connection")]
    Closed,

    #[error("the daemon sent something unintelligible: {0}")]
    Malformed(String),

    /// The event stream broke its contract. Never retried: a client that papered
    /// over this would show a history with a hole in it.
    #[error("event stream out of sequence: expected seq {expected}, received {got}")]
    Sequence { expected: u64, got: u64 },

    #[error(
        "lost the event stream and could not re-establish it after {attempts} attempts: {last}"
    )]
    Reconnect { attempts: u32, last: String },

    /// A call was made on a connection that already timed out or failed.
    #[error("this connection is no longer usable ({0}); connect again")]
    Poisoned(String),
}

impl ClientError {
    /// The daemon's structured error, when that is what this is.
    #[must_use]
    pub fn protocol(&self) -> Option<&ProtocolError> {
        match self {
            Self::Protocol(error) => Some(error),
            _ => None,
        }
    }

    /// True when the daemon is simply not there (as opposed to unhealthy).
    #[must_use]
    pub fn is_not_running(&self) -> bool {
        matches!(self, Self::NotRunning(_))
    }

    /// Whether trying again could help: a lost connection, a timeout, or a daemon
    /// error it flagged retryable.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Protocol(error) => error.retryable,
            Self::Io(_)
            | Self::Closed
            | Self::ConnectTimeout(_)
            | Self::Timeout { .. }
            | Self::Connect { .. }
            | Self::NotRunning(_) => true,
            _ => false,
        }
    }
}

// ---------------------------------------------------------------------------
// One connection
// ---------------------------------------------------------------------------

/// A socket with a cancel-safe line reader: the only await in [`Self::read_message`]
/// is `fill_buf`, and state changes happen synchronously after it, so a caller may
/// race it against anything (a key press, Ctrl-C) without losing bytes.
pub(crate) struct Connection {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    pending: Vec<u8>,
}

impl Connection {
    pub(crate) async fn connect(path: &Path, limit: Duration) -> Result<Self, ClientError> {
        let stream = match timeout(limit, UnixStream::connect(path)).await {
            Err(_) => return Err(ClientError::ConnectTimeout(limit)),
            Ok(Err(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                ) =>
            {
                return Err(ClientError::NotRunning(path.to_owned()));
            }
            Ok(Err(source)) => {
                return Err(ClientError::Connect {
                    path: path.to_owned(),
                    source,
                });
            }
            Ok(Ok(stream)) => stream,
        };
        let (read, writer) = stream.into_split();
        Ok(Self {
            reader: BufReader::with_capacity(64 * 1024, read),
            writer,
            pending: Vec::new(),
        })
    }

    async fn send(&mut self, line: &str, limit: Duration) -> Result<(), ClientError> {
        match timeout(limit, self.writer.write_all(line.as_bytes())).await {
            Err(_) => Err(ClientError::Timeout {
                method: "write",
                after: limit,
            }),
            Ok(Err(error)) => Err(ClientError::Io(error)),
            Ok(Ok(())) => Ok(()),
        }
    }

    /// The next server message. Cancel-safe.
    pub(crate) async fn read_message(&mut self) -> Result<ServerMessage, ClientError> {
        loop {
            let (take, newline, empty) = {
                let available = self.reader.fill_buf().await?;
                let newline = available.iter().position(|b| *b == b'\n');
                let take = newline.unwrap_or(available.len());
                if self.pending.len() + take > MAX_SERVER_LINE_BYTES {
                    return Err(ClientError::Malformed(format!(
                        "a line longer than {MAX_SERVER_LINE_BYTES} bytes"
                    )));
                }
                self.pending.extend_from_slice(&available[..take]);
                (take, newline.is_some(), available.is_empty())
            };
            if empty {
                return Err(ClientError::Closed);
            }
            self.reader.consume(take + usize::from(newline));
            if !newline {
                continue;
            }
            let bytes = std::mem::take(&mut self.pending);
            let text = String::from_utf8(bytes)
                .map_err(|_| ClientError::Malformed("a line that is not UTF-8".to_owned()))?;
            if text.trim().is_empty() {
                continue;
            }
            return ServerMessage::decode(&text)
                .map_err(|error| ClientError::Malformed(error.message));
        }
    }

    /// Send `request` and read until its response, stashing event pushes that arrive
    /// meanwhile into `events`. Bounded by `limit` for the whole exchange.
    pub(crate) async fn exchange(
        &mut self,
        id: u64,
        request: &Request,
        limit: Duration,
        events: &mut VecDeque<EventEnvelope>,
    ) -> Result<Response, ClientError> {
        let method = request.method().as_str();
        let wire = request.to_wire(id).map_err(ClientError::Protocol)?;
        let line = encode_line(&wire).map_err(ClientError::Protocol)?;
        let work = async {
            self.send(&line, limit).await?;
            loop {
                match self.read_message().await? {
                    ServerMessage::Response(response) if response.id == id => {
                        return Ok(response);
                    }
                    // A response to something abandoned earlier.
                    ServerMessage::Response(_) => {}
                    ServerMessage::Event(push) => events.push_back(push.event),
                    ServerMessage::Error(push) => return Err(ClientError::Protocol(push.error)),
                }
            }
        };
        match timeout(limit, work).await {
            Ok(result) => result,
            Err(_) => Err(ClientError::Timeout {
                method,
                after: limit,
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// Request / response
// ---------------------------------------------------------------------------

/// A connection to the daemon that makes requests, one at a time.
pub struct Client {
    conn: Connection,
    path: PathBuf,
    config: ClientConfig,
    next_id: u64,
    poisoned: Option<String>,
    /// Event pushes that arrived while waiting for a response. Only non-empty after
    /// [`Client::subscription_open`]; use [`Follow`] to consume a stream.
    stray_events: VecDeque<EventEnvelope>,
}

impl Client {
    /// Connect with default timeouts.
    ///
    /// # Errors
    /// [`ClientError::NotRunning`] when no daemon listens at `path`.
    pub async fn connect(path: &Path) -> Result<Self, ClientError> {
        Self::connect_with(path, ClientConfig::default()).await
    }

    /// Connect with explicit timeouts.
    ///
    /// # Errors
    /// As [`Client::connect`], plus [`ClientError::ConnectTimeout`].
    pub async fn connect_with(path: &Path, config: ClientConfig) -> Result<Self, ClientError> {
        Ok(Self {
            conn: Connection::connect(path, config.connect_timeout).await?,
            path: path.to_owned(),
            config,
            next_id: 1,
            poisoned: None,
            stray_events: VecDeque::new(),
        })
    }

    /// The socket this client is connected to.
    #[must_use]
    pub fn socket(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub fn config(&self) -> ClientConfig {
        self.config
    }

    async fn call<T: DeserializeOwned>(&mut self, request: Request) -> Result<T, ClientError> {
        if let Some(why) = &self.poisoned {
            return Err(ClientError::Poisoned(why.clone()));
        }
        let id = self.next_id;
        self.next_id += 1;
        let result = self
            .conn
            .exchange(
                id,
                &request,
                self.config.request_timeout,
                &mut self.stray_events,
            )
            .await;
        match result {
            Ok(response) => response.into_result().map_err(ClientError::Protocol),
            Err(error) => {
                // After a timeout or I/O failure the framing state is unknown; a late
                // response could be mistaken for the next one. A protocol error is a
                // clean answer and leaves the connection fine.
                if !matches!(error, ClientError::Protocol(_)) {
                    self.poisoned = Some("an earlier request failed".to_owned());
                }
                Err(error)
            }
        }
    }

    /// `daemon.status`.
    ///
    /// # Errors
    /// Any [`ClientError`].
    pub async fn status(&mut self) -> Result<DaemonStatusResult, ClientError> {
        self.call(Request::DaemonStatus).await
    }

    /// `daemon.stop`: the daemon replies, then shuts down gracefully.
    ///
    /// # Errors
    /// Any [`ClientError`].
    pub async fn stop(&mut self) -> Result<(), ClientError> {
        self.call::<OkResult>(Request::DaemonStop).await.map(|_| ())
    }

    /// `run.start`.
    ///
    /// # Errors
    /// `invalid_repo`, `invalid_config`, `model_unavailable`, `rate_limited`, ...
    pub async fn run_start(&mut self, params: RunStartParams) -> Result<RunId, ClientError> {
        self.call::<RunStartResult>(Request::RunStart(params))
            .await
            .map(|r| r.run_id)
    }

    /// `run.list`: one page.
    ///
    /// # Errors
    /// Any [`ClientError`].
    pub async fn run_list(&mut self, params: RunListParams) -> Result<RunListResult, ClientError> {
        self.call(Request::RunList(params)).await
    }

    /// `run.get`.
    ///
    /// # Errors
    /// `unknown_run`, ...
    pub async fn run_get(&mut self, run_id: RunId) -> Result<RunState, ClientError> {
        self.call(Request::RunGet(RunIdParams { run_id })).await
    }

    /// `run.events`: one page.
    ///
    /// # Errors
    /// `unknown_run`, ...
    pub async fn run_events(
        &mut self,
        params: RunEventsParams,
    ) -> Result<RunEventsResult, ClientError> {
        self.call(Request::RunEvents(params)).await
    }

    /// `subscription.open` on this connection. The pushes that follow are not
    /// surfaced by `Client`; this exists for completeness and tests, and real
    /// consumers use [`Follow`].
    ///
    /// # Errors
    /// `unknown_run`, `invalid_request`, ...
    pub async fn subscription_open(
        &mut self,
        params: SubscriptionOpenParams,
    ) -> Result<SubscriptionOpenResult, ClientError> {
        self.call(Request::SubscriptionOpen(params)).await
    }

    /// `subscription.close`; true when there was a subscription to close.
    ///
    /// # Errors
    /// Any [`ClientError`].
    pub async fn subscription_close(&mut self) -> Result<bool, ClientError> {
        self.call::<SubscriptionCloseResult>(Request::SubscriptionClose)
            .await
            .map(|r| r.closed)
    }

    /// `run.steer`. A backend that *rejects* the message is `Ok(Rejected)`; a backend
    /// with no input channel is the `steering_unsupported` protocol error.
    ///
    /// # Errors
    /// `unknown_run`, `wrong_state`, `steering_unsupported`, ...
    pub async fn run_steer(
        &mut self,
        run_id: RunId,
        message: impl Into<String>,
    ) -> Result<SteerOutcome, ClientError> {
        self.call(Request::RunSteer(RunSteerParams {
            run_id,
            message: message.into(),
        }))
        .await
    }

    /// `run.cancel`.
    ///
    /// # Errors
    /// `unknown_run`, `wrong_state`.
    pub async fn run_cancel(&mut self, run_id: RunId) -> Result<(), ClientError> {
        self.call::<OkResult>(Request::RunCancel(RunIdParams { run_id }))
            .await
            .map(|_| ())
    }

    /// `run.answer`.
    ///
    /// # Errors
    /// `unknown_run`, `wrong_state`, `invalid_request`.
    pub async fn run_answer(
        &mut self,
        run_id: RunId,
        escalation_id: EscalationId,
        decision: HumanDecision,
        note: impl Into<String>,
    ) -> Result<(), ClientError> {
        self.call::<OkResult>(Request::RunAnswer(
            aftman_daemon::protocol::RunAnswerParams {
                run_id,
                escalation_id,
                decision,
                note: note.into(),
            },
        ))
        .await
        .map(|_| ())
    }

    /// `run.purge`: how many runs were deleted.
    ///
    /// # Errors
    /// `unknown_run`, `wrong_state`.
    pub async fn run_purge(&mut self, params: RunPurgeParams) -> Result<u64, ClientError> {
        self.call::<RunPurgeResult>(Request::RunPurge(params))
            .await
            .map(|r| r.purged)
    }

    /// `responsibility.list`.
    ///
    /// # Errors
    /// Any [`ClientError`].
    pub async fn responsibilities(&mut self) -> Result<ResponsibilityListResult, ClientError> {
        self.call(Request::ResponsibilityList).await
    }

    /// `jev.usage`: opaque JSON, interpreted by whoever owns the provider.
    ///
    /// # Errors
    /// Any [`ClientError`].
    pub async fn jev_usage(
        &mut self,
        params: JevUsageParams,
    ) -> Result<serde_json::Value, ClientError> {
        self.call(Request::JevUsage(params)).await
    }

    /// Every page of `run.events` from `from_seq`, in order.
    ///
    /// # Errors
    /// `unknown_run`, or a stream that is not contiguous.
    pub async fn all_events(
        &mut self,
        run_id: RunId,
        from_seq: EventSeq,
    ) -> Result<Vec<EventEnvelope>, ClientError> {
        let mut out: Vec<EventEnvelope> = Vec::new();
        let mut next = from_seq;
        loop {
            let page = self
                .run_events(RunEventsParams {
                    run_id,
                    from_seq: Some(next),
                    limit: Some(aftman_daemon::protocol::MAX_EVENTS_LIMIT),
                })
                .await?;
            for event in page.events {
                if event.seq != next {
                    return Err(ClientError::Sequence {
                        expected: next.get(),
                        got: event.seq.get(),
                    });
                }
                next = next.next();
                out.push(event);
            }
            if !page.has_more {
                return Ok(out);
            }
        }
    }

    /// Every run, newest first, across pages (at most `max` of them).
    ///
    /// # Errors
    /// Any [`ClientError`].
    pub async fn all_runs(
        &mut self,
        status: Option<Vec<aftman_core::job::FactoryStatus>>,
        max: usize,
    ) -> Result<Vec<aftman_core::run::RunSummary>, ClientError> {
        let mut out = Vec::new();
        let mut cursor = None;
        while out.len() < max {
            let page = self
                .run_list(RunListParams {
                    status: status.clone(),
                    limit: Some((max - out.len()).min(aftman_daemon::protocol::MAX_RUN_LIST_LIMIT)),
                    cursor,
                })
                .await?;
            out.extend(page.runs);
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        Ok(out)
    }
}

pub use crate::follow::{Follow, FollowStats};
