//! The Unix-socket transport: NDJSON, one object per line (`docs/02-lld.md` §11).
//!
//! The schema and its bounds are [`crate::protocol`]; every method is a
//! [`crate::manager::RunManager`] call. This module is framing, concurrency, and
//! backpressure, and nothing else.
//!
//! # One connection
//!
//! ```text
//!   socket --read--> bounded line reader --> decode --+--> subscription.* , daemon.stop  (inline)
//!                                                     +--> one task per other request
//!   forwarder task (hub Subscription) ----+
//!   request tasks ------------------------+--> bounded outbound queue --> writer task --write(timeout)--> socket
//! ```
//!
//! - **Bounded input.** The reader never holds more than
//!   [`MAX_REQUEST_LINE_BYTES`] of an unfinished line; a longer one is answered with
//!   `invalid_request` and the connection is closed.
//! - **Pipelining.** Requests other than the subscription calls run in their own
//!   tasks (bounded per connection), so a `run.cancel` waiting for a model cycle
//!   neither blocks the next request nor stops event pushes. Responses carry the
//!   request `id` and may arrive out of order.
//! - **Backpressure.** Every byte to a client goes through one bounded queue and one
//!   writer task with a write timeout. A client that stops reading fills the kernel
//!   buffer, then the queue, then the hub's per-subscriber channel (which marks it
//!   lagged: the run never waits, see `hub.rs`); the writer's timeout then ends the
//!   connection. A stalled client costs one task and a bounded queue, nothing else.
//! - **Bounded connections.** Over [`ServerConfig::max_connections`] a new
//!   connection is told `rate_limited` and closed.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Notify, Semaphore, mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};

use crate::hub::Subscription;
use crate::manager::RunManager;
use crate::protocol::{
    Envelope, ErrorCode, ErrorPush, MAX_REQUEST_LINE_BYTES, OkResult, ProtocolError, Request,
    Response, ServerMessage, SubscriptionCloseResult, decode_request, encode_line,
};

/// Default for [`ServerConfig::idle_timeout`].
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Socket server tuning. Every bound is explicit (hard rule 5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerConfig {
    /// Simultaneous client connections.
    pub max_connections: usize,
    /// Lines queued for one client before senders wait.
    pub outbound_queue: usize,
    /// How long one write to a client may take before the client is dropped.
    pub write_timeout: Duration,
    /// Requests being handled at once, per connection. When this many are running
    /// the reader stops reading that connection until one finishes. It bounds the
    /// work and memory one client can have in progress (each request is at most
    /// `MAX_REQUEST_LINE_BYTES`); it does not bound the total across connections
    /// (`max_connections` x this), nor the responses queued for a slow reader (that
    /// is `outbound_queue` and `write_timeout`).
    pub max_in_flight: usize,
    /// A connection with no subscription and no request in flight is closed after
    /// this long without a request, so idle clients cannot hold every slot and lock
    /// out `aftman stop`. A connection with an open subscription is never idle.
    pub idle_timeout: Duration,
    /// How long, at shutdown or disconnect, in-flight work may take to finish.
    pub drain_timeout: Duration,
    /// The surface name recorded with a `run.answer`.
    pub surface: &'static str,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_connections: 64,
            outbound_queue: 256,
            write_timeout: Duration::from_secs(5),
            max_in_flight: 32,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            drain_timeout: Duration::from_secs(5),
            surface: "cli",
        }
    }
}

/// The socket transport. Cheap to build; [`ProtocolServer::serve`] does the work.
pub struct ProtocolServer {
    manager: Arc<RunManager>,
    config: ServerConfig,
    stop: Arc<Notify>,
}

impl ProtocolServer {
    #[must_use]
    pub fn new(manager: Arc<RunManager>, config: ServerConfig) -> Self {
        Self {
            manager,
            config,
            stop: Arc::new(Notify::new()),
        }
    }

    /// Accept connections until `shutdown` resolves or a client sends `daemon.stop`;
    /// then shut down gracefully, in this order: stop accepting, shut the runs down
    /// through the manager (each is persisted `Cancelled` with reason
    /// `daemon shutdown`, then every subscription is closed), and let the
    /// connection tasks flush and finish (bounded by
    /// [`ServerConfig::drain_timeout`]). Removing the socket and pid files is the
    /// caller's job (`lifecycle::ClaimGuard::cleanup`).
    pub async fn serve(&self, listener: UnixListener, shutdown: impl Future<Output = ()>) {
        let limit = Arc::new(Semaphore::new(self.config.max_connections));
        let (close_tx, close_rx) = watch::channel(false);
        let mut connections: JoinSet<()> = JoinSet::new();
        let mut shutdown = std::pin::pin!(shutdown);

        loop {
            tokio::select! {
                accepted = listener.accept() => match accepted {
                    Ok((stream, _)) => match Arc::clone(&limit).try_acquire_owned() {
                        Ok(permit) => {
                            let connection = Connection {
                                manager: Arc::clone(&self.manager),
                                config: self.config,
                                stop: Arc::clone(&self.stop),
                                close: close_rx.clone(),
                            };
                            connections.spawn(async move {
                                connection.run(stream).await;
                                drop(permit);
                            });
                        }
                        Err(_) => {
                            connections.spawn(reject(stream, self.config.write_timeout));
                        }
                    },
                    Err(error) => {
                        tracing::warn!(%error, "accept failed");
                        // Avoid a hot loop on a persistent error such as EMFILE.
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                },
                () = &mut shutdown => break,
                () = self.stop.notified() => break,
                Some(_) = connections.join_next(), if !connections.is_empty() => {}
            }
        }

        drop(listener);
        self.manager.shutdown().await;
        let _ = close_tx.send(true);
        let drain = async { while connections.join_next().await.is_some() {} };
        if tokio::time::timeout(self.config.drain_timeout, drain)
            .await
            .is_err()
        {
            tracing::warn!("connections did not finish in time; dropping them");
            connections.abort_all();
        }
    }
}

/// Tell an over-limit client why it is being dropped.
async fn reject(mut stream: UnixStream, timeout: Duration) {
    let push = ServerMessage::Error(ErrorPush {
        error: ProtocolError::new(
            ErrorCode::RateLimited,
            "too many connections to the daemon; try again shortly",
        ),
    });
    if let Ok(line) = encode_line(&push) {
        let _ = tokio::time::timeout(timeout, stream.write_all(line.as_bytes())).await;
    }
}

// ---------------------------------------------------------------------------
// The bounded line reader
// ---------------------------------------------------------------------------

enum Line {
    Bytes(Vec<u8>),
    TooLong,
    Eof,
    Failed(std::io::Error),
}

/// Reads newline-terminated lines, never holding more than
/// [`MAX_REQUEST_LINE_BYTES`] of an unfinished one. Cancel-safe: the only await is
/// `fill_buf`, and state is updated synchronously after it.
struct LineReader {
    reader: BufReader<OwnedReadHalf>,
    pending: Vec<u8>,
}

impl LineReader {
    fn new(half: OwnedReadHalf) -> Self {
        Self {
            reader: BufReader::with_capacity(8 * 1024, half),
            pending: Vec::new(),
        }
    }

    async fn next_line(&mut self) -> Line {
        loop {
            let (take, newline, empty) = match self.reader.fill_buf().await {
                Ok(available) => {
                    let newline = available.iter().position(|b| *b == b'\n');
                    let take = newline.unwrap_or(available.len());
                    if self.pending.len() + take > MAX_REQUEST_LINE_BYTES {
                        return Line::TooLong;
                    }
                    self.pending.extend_from_slice(&available[..take]);
                    (take, newline.is_some(), available.is_empty())
                }
                Err(error) => return Line::Failed(error),
            };
            if empty {
                // End of stream: an unterminated final line still counts.
                return if self.pending.is_empty() {
                    Line::Eof
                } else {
                    Line::Bytes(std::mem::take(&mut self.pending))
                };
            }
            self.reader.consume(take + usize::from(newline));
            if newline {
                let mut line = std::mem::take(&mut self.pending);
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                return Line::Bytes(line);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// One connection
// ---------------------------------------------------------------------------

struct Connection {
    manager: Arc<RunManager>,
    config: ServerConfig,
    stop: Arc<Notify>,
    close: watch::Receiver<bool>,
}

type Outbound = mpsc::Sender<String>;

/// Queue one message. `false` when the connection is gone.
async fn send(out: &Outbound, message: &ServerMessage) -> bool {
    match encode_line(message) {
        Ok(line) => out.send(line).await.is_ok(),
        Err(error) => {
            tracing::error!(%error, "could not encode a server message");
            true
        }
    }
}

async fn send_response(out: &Outbound, response: Response) -> bool {
    send(out, &ServerMessage::Response(response)).await
}

fn respond<T: Serialize>(id: u64, result: Result<T, ProtocolError>) -> Response {
    match result.and_then(|value| Response::ok(id, &value)) {
        Ok(response) => response,
        Err(error) => Response::err(id, error),
    }
}

/// Owns the write half: drains the queue, one timed write at a time. Returning
/// drops the receiver, which is how producers learn the client is gone.
async fn writer(mut half: OwnedWriteHalf, mut queue: mpsc::Receiver<String>, timeout: Duration) {
    while let Some(line) = queue.recv().await {
        match tokio::time::timeout(timeout, half.write_all(line.as_bytes())).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => return,
            Err(_) => {
                tracing::warn!("dropping a client that stopped reading (write timed out)");
                return;
            }
        }
    }
    let _ = tokio::time::timeout(timeout, half.shutdown()).await;
}

/// Forward a hub subscription to one client until it ends, lags, or the client goes.
async fn forward(mut subscription: Subscription, out: Outbound) {
    while let Some(item) = subscription.next().await {
        let message = match item {
            Ok(event) => ServerMessage::event((*event).clone()),
            Err(lagged) => {
                // The client has a correct gap-free prefix; tell it where to resume.
                let _ = send(
                    &out,
                    &ServerMessage::lagged(lagged.run_id, lagged.resume_from),
                )
                .await;
                return;
            }
        };
        if !send(&out, &message).await {
            return;
        }
    }
    // The stream ended. If it ended because the daemon stopped driving the run without
    // being able to record its end, say so explicitly: the client would otherwise wait
    // on a socket that will never speak again.
    if let Some(gone) = subscription.abandoned() {
        let _ = send(&out, &ServerMessage::abandoned(gone.run_id, &gone.reason)).await;
    }
}

/// Run one request that is not a subscription call, and answer it. A panicking
/// handler becomes an `internal` error rather than silence.
async fn execute(
    manager: Arc<RunManager>,
    surface: &'static str,
    id: u64,
    request: Request,
) -> Response {
    let work = tokio::spawn(async move {
        match request {
            Request::DaemonStatus => respond(id, manager.status().await),
            Request::RunStart(p) => respond(id, manager.run_start(p).await),
            Request::RunList(p) => respond(id, manager.run_list(p).await),
            Request::RunGet(p) => respond(id, manager.run_get(p).await),
            Request::RunEvents(p) => respond(id, manager.run_events(p).await),
            Request::RunSteer(p) => respond(id, manager.run_steer(p).await),
            Request::RunCancel(p) => respond(id, manager.run_cancel(p).await),
            Request::RunAnswer(p) => {
                // The client may name its own surface (validated against a closed set);
                // otherwise it is the transport's.
                let who = p.surface.clone().unwrap_or_else(|| surface.to_owned());
                respond(id, manager.run_answer(p, &who).await)
            }
            Request::RunPurge(p) => respond(id, manager.run_purge(p).await),
            Request::ResponsibilityList => respond(id, manager.responsibilities().await),
            Request::JevUsage(p) => respond(id, manager.jev_usage(p).await),
            Request::DaemonStop | Request::SubscriptionOpen(_) | Request::SubscriptionClose => {
                respond::<Value>(
                    id,
                    Err(ProtocolError::new(
                        ErrorCode::Internal,
                        "request routed to the wrong handler",
                    )),
                )
            }
        }
    });
    work.await.unwrap_or_else(|_| {
        Response::err(
            id,
            ProtocolError::new(ErrorCode::Internal, "the request handler failed"),
        )
    })
}

impl Connection {
    async fn run(mut self, stream: UnixStream) {
        let (read_half, write_half) = stream.into_split();
        let (out, queue) = mpsc::channel::<String>(self.config.outbound_queue);
        let writer_task = tokio::spawn(writer(write_half, queue, self.config.write_timeout));
        let mut reader = LineReader::new(read_half);
        let in_flight = Arc::new(Semaphore::new(self.config.max_in_flight));
        let mut requests: JoinSet<()> = JoinSet::new();
        let mut subscription: Option<JoinHandle<()>> = None;
        let mut server_shutdown = false;
        // The client closed its sending side (`nc -N`, `socat`): there is nothing
        // more to read, but responses still in flight and an open subscription are
        // still owed to it. The connection ends when those do, or the writer fails.
        let mut eof = false;
        let mut last_request = tokio::time::Instant::now();

        loop {
            if eof && subscription.is_none() && requests.is_empty() {
                break;
            }
            let idle = subscription.is_none() && requests.is_empty() && !eof;
            let line = tokio::select! {
                line = reader.next_line(), if !eof => line,
                // The writer has ended (client gone or stalled): nothing more can
                // be said to it.
                () = out.closed() => break,
                _ = self.close.wait_for(|closing| *closing) => {
                    server_shutdown = true;
                    break;
                }
                Some(_) = requests.join_next(), if !requests.is_empty() => {
                    // A request that has just finished is activity. Without this, a
                    // `run.cancel` that took longer than `idle_timeout` left the idle
                    // deadline in the past, and the next iteration could close a connection
                    // the client was still using.
                    last_request = tokio::time::Instant::now();
                    continue;
                }
                _ = async {
                    if let Some(task) = subscription.as_mut() {
                        let _ = task.await;
                    }
                }, if eof && subscription.is_some() => {
                    subscription = None;
                    continue;
                }
                () = tokio::time::sleep_until(last_request + self.config.idle_timeout), if idle => {
                    tracing::debug!("closing an idle connection");
                    break;
                }
            };
            last_request = tokio::time::Instant::now();
            let bytes = match line {
                Line::Bytes(bytes) => bytes,
                Line::Eof => {
                    eof = true;
                    continue;
                }
                Line::Failed(error) => {
                    tracing::debug!(%error, "client read failed");
                    break;
                }
                Line::TooLong => {
                    let error = ProtocolError::invalid(format!(
                        "request line exceeds {MAX_REQUEST_LINE_BYTES} bytes; closing the connection"
                    ));
                    send(&out, &ServerMessage::Error(ErrorPush { error })).await;
                    break;
                }
            };
            let Ok(text) = String::from_utf8(bytes) else {
                let error = ProtocolError::invalid("request is not valid UTF-8");
                if !send(&out, &ServerMessage::Error(ErrorPush { error })).await {
                    break;
                }
                continue;
            };
            if text.trim().is_empty() {
                continue;
            }
            let Envelope { id, request } = match decode_request(&text) {
                Ok(envelope) => envelope,
                Err(error) => {
                    if !send(&out, &error.into_message()).await {
                        break;
                    }
                    continue;
                }
            };
            let alive = match request {
                Request::SubscriptionOpen(params) => {
                    let result = self.manager.subscribe(&params).await;
                    match result {
                        Ok(opened) => {
                            // Retire the old stream completely before answering, so
                            // none of its events can follow the reply.
                            let replaced = Self::retire(&mut subscription).await;
                            let reply = respond(
                                id,
                                Ok(self.manager.subscription_result(&params, replaced)),
                            );
                            let alive = send_response(&out, reply).await;
                            subscription = Some(tokio::spawn(forward(opened, out.clone())));
                            alive
                        }
                        Err(error) => send_response(&out, Response::err(id, error)).await,
                    }
                }
                Request::SubscriptionClose => {
                    let closed = Self::retire(&mut subscription).await;
                    send_response(&out, respond(id, Ok(SubscriptionCloseResult { closed }))).await
                }
                Request::DaemonStop => {
                    let alive = send_response(&out, respond(id, Ok(OkResult::OK))).await;
                    // The reply is queued first; the shutdown that follows flushes it.
                    self.stop.notify_one();
                    alive
                }
                other => {
                    let Ok(permit) = Arc::clone(&in_flight).acquire_owned().await else {
                        break;
                    };
                    let manager = Arc::clone(&self.manager);
                    let out = out.clone();
                    let surface = self.config.surface;
                    requests.spawn(async move {
                        let response = execute(manager, surface, id, other).await;
                        send_response(&out, response).await;
                        drop(permit);
                    });
                    true
                }
            };
            if !alive {
                break;
            }
        }

        self.wind_down(subscription, requests, server_shutdown)
            .await;
        drop(out);
        // The writer finishes the queue (each write is individually bounded).
        let patience = self.config.write_timeout * 2 + self.config.drain_timeout;
        if tokio::time::timeout(patience, writer_task).await.is_err() {
            tracing::warn!("a connection writer did not finish; dropping it");
        }
    }

    /// Stop the connection's subscription, waiting until its task is really gone.
    /// Returns whether there was one.
    async fn retire(subscription: &mut Option<JoinHandle<()>>) -> bool {
        match subscription.take() {
            Some(task) => {
                task.abort();
                let _ = task.await;
                true
            }
            None => false,
        }
    }

    /// Let accepted requests finish, and (at server shutdown) let the subscription
    /// deliver what the hub still holds for it; both bounded.
    async fn wind_down(
        &self,
        subscription: Option<JoinHandle<()>>,
        mut requests: JoinSet<()>,
        server_shutdown: bool,
    ) {
        let drain = async { while requests.join_next().await.is_some() {} };
        if tokio::time::timeout(self.config.drain_timeout, drain)
            .await
            .is_err()
        {
            requests.abort_all();
        }
        if let Some(mut task) = subscription {
            if server_shutdown {
                // The hub is closed by now, so the forwarder drains and ends.
                if tokio::time::timeout(self.config.drain_timeout, &mut task)
                    .await
                    .is_err()
                {
                    task.abort();
                }
            } else {
                task.abort();
            }
        }
    }
}
