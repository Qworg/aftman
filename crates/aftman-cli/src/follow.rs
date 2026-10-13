//! One run's event stream, with recovery.
//!
//! [`Follow`] owns a **dedicated connection** (one subscription per connection is the
//! protocol rule) and recovers by itself:
//!
//! - a `subscriber_lagged` push, or a lost connection, is answered by establishing a
//!   fresh connection and subscribing from `last_seen + 1`;
//! - every event's `seq` is checked against the one expected next; a gap or a
//!   duplicate is [`ClientError::Sequence`] and kills the stream, never papered over;
//! - a run this daemon is **not driving** (it ended, belongs to a `--no-daemon`
//!   process, or is an orphan) would never push another event, so for those the stream
//!   polls `run.events` until `RUN_FINISHED` or a terminal status ([`Follow::is_polling`]).
//!
//! # Cancellation
//!
//! [`Follow::next`] is cancel-safe in the strong sense a UI needs. All reconnect state
//! (attempts so far, backoff, the time of the next attempt, the attempt in flight)
//! lives in the struct, and the attempt itself runs in a spawned task, so dropping the
//! future (a `select!` with a tick) neither loses an event nor restarts the retry
//! budget: a daemon that stays away is reported after `reconnect_attempts`, however
//! often the caller polls.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio::time::Instant;

use aftman_core::event::{EventEnvelope, EventKind};
use aftman_core::ids::{EventSeq, RunId};
use aftman_daemon::lifecycle::{DaemonPaths, DaemonPresence, DirPolicy, probe};
use aftman_daemon::protocol::{
    ErrorCode, MAX_EVENTS_LIMIT, Request, RunEventsParams, RunEventsResult, RunIdParams,
    ServerMessage, SubscriptionOpenParams, SubscriptionOpenResult,
};

use crate::client::{ClientConfig, ClientError, Connection};

/// How long a polling stream waits between looks at the log.
pub const POLL_INTERVAL: Duration = Duration::from_millis(300);

/// How long a stream waits for a daemon that holds its lock but does not answer (it is
/// recovering after a crash, or restarting) before giving up. The same documented bound
/// auto-start uses. While it waits, the attempt budget is not spent.
pub const BUSY_WAIT: Duration = Duration::from_secs(75);

/// How long a live stream may be silent before it asks the daemon whether the run is
/// still being driven. Quiet is normal (a worker thinking); a daemon that has given up
/// on a run it could not record the end of is not, and says nothing either.
pub const LIVENESS_INTERVAL: Duration = Duration::from_secs(30);

/// How often a stream looks again while the daemon is busy.
const BUSY_POLL: Duration = Duration::from_millis(200);

/// What recovery has happened on a [`Follow`] so far. For tests and status lines.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FollowStats {
    /// `subscriber_lagged` pushes handled.
    pub lagged: u32,
    /// Connections lost and re-established.
    pub reconnects: u32,
}

struct Established {
    conn: Connection,
    live: bool,
    early: VecDeque<EventEnvelope>,
}

/// Connect and subscribe once. If the daemon says the run is not live here, the
/// subscription is closed again: nothing will be pushed, and the caller polls.
async fn establish(
    path: PathBuf,
    run_id: RunId,
    from_seq: EventSeq,
    config: ClientConfig,
) -> Result<Established, ClientError> {
    let mut conn = Connection::connect(&path, config.connect_timeout).await?;
    let mut early = VecDeque::new();
    let open = Request::SubscriptionOpen(SubscriptionOpenParams {
        run_id: Some(run_id),
        from_seq: Some(from_seq),
    });
    let opened: SubscriptionOpenResult = conn
        .exchange(1, &open, config.request_timeout, &mut early)
        .await?
        .into_result()
        .map_err(ClientError::Protocol)?;
    if !opened.live {
        // Whatever backlog was already queued stays in `early`, contiguous from
        // `from_seq`; the close retires the stream so nothing follows it.
        conn.exchange(
            2,
            &Request::SubscriptionClose,
            config.request_timeout,
            &mut early,
        )
        .await?;
    }
    Ok(Established {
        conn,
        live: opened.live,
        early,
    })
}

/// One look at the log of a run nobody pushes for: a page of events, or, when there are
/// none and the run might be over, its status.
struct PollRound {
    events: Vec<EventEnvelope>,
    /// `(status name, terminal)`, fetched only when the page was empty.
    state: Option<(String, bool)>,
}

type PollJob = JoinHandle<(Option<Connection>, Result<PollRound, ClientError>)>;

/// Run one poll round on `conn`, which it owns for the duration, so the work does not
/// depend on whoever started it still waiting (see [`Follow`] on cancellation). The
/// connection comes back only if the exchange left it usable.
async fn poll_round(
    mut conn: Connection,
    run_id: RunId,
    from_seq: EventSeq,
    want_state: bool,
    first_id: u64,
    limit: Duration,
) -> (Option<Connection>, Result<PollRound, ClientError>) {
    let mut stray = VecDeque::new();
    let events = Request::RunEvents(RunEventsParams {
        run_id,
        from_seq: Some(from_seq),
        limit: Some(MAX_EVENTS_LIMIT),
    });
    let page: RunEventsResult = match conn.exchange(first_id, &events, limit, &mut stray).await {
        Ok(response) => match response.into_result() {
            Ok(page) => page,
            Err(error) => return (Some(conn), Err(ClientError::Protocol(error))),
        },
        Err(error) => return (None, Err(error)),
    };
    if !page.events.is_empty() || !want_state {
        return (
            Some(conn),
            Ok(PollRound {
                events: page.events,
                state: None,
            }),
        );
    }
    let get = Request::RunGet(RunIdParams { run_id });
    match conn.exchange(first_id + 1, &get, limit, &mut stray).await {
        Ok(response) => match response.into_result::<aftman_core::run::RunState>() {
            Ok(state) => (
                Some(conn),
                Ok(PollRound {
                    events: Vec::new(),
                    state: Some((state.status.as_str().to_owned(), state.status.is_terminal())),
                }),
            ),
            Err(error) => (Some(conn), Err(ClientError::Protocol(error))),
        },
        Err(error) => (None, Err(error)),
    }
}

/// A cheap, cloneable window onto a [`Follow`]'s health, kept up to date by the stream
/// itself. A UI holds one and reads it on its own schedule: no call into `Follow`, no
/// cancelled read.
#[derive(Debug, Clone, Default)]
pub struct FollowStatus(std::sync::Arc<StatusCells>);

#[derive(Debug, Default)]
struct StatusCells {
    reconnect_failures: std::sync::atomic::AtomicU32,
    polling: std::sync::atomic::AtomicBool,
    connected: std::sync::atomic::AtomicBool,
    finished: std::sync::atomic::AtomicBool,
    next_seq: std::sync::atomic::AtomicU64,
}

impl FollowStatus {
    /// Consecutive failed attempts to re-establish a lost stream (`0` when healthy).
    #[must_use]
    pub fn reconnect_failures(&self) -> u32 {
        self.0
            .reconnect_failures
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The daemon is not driving the run, so the stream polls its log ("not live").
    #[must_use]
    pub fn is_polling(&self) -> bool {
        self.0.polling.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Whether a connection to the daemon is currently up ("reconnecting" when not).
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.0.connected.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The stream has delivered the run's end.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.0.finished.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The `seq` the next event will have.
    #[must_use]
    pub fn next_seq(&self) -> EventSeq {
        EventSeq(self.0.next_seq.load(std::sync::atomic::Ordering::Relaxed))
    }
}

struct Retry {
    failed: u32,
    backoff: Duration,
    retry_at: Instant,
    /// When the daemon was first seen busy during this outage, if it still is.
    busy_since: Option<Instant>,
}

/// One run's events, in order, with no gap and no duplicate.
pub struct Follow {
    path: PathBuf,
    run_id: RunId,
    config: ClientConfig,
    next_seq: EventSeq,
    conn: Option<Connection>,
    pending: VecDeque<EventEnvelope>,
    finished: bool,
    failed: Option<String>,
    next_request_id: u64,
    stats: FollowStats,
    polling: bool,
    terminal_seen: bool,
    retry: Retry,
    attempt: Option<JoinHandle<Result<Established, ClientError>>>,
    liveness: Duration,
    /// When an event last arrived (or the stream last became healthy).
    quiet_since: Instant,
    /// The poll round in flight, if any. Kept here so a dropped `next()` resumes it
    /// instead of asking for the same page again.
    poll_job: Option<PollJob>,
    /// Earliest time of the next poll round.
    poll_at: Instant,
    status: FollowStatus,
}

impl Drop for Follow {
    fn drop(&mut self) {
        if let Some(attempt) = self.attempt.take() {
            attempt.abort();
        }
        if let Some(job) = self.poll_job.take() {
            job.abort();
        }
    }
}

impl Follow {
    /// Subscribe to `run_id` from `from_seq` on a fresh connection.
    ///
    /// The first attempt is not retried: `unknown_run` or a missing daemon is the
    /// caller's to report. Only a stream that was working is recovered.
    ///
    /// # Errors
    /// Connection and subscription errors.
    pub async fn open(
        path: &Path,
        run_id: RunId,
        from_seq: EventSeq,
        config: ClientConfig,
    ) -> Result<Self, ClientError> {
        let from_seq = EventSeq(from_seq.get().max(1));
        let established = establish(path.to_owned(), run_id, from_seq, config).await?;
        let mut follow = Self {
            path: path.to_owned(),
            run_id,
            config,
            next_seq: from_seq,
            conn: None,
            pending: VecDeque::new(),
            finished: false,
            failed: None,
            next_request_id: 10,
            stats: FollowStats::default(),
            polling: false,
            terminal_seen: false,
            retry: Retry {
                failed: 0,
                backoff: config.reconnect_backoff,
                retry_at: Instant::now(),
                busy_since: None,
            },
            attempt: None,
            liveness: LIVENESS_INTERVAL,
            quiet_since: Instant::now(),
            poll_job: None,
            poll_at: Instant::now(),
            status: FollowStatus::default(),
        };
        follow
            .status
            .0
            .next_seq
            .store(from_seq.get(), std::sync::atomic::Ordering::Relaxed);
        follow.adopt(established);
        Ok(follow)
    }

    /// How long the stream may be silent before it checks the run is still driven
    /// (default [`LIVENESS_INTERVAL`]). A run that is neither driven nor finished and
    /// whose log has not moved for three of these ends the stream with
    /// [`ClientError::NotDriven`].
    #[must_use]
    pub fn with_liveness(mut self, interval: Duration) -> Self {
        self.liveness = interval;
        self
    }

    fn adopt(&mut self, established: Established) {
        self.polling = !established.live;
        self.conn = Some(established.conn);
        self.pending.extend(established.early);
        self.retry = Retry {
            failed: 0,
            backoff: self.config.reconnect_backoff,
            retry_at: Instant::now(),
            busy_since: None,
        };
        self.sync_status();
    }

    /// The `seq` the next event will have.
    #[must_use]
    pub fn next_seq(&self) -> EventSeq {
        self.next_seq
    }

    #[must_use]
    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    #[must_use]
    pub fn stats(&self) -> FollowStats {
        self.stats
    }

    /// Whether the run's `RUN_FINISHED` has been delivered. The stream ends after it.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Consecutive failed attempts to re-establish a lost connection; `0` while the
    /// stream is connected (and during the first, quick retry). For a "reconnecting"
    /// indication: read-only.
    #[must_use]
    pub fn reconnect_failures(&self) -> u32 {
        if self.conn.is_none() {
            self.retry.failed
        } else {
            0
        }
    }

    /// True when the daemon is not driving this run, so the stream polls the log
    /// instead of waiting for pushes.
    #[must_use]
    pub fn is_polling(&self) -> bool {
        self.polling
    }

    /// The next event, or `None` once the run has ended (`RUN_FINISHED` delivered, or,
    /// for a run nobody drives, a terminal status with the log exhausted). Waits as
    /// long as the run is silent. Cancel-safe: dropping the future loses nothing and
    /// does not reset the reconnect budget.
    ///
    /// # Errors
    /// [`ClientError::Sequence`] when the stream is not contiguous,
    /// [`ClientError::Reconnect`] when a lost connection cannot be re-established,
    /// and any non-retryable daemon error. After an error the stream is dead.
    pub async fn next(&mut self) -> Result<Option<EventEnvelope>, ClientError> {
        if let Some(message) = &self.failed {
            return Err(ClientError::Poisoned(format!(
                "the stream already failed: {message}"
            )));
        }
        match self.next_inner().await {
            Ok(event) => Ok(event),
            Err(error) => {
                self.failed = Some(error.to_string());
                Err(error)
            }
        }
    }

    fn lose_connection(&mut self) {
        self.conn = None;
        self.stats.reconnects += 1;
        self.sync_status();
    }

    /// Publish the stream's health to its [`FollowStatus`] handles.
    fn sync_status(&self) {
        use std::sync::atomic::Ordering::Relaxed;
        let cells = &self.status.0;
        cells.reconnect_failures.store(self.retry.failed, Relaxed);
        cells.polling.store(self.polling, Relaxed);
        cells
            .connected
            .store(self.conn.is_some() || self.poll_job.is_some(), Relaxed);
        cells.finished.store(self.finished, Relaxed);
        cells.next_seq.store(self.next_seq.get(), Relaxed);
    }

    /// A handle onto this stream's health that can be read without touching the stream.
    #[must_use]
    pub fn status(&self) -> FollowStatus {
        self.status.clone()
    }

    async fn next_inner(&mut self) -> Result<Option<EventEnvelope>, ClientError> {
        loop {
            self.sync_status();
            if let Some(event) = self.pending.pop_front() {
                return self.accept(event).map(Some);
            }
            if self.finished {
                return Ok(None);
            }
            if self.conn.is_none() && self.poll_job.is_none() {
                self.reestablish().await?;
                continue;
            }
            if self.polling {
                if self.poll_once().await? {
                    return Ok(None);
                }
                continue;
            }
            let Some(conn) = self.conn.as_mut() else {
                continue;
            };
            let read = match tokio::time::timeout(self.liveness, conn.read_message()).await {
                Ok(read) => read,
                Err(_silent) => {
                    self.check_driven().await?;
                    continue;
                }
            };
            match read {
                Ok(ServerMessage::Event(push)) => return self.accept(push.event).map(Some),
                Ok(ServerMessage::Response(_)) => {}
                Ok(ServerMessage::Error(push)) => match push.error.code {
                    ErrorCode::SubscriberLagged => {
                        // The daemon dropped us after a correct prefix. Resume from
                        // our own count, which is the authority on what we have.
                        self.stats.lagged += 1;
                        self.conn = None;
                    }
                    ErrorCode::RunAbandoned => {
                        let data = push.error.data.as_ref();
                        let field = |key: &str| {
                            data.and_then(|d| d.get(key))
                                .and_then(serde_json::Value::as_str)
                                .map(ToOwned::to_owned)
                        };
                        return Err(ClientError::RunAbandoned {
                            run_id: field("run_id").unwrap_or_else(|| self.run_id.to_string()),
                            reason: field("reason").unwrap_or(push.error.message),
                        });
                    }
                    _ if push.error.retryable => self.lose_connection(),
                    _ => return Err(ClientError::Protocol(push.error)),
                },
                Err(ClientError::Closed | ClientError::Io(_)) => self.lose_connection(),
                Err(other) => return Err(other),
            }
        }
    }

    /// One look at the log of a run nobody pushes for. `true` when the run is over.
    async fn poll_once(&mut self) -> Result<bool, ClientError> {
        if self.poll_job.is_none() {
            // Waiting out the interval is cancel-safe: the deadline is state.
            tokio::time::sleep_until(self.poll_at).await;
            let Some(conn) = self.conn.take() else {
                return Ok(false);
            };
            let first_id = self.next_request_id;
            self.next_request_id += 2;
            self.poll_job = Some(tokio::spawn(poll_round(
                conn,
                self.run_id,
                self.next_seq,
                !self.terminal_seen,
                first_id,
                self.config.request_timeout,
            )));
        }
        let Some(job) = self.poll_job.as_mut() else {
            return Ok(false);
        };
        // Awaiting the handle by reference: if this future is dropped the job keeps
        // running and the next call picks up the same page.
        let joined = job.await;
        self.poll_job = None;
        let (conn, result) =
            joined.map_err(|e| ClientError::Malformed(format!("a poll task failed: {e}")))?;
        self.conn = conn;
        let round = match result {
            Ok(round) => round,
            Err(ClientError::Closed | ClientError::Io(_) | ClientError::Timeout { .. }) => {
                self.lose_connection();
                return Ok(false);
            }
            Err(other) => return Err(other),
        };
        self.poll_at = Instant::now();
        if !round.events.is_empty() {
            self.pending.extend(round.events);
            return Ok(false);
        }
        if self.terminal_seen {
            // Terminal, and the log was empty after we saw that: it is over.
            self.finished = true;
            self.sync_status();
            return Ok(true);
        }
        let Some((status, terminal)) = round.state else {
            return Ok(false);
        };
        if terminal {
            // Look at the log once more before declaring it over: events may have
            // landed between the two reads.
            self.terminal_seen = true;
            return Ok(false);
        }
        // Not driven, not finished, nothing new. A run another process drives still
        // writes its log; one that has been silent this long is not going to end.
        let silent = self.quiet_since.elapsed();
        if silent >= self.liveness * 3 {
            return Err(self.not_driven(silent, &status).await);
        }
        self.poll_at = Instant::now() + POLL_INTERVAL.min(self.liveness);
        Ok(false)
    }

    async fn not_driven(&self, silent: Duration, status: &str) -> ClientError {
        ClientError::NotDriven {
            run_id: self.run_id.to_string(),
            status: status.to_owned(),
            last_seq: self.next_seq.get().saturating_sub(1),
            silent_secs: silent.as_secs(),
        }
    }

    /// The stream has been silent for a while: is the daemon still driving this run?
    ///
    /// Cheap: one `run.get`, and, only if the run is not finished, one fresh
    /// subscription (which reports whether the daemon is driving it and replays
    /// anything missed). A live answer replaces the connection; a not-live answer
    /// switches to polling, which ends the stream if the log stays quiet.
    async fn check_driven(&mut self) -> Result<(), ClientError> {
        let id = self.next_request_id;
        self.next_request_id += 1;
        let limit = self.config.request_timeout;
        let get = Request::RunGet(RunIdParams {
            run_id: self.run_id,
        });
        let Some(conn) = self.conn.as_mut() else {
            return Ok(());
        };
        let state: aftman_core::run::RunState =
            match conn.exchange(id, &get, limit, &mut self.pending).await {
                Ok(response) => response.into_result().map_err(ClientError::Protocol)?,
                Err(ClientError::Closed | ClientError::Io(_) | ClientError::Timeout { .. }) => {
                    self.lose_connection();
                    return Ok(());
                }
                Err(other) => return Err(other),
            };
        if !self.pending.is_empty() {
            // Events arrived while we asked; deliver them first.
            return Ok(());
        }
        if state.status.is_terminal() {
            // The run ended and we were not told: the log has what we missed.
            self.polling = true;
            return Ok(());
        }
        match establish(self.path.clone(), self.run_id, self.next_seq, self.config).await {
            Ok(established) if established.live => {
                self.conn = Some(established.conn);
                self.pending.extend(established.early);
                Ok(())
            }
            Ok(established) => {
                // The daemon is not driving it. Poll; the quiet clock keeps running.
                let quiet = self.quiet_since;
                self.adopt(established);
                self.quiet_since = quiet;
                Ok(())
            }
            Err(error) if error.is_transient() => {
                self.lose_connection();
                Ok(())
            }
            Err(other) => Err(other),
        }
    }

    /// Run (or continue) one re-establish attempt. All state is in `self`, so being
    /// dropped mid-way costs nothing.
    async fn reestablish(&mut self) -> Result<(), ClientError> {
        if self.attempt.is_none() {
            tokio::time::sleep_until(self.retry.retry_at).await;
            self.attempt = Some(tokio::spawn(establish(
                self.path.clone(),
                self.run_id,
                self.next_seq,
                self.config,
            )));
        }
        let Some(handle) = self.attempt.as_mut() else {
            return Ok(());
        };
        let joined = handle.await;
        self.attempt = None;
        match joined {
            Ok(Ok(established)) => {
                self.adopt(established);
                Ok(())
            }
            Ok(Err(error)) if error.is_transient() => {
                // A daemon that holds its lock but does not answer is working (restart
                // recovery), not gone: keep waiting for it, bounded, without spending
                // the attempt budget that a daemon which is really absent runs out of.
                if let DaemonPresence::Busy { .. } = probe(&self.paths()).await {
                    let now = Instant::now();
                    let since = *self.retry.busy_since.get_or_insert(now);
                    if now.duration_since(since) >= BUSY_WAIT {
                        return Err(ClientError::Reconnect {
                            attempts: self.retry.failed,
                            last: format!("the daemon was still busy after {BUSY_WAIT:?}"),
                        });
                    }
                    self.retry.retry_at = now + BUSY_POLL;
                    return Ok(());
                }
                self.retry.busy_since = None;
                self.retry.failed += 1;
                self.sync_status();
                let attempts = self.config.reconnect_attempts.max(1);
                if self.retry.failed >= attempts {
                    return Err(ClientError::Reconnect {
                        attempts,
                        last: error.to_string(),
                    });
                }
                self.retry.retry_at = Instant::now() + self.retry.backoff;
                self.retry.backoff =
                    (self.retry.backoff * 2).min(self.config.reconnect_max_backoff);
                Ok(())
            }
            // `unknown_run` after the run was purged, a shutting-down daemon, and the
            // like: retrying cannot help.
            Ok(Err(error)) => Err(error),
            Err(join) => Err(ClientError::Malformed(format!(
                "the reconnect task failed: {join}"
            ))),
        }
    }

    /// The daemon's lock and socket, derived from the socket path the way
    /// `resolve_paths` derives them for a user-chosen socket.
    fn paths(&self) -> DaemonPaths {
        DaemonPaths {
            socket: self.path.clone(),
            pid_file: self.path.with_extension("pid"),
            dir_policy: DirPolicy::UserChosen,
        }
    }

    /// Validate contiguity and book-keep one event.
    fn accept(&mut self, event: EventEnvelope) -> Result<EventEnvelope, ClientError> {
        if event.run_id != self.run_id {
            return Err(ClientError::Malformed(format!(
                "an event for run {} on a stream for run {}",
                event.run_id, self.run_id
            )));
        }
        if event.seq != self.next_seq {
            return Err(ClientError::Sequence {
                expected: self.next_seq.get(),
                got: event.seq.get(),
            });
        }
        self.next_seq = event.seq.next();
        self.quiet_since = Instant::now();
        self.status
            .0
            .next_seq
            .store(self.next_seq.get(), std::sync::atomic::Ordering::Relaxed);
        if event.kind == EventKind::RunFinished {
            self.finished = true;
            self.sync_status();
        }
        Ok(event)
    }
}
