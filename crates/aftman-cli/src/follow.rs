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
use aftman_daemon::protocol::{
    ErrorCode, MAX_EVENTS_LIMIT, Request, RunEventsParams, RunEventsResult, RunIdParams,
    ServerMessage, SubscriptionOpenParams, SubscriptionOpenResult,
};

use crate::client::{ClientConfig, ClientError, Connection};

/// How long a polling stream waits between looks at the log.
pub const POLL_INTERVAL: Duration = Duration::from_millis(300);

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

struct Retry {
    failed: u32,
    backoff: Duration,
    retry_at: Instant,
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
}

impl Drop for Follow {
    fn drop(&mut self) {
        if let Some(attempt) = self.attempt.take() {
            attempt.abort();
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
            },
            attempt: None,
        };
        follow.adopt(established);
        Ok(follow)
    }

    fn adopt(&mut self, established: Established) {
        self.polling = !established.live;
        self.conn = Some(established.conn);
        self.pending.extend(established.early);
        self.retry = Retry {
            failed: 0,
            backoff: self.config.reconnect_backoff,
            retry_at: Instant::now(),
        };
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
    }

    async fn next_inner(&mut self) -> Result<Option<EventEnvelope>, ClientError> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                return self.accept(event).map(Some);
            }
            if self.finished {
                return Ok(None);
            }
            if self.conn.is_none() {
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
            match conn.read_message().await {
                Ok(ServerMessage::Event(push)) => return self.accept(push.event).map(Some),
                Ok(ServerMessage::Response(_)) => {}
                Ok(ServerMessage::Error(push)) => match push.error.code {
                    ErrorCode::SubscriberLagged => {
                        // The daemon dropped us after a correct prefix. Resume from
                        // our own count, which is the authority on what we have.
                        self.stats.lagged += 1;
                        self.conn = None;
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
        let id = self.next_request_id;
        self.next_request_id += 1;
        let limit = self.config.request_timeout;
        let events = Request::RunEvents(RunEventsParams {
            run_id: self.run_id,
            from_seq: Some(self.next_seq),
            limit: Some(MAX_EVENTS_LIMIT),
        });
        let Some(conn) = self.conn.as_mut() else {
            return Ok(false);
        };
        let mut stray = VecDeque::new();
        let page: RunEventsResult = match conn.exchange(id, &events, limit, &mut stray).await {
            Ok(response) => response.into_result().map_err(ClientError::Protocol)?,
            Err(ClientError::Closed | ClientError::Io(_) | ClientError::Timeout { .. }) => {
                self.lose_connection();
                return Ok(false);
            }
            Err(other) => return Err(other),
        };
        if !page.events.is_empty() {
            self.pending.extend(page.events);
            return Ok(false);
        }
        if self.terminal_seen {
            // Terminal, and the log was empty after we saw that: it is over.
            self.finished = true;
            return Ok(true);
        }
        let id = self.next_request_id;
        self.next_request_id += 1;
        let get = Request::RunGet(RunIdParams {
            run_id: self.run_id,
        });
        let Some(conn) = self.conn.as_mut() else {
            return Ok(false);
        };
        match conn.exchange(id, &get, limit, &mut stray).await {
            Ok(response) => {
                let state: aftman_core::run::RunState =
                    response.into_result().map_err(ClientError::Protocol)?;
                if state.status.is_terminal() {
                    // Look at the log once more before declaring it over: events may
                    // have landed between the two reads.
                    self.terminal_seen = true;
                    return Ok(false);
                }
            }
            Err(ClientError::Closed | ClientError::Io(_) | ClientError::Timeout { .. }) => {
                self.lose_connection();
                return Ok(false);
            }
            Err(other) => return Err(other),
        }
        tokio::time::sleep(POLL_INTERVAL).await;
        Ok(false)
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
                self.retry.failed += 1;
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
        if event.kind == EventKind::RunFinished {
            self.finished = true;
        }
        Ok(event)
    }
}
