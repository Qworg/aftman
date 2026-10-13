//! The worker boundary: how aftman drives a coding agent.
//!
//! A worker is a coding agent — or a verifier — plus the small amount of protocol
//! needed to supervise it. The trait is deliberately tiny, because every backend
//! (`pi`, `agy`, `claude`, a generic command, or a herdr pane) must be able to
//! implement it without leaking its own concepts into the runtime.
//!
//! Two rules are load-bearing and are enforced by the runtime, not by convention:
//!
//! - `supports_steering` must be **honest**. A backend that cannot accept mid-turn
//!   input returns `false`, and policy degrades to stop/retry. A `true` that
//!   silently drops messages is worse than a `false`.
//! - Every child process runs in its own process group, so termination reaches
//!   grandchildren (shells, test runners, language servers).

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::ids::WorkerId;
use crate::job::{TaskKind, WorkerExit, WorkerKind, WorkerStatus};
use crate::log::{BoundedLog, Stream};

/// How a steering attempt turned out.
///
/// `Queued` and `Delivered` are distinct on purpose: a backend that reports
/// acceptance without delivery confirmation must not claim `Delivered`, because
/// policy counts steers against a budget and a queue that never drains is
/// indistinguishable from a stuck worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "outcome")]
pub enum SteerOutcome {
    /// The backend acknowledged delivery into the running turn.
    Delivered,
    /// The backend accepted the message; delivery is not confirmed.
    Queued,
    /// The backend refused, with a reason.
    Rejected { reason: String },
    /// The backend has no live input channel. Policy must not have asked.
    Unsupported,
}

impl SteerOutcome {
    #[must_use]
    pub const fn is_accepted(&self) -> bool {
        matches!(self, Self::Delivered | Self::Queued)
    }
}

/// Why a worker is being stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminateReason {
    /// The worker exceeded `worker_timeout`.
    Timeout,
    /// The operator or the daemon cancelled the run.
    Cancelled,
    /// Policy decided to stop the worker.
    Stopped,
    /// A retry superseded this worker.
    Superseded,
}

/// Who originated a steer.
///
/// Only [`SteerSource::Supervisor`] steers count against `max_steers_per_worker`: that
/// budget bounds how often *aftman's own policy* may interrupt a worker, and an
/// operator typing guidance must not use it up.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum SteerSource {
    /// A directive chosen by policy. The default, so snapshots written before this
    /// field existed load unchanged.
    #[default]
    Supervisor,
    /// A person, through `run.steer` or a `Guidance` answer.
    Human,
}

impl SteerSource {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Supervisor => "supervisor",
            Self::Human => "human",
        }
    }
}

/// One delivered steering attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SteerRecord {
    #[serde(with = "crate::timefmt")]
    pub at: OffsetDateTime,
    pub message_excerpt: String,
    pub outcome: SteerOutcome,
    pub queue_depth: Option<u32>,
    #[serde(default)]
    pub source: SteerSource,
}

/// How a worker run ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "outcome")]
pub enum WorkerOutcome {
    Completed,
    Failed {
        code: Option<i32>,
    },
    TimedOut,
    Terminated {
        reason: TerminateReason,
    },
    /// A pipe or protocol failure after launch.
    StreamError {
        message: String,
    },
}

impl WorkerOutcome {
    #[must_use]
    pub const fn is_success(&self) -> bool {
        matches!(self, Self::Completed)
    }

    #[must_use]
    pub const fn status(&self) -> WorkerStatus {
        match self {
            Self::Completed => WorkerStatus::Completed,
            Self::Failed { .. } | Self::StreamError { .. } => WorkerStatus::Failed,
            Self::TimedOut => WorkerStatus::TimedOut,
            Self::Terminated { .. } => WorkerStatus::Stopped,
        }
    }

    #[must_use]
    pub const fn exit(&self) -> Option<WorkerExit> {
        match self {
            Self::Completed => Some(WorkerExit::completed(Some(0))),
            Self::Failed { code } => Some(WorkerExit::failed(*code)),
            Self::TimedOut => Some(WorkerExit::timed_out()),
            Self::Terminated { .. } => Some(WorkerExit::terminated()),
            Self::StreamError { .. } => Some(WorkerExit::failed(None)),
        }
    }
}

/// Everything a worker needs to start.
#[derive(Debug, Clone)]
pub struct WorkerCtx {
    pub id: WorkerId,
    pub kind: WorkerKind,
    pub task: TaskKind,
    pub repo: PathBuf,
    /// The prompt, assembled deterministically and stored verbatim.
    pub mission: String,
    pub model: Option<String>,
    pub timeout: Duration,
    /// Extra environment entries, applied after scrubbing.
    pub env: Vec<(String, String)>,
    /// Where streamed output goes.
    ///
    /// Shared rather than returned, because that is what a real backend does: its
    /// pipe reader pushes chunks as they arrive and signals the runtime, so the
    /// supervisor can react long before the process exits. The lock is held only
    /// for the duration of one push.
    pub output: Arc<Mutex<BoundedLog>>,
    /// Where a backend that launches a process records it, so that a supervisor that
    /// dies abruptly cannot leave an agent running unaccounted for (see
    /// [`ProcessLedger`]). `None` for backends that launch nothing, and in tests.
    pub ledger: Option<Arc<dyn ProcessLedger>>,
}

/// Enough to find, and to be *sure* of, a worker's process group later.
///
/// Written to the run directory when a subprocess worker starts and cleared when it is
/// reaped. If the supervisor dies in between, restart recovery reads it, checks that
/// the process now at `pgid` is still the same one (`start_token`, `boot_id`), and only
/// then ends the group. A pid number alone proves nothing: pids are reused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessIdentity {
    /// The process group (and, since the worker leads it, the leader's pid).
    pub pgid: i32,
    /// The leader's start time as the OS reports it: `/proc/<pid>/stat` field 22 on
    /// Linux, `ps -o lstart=` elsewhere. `None` when it could not be read, in which case
    /// the process can never be positively identified and will not be signalled.
    pub start_token: Option<String>,
    /// The boot the process belongs to (`/proc/sys/kernel/random/boot_id` on Linux).
    pub boot_id: Option<String>,
    /// The mission file this worker was given, to be removed with the run.
    pub mission_file: Option<PathBuf>,
}

/// Where a process-launching worker records the process it started.
///
/// Implemented by the runtime over the run store. Both methods are best-effort and
/// never fail the worker: a ledger that cannot be written is logged by the
/// implementation, because refusing to run an agent over bookkeeping would be worse
/// than the (small) orphan risk it exists to cover.
#[async_trait]
pub trait ProcessLedger: Send + Sync + std::fmt::Debug {
    /// The worker's process is up.
    async fn record(&self, identity: ProcessIdentity);
    /// The worker's process has been reaped (or never started).
    async fn clear(&self);
}

/// A signal from a worker to the runtime.
///
/// Note this is *not* the persisted event log. The activity signal is unpersisted
/// and coalesced: it exists so the watcher can react within milliseconds without
/// the log growing with agent verbosity. See `docs/02-lld.md` §7.1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerSignal {
    /// New bytes arrived on stdout or stderr. Carries no payload.
    Activity,
    /// The worker finished. The outcome is also recorded on the worker record.
    Finished,
}

/// The channel a worker uses to tell the runtime something happened.
#[derive(Debug, Clone)]
pub struct WorkerEventSink {
    tx: tokio::sync::mpsc::UnboundedSender<WorkerSignal>,
}

impl WorkerEventSink {
    #[must_use]
    pub fn new(tx: tokio::sync::mpsc::UnboundedSender<WorkerSignal>) -> Self {
        Self { tx }
    }

    /// Signal that new output arrived. Cheap, lossy, and safe to call per chunk.
    pub fn activity(&self) {
        // An unbounded send cannot fail unless the receiver is gone, which means
        // the run is over; ignoring that is correct here.
        let _ = self.tx.send(WorkerSignal::Activity);
    }

    /// Signal that the worker finished.
    pub fn finished(&self) {
        let _ = self.tx.send(WorkerSignal::Finished);
    }
}

/// The runtime's record of one worker.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerRecord {
    pub id: WorkerId,
    pub kind: WorkerKind,
    pub task: TaskKind,
    pub status: WorkerStatus,
    /// The prompt actually sent, verbatim.
    pub mission: String,
    /// Which worker this one derives from (a retry, or a verifier of it).
    pub parent: Option<WorkerId>,
    pub exit: Option<WorkerExit>,
    #[serde(with = "crate::timefmt::opt", default)]
    pub started_at: Option<OffsetDateTime>,
    #[serde(with = "crate::timefmt::opt", default)]
    pub ended_at: Option<OffsetDateTime>,
    pub output: BoundedLog,
    pub steers: Vec<SteerRecord>,
    #[serde(with = "crate::timefmt::opt", default)]
    pub last_steered_at: Option<OffsetDateTime>,
    /// Backend-specific handle for a surface to deep-link to, for example a herdr
    /// pane id. `None` for a plain subprocess.
    pub artifact_ref: Option<String>,
    /// Whether the backend can accept mid-turn input. Recorded per worker so that
    /// policy can reason about it without holding the backend object.
    pub supports_steering: bool,
}

impl WorkerRecord {
    #[must_use]
    pub fn new(
        id: WorkerId,
        kind: WorkerKind,
        task: TaskKind,
        mission: impl Into<String>,
        output: BoundedLog,
        supports_steering: bool,
    ) -> Self {
        Self {
            id,
            kind,
            task,
            status: WorkerStatus::Pending,
            mission: mission.into(),
            parent: None,
            exit: None,
            started_at: None,
            ended_at: None,
            output,
            steers: Vec::new(),
            last_steered_at: None,
            artifact_ref: None,
            supports_steering,
        }
    }

    pub fn append_output(&mut self, stream: Stream, text: &str) {
        self.output.push(stream, text);
    }

    #[must_use]
    pub fn elapsed(&self, now: OffsetDateTime) -> Duration {
        match (self.started_at, self.ended_at) {
            (Some(start), Some(end)) => (end - start).try_into().unwrap_or(Duration::ZERO),
            (Some(start), None) => (now - start).try_into().unwrap_or(Duration::ZERO),
            _ => Duration::ZERO,
        }
    }

    /// Accepted steers that count against the supervisor's steer budget.
    ///
    /// Human steers are excluded: policy compares this to `max_steers_per_worker`.
    #[must_use]
    pub fn steer_count(&self) -> u32 {
        self.accepted_steers(SteerSource::Supervisor)
    }

    /// Accepted steers an operator delivered to this worker.
    #[must_use]
    pub fn human_steer_count(&self) -> u32 {
        self.accepted_steers(SteerSource::Human)
    }

    fn accepted_steers(&self, source: SteerSource) -> u32 {
        self.steers
            .iter()
            .filter(|s| s.source == source && s.outcome.is_accepted())
            .count()
            .try_into()
            .unwrap_or(u32::MAX)
    }
}

/// A coding agent that aftman can supervise.
#[async_trait]
pub trait Worker: Send + Sync {
    fn kind(&self) -> WorkerKind;

    /// Whether this backend can accept input into an in-flight turn.
    ///
    /// Must be accurate: policy will call [`Worker::steer`] if and only if this is
    /// `true`.
    fn supports_steering(&self) -> bool;

    /// A surface-visible handle, for example a herdr agent name and pane id.
    fn artifact_ref(&self) -> Option<String> {
        None
    }

    /// Run the agent to completion, streaming bounded output and signalling the
    /// runtime as it goes.
    async fn run(
        &self,
        ctx: WorkerCtx,
        sink: WorkerEventSink,
    ) -> Result<WorkerOutcome, WorkerError>;

    /// Deliver guidance into a running agent.
    async fn steer(&self, message: &str) -> Result<SteerOutcome, WorkerError>;

    /// Stop the agent promptly: interrupt, then a bounded grace, then kill the
    /// process group. Must be idempotent.
    async fn terminate(&self, reason: TerminateReason) -> Result<(), WorkerError>;
}

/// A worker failed in a way that is not the *agent's* failure.
///
/// A non-zero agent exit is a [`WorkerOutcome`], not an error here: the agent
/// failing is a normal, supervised event. This type is for aftman's own inability
/// to launch or talk to the process.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WorkerError {
    #[error("could not launch {kind}: {message}")]
    Launch { kind: String, message: String },

    #[error("worker I/O failed: {0}")]
    Io(String),

    #[error("worker is no longer running")]
    NotRunning,

    #[error("backend does not support steering")]
    SteeringUnsupported,

    #[error("backend error: {0}")]
    Backend(String),
}

#[cfg(test)]
mod tests {
    use super::{SteerOutcome, TerminateReason, WorkerOutcome, WorkerRecord};
    use crate::ids::WorkerId;
    use crate::job::{WorkerKind, WorkerStatus};
    use crate::log::BoundedLog;
    use crate::log::Stream;
    use time::OffsetDateTime;
    use time::macros::datetime;

    #[test]
    fn a_completed_worker_is_the_only_success() {
        assert!(WorkerOutcome::Completed.is_success());
        assert!(!WorkerOutcome::Failed { code: Some(1) }.is_success());
        assert!(!WorkerOutcome::TimedOut.is_success());
        assert!(
            !WorkerOutcome::Terminated {
                reason: TerminateReason::Stopped
            }
            .is_success()
        );
        assert!(
            !WorkerOutcome::StreamError {
                message: "x".into()
            }
            .is_success()
        );
    }

    #[test]
    fn outcomes_map_to_statuses_and_exits() {
        assert_eq!(WorkerOutcome::Completed.status(), WorkerStatus::Completed);
        assert_eq!(WorkerOutcome::TimedOut.status(), WorkerStatus::TimedOut);
        assert_eq!(
            WorkerOutcome::Terminated {
                reason: TerminateReason::Stopped
            }
            .status(),
            WorkerStatus::Stopped
        );
        assert!(WorkerOutcome::Completed.exit().is_some());
        assert!(
            WorkerOutcome::StreamError {
                message: String::new()
            }
            .exit()
            .is_some()
        );
    }

    #[test]
    fn queueing_is_not_the_same_as_delivering() {
        assert!(SteerOutcome::Delivered.is_accepted());
        assert!(SteerOutcome::Queued.is_accepted());
        assert!(
            !SteerOutcome::Rejected {
                reason: "busy".into()
            }
            .is_accepted()
        );
        assert!(!SteerOutcome::Unsupported.is_accepted());
    }

    #[test]
    fn steer_count_ignores_rejected_and_unsupported_attempts() {
        let mut record = WorkerRecord::new(
            WorkerId::new(),
            WorkerKind::Simulated,
            crate::job::TaskKind::Code,
            "do the thing",
            BoundedLog::new(16, 16),
            true,
        );
        let at = datetime!(2026-01-01 00:00:00 UTC);
        for outcome in [
            SteerOutcome::Queued,
            SteerOutcome::Unsupported,
            SteerOutcome::Rejected {
                reason: "blocked".into(),
            },
        ] {
            record.steers.push(super::SteerRecord {
                at,
                message_excerpt: "x".into(),
                outcome,
                queue_depth: None,
                source: super::SteerSource::Supervisor,
            });
        }
        assert_eq!(record.steer_count(), 1, "only the accepted steer counts");
    }

    #[test]
    fn human_steers_do_not_count_against_the_supervisor_budget() {
        let mut record = WorkerRecord::new(
            WorkerId::new(),
            WorkerKind::Simulated,
            crate::job::TaskKind::Code,
            "do the thing",
            BoundedLog::new(16, 16),
            true,
        );
        let at = datetime!(2026-01-01 00:00:00 UTC);
        for source in [super::SteerSource::Human, super::SteerSource::Human] {
            record.steers.push(super::SteerRecord {
                at,
                message_excerpt: "x".into(),
                outcome: SteerOutcome::Queued,
                queue_depth: None,
                source,
            });
        }
        assert_eq!(record.steer_count(), 0);
        assert_eq!(record.human_steer_count(), 2);
    }

    #[test]
    fn a_steer_record_without_a_source_loads_as_supervisor() -> Result<(), serde_json::Error> {
        let old = r#"{"at":"2026-01-01T00:00:00Z","message_excerpt":"x",
            "outcome":{"outcome":"queued"},"queue_depth":null}"#;
        let record: super::SteerRecord = serde_json::from_str(old)?;
        assert_eq!(record.source, super::SteerSource::Supervisor);
        Ok(())
    }

    #[test]
    fn elapsed_uses_the_ending_timestamp_once_set() {
        let mut record = WorkerRecord::new(
            WorkerId::new(),
            WorkerKind::Simulated,
            crate::job::TaskKind::Code,
            "m",
            BoundedLog::new(8, 8),
            true,
        );
        assert_eq!(
            record.elapsed(datetime!(2026-01-01 00:00:00 UTC)),
            std::time::Duration::ZERO
        );

        record.started_at = Some(datetime!(2026-01-01 00:00:00 UTC));
        assert_eq!(
            record.elapsed(datetime!(2026-01-01 00:00:30 UTC)),
            std::time::Duration::from_secs(30)
        );
        record.ended_at = Some(datetime!(2026-01-01 00:00:10 UTC));
        // Once ended, a later `now` must not keep inflating the duration.
        assert_eq!(
            record.elapsed(OffsetDateTime::UNIX_EPOCH),
            std::time::Duration::from_secs(10)
        );
    }

    #[test]
    fn output_appends_through_the_bounded_log() {
        let mut record = WorkerRecord::new(
            WorkerId::new(),
            WorkerKind::Simulated,
            crate::job::TaskKind::Code,
            "m",
            BoundedLog::new(4, 4),
            false,
        );
        record.append_output(Stream::Stdout, "hello world");
        assert_eq!(record.output.head(16), "hell");
        assert_eq!(record.output.tail(16), "orld");
        assert!(record.output.total_bytes() > 0);
    }
}
