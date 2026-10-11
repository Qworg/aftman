//! The job, its configuration, and the factory state machine.

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::TransitionError;

/// Where a job came from. Recorded for auditability; it does not change behaviour.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum JobSource {
    Cli,
    Web,
    Daemon,
    File { path: PathBuf },
}

/// A free-form software job: a ticket, a specification, a bug report, a request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Job {
    /// The job text, verbatim. Never paraphrased before it reaches a worker.
    pub prompt: String,
    /// Absolute path to the repository. Validated to be a directory before a run starts.
    pub repo: PathBuf,
    pub source: JobSource,
}

/// A capped excerpt of the job, for the observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobSummary {
    pub prompt_excerpt: String,
    pub prompt_truncated: bool,
    pub repo: PathBuf,
    pub elapsed_secs: u64,
}

/// Whether independent verification is expected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerifyPolicy {
    /// Never spawn a verifier.
    Never,
    /// Let the verification responsibility decide, which is the normal case.
    #[default]
    WhenAsked,
    /// Always require a verifier before finishing.
    Always,
}

/// Which coding agent performs the work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerKind {
    Pi,
    Agy,
    Claude,
    Generic,
    HerdrPane,
    Simulated,
}

impl WorkerKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pi => "pi",
            Self::Agy => "agy",
            Self::Claude => "claude",
            Self::Generic => "generic",
            Self::HerdrPane => "herdr:pane",
            Self::Simulated => "simulated",
        }
    }
}

/// Whether a worker is doing the work or checking it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    /// A coding agent working the job.
    Code,
    /// An independent verifier that must not trust the coding agent's reports.
    Verify,
}

impl TaskKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Code => "code",
            Self::Verify => "verify",
        }
    }
}

/// How a worker ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExitCause {
    Completed,
    Failed,
    TimedOut,
    Terminated,
    LaunchFailed,
}

/// A worker's terminal status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerStatus {
    Pending,
    Running,
    Completed,
    Failed,
    TimedOut,
    Stopped,
}

impl WorkerStatus {
    #[must_use]
    pub const fn is_running(self) -> bool {
        matches!(self, Self::Pending | Self::Running)
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        !self.is_running()
    }

    /// The wire name, matching the `snake_case` serde representation.
    ///
    /// Exists so that everything reaching a model uses the same spelling as the
    /// JSON. Using `{:?}` anywhere would produce `"Completed"` and silently fail
    /// every comparison against `"completed"`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
            Self::Stopped => "stopped",
        }
    }
}

/// How a worker process exited.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerExit {
    /// Process exit code, when the platform reported one.
    pub code: Option<i32>,
    /// Terminating signal, when the worker was killed rather than exiting.
    pub signal: Option<i32>,
    pub cause: ExitCause,
}

impl WorkerExit {
    #[must_use]
    pub const fn completed(code: Option<i32>) -> Self {
        Self {
            code,
            signal: None,
            cause: ExitCause::Completed,
        }
    }

    #[must_use]
    pub const fn failed(code: Option<i32>) -> Self {
        Self {
            code,
            signal: None,
            cause: ExitCause::Failed,
        }
    }

    #[must_use]
    pub const fn timed_out() -> Self {
        Self {
            code: None,
            signal: None,
            cause: ExitCause::TimedOut,
        }
    }

    #[must_use]
    pub const fn terminated() -> Self {
        Self {
            code: None,
            signal: None,
            cause: ExitCause::Terminated,
        }
    }
}

/// Everything resolved for one run: CLI flags, environment, and config file merged.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunConfig {
    pub worker: WorkerKind,
    pub worker_model: Option<String>,
    pub verify: VerifyPolicy,
    /// Total workers allowed across the run, including verifiers and retries.
    pub max_workers: u32,
    /// Retries of the coding task.
    pub max_attempts: u32,
    pub overall_timeout: Duration,
    pub worker_timeout: Duration,
    /// Debounce window: the shortest gap between two assessment cycles.
    pub min_assessment_interval: Duration,
    /// The idle ticker, deliberately coarser than the debounce interval. A single
    /// 5-second ticker would let a silent worker exhaust the assessment budget
    /// well inside its own timeout. See `docs/02-lld.md` §7.1.
    pub idle_poll_interval: Duration,
    /// Skip the model call when the observation content is unchanged.
    pub skip_unchanged_assessments: bool,
    /// A hard ceiling on model calls per run.
    pub max_assessments: u32,
    pub steering_enabled: bool,
    pub max_steers_per_worker: u32,
    /// Traffic in `docs/01-hld.md` §9.
    pub obs_diff_chars: usize,
    pub obs_tail_chars: usize,
    pub obs_events_history: usize,
    pub obs_workers_history: usize,
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            worker: WorkerKind::Simulated,
            worker_model: None,
            verify: VerifyPolicy::WhenAsked,
            max_workers: 3,
            max_attempts: 2,
            overall_timeout: Duration::from_secs(2 * 60 * 60),
            worker_timeout: Duration::from_secs(45 * 60),
            min_assessment_interval: Duration::from_secs(5),
            idle_poll_interval: Duration::from_secs(30),
            skip_unchanged_assessments: true,
            max_assessments: 400,
            steering_enabled: true,
            max_steers_per_worker: 1,
            obs_diff_chars: 20_000,
            obs_tail_chars: 12_000,
            obs_events_history: 30,
            obs_workers_history: 10,
        }
    }
}

/// Where a run is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FactoryStatus {
    Queued,
    Routing,
    /// A worker is active.
    Running,
    /// A judgement is being formed.
    Assessing,
    /// A worker was stopped and no worker is active. **Not** terminal: the next
    /// cycle may propose `Retry`, which returns the run to `Running`.
    Stopped,
    Verifying,
    /// Waiting for a human answer. A wait state, not a failure.
    NeedsHuman,
    Finished,
    Failed,
    Cancelled,
}

impl FactoryStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Finished | Self::Failed | Self::Cancelled)
    }

    /// Whether the state machine permits `self -> next`.
    ///
    /// The table is exhaustive and lives in one place so that an unreachable
    /// transition is a compile-visible omission rather than a runtime surprise.
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        use FactoryStatus::{
            Assessing, Cancelled, Failed, Finished, NeedsHuman, Queued, Routing, Running, Stopped,
            Verifying,
        };
        match self {
            Queued => matches!(next, Routing | Cancelled | Failed),
            Routing => matches!(next, Running | Failed | Cancelled),
            Running => matches!(next, Assessing | Stopped | NeedsHuman | Failed | Cancelled),
            Assessing => matches!(
                next,
                Running | Verifying | Stopped | NeedsHuman | Finished | Failed | Cancelled
            ),
            // `Stop` terminates the worker; a later `Retry` may resume the run.
            Stopped => matches!(next, Running | Finished | NeedsHuman | Failed | Cancelled),
            Verifying => matches!(next, Running | Stopped | NeedsHuman | Failed | Cancelled),
            NeedsHuman => matches!(next, Running | Stopped | Finished | Failed | Cancelled),
            Finished | Failed | Cancelled => false,
        }
    }

    /// Apply a transition, or explain why it is illegal.
    pub fn transition(self, next: Self) -> Result<Self, TransitionError> {
        if self.can_transition_to(next) {
            Ok(next)
        } else {
            Err(TransitionError {
                from: self,
                to: next,
            })
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Routing => "routing",
            Self::Running => "running",
            Self::Assessing => "assessing",
            Self::Stopped => "stopped",
            Self::Verifying => "verifying",
            Self::NeedsHuman => "needs_human",
            Self::Finished => "finished",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl std::fmt::Display for FactoryStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::FactoryStatus::{
        self, Assessing, Cancelled, Failed, Finished, NeedsHuman, Queued, Routing, Running,
        Stopped, Verifying,
    };
    use super::{RunConfig, WorkerKind};

    const ALL: [FactoryStatus; 10] = [
        Queued, Routing, Running, Assessing, Stopped, Verifying, NeedsHuman, Finished, Failed,
        Cancelled,
    ];

    #[test]
    fn terminal_states_reject_every_attempted_transition() {
        for from in [Finished, Failed, Cancelled] {
            assert!(from.is_terminal(), "{from} should be terminal");
            for to in ALL {
                assert!(
                    !from.can_transition_to(to),
                    "{from} -> {to} must be rejected"
                );
            }
        }
    }

    #[test]
    fn non_terminal_states_have_at_least_one_way_out() {
        for from in ALL.iter().copied().filter(|s| !s.is_terminal()) {
            assert!(
                ALL.iter().any(|to| from.can_transition_to(*to)),
                "{from} has no reachable successor"
            );
        }
    }

    #[test]
    fn stop_is_not_terminal_and_retry_is_reachable() {
        // The whole stop-then-retry recovery path depends on this: `Stop` parks the
        // run in `Stopped`, whose successor set must include `Running`.
        assert!(!Stopped.is_terminal());
        assert!(Assessing.can_transition_to(Stopped));
        assert!(Stopped.can_transition_to(Running));
    }

    #[test]
    fn queued_cannot_jump_straight_to_running() {
        // Routing must happen first, so that the responsibility set is fixed
        // before any worker starts.
        assert!(!Queued.can_transition_to(Running));
        assert!(Queued.can_transition_to(Routing));
    }

    #[test]
    fn no_state_transitions_to_itself() {
        for from in ALL {
            assert!(!from.can_transition_to(from), "{from} -> {from}");
        }
    }

    #[test]
    fn transition_helper_reports_the_offending_pair() {
        let err = Finished.transition(Running).unwrap_err();
        assert_eq!(err.from, Finished);
        assert_eq!(err.to, Running);
        assert!(Finished.transition(Finished).is_err());
    }

    #[test]
    fn default_config_is_offline_safe() {
        let cfg = RunConfig::default();
        assert_eq!(cfg.worker, WorkerKind::Simulated);
        assert!(cfg.skip_unchanged_assessments);
        // The idle ticker must be strictly coarser than the debounce window,
        // otherwise a silent worker burns the assessment budget.
        assert!(cfg.idle_poll_interval > cfg.min_assessment_interval);
    }
}
