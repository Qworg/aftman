//! A worker that does not exist.
//!
//! `SimulatedWorker` streams a few chunks of plausible output, honours termination,
//! and accepts steering — in-process, with no subprocess, no network, and no agent
//! binary. It exists so that the whole runtime can be exercised offline: the M0
//! deliverable is a run that completes with no credentials, and the runtime's own
//! tests need a worker that cannot fail for environmental reasons.
//!
//! It deliberately does **not** touch the repository. A simulated run must never
//! leave a user's working tree dirty, so the synthetic evidence it implies is
//! produced by the runtime's observation builder in simulation mode and is labelled
//! as synthetic there.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;

use aftman_core::job::{WorkerKind, WorkerStatus};
use aftman_core::log::Stream;
use aftman_core::worker::{
    SteerOutcome, TerminateReason, Worker, WorkerCtx, WorkerError, WorkerEventSink, WorkerOutcome,
};

/// The chunks a simulated worker emits, in order.
const CHUNKS: &[&str] = &[
    "reading the job and the repository layout\n",
    "planning: add a retry helper and use it at the two call sites\n",
    "editing src/http.rs\n",
    "editing tests/http_retry.rs\n",
    "running the test suite\n",
    "test result: ok. 3 passed; 0 failed; 0 ignored\n",
];

/// A deterministic in-process worker.
#[derive(Debug, Default)]
pub struct SimulatedWorker {
    /// Set by [`Worker::terminate`]. Checked between chunks, so termination is
    /// responsive without needing to interrupt anything.
    terminated: AtomicBool,
    /// Messages accepted by [`Worker::steer`], for assertions.
    steered: Mutex<Vec<String>>,
    /// How long to pause between chunks. Kept small; a test may set it to zero.
    chunk_delay: Duration,
    /// If set, the worker reports a failure instead of completing.
    fail_with_code: Option<i32>,
}

impl SimulatedWorker {
    #[must_use]
    pub fn new() -> Self {
        Self {
            chunk_delay: Duration::from_millis(5),
            ..Self::default()
        }
    }

    /// Run with no delay at all, for tests that do not need interleaving.
    #[must_use]
    pub fn instant() -> Self {
        Self {
            chunk_delay: Duration::ZERO,
            ..Self::default()
        }
    }

    /// Report a non-zero exit instead of completing.
    #[must_use]
    pub fn failing(code: i32) -> Self {
        Self {
            fail_with_code: Some(code),
            ..Self::new()
        }
    }

    /// Messages accepted so far.
    #[must_use]
    pub fn steered_messages(&self) -> Vec<String> {
        self.steered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

#[async_trait]
impl Worker for SimulatedWorker {
    fn kind(&self) -> WorkerKind {
        WorkerKind::Simulated
    }

    fn supports_steering(&self) -> bool {
        true
    }

    async fn run(
        &self,
        ctx: WorkerCtx,
        sink: WorkerEventSink,
    ) -> Result<WorkerOutcome, WorkerError> {
        self.terminated.store(false, Ordering::SeqCst);

        for chunk in CHUNKS {
            if self.terminated.load(Ordering::SeqCst) {
                return Ok(WorkerOutcome::Terminated {
                    reason: TerminateReason::Stopped,
                });
            }
            {
                // The lock is held only for the push, exactly as a real pipe reader
                // holds it only for one chunk.
                let mut output = ctx
                    .output
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                output.push(Stream::Stdout, chunk);
            }
            // Signal after the output is visible, so a woken watcher always sees the
            // bytes that woke it.
            sink.activity();

            if !self.chunk_delay.is_zero() {
                tokio::time::sleep(self.chunk_delay).await;
            }
        }

        if self.terminated.load(Ordering::SeqCst) {
            return Ok(WorkerOutcome::Terminated {
                reason: TerminateReason::Stopped,
            });
        }
        match self.fail_with_code {
            Some(code) => Ok(WorkerOutcome::Failed { code: Some(code) }),
            None => Ok(WorkerOutcome::Completed),
        }
    }

    async fn steer(&self, message: &str) -> Result<SteerOutcome, WorkerError> {
        if self.terminated.load(Ordering::SeqCst) {
            return Ok(SteerOutcome::Rejected {
                reason: "worker is no longer running".into(),
            });
        }
        self.steered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(message.to_owned());
        // `Queued`, not `Delivered`: this backend cannot confirm that its
        // counterpart acted on the guidance, and claiming otherwise would make the
        // steer budget meaningless.
        Ok(SteerOutcome::Queued)
    }

    async fn terminate(&self, _reason: TerminateReason) -> Result<(), WorkerError> {
        self.terminated.store(true, Ordering::SeqCst);
        Ok(())
    }
}

/// The status a simulated worker ends in, for a surface to display.
#[must_use]
pub const fn simulated_status() -> WorkerStatus {
    WorkerStatus::Completed
}

#[cfg(test)]
mod tests {
    use super::{CHUNKS, SimulatedWorker};
    use aftman_core::job::{TaskKind, WorkerKind};
    use aftman_core::log::BoundedLog;
    use aftman_core::worker::{
        SteerOutcome, TerminateReason, Worker, WorkerCtx, WorkerEventSink, WorkerOutcome,
    };
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    fn ctx(output: Arc<Mutex<BoundedLog>>) -> WorkerCtx {
        WorkerCtx {
            id: aftman_core::ids::WorkerId::new(),
            kind: WorkerKind::Simulated,
            task: TaskKind::Code,
            repo: PathBuf::from("/repo"),
            mission: "add retries".into(),
            model: None,
            timeout: Duration::from_secs(30),
            env: Vec::new(),
            output,
            ledger: None,
        }
    }

    async fn run_with(
        worker: &SimulatedWorker,
    ) -> Result<(WorkerOutcome, Arc<Mutex<BoundedLog>>), Box<dyn std::error::Error>> {
        let output = Arc::new(Mutex::new(BoundedLog::new(4096, 4096)));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let sink = WorkerEventSink::new(tx);

        let outcome = worker.run(ctx(Arc::clone(&output)), sink).await?;
        // Drain the signals so the assertion below can count them.
        let mut signals = 0;
        while rx.try_recv().is_ok() {
            signals += 1;
        }
        assert!(signals > 0, "a worker must signal activity at least once");
        Ok((outcome, output))
    }

    #[tokio::test]
    async fn a_simulated_run_completes_and_streams_output() -> Result<(), Box<dyn std::error::Error>>
    {
        let worker = SimulatedWorker::instant();
        let (outcome, output) = run_with(&worker).await?;
        assert_eq!(outcome, WorkerOutcome::Completed);

        let log = output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(!log.is_empty());
        assert!(
            log.tail(4096).contains("3 passed"),
            "the simulated worker must produce a parseable test summary"
        );
        Ok(())
    }

    #[tokio::test]
    async fn simulated_failure_is_reported_not_hidden() -> Result<(), Box<dyn std::error::Error>> {
        let worker = SimulatedWorker::failing(2);
        let (outcome, _) = run_with(&worker).await?;
        assert_eq!(outcome, WorkerOutcome::Failed { code: Some(2) });
        assert!(!outcome.is_success());
        Ok(())
    }

    #[tokio::test]
    async fn terminate_before_a_run_has_nothing_to_stop() -> Result<(), Box<dyn std::error::Error>>
    {
        // The terminate flag is per-run, not per-instance: one `SimulatedWorker`
        // serves every worker in a run (the coding worker, a retry, a verifier), so a
        // flag left set by an earlier worker must not instantly kill the next one.
        // A `terminate` issued before any run therefore applies to no run at all.
        let worker = SimulatedWorker::instant();
        worker.terminate(TerminateReason::Stopped).await?;
        let (outcome, output) = run_with(&worker).await?;
        assert_eq!(outcome, WorkerOutcome::Completed);
        assert!(
            output
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .total_bytes()
                > 0
        );
        Ok(())
    }

    #[tokio::test]
    async fn terminate_stops_a_worker_that_is_already_running()
    -> Result<(), Box<dyn std::error::Error>> {
        // The case the runtime actually exercises: a `Stop` directive arriving while
        // the worker is mid-flight.
        let worker = Arc::new(SimulatedWorker::new());
        let output = Arc::new(Mutex::new(BoundedLog::new(4096, 4096)));
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let sink = WorkerEventSink::new(tx);

        let handle = {
            let worker = Arc::clone(&worker);
            let ctx = ctx(Arc::clone(&output));
            tokio::spawn(async move { worker.run(ctx, sink).await })
        };

        // Long enough for a chunk or two, short enough that the run is not over.
        tokio::time::sleep(Duration::from_millis(12)).await;
        worker.terminate(TerminateReason::Stopped).await?;

        let outcome = handle.await??;
        assert_eq!(
            outcome,
            WorkerOutcome::Terminated {
                reason: TerminateReason::Stopped
            },
            "a mid-flight terminate must end the run as Terminated"
        );

        let bytes = output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .total_bytes();
        assert!(
            bytes < (CHUNKS.iter().map(|c| c.len()).sum::<usize>() as u64),
            "termination must cut the worker short, but {bytes} bytes were emitted"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_terminated_worker_can_be_run_again() -> Result<(), Box<dyn std::error::Error>> {
        // The per-run reset: a fresh run after a terminate must not inherit the flag,
        // otherwise a `Stop` followed by a `Retry` would kill the retry instantly.
        let worker = SimulatedWorker::instant();
        worker.terminate(TerminateReason::Stopped).await?;
        let (first, _) = run_with(&worker).await?;
        assert_eq!(first, WorkerOutcome::Completed, "the flag resets per run");
        let (second, _) = run_with(&worker).await?;
        assert_eq!(second, WorkerOutcome::Completed);
        Ok(())
    }

    #[tokio::test]
    async fn terminate_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
        let worker = SimulatedWorker::instant();
        worker.terminate(TerminateReason::Timeout).await?;
        worker.terminate(TerminateReason::Timeout).await?;
        Ok(())
    }

    #[tokio::test]
    async fn steering_is_accepted_and_recorded() -> Result<(), Box<dyn std::error::Error>> {
        let worker = SimulatedWorker::instant();
        let outcome = worker.steer("please also add a timeout").await?;
        // Queued, never Delivered: this backend cannot confirm that its counterpart
        // acted on the guidance, and claiming otherwise would make the steer budget
        // meaningless.
        assert_eq!(outcome, SteerOutcome::Queued);
        assert!(outcome.is_accepted());
        assert_eq!(worker.steered_messages().len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn steering_a_stopped_worker_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let worker = Arc::new(SimulatedWorker::new());
        let output = Arc::new(Mutex::new(BoundedLog::new(4096, 4096)));
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let sink = WorkerEventSink::new(tx);

        let handle = {
            let worker = Arc::clone(&worker);
            let ctx = ctx(Arc::clone(&output));
            tokio::spawn(async move { worker.run(ctx, sink).await })
        };
        tokio::time::sleep(Duration::from_millis(12)).await;
        worker.terminate(TerminateReason::Stopped).await?;
        let _ = handle.await?;

        // Once stopped, guidance must be refused rather than silently accepted and
        // dropped — an accepted steer counts against the budget and would make a
        // stopped worker look like it might still act.
        let outcome = worker.steer("too late").await?;
        assert!(
            matches!(outcome, SteerOutcome::Rejected { .. }),
            "a stopped worker must reject steering, got {outcome:?}"
        );
        assert!(worker.steered_messages().is_empty());
        Ok(())
    }

    #[test]
    fn the_backend_advertises_itself_honestly() {
        let worker = SimulatedWorker::new();
        assert_eq!(worker.kind(), WorkerKind::Simulated);
        assert!(
            worker.supports_steering(),
            "this backend really can be steered"
        );
        assert!(
            worker.artifact_ref().is_none(),
            "there is no pane to link to"
        );
    }
}
