//! Driving a coding-agent CLI as a supervised subprocess.
//!
//! Every real backend shares this machinery, and the requirements are the same for all
//! of them:
//!
//! - **An own process group.** Coding agents spawn shells, test runners, and language
//!   servers. Killing only the direct child leaves those orphaned and still writing to
//!   the repository, so the child is made a process-group leader and termination
//!   signals the whole group.
//! - **Bounded output.** A worker that prints ten gigabytes must not be able to grow
//!   the supervisor's footprint, so reads are capped and evicted through
//!   [`BoundedLog`].
//! - **Interrupt before kill.** `SIGINT` first, then a bounded grace period, then
//!   `SIGKILL` to the group. A coding agent that is mid-edit deserves the chance to
//!   finish the write it started.
//! - **A scrubbed environment.** Built through
//!   [`worker_environment`](crate::worker_environment), never inherited raw.
//!
//! See `docs/04-workers.md` §11.

use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc;

use aftman_core::activity::StreamActivity;
use aftman_core::job::WorkerKind;
use aftman_core::log::Stream;
use aftman_core::worker::{
    SteerOutcome, TerminateReason, Worker, WorkerCtx, WorkerError, WorkerEventSink, WorkerOutcome,
};

use crate::backends::{BackendOptions, SteerProtocol, build, supports_steering};
use crate::output::StreamDecoder;

/// How long to wait between `SIGINT` and `SIGKILL`.
pub const DEFAULT_TERMINATE_GRACE: Duration = Duration::from_secs(5);

/// The bytes read from a pipe in one call.
const READ_CHUNK: usize = 8 * 1024;

/// How to launch one backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSpec {
    pub program: String,
    pub args: Vec<String>,
    /// Extra environment entries, applied after scrubbing.
    pub env: Vec<(String, String)>,
    /// How the mission reaches the agent, and whether it can be steered.
    pub steer: SteerProtocol,
    /// Retained for compatibility; it no longer has any effect. **Every** worker runs
    /// with its working directory set to the run's repository, whatever backend
    /// built the command. (A worker that inherited the daemon's cwd would edit
    /// whatever directory the first CLI happened to auto-start the daemon from.)
    pub working_dir_is_repo: bool,
}

impl CommandSpec {
    /// The argv as a single string, for logging. Never contains the environment.
    #[must_use]
    pub fn display(&self) -> String {
        let mut out = self.program.clone();
        for arg in &self.args {
            out.push(' ');
            // A mission is long and multi-line; show that it is there without
            // dumping it into every log line.
            if arg.len() > 120 {
                out.push_str(&format!("<{} bytes>", arg.len()));
            } else {
                out.push_str(arg);
            }
        }
        out
    }
}

/// Where the argv comes from.
///
/// A one-shot backend embeds the mission in argv, and the mission is only known when
/// `run` is called — the same worker instance serves the coding worker and the verifier,
/// whose missions differ. So the spec is built per run, not at construction.
#[derive(Debug, Clone)]
enum SpecSource {
    /// A fully-formed argv. Used by tests and by any caller that already knows it.
    Fixed(CommandSpec),
    /// Built from the run's mission by the backend builders.
    Backend {
        options: BackendOptions,
        mission_dir: std::path::PathBuf,
    },
}

/// A worker backed by a subprocess.
///
/// State is split so that nothing is held across an `await`:
///
/// - the **process-group id** is a plain atomic. `terminate` only needs the group to
///   signal, and it must not have to wait for `run` to finish first — that is the whole
///   point of being able to terminate a running worker.
/// - **stdin** is behind an async mutex, which `steer` is the only user of.
/// - the **child handle** stays local to `run`, which is the only place that waits on it.
///
/// An earlier layout kept the child behind a `std::sync::Mutex` and locked it across
/// `child.wait()`; that both blocks `terminate` until the worker exits and makes the
/// future non-`Send`, so it cannot implement `Worker` at all.
pub struct SubprocessWorker {
    kind: WorkerKind,
    source: SpecSource,
    terminate_grace: Duration,
    /// The process group to signal. Zero means no worker is running.
    pgid: std::sync::atomic::AtomicI32,
    /// Set by `terminate` once it has a group to signal, so that `run` finishes the
    /// job with a `SIGKILL` of the whole group after the child is gone: an agent
    /// that exits on `SIGINT` can leave a grandchild that ignored it.
    terminating: std::sync::atomic::AtomicBool,
    stdin: tokio::sync::Mutex<Option<ChildStdin>>,
}

/// A mission file that is removed when this goes out of scope: when the worker ends by any
/// route, including the `run` future being dropped. (If the whole process is killed
/// instead, restart recovery removes the file named in the worker's recorded identity.)
struct MissionFile(std::path::PathBuf);

impl Drop for MissionFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Kills the process group if the `run` future is dropped (aborted) while the
/// worker is live. `kill_on_drop` alone reaches only the direct child.
struct GroupGuard<'a> {
    pgid: i32,
    slot: &'a std::sync::atomic::AtomicI32,
    armed: bool,
}

impl Drop for GroupGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            SubprocessWorker::signal_group(self.pgid, Signal::SIGKILL);
            let _ = self.slot.compare_exchange(
                self.pgid,
                0,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            );
        }
    }
}

impl std::fmt::Debug for SubprocessWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubprocessWorker")
            .field("kind", &self.kind)
            .field("source", &self.source)
            .finish()
    }
}

impl SubprocessWorker {
    /// A worker with a fixed argv.
    #[must_use]
    pub fn new(kind: WorkerKind, spec: CommandSpec) -> Self {
        Self {
            kind,
            source: SpecSource::Fixed(spec),
            terminate_grace: DEFAULT_TERMINATE_GRACE,
            pgid: std::sync::atomic::AtomicI32::new(0),
            terminating: std::sync::atomic::AtomicBool::new(false),
            stdin: tokio::sync::Mutex::new(None),
        }
    }

    /// A worker that builds its argv per run from the mission.
    ///
    /// `mission_dir` is where a `generic` backend writes its mission file.
    #[must_use]
    pub fn backend(
        kind: WorkerKind,
        options: BackendOptions,
        mission_dir: std::path::PathBuf,
    ) -> Self {
        Self {
            kind,
            source: SpecSource::Backend {
                options,
                mission_dir,
            },
            terminate_grace: DEFAULT_TERMINATE_GRACE,
            pgid: std::sync::atomic::AtomicI32::new(0),
            terminating: std::sync::atomic::AtomicBool::new(false),
            stdin: tokio::sync::Mutex::new(None),
        }
    }

    /// Whether this backend advertises a steering channel.
    #[must_use]
    pub fn steering_available(&self) -> bool {
        match &self.source {
            SpecSource::Fixed(spec) => spec.steer.supports_steering(),
            SpecSource::Backend { options, .. } => supports_steering(self.kind, options),
        }
    }

    /// The argv for this run, and the guard for any mission file written for it. The
    /// guard exists *before* the file does, so a failed build cannot leave one behind.
    fn resolve_spec(
        &self,
        ctx: &WorkerCtx,
    ) -> Result<(CommandSpec, Option<MissionFile>), WorkerError> {
        match &self.source {
            SpecSource::Fixed(spec) => Ok((spec.clone(), None)),
            SpecSource::Backend {
                options,
                mission_dir,
            } => {
                let mission_file = mission_dir.join(format!("mission-{}.md", ctx.id));
                let guard = MissionFile(mission_file.clone());
                let spec = build(self.kind, &ctx.mission, options, &mission_file, &ctx.repo)
                    .map_err(|error| WorkerError::Launch {
                        kind: self.kind.as_str().to_owned(),
                        message: error.to_string(),
                    })?;
                Ok((spec, Some(guard)))
            }
        }
    }

    #[must_use]
    pub fn with_terminate_grace(mut self, grace: Duration) -> Self {
        self.terminate_grace = grace;
        self
    }

    /// The argv, when it is fixed. `None` for a per-run backend.
    #[must_use]
    pub fn fixed_spec(&self) -> Option<&CommandSpec> {
        match &self.source {
            SpecSource::Fixed(spec) => Some(spec),
            SpecSource::Backend { .. } => None,
        }
    }

    fn current_pgid(&self) -> i32 {
        self.pgid.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Launch the command, returning the child and its process-group id.
    fn spawn(&self, ctx: &WorkerCtx, spec: &CommandSpec) -> Result<(Child, i32), WorkerError> {
        let mut command = Command::new(&spec.program);
        command.args(&spec.args);
        // Always the run's repository, for every backend including `generic`.
        command.current_dir(&ctx.repo);
        // Scrubbed, then the caller's extras, then the backend's own.
        command.env_clear();
        command.envs(crate::worker_environment(&ctx.env));
        command.envs(spec.env.iter().cloned());
        // stdin follows the steer protocol, and this is not cosmetic. A one-shot backend
        // does not read it, but an *open* pipe is not the same as no input: a CLI that
        // drains stdin before starting (pi does) reads until EOF, and a pipe nobody writes
        // to never reaches EOF. Every one-shot run then hung until the 45-minute worker
        // timeout, silently — which is what the first live run found.
        //
        // `Stdio::null()` gives an immediate EOF, which is what "no input" means.
        command.stdin(match spec.steer {
            SteerProtocol::None => std::process::Stdio::null(),
            SteerProtocol::PiRpc => std::process::Stdio::piped(),
        });
        command.stdout(std::process::Stdio::piped());
        command.stderr(std::process::Stdio::piped());
        // Its own process group, so termination reaches grandchildren.
        command.process_group(0);
        command.kill_on_drop(true);

        let mut child = command.spawn().map_err(|error| WorkerError::Launch {
            kind: spec.program.clone(),
            message: error.to_string(),
        })?;
        let pid = child.id().ok_or_else(|| WorkerError::Launch {
            kind: spec.program.clone(),
            message: "the child exited before it could be identified".to_owned(),
        })?;
        let pgid = pid as i32;
        self.terminating
            .store(false, std::sync::atomic::Ordering::SeqCst);
        self.pgid.store(pgid, std::sync::atomic::Ordering::SeqCst);

        // Move stdin into the shared slot so steering can reach it.
        if let Some(stdin) = child.stdin.take() {
            let mut slot = self.stdin.try_lock();
            if let Ok(slot) = &mut slot {
                **slot = Some(stdin);
            }
        }
        Ok((child, pgid))
    }

    /// Signal the whole process group.
    fn signal_group(pgid: i32, signal: Signal) {
        // A negative pid targets the process group. Failure is not fatal: the group
        // may already be gone, which is the outcome we wanted.
        let _ = kill(Pid::from_raw(-pgid), signal);
    }

    /// Interrupt the group and, if it does not go quietly, kill it.
    async fn stop_group(&self, pgid: i32) {
        if pgid == 0 {
            return;
        }
        tracing::debug!(pgid, "interrupting the worker process group");
        Self::signal_group(pgid, Signal::SIGINT);
        tokio::time::sleep(self.terminate_grace).await;
        // Always follow with a kill: signalling a group that is already gone is
        // harmless, and checking first would need the child handle that `run` owns.
        tracing::debug!(pgid, "killing the worker process group");
        Self::signal_group(pgid, Signal::SIGKILL);
    }

    /// Write one JSONL command to the child's stdin.
    async fn write_stdin(&self, line: &str) -> Result<(), WorkerError> {
        let mut slot = self.stdin.lock().await;
        let Some(stdin) = slot.as_mut() else {
            return Err(WorkerError::SteeringUnsupported);
        };
        let payload = format!("{line}\n");
        // Bounded, so a child that has stopped reading cannot wedge the runtime.
        let write = async {
            stdin.write_all(payload.as_bytes()).await?;
            stdin.flush().await
        };
        match tokio::time::timeout(Duration::from_millis(500), write).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(WorkerError::Io(error.to_string())),
            Err(_) => Err(WorkerError::Backend(
                "the agent is not reading its input channel".to_owned(),
            )),
        }
    }

    /// Forget the live worker, but only if `pgid` is still the registered one: a
    /// superseded `run` finishing late must not zero the group a newer worker just
    /// stored, which would turn its `terminate` and `steer` into no-ops.
    fn clear_live(&self, pgid: i32) {
        let cleared = self
            .pgid
            .compare_exchange(
                pgid,
                0,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .is_ok();
        if cleared && let Ok(mut slot) = self.stdin.try_lock() {
            *slot = None;
        }
    }
}

/// Read one pipe into the shared bounded log, signalling as it goes.
async fn pump(
    mut reader: impl AsyncReadExt + Unpin,
    stream: Stream,
    output: std::sync::Arc<Mutex<aftman_core::log::BoundedLog>>,
    activity: std::sync::Arc<Mutex<Option<StreamActivity>>>,
    sink: WorkerEventSink,
    kind: WorkerKind,
) {
    // Only stdout carries the backend's structured stream. Stderr is the CLI's own
    // diagnostics in whatever shape the CLI felt like emitting, so it passes through.
    let mut decoder = match stream {
        Stream::Stdout => Some(StreamDecoder::new(kind)),
        Stream::Stderr => None,
    };
    let mut buffer = vec![0u8; READ_CHUNK];
    loop {
        match reader.read(&mut buffer).await {
            Ok(0) => break,
            Ok(read) => {
                // Lossy: an agent's output is not guaranteed to be valid UTF-8, and
                // dropping the process over one bad byte would be absurd.
                let text = String::from_utf8_lossy(&buffer[..read]);
                {
                    let mut log = output
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    match &mut decoder {
                        // A structured stream becomes the agent's own words and the tools
                        // it ran, which is what a model judging it and a human watching it
                        // both need. The JSON does not reach the log, so a bounded output
                        // tail spends its bytes on prose rather than on envelope keys.
                        Some(decoder) => decoder.push(&text, &mut |line| {
                            log.push(Stream::Stdout, line);
                            log.push(Stream::Stdout, "\n");
                        }),
                        None => log.push(stream, &text),
                    }
                }
                // Publish what the stream has said so far. A worker that is looping is
                // exactly the one worth judging *before* it finishes, so this cannot wait
                // for the process to exit.
                if let Some(decoder) = decoder.as_ref() {
                    *activity
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) =
                        Some(decoder.activity().clone());
                }
                // Signal after the bytes are visible, so a woken watcher always sees
                // the output that woke it.
                sink.activity();
            }
            Err(error) => {
                tracing::debug!(%error, "worker pipe read ended");
                break;
            }
        }
    }
    // A backend that exits mid-line still said something.
    if let Some(mut decoder) = decoder {
        let mut log = output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        decoder.finish(&mut |line| {
            log.push(Stream::Stdout, line);
            log.push(Stream::Stdout, "\n");
        });
    }
}

#[async_trait]
impl Worker for SubprocessWorker {
    fn kind(&self) -> WorkerKind {
        self.kind
    }

    fn supports_steering(&self) -> bool {
        self.steering_available()
    }

    async fn run(
        &self,
        ctx: WorkerCtx,
        sink: WorkerEventSink,
    ) -> Result<WorkerOutcome, WorkerError> {
        let (spec, mission_file) = self.resolve_spec(&ctx)?;
        let (mut child, pgid) = self.spawn(&ctx, &spec)?;
        let mut guard = GroupGuard {
            pgid,
            slot: &self.pgid,
            armed: true,
        };
        // Leave a trail in the run directory so that, if the supervisor dies before it
        // reaps this process, recovery can find and end it.
        if let Some(ledger) = &ctx.ledger {
            let mission_path = mission_file.as_ref().map(|m| m.0.clone());
            ledger
                .record(crate::process::identify(pgid, mission_path))
                .await;
        }

        // For an RPC backend the mission is the first line of input, not an argument.
        if let Some(initial) = spec.steer.initial_prompt(&ctx.mission)
            && let Err(error) = self.write_stdin(&initial).await
        {
            tracing::warn!(%error, "could not deliver the mission over stdin");
        }

        let stdout_task = child.stdout.take().map(|pipe| {
            tokio::spawn(pump(
                pipe,
                Stream::Stdout,
                std::sync::Arc::clone(&ctx.output),
                std::sync::Arc::clone(&ctx.activity),
                sink.clone(),
                self.kind,
            ))
        });
        let stderr_task = child.stderr.take().map(|pipe| {
            tokio::spawn(pump(
                pipe,
                Stream::Stderr,
                std::sync::Arc::clone(&ctx.output),
                std::sync::Arc::clone(&ctx.activity),
                sink.clone(),
                self.kind,
            ))
        });

        let outcome = match tokio::time::timeout(ctx.timeout, child.wait()).await {
            Ok(Ok(status)) => {
                if self.terminating.load(std::sync::atomic::Ordering::SeqCst) {
                    Self::signal_group(pgid, Signal::SIGKILL);
                }
                if status.success() {
                    WorkerOutcome::Completed
                } else {
                    WorkerOutcome::Failed {
                        code: status.code(),
                    }
                }
            }
            Ok(Err(error)) => {
                guard.armed = false;
                self.clear_live(pgid);
                if let Some(ledger) = &ctx.ledger {
                    ledger.clear().await;
                }
                return Err(WorkerError::Io(error.to_string()));
            }
            Err(_elapsed) => {
                // Timed out: interrupt, then kill the group.
                self.stop_group(pgid).await;
                let _ = child.wait().await;
                WorkerOutcome::TimedOut
            }
        };

        // Let the pipes drain before returning, so the final output reaches the log.
        for task in [stdout_task, stderr_task].into_iter().flatten() {
            let _ = task.await;
        }
        guard.armed = false;
        self.clear_live(pgid);
        if let Some(ledger) = &ctx.ledger {
            ledger.clear().await;
        }
        sink.finished();
        Ok(outcome)
    }

    async fn steer(&self, message: &str) -> Result<SteerOutcome, WorkerError> {
        let protocol = match &self.source {
            SpecSource::Fixed(spec) => spec.steer,
            SpecSource::Backend { options, .. } => {
                if supports_steering(self.kind, options) {
                    SteerProtocol::PiRpc
                } else {
                    SteerProtocol::None
                }
            }
        };
        let Some(line) = protocol.steer_command(message) else {
            return Ok(SteerOutcome::Unsupported);
        };
        if self.current_pgid() == 0 {
            // Accepting a steer for a worker that is not running would count against
            // the steer budget and misrepresent what happened.
            return Ok(SteerOutcome::Rejected {
                reason: "worker is not running".to_owned(),
            });
        }
        match self.write_stdin(&line).await {
            Ok(()) => {
                // `Queued`, not `Delivered`: pi documents that a steer is delivered
                // after the current turn's tool calls finish, and does not guarantee it
                // stays queued. Claiming delivery would make the steer budget
                // meaningless.
                Ok(SteerOutcome::Queued)
            }
            Err(WorkerError::SteeringUnsupported) => Ok(SteerOutcome::Unsupported),
            Err(error) => Ok(SteerOutcome::Rejected {
                reason: error.to_string(),
            }),
        }
    }

    async fn terminate(&self, reason: TerminateReason) -> Result<(), WorkerError> {
        let pgid = self.current_pgid();
        if pgid == 0 {
            // Terminating a worker that never started is a no-op, and stays one.
            return Ok(());
        }
        tracing::debug!(pgid, ?reason, "terminating the worker process group");
        self.terminating
            .store(true, std::sync::atomic::Ordering::SeqCst);
        // `run` observes the process exit and returns; this call does not need to wait
        // for it, which is what lets a termination interrupt a running worker.
        self.stop_group(pgid).await;
        Ok(())
    }
}

/// A sink and its receiver, for callers that need to drive a worker directly.
#[must_use]
pub fn signal_channel() -> (
    WorkerEventSink,
    mpsc::UnboundedReceiver<aftman_core::worker::WorkerSignal>,
) {
    let (tx, rx) = mpsc::unbounded_channel();
    (WorkerEventSink::new(tx), rx)
}

#[cfg(test)]
mod tests {
    use super::{CommandSpec, DEFAULT_TERMINATE_GRACE, SubprocessWorker};
    use crate::backends::SteerProtocol;
    use aftman_core::ids::WorkerId;
    use aftman_core::job::{TaskKind, WorkerKind};
    use aftman_core::log::BoundedLog;
    use aftman_core::worker::{
        SteerOutcome, TerminateReason, Worker, WorkerCtx, WorkerError, WorkerEventSink,
        WorkerOutcome,
    };
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::mpsc;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn spec(script: &str) -> CommandSpec {
        CommandSpec {
            program: "/bin/sh".to_owned(),
            args: vec!["-c".to_owned(), script.to_owned()],
            env: Vec::new(),
            steer: SteerProtocol::None,
            working_dir_is_repo: true,
        }
    }

    fn ctx(output: Arc<Mutex<BoundedLog>>, timeout: Duration) -> WorkerCtx {
        WorkerCtx {
            id: WorkerId::new(),
            kind: WorkerKind::Generic,
            task: TaskKind::Code,
            repo: std::env::temp_dir(),
            mission: "do the thing".to_owned(),
            model: None,
            timeout,
            env: Vec::new(),
            output,
            activity: Arc::new(Mutex::new(None)),
            ledger: None,
        }
    }

    /// Run a worker to completion, returning its outcome and captured output.
    async fn drive(
        worker: &SubprocessWorker,
        context: WorkerCtx,
    ) -> Result<(WorkerOutcome, String), WorkerError> {
        let log = Arc::clone(&context.output);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let sink = WorkerEventSink::new(tx);
        let outcome = worker.run(context, sink).await?;
        // Drain so the caller can assert that activity was signalled.
        let mut signals = 0;
        while rx.try_recv().is_ok() {
            signals += 1;
        }
        assert!(signals > 0, "a running worker must signal activity");
        let text = log
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .tail(64 * 1024)
            .to_owned();
        Ok((outcome, text))
    }

    fn fresh_log() -> Arc<Mutex<BoundedLog>> {
        Arc::new(Mutex::new(BoundedLog::new(64 * 1024, 64 * 1024)))
    }

    #[tokio::test]
    async fn a_zero_exit_is_completion_and_its_output_is_captured() -> TestResult {
        let worker = SubprocessWorker::new(WorkerKind::Generic, spec("echo hello; echo oops >&2"));
        let (outcome, text) = drive(&worker, ctx(fresh_log(), Duration::from_secs(10))).await?;
        assert_eq!(outcome, WorkerOutcome::Completed);
        assert!(text.contains("hello"), "{text}");
        assert!(text.contains("oops"), "stderr must be captured too: {text}");
        Ok(())
    }

    #[tokio::test]
    async fn a_one_shot_backend_gets_an_immediate_eof_on_stdin() -> TestResult {
        // The regression this exists for: a one-shot CLI that drains stdin before starting
        // (pi does) must not wait for an EOF that never arrives. `cat` blocks exactly when
        // stdin is an open pipe and exits at once when it is closed, so this distinguishes
        // the two without needing a real agent binary.
        let worker = SubprocessWorker::new(WorkerKind::Generic, spec("cat; echo done"));
        let (outcome, text) = drive(&worker, ctx(fresh_log(), Duration::from_secs(5))).await?;
        assert_eq!(
            outcome,
            WorkerOutcome::Completed,
            "a one-shot backend must not wait on stdin: {text}"
        );
        assert!(text.contains("done"), "{text}");
        Ok(())
    }

    #[tokio::test]
    async fn a_steering_backend_takes_its_mission_over_stdin() -> TestResult {
        // The other half of the same decision: an RPC backend is fed on stdin, so the pipe
        // must stay open and writable. `Generic` is used only so the echo is not decoded;
        // the mechanism under test is that `SteerProtocol::PiRpc` selects a pipe.
        let mut rpc = spec("head -n 1");
        rpc.steer = SteerProtocol::PiRpc;
        let worker = SubprocessWorker::new(WorkerKind::Generic, rpc);
        let (_, text) = drive(&worker, ctx(fresh_log(), Duration::from_secs(5))).await?;
        assert!(
            text.contains("do the thing"),
            "the mission must reach the child over stdin: {text}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_structured_backend_reaches_the_log_as_prose_not_json() -> TestResult {
        // The recorded `pi --mode json` stream, played back through the real pipe path.
        // This is what makes the decoder live: a model judging this worker, and a human
        // watching it, must see the agent's words and the tools it ran, not envelope keys.
        // The fixture is `cat`ed rather than embedded because it contains a single quote.
        let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/pi/one-shot.jsonl");
        let worker = SubprocessWorker::new(
            WorkerKind::Pi,
            spec(&format!("cat '{}'", fixture.display())),
        );
        let (outcome, text) = drive(&worker, ctx(fresh_log(), Duration::from_secs(10))).await?;
        assert_eq!(outcome, WorkerOutcome::Completed);
        assert!(
            text.contains("▸ read"),
            "the tool call should be named in the log: {text}"
        );
        assert!(
            text.contains("done"),
            "the agent's words should be in the log: {text}"
        );
        assert!(
            !text.contains("\"type\":"),
            "no raw JSON envelope should reach the log: {text}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_structured_backend_publishes_what_the_agent_did() -> TestResult {
        // The whole chain in one test: bytes from the recorded `pi --mode json` stream,
        // through the decoder, into the slot the runtime copies onto the worker record.
        // The recording has one `read` call and the reply "done".
        let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/pi/one-shot.jsonl");
        let worker = SubprocessWorker::new(
            WorkerKind::Pi,
            spec(&format!("cat '{}'", fixture.display())),
        );
        let context = ctx(fresh_log(), Duration::from_secs(10));
        let published = Arc::clone(&context.activity);
        let (outcome, _) = drive(&worker, context).await?;
        assert_eq!(outcome, WorkerOutcome::Completed);

        let activity = published
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or("a structured stream must publish what it decoded")?;
        assert_eq!(activity.tool_calls(), 1);
        assert_eq!(activity.tool_names(), vec!["read"]);
        assert_eq!(activity.failed_tools(), 0);
        assert_eq!(activity.assistant_text.trim(), "done");
        assert!(activity.usage.is_some(), "pi reports usage");
        assert_eq!(activity.aborted, Some(false));
        Ok(())
    }

    #[tokio::test]
    async fn activity_is_visible_while_the_worker_is_still_running() -> TestResult {
        // The property that makes the summary worth having. A worker that is looping has to
        // read as looping *before* it finishes, or the supervisor only finds out once the
        // work is done and the bypass already happened.
        let worker = SubprocessWorker::new(
            WorkerKind::Pi,
            spec("printf '{\"type\":\"tool_execution_start\",\"toolName\":\"read\"}\\n'; sleep 30"),
        )
        .with_terminate_grace(Duration::from_millis(200));
        let context = ctx(fresh_log(), Duration::from_secs(30));
        let published = Arc::clone(&context.activity);
        let (tx, _rx) = mpsc::unbounded_channel();
        let running =
            tokio::spawn(async move { worker.run(context, WorkerEventSink::new(tx)).await });

        // Poll while the process is still alive: `sleep 30` is nowhere near done when the
        // first line has been decoded.
        let mut seen = false;
        for _ in 0..200 {
            let current = published
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            if current.is_some_and(|a| a.tool_calls() == 1) {
                seen = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        running.abort();
        assert!(
            seen,
            "the summary must be published before the worker exits"
        );
        Ok(())
    }

    #[tokio::test]
    async fn an_unstructured_backend_keeps_its_output_verbatim() -> TestResult {
        // The other half of the switch. A `Generic` worker's own output is already the
        // readable form, so decoding it would corrupt the only copy of it.
        let worker =
            SubprocessWorker::new(WorkerKind::Generic, spec("printf 'hello {\"a\":1}\\n'"));
        let (_, text) = drive(&worker, ctx(fresh_log(), Duration::from_secs(10))).await?;
        assert!(text.contains("hello {\"a\":1}"), "{text}");
        Ok(())
    }

    #[tokio::test]
    async fn a_non_zero_exit_is_a_failed_outcome_not_an_error() -> TestResult {
        // The agent failing is a normal supervised event, not a supervisor fault.
        let worker = SubprocessWorker::new(WorkerKind::Generic, spec("exit 3"));
        let (outcome, _) = drive(&worker, ctx(fresh_log(), Duration::from_secs(10))).await?;
        assert_eq!(outcome, WorkerOutcome::Failed { code: Some(3) });
        assert!(!outcome.is_success());
        Ok(())
    }

    #[tokio::test]
    async fn a_missing_binary_is_a_launch_error() -> TestResult {
        let worker = SubprocessWorker::new(
            WorkerKind::Generic,
            CommandSpec {
                program: "/definitely/not/a/binary".to_owned(),
                args: Vec::new(),
                env: Vec::new(),
                steer: SteerProtocol::None,
                working_dir_is_repo: false,
            },
        );
        let (tx, _rx) = mpsc::unbounded_channel();
        let error = worker
            .run(
                ctx(fresh_log(), Duration::from_secs(5)),
                WorkerEventSink::new(tx),
            )
            .await
            .err()
            .ok_or("a missing binary must not succeed")?;
        assert!(matches!(error, WorkerError::Launch { .. }), "{error:?}");
        Ok(())
    }

    #[tokio::test]
    async fn a_timeout_interrupts_and_reports_timed_out() -> TestResult {
        let worker = SubprocessWorker::new(WorkerKind::Generic, spec("sleep 60"))
            .with_terminate_grace(Duration::from_millis(200));
        let started = std::time::Instant::now();
        let (outcome, _) = drive(&worker, ctx(fresh_log(), Duration::from_millis(150))).await?;
        assert_eq!(outcome, WorkerOutcome::TimedOut);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "termination must not wait out the grace period when the child is gone"
        );
        Ok(())
    }

    #[tokio::test]
    async fn termination_reaches_grandchildren() -> TestResult {
        // The reason for the process group: a shell that spawns `sleep` and waits would
        // leave the grandchild running if only the direct child were signalled.
        let worker = Arc::new(
            SubprocessWorker::new(WorkerKind::Generic, spec("sh -c 'sleep 60' & wait"))
                .with_terminate_grace(Duration::from_millis(200)),
        );
        let (tx, _rx) = mpsc::unbounded_channel();
        let context = ctx(fresh_log(), Duration::from_secs(30));
        let handle = {
            let worker = Arc::clone(&worker);
            tokio::spawn(async move { worker.run(context, WorkerEventSink::new(tx)).await })
        };

        tokio::time::sleep(Duration::from_millis(80)).await;
        worker.terminate(TerminateReason::Stopped).await?;

        let outcome = handle.await??;
        assert!(!outcome.is_success(), "{outcome:?}");
        Ok(())
    }

    #[tokio::test]
    async fn terminate_is_idempotent() -> TestResult {
        let worker = SubprocessWorker::new(WorkerKind::Generic, spec("sleep 60"));
        worker.terminate(TerminateReason::Cancelled).await?;
        worker.terminate(TerminateReason::Cancelled).await?;
        Ok(())
    }

    #[tokio::test]
    async fn steering_a_non_steerable_backend_reports_unsupported() -> TestResult {
        let worker = SubprocessWorker::new(WorkerKind::Generic, spec("cat"));
        assert!(!worker.supports_steering());
        assert_eq!(
            worker.steer("hello").await?,
            SteerOutcome::Unsupported,
            "an honest false, not a silent drop"
        );
        Ok(())
    }

    #[tokio::test]
    async fn steering_a_worker_that_is_not_running_is_rejected() -> TestResult {
        let worker = SubprocessWorker::new(
            WorkerKind::Pi,
            CommandSpec {
                program: "/bin/sh".to_owned(),
                args: vec!["-c".to_owned(), "cat".to_owned()],
                env: Vec::new(),
                steer: SteerProtocol::PiRpc,
                working_dir_is_repo: false,
            },
        );
        assert!(worker.supports_steering());
        let outcome = worker.steer("too early").await?;
        assert!(
            matches!(outcome, SteerOutcome::Rejected { .. }),
            "{outcome:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn the_environment_is_scrubbed_before_the_child_sees_it() -> TestResult {
        // The child's own view of the variable is the evidence.
        let worker = SubprocessWorker::new(
            WorkerKind::Generic,
            spec("echo \"key=${TYPESAFE_API_KEY:-absent}\" "),
        );
        let (_, text) = drive(&worker, ctx(fresh_log(), Duration::from_secs(10))).await?;
        assert!(text.contains("key=absent"), "the child saw {text:?}");
        Ok(())
    }

    #[tokio::test]
    async fn a_worker_runs_in_the_repository_directory() -> TestResult {
        let dir = tempfile::tempdir()?;
        let worker = SubprocessWorker::new(WorkerKind::Generic, spec("pwd"));
        let mut context = ctx(fresh_log(), Duration::from_secs(10));
        context.repo = dir.path().to_path_buf();
        let (outcome, text) = drive(&worker, context).await?;
        assert_eq!(outcome, WorkerOutcome::Completed);
        // `pwd` may report the symlinked /tmp path, so compare the final component.
        let leaf = dir
            .path()
            .file_name()
            .ok_or("a tempdir name")?
            .to_string_lossy()
            .into_owned();
        assert!(text.contains(&leaf), "expected {leaf:?} in {text:?}");
        Ok(())
    }

    #[test]
    fn the_grace_period_default_is_bounded() {
        assert_eq!(DEFAULT_TERMINATE_GRACE, Duration::from_secs(5));
        assert!(DEFAULT_TERMINATE_GRACE <= Duration::from_secs(10));
    }

    #[test]
    fn the_argv_display_truncates_a_long_mission() {
        let spec = CommandSpec {
            program: "agent".to_owned(),
            args: vec!["--print".to_owned(), "x".repeat(500)],
            env: vec![("SECRET".to_owned(), "hunter2".to_owned())],
            steer: SteerProtocol::None,
            working_dir_is_repo: false,
        };
        let rendered = spec.display();
        assert!(rendered.contains("<500 bytes>"), "{rendered}");
        assert!(
            !rendered.contains("hunter2"),
            "the environment is never part of the argv display"
        );
    }

    // ---- cwd, mission file, and the process ledger --------------------------------

    use crate::backends::{BackendOptions, PermissionMode};
    use aftman_core::worker::{ProcessIdentity, ProcessLedger};

    #[derive(Debug, Default)]
    struct TestLedger {
        events: Mutex<Vec<String>>,
        recorded: Mutex<Option<ProcessIdentity>>,
    }

    #[async_trait::async_trait]
    impl ProcessLedger for TestLedger {
        async fn record(&self, identity: ProcessIdentity) {
            self.events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push("record".into());
            *self
                .recorded
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(identity);
        }
        async fn clear(&self) {
            self.events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push("clear".into());
        }
    }

    fn generic_worker(template: &str, mission_dir: &std::path::Path) -> SubprocessWorker {
        SubprocessWorker::backend(
            WorkerKind::Generic,
            BackendOptions {
                command_template: Some(template.to_owned()),
                permissions: PermissionMode::AutoApprove,
                ..BackendOptions::default()
            },
            mission_dir.to_path_buf(),
        )
    }

    #[tokio::test]
    async fn a_generic_worker_runs_in_the_repository_not_the_daemons_directory() -> TestResult {
        let repo = tempfile::tempdir()?;
        let missions = tempfile::tempdir()?;
        // A relative write: lands wherever the worker's cwd is.
        let worker = generic_worker(
            "/bin/sh -c 'pwd -P > cwd-out; touch created-by-agent'",
            missions.path(),
        );
        let mut context = ctx(fresh_log(), Duration::from_secs(10));
        context.repo = repo.path().to_path_buf();
        let (outcome, _) = drive_quiet(&worker, context).await?;
        assert_eq!(outcome, WorkerOutcome::Completed);
        assert!(
            repo.path().join("created-by-agent").exists(),
            "created in the repo"
        );
        let cwd = std::fs::read_to_string(repo.path().join("cwd-out"))?;
        assert_eq!(
            std::path::Path::new(cwd.trim()),
            std::fs::canonicalize(repo.path())?,
            "the worker's cwd is the run's repository"
        );
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn the_mission_file_is_private_while_it_exists_and_gone_when_the_worker_ends()
    -> TestResult {
        use std::os::unix::fs::PermissionsExt as _;
        let repo = tempfile::tempdir()?;
        let base = tempfile::tempdir()?;
        let missions = base.path().join("missions");
        let worker = generic_worker(
            "/bin/sh -c 'stat -c %a {mission_file} > file-mode; stat -c %a $(dirname {mission_file}) > dir-mode; cp {mission_file} copy'",
            &missions,
        );
        let mut context = ctx(fresh_log(), Duration::from_secs(10));
        context.repo = repo.path().to_path_buf();
        context.mission = "SECRET JOB TEXT".to_owned();
        let (outcome, _) = drive_quiet(&worker, context).await?;
        assert_eq!(outcome, WorkerOutcome::Completed);
        assert_eq!(
            std::fs::read_to_string(repo.path().join("file-mode"))?.trim(),
            "600"
        );
        assert_eq!(
            std::fs::read_to_string(repo.path().join("dir-mode"))?.trim(),
            "700"
        );
        assert_eq!(
            std::fs::read_to_string(repo.path().join("copy"))?,
            "SECRET JOB TEXT"
        );
        let left: Vec<_> = std::fs::read_dir(&missions)?.collect();
        assert!(left.is_empty(), "the mission file was removed: {left:?}");
        assert_eq!(
            std::fs::metadata(&missions)?.permissions().mode() & 0o777,
            0o700
        );
        Ok(())
    }

    #[tokio::test]
    async fn the_mission_file_is_removed_when_the_worker_is_dropped_mid_run() -> TestResult {
        let repo = tempfile::tempdir()?;
        let missions = tempfile::tempdir()?;
        let worker = Arc::new(generic_worker(
            "/bin/sh -c 'sleep 60' {mission_file}",
            missions.path(),
        ));
        let mut context = ctx(fresh_log(), Duration::from_secs(60));
        context.repo = repo.path().to_path_buf();
        let (tx, _rx) = mpsc::unbounded_channel();
        let task = tokio::spawn({
            let worker = Arc::clone(&worker);
            async move { worker.run(context, WorkerEventSink::new(tx)).await }
        });
        // Wait until the file exists, i.e. the worker is up.
        for _ in 0..200 {
            if std::fs::read_dir(missions.path())?.next().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(std::fs::read_dir(missions.path())?.next().is_some());
        task.abort();
        let _ = task.await;
        assert!(
            std::fs::read_dir(missions.path())?.next().is_none(),
            "removed on the abort path too"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_failed_launch_leaves_no_mission_file() -> TestResult {
        let repo = tempfile::tempdir()?;
        let missions = tempfile::tempdir()?;
        let worker = generic_worker("/definitely/not/a/binary {mission_file}", missions.path());
        let mut context = ctx(fresh_log(), Duration::from_secs(5));
        context.repo = repo.path().to_path_buf();
        let (tx, _rx) = mpsc::unbounded_channel();
        let result = worker.run(context, WorkerEventSink::new(tx)).await;
        assert!(matches!(result, Err(WorkerError::Launch { .. })));
        assert!(std::fs::read_dir(missions.path())?.next().is_none());
        Ok(())
    }

    #[tokio::test]
    async fn the_process_is_recorded_while_it_runs_and_cleared_when_reaped() -> TestResult {
        let ledger = Arc::new(TestLedger::default());
        let worker = SubprocessWorker::new(WorkerKind::Generic, spec("echo hi"));
        let mut context = ctx(fresh_log(), Duration::from_secs(10));
        context.ledger = Some(ledger.clone());
        let (outcome, _) = drive_quiet(&worker, context).await?;
        assert_eq!(outcome, WorkerOutcome::Completed);
        let events = ledger
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        assert_eq!(events, vec!["record", "clear"]);
        let identity = ledger
            .recorded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or("recorded")?;
        assert!(identity.pgid > 1);
        Ok(())
    }

    /// Like `drive`, without requiring any output.
    async fn drive_quiet(
        worker: &SubprocessWorker,
        context: WorkerCtx,
    ) -> Result<(WorkerOutcome, String), WorkerError> {
        let log = Arc::clone(&context.output);
        let (tx, _rx) = mpsc::unbounded_channel();
        let outcome = worker.run(context, WorkerEventSink::new(tx)).await?;
        let text = log
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .tail(64 * 1024)
            .to_owned();
        Ok((outcome, text))
    }
}
