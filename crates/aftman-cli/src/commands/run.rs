//! `aftman run` and `aftman demo`.
//!
//! By default a run goes through the daemon: `run.start`, then the run's events are
//! followed from seq 1 until `RUN_FINISHED`. `--no-daemon` embeds the runtime in this
//! process, builds it through the same [`CliFactory`], and streams to the terminal as
//! the CLI always has.

use std::sync::Arc;

use tokio::signal::unix::{SignalKind, signal};

use aftman_core::clock::{Clock, SystemClock};
use aftman_core::event::{EventEnvelope, EventKind};
use aftman_core::ids::{EventSeq, RunId};
use aftman_core::job::FactoryStatus;
use aftman_daemon::protocol::RunStartParams;
use aftman_daemon::{EventSink, RunControl, RunOutcome, RuntimeContext, mark_run_failed};

use crate::cli::{DemoArgs, ProcessEnv, RunArgs};
use crate::client::{Client, ClientError};
use crate::commands::Ctx;
use crate::error::{CliError, exit, run_exit_code};
use crate::factory::{CliFactory, Mode};
use crate::follow::Follow;
use crate::render::{self, RaisedEscalation, RunReport};
use crate::{errln, outln};

/// `aftman run`.
///
/// # Errors
/// Client-side validation, daemon errors, or a failed embedded run.
pub async fn run(ctx: &Ctx, args: RunArgs) -> Result<u8, CliError> {
    let params = args.prepare(&ProcessEnv)?;
    if params.permissions.as_deref() == Some("auto-approve") {
        // Every run, both modes, whatever --quiet says.
        errln!("{}", render::auto_approve_warning());
    }
    if ctx.no_daemon {
        embedded(ctx, params).await
    } else {
        through_daemon(ctx, params).await
    }
}

/// `aftman demo`: always embedded and offline, whatever else is running.
///
/// # Errors
/// As [`run`].
pub async fn demo(ctx: &Ctx, args: DemoArgs) -> Result<u8, CliError> {
    let run_args = args.as_run_args();
    let params = run_args.prepare(&ProcessEnv)?;
    if !ctx.quiet {
        outln!(
            "aftman demo · simulated worker and simulated model · {} is not modified\n",
            params.repo.display()
        )?;
    }
    embedded(ctx, params).await
}

/// Print an event as the terminal shows it while a run is followed: the summary line,
/// the reason for a decision, and, for `RUN_CREATED`, how the run was launched.
fn print_event(event: &EventEnvelope) -> Result<(), CliError> {
    for line in render::event_lines(event) {
        crate::out::line(&line)?;
    }
    if event.kind == EventKind::RunCreated {
        for line in render::launch_lines(&event.payload) {
            // The auto-approve warning was already printed, once, before the run began.
            if !line.starts_with("WARNING") {
                crate::out::line(&line)?;
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Embedded
// ---------------------------------------------------------------------------

/// Renders events to the terminal as they are emitted.
struct TerminalSink {
    quiet: bool,
}

impl EventSink for TerminalSink {
    fn emit(&self, event: &EventEnvelope) {
        if !self.quiet {
            // A closed stdout must not stop the run it was watching.
            let _ = print_event(event);
        }
    }
}

async fn embedded(ctx: &Ctx, params: RunStartParams) -> Result<u8, CliError> {
    let store = ctx.store()?;
    let sink: Arc<dyn EventSink> = Arc::new(TerminalSink { quiet: ctx.quiet });
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let factory = CliFactory::new(ctx.data_dir()?, ctx.inputs.clone(), Mode::Embedded);
    let prepared = factory
        .prepare(
            &params,
            RuntimeContext {
                store: store.clone(),
                sink: Arc::clone(&sink),
                clock: Arc::clone(&clock),
            },
        )
        .await?;
    let runtime = prepared.built.runtime;

    // What a CI cancel sends. Installed before the run exists, so there is no window.
    let mut sigint = signal(SignalKind::interrupt())
        .map_err(|e| CliError::io("installing a SIGINT handler", e))?;
    let mut sigterm = signal(SignalKind::terminate())
        .map_err(|e| CliError::io("installing a SIGTERM handler", e))?;

    let started = runtime
        .start(prepared.built.job)
        .await
        .map_err(|e| CliError::internal(format!("starting the run: {e}")))?;
    let run_id = started.run_id();
    let (control, receiver) = RunControl::channel();
    let drive = runtime.drive(started, Some(receiver));
    tokio::pin!(drive);

    // A signal cancels the run through its control channel and then keeps driving until
    // `drive` returns, so the worker is terminated through the normal path and the run
    // is recorded cancelled, instead of the process dying with an agent still running.
    let mut interrupted: Option<u8> = None;
    let result = loop {
        let signalled = tokio::select! {
            result = &mut drive => break result,
            _ = sigint.recv() => exit::INTERRUPTED,
            _ = sigterm.recv() => exit::TERMINATED,
        };
        if interrupted.is_some() {
            errln!("already cancelling the run; waiting for its worker to stop");
            continue;
        }
        interrupted = Some(signalled);
        errln!("\ninterrupted: cancelling run {run_id} and stopping its worker");
        let control = control.clone();
        tokio::spawn(async move {
            let _ = control.cancel("interrupted").await;
        });
    };

    match result {
        Ok(outcome) => {
            outln!()?;
            for line in outcome_report(&outcome).lines() {
                outln!("{line}")?;
            }
            if let Some(code) = interrupted {
                return Ok(code);
            }
            Ok(match outcome.status {
                FactoryStatus::Finished => exit::FINISHED,
                FactoryStatus::NeedsHuman => exit::NEEDS_HUMAN,
                _ => exit::FAILED,
            })
        }
        Err(error) => {
            // The run must not stay "running" in the store with nobody driving it.
            let reason = format!("run failed: {error}");
            if let Err(record) = mark_run_failed(
                store.as_ref(),
                sink.as_ref(),
                clock.as_ref(),
                run_id,
                &reason,
            )
            .await
            {
                errln!("could not record the failure of run {run_id}: {record}");
            }
            Err(CliError::Failed(format!("{reason} (run {run_id})")))
        }
    }
}

fn outcome_report(outcome: &RunOutcome) -> RunReport {
    RunReport {
        run_id: outcome.run_id,
        status: outcome.status.as_str().to_owned(),
        directive: outcome.directive.name().to_owned(),
        assessments: outcome.assessments,
        skipped: outcome.skipped,
        workers: outcome.counters.workers_run,
        attempts: outcome.counters.attempts,
        verifiers: outcome.counters.verifiers_run,
    }
}

// ---------------------------------------------------------------------------
// Through the daemon
// ---------------------------------------------------------------------------

/// Whether an error leaves open the possibility that the request was acted on.
fn may_have_started(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::Timeout { .. }
            | ClientError::Io(_)
            | ClientError::Closed
            | ClientError::Poisoned(_)
    )
}

async fn through_daemon(ctx: &Ctx, params: RunStartParams) -> Result<u8, CliError> {
    // Installed before anything slow, and kept: a Ctrl-C that lands between two
    // awaits is remembered by the stream, not lost.
    let mut interrupt = signal(SignalKind::interrupt())
        .map_err(|e| CliError::io("installing a Ctrl-C handler", e))?;

    let (mut client, paths, _status) = tokio::select! {
        biased;
        _ = interrupt.recv() => {
            errln!("interrupted while starting the daemon; no run was started");
            return Ok(exit::DETACHED);
        }
        connected = ctx.daemon() => connected?,
    };

    let started = tokio::select! {
        biased;
        _ = interrupt.recv() => {
            errln!(
                "interrupted while starting the run. It may have been started anyway: \
                 `aftman ls` shows it."
            );
            return Ok(exit::DETACHED);
        }
        started = client.run_start(params) => started,
    };
    let run_id = match started {
        Ok(run_id) => run_id,
        Err(error) if may_have_started(&error) => {
            return Err(CliError::Failed(format!(
                "{error}. The run may have been started anyway: `aftman ls` shows it."
            )));
        }
        Err(error) => return Err(error.into()),
    };
    // This connection will sit idle for the whole run, and the daemon closes idle
    // connections; later calls open their own.
    drop(client);
    if !ctx.quiet {
        outln!("run id      {run_id}\n")?;
    }
    // A dedicated connection for the stream (one subscription per connection).
    let mut follow = Follow::open(&paths.socket, run_id, EventSeq::FIRST, ctx.client).await?;

    let mut raised: Option<RaisedEscalation> = None;
    loop {
        let event = tokio::select! {
            biased;
            _ = interrupt.recv() => {
                outln!("\n{}", render::detached_message(&ctx.flags(), run_id))?;
                return Ok(exit::DETACHED);
            }
            next = follow.next() => next?,
        };
        let Some(event) = event else {
            return Err(CliError::Failed(format!(
                "the event stream for run {run_id} ended before the run finished"
            )));
        };
        if !ctx.quiet {
            print_event(&event)?;
        }
        if let Some(escalation) = render::raised_escalation(&event) {
            raised = Some(escalation);
        }
        if render::enters_needs_human(&event) {
            return needs_human(ctx, run_id, raised.take()).await;
        }
        if let Some(status) = render::finished_status(&event) {
            // The event decided; the summary below is a nicety and must not replace it.
            let code = run_exit_code(status);
            finish(ctx, run_id, status).await?;
            return Ok(code);
        }
    }
}

/// A short-lived connection of our own, because the one used to start the run has long
/// since been closed for idleness.
async fn fresh(ctx: &Ctx) -> Result<Client, CliError> {
    let paths = ctx.paths()?;
    Ok(Client::connect_with(&paths.socket, ctx.client).await?)
}

/// Print what a person needs to answer, and leave the run waiting.
async fn needs_human(
    ctx: &Ctx,
    run_id: RunId,
    raised: Option<RaisedEscalation>,
) -> Result<u8, CliError> {
    let escalation = match raised {
        Some(escalation) => Some(escalation),
        None => {
            // The stream did not carry the escalation: ask, best effort.
            match fresh(ctx).await {
                Ok(mut client) => client.run_get(run_id).await.ok().and_then(|state| {
                    state
                        .open_escalations()
                        .next()
                        .map(|open| RaisedEscalation {
                            id: open.id.to_string(),
                            ask: open.ask.clone(),
                        })
                }),
                Err(_) => None,
            }
        }
    };
    outln!()?;
    match escalation {
        Some(escalation) => outln!(
            "{}",
            render::needs_human_message(&ctx.flags(), run_id, &escalation.id, &escalation.ask)
        )?,
        None => outln!(
            "run {run_id} needs a human; `{} show {run_id}` lists what it is waiting for",
            ctx.flags().prefix()
        )?,
    }
    Ok(exit::NEEDS_HUMAN)
}

/// The closing summary. The run's own status (from `RUN_FINISHED`) is reported
/// whatever happens here: the stored state is read on a best-effort basis.
async fn finish(ctx: &Ctx, run_id: RunId, status: &str) -> Result<(), CliError> {
    let state = match fresh(ctx).await {
        Ok(mut client) => {
            // The runtime snapshots just *after* it emits `RUN_FINISHED`, so the state
            // may trail the event by a moment.
            let mut state = client.run_get(run_id).await.ok();
            for _ in 0..40 {
                if state.as_ref().is_none_or(|s| s.status.is_terminal()) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                state = client.run_get(run_id).await.ok();
            }
            state
        }
        Err(_) => None,
    };
    outln!()?;
    match state {
        Some(state) => {
            let mut report = RunReport::from_state(&state);
            status.clone_into(&mut report.status);
            for line in report.lines() {
                outln!("{line}")?;
            }
        }
        None => {
            outln!("run id      {run_id}")?;
            outln!("status      {status}")?;
        }
    }
    Ok(())
}
