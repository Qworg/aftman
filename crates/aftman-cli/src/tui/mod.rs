//! `aftman watch <run> | --last`: the terminal UI (`docs/07-surfaces.md` section 4).
//!
//! | Module | Owns |
//! | --- | --- |
//! | [`sanitize`] | the one function that makes untrusted text safe to show |
//! | [`state`] | [`WatchState`]: the pure fold of a run's events into what the screen shows |
//! | [`view`] | `draw(frame, state, ui)`: the pure view |
//! | [`input`] | `handle_key(ui, state, key)`: pure key handling, modes, the line editor |
//! | [`driver`] | the async loop, generic over the ratatui backend and the input channel |
//! | [`terminal`] | raw mode, the alternate screen, the input thread, the panic hook |
//!
//! The TUI is a surface in the sense of `docs/07-surfaces.md` section 10: it reads the
//! event stream and sends `run.steer`, `run.cancel` and `run.answer`, exactly as the CLI
//! does, and decides nothing itself. It has no herdr awareness whatsoever.
//!
//! When stdout (or stdin) is not a terminal, [`watch`] does not draw: it falls back to
//! the plain line-per-event follow that `aftman run` uses, so `aftman watch --last |
//! tee log` works and never emits an escape sequence.

pub mod driver;
pub mod input;
pub mod sanitize;
pub mod state;
pub mod terminal;
pub mod view;
#[cfg(test)]
mod view_tests;

use std::io::IsTerminal as _;
use std::path::Path;
use std::sync::Arc;

use tokio::signal::unix::{SignalKind, signal};

use aftman_core::clock::SystemClock;
use aftman_core::ids::{EventSeq, RunId};

use crate::cli::RunTarget;
use crate::commands::Ctx;
use crate::commands::inspect::{Reader, target};
use crate::error::{CliError, exit, run_exit_code};
use crate::follow::Follow;
use crate::{outln, render};

use self::driver::{How, TermEvent, WatchOptions, WatchOutcome, run_watch};
use self::sanitize::{sanitize, sanitize_inline};
use self::state::WatchState;

pub use self::driver::WatchError;

/// `aftman watch`.
///
/// # Errors
/// `unknown_run` and other daemon errors, a daemon that cannot be reached or started,
/// a daemon lost while watching, or a terminal that cannot be configured.
pub async fn watch(ctx: &Ctx, run: RunTarget) -> Result<u8, CliError> {
    ctx.require_daemon("watch")?;
    let mut reader = Reader::open(ctx).await?;
    // The argument is echoed in some of these errors: make it inert first.
    let run_id = target(&mut reader, run)
        .await
        .map_err(|error| match error {
            CliError::Usage(message) => CliError::Usage(sanitize(&message)),
            other => other,
        })?;
    drop(reader);
    let socket = ctx.paths()?.socket;

    if std::io::stdout().is_terminal() && std::io::stdin().is_terminal() {
        interactive(ctx, &socket, run_id).await
    } else {
        plain(ctx, &socket, run_id).await
    }
}

async fn interactive(ctx: &Ctx, socket: &Path, run_id: RunId) -> Result<u8, CliError> {
    let mut options = WatchOptions::new(socket, Arc::new(SystemClock));
    options.client = ctx.client;

    // Registered before the terminal is touched, so no signal can arrive unhandled
    // between entering raw mode and the listener starting.
    let signals = TerminationSignals::register()
        .map_err(|e| CliError::io("installing signal handlers", e))?;
    terminal::install_panic_hook();
    let mut session = terminal::TerminalSession::enter()
        .map_err(|e| CliError::io("setting up the terminal", e))?;
    let (mut input, tx, thread) = terminal::spawn_input_thread(Arc::clone(&options.clock))
        .map_err(|e| CliError::io("reading the keyboard", e))?;
    // SIGINT / SIGQUIT / SIGTERM / SIGHUP end the TUI like a quit, so the terminal is
    // restored (in raw mode Ctrl-C is a key, but `kill -INT` is a signal).
    let signals = signals.spawn(tx);

    let outcome = run_watch(&mut session.terminal, &mut input, run_id, &options).await;

    // Restore the terminal before anything is printed.
    signals.abort();
    drop(thread);
    drop(session);

    let outcome = outcome.map_err(|error| match error {
        WatchError::Client(error) => CliError::Client(error),
        WatchError::Terminal(error) => CliError::io("drawing the screen", error),
    })?;
    epilogue(ctx, run_id, &outcome)
}

struct TerminationSignals {
    interrupt: tokio::signal::unix::Signal,
    quit: tokio::signal::unix::Signal,
    term: tokio::signal::unix::Signal,
    hangup: tokio::signal::unix::Signal,
}

impl TerminationSignals {
    fn register() -> std::io::Result<Self> {
        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            quit: signal(SignalKind::quit())?,
            term: signal(SignalKind::terminate())?,
            hangup: signal(SignalKind::hangup())?,
        })
    }

    fn spawn(mut self, tx: tokio::sync::mpsc::Sender<TermEvent>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            tokio::select! {
                _ = self.interrupt.recv() => {}
                _ = self.quit.recv() => {}
                _ = self.term.recv() => {}
                _ = self.hangup.recv() => {}
            }
            let _ = tx.send(TermEvent::Closed).await;
        })
    }
}

/// What is printed on the normal screen once the TUI is gone. Everything goes through
/// `out`, and the commands it suggests carry the user's `--socket` / `--data-dir`.
fn epilogue(ctx: &Ctx, run_id: RunId, outcome: &WatchOutcome) -> Result<u8, CliError> {
    let aftman = ctx.flags().prefix();
    match &outcome.how {
        How::Disconnected(error) => Err(CliError::Failed(format!(
            "lost the connection to the daemon while watching run {run_id}: {error}\n  \
             this cannot tell what happened to the run: if the daemon crashed, the run is \
             marked failed when a daemon next starts; if it only stopped answering, the run \
             may be fine.\n  \
             find out with `{aftman} status` and then `{aftman} show {run_id}`"
        ))),
        How::Quit => {
            let state = &outcome.state;
            if let Some(finish) = &state.finished {
                outln!("run {run_id} {}", finish.status)?;
            } else if let Some(escalation) = state.open_escalations().next() {
                outln!(
                    "{}",
                    render::needs_human_message(
                        &ctx.flags(),
                        run_id,
                        &escalation.id,
                        &escalation.ask
                    )
                )?;
            } else {
                outln!("{}", render::detached_message(&ctx.flags(), run_id))?;
            }
            Ok(outcome.exit_code())
        }
    }
}

/// No terminal: follow the run and print one line per event, ending when it does.
/// Everything is printed through `out` (sanitised; a closed pipe ends it quietly).
async fn plain(ctx: &Ctx, socket: &Path, run_id: RunId) -> Result<u8, CliError> {
    let mut interrupt = signal(SignalKind::interrupt())
        .map_err(|e| CliError::io("installing a Ctrl-C handler", e))?;
    let mut follow = Follow::open(socket, run_id, EventSeq::FIRST, ctx.client).await?;
    if follow.is_polling() {
        crate::errln!(
            "run {run_id} is not being driven by this daemon (it has ended, or another \
             process owns it); following its log until it ends"
        );
    }
    let mut state = WatchState::new();
    loop {
        let event = tokio::select! {
            biased;
            _ = interrupt.recv() => {
                outln!("\n{}", render::detached_message(&ctx.flags(), run_id))?;
                return Ok(if state.needs_human() { exit::NEEDS_HUMAN } else { exit::DETACHED });
            }
            next = follow.next() => next?,
        };
        let Some(event) = event else {
            // A run nobody drives ends when its log does, with or without a
            // `RUN_FINISHED` the stream could carry.
            return match state.status {
                Some(status) if status.is_terminal() => Ok(run_exit_code(status.as_str())),
                _ => Err(CliError::Failed(format!(
                    "the event stream for run {run_id} ended before the run finished"
                ))),
            };
        };
        state.apply(&event);
        if !ctx.quiet {
            for line in render::event_lines(&event) {
                crate::out::line(&plain_line(&line))?;
            }
            if event.kind == aftman_core::event::EventKind::RunCreated {
                for line in render::launch_lines(&event.payload) {
                    crate::out::line(&line)?;
                }
            }
        }
        if let Some(status) = render::finished_status(&event) {
            return Ok(run_exit_code(status));
        }
    }
}

/// One output line for the plain fallback: indentation kept, everything else one line
/// of inert text. A reason containing `\r= [  12] RUN_FINISHED` must not be able to
/// print a line that looks like one of ours.
fn plain_line(line: &str) -> String {
    let indent = line.len() - line.trim_start().len();
    format!("{}{}", &line[..indent], sanitize_inline(line))
}

#[cfg(test)]
mod tests {
    use super::plain_line;

    #[test]
    fn a_forged_event_line_cannot_start_a_new_line() {
        let line =
            plain_line("        reason: keep going\r= [  12] RUN_FINISHED\u{2028}> [   1] X");
        assert!(!line.contains('\n') && !line.contains('\r'), "{line:?}");
        assert!(line.starts_with("        reason: keep going"));
    }

    #[test]
    fn indentation_is_kept_and_escapes_are_removed() {
        assert_eq!(plain_line("    a\u{1b}[31mb"), "    ab");
        assert_eq!(plain_line("> [   1] RUN_CREATED"), "> [   1] RUN_CREATED");
    }
}
