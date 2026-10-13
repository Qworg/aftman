//! The async driver: ties the event stream, the keyboard and the screen together.
//!
//! [`run_watch`] is generic over the ratatui [`Backend`] and takes its input as a
//! channel of [`TermEvent`], so the tests drive it with a `TestBackend` and scripted
//! keys and the real terminal (`terminal.rs`) feeds the same channel from a thread.
//!
//! It `select!`s over four sources, all cancel-safe:
//!
//! - [`Follow::next`], the run's events from `seq` 1 (one code path for catching up
//!   and for live; a reconnect resumes from `last_seq + 1` inside `Follow`);
//! - the input channel;
//! - a frame tick, which advances the clock (elapsed time, message expiry) and draws
//!   when something changed. Events only mark the screen dirty, so replaying a long
//!   run does not draw once per event; a key press draws at once;
//! - finished actions.
//!
//! # Actions
//!
//! A steer, cancel or answer runs in its own task on its **own connection**, so the
//! screen keeps updating while it is in flight (a cancel can wait as long as a model
//! cycle). One action at a time; the footer says what is in flight, and the outcome,
//! including a refusal (`wrong_state`, `steering_unsupported`, a rejected steer) or a
//! lost daemon, is shown in the footer instead of ending the TUI.
//!
//! # Ending
//!
//! Quitting never cancels the run. When the run finishes the TUI stays up on the final
//! state (browsing the history is part of the TUI's job, `docs/07-surfaces.md` section
//! 8) until the user quits. If the event stream cannot be kept (the daemon went away
//! and could not be reached again), [`How::Disconnected`] is returned after one last
//! frame, and the caller reports the error on the normal screen.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use ratatui::Terminal;
use ratatui::backend::Backend;
use ratatui::crossterm::event::KeyEvent;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::{MissedTickBehavior, interval, timeout};

use aftman_core::assessment::HumanDecision;
use aftman_core::clock::Clock;
use aftman_core::ids::{EscalationId, EventSeq, RunId};
use aftman_core::worker::SteerOutcome;

use time::OffsetDateTime;

use crate::client::{Client, ClientConfig, ClientError};
use crate::error::{client_exit_code, exit, run_exit_code};
use crate::follow::Follow;

use super::input::{Action, Link, Tone, UiState, handle_key, handle_key_at, handle_paste};
use super::state::WatchState;
use super::view;

/// Something the terminal produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TermEvent {
    /// A key with no timestamp: never treated as part of a burst (tests, and anything
    /// that is not a real keyboard).
    Key(KeyEvent),
    /// A key read from the keyboard, with the time it was read. Keys closer together
    /// than [`super::input::BURST_GAP`] are a paste or type-ahead.
    Stamped(KeyEvent, OffsetDateTime),
    Paste(String),
    Resize,
    /// The input source is gone (the terminal closed, or a termination signal): quit.
    Closed,
}

/// How a watch is set up.
#[derive(Clone)]
pub struct WatchOptions {
    pub socket: PathBuf,
    pub client: ClientConfig,
    pub clock: Arc<dyn Clock>,
    /// How often the clock advances and a dirty screen is drawn.
    pub frame_interval: Duration,
    /// How long a quit waits for an action that is in flight to be answered.
    pub quit_grace: Duration,
}

impl WatchOptions {
    #[must_use]
    pub fn new(socket: impl Into<PathBuf>, clock: Arc<dyn Clock>) -> Self {
        Self {
            socket: socket.into(),
            client: ClientConfig::default(),
            clock,
            frame_interval: Duration::from_millis(50),
            quit_grace: Duration::from_secs(2),
        }
    }
}

/// Why the watch ended.
#[derive(Debug)]
pub enum How {
    /// The user left (or the input source closed). The run is untouched.
    Quit,
    /// The event stream was lost and could not be re-established.
    Disconnected(ClientError),
}

/// What a watch leaves behind.
#[derive(Debug)]
pub struct WatchOutcome {
    pub state: WatchState,
    pub how: How,
}

impl WatchOutcome {
    /// The process exit code: the run's code when it ended, `3` when it is waiting for
    /// a person, `130` when the user detached from a live run, and the client error's
    /// code when the daemon was lost.
    #[must_use]
    pub fn exit_code(&self) -> u8 {
        match &self.how {
            How::Disconnected(error) => client_exit_code(error),
            How::Quit => match &self.state.finished {
                Some(finish) => run_exit_code(&finish.status),
                None if self.state.needs_human() => exit::NEEDS_HUMAN,
                None => exit::DETACHED,
            },
        }
    }
}

/// A terminal or I/O failure in the driver itself.
#[derive(Debug, thiserror::Error)]
pub enum WatchError {
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error("terminal error: {0}")]
    Terminal(#[from] std::io::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActionKind {
    Steer,
    Cancel,
    Answer,
}

struct Done {
    kind: ActionKind,
    result: Result<String, String>,
}

/// Perform one action on a connection of its own.
async fn perform(socket: PathBuf, config: ClientConfig, run_id: RunId, action: Action) -> Done {
    let kind = match &action {
        Action::Steer { .. } => ActionKind::Steer,
        Action::Cancel => ActionKind::Cancel,
        Action::Answer { .. } | Action::Quit => ActionKind::Answer,
    };
    let result = async {
        let mut client = Client::connect_with(&socket, config)
            .await
            .map_err(|e| e.to_string())?;
        match action {
            Action::Steer { message } => match client.run_steer(run_id, message).await {
                Ok(SteerOutcome::Delivered) => Ok("steer delivered to the worker".to_owned()),
                Ok(SteerOutcome::Queued) => {
                    Ok("steer accepted; delivery to the worker is not confirmed".to_owned())
                }
                Ok(SteerOutcome::Rejected { reason }) => {
                    Err(format!("the worker rejected the steer: {reason}"))
                }
                Ok(SteerOutcome::Unsupported) => {
                    Err("this worker has no live input channel; it cannot be steered".to_owned())
                }
                Err(error) => Err(error.to_string()),
            },
            Action::Cancel => client
                .run_cancel(run_id)
                .await
                .map(|()| "cancel requested; waiting for the run to stop".to_owned())
                .map_err(|e| e.to_string()),
            Action::Answer {
                escalation,
                decision,
                note,
            } => answer(&mut client, run_id, escalation, decision, note).await,
            Action::Quit => Ok(String::new()),
        }
    }
    .await;
    Done { kind, result }
}

async fn answer(
    client: &mut Client,
    run_id: RunId,
    escalation: EscalationId,
    decision: HumanDecision,
    note: String,
) -> Result<String, String> {
    let what = match &decision {
        HumanDecision::Accept => "accept",
        HumanDecision::Recheck => "recheck",
        HumanDecision::Stop => "stop",
        HumanDecision::ClaimComplete => "claim-complete",
        HumanDecision::Guidance { .. } => "guidance",
    };
    client
        .run_answer(run_id, escalation, decision, note)
        .await
        .map(|()| format!("answered the escalation: {what}"))
        .map_err(|e| e.to_string())
}

fn busy_label(action: &Action) -> &'static str {
    match action {
        Action::Steer { .. } => "sending steer...",
        Action::Cancel => "cancelling the run...",
        Action::Answer { .. } => "sending your answer...",
        Action::Quit => "",
    }
}

fn viewport<B: Backend>(terminal: &Terminal<B>, ui: &mut UiState) {
    ui.viewport = terminal.size().ok().map(|size| (size.width, size.height));
}

fn draw<B: Backend>(
    terminal: &mut Terminal<B>,
    state: &WatchState,
    ui: &UiState,
) -> Result<(), WatchError> {
    terminal
        .draw(|frame| view::draw(frame, state, ui))
        .map(|_| ())
        .map_err(|e| WatchError::Terminal(std::io::Error::other(e.to_string())))
}

/// Watch one run until the user quits or the daemon is lost.
///
/// # Errors
/// The stream could not be opened at all (`unknown_run`, no daemon), or the terminal
/// failed. A stream lost mid-way is not an error here: it is [`How::Disconnected`].
pub async fn run_watch<B: Backend>(
    terminal: &mut Terminal<B>,
    input: &mut mpsc::Receiver<TermEvent>,
    run_id: RunId,
    options: &WatchOptions,
) -> Result<WatchOutcome, WatchError> {
    let mut follow = Follow::open(&options.socket, run_id, EventSeq::FIRST, options.client).await?;

    let mut state = WatchState::new();
    let mut ui = UiState::new(options.clock.now());
    match header(options, run_id).await {
        Ok(run) => state.set_header(&run),
        Err(error) => ui.set_message(
            Tone::Info,
            &format!("could not read the run header: {error}"),
        ),
    }

    let mut actions: JoinSet<Done> = JoinSet::new();
    let mut ticker = interval(options.frame_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut stream_open = true;
    let mut dirty = true;

    viewport(terminal, &mut ui);
    draw(terminal, &state, &ui)?;

    let how = loop {
        tokio::select! {
            next = follow.next(), if stream_open => match next {
                Ok(Some(event)) => {
                    state.apply(&event);
                    dirty = true;
                    // The stream moved: whatever was wrong with the link is over.
                    if ui.link != Link::Live {
                        ui.link = Link::Live;
                    }
                }
                Ok(None) => {
                    stream_open = false;
                    ui.stream_ended = true;
                    // Do not bury a more specific message (a refusal, a result) that
                    // arrived in the same breath.
                    if ui.message.is_none() {
                        ui.set_message(
                            Tone::Info,
                            &format!("the run ended ({}); press q to quit", state.status_label()),
                        );
                    }
                    dirty = true;
                }
                Err(error) => {
                    ui.set_message(Tone::Error, &format!("lost the daemon: {error}"));
                    if !matches!(ui.link, Link::Lost { .. }) {
                        ui.link = Link::Lost { failures: 1 };
                    }
                    viewport(terminal, &mut ui);
                    draw(terminal, &state, &ui)?;
                    break How::Disconnected(error);
                }
            },
            event = input.recv() => match event {
                None | Some(TermEvent::Closed) => break How::Quit,
                Some(TermEvent::Resize) => {
                    viewport(terminal, &mut ui);
                    dirty = true;
                }
                Some(TermEvent::Paste(text)) => {
                    handle_paste(&mut ui, &text);
                    dirty = true;
                }
                Some(key @ (TermEvent::Key(_) | TermEvent::Stamped(..))) => {
                    viewport(terminal, &mut ui);
                    let action = match key {
                        TermEvent::Stamped(key, at) => {
                            handle_key_at(&mut ui, &state, key, Some(at))
                        }
                        TermEvent::Key(key) => handle_key(&mut ui, &state, key),
                        _ => None,
                    };
                    match action {
                        Some(Action::Quit) => break How::Quit,
                        Some(action) => start(&mut actions, &mut ui, options, run_id, action),
                        None => {}
                    }
                    // A key is answered at once, not on the next frame.
                    draw(terminal, &state, &ui)?;
                    dirty = false;
                }
            },
            _ = ticker.tick() => {
                dirty |= ui.tick(options.clock.now());
                // What the stream knows about its own health: failed attempts to
                // re-establish it (a first quick retry is not alarming), and whether
                // the daemon is driving this run at all.
                let failures = follow.reconnect_failures();
                let link = if failures > 0 { Link::Lost { failures } } else { Link::Live };
                let polling = follow.is_polling();
                if link != ui.link || polling != ui.polling {
                    ui.link = link;
                    ui.polling = polling;
                    dirty = true;
                }
                if dirty {
                    draw(terminal, &state, &ui)?;
                    dirty = false;
                }
            }
            Some(joined) = actions.join_next(), if !actions.is_empty() => {
                finish(&mut ui, joined, actions.len());
                draw(terminal, &state, &ui)?;
                dirty = false;
            }
        }
    };

    // An action the user confirmed should not be lost to the very next key. But the
    // screen must not freeze while it is waited for: say so, and let any key (or the
    // end of the grace period) end the wait.
    if !actions.is_empty() && matches!(how, How::Quit) {
        let what = ui.busy.clone().unwrap_or_default();
        ui.set_message(
            Tone::Info,
            &format!("finishing {what} before leaving; any key leaves now"),
        );
        draw(terminal, &state, &ui)?;
        let _ = timeout(options.quit_grace, async {
            loop {
                tokio::select! {
                    done = actions.join_next() => if done.is_none() { break },
                    key = input.recv() => if matches!(key, None | Some(TermEvent::Key(_) | TermEvent::Stamped(..) | TermEvent::Closed)) { break },
                }
            }
        })
        .await;
    }
    Ok(WatchOutcome { state, how })
}

async fn header(
    options: &WatchOptions,
    run_id: RunId,
) -> Result<aftman_core::run::RunState, ClientError> {
    let mut client = Client::connect_with(&options.socket, options.client).await?;
    client.run_get(run_id).await
}

fn start(
    actions: &mut JoinSet<Done>,
    ui: &mut UiState,
    options: &WatchOptions,
    run_id: RunId,
    action: Action,
) {
    // The keys refuse a second steer or answer before their prompt closes; this is the
    // backstop. A cancel is always issued, whatever else is in flight.
    if let Some(busy) = &ui.busy {
        if !matches!(action, Action::Cancel) {
            let text = format!("still busy ({busy}); try again when it finishes");
            ui.set_message(Tone::Info, &text);
            return;
        }
    }
    ui.busy = Some(busy_label(&action).to_owned());
    actions.spawn(perform(
        options.socket.clone(),
        options.client,
        run_id,
        action,
    ));
}

fn finish(ui: &mut UiState, joined: Result<Done, tokio::task::JoinError>, still_running: usize) {
    if still_running == 0 {
        ui.busy = None;
    }
    match joined {
        Ok(Done {
            kind,
            result: Ok(text),
        }) => {
            if kind == ActionKind::Steer {
                ui.draft.clear();
            }
            ui.set_message(Tone::Ok, &text);
        }
        Ok(Done {
            result: Err(text), ..
        }) => ui.set_message(Tone::Error, &text),
        Err(error) => ui.set_message(
            Tone::Error,
            &format!("the action did not complete: {error}"),
        ),
    }
}
