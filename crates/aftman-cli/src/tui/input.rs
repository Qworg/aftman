//! Pure input handling for `aftman watch`.
//!
//! [`handle_key`] takes the interface state, the run state and one key, mutates the
//! interface state and optionally returns an [`Action`] for the driver to perform. It
//! does no I/O and knows nothing about the terminal, so every key path is a unit test.
//!
//! Modes:
//!
//! - **Normal**: scroll, switch pane, and the commands `s` steer, `a` answer, `c`
//!   cancel, `?` help, `q` / Esc / Ctrl-C quit (detach: the run keeps going).
//! - **Input**: a one-line editor ([`LineEditor`]) for a steer message, answer
//!   guidance, or an answer note. It counts bytes and characters against the protocol's
//!   limits and refuses what would not fit rather than letting the daemon reject it.
//! - **Decide**: pick how to answer an escalation (`docs/06-ensemble.md` "R3 - Human").
//! - **Confirm**: `cancel` and an answer of `stop` ask first, and the confirmation is a whole
//!   word (`cancel` / `stop`) plus Enter, which type-ahead or an unbracketed paste cannot
//!   produce by accident. Two narrow rules do that (see [`BURST_GAP`]): the rest of a burst
//!   that opened a modal is swallowed, and an Enter that arrives in the same burst as the
//!   keys before it never confirms. Characters typed quickly inside the confirmation are
//!   just text.
//! - **Help**: the key list.

use aftman_core::assessment::HumanDecision;
use aftman_core::ids::EscalationId;
use aftman_daemon::protocol::{MAX_ANSWER_BYTES, MAX_STEER_BYTES};
use aftman_daemon::runtime::{MAX_NOTE_CHARS, MAX_STEER_CHARS};
use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use std::cell::Cell;
use time::OffsetDateTime;

use unicode_width::UnicodeWidthStr as _;

use super::sanitize::{clip, sanitize, sanitize_inline};
use super::state::WatchState;

/// A pane that can have the focus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pane {
    Timeline,
    Assessment,
    Tail,
}

impl Pane {
    #[must_use]
    pub fn next(self) -> Self {
        match self {
            Self::Timeline => Self::Assessment,
            Self::Assessment => Self::Tail,
            Self::Tail => Self::Timeline,
        }
    }

    #[must_use]
    pub fn prev(self) -> Self {
        match self {
            Self::Timeline => Self::Tail,
            Self::Assessment => Self::Timeline,
            Self::Tail => Self::Assessment,
        }
    }
}

/// How the human can answer an escalation (`HumanDecision`, in the order shown).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Choice {
    Accept,
    Recheck,
    Stop,
    ClaimComplete,
    Guidance,
}

impl Choice {
    pub const ALL: [Self; 5] = [
        Self::Accept,
        Self::Recheck,
        Self::Stop,
        Self::ClaimComplete,
        Self::Guidance,
    ];

    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Accept => "accept",
            Self::Recheck => "recheck",
            Self::Stop => "stop",
            Self::ClaimComplete => "claim-complete",
            Self::Guidance => "guidance",
        }
    }

    /// One plain-language line about what choosing it does.
    #[must_use]
    pub const fn explanation(self) -> &'static str {
        match self {
            Self::Accept => {
                "Accept the uncertainty and carry on. Nothing is unlocked; the next assessment decides."
            }
            Self::Recheck => {
                "Look again: re-run the evidence providers, then assess straight away."
            }
            Self::Stop => {
                "Stop the run now. A person may always stop work; the run ends as cancelled."
            }
            Self::ClaimComplete => {
                "Say you believe it is done. This re-measures with fresh evidence and cannot finish the run by itself: the measurement has to clear the bar on its own."
            }
            Self::Guidance => "Send your own words to the worker as guidance, then carry on.",
        }
    }
}

/// What the line editor is collecting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Purpose {
    Steer,
    /// Guidance text for the worker (required).
    Guidance {
        escalation: EscalationId,
    },
    /// An optional note recorded with a decision.
    Note {
        escalation: EscalationId,
        choice: Choice,
    },
}

impl Purpose {
    fn limits(&self) -> (usize, usize) {
        match self {
            Self::Steer => (MAX_STEER_BYTES, MAX_STEER_CHARS),
            Self::Guidance { .. } => (MAX_ANSWER_BYTES, MAX_STEER_CHARS),
            Self::Note { .. } => (MAX_ANSWER_BYTES, MAX_NOTE_CHARS),
        }
    }

    /// The prompt shown above the editor.
    #[must_use]
    pub fn title(&self) -> String {
        match self {
            Self::Steer => "Steer the worker".to_owned(),
            Self::Guidance { .. } => "Guidance for the worker (required)".to_owned(),
            Self::Note {
                choice: Choice::Stop,
                ..
            } => "Note for STOP (optional; the run will end, and you will be asked to confirm)"
                .to_owned(),
            Self::Note { choice, .. } => format!("Note for '{}' (optional)", choice.name()),
        }
    }
}

/// What a pending confirmation will do once its word is typed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Confirm {
    CancelRun,
    StopAnswer {
        escalation: EscalationId,
        note: String,
    },
}

/// The interaction mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mode {
    Normal,
    Input {
        purpose: Purpose,
        editor: LineEditor,
    },
    Decide {
        escalation: EscalationId,
        /// Which of the open escalations this is, and how many there are.
        position: (usize, usize),
        selected: usize,
    },
    /// A destructive action waiting for its word.
    Confirm {
        what: Confirm,
        typed: String,
    },
    Help,
}

impl Confirm {
    /// The word that has to be typed (then Enter). A word, not a letter: a stream of
    /// text can contain a `y`, and rarely contains the word at a human pace.
    #[must_use]
    pub const fn word(&self) -> &'static str {
        match self {
            Self::CancelRun => "cancel",
            Self::StopAnswer { .. } => "stop",
        }
    }
}

/// What the driver should do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Leave the TUI. The run keeps going.
    Quit,
    Steer {
        message: String,
    },
    Cancel,
    Answer {
        escalation: EscalationId,
        decision: HumanDecision,
        note: String,
    },
}

/// How a footer message is coloured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tone {
    Info,
    Ok,
    Error,
}

/// A transient footer message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub text: String,
    pub tone: Tone,
    pub expires: Option<OffsetDateTime>,
}

/// Characters kept of a footer message.
pub const MAX_MESSAGE_CHARS: usize = 400;

/// How long an informational message stays.
const INFO_TTL: time::Duration = time::Duration::seconds(6);

/// Lines the assessment pane uses per check (key line, numbers line).
pub const LINES_PER_CHECK: usize = 2;

/// Rows a page key moves.
const PAGE: usize = 10;

/// Keys closer together than this are a burst: a paste or type-ahead, or a fast typist.
/// What a burst may not do is narrow: issue a command from the main screen; carry on
/// driving a modal that it opened (the keys that follow the opener without a pause are
/// swallowed); and confirm (an Enter in the same burst as the keys before it is a space
/// in a text editor and "press Enter again" in a confirmation). Plain characters typed
/// quickly into an editor or a confirmation are text. Only keys that carry a timestamp
/// ([`handle_key_at`]) are subject to any of this.
pub const BURST_GAP: time::Duration = time::Duration::milliseconds(25);

/// The smallest terminal the confirmation popup is drawn in full on. Below it the
/// popup says so and the confirmation is refused (Esc backs out).
pub const CONFIRM_MIN: (u16, u16) = (44, 9);
/// The smallest terminal the decision popup is drawn usably on.
pub const DECIDE_MIN: (u16, u16) = (44, 10);

/// The state of the link to the daemon, as far as the TUI can tell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Link {
    Live,
    /// The daemon did not answer this many probes in a row; what is on screen is stale.
    Lost {
        failures: u32,
    },
}

/// Everything about the interface that is not the run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UiState {
    pub focus: Pane,
    pub mode: Mode,
    /// The selected timeline entry, by `seq`; `None` follows the newest.
    pub timeline_sel: Option<u64>,
    /// First visible line of the assessment pane.
    pub assess_scroll: usize,
    /// The absolute index of the newest tail line being read; `None` follows the
    /// output. Anchored to the line, so new output does not move what is being read.
    pub tail_anchor: Option<u64>,
    /// How far the assessment pane can scroll, as measured by the last draw. The one
    /// clamp both the view and the keys use.
    pub assess_max: Cell<usize>,
    /// The terminal size as of the last draw, when known.
    pub viewport: Option<(u16, u16)>,
    /// The time of the previous timestamped key (for burst detection).
    pub last_key_at: Option<OffsetDateTime>,
    /// The current burst began with the key that opened a modal: its remainder is
    /// swallowed. Cleared by the first key after a pause.
    pub opener_chain: bool,
    pub link: Link,
    /// The daemon is not driving this run (it lives in another process, or is an
    /// orphan): the stream polls the log.
    pub polling: bool,
    /// The highest `seq` the run's snapshot held when the watch began. The fold has
    /// "caught up" once it reaches it (or the stream has ended); until then what is
    /// on screen is history being replayed, not the run's present.
    pub replay_target: Option<u64>,
    pub message: Option<Message>,
    /// An action in flight ("cancelling...").
    pub busy: Option<String>,
    /// The wall clock, as of the last tick. Elapsed time and message expiry use it.
    pub now: OffsetDateTime,
    /// The steer message that was last submitted and not yet known to have worked.
    pub draft: String,
    /// The event stream is over (the run ended, or it could not be kept).
    pub stream_ended: bool,
}

impl UiState {
    #[must_use]
    pub fn new(now: OffsetDateTime) -> Self {
        Self {
            focus: Pane::Timeline,
            mode: Mode::Normal,
            timeline_sel: None,
            assess_scroll: 0,
            tail_anchor: None,
            assess_max: Cell::new(0),
            viewport: None,
            last_key_at: None,
            opener_chain: false,
            link: Link::Live,
            polling: false,
            replay_target: None,
            message: None,
            busy: None,
            now,
            draft: String::new(),
            stream_ended: false,
        }
    }

    /// Show `text` in the footer. Sanitised and bounded: it may carry a daemon error
    /// that quotes worker or repository text.
    pub fn set_message(&mut self, tone: Tone, text: &str) {
        let text = clip(&sanitize_inline(text), MAX_MESSAGE_CHARS);
        let expires = match tone {
            Tone::Error => None,
            Tone::Info | Tone::Ok => Some(self.now + INFO_TTL),
        };
        self.message = Some(Message {
            text,
            tone,
            expires,
        });
    }

    /// Advance the clock; drops an expired message. Returns whether anything changed.
    pub fn tick(&mut self, now: OffsetDateTime) -> bool {
        let seconds_changed = now.unix_timestamp() != self.now.unix_timestamp();
        self.now = now;
        let expired = self
            .message
            .as_ref()
            .and_then(|m| m.expires)
            .is_some_and(|at| now >= at);
        if expired {
            self.message = None;
        }
        seconds_changed || expired
    }

    /// While the fold is still catching up with the log: `(applied seq, target seq)`.
    #[must_use]
    pub fn replaying(&self, applied: u64) -> Option<(u64, u64)> {
        let target = self.replay_target?;
        (!self.stream_ended && applied < target).then_some((applied, target))
    }

    /// Whether the terminal is known to be too small for the popup `min` describes.
    #[must_use]
    pub fn too_small_for(&self, min: (u16, u16)) -> bool {
        self.viewport.is_some_and(|(w, h)| w < min.0 || h < min.1)
    }

    /// Whether the user is typing (the cursor is shown and keys are text).
    #[must_use]
    pub fn is_typing(&self) -> bool {
        matches!(self.mode, Mode::Input { .. })
    }

    /// The timeline index the selection points at (the newest when following).
    #[must_use]
    pub fn selected_index(&self, state: &WatchState) -> Option<usize> {
        let len = state.timeline.len();
        if len == 0 {
            return None;
        }
        match self.timeline_sel {
            None => Some(len - 1),
            Some(seq) => Some(
                state
                    .timeline
                    .iter()
                    .position(|e| e.seq() >= seq)
                    .unwrap_or(len - 1),
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// The line editor
// ---------------------------------------------------------------------------

/// A one-line text editor over a `String`. The cursor is a byte offset kept on a
/// character boundary, so multi-byte text cannot be split; it moves by `char`
/// (a combining mark is its own step).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineEditor {
    text: String,
    cursor: usize,
    max_bytes: usize,
    max_chars: usize,
}

/// What an insert did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Inserted {
    pub chars: usize,
    /// Some of the text did not fit.
    pub clipped: bool,
}

impl LineEditor {
    #[must_use]
    pub fn new(max_bytes: usize, max_chars: usize) -> Self {
        Self {
            text: String::new(),
            cursor: 0,
            max_bytes,
            max_chars,
        }
    }

    #[must_use]
    pub fn with_text(max_bytes: usize, max_chars: usize, text: &str) -> Self {
        let mut editor = Self::new(max_bytes, max_chars);
        editor.insert_str(text);
        editor
    }

    #[must_use]
    pub fn text(&self) -> &str {
        &self.text
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.text.is_empty()
    }

    /// Cursor position as a character index.
    #[must_use]
    pub fn cursor_chars(&self) -> usize {
        self.text[..self.cursor].chars().count()
    }

    /// Terminal columns from the start of the text to the cursor.
    #[must_use]
    pub fn cursor_col(&self) -> usize {
        self.text[..self.cursor].width()
    }

    #[must_use]
    pub fn bytes(&self) -> usize {
        self.text.len()
    }

    #[must_use]
    pub fn chars(&self) -> usize {
        self.text.chars().count()
    }

    #[must_use]
    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    #[must_use]
    pub fn max_chars(&self) -> usize {
        self.max_chars
    }

    /// Insert `c` at the cursor. False (and unchanged) when it would exceed a limit.
    pub fn insert_char(&mut self, c: char) -> bool {
        if self.text.len() + c.len_utf8() > self.max_bytes || self.chars() + 1 > self.max_chars {
            return false;
        }
        self.text.insert(self.cursor, c);
        self.cursor += c.len_utf8();
        true
    }

    /// Insert as much of `text` as fits, in order. The text is made safe first
    /// (escape sequences removed, line breaks folded to spaces).
    pub fn insert_str(&mut self, text: &str) -> Inserted {
        let clean = sanitize_inline(text);
        let mut chars = 0;
        let mut clipped = false;
        for c in clean.chars() {
            if self.insert_char(c) {
                chars += 1;
            } else {
                clipped = true;
                break;
            }
        }
        Inserted { chars, clipped }
    }

    pub fn left(&mut self) {
        if let Some(c) = self.text[..self.cursor].chars().next_back() {
            self.cursor -= c.len_utf8();
        }
    }

    pub fn right(&mut self) {
        if let Some(c) = self.text[self.cursor..].chars().next() {
            self.cursor += c.len_utf8();
        }
    }

    pub fn home(&mut self) {
        self.cursor = 0;
    }

    pub fn end(&mut self) {
        self.cursor = self.text.len();
    }

    pub fn backspace(&mut self) {
        if let Some(c) = self.text[..self.cursor].chars().next_back() {
            self.cursor -= c.len_utf8();
            self.text.remove(self.cursor);
        }
    }

    pub fn delete(&mut self) {
        if self.cursor < self.text.len() {
            self.text.remove(self.cursor);
        }
    }

    /// Delete from the cursor to the start of the line.
    pub fn kill_to_start(&mut self) {
        self.text.drain(..self.cursor);
        self.cursor = 0;
    }

    /// Delete from the cursor to the end of the line.
    pub fn kill_to_end(&mut self) {
        self.text.truncate(self.cursor);
    }

    fn word_start(&self) -> usize {
        let before = &self.text[..self.cursor];
        let trimmed = before.trim_end_matches(char::is_whitespace);
        trimmed.rfind(char::is_whitespace).map_or(0, |i| {
            i + trimmed[i..].chars().next().map_or(1, char::len_utf8)
        })
    }

    /// Delete the word before the cursor.
    pub fn delete_word_back(&mut self) {
        let start = self.word_start();
        self.text.drain(start..self.cursor);
        self.cursor = start;
    }

    pub fn word_left(&mut self) {
        self.cursor = self.word_start();
    }

    pub fn word_right(&mut self) {
        let after = &self.text[self.cursor..];
        let skipped_space = after.len() - after.trim_start_matches(char::is_whitespace).len();
        let rest = &after[skipped_space..];
        let word = rest.find(char::is_whitespace).unwrap_or(rest.len());
        self.cursor += skipped_space + word;
    }
}

// ---------------------------------------------------------------------------
// Keys
// ---------------------------------------------------------------------------

fn ctrl(key: &KeyEvent) -> bool {
    key.modifiers.contains(KeyModifiers::CONTROL)
}

fn alt(key: &KeyEvent) -> bool {
    key.modifiers.contains(KeyModifiers::ALT)
}

/// Handle one key with no timestamp (no burst detection).
pub fn handle_key(ui: &mut UiState, state: &WatchState, key: KeyEvent) -> Option<Action> {
    handle_key_at(ui, state, key, None)
}

fn is_navigation(key: &KeyEvent) -> bool {
    !ctrl(key)
        && matches!(
            key.code,
            KeyCode::Up
                | KeyCode::Down
                | KeyCode::PageUp
                | KeyCode::PageDown
                | KeyCode::Home
                | KeyCode::End
                | KeyCode::Char('j' | 'k')
        )
}

/// Handle one key that arrived at `at`. Returns what the driver must do, if anything.
pub fn handle_key_at(
    ui: &mut UiState,
    state: &WatchState,
    key: KeyEvent,
    at: Option<OffsetDateTime>,
) -> Option<Action> {
    if key.kind == KeyEventKind::Release {
        return None;
    }
    let burst = match at {
        Some(at) => {
            let burst = ui.last_key_at.is_some_and(|prev| at - prev < BURST_GAP);
            ui.last_key_at = Some(at);
            burst
        }
        None => false,
    };
    if !burst {
        ui.opener_chain = false;
    }
    let interrupt = ctrl(&key) && matches!(key.code, KeyCode::Char('c'));
    let was_plain = !matches!(
        ui.mode,
        Mode::Confirm { .. } | Mode::Decide { .. } | Mode::Help
    );
    // The rest of a burst that opened a modal is not for the modal.
    let swallow = burst && ui.opener_chain;
    let action = match ui.mode.clone() {
        Mode::Normal => {
            if burst && !is_navigation(&key) && !interrupt {
                return None;
            }
            normal_key(ui, state, key)
        }
        Mode::Help => {
            if swallow {
                return None;
            }
            if interrupt {
                return Some(Action::Quit);
            }
            ui.mode = Mode::Normal;
            None
        }
        Mode::Input { purpose, editor } => input_key(ui, state, purpose, editor, key, burst),
        Mode::Decide {
            escalation,
            position,
            selected,
        } => {
            if swallow {
                return None;
            }
            decide_key(ui, state, escalation, position, selected, key)
        }
        Mode::Confirm { what, typed } => {
            if swallow {
                return None;
            }
            confirm_key(ui, what, typed, key, burst)
        }
    };
    if at.is_some()
        && was_plain
        && matches!(
            ui.mode,
            Mode::Confirm { .. } | Mode::Decide { .. } | Mode::Help
        )
    {
        ui.opener_chain = true;
    }
    action
}

/// Handle pasted text. Only meaningful while typing.
pub fn handle_paste(ui: &mut UiState, text: &str) {
    let note = if let Mode::Input { editor, .. } = &mut ui.mode {
        let inserted = editor.insert_str(text);
        inserted.clipped.then(|| {
            format!(
                "paste clipped at the limit ({} bytes, {} characters)",
                editor.max_bytes(),
                editor.max_chars()
            )
        })
    } else {
        None
    };
    if let Some(note) = note {
        ui.set_message(Tone::Error, &note);
    }
}

fn normal_key(ui: &mut UiState, state: &WatchState, key: KeyEvent) -> Option<Action> {
    let code = key.code;
    if ctrl(&key) {
        return matches!(code, KeyCode::Char('c' | 'd')).then_some(Action::Quit);
    }
    match code {
        KeyCode::Char('q') | KeyCode::Esc => return Some(Action::Quit),
        KeyCode::Char('?') | KeyCode::F(1) => ui.mode = Mode::Help,
        KeyCode::Tab => ui.focus = ui.focus.next(),
        KeyCode::BackTab => ui.focus = ui.focus.prev(),
        KeyCode::Char('1') => ui.focus = Pane::Timeline,
        KeyCode::Char('2') => ui.focus = Pane::Assessment,
        KeyCode::Char('3') => ui.focus = Pane::Tail,
        KeyCode::Up | KeyCode::Char('k') => scroll(ui, state, -1),
        KeyCode::Down | KeyCode::Char('j') => scroll(ui, state, 1),
        KeyCode::PageUp => scroll(ui, state, -(PAGE as isize)),
        KeyCode::PageDown => scroll(ui, state, PAGE as isize),
        KeyCode::Home | KeyCode::Char('g') => jump(ui, state, false),
        KeyCode::End | KeyCode::Char('G') => jump(ui, state, true),
        KeyCode::Char('s') => start_steer(ui, state),
        KeyCode::Char('a') => start_answer(ui, state),
        KeyCode::Char('c') => {
            if state.is_finished() {
                ui.set_message(Tone::Info, "the run has ended; there is nothing to cancel");
            } else {
                ui.mode = Mode::Confirm {
                    what: Confirm::CancelRun,
                    typed: String::new(),
                };
            }
        }
        _ => {}
    }
    None
}

fn scroll(ui: &mut UiState, state: &WatchState, by: isize) {
    let step = by.unsigned_abs();
    match ui.focus {
        Pane::Timeline => {
            let Some(current) = ui.selected_index(state) else {
                return;
            };
            let last = state.timeline.len() - 1;
            let next = if by < 0 {
                current.saturating_sub(step)
            } else {
                (current + step).min(last)
            };
            // Reaching the newest entry resumes following.
            ui.timeline_sel = if next == last {
                None
            } else {
                state.timeline.get(next).map(super::state::Entry::seq)
            };
        }
        Pane::Assessment => {
            let max = ui.assess_max.get();
            ui.assess_scroll = if by < 0 {
                ui.assess_scroll.min(max).saturating_sub(step)
            } else {
                (ui.assess_scroll + step).min(max)
            };
        }
        Pane::Tail => {
            // Up goes back in time, from the line being read, not from the end.
            let Some(last) = state.tail.last_index() else {
                return;
            };
            let first = state.tail.first_index();
            let current = ui.tail_anchor.unwrap_or(last).clamp(first, last);
            let step = step as u64;
            let next = if by < 0 {
                current.saturating_sub(step).max(first)
            } else {
                current.saturating_add(step)
            };
            ui.tail_anchor = if next >= last { None } else { Some(next) };
        }
    }
}

fn jump(ui: &mut UiState, state: &WatchState, to_end: bool) {
    match ui.focus {
        Pane::Timeline => {
            ui.timeline_sel = if to_end {
                None
            } else {
                state.timeline.front().map(super::state::Entry::seq)
            };
        }
        Pane::Assessment => {
            ui.assess_scroll = if to_end { ui.assess_max.get() } else { 0 };
        }
        Pane::Tail => {
            ui.tail_anchor = if to_end || state.tail.is_empty() {
                None
            } else {
                Some(state.tail.first_index())
            };
        }
    }
}

fn editor_for(purpose: &Purpose, text: &str) -> LineEditor {
    let (bytes, chars) = purpose.limits();
    LineEditor::with_text(bytes, chars, text)
}

fn start_steer(ui: &mut UiState, state: &WatchState) {
    if state.is_finished() {
        ui.set_message(Tone::Info, "the run has ended; there is no worker to steer");
        return;
    }
    let purpose = Purpose::Steer;
    let editor = editor_for(&purpose, &ui.draft.clone());
    ui.mode = Mode::Input { purpose, editor };
}

fn start_answer(ui: &mut UiState, state: &WatchState) {
    if state.is_finished() {
        ui.set_message(Tone::Info, "the run has ended; there is nothing to answer");
        return;
    }
    let open: Vec<_> = state.open_escalations().collect();
    let Some(first) = open.first() else {
        ui.set_message(Tone::Info, "no open escalation to answer");
        return;
    };
    let Ok(escalation) = first.id.parse::<EscalationId>() else {
        ui.set_message(
            Tone::Error,
            "the escalation id on the stream is not an id this client can answer with",
        );
        return;
    };
    ui.mode = Mode::Decide {
        escalation,
        position: (1, open.len()),
        selected: 0,
    };
}

fn decide_key(
    ui: &mut UiState,
    state: &WatchState,
    escalation: EscalationId,
    position: (usize, usize),
    selected: usize,
    key: KeyEvent,
) -> Option<Action> {
    let set = |ui: &mut UiState, selected: usize| {
        ui.mode = Mode::Decide {
            escalation,
            position,
            selected,
        };
    };
    if ctrl(&key) {
        if matches!(key.code, KeyCode::Char('c')) {
            ui.mode = Mode::Normal;
        }
        return None;
    }
    if ui.too_small_for(DECIDE_MIN) {
        // The choices cannot be shown in full: only a way out is offered.
        if matches!(key.code, KeyCode::Esc | KeyCode::Char('q')) {
            ui.mode = Mode::Normal;
        }
        return None;
    }
    let count = Choice::ALL.len();
    match key.code {
        KeyCode::Esc | KeyCode::Char('q') => ui.mode = Mode::Normal,
        // With several open escalations the operator chooses which one this answers.
        KeyCode::Tab | KeyCode::BackTab => {
            let ids: Vec<EscalationId> = state
                .open_escalations()
                .filter_map(|e| e.id.parse().ok())
                .collect();
            if ids.len() > 1 {
                let here = ids.iter().position(|id| *id == escalation).unwrap_or(0);
                let next = if matches!(key.code, KeyCode::Tab) {
                    (here + 1) % ids.len()
                } else {
                    here.checked_sub(1).unwrap_or(ids.len() - 1)
                };
                ui.mode = Mode::Decide {
                    escalation: ids[next],
                    position: (next + 1, ids.len()),
                    selected,
                };
            }
        }
        KeyCode::Up | KeyCode::Char('k') => set(ui, selected.checked_sub(1).unwrap_or(count - 1)),
        KeyCode::Down | KeyCode::Char('j') => set(ui, (selected + 1) % count),
        KeyCode::Char(c @ '1'..='5') => {
            let index = usize::from(c as u8 - b'1');
            set(ui, index);
        }
        KeyCode::Enter => choose(ui, escalation, Choice::ALL[selected.min(count - 1)]),
        _ => {}
    }
    None
}

fn choose(ui: &mut UiState, escalation: EscalationId, choice: Choice) {
    let purpose = match choice {
        Choice::Guidance => Purpose::Guidance { escalation },
        other => Purpose::Note {
            escalation,
            choice: other,
        },
    };
    let editor = editor_for(&purpose, "");
    ui.mode = Mode::Input { purpose, editor };
}

fn confirm_key(
    ui: &mut UiState,
    what: Confirm,
    mut typed: String,
    key: KeyEvent,
    burst: bool,
) -> Option<Action> {
    let back = |ui: &mut UiState, why: &str| {
        ui.mode = Mode::Normal;
        ui.set_message(Tone::Info, why);
    };
    if ctrl(&key) {
        if matches!(key.code, KeyCode::Char('c')) {
            back(ui, "not confirmed; nothing was sent");
        }
        return None;
    }
    // A confirmation that cannot be shown in full cannot be given.
    let too_small = ui.too_small_for(CONFIRM_MIN);
    match key.code {
        KeyCode::Esc => back(ui, "not confirmed; nothing was sent"),
        _ if too_small => {}
        KeyCode::Enter if burst => {
            // Typed or pasted in one go with what came before: not a deliberate act.
            ui.mode = Mode::Confirm { what, typed };
            ui.set_message(Tone::Info, "press Enter again to confirm");
        }
        KeyCode::Enter => {
            if !typed.trim().eq_ignore_ascii_case(what.word()) {
                back(
                    ui,
                    &format!("not confirmed: you have to type {} and Enter", what.word()),
                );
                return None;
            }
            if matches!(what, Confirm::StopAnswer { .. }) {
                if let Some(busy) = ui.busy.clone() {
                    ui.mode = Mode::Confirm { what, typed };
                    ui.set_message(
                        Tone::Error,
                        &format!("still busy ({busy}); wait, then Enter"),
                    );
                    return None;
                }
            }
            ui.mode = Mode::Normal;
            return Some(match what {
                Confirm::CancelRun => Action::Cancel,
                Confirm::StopAnswer { escalation, note } => Action::Answer {
                    escalation,
                    decision: HumanDecision::Stop,
                    note,
                },
            });
        }
        KeyCode::Backspace => {
            typed.pop();
            ui.mode = Mode::Confirm { what, typed };
        }
        KeyCode::Char(c) if !c.is_control() && !alt(&key) => {
            if typed.chars().count() < 16 {
                typed.push(c);
            }
            ui.mode = Mode::Confirm { what, typed };
        }
        _ => {}
    }
    None
}

fn input_key(
    ui: &mut UiState,
    _state: &WatchState,
    purpose: Purpose,
    mut editor: LineEditor,
    key: KeyEvent,
    burst: bool,
) -> Option<Action> {
    let mut finished = None;
    let mut refused = false;
    if ctrl(&key) {
        match key.code {
            KeyCode::Char('c') => {
                if editor.is_empty() {
                    finished = Some(Outcome::Leave);
                } else {
                    editor = editor_for(&purpose, "");
                }
            }
            KeyCode::Char('a') => editor.home(),
            KeyCode::Char('e') => editor.end(),
            KeyCode::Char('u') => editor.kill_to_start(),
            KeyCode::Char('k') => editor.kill_to_end(),
            KeyCode::Char('w') => editor.delete_word_back(),
            KeyCode::Char('b') => editor.left(),
            KeyCode::Left => editor.word_left(),
            KeyCode::Char('f') => editor.right(),
            KeyCode::Right => editor.word_right(),
            _ => {}
        }
    } else {
        match key.code {
            KeyCode::Esc => finished = Some(Outcome::Leave),
            // A newline inside a burst is part of pasted text, not a person pressing
            // Enter: it becomes a space and nothing is submitted.
            KeyCode::Enter if burst => refused = !editor.insert_char(' '),
            KeyCode::Enter => finished = Some(Outcome::Submit),
            KeyCode::Left if alt(&key) => editor.word_left(),
            KeyCode::Right if alt(&key) => editor.word_right(),
            KeyCode::Left => editor.left(),
            KeyCode::Right => editor.right(),
            KeyCode::Home => editor.home(),
            KeyCode::End => editor.end(),
            KeyCode::Backspace => editor.backspace(),
            KeyCode::Delete => editor.delete(),
            // One character through the same filter as everything else: a control
            // character or an invisible format character is not text, and neither is
            // an Alt-modified key.
            KeyCode::Char(c) if !alt(&key) => {
                let mut one = String::new();
                one.push(c);
                if sanitize(&one) == one && !c.is_control() {
                    refused = !editor.insert_char(c);
                }
            }
            _ => {}
        }
    }
    match finished {
        None => {
            ui.mode = Mode::Input { purpose, editor };
            if refused {
                ui.set_message(
                    Tone::Error,
                    &format!(
                        "limit reached: at most {} bytes / {} characters",
                        ui.input_limits().0,
                        ui.input_limits().1
                    ),
                );
            }
            None
        }
        Some(Outcome::Leave) => {
            ui.mode = Mode::Normal;
            None
        }
        Some(Outcome::Submit) => submit(ui, purpose, &editor),
    }
}

enum Outcome {
    Leave,
    Submit,
}

impl UiState {
    fn input_limits(&self) -> (usize, usize) {
        match &self.mode {
            Mode::Input { editor, .. } => (editor.max_bytes(), editor.max_chars()),
            _ => (0, 0),
        }
    }
}

fn submit(ui: &mut UiState, purpose: Purpose, editor: &LineEditor) -> Option<Action> {
    let text = editor.text().trim().to_owned();
    let again = |ui: &mut UiState, message: &str| {
        ui.mode = Mode::Input {
            purpose: purpose.clone(),
            editor: editor.clone(),
        };
        ui.set_message(Tone::Error, message);
    };
    // Refuse before the modal closes, so nothing typed is lost; cancel is not
    // subject to this (it is issued from the confirmation).
    let stop_note = matches!(
        purpose,
        Purpose::Note {
            choice: Choice::Stop,
            ..
        }
    );
    if !stop_note {
        if let Some(busy) = ui.busy.clone() {
            again(
                ui,
                &format!("still busy ({busy}); press Enter again when it finishes"),
            );
            return None;
        }
    }
    match &purpose {
        Purpose::Steer => {
            if text.is_empty() {
                again(ui, "nothing to send: type a message, or Esc to cancel");
                return None;
            }
            ui.draft.clone_from(&text);
            ui.mode = Mode::Normal;
            Some(Action::Steer { message: text })
        }
        Purpose::Guidance { escalation } => {
            if text.is_empty() {
                again(
                    ui,
                    "guidance needs some text: say what the worker should do",
                );
                return None;
            }
            ui.mode = Mode::Normal;
            Some(Action::Answer {
                escalation: *escalation,
                decision: HumanDecision::Guidance { text },
                note: String::new(),
            })
        }
        Purpose::Note { escalation, choice } => {
            let decision = match choice {
                Choice::Accept => HumanDecision::Accept,
                Choice::Recheck => HumanDecision::Recheck,
                Choice::ClaimComplete => HumanDecision::ClaimComplete,
                Choice::Stop => {
                    ui.mode = Mode::Confirm {
                        what: Confirm::StopAnswer {
                            escalation: *escalation,
                            note: text,
                        },
                        typed: String::new(),
                    };
                    return None;
                }
                // Guidance has its own purpose; this is unreachable by construction,
                // and the safe reading is "ask again".
                Choice::Guidance => {
                    again(ui, "guidance needs some text");
                    return None;
                }
            };
            ui.mode = Mode::Normal;
            Some(Action::Answer {
                escalation: *escalation,
                decision,
                note: text,
            })
        }
    }
}

/// The text for the footer's key hints in `mode`.
#[must_use]
pub fn hints(mode: &Mode, finished: bool, needs_human: bool) -> String {
    match mode {
        Mode::Normal => {
            // The one thing a person must not miss goes first: at a narrow width it is
            // the tail of the line that is clipped.
            let mut out = String::new();
            if needs_human {
                out.push_str("a ANSWER  ");
            }
            out.push_str("q quit  Tab pane  j/k scroll  ");
            if !finished {
                out.push_str("s steer  c cancel  ");
            }
            out.push_str("? help");
            out
        }
        Mode::Input { .. } => "Enter send  Esc cancel  Ctrl-U clear line".to_owned(),
        Mode::Decide { .. } => {
            "j/k or 1-5 choose  Enter select  Tab next escalation  Esc back".to_owned()
        }
        Mode::Confirm { what, .. } => {
            format!("type {} then Enter to confirm  Esc back", what.word())
        }
        Mode::Help => "any key closes help".to_owned(),
    }
}

/// Truncate `text` for display in a prompt.
#[must_use]
pub fn preview(text: &str, cols: usize) -> String {
    clip(&sanitize_inline(text), cols)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aftman_core::event::{EventEnvelope, EventKind};
    use aftman_core::ids::{EventSeq, RunId};
    use serde_json::json;
    use time::macros::datetime;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn chr(c: char) -> KeyEvent {
        key(KeyCode::Char(c))
    }

    fn ctrl_key(c: char) -> KeyEvent {
        KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL)
    }

    fn ui() -> UiState {
        UiState::new(datetime!(2026-01-01 00:00:00 UTC))
    }

    fn ev(seq: u64, kind: EventKind, payload: serde_json::Value) -> EventEnvelope {
        EventEnvelope::new(
            EventSeq(seq),
            RunId::new(),
            datetime!(2026-01-01 00:00:00 UTC) + time::Duration::seconds(seq as i64),
            kind,
            payload,
        )
    }

    fn live() -> WatchState {
        let mut state = WatchState::new();
        state.apply(&ev(
            1,
            EventKind::StateChanged,
            json!({"from": "routing", "to": "running"}),
        ));
        state
    }

    fn waiting(id: &EscalationId) -> WatchState {
        let mut state = live();
        state.apply(&ev(
            2,
            EventKind::EscalationRaised,
            json!({"escalation_id": id.to_string(), "kind": "check", "ask": "ok?"}),
        ));
        state.apply(&ev(
            3,
            EventKind::StateChanged,
            json!({"from": "running", "to": "needs_human"}),
        ));
        state
    }

    fn finished() -> WatchState {
        let mut state = live();
        state.apply(&ev(
            2,
            EventKind::RunFinished,
            json!({"status": "finished"}),
        ));
        state
    }

    fn type_text(ui: &mut UiState, state: &WatchState, text: &str) {
        for c in text.chars() {
            assert_eq!(handle_key(ui, state, chr(c)), None);
        }
    }

    // ---- quitting and navigation -----------------------------------------

    #[test]
    fn q_esc_and_ctrl_c_quit_from_normal_mode() {
        let state = live();
        for k in [chr('q'), key(KeyCode::Esc), ctrl_key('c')] {
            assert_eq!(handle_key(&mut ui(), &state, k), Some(Action::Quit));
        }
        assert_eq!(handle_key(&mut ui(), &state, chr('x')), None);
    }

    #[test]
    fn key_release_events_are_ignored() {
        let mut k = chr('q');
        k.kind = KeyEventKind::Release;
        assert_eq!(handle_key(&mut ui(), &live(), k), None);
    }

    #[test]
    fn tab_and_numbers_move_the_focus() {
        let state = live();
        let mut u = ui();
        assert_eq!(u.focus, Pane::Timeline);
        handle_key(&mut u, &state, key(KeyCode::Tab));
        assert_eq!(u.focus, Pane::Assessment);
        handle_key(&mut u, &state, key(KeyCode::Tab));
        assert_eq!(u.focus, Pane::Tail);
        handle_key(&mut u, &state, key(KeyCode::Tab));
        assert_eq!(u.focus, Pane::Timeline);
        handle_key(&mut u, &state, key(KeyCode::BackTab));
        assert_eq!(u.focus, Pane::Tail);
        handle_key(&mut u, &state, chr('2'));
        assert_eq!(u.focus, Pane::Assessment);
        handle_key(&mut u, &state, chr('3'));
        assert_eq!(u.focus, Pane::Tail);
        handle_key(&mut u, &state, chr('1'));
        assert_eq!(u.focus, Pane::Timeline);
    }

    fn with_decisions(n: u64) -> WatchState {
        let mut state = WatchState::new();
        for seq in 1..=n {
            state.apply(&ev(
                seq,
                EventKind::DirectiveSelected,
                json!({"directive": "continue", "reason": format!("r{seq}")}),
            ));
        }
        state
    }

    #[test]
    fn the_timeline_selection_moves_and_resumes_following_at_the_end() {
        let state = with_decisions(5);
        let mut u = ui();
        assert_eq!(u.selected_index(&state), Some(4), "follows the newest");
        handle_key(&mut u, &state, chr('k'));
        assert_eq!(u.timeline_sel, Some(4));
        assert_eq!(u.selected_index(&state), Some(3));
        handle_key(&mut u, &state, key(KeyCode::Up));
        handle_key(&mut u, &state, key(KeyCode::Up));
        assert_eq!(u.selected_index(&state), Some(1));
        handle_key(&mut u, &state, key(KeyCode::Home));
        assert_eq!(u.selected_index(&state), Some(0));
        handle_key(&mut u, &state, key(KeyCode::Up));
        assert_eq!(u.selected_index(&state), Some(0), "stops at the top");
        handle_key(&mut u, &state, key(KeyCode::PageDown));
        assert_eq!(u.timeline_sel, None, "reaching the newest follows again");
        handle_key(&mut u, &state, key(KeyCode::PageUp));
        assert_eq!(u.selected_index(&state), Some(0));
        handle_key(&mut u, &state, chr('G'));
        assert_eq!(u.timeline_sel, None);
        // Nothing to select: no panic.
        let mut empty = ui();
        handle_key(&mut empty, &WatchState::new(), chr('j'));
        handle_key(&mut empty, &WatchState::new(), chr('k'));
        assert_eq!(empty.selected_index(&WatchState::new()), None);
    }

    #[test]
    fn a_selection_survives_eviction_by_following_its_seq() {
        let mut state = with_decisions(5);
        let mut u = ui();
        handle_key(&mut u, &state, chr('k'));
        handle_key(&mut u, &state, chr('k'));
        assert_eq!(u.timeline_sel, Some(3));
        state.timeline.pop_front();
        assert_eq!(u.selected_index(&state), Some(1), "seq 3 is now at index 1");
        state.timeline.pop_front();
        state.timeline.pop_front();
        assert_eq!(
            u.selected_index(&state),
            Some(0),
            "an evicted selection clamps"
        );
    }

    #[test]
    fn the_tail_scrolls_back_from_the_line_being_read_and_never_runs_off_the_top() {
        let mut state = WatchState::new();
        let text: String = (0..30).map(|i| format!("l{i}\n")).collect();
        state.apply(&ev(
            1,
            EventKind::WorkerOutput,
            json!({"worker_id": "w", "text": text}),
        ));
        // A marker and 30 lines: absolute 0..=30.
        let mut u = ui();
        u.focus = Pane::Tail;
        handle_key(&mut u, &state, chr('k'));
        assert_eq!(u.tail_anchor, Some(29));
        handle_key(&mut u, &state, key(KeyCode::PageUp));
        assert_eq!(u.tail_anchor, Some(19));
        handle_key(&mut u, &state, key(KeyCode::Home));
        assert_eq!(u.tail_anchor, Some(0));
        handle_key(&mut u, &state, key(KeyCode::PageUp));
        assert_eq!(
            u.tail_anchor,
            Some(0),
            "clamped at the first line, not past it"
        );
        handle_key(&mut u, &state, chr('j'));
        assert_eq!(u.tail_anchor, Some(1));
        handle_key(&mut u, &state, key(KeyCode::End));
        assert_eq!(u.tail_anchor, None);
        handle_key(&mut u, &state, chr('j'));
        assert_eq!(u.tail_anchor, None);
        // Scrolling down to the newest line follows again.
        u.tail_anchor = Some(29);
        handle_key(&mut u, &state, chr('j'));
        assert_eq!(u.tail_anchor, None);
    }

    #[test]
    fn a_scrolled_back_tail_stays_on_its_line_when_new_output_arrives() {
        let mut state = WatchState::new();
        state.apply(&ev(
            1,
            EventKind::WorkerOutput,
            json!({"worker_id": "w", "text": "a\nb\nc\n"}),
        ));
        let mut u = ui();
        u.focus = Pane::Tail;
        handle_key(&mut u, &state, chr('k'));
        let anchor = u.tail_anchor;
        assert!(anchor.is_some());
        state.apply(&ev(
            2,
            EventKind::WorkerOutput,
            json!({"worker_id": "w", "text": "d\ne\n"}),
        ));
        assert_eq!(
            u.tail_anchor, anchor,
            "the anchor is a line, not an offset from the end"
        );
        let at = anchor.unwrap_or(0);
        assert_eq!(state.tail.line_at(at).map(|(_, t)| t), Some("b"));
    }

    #[test]
    fn the_assessment_pane_scrolls_within_bounds() {
        let mut state = WatchState::new();
        state.apply(&ev(
            1,
            EventKind::AssessmentCompleted,
            json!({"checks": [{"key": "a"}, {"key": "b"}]}),
        ));
        let mut u = ui();
        u.focus = Pane::Assessment;
        // The last draw measured how far the pane can really scroll.
        u.assess_max.set(5);
        for _ in 0..50 {
            handle_key(&mut u, &state, chr('j'));
        }
        assert_eq!(
            u.assess_scroll, 5,
            "one clamp: no dead zone on the way back"
        );
        handle_key(&mut u, &state, chr('k'));
        assert_eq!(u.assess_scroll, 4, "the first press back already moves");
        handle_key(&mut u, &state, key(KeyCode::Home));
        assert_eq!(u.assess_scroll, 0);
        handle_key(&mut u, &state, chr('k'));
        assert_eq!(u.assess_scroll, 0);
    }

    #[test]
    fn help_opens_and_any_key_closes_it() {
        let state = live();
        let mut u = ui();
        handle_key(&mut u, &state, chr('?'));
        assert_eq!(u.mode, Mode::Help);
        assert_eq!(handle_key(&mut u, &state, chr('x')), None);
        assert_eq!(u.mode, Mode::Normal);
        handle_key(&mut u, &state, chr('?'));
        assert_eq!(
            handle_key(&mut u, &state, ctrl_key('c')),
            Some(Action::Quit)
        );
    }

    // ---- steer ------------------------------------------------------------

    #[test]
    fn steer_types_a_message_and_sends_it() {
        let state = live();
        let mut u = ui();
        handle_key(&mut u, &state, chr('s'));
        assert!(u.is_typing());
        type_text(&mut u, &state, "go left");
        let action = handle_key(&mut u, &state, key(KeyCode::Enter));
        assert_eq!(
            action,
            Some(Action::Steer {
                message: "go left".into()
            })
        );
        assert_eq!(u.mode, Mode::Normal);
        assert_eq!(u.draft, "go left");
    }

    #[test]
    fn a_failed_steer_is_still_in_the_editor_next_time() {
        let state = live();
        let mut u = ui();
        handle_key(&mut u, &state, chr('s'));
        type_text(&mut u, &state, "retry me");
        handle_key(&mut u, &state, key(KeyCode::Enter));
        handle_key(&mut u, &state, chr('s'));
        let Mode::Input { editor, .. } = &u.mode else {
            panic!("expected the editor");
        };
        assert_eq!(editor.text(), "retry me");
    }

    #[test]
    fn an_empty_steer_is_refused_and_esc_abandons_it() {
        let state = live();
        let mut u = ui();
        handle_key(&mut u, &state, chr('s'));
        type_text(&mut u, &state, "   ");
        assert_eq!(handle_key(&mut u, &state, key(KeyCode::Enter)), None);
        assert!(u.is_typing(), "still typing");
        assert_eq!(u.message.as_ref().map(|m| m.tone), Some(Tone::Error));
        assert_eq!(handle_key(&mut u, &state, key(KeyCode::Esc)), None);
        assert_eq!(u.mode, Mode::Normal);
    }

    #[test]
    fn steer_and_cancel_are_refused_once_the_run_has_ended() {
        let state = finished();
        let mut u = ui();
        handle_key(&mut u, &state, chr('s'));
        assert_eq!(u.mode, Mode::Normal);
        assert!(u.message.is_some());
        handle_key(&mut u, &state, chr('c'));
        assert_eq!(u.mode, Mode::Normal);
    }

    #[test]
    fn q_is_a_letter_while_typing_and_ctrl_c_clears_then_leaves() {
        let state = live();
        let mut u = ui();
        handle_key(&mut u, &state, chr('s'));
        type_text(&mut u, &state, "quit q");
        let Mode::Input { editor, .. } = &u.mode else {
            panic!()
        };
        assert_eq!(editor.text(), "quit q");
        assert_eq!(handle_key(&mut u, &state, ctrl_key('c')), None);
        let Mode::Input { editor, .. } = &u.mode else {
            panic!("still typing")
        };
        assert!(editor.is_empty());
        assert_eq!(handle_key(&mut u, &state, ctrl_key('c')), None);
        assert_eq!(u.mode, Mode::Normal);
    }

    // ---- the line editor ---------------------------------------------------

    #[test]
    fn the_editor_handles_multibyte_text() {
        let mut e = LineEditor::new(1000, 1000);
        e.insert_str("héllo 日本語 🙂");
        assert_eq!(e.text(), "héllo 日本語 🙂");
        assert_eq!(e.cursor_chars(), 11);
        assert_eq!(e.cursor_col(), 5 + 1 + 6 + 1 + 2);
        e.left();
        e.left();
        assert_eq!(e.cursor_chars(), 9);
        e.backspace(); // removes 語
        assert_eq!(e.text(), "héllo 日本 🙂");
        e.insert_char('é');
        assert_eq!(e.text(), "héllo 日本é 🙂");
        e.home();
        e.delete(); // h
        assert_eq!(e.text(), "éllo 日本é 🙂");
        e.right();
        e.right();
        e.delete();
        assert_eq!(e.text(), "élo 日本é 🙂");
        e.end();
        e.backspace();
        assert_eq!(e.text(), "élo 日本é ");
        // Never lands inside a character.
        for _ in 0..40 {
            e.left();
            assert!(e.text().is_char_boundary(e.cursor));
        }
        for _ in 0..40 {
            e.right();
            assert!(e.text().is_char_boundary(e.cursor));
        }
        // Backspace/delete at the edges do nothing.
        e.end();
        e.delete();
        e.home();
        e.backspace();
        assert_eq!(e.text(), "élo 日本é ");
    }

    #[test]
    fn the_editor_kills_words_and_lines() {
        let mut e = LineEditor::with_text(100, 100, "one two  three");
        e.delete_word_back();
        assert_eq!(e.text(), "one two  ");
        e.delete_word_back();
        assert_eq!(e.text(), "one ");
        e.home();
        e.word_right();
        assert_eq!(e.cursor_chars(), 3);
        e.word_right();
        assert_eq!(e.cursor_chars(), 4);
        e.word_left();
        assert_eq!(e.cursor_chars(), 0);
        e.end();
        e.left();
        e.kill_to_end();
        assert_eq!(e.text(), "one");
        e.left();
        e.kill_to_start();
        assert_eq!(e.text(), "e");
    }

    #[test]
    fn the_editor_enforces_byte_and_character_limits() {
        let mut e = LineEditor::new(10, 100);
        let inserted = e.insert_str("日本語日本語");
        assert_eq!(
            e.text(),
            "日本語",
            "9 bytes fit, the 10th byte cannot hold another 3-byte char"
        );
        assert_eq!(
            inserted,
            Inserted {
                chars: 3,
                clipped: true
            }
        );
        assert!(e.insert_char('x'));
        assert!(!e.insert_char('y'));
        assert_eq!(e.bytes(), 10);

        let mut e = LineEditor::new(1000, 5);
        let inserted = e.insert_str("abcdefgh");
        assert_eq!((e.text(), inserted.clipped), ("abcde", true));
        assert!(!e.insert_char('z'));
        e.backspace();
        assert!(e.insert_char('z'));
    }

    #[test]
    fn typing_past_the_limit_says_so() {
        let state = live();
        let mut u = ui();
        handle_key(&mut u, &state, chr('s'));
        let Mode::Input { editor, .. } = &mut u.mode else {
            panic!()
        };
        *editor = LineEditor::with_text(4, 4, "abcd");
        handle_key(&mut u, &state, chr('e'));
        let Mode::Input { editor, .. } = &u.mode else {
            panic!()
        };
        assert_eq!(editor.text(), "abcd");
        assert!(
            u.message
                .as_ref()
                .is_some_and(|m| m.text.contains("limit reached"))
        );
    }

    #[test]
    fn the_real_protocol_limits_apply_to_each_purpose() {
        let state = live();
        let mut u = ui();
        handle_key(&mut u, &state, chr('s'));
        let Mode::Input { editor, .. } = &u.mode else {
            panic!()
        };
        assert_eq!(editor.max_bytes(), MAX_STEER_BYTES);
        assert_eq!(editor.max_chars(), MAX_STEER_CHARS);

        let id = EscalationId::new();
        let state = waiting(&id);
        let mut u = ui();
        handle_key(&mut u, &state, chr('a'));
        handle_key(&mut u, &state, chr('1'));
        handle_key(&mut u, &state, key(KeyCode::Enter));
        let Mode::Input { editor, .. } = &u.mode else {
            panic!()
        };
        assert_eq!(editor.max_bytes(), MAX_ANSWER_BYTES);
        assert_eq!(editor.max_chars(), MAX_NOTE_CHARS);
    }

    #[test]
    fn paste_is_sanitised_folded_to_one_line_and_clipped() {
        let state = live();
        let mut u = ui();
        handle_key(&mut u, &state, chr('s'));
        handle_paste(
            &mut u,
            "line one\nline two\u{1b}[31m\r\n\u{1b}]0;x\u{7}end\n",
        );
        let Mode::Input { editor, .. } = &u.mode else {
            panic!()
        };
        assert_eq!(editor.text(), "line one line two end");

        let Mode::Input { editor, .. } = &mut u.mode else {
            panic!()
        };
        *editor = LineEditor::new(8, 8);
        handle_paste(&mut u, "0123456789");
        let Mode::Input { editor, .. } = &u.mode else {
            panic!()
        };
        assert_eq!(editor.text(), "01234567");
        assert!(
            u.message
                .as_ref()
                .is_some_and(|m| m.text.contains("clipped"))
        );

        // Paste outside the editor does nothing (also not in the other modes).
        let mut n = ui();
        handle_paste(&mut n, "x");
        assert_eq!(n.mode, Mode::Normal);
    }

    #[test]
    fn control_and_invisible_characters_are_not_typed() {
        let state = live();
        let mut u = ui();
        handle_key(&mut u, &state, chr('s'));
        for c in ['\u{7}', '\u{1b}', '\u{202e}', '\u{200b}', '\u{7f}'] {
            handle_key(&mut u, &state, chr(c));
        }
        // Alt-modified characters are not text either.
        handle_key(
            &mut u,
            &state,
            KeyEvent::new(KeyCode::Char('z'), KeyModifiers::ALT),
        );
        let Mode::Input { editor, .. } = &u.mode else {
            panic!()
        };
        assert!(editor.is_empty());
    }

    #[test]
    fn editing_keys_work_through_handle_key() {
        let state = live();
        let mut u = ui();
        handle_key(&mut u, &state, chr('s'));
        type_text(&mut u, &state, "abcdef");
        handle_key(&mut u, &state, key(KeyCode::Left));
        handle_key(&mut u, &state, key(KeyCode::Backspace));
        handle_key(&mut u, &state, ctrl_key('a'));
        handle_key(&mut u, &state, key(KeyCode::Delete));
        handle_key(&mut u, &state, ctrl_key('e'));
        handle_key(&mut u, &state, chr('!'));
        handle_key(&mut u, &state, ctrl_key('w'));
        let Mode::Input { editor, .. } = &u.mode else {
            panic!()
        };
        assert_eq!(editor.text(), "");
        type_text(&mut u, &state, "xyz");
        handle_key(&mut u, &state, ctrl_key('u'));
        handle_key(&mut u, &state, key(KeyCode::Home));
        handle_key(&mut u, &state, key(KeyCode::End));
        handle_key(&mut u, &state, key(KeyCode::Right));
        let Mode::Input { editor, .. } = &u.mode else {
            panic!()
        };
        assert!(editor.is_empty());
    }

    // ---- cancel -------------------------------------------------------------

    fn type_word(u: &mut UiState, state: &WatchState, word: &str) {
        for c in word.chars() {
            assert_eq!(handle_key(u, state, chr(c)), None);
        }
    }

    #[test]
    fn cancel_needs_the_word_typed_and_enter() {
        let state = live();
        let mut u = ui();
        handle_key(&mut u, &state, chr('c'));
        assert!(matches!(
            u.mode,
            Mode::Confirm {
                what: Confirm::CancelRun,
                ..
            }
        ));
        // A lone `y` is not a confirmation any more.
        assert_eq!(handle_key(&mut u, &state, chr('y')), None);
        assert_eq!(handle_key(&mut u, &state, key(KeyCode::Enter)), None);
        assert_eq!(u.mode, Mode::Normal, "a wrong word backs out");

        handle_key(&mut u, &state, chr('c'));
        type_word(&mut u, &state, "cancel");
        assert_eq!(
            handle_key(&mut u, &state, key(KeyCode::Enter)),
            Some(Action::Cancel)
        );
        assert_eq!(u.mode, Mode::Normal);

        // Backspace edits; case does not matter.
        handle_key(&mut u, &state, chr('c'));
        type_word(&mut u, &state, "cancex");
        handle_key(&mut u, &state, key(KeyCode::Backspace));
        type_word(&mut u, &state, "L");
        assert_eq!(
            handle_key(&mut u, &state, key(KeyCode::Enter)),
            Some(Action::Cancel)
        );
    }

    #[test]
    fn every_other_way_out_of_the_cancel_prompt_does_nothing() {
        let state = live();
        let mut u = ui();
        for no in [key(KeyCode::Esc), ctrl_key('c')] {
            handle_key(&mut u, &state, chr('c'));
            assert_eq!(handle_key(&mut u, &state, no), None);
            assert_eq!(u.mode, Mode::Normal);
        }
        // A Ctrl-modified key neither types into the word nor confirms.
        handle_key(&mut u, &state, chr('c'));
        type_word(&mut u, &state, "cance");
        assert_eq!(handle_key(&mut u, &state, ctrl_key('l')), None);
        assert!(matches!(&u.mode, Mode::Confirm { typed, .. } if typed == "cance"));
        assert_eq!(handle_key(&mut u, &state, key(KeyCode::Enter)), None);
        assert_eq!(u.mode, Mode::Normal);
    }

    /// Keys at human pace: 100 ms apart. Bursts: 1 ms apart.
    fn at(ms: i64) -> Option<OffsetDateTime> {
        Some(datetime!(2026-01-01 00:00:00 UTC) + time::Duration::milliseconds(ms))
    }

    /// Feed `text` as a burst (1 ms apart) starting at `start_ms`; returns every action.
    fn burst(u: &mut UiState, state: &WatchState, text: &str, start_ms: i64) -> Vec<Action> {
        let mut actions = Vec::new();
        for (i, c) in text.chars().enumerate() {
            let k = if c == '\n' {
                key(KeyCode::Enter)
            } else {
                chr(c)
            };
            actions.extend(handle_key_at(u, state, k, at(start_ms + i as i64)));
        }
        actions
    }

    #[test]
    fn typing_a_sentence_into_normal_mode_does_not_cancel_the_run() {
        // The reviewer's exact string: the `c` then `y` inside "policy".
        let state = live();
        let mut u = ui();
        let actions = burst(&mut u, &state, "update the policy doc", 0);
        assert_eq!(actions, vec![], "nothing was sent");
        assert_eq!(u.mode, Mode::Normal, "no prompt was opened by the stream");
        assert!(!actions.contains(&Action::Cancel));
    }

    #[test]
    fn a_paste_that_opens_the_cancel_prompt_cannot_drive_it() {
        // The first key is a person; the rest of the paste is a burst.
        let state = live();
        let mut u = ui();
        handle_key_at(&mut u, &state, chr('c'), at(0));
        assert!(matches!(u.mode, Mode::Confirm { .. }));
        let actions = burst(&mut u, &state, "ancel\n", 1);
        assert_eq!(actions, vec![], "the word typed by a burst does not count");
        assert!(matches!(&u.mode, Mode::Confirm { typed, .. } if typed.is_empty()));
        // A person typing it can.
        let mut t = 1000;
        for c in "cancel".chars() {
            t += 120;
            assert_eq!(handle_key_at(&mut u, &state, chr(c), at(t)), None);
        }
        assert_eq!(
            handle_key_at(&mut u, &state, key(KeyCode::Enter), at(t + 120)),
            Some(Action::Cancel)
        );
    }

    #[test]
    fn a_multi_line_paste_into_the_steer_editor_submits_nothing() {
        let state = live();
        let mut u = ui();
        handle_key_at(&mut u, &state, chr('s'), at(0));
        assert!(u.is_typing());
        let actions = burst(&mut u, &state, "first line\nc\nyes cancel\nlast", 1000);
        assert_eq!(actions, vec![], "no newline in a burst submits");
        let Mode::Input { editor, .. } = &u.mode else {
            panic!("still typing")
        };
        assert_eq!(editor.text(), "first line c yes cancel last");
        // A person pressing Enter afterwards does submit.
        assert_eq!(
            handle_key_at(&mut u, &state, key(KeyCode::Enter), at(5000)),
            Some(Action::Steer {
                message: "first line c yes cancel last".into()
            })
        );
    }

    #[test]
    fn a_two_line_stop_note_cannot_confirm_the_stop() {
        let id = EscalationId::new();
        let state = waiting(&id);
        let mut u = ui();
        // Open the note prompt for `stop`, as a person.
        handle_key_at(&mut u, &state, chr('a'), at(0));
        handle_key_at(&mut u, &state, chr('3'), at(200));
        handle_key_at(&mut u, &state, key(KeyCode::Enter), at(400));
        assert!(matches!(
            &u.mode,
            Mode::Input {
                purpose: Purpose::Note {
                    choice: Choice::Stop,
                    ..
                },
                ..
            }
        ));
        // A pasted note whose second line starts with `y`.
        let actions = burst(&mut u, &state, "not worth it\nyes really", 2000);
        assert_eq!(actions, vec![]);
        assert!(u.is_typing(), "still in the note, not in a confirmation");
        // Enter by a person moves to the confirmation, where only the word counts.
        assert_eq!(
            handle_key_at(&mut u, &state, key(KeyCode::Enter), at(5000)),
            None
        );
        assert!(matches!(
            &u.mode,
            Mode::Confirm {
                what: Confirm::StopAnswer { .. },
                ..
            }
        ));
        assert_eq!(handle_key_at(&mut u, &state, chr('y'), at(5200)), None);
        assert_eq!(
            handle_key_at(&mut u, &state, key(KeyCode::Enter), at(5400)),
            None
        );
        assert_eq!(u.mode, Mode::Normal);
    }

    #[test]
    fn commands_in_a_burst_are_ignored_but_scrolling_is_not() {
        let state = with_decisions(5);
        let mut u = ui();
        handle_key_at(&mut u, &state, chr('x'), at(0));
        for c in ['s', 'a', 'c', '?', 'q', '2'] {
            assert_eq!(handle_key_at(&mut u, &state, chr(c), at(1)), None);
            assert_eq!(u.mode, Mode::Normal);
            assert_eq!(u.focus, Pane::Timeline);
        }
        handle_key_at(&mut u, &state, chr('k'), at(2));
        assert_eq!(u.selected_index(&state), Some(3), "navigation still works");
        // Ctrl-C always quits.
        assert_eq!(
            handle_key_at(&mut u, &state, ctrl_key('c'), at(3)),
            Some(Action::Quit)
        );
    }

    #[test]
    fn confirmations_are_refused_when_the_terminal_cannot_show_them() {
        let state = live();
        let mut u = ui();
        u.viewport = Some((11, 2));
        handle_key(&mut u, &state, chr('c'));
        type_word(&mut u, &state, "cancel");
        // The word is not even accepted into the field.
        assert_eq!(handle_key(&mut u, &state, key(KeyCode::Enter)), None);
        assert!(
            matches!(&u.mode, Mode::Confirm { typed, .. } if typed.is_empty()),
            "{:?}",
            u.mode
        );
        // After a resize it works.
        u.viewport = Some((80, 24));
        type_word(&mut u, &state, "cancel");
        assert_eq!(
            handle_key(&mut u, &state, key(KeyCode::Enter)),
            Some(Action::Cancel)
        );
        // And Esc always gets out.
        u.viewport = Some((11, 2));
        handle_key(&mut u, &state, chr('c'));
        handle_key(&mut u, &state, key(KeyCode::Esc));
        assert_eq!(u.mode, Mode::Normal);
    }

    #[test]
    fn the_decision_list_does_not_act_when_it_cannot_be_shown() {
        let id = EscalationId::new();
        let state = waiting(&id);
        let mut u = ui();
        handle_key(&mut u, &state, chr('a'));
        u.viewport = Some((100, 7));
        handle_key(&mut u, &state, chr('2'));
        handle_key(&mut u, &state, key(KeyCode::Enter));
        assert!(
            matches!(u.mode, Mode::Decide { selected: 0, .. }),
            "{:?}",
            u.mode
        );
        handle_key(&mut u, &state, key(KeyCode::Esc));
        assert_eq!(u.mode, Mode::Normal);
    }

    #[test]
    fn a_second_action_is_refused_before_its_prompt_closes_and_cancel_still_goes() {
        let state = live();
        let mut u = ui();
        u.busy = Some("sending steer...".into());
        handle_key(&mut u, &state, chr('s'));
        type_text(&mut u, &state, "left");
        assert_eq!(handle_key(&mut u, &state, key(KeyCode::Enter)), None);
        let Mode::Input { editor, .. } = &u.mode else {
            panic!("the prompt must stay open with its text")
        };
        assert_eq!(editor.text(), "left");
        assert!(
            u.message
                .as_ref()
                .is_some_and(|m| m.text.contains("still busy"))
        );
        handle_key(&mut u, &state, key(KeyCode::Esc));
        // Cancel is never held back.
        handle_key(&mut u, &state, chr('c'));
        type_word(&mut u, &state, "cancel");
        assert_eq!(
            handle_key(&mut u, &state, key(KeyCode::Enter)),
            Some(Action::Cancel)
        );
    }

    #[test]
    fn answering_is_refused_on_a_run_that_has_ended() {
        let mut u = ui();
        handle_key(&mut u, &finished(), chr('a'));
        assert_eq!(u.mode, Mode::Normal);
        assert!(u.message.is_some());
    }

    #[test]
    fn a_new_escalation_does_not_change_the_open_dialog() {
        let first = EscalationId::new();
        let second = EscalationId::new();
        let mut state = waiting(&first);
        let mut u = ui();
        handle_key(&mut u, &state, chr('a'));
        state.apply(&ev(
            4,
            EventKind::EscalationRaised,
            json!({"escalation_id": second.to_string()}),
        ));
        assert_eq!(
            u.mode,
            Mode::Decide {
                escalation: first,
                position: (1, 1),
                selected: 0
            }
        );
        handle_key(&mut u, &state, chr('1'));
        handle_key(&mut u, &state, key(KeyCode::Enter));
        let action = handle_key(&mut u, &state, key(KeyCode::Enter));
        assert_eq!(
            action,
            Some(Action::Answer {
                escalation: first,
                decision: HumanDecision::Accept,
                note: String::new()
            })
        );
    }

    #[test]
    fn the_stop_note_prompt_says_it_is_for_stop() {
        let purpose = Purpose::Note {
            escalation: EscalationId::new(),
            choice: Choice::Stop,
        };
        assert!(purpose.title().contains("STOP"), "{}", purpose.title());
    }

    #[test]
    fn answer_leads_the_hints_when_a_human_is_needed() {
        assert!(hints(&Mode::Normal, false, true).starts_with("a ANSWER"));
        assert!(hints(&Mode::Normal, false, false).starts_with("q quit"));
    }

    // ---- answering -----------------------------------------------------------

    #[test]
    fn answering_needs_an_open_escalation() {
        let mut u = ui();
        handle_key(&mut u, &live(), chr('a'));
        assert_eq!(u.mode, Mode::Normal);
        assert!(
            u.message
                .as_ref()
                .is_some_and(|m| m.text.contains("no open escalation"))
        );
    }

    #[test]
    fn every_decision_has_a_plain_explanation_and_claim_complete_is_honest() {
        for choice in Choice::ALL {
            assert!(choice.explanation().len() > 20, "{}", choice.name());
        }
        let text = Choice::ClaimComplete.explanation();
        assert!(text.contains("re-measures"), "{text}");
        assert!(text.contains("cannot finish the run by itself"), "{text}");
        assert_eq!(
            Choice::ALL.map(Choice::name),
            ["accept", "recheck", "stop", "claim-complete", "guidance"]
        );
    }

    fn decide(u: &mut UiState, state: &WatchState, number: char) {
        handle_key(u, state, chr('a'));
        assert!(matches!(u.mode, Mode::Decide { .. }), "{:?}", u.mode);
        handle_key(u, state, chr(number));
        handle_key(u, state, key(KeyCode::Enter));
    }

    #[test]
    fn accept_recheck_and_claim_complete_send_with_an_optional_note() {
        let id = EscalationId::new();
        let state = waiting(&id);
        for (number, want) in [
            ('1', HumanDecision::Accept),
            ('2', HumanDecision::Recheck),
            ('4', HumanDecision::ClaimComplete),
        ] {
            let mut u = ui();
            decide(&mut u, &state, number);
            type_text(&mut u, &state, "because");
            let action = handle_key(&mut u, &state, key(KeyCode::Enter));
            assert_eq!(
                action,
                Some(Action::Answer {
                    escalation: id,
                    decision: want,
                    note: "because".into()
                })
            );
            assert_eq!(u.mode, Mode::Normal);
        }
        // Empty note is fine.
        let mut u = ui();
        decide(&mut u, &state, '1');
        assert_eq!(
            handle_key(&mut u, &state, key(KeyCode::Enter)),
            Some(Action::Answer {
                escalation: id,
                decision: HumanDecision::Accept,
                note: String::new()
            })
        );
    }

    #[test]
    fn guidance_requires_text_and_is_the_decision_payload() {
        let id = EscalationId::new();
        let state = waiting(&id);
        let mut u = ui();
        decide(&mut u, &state, '5');
        assert_eq!(handle_key(&mut u, &state, key(KeyCode::Enter)), None);
        assert!(u.is_typing(), "empty guidance is refused");
        type_text(&mut u, &state, "use staging");
        assert_eq!(
            handle_key(&mut u, &state, key(KeyCode::Enter)),
            Some(Action::Answer {
                escalation: id,
                decision: HumanDecision::Guidance {
                    text: "use staging".into()
                },
                note: String::new(),
            })
        );
    }

    #[test]
    fn answering_stop_asks_for_confirmation() {
        let id = EscalationId::new();
        let state = waiting(&id);
        let mut u = ui();
        decide(&mut u, &state, '3');
        type_text(&mut u, &state, "not worth it");
        assert_eq!(handle_key(&mut u, &state, key(KeyCode::Enter)), None);
        assert_eq!(
            u.mode,
            Mode::Confirm {
                what: Confirm::StopAnswer {
                    escalation: id,
                    note: "not worth it".into()
                },
                typed: String::new(),
            }
        );
        type_word(&mut u, &state, "stop");
        assert_eq!(
            handle_key(&mut u, &state, key(KeyCode::Enter)),
            Some(Action::Answer {
                escalation: id,
                decision: HumanDecision::Stop,
                note: "not worth it".into()
            })
        );

        let mut u = ui();
        decide(&mut u, &state, '3');
        handle_key(&mut u, &state, key(KeyCode::Enter));
        assert_eq!(handle_key(&mut u, &state, chr('n')), None);
        assert_eq!(handle_key(&mut u, &state, key(KeyCode::Enter)), None);
        assert_eq!(u.mode, Mode::Normal);
    }

    #[test]
    fn the_decision_list_navigates_wraps_and_backs_out() {
        let id = EscalationId::new();
        let state = waiting(&id);
        let mut u = ui();
        handle_key(&mut u, &state, chr('a'));
        handle_key(&mut u, &state, chr('k'));
        assert!(
            matches!(u.mode, Mode::Decide { selected: 4, .. }),
            "wraps up"
        );
        handle_key(&mut u, &state, chr('j'));
        assert!(
            matches!(u.mode, Mode::Decide { selected: 0, .. }),
            "wraps down"
        );
        handle_key(&mut u, &state, key(KeyCode::Down));
        handle_key(&mut u, &state, key(KeyCode::Down));
        assert!(matches!(u.mode, Mode::Decide { selected: 2, .. }));
        handle_key(&mut u, &state, chr('9'));
        handle_key(&mut u, &state, chr('x'));
        assert!(
            matches!(u.mode, Mode::Decide { selected: 2, .. }),
            "other keys do nothing"
        );
        handle_key(&mut u, &state, key(KeyCode::Esc));
        assert_eq!(u.mode, Mode::Normal);
        handle_key(&mut u, &state, chr('a'));
        handle_key(&mut u, &state, ctrl_key('c'));
        assert_eq!(u.mode, Mode::Normal);
    }

    #[test]
    fn with_several_open_escalations_the_oldest_is_answered_first() {
        let first = EscalationId::new();
        let second = EscalationId::new();
        let mut state = waiting(&first);
        state.apply(&ev(
            4,
            EventKind::EscalationRaised,
            json!({"escalation_id": second.to_string()}),
        ));
        let mut u = ui();
        handle_key(&mut u, &state, chr('a'));
        assert_eq!(
            u.mode,
            Mode::Decide {
                escalation: first,
                position: (1, 2),
                selected: 0
            }
        );
    }

    #[test]
    fn an_escalation_id_that_is_not_an_id_is_reported_not_sent() {
        let mut state = live();
        state.apply(&ev(
            2,
            EventKind::EscalationRaised,
            json!({"escalation_id": "not-an-id"}),
        ));
        let mut u = ui();
        handle_key(&mut u, &state, chr('a'));
        assert_eq!(u.mode, Mode::Normal);
        assert_eq!(u.message.as_ref().map(|m| m.tone), Some(Tone::Error));
    }

    // ---- messages -------------------------------------------------------------

    #[test]
    fn messages_are_sanitised_bounded_and_expire() {
        let mut u = ui();
        u.set_message(Tone::Info, &format!("a\u{1b}[31mb\n{}", "x".repeat(2000)));
        let text = u.message.clone().expect("message").text;
        assert!(text.starts_with("ab "), "{text}");
        assert!(text.chars().count() <= MAX_MESSAGE_CHARS);
        assert!(!text.contains('\u{1b}'));
        let later = u.now + time::Duration::seconds(30);
        assert!(u.tick(later));
        assert!(u.message.is_none());
        // Errors stay until replaced.
        u.set_message(Tone::Error, "daemon gone");
        u.tick(later + time::Duration::seconds(3600));
        assert!(u.message.is_some());
    }

    #[test]
    fn hints_follow_the_mode() {
        assert!(hints(&Mode::Normal, false, false).contains("s steer"));
        assert!(!hints(&Mode::Normal, true, false).contains("s steer"));
        assert!(hints(&Mode::Normal, false, true).contains("a ANSWER"));
        assert!(hints(&Mode::Help, false, false).contains("help"));
    }

    // ---- the narrowed burst guard ---------------------------------------------

    fn typed_in(u: &UiState) -> String {
        match &u.mode {
            Mode::Confirm { typed, .. } => typed.clone(),
            other => panic!("not in a confirmation: {other:?}"),
        }
    }

    #[test]
    fn a_fast_typist_confirms_with_a_separate_enter() {
        // The reviewer's sequence: `c` opens the prompt; then c,a 10 ms apart and
        // n,c,e,l; then Enter as its own act.
        let state = live();
        let mut u = ui();
        handle_key_at(&mut u, &state, chr('c'), at(0));
        assert!(matches!(u.mode, Mode::Confirm { .. }));
        for (c, ms) in [
            ('c', 400),
            ('a', 410),
            ('n', 600),
            ('c', 700),
            ('e', 800),
            ('l', 900),
        ] {
            assert_eq!(handle_key_at(&mut u, &state, chr(c), at(ms)), None);
        }
        assert_eq!(typed_in(&u), "cancel");
        assert_eq!(
            handle_key_at(&mut u, &state, key(KeyCode::Enter), at(1500)),
            Some(Action::Cancel)
        );
    }

    #[test]
    fn characters_typed_quickly_inside_the_confirmation_are_text() {
        let state = live();
        let mut u = ui();
        handle_key_at(&mut u, &state, chr('c'), at(0));
        // After a pause, six characters 2 ms apart (key repeat, a jittery link).
        for (i, c) in "cancel".chars().enumerate() {
            handle_key_at(&mut u, &state, chr(c), at(1000 + 2 * i as i64));
        }
        assert_eq!(typed_in(&u), "cancel");
    }

    #[test]
    fn an_enter_in_the_same_burst_as_the_text_does_not_confirm_and_keeps_the_text() {
        let state = live();
        let mut u = ui();
        handle_key_at(&mut u, &state, chr('c'), at(0));
        // A paste of `cancel\n` into the open confirmation, after a pause.
        let actions = burst(&mut u, &state, "cancel\n", 1000);
        assert_eq!(actions, vec![]);
        assert_eq!(
            typed_in(&u),
            "cancel",
            "the text is intact and the prompt is open"
        );
        assert!(
            u.message
                .as_ref()
                .is_some_and(|m| m.text.contains("press Enter again"))
        );
        // A deliberate Enter afterwards does confirm.
        assert_eq!(
            handle_key_at(&mut u, &state, key(KeyCode::Enter), at(4000)),
            Some(Action::Cancel)
        );
    }

    #[test]
    fn plain_keystrokes_spelling_a_sentence_cannot_cancel_at_any_pace() {
        for pace in [1, 20, 120, 400] {
            let state = live();
            let mut u = ui();
            let mut actions = Vec::new();
            for (i, c) in "update the policy doc".chars().enumerate() {
                actions.extend(handle_key_at(&mut u, &state, chr(c), at(i as i64 * pace)));
            }
            assert_eq!(actions, vec![], "pace {pace} ms");
        }
        // Unstamped keys (tests, non-keyboard sources) too.
        let state = live();
        let mut u = ui();
        for c in "update the policy doc".chars() {
            assert_eq!(handle_key(&mut u, &state, chr(c)), None);
        }
    }

    #[test]
    fn an_unbracketed_multi_line_paste_with_cancel_in_it_cannot_cancel() {
        for text in [
            "notes\nc\ncancel\n",
            "cancel\n",
            "cancel\ncancel\n",
            "ok\ncancel\n\ncancel\n",
        ] {
            let state = live();
            let mut u = ui();
            let actions = burst(&mut u, &state, text, 0);
            assert_eq!(actions, vec![], "{text:?}");
        }
        // The same into the steer editor.
        let state = live();
        let mut u = ui();
        handle_key_at(&mut u, &state, chr('s'), at(0));
        assert_eq!(burst(&mut u, &state, "c\ncancel\n", 1000), vec![]);
    }

    #[test]
    fn the_rest_of_a_burst_that_opened_a_modal_is_swallowed_but_a_pause_ends_it() {
        let id = EscalationId::new();
        let state = waiting(&id);
        let mut u = ui();
        // `a` by a person, then `3` and Enter in the same burst: nothing is chosen.
        handle_key_at(&mut u, &state, chr('a'), at(0));
        assert!(matches!(u.mode, Mode::Decide { selected: 0, .. }));
        handle_key_at(&mut u, &state, chr('3'), at(2));
        handle_key_at(&mut u, &state, key(KeyCode::Enter), at(3));
        assert!(
            matches!(u.mode, Mode::Decide { selected: 0, .. }),
            "{:?}",
            u.mode
        );
        // After a pause the same keys work.
        handle_key_at(&mut u, &state, chr('3'), at(500));
        assert!(matches!(u.mode, Mode::Decide { selected: 2, .. }));
    }

    #[test]
    fn with_several_open_escalations_tab_chooses_which_one_is_answered() {
        let first = EscalationId::new();
        let second = EscalationId::new();
        let third = EscalationId::new();
        let mut state = waiting(&first);
        state.apply(&ev(
            4,
            EventKind::EscalationRaised,
            json!({"escalation_id": second.to_string()}),
        ));
        state.apply(&ev(
            5,
            EventKind::EscalationRaised,
            json!({"escalation_id": third.to_string()}),
        ));
        let mut u = ui();
        handle_key(&mut u, &state, chr('a'));
        assert_eq!(
            u.mode,
            Mode::Decide {
                escalation: first,
                position: (1, 3),
                selected: 0
            }
        );
        handle_key(&mut u, &state, key(KeyCode::Tab));
        assert_eq!(
            u.mode,
            Mode::Decide {
                escalation: second,
                position: (2, 3),
                selected: 0
            }
        );
        handle_key(&mut u, &state, key(KeyCode::Tab));
        handle_key(&mut u, &state, key(KeyCode::Tab));
        assert!(
            matches!(u.mode, Mode::Decide { escalation, position: (1, 3), .. } if escalation == first),
            "wraps"
        );
        handle_key(&mut u, &state, key(KeyCode::BackTab));
        assert!(
            matches!(u.mode, Mode::Decide { escalation, position: (3, 3), .. } if escalation == third)
        );
        handle_key(&mut u, &state, chr('1'));
        handle_key(&mut u, &state, key(KeyCode::Enter));
        assert_eq!(
            handle_key(&mut u, &state, key(KeyCode::Enter)),
            Some(Action::Answer {
                escalation: third,
                decision: HumanDecision::Accept,
                note: String::new()
            })
        );
    }

    #[test]
    fn tab_with_one_open_escalation_changes_nothing() {
        let id = EscalationId::new();
        let state = waiting(&id);
        let mut u = ui();
        handle_key(&mut u, &state, chr('a'));
        let before = u.mode.clone();
        handle_key(&mut u, &state, key(KeyCode::Tab));
        assert_eq!(u.mode, before);
    }
}
