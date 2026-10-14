//! The real terminal: raw mode, the alternate screen, and the thread that reads keys.
//!
//! Everything that touches the actual terminal is here, and nothing else in the TUI
//! does. The driver is generic over the ratatui `Backend` and over an input channel,
//! so the tests never reach this file.
//!
//! # Hygiene
//!
//! Raw mode, the alternate screen and bracketed paste are entered once by
//! [`TerminalSession::enter`] and undone by [`restore`], which is idempotent and is
//! called from three places so that no exit path leaves a person with a broken shell:
//! the session's `Drop` (normal return, `?`, and unwinding), the panic hook installed
//! by [`install_panic_hook`] (which restores *before* the panic message prints, so the
//! message lands on the normal screen and stays readable), and the termination-signal
//! handler in the entry point, which turns SIGTERM / SIGHUP into an ordinary quit.
//!
//! # Input
//!
//! crossterm's `poll`/`read` block, so they run on a dedicated OS thread that forwards
//! events into a channel; the async driver `recv`s from the channel and never blocks the
//! runtime. The thread polls with a short timeout so it notices the stop flag and exits
//! on its own shortly after the TUI does.

use std::io::{self, Stdout, Write};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::ThreadId;
use std::time::Duration;

use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::crossterm::cursor::Show;
use ratatui::crossterm::event::{self, DisableBracketedPaste, EnableBracketedPaste, Event};
use ratatui::crossterm::execute;
use ratatui::crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use tokio::sync::mpsc;

use aftman_core::clock::Clock;

use super::driver::TermEvent;

/// How often the input thread wakes to check whether it should stop.
const POLL: Duration = Duration::from_millis(100);

/// Events buffered between the input thread and the driver. Bounded: a driver that is
/// busy drawing applies back-pressure to the thread, not to memory.
const INPUT_QUEUE: usize = 256;

/// Whether a [`TerminalSession`] currently owns the terminal.
static ACTIVE: AtomicBool = AtomicBool::new(false);
/// The thread that runs the TUI's loop; only a panic there leaves the terminal unusable.
static OWNER: OnceLock<ThreadId> = OnceLock::new();

/// The escape sequences that undo [`enter_to`]: bracketed paste off, leave the
/// alternate screen, show the cursor. Written to any writer so it can be tested.
pub fn restore_to<W: Write>(out: &mut W) {
    let _ = execute!(out, DisableBracketedPaste, LeaveAlternateScreen, Show);
    let _ = out.flush();
}

/// The escape sequences for entering TUI mode (raw mode is separate).
///
/// # Errors
/// The writer failed.
pub fn enter_to<W: Write>(out: &mut W) -> io::Result<()> {
    execute!(out, EnterAlternateScreen, EnableBracketedPaste)
}

/// Undo everything [`TerminalSession::enter`] did. Safe to call any number of times,
/// and when nothing was entered.
pub fn restore() {
    restore_to(&mut io::stdout());
    let _ = disable_raw_mode();
}

/// Restore the terminal before a panic message is printed, when the panic is on the
/// thread that runs the TUI.
///
/// A panic on another thread (an action or stream task: the driver reports those as
/// errors and carries on) must not restore, or the loop would keep drawing onto the
/// normal screen; and while the TUI is up its default message would garble the
/// alternate screen, so it is held back (the driver shows "the action did not
/// complete"). Outside a session the previous hook runs as usual.
pub fn install_panic_hook() {
    let _ = OWNER.set(std::thread::current().id());
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if !ACTIVE.load(Ordering::SeqCst) {
            previous(info);
        } else if OWNER.get() == Some(&std::thread::current().id()) {
            restore();
            previous(info);
        }
    }));
}

/// The terminal in TUI mode. Dropping it restores the terminal.
pub struct TerminalSession {
    pub terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl TerminalSession {
    /// Enter raw mode and the alternate screen.
    ///
    /// # Errors
    /// The terminal could not be configured; it is restored before returning.
    pub fn enter() -> io::Result<Self> {
        enable_raw_mode()?;
        ACTIVE.store(true, Ordering::SeqCst);
        let mut out = io::stdout();
        if let Err(error) = enter_to(&mut out) {
            ACTIVE.store(false, Ordering::SeqCst);
            restore();
            return Err(error);
        }
        match Terminal::new(CrosstermBackend::new(out)) {
            Ok(mut terminal) => {
                let _ = terminal.clear();
                Ok(Self { terminal })
            }
            Err(error) => {
                ACTIVE.store(false, Ordering::SeqCst);
                restore();
                Err(error)
            }
        }
    }
}

impl Drop for TerminalSession {
    fn drop(&mut self) {
        ACTIVE.store(false, Ordering::SeqCst);
        restore();
    }
}

/// The key-reading thread. Dropping it asks the thread to stop.
pub struct InputThread {
    stop: Arc<AtomicBool>,
}

impl Drop for InputThread {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
    }
}

/// The input channel, a sender for other sources (signals), and the thread's handle.
pub type InputParts = (
    mpsc::Receiver<TermEvent>,
    mpsc::Sender<TermEvent>,
    InputThread,
);

/// Start reading terminal events on a dedicated thread. Keys are stamped with `clock`
/// (the injected one) when they are read.
///
/// # Errors
/// The thread could not be spawned.
/// Tells the driver the keyboard is gone if the input thread ends for any reason,
/// a panic included, so the TUI quits instead of sitting there deaf.
struct ClosedOnDrop(mpsc::Sender<TermEvent>);

impl Drop for ClosedOnDrop {
    fn drop(&mut self) {
        let _ = self.0.try_send(TermEvent::Closed);
    }
}

pub fn spawn_input_thread(clock: Arc<dyn Clock>) -> io::Result<InputParts> {
    let (tx, rx) = mpsc::channel(INPUT_QUEUE);
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = Arc::clone(&stop);
    let thread_tx = tx.clone();
    std::thread::Builder::new()
        .name("aftman-tui-input".to_owned())
        .spawn(move || {
            let _closed = ClosedOnDrop(thread_tx.clone());
            while !thread_stop.load(Ordering::SeqCst) {
                match event::poll(POLL) {
                    Ok(false) => {}
                    Ok(true) => {
                        let forwarded = match event::read() {
                            Ok(Event::Key(key)) => Some(TermEvent::Stamped(key, clock.now())),
                            Ok(Event::Paste(text)) => Some(TermEvent::Paste(text)),
                            Ok(Event::Resize(..)) => Some(TermEvent::Resize),
                            Ok(_) => None,
                            Err(_) => {
                                let _ = thread_tx.blocking_send(TermEvent::Closed);
                                return;
                            }
                        };
                        if let Some(event) = forwarded
                            && thread_tx.blocking_send(event).is_err()
                        {
                            return;
                        }
                    }
                    Err(_) => {
                        let _ = thread_tx.blocking_send(TermEvent::Closed);
                        return;
                    }
                }
            }
        })?;
    Ok((rx, tx, InputThread { stop }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn written(f: impl FnOnce(&mut Vec<u8>)) -> String {
        let mut out = Vec::new();
        f(&mut out);
        String::from_utf8_lossy(&out).into_owned()
    }

    #[test]
    fn restore_writes_paste_off_then_leaves_the_alternate_screen_then_shows_the_cursor() {
        let text = written(restore_to);
        let paste = text.find("\u{1b}[?2004l").expect("bracketed paste off");
        let leave = text
            .find("\u{1b}[?1049l")
            .expect("leave the alternate screen");
        let show = text.find("\u{1b}[?25h").expect("show the cursor");
        assert!(paste < leave && leave < show, "{text:?}");
    }

    #[test]
    fn enter_writes_the_alternate_screen_and_bracketed_paste_and_restore_undoes_both() {
        let entered = written(|out| enter_to(out).expect("enter"));
        assert!(entered.contains("\u{1b}[?1049h") && entered.contains("\u{1b}[?2004h"));
        // Every mode enter turned on, restore turns off.
        let restored = written(restore_to);
        assert!(restored.contains("\u{1b}[?1049l") && restored.contains("\u{1b}[?2004l"));
    }

    #[test]
    fn restore_is_idempotent() {
        let once = written(restore_to);
        let twice = written(|out| {
            restore_to(out);
            restore_to(out);
        });
        assert_eq!(twice, format!("{once}{once}"));
    }
}
