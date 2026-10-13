//! The only way commands write to the terminal.
//!
//! Two guarantees, both here so no command can forget them:
//!
//! - everything is passed through [`sanitize`](crate::sanitize::sanitize), so no
//!   worker, model, job or daemon text can drive the operator's terminal;
//! - a closed pipe (`aftman ls | head`) is [`CliError::BrokenPipe`], which ends the
//!   process quietly with success, instead of the panic `println!` would raise.
//!
//! Use the [`outln!`] macro (stdout) and [`errln!`] (stderr) instead of `println!`.

use std::io::Write as _;

use crate::error::CliError;
use crate::sanitize::sanitize;

fn broken(error: &std::io::Error) -> CliError {
    if error.kind() == std::io::ErrorKind::BrokenPipe {
        CliError::BrokenPipe
    } else {
        CliError::io(
            "writing to the terminal",
            std::io::Error::from(error.kind()),
        )
    }
}

/// Write `text` (sanitised, exactly as given: no newline added) to stdout.
///
/// # Errors
/// [`CliError::BrokenPipe`] when the reader has gone.
pub fn text(text: &str) -> Result<(), CliError> {
    let clean = sanitize(text);
    let mut out = std::io::stdout().lock();
    out.write_all(clean.as_bytes())
        .and_then(|()| out.flush())
        .map_err(|e| broken(&e))
}

/// Write `text` and a newline to stdout.
///
/// # Errors
/// As [`text`].
pub fn line(text: &str) -> Result<(), CliError> {
    let mut clean = sanitize(text);
    clean.push('\n');
    let mut out = std::io::stdout().lock();
    out.write_all(clean.as_bytes())
        .and_then(|()| out.flush())
        .map_err(|e| broken(&e))
}

/// Write `text` and a newline to stderr. A stderr that cannot be written is ignored:
/// there is nowhere left to report it.
pub fn err_line(text: &str) {
    let mut clean = sanitize(text);
    clean.push('\n');
    let _ = std::io::stderr().lock().write_all(clean.as_bytes());
}

/// `println!` for commands: sanitised, and a closed pipe is an `Err`.
#[macro_export]
macro_rules! outln {
    () => { $crate::out::line("") };
    ($($arg:tt)*) => { $crate::out::line(&format!($($arg)*)) };
}

/// `eprintln!` for commands: sanitised.
#[macro_export]
macro_rules! errln {
    ($($arg:tt)*) => { $crate::out::err_line(&format!($($arg)*)) };
}
