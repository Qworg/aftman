//! The `aftman` command-line interface, as a library so it can be tested.
//!
//! `aftman serve` runs the daemon; every other subcommand is a client of it, or of the
//! embedded runtime under `--no-daemon` (`docs/02-lld.md` section 2,
//! `docs/07-surfaces.md` section 4).
//!
//! | Module | Owns |
//! | --- | --- |
//! | [`client`] | the typed NDJSON client, and the self-recovering event [`client::Follow`] stream (the TUI builds on both) |
//! | [`autostart`] | finding the daemon and starting it detached |
//! | [`factory`] | the real `RuntimeFactory`: the composition root |
//! | [`render`] | terminal formatting |
//! | [`cli`] | the command line and client-side validation |
//! | [`commands`] | one function per subcommand |
//! | [`follow`] | the self-recovering event stream |
//! | [`out`], [`sanitize`] | the only way to the terminal, and what it strips |
//! | [`statedir`] | private creation of the state directory, log rotation |
//! | [`tui`] | `aftman watch`: the terminal UI (pure state, view and input; async driver) |
//! | [`runid`], [`error`] | run-id prefixes, exit codes |

#![forbid(unsafe_code)]

pub mod autostart;
pub mod cli;
pub mod client;
pub mod commands;
pub mod error;
pub mod factory;
pub mod follow;
pub mod out;
pub mod render;
pub mod runid;
pub mod sanitize;
pub mod statedir;
pub mod tui;

use std::process::ExitCode;

use clap::Parser as _;

use crate::cli::{Cli, Command};

/// The process entry point, called by `main`.
///
/// Deliberately **not** `#[tokio::main]`: a detached daemon must call `setsid()`
/// before any thread exists, so the runtime is built by hand after that step.
#[must_use]
pub fn main_entry() -> ExitCode {
    let cli = Cli::parse();

    if matches!(cli.command, Command::Serve { detached: true }) {
        // Synchronously, first, single-threaded. Failure (already a group leader) is
        // not fatal: the daemon then simply keeps the session it has.
        if let Err(error) = autostart::leave_session() {
            eprintln!("warning: could not detach cleanly: {error}");
        }
    }

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("error: could not start the async runtime: {error}");
            return ExitCode::from(error::exit::CONFIG);
        }
    };
    match runtime.block_on(commands::dispatch(cli)) {
        Ok(code) => ExitCode::from(code),
        Err(error) => {
            // A closed pipe is the reader's choice, not a failure. Anything else is
            // printed sanitised: it may carry text from a daemon or a worker.
            if !matches!(error, error::CliError::BrokenPipe) {
                out::err_line(&format!("error: {error}"));
            }
            ExitCode::from(error.exit_code())
        }
    }
}
