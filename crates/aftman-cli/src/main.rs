//! The `aftman` binary: a thin shell over [`aftman_cli`].

use std::process::ExitCode;

fn main() -> ExitCode {
    aftman_cli::main_entry()
}
