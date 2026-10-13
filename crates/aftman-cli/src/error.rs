//! The CLI's error type and the exit-code policy.
//!
//! Exit codes (`docs/07-surfaces.md` section 4): `0` finished, `1` failed, stopped,
//! cancelled, or the operation did not take effect, `2` configuration, credential or
//! usage error, `3` a run needs a human.

use aftman_daemon::protocol::ErrorCode;

use crate::client::ClientError;

pub mod exit {
    pub const FINISHED: u8 = 0;
    pub const FAILED: u8 = 1;
    pub const CONFIG: u8 = 2;
    pub const NEEDS_HUMAN: u8 = 3;
    /// The user detached from a run with Ctrl-C. Conventional for SIGINT (128 + 2);
    /// it says nothing about how the run ends.
    pub const DETACHED: u8 = 130;
    /// An embedded run cancelled by SIGINT (`128 + 2`), after its worker was stopped.
    pub const INTERRUPTED: u8 = 130;
    /// An embedded run cancelled by SIGTERM (`128 + 15`), after its worker was stopped.
    pub const TERMINATED: u8 = 143;
}

/// Why a command did not complete.
#[derive(Debug, thiserror::Error)]
pub enum CliError {
    /// The command line, or the environment, is wrong. Exit `2`.
    #[error("{0}")]
    Usage(String),

    /// The operation ran and did not take effect (a refused steer, ...). Exit `1`.
    #[error("{0}")]
    Failed(String),

    #[error(transparent)]
    Client(#[from] ClientError),

    #[error(transparent)]
    Location(#[from] crate::autostart::LocationError),

    #[error(transparent)]
    Autostart(#[from] crate::autostart::AutostartError),

    #[error(transparent)]
    Lifecycle(#[from] aftman_daemon::LifecycleError),

    #[error(transparent)]
    Store(#[from] aftman_store::StoreError),

    #[error("{context}: {source}")]
    Io {
        context: String,
        #[source]
        source: std::io::Error,
    },

    #[error("{0}")]
    Internal(String),

    /// The reader of our output went away (`aftman ls | head`). Not a failure: the
    /// process ends quietly with success.
    #[error("output closed")]
    BrokenPipe,
}

impl CliError {
    #[must_use]
    pub fn usage(message: impl Into<String>) -> Self {
        Self::Usage(message.into())
    }

    #[must_use]
    pub fn io(context: impl Into<String>, source: std::io::Error) -> Self {
        Self::Io {
            context: context.into(),
            source,
        }
    }

    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }

    /// The process exit code for this error.
    #[must_use]
    pub fn exit_code(&self) -> u8 {
        match self {
            Self::Failed(_) => exit::FAILED,
            Self::Client(error) => client_exit_code(error),
            Self::Autostart(crate::autostart::AutostartError::Client(error)) => {
                client_exit_code(error)
            }
            Self::BrokenPipe => exit::FINISHED,
            Self::Usage(_)
            | Self::Location(_)
            | Self::Autostart(_)
            | Self::Lifecycle(_)
            | Self::Store(_)
            | Self::Io { .. }
            | Self::Internal(_) => exit::CONFIG,
        }
    }
}

/// `2` for errors the user fixes by changing what they asked for or how the machine
/// is set up; `1` for a request that was fine and did not work out.
#[must_use]
pub fn client_exit_code(error: &ClientError) -> u8 {
    match error {
        ClientError::Protocol(protocol) => protocol_exit_code(protocol.code),
        ClientError::NotRunning(_)
        | ClientError::Connect { .. }
        | ClientError::ConnectTimeout(_) => exit::CONFIG,
        _ => exit::FAILED,
    }
}

#[must_use]
pub const fn protocol_exit_code(code: ErrorCode) -> u8 {
    match code {
        ErrorCode::InvalidRepo
        | ErrorCode::InvalidConfig
        | ErrorCode::ModelUnavailable
        | ErrorCode::InvalidRequest
        | ErrorCode::UnknownRun
        | ErrorCode::UnknownMethod => exit::CONFIG,
        ErrorCode::WrongState
        | ErrorCode::SteeringUnsupported
        | ErrorCode::RateLimited
        | ErrorCode::Internal
        | ErrorCode::SubscriberLagged
        | ErrorCode::Unknown => exit::FAILED,
    }
}

/// The exit code for a run that ended with `status` (the `status` field of its
/// `RUN_FINISHED` event, or a `FactoryStatus` name).
#[must_use]
pub fn run_exit_code(status: &str) -> u8 {
    match status {
        "finished" => exit::FINISHED,
        "needs_human" => exit::NEEDS_HUMAN,
        _ => exit::FAILED,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aftman_daemon::protocol::ProtocolError;

    #[test]
    fn run_statuses_map_to_the_documented_exit_codes() {
        assert_eq!(run_exit_code("finished"), 0);
        for failed in ["failed", "cancelled", "stopped"] {
            assert_eq!(run_exit_code(failed), 1, "{failed}");
        }
        assert_eq!(run_exit_code("needs_human"), 3);
    }

    #[test]
    fn configuration_and_usage_errors_exit_two() {
        for code in [
            ErrorCode::InvalidRepo,
            ErrorCode::InvalidConfig,
            ErrorCode::ModelUnavailable,
            ErrorCode::InvalidRequest,
            ErrorCode::UnknownRun,
        ] {
            let error = CliError::Client(ClientError::Protocol(ProtocolError::new(code, "x")));
            assert_eq!(error.exit_code(), 2, "{code:?}");
        }
        assert_eq!(CliError::usage("bad").exit_code(), 2);
        assert_eq!(CliError::internal("no home directory").exit_code(), 2);
    }

    #[test]
    fn operations_that_did_not_take_effect_exit_one() {
        for code in [
            ErrorCode::WrongState,
            ErrorCode::SteeringUnsupported,
            ErrorCode::RateLimited,
            ErrorCode::Internal,
        ] {
            let error = CliError::Client(ClientError::Protocol(ProtocolError::new(code, "x")));
            assert_eq!(error.exit_code(), 1, "{code:?}");
        }
        assert_eq!(CliError::Failed("rejected".into()).exit_code(), 1);
    }

    #[test]
    fn a_closed_stdout_is_a_quiet_success() {
        assert_eq!(CliError::BrokenPipe.exit_code(), 0);
    }
}
