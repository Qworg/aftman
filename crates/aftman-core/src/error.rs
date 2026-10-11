//! Errors raised by the domain layer.

use thiserror::Error;

use crate::job::FactoryStatus;

/// A string could not be parsed into an identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum IdError {
    #[error("identifier is not a valid UUID")]
    MalformedUuid,
}

/// An attempted status change that the state machine does not permit.
///
/// This is a programming error rather than a runtime condition. In debug builds
/// and tests it should abort loudly; the runtime records it as an `ERROR` event
/// rather than silently accepting an illegal transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("illegal factory status transition: {from} -> {to}")]
pub struct TransitionError {
    pub from: FactoryStatus,
    pub to: FactoryStatus,
}

/// A decision-model call failed.
///
/// The variants map one-to-one onto the error table in `docs/03-jev.md` so that
/// retry behaviour and operator messaging can be decided without inspecting a
/// provider-specific error body.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ModelError {
    /// Connection failure, DNS failure, TLS failure.
    #[error("model transport failure: {0}")]
    Transport(String),

    #[error("model request timed out after {0:?}")]
    Timeout(std::time::Duration),

    /// The provider asked us to slow down. `retry_after` is honoured when present.
    #[error("rate limited{}", match .retry_after {
        Some(d) => format!(", retry after {d:?}"),
        None => String::new(),
    })]
    RateLimited {
        retry_after: Option<std::time::Duration>,
    },

    /// A 5xx from the provider.
    #[error("provider error {status}: {message}")]
    Server { status: u16, message: String },

    /// A 4xx that is the caller's fault (bad request, validation).
    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("authentication rejected")]
    Auth,

    #[error("endpoint not found")]
    Endpoint,

    /// The body was readable but did not match the expected shape.
    #[error("malformed response: {0}")]
    Malformed(String),

    /// A requested check had no answer. Fatal: supervising with a missing
    /// dimension is worse than not supervising, because a missing check silently
    /// reads as "no signal" rather than "unknown".
    #[error("response omitted requested answer(s): {0}")]
    MissingAnswer(String),

    /// An answer was present but out of range or of the wrong type.
    #[error("response contained an invalid answer: {0}")]
    InvalidAnswer(String),

    /// The observation could not be compacted inside the byte budget without
    /// dropping required evidence, so the request was refused rather than sent
    /// with the job or the current status missing.
    #[error("request exceeds the evidence budget: {0}")]
    BudgetExceeded(String),
}

impl ModelError {
    /// Whether a retry could plausibly succeed.
    ///
    /// Only transport problems, timeouts, rate limits, and 5xx are retryable.
    /// A missing answer and a malformed body are *not*: the provider will almost
    /// certainly answer the same way, and a retry loop on those burns money.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Transport(_) | Self::Timeout(_) | Self::RateLimited { .. } | Self::Server { .. }
        )
    }

    /// A short stable code suitable for a log field or a protocol error payload.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Transport(_) => "transport",
            Self::Timeout(_) => "timeout",
            Self::RateLimited { .. } => "rate_limited",
            Self::Server { .. } => "server",
            Self::BadRequest(_) => "bad_request",
            Self::Auth => "auth",
            Self::Endpoint => "endpoint",
            Self::Malformed(_) => "malformed",
            Self::MissingAnswer(_) => "missing_answer",
            Self::InvalidAnswer(_) => "invalid_answer",
            Self::BudgetExceeded(_) => "budget_exceeded",
        }
    }
}
