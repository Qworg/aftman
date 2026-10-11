//! The System One decision-model client.
//!
//! **This is the only crate in aftman that speaks Jev.** It owns the endpoint, the
//! headers, the retry policy, the answer wire shapes, the request budget, and usage
//! accounting. Nothing outside it names `systemone`, reads `TYPESAFE_*`, or sees an
//! answer type — everything else depends on [`aftman_core::model::DecisionModel`].
//!
//! That isolation is `AGENTS.md` hard rule 2, and it is also what keeps the rest of
//! the system testable without a provider.
//!
//! Module map:
//!
//! | Module | Owns |
//! | --- | --- |
//! | [`wire`] | the request and response JSON shapes |
//! | [`config`] | settings, env resolution, and base-URL safety |
//! | [`transport`] | the HTTP boundary, and the offline fixture double |
//! | [`retry`] | backoff and `Retry-After` |
//! | [`budget`] | fitting an observation into a byte budget |
//! | [`usage`] | the per-request token log |
//! | [`error`] | HTTP status to [`ModelError`](aftman_core::error::ModelError) |
//! | [`client`] | the [`DecisionModel`](aftman_core::model::DecisionModel) implementation |

#![forbid(unsafe_code)]

pub mod budget;
pub mod client;
pub mod config;
pub mod error;
pub mod retry;
pub mod transport;
pub mod usage;
pub mod wire;

pub use budget::{BudgetFit, fit};
pub use client::{ProbeResult, SystemOneClient};
pub use config::{
    JevSettings, SettingsError, default_env_file, load_env_file, parse_env_file, resolve,
};
pub use retry::{RetryAfter, RetryPolicy, retry_after};
pub use transport::{
    FixtureResponse, FixtureTransport, HttpRequest, HttpResponse, ReqwestTransport, Transport,
};
pub use usage::{UsageEntry, UsageRecorder, UsageTotals};
