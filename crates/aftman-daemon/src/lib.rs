//! The run loop and its supporting pieces.
//!
//! This is the layer that owns lifecycle, concurrency, and I/O, and it is
//! deliberately thin: evidence assembly is [`obs`], event fan-out is [`sink`], and
//! the decision itself is entirely in `aftman_core` policy. Nothing here interprets
//! a probability.
//!
//! The daemon protocol and the HTTP/SSE surface arrive in M3; M0 exposes the
//! runtime directly to the CLI in `--no-daemon` mode.

#![forbid(unsafe_code)]

pub mod hub;
pub mod obs;
pub mod protocol;
pub mod runtime;
pub mod sink;

pub use hub::{EventHub, HubConfig, Lagged, SubscribeError, Subscription};
pub use obs::{ObsCaps, ObservationBuilder, parse_test_summaries};
pub use runtime::{
    FactoryRuntime, RunOutcome, RuntimeDeps, RuntimeError, code_mission, default_margin,
    verify_mission,
};
pub use sink::{EventSink, MultiSink, NullSink, VecSink};
