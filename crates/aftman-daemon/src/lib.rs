//! The run loop and its supporting pieces.
//!
//! This is the layer that owns lifecycle, concurrency, and I/O, and it is
//! deliberately thin: evidence assembly is [`obs`], event fan-out is [`sink`], and
//! the decision itself is entirely in `aftman_core` policy. Nothing here interprets
//! a probability.
//!
//! The daemon (M3) is [`daemon::Daemon`]: [`lifecycle`] claims the socket and pid
//! file, [`manager::RunManager`] owns the runs (one method per protocol method),
//! and [`server::ProtocolServer`] speaks the [`protocol`] over a Unix socket.
//! `--no-daemon` embeds [`runtime::FactoryRuntime`] directly.

#![forbid(unsafe_code)]

pub mod control;
pub mod daemon;
pub mod hub;
pub mod lifecycle;
pub mod manager;
pub mod obs;
pub mod protocol;
pub mod runtime;
pub mod server;
pub mod sink;

pub use control::{ControlError, ControlReceiver, RunControl};
pub use daemon::{Daemon, DaemonConfig, shutdown_signal};
pub use hub::{EventHub, HubConfig, Lagged, SubscribeError, Subscription};
pub use lifecycle::{DaemonPaths, LifecycleError, PathInputs, resolve_paths};
pub use manager::{
    BuiltRun, ManagerConfig, RecoveryReport, RunManager, RuntimeContext, RuntimeFactory,
};
pub use obs::{ObsCaps, ObservationBuilder, parse_test_summaries};
pub use runtime::{
    FactoryRuntime, RunOutcome, RuntimeDeps, RuntimeError, StartedRun, code_mission,
    default_margin, verify_mission,
};
pub use server::{ProtocolServer, ServerConfig};
pub use sink::{EventSink, MultiSink, NullSink, VecSink};
