//! The daemon, assembled: lifecycle, manager, and socket server.
//!
//! Start order is deliberate (`docs/07-surfaces.md` section 9):
//!
//! 1. **Take the pid lock.** A second daemon stops here with
//!    [`LifecycleError::AlreadyRunning`], before it has touched a run or the socket.
//! 2. **Recover.** Runs an earlier process left unfinished are marked `Failed`.
//!    This happens before the socket exists, so no client ever observes a zombie.
//! 3. **Bind** the socket (replacing a stale one, never a live one).
//! 4. **Serve** until the caller's shutdown future resolves or a client sends
//!    `daemon.stop`, then shut down gracefully and remove the socket and pid files.

use std::future::Future;
use std::sync::Arc;

use aftman_core::clock::Clock;
use aftman_store::RunStore;

use crate::hub::{EventHub, HubConfig};
use crate::lifecycle::{self, ClaimGuard, DaemonPaths, LifecycleError};
use crate::manager::{ManagerConfig, RecoveryReport, RunManager, RuntimeFactory};
use crate::server::{ProtocolServer, ServerConfig};

/// Everything tunable about a daemon.
#[derive(Debug, Clone)]
pub struct DaemonConfig {
    pub paths: DaemonPaths,
    pub manager: ManagerConfig,
    pub server: ServerConfig,
    pub hub: HubConfig,
}

impl DaemonConfig {
    /// Defaults everywhere except the paths.
    #[must_use]
    pub fn new(paths: DaemonPaths) -> Self {
        Self {
            paths,
            manager: ManagerConfig::default(),
            server: ServerConfig::default(),
            hub: HubConfig::default(),
        }
    }
}

/// A daemon that has claimed its place, recovered, and bound its socket, and is
/// ready to [`Daemon::serve`]. Once `start` returns, clients can connect.
pub struct Daemon {
    manager: Arc<RunManager>,
    server: ProtocolServer,
    listener: tokio::net::UnixListener,
    guard: ClaimGuard,
    recovery: RecoveryReport,
}

impl Daemon {
    /// Claim the pid lock, run recovery, and bind the socket.
    ///
    /// # Errors
    /// [`LifecycleError::AlreadyRunning`] (print the pid, exit 0),
    /// [`LifecycleError::SocketInUse`], path and permission errors.
    pub async fn start(
        config: DaemonConfig,
        store: Arc<dyn RunStore>,
        factory: Arc<dyn RuntimeFactory>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, LifecycleError> {
        let lock = lifecycle::lock(&config.paths).await?;
        let hub = EventHub::new(Arc::clone(&store), config.hub);
        let manager = RunManager::new(store, hub, factory, clock, config.manager);
        let recovery = manager.recover().await;
        let claimed = lock.bind().await?;
        tracing::info!(
            socket = %claimed.guard.paths().socket.display(),
            recovered = recovery.failed.len(),
            "daemon listening"
        );
        Ok(Self {
            server: ProtocolServer::new(Arc::clone(&manager), config.server),
            manager,
            listener: claimed.listener,
            guard: claimed.guard,
            recovery,
        })
    }

    /// What restart recovery did.
    #[must_use]
    pub fn recovery(&self) -> &RecoveryReport {
        &self.recovery
    }

    /// The manager, for transports that share this process (HTTP in M4).
    #[must_use]
    pub fn manager(&self) -> &Arc<RunManager> {
        &self.manager
    }

    /// The paths this daemon holds.
    #[must_use]
    pub fn paths(&self) -> &DaemonPaths {
        self.guard.paths()
    }

    /// Serve until `shutdown` resolves or a client sends `daemon.stop`, shut down
    /// gracefully (live runs end `Cancelled`, reason `daemon shutdown`), and remove
    /// the socket and pid files.
    pub async fn serve(self, shutdown: impl Future<Output = ()>) {
        let Self {
            server,
            listener,
            guard,
            ..
        } = self;
        server.serve(listener, shutdown).await;
        guard.cleanup().await;
    }
}

/// A future that resolves on SIGINT or SIGTERM, for `Daemon::serve`.
///
/// The handlers are installed when this function is called, so a signal arriving
/// between calling it and awaiting the result is not lost.
///
/// # Errors
/// If the signal handlers cannot be installed.
pub fn shutdown_signal() -> std::io::Result<impl Future<Output = ()>> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate())?;
    let mut int = signal(SignalKind::interrupt())?;
    Ok(async move {
        tokio::select! {
            _ = term.recv() => {}
            _ = int.recv() => {}
        }
    })
}
