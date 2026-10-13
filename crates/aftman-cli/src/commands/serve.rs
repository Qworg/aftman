//! `aftman serve`, `aftman stop`, `aftman status`.
//!
//! `status` and `stop` never start a daemon: asking whether one runs must not make
//! one run.

use std::sync::Arc;
use std::time::Duration;

use tokio::signal::unix::{SignalKind, signal};

use aftman_core::clock::SystemClock;
use aftman_daemon::{Daemon, DaemonConfig, DaemonPresence, LifecycleError, probe, shutdown_signal};
use aftman_store::FileRunStore;

use crate::autostart::{connect_existing, wait_until_gone};
use crate::commands::{Ctx, provider_comparison};
use crate::error::{CliError, exit};
use crate::factory::{CliFactory, Mode};
use crate::outln;

/// How long `stop` waits for the daemon to finish shutting down: the manager's own
/// shutdown grace (60 s) plus the connection drain.
pub const STOP_WAIT: Duration = Duration::from_secs(75);

/// `aftman serve [--detached]`: run the daemon in the foreground.
///
/// `--detached` is how an auto-started daemon is launched; by the time this runs,
/// `main` has already left the terminal's session and directory (see `autostart`).
/// Either way the daemon ignores SIGHUP: a running job must not die with a terminal.
///
/// Exit `0` also when another daemon already holds the pid lock (its pid is printed).
///
/// # Errors
/// The socket location is unusable, or the daemon could not take its place
/// (including: another daemon serves this state directory).
pub async fn serve(ctx: &Ctx, detached: bool) -> Result<u8, CliError> {
    init_tracing();
    let paths = ctx.paths()?;
    let data_dir = ctx.data_dir()?;

    // Handlers first, so a signal during startup (recovery can take a while) is not lost.
    let shutdown = shutdown_signal().map_err(|e| CliError::io("installing signal handlers", e))?;
    let mut hangup =
        signal(SignalKind::hangup()).map_err(|e| CliError::io("ignoring SIGHUP", e))?;
    tokio::spawn(async move { while hangup.recv().await.is_some() {} });

    let store = Arc::new(FileRunStore::new(&data_dir));
    let factory = Arc::new(CliFactory::new(&data_dir, ctx.inputs.clone(), Mode::Daemon));
    let mut config = DaemonConfig::new(paths.clone());
    config.state_dir = Some(data_dir.clone());
    config.info = factory.daemon_info();
    let started = Daemon::start(config, store, factory, Arc::new(SystemClock)).await;
    let daemon = match started {
        Ok(daemon) => daemon,
        Err(LifecycleError::AlreadyRunning { pid }) => {
            if pid == 0 {
                outln!("aftman daemon already running (pid not yet written)")?;
            } else {
                outln!("aftman daemon already running (pid {pid})")?;
            }
            return Ok(exit::FINISHED);
        }
        Err(error) => return Err(CliError::Lifecycle(error)),
    };

    outln!(
        "aftman daemon listening on {} (pid {}{})",
        daemon.paths().socket.display(),
        std::process::id(),
        if detached { ", detached" } else { "" }
    )?;
    outln!("state dir {}", data_dir.display())?;
    let recovery = daemon.recovery();
    if recovery.failed.is_empty() {
        outln!("recovery: no unfinished runs")?;
    } else {
        outln!(
            "recovery: marked {} unfinished run(s) failed (daemon restarted):",
            recovery.failed.len()
        )?;
        for id in &recovery.failed {
            outln!("  {id}")?;
        }
    }
    for id in &recovery.claimed_elsewhere {
        outln!("recovery: left {id} alone (driven by another process)")?;
    }
    for (id, why) in &recovery.unreadable {
        outln!("recovery: skipped unreadable run {id}: {why}")?;
    }

    daemon.serve(shutdown).await;
    outln!("aftman daemon stopped")?;
    Ok(exit::FINISHED)
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    // Ignore the error: a second initialisation (tests) is harmless.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .try_init();
}

/// `aftman stop`: ask the daemon to stop and wait until it is gone (nothing holds its
/// lock, its socket and pid file are removed). Exit `0` when it stopped, and also when
/// none was running. A daemon that is already shutting down is waited for, not
/// reported as "not running".
///
/// # Errors
/// The daemon did not finish shutting down in time.
pub async fn stop(ctx: &Ctx) -> Result<u8, CliError> {
    let paths = ctx.paths()?;
    match connect_existing(&paths, ctx.client).await {
        Ok(mut client) => {
            client.stop().await?;
        }
        Err(error) if error.is_not_running() => match probe(&paths).await {
            DaemonPresence::Busy { pid } => {
                outln!(
                    "aftman daemon (pid {pid}) is already starting up or shutting down; waiting"
                )?;
            }
            _ => {
                outln!("aftman daemon is not running")?;
                return Ok(exit::FINISHED);
            }
        },
        Err(error) => return Err(error.into()),
    }
    if wait_until_gone(&paths, STOP_WAIT).await {
        outln!("aftman daemon stopped")?;
        Ok(exit::FINISHED)
    } else {
        let holder = match probe(&paths).await {
            DaemonPresence::Busy { pid } | DaemonPresence::Running { pid } => {
                format!(" (pid {pid})")
            }
            DaemonPresence::NotRunning => String::new(),
        };
        Err(CliError::Failed(format!(
            "asked the daemon{holder} to stop, but it is still shutting down after \
             {STOP_WAIT:?}: it is waiting for its runs to be cancelled"
        )))
    }
}

/// `aftman status`: print the daemon's status. Exit `0` when it is running, `1` when
/// it is not (or is starting up or shutting down), so `aftman status && ...` reads
/// naturally in a script.
///
/// # Errors
/// The daemon is reachable but unhealthy.
pub async fn status(ctx: &Ctx) -> Result<u8, CliError> {
    let paths = ctx.paths()?;
    let mut client = match connect_existing(&paths, ctx.client).await {
        Ok(client) => client,
        Err(error) if error.is_not_running() => {
            match probe(&paths).await {
                DaemonPresence::Busy { pid } => outln!(
                    "aftman daemon (pid {pid}) is starting up or shutting down: it holds its \
                     lock but is not answering on {}",
                    paths.socket.display()
                )?,
                _ => outln!(
                    "aftman daemon is not running (socket {})",
                    paths.socket.display()
                )?,
            }
            return Ok(exit::FAILED);
        }
        Err(error) => return Err(error.into()),
    };
    let status = client.status().await?;
    outln!("aftman daemon running")?;
    outln!("  pid        {}", status.pid)?;
    outln!("  version    {}", status.version)?;
    outln!("  uptime     {}s", status.uptime_secs)?;
    if let Some(started) = &status.started_at {
        outln!("  started    {started}")?;
    }
    outln!(
        "  runs       {} total, {} live",
        status.runs_total,
        status.runs_active
    )?;
    outln!("  socket     {}", paths.socket.display())?;
    match &status.state_dir {
        Some(dir) => outln!("  state dir  {}", dir.display())?,
        None => outln!("  state dir  (not reported)")?,
    }
    if let Err(error) = ctx.check_daemon(&paths, &status) {
        outln!("  NOTE: {error}")?;
    }
    for line in provider_comparison(ctx, &status) {
        outln!("{line}")?;
    }
    Ok(exit::FINISHED)
}
