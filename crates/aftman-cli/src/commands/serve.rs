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

/// The `<config home>` directory, where `[web]` settings live.
fn config_home() -> Option<std::path::PathBuf> {
    std::env::var_os("XDG_CONFIG_HOME")
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|home| std::path::PathBuf::from(home).join(".config"))
        })
}

/// Resolve the `[web]` configuration, including `--bind`.
///
/// Resolved *before* the daemon starts, so a refused posture is a startup error with
/// nothing listening, rather than a surface that comes up and then argues.
fn resolve_web(bind: Option<std::net::SocketAddr>) -> Result<aftman_web::WebConfig, CliError> {
    let env = |key: &str| std::env::var(key).ok();
    let config_home = config_home();
    let mut config = aftman_web::WebConfig::load(config_home.as_deref(), &env)
        .map_err(|error| CliError::usage(error.to_string()))?;
    if let Some(bind) = bind {
        let home = std::env::var_os("HOME").map(std::path::PathBuf::from);
        config = aftman_web::WebConfig::from_parts(
            aftman_web::config::WebParts {
                bind,
                auth: config.auth(),
                token: config.configured_token().map(ToOwned::to_owned),
                allowed_repo_roots: config.allowed_repo_roots().to_vec(),
                allow_non_git_repos: config.allow_non_git_repos(),
                allowed_origins: config.allowed_origins().to_vec(),
                allowed_hosts: config.allowed_hosts().to_vec(),
            },
            home.as_deref(),
        )
        .map_err(|error| CliError::usage(error.to_string()))?;
    }
    Ok(config)
}

/// `aftman serve [--detached] [--bind <addr>] [--print-url]`: run the daemon in the
/// foreground.
///
/// `--detached` is how an auto-started daemon is launched; by the time this runs,
/// `main` has already left the terminal's session and directory (see `autostart`).
/// Either way the daemon ignores SIGHUP: a running job must not die with a terminal.
///
/// The HTTP surface is a second transport in front of the same manager, started here and
/// stopped when the daemon stops. It is off unless `[web] enabled = true` or `--bind` is
/// given: the browser is never required.
///
/// Exit `0` also when another daemon already holds the pid lock (its pid is printed).
///
/// # Errors
/// The socket location is unusable, the web configuration refuses to serve (a non-loopback
/// bind without a token), the port is taken, or the daemon could not take its place
/// (including: another daemon serves this state directory).
pub async fn serve(
    ctx: &Ctx,
    detached: bool,
    bind: Option<std::net::SocketAddr>,
    print_url: bool,
) -> Result<u8, CliError> {
    init_tracing();
    let paths = ctx.paths()?;
    let data_dir = ctx.data_dir()?;
    let web_config = resolve_web(bind)?;
    if print_url {
        // A dry run: say where the UI would be and start nothing. The URL carries the
        // token only when one is configured; a generated token does not exist yet.
        if !web_config.enabled() && bind.is_none() {
            return Err(CliError::usage(
                "the web surface is not enabled: set [web] enabled = true, or pass --bind",
            ));
        }
        outln!("{}", web_config.url(web_config.configured_token()))?;
        if web_config.auth() == aftman_web::AuthMode::Token
            && web_config.configured_token().is_none()
        {
            outln!("(a token will be generated at startup, and the URL will carry it)")?;
        }
        return Ok(exit::FINISHED);
    }

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
    if web_config.enabled() {
        // The daemon reports the posture, so `doctor` and `status` can show what is
        // actually being served. Never the token: `info` is a status field.
        config
            .info
            .insert("web.bind".to_owned(), web_config.bind().to_string());
        config
            .info
            .insert("web.auth".to_owned(), web_config.auth().as_str().to_owned());
    }
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

    // The HTTP surface, if it is wanted. Prepared before it is announced, so a bad
    // configuration cannot leave a half-started daemon behind.
    let mut web_task = None;
    if web_config.enabled() {
        let prepared = Arc::new(
            aftman_web::Web::prepare(web_config.clone())
                .map_err(|error| CliError::usage(error.to_string()))?,
        );
        let listener = tokio::net::TcpListener::bind(prepared.config().bind())
            .await
            .map_err(|error| CliError::io("binding the web surface", error))?;
        let address = listener
            .local_addr()
            .map_err(|error| CliError::io("reading the web surface's address", error))?;
        // The token rides in this URL only when `serve` generated it, which is the "printed
        // once" the design promises. A configured token is the caller's own and is not
        // echoed into a log.
        if prepared.token_was_generated() {
            outln!("web ui      {}", prepared.url())?;
            outln!(
                "            (this token is generated per start and is not stored: save the URL now)"
            )?;
        } else {
            outln!(
                "web ui      {} (auth {})",
                prepared.config().url(None),
                prepared.config().auth().as_str()
            )?;
        }
        let _ = address;
        let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
        let manager = Arc::clone(daemon.manager());
        web_task = Some((
            stop_tx,
            tokio::spawn(async move {
                let _ = aftman_web::serve(prepared, manager, listener, async move {
                    let _ = stop_rx.changed().await;
                })
                .await;
            }),
        ));
    }
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
    // The daemon has stopped (a signal, or a client's `daemon.stop`); the web surface goes
    // with it rather than outliving the manager it reports on.
    if let Some((stop, task)) = web_task.take() {
        let _ = stop.send(true);
        let _ = task.await;
    }
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
