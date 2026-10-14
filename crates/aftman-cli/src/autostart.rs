//! Finding the daemon, and starting it when a command needs one.
//!
//! # Detaching without `unsafe`
//!
//! A classic daemon double-forks. `fork` is `unsafe` and this workspace forbids
//! `unsafe`, so the detach is done another way (docs/07-surfaces.md section 1):
//!
//! 1. the CLI spawns `current_exe() serve --detached` with stdin closed and
//!    stdout/stderr appended to `<data dir>/daemon.log` (`0600`, rotated at a size
//!    cap), and does not wait for it;
//! 2. that child, **before any tokio runtime or thread exists**, calls
//!    `nix::unistd::setsid()` (a safe wrapper) to leave the terminal's session, so
//!    closing the terminal sends it no SIGHUP and a running job survives, and
//!    `chdir("/")`, so it does not keep the first CLI's working directory (which it
//!    would otherwise hand to every later job). Every path passed to it is therefore
//!    made absolute by the client first ([`Locations::absolute`]).
//!
//! The environment is deliberately inherited whole. This is the supervisor
//! re-executing itself, not a worker, and it needs the provider credentials the user
//! set; hard rule 3 (`worker_environment()`) governs subprocesses that run *agents*
//! and does not apply here. Anything the daemon later spawns as a worker still goes
//! through `worker_environment()`. The consequence, documented in docs/07 and 08: the
//! daemon keeps the environment it was started with; `aftman stop` restarts it.
//!
//! Readiness is "the socket accepts a connection", polled with a short, growing
//! interval for [`READY_TIMEOUT`]. Two CLIs racing to start a daemon both succeed:
//! each spawns one, the pid-file lock lets exactly one win, and the loser exits `0`
//! (`AlreadyRunning`); both CLIs then connect to the winner. A daemon that is shutting
//! down (it has stopped listening but still holds its lock while runs are cancelled) is
//! waited for, not raced.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use tokio::time::Instant;

use aftman_daemon::lifecycle::{
    DaemonPaths, DaemonPresence, LifecycleError, PathInputs, probe, probe_state_dir, resolve_paths,
};

use crate::client::{Client, ClientConfig, ClientError};

/// How long to wait for a freshly started daemon to accept connections.
pub const READY_TIMEOUT: Duration = Duration::from_secs(5);
/// How long to wait for a daemon that is shutting down before starting a new one: the
/// manager's shutdown grace (60 s) plus the connection drain.
pub const SHUTDOWN_WAIT: Duration = Duration::from_secs(75);
const FIRST_POLL: Duration = Duration::from_millis(15);
const MAX_POLL: Duration = Duration::from_millis(150);

/// File name of the detached daemon's log inside the data directory.
pub const LOG_FILE_NAME: &str = crate::statedir::LOG_FILE;

/// Where the daemon lives could not be determined.
#[derive(Debug, thiserror::Error)]
pub enum LocationError {
    #[error(transparent)]
    Paths(#[from] LifecycleError),
    #[error("could not determine a state directory; set --data-dir or $XDG_STATE_HOME")]
    NoStateDir,
    #[error("could not determine the current directory: {0}")]
    Cwd(#[source] std::io::Error),
}

/// Where the daemon lives, as far as the command line and environment say.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Locations {
    /// `--socket` / `$AFTMAN_SOCKET`.
    pub socket_flag: Option<PathBuf>,
    /// `--data-dir` / `$AFTMAN_DATA_DIR`.
    pub data_dir_flag: Option<PathBuf>,
}

impl Locations {
    /// The same locations with relative paths made absolute against the current
    /// directory. A daemon started by this CLI does not share its working directory,
    /// so a relative `--data-dir state` must not be handed over as written.
    ///
    /// # Errors
    /// The current directory cannot be determined.
    pub fn absolute(self) -> Result<Self, LocationError> {
        let make = |path: PathBuf| -> Result<PathBuf, LocationError> {
            if path.is_absolute() {
                Ok(path)
            } else {
                Ok(std::env::current_dir()
                    .map_err(LocationError::Cwd)?
                    .join(path))
            }
        };
        Ok(Self {
            socket_flag: self.socket_flag.map(make).transpose()?,
            data_dir_flag: self.data_dir_flag.map(make).transpose()?,
        })
    }
}

/// Fill [`PathInputs`] from the flag and the process environment
/// (`XDG_RUNTIME_DIR`, `HOME`).
#[must_use]
pub fn path_inputs(socket_flag: Option<PathBuf>) -> PathInputs {
    PathInputs {
        socket_override: socket_flag,
        pid_override: None,
        xdg_runtime_dir: std::env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from),
        home: std::env::var_os("HOME").map(PathBuf::from),
    }
}

/// Resolve the daemon's paths.
///
/// # Errors
/// No location can be determined, or the path is unusable (relative, too long for
/// `sun_path`).
pub fn resolve(locations: &Locations) -> Result<DaemonPaths, LocationError> {
    Ok(resolve_paths(&path_inputs(locations.socket_flag.clone()))?)
}

/// The state directory: `--data-dir`, else the default.
///
/// # Errors
/// Neither is available.
pub fn data_dir(locations: &Locations) -> Result<PathBuf, LocationError> {
    match &locations.data_dir_flag {
        Some(dir) => Ok(dir.clone()),
        None => aftman_store::FileRunStore::default_root().ok_or(LocationError::NoStateDir),
    }
}

/// Why a daemon could not be started.
#[derive(Debug, thiserror::Error)]
pub enum AutostartError {
    #[error(transparent)]
    Client(#[from] ClientError),

    #[error(transparent)]
    Location(#[from] LocationError),

    #[error("could not start the daemon: {0}")]
    Spawn(String),

    #[error(
        "the daemon did not accept connections within {after:?}; see its log at {}",
        log.display()
    )]
    NotReady { after: Duration, log: PathBuf },

    #[error(
        "the daemon exited ({status}) before it was ready; see its log at {}",
        log.display()
    )]
    Exited { status: String, log: PathBuf },

    #[error(
        "another aftman daemon (pid {pid}) already serves the state directory {} on a \
         different socket; use that daemon's socket (pass --socket, or unset it), or stop \
         it first. Two daemons on one state directory would corrupt each other's runs.",
        state_dir.display()
    )]
    StateDirOwned { pid: u32, state_dir: PathBuf },

    #[error("the daemon (pid {pid}) is still shutting down after {after:?}; try again shortly")]
    StillShuttingDown { pid: u32, after: Duration },
}

/// Where a long wait reports to ("the daemon is recovering runs"). A command prints it
/// to stderr; a full-screen UI shows it in its own way.
#[derive(Clone)]
pub struct Progress(pub std::sync::Arc<dyn Fn(&str) + Send + Sync>);

impl std::fmt::Debug for Progress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Progress(..)")
    }
}

/// How long to keep waiting, silently, before saying the daemon is busy.
pub const PROGRESS_AFTER: Duration = Duration::from_millis(1500);

/// How to start the daemon.
#[derive(Debug, Clone)]
pub struct Starter {
    /// The executable to run as `serve --detached`. `current_exe()` in the CLI.
    pub exe: PathBuf,
    pub locations: Locations,
    pub data_dir: PathBuf,
    pub ready_timeout: Duration,
    pub shutdown_wait: Duration,
    pub client: ClientConfig,
    /// Told when a wait is long enough to explain.
    pub progress: Option<Progress>,
}

impl Starter {
    /// A starter for this executable. `locations` must already be absolute.
    ///
    /// # Errors
    /// The executable's path or the data directory cannot be determined.
    pub fn for_current_exe(locations: &Locations) -> Result<Self, AutostartError> {
        Ok(Self {
            exe: std::env::current_exe()
                .map_err(|e| AutostartError::Spawn(format!("locating this executable: {e}")))?,
            data_dir: data_dir(locations)?,
            locations: locations.clone(),
            ready_timeout: READY_TIMEOUT,
            shutdown_wait: SHUTDOWN_WAIT,
            client: ClientConfig::default(),
            progress: None,
        })
    }

    /// The daemon's log file.
    #[must_use]
    pub fn log_path(&self) -> PathBuf {
        self.data_dir.join(LOG_FILE_NAME)
    }

    /// The argument list: `serve --detached`, plus the location flags exactly as the
    /// user gave them (so a default location resolves identically in the child,
    /// which inherits the environment).
    #[must_use]
    pub fn args(&self) -> Vec<std::ffi::OsString> {
        let mut args: Vec<std::ffi::OsString> = vec!["serve".into(), "--detached".into()];
        if let Some(socket) = &self.locations.socket_flag {
            args.push("--socket".into());
            args.push(socket.into());
        }
        args.push("--data-dir".into());
        args.push(self.data_dir.clone().into());
        args
    }
}

/// Connect to a running daemon, or fail with [`ClientError::NotRunning`]. Never starts
/// one: `status` and `stop` use this.
///
/// # Errors
/// As [`Client::connect_with`].
pub async fn connect_existing(
    paths: &DaemonPaths,
    config: ClientConfig,
) -> Result<Client, ClientError> {
    Client::connect_with(&paths.socket, config).await
}

/// Wait while a daemon holds its lock but does not answer (starting up, or shutting
/// down). Returns the last presence seen, which is not `Busy` unless `limit` ran out.
pub async fn wait_while_busy(paths: &DaemonPaths, limit: Duration) -> DaemonPresence {
    let deadline = Instant::now() + limit;
    loop {
        let presence = probe(paths).await;
        if !matches!(presence, DaemonPresence::Busy { .. }) || Instant::now() >= deadline {
            return presence;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Connect to the daemon, starting it first if the socket is absent or stale.
///
/// A daemon that is shutting down is waited for (bounded by `shutdown_wait`) rather
/// than raced; a daemon on another socket that owns this state directory is reported,
/// not fought.
///
/// # Errors
/// [`AutostartError`]: the spawn failed, the daemon exited early, it was not ready in
/// time, or the state directory belongs to another daemon.
pub async fn ensure_daemon(
    paths: &DaemonPaths,
    starter: &Starter,
) -> Result<Client, AutostartError> {
    match Client::connect_with(&paths.socket, starter.client).await {
        Ok(client) => return Ok(client),
        Err(error) if error.is_not_running() => {}
        Err(error) => return Err(error.into()),
    }

    match wait_while_busy(paths, starter.shutdown_wait).await {
        DaemonPresence::Busy { pid } => {
            return Err(AutostartError::StillShuttingDown {
                pid,
                after: starter.shutdown_wait,
            });
        }
        DaemonPresence::Running { .. } => {
            // It came up while we looked.
            if let Ok(client) = Client::connect_with(&paths.socket, starter.client).await {
                return Ok(client);
            }
        }
        DaemonPresence::NotRunning => {}
    }
    if let Some(pid) = probe_state_dir(&starter.data_dir).await {
        // Our own socket is not answering and holds no lock, so this daemon is
        // somewhere else. (A daemon that is merely starting on our socket shows up as
        // `Busy` above.)
        if !matches!(probe(paths).await, DaemonPresence::Busy { .. }) {
            return Err(AutostartError::StateDirOwned {
                pid,
                state_dir: starter.data_dir.clone(),
            });
        }
    }

    let mut child = spawn_detached(starter).await?;
    // A freshly started daemon holds its lock from the first moment but binds its socket
    // only after restart recovery, which can take a while (each orphaned worker is given
    // a grace period to end). While it holds the lock it is working, not stuck: wait for
    // it, up to the documented bound, and say what it is doing. Only a daemon that
    // holds nothing and does not answer for `ready_timeout` is given up on.
    let started = Instant::now();
    let mut quiet_since = started;
    let mut busy_since: Option<Instant> = None;
    let mut told = false;
    let mut pause = FIRST_POLL;
    loop {
        match Client::connect_with(&paths.socket, starter.client).await {
            Ok(client) => {
                // Reap the child if it already finished (a lost race exits 0); a
                // running daemon is left alone, which is the point.
                let _ = child.try_wait();
                return Ok(client);
            }
            Err(error) if error.is_not_running() => {}
            Err(error) => return Err(error.into()),
        }
        let now = Instant::now();
        match probe(paths).await {
            DaemonPresence::Busy { pid } => {
                let since = *busy_since.get_or_insert(now);
                quiet_since = now;
                if now.duration_since(since) >= starter.shutdown_wait {
                    return Err(AutostartError::NotReady {
                        after: starter.shutdown_wait,
                        log: starter.log_path(),
                    });
                }
                if !told && now.duration_since(started) >= PROGRESS_AFTER {
                    told = true;
                    if let Some(progress) = &starter.progress {
                        (progress.0)(&format!(
                            "the aftman daemon (pid {pid}) is starting up, recovering runs \
                             from before; waiting (this can take a few seconds per orphaned \
                             worker)..."
                        ));
                    }
                }
            }
            DaemonPresence::Running { .. } => quiet_since = now,
            DaemonPresence::NotRunning => {
                busy_since = None;
                // A child that died with an error will never become ready. One that
                // exited 0 lost the start race to a daemon whose socket we are about
                // to see.
                if let Ok(Some(status)) = child.try_wait()
                    && !status.success()
                {
                    return Err(AutostartError::Exited {
                        status: status.to_string(),
                        log: starter.log_path(),
                    });
                }
                if now.duration_since(quiet_since) >= starter.ready_timeout {
                    return Err(AutostartError::NotReady {
                        after: starter.ready_timeout,
                        log: starter.log_path(),
                    });
                }
            }
        }
        tokio::time::sleep(pause).await;
        pause = (pause * 3 / 2).min(MAX_POLL);
    }
}

/// Spawn `serve --detached`, and do not wait for it.
async fn spawn_detached(starter: &Starter) -> Result<tokio::process::Child, AutostartError> {
    let log_path = starter.log_path();
    let dir = starter.data_dir.clone();
    let log = tokio::task::spawn_blocking(move || {
        crate::statedir::open_log(&dir, crate::statedir::MAX_LOG_BYTES)
    })
    .await
    .map_err(|e| AutostartError::Spawn(e.to_string()))?
    .map_err(|e| AutostartError::Spawn(format!("opening {}: {e}", log_path.display())))?;
    let log_err = log
        .try_clone()
        .map_err(|e| AutostartError::Spawn(e.to_string()))?;
    // HARD RULE 3 EXCEPTION: no `worker_environment()` here, on purpose. This process
    // is the supervisor starting its own daemon, which needs the provider credentials
    // in this environment. See the module documentation and docs/08 section 2.2.
    let mut command = tokio::process::Command::new(&starter.exe);
    command
        .args(starter.args())
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_err))
        // The daemon outlives this process by design.
        .kill_on_drop(false);
    command
        .spawn()
        .map_err(|e| AutostartError::Spawn(format!("running {}: {e}", starter.exe.display())))
}

/// Wait until no daemon holds the lock or answers on the socket, up to `limit`.
/// `true` when the daemon is gone.
pub async fn wait_until_gone(paths: &DaemonPaths, limit: Duration) -> bool {
    let deadline = Instant::now() + limit;
    loop {
        let gone = matches!(probe(paths).await, DaemonPresence::NotRunning)
            && tokio::fs::symlink_metadata(&paths.socket).await.is_err()
            && tokio::fs::symlink_metadata(&paths.pid_file).await.is_err();
        if gone {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Leave the terminal's session and the first CLI's working directory. Called by
/// `serve --detached` as the **first** thing `main` does after parsing arguments:
/// `setsid` fails if the caller is a process group leader, and threads must not exist
/// yet for the session change to cover the whole process. Failure of either step is
/// returned for the caller to log, and is not fatal.
///
/// # Errors
/// `setsid` (`EPERM` when this process already leads a process group) or `chdir`.
pub fn leave_session() -> Result<(), String> {
    let session = nix::unistd::setsid()
        .map(|_| ())
        .map_err(|e| format!("setsid: {e}"));
    let cwd = std::env::set_current_dir("/").map_err(|e| format!("chdir /: {e}"));
    session.and(cwd)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_flag_wins_and_environment_fills_the_rest() {
        let inputs = PathInputs {
            socket_override: Some(PathBuf::from("/tmp/x/s.sock")),
            pid_override: None,
            xdg_runtime_dir: Some(PathBuf::from("/run/user/1")),
            home: Some(PathBuf::from("/home/u")),
        };
        let paths = resolve_paths(&inputs).unwrap();
        assert_eq!(paths.socket, PathBuf::from("/tmp/x/s.sock"));
        assert_eq!(paths.pid_file, PathBuf::from("/tmp/x/s.pid"));

        let xdg = PathInputs {
            socket_override: None,
            ..inputs.clone()
        };
        assert_eq!(
            resolve_paths(&xdg).unwrap().socket,
            PathBuf::from("/run/user/1/aftman/aftman.sock")
        );
        let home_only = PathInputs {
            xdg_runtime_dir: None,
            ..xdg
        };
        assert_eq!(
            resolve_paths(&home_only).unwrap().socket,
            PathBuf::from("/home/u/.aftman/aftman.sock")
        );
        assert!(resolve_paths(&PathInputs::default()).is_err());
    }

    #[test]
    fn relative_locations_become_absolute_before_a_daemon_sees_them() {
        let cwd = std::env::current_dir().unwrap();
        let locations = Locations {
            socket_flag: Some(PathBuf::from("run/s.sock")),
            data_dir_flag: Some(PathBuf::from("/already/absolute")),
        }
        .absolute()
        .unwrap();
        assert_eq!(locations.socket_flag, Some(cwd.join("run/s.sock")));
        assert_eq!(
            locations.data_dir_flag,
            Some(PathBuf::from("/already/absolute"))
        );
        let relative_state = Locations {
            socket_flag: None,
            data_dir_flag: Some(PathBuf::from("state")),
        }
        .absolute()
        .unwrap();
        assert_eq!(relative_state.data_dir_flag, Some(cwd.join("state")));
    }

    #[test]
    fn the_child_is_started_detached_with_the_users_location_flags() {
        let starter = Starter {
            exe: PathBuf::from("/usr/bin/aftman"),
            locations: Locations {
                socket_flag: Some(PathBuf::from("/tmp/s.sock")),
                data_dir_flag: None,
            },
            data_dir: PathBuf::from("/var/state"),
            ready_timeout: READY_TIMEOUT,
            shutdown_wait: SHUTDOWN_WAIT,
            client: ClientConfig::default(),
            progress: None,
        };
        let args: Vec<String> = starter
            .args()
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            args,
            [
                "serve",
                "--detached",
                "--socket",
                "/tmp/s.sock",
                "--data-dir",
                "/var/state"
            ]
        );
        assert_eq!(starter.log_path(), PathBuf::from("/var/state/daemon.log"));

        let default_socket = Starter {
            locations: Locations::default(),
            ..starter
        };
        assert!(!default_socket.args().iter().any(|a| a == "--socket"));
    }

    #[tokio::test]
    async fn a_missing_socket_is_not_running_and_a_stale_one_too() {
        let dir = tempfile::Builder::new()
            .prefix("aft")
            .tempdir_in("/tmp")
            .unwrap();
        let paths = DaemonPaths::in_dir(dir.path());
        let error = connect_existing(&paths, ClientConfig::default())
            .await
            .err()
            .unwrap();
        assert!(error.is_not_running());

        // A socket file nobody listens on: bind, then drop the listener.
        drop(tokio::net::UnixListener::bind(&paths.socket).unwrap());
        let error = connect_existing(&paths, ClientConfig::default())
            .await
            .err()
            .unwrap();
        assert!(error.is_not_running(), "{error}");
    }

    #[tokio::test]
    async fn waiting_for_shutdown_has_a_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let paths = DaemonPaths::in_dir(dir.path());
        assert!(wait_until_gone(&paths, Duration::from_millis(50)).await);
        std::fs::write(&paths.pid_file, "1\n").unwrap();
        assert!(!wait_until_gone(&paths, Duration::from_millis(100)).await);
    }
}
