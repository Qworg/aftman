//! Where the daemon lives on disk, and how it claims that place.
//!
//! Everything path-shaped is a **pure function of its inputs** ([`resolve_paths`]
//! takes the environment as a value), so tests need no environment variables.
//!
//! # Policy
//!
//! - **Paths.** Socket `$XDG_RUNTIME_DIR/aftman/aftman.sock`, else
//!   `~/.aftman/aftman.sock`; the pid file is the socket path with a `.pid`
//!   extension, so it sits alongside it. Both can be overridden.
//! - **Directory.** The daemon's own directory is created `0700`. When aftman
//!   *manages* the directory (the default location) an existing directory owned by
//!   someone else is refused, and one with group/other permission bits is
//!   **tightened** to `0700` (and logged), rather than refused: it is the user's own
//!   directory and fixing it is what they would ask for. When the user supplies an
//!   explicit socket path, only missing parent directories are created (`0700`); an
//!   existing parent is theirs to manage and is left alone. The socket itself is
//!   `0600` either way.
//! - **Single instance.** An exclusive `flock` on the pid file, with the pid
//!   written into it. The lock is the arbiter, never the pid number, so a stale pid
//!   file from a crash is harmless. A second daemon gets
//!   [`LifecycleError::AlreadyRunning`].
//! - **Stale sockets.** Only ever examined while holding the pid lock, so two
//!   daemons starting at once cannot both reach the socket. A socket file whose
//!   `connect` fails is stale and is replaced; one that answers is never stolen.
//!   The new socket is bound under a temporary name, made `0600`, and renamed into
//!   place, so there is no moment at which it exists with the wrong mode, and a
//!   failure leaves any existing path untouched.

use std::io::Write as _;
use std::os::unix::fs::{
    DirBuilderExt as _, FileTypeExt as _, MetadataExt as _, OpenOptionsExt as _,
    PermissionsExt as _,
};
use std::path::{Path, PathBuf};
use std::time::Duration;

use nix::errno::Errno;
use nix::fcntl::{Flock, FlockArg};
use tokio::net::{UnixListener, UnixStream};

/// File name of the socket inside the daemon directory.
pub const SOCKET_FILE_NAME: &str = "aftman.sock";

/// Longest socket path accepted, in bytes. `sun_path` is 108 bytes on Linux and
/// 104 on the BSDs and macOS, including the terminating NUL; refusing anything the
/// stricter platform would refuse keeps behaviour the same everywhere.
pub const MAX_SOCKET_PATH_BYTES: usize = 103;

/// How long a probe connect to an existing socket may take before the socket is
/// treated as live (a daemon too busy to accept is still not stale).
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Why the daemon could not claim its place.
#[derive(Debug, thiserror::Error)]
pub enum LifecycleError {
    #[error("cannot locate a runtime directory: neither XDG_RUNTIME_DIR nor HOME is set")]
    NoHome,

    #[error("path {0} must be absolute")]
    NotAbsolute(PathBuf),

    #[error(
        "socket path {path} is {len} bytes; the limit is {max} (sun_path). \
         Use a shorter --socket path"
    )]
    SocketPathTooLong {
        path: PathBuf,
        len: usize,
        max: usize,
    },

    #[error("I/O error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("refusing to use directory {path}: {reason}")]
    UnsafeDirectory { path: PathBuf, reason: String },

    /// Another daemon holds the pid-file lock. `pid` is the pid it wrote, or `0`
    /// when it had not written one yet. The CLI prints it and exits 0.
    #[error("an aftman daemon is already running (pid {pid})")]
    AlreadyRunning { pid: u32 },

    /// The socket path answers connections but nobody holds the pid lock: a daemon
    /// using a different pid file, or a foreign server. Never stolen.
    /// Another daemon is serving this state directory (a different socket, the same
    /// runs). `pid` is its pid, or `0` when it had not written one yet.
    #[error(
        "an aftman daemon (pid {pid}) is already serving the state directory {}; \
         stop it first or use its socket",
        state_dir.display()
    )]
    StateDirInUse { pid: u32, state_dir: PathBuf },

    #[error("{0} is in use by a live server; refusing to replace it")]
    SocketInUse(PathBuf),

    #[error("{0} exists and is not a socket; refusing to replace it")]
    NotASocket(PathBuf),
}

fn io(path: &Path, source: std::io::Error) -> LifecycleError {
    LifecycleError::Io {
        path: path.to_owned(),
        source,
    }
}

/// The inputs to path resolution. Plain values: the CLI fills them from
/// `std::env` and flags, tests fill them directly.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PathInputs {
    pub socket_override: Option<PathBuf>,
    pub pid_override: Option<PathBuf>,
    pub xdg_runtime_dir: Option<PathBuf>,
    pub home: Option<PathBuf>,
}

/// Whether aftman owns the socket's directory (see the module documentation).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirPolicy {
    /// The default location: enforce `0700` and ownership.
    Managed,
    /// A user-chosen socket path: create missing parents `0700`, leave existing
    /// ones alone.
    UserChosen,
}

/// Where the daemon's socket and pid file are.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonPaths {
    pub socket: PathBuf,
    pub pid_file: PathBuf,
    pub dir_policy: DirPolicy,
}

impl DaemonPaths {
    /// `aftman.sock` and `aftman.pid` inside a directory aftman manages.
    #[must_use]
    pub fn in_dir(dir: &Path) -> Self {
        Self {
            socket: dir.join(SOCKET_FILE_NAME),
            pid_file: dir.join("aftman.pid"),
            dir_policy: DirPolicy::Managed,
        }
    }

    /// The directory holding the socket.
    #[must_use]
    pub fn dir(&self) -> &Path {
        self.socket.parent().unwrap_or_else(|| Path::new("."))
    }

    /// Check the paths are usable: absolute and short enough for `sun_path`.
    ///
    /// # Errors
    /// [`LifecycleError::NotAbsolute`] or [`LifecycleError::SocketPathTooLong`].
    pub fn validate(&self) -> Result<(), LifecycleError> {
        for path in [&self.socket, &self.pid_file] {
            if !path.is_absolute() {
                return Err(LifecycleError::NotAbsolute(path.clone()));
            }
        }
        // What is actually bound is the staged path, which is longer than the final
        // one; validate that, so a path that passes here can always be bound.
        let len = self.socket.as_os_str().len();
        let staged = staged_socket(&self.socket).as_os_str().len();
        if staged > MAX_SOCKET_PATH_BYTES {
            return Err(LifecycleError::SocketPathTooLong {
                path: self.socket.clone(),
                len,
                max: MAX_SOCKET_PATH_BYTES - (staged - len),
            });
        }
        Ok(())
    }
}

/// The private directory the socket is bound in before it is renamed into place.
fn staging_dir(socket: &Path) -> PathBuf {
    socket
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!(".st{}", std::process::id()))
}

/// Where the socket is bound before it is renamed to its final name.
fn staged_socket(socket: &Path) -> PathBuf {
    staging_dir(socket).join(
        socket
            .file_name()
            .unwrap_or(std::ffi::OsStr::new(SOCKET_FILE_NAME)),
    )
}

/// The longest final socket path that can be bound under the current pid
/// (the staging directory adds `/.st<pid>` to the path that is bound).
#[must_use]
pub fn max_socket_path_bytes() -> usize {
    MAX_SOCKET_PATH_BYTES - format!("/.st{}", std::process::id()).len()
}

/// Resolve the daemon's paths from explicit inputs. Pure.
///
/// # Errors
/// [`LifecycleError::NoHome`] when nothing locates a directory, and the errors of
/// [`DaemonPaths::validate`].
pub fn resolve_paths(inputs: &PathInputs) -> Result<DaemonPaths, LifecycleError> {
    let paths = if let Some(socket) = &inputs.socket_override {
        DaemonPaths {
            socket: socket.clone(),
            pid_file: inputs
                .pid_override
                .clone()
                .unwrap_or_else(|| socket.with_extension("pid")),
            dir_policy: DirPolicy::UserChosen,
        }
    } else {
        // A relative XDG_RUNTIME_DIR is invalid per the XDG spec; ignore it.
        let dir = match (&inputs.xdg_runtime_dir, &inputs.home) {
            (Some(xdg), _) if xdg.is_absolute() => xdg.join("aftman"),
            (_, Some(home)) => home.join(".aftman"),
            _ => return Err(LifecycleError::NoHome),
        };
        // The directory policy follows where the socket lives; moving only the pid
        // file does not change it.
        let mut paths = DaemonPaths::in_dir(&dir);
        if let Some(pid) = &inputs.pid_override {
            paths.pid_file.clone_from(pid);
        }
        paths
    };
    paths.validate()?;
    Ok(paths)
}

/// Create the socket's directory (and parents) `0700`, and enforce the policy on an
/// existing one. Blocking: call from `spawn_blocking`.
fn prepare_dir(dir: &Path, policy: DirPolicy) -> Result<(), LifecycleError> {
    match std::fs::symlink_metadata(dir) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)
            .map_err(|e| io(dir, e)),
        Err(error) => Err(io(dir, error)),
        Ok(meta) => {
            // Follow a symlink to a directory the user pointed us at; refuse
            // anything else.
            let meta = if meta.file_type().is_symlink() {
                std::fs::metadata(dir).map_err(|e| io(dir, e))?
            } else {
                meta
            };
            if !meta.is_dir() {
                return Err(LifecycleError::UnsafeDirectory {
                    path: dir.to_owned(),
                    reason: "not a directory".to_owned(),
                });
            }
            if policy == DirPolicy::UserChosen {
                return Ok(());
            }
            if meta.uid() != nix::unistd::geteuid().as_raw() {
                return Err(LifecycleError::UnsafeDirectory {
                    path: dir.to_owned(),
                    reason: format!("owned by uid {}, not by this user", meta.uid()),
                });
            }
            if meta.mode() & 0o077 != 0 {
                tracing::warn!(
                    path = %dir.display(),
                    mode = format_args!("{:o}", meta.mode() & 0o7777),
                    "tightening the daemon directory to 0700"
                );
                std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
                    .map_err(|e| io(dir, e))?;
            }
            Ok(())
        }
    }
}

/// The single-instance lock: an exclusive `flock` on the pid file, with the pid in
/// it. Released when dropped, and by the OS if the process dies.
#[derive(Debug)]
pub struct PidLock {
    path: PathBuf,
    /// `(dev, ino)` of the file we locked; only that file is ever removed.
    id: (u64, u64),
    _lock: Flock<std::fs::File>,
}

impl PidLock {
    /// Take the lock. Blocking (file I/O and a short retry): call from
    /// `spawn_blocking`.
    fn acquire(path: &Path) -> Result<Self, LifecycleError> {
        Self::acquire_with(path, &|| {})
    }

    /// [`PidLock::acquire`], calling `between_open_and_lock` in the window a racing
    /// daemon's cleanup can use. A test hook; a no-op in production.
    fn acquire_with(path: &Path, between_open_and_lock: &dyn Fn()) -> Result<Self, LifecycleError> {
        // A previous daemon removes the pid file while holding the lock. Someone
        // who opened the old file before that holds a lock on an unlinked inode, so
        // after locking, confirm the path still names the inode we locked.
        // A probe (`lock_holder`) takes the lock shared for an instant; a starter that
        // collides with one retries briefly instead of reporting a daemon that is not
        // there.
        let mut contended = 0;
        for _ in 0..12 {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .mode(0o600)
                .open(path)
                .map_err(|e| io(path, e))?;
            between_open_and_lock();
            match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
                Ok(mut lock) => {
                    let locked = lock.metadata().map_err(|e| io(path, e))?;
                    match std::fs::metadata(path) {
                        Ok(on_disk)
                            if on_disk.ino() == locked.ino() && on_disk.dev() == locked.dev() => {}
                        _ => continue,
                    }
                    lock.set_len(0).map_err(|e| io(path, e))?;
                    writeln!(&mut *lock, "{}", std::process::id()).map_err(|e| io(path, e))?;
                    lock.sync_all().map_err(|e| io(path, e))?;
                    return Ok(Self {
                        path: path.to_owned(),
                        id: (locked.dev(), locked.ino()),
                        _lock: lock,
                    });
                }
                Err((_, Errno::EWOULDBLOCK)) => {
                    contended += 1;
                    if contended < 4 {
                        std::thread::sleep(Duration::from_millis(15));
                        continue;
                    }
                    return Err(LifecycleError::AlreadyRunning {
                        pid: read_pid(path),
                    });
                }
                Err((_, errno)) => return Err(io(path, errno.into())),
            }
        }
        Err(io(
            path,
            std::io::Error::other("the pid file kept being replaced while locking it"),
        ))
    }

    /// Remove the pid file, then release the lock. Safe because the lock is held
    /// and every locker re-checks the inode after locking.
    fn release(self) {
        // Only the file we locked: if the path now names something else, a newer
        // daemon (or the user) put it there and it is not ours to delete.
        let ours = std::fs::metadata(&self.path).is_ok_and(|m| (m.dev(), m.ino()) == self.id);
        if !ours {
            return;
        }
        if let Err(error) = std::fs::remove_file(&self.path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(path = %self.path.display(), %error, "could not remove the pid file");
        }
    }
}

/// The pid written in the file, waiting briefly for a daemon that has locked the
/// file but not yet written. `0` if it never appears.
fn read_pid(path: &Path) -> u32 {
    for _ in 0..20 {
        if let Some(pid) = std::fs::read_to_string(path)
            .ok()
            .and_then(|text| text.trim().parse::<u32>().ok())
        {
            return pid;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    0
}

/// Who holds the lock on `path`, without taking or creating it.
///
/// Opens the file read-only (never creating it) and tries a **shared** non-blocking
/// lock: if that succeeds nobody holds the exclusive one, and it is released at once.
/// A daemon starting at that instant retries briefly (see [`PidLock::acquire_with`]),
/// so a probe never makes it fail. `Some(pid)` when held (`0` if no pid was readable).
fn lock_holder(path: &Path) -> Option<u32> {
    let file = std::fs::File::open(path).ok()?;
    match Flock::lock(file, FlockArg::LockSharedNonblock) {
        Ok(_released_on_drop) => None,
        Err((_, Errno::EWOULDBLOCK)) => Some(read_pid(path)),
        Err(_) => None,
    }
}

/// What [`probe`] found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DaemonPresence {
    /// No daemon holds the pid lock and nothing answers on the socket.
    NotRunning,
    /// A daemon is up and answering on its socket. `pid` is `0` if the socket answers
    /// but no pid lock is held (a server this crate does not manage).
    Running { pid: u32 },
    /// A daemon holds the pid lock but the socket does not answer: it is starting up
    /// (recovery, binding) or shutting down (it stops listening first and then waits,
    /// up to the shutdown grace, for runs to be cancelled). Retry, or wait.
    Busy { pid: u32 },
}

/// Is a daemon using `paths`? Takes no lock, creates nothing, and disturbs nothing: it
/// reads the pid lock's state (see [`lock_holder`]) and makes one connection that it
/// drops at once.
pub async fn probe(paths: &DaemonPaths) -> DaemonPresence {
    let pid_file = paths.pid_file.clone();
    let holder = tokio::task::spawn_blocking(move || lock_holder(&pid_file))
        .await
        .ok()
        .flatten();
    let answers = matches!(
        tokio::time::timeout(PROBE_TIMEOUT, UnixStream::connect(&paths.socket)).await,
        Ok(Ok(_))
    );
    match (holder, answers) {
        (Some(pid), true) => DaemonPresence::Running { pid },
        (Some(pid), false) => DaemonPresence::Busy { pid },
        (None, true) => DaemonPresence::Running { pid: 0 },
        (None, false) => DaemonPresence::NotRunning,
    }
}

/// File name of the per-state-directory daemon lock.
pub const STATE_LOCK_FILE_NAME: &str = "daemon.lock";

/// Is a daemon serving the state directory `state_dir`? `Some(pid)` if so (`0` when its
/// pid is not readable yet). Non-destructive, like [`probe`]. `aftman --no-daemon purge`
/// must refuse (or defer to the daemon) while this is `Some`: deleting run files from
/// under a daemon leaves its replay ring serving a run that no longer exists.
pub async fn probe_state_dir(state_dir: &Path) -> Option<u32> {
    let lock_file = state_dir.join(STATE_LOCK_FILE_NAME);
    tokio::task::spawn_blocking(move || lock_holder(&lock_file))
        .await
        .ok()
        .flatten()
}

/// Bind the daemon socket at `paths.socket`, replacing a stale one. Returns the
/// listener and the `(dev, ino)` of the socket file now at the final path.
///
/// The socket is bound inside a private `0700` staging directory, made `0600`, and
/// only then renamed into place, so it is never visible at its final path with a
/// wider mode (the final directory may be one the user chose, and readable).
async fn bind_socket(paths: &DaemonPaths) -> Result<(UnixListener, (u64, u64)), LifecycleError> {
    let socket = &paths.socket;
    let stage = staging_dir(socket);
    let staged = staged_socket(socket);
    // Our own leftovers from a crashed run with the same pid.
    let _ = tokio::fs::remove_dir_all(&stage).await;
    {
        let stage = stage.clone();
        tokio::task::spawn_blocking(move || {
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(&stage)
                .map_err(|e| io(&stage, e))
        })
        .await
        .map_err(|e| io(&paths.socket, std::io::Error::other(e)))??;
    }

    let result = async {
        let listener = UnixListener::bind(&staged).map_err(|e| io(&staged, e))?;
        tokio::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o600))
            .await
            .map_err(|e| io(&staged, e))?;
        match tokio::fs::symlink_metadata(socket).await {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io(socket, error)),
            Ok(meta) if meta.file_type().is_socket() => {
                match tokio::time::timeout(PROBE_TIMEOUT, UnixStream::connect(socket)).await {
                    // Only an outright refusal (or a vanished file) proves it stale.
                    Ok(Err(error))
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
                        ) =>
                    {
                        tracing::warn!(path = %socket.display(), "replacing a stale daemon socket");
                    }
                    _ => return Err(LifecycleError::SocketInUse(socket.clone())),
                }
            }
            Ok(_) => return Err(LifecycleError::NotASocket(socket.clone())),
        }
        tokio::fs::rename(&staged, socket)
            .await
            .map_err(|e| io(socket, e))?;
        let bound = tokio::fs::metadata(socket)
            .await
            .map_err(|e| io(socket, e))?;
        Ok((listener, (bound.dev(), bound.ino())))
    }
    .await;

    let _ = tokio::fs::remove_dir_all(&stage).await;
    result
}

/// A claimed place: the bound listener, and the guard that holds the pid lock.
#[derive(Debug)]
pub struct Claimed {
    pub listener: UnixListener,
    pub guard: ClaimGuard,
}

/// Holds the pid lock for the daemon's lifetime. Dropping it (a crash, a test that
/// kills the daemon) releases the lock but leaves the files, exactly as a dead
/// process would; [`ClaimGuard::cleanup`] is the orderly exit.
#[derive(Debug)]
pub struct ClaimGuard {
    pid_lock: PidLock,
    /// The lock on the state directory, when the daemon was started with one.
    state_lock: Option<PidLock>,
    paths: DaemonPaths,
    /// `(dev, ino)` of the socket file this daemon bound.
    socket_id: (u64, u64),
}

impl ClaimGuard {
    /// The paths this claim holds.
    #[must_use]
    pub fn paths(&self) -> &DaemonPaths {
        &self.paths
    }

    /// Remove the socket file and the pid file, then release the lock. Call after
    /// the listener has been dropped and shutdown is complete.
    pub async fn cleanup(self) {
        let Self {
            pid_lock,
            state_lock,
            paths,
            socket_id,
        } = self;
        // Only the socket we bound: a path that now names something else belongs to
        // a newer daemon (or the user) and is not ours to remove.
        let ours = tokio::fs::metadata(&paths.socket)
            .await
            .is_ok_and(|m| (m.dev(), m.ino()) == socket_id);
        if ours
            && let Err(error) = tokio::fs::remove_file(&paths.socket).await
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(path = %paths.socket.display(), %error, "could not remove the socket");
        }
        // `release` removes a file and drops the lock: quick, but still I/O.
        let _ = tokio::task::spawn_blocking(move || {
            pid_lock.release();
            if let Some(state_lock) = state_lock {
                state_lock.release();
            }
        })
        .await;
    }
}

/// Take the pid lock. The first half of starting; recovery runs after this and
/// before [`PidLockGuard::bind`], so a second daemon is rejected before it touches
/// any run.
///
/// # Errors
/// [`LifecycleError::AlreadyRunning`], directory-policy errors, I/O errors.
pub async fn lock(paths: &DaemonPaths) -> Result<PidLockGuard, LifecycleError> {
    paths.validate()?;
    let socket_dir = paths.dir().to_owned();
    let pid_dir = paths
        .pid_file
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_owned();
    let policy = paths.dir_policy;
    let pid_file = paths.pid_file.clone();
    let lock = tokio::task::spawn_blocking(move || {
        prepare_dir(&socket_dir, policy)?;
        if pid_dir != socket_dir {
            prepare_dir(&pid_dir, DirPolicy::UserChosen)?;
        }
        PidLock::acquire(&pid_file)
    })
    .await
    .map_err(|e| io(&paths.pid_file, std::io::Error::other(e)))??;
    Ok(PidLockGuard {
        lock,
        state_lock: None,
        paths: paths.clone(),
    })
}

/// The pid lock, held. Turn it into a [`Claimed`] with [`PidLockGuard::bind`].
#[derive(Debug)]
pub struct PidLockGuard {
    lock: PidLock,
    state_lock: Option<PidLock>,
    paths: DaemonPaths,
}

impl PidLockGuard {
    /// Also take the lock on the state directory: one daemon per state directory, whatever
    /// socket it listens on. The file is `<state_dir>/daemon.lock`, with the same
    /// discipline as the pid lock (exclusive `flock`, pid written in, inode re-checked).
    /// The directory is created `0700` if missing.
    ///
    /// # Errors
    /// [`LifecycleError::StateDirInUse`] naming the holder's pid, or an I/O error.
    pub async fn with_state_dir(mut self, state_dir: &Path) -> Result<Self, LifecycleError> {
        let state_dir = state_dir.to_owned();
        let lock = tokio::task::spawn_blocking({
            let state_dir = state_dir.clone();
            move || {
                prepare_dir(&state_dir, DirPolicy::UserChosen)?;
                PidLock::acquire(&state_dir.join(STATE_LOCK_FILE_NAME)).map_err(|e| match e {
                    LifecycleError::AlreadyRunning { pid } => {
                        LifecycleError::StateDirInUse { pid, state_dir }
                    }
                    other => other,
                })
            }
        })
        .await
        .map_err(|e| io(&self.paths.pid_file, std::io::Error::other(e)))??;
        self.state_lock = Some(lock);
        Ok(self)
    }

    /// Bind the socket (replacing a stale one) while holding the lock.
    ///
    /// # Errors
    /// [`LifecycleError::SocketInUse`], [`LifecycleError::NotASocket`], I/O errors.
    pub async fn bind(self) -> Result<Claimed, LifecycleError> {
        let (listener, socket_id) = bind_socket(&self.paths).await?;
        Ok(Claimed {
            listener,
            guard: ClaimGuard {
                pid_lock: self.lock,
                state_lock: self.state_lock,
                paths: self.paths,
                socket_id,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::PidLock;
    use std::os::unix::fs::MetadataExt as _;

    #[test]
    fn the_pid_lock_rechecks_the_inode_after_locking() {
        // A previous daemon cleans up (unlinks the pid file) after we opened it and
        // before we locked it; a third daemon then creates a fresh file. Without the
        // re-check we would hold a lock on an unlinked inode while the new daemon
        // holds the real one: two "single instances".
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("aftman.pid");
        std::fs::write(&path, "1\n").expect("seed");
        let raced = std::cell::Cell::new(false);
        let lock = PidLock::acquire_with(&path, &|| {
            if !raced.replace(true) {
                std::fs::remove_file(&path).expect("unlink");
                std::fs::write(&path, "2\n").expect("recreate");
            }
        })
        .expect("acquire");
        let on_disk = std::fs::metadata(&path).expect("pid file");
        assert_eq!(
            lock.id,
            (on_disk.dev(), on_disk.ino()),
            "the lock is on the file the path names now"
        );
        assert!(raced.get());
    }
}
