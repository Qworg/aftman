//! Where the daemon lives: paths, permissions, single instance, stale sockets.
//! Path resolution takes its inputs as a value, so none of this reads the
//! environment.

mod common;

use std::os::unix::fs::{FileTypeExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::json;

use aftman_core::clock::SystemClock;
use aftman_daemon::lifecycle::{DirPolicy, max_socket_path_bytes};
use aftman_daemon::{Daemon, DaemonConfig, DaemonPaths, LifecycleError, PathInputs, resolve_paths};
use aftman_store::{FileRunStore, RunStore};
use common::{Client, Harness, TestFactory, TestResult};

fn mode(path: &Path) -> std::io::Result<u32> {
    Ok(std::fs::metadata(path)?.permissions().mode() & 0o7777)
}

async fn try_start(paths: &DaemonPaths, scratch: &Path) -> Result<Daemon, LifecycleError> {
    let store: Arc<dyn RunStore> = Arc::new(FileRunStore::new(scratch.join("state")));
    Daemon::start(
        DaemonConfig::new(paths.clone()),
        store,
        TestFactory::new(scratch),
        Arc::new(SystemClock),
    )
    .await
}

// ---- path resolution (pure) -------------------------------------------------

#[test]
fn paths_follow_xdg_runtime_dir_then_home() -> TestResult {
    let xdg = resolve_paths(&PathInputs {
        xdg_runtime_dir: Some("/run/user/1000".into()),
        home: Some("/home/jen".into()),
        ..PathInputs::default()
    })?;
    assert_eq!(
        xdg.socket,
        PathBuf::from("/run/user/1000/aftman/aftman.sock")
    );
    assert_eq!(
        xdg.pid_file,
        PathBuf::from("/run/user/1000/aftman/aftman.pid")
    );
    assert_eq!(xdg.dir_policy, DirPolicy::Managed);

    let home = resolve_paths(&PathInputs {
        home: Some("/home/jen".into()),
        ..PathInputs::default()
    })?;
    assert_eq!(home.socket, PathBuf::from("/home/jen/.aftman/aftman.sock"));
    assert_eq!(home.pid_file, PathBuf::from("/home/jen/.aftman/aftman.pid"));

    // A relative XDG_RUNTIME_DIR is invalid per the spec and is ignored.
    let relative = resolve_paths(&PathInputs {
        xdg_runtime_dir: Some("runtime".into()),
        home: Some("/home/jen".into()),
        ..PathInputs::default()
    })?;
    assert_eq!(relative.socket, home.socket);
    Ok(())
}

#[test]
fn explicit_paths_override_and_the_pid_file_follows_the_socket() -> TestResult {
    let custom = resolve_paths(&PathInputs {
        socket_override: Some("/tmp/x/custom.sock".into()),
        home: Some("/home/jen".into()),
        ..PathInputs::default()
    })?;
    assert_eq!(custom.socket, PathBuf::from("/tmp/x/custom.sock"));
    assert_eq!(custom.pid_file, PathBuf::from("/tmp/x/custom.pid"));
    assert_eq!(custom.dir_policy, DirPolicy::UserChosen);

    let both = resolve_paths(&PathInputs {
        socket_override: Some("/tmp/x/custom.sock".into()),
        pid_override: Some("/tmp/y/mine.pid".into()),
        ..PathInputs::default()
    })?;
    assert_eq!(both.pid_file, PathBuf::from("/tmp/y/mine.pid"));
    Ok(())
}

#[test]
fn unresolvable_and_relative_paths_are_clear_errors() {
    assert!(matches!(
        resolve_paths(&PathInputs::default()),
        Err(LifecycleError::NoHome)
    ));
    assert!(matches!(
        resolve_paths(&PathInputs {
            socket_override: Some("relative.sock".into()),
            ..PathInputs::default()
        }),
        Err(LifecycleError::NotAbsolute(_))
    ));
}

#[test]
fn an_overlong_socket_path_is_a_clear_error() {
    let long = format!("/tmp/{}/aftman.sock", "d".repeat(200));
    let error = resolve_paths(&PathInputs {
        socket_override: Some(long.clone().into()),
        ..PathInputs::default()
    })
    .expect_err("too long for sun_path");
    match error {
        LifecycleError::SocketPathTooLong { len, max, .. } => {
            assert_eq!(len, long.len());
            assert_eq!(max, max_socket_path_bytes());
        }
        other => panic!("wrong error: {other}"),
    }
    assert!(error_text(&long).contains("sun_path"));
}

fn error_text(long: &str) -> String {
    resolve_paths(&PathInputs {
        socket_override: Some(long.into()),
        ..PathInputs::default()
    })
    .err()
    .map(|e| e.to_string())
    .unwrap_or_default()
}

#[tokio::test]
async fn starting_with_an_overlong_path_fails_before_touching_the_disk() -> TestResult {
    let dir = common::short_tempdir()?;
    let paths = DaemonPaths::in_dir(&dir.path().join("d".repeat(150)));
    let error = try_start(&paths, dir.path())
        .await
        .err()
        .ok_or("must fail")?;
    assert!(
        matches!(error, LifecycleError::SocketPathTooLong { .. }),
        "{error}"
    );
    assert!(!paths.dir().exists(), "nothing was created");
    Ok(())
}

// ---- permissions -------------------------------------------------------------

#[tokio::test]
async fn the_socket_is_0600_and_its_directory_0700() -> TestResult {
    let h = Harness::start().await?;
    assert_eq!(mode(&h.paths.socket)?, 0o600);
    assert_eq!(mode(h.paths.dir())?, 0o700);
    assert_eq!(mode(&h.paths.pid_file)?, 0o600);
    assert!(std::fs::metadata(&h.paths.socket)?.file_type().is_socket());
    // The temporary name used while binding is gone.
    let leftovers: Vec<_> = std::fs::read_dir(h.paths.dir())?
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".tmp"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
    Ok(())
}

#[tokio::test]
async fn an_existing_directory_with_wide_permissions_is_tightened() -> TestResult {
    let dir = common::short_tempdir()?;
    let run_dir = dir.path().join("run");
    std::fs::create_dir_all(&run_dir)?;
    std::fs::set_permissions(&run_dir, std::fs::Permissions::from_mode(0o755))?;
    let repo = dir.path().join("repo");
    std::fs::create_dir_all(&repo)?;
    let state = dir.path().join("state");

    let _h = Harness::start_in(dir, repo, state, |_| {}).await?;
    assert_eq!(mode(&run_dir)?, 0o700, "tightened, not left open");
    Ok(())
}

#[tokio::test]
async fn a_directory_path_that_is_a_file_is_refused() -> TestResult {
    let dir = common::short_tempdir()?;
    let blocker = dir.path().join("run");
    std::fs::write(&blocker, "not a directory")?;
    let paths = DaemonPaths::in_dir(&blocker);
    let error = try_start(&paths, dir.path())
        .await
        .err()
        .ok_or("must fail")?;
    assert!(
        matches!(
            error,
            LifecycleError::UnsafeDirectory { .. } | LifecycleError::Io { .. }
        ),
        "{error}"
    );
    assert_eq!(std::fs::read_to_string(&blocker)?, "not a directory");
    Ok(())
}

// ---- single instance and stale sockets ---------------------------------------

#[tokio::test]
async fn a_stale_socket_is_replaced() -> TestResult {
    let dir = common::short_tempdir()?;
    let run_dir = dir.path().join("run");
    std::fs::create_dir_all(&run_dir)?;
    let socket = run_dir.join("aftman.sock");
    // A crashed daemon's leftover: a socket file nobody listens on.
    drop(std::os::unix::net::UnixListener::bind(&socket)?);
    assert!(socket.exists());
    assert!(tokio::net::UnixStream::connect(&socket).await.is_err());
    // And its pid file, naming a process that is long gone.
    std::fs::write(run_dir.join("aftman.pid"), "999999\n")?;

    let (repo, state) = (dir.path().join("repo"), dir.path().join("state"));
    std::fs::create_dir_all(&repo)?;
    let h = Harness::start_in(dir, repo, state, |_| {}).await?;
    let mut c = h.client().await?;
    assert_eq!(
        c.result("daemon.status", json!({})).await?["pid"],
        std::process::id()
    );
    assert_eq!(mode(&h.paths.socket)?, 0o600);
    assert_eq!(
        std::fs::read_to_string(&h.paths.pid_file)?.trim(),
        std::process::id().to_string(),
        "the stale pid was replaced"
    );
    Ok(())
}

#[tokio::test]
async fn a_second_daemon_is_told_who_is_already_running() -> TestResult {
    let h = Harness::start().await?;
    let error = try_start(&h.paths, h.dir.path())
        .await
        .err()
        .ok_or("must fail")?;
    match error {
        LifecycleError::AlreadyRunning { pid } => assert_eq!(pid, std::process::id()),
        other => panic!("wrong error: {other}"),
    }

    // The first daemon is undisturbed: still answering, files intact.
    let mut c = h.client().await?;
    assert!(c.result("daemon.status", json!({})).await?["pid"].is_u64());
    assert_eq!(
        std::fs::read_to_string(&h.paths.pid_file)?.trim(),
        std::process::id().to_string()
    );
    Ok(())
}

#[tokio::test]
async fn a_live_socket_is_never_stolen() -> TestResult {
    let h = Harness::start().await?;
    // A second daemon with a *different* pid file: the lock cannot protect the
    // socket, so the connect probe must.
    let other = DaemonPaths {
        socket: h.paths.socket.clone(),
        pid_file: h.paths.dir().join("other.pid"),
        dir_policy: DirPolicy::UserChosen,
    };
    let error = try_start(&other, h.dir.path())
        .await
        .err()
        .ok_or("must fail")?;
    assert!(matches!(error, LifecycleError::SocketInUse(_)), "{error}");

    let mut c = Client::connect(&h.paths.socket).await?;
    assert!(c.result("daemon.status", json!({})).await?["pid"].is_u64());
    Ok(())
}

#[tokio::test]
async fn something_that_is_not_a_socket_is_left_alone() -> TestResult {
    let dir = common::short_tempdir()?;
    let run_dir = dir.path().join("run");
    std::fs::create_dir_all(&run_dir)?;
    std::fs::write(run_dir.join("aftman.sock"), "precious")?;
    let paths = DaemonPaths::in_dir(&run_dir);
    let error = try_start(&paths, dir.path())
        .await
        .err()
        .ok_or("must fail")?;
    assert!(matches!(error, LifecycleError::NotASocket(_)), "{error}");
    assert_eq!(
        std::fs::read_to_string(run_dir.join("aftman.sock"))?,
        "precious"
    );
    Ok(())
}

#[tokio::test]
async fn two_daemons_starting_at_once_cannot_both_win() -> TestResult {
    for round in 0..20 {
        let dir = common::short_tempdir()?;
        let paths = DaemonPaths::in_dir(&dir.path().join("run"));
        let (a, b) = tokio::join!(try_start(&paths, dir.path()), try_start(&paths, dir.path()));
        let outcomes = [a, b];
        let winners = outcomes.iter().filter(|r| r.is_ok()).count();
        assert_eq!(winners, 1, "round {round}: exactly one daemon may win");
        for outcome in &outcomes {
            if let Err(error) = outcome {
                assert!(
                    matches!(error, LifecycleError::AlreadyRunning { pid } if *pid == std::process::id()),
                    "round {round}: the loser is told who won: {error}"
                );
            }
        }
        drop(outcomes);
    }
    Ok(())
}

#[tokio::test]
async fn a_clean_shutdown_leaves_nothing_for_the_next_daemon_to_trip_on() -> TestResult {
    let mut h = Harness::start().await?;
    let (socket, pid_file) = (h.paths.socket.clone(), h.paths.pid_file.clone());
    h.shutdown_via_future().await;
    assert!(!socket.exists() && !pid_file.exists());

    // The same paths are immediately usable again.
    let again = try_start(&h.paths, h.dir.path()).await;
    assert!(again.is_ok(), "{:?}", again.err());
    Ok(())
}
