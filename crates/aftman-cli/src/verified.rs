//! Launch shapes a live probe has verified on this machine.
//!
//! A backend is trusted to speak the format aftman decodes when a recording of its real output
//! is in `crates/aftman-workers/tests/fixtures/`. One launch shape has no recording yet:
//! `pi --mode rpc`, which `--steer` selects. Until it does, that shape is **gated behind
//! `aftman doctor --probe-workers`** (`docs/04-workers.md` §12): the probe runs one RPC turn
//! against the installed `pi`, and only a turn that decodes, settles, and exits cleanly when its
//! input closes is written down here, with the executable's path, its version and aftman's
//! version.
//!
//! "The installed `pi`" is the file a run would execute: `pi` resolved against the **run's own
//! worker environment** (the client's forwarded `PATH`, not the daemon's). A run is refused
//! unless that file is the one the probe verified and still reports the same version, and the
//! run then executes it by that absolute path, so the file checked is the file run.
//!
//! The record is `<state dir>/verified-streams.json`, `0600`. It is the supervisor's own file
//! in its own private directory; an unreadable or malformed one is treated as no record at all.

use std::io::{BufRead as _, Read as _};
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// The record's file name inside the state directory.
pub const FILE: &str = "verified-streams.json";
/// The most of the record that is read. It holds a handful of short strings.
const MAX_FILE_BYTES: u64 = 64 * 1024;
/// How long `<cli> --version` may take, in total, before the answer is "unknown".
const VERSION_TIMEOUT: Duration = Duration::from_secs(10);
/// The most of a version line that is kept.
const MAX_VERSION_CHARS: usize = 200;

/// A launch shape with no recorded fixture, which a live probe can unlock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    /// `pi --mode rpc`, selected by `--worker pi --steer`.
    PiRpc,
}

impl Shape {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PiRpc => "pi-rpc",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    streams: Vec<Entry>,
}

/// What a probe verified.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entry {
    shape: String,
    /// The absolute path of the executable the probe ran.
    pub cli_path: PathBuf,
    /// The first line of its `--version`, as [`installed_version`] reads it.
    pub cli_version: String,
    /// The aftman that ran the probe. Its decoder is half of what was verified.
    aftman: String,
}

fn path(data_dir: &Path) -> PathBuf {
    data_dir.join(FILE)
}

fn load(data_dir: &Path) -> Option<Record> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(nix::fcntl::OFlag::O_NOFOLLOW.bits())
        .open(path(data_dir))
        .ok()?;
    let mut text = String::new();
    file.take(MAX_FILE_BYTES).read_to_string(&mut text).ok()?;
    serde_json::from_str(&text).ok()
}

/// What a probe by this aftman verified for `shape`, if anything.
#[must_use]
pub fn verified(data_dir: &Path, shape: Shape) -> Option<Entry> {
    let aftman = env!("CARGO_PKG_VERSION");
    load(data_dir)?
        .streams
        .into_iter()
        .find(|entry| entry.shape == shape.as_str() && entry.aftman == aftman)
}

/// Record that `shape` was verified against the executable at `cli_path`, reporting
/// `cli_version`, by this aftman.
///
/// Written to a fresh temporary file and renamed, so a reader never sees half a record and the
/// record is `0600` whatever was left at the temporary path before.
///
/// # Errors
/// I/O errors.
pub fn record(
    data_dir: &Path,
    shape: Shape,
    cli_path: &Path,
    cli_version: &str,
) -> std::io::Result<()> {
    use std::io::Write as _;
    crate::statedir::create_private_dir(data_dir)?;
    let mut current = load(data_dir).unwrap_or_default();
    current
        .streams
        .retain(|entry| entry.shape != shape.as_str());
    current.streams.push(Entry {
        shape: shape.as_str().to_owned(),
        cli_path: cli_path.to_path_buf(),
        cli_version: cli_version.to_owned(),
        aftman: env!("CARGO_PKG_VERSION").to_owned(),
    });
    let text = serde_json::to_string_pretty(&current).map_err(std::io::Error::other)?;
    let target = path(data_dir);
    let temporary = data_dir.join(format!("{FILE}.tmp"));
    // A leftover temporary file keeps its own mode through a truncate, so it is removed and
    // the file created anew; `create_new` with `O_NOFOLLOW` refuses anything planted between.
    match std::fs::remove_file(&temporary) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(nix::fcntl::OFlag::O_NOFOLLOW.bits())
        .open(&temporary)?;
    file.write_all(text.as_bytes())?;
    file.sync_all()?;
    std::fs::rename(&temporary, &target)
}

/// Why a run asking for `shape` may not start.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Refusal {
    #[error(
        "`--steer` drives `pi --mode rpc`, whose output has no recorded fixture, so aftman will \
         not supervise it until a live probe has verified it on this machine. Run `aftman \
         doctor --probe-workers` (one trivial turn, at the agent's usual cost) from the shell \
         you start runs from, then retry. Without `--steer`, pi runs one-shot and needs no probe"
    )]
    NeverProbed,
    #[error(
        "`--steer` drives `pi --mode rpc`, but there is no `pi` on this run's PATH, so there is \
         nothing to compare with the one `aftman doctor --probe-workers` verified ({verified})"
    )]
    NotFound { verified: String },
    #[error(
        "`--steer` drives `pi --mode rpc`, which was verified for `{verified}`, but this run's \
         PATH finds `{found}`. Run `aftman doctor --probe-workers` from this shell, then retry"
    )]
    PathChanged { verified: String, found: String },
    #[error(
        "`--steer` drives `pi --mode rpc`, which was verified against `{verified}`, but the \
         installed pi reports `{installed}`. Run `aftman doctor --probe-workers` again for \
         this version, then retry"
    )]
    VersionChanged { verified: String, installed: String },
    #[error(
        "`--steer` drives `pi --mode rpc`, which was verified against `{verified}`, but the \
         installed pi's version could not be read. Check that `pi --version` works, then run \
         `aftman doctor --probe-workers` again"
    )]
    VersionUnknown { verified: String },
}

/// Allow `shape` only for the executable a probe verified, still at the version it reported.
///
/// `found` is the executable the run would start (`None`: nothing on its PATH), and
/// `installed` reads that executable's version. Neither is consulted when no record exists, so
/// a machine that never ran the probe — every test sandbox — is refused without starting
/// anything. On success, returns the verified executable, for the run to start by that path.
///
/// # Errors
/// The [`Refusal`] to show the operator.
pub fn require(
    data_dir: &Path,
    shape: Shape,
    found: impl FnOnce() -> Option<PathBuf>,
    installed: impl FnOnce(&Path) -> Option<String>,
) -> Result<Entry, Refusal> {
    let Some(entry) = verified(data_dir, shape) else {
        return Err(Refusal::NeverProbed);
    };
    let Some(found) = found() else {
        return Err(Refusal::NotFound {
            verified: entry.cli_path.display().to_string(),
        });
    };
    if found != entry.cli_path {
        return Err(Refusal::PathChanged {
            verified: entry.cli_path.display().to_string(),
            found: found.display().to_string(),
        });
    }
    match installed(&found) {
        Some(installed) if installed == entry.cli_version => Ok(entry),
        Some(installed) => Err(Refusal::VersionChanged {
            verified: entry.cli_version,
            installed,
        }),
        None => Err(Refusal::VersionUnknown {
            verified: entry.cli_version,
        }),
    }
}

/// The executable `program` names in `environment`, the way a spawn with that environment
/// would find it — as given if it contains a `/`, otherwise the first executable regular file
/// on the environment's `PATH` — with every symlink resolved. Nothing is run.
///
/// Canonical because version managers put a per-shell directory of symlinks first on `PATH`
/// (fnm's `fnm_multishells/<pid>_<ts>/bin`): the same `pi` reached from two shells must be one
/// file, or the record would die with the shell that made it. The run then starts this
/// canonical path, so the file compared is the file run.
#[must_use]
pub fn resolve(program: &str, environment: &[(String, String)]) -> Option<PathBuf> {
    fn executable(path: &Path) -> bool {
        std::fs::metadata(path)
            .is_ok_and(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
    }
    if program.contains('/') {
        let path = std::path::absolute(program).ok()?;
        return executable(&path)
            .then(|| std::fs::canonicalize(&path).ok())
            .flatten();
    }
    let search = environment
        .iter()
        .rev()
        .find(|(name, _)| name == "PATH")
        .map(|(_, value)| value.as_str())?;
    std::env::split_paths(search)
        // An empty entry means the working directory, which for a worker is the repository it
        // is supervising: never a place to find the agent.
        .filter(|dir| dir.is_absolute())
        .map(|dir| dir.join(program))
        .find(|candidate| executable(candidate))
        .and_then(|found| std::fs::canonicalize(found).ok())
}

/// The first non-empty line of `<program> --version`, or `None` if it cannot be read within
/// [`VERSION_TIMEOUT`].
///
/// The one definition both the probe and the gate use, so a version recorded by one is
/// compared like for like by the other. The environment comes from `worker_environment`: this
/// spawns a subprocess like any other (hard rule 3). The CLI runs in its own process group,
/// which is ended once the line is read or the deadline passes, so neither a slow CLI nor a
/// background child still holding its stdout can hold a run start.
#[must_use]
pub fn installed_version(program: &Path, extra_env: &[(String, String)]) -> Option<String> {
    let mut child = std::process::Command::new(program)
        .arg("--version")
        .env_clear()
        .envs(aftman_workers::worker_environment(extra_env))
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .process_group(0)
        .spawn()
        .ok()?;
    let stdout = child.stdout.take()?;
    let (sender, receiver) = std::sync::mpsc::channel();
    // The read is on its own thread so the deadline covers it: a pipe stays open for as long
    // as any process holding it lives, not only the CLI itself.
    std::thread::spawn(move || {
        let mut lines = std::io::BufReader::new(stdout.take(MAX_FILE_BYTES)).lines();
        let first: Option<String> = lines.find_map(|line| {
            let line = line.ok()?;
            let line = line.trim();
            (!line.is_empty()).then(|| line.chars().take(MAX_VERSION_CHARS).collect())
        });
        let _ = sender.send(first);
    });
    let version = receiver.recv_timeout(VERSION_TIMEOUT).ok().flatten();
    // The leader is not yet reaped, so its pid — the group id — cannot have been reused.
    if let Ok(pid) = i32::try_from(child.id()) {
        let _ = nix::sys::signal::killpg(
            nix::unistd::Pid::from_raw(pid),
            nix::sys::signal::Signal::SIGKILL,
        );
    }
    let _ = child.wait();
    version
}

#[cfg(test)]
mod tests {
    use super::{FILE, Refusal, Shape, installed_version, record, require, resolve, verified};
    use std::os::unix::fs::PermissionsExt as _;
    use std::path::{Path, PathBuf};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    const PI: &str = "/opt/pi/bin/pi";

    fn found(path: &str) -> impl FnOnce() -> Option<PathBuf> {
        let path = PathBuf::from(path);
        move || Some(path)
    }

    fn reports(version: &str) -> impl FnOnce(&Path) -> Option<String> {
        let version = version.to_owned();
        move |_| Some(version)
    }

    #[test]
    fn a_machine_that_never_probed_is_refused_without_asking_the_cli() -> TestResult {
        let dir = tempfile::tempdir()?;
        let refused = require(
            dir.path(),
            Shape::PiRpc,
            || panic!("no record: nothing is looked up"),
            |_| panic!("no record: the installed CLI must not be run"),
        );
        assert_eq!(refused, Err(Refusal::NeverProbed));
        Ok(())
    }

    #[test]
    fn a_probe_of_the_executable_the_run_finds_unlocks_the_shape() -> TestResult {
        let dir = tempfile::tempdir()?;
        record(dir.path(), Shape::PiRpc, Path::new(PI), "1.5.0")?;
        let entry = require(dir.path(), Shape::PiRpc, found(PI), reports("1.5.0"))
            .map_err(|refusal| refusal.to_string())?;
        assert_eq!(entry.cli_path, PathBuf::from(PI));
        Ok(())
    }

    #[test]
    fn a_different_pi_on_the_runs_path_is_refused_even_at_the_same_version() -> TestResult {
        // The probe ran in one shell and the run comes from another, whose PATH finds a
        // different pi. Its version is not consulted: it is not the file that was verified.
        let dir = tempfile::tempdir()?;
        record(dir.path(), Shape::PiRpc, Path::new(PI), "1.5.0")?;
        let refused = require(
            dir.path(),
            Shape::PiRpc,
            found("/home/me/.local/bin/pi"),
            |_| panic!("a different file is not version-checked"),
        );
        assert!(
            matches!(refused, Err(Refusal::PathChanged { .. })),
            "{refused:?}"
        );
        assert!(matches!(
            require(dir.path(), Shape::PiRpc, || None, reports("1.5.0")),
            Err(Refusal::NotFound { .. })
        ));
        Ok(())
    }

    #[test]
    fn an_upgraded_cli_needs_the_probe_again() -> TestResult {
        let dir = tempfile::tempdir()?;
        record(dir.path(), Shape::PiRpc, Path::new(PI), "1.5.0")?;
        assert_eq!(
            require(dir.path(), Shape::PiRpc, found(PI), reports("1.6.0")),
            Err(Refusal::VersionChanged {
                verified: "1.5.0".to_owned(),
                installed: "1.6.0".to_owned()
            })
        );
        assert!(matches!(
            require(dir.path(), Shape::PiRpc, found(PI), |_| None),
            Err(Refusal::VersionUnknown { .. })
        ));
        Ok(())
    }

    #[test]
    fn a_record_from_another_aftman_does_not_count() -> TestResult {
        // The decoder is half of what a probe verified, so a different aftman's record is
        // not evidence about this one.
        let dir = tempfile::tempdir()?;
        std::fs::write(
            dir.path().join(FILE),
            format!(
                r#"{{"streams":[{{"shape":"pi-rpc","cli_path":"{PI}","cli_version":"1.5.0","aftman":"0.0.0-other"}}]}}"#
            ),
        )?;
        assert_eq!(verified(dir.path(), Shape::PiRpc), None);
        Ok(())
    }

    #[test]
    fn a_malformed_or_planted_record_is_no_record() -> TestResult {
        let dir = tempfile::tempdir()?;
        std::fs::write(dir.path().join(FILE), "{ not json")?;
        assert_eq!(verified(dir.path(), Shape::PiRpc), None);
        // Unknown fields are refused on input, not ignored.
        std::fs::write(
            dir.path().join(FILE),
            format!(
                r#"{{"streams":[{{"shape":"pi-rpc","cli_path":"{PI}","cli_version":"1.5.0","aftman":"{}","x":1}}]}}"#,
                env!("CARGO_PKG_VERSION")
            ),
        )?;
        assert_eq!(verified(dir.path(), Shape::PiRpc), None);
        // A symlink at the record's path is not followed.
        let elsewhere = dir.path().join("elsewhere.json");
        record(dir.path(), Shape::PiRpc, Path::new(PI), "1.5.0")?;
        std::fs::rename(dir.path().join(FILE), &elsewhere)?;
        std::os::unix::fs::symlink(&elsewhere, dir.path().join(FILE))?;
        assert_eq!(verified(dir.path(), Shape::PiRpc), None);
        Ok(())
    }

    #[test]
    fn the_record_is_private_and_rerecording_replaces_the_entry() -> TestResult {
        let dir = tempfile::tempdir()?;
        record(dir.path(), Shape::PiRpc, Path::new(PI), "1.5.0")?;
        record(dir.path(), Shape::PiRpc, Path::new(PI), "1.6.0")?;
        assert_eq!(
            verified(dir.path(), Shape::PiRpc).map(|entry| entry.cli_version),
            Some("1.6.0".to_owned())
        );
        let text = std::fs::read_to_string(dir.path().join(FILE))?;
        assert_eq!(text.matches("pi-rpc").count(), 1, "{text}");
        let mode = std::fs::metadata(dir.path().join(FILE))?
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        Ok(())
    }

    #[test]
    fn a_stale_temporary_file_cannot_make_the_record_readable() -> TestResult {
        // A truncate keeps the mode of the file it opens; the record must not inherit 0644
        // from a temporary file left behind by an interrupted write.
        let dir = tempfile::tempdir()?;
        let temporary = dir.path().join(format!("{FILE}.tmp"));
        std::fs::write(&temporary, "stale")?;
        std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o644))?;
        record(dir.path(), Shape::PiRpc, Path::new(PI), "1.5.0")?;
        let mode = std::fs::metadata(dir.path().join(FILE))?
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        Ok(())
    }

    fn script(dir: &Path, name: &str, body: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let path = dir.join(name);
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n"))?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))?;
        Ok(path)
    }

    #[test]
    fn the_executable_is_found_on_the_given_path_not_this_processs() -> TestResult {
        let first = tempfile::tempdir()?;
        let second = tempfile::tempdir()?;
        let wanted = script(second.path(), "pi-fake", "echo second")?;
        // Not executable: skipped, as a spawn would skip it.
        std::fs::write(first.path().join("pi-fake"), "not a program")?;
        let path = std::env::join_paths([first.path(), second.path()])?
            .into_string()
            .map_err(|_| "a temporary path is text")?;
        let environment = vec![("PATH".to_owned(), path)];
        let wanted = std::fs::canonicalize(wanted)?;
        assert_eq!(resolve("pi-fake", &environment), Some(wanted.clone()));
        assert_eq!(resolve("pi-fake", &[]), None, "no PATH, nothing found");
        assert_eq!(
            resolve(&wanted.display().to_string(), &[]),
            Some(wanted),
            "a path is taken as given"
        );
        Ok(())
    }

    #[test]
    fn the_same_file_behind_two_symlink_directories_is_one_executable() -> TestResult {
        // fnm's per-shell `fnm_multishells/<pid>_<ts>/bin`: each shell reaches the same pi
        // through a different symlink, and a probe in one must hold for a run from another.
        let real = tempfile::tempdir()?;
        let program = script(real.path(), "pi", "echo 'pi 1.5.0'")?;
        let shell = |name: &str| -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
            let dir = real.path().join(name);
            std::fs::create_dir(&dir)?;
            std::os::unix::fs::symlink(&program, dir.join("pi"))?;
            Ok(vec![("PATH".to_owned(), dir.display().to_string())])
        };
        let (one, two) = (shell("shell1")?, shell("shell2")?);
        let found = resolve("pi", &one).ok_or("found from shell one")?;
        assert_eq!(Some(found.clone()), resolve("pi", &two));
        assert_eq!(found, std::fs::canonicalize(&program)?);
        Ok(())
    }

    #[test]
    fn a_version_is_read_from_the_first_line_and_the_cli_is_ended() -> TestResult {
        let dir = tempfile::tempdir()?;
        // Prints its version, then leaves a child holding stdout open for longer than the
        // deadline: the line is enough, and nothing waits for the child.
        let program = script(dir.path(), "pi", "echo; echo 'pi 9.9.9'; sleep 60 & wait")?;
        let started = std::time::Instant::now();
        assert_eq!(
            installed_version(&program, &[]).as_deref(),
            Some("pi 9.9.9")
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
        Ok(())
    }

    #[test]
    fn a_cli_that_never_answers_is_unknown_within_the_deadline() -> TestResult {
        let dir = tempfile::tempdir()?;
        let program = script(dir.path(), "pi", "sleep 60 & wait")?;
        let started = std::time::Instant::now();
        assert_eq!(installed_version(&program, &[]), None);
        assert!(started.elapsed() < std::time::Duration::from_secs(15));
        Ok(())
    }
}
