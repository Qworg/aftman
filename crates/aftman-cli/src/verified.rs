//! Launch shapes a live probe has verified on this machine.
//!
//! A backend is trusted to speak the format aftman decodes when a recording of its real output
//! is in `crates/aftman-workers/tests/fixtures/`. One launch shape has no recording yet:
//! `pi --mode rpc`, which `--steer` selects. Until it does, that shape is **gated behind
//! `aftman doctor --probe-workers`** (`docs/04-workers.md` §12): the probe runs one RPC turn
//! against the installed `pi`, and only a turn that decodes, settles, and exits when its input
//! closes is written down here, with the CLI's version and aftman's. A run that asks for the
//! shape is refused unless the record matches the `pi` installed now, so an upgrade of either
//! side asks for the probe again.
//!
//! The record is `<state dir>/verified-streams.json`, `0600`. It is the supervisor's own file
//! in its own private directory; an unreadable or malformed one is treated as no record at all.

use std::io::Read as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// The record's file name inside the state directory.
pub const FILE: &str = "verified-streams.json";
/// The most of the record that is read. It holds a handful of short strings.
const MAX_FILE_BYTES: u64 = 64 * 1024;
/// How long `<cli> --version` may take before the answer is "unknown".
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    shape: String,
    /// The first line of the CLI's `--version`, as [`installed_version`] reads it.
    cli_version: String,
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

/// The CLI version a probe verified `shape` against, if this aftman verified it.
#[must_use]
pub fn verified_version(data_dir: &Path, shape: Shape) -> Option<String> {
    let aftman = env!("CARGO_PKG_VERSION");
    load(data_dir)?
        .streams
        .into_iter()
        .find(|entry| entry.shape == shape.as_str() && entry.aftman == aftman)
        .map(|entry| entry.cli_version)
}

/// Record that `shape` was verified against `cli_version` by this aftman.
///
/// Written to a temporary file and renamed, so a reader never sees half a record.
///
/// # Errors
/// I/O errors.
pub fn record(data_dir: &Path, shape: Shape, cli_version: &str) -> std::io::Result<()> {
    use std::io::Write as _;
    crate::statedir::create_private_dir(data_dir)?;
    let mut current = load(data_dir).unwrap_or_default();
    current
        .streams
        .retain(|entry| entry.shape != shape.as_str());
    current.streams.push(Entry {
        shape: shape.as_str().to_owned(),
        cli_version: cli_version.to_owned(),
        aftman: env!("CARGO_PKG_VERSION").to_owned(),
    });
    let text = serde_json::to_string_pretty(&current).map_err(std::io::Error::other)?;
    let target = path(data_dir);
    let temporary = data_dir.join(format!("{FILE}.tmp"));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
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
         doctor --probe-workers` (one trivial turn, at the agent's usual cost), then retry. \
         Without `--steer`, pi runs one-shot and needs no probe"
    )]
    NeverProbed,
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

/// Allow `shape` only where a probe verified the CLI installed now.
///
/// `installed` is asked only when a record exists, so a machine that never ran the probe —
/// every test sandbox — is refused without starting anything.
///
/// # Errors
/// The [`Refusal`] to show the operator.
pub fn require(
    data_dir: &Path,
    shape: Shape,
    installed: impl FnOnce() -> Option<String>,
) -> Result<String, Refusal> {
    let Some(verified) = verified_version(data_dir, shape) else {
        return Err(Refusal::NeverProbed);
    };
    match installed() {
        Some(installed) if installed == verified => Ok(verified),
        Some(installed) => Err(Refusal::VersionChanged {
            verified,
            installed,
        }),
        None => Err(Refusal::VersionUnknown { verified }),
    }
}

/// The first non-empty line of `<program> --version`, or `None` if it cannot be read in time.
///
/// The one definition both the probe and the gate use, so a version recorded by one is
/// compared like for like by the other. The environment comes from `worker_environment`:
/// this spawns a subprocess like any other (hard rule 3).
#[must_use]
pub fn installed_version(program: &str) -> Option<String> {
    let mut child = std::process::Command::new(program)
        .arg("--version")
        .env_clear()
        .envs(aftman_workers::worker_environment(&[]))
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .ok()?;
    // Bounded: a version that never answers is unknown, not a hung run start. A version
    // line is small, so the pipe cannot fill before the child exits.
    let deadline = std::time::Instant::now() + VERSION_TIMEOUT;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) if std::time::Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(20));
            }
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    }
    let mut text = String::new();
    child
        .stdout
        .take()?
        .take(MAX_FILE_BYTES)
        .read_to_string(&mut text)
        .ok()?;
    text.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(|line| line.chars().take(MAX_VERSION_CHARS).collect())
}

#[cfg(test)]
mod tests {
    use super::{FILE, Refusal, Shape, record, require, verified_version};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn a_machine_that_never_probed_is_refused_without_asking_the_cli() -> TestResult {
        let dir = tempfile::tempdir()?;
        let refused = require(dir.path(), Shape::PiRpc, || {
            panic!("no record: the installed CLI must not be run")
        });
        assert_eq!(refused, Err(Refusal::NeverProbed));
        Ok(())
    }

    #[test]
    fn a_probe_for_the_installed_version_unlocks_the_shape() -> TestResult {
        let dir = tempfile::tempdir()?;
        record(dir.path(), Shape::PiRpc, "1.5.0")?;
        assert_eq!(
            require(dir.path(), Shape::PiRpc, || Some("1.5.0".to_owned())),
            Ok("1.5.0".to_owned())
        );
        Ok(())
    }

    #[test]
    fn an_upgraded_cli_needs_the_probe_again() -> TestResult {
        let dir = tempfile::tempdir()?;
        record(dir.path(), Shape::PiRpc, "1.5.0")?;
        assert_eq!(
            require(dir.path(), Shape::PiRpc, || Some("1.6.0".to_owned())),
            Err(Refusal::VersionChanged {
                verified: "1.5.0".to_owned(),
                installed: "1.6.0".to_owned()
            })
        );
        assert!(matches!(
            require(dir.path(), Shape::PiRpc, || None),
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
            r#"{"streams":[{"shape":"pi-rpc","cli_version":"1.5.0","aftman":"0.0.0-other"}]}"#,
        )?;
        assert_eq!(verified_version(dir.path(), Shape::PiRpc), None);
        Ok(())
    }

    #[test]
    fn a_malformed_or_planted_record_is_no_record() -> TestResult {
        let dir = tempfile::tempdir()?;
        std::fs::write(dir.path().join(FILE), "{ not json")?;
        assert_eq!(verified_version(dir.path(), Shape::PiRpc), None);
        // Unknown fields are refused on input, not ignored.
        std::fs::write(
            dir.path().join(FILE),
            format!(
                r#"{{"streams":[{{"shape":"pi-rpc","cli_version":"1.5.0","aftman":"{}","x":1}}]}}"#,
                env!("CARGO_PKG_VERSION")
            ),
        )?;
        assert_eq!(verified_version(dir.path(), Shape::PiRpc), None);
        // A symlink at the record's path is not followed.
        let elsewhere = dir.path().join("elsewhere.json");
        record(dir.path(), Shape::PiRpc, "1.5.0")?;
        std::fs::rename(dir.path().join(FILE), &elsewhere)?;
        std::os::unix::fs::symlink(&elsewhere, dir.path().join(FILE))?;
        assert_eq!(verified_version(dir.path(), Shape::PiRpc), None);
        Ok(())
    }

    #[test]
    fn the_record_is_private_and_rerecording_replaces_the_entry() -> TestResult {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir()?;
        record(dir.path(), Shape::PiRpc, "1.5.0")?;
        record(dir.path(), Shape::PiRpc, "1.6.0")?;
        assert_eq!(
            verified_version(dir.path(), Shape::PiRpc).as_deref(),
            Some("1.6.0")
        );
        let text = std::fs::read_to_string(dir.path().join(FILE))?;
        assert_eq!(text.matches("pi-rpc").count(), 1, "{text}");
        let mode = std::fs::metadata(dir.path().join(FILE))?
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        Ok(())
    }
}
