//! The state directory, created private.
//!
//! The state directory holds job text, diffs and model reasoning: as sensitive as the
//! repository (docs/08 section 7). Whatever aftman creates under it is created `0700`
//! (directories) or `0600` (files), on every path: embedded runs, foreground `serve`,
//! auto-start. A directory the user already made is never chmodded; if they made it
//! world-readable, the subdirectories aftman puts inside it are still private.

use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};
use std::path::Path;

/// Where the file store keeps runs.
pub const RUNS_DIR: &str = "runs";
/// Mission files handed to workers.
pub const MISSIONS_DIR: &str = "missions";
/// Scratch space for evidence providers.
pub const EVIDENCE_DIR: &str = "evidence-tmp";
/// The detached daemon's log.
pub const LOG_FILE: &str = "daemon.log";
/// Past this size the log is rotated to `daemon.log.old` when a daemon starts.
pub const MAX_LOG_BYTES: u64 = 4 * 1024 * 1024;

/// Create `path` and any missing parents `0700`; leave existing directories alone.
///
/// # Errors
/// I/O errors.
pub fn create_private_dir(path: &Path) -> std::io::Result<()> {
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)
}

/// Make sure the state directory and the subdirectories aftman writes to exist, private.
///
/// # Errors
/// I/O errors.
pub fn ensure(data_dir: &Path) -> std::io::Result<()> {
    create_private_dir(data_dir)?;
    for sub in [RUNS_DIR, MISSIONS_DIR, EVIDENCE_DIR] {
        create_private_dir(&data_dir.join(sub))?;
    }
    Ok(())
}

/// Rotate `log` to `<log>.old` if it exceeds `max` bytes (one generation kept).
///
/// # Errors
/// I/O errors renaming; a missing log is fine.
pub fn rotate_log(log: &Path, max: u64) -> std::io::Result<bool> {
    match std::fs::metadata(log) {
        Ok(meta) if meta.len() > max => {
            let mut old = log.as_os_str().to_owned();
            old.push(".old");
            std::fs::rename(log, old)?;
            Ok(true)
        }
        Ok(_) => Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

/// Open the daemon log for appending, `0600`, after making the state directory and
/// rotating an oversized log.
///
/// # Errors
/// I/O errors.
pub fn open_log(data_dir: &Path, max: u64) -> std::io::Result<std::fs::File> {
    ensure(data_dir)?;
    let log = data_dir.join(LOG_FILE);
    rotate_log(&log, max)?;
    // `O_NOFOLLOW`: the log is appended to, so a symlink planted at its path would grow
    // whatever it points at. Nothing in a state directory is legitimately a symlink.
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .custom_flags(nix::fcntl::OFlag::O_NOFOLLOW.bits())
        .open(log)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt as _;

    fn mode(path: &Path) -> u32 {
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[test]
    fn everything_created_is_private() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("a/b/state");
        ensure(&state).unwrap();
        for p in [
            &state,
            &state.join("runs"),
            &state.join("missions"),
            &state.join("evidence-tmp"),
        ] {
            assert_eq!(mode(p), 0o700, "{}", p.display());
        }
        let log = open_log(&state, MAX_LOG_BYTES).unwrap();
        drop(log);
        assert_eq!(mode(&state.join(LOG_FILE)), 0o600);
    }

    #[test]
    fn a_directory_the_user_made_is_not_chmodded_but_its_children_are_private() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("mine");
        std::fs::create_dir(&state).unwrap();
        std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o755)).unwrap();
        ensure(&state).unwrap();
        assert_eq!(mode(&state), 0o755, "left as the user made it");
        assert_eq!(mode(&state.join("runs")), 0o700);
        assert_eq!(mode(&state.join("missions")), 0o700);
    }

    #[test]
    fn an_oversized_log_rotates_to_one_old_generation() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join(LOG_FILE);
        std::fs::write(&log, vec![b'x'; 100]).unwrap();
        assert!(!rotate_log(&log, 100).unwrap(), "at the cap is fine");
        std::fs::write(&log, vec![b'x'; 101]).unwrap();
        assert!(rotate_log(&log, 100).unwrap());
        assert!(!log.exists());
        let old = dir.path().join("daemon.log.old");
        assert_eq!(std::fs::metadata(&old).unwrap().len(), 101);
        // A second rotation replaces the old generation rather than accumulating.
        std::fs::write(&log, vec![b'y'; 150]).unwrap();
        assert!(rotate_log(&log, 100).unwrap());
        assert_eq!(std::fs::metadata(&old).unwrap().len(), 150);
        assert!(!rotate_log(&dir.path().join("none"), 1).unwrap());
    }
}
