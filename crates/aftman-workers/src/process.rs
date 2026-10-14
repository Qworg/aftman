//! Identifying a worker's process group, and ending one that its supervisor left behind.
//!
//! A worker runs in its own process group so that termination reaches its children. That
//! is also why it **survives** its supervisor: `kill -9` of the daemon leaves the group
//! running, still editing the repository. [`identify`] captures what is needed to find the
//! group again; [`end_orphan`] uses it, but only after it has positively established that
//! the process now at that pid is the same one.
//!
//! # What "positively identified" means
//!
//! Pids are reused, so a pid is never trusted on its own. The identity is the pid together
//! with the leader's **start time** (an unforgeable-by-accident value that differs for any
//! later process given the same pid) and the **boot id** (so a recorded start time from
//! before a reboot never matches a process after it).
//!
//! - **Linux:** start time is `/proc/<pid>/stat` field 22 (clock ticks since boot); the boot
//!   id is `/proc/sys/kernel/random/boot_id`. The process group is read from the same file
//!   (field 5) and must equal the recorded `pgid`.
//! - **Other Unix (macOS, BSD):** there is no `/proc`. The start token is the output of
//!   `ps -o lstart= -p <pid>` (second resolution), the group is `ps -o pgid= -p <pid>`,
//!   and there is no boot id. This is weaker (two processes started in the same second
//!   with the same pid cannot be told apart, which would need a pid wrap within one
//!   second) and is untested here; where `ps` is unavailable no token is recorded and
//!   the worker is never signalled.
//!
//! The limit that cannot be removed: if the group **leader** has already exited while
//! other members live on, nothing identifies those members (their start times fall after
//! the leader's, and so do an unrelated new group's), so they are reported
//! [`OrphanOutcome::Unverified`] and left alone. Signalling something that might be
//! someone else's process is worse than reporting a leak.

use std::time::Duration;

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;

use aftman_core::worker::ProcessIdentity;

/// How long a polite interrupt is given before the group is killed.
pub const ORPHAN_GRACE: Duration = Duration::from_secs(5);

/// How long to wait for the group to disappear after a `SIGKILL`.
const KILL_SETTLE: Duration = Duration::from_secs(3);

/// What [`end_orphan`] found and did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrphanOutcome {
    /// The recorded process was identified and its group has been ended.
    Killed,
    /// Nothing is left: the group is gone, or the machine rebooted since.
    AlreadyGone,
    /// A process (or group) is there but could not be positively identified as the
    /// worker, so it was **not** signalled. The string says why.
    Unverified(String),
}

/// `(start token, process group)` of a live pid, as the OS reports them.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Observed {
    start_token: String,
    pgid: i32,
}

#[cfg(target_os = "linux")]
fn observe(pid: i32) -> Option<Observed> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // "pid (comm) state ppid pgrp session tty_nr tpgid flags ... starttime ..."; comm may
    // itself contain spaces and parentheses, so split after the *last* ')'.
    let after = &stat[stat.rfind(')')? + 1..];
    let fields: Vec<&str> = after.split_whitespace().collect();
    // fields[0] is field 3 (state); pgrp is field 5; starttime is field 22.
    let pgid = fields.get(2)?.parse().ok()?;
    let start_token = (*fields.get(19)?).to_owned();
    Some(Observed { start_token, pgid })
}

#[cfg(not(target_os = "linux"))]
fn observe(pid: i32) -> Option<Observed> {
    let ps = |column: &str| -> Option<String> {
        // Hard rule 3: every subprocess gets the scrubbed environment (`ps` needs none
        // of the supervisor's secrets).
        let out = std::process::Command::new("ps")
            .env_clear()
            .envs(crate::worker_environment(&[]))
            .args(["-o", column, "-p", &pid.to_string()])
            .output()
            .ok()?;
        let text = String::from_utf8_lossy(&out.stdout).trim().to_owned();
        (out.status.success() && !text.is_empty()).then_some(text)
    };
    Some(Observed {
        start_token: ps("lstart=")?,
        pgid: ps("pgid=")?.parse().ok()?,
    })
}

#[cfg(target_os = "linux")]
fn current_boot_id() -> Option<String> {
    std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .ok()
        .map(|s| s.trim().to_owned())
}

#[cfg(not(target_os = "linux"))]
fn current_boot_id() -> Option<String> {
    None
}

/// Capture the identity of the worker whose process group is `pgid` (its leader's pid).
/// Call right after spawning. Never fails: what cannot be read is left `None`, which
/// makes the worker unidentifiable later (and therefore never signalled by recovery).
#[must_use]
pub fn identify(pgid: i32, mission_file: Option<std::path::PathBuf>) -> ProcessIdentity {
    ProcessIdentity {
        pgid,
        start_token: observe(pgid)
            .filter(|seen| seen.pgid == pgid)
            .map(|seen| seen.start_token),
        boot_id: current_boot_id(),
        mission_file,
    }
}

/// Whether any process is in the group.
fn group_exists(pgid: i32) -> bool {
    // Signal "none" only checks existence and permission. EPERM means it exists but is
    // not ours, which still counts as existing.
    !matches!(
        kill(Pid::from_raw(-pgid), None),
        Err(nix::errno::Errno::ESRCH)
    )
}

async fn wait_gone(pgid: i32, within: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        if !group_exists(pgid) {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// End the worker described by `identity`, if and only if it can be positively
/// identified: `SIGINT` to the group, `grace` for it to go quietly, then `SIGKILL`.
pub async fn end_orphan(identity: &ProcessIdentity, grace: Duration) -> OrphanOutcome {
    let pgid = identity.pgid;
    if pgid <= 1 {
        return OrphanOutcome::Unverified(format!("{pgid} is not a worker process group"));
    }
    // A different boot: everything from the recorded one is gone.
    if let (Some(recorded), Some(now)) = (&identity.boot_id, current_boot_id())
        && *recorded != now
    {
        return OrphanOutcome::AlreadyGone;
    }
    let Some(recorded_token) = identity.start_token.clone() else {
        return if group_exists(pgid) {
            OrphanOutcome::Unverified(
                "no start time was recorded for the worker, so it cannot be identified".to_owned(),
            )
        } else {
            OrphanOutcome::AlreadyGone
        };
    };

    let seen = tokio::task::spawn_blocking(move || observe(pgid))
        .await
        .ok()
        .flatten();
    match seen {
        Some(seen) if seen.start_token == recorded_token && seen.pgid == pgid => {
            // The worker's leader, positively identified.
            let _ = kill(Pid::from_raw(-pgid), Signal::SIGINT);
            if wait_gone(pgid, grace).await {
                return OrphanOutcome::Killed;
            }
            let _ = kill(Pid::from_raw(-pgid), Signal::SIGKILL);
            if wait_gone(pgid, KILL_SETTLE).await {
                OrphanOutcome::Killed
            } else {
                OrphanOutcome::Unverified(
                    "the worker's process group survived SIGKILL (uninterruptible)".to_owned(),
                )
            }
        }
        Some(_) => {
            // Something else is at that pid now: the leader is gone and the pid reused.
            if group_exists(pgid) {
                OrphanOutcome::Unverified(
                    "the pid now belongs to a different process and a group with that id \
                     still exists; its members cannot be identified"
                        .to_owned(),
                )
            } else {
                OrphanOutcome::AlreadyGone
            }
        }
        None => {
            if group_exists(pgid) {
                OrphanOutcome::Unverified(
                    "the group leader has exited but group members remain; they cannot be \
                     identified"
                        .to_owned(),
                )
            } else {
                OrphanOutcome::AlreadyGone
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{OrphanOutcome, end_orphan, identify};
    use std::os::unix::process::CommandExt as _;
    use std::time::Duration;

    fn spawn_group(script: &str) -> std::process::Child {
        std::process::Command::new("/bin/sh")
            .args(["-c", script])
            .process_group(0)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn")
    }

    /// Tests leave nothing running: kill the whole group (a forked `sleep` included).
    fn kill_group_and_reap(child: &mut std::process::Child) {
        let _ = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(-i32::try_from(child.id()).expect("pid")),
            nix::sys::signal::Signal::SIGKILL,
        );
        let _ = child.wait();
    }

    #[tokio::test]
    async fn an_identified_group_is_ended_and_an_absent_one_is_not_an_error() {
        let child = spawn_group("sleep 60");
        let pid = i32::try_from(child.id()).expect("pid");
        let identity = identify(pid, None);
        assert!(identity.start_token.is_some(), "{identity:?}");
        // In production the dead supervisor's orphan is reaped by init; here, a thread.
        let reaper = std::thread::spawn(move || {
            let mut child = child;
            child.wait()
        });
        assert_eq!(
            end_orphan(&identity, Duration::from_millis(500)).await,
            OrphanOutcome::Killed
        );
        reaper.join().expect("join").expect("reap");
        assert_eq!(
            end_orphan(&identity, Duration::from_millis(200)).await,
            OrphanOutcome::AlreadyGone
        );
    }

    #[tokio::test]
    async fn a_pid_that_now_belongs_to_another_process_is_not_signalled() {
        let mut child = spawn_group("sleep 60");
        let pid = i32::try_from(child.id()).expect("pid");
        let mut identity = identify(pid, None);
        // As if the recorded process had died and its pid been reused by this one.
        identity.start_token = Some("1".to_owned());
        let outcome = end_orphan(&identity, Duration::from_millis(200)).await;
        assert!(
            matches!(outcome, OrphanOutcome::Unverified(_)),
            "{outcome:?}"
        );
        assert!(child.try_wait().expect("wait").is_none(), "still running");
        kill_group_and_reap(&mut child);
    }

    #[tokio::test]
    async fn a_worker_with_no_recorded_start_time_is_never_signalled() {
        let mut child = spawn_group("sleep 60");
        let pid = i32::try_from(child.id()).expect("pid");
        let mut identity = identify(pid, None);
        identity.start_token = None;
        assert!(matches!(
            end_orphan(&identity, Duration::from_millis(200)).await,
            OrphanOutcome::Unverified(_)
        ));
        assert!(child.try_wait().expect("wait").is_none());
        kill_group_and_reap(&mut child);
    }

    #[tokio::test]
    async fn a_different_boot_means_everything_recorded_is_gone() {
        let mut child = spawn_group("sleep 60");
        let pid = i32::try_from(child.id()).expect("pid");
        let mut identity = identify(pid, None);
        identity.boot_id = Some("not-this-boot".to_owned());
        if super::current_boot_id().is_some() {
            assert_eq!(
                end_orphan(&identity, Duration::from_millis(200)).await,
                OrphanOutcome::AlreadyGone
            );
            assert!(child.try_wait().expect("wait").is_none(), "left alone");
        }
        kill_group_and_reap(&mut child);
    }

    #[tokio::test]
    async fn surviving_members_of_a_group_whose_leader_left_are_not_guessed_at() {
        // The leader exits at once; a background child stays in its group.
        let mut leader = spawn_group("sleep 30 & exit 0");
        let pid = i32::try_from(leader.id()).expect("pid");
        let identity = identify(pid, None);
        leader.wait().expect("reap the leader");
        // Give the shell a moment to be gone and the background sleep to be adopted.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let outcome = end_orphan(&identity, Duration::from_millis(200)).await;
        assert!(
            matches!(
                outcome,
                OrphanOutcome::Unverified(_) | OrphanOutcome::AlreadyGone
            ),
            "{outcome:?}"
        );
        // Clean up the stray sleep ourselves (we know it is ours).
        let _ = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(-pid),
            nix::sys::signal::Signal::SIGKILL,
        );
    }
}
