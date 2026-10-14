//! Running a configured evidence command.
//!
//! The requirements are the same as for a worker, and for the same reasons: an own
//! process group so termination reaches a test runner's children, bounded output so a
//! noisy command cannot grow the supervisor, a hard timeout, and an environment built
//! through [`worker_environment`](crate::worker_environment) so repository code cannot
//! read the supervisor's credentials.
//!
//! Two differences from a worker:
//!
//! - **`argv` is literal.** There is no shell and no interpolation, so a job title
//!   containing `;` cannot become a command.
//! - **A non-zero exit is a result, not a failure.** A test command that exits 1 has
//!   *run*, and that is exactly the measurement the supervisor wanted.

use std::path::{Path, PathBuf};
use std::time::Duration;

use time::OffsetDateTime;
use tokio::io::AsyncReadExt;
use tokio::process::Command;

use aftman_core::evidence::{CommandProvider, EvidenceOutcome, ProviderCwd};

/// Run one provider.
///
/// Never returns an error: a command that cannot be launched, times out, or is killed is
/// reported as an outcome, because all three are things the supervisor should *see*
/// rather than exceptions it should handle.
///
/// `forwarded_env` is the run's allowlisted environment, so a test command finds the
/// same toolchain the agent did. It is passed through `worker_environment`, which
/// scrubs supervisor names a second time.
pub async fn run_provider(
    provider: &CommandProvider,
    repo: &Path,
    temp_dir: &Path,
    forwarded_env: &[(String, String)],
    now: OffsetDateTime,
) -> EvidenceOutcome {
    let cwd = match provider.cwd {
        ProviderCwd::Repo => repo.to_path_buf(),
        ProviderCwd::Temp => temp_dir.to_path_buf(),
    };
    let started = tokio::time::Instant::now();
    let outcome = run(provider, &cwd, forwarded_env).await;
    let duration = started.elapsed();

    match outcome {
        Ok((exit_code, output, truncated)) => EvidenceOutcome {
            provider: provider.id.clone(),
            argv: provider.argv.clone(),
            cwd,
            exit_code,
            success: provider.exit_was_success(exit_code),
            duration,
            output_excerpt: output,
            truncated,
            at: now,
            launch_error: None,
        },
        Err(error) => EvidenceOutcome {
            provider: provider.id.clone(),
            argv: provider.argv.clone(),
            cwd,
            exit_code: None,
            success: false,
            duration,
            output_excerpt: String::new(),
            truncated: false,
            at: now,
            launch_error: Some(error),
        },
    }
}

/// Launch and collect, returning `(exit_code, output, truncated)`.
async fn run(
    provider: &CommandProvider,
    cwd: &Path,
    forwarded_env: &[(String, String)],
) -> Result<(Option<i32>, String, bool), String> {
    let (program, args) = provider
        .argv
        .split_first()
        .ok_or_else(|| "empty argv".to_owned())?;

    if !cwd.is_dir() {
        return Err(format!("{} is not a directory", cwd.display()));
    }

    let mut command = Command::new(program);
    command.args(args);
    command.current_dir(cwd);
    // Scrubbed, exactly as a worker's environment is.
    command.env_clear();
    command.envs(crate::worker_environment(forwarded_env));
    command.stdin(std::process::Stdio::null());
    command.stdout(std::process::Stdio::piped());
    command.stderr(std::process::Stdio::piped());
    // Its own process group, so a termination reaches a test runner's children.
    command.process_group(0);
    command.kill_on_drop(true);

    let mut child = command
        .spawn()
        .map_err(|error| format!("{program}: {error}"))?;

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let budget = provider.max_output_bytes;

    let collect = async {
        let (out, err) = tokio::join!(read_bounded(stdout, budget), read_bounded(stderr, budget));
        let status = child.wait().await;
        (out, err, status)
    };

    match tokio::time::timeout(provider.timeout(), collect).await {
        Ok((Ok(stdout), Ok(stderr), Ok(status))) => {
            let truncated = stdout.1 || stderr.1;
            let mut output = stdout.0;
            if !stderr.0.is_empty() {
                if !output.is_empty() {
                    output.push('\n');
                }
                output.push_str("--- stderr ---\n");
                output.push_str(&stderr.0);
            }
            Ok((status.code(), output, truncated))
        }
        Ok((Err(error), _, _)) | Ok((_, Err(error), _)) => Err(format!("reading output: {error}")),
        Ok((_, _, Err(error))) => Err(format!("waiting for the command: {error}")),
        Err(_elapsed) => {
            // Interrupt, then kill the group.
            if let Some(pid) = child.id() {
                let pid = pid as i32;
                let _ = nix::sys::signal::kill(
                    nix::unistd::Pid::from_raw(-pid),
                    nix::sys::signal::SIGKILL,
                );
            }
            let _ = child.wait().await;
            Err(format!("timed out after {:?}", provider.timeout()))
        }
    }
}

/// Read a pipe, keeping at most `budget` bytes and reporting whether more arrived.
///
/// The pipe keeps being drained after the budget is reached: not reading would fill the
/// pipe buffer and block the command, turning a bounded read into a hang.
async fn read_bounded(
    pipe: Option<impl AsyncReadExt + Unpin>,
    budget: usize,
) -> std::io::Result<(String, bool)> {
    let Some(mut pipe) = pipe else {
        return Ok((String::new(), false));
    };
    let mut kept: Vec<u8> = Vec::with_capacity(budget.min(64 * 1024));
    let mut truncated = false;
    let mut buffer = vec![0u8; 8 * 1024];

    loop {
        let read = pipe.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        if kept.len() < budget {
            let room = budget - kept.len();
            let take = room.min(read);
            kept.extend_from_slice(&buffer[..take]);
            if take < read {
                truncated = true;
            }
        } else {
            truncated = true;
        }
    }
    // Truncated on a raw byte boundary, so multibyte text cannot exceed the budget.
    Ok((String::from_utf8_lossy(&kept).into_owned(), truncated))
}

/// The scratch directory a `cwd = "temp"` provider runs in.
#[must_use]
pub fn temp_cwd(state_dir: &Path, run_id: &str) -> PathBuf {
    state_dir.join("evidence-tmp").join(run_id)
}

/// How long a provider's termination grace is, when it has to be killed.
///
/// Providers are killed outright rather than interrupted: unlike a coding agent, a test
/// command has no in-flight edit worth letting finish.
pub const EVIDENCE_KILL_GRACE: Duration = Duration::ZERO;

#[cfg(test)]
mod tests {
    use super::{run_provider, temp_cwd};
    use aftman_core::evidence::{CommandProvider, ProviderCwd, RunWhen};
    use aftman_core::ids::EvidenceId;
    use std::time::Duration;
    use time::macros::datetime;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn provider(argv: &[&str]) -> CommandProvider {
        CommandProvider {
            id: EvidenceId::new("probe"),
            argv: argv.iter().map(|a| (*a).to_owned()).collect(),
            cwd: ProviderCwd::Repo,
            timeout_seconds: 10,
            max_output_bytes: 4096,
            run_when: RunWhen::WorkerCompleted,
            success_exit_codes: vec![0],
        }
    }

    fn now() -> time::OffsetDateTime {
        datetime!(2026-01-01 00:00:00 UTC)
    }

    #[tokio::test]
    async fn a_command_runs_and_its_output_is_captured() -> TestResult {
        let dir = tempfile::tempdir()?;
        let outcome = run_provider(
            &provider(&["sh", "-c", "echo hello"]),
            dir.path(),
            dir.path(),
            &[],
            now(),
        )
        .await;
        assert!(outcome.success);
        assert_eq!(outcome.exit_code, Some(0));
        assert!(
            outcome.output_excerpt.contains("hello"),
            "{}",
            outcome.output_excerpt
        );
        assert!(outcome.launch_error.is_none());
        assert_eq!(outcome.argv[0], "sh");
        Ok(())
    }

    #[tokio::test]
    async fn a_non_zero_exit_is_a_measurement_not_a_failure() -> TestResult {
        // The whole point: a failing test command has still run, and that is the
        // measurement the supervisor wanted.
        let dir = tempfile::tempdir()?;
        let outcome = run_provider(
            &provider(&["sh", "-c", "echo '1 failed'; exit 1"]),
            dir.path(),
            dir.path(),
            &[],
            now(),
        )
        .await;
        assert_eq!(outcome.exit_code, Some(1));
        assert!(!outcome.success, "exit 1 is not in the accepted codes");
        assert!(
            outcome.output_excerpt.contains("1 failed"),
            "the output must survive a non-zero exit"
        );
        assert!(
            outcome.launch_error.is_none(),
            "running and failing is not a launch error"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_declared_success_code_makes_exit_one_a_success() -> TestResult {
        let dir = tempfile::tempdir()?;
        let mut configured = provider(&["sh", "-c", "exit 1"]);
        configured.success_exit_codes = vec![0, 1];
        let outcome = run_provider(&configured, dir.path(), dir.path(), &[], now()).await;
        assert!(outcome.success);
        Ok(())
    }

    #[tokio::test]
    async fn stderr_is_kept_and_labelled() -> TestResult {
        let dir = tempfile::tempdir()?;
        let outcome = run_provider(
            &provider(&["sh", "-c", "echo out; echo err >&2"]),
            dir.path(),
            dir.path(),
            &[],
            now(),
        )
        .await;
        assert!(outcome.output_excerpt.contains("out"));
        assert!(outcome.output_excerpt.contains("--- stderr ---"));
        assert!(outcome.output_excerpt.contains("err"));
        Ok(())
    }

    #[tokio::test]
    async fn output_is_bounded_and_the_truncation_is_visible() -> TestResult {
        let dir = tempfile::tempdir()?;
        let mut configured = provider(&["sh", "-c", "yes | head -c 500000"]);
        configured.max_output_bytes = 1000;
        let outcome = run_provider(&configured, dir.path(), dir.path(), &[], now()).await;
        assert!(
            outcome.output_excerpt.len() <= 1000,
            "got {} bytes",
            outcome.output_excerpt.len()
        );
        assert!(outcome.truncated, "a clip must be reported, not silent");
        // The command still exited normally: the pipe kept being drained.
        assert_eq!(outcome.exit_code, Some(0));
        Ok(())
    }

    #[tokio::test]
    async fn a_timeout_is_reported_rather_than_hanging() -> TestResult {
        let dir = tempfile::tempdir()?;
        let mut configured = provider(&["sleep", "60"]);
        configured.timeout_seconds = 1;
        let started = tokio::time::Instant::now();
        let outcome = run_provider(&configured, dir.path(), dir.path(), &[], now()).await;
        assert!(outcome.launch_error.is_some(), "a timeout is reported");
        assert!(
            outcome.summary().contains("could not launch"),
            "{}",
            outcome.summary()
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the timeout must be enforced, not waited out"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_missing_command_is_a_launch_error() -> TestResult {
        let dir = tempfile::tempdir()?;
        let outcome = run_provider(
            &provider(&["/definitely/not/a/binary"]),
            dir.path(),
            dir.path(),
            &[],
            now(),
        )
        .await;
        assert!(outcome.launch_error.is_some());
        assert!(!outcome.success);
        assert_eq!(outcome.exit_code, None);
        Ok(())
    }

    #[tokio::test]
    async fn a_missing_working_directory_is_a_launch_error() -> TestResult {
        let outcome = run_provider(
            &provider(&["true"]),
            std::path::Path::new("/definitely/not/here"),
            std::path::Path::new("/tmp"),
            &[],
            now(),
        )
        .await;
        assert!(outcome.launch_error.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn the_environment_is_scrubbed() -> TestResult {
        let dir = tempfile::tempdir()?;
        let outcome = run_provider(
            &provider(&["sh", "-c", "echo \"key=${TYPESAFE_API_KEY:-absent}\""]),
            dir.path(),
            dir.path(),
            &[],
            now(),
        )
        .await;
        assert!(
            outcome.output_excerpt.contains("key=absent"),
            "the command saw {}",
            outcome.output_excerpt
        );
        Ok(())
    }

    #[tokio::test]
    async fn running_in_the_repository_uses_the_repository_directory() -> TestResult {
        let dir = tempfile::tempdir()?;
        let repo = dir.path().join("repo");
        std::fs::create_dir_all(&repo)?;
        let outcome = run_provider(&provider(&["pwd"]), &repo, dir.path(), &[], now()).await;
        let leaf = repo
            .file_name()
            .ok_or("a name")?
            .to_string_lossy()
            .into_owned();
        assert!(
            outcome.output_excerpt.contains(&leaf),
            "expected {leaf:?} in {:?}",
            outcome.output_excerpt
        );
        assert_eq!(outcome.cwd, repo);
        Ok(())
    }

    #[tokio::test]
    async fn running_in_a_temp_directory_does_not_touch_the_repository() -> TestResult {
        let dir = tempfile::tempdir()?;
        let repo = dir.path().join("repo");
        let scratch = dir.path().join("scratch");
        std::fs::create_dir_all(&repo)?;
        std::fs::create_dir_all(&scratch)?;

        let mut configured = provider(&["sh", "-c", "touch marker"]);
        configured.cwd = ProviderCwd::Temp;
        let outcome = run_provider(&configured, &repo, &scratch, &[], now()).await;
        assert!(outcome.success);
        assert!(
            scratch.join("marker").exists(),
            "it ran in the scratch directory"
        );
        assert!(
            !repo.join("marker").exists(),
            "the repository must be untouched"
        );
        Ok(())
    }

    #[test]
    fn the_temp_directory_is_derived_from_the_run() {
        let path = temp_cwd(std::path::Path::new("/state"), "run-1");
        assert!(path.starts_with("/state"));
        assert!(path.ends_with("run-1"));
        assert!(path.to_string_lossy().contains("evidence-tmp"));
    }
}
