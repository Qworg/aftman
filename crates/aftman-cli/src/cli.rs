//! The command line, as clap sees it, and the client-side preparation of `run`.

use std::path::PathBuf;

use clap::{ArgGroup, Parser, Subcommand};

use aftman_core::assessment::HumanDecision;
use aftman_core::job::FactoryStatus;
use aftman_daemon::protocol::{MAX_JOB_BYTES, Request, RunStartParams};

use crate::error::CliError;

/// The environment variable that acknowledges `--permissions auto-approve`.
pub const ACK_UNSANDBOXED: &str = "AFTMAN_ACK_UNSANDBOXED";

#[derive(Parser, Debug)]
#[command(
    name = "aftman",
    version,
    about = "Supervise a coding agent: a fast decision model above slower agents",
    long_about = None,
)]
pub struct Cli {
    /// Where runs are stored. Defaults to $XDG_STATE_HOME/aftman.
    ///
    /// This is a property of the daemon: a client talking to a running daemon uses the
    /// daemon's store, whatever this says. It is what an auto-started daemon is given,
    /// and what `--no-daemon` and `usage` read.
    #[arg(long, global = true, env = "AFTMAN_DATA_DIR")]
    pub data_dir: Option<PathBuf>,

    /// The daemon's socket. Defaults to $XDG_RUNTIME_DIR/aftman/aftman.sock, else
    /// ~/.aftman/aftman.sock.
    #[arg(long, global = true, env = "AFTMAN_SOCKET", value_name = "PATH")]
    pub socket: Option<PathBuf>,

    /// Suppress per-event output.
    #[arg(long, global = true)]
    pub quiet: bool,

    /// Do not use a daemon: embed the runtime in this process (run), or read the
    /// store directly (ls, show, decisions, logs).
    #[arg(long, global = true)]
    pub no_daemon: bool,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Supervise a coding worker on one job.
    Run(RunArgs),
    /// Run a fully offline demonstration with a simulated worker and model.
    Demo(DemoArgs),
    /// List runs.
    Ls {
        /// Only these statuses (comma-separated): running, finished, needs-human, ...
        #[arg(long, value_delimiter = ',')]
        status: Vec<String>,
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    /// Show one run's summary, decision timeline, and open escalations.
    Show(RunTarget),
    /// Show every proposal, suppression, and invariant for a run.
    Decisions(RunTarget),
    /// Print a run's worker output.
    Logs {
        #[command(flatten)]
        target: RunTarget,
        /// Keep printing until the run ends.
        #[arg(short = 'f', long)]
        follow: bool,
    },
    /// Send guidance to a run's worker.
    Steer {
        /// The run: an id or an unambiguous prefix.
        run: String,
        message: String,
    },
    /// Cancel a live run.
    Cancel(RunTarget),
    /// Answer a run's open escalation.
    Answer(AnswerArgs),
    /// Delete a run's data, or all of it.
    Purge {
        /// The run to delete: an id or an unambiguous prefix.
        #[arg(conflicts_with = "all", required_unless_present = "all")]
        run: Option<String>,
        /// Delete every run that is not live.
        #[arg(long)]
        all: bool,
    },
    /// The responsibilities that judge a run.
    Responsibilities {
        #[command(subcommand)]
        action: ResponsibilitiesAction,
    },
    /// Show decision-model token usage.
    Usage {
        /// Only requests since then: a duration (7d, 12h, 30m) or an RFC 3339 time.
        #[arg(long)]
        since: Option<String>,
        /// Restrict to one run.
        #[arg(long)]
        run_id: Option<String>,
    },
    /// Report the resolved configuration and what is available.
    Doctor {
        /// Ask the decision model one question to prove connectivity. Costs tokens.
        #[arg(long)]
        probe: bool,
    },
    /// Run the daemon in the foreground.
    Serve {
        /// Internal: how an auto-started daemon is launched. Leaves the terminal's
        /// session before doing anything else.
        #[arg(long, hide = true)]
        detached: bool,
    },
    /// Ask the daemon to shut down, and wait until it has.
    Stop,
    /// Report whether the daemon is running. Exits 1 when it is not.
    Status,
}

#[derive(Subcommand, Debug)]
pub enum ResponsibilitiesAction {
    /// List the active responsibilities and their checks.
    List,
    /// Show one responsibility's definition.
    Show { id: String },
}

/// Which run a command is about.
#[derive(Parser, Debug, Clone)]
pub struct RunTarget {
    /// The run: an id or an unambiguous prefix.
    #[arg(conflicts_with = "last")]
    pub run: Option<String>,
    /// The newest run.
    #[arg(long)]
    pub last: bool,
}

#[derive(Parser, Debug, Clone)]
#[command(group(ArgGroup::new("decision").multiple(false)))]
pub struct AnswerArgs {
    /// The run: an id or an unambiguous prefix.
    pub run: String,
    /// The escalation id (printed by `show` and by `run`).
    pub escalation_id: String,
    /// Guidance for the worker. With a decision flag, a note recorded with the answer.
    pub text: Option<String>,
    /// Accept the uncertainty and continue.
    #[arg(long, group = "decision")]
    pub accept: bool,
    /// Look again with fresh evidence.
    #[arg(long, group = "decision")]
    pub recheck: bool,
    /// Stop the run.
    #[arg(long, group = "decision")]
    pub stop: bool,
    /// Claim the job is complete; evidence must still agree.
    #[arg(long, group = "decision")]
    pub claim_complete: bool,
}

impl AnswerArgs {
    /// The decision and note this command line asks for.
    ///
    /// Text alone is guidance for the worker. Text with a flag is a note recorded
    /// with that decision.
    ///
    /// # Errors
    /// No flag and no text, or a blank guidance text.
    pub fn decision(&self) -> Result<(HumanDecision, String), CliError> {
        let note = self.text.clone().unwrap_or_default();
        let flagged = if self.accept {
            Some(HumanDecision::Accept)
        } else if self.recheck {
            Some(HumanDecision::Recheck)
        } else if self.stop {
            Some(HumanDecision::Stop)
        } else if self.claim_complete {
            Some(HumanDecision::ClaimComplete)
        } else {
            None
        };
        match flagged {
            Some(decision) => Ok((decision, note)),
            None if note.trim().is_empty() => Err(CliError::usage(
                "say what to do: guidance text, or one of --accept --recheck --stop --claim-complete",
            )),
            None => Ok((HumanDecision::Guidance { text: note }, String::new())),
        }
    }
}

#[derive(Parser, Debug, Clone)]
pub struct RunArgs {
    /// Repository to work in.
    #[arg(long)]
    pub repo: PathBuf,

    /// The job, in free form: a ticket, a spec, a bug report, a request.
    #[arg(long, conflicts_with = "job_file")]
    pub job: Option<String>,

    /// Read the job from a file.
    #[arg(long, value_name = "PATH", conflicts_with = "job")]
    pub job_file: Option<PathBuf>,

    /// Which coding agent performs the work.
    #[arg(long, default_value = "simulated")]
    pub worker: String,

    /// argv template for `--worker generic`. Placeholders: {mission_file}, {repo}, {model}.
    #[arg(long)]
    pub cmd: Option<String>,

    /// Permission posture: sandboxed (default), auto-approve, or interactive (refused
    /// where no human is attached to the worker, which is everywhere but herdr).
    #[arg(long, default_value = "sandboxed")]
    pub permissions: String,

    /// Reasoning effort, where the backend accepts one.
    #[arg(long)]
    pub effort: Option<String>,

    /// Prefer a live-input mode where the backend has one (currently only pi).
    #[arg(long)]
    pub steer: bool,

    /// Assert that isolation is provided by a container, VM, or throwaway checkout.
    ///
    /// This is what makes `sandboxed` honest for a backend that has no sandbox flag.
    #[arg(long)]
    pub external_sandbox: bool,

    /// Model to request from the provider.
    #[arg(long)]
    pub model: Option<String>,

    /// Independent verification policy.
    #[arg(long, value_parser = ["never", "when-asked", "always"], default_value = "when-asked")]
    pub verify: String,

    /// Enable the uncertainty band and the second-opinion path.
    #[arg(long)]
    pub ensemble: bool,

    /// Use the offline simulated decision model instead of the real provider.
    ///
    /// Without this, a run requires a configured provider so that supervision is
    /// real. A run that silently fell back to a simulation would look supervised
    /// when it was not.
    #[arg(long)]
    pub simulate_model: bool,
}

#[derive(Parser, Debug, Clone)]
pub struct DemoArgs {
    /// Repository the simulated worker operates on. Not modified.
    #[arg(long, default_value = ".")]
    pub repo: PathBuf,

    /// The job text to demonstrate.
    #[arg(
        long,
        default_value = "Add request retries with backoff, and cover them with tests."
    )]
    pub job: String,
}

/// Parse a `--status` value (`needs-human`, `running`, ...).
///
/// # Errors
/// An unknown status name.
pub fn parse_status(text: &str) -> Result<FactoryStatus, CliError> {
    let snake = text.trim().to_ascii_lowercase().replace('-', "_");
    serde_json::from_value(serde_json::Value::String(snake.clone())).map_err(|_| {
        CliError::usage(format!(
            "unknown status `{text}` (queued, routing, running, assessing, stopped, \
             verifying, needs-human, finished, failed, cancelled)"
        ))
    })
}

/// The environment lookup `run` validation uses, as a value so tests need no real
/// environment variables.
pub trait Env {
    fn var_is_set(&self, name: &str) -> bool;
}

/// The process environment.
pub struct ProcessEnv;

impl Env for ProcessEnv {
    fn var_is_set(&self, name: &str) -> bool {
        std::env::var_os(name).is_some()
    }
}

impl RunArgs {
    /// Everything the **client** must do before a run can be sent anywhere: the
    /// daemon's working directory, terminal and environment are not ours.
    ///
    /// - the repository is canonicalized to an absolute path;
    /// - `--job-file` is read here;
    /// - `--permissions interactive` is refused (no human is attached to a worker the
    ///   daemon or this process runs; docs/04, docs/08 section 3.1);
    /// - `--permissions auto-approve` requires the acknowledgement in *this*
    ///   environment. The daemon's environment is whichever shell happened to start
    ///   it, so checking there would test the wrong person's intent.
    ///
    /// The socket is same-user-only (docs/08 section 5), so the acknowledgement is an
    /// operator guard against a slip, not a defence against another principal.
    ///
    /// # Errors
    /// [`CliError::Usage`] for each of the above.
    pub fn prepare(&self, env: &dyn Env) -> Result<RunStartParams, CliError> {
        let job = match (&self.job, &self.job_file) {
            (Some(job), _) => job.clone(),
            (None, Some(path)) => read_job_file(path)?,
            (None, None) => return Err(CliError::usage("provide --job or --job-file")),
        };
        let repo = self.repo.canonicalize().map_err(|error| {
            CliError::usage(format!(
                "resolving repository {}: {error}",
                self.repo.display()
            ))
        })?;
        if !repo.is_dir() {
            return Err(CliError::usage(format!(
                "{} is not a directory",
                repo.display()
            )));
        }
        match self.permissions.as_str() {
            "sandboxed" => {}
            "auto-approve" => {
                if !env.var_is_set(ACK_UNSANDBOXED) {
                    return Err(CliError::usage(format!(
                        "--permissions auto-approve suppresses the agent's approval prompts. \
                         Set {ACK_UNSANDBOXED}=1 to acknowledge that, or use the default \
                         `sandboxed` posture."
                    )));
                }
            }
            "interactive" => {
                return Err(CliError::usage(
                    "--permissions interactive needs a human attached to the worker, and \
                     none is attached to a job run from the command line: it is available \
                     only on the herdr surface. Use `sandboxed` or `auto-approve`.",
                ));
            }
            other => {
                return Err(CliError::usage(format!(
                    "unknown permission posture `{other}` (sandboxed, auto-approve, interactive)"
                )));
            }
        }
        let params = RunStartParams {
            repo,
            job,
            worker: Some(self.worker.clone()),
            cmd: self.cmd.clone(),
            permissions: Some(self.permissions.clone()),
            effort: self.effort.clone(),
            steer: self.steer,
            external_sandbox: self.external_sandbox,
            model: self.model.clone(),
            verify: Some(self.verify.clone()),
            ensemble: self.ensemble,
            simulate_model: self.simulate_model,
        };
        // The same validation the daemon applies, so the embedded path cannot accept
        // what the daemon would refuse (an empty job, an oversized one).
        Request::RunStart(params.clone()).validate()?;
        Ok(params)
    }
}

/// Read a job file, but never more than the protocol's job limit plus one byte: a
/// `/dev/zero` or a 2 GB file must be a size error, not an out-of-memory.
fn read_job_file(path: &std::path::Path) -> Result<String, CliError> {
    use std::io::Read as _;
    let unreadable = |error: std::io::Error| {
        CliError::usage(format!("reading job file {}: {error}", path.display()))
    };
    let file = std::fs::File::open(path).map_err(unreadable)?;
    let mut bytes = Vec::new();
    file.take(MAX_JOB_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(unreadable)?;
    if bytes.len() > MAX_JOB_BYTES {
        return Err(CliError::usage(format!(
            "job file {} is larger than the {MAX_JOB_BYTES}-byte limit",
            path.display()
        )));
    }
    String::from_utf8(bytes)
        .map_err(|_| CliError::usage(format!("job file {} is not valid UTF-8", path.display())))
}

impl DemoArgs {
    /// The demo is a `run` with the simulated worker and model.
    #[must_use]
    pub fn as_run_args(&self) -> RunArgs {
        RunArgs {
            repo: self.repo.clone(),
            job: Some(self.job.clone()),
            job_file: None,
            worker: "simulated".to_owned(),
            cmd: None,
            permissions: "sandboxed".to_owned(),
            effort: None,
            steer: false,
            external_sandbox: false,
            model: None,
            verify: "when-asked".to_owned(),
            ensemble: false,
            simulate_model: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    struct FakeEnv(HashSet<&'static str>);

    impl Env for FakeEnv {
        fn var_is_set(&self, name: &str) -> bool {
            self.0.contains(name)
        }
    }

    fn args(repo: &std::path::Path) -> RunArgs {
        RunArgs {
            repo: repo.to_owned(),
            job: Some("do the thing".into()),
            ..DemoArgs {
                repo: repo.to_owned(),
                job: String::new(),
            }
            .as_run_args()
        }
    }

    fn answer(accept: bool, stop: bool, text: Option<&str>) -> AnswerArgs {
        AnswerArgs {
            run: "abcd".into(),
            escalation_id: "e".into(),
            text: text.map(ToOwned::to_owned),
            accept,
            recheck: false,
            stop,
            claim_complete: false,
        }
    }

    #[test]
    fn answer_text_alone_is_guidance() {
        let (decision, note) = answer(false, false, Some("use staging"))
            .decision()
            .unwrap();
        assert_eq!(
            decision,
            HumanDecision::Guidance {
                text: "use staging".into()
            }
        );
        assert_eq!(note, "");
    }

    #[test]
    fn answer_text_with_a_flag_is_a_note() {
        let (decision, note) = answer(true, false, Some("looks fine to me"))
            .decision()
            .unwrap();
        assert_eq!(decision, HumanDecision::Accept);
        assert_eq!(note, "looks fine to me");
        let (decision, note) = answer(false, true, None).decision().unwrap();
        assert_eq!(decision, HumanDecision::Stop);
        assert_eq!(note, "");
    }

    #[test]
    fn every_flag_maps_to_its_decision() {
        let mut a = answer(false, false, None);
        a.recheck = true;
        assert_eq!(a.decision().unwrap().0, HumanDecision::Recheck);
        let mut a = answer(false, false, None);
        a.claim_complete = true;
        assert_eq!(a.decision().unwrap().0, HumanDecision::ClaimComplete);
    }

    #[test]
    fn answer_with_nothing_to_say_is_a_usage_error() {
        assert!(answer(false, false, None).decision().is_err());
        assert!(answer(false, false, Some("   ")).decision().is_err());
    }

    #[test]
    fn at_most_one_decision_flag_parses() {
        assert!(
            Cli::try_parse_from(["aftman", "answer", "abcd", "e1", "--accept", "--stop"]).is_err()
        );
        assert!(Cli::try_parse_from(["aftman", "answer", "abcd", "e1", "--accept"]).is_ok());
        assert!(Cli::try_parse_from(["aftman", "answer", "abcd", "e1", "go left"]).is_ok());
    }

    #[test]
    fn the_repo_is_canonicalized_and_the_job_file_is_read() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("repo");
        std::fs::create_dir(&repo).unwrap();
        std::fs::write(dir.path().join("job.txt"), "from a file").unwrap();
        let mut a = args(&dir.path().join("repo/../repo"));
        a.job = None;
        a.job_file = Some(dir.path().join("job.txt"));
        let params = a.prepare(&FakeEnv(HashSet::new())).unwrap();
        assert_eq!(params.job, "from a file");
        assert!(params.repo.is_absolute());
        assert!(!params.repo.to_string_lossy().contains(".."));
        assert_eq!(params.repo, repo.canonicalize().unwrap());
    }

    #[test]
    fn an_empty_or_oversized_job_is_refused_by_the_same_validation_as_the_daemon() {
        let dir = tempfile::tempdir().unwrap();
        let none = FakeEnv(HashSet::new());
        let mut a = args(dir.path());
        a.job = Some("   ".into());
        assert_eq!(a.prepare(&none).unwrap_err().exit_code(), 2);
        a.job = Some("x".repeat(MAX_JOB_BYTES + 1));
        assert_eq!(a.prepare(&none).unwrap_err().exit_code(), 2);
        // A file is read only up to the limit, so a huge one is a size error.
        let big = dir.path().join("big.txt");
        std::fs::write(&big, vec![b'a'; MAX_JOB_BYTES + 10]).unwrap();
        a.job = None;
        a.job_file = Some(big);
        let error = a.prepare(&none).unwrap_err();
        assert!(error.to_string().contains("limit"), "{error}");
        assert_eq!(error.exit_code(), 2);
        let ok = dir.path().join("ok.txt");
        std::fs::write(&ok, "x".repeat(MAX_JOB_BYTES)).unwrap();
        a.job_file = Some(ok);
        assert!(a.prepare(&none).is_ok());
    }

    #[test]
    fn a_missing_repo_or_job_is_a_usage_error() {
        let dir = tempfile::tempdir().unwrap();
        let missing = args(&dir.path().join("nope"));
        assert_eq!(
            missing
                .prepare(&FakeEnv(HashSet::new()))
                .unwrap_err()
                .exit_code(),
            2
        );
        let mut a = args(dir.path());
        a.job = None;
        assert!(a.prepare(&FakeEnv(HashSet::new())).is_err());
        let file = dir.path().join("a-file");
        std::fs::write(&file, "x").unwrap();
        assert!(args(&file).prepare(&FakeEnv(HashSet::new())).is_err());
    }

    #[test]
    fn auto_approve_needs_the_acknowledgement_in_the_clients_environment() {
        let dir = tempfile::tempdir().unwrap();
        let mut a = args(dir.path());
        a.permissions = "auto-approve".into();
        let refused = a.prepare(&FakeEnv(HashSet::new())).unwrap_err();
        assert!(refused.to_string().contains(ACK_UNSANDBOXED));
        assert_eq!(refused.exit_code(), 2);
        let acked = FakeEnv(HashSet::from([ACK_UNSANDBOXED]));
        assert_eq!(
            a.prepare(&acked).unwrap().permissions.as_deref(),
            Some("auto-approve")
        );
    }

    #[test]
    fn interactive_and_unknown_postures_are_refused_client_side() {
        let dir = tempfile::tempdir().unwrap();
        let mut a = args(dir.path());
        a.permissions = "interactive".into();
        let refused = a.prepare(&FakeEnv(HashSet::new())).unwrap_err();
        assert!(refused.to_string().contains("herdr"), "{refused}");
        a.permissions = "yolo".into();
        assert!(a.prepare(&FakeEnv(HashSet::new())).is_err());
    }

    #[test]
    fn statuses_parse_in_kebab_case() {
        assert_eq!(
            parse_status("needs-human").unwrap(),
            FactoryStatus::NeedsHuman
        );
        assert_eq!(parse_status("Running").unwrap(), FactoryStatus::Running);
        assert!(parse_status("sleeping").is_err());
    }
}
