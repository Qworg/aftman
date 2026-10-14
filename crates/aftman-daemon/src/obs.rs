//! Assembling bounded evidence from a run.
//!
//! Every field here has a byte or character cap, and the whole thing is designed so
//! that a repository of any size produces an observation of bounded size. This is
//! the only place repository state becomes model input, which is what makes both
//! the request budget and the injection surface auditable in one place.
//!
//! ## Simulation mode
//!
//! When the worker is [`WorkerKind::Simulated`] the builder produces **synthetic**
//! git and test evidence rather than reading the repository. Two reasons: a
//! simulated run must never dirty a user's working tree, and the runtime's own
//! tests must not depend on `git` being installed or on a fixture repository being
//! present. The synthesis is labelled in `evidence["simulated"]`, so a reader — or a
//! model — can tell that the diff did not come from a real tree.

use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::io::AsyncReadExt as _;
use tokio::process::Command;

use time::OffsetDateTime;

use aftman_core::job::{Job, WorkerKind, WorkerStatus};
use aftman_core::log::BoundedLog;
use aftman_core::observation::{
    Counters, EventSummary, GitEvidence, Observation, RepoInstructions, TestResult, TestSource,
    TestStatus, UntrackedExcerpt, cap_text, summarize_job, summarize_worker,
};
use aftman_core::run::RunState;
use aftman_core::worker::WorkerRecord;

/// How much of the job prompt is shown to a model.
pub const JOB_PROMPT_CAP: usize = 4_000;

/// Caps applied to the assembled observation.
#[derive(Debug, Clone, Copy)]
pub struct ObsCaps {
    pub diff_chars: usize,
    pub tail_chars: usize,
    pub events_history: usize,
    pub workers_history: usize,
    pub untracked_files: usize,
    pub untracked_bytes: usize,
    pub instruction_chars: usize,
}

impl Default for ObsCaps {
    fn default() -> Self {
        Self {
            diff_chars: 20_000,
            tail_chars: 12_000,
            events_history: 30,
            workers_history: 10,
            untracked_files: 3,
            untracked_bytes: 4_096,
            instruction_chars: 4_000,
        }
    }
}

/// Builds observations.
#[derive(Debug, Clone)]
pub struct ObservationBuilder {
    pub caps: ObsCaps,
    /// Produce synthetic evidence instead of reading the repository.
    pub simulation: bool,
    /// The program run as `git`. `"git"` (found on `PATH`) unless a test substitutes a
    /// fake one.
    pub git_program: std::ffi::OsString,
    /// The overall limit on assembling the repository evidence of one observation.
    pub timeout: Duration,
}

impl ObservationBuilder {
    #[must_use]
    pub fn new(simulation: bool) -> Self {
        Self {
            caps: ObsCaps::default(),
            simulation,
            git_program: "git".into(),
            timeout: OBSERVATION_TIMEOUT,
        }
    }

    #[must_use]
    pub fn for_worker(kind: WorkerKind) -> Self {
        Self::new(matches!(kind, WorkerKind::Simulated))
    }

    /// Assemble one observation.
    ///
    /// `events` are the recent persisted event summaries, already capped by the
    /// caller's window.
    ///
    /// Reading the repository (git, untracked files, `AGENTS.md`) is bounded and never
    /// blocks the caller's thread: git runs as a child process under a timeout, files
    /// are read on blocking threads, only if they are regular files, up to a byte cap,
    /// and the whole assembly shares one deadline ([`OBSERVATION_TIMEOUT`]). On
    /// timeout the repository evidence is omitted and a note says so under
    /// `evidence["observation_note"]`.
    pub async fn build(
        &self,
        state: &RunState,
        job: &Job,
        active: Option<&WorkerRecord>,
        event_summaries: Vec<EventSummary>,
        evidence: &[aftman_core::evidence::EvidenceOutcome],
        now: OffsetDateTime,
    ) -> Observation {
        let counters = Counters {
            elapsed_secs: elapsed_secs(state, now),
            ..state.counters.clone()
        };

        let active_summary = active.map(|w| summarize_worker(w, now));
        // History is the *other* workers. `active` already carries the most recent
        // one, whether it is still running or has finished.
        let active_id = active.map(|w| w.id);
        let history: Vec<_> = state
            .workers
            .iter()
            .filter(|w| Some(w.id) != active_id)
            .rev()
            .take(self.caps.workers_history)
            .map(|w| summarize_worker(w, now))
            .collect();

        let mut note = None;
        let (git, untracked, instructions, mut tests) = if self.simulation {
            (
                self.synthetic_git(active),
                Vec::new(),
                None,
                self.synthetic_tests(active),
            )
        } else {
            let tests = active.map_or_else(Vec::new, |w| parse_worker_tests(&w.output));
            match collect_repo(&job.repo, self.caps, self.timeout, &self.git_program).await {
                Ok(repo) => {
                    if let Some(why) = repo.note {
                        note = Some(why);
                    }
                    (repo.git, repo.untracked, repo.instructions, tests)
                }
                Err(why) => {
                    tracing::warn!(repo = %job.repo.display(), why, "repository evidence omitted");
                    note = Some(why);
                    (GitEvidence::default(), Vec::new(), None, tests)
                }
            }
        };

        // Command evidence arrives as *facts*, tagged so a check can tell them apart
        // from a worker's claim about the same thing.
        let mut evidence_map = std::collections::BTreeMap::new();
        for outcome in evidence {
            evidence_map.insert(outcome.evidence_key(), outcome.output_excerpt.clone());
            // A provider that printed test summaries is the measurement the
            // test-sufficiency check actually wants, rather than the agent's report of
            // having run something.
            tests.extend(parse_test_summaries(
                &outcome.output_excerpt,
                TestSource::Command {
                    provider: outcome.provider.to_string(),
                },
            ));
        }
        if let Some(why) = note {
            evidence_map.insert("observation_note".to_owned(), why);
        }
        if self.simulation {
            evidence_map.insert(
                "simulated".to_owned(),
                "synthetic diff and test results; the repository was not touched".to_owned(),
            );
        }

        Observation {
            run_id: state.header.id,
            status: state.status,
            job: summarize_job(job, counters.elapsed_secs, JOB_PROMPT_CAP),
            active: active_summary,
            history,
            git,
            untracked,
            instructions,
            tests,
            events: event_summaries
                .into_iter()
                .rev()
                .take(self.caps.events_history)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect(),
            prior: state
                .last_directive
                .as_ref()
                .map(|d| aftman_core::observation::PriorDecision {
                    directive: d.name().to_owned(),
                    reason: d.reason().unwrap_or_default().to_owned(),
                }),
            counters,
            evidence: evidence_map,
        }
    }

    /// A diff that only exists in simulation mode.
    fn synthetic_git(&self, active: Option<&WorkerRecord>) -> GitEvidence {
        let produced_output = active.is_some_and(|w| !w.output.is_empty());
        if !produced_output {
            return GitEvidence::default();
        }
        GitEvidence {
            status_short: " M src/http.rs\n?? tests/http_retry.rs".to_owned(),
            diff_excerpt: "+++ b/src/http.rs\n+ fn with_retry(req: Request) -> Request {\n+     req.retries(3)\n+ }\n"
                .to_owned(),
            diff_stat: " 1 file changed, 3 insertions(+)".to_owned(),
            changed_files: vec!["src/http.rs".to_owned(), "tests/http_retry.rs".to_owned()],
            head_sha: Some("0000000000000000000000000000000000000000".to_owned()),
            branch: Some("simulated".to_owned()),
            truncated: false,
        }
    }

    /// Test results derived from the simulated worker's own output.
    fn synthetic_tests(&self, active: Option<&WorkerRecord>) -> Vec<TestResult> {
        let Some(worker) = active else {
            return Vec::new();
        };
        let tail = worker.output.tail(self.caps.tail_chars).to_owned();
        let mut parsed = parse_test_summaries(&tail, TestSource::WorkerOutput);
        if parsed.is_empty() && worker.status == WorkerStatus::Completed {
            // The simulated worker's script may not have reached its summary line
            // yet; a completed simulation is a passing one by construction.
            parsed.push(TestResult {
                source: TestSource::WorkerOutput,
                passed: 3,
                failed: 0,
                errored: 0,
                skipped: 0,
                status: TestStatus::Passed,
                raw: "simulated: 3 passed".to_owned(),
            });
        }
        parsed
    }
}

/// Elapsed supervised time for a run: wall time since creation less any time spent
/// waiting for a human, because this is the figure policy compares to
/// `overall_timeout`.
fn elapsed_secs(state: &RunState, now: OffsetDateTime) -> u64 {
    state.supervised_elapsed(now).as_secs()
}

/// The hard limit on assembling the repository part of one observation.
pub const OBSERVATION_TIMEOUT: Duration = Duration::from_secs(8);

/// Most bytes of one git command's output that are kept; git is stopped past this.
pub const MAX_GIT_OUTPUT_BYTES: usize = 256 * 1024;

/// Repository evidence for one observation.
struct RepoEvidence {
    git: GitEvidence,
    untracked: Vec<UntrackedExcerpt>,
    instructions: Option<RepoInstructions>,
    /// Set when the code-executing configuration changed while the observation ran, in
    /// which case `git` and `untracked` have been discarded.
    note: Option<String>,
}

/// Collect everything repository-shaped, under one deadline. `Err` carries the note to
/// record when the deadline passes.
async fn collect_repo(
    repo: &Path,
    caps: ObsCaps,
    limit: Duration,
    program: &std::ffi::OsStr,
) -> Result<RepoEvidence, String> {
    let deadline = tokio::time::Instant::now() + limit;
    let work = async {
        let before = config_scan(repo, deadline, program).await;
        let mut git = git_evidence(repo, caps, deadline, program, &before.overrides).await;
        let mut untracked =
            untracked_excerpts(repo, caps, deadline, program, &before.overrides).await;
        let instructions = read_instructions(repo, caps.instruction_chars).await;
        // The worker runs concurrently and controls the configuration: if the set of
        // code-executing entries changed under us, a command above may have run something
        // the scan did not know to neutralize. Name-based overriding cannot close that
        // (git has no switch for it), so the cycle's git evidence is thrown away and the
        // fact is recorded.
        let after = config_scan(repo, deadline, program).await;
        let mut note = None;
        if before.exec_keys != after.exec_keys {
            let added: Vec<&String> = after
                .exec_keys
                .iter()
                .filter(|k| !before.exec_keys.contains(k))
                .collect();
            tracing::warn!(
                repo = %repo.display(),
                ?added,
                "git configuration that can run programs changed during the observation; \
                 discarding this cycle's git evidence"
            );
            note = Some(
                "the repository's git configuration (filters, diff drivers, includes, ...) \
                 changed during the observation; git and untracked-file evidence for this \
                 cycle was discarded"
                    .to_owned(),
            );
            git = GitEvidence::default();
            untracked = Vec::new();
        }
        RepoEvidence {
            git,
            untracked,
            instructions,
            note,
        }
    };
    tokio::time::timeout_at(deadline, work).await.map_err(|_| {
        format!(
            "repository observation exceeded {}s; git, untracked-file and AGENTS.md evidence \
             were omitted for this cycle",
            limit.as_secs()
        )
    })
}

/// Collect git state, degrading to an empty record rather than failing a cycle.
///
/// Every command has a bounded output budget and a deadline, and any failure is
/// reported as absent evidence rather than as an error.
async fn git_evidence(
    repo: &Path,
    caps: ObsCaps,
    deadline: tokio::time::Instant,
    program: &std::ffi::OsStr,
    config: &[(String, String)],
) -> GitEvidence {
    if !repo.is_dir() {
        return GitEvidence::default();
    }
    let git = |args: &'static [&'static str]| run_git(repo, args, config, deadline, program);
    // `--untracked-files=all` matters: a collapsed `dir/` entry hides the individual
    // new files a worker just wrote.
    let status_short = git(&[
        "status",
        "--short",
        "--untracked-files=all",
        "--ignore-submodules=all",
    ])
    .await
    .unwrap_or_default();
    let diff_stat = git(&[
        "diff",
        "--stat",
        "--no-ext-diff",
        "--no-textconv",
        "--ignore-submodules=all",
    ])
    .await
    .unwrap_or_default();
    let full_diff = git(&[
        "diff",
        "--no-ext-diff",
        "--no-textconv",
        "--ignore-submodules=all",
    ])
    .await
    .unwrap_or_default();
    let (diff_excerpt, truncated) = cap_text(&full_diff, caps.diff_chars);
    let changed_files = status_short
        .lines()
        .filter_map(|line| line.get(3..).map(str::trim))
        .filter(|path| !path.is_empty())
        .map(ToOwned::to_owned)
        .collect();

    GitEvidence {
        status_short,
        diff_excerpt,
        diff_stat,
        changed_files,
        head_sha: git(&["rev-parse", "HEAD"])
            .await
            .map(|s| s.trim().to_owned()),
        branch: git(&["branch", "--show-current"])
            .await
            .map(|s| s.trim().to_owned()),
        truncated,
    }
}

/// Configuration that is **forced** on every `git` the supervisor runs (the `-c` options
/// below have higher precedence than any config file), and why. The repository is
/// controlled by the worker, which can write `.git/config`, `.gitattributes`,
/// `.git/info/attributes`, and hooks; git reads all of them. A `git status` in a
/// worker-written repository must not be a way to run the worker's code with the
/// supervisor's privileges and credentials (hard rule 3), nor to escape a sandboxed
/// worker.
///
/// | Disabled | Why |
/// | --- | --- |
/// | `core.fsmonitor=false` | an fsmonitor hook/daemon is an arbitrary program run by `status`/`diff` |
/// | `core.hooksPath=/dev/null` | hooks (none run for these commands today; defence in depth) |
/// | `--no-ext-diff`, `--no-textconv` | `diff.external` and `diff.<driver>.textconv` are programs |
/// | `filter.<name>.{clean,smudge,process,required}` | clean filters run when git compares work tree to index. Every filter defined in the configuration *as it stands when the observation starts, in any scope* is overridden (`cat` / empty); a filter added *during* the observation is only detected (see `config_scan`) |
/// | `core.pager=cat`, `GIT_PAGER=cat`, `PAGER=cat` | a pager is a program |
/// | `core.sshCommand`, `core.gitProxy`, `core.askPass`, `credential.helper`, `GIT_ASKPASS`, `SSH_ASKPASS`, `GIT_TERMINAL_PROMPT=0` | transports and credential helpers (a lazy fetch would run them) |
/// | `GIT_NO_LAZY_FETCH=1` | a partial clone's promisor remote must not be contacted |
/// | `--no-optional-locks`, `GIT_OPTIONAL_LOCKS=0` | `status` must not write `index` behind the worker's back |
/// | `--ignore-submodules=all` | do not descend into submodules, whose configs are repository-controlled too |
/// | `GIT_CONFIG_GLOBAL=/dev/null`, `GIT_CONFIG_SYSTEM=/dev/null`, `GIT_CONFIG_NOSYSTEM=1` | user/system config is not the supervisor's to depend on (this also means a global `safe.directory` is ignored; a repository owned by another user is "dubious" and yields no evidence rather than being trusted) |
/// | every `GIT_*` variable of the daemon | `GIT_DIR`, `GIT_EXTERNAL_DIFF`, `GIT_SSH_COMMAND`, ... must not redirect or execute anything |
///
/// The environment of the child is `worker_environment()`, i.e. without
/// `TYPESAFE_*`/`AFTMAN_*` secrets, so whatever else a repository manages to run cannot
/// read them.
const GIT_FORCED_CONFIG: &[&str] = &[
    "core.fsmonitor=false",
    "core.hooksPath=/dev/null",
    "core.pager=cat",
    "core.sshCommand=/bin/false",
    "core.gitProxy=",
    "core.askPass=/bin/false",
    "credential.helper=",
    "protocol.ext.allow=never",
];

/// Build the (sanitized) command. Shared by the real run and the tests.
fn git_command(
    repo: &Path,
    args: &[&str],
    config: &[(String, String)],
    program: &std::ffi::OsStr,
) -> Command {
    git_command_with_base(repo, args, config, program, std::env::vars_os())
}

/// [`git_command`] over an explicit base environment (so the scrubbing is testable
/// without touching the test process's own environment).
fn git_command_with_base(
    repo: &Path,
    args: &[&str],
    config: &[(String, String)],
    program: &std::ffi::OsStr,
    base: impl IntoIterator<Item = (std::ffi::OsString, std::ffi::OsString)>,
) -> Command {
    let mut command = Command::new(program);
    command.env_clear();
    // Hard rule 3: the supervisor's credentials never reach a subprocess. Then drop every
    // GIT_* variable the daemon happened to inherit; ours are set below.
    // `build_worker_environment` is deliberately *not* used here. It narrows the
    // environment to a worker baseline, and the supervisor's own `git` needs the daemon's
    // environment to run at all — it is not an agent. What matters is the same in both
    // cases: no secret, and no `GIT_*` variable the daemon happened to inherit.
    command.envs(
        aftman_workers::build_supervisor_environment(
            base.into_iter()
                .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?))),
            &[],
        )
        .into_iter()
        .filter(|(key, _)| !key.starts_with("GIT_")),
    );
    command.envs([
        ("GIT_TERMINAL_PROMPT", "0"),
        ("GIT_ASKPASS", "/bin/false"),
        ("SSH_ASKPASS", "/bin/false"),
        ("GIT_PAGER", "cat"),
        ("PAGER", "cat"),
        ("GIT_OPTIONAL_LOCKS", "0"),
        ("GIT_NO_LAZY_FETCH", "1"),
        ("GIT_CONFIG_GLOBAL", "/dev/null"),
        ("GIT_CONFIG_SYSTEM", "/dev/null"),
        ("GIT_CONFIG_NOSYSTEM", "1"),
    ]);
    command.arg("--no-optional-locks");
    for forced in GIT_FORCED_CONFIG {
        command.arg("-c").arg(forced);
    }
    // Key/value pairs go through the environment (`GIT_CONFIG_COUNT`, ...), which has the
    // same precedence as `-c` but needs no parsing, so a key containing `=` or spaces
    // cannot be mis-split into a different key.
    if !config.is_empty() {
        command.env("GIT_CONFIG_COUNT", config.len().to_string());
        for (index, (key, value)) in config.iter().enumerate() {
            command.env(format!("GIT_CONFIG_KEY_{index}"), key);
            command.env(format!("GIT_CONFIG_VALUE_{index}"), value);
        }
    }
    command.arg("-C").arg(repo).args(args);
    command.stdin(std::process::Stdio::null());
    command.stdout(std::process::Stdio::piped());
    command.stderr(std::process::Stdio::null());
    command.kill_on_drop(true);
    command.process_group(0);
    command
}

/// What the repository's configuration holds that can run programs, as the hardened
/// commands will see it.
struct ConfigScan {
    /// Forced values for every name-keyed, code-executing entry found (`filter.<n>.*`).
    overrides: Vec<(String, String)>,
    /// Every key in the families that can execute code, sorted: compared before and after an
    /// observation to detect a change made while it ran.
    exec_keys: Vec<String>,
}

/// Whether a configuration key belongs to a family that can make git run a program or
/// change where it reads configuration from. Used to detect change, not to override.
fn is_exec_key(key: &str) -> bool {
    key.starts_with("filter.")
        || key.starts_with("diff.")
        || key.starts_with("include.")
        || key.starts_with("includeif.")
        || key.starts_with("extensions.")
        || matches!(
            key,
            "core.fsmonitor" | "core.hookspath" | "core.attributesfile" | "core.worktree"
        )
}

/// Enumerate the configuration and derive the overrides that neutralize the repository's
/// own `filter.<name>.*` drivers.
///
/// A clean filter named by `.gitattributes` or `.git/info/attributes` is run by `git
/// status`/`git diff` to compare the work tree with the index. There is no switch that
/// turns filters off, so each filter the configuration defines is overridden: `clean` and
/// `smudge` become `cat`, `process` becomes empty (an empty command is not run) and
/// `required` becomes false.
///
/// The list is taken from **exactly what the commands will read**: it is produced by the
/// same hardened invocation (same environment, same forced `-c` settings), without
/// `--local` or any scope restriction, with `--includes`, so every scope git consults is
/// covered: system and global (both `/dev/null` here), local, worktree-scoped
/// (`config.worktree`, when `extensions.worktreeConfig` is on), anything pulled in by
/// `include.path` and `includeIf.*.path` from any of them, and the command line. Names may
/// contain dots, spaces, and upper case; the list is NUL-separated (`-z`) so a name cannot
/// be split, and the overrides travel through the environment so a name cannot be mis-parsed.
/// Reading configuration executes nothing.
async fn config_scan(
    repo: &Path,
    deadline: tokio::time::Instant,
    program: &std::ffi::OsStr,
) -> ConfigScan {
    let listing = run_git(
        repo,
        &["config", "--list", "--includes", "--name-only", "-z"],
        &[],
        deadline,
        program,
    )
    .await
    .unwrap_or_default();
    let mut overrides = std::collections::BTreeMap::new();
    let mut exec_keys = Vec::new();
    for key in listing.split('\0').filter(|k| !k.is_empty()) {
        // `--name-only` prints section and variable lower-cased, subsection verbatim.
        if is_exec_key(key) {
            exec_keys.push(key.to_owned());
        }
        let Some(rest) = key.strip_prefix("filter.") else {
            continue;
        };
        let Some((_, field)) = rest.rsplit_once('.') else {
            continue;
        };
        let forced = match field {
            "clean" | "smudge" => "cat",
            "process" => "",
            "required" => "false",
            _ => continue,
        };
        overrides.insert(key.to_owned(), forced.to_owned());
    }
    exec_keys.sort();
    exec_keys.dedup();
    ConfigScan {
        overrides: overrides.into_iter().collect(),
        exec_keys,
    }
}

/// Kills a git process group when dropped while armed: a timed-out or abandoned git
/// (or something it spawned) must not outlive the observation that asked for it.
struct KillGroup {
    pgid: Option<i32>,
}

impl Drop for KillGroup {
    fn drop(&mut self) {
        if let Some(pgid) = self.pgid {
            let _ = nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(-pgid),
                nix::sys::signal::Signal::SIGKILL,
            );
        }
    }
}

/// Run one git command, returning `None` on any failure or when `deadline` passes.
///
/// Output is capped at [`MAX_GIT_OUTPUT_BYTES`] (git is killed past it, and what was read
/// is kept); the child is killed if the future is dropped, and its group on timeout.
async fn run_git(
    repo: &Path,
    args: &[&str],
    config: &[(String, String)],
    deadline: tokio::time::Instant,
    program: &std::ffi::OsStr,
) -> Option<String> {
    let mut child = git_command(repo, args, config, program).spawn().ok()?;
    let mut group = KillGroup {
        pgid: child.id().and_then(|pid| i32::try_from(pid).ok()),
    };
    let mut stdout = child.stdout.take()?;
    let work = async {
        let mut kept = Vec::new();
        let mut reader = (&mut stdout).take(MAX_GIT_OUTPUT_BYTES as u64 + 1);
        reader.read_to_end(&mut kept).await.ok()?;
        let overflowed = kept.len() > MAX_GIT_OUTPUT_BYTES;
        kept.truncate(MAX_GIT_OUTPUT_BYTES);
        if overflowed {
            // More than we will use: stop git rather than wait for it to finish.
            let _ = child.start_kill();
        }
        let status = child.wait().await.ok()?;
        // Reaped: the group id may be reused from now on, so it is never signalled again.
        group.pgid = None;
        (overflowed || status.success()).then_some(kept)
    };
    let kept = match tokio::time::timeout_at(deadline, work).await {
        Ok(kept) => kept?,
        Err(_) => return None, // kill_on_drop reaps the child
    };
    Some(String::from_utf8_lossy(&kept).into_owned())
}

/// Open `path` for reading only if it is a regular file, without ever blocking and
/// without following a symlink: `O_NONBLOCK` makes opening a FIFO return at once (it is
/// then rejected), `O_NOFOLLOW` rejects a symlink (a link to `/dev/zero`, or out of the
/// repository). The type is re-checked on the opened handle, so a swap between check and
/// open cannot slip through. Returns the file and its size.
fn open_regular(path: &Path) -> Option<(std::fs::File, u64)> {
    use nix::fcntl::OFlag;
    use std::os::unix::fs::OpenOptionsExt as _;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags((OFlag::O_NONBLOCK | OFlag::O_NOFOLLOW).bits())
        .open(path)
        .ok()?;
    let meta = file.metadata().ok()?;
    meta.is_file().then_some((file, meta.len()))
}

/// Read at most `cap` bytes of a regular file, on a blocking thread. `None` for anything
/// that is not a regular file. The size reported is the file's whole size.
async fn read_capped(path: PathBuf, cap: usize) -> Option<(Vec<u8>, u64)> {
    tokio::task::spawn_blocking(move || {
        use std::io::Read as _;
        let (file, size) = open_regular(&path)?;
        let mut bytes = Vec::new();
        file.take(cap as u64).read_to_end(&mut bytes).ok()?;
        Some((bytes, size))
    })
    .await
    .ok()
    .flatten()
}

/// Bounded excerpts of untracked text files.
///
/// `git diff` never shows untracked files, so a new test or source file written by a
/// worker is invisible to the supervisor without this.
async fn untracked_excerpts(
    repo: &Path,
    caps: ObsCaps,
    deadline: tokio::time::Instant,
    program: &std::ffi::OsStr,
    config: &[(String, String)],
) -> Vec<UntrackedExcerpt> {
    let status = run_git(
        repo,
        &[
            "status",
            "--short",
            "--untracked-files=all",
            "--ignore-submodules=all",
        ],
        config,
        deadline,
        program,
    )
    .await
    .unwrap_or_default();
    let mut budget = caps.untracked_bytes;
    let mut excerpts = Vec::new();

    for line in status.lines() {
        if excerpts.len() >= caps.untracked_files || budget == 0 {
            break;
        }
        if !line.starts_with("??") {
            continue;
        }
        let Some(relative) = line.get(3..).map(str::trim) else {
            continue;
        };
        if relative.is_empty() || !is_safe_to_read(relative) {
            continue;
        }
        // Read at most the budget, plus enough to see a NUL in the first 4 KiB.
        let Some((bytes, size)) = read_capped(repo.join(relative), budget.max(4096)).await else {
            continue;
        };
        // Binary-looking files are skipped: a NUL in the first 4 KiB is the same
        // heuristic `git` uses.
        if bytes.iter().take(4096).any(|b| *b == 0) {
            continue;
        }
        let take = bytes.len().min(budget);
        let truncated = size > take as u64;
        // Truncate on a raw byte boundary *before* decoding, so multibyte text can
        // never exceed the byte budget.
        let content = String::from_utf8_lossy(&bytes[..take]).into_owned();
        budget = budget.saturating_sub(take);
        excerpts.push(UntrackedExcerpt {
            path: relative.to_owned(),
            bytes: usize::try_from(size).unwrap_or(usize::MAX),
            content,
            truncated,
        });
    }
    excerpts
}

/// Names that must never be read into an observation.
fn is_safe_to_read(relative: &str) -> bool {
    let lower = relative.to_ascii_lowercase();
    let name = lower.rsplit('/').next().unwrap_or(&lower);
    const DENY_SUBSTRINGS: &[&str] = &["secret", "token", "credential"];
    const DENY_SUFFIXES: &[&str] = &[".pem", ".key", ".p12", ".keystore"];
    if lower.starts_with("..") || lower.contains("/../") {
        return false;
    }
    if name == ".env" || name.starts_with(".env.") {
        return false;
    }
    if name == "id_rsa" || name == "id_ed25519" || name == "id_ecdsa" {
        return false;
    }
    if DENY_SUBSTRINGS.iter().any(|s| name.contains(s)) {
        return false;
    }
    !DENY_SUFFIXES.iter().any(|s| name.ends_with(s))
}

/// Read repository-root instructions.
///
/// Transient: the contents are included in a request and never persisted, because
/// repository prose must not land in the run log. Only a regular file is read (a FIFO or
/// a symlink named `AGENTS.md` is skipped), and at most `4 * cap_chars + 4` bytes of it.
async fn read_instructions(repo: &Path, cap_chars: usize) -> Option<RepoInstructions> {
    for file in ["AGENTS.override.md", "AGENTS.md"] {
        let byte_cap = cap_chars.saturating_mul(4).saturating_add(4);
        let Some((bytes, size)) = read_capped(repo.join(file), byte_cap).await else {
            continue;
        };
        let text = String::from_utf8_lossy(&bytes);
        let (excerpt, truncated) = cap_text(&text, cap_chars);
        return Some(RepoInstructions {
            file: file.to_owned(),
            excerpt,
            truncated: truncated || size > bytes.len() as u64,
        });
    }
    None
}

/// Parse test summaries out of a worker's own captured output.
///
/// This is the "claim" path: the text is whatever the agent chose to print. A
/// command evidence result is tagged differently, and the checks are instructed to
/// weight the two differently.
#[must_use]
pub fn parse_worker_tests(log: &BoundedLog) -> Vec<TestResult> {
    let mut text = String::new();
    text.push_str(log.head(8_000));
    text.push('\n');
    text.push_str(log.tail(8_000));
    parse_test_summaries(&text, TestSource::WorkerOutput)
}

/// Parse pytest-style summary lines.
///
/// Handles `passed`-first and failure-first orderings, the `N error`/`N errors`
/// forms, skipped counts, optional `=== … ===` wrappers, and a trailing `in Ns`.
/// Lines without a recognized count are not summaries — `2 warnings in 0.12s` is
/// explicitly *not* a test result, which is a real false positive to avoid.
#[must_use]
pub fn parse_test_summaries(text: &str, source: TestSource) -> Vec<TestResult> {
    let mut results = Vec::new();
    for raw in text.lines() {
        let line = raw.trim().trim_matches('=').trim();
        if line.is_empty() {
            continue;
        }
        let mut passed = 0;
        let mut failed = 0;
        let mut errored = 0;
        let mut skipped = 0;
        let mut recognized = false;

        let tokens: Vec<&str> = line.split_whitespace().collect();
        for pair in tokens.windows(2) {
            let Ok(count) = pair[0].parse::<u32>() else {
                continue;
            };
            let label = pair[1].trim_end_matches(',');
            match label {
                "passed" => {
                    passed = count;
                    recognized = true;
                }
                "failed" | "failure" | "failures" => {
                    failed = count;
                    recognized = true;
                }
                "error" | "errors" => {
                    errored = count;
                    recognized = true;
                }
                "skipped" | "xfailed" | "xpassed" => {
                    skipped += count;
                    recognized = true;
                }
                _ => {}
            }
        }
        if !recognized {
            continue;
        }

        let status = match (failed > 0, errored > 0) {
            (true, true) => TestStatus::Mixed,
            (true, false) => TestStatus::Failed,
            (false, true) => TestStatus::Errored,
            (false, false) if passed > 0 => TestStatus::Passed,
            (false, false) => TestStatus::Unknown,
        };

        results.push(TestResult {
            source: source.clone(),
            passed,
            failed,
            errored,
            skipped,
            status,
            raw: raw.trim().to_owned(),
        });
    }
    results
}

#[cfg(test)]
mod tests {
    use super::{is_safe_to_read, parse_test_summaries};
    use aftman_core::observation::{TestSource, TestStatus};

    fn parse(text: &str) -> Vec<aftman_core::observation::TestResult> {
        parse_test_summaries(text, TestSource::WorkerOutput)
    }

    #[test]
    fn a_passing_summary_is_recognized() {
        let results = parse("3 passed in 0.12s");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].passed, 3);
        assert_eq!(results[0].status, TestStatus::Passed);
    }

    #[test]
    fn failure_first_and_passed_first_both_parse() {
        let a = parse("1 failed, 3 passed in 0.4s");
        assert_eq!(a.len(), 1);
        assert_eq!((a[0].failed, a[0].passed), (1, 3));
        assert_eq!(a[0].status, TestStatus::Failed);

        let b = parse("3 passed, 1 failed");
        assert_eq!((b[0].passed, b[0].failed), (3, 1));
        assert_eq!(b[0].status, TestStatus::Failed);
    }

    #[test]
    fn error_forms_are_handled() {
        for text in ["2 errors in 1.0s", "2 error in 1.0s"] {
            let results = parse(text);
            assert_eq!(results[0].errored, 2, "{text}");
            assert_eq!(results[0].status, TestStatus::Errored, "{text}");
        }
    }

    #[test]
    fn decorated_summary_lines_parse() {
        let results = parse("=== 4 passed, 1 skipped in 0.88s ===");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].passed, 4);
        assert_eq!(results[0].skipped, 1);
        assert_eq!(results[0].status, TestStatus::Passed);
    }

    #[test]
    fn a_warning_line_is_not_a_test_summary() {
        // The documented false positive: this must not read as a test result.
        assert!(parse("2 warnings in 0.12s").is_empty());
    }

    #[test]
    fn unrelated_output_is_not_a_summary() {
        for text in [
            "editing src/http.rs",
            "3 files changed",
            "",
            "running the test suite",
        ] {
            assert!(parse(text).is_empty(), "{text:?} must not parse");
        }
    }

    #[test]
    fn multiple_summaries_are_all_reported() {
        let results = parse("3 passed in 0.1s\nnow running integration\n1 failed, 2 passed");
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].status, TestStatus::Passed);
        assert_eq!(results[1].status, TestStatus::Failed);
    }

    #[test]
    fn sensitive_paths_are_never_read() {
        for path in [
            ".env",
            ".env.local",
            "config/.env",
            "id_rsa",
            "certs/server.pem",
            "deploy.key",
            "src/secrets.rs",
            "lib/token_store.rs",
            "../outside.txt",
            "a/../../outside.txt",
        ] {
            assert!(!is_safe_to_read(path), "{path} must not be read");
        }
    }

    #[test]
    fn ordinary_paths_are_readable() {
        for path in [
            "src/http.rs",
            "tests/http_retry.rs",
            "README.md",
            "docs/notes.md",
        ] {
            assert!(is_safe_to_read(path), "{path} should be readable");
        }
    }
}
