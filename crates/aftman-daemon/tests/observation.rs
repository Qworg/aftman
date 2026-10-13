//! The supervisor's own repository reads: they must not run repository-controlled code
//! (hard rule 3), must not carry supervisor credentials, and must never block or hang
//! the run loop. Uses a real `git` in temp repositories.

mod common;

use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use time::OffsetDateTime;

use aftman_core::clock::SystemClock;
use aftman_core::evidence::EvidencePlan;
use aftman_core::job::{FactoryStatus, Job, JobSource, RunConfig, WorkerKind};
use aftman_core::observation::Observation;
use aftman_core::responsibility::ResponsibilityRegistry;
use aftman_core::run::{RunHeader, RunState};
use aftman_daemon::{FactoryRuntime, NullSink, ObservationBuilder, RunControl, RuntimeDeps};
use aftman_store::{FileRunStore, RunStore};
use aftman_workers::{CommandSpec, SteerProtocol, SubprocessWorker};
use common::{DialModel, Mode, TestResult, short_tempdir, wait_until};

fn have_git() -> bool {
    Command::new("git")
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success())
}

fn real_git() -> PathBuf {
    let out = Command::new("sh")
        .args(["-c", "command -v git"])
        .output()
        .expect("sh");
    PathBuf::from(String::from_utf8_lossy(&out.stdout).trim())
}

/// A plain git (no hardening) for setting up repositories and for the control runs.
fn git(repo: &Path, args: &[&str]) -> std::process::Output {
    Command::new("git")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .args(["-C"])
        .arg(repo)
        .args(args)
        .output()
        .expect("git")
}

fn make_repo(dir: &Path) -> PathBuf {
    let repo = dir.join("repo");
    std::fs::create_dir_all(&repo).expect("mkdir");
    git(&repo, &["init", "-q"]);
    git(&repo, &["config", "user.email", "t@example.com"]);
    git(&repo, &["config", "user.name", "t"]);
    std::fs::write(repo.join("a.txt"), "one\n").expect("write");
    std::fs::write(repo.join("b.rs"), "fn main() {}\n").expect("write");
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-q", "-m", "init"]);
    repo
}

fn script(dir: &Path, name: &str, body: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).expect("script");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    path
}

fn append(path: &Path, text: &str) {
    use std::io::Write as _;
    std::fs::OpenOptions::new()
        .append(true)
        .open(path)
        .expect("open")
        .write_all(text.as_bytes())
        .expect("append");
}

fn state_and_job(repo: &Path) -> (RunState, Job) {
    let job = Job {
        prompt: "do it".into(),
        repo: repo.to_path_buf(),
        source: JobSource::Cli,
    };
    let header = RunHeader {
        id: aftman_core::ids::RunId::new(),
        created_at: OffsetDateTime::now_utc(),
        job: job.clone(),
        config: RunConfig::default(),
        aftman_version: "0".into(),
        worker_versions: Default::default(),
        granted_key_names: Vec::new(),
    };
    (RunState::new(header, FactoryStatus::Running), job)
}

async fn observe(builder: &ObservationBuilder, repo: &Path) -> Observation {
    let (state, job) = state_and_job(repo);
    builder
        .build(
            &state,
            &job,
            None,
            Vec::new(),
            &[],
            OffsetDateTime::now_utc(),
        )
        .await
}

/// Make tracked files stat-dirty and modified, so status/diff do real work.
fn dirty(repo: &Path) {
    std::fs::write(repo.join("a.txt"), "two\n").expect("write");
    std::fs::write(repo.join("b.rs"), "fn main() { /* x */ }\n").expect("write");
}

/// Plant `config`, run a plain `control` git command to prove the plant is live (the
/// marker appears), then check the hardened observation does not trigger it.
async fn assert_not_run(config: &str, attributes: Option<&str>, control: &[&str], name: &str) {
    if !have_git() {
        return;
    }
    let dir = short_tempdir().expect("tempdir");
    let marker = dir.path().join(format!("marker-{name}"));
    let evil = script(
        dir.path(),
        "evil.sh",
        &format!(
            "touch {}\nenv > {}.env\ncat",
            marker.display(),
            marker.display()
        ),
    );
    let repo = make_repo(dir.path());
    append(
        &repo.join(".git/config"),
        &config.replace("EVIL", &evil.display().to_string()),
    );
    if let Some(attributes) = attributes {
        std::fs::write(repo.join(".gitattributes"), attributes).expect("attributes");
    }
    dirty(&repo);

    // Control: an unhardened git does run it.
    git(&repo, control);
    assert!(
        marker.exists(),
        "{name}: the plant is not live; the test would prove nothing"
    );
    std::fs::remove_file(&marker).expect("reset");

    let observation = observe(&ObservationBuilder::new(false), &repo).await;
    assert!(
        !marker.exists(),
        "{name}: repository-controlled code ran during observation"
    );
    // And the observation still has real content.
    assert!(
        observation
            .git
            .changed_files
            .iter()
            .any(|f| f.ends_with("a.txt")),
        "{name}: {:?}",
        observation.git
    );
}

#[tokio::test]
async fn a_planted_fsmonitor_is_not_run() {
    assert_not_run(
        "[core]\n\tfsmonitor = EVIL\n",
        None,
        &["status"],
        "fsmonitor",
    )
    .await;
}

#[tokio::test]
async fn a_planted_external_diff_is_not_run() {
    assert_not_run("[diff]\n\texternal = EVIL\n", None, &["diff"], "extdiff").await;
}

#[tokio::test]
async fn a_planted_textconv_is_not_run() {
    assert_not_run(
        "[diff \"evil\"]\n\ttextconv = EVIL\n",
        Some("*.txt diff=evil\n"),
        &["diff"],
        "textconv",
    )
    .await;
}

#[tokio::test]
async fn a_planted_clean_filter_is_not_run() {
    assert_not_run(
        "[filter \"evil\"]\n\tclean = EVIL\n\tsmudge = EVIL\n",
        Some("*.txt filter=evil\n"),
        &["status"],
        "filter",
    )
    .await;
}

#[tokio::test]
async fn a_planted_process_filter_is_not_run() {
    assert_not_run(
        "[filter \"evil\"]\n\tprocess = EVIL\n\trequired = true\n",
        Some("*.txt filter=evil\n"),
        &["status"],
        "filter-process",
    )
    .await;
}

#[tokio::test]
async fn planted_hooks_are_not_run() {
    if !have_git() {
        return;
    }
    let dir = short_tempdir().expect("tempdir");
    let marker = dir.path().join("marker-hooks");
    let repo = make_repo(dir.path());
    for hook in [
        "post-index-change",
        "pre-commit",
        "reference-transaction",
        "fsmonitor-watchman",
        "post-checkout",
    ] {
        script(
            &repo.join(".git/hooks"),
            hook,
            &format!("touch {}", marker.display()),
        );
    }
    append(
        &repo.join(".git/config"),
        "[core]\n\thooksPath = .git/hooks\n",
    );
    dirty(&repo);
    let _ = observe(&ObservationBuilder::new(false), &repo).await;
    assert!(!marker.exists(), "a hook ran during observation");
}

#[tokio::test]
async fn git_sees_a_scrubbed_environment() {
    if !have_git() {
        return;
    }
    let dir = short_tempdir().expect("tempdir");
    let repo = make_repo(dir.path());
    let dump = dir.path().join("env-dump");
    let fake = script(
        dir.path(),
        "fake-git",
        &format!(
            "env > {}\nexec {} \"$@\"",
            dump.display(),
            real_git().display()
        ),
    );
    let mut builder = ObservationBuilder::new(false);
    builder.git_program = fake.into_os_string();
    let observation = observe(&builder, &repo).await;
    assert!(!observation.git.status_short.is_empty() || observation.git.head_sha.is_some());

    let env = std::fs::read_to_string(&dump).expect("the fake git ran");
    for line in env.lines() {
        let key = line.split('=').next().unwrap_or("");
        assert!(
            !key.starts_with("TYPESAFE_") && !key.starts_with("AFTMAN_"),
            "a supervisor variable reached git: {key}"
        );
    }
    for expected in [
        "GIT_PAGER=cat",
        "GIT_CONFIG_GLOBAL=/dev/null",
        "GIT_TERMINAL_PROMPT=0",
        "GIT_OPTIONAL_LOCKS=0",
    ] {
        assert!(
            env.lines().any(|l| l == expected),
            "missing {expected}:\n{env}"
        );
    }
}

// ---- F4: nothing blocks, everything is bounded ------------------------------------

fn mkfifo(path: &Path) {
    nix::unistd::mkfifo(path, nix::sys::stat::Mode::from_bits_truncate(0o600)).expect("mkfifo");
}

#[tokio::test]
async fn special_files_in_the_repository_are_skipped_without_blocking() {
    if !have_git() {
        return;
    }
    let dir = short_tempdir().expect("tempdir");
    let repo = make_repo(dir.path());
    mkfifo(&repo.join("AGENTS.md"));
    mkfifo(&repo.join("pipe.txt"));
    std::os::unix::fs::symlink("/dev/zero", repo.join("zero.txt")).expect("symlink");
    std::os::unix::fs::symlink("/dev/zero", repo.join("AGENTS.override.md")).expect("symlink");
    std::fs::write(repo.join("real.txt"), "hello\n").expect("write");

    let started = tokio::time::Instant::now();
    let observation = tokio::time::timeout(
        Duration::from_secs(5),
        observe(&ObservationBuilder::new(false), &repo),
    )
    .await
    .expect("observation hung on a special file");
    assert!(started.elapsed() < Duration::from_secs(3));
    assert!(
        observation.instructions.is_none(),
        "a FIFO/symlink AGENTS.md is not read"
    );
    let paths: Vec<_> = observation
        .untracked
        .iter()
        .map(|u| u.path.as_str())
        .collect();
    assert_eq!(
        paths,
        vec!["real.txt"],
        "only the regular file is excerpted"
    );
}

#[tokio::test]
async fn a_large_regular_file_is_read_only_up_to_its_cap() {
    if !have_git() {
        return;
    }
    let dir = short_tempdir().expect("tempdir");
    let repo = make_repo(dir.path());
    std::fs::write(repo.join("AGENTS.md"), "x".repeat(5_000_000)).expect("write");
    git(&repo, &["add", "AGENTS.md"]);
    git(&repo, &["commit", "-q", "-m", "instructions"]);
    std::fs::write(repo.join("big.txt"), "y".repeat(5_000_000)).expect("write");
    let observation = observe(&ObservationBuilder::new(false), &repo).await;
    let instructions = observation.instructions.expect("instructions");
    assert!(instructions.truncated);
    assert!(instructions.excerpt.len() <= 4_000);
    let big = observation
        .untracked
        .iter()
        .find(|u| u.path == "big.txt")
        .expect("excerpt");
    assert!(big.truncated);
    assert_eq!(big.bytes, 5_000_000, "the real size is still reported");
    assert!(big.content.len() <= 4_096);
}

#[tokio::test]
async fn a_hanging_git_costs_the_timeout_and_is_killed() {
    let dir = short_tempdir().expect("tempdir");
    let repo = dir.path().join("repo");
    std::fs::create_dir_all(&repo).expect("mkdir");
    let pids = dir.path().join("pids");
    let fake = script(
        dir.path(),
        "hang-git",
        &format!(
            "echo $$ >> {}\nsleep 600 &\necho $! >> {}\nwait",
            pids.display(),
            pids.display()
        ),
    );
    let mut builder = ObservationBuilder::new(false);
    builder.git_program = fake.into_os_string();
    builder.timeout = Duration::from_millis(400);

    let started = tokio::time::Instant::now();
    let observation = observe(&builder, &repo).await;
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
    assert!(observation.evidence.contains_key("observation_note"));
    assert!(observation.git.status_short.is_empty());

    // Neither the fake git nor what it spawned outlives the observation.
    wait_until("the hung git to be killed", || async {
        std::fs::read_to_string(&pids)
            .unwrap_or_default()
            .lines()
            .filter_map(|l| l.trim().parse::<u32>().ok())
            .all(|pid| {
                !Path::new(&format!("/proc/{pid}")).exists()
                    || std::fs::read_to_string(format!("/proc/{pid}/stat"))
                        .is_ok_and(|s| s.contains(") Z "))
            })
    })
    .await;
}

fn real_runtime(dir: &Path, store: Arc<dyn RunStore>, model: Arc<DialModel>) -> FactoryRuntime {
    let temp = dir.join("evidence-tmp");
    std::fs::create_dir_all(&temp).expect("mkdir");
    FactoryRuntime::new(RuntimeDeps {
        config: RunConfig {
            // A real (non-simulated) kind, so the runtime reads the repository.
            worker: WorkerKind::Generic,
            ..common::fast_config()
        },
        store,
        model,
        worker: Arc::new(SubprocessWorker::new(
            WorkerKind::Generic,
            CommandSpec {
                program: "/bin/sh".to_owned(),
                args: vec!["-c".to_owned(), "sleep 120".to_owned()],
                env: Vec::new(),
                steer: SteerProtocol::None,
                working_dir_is_repo: true,
            },
        )),
        registry: ResponsibilityRegistry::builtin(),
        definitions: aftman_core::definitions::DefinitionSet::bundled().unwrap_or_default(),
        evidence: EvidencePlan::default(),
        evidence_temp_dir: temp,
        clock: Arc::new(SystemClock),
        sink: Arc::new(NullSink),
        ensemble_enabled: false,
        uncertainty_margin: 0.15,
        steering_grace: Duration::from_secs(30),
        launch: Default::default(),
    })
}

/// A cancel must be answered promptly and the run must end properly, whatever the
/// repository looks like.
async fn cancel_is_prompt(
    runtime: FactoryRuntime,
    store: Arc<dyn RunStore>,
    repo: PathBuf,
    model: Arc<DialModel>,
) -> TestResult {
    let runtime = Arc::new(runtime);
    let started = runtime
        .start(Job {
            prompt: "x".into(),
            repo,
            source: JobSource::Cli,
        })
        .await?;
    let id = started.run_id();
    let (control, receiver) = RunControl::channel();
    let task = tokio::spawn({
        let runtime = runtime.clone();
        async move { runtime.drive(started, Some(receiver)).await }
    });
    wait_until("assessments to be running", || async {
        model.calls.load(std::sync::atomic::Ordering::SeqCst) >= 3
    })
    .await;
    tokio::time::timeout(Duration::from_secs(5), control.cancel("test"))
        .await
        .map_err(|_| "cancel was not answered promptly")??;
    let outcome = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .map_err(|_| "the run did not end")???;
    assert_eq!(outcome.status, FactoryStatus::Cancelled);
    assert_eq!(store.load(id).await?.status, FactoryStatus::Cancelled);
    assert!(
        store
            .read_log(id)
            .await?
            .events
            .iter()
            .any(|e| e.kind == aftman_core::event::EventKind::RunFinished)
    );
    Ok(())
}

#[tokio::test]
async fn a_run_with_a_fifo_agents_md_can_be_cancelled_promptly() -> TestResult {
    if !have_git() {
        return Ok(());
    }
    let dir = short_tempdir()?;
    let repo = make_repo(dir.path());
    mkfifo(&repo.join("AGENTS.md"));
    mkfifo(&repo.join("untracked-pipe.txt"));
    std::os::unix::fs::symlink("/dev/zero", repo.join("zero.txt"))?;
    let store: Arc<dyn RunStore> = Arc::new(FileRunStore::new(dir.path().join("state")));
    let model = Arc::new(DialModel::new(Mode::Working));
    let runtime = real_runtime(dir.path(), store.clone(), model.clone());
    cancel_is_prompt(runtime, store, repo, model).await
}

#[tokio::test]
async fn a_run_whose_git_hangs_can_still_be_cancelled() -> TestResult {
    let dir = short_tempdir()?;
    let repo = dir.path().join("repo");
    std::fs::create_dir_all(&repo)?;
    let fake = script(dir.path(), "hang-git", "sleep 600");
    let store: Arc<dyn RunStore> = Arc::new(FileRunStore::new(dir.path().join("state")));
    let model = Arc::new(DialModel::new(Mode::Working));
    let runtime = real_runtime(dir.path(), store.clone(), model.clone())
        .with_git_program(fake)
        .with_observation_timeout(Duration::from_millis(300));
    cancel_is_prompt(runtime, store, repo, model).await
}

// ---- the environment construction, on a synthetic base --------------------------------

#[test]
fn the_supervisors_credentials_and_git_variables_are_not_passed_on() {
    // Exercised through the public environment builder the git command uses.
    let base = [
        ("PATH", "/usr/bin"),
        ("HOME", "/home/x"),
        ("TYPESAFE_API_KEY", "canary-1"),
        ("AFTMAN_JEV_API_KEY", "canary-2"),
        ("GIT_DIR", "/evil"),
        ("GIT_EXTERNAL_DIFF", "/evil"),
    ]
    .map(|(k, v)| (k.to_owned(), v.to_owned()));
    let env = aftman_workers::build_worker_environment(base, &[]);
    assert!(env.iter().any(|(k, _)| k == "PATH"));
    assert!(
        !env.iter()
            .any(|(k, _)| k.starts_with("TYPESAFE_") || k.starts_with("AFTMAN_"))
    );
}

// ---- every configuration scope ---------------------------------------------------------

/// Like `assert_not_run`, but the plant is written by `setup(repo, evil_script, dir)`, so it
/// can go in any scope (worktree config, an included file, ...).
async fn assert_planted_not_run(
    name: &str,
    attributes: Option<&str>,
    control: &[&str],
    setup: impl Fn(&Path, &Path, &Path),
) {
    if !have_git() {
        return;
    }
    let dir = short_tempdir().expect("tempdir");
    let marker = dir.path().join(format!("marker-{name}"));
    let evil = script(
        dir.path(),
        "evil.sh",
        &format!("touch {}\ncat", marker.display()),
    );
    let repo = make_repo(dir.path());
    setup(&repo, &evil, dir.path());
    if let Some(attributes) = attributes {
        std::fs::write(repo.join(".gitattributes"), attributes).expect("attributes");
    }
    dirty(&repo);

    git(&repo, control);
    assert!(
        marker.exists(),
        "{name}: the plant is not live; the test proves nothing"
    );
    std::fs::remove_file(&marker).expect("reset");

    let observation = observe(&ObservationBuilder::new(false), &repo).await;
    assert!(
        !marker.exists(),
        "{name}: repository-controlled code ran during observation"
    );
    assert!(
        observation
            .git
            .changed_files
            .iter()
            .any(|f| f.ends_with("a.txt")),
        "{name}: the observation still has content: {:?}",
        observation.git
    );
}

const WORKTREE_ON: &str = "[extensions]\n\tworktreeConfig = true\n";

fn worktree_config(repo: &Path, text: &str, evil: &Path) {
    append(&repo.join(".git/config"), WORKTREE_ON);
    std::fs::write(
        repo.join(".git/config.worktree"),
        text.replace("EVIL", &evil.display().to_string()),
    )
    .expect("config.worktree");
}

#[tokio::test]
async fn a_clean_filter_in_worktree_scoped_config_is_not_run() {
    assert_planted_not_run(
        "wt-clean",
        Some("*.txt filter=evil\n"),
        &["status"],
        |repo, evil, _| {
            worktree_config(
                repo,
                "[filter \"evil\"]\n\tclean = EVIL\n\tsmudge = EVIL\n",
                evil,
            );
        },
    )
    .await;
}

#[tokio::test]
async fn a_process_filter_in_worktree_scoped_config_is_not_run() {
    assert_planted_not_run(
        "wt-process",
        Some("*.txt filter=evil\n"),
        &["status"],
        |repo, evil, _| {
            worktree_config(
                repo,
                "[filter \"evil\"]\n\tprocess = EVIL\n\trequired = true\n",
                evil,
            );
        },
    )
    .await;
}

#[tokio::test]
async fn a_filter_pulled_in_by_include_from_worktree_config_is_not_run() {
    assert_planted_not_run(
        "wt-include",
        Some("*.txt filter=evil\n"),
        &["status"],
        |repo, evil, dir| {
            let extra = dir.join("extra.cfg");
            std::fs::write(
                &extra,
                format!("[filter \"evil\"]\n\tclean = {}\n", evil.display()),
            )
            .expect("extra");
            worktree_config(
                repo,
                &format!("[include]\n\tpath = {}\n", extra.display()),
                evil,
            );
        },
    )
    .await;
}

#[tokio::test]
async fn a_filter_pulled_in_by_include_if_from_local_config_is_not_run() {
    assert_planted_not_run(
        "local-includeif",
        Some("*.txt filter=evil\n"),
        &["status"],
        |repo, evil, dir| {
            let extra = dir.join("extra.cfg");
            std::fs::write(
                &extra,
                format!("[filter \"evil\"]\n\tclean = {}\n", evil.display()),
            )
            .expect("extra");
            append(
                &repo.join(".git/config"),
                &format!(
                    "[includeIf \"gitdir:/tmp/**\"]\n\tpath = {}\n",
                    extra.display()
                ),
            );
        },
    )
    .await;
}

#[tokio::test]
async fn fsmonitor_set_in_worktree_config_does_not_win_over_the_forced_setting() {
    assert_planted_not_run("wt-fsmonitor", None, &["status"], |repo, evil, _| {
        worktree_config(repo, "[core]\n\tfsmonitor = EVIL\n", evil);
    })
    .await;
}

#[tokio::test]
async fn hooks_path_set_in_worktree_config_does_not_win_over_the_forced_setting() {
    if !have_git() {
        return;
    }
    let dir = short_tempdir().expect("tempdir");
    let marker = dir.path().join("marker-wt-hooks");
    let repo = make_repo(dir.path());
    let hooks = dir.path().join("evil-hooks");
    std::fs::create_dir_all(&hooks).expect("hooks dir");
    for hook in [
        "post-index-change",
        "fsmonitor-watchman",
        "pre-commit",
        "reference-transaction",
    ] {
        script(&hooks, hook, &format!("touch {}", marker.display()));
    }
    append(&repo.join(".git/config"), WORKTREE_ON);
    std::fs::write(
        repo.join(".git/config.worktree"),
        format!("[core]\n\thooksPath = {}\n", hooks.display()),
    )
    .expect("config.worktree");
    dirty(&repo);
    let _ = observe(&ObservationBuilder::new(false), &repo).await;
    assert!(
        !marker.exists(),
        "a hook from a worktree-scoped hooksPath ran"
    );
}

#[tokio::test]
async fn a_filter_whose_name_has_dots_spaces_and_capitals_is_neutralized() {
    // An attribute value cannot contain a space, so the filter the attributes name has dots,
    // capitals and an `=`; a second one with spaces in its name is defined too, to prove
    // the scan copes with it.
    let name = "My.Odd=Filter.V2";
    let config = format!(
        "[filter \"{name}\"]\n\tclean = EVIL\n\tsmudge = EVIL\n[filter \"has some spaces\"]\n\tclean = EVIL\n"
    );
    // In local config...
    assert_planted_not_run(
        "odd-local",
        Some(&format!("*.txt filter={name}\n")),
        &["status"],
        |repo, evil, _| {
            append(
                &repo.join(".git/config"),
                &config.replace("EVIL", &evil.display().to_string()),
            );
        },
    )
    .await;
    // ... and in worktree config.
    assert_planted_not_run(
        "odd-worktree",
        Some(&format!("*.txt filter={name}\n")),
        &["status"],
        |repo, evil, _| worktree_config(repo, &config, evil),
    )
    .await;
}

#[tokio::test]
async fn diff_drivers_in_worktree_config_are_not_run_by_status_or_diff() {
    // `--no-ext-diff --no-textconv` cover `diff` and `diff --stat`; `status` never runs
    // either. `merge.<driver>.driver` is only reachable from merge, which is never run.
    assert_planted_not_run(
        "wt-diff-drivers",
        Some("*.txt diff=evil\n"),
        &["diff"],
        |repo, evil, _| {
            worktree_config(
                repo,
                "[diff \"evil\"]\n\ttextconv = EVIL\n\tcommand = EVIL\n[diff]\n\texternal = EVIL\n",
                evil,
            );
        },
    )
    .await;
}

/// A fake git that plants a filter in the repository the first time it is asked to `diff`,
/// then runs the real git: configuration changing mid-observation, deterministically.
#[tokio::test]
async fn configuration_that_changes_during_the_observation_discards_the_git_evidence() {
    if !have_git() {
        return;
    }
    let dir = short_tempdir().expect("tempdir");
    let repo = make_repo(dir.path());
    dirty(&repo);
    let once = dir.path().join("mutated-once");
    let fake = script(
        dir.path(),
        "mutating-git",
        &format!(
            "for a in \"$@\"; do\n  if [ \"$a\" = diff ] && [ ! -e {once} ]; then\n    touch {once}\n    printf '[filter \"late\"]\\n\\tclean = /bin/true\\n' >> {repo}/.git/config\n  fi\ndone\nexec {real} \"$@\"",
            once = once.display(),
            repo = repo.display(),
            real = real_git().display(),
        ),
    );
    let mut builder = ObservationBuilder::new(false);
    builder.git_program = fake.into_os_string();

    let observation = observe(&builder, &repo).await;
    assert!(once.exists(), "the fake git mutated the configuration");
    let note = observation
        .evidence
        .get("observation_note")
        .expect("a note records that the evidence was discarded");
    assert!(note.contains("changed during the observation"), "{note}");
    assert!(observation.git.status_short.is_empty() && observation.git.diff_excerpt.is_empty());
    assert!(observation.untracked.is_empty());

    // The next cycle sees the new configuration from the start, so it is both safe
    // (overridden) and complete.
    let next = observe(&builder, &repo).await;
    assert!(
        !next.evidence.contains_key("observation_note"),
        "{:?}",
        next.evidence
    );
    assert!(next.git.changed_files.iter().any(|f| f.ends_with("a.txt")));
}
