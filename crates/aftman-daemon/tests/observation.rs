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
async fn a_fifo_attributes_file_cannot_block_the_observation() {
    if !have_git() {
        return;
    }
    let dir = short_tempdir().expect("tempdir");
    let repo = make_repo(dir.path());
    dirty(&repo);
    // A worker can plant a FIFO where gix looks for attributes; opening it must not hang.
    mkfifo(&repo.join(".gitattributes"));
    let observation = tokio::time::timeout(
        Duration::from_secs(5),
        observe(&ObservationBuilder::new(false), &repo),
    )
    .await;
    assert!(
        observation.is_ok(),
        "the observation hung on a FIFO .gitattributes"
    );
}

#[tokio::test]
async fn a_fifo_gitignore_cannot_block_the_observation() {
    if !have_git() {
        return;
    }
    let dir = short_tempdir().expect("tempdir");
    let repo = make_repo(dir.path());
    dirty(&repo);
    mkfifo(&repo.join(".gitignore"));
    let observation = tokio::time::timeout(
        Duration::from_secs(5),
        observe(&ObservationBuilder::new(false), &repo),
    )
    .await;
    assert!(
        observation.is_ok(),
        "the observation hung on a FIFO .gitignore"
    );
}

#[tokio::test]
async fn a_fifo_configured_attributes_file_cannot_block_the_observation() {
    if !have_git() {
        return;
    }
    let dir = short_tempdir().expect("tempdir");
    let repo = make_repo(dir.path());
    dirty(&repo);
    let fifo = repo.join("attributes-fifo");
    mkfifo(&fifo);
    git(
        &repo,
        &[
            "config",
            "core.attributesFile",
            fifo.to_str().expect("utf8"),
        ],
    );
    let observation = tokio::time::timeout(
        Duration::from_secs(5),
        observe(&ObservationBuilder::new(false), &repo),
    )
    .await;
    assert!(
        observation.is_ok(),
        "the observation hung on a configured FIFO attributes file"
    );
}

#[tokio::test]
async fn a_fifo_info_attributes_yields_a_note_not_a_hang() {
    if !have_git() {
        return;
    }
    let dir = short_tempdir().expect("tempdir");
    let repo = make_repo(dir.path());
    dirty(&repo);
    mkfifo(&repo.join(".git/info/attributes"));
    let observation = tokio::time::timeout(
        Duration::from_secs(5),
        observe(&ObservationBuilder::new(false), &repo),
    )
    .await
    .expect("the observation hung on a FIFO .git/info/attributes");
    assert!(
        observation.observation_note.is_some(),
        "a non-regular info/attributes is refused with a note"
    );
    assert!(observation.git.status_short.is_empty());
}

#[tokio::test]
async fn ignored_files_are_not_reported_as_untracked() {
    if !have_git() {
        return;
    }
    let dir = short_tempdir().expect("tempdir");
    let repo = make_repo(dir.path());
    std::fs::write(repo.join(".gitignore"), "ignored.txt\n").expect("write");
    git(&repo, &["add", ".gitignore"]);
    git(&repo, &["commit", "-q", "-m", "ignore"]);
    std::fs::write(repo.join("ignored.txt"), "ignored\n").expect("write");
    std::fs::write(repo.join("real.txt"), "real\n").expect("write");
    let observation = observe(&ObservationBuilder::new(false), &repo).await;
    let status = &observation.git.status_short;
    assert!(status.contains("?? real.txt"), "{status:?}");
    assert!(!status.contains("ignored.txt"), "{status:?}");
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
async fn a_timed_out_observation_reports_a_note_instead_of_hanging() {
    if !have_git() {
        return;
    }
    let dir = short_tempdir().expect("tempdir");
    let repo = make_repo(dir.path());
    // A file whose hash must be computed keeps the blocking observer busy past the zero
    // deadline, so the timeout path is exercised rather than a lucky fast finish.
    let big = repo.join("big.bin");
    std::fs::write(&big, vec![7u8; 24 * 1024 * 1024]).expect("write");
    git(&repo, &["add", "big.bin"]);
    git(&repo, &["commit", "-q", "-m", "big"]);
    std::fs::write(&big, vec![8u8; 24 * 1024 * 1024]).expect("write");
    dirty(&repo);

    let mut builder = ObservationBuilder::new(false);
    builder.timeout = Duration::ZERO;

    let started = tokio::time::Instant::now();
    let observation = observe(&builder, &repo).await;
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
    let note = observation
        .observation_note
        .as_deref()
        .expect("a timeout note");
    assert!(note.contains("exceeded"), "{note}");
    assert!(observation.git.status_short.is_empty());
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

        forwarded_env: Default::default(),
        jev_budget: Default::default(),
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
async fn a_run_whose_observation_times_out_can_still_be_cancelled() -> TestResult {
    if !have_git() {
        return Ok(());
    }
    let dir = short_tempdir()?;
    let repo = make_repo(dir.path());
    let store: Arc<dyn RunStore> = Arc::new(FileRunStore::new(dir.path().join("state")));
    let model = Arc::new(DialModel::new(Mode::Working));
    let runtime = real_runtime(dir.path(), store.clone(), model.clone())
        .with_observation_timeout(Duration::from_millis(1));
    cancel_is_prompt(runtime, store, repo, model).await
}

// ---- the environment construction, on a synthetic base --------------------------------

#[test]
fn the_supervisors_credentials_and_git_variables_are_not_passed_on() {
    // Exercised through the public environment builder the worker subprocess uses.
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

/// A filter inserted into `.git/config` **after** the observation has started must be a
/// no-op. This is the window the CLI path could not close: it overrode the driver names it
/// saw in a scan, so a driver added between the scan and the `git diff` was still run. With
/// an empty driver list there is no name to add that can select a program, and gix's config
/// snapshot at open time means the insertion is not even seen this cycle.
#[tokio::test]
async fn a_filter_inserted_mid_observation_is_never_run() {
    if !have_git() {
        return;
    }
    let dir = short_tempdir().expect("tempdir");
    let marker = dir.path().join("marker-mid");
    let evil = script(
        dir.path(),
        "evil.sh",
        &format!("touch {}\ncat", marker.display()),
    );
    let repo = make_repo(dir.path());
    std::fs::write(repo.join(".gitattributes"), "*.txt filter=late\n").expect("attributes");
    dirty(&repo);

    // Prove the plant is live when the driver exists (via `-c`, so the repository's own
    // config stays clean until the mid-observation insertion).
    git(
        &repo,
        &[
            "-c",
            &format!("filter.late.clean={}", evil.display()),
            "status",
        ],
    );
    assert!(
        marker.exists(),
        "the plant is not live; the test proves nothing"
    );
    std::fs::remove_file(&marker).expect("reset");

    let builder = ObservationBuilder::new(false);
    let (state, job) = state_and_job(&repo);
    let task = tokio::spawn(async move {
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
    });
    // Let the observer open and snapshot the configuration, then plant the driver.
    tokio::time::sleep(Duration::from_millis(20)).await;
    append(
        &repo.join(".git/config"),
        &format!("[filter \"late\"]\n\tclean = {}\n", evil.display()),
    );
    let observation = task.await.expect("the observation task did not panic");
    assert!(
        !marker.exists(),
        "a filter inserted mid-observation ran during observation"
    );
    assert!(
        observation
            .git
            .changed_files
            .iter()
            .any(|f| f.ends_with("a.txt")),
        "the observation still has content: {:?}",
        observation.git
    );
}

// ---- fidelity: the text the model sees -----------------------------------------------

#[tokio::test]
async fn status_letters_and_head_and_branch_are_reported() {
    if !have_git() {
        return;
    }
    let dir = short_tempdir().expect("tempdir");
    let repo = make_repo(dir.path());
    std::fs::write(repo.join("a.txt"), "two\n").expect("write");
    std::fs::remove_file(repo.join("b.rs")).expect("remove");
    std::fs::write(repo.join("new.txt"), "new\n").expect("write");
    let staged = repo.join("staged.txt");
    std::fs::write(&staged, "staged\n").expect("write");
    git(&repo, &["add", "staged.txt"]);

    let observation = observe(&ObservationBuilder::new(false), &repo).await;
    let status = &observation.git.status_short;
    assert!(status.contains(" M a.txt"), "{status:?}");
    assert!(status.contains(" D b.rs"), "{status:?}");
    assert!(status.contains("?? new.txt"), "{status:?}");
    assert!(status.contains("A  staged.txt"), "{status:?}");
    assert!(observation.git.changed_files.iter().any(|f| f == "a.txt"));
    assert_eq!(observation.git.head_sha.as_deref().map(str::len), Some(40));
    let expected_branch =
        String::from_utf8_lossy(&git(&repo, &["rev-parse", "--abbrev-ref", "HEAD"]).stdout)
            .trim()
            .to_owned();
    assert_eq!(
        observation.git.branch.as_deref(),
        Some(expected_branch.as_str())
    );
}

#[tokio::test]
async fn a_modified_file_reaches_the_diff_and_the_stat_with_a_hunk() {
    if !have_git() {
        return;
    }
    let dir = short_tempdir().expect("tempdir");
    let repo = make_repo(dir.path());
    std::fs::write(repo.join("a.txt"), "one\ntwo\nthree\n").expect("write");
    git(&repo, &["add", "a.txt"]);
    git(&repo, &["commit", "-q", "-m", "three"]);
    std::fs::write(repo.join("a.txt"), "one\nTWO\nthree\n").expect("write");

    let observation = observe(&ObservationBuilder::new(false), &repo).await;
    let diff = &observation.git.diff_excerpt;
    assert!(diff.contains("diff --git a/a.txt b/a.txt"), "{diff:?}");
    assert!(diff.contains("--- a/a.txt"), "{diff:?}");
    assert!(diff.contains("+++ b/a.txt"), "{diff:?}");
    assert!(diff.contains("@@ -1,3 +1,3 @@"), "{diff:?}");
    assert!(diff.contains("-two"), "{diff:?}");
    assert!(diff.contains("+TWO"), "{diff:?}");
    let stat = &observation.git.diff_stat;
    assert!(stat.contains("a.txt | 2 +-"), "{stat:?}");
    assert!(
        stat.contains("1 file changed, 1 insertion(+), 1 deletion(-)"),
        "{stat:?}"
    );
}

#[tokio::test]
async fn a_deleted_file_is_a_deletion_in_the_diff() {
    if !have_git() {
        return;
    }
    let dir = short_tempdir().expect("tempdir");
    let repo = make_repo(dir.path());
    std::fs::remove_file(repo.join("b.rs")).expect("remove");

    let observation = observe(&ObservationBuilder::new(false), &repo).await;
    let diff = &observation.git.diff_excerpt;
    assert!(diff.contains("deleted file mode 100644"), "{diff:?}");
    assert!(diff.contains("+++ /dev/null"), "{diff:?}");
    assert!(diff.contains("@@ -1 +0,0 @@"), "{diff:?}");
    assert!(
        observation.git.diff_stat.contains("b.rs | 1 -"),
        "{:?}",
        observation.git.diff_stat
    );
}

#[tokio::test]
async fn a_new_repository_with_no_commits_yields_no_head_and_no_error() {
    if !have_git() {
        return;
    }
    let dir = short_tempdir().expect("tempdir");
    let repo = dir.path().join("repo");
    std::fs::create_dir_all(&repo).expect("mkdir");
    git(&repo, &["init", "-q"]);
    let observation = observe(&ObservationBuilder::new(false), &repo).await;
    assert!(
        observation.observation_note.is_none(),
        "{:?}",
        observation.observation_note
    );
    assert!(
        observation.git.head_sha.is_none(),
        "{:?}",
        observation.git.head_sha
    );
    assert!(observation.git.status_short.is_empty());
}

#[tokio::test]
async fn a_directory_that_is_not_a_repository_yields_a_note() {
    let dir = short_tempdir().expect("tempdir");
    let repo = dir.path().join("not-a-repo");
    std::fs::create_dir_all(&repo).expect("mkdir");
    let observation = observe(&ObservationBuilder::new(false), &repo).await;
    let note = observation
        .observation_note
        .as_deref()
        .expect("a note explains the missing git evidence");
    assert!(
        note.contains("repository") || note.contains("open"),
        "{note}"
    );
    assert!(observation.git.status_short.is_empty());
}

#[tokio::test]
async fn a_binary_change_is_labelled_not_inlined() {
    if !have_git() {
        return;
    }
    let dir = short_tempdir().expect("tempdir");
    let repo = make_repo(dir.path());
    std::fs::write(repo.join("blob.dat"), b"\x00\x01binary\n").expect("write");
    git(&repo, &["add", "blob.dat"]);
    git(&repo, &["commit", "-q", "-m", "bin"]);
    std::fs::write(repo.join("blob.dat"), b"\x00\x02binary\n").expect("write");
    let observation = observe(&ObservationBuilder::new(false), &repo).await;
    assert!(
        observation.git.diff_excerpt.contains("Binary files"),
        "{:?}",
        observation.git.diff_excerpt
    );
    assert!(
        observation.git.diff_stat.contains("Bin "),
        "{:?}",
        observation.git.diff_stat
    );
}

/// A FIFO at the index — a file gix opens for every status — is refused rather than read.
///
/// This is what keeps an ordinary cycle ordinary. The refusals cannot cover every path
/// (`.git/config` is read during discovery, before any check can run, and a loose object is
/// opened wherever the diff needs it); the observation slot bounds those. See
/// `obs::slot_tests` for the bound itself, and `docs/08-security.md` §3.2 for the pair.
#[tokio::test]
async fn a_fifo_at_the_index_is_refused_rather_than_read() {
    let dir = short_tempdir().expect("tempdir");
    let repo = make_repo(dir.path());
    // `mkfifo` will not replace the index git already wrote.
    std::fs::remove_file(repo.join(".git/index")).expect("remove the index");
    mkfifo(&repo.join(".git/index"));

    let builder = ObservationBuilder::new(false);
    let started = tokio::time::Instant::now();
    let observation = observe(&builder, &repo).await;
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "a refusal must be immediate, not a wait for the deadline: {:?}",
        started.elapsed()
    );
    let note = observation
        .observation_note
        .as_deref()
        .expect("a refusal note");
    assert!(note.contains("not a regular file"), "{note}");
    assert!(observation.git.status_short.is_empty());
}

/// An intent-to-add entry has no blob pair to diff, and it must not stop the walk.
///
/// `tracked` is sorted by path, so a `break` on the first non-diffable entry silently
/// dropped the diff and stat of every path sorting after it — the model saw "modified, but
/// nothing to show", with `truncated` false.
#[tokio::test]
async fn an_intent_to_add_entry_does_not_hide_the_diffs_after_it() {
    if !have_git() {
        return;
    }
    let dir = short_tempdir().expect("tempdir");
    let repo = make_repo(dir.path());
    // `a0.txt` sorts before both committed files, so it hits the walk first.
    std::fs::write(repo.join("a0.txt"), "new\n").expect("write");
    git(&repo, &["add", "-N", "a0.txt"]);
    dirty(&repo);

    let observation = observe(&ObservationBuilder::new(false), &repo).await;
    assert!(
        observation.git.diff_excerpt.contains("b.rs"),
        "the diff stopped at the non-diffable entry: {:?}",
        observation.git.diff_excerpt
    );
    assert!(
        observation.git.diff_stat.contains("b.rs"),
        "the stat stopped too: {:?}",
        observation.git.diff_stat
    );
}

/// A path inside a repository is not the repository, and observing the enclosing one would
/// report changes outside the job — silently, so `UnobservedRepository` would not fire.
#[tokio::test]
async fn a_path_inside_a_repository_is_refused_rather_than_widened() {
    let dir = short_tempdir().expect("tempdir");
    let repo = make_repo(dir.path());
    let sub = repo.join("sub");
    std::fs::create_dir_all(&sub).expect("mkdir");
    std::fs::write(sub.join("x.txt"), "x\n").expect("write");

    let observation = observe(&ObservationBuilder::new(false), &sub).await;
    let note = observation.observation_note.as_deref().unwrap_or_default();
    assert!(note.contains("not the root"), "{note}");
    assert!(observation.git.status_short.is_empty());
}
