//! End-to-end: the built `aftman` binary, auto-starting real detached daemons.
//!
//! Hermetic and offline: every process runs with a cleared environment holding only
//! `PATH` and `HOME`/`XDG_*` pointed into a temp directory (so the user's real
//! directories are never touched and no credential can leak in), with an explicit
//! `--socket` and `--data-dir` in a short `/tmp` path (the `sun_path` limit). The
//! decision model is always `--simulate-model`; the worker is the simulated one, or
//! `sh`/`true` behind the `generic` backend to get a run that waits or escalates.
//!
//! **No test may leak a daemon.** Every [`Sandbox`] stops its daemon when dropped
//! (`aftman stop`), then kills whatever is still serving on its socket, found by
//! scanning `/proc` for `serve` with that socket in its argv, so a failing assertion
//! cannot leave one behind. Every wait has a deadline.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;

const PATIENCE: Duration = Duration::from_secs(30);
/// The provider variables, named by the one crate that may (hard rule 2).
const API_KEY_VAR: &str = aftman_jev::ENV_KEYS[0];
const BASE_URL_VAR: &str = aftman_jev::ENV_KEYS[1];

// ===========================================================================
// The sandbox
// ===========================================================================

struct Sandbox {
    dir: tempfile::TempDir,
    socket: PathBuf,
    data: PathBuf,
    repo: PathBuf,
}

struct Out {
    code: i32,
    stdout: String,
    stderr: String,
}

impl Out {
    fn all(&self) -> String {
        format!("{}\n{}", self.stdout, self.stderr)
    }
}

impl Sandbox {
    fn new() -> Self {
        let dir = tempfile::Builder::new()
            .prefix("aft")
            .tempdir_in("/tmp")
            .expect("a temp dir");
        let repo = dir.path().join("repo");
        std::fs::create_dir_all(&repo).expect("repo dir");
        Self {
            socket: dir.path().join("s.sock"),
            data: dir.path().join("d"),
            repo,
            dir,
        }
    }

    fn pid_file(&self) -> PathBuf {
        self.socket.with_extension("pid")
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_aftman"));
        command
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", self.dir.path().join("home"))
            .env("XDG_RUNTIME_DIR", self.dir.path().join("xdg"))
            .env("XDG_STATE_HOME", self.dir.path().join("xdgstate"))
            .env("XDG_CONFIG_HOME", self.dir.path().join("xdgconfig"))
            .arg("--socket")
            .arg(&self.socket)
            .arg("--data-dir")
            .arg(&self.data)
            .args(args)
            .stdin(Stdio::null());
        command
    }

    /// Run to completion within `PATIENCE`.
    fn run(&self, args: &[&str]) -> Out {
        self.run_with(args, |_| {})
    }

    fn run_with(&self, args: &[&str], tweak: impl FnOnce(&mut Command)) -> Out {
        let mut command = self.command(args);
        tweak(&mut command);
        self.finish(Spawned::start(command, self.dir.path()))
    }

    fn finish(&self, spawned: Spawned) -> Out {
        spawned.wait(PATIENCE)
    }

    fn spawn(&self, args: &[&str]) -> Spawned {
        Spawned::start(self.command(args), self.dir.path())
    }

    /// `run --repo REPO --job ... --simulate-model` plus `extra`.
    fn run_args<'a>(&'a self, extra: &[&'a str]) -> Vec<&'a str> {
        let mut args = vec![
            "run",
            "--repo",
            self.repo.to_str().expect("utf-8 path"),
            "--job",
            "Add retries with backoff.",
            "--simulate-model",
        ];
        args.extend_from_slice(extra);
        args
    }

    fn daemon_pids(&self) -> Vec<u32> {
        serving(&self.socket)
    }

    fn status_pid(&self) -> Option<u32> {
        let out = self.run(&["status"]);
        if out.code != 0 {
            return None;
        }
        out.stdout
            .lines()
            .find_map(|l| l.trim().strip_prefix("pid"))
            .and_then(|rest| rest.trim().parse().ok())
    }

    fn wait_for<T>(&self, what: &str, mut probe: impl FnMut() -> Option<T>) -> T {
        let deadline = Instant::now() + PATIENCE;
        loop {
            if let Some(value) = probe() {
                return value;
            }
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            std::thread::sleep(Duration::from_millis(40));
        }
    }

    /// Wait until `ls` lists a run with this status; returns the run id.
    fn wait_for_run_with_status(&self, status: &str) -> String {
        self.wait_for(&format!("a {status} run"), || {
            let out = self.run(&["ls", "--status", status]);
            out.stdout
                .lines()
                .nth(1)
                .and_then(|l| l.split_whitespace().next())
                .map(ToOwned::to_owned)
        })
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        // The polite way first, with a bound, then whatever is left.
        if let Ok(mut stop) = self
            .command(&["stop"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        {
            let deadline = Instant::now() + Duration::from_secs(20);
            while Instant::now() < deadline {
                if matches!(stop.try_wait(), Ok(Some(_))) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(25));
            }
            let _ = stop.kill();
            let _ = stop.wait();
        }
        for pid in serving(&self.socket) {
            let _ = kill(Pid::from_raw(pid as i32), Signal::SIGKILL);
        }
        // Any worker still running under this sandbox (its repo, or a script in it).
        for pid in processes_mentioning(self.dir.path().to_str().unwrap_or("/nonexistent")) {
            let _ = kill(Pid::from_raw(pid as i32), Signal::SIGKILL);
        }
    }
}

/// A child process with piped output (to files, so a full pipe cannot wedge it) that
/// is killed if the test ends before it does.
struct Spawned {
    child: Option<Child>,
    stdout: PathBuf,
    stderr: PathBuf,
}

impl Spawned {
    fn start(mut command: Command, dir: &Path) -> Self {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let stdout = dir.join(format!("out-{n}.txt"));
        let stderr = dir.join(format!("err-{n}.txt"));
        command
            .stdout(std::fs::File::create(&stdout).expect("stdout file"))
            .stderr(std::fs::File::create(&stderr).expect("stderr file"));
        let child = command.spawn().expect("spawning aftman");
        Self {
            child: Some(child),
            stdout,
            stderr,
        }
    }

    fn pid(&self) -> u32 {
        self.child.as_ref().map_or(0, Child::id)
    }

    fn try_wait(&mut self) -> Option<i32> {
        let child = self.child.as_mut()?;
        child.try_wait().expect("try_wait").map(|s| status_code(&s))
    }

    fn output(&self) -> (String, String) {
        (
            std::fs::read_to_string(&self.stdout).unwrap_or_default(),
            std::fs::read_to_string(&self.stderr).unwrap_or_default(),
        )
    }

    /// Wait for exit, with a deadline.
    fn wait(mut self, limit: Duration) -> Out {
        let deadline = Instant::now() + limit;
        loop {
            if let Some(code) = self.try_wait() {
                let (stdout, stderr) = self.output();
                return Out {
                    code,
                    stdout,
                    stderr,
                };
            }
            if Instant::now() >= deadline {
                let (stdout, stderr) = self.output();
                if let Some(child) = self.child.as_mut() {
                    let _ = child.kill();
                    let _ = child.wait();
                }
                panic!("timed out after {limit:?}\nstdout:\n{stdout}\nstderr:\n{stderr}");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn signal(&self, signal: Signal) {
        kill(Pid::from_raw(self.pid() as i32), signal).expect("signalling the child");
    }
}

impl Drop for Spawned {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            if matches!(child.try_wait(), Ok(None)) {
                let _ = child.kill();
            }
            let _ = child.wait();
        }
    }
}

fn status_code(status: &std::process::ExitStatus) -> i32 {
    use std::os::unix::process::ExitStatusExt as _;
    status
        .code()
        .unwrap_or_else(|| 128 + status.signal().unwrap_or(0))
}

/// Pids whose argv contains `needle` (as a whole argument or a substring).
fn processes_mentioning(needle: &str) -> Vec<u32> {
    let me = std::process::id();
    let mut found = Vec::new();
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return found;
    };
    for entry in entries.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|n| n.parse::<u32>().ok())
        else {
            continue;
        };
        if pid == me {
            continue;
        }
        let Ok(cmdline) = std::fs::read(entry.path().join("cmdline")) else {
            continue;
        };
        let text = String::from_utf8_lossy(&cmdline);
        if text.contains(needle) {
            found.push(pid);
        }
    }
    found
}

/// Daemons serving on `socket`: `aftman ... serve ... --socket <socket>`.
fn serving(socket: &Path) -> Vec<u32> {
    let needle = socket.to_string_lossy().into_owned();
    processes_mentioning(&needle)
        .into_iter()
        .filter(|pid| {
            std::fs::read(format!("/proc/{pid}/cmdline"))
                .map(|raw| raw.split(|b| *b == 0).any(|arg| arg == b"serve"))
                .unwrap_or(false)
        })
        .collect()
}

/// Pids running exactly `sleep <arg>`: the worker itself, not a command line that
/// merely mentions it.
fn sleepers(arg: &str) -> Vec<u32> {
    let want = format!("sleep\0{arg}\0");
    processes_mentioning(arg)
        .into_iter()
        .filter(|pid| {
            std::fs::read(format!("/proc/{pid}/cmdline"))
                .is_ok_and(|raw| String::from_utf8_lossy(&raw) == want)
        })
        .collect()
}

fn alive(pid: u32) -> bool {
    // A zombie still has a /proc entry; its state is `Z`.
    std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .map(|stat| {
            stat.rsplit(')')
                .next()
                .and_then(|rest| rest.split_whitespace().next())
                .is_some_and(|state| state != "Z")
        })
        .unwrap_or(false)
}

fn after<'a>(text: &'a str, label: &str) -> Option<&'a str> {
    text.lines()
        .find_map(|l| l.trim_start().strip_prefix(label))
        .map(str::trim)
}

fn run_id_of(out: &Out) -> String {
    after(&out.stdout, "run id")
        .unwrap_or_else(|| panic!("no run id in:\n{}", out.all()))
        .to_owned()
}

/// A generic worker that sleeps until it is killed. `unique` keeps `pgrep` honest.
/// A `sleep` duration unique to this test process, so copies of the suite running at
/// the same time never mistake each other's workers for their own.
fn tag(base: &str) -> String {
    format!("{base}{}", std::process::id())
}

fn sleeper(unique: &str) -> (String, String) {
    (
        "--cmd".to_owned(),
        format!("sh -c 'sleep {unique}' {{repo}}"),
    )
}

// ===========================================================================
// Auto-start, reuse, and inspection
// ===========================================================================

#[test]
fn run_auto_starts_a_daemon_that_outlives_it_and_inspection_agrees() {
    let sb = Sandbox::new();
    assert!(sb.daemon_pids().is_empty());

    let first = sb.run(&sb.run_args(&[]));
    assert_eq!(first.code, 0, "{}", first.all());
    assert!(first.stdout.contains("RUN_FINISHED"), "{}", first.stdout);
    assert_eq!(after(&first.stdout, "status"), Some("finished"));
    let id = run_id_of(&first);

    // The daemon the run started is still up afterwards.
    let pid = sb.status_pid().expect("the daemon is still running");
    assert_eq!(sb.daemon_pids(), [pid]);
    assert!(sb.socket.exists() && sb.pid_file().exists());
    assert_eq!(
        std::fs::read_to_string(sb.pid_file())
            .expect("pid file")
            .trim(),
        pid.to_string()
    );
    // It logs to the state directory, not to a terminal.
    let log = std::fs::read_to_string(sb.data.join("daemon.log")).expect("daemon log");
    assert!(log.contains("listening"), "{log}");

    // ls, show, decisions, logs agree with the run.
    let ls = sb.run(&["ls"]);
    assert_eq!(ls.code, 0, "{}", ls.all());
    assert!(
        ls.stdout.contains(&id) && ls.stdout.contains("finished"),
        "{}",
        ls.stdout
    );
    let filtered = sb.run(&["ls", "--status", "needs-human"]);
    assert!(!filtered.stdout.contains(&id), "{}", filtered.stdout);
    assert!(sb.run(&["ls", "--status", "sleeping"]).code == 2);

    let prefix = &id[..18];
    let show = sb.run(&["show", prefix]);
    assert_eq!(show.code, 0, "{}", show.all());
    assert!(
        show.stdout.contains(&id) && show.stdout.contains("finished"),
        "{}",
        show.stdout
    );
    assert!(show.stdout.contains("RUN_FINISHED") && show.stdout.contains("timeline"));
    let last = sb.run(&["show", "--last"]);
    assert!(last.stdout.contains(&id));

    let decisions = sb.run(&["decisions", &id]);
    assert_eq!(decisions.code, 0, "{}", decisions.all());
    assert!(
        decisions.stdout.contains("selected StartWorker"),
        "{}",
        decisions.stdout
    );
    assert!(
        decisions.stdout.contains("selected Finish"),
        "{}",
        decisions.stdout
    );

    let logs = sb.run(&["logs", &id]);
    assert_eq!(logs.code, 0, "{}", logs.all());
    assert!(logs.stdout.contains("test result: ok"), "{}", logs.stdout);
    let followed = sb.run(&["logs", &id, "-f"]);
    assert_eq!(followed.code, 0, "{}", followed.all());
    assert_eq!(
        followed.stdout, logs.stdout,
        "-f on a finished run replays and ends"
    );

    // An ambiguous or unknown prefix is a usage error.
    assert_eq!(sb.run(&["show", "ffffffff"]).code, 2);
    assert_eq!(sb.run(&["show", "ab"]).code, 2);

    // A second run reuses the same daemon.
    let second = sb.run(&sb.run_args(&[]));
    assert_eq!(second.code, 0, "{}", second.all());
    assert_ne!(run_id_of(&second), id);
    assert_eq!(sb.status_pid(), Some(pid), "the same daemon served both");
    assert_eq!(sb.daemon_pids(), [pid]);
}

#[test]
fn two_runs_started_together_with_no_daemon_both_succeed_on_one_daemon() {
    let sb = Sandbox::new();
    let args = sb.run_args(&[]);
    let a = sb.spawn(&args);
    let b = sb.spawn(&args);
    let (a, b) = (a.wait(PATIENCE), b.wait(PATIENCE));
    assert_eq!(a.code, 0, "{}", a.all());
    assert_eq!(b.code, 0, "{}", b.all());
    assert_ne!(run_id_of(&a), run_id_of(&b));

    // The loser of the start race exits promptly; wait for exactly one to remain.
    let pids = sb.wait_for("exactly one daemon", || {
        let pids = sb
            .daemon_pids()
            .into_iter()
            .filter(|p| alive(*p))
            .collect::<Vec<_>>();
        (pids.len() == 1).then_some(pids)
    });
    assert_eq!(sb.status_pid(), Some(pids[0]));
    let ls = sb.run(&["ls"]);
    assert_eq!(ls.stdout.matches("finished").count(), 2, "{}", ls.stdout);
}

#[test]
fn serve_while_one_is_running_prints_its_pid_and_exits_zero() {
    let sb = Sandbox::new();
    assert_eq!(sb.run(&sb.run_args(&[])).code, 0);
    let pid = sb.status_pid().expect("running");

    let again = sb.run(&["serve"]);
    assert_eq!(again.code, 0, "{}", again.all());
    assert!(
        again
            .stdout
            .contains(&format!("already running (pid {pid})")),
        "{}",
        again.stdout
    );
    assert_eq!(sb.daemon_pids(), [pid], "no second daemon");
}

#[test]
fn stop_stops_and_cleans_up_and_status_and_stop_never_start_a_daemon() {
    let sb = Sandbox::new();

    // Neither starts anything when nothing runs.
    let status = sb.run(&["status"]);
    assert_eq!(status.code, 1, "{}", status.all());
    assert!(status.stdout.contains("not running"), "{}", status.stdout);
    let stop = sb.run(&["stop"]);
    assert_eq!(stop.code, 0, "{}", stop.all());
    assert!(stop.stdout.contains("not running"), "{}", stop.stdout);
    assert!(!sb.socket.exists() && !sb.pid_file().exists());
    assert!(sb.daemon_pids().is_empty());

    assert_eq!(sb.run(&sb.run_args(&[])).code, 0);
    let pid = sb.status_pid().expect("running");
    let stop = sb.run(&["stop"]);
    assert_eq!(stop.code, 0, "{}", stop.all());
    assert!(!sb.socket.exists(), "socket removed");
    assert!(!sb.pid_file().exists(), "pid file removed");
    sb.wait_for("the daemon process to exit", || (!alive(pid)).then_some(()));

    let status = sb.run(&["status"]);
    assert_eq!(status.code, 1);
    assert!(status.stdout.contains("not running"));
    assert!(!sb.socket.exists(), "status did not start a daemon");
    assert!(sb.daemon_pids().iter().all(|p| !alive(*p)));

    // The run is still on disk and readable without a daemon.
    let ls = sb.run(&["--no-daemon", "ls"]);
    assert_eq!(ls.code, 0, "{}", ls.all());
    assert!(ls.stdout.contains("finished"), "{}", ls.stdout);
    assert!(!sb.socket.exists());
}

#[test]
fn a_foreground_serve_prints_its_socket_and_stops_on_sigterm() {
    let sb = Sandbox::new();
    let mut serve = sb.spawn(&["serve"]);
    sb.wait_for("the daemon to report listening", || {
        serve.output().0.contains("listening").then_some(())
    });
    let (stdout, _) = serve.output();
    assert!(
        stdout.contains(sb.socket.to_str().expect("utf-8")),
        "{stdout}"
    );
    assert!(stdout.contains("recovery: no unfinished runs"), "{stdout}");
    assert!(sb.status_pid().is_some());

    serve.signal(Signal::SIGTERM);
    let code = sb.wait_for("serve to exit", || serve.try_wait());
    assert_eq!(code, 0);
    assert!(!sb.socket.exists() && !sb.pid_file().exists());
}

// ===========================================================================
// --no-daemon
// ===========================================================================

#[test]
fn no_daemon_runs_embedded_and_never_creates_a_socket() {
    let sb = Sandbox::new();
    let out = sb.run(&sb.run_args(&["--no-daemon"]));
    // `--no-daemon` is a global flag: either position works, and the run is the same.
    let mut args = vec!["--no-daemon"];
    args.extend(sb.run_args(&[]));
    let again = sb.run(&args);
    for out in [&out, &again] {
        assert_eq!(out.code, 0, "{}", out.all());
        assert!(out.stdout.contains("RUN_FINISHED"), "{}", out.stdout);
        assert_eq!(after(&out.stdout, "status"), Some("finished"));
        assert!(out.stdout.contains("using the simulated decision model"));
    }
    assert!(
        !sb.socket.exists() && !sb.pid_file().exists(),
        "no daemon was involved"
    );
    assert!(sb.daemon_pids().is_empty());

    // Read-only commands read the store directly.
    let id = run_id_of(&out);
    let ls = sb.run(&["--no-daemon", "ls"]);
    assert!(ls.stdout.contains(&id), "{}", ls.stdout);
    assert_eq!(sb.run(&["--no-daemon", "show", &id]).code, 0);
    let decisions = sb.run(&["--no-daemon", "decisions", &id]);
    assert!(
        decisions.stdout.contains("selected Finish"),
        "{}",
        decisions.stdout
    );
    let logs = sb.run(&["--no-daemon", "logs", &id]);
    assert!(logs.stdout.contains("test result: ok"), "{}", logs.stdout);
    assert!(!sb.socket.exists(), "reading did not start a daemon");

    // Commands that need a live daemon are a clear error, not a surprise.
    for args in [
        vec!["--no-daemon", "steer", "abcd1234", "hello"],
        vec!["--no-daemon", "cancel", "--last"],
        vec![
            "--no-daemon",
            "answer",
            "abcd1234",
            "0193a000-0000-7000-8000-000000000001",
            "--stop",
        ],
        vec!["--no-daemon", "logs", "--last", "-f"],
    ] {
        let refused = sb.run(&args);
        assert_eq!(refused.code, 2, "{args:?}: {}", refused.all());
        assert!(refused.stderr.contains("--no-daemon") || refused.stderr.contains("daemon"));
    }
    assert!(!sb.socket.exists());

    // purge works on the store, and never starts a daemon either.
    let purge = sb.run(&["--no-daemon", "purge", &id]);
    assert_eq!(purge.code, 0, "{}", purge.all());
    assert!(purge.stdout.contains("purged 1"));
    let purge = sb.run(&["--no-daemon", "purge", "--all"]);
    assert!(purge.stdout.contains("purged 1"), "{}", purge.stdout);
    assert!(sb.run(&["--no-daemon", "ls"]).stdout.contains("no runs"));
    assert!(!sb.socket.exists());
}

#[test]
fn the_demo_is_embedded_and_offline() {
    let sb = Sandbox::new();
    let out = sb.run(&["demo", "--repo", sb.repo.to_str().expect("utf-8")]);
    assert_eq!(out.code, 0, "{}", out.all());
    assert!(out.stdout.contains("aftman demo"), "{}", out.stdout);
    assert_eq!(after(&out.stdout, "status"), Some("finished"));
    assert!(!sb.socket.exists(), "the demo never needs a daemon");
}

// ===========================================================================
// Intervening
// ===========================================================================

#[test]
fn cancel_from_a_second_process_ends_the_first_run_with_exit_one() {
    let sb = Sandbox::new();
    let (cmd_flag, cmd) = sleeper(&tag("600.301"));
    let mut extra = vec![
        "--worker",
        "generic",
        "--external-sandbox",
        cmd_flag.as_str(),
        cmd.as_str(),
    ];
    let args = sb.run_args(&extra);
    let long = sb.spawn(&args);
    extra.clear();

    let id = sb.wait_for_run_with_status("running");

    // A worker with no input channel says so, with a non-zero exit.
    let steer = sb.run(&["steer", &id, "go left"]);
    assert_eq!(steer.code, 1, "{}", steer.all());
    assert!(
        steer.stderr.contains("steering_unsupported"),
        "{}",
        steer.stderr
    );

    let cancel = sb.run(&["cancel", &id[..18]]);
    assert_eq!(cancel.code, 0, "{}", cancel.all());

    let finished = long.wait(PATIENCE);
    assert_eq!(finished.code, 1, "{}", finished.all());
    assert_eq!(after(&finished.stdout, "status"), Some("cancelled"));

    // The ended run cannot be cancelled again; it says why.
    let again = sb.run(&["cancel", &id]);
    assert_eq!(again.code, 1, "{}", again.all());
    assert!(again.stderr.contains("wrong_state"), "{}", again.stderr);
    // And the worker is gone, not orphaned.
    sb.wait_for("the worker to die", || {
        processes_mentioning(&format!("sleep {}", tag("600.301")))
            .is_empty()
            .then_some(())
    });
}

#[test]
fn an_escalation_exits_three_and_answer_stop_ends_it() {
    let sb = Sandbox::new();
    let args = sb.run_args(&[
        "--worker",
        "generic",
        "--cmd",
        "true {repo}",
        "--external-sandbox",
    ]);
    let out = sb.run(&args);
    assert_eq!(out.code, 3, "{}", out.all());
    let id = run_id_of(&out);
    let escalation = after(&out.stdout, "escalation")
        .unwrap_or_else(|| panic!("no escalation id:\n{}", out.stdout))
        .to_owned();
    assert!(out.stdout.contains("keeps waiting"), "{}", out.stdout);
    // The exact command to answer, with this daemon's flags.
    assert!(
        out.stdout.contains(&format!(
            "answer {id} {escalation} \"<what the worker should do>\""
        )),
        "{}",
        out.stdout
    );
    assert!(
        out.stdout
            .contains(&format!("--socket {}", sb.socket.display()))
    );

    // The run is waiting in the daemon, and `show` lists the escalation with its id.
    let show = sb.run(&["show", &id]);
    assert!(show.stdout.contains("needs_human"), "{}", show.stdout);
    assert!(
        show.stdout
            .contains(&format!("open escalation {escalation}")),
        "{}",
        show.stdout
    );
    let status = sb.run(&["status"]);
    assert!(status.stdout.contains("1 live"), "{}", status.stdout);

    // Mistakes are reported, not swallowed.
    assert_eq!(sb.run(&["answer", &id, "not-an-id", "--stop"]).code, 2);
    let nothing = sb.run(&["answer", &id, &escalation]);
    assert_eq!(nothing.code, 2, "{}", nothing.all());
    let wrong = sb.run(&[
        "answer",
        &id,
        "0193a000-0000-7000-8000-000000000001",
        "--stop",
    ]);
    assert_eq!(wrong.code, 1, "{}", wrong.all());
    assert!(wrong.stderr.contains("wrong_state"), "{}", wrong.stderr);

    let answer = sb.run(&["answer", &id, &escalation, "--stop", "giving up"]);
    assert_eq!(answer.code, 0, "{}", answer.all());
    assert!(
        answer.stdout.contains("answered escalation"),
        "{}",
        answer.stdout
    );

    let ended = sb.wait_for("the run to leave needs_human", || {
        let show = sb.run(&["show", &id]);
        let status = after(&show.stdout, "status")
            .unwrap_or("needs_human")
            .to_owned();
        (status != "needs_human").then_some(status)
    });
    assert_eq!(ended, "cancelled", "a stop answer ends the run");
    let show = sb.run(&["show", &id]);
    assert!(!show.stdout.contains("open escalation"), "{}", show.stdout);
    assert!(
        show.stdout.contains("ESCALATION_ANSWERED"),
        "{}",
        show.stdout
    );
    // Answered once; answering again says so.
    let twice = sb.run(&["answer", &id, &escalation, "--accept"]);
    assert_eq!(twice.code, 1, "{}", twice.all());
}

#[test]
fn ctrl_c_detaches_and_the_run_keeps_going() {
    let sb = Sandbox::new();
    let (cmd_flag, cmd) = sleeper(&tag("600.302"));
    let args = sb.run_args(&["--worker", "generic", "--external-sandbox", &cmd_flag, &cmd]);
    let long = sb.spawn(&args);
    let id = sb.wait_for_run_with_status("running");

    long.signal(Signal::SIGINT);
    let detached = long.wait(PATIENCE);
    assert_eq!(detached.code, 130, "{}", detached.all());
    assert!(detached.stdout.contains("detached"), "{}", detached.stdout);
    assert!(
        detached.stdout.contains(&format!("logs {id} -f")),
        "{}",
        detached.stdout
    );
    assert!(
        detached.stdout.contains(&format!("cancel {id}")),
        "{}",
        detached.stdout
    );

    // Still running: Ctrl-C did not cancel it.
    let show = sb.run(&["show", &id]);
    assert!(show.stdout.contains("running"), "{}", show.stdout);
    assert_eq!(sb.run(&["cancel", &id]).code, 0);
    sb.wait_for("the cancelled run", || {
        sb.run(&["show", &id])
            .stdout
            .contains("cancelled")
            .then_some(())
    });
}

#[test]
fn the_detached_daemon_survives_the_death_of_the_cli_that_started_it() {
    let sb = Sandbox::new();
    let (cmd_flag, cmd) = sleeper(&tag("600.303"));
    let args = sb.run_args(&["--worker", "generic", "--external-sandbox", &cmd_flag, &cmd]);
    let long = sb.spawn(&args);
    let id = sb.wait_for_run_with_status("running");
    let pid = sb.status_pid().expect("the daemon is up");

    // Kill the CLI outright: no handler runs, nothing is cleaned up.
    long.signal(Signal::SIGKILL);
    let dead = long.wait(PATIENCE);
    assert_eq!(dead.code, 128 + 9);

    // The daemon, and the job it supervises, carry on.
    assert!(alive(pid));
    assert_eq!(sb.status_pid(), Some(pid));
    assert!(sb.run(&["show", &id]).stdout.contains("running"));
    // It left the CLI's session: closing a terminal would not have reached it.
    let mine = nix::unistd::getsid(None).expect("my session");
    let theirs = nix::unistd::getsid(Some(Pid::from_raw(pid as i32))).expect("daemon session");
    assert_ne!(mine, theirs, "the daemon is in its own session");
    assert_eq!(theirs, Pid::from_raw(pid as i32), "and leads it");

    assert_eq!(sb.run(&["cancel", &id]).code, 0);
}

// ===========================================================================
// Client-side checks and daemon-side errors
// ===========================================================================

#[test]
fn bad_requests_exit_two_with_a_code_and_never_start_what_they_do_not_need() {
    let sb = Sandbox::new();

    // Checked in the client, before any daemon is started.
    let no_ack = sb.run(&sb.run_args(&["--permissions", "auto-approve"]));
    assert_eq!(no_ack.code, 2, "{}", no_ack.all());
    assert!(
        no_ack.stderr.contains("AFTMAN_ACK_UNSANDBOXED"),
        "{}",
        no_ack.stderr
    );
    let interactive = sb.run(&sb.run_args(&["--permissions", "interactive"]));
    assert_eq!(interactive.code, 2, "{}", interactive.all());
    assert!(
        interactive.stderr.contains("herdr"),
        "{}",
        interactive.stderr
    );
    let missing = sb.run(&[
        "run",
        "--repo",
        "/nonexistent/path/for/aftman",
        "--job",
        "x",
        "--simulate-model",
    ]);
    assert_eq!(missing.code, 2, "{}", missing.all());
    assert!(
        !sb.socket.exists(),
        "client-side failures started no daemon"
    );

    // The acknowledgement is read from the *client's* environment.
    let acked = sb.run_with(&sb.run_args(&["--permissions", "auto-approve"]), |c| {
        c.env("AFTMAN_ACK_UNSANDBOXED", "1");
    });
    assert_eq!(acked.code, 0, "{}", acked.all());

    // Daemon-side validation arrives as a coded error.
    let worker = sb.run(&sb.run_args(&["--worker", "gpt-9"]));
    assert_eq!(worker.code, 2, "{}", worker.all());
    assert!(
        worker.stderr.contains("invalid_config"),
        "{}",
        worker.stderr
    );

    // No provider and no --simulate-model: model_unavailable, with the explanation.
    let no_model = sb.run(&[
        "run",
        "--repo",
        sb.repo.to_str().expect("utf-8"),
        "--job",
        "x",
    ]);
    assert_eq!(no_model.code, 2, "{}", no_model.all());
    assert!(
        no_model.stderr.contains("model_unavailable"),
        "{}",
        no_model.stderr
    );
    assert!(
        no_model.stderr.contains("--simulate-model"),
        "{}",
        no_model.stderr
    );

    // `--no-daemon` gives the same refusal, with the same text.
    let embedded = sb.run(&[
        "--no-daemon",
        "run",
        "--repo",
        sb.repo.to_str().expect("utf-8"),
        "--job",
        "x",
    ]);
    assert_eq!(embedded.code, 2, "{}", embedded.all());
    assert!(
        embedded.stderr.contains("--simulate-model"),
        "{}",
        embedded.stderr
    );
}

#[test]
fn purge_responsibilities_usage_and_doctor() {
    let sb = Sandbox::new();
    let out = sb.run(&sb.run_args(&[]));
    assert_eq!(out.code, 0, "{}", out.all());
    let id = run_id_of(&out);

    // A live run is never purged.
    let (cmd_flag, cmd) = sleeper(&tag("600.304"));
    let args = sb.run_args(&["--worker", "generic", "--external-sandbox", &cmd_flag, &cmd]);
    let long = sb.spawn(&args);
    let live = sb.wait_for_run_with_status("running");
    let refused = sb.run(&["purge", &live]);
    assert_eq!(refused.code, 1, "{}", refused.all());
    assert!(refused.stderr.contains("wrong_state"), "{}", refused.stderr);
    let all = sb.run(&["purge", "--all"]);
    assert!(
        all.stdout.contains("purged 1"),
        "the finished one only: {}",
        all.stdout
    );
    assert!(
        sb.run(&["show", &id]).code == 2,
        "the purged run is unknown now"
    );
    assert_eq!(sb.run(&["cancel", &live]).code, 0);
    let _ = long.wait(PATIENCE);
    let one = sb.run(&["purge", &live]);
    assert_eq!(one.code, 0, "{}", one.all());
    assert_eq!(sb.run(&["purge"]).code, 2, "purge needs a run or --all");

    // None of these needs a daemon, and none starts one.
    sb.run(&["stop"]);
    let list = sb.run(&["responsibilities", "list"]);
    assert_eq!(list.code, 0, "{}", list.all());
    assert!(list.stdout.contains("core.completion"), "{}", list.stdout);
    let show = sb.run(&["responsibilities", "show", "core.completion"]);
    assert_eq!(show.code, 0, "{}", show.all());
    assert!(show.stdout.contains("ready_to_finish"), "{}", show.stdout);
    assert_eq!(sb.run(&["responsibilities", "show", "no.such"]).code, 2);
    let usage = sb.run(&["usage", "--since", "7d"]);
    assert_eq!(usage.code, 0, "{}", usage.all());
    assert!(usage.stdout.contains("no usage recorded") || usage.stdout.contains("no recorded"));
    assert_eq!(sb.run(&["usage", "--since", "yesterday"]).code, 2);
    let doctor = sb.run(&["doctor"]);
    // No provider configured in this sandbox: exit 2, but the report is all there.
    assert_eq!(doctor.code, 2, "{}", doctor.all());
    assert!(doctor.stdout.contains("daemon"), "{}", doctor.stdout);
    assert!(doctor.stdout.contains(sb.socket.to_str().expect("utf-8")));
    assert!(
        doctor.stdout.contains("reachable   no"),
        "{}",
        doctor.stdout
    );
    assert!(
        doctor.stdout.contains("NOT CONFIGURED"),
        "{}",
        doctor.stdout
    );
    assert!(!sb.socket.exists(), "none of them started a daemon");

    // With a daemon up, doctor sees it.
    assert_eq!(sb.run(&sb.run_args(&[])).code, 0);
    let doctor = sb.run(&["doctor"]);
    assert!(
        doctor.stdout.contains("reachable   yes"),
        "{}",
        doctor.stdout
    );
    assert!(
        doctor.stdout.contains("socket mode 0600"),
        "{}",
        doctor.stdout
    );
}

#[test]
fn a_daemon_with_a_stale_socket_is_replaced_not_trusted() {
    let sb = Sandbox::new();
    // A socket file left by a daemon that died: bound, then abandoned.
    drop(std::os::unix::net::UnixListener::bind(&sb.socket).expect("bind"));
    assert!(sb.socket.exists());
    assert_eq!(sb.run(&["status"]).code, 1, "status sees it as not running");
    let out = sb.run(&sb.run_args(&[]));
    assert_eq!(out.code, 0, "{}", out.all());
    assert!(sb.status_pid().is_some());
}

#[test]
fn the_daemon_ignores_sighup() {
    let sb = Sandbox::new();
    assert_eq!(sb.run(&sb.run_args(&[])).code, 0);
    let pid = sb.status_pid().expect("running");
    kill(Pid::from_raw(pid as i32), Signal::SIGHUP).expect("SIGHUP");
    std::thread::sleep(Duration::from_millis(300));
    assert!(alive(pid), "the daemon survived SIGHUP");
    assert_eq!(sb.status_pid(), Some(pid));
}

// ===========================================================================
// Review fixes
// ===========================================================================

impl Sandbox {
    /// A command with this sandbox's environment but none of its flags, run from `cwd`.
    fn bare(&self, cwd: &Path, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_aftman"));
        command
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", self.dir.path().join("home"))
            .env("XDG_RUNTIME_DIR", self.dir.path().join("xdg"))
            .env("XDG_STATE_HOME", self.dir.path().join("xdgstate"))
            .env("XDG_CONFIG_HOME", self.dir.path().join("xdgconfig"))
            .current_dir(cwd)
            .args(args)
            .stdin(Stdio::null());
        command
    }

    fn bare_run(&self, cwd: &Path, args: &[&str]) -> Out {
        self.finish(Spawned::start(self.bare(cwd, args), self.dir.path()))
    }

    fn mkdir(&self, name: &str) -> PathBuf {
        let path = self.dir.path().join(name);
        std::fs::create_dir_all(&path).expect("mkdir");
        path
    }

    fn mode(&self, path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::metadata(path)
            .unwrap_or_else(|e| panic!("{}: {e}", path.display()))
            .permissions()
            .mode()
            & 0o777
    }
}

/// All text under `dir`, to look for something that should not be there.
fn tree_contains(dir: &Path, needle: &str) -> Vec<PathBuf> {
    let mut hits = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return hits;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            hits.extend(tree_contains(&path, needle));
        } else if std::fs::read(&path).is_ok_and(|b| {
            b.windows(needle.len().max(1))
                .any(|w| w == needle.as_bytes())
        }) {
            hits.push(path);
        }
    }
    hits
}

#[test]
fn the_daemon_does_not_inherit_the_first_clis_directory() {
    let sb = Sandbox::new();
    let first_dir = sb.mkdir("first-cwd");
    let elsewhere = sb.mkdir("elsewhere");
    let repo_b = sb.mkdir("repo-b");
    let socket = sb.socket.to_str().expect("utf-8").to_owned();
    let repo_arg = repo_b.to_str().expect("utf-8").to_owned();

    // The daemon is auto-started from `first-cwd`, with *relative* locations.
    let started = sb.bare_run(
        &first_dir,
        &[
            "--socket",
            "../s.sock",
            "--data-dir",
            "../d",
            "run",
            "--repo",
            &repo_arg,
            "--job",
            "x",
            "--simulate-model",
        ],
    );
    assert_eq!(started.code, 0, "{}", started.all());
    let pid = sb.status_pid().expect("running");
    assert_eq!(
        std::fs::read_link(format!("/proc/{pid}/cwd")).expect("cwd link"),
        PathBuf::from("/"),
        "the daemon left the first CLI's directory"
    );

    // A job from another directory: the agent's relative path lands in the repo.
    let touched = sb.bare_run(
        &elsewhere,
        &[
            "--socket",
            &socket,
            "--data-dir",
            sb.data.to_str().expect("utf-8"),
            "run",
            "--repo",
            &repo_arg,
            "--job",
            "make a file",
            "--simulate-model",
            "--worker",
            "generic",
            "--external-sandbox",
            "--cmd",
            "touch created-by-agent",
        ],
    );
    assert_eq!(touched.code, 3, "{}", touched.all());
    assert!(repo_b.join("created-by-agent").exists(), "in the repo");
    assert!(!first_dir.join("created-by-agent").exists());
    assert!(!elsewhere.join("created-by-agent").exists());
    assert!(!Path::new("/created-by-agent").exists());
}

#[test]
fn the_daemons_environment_is_frozen_and_the_cli_says_so() {
    let sb = Sandbox::new();
    // The daemon starts in a shell with no provider.
    assert_eq!(sb.run(&sb.run_args(&[])).code, 0);

    // A later shell has one. `status` shows both views and flags the disagreement.
    let status = sb.run_with(&["status"], |c| {
        c.env(API_KEY_VAR, "sk-test-secret-1234")
            .env(BASE_URL_VAR, "https://user:hunter2@api.example.com");
    });
    assert_eq!(status.code, 0, "{}", status.all());
    assert!(
        status.stdout.contains("provider   not configured"),
        "{}",
        status.stdout
    );
    assert!(status.stdout.contains("(this shell)"), "{}", status.stdout);
    assert!(
        status
            .stdout
            .contains("NOTE: this shell has a provider configured"),
        "{}",
        status.stdout
    );
    for secret in ["sk-test-secret-1234", "hunter2"] {
        assert!(
            !status.all().contains(secret),
            "leaked {secret}: {}",
            status.all()
        );
    }
    assert!(
        status.stdout.contains("https://api.example.com"),
        "{}",
        status.stdout
    );
    let doctor = sb.run_with(&["doctor"], |c| {
        c.env(API_KEY_VAR, "sk-test-secret-1234")
            .env(BASE_URL_VAR, "https://user:hunter2@api.example.com");
    });
    assert!(
        doctor
            .stdout
            .contains("NOTE: this shell has a provider configured"),
        "{}",
        doctor.stdout
    );
    assert!(!doctor.all().contains("hunter2") && !doctor.all().contains("sk-test"));

    // And the failure says what happened and how to fix it.
    let run = sb.run_with(
        &[
            "run",
            "--repo",
            sb.repo.to_str().expect("utf-8"),
            "--job",
            "x",
        ],
        |c| {
            c.env(API_KEY_VAR, "sk-test-secret-1234");
        },
    );
    assert_eq!(run.code, 2, "{}", run.all());
    assert!(run.stderr.contains("model_unavailable"), "{}", run.stderr);
    assert!(
        run.stderr.contains("started without a provider"),
        "{}",
        run.stderr
    );
    assert!(run.stderr.contains("aftman stop"), "{}", run.stderr);
}

#[test]
fn a_provider_run_through_the_daemon_prints_the_supervisor_line_and_never_the_key() {
    let sb = Sandbox::new();
    let secret = "sk-test-secret-9876";
    // Nothing listens on port 9: the model is unreachable, but the run starts and says
    // what it is supervised by.
    let env = |c: &mut Command| {
        c.env(API_KEY_VAR, secret)
            .env(BASE_URL_VAR, "http://user:hunter2@127.0.0.1:9");
    };
    let mut command = sb.command(&sb.run_args(&[]));
    // (`--simulate-model` is part of run_args; run the real-provider variant by hand.)
    drop(command);
    command = sb.command(&[
        "run",
        "--repo",
        sb.repo.to_str().expect("utf-8"),
        "--job",
        "x",
    ]);
    env(&mut command);
    let run = Spawned::start(command, sb.dir.path());
    sb.wait_for("the supervisor line", || {
        run.output().0.contains("supervising with").then_some(())
    });
    let (stdout, _) = run.output();
    assert!(
        stdout.contains("supervising with jev-latest at http://127.0.0.1:9"),
        "{stdout}"
    );
    assert!(stdout.contains("(loopback)"), "{stdout}");
    let id = sb.wait_for_run_with_status("running");
    assert_eq!(sb.run(&["cancel", &id]).code, 0);
    let out = run.wait(PATIENCE);
    assert_eq!(out.code, 1, "{}", out.all());
    // The key and the URL's password are nowhere: not in output, log, or run files.
    for needle in [secret, "hunter2"] {
        assert!(!out.all().contains(needle));
        let hits = tree_contains(&sb.data, needle);
        assert!(hits.is_empty(), "{needle} found in {hits:?}");
    }
}

#[test]
fn a_daemon_serving_another_state_directory_is_a_hard_error() {
    let sb = Sandbox::new();
    assert_eq!(sb.run(&sb.run_args(&[])).code, 0);
    let other = sb.mkdir("other-state");
    let socket = sb.socket.to_str().expect("utf-8").to_owned();
    let out = sb.bare_run(
        &sb.repo,
        &[
            "--socket",
            &socket,
            "--data-dir",
            other.to_str().expect("utf-8"),
            "ls",
        ],
    );
    assert_eq!(out.code, 2, "{}", out.all());
    assert!(
        out.stderr.contains("serves the state directory"),
        "{}",
        out.stderr
    );
    // `status` still works, and flags it.
    let status = sb.bare_run(
        &sb.repo,
        &[
            "--socket",
            &socket,
            "--data-dir",
            other.to_str().expect("utf-8"),
            "status",
        ],
    );
    assert_eq!(status.code, 0, "{}", status.all());
    assert!(status.stdout.contains("NOTE"), "{}", status.stdout);

    // A second daemon on the same state directory but another socket is refused, and
    // the user is told which daemon owns it (no start loop).
    let second_socket = sb.dir.path().join("t.sock");
    let refused = sb.bare_run(
        &sb.repo,
        &[
            "--socket",
            second_socket.to_str().expect("utf-8"),
            "--data-dir",
            sb.data.to_str().expect("utf-8"),
            "ls",
        ],
    );
    assert_eq!(refused.code, 2, "{}", refused.all());
    assert!(
        refused
            .stderr
            .contains("already serves the state directory"),
        "{}",
        refused.stderr
    );
    assert!(!second_socket.exists());
}

#[test]
fn the_embedded_path_validates_like_the_daemon_does() {
    let sb = Sandbox::new();
    let repo = sb.repo.to_str().expect("utf-8").to_owned();
    for args in [
        vec![
            "--no-daemon",
            "run",
            "--repo",
            &repo,
            "--job",
            "",
            "--simulate-model",
        ],
        vec![
            "--no-daemon",
            "run",
            "--repo",
            &repo,
            "--job",
            "   ",
            "--simulate-model",
        ],
    ] {
        let out = sb.run(&args);
        assert_eq!(out.code, 2, "{}", out.all());
        assert!(out.stderr.contains("invalid_request"), "{}", out.stderr);
    }
    let big = sb.dir.path().join("big.txt");
    std::fs::write(&big, vec![b'a'; 300 * 1024]).expect("big job");
    let out = sb.run(&[
        "--no-daemon",
        "run",
        "--repo",
        &repo,
        "--job-file",
        big.to_str().expect("utf-8"),
        "--simulate-model",
    ]);
    assert_eq!(out.code, 2, "{}", out.all());
    assert!(out.stderr.contains("limit"), "{}", out.stderr);
    // /dev/zero is a size error, not an out-of-memory.
    let out = sb.run(&[
        "run",
        "--repo",
        &repo,
        "--job-file",
        "/dev/zero",
        "--simulate-model",
    ]);
    assert_eq!(out.code, 2, "{}", out.all());
    assert!(!sb.socket.exists(), "refused before any daemon was started");
}

#[test]
fn an_embedded_worker_that_cannot_launch_leaves_a_failed_run_not_a_running_one() {
    let sb = Sandbox::new();
    let args = sb.run_args(&[
        "--no-daemon",
        "--worker",
        "generic",
        "--external-sandbox",
        "--cmd",
        "/nonexistent/aftman-test-binary {repo}",
    ]);
    let out = sb.run(&args);
    assert_eq!(out.code, 1, "{}", out.all());
    assert!(out.stderr.contains("run failed"), "{}", out.stderr);
    let ls = sb.run(&["--no-daemon", "ls"]);
    assert!(ls.stdout.contains("failed"), "{}", ls.stdout);
    assert!(!ls.stdout.contains("running"), "{}", ls.stdout);
}

#[test]
fn sigterm_cancels_an_embedded_run_and_reaps_its_worker() {
    let sb = Sandbox::new();
    let (cmd_flag, cmd) = sleeper(&tag("600.401"));
    let args = sb.run_args(&[
        "--no-daemon",
        "--worker",
        "generic",
        "--external-sandbox",
        &cmd_flag,
        &cmd,
    ]);
    let run = sb.spawn(&args);
    sb.wait_for("the worker to start", || {
        (!sleepers(&tag("600.401")).is_empty()).then_some(())
    });
    run.signal(Signal::SIGTERM);
    let out = run.wait(PATIENCE);
    assert_eq!(out.code, 143, "{}", out.all());
    assert!(out.stderr.contains("interrupted"), "{}", out.stderr);
    sb.wait_for("the worker to be reaped", || {
        sleepers(&tag("600.401")).is_empty().then_some(())
    });
    let ls = sb.run(&["--no-daemon", "ls"]);
    assert!(ls.stdout.contains("cancelled"), "{}", ls.stdout);
    assert!(!ls.stdout.contains("running"), "{}", ls.stdout);
}

#[test]
fn worker_output_cannot_drive_the_operators_terminal() {
    let sb = Sandbox::new();
    // The worker prints a title change, a clipboard write, a clear-screen, and a CR
    // overwrite.
    let payload = sb.dir.path().join("payload.bin");
    std::fs::write(
        &payload,
        "\u{1b}]0;hacked\u{7}\u{1b}]52;c;cm0gLXJm\u{7}\u{1b}[2Jvisible\rovertyped\n",
    )
    .expect("payload");
    let cmd = format!("cat {}", payload.display());
    let out = sb.run(&sb.run_args(&["--worker", "generic", "--external-sandbox", "--cmd", &cmd]));
    assert_eq!(out.code, 3, "{}", out.all());
    let id = run_id_of(&out);
    for args in [
        vec!["logs", id.as_str()],
        vec!["--no-daemon", "logs", id.as_str()],
    ] {
        let logs = sb.run(&args);
        assert_eq!(logs.code, 0, "{}", logs.all());
        assert!(logs.stdout.contains("visible"), "{:?}", logs.stdout);
        assert!(
            !logs.stdout.contains('\u{1b}') && !logs.stdout.contains('\u{7}'),
            "{:?}",
            logs.stdout
        );
        assert!(
            !logs.stdout.contains("hacked") && !logs.stdout.contains("cm0g"),
            "{:?}",
            logs.stdout
        );
        assert!(
            logs.stdout.contains("visible\novertyped"),
            "CR becomes a newline: {:?}",
            logs.stdout
        );
    }
    // The terminal view of the run is clean too.
    assert!(!out.stdout.contains('\u{1b}'), "{:?}", out.stdout);
}

#[test]
fn purge_leaves_no_trace_of_the_job_and_refuses_to_run_under_a_daemon() {
    let sb = Sandbox::new();
    let unique = "UNIQUE-JOB-TEXT-5c1e9a";
    let out = sb.run(&[
        "run",
        "--repo",
        sb.repo.to_str().expect("utf-8"),
        "--job",
        unique,
        "--simulate-model",
        "--worker",
        "generic",
        "--external-sandbox",
        "--cmd",
        "true {repo}",
    ]);
    assert_eq!(out.code, 3, "{}", out.all());
    let id = run_id_of(&out);
    // While a daemon serves the directory, a --no-daemon purge would strand its ring.
    let refused = sb.run(&["--no-daemon", "purge", &id]);
    assert_eq!(refused.code, 2, "{}", refused.all());
    assert!(
        refused.stderr.contains("is serving this state directory"),
        "{}",
        refused.stderr
    );
    assert!(
        !tree_contains(&sb.data, unique).is_empty(),
        "the job text was stored"
    );

    assert_eq!(sb.run(&["cancel", &id]).code, 0);
    sb.wait_for("the run to end", || {
        sb.run(&["show", &id])
            .stdout
            .contains("status      cancelled")
            .then_some(())
    });
    let purge = sb.run(&["purge", "--all"]);
    assert_eq!(purge.code, 0, "{}", purge.all());
    let left: Vec<_> = tree_contains(&sb.data, unique)
        .into_iter()
        .filter(|p| !p.ends_with("daemon.log"))
        .collect();
    assert!(left.is_empty(), "the job text survives in {left:?}");
    assert!(
        sb.run(&["logs", &id]).code == 2,
        "the purged run is unknown"
    );
}

#[test]
fn everything_aftman_creates_is_private_on_every_path() {
    for embedded in [false, true] {
        let sb = Sandbox::new();
        let args = if embedded {
            sb.run_args(&["--no-daemon"])
        } else {
            sb.run_args(&[])
        };
        assert_eq!(sb.run(&args).code, 0);
        for sub in ["", "missions", "evidence-tmp", "runs"] {
            assert_eq!(
                sb.mode(&sb.data.join(sub)),
                0o700,
                "embedded={embedded} {sub:?}"
            );
        }
        if !embedded {
            assert_eq!(sb.mode(&sb.data.join("daemon.log")), 0o600);
        }
    }

    // A directory the user made is not chmodded, but what goes in it is private.
    use std::os::unix::fs::PermissionsExt as _;
    let sb = Sandbox::new();
    std::fs::create_dir_all(&sb.data).expect("mkdir");
    std::fs::set_permissions(&sb.data, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    assert_eq!(sb.run(&sb.run_args(&[])).code, 0);
    assert_eq!(sb.mode(&sb.data), 0o755);
    assert_eq!(sb.mode(&sb.data.join("runs")), 0o700);
    assert_eq!(sb.mode(&sb.data.join("daemon.log")), 0o600);
}

#[test]
fn an_oversized_daemon_log_is_rotated_at_start() {
    let sb = Sandbox::new();
    std::fs::create_dir_all(&sb.data).expect("mkdir");
    std::fs::write(sb.data.join("daemon.log"), vec![b'x'; 5 * 1024 * 1024]).expect("big log");
    assert_eq!(sb.run(&sb.run_args(&[])).code, 0);
    let old = std::fs::metadata(sb.data.join("daemon.log.old")).expect("rotated");
    assert_eq!(old.len(), 5 * 1024 * 1024);
    let fresh = std::fs::metadata(sb.data.join("daemon.log")).expect("new log");
    assert!(fresh.len() < 64 * 1024, "{}", fresh.len());
}

#[test]
fn auto_approve_is_warned_about_and_recorded() {
    for embedded in [false, true] {
        let sb = Sandbox::new();
        let mut extra = vec!["--permissions", "auto-approve"];
        if embedded {
            extra.push("--no-daemon");
        }
        let args = sb.run_args(&extra);
        let out = sb.run_with(&args, |c| {
            c.env("AFTMAN_ACK_UNSANDBOXED", "1");
        });
        assert_eq!(out.code, 0, "{}", out.all());
        assert!(
            out.stderr.contains("WARNING: --permissions auto-approve"),
            "{}",
            out.stderr
        );
        // --quiet does not hide it.
        let mut quiet = vec!["--quiet", "--permissions", "auto-approve"];
        if embedded {
            quiet.push("--no-daemon");
        }
        let args = sb.run_args(&quiet);
        let out = sb.run_with(&args, |c| {
            c.env("AFTMAN_ACK_UNSANDBOXED", "1");
        });
        assert!(out.stderr.contains("WARNING"), "{}", out.stderr);

        let id = run_id_of(&out);
        let show = sb.run(&if embedded {
            vec!["--no-daemon", "show", &id]
        } else {
            vec!["show", &id]
        });
        assert!(
            show.stdout.contains("permissions auto-approve"),
            "{}",
            show.stdout
        );
        assert!(show.stdout.contains("WARNING"), "{}", show.stdout);
        let hits = tree_contains(&sb.data.join("runs"), "\"permissions\":\"auto-approve\"");
        assert!(!hits.is_empty(), "the posture is in the run's log");
    }
    // A sandboxed run carries no warning.
    let sb = Sandbox::new();
    let out = sb.run(&sb.run_args(&[]));
    assert!(!out.all().contains("WARNING"), "{}", out.all());
    assert!(
        sb.run(&["show", "--last"])
            .stdout
            .contains("permissions sandboxed")
    );
}

#[test]
fn a_closed_stdout_ends_quietly() {
    let sb = Sandbox::new();
    assert_eq!(sb.run(&sb.run_args(&["--no-daemon"])).code, 0);
    let (reader, writer) = std::io::pipe().expect("pipe");
    drop(reader);
    let mut command = sb.command(&["--no-daemon", "ls"]);
    command.stdout(writer).stderr(Stdio::piped());
    let output = command.output().expect("running ls");
    assert_eq!(output.status.code(), Some(0), "{:?}", output);
    assert!(
        output.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn logs_follow_on_a_run_the_daemon_is_not_driving_ends_with_it() {
    let sb = Sandbox::new();
    // An embedded run owns this run; the daemon will not push its events.
    let (cmd_flag, cmd) = sleeper(&tag("600.402"));
    let args = sb.run_args(&[
        "--no-daemon",
        "--worker",
        "generic",
        "--external-sandbox",
        &cmd_flag,
        &cmd,
    ]);
    let embedded = sb.spawn(&args);
    let id = sb.wait_for("the embedded run", || {
        let ls = sb.run(&["--no-daemon", "ls", "--status", "running"]);
        ls.stdout
            .lines()
            .nth(1)
            .and_then(|l| l.split_whitespace().next())
            .map(ToOwned::to_owned)
    });
    // Start a daemon on the same state directory.
    assert_eq!(sb.run(&sb.run_args(&[])).code, 0);

    let follower = sb.spawn(&["logs", &id, "-f"]);
    sb.wait_for("the follower to notice", || {
        follower
            .output()
            .1
            .contains("not being driven")
            .then_some(())
    });
    embedded.signal(Signal::SIGTERM);
    assert_eq!(embedded.wait(PATIENCE).code, 143);
    let done = follower.wait(PATIENCE);
    assert_eq!(done.code, 0, "{}", done.all());
}

#[test]
fn status_says_when_a_daemon_holds_its_lock_but_does_not_answer() {
    use nix::fcntl::{Flock, FlockArg};
    let sb = Sandbox::new();
    std::fs::create_dir_all(sb.socket.parent().expect("parent")).expect("mkdir");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(sb.pid_file())
        .expect("pid file");
    let _lock = Flock::lock(file, FlockArg::LockExclusiveNonblock).expect("lock");
    let status = sb.run(&["status"]);
    assert_eq!(status.code, 1, "{}", status.all());
    assert!(
        status.stdout.contains("starting up or shutting down"),
        "{}",
        status.stdout
    );
    assert!(
        !status.stdout.contains("is not running"),
        "{}",
        status.stdout
    );
}

// ===========================================================================
// Final-review fixes, part 1
// ===========================================================================

#[test]
fn an_embedded_run_that_needs_a_human_ends_with_exit_three() {
    // A control channel is attached (so a signal can cancel the run) but nothing can
    // answer it: NeedsHuman must end the run with the question recorded, not park it.
    let sb = Sandbox::new();
    let args = sb.run_args(&[
        "--no-daemon",
        "--worker",
        "generic",
        "--cmd",
        "true {repo}",
        "--external-sandbox",
    ]);
    let out = sb.run(&args); // `wait` has a deadline: a hang is a failure, not a stall
    assert_eq!(out.code, 3, "{}", out.all());
    assert!(out.stdout.contains("ESCALATION_RAISED"), "{}", out.stdout);
    assert!(out.stdout.contains("RUN_FINISHED"), "{}", out.stdout);
    assert_eq!(after(&out.stdout, "status"), Some("needs_human"));
    assert!(!sb.socket.exists(), "no daemon was involved");

    // The question is on record, and the run is over (nobody is driving it).
    let id = run_id_of(&out);
    let show = sb.run(&["--no-daemon", "show", &id]);
    assert!(show.stdout.contains("open escalation"), "{}", show.stdout);
    assert!(
        !show.stdout.contains("aftman answer"),
        "no answer hint for a run nobody can answer: {}",
        show.stdout
    );
}

#[test]
fn the_other_embedded_outcomes_still_behave() {
    let sb = Sandbox::new();
    // Finished.
    let done = sb.run(&sb.run_args(&["--no-daemon"]));
    assert_eq!(done.code, 0, "{}", done.all());
    // Failed (the worker cannot launch).
    let failed = sb.run(&sb.run_args(&[
        "--no-daemon",
        "--worker",
        "generic",
        "--external-sandbox",
        "--cmd",
        "/nonexistent/aftman-test-binary {repo}",
    ]));
    assert_eq!(failed.code, 1, "{}", failed.all());
    // Cancelled by a signal, while the worker is running.
    let (cmd_flag, cmd) = sleeper(&tag("600.411"));
    let args = sb.run_args(&[
        "--no-daemon",
        "--worker",
        "generic",
        "--external-sandbox",
        &cmd_flag,
        &cmd,
    ]);
    let run = sb.spawn(&args);
    sb.wait_for("the worker", || {
        (!sleepers(&tag("600.411")).is_empty()).then_some(())
    });
    run.signal(Signal::SIGINT);
    let out = run.wait(PATIENCE);
    assert_eq!(out.code, 130, "{}", out.all());
    let ls = sb.run(&["--no-daemon", "ls"]);
    assert!(ls.stdout.contains("cancelled"), "{}", ls.stdout);
    assert!(ls.stdout.contains("failed") && ls.stdout.contains("finished"));
}

/// A worker that ignores the polite interrupt (so ending it costs the full grace) and
/// would outlive its supervisor.
fn stubborn_worker(sb: &Sandbox) -> (PathBuf, String) {
    let script = sb.dir.path().join("stubborn.sh");
    std::fs::write(
        &script,
        "#!/bin/sh\ntrap '' INT\nwhile :; do sleep 1; done\n",
    )
    .expect("script");
    let cmd = format!("sh {}", script.display());
    (script, cmd)
}

#[test]
fn the_first_command_after_a_crash_waits_for_recovery_and_followers_survive_it() {
    let sb = Sandbox::new();
    let (script, cmd) = stubborn_worker(&sb);
    let args = sb.run_args(&["--worker", "generic", "--external-sandbox", "--cmd", &cmd]);
    let long = sb.spawn(&args);
    let id = sb.wait_for_run_with_status("running");
    let pid = sb.status_pid().expect("the daemon is up");
    sb.wait_for("the stubborn worker", || {
        (!processes_mentioning(script.to_str().expect("utf-8")).is_empty()).then_some(())
    });

    // A follower attached across the crash.
    let mut follower = sb.spawn(&["logs", &id, "-f"]);
    std::thread::sleep(Duration::from_millis(600));
    assert!(
        follower.try_wait().is_none(),
        "the follower is attached and waiting"
    );

    // The daemon dies with the worker still running (its group ignores SIGINT).
    kill(Pid::from_raw(pid as i32), Signal::SIGKILL).expect("kill the daemon");
    sb.wait_for("the daemon to die", || (!alive(pid)).then_some(()));
    drop(long);

    // The first command afterwards starts a new daemon. Recovery gives the stubborn
    // worker its grace (about 5 s) before the socket exists; the command must wait for
    // it, say so, and succeed.
    let began = Instant::now();
    let ls = sb.run(&["ls"]);
    assert_eq!(ls.code, 0, "{}", ls.all());
    assert!(
        began.elapsed() > Duration::from_secs(2),
        "recovery really did take a while ({:?})",
        began.elapsed()
    );
    assert!(ls.stdout.contains("failed"), "{}", ls.stdout);
    assert!(ls.stderr.contains("recovering"), "{}", ls.stderr);
    sb.wait_for("the orphan to be gone", || {
        processes_mentioning(script.to_str().expect("utf-8"))
            .is_empty()
            .then_some(())
    });

    // The follower rode out the outage and ended with the run.
    let done = follower.wait(PATIENCE);
    assert_eq!(done.code, 0, "{}", done.all());
    // The orphan's fate is on the record, truthfully.
    assert!(!tree_contains(&sb.data.join("runs"), "\"cause\":\"orphaned\"").is_empty());
    assert!(!tree_contains(&sb.data.join("runs"), "\"result\":\"killed\"").is_empty());
}

// ===========================================================================
// Final-review fixes, part 2
// ===========================================================================

#[test]
fn purge_after_a_crash_ends_the_orphan_and_leaves_no_trace_of_the_job() {
    let sb = Sandbox::new();
    let (script, cmd) = stubborn_worker(&sb);
    let unique = "UNIQUE-JOB-TEXT-77aa31";
    let long = sb.spawn(&[
        "run",
        "--repo",
        sb.repo.to_str().expect("utf-8"),
        "--job",
        unique,
        "--simulate-model",
        "--worker",
        "generic",
        "--external-sandbox",
        "--cmd",
        &cmd,
    ]);
    let id = sb.wait_for_run_with_status("running");
    let pid = sb.status_pid().expect("the daemon is up");
    let script_path = script.to_str().expect("utf-8").to_owned();
    sb.wait_for("the worker", || {
        (!processes_mentioning(&script_path).is_empty()).then_some(())
    });
    assert!(
        !tree_contains(&sb.data.join("missions"), unique).is_empty(),
        "the mission file holds the job text while the worker runs"
    );

    // The daemon dies mid-run; nothing has recovered the run yet.
    kill(Pid::from_raw(pid as i32), Signal::SIGKILL).expect("kill the daemon");
    sb.wait_for("the daemon to die", || (!alive(pid)).then_some(()));
    drop(long);
    assert!(
        !processes_mentioning(&script_path).is_empty(),
        "the orphan lives on"
    );

    // Purging without a daemon does what recovery would: ends the worker, removes its
    // mission file, and only then deletes the run.
    let purge = sb.run(&["--no-daemon", "purge", &id]);
    assert_eq!(purge.code, 0, "{}", purge.all());
    assert!(purge.stdout.contains("purged 1"), "{}", purge.stdout);
    assert!(
        tree_contains(&sb.data, unique).is_empty(),
        "the job text survives under the state dir: {:?}",
        tree_contains(&sb.data, unique)
    );
    sb.wait_for("the orphan to be gone", || {
        processes_mentioning(&script_path).is_empty().then_some(())
    });
}

#[test]
fn a_daemon_purge_of_a_crashed_run_also_leaves_nothing() {
    // Same, through a daemon: restart recovery has already failed the run and reaped the
    // worker; the purge must still leave no mission file or job text.
    let sb = Sandbox::new();
    let (script, cmd) = stubborn_worker(&sb);
    let unique = "UNIQUE-JOB-TEXT-5d2c90";
    let long = sb.spawn(&[
        "run",
        "--repo",
        sb.repo.to_str().expect("utf-8"),
        "--job",
        unique,
        "--simulate-model",
        "--worker",
        "generic",
        "--external-sandbox",
        "--cmd",
        &cmd,
    ]);
    let id = sb.wait_for_run_with_status("running");
    let pid = sb.status_pid().expect("the daemon is up");
    let script_path = script.to_str().expect("utf-8").to_owned();
    sb.wait_for("the worker", || {
        (!processes_mentioning(&script_path).is_empty()).then_some(())
    });
    kill(Pid::from_raw(pid as i32), Signal::SIGKILL).expect("kill the daemon");
    sb.wait_for("the daemon to die", || (!alive(pid)).then_some(()));
    drop(long);

    let purge = sb.run(&["purge", &id]);
    assert_eq!(purge.code, 0, "{}", purge.all());
    assert!(tree_contains(&sb.data, unique).is_empty());
    sb.wait_for("the orphan to be gone", || {
        processes_mentioning(&script_path).is_empty().then_some(())
    });
}

#[test]
fn stop_and_auto_start_during_a_daemon_shutdown_wait_for_it() {
    let sb = Sandbox::new();
    let (_script, cmd) = stubborn_worker(&sb);
    // A worker that ignores the polite interrupt makes the shutdown take a while.
    let long =
        sb.spawn(&sb.run_args(&["--worker", "generic", "--external-sandbox", "--cmd", &cmd]));
    let _id = sb.wait_for_run_with_status("running");
    let pid = sb.status_pid().expect("the daemon is up");

    let first_stop = sb.spawn(&["stop"]);
    sb.wait_for("the daemon to be shutting down", || {
        let status = sb.run(&["status"]);
        status
            .stdout
            .contains("starting up or shutting down")
            .then_some(())
    });
    let status = sb.run(&["status"]);
    assert_eq!(status.code, 1, "{}", status.all());
    assert!(
        status.stdout.contains(&format!("pid {pid}")),
        "{}",
        status.stdout
    );

    // A second stop does not claim nothing is running; it waits for the first to finish.
    let second_stop = sb.run(&["stop"]);
    assert_eq!(second_stop.code, 0, "{}", second_stop.all());
    assert!(
        !second_stop.stdout.contains("is not running"),
        "{}",
        second_stop.stdout
    );

    // A command that needs a daemon waits for the old one to finish, then starts a new
    // one (a different pid), instead of failing or racing it.
    let ls = sb.run(&["ls"]);
    assert_eq!(ls.code, 0, "{}", ls.all());
    let new_pid = sb.status_pid().expect("a new daemon");
    assert_ne!(new_pid, pid);
    let done = first_stop.wait(PATIENCE);
    assert_eq!(done.code, 0, "{}", done.all());
    drop(long);
}

#[test]
fn ctrl_c_while_the_run_is_being_started_is_effective_and_says_what_may_have_happened() {
    use std::io::{BufRead as _, BufReader, Write as _};
    use std::os::unix::net::UnixListener;
    let sb = Sandbox::new();
    std::fs::create_dir_all(&sb.data).expect("data dir");
    // A daemon that answers `daemon.status` correctly and then never answers `run.start`.
    let listener = UnixListener::bind(&sb.socket).expect("bind");
    let data = sb.data.display().to_string();
    let started = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seen = started.clone();
    std::thread::spawn(move || {
        let Ok((stream, _)) = listener.accept() else {
            return;
        };
        let mut writer = stream.try_clone().expect("clone");
        for line in BufReader::new(stream).lines().map_while(Result::ok) {
            let value: serde_json::Value = serde_json::from_str(&line).unwrap_or_default();
            let id = value["id"].as_u64().unwrap_or(0);
            match value["method"].as_str() {
                Some("daemon.status") => {
                    let reply = serde_json::json!({"id": id, "error": null, "result": {
                        "version": env!("CARGO_PKG_VERSION"), "uptime_secs": 1, "pid": 1,
                        "runs_total": 0, "runs_active": 0, "state_dir": data,
                    }});
                    let _ = writeln!(writer, "{reply}");
                }
                Some("run.start") => {
                    seen.store(true, std::sync::atomic::Ordering::SeqCst);
                    std::thread::sleep(Duration::from_secs(120));
                }
                _ => {}
            }
        }
    });
    let run = sb.spawn(&sb.run_args(&[]));
    sb.wait_for("run.start to be sent", || {
        started
            .load(std::sync::atomic::Ordering::SeqCst)
            .then_some(())
    });
    run.signal(Signal::SIGINT);
    let out = run.wait(Duration::from_secs(10)); // effective at once, not after the 30 s timeout
    assert_eq!(out.code, 130, "{}", out.all());
    assert!(
        out.stderr.contains("may have been started"),
        "{}",
        out.stderr
    );
    assert!(out.stderr.contains("aftman ls"), "{}", out.stderr);
}

// ===========================================================================
// Integration: an abandoned run ends its followers
// ===========================================================================

#[test]
fn followers_of_a_run_the_daemon_could_not_record_exit_with_the_reason() {
    if nix::unistd::geteuid().is_root() {
        return; // root ignores file modes: the log would stay writable
    }
    use std::os::unix::fs::PermissionsExt as _;
    let sb = Sandbox::new();
    let (cmd_flag, cmd) = sleeper(&tag("600.421"));
    let args = sb.run_args(&["--worker", "generic", "--external-sandbox", &cmd_flag, &cmd]);
    let run = sb.spawn(&args);
    let id = sb.wait_for_run_with_status("running");
    let follower = sb.spawn(&["logs", &id, "-f"]);
    std::thread::sleep(Duration::from_millis(500));

    // The run's log becomes unwritable mid-run; the next thing the run records (here, a
    // cancel) cannot be written, and the daemon gives the run up.
    let log = sb.data.join("runs").join(&id).join("events.jsonl");
    std::fs::set_permissions(&log, std::fs::Permissions::from_mode(0o444)).expect("chmod");
    let _ = sb.run(&["cancel", &id]);

    let ran = run.wait(PATIENCE);
    assert_ne!(ran.code, 0, "{}", ran.all());
    assert!(
        ran.stderr.contains("abandoned") || ran.stdout.contains("abandoned"),
        "the reason is printed: {}",
        ran.all()
    );
    let followed = follower.wait(PATIENCE);
    assert_ne!(followed.code, 0, "{}", followed.all());
    assert!(followed.stderr.contains("abandoned"), "{}", followed.stderr);
    std::fs::set_permissions(&log, std::fs::Permissions::from_mode(0o644)).expect("chmod back");
}

// ---------------------------------------------------------------------------
// M3 follow-up: a run forwards the *calling* shell's environment.
//
// The daemon is started by the first client that needs it and keeps that shell's
// environment for life. Before forwarding, a run from a second shell therefore used the
// first shell's `PATH`, virtualenv, and anything else exported there — proved in the M3
// review with a `GITHUB_TOKEN` exported in only one shell reaching every later worker.
// ---------------------------------------------------------------------------

#[test]
fn a_worker_sees_the_calling_shell_environment_not_the_daemons() {
    let sb = Sandbox::new();
    let real_path = std::env::var("PATH").unwrap_or_default();
    let shell_a = format!("/shell-a/bin:{real_path}");
    let shell_b = format!("/shell-b/bin:{real_path}");

    // Start the daemon from shell A: this process's environment is what it keeps.
    let started = sb.run_with(
        &sb.run_args(&["--worker", "generic", "--external-sandbox", "--cmd", "true"]),
        |c| {
            c.env("PATH", &shell_a)
                .env("SHELL_A_ONLY_TOKEN", "canary-not-allowlisted");
        },
    );
    assert_eq!(started.code, 3, "the first job runs: {}", started.all());

    // Now run from shell B, which has a different PATH and its own variable.
    let cmd = "sh -c 'echo worker-path=$PATH; if [ -n \"$SHELL_A_ONLY_TOKEN\" ]; then echo shell-a-token-present; fi'";
    let out = sb.run_with(
        &sb.run_args(&["--worker", "generic", "--external-sandbox", "--cmd", cmd]),
        |c| {
            c.env("PATH", &shell_b);
        },
    );
    let id = run_id_of(&out);
    let logs = sb.run(&["logs", id.as_str()]);
    assert_eq!(logs.code, 0, "{}", logs.all());

    assert!(
        logs.stdout.contains("/shell-b/bin:"),
        "the worker must use the calling shell's PATH:\n{}",
        logs.stdout
    );
    assert!(
        !logs.stdout.contains("/shell-a/bin:"),
        "the daemon's own shell must not leak into a later run:\n{}",
        logs.stdout
    );
    assert!(
        !logs.stdout.contains("shell-a-token-present"),
        "a variable that was never allowlisted must not be forwarded:\n{}",
        logs.stdout
    );
}

#[test]
fn pass_env_forwards_a_named_variable_and_refuses_a_supervisor_one() {
    let sb = Sandbox::new();
    let cmd = "sh -c 'if [ -n \"$CARGO_TARGET_DIR\" ]; then echo forwarded=$CARGO_TARGET_DIR; fi'";

    let out = sb.run_with(
        &sb.run_args(&[
            "--worker",
            "generic",
            "--external-sandbox",
            "--cmd",
            cmd,
            "--pass-env",
            "CARGO_TARGET_DIR",
        ]),
        |c| {
            c.env("CARGO_TARGET_DIR", "/forwarded-target");
        },
    );
    let id = run_id_of(&out);
    let logs = sb.run(&["logs", id.as_str()]);
    assert!(
        logs.stdout.contains("forwarded=/forwarded-target"),
        "--pass-env must forward the named variable:\n{}",
        logs.stdout
    );

    // A name from the supervisor's own namespace is refused, and the refusal must not
    // echo the value. `AFTMAN_*` is the supervisor's namespace, so naming it is not a
    // hard-rule-2 concern.
    let refused = sb.run_with(
        &sb.run_args(&[
            "--worker",
            "generic",
            "--external-sandbox",
            "--cmd",
            "true",
            "--pass-env",
            "AFTMAN_WEB_TOKEN",
        ]),
        |c| {
            c.env("AFTMAN_WEB_TOKEN", "canary-token-value");
        },
    );
    assert_ne!(refused.code, 0, "a supervisor name must be refused");
    assert!(
        refused.all().contains("belongs to the supervisor"),
        "{}",
        refused.all()
    );
    assert!(
        !refused.all().contains("canary-token-value"),
        "the refusal must not echo the value: {}",
        refused.all()
    );

    // A name that is unset in this shell is an error rather than a silent no-op.
    let unset = sb.run(&sb.run_args(&[
        "--worker",
        "generic",
        "--external-sandbox",
        "--cmd",
        "true",
        "--pass-env",
        "DEFINITELY_NOT_SET_ANYWHERE",
    ]));
    assert_ne!(unset.code, 0, "an unset name must be refused");
    assert!(unset.all().contains("not set"), "{}", unset.all());
}
