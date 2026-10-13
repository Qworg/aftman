//! `aftman watch` as the built binary, where stdout is not a terminal: the plain
//! line-per-event fallback. No escape sequence may reach a pipe or a file, whatever the
//! run printed.
//!
//! Hermetic and offline, like `e2e.rs`: a cleared environment with `HOME`/`XDG_*` in a
//! temp directory, an explicit `--socket` and `--data-dir`, the simulated model, and
//! the simulated worker (or `sh` behind the `generic` backend for a run that waits).
//! Every sandbox stops its daemon when dropped, then kills whatever still serves on
//! its socket. Every wait has a deadline.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;

const PATIENCE: Duration = Duration::from_secs(30);

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

    fn run(&self, args: &[&str]) -> Out {
        Spawned::start(self.command(args), self.dir.path()).wait(PATIENCE)
    }

    fn spawn(&self, args: &[&str]) -> Spawned {
        Spawned::start(self.command(args), self.dir.path())
    }

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
        for pid in processes_mentioning(self.repo.to_str().unwrap_or("/nonexistent")) {
            let _ = kill(Pid::from_raw(pid as i32), Signal::SIGKILL);
        }
    }
}

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
        if String::from_utf8_lossy(&cmdline).contains(needle) {
            found.push(pid);
        }
    }
    found
}

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

fn assert_no_escapes(out: &Out) {
    assert!(
        !out.stdout.contains('\u{1b}') && !out.stderr.contains('\u{1b}'),
        "an escape sequence was written to a non-terminal:\n{}",
        out.all().escape_debug()
    );
}

#[test]
fn watching_a_finished_run_without_a_terminal_prints_the_events_and_exits_zero() {
    let sb = Sandbox::new();
    let run = sb.run(&sb.run_args(&["--worker", "simulated"]));
    assert_eq!(run.code, 0, "{}", run.all());

    let watch = sb.run(&["watch", "--last"]);
    assert_eq!(watch.code, 0, "{}", watch.all());
    assert_no_escapes(&watch);
    for needle in [
        "RUN_CREATED",
        "WORKER_STARTED",
        "DIRECTIVE_SELECTED",
        "reason:",
        "RUN_FINISHED",
    ] {
        assert!(watch.stdout.contains(needle), "{needle}:\n{}", watch.stdout);
    }
}

#[test]
fn watch_prefix_and_unknown_run_follow_the_cli_conventions() {
    let sb = Sandbox::new();
    let run = sb.run(&sb.run_args(&["--worker", "simulated"]));
    assert_eq!(run.code, 0, "{}", run.all());
    let id = run
        .stdout
        .lines()
        .find_map(|l| l.strip_prefix("run id").map(|r| r.trim().to_owned()))
        .unwrap_or_else(|| panic!("no run id in:\n{}", run.all()));

    let by_prefix = sb.run(&["watch", &id[..8]]);
    assert_eq!(by_prefix.code, 0, "{}", by_prefix.all());

    let unknown = sb.run(&["watch", "0193a000-0000-7000-8000-000000000001"]);
    assert_eq!(unknown.code, 2, "{}", unknown.all());
    assert!(unknown.stderr.contains("unknown_run"), "{}", unknown.stderr);
    assert_no_escapes(&unknown);

    let neither = sb.run(&["watch"]);
    assert_eq!(neither.code, 2, "{}", neither.all());

    let embedded = sb.run(&["--no-daemon", "watch", "--last"]);
    assert_eq!(embedded.code, 2, "{}", embedded.all());
    assert!(
        embedded.stderr.contains("watch"),
        "a clear message: {}",
        embedded.stderr
    );
}

#[test]
fn ctrl_c_detaches_a_plain_watch_and_the_run_keeps_going() {
    let sb = Sandbox::new();
    let cmd = "sh -c 'sleep 600.401' {repo}".to_owned();
    let args = sb.run_args(&["--worker", "generic", "--external-sandbox", "--cmd", &cmd]);
    let long = sb.spawn(&args);
    let id = sb.wait_for_run_with_status("running");

    let watch = sb.spawn(&["watch", &id]);
    sb.wait_for("the watch to print the run's history", || {
        let (stdout, _) = watch.output();
        stdout.contains("WORKER_STARTED").then_some(())
    });
    watch.signal(Signal::SIGINT);
    let out = watch.wait(PATIENCE);
    assert_eq!(out.code, 130, "{}", out.all());
    assert!(out.stdout.contains("detached"), "{}", out.stdout);
    assert_no_escapes(&out);

    // The run was not touched.
    let show = sb.run(&["show", &id]);
    assert!(show.stdout.contains("running"), "{}", show.stdout);
    assert_eq!(sb.run(&["cancel", &id]).code, 0);
    let ended = long.wait(PATIENCE);
    assert_ne!(ended.code, 130, "{}", ended.all());
}

#[test]
fn watch_last_with_no_runs_and_a_hostile_run_argument_are_plain_usage_errors() {
    let sb = Sandbox::new();
    let none = sb.run(&["watch", "--last"]);
    assert_eq!(none.code, 2, "{}", none.all());
    assert!(none.stderr.contains("no runs"), "{}", none.stderr);
    assert_no_escapes(&none);

    let hostile = sb.run(&["watch", "zz\u{1b}]0;pwned\u{7}zz"]);
    assert_eq!(hostile.code, 2, "{}", hostile.all());
    assert_no_escapes(&hostile);
    assert!(!hostile.all().contains("pwned\u{7}"), "{}", hostile.all());
}
