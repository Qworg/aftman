//! Operating aftman: `responsibilities`, `usage`, `doctor`.
//!
//! None of these needs a daemon, and none starts one: they read configuration and the
//! state directory, which is the same on disk whoever is looking.

use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::sync::Arc;

use aftman_core::clock::SystemClock;
use aftman_core::ids::RunId;
use aftman_core::job::WorkerKind;
use aftman_daemon::RuntimeFactory as _;

use crate::autostart;
use crate::commands::{Ctx, provider_comparison};
use crate::error::{CliError, exit};
use crate::factory::{CliFactory, Mode, load_definitions_from, usage_report};
use crate::outln;
use crate::render::parse_since;

/// `aftman responsibilities list`.
///
/// # Errors
/// A definition that does not load.
pub async fn responsibilities_list(ctx: &Ctx) -> Result<u8, CliError> {
    let list = CliFactory::new(std::env::temp_dir(), ctx.inputs.clone(), Mode::Embedded)
        .responsibilities()
        .await?;
    outln!("{:<28} CHECKS", "RESPONSIBILITY")?;
    for responsibility in &list.responsibilities {
        outln!(
            "{:<28} {}",
            responsibility.id,
            responsibility.checks.join(", ")
        )?;
        if !responsibility.description.is_empty() {
            outln!("  {}", responsibility.description)?;
        }
    }
    Ok(exit::FINISHED)
}

/// `aftman responsibilities show <id>`.
///
/// # Errors
/// An unknown id, or a definition that does not load.
pub fn responsibilities_show(ctx: &Ctx, id: &str) -> Result<u8, CliError> {
    let set = load_definitions_from(ctx.inputs.responsibilities_dir.as_deref())
        .map_err(|e| CliError::internal(e.to_string()))?;
    let Some(definition) = set.definitions.get(id) else {
        let known: Vec<&str> = set.definitions.keys().map(String::as_str).collect();
        return Err(CliError::usage(format!(
            "no responsibility `{id}` (known: {})",
            known.join(", ")
        )));
    };
    outln!("responsibility {id}")?;
    if let Some(title) = &definition.title {
        outln!("title          {title}")?;
    }
    outln!(
        "source         {}",
        set.sources.get(id).map_or("?", String::as_str)
    )?;
    outln!("enabled        {}", definition.enabled)?;
    outln!(
        "routing        {}",
        if definition.always {
            "always active".to_owned()
        } else {
            format!(
                "when the router judges it relevant (threshold {})",
                definition.routing_threshold.unwrap_or(0.5)
            )
        }
    )?;
    if let Some(instructions) = &definition.routing_instructions {
        outln!("               {}", instructions.trim())?;
    }
    outln!("checks")?;
    for check in definition.checks(id) {
        outln!(
            "  {:<24} threshold {}",
            check.id.as_str(),
            check
                .threshold
                .map_or_else(|| "-".to_owned(), |t| format!("{t:.2}"))
        )?;
    }
    if !definition.rules.is_empty() {
        outln!("rules")?;
        for rule in &definition.rules {
            outln!("  {:<34} when {}  ->  {}", rule.id, rule.when, rule.propose)?;
        }
    }
    Ok(exit::FINISHED)
}

/// `aftman usage [--since 7d] [--run-id ID]`.
///
/// # Errors
/// A bad `--since` or run id, or an unreadable usage log.
pub fn usage(ctx: &Ctx, since: Option<String>, run_id: Option<String>) -> Result<u8, CliError> {
    let data_dir = ctx.data_dir()?;
    let since = match since {
        Some(text) => Some(parse_since(&text, time::OffsetDateTime::now_utc())?),
        None => None,
    };
    let run_id = match run_id {
        Some(raw) => Some(
            raw.parse::<RunId>()
                .map_err(|_| CliError::usage("run id must be a UUID"))?,
        ),
        None => None,
    };
    let report = usage_report(&data_dir, since, run_id)?;
    if report.entries.is_empty() {
        if since.is_some() || run_id.is_some() {
            outln!("no usage recorded for that selection")?;
        } else {
            outln!("no recorded model usage in {}", data_dir.display())?;
        }
        return Ok(exit::FINISHED);
    }

    outln!(
        "{:<38} {:<14} {:>9} {:>9} {:>8}",
        "WHEN",
        "PURPOSE",
        "IN",
        "OUT",
        "MS"
    )?;
    for entry in &report.entries {
        outln!(
            "{:<38} {:<14} {:>9} {:>9} {:>8}",
            entry.at.to_string(),
            format!("{:?}", entry.purpose).to_lowercase(),
            entry.input_tokens.unwrap_or(0),
            entry.output_tokens.unwrap_or(0),
            entry.latency_ms
        )?;
    }
    let totals = &report.totals;
    outln!()?;
    outln!("requests            {}", totals.requests)?;
    outln!("failed              {}", totals.failed_requests)?;
    outln!("input tokens        {}", totals.input_tokens)?;
    outln!("output tokens       {}", totals.output_tokens)?;
    outln!("total tokens        {}", totals.total_tokens())?;
    outln!("retried             {}", totals.retried_requests)?;
    outln!("compacted           {}", totals.compacted_requests)?;
    Ok(exit::FINISHED)
}

/// `aftman doctor [--probe] [--probe-workers] [--repo <path>]`.
///
/// # Errors
/// Provider settings that do not resolve.
pub async fn doctor(
    ctx: &Ctx,
    probe: bool,
    probe_workers: bool,
    repo: Option<&Path>,
) -> Result<u8, CliError> {
    let data_dir = ctx.data_dir()?;
    outln!("aftman {}", env!("CARGO_PKG_VERSION"))?;
    outln!("state dir   {}", data_dir.display())?;
    outln!(
        "            {}",
        if data_dir.exists() {
            "present"
        } else {
            "absent (created on first run)"
        }
    )?;
    outln!()?;
    daemon_section(ctx, &data_dir).await?;

    outln!()?;
    outln!("workers")?;
    outln!("  simulated  available (offline; no agent binary needed)")?;
    // Anything found here that will not work is collected rather than returned, so one
    // pass reports every problem instead of the first.
    let mut problems = false;
    for kind in [WorkerKind::Pi, WorkerKind::Agy, WorkerKind::Claude] {
        worker_line(kind, &mut problems)?;
    }
    outln!("  generic    available (needs --cmd)")?;
    outln!("  herdr:pane not available from the command line (herdr surface, M6)")?;
    outln!()?;
    outln!("decision model")?;

    if let Some(path) = &ctx.inputs.env_file {
        outln!(
            "  config file {} ({})",
            path.display(),
            if aftman_jev::load_env_file(path).is_empty() {
                "absent or empty"
            } else {
                "loaded"
            }
        )?;
    }
    let settings = ctx
        .inputs
        .settings()
        .map_err(|e| CliError::usage(format!("resolving provider settings: {e}")))?;
    let view = ctx
        .inputs
        .provider_view()
        .map_err(|e| CliError::usage(format!("resolving provider settings: {e}")))?;
    outln!("  base url    {}", view.base_url)?;
    outln!("  model       {}", view.model)?;
    outln!("  timeout     {:?}", settings.timeout)?;
    outln!("  retries     {}", settings.retry.max_retries)?;
    outln!(
        "  budget      pair {} bytes, request {} bytes",
        settings.budget.pair_bytes,
        settings.budget.request_bytes
    )?;
    // Presence and source only. Never a value, a prefix, or a length.
    outln!(
        "  credential  {}",
        match view.credential {
            aftman_jev::CredentialSource::Environment => "set in the environment",
            aftman_jev::CredentialSource::ConfigFile => "present in the config file",
            aftman_jev::CredentialSource::Missing => "NOT CONFIGURED",
        }
    )?;
    outln!(
        "  endpoint    {}",
        aftman_jev::redact_url(&settings.endpoint())
    )?;
    if !view.loopback {
        outln!("  transport   HTTPS")?;
    }
    // A preflight reports everything that is wrong in one pass, so an unconfigured
    // provider is recorded rather than returned: the repository and worker sections still
    // have something useful to say.
    if !view.configured {
        outln!()?;
        outln!("  Configure a provider to supervise with a real model, or pass")?;
        outln!("  --simulate-model to run offline.")?;
        problems = true;
    }

    if probe && !view.configured {
        outln!()?;
        outln!("  probe skipped: no provider is configured")?;
    } else if probe {
        outln!()?;
        outln!("  probing (one billable question, no retries)...")?;
        let client = aftman_jev::SystemOneClient::new(
            settings,
            Arc::new(aftman_jev::ReqwestTransport::new()),
            Arc::new(SystemClock),
        );
        match client.probe().await {
            Ok(result) => {
                outln!("  reachable   yes")?;
                outln!("  answered by {}", result.model)?;
                outln!("  latency     {:?}", result.latency)?;
                if let Some(p) = result.probability {
                    outln!(
                        "  sample      p = {p:.2} (value is arbitrary; this is a liveness check)"
                    )?;
                }
            }
            Err(error) => {
                outln!("  reachable   no")?;
                outln!("  error       {} ({})", error, error.code())?;
                problems = true;
            }
        }
    }
    outln!()?;
    outln!(
        "git         {} (not needed by aftman; repositories are read in-process)",
        if which("git") { "found" } else { "not found" }
    )?;

    if let Some(repo) = repo {
        repository_section(repo, &mut problems)?;
    }

    if probe_workers {
        outln!()?;
        outln!("worker probe (each installed CLI, one trivial turn; costs the agent's tokens)")?;
        probe_workers_live(&mut problems).await?;
    }

    outln!()?;
    if problems {
        outln!("NOT READY: fix the items marked above before a live run.")?;
        return Ok(exit::CONFIG);
    }
    outln!("ready for a live run")?;
    Ok(exit::FINISHED)
}

/// How long one probe turn may take. A coding agent starting cold is slow; a probe that
/// hangs is not worth waiting for.
const PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(180);

/// The most of a CLI's own output to keep when running `--version` / `--help`.
const PROBE_CAPTURE_CAP: usize = 64 * 1024;

/// Run a CLI read-only and return what it printed.
///
/// The environment comes from `worker_environment`: this spawns a subprocess like any other
/// (hard rule 3), and diagnostics have no reason to follow a second policy.
fn capture(binary: &str, args: &[&str]) -> Option<String> {
    let output = std::process::Command::new(binary)
        .args(args)
        .env_clear()
        .envs(aftman_workers::worker_environment(&[]))
        .stdin(std::process::Stdio::null())
        .output()
        .ok()?;
    let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
    text.push_str(&String::from_utf8_lossy(&output.stderr));
    text.truncate(PROBE_CAPTURE_CAP);
    Some(text)
}

/// Whether a `--help` text documents a flag.
///
/// A token comparison, not a substring one: `--mode-list` or a sentence containing `--model`
/// must not count as the flag being there. Lenient about punctuation, because every CLI
/// formats its help differently.
fn help_mentions(help: &str, flag: &str) -> bool {
    help.split(|c: char| c.is_whitespace() || matches!(c, ',' | '(' | ')' | '[' | ']' | '|'))
        .any(|token| {
            token == flag
                // `--model` listed as bare, or with a trailing `=`.
                || token.strip_suffix('=') == Some(flag)
                // `--print=<PROMPT>`, which is how most help formats attach a value.
                || token
                    .strip_prefix(flag)
                    .is_some_and(|rest| rest.starts_with('='))
        })
}

/// One agent CLI: present, which version, and whether it still accepts every flag aftman
/// passes.
///
/// The flag list comes from the argv the backend itself builds
/// (`aftman_workers::backends::spec_flags`), so it cannot drift from what a run would pass —
/// which is exactly the failure this is here to catch.
fn worker_line(kind: WorkerKind, problems: &mut bool) -> Result<(), CliError> {
    let name = kind.as_str();
    let Some(spec) = aftman_workers::backends::preflight_spec(kind, "preflight") else {
        return Ok(());
    };
    let spec = match spec {
        Ok(spec) => spec,
        Err(error) => {
            outln!("  {name:<10} not usable: {error}")?;
            *problems = true;
            return Ok(());
        }
    };
    if !which(&spec.program) {
        outln!("  {name:<10} not installed")?;
        return Ok(());
    }
    let version = capture(&spec.program, &["--version"])
        .and_then(|text| {
            text.lines()
                .map(str::trim)
                .find(|line| !line.is_empty())
                .map(ToOwned::to_owned)
        })
        .unwrap_or_else(|| "version unknown".to_owned());
    let help = capture(&spec.program, &["--help"]).unwrap_or_default();
    let flags = aftman_workers::backends::spec_flags(&spec.args);
    let missing: Vec<&str> = flags
        .iter()
        .filter(|flag| !help_mentions(&help, flag))
        .map(String::as_str)
        .collect();
    outln!("  {name:<10} {version}")?;
    if missing.is_empty() {
        outln!(
            "             all {} flag(s) aftman passes are still in --help",
            flags.len()
        )?;
    } else {
        outln!(
            "             DRIFT: --help does not mention {}. A run will probably fail; \
             re-record the backend's output and update backends.rs.",
            missing.join(", ")
        )?;
        *problems = true;
    }
    Ok(())
}

/// Whether the path is somewhere aftman can observe: a git repository with a working tree.
///
/// Not pedantry. `Invariant::UnobservedRepository` refuses to finish a run whose repository
/// cannot be read, so a job pointed at a plain directory can never complete. Saying so here
/// costs nothing and saves the run.
fn repository_section(repo: &Path, problems: &mut bool) -> Result<(), CliError> {
    outln!()?;
    outln!("repository  {}", repo.display())?;
    if !repo.is_dir() {
        outln!("  NOT a directory")?;
        *problems = true;
        return Ok(());
    }
    let worktree = repo.join(".git").exists();
    let bare = repo.join("HEAD").exists() && repo.join("objects").is_dir();
    if bare && !worktree {
        outln!("  bare repository: there is no working tree to observe")?;
        outln!("  point aftman at a checkout instead")?;
        *problems = true;
        return Ok(());
    }
    if !worktree {
        outln!("  NOT a git repository")?;
        outln!("  aftman reads a working tree through git; with no repository here there is")?;
        outln!("  no diff evidence, and a run will not finish what it cannot observe")?;
        *problems = true;
        return Ok(());
    }
    outln!("  git repository")?;
    Ok(())
}

/// Run each installed agent CLI once on a trivial mission and decode what it printed.
///
/// The check that fixtures cannot make: whether the CLI, as installed right now, produces
/// the stream the decoder expects. Costs the agent's tokens and takes as long as the CLI
/// does, so it is opt-in.
async fn probe_workers_live(problems: &mut bool) -> Result<(), CliError> {
    for kind in [WorkerKind::Pi, WorkerKind::Agy, WorkerKind::Claude] {
        let name = kind.as_str();
        let Some(spec) = aftman_workers::backends::probe_spec(kind, "Reply with exactly: ok")
        else {
            continue;
        };
        let spec = match spec {
            Ok(spec) => spec,
            Err(error) => {
                outln!("  {name:<10} not usable: {error}")?;
                *problems = true;
                continue;
            }
        };
        if !which(&spec.program) {
            outln!("  {name:<10} skipped (not installed)")?;
            continue;
        }
        let dir =
            std::env::temp_dir().join(format!("aftman-probe-{}-{}", name, std::process::id()));
        if let Err(error) = std::fs::create_dir_all(&dir) {
            outln!("             cannot create the probe directory: {error}")?;
            *problems = true;
            continue;
        }
        outln!("  {name:<10} running one turn...")?;
        let started = std::time::Instant::now();
        let run = tokio::time::timeout(
            PROBE_TIMEOUT,
            tokio::process::Command::new(&spec.program)
                .args(&spec.args)
                .current_dir(&dir)
                .env_clear()
                .envs(aftman_workers::worker_environment(&[]))
                .stdin(std::process::Stdio::null())
                .kill_on_drop(true)
                .output(),
        )
        .await;
        let _ = std::fs::remove_dir_all(&dir);
        match run {
            Ok(Ok(output)) => {
                let code = output.status.code().unwrap_or(-1);
                let text = String::from_utf8_lossy(&output.stdout);
                let (_, activity) = aftman_workers::decode_all(kind, &text);
                outln!("             exit {code} in {:?}", started.elapsed())?;
                if activity.lines == 0 {
                    // Nothing on stdout is not format drift: the CLI probably refused the
                    // turn (no login, no quota). Say what it said rather than guessing.
                    outln!("             no stream on stdout; the CLI produced nothing to decode")?;
                    let why = stderr_excerpt(&String::from_utf8_lossy(&output.stderr));
                    if !why.is_empty() {
                        outln!("             stderr: {why}")?;
                    }
                    *problems = true;
                } else if activity.looks_structured() {
                    outln!("             {}", activity.summary())?;
                    outln!(
                        "             stream parsed: the installed CLI still speaks the format aftman decodes"
                    )?;
                } else {
                    outln!(
                        "             DRIFT: {} line(s), {} unparsed — the stream is not the \
                         format aftman decodes",
                        activity.lines,
                        activity.unparsed_lines
                    )?;
                    let why = stderr_excerpt(&String::from_utf8_lossy(&output.stderr));
                    if !why.is_empty() {
                        outln!("             stderr: {why}")?;
                    }
                    *problems = true;
                }
            }
            Ok(Err(error)) => {
                outln!("             could not run: {error}")?;
                *problems = true;
            }
            Err(_) => {
                outln!("             timed out after {}s", PROBE_TIMEOUT.as_secs())?;
                *problems = true;
            }
        }
    }
    Ok(())
}

/// Up to three non-empty lines of a CLI's stderr, bounded, for a one-line explanation.
///
/// Three rather than one because the useful line is often not the first — a banner or a
/// warning usually comes before the actual error — and the whole of a stack trace never is.
fn stderr_excerpt(text: &str) -> String {
    let lines: Vec<String> = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .take(3)
        .map(|line| line.chars().take(160).collect())
        .collect();
    lines.join(" | ")
}

/// Is the daemon reachable, and where does it live? Never starts one. Prints the
/// daemon's own view (its state directory, its provider) next to this shell's, and
/// flags any disagreement.
async fn daemon_section(ctx: &Ctx, data_dir: &Path) -> Result<(), CliError> {
    outln!("daemon")?;
    let paths = match ctx.paths() {
        Ok(paths) => paths,
        Err(error) => {
            outln!("  location    cannot be determined: {error}")?;
            return Ok(());
        }
    };
    outln!("  socket      {}", paths.socket.display())?;
    outln!("  socket mode {}", describe_mode(&paths.socket))?;
    outln!(
        "  directory   {} ({})",
        paths.dir().display(),
        describe_mode(paths.dir())
    )?;
    outln!("  pid file    {}", paths.pid_file.display())?;
    match autostart::connect_existing(&paths, ctx.client).await {
        Ok(mut client) => match client.status().await {
            Ok(status) => {
                outln!(
                    "  reachable   yes (pid {}, version {}, up {}s, {} run(s) live of {})",
                    status.pid,
                    status.version,
                    status.uptime_secs,
                    status.runs_active,
                    status.runs_total
                )?;
                if let Some(theirs) = &status.state_dir {
                    outln!("  its state   {}", theirs.display())?;
                    if ctx.check_daemon(&paths, &status).is_err() {
                        outln!(
                            "  NOTE: that is not this command's state directory ({}); \
                             commands that use the daemon will refuse to run",
                            data_dir.display()
                        )?;
                    }
                }
                if status.version != env!("CARGO_PKG_VERSION") {
                    outln!(
                        "  NOTE: the daemon is aftman {}, this is {}; `aftman stop` restarts it",
                        status.version,
                        env!("CARGO_PKG_VERSION")
                    )?;
                }
                for line in provider_comparison(ctx, &status) {
                    outln!("{line}")?;
                }
            }
            Err(error) => outln!("  reachable   connected, but status failed: {error}")?,
        },
        Err(error) if error.is_not_running() => match aftman_daemon::probe(&paths).await {
            aftman_daemon::DaemonPresence::Busy { pid } => outln!(
                "  reachable   no (pid {pid} holds the lock but does not answer: it is \
                     starting up or shutting down)"
            )?,
            _ => outln!("  reachable   no (not running; it starts on demand)")?,
        },
        Err(error) => outln!("  reachable   no ({error})")?,
    }
    if let Some(pid) = aftman_daemon::probe_state_dir(data_dir).await {
        outln!("  state lock  held by pid {pid}")?;
    }
    Ok(())
}

fn describe_mode(path: &Path) -> String {
    match std::fs::metadata(path) {
        Ok(meta) => format!("{:04o}", meta.permissions().mode() & 0o7777),
        Err(_) => "absent".to_owned(),
    }
}

fn which(binary: &str) -> bool {
    std::env::var_os("PATH").is_some_and(|paths| {
        std::env::split_paths(&paths).any(|dir| {
            let candidate = dir.join(binary);
            candidate.is_file()
        })
    })
}

#[cfg(test)]
mod tests {
    use super::{help_mentions, stderr_excerpt};

    #[test]
    fn a_flag_is_matched_as_a_token_not_a_substring() {
        let help = "  -m, --model <MODEL>  the model\n  --mode-list  unrelated\n";
        assert!(help_mentions(help, "--model"), "listed with a value");
        assert!(help_mentions(help, "-m"), "short form");
        // `--mode` is a prefix of `--mode-list`; being a prefix is not being documented.
        assert!(
            !help_mentions(help, "--mode"),
            "a longer flag must not satisfy a shorter one"
        );
        assert!(!help_mentions(help, "--thinking"), "absent");
    }

    #[test]
    fn an_attached_value_still_matches_its_flag() {
        assert!(help_mentions("  --print=<PROMPT>  run once\n", "--print"));
        assert!(help_mentions("  --print=TEXT\n", "--print"));
        assert!(!help_mentions("  --printout\n", "--print"));
    }

    #[test]
    fn the_stderr_excerpt_skips_blanks_and_takes_a_few_lines() {
        assert_eq!(
            stderr_excerpt("\n\n   boom: something went wrong   \nmore\nand more\n"),
            "boom: something went wrong | more | and more"
        );
        // Bounded per line, and it stops at three.
        let noisy: String = (0..10)
            .map(|i| format!("{}{}\n", i, "x".repeat(400)))
            .collect();
        let excerpt = stderr_excerpt(&noisy);
        assert_eq!(excerpt.split(" | ").count(), 3);
        assert!(excerpt.split(" | ").all(|line| line.chars().count() <= 160));
        assert_eq!(stderr_excerpt("   \n\n"), "");
    }
}
