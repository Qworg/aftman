//! Operating aftman: `responsibilities`, `usage`, `doctor`.
//!
//! None of these needs a daemon, and none starts one: they read configuration and the
//! state directory, which is the same on disk whoever is looking.

use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::sync::Arc;

use aftman_core::clock::SystemClock;
use aftman_core::ids::RunId;
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

/// `aftman doctor [--probe]`.
///
/// # Errors
/// Provider settings that do not resolve.
pub async fn doctor(ctx: &Ctx, probe: bool) -> Result<u8, CliError> {
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
    for name in ["pi", "agy", "claude"] {
        outln!(
            "  {:<10} {}",
            name,
            if which(name) {
                "binary found"
            } else {
                "binary not found"
            }
        )?;
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
    if !view.configured {
        outln!()?;
        outln!("  Configure a provider to supervise with a real model, or pass")?;
        outln!("  --simulate-model to run offline.")?;
        return Ok(exit::CONFIG);
    }

    if probe {
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
                return Ok(exit::FAILED);
            }
        }
    }
    outln!()?;
    outln!(
        "git         {}",
        if which("git") { "found" } else { "not found" }
    )?;
    Ok(exit::FINISHED)
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
