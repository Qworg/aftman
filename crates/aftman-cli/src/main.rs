//! The `aftman` command-line interface.
//!
//! M0 exposes the runtime directly, in `--no-daemon` mode. The daemon, the socket
//! protocol, herdr, and the web surface arrive in M3–M5; the subcommands here are
//! the ones that make M0's deliverable usable and testable.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use time::OffsetDateTime;

use aftman_core::clock::SystemClock;
use aftman_core::event::{EventEnvelope, EventKind};
use aftman_core::ids::{EventSeq, RunId};
use aftman_core::job::{Job, JobSource, RunConfig, WorkerKind};
use aftman_core::model::DecisionModel;
use aftman_core::responsibility::ResponsibilityRegistry;
use aftman_core::testing::SimulatedModel;
use aftman_core::worker::Worker;
use aftman_daemon::{EventSink, FactoryRuntime, RuntimeDeps};
use aftman_jev::UsageRecorder;
use aftman_store::{FileRunStore, RunStore};
use aftman_workers::SimulatedWorker;

/// Exit codes. `3` is distinct so a script can tell "a person must look" from
/// "it broke", which are very different operational events.
mod exit {
    pub const FINISHED: u8 = 0;
    pub const FAILED: u8 = 1;
    pub const CONFIG: u8 = 2;
    pub const NEEDS_HUMAN: u8 = 3;
}

#[derive(Parser, Debug)]
#[command(
    name = "aftman",
    version,
    about = "Supervise a coding agent: a fast decision model above slower agents",
    long_about = None,
)]
struct Cli {
    /// Where runs are stored. Defaults to $XDG_STATE_HOME/aftman.
    #[arg(long, global = true, env = "AFTMAN_DATA_DIR")]
    data_dir: Option<PathBuf>,

    /// Suppress per-event output.
    #[arg(long, global = true)]
    quiet: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Supervise a coding worker on one job.
    Run(RunArgs),
    /// Run a fully offline demonstration with a simulated worker and model.
    Demo(DemoArgs),
    /// List runs.
    Ls {
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Show one run's summary and decision timeline.
    Show { run_id: String },
    /// Show every proposal, suppression, and invariant for a run.
    Decisions { run_id: String },
    /// Report the resolved configuration and what is available.
    Doctor {
        /// Ask the decision model one question to prove connectivity. Costs tokens.
        #[arg(long)]
        probe: bool,
    },
    /// Show decision-model token usage.
    Usage {
        /// Restrict to one run.
        #[arg(long)]
        run_id: Option<String>,
    },
}

#[derive(Parser, Debug)]
struct RunArgs {
    /// Repository to work in.
    #[arg(long)]
    repo: PathBuf,

    /// The job, in free form: a ticket, a spec, a bug report, a request.
    #[arg(long, conflicts_with = "job_file")]
    job: Option<String>,

    /// Read the job from a file.
    #[arg(long, value_name = "PATH", conflicts_with = "job")]
    job_file: Option<PathBuf>,

    /// Which coding agent performs the work.
    #[arg(long, default_value = "simulated")]
    worker: String,

    /// Model to request from the provider.
    #[arg(long)]
    model: Option<String>,

    /// Independent verification policy.
    #[arg(long, value_parser = ["never", "when-asked", "always"], default_value = "when-asked")]
    verify: String,

    /// Enable the uncertainty band and the second-opinion path.
    #[arg(long)]
    ensemble: bool,

    /// Use the offline simulated decision model instead of the real provider.
    ///
    /// Without this, a run requires a configured provider so that supervision is
    /// real. A run that silently fell back to a simulation would look supervised
    /// when it was not.
    #[arg(long)]
    simulate_model: bool,
}

#[derive(Parser, Debug)]
struct DemoArgs {
    /// Repository the simulated worker operates on. Not modified.
    #[arg(long, default_value = ".")]
    repo: PathBuf,

    /// The job text to demonstrate.
    #[arg(
        long,
        default_value = "Add request retries with backoff, and cover them with tests."
    )]
    job: String,
}

/// Renders events to the terminal.
struct TerminalSink {
    quiet: bool,
}

impl EventSink for TerminalSink {
    fn emit(&self, event: &EventEnvelope) {
        if self.quiet {
            return;
        }
        // A leading marker makes the decision path skimmable: `>` is a state or
        // worker transition, `*` is a decision, `!` is something going wrong.
        let marker = match event.kind {
            EventKind::DirectiveSelected => "*",
            EventKind::ModelError | EventKind::Error | EventKind::RateLimited => "!",
            EventKind::EscalationRaised => "?",
            EventKind::RunFinished => "=",
            _ => ">",
        };
        println!("{marker} [{:>4}] {}", event.seq.get(), event.summary());
        if event.kind == EventKind::DirectiveSelected {
            if let Some(reason) = event.payload.get("reason").and_then(|v| v.as_str()) {
                if !reason.is_empty() {
                    println!("        reason: {reason}");
                }
            }
        }
    }
}

fn resolve_store(data_dir: Option<PathBuf>) -> Result<Arc<FileRunStore>> {
    let root = match data_dir {
        Some(dir) => dir,
        None => FileRunStore::default_root()
            .context("could not determine a state directory; set --data-dir or $XDG_STATE_HOME")?,
    };
    Ok(Arc::new(FileRunStore::new(root)))
}

/// Parse a worker name, refusing the ones that do not exist yet.
///
/// A real backend that silently fell back to a simulation would make a run look
/// supervised when it was not, so this fails loudly instead.
fn resolve_worker(name: &str) -> Result<WorkerKind> {
    match name {
        "simulated" => Ok(WorkerKind::Simulated),
        "pi" | "agy" | "claude" | "generic" | "herdr:pane" => bail!(
            "the `{name}` backend arrives in milestone M2/M8; M0 ships only \
             `--worker simulated`, which needs no agent binary and no credentials"
        ),
        other => bail!("unknown worker `{other}` (try: simulated)"),
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli).await {
        Ok(code) => ExitCode::from(code),
        Err(error) => {
            eprintln!("error: {error:#}");
            ExitCode::from(exit::CONFIG)
        }
    }
}

async fn run(cli: Cli) -> Result<u8> {
    let quiet = cli.quiet;
    let store = resolve_store(cli.data_dir.clone())?;
    match cli.command {
        Command::Run(args) => run_job(&store, args, quiet).await,
        Command::Demo(args) => demo(&store, args, quiet).await,
        Command::Ls { limit } => list_runs(&store, limit).await,
        Command::Show { run_id } => show(&store, &run_id).await,
        Command::Decisions { run_id } => decisions(&store, &run_id).await,
        Command::Doctor { probe } => doctor(&store, probe).await,
        Command::Usage { run_id } => usage(&store, run_id),
    }
}

fn config_from(args: &RunArgs) -> Result<RunConfig> {
    Ok(RunConfig {
        worker: resolve_worker(&args.worker)?,
        worker_model: args.model.clone(),
        verify: match args.verify.as_str() {
            "never" => aftman_core::job::VerifyPolicy::Never,
            "always" => aftman_core::job::VerifyPolicy::Always,
            _ => aftman_core::job::VerifyPolicy::WhenAsked,
        },
        ..RunConfig::default()
    })
}

/// Build the runtime, choosing the decision model the way `run` describes.
///
/// A configured provider is used by default; the simulated model has to be asked for.
/// That direction matters: a supervised run that quietly used a simulation would be a
/// lie about what happened.
async fn build_runtime(
    store: &Arc<FileRunStore>,
    config: RunConfig,
    ensemble: bool,
    simulate_model: bool,
    quiet: bool,
) -> Result<FactoryRuntime> {
    let model = build_model(store, simulate_model, quiet)?;
    let worker: Arc<dyn Worker> = Arc::new(SimulatedWorker::new());
    Ok(FactoryRuntime::new(RuntimeDeps {
        config,
        store: store.clone(),
        model,
        worker,
        registry: ResponsibilityRegistry::builtin(),
        clock: Arc::new(SystemClock),
        sink: Arc::new(TerminalSink { quiet }),
        ensemble_enabled: ensemble,
        uncertainty_margin: aftman_daemon::default_margin(),
        steering_grace: std::time::Duration::from_secs(30),
    }))
}

/// Pick the decision model, or explain what is missing.
fn build_model(
    store: &Arc<FileRunStore>,
    simulate_model: bool,
    quiet: bool,
) -> Result<Arc<dyn DecisionModel>> {
    if simulate_model {
        if !quiet {
            println!("using the simulated decision model (--simulate-model)\n");
        }
        return Ok(Arc::new(SimulatedModel::new()));
    }

    let env_file = aftman_jev::default_env_file().map(|path| aftman_jev::load_env_file(&path));
    let settings = aftman_jev::resolve(env_file.as_ref()).context("resolving provider settings")?;
    if !settings.is_configured() {
        bail!(
            "no decision-model provider is configured.\n\
             \n\
             Set TYPESAFE_API_KEY (and optionally TYPESAFE_BASE_URL) in the environment,\n\
             or in {}\n\
             \n\
             To run offline instead, pass --simulate-model. A run needs a supervisor;\n\
             silently simulating one would misrepresent what happened.",
            aftman_jev::default_env_file()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "$XDG_CONFIG_HOME/aftman/.env".to_owned())
        );
    }
    let transport = Arc::new(aftman_jev::ReqwestTransport::new());
    let recorder = Arc::new(UsageRecorder::in_state_dir(store.root()));
    if !quiet {
        println!(
            "supervising with {} at {}{}\n",
            settings.model,
            settings.base_url,
            if settings.is_loopback() {
                " (loopback)"
            } else {
                ""
            }
        );
    }
    Ok(Arc::new(
        aftman_jev::SystemOneClient::new(settings, transport, Arc::new(SystemClock))
            .with_usage(recorder),
    ))
}

async fn run_job(store: &Arc<FileRunStore>, args: RunArgs, quiet: bool) -> Result<u8> {
    let prompt = match (&args.job, &args.job_file) {
        (Some(job), _) => job.clone(),
        (None, Some(path)) => std::fs::read_to_string(path)
            .with_context(|| format!("reading job file {}", path.display()))?,
        (None, None) => bail!("provide --job or --job-file"),
    };
    let repo = args
        .repo
        .canonicalize()
        .with_context(|| format!("resolving repository {}", args.repo.display()))?;
    if !repo.is_dir() {
        bail!("{} is not a directory", repo.display());
    }

    let config = config_from(&args)?;
    let runtime = build_runtime(store, config, args.ensemble, args.simulate_model, quiet).await?;
    let job = Job {
        prompt,
        repo,
        source: JobSource::Cli,
    };
    let outcome = runtime.run(job).await.context("running the job")?;
    Ok(report(&outcome))
}

async fn demo(store: &Arc<FileRunStore>, args: DemoArgs, quiet: bool) -> Result<u8> {
    let repo = args
        .repo
        .canonicalize()
        .with_context(|| format!("resolving repository {}", args.repo.display()))?;
    if !quiet {
        println!(
            "aftman demo · simulated worker and simulated model · {} is not modified\n",
            repo.display()
        );
    }
    let args = RunArgs {
        repo,
        job: Some(args.job),
        job_file: None,
        worker: "simulated".to_owned(),
        model: None,
        verify: "when-asked".to_owned(),
        ensemble: false,
        // The demo is the offline path by definition.
        simulate_model: true,
    };
    run_job(store, args, quiet).await
}

fn report(outcome: &aftman_daemon::RunOutcome) -> u8 {
    println!();
    println!("run id      {}", outcome.run_id);
    println!("status      {}", outcome.status);
    println!("directive   {}", outcome.directive.name());
    println!(
        "assessments {} ({} skipped as unchanged)",
        outcome.assessments, outcome.skipped
    );
    println!(
        "workers     {} ({} attempts, {} verifiers)",
        outcome.counters.workers_run, outcome.counters.attempts, outcome.counters.verifiers_run
    );
    match outcome.status {
        aftman_core::job::FactoryStatus::Finished => exit::FINISHED,
        aftman_core::job::FactoryStatus::NeedsHuman => exit::NEEDS_HUMAN,
        _ => exit::FAILED,
    }
}

async fn list_runs(store: &Arc<FileRunStore>, limit: Option<usize>) -> Result<u8> {
    let filter = aftman_core::run::RunFilter {
        limit,
        ..Default::default()
    };
    let runs = store.list(&filter).await.context("listing runs")?;
    if runs.is_empty() {
        println!("no runs in {}", store.root().display());
        return Ok(exit::FINISHED);
    }
    println!("{:<38} {:<12} {:>8}  JOB", "RUN ID", "STATUS", "ELAPSED");
    for run in runs {
        println!(
            "{:<38} {:<12} {:>7}s  {}",
            run.id,
            run.status.as_str(),
            run.elapsed_secs,
            truncate(&run.job_excerpt, 52)
        );
    }
    Ok(exit::FINISHED)
}

async fn show(store: &Arc<FileRunStore>, run_id: &str) -> Result<u8> {
    let id: RunId = run_id.parse().context("run id must be a UUID")?;
    let state = store.load(id).await.context("loading the run")?;
    println!("run id      {}", state.header.id);
    println!("created     {}", state.header.created_at);
    println!("repo        {}", state.header.job.repo.display());
    println!("status      {}", state.status);
    println!("worker      {}", state.header.config.worker.as_str());
    println!("last seq    {}", state.last_seq);
    println!("job         {}", truncate(&state.header.job.prompt, 200));
    if let Some(directive) = &state.last_directive {
        println!(
            "directive   {} ({})",
            directive.name(),
            directive.reason().unwrap_or("")
        );
    }
    for escalation in state.open_escalations() {
        println!(
            "\nopen escalation [{}] {:?}",
            escalation.id, escalation.kind
        );
        println!("  {}", escalation.ask);
    }
    Ok(exit::FINISHED)
}

async fn decisions(store: &Arc<FileRunStore>, run_id: &str) -> Result<u8> {
    let id: RunId = run_id.parse().context("run id must be a UUID")?;
    let events = store
        .events(id, EventSeq::FIRST)
        .await
        .context("reading the event log")?;
    let mut printed = 0;
    for event in &events {
        match event.kind {
            EventKind::DirectiveSelected => {
                printed += 1;
                let directive = event.payload["directive"].as_str().unwrap_or("?");
                let invariants = event.payload["invariants"]
                    .as_array()
                    .map(Vec::len)
                    .unwrap_or(0);
                println!(
                    "[{:>4}] selected {directive} ({invariants} invariants)",
                    event.seq.get()
                );
                if let Some(suppressed) = event.payload["suppressed"].as_array() {
                    for entry in suppressed {
                        println!(
                            "        suppressed {:<34} {}",
                            entry["rule"].as_str().unwrap_or("?"),
                            entry["reason"].as_str().unwrap_or("?")
                        );
                    }
                }
            }
            EventKind::EnsembleResolved => {
                let unresolved = event.payload["unresolved"]
                    .as_array()
                    .map(|a| a.len())
                    .unwrap_or(0);
                println!(
                    "[{:>4}] ensemble enabled={} margin={} unresolved={unresolved}",
                    event.seq.get(),
                    event.payload["enabled"],
                    event.payload["margin"]
                );
            }
            EventKind::AssessmentSkippedUnchanged => {
                println!("[{:>4}] skipped: observation unchanged", event.seq.get());
            }
            EventKind::ModelError => {
                println!(
                    "[{:>4}] model error: {}",
                    event.seq.get(),
                    event.payload["code"].as_str().unwrap_or("?")
                );
            }
            _ => {}
        }
    }
    if printed == 0 {
        println!("no decisions recorded yet");
    }
    Ok(exit::FINISHED)
}

async fn doctor(store: &Arc<FileRunStore>, probe: bool) -> Result<u8> {
    println!("aftman {}", env!("CARGO_PKG_VERSION"));
    println!("state dir   {}", store.root().display());
    println!(
        "repo state  {}",
        if store.root().exists() {
            "present"
        } else {
            "absent (created on first run)"
        }
    );
    println!();
    println!("workers");
    println!("  simulated  available (offline; no agent binary needed)");
    for name in ["pi", "agy", "claude", "herdr:pane"] {
        let found = which(name.split(':').next().unwrap_or(name));
        println!(
            "  {:<10} {} (backend not implemented until M2/M8)",
            name,
            if found {
                "binary found"
            } else {
                "binary not found"
            }
        );
    }
    println!();
    println!("decision model");

    let env_file = aftman_jev::default_env_file();
    let loaded = env_file
        .as_ref()
        .map(|path| (path.clone(), aftman_jev::load_env_file(path)));
    if let Some((path, map)) = &loaded {
        println!(
            "  config file {} ({})",
            path.display(),
            if map.is_empty() {
                "absent or empty"
            } else {
                "loaded"
            }
        );
    }
    let settings = aftman_jev::resolve(loaded.as_ref().map(|(_, map)| map))
        .context("resolving provider settings")?;
    println!("  base url    {}", settings.base_url);
    println!("  model       {}", settings.model);
    println!("  timeout     {:?}", settings.timeout);
    println!("  retries     {}", settings.retry.max_retries);
    println!(
        "  budget      pair {} bytes, request {} bytes",
        settings.budget.pair_bytes, settings.budget.request_bytes
    );
    // Presence only. Never a value, a prefix, or a length.
    println!(
        "  credential  {}",
        match std::env::var_os("TYPESAFE_API_KEY") {
            Some(_) => "TYPESAFE_API_KEY is set",
            None => match &settings.api_key {
                Some(_) => "present in the config file",
                None => "NOT CONFIGURED",
            },
        }
    );
    println!("  endpoint    {}", settings.endpoint());
    if !settings.is_loopback() {
        println!("  transport   HTTPS");
    }
    if !settings.is_configured() {
        println!();
        println!("  Set TYPESAFE_API_KEY to supervise with a real model, or pass");
        println!("  --simulate-model to run offline.");
        return Ok(exit::CONFIG);
    }

    if probe {
        println!();
        println!("  probing (one billable question, no retries)...");
        let client = aftman_jev::SystemOneClient::new(
            settings,
            Arc::new(aftman_jev::ReqwestTransport::new()),
            Arc::new(SystemClock),
        );
        match client.probe().await {
            Ok(result) => {
                println!("  reachable   yes");
                println!("  answered by {}", result.model);
                println!("  latency     {:?}", result.latency);
                if let Some(p) = result.probability {
                    println!(
                        "  sample      p = {p:.2} (value is arbitrary; this is a liveness check)"
                    );
                }
            }
            Err(error) => {
                println!("  reachable   no");
                println!("  error       {} ({})", error, error.code());
                return Ok(exit::FAILED);
            }
        }
    }
    println!();
    println!(
        "git         {}",
        if which("git") { "found" } else { "not found" }
    );
    Ok(exit::FINISHED)
}

/// Show decision-model usage.
fn usage(store: &Arc<FileRunStore>, run_id: Option<String>) -> Result<u8> {
    let recorder = UsageRecorder::in_state_dir(store.root());
    let entries = recorder.read().context("reading the usage log")?;
    if entries.is_empty() {
        println!("no recorded model usage in {}", store.root().display());
        return Ok(exit::FINISHED);
    }
    let filter = match run_id {
        Some(raw) => Some(raw.parse::<RunId>().context("run id must be a UUID")?),
        None => None,
    };
    let selected: Vec<_> = entries
        .into_iter()
        .filter(|entry| filter.is_none_or(|id| entry.run_id == Some(id)))
        .collect();
    if selected.is_empty() {
        println!("no usage recorded for that run");
        return Ok(exit::FINISHED);
    }

    let totals = aftman_jev::usage::totals(&selected);
    println!(
        "{:<38} {:<14} {:>9} {:>9} {:>8}",
        "WHEN", "PURPOSE", "IN", "OUT", "MS"
    );
    for entry in &selected {
        println!(
            "{:<38} {:<14} {:>9} {:>9} {:>8}",
            entry.at.to_string(),
            format!("{:?}", entry.purpose).to_lowercase(),
            entry.input_tokens.unwrap_or(0),
            entry.output_tokens.unwrap_or(0),
            entry.latency_ms
        );
    }
    println!();
    println!("requests            {}", totals.requests);
    println!("failed              {}", totals.failed_requests);
    println!("input tokens        {}", totals.input_tokens);
    println!("output tokens       {}", totals.output_tokens);
    println!("total tokens        {}", totals.total_tokens());
    println!("retried             {}", totals.retried_requests);
    println!("compacted           {}", totals.compacted_requests);
    Ok(exit::FINISHED)
}

fn which(binary: &str) -> bool {
    std::env::var_os("PATH").is_some_and(|paths| {
        std::env::split_paths(&paths).any(|dir| {
            let candidate = dir.join(binary);
            candidate.is_file()
        })
    })
}

fn truncate(text: &str, max: usize) -> String {
    let flattened = text.replace('\n', " ");
    if flattened.chars().count() <= max {
        return flattened;
    }
    let mut out: String = flattened.chars().take(max).collect();
    out.push('…');
    out
}

/// The wall-clock time now, for callers that need to stamp something outside a run.
#[must_use]
pub fn now() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}
