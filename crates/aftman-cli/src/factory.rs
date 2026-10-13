//! The composition root: the real [`RuntimeFactory`].
//!
//! This is the one place that turns `run.start` params into a running stack: a
//! decision model (Jev, or the offline simulation when asked for), a worker backend,
//! the responsibility definitions, and command evidence providers. The daemon
//! (`aftman serve`) and the embedded runtime (`--no-daemon`, `demo`) both build runs
//! here, so the two cannot drift.
//!
//! # Inputs are injected
//!
//! Everything the factory reads from outside the program (provider environment,
//! provider `.env` file, responsibility overrides, evidence config) arrives in a
//! [`FactoryInputs`] gathered once in `main`. Tests build it empty or explicit and so
//! never touch the developer's environment or home directory (hard rule 4).
//!
//! # Hard rules
//!
//! - Rule 2: this crate's composition code and `aftman-jev` are the only places that
//!   touch the provider; nothing here names an answer type or reads a provider
//!   variable directly (the variable names live in `aftman-jev`).
//! - Rule 3: workers get their environment from `worker_environment()` (inside
//!   `SubprocessWorker`), never from here.
//! - The factory does **not** check `AFTMAN_ACK_UNSANDBOXED`. The daemon's environment
//!   is whichever shell happened to start it, so the acknowledgement is the client's to
//!   give (`cli::RunArgs::prepare`). What the factory does is *record* the posture in
//!   the run (`launch`), so an unacknowledged `auto-approve` is visible afterwards.
//! - `--permissions interactive` is refused: no human is attached to a worker started
//!   here (docs/04, docs/08 section 3.1).
//!
//! # Blocking
//!
//! Assembling a run reads files and creates directories, so it runs on the blocking
//! pool; the async trait method only awaits it.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use aftman_core::clock::SystemClock;
use aftman_core::definitions::DefinitionSet;
use aftman_core::evidence::EvidencePlan;
use aftman_core::ids::RunId;
use aftman_core::job::{Job, JobSource, RunConfig, VerifyPolicy, WorkerKind};
use aftman_core::model::DecisionModel;
use aftman_core::responsibility::ResponsibilityRegistry;
use aftman_core::testing::SimulatedModel;
use aftman_core::worker::Worker;
use aftman_daemon::protocol::{
    ErrorCode, JevUsageParams, ProtocolError, ResponsibilityInfo, ResponsibilityListResult,
    RunStartParams,
};
use aftman_daemon::{BuiltRun, FactoryRuntime, RuntimeContext, RuntimeDeps, RuntimeFactory};
use aftman_jev::{UsageEntry, UsageRecorder, UsageTotals};
use aftman_workers::{BackendOptions, PermissionMode, SimulatedWorker, SubprocessWorker};

/// Environment variable naming a directory of responsibility definition overrides.
pub const RESPONSIBILITIES_DIR_ENV: &str = "AFTMAN_RESPONSIBILITIES_DIR";

fn config_error(message: impl Into<String>) -> ProtocolError {
    ProtocolError::new(ErrorCode::InvalidConfig, message)
}

fn internal(message: impl Into<String>) -> ProtocolError {
    ProtocolError::new(ErrorCode::Internal, message)
}

/// Why definitions or evidence configuration did not load.
#[derive(Debug, thiserror::Error)]
pub enum FactoryError {
    #[error("loading the bundled responsibility definitions: {0}")]
    Bundled(String),
    #[error("loading definitions from {}: {message}", dir.display())]
    Overlay { dir: PathBuf, message: String },
    #[error(
        "these definitions are neither built-in nor declare any rules, so nothing \
         would ever propose a directive from them: {0}"
    )]
    Unknown(String),
    #[error("reading {}: {source}", path.display())]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("loading command evidence providers: {0}")]
    Evidence(String),
}

/// Everything the factory reads from outside the program, gathered once in `main`
/// (or built by hand in a test) instead of being fished out of the process
/// environment and the user's home directory at every call.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FactoryInputs {
    /// The provider-related environment values (`aftman_jev::snapshot_env`).
    pub provider_env: BTreeMap<String, String>,
    /// The provider `.env` file, re-read on every run (so editing it needs no restart).
    pub env_file: Option<PathBuf>,
    /// A directory of responsibility definition overrides.
    pub responsibilities_dir: Option<PathBuf>,
    /// The command-evidence configuration file.
    pub evidence_config: Option<PathBuf>,
}

impl FactoryInputs {
    /// Nothing configured: no provider, no overrides, no evidence commands.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Read the process environment, once. Paths are made absolute against the current
    /// directory, because a daemon outlives the directory it was started from.
    #[must_use]
    pub fn from_process() -> Self {
        let env = |key: &str| std::env::var(key).ok();
        let absolute = |path: PathBuf| {
            if path.is_absolute() {
                path
            } else {
                std::env::current_dir().map_or(path.clone(), |cwd| cwd.join(path))
            }
        };
        let config_home = env("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .or_else(|| env("HOME").map(|h| PathBuf::from(h).join(".config")))
            .map(absolute);
        Self {
            provider_env: aftman_jev::snapshot_env(&env),
            env_file: aftman_jev::default_env_file_with(&env).map(absolute),
            responsibilities_dir: env(RESPONSIBILITIES_DIR_ENV)
                .map(PathBuf::from)
                .map(absolute),
            evidence_config: config_home.map(|dir| dir.join("aftman/evidence.toml")),
        }
    }

    fn lookup(&self) -> impl Fn(&str) -> Option<String> + '_ {
        |key| self.provider_env.get(key).cloned()
    }

    fn env_file_map(&self) -> Option<BTreeMap<String, String>> {
        self.env_file
            .as_ref()
            .map(|path| aftman_jev::load_env_file(path))
    }

    /// The provider settings as resolved from these inputs (holds the key: never print).
    ///
    /// # Errors
    /// Settings that do not resolve.
    pub fn settings(&self) -> Result<aftman_jev::JevSettings, aftman_jev::SettingsError> {
        aftman_jev::resolve_with(&self.lookup(), self.env_file_map().as_ref())
    }

    /// The provider as resolved from these inputs, redacted.
    ///
    /// # Errors
    /// Settings that do not resolve (a bad base URL, a non-numeric number).
    pub fn provider_view(&self) -> Result<aftman_jev::SettingsView, aftman_jev::SettingsError> {
        aftman_jev::describe(&self.lookup(), self.env_file_map().as_ref())
    }
}

/// Which process this factory serves; it changes what an error tells the user to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// The daemon: its environment is frozen at start.
    Daemon,
    /// `--no-daemon` and `demo`: this process.
    Embedded,
}

/// A built run.
pub struct Prepared {
    pub built: BuiltRun,
}

/// The real [`RuntimeFactory`].
#[derive(Debug, Clone)]
pub struct CliFactory {
    /// Where runs, missions, evidence scratch space and the usage log live.
    data_dir: PathBuf,
    /// Whether a human can answer a worker's approval prompt. False everywhere until
    /// the herdr surface exists (M6).
    human_available: bool,
    inputs: Arc<FactoryInputs>,
    mode: Mode,
}

impl CliFactory {
    #[must_use]
    pub fn new(data_dir: impl Into<PathBuf>, inputs: FactoryInputs, mode: Mode) -> Self {
        Self {
            data_dir: data_dir.into(),
            human_available: false,
            inputs: Arc::new(inputs),
            mode,
        }
    }

    #[must_use]
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// What this process can tell a client about itself, for `daemon.status`: the
    /// provider as resolved *here* (never the key; the URL without userinfo).
    #[must_use]
    pub fn daemon_info(&self) -> BTreeMap<String, String> {
        let mut info = BTreeMap::new();
        match self.inputs.provider_view() {
            Ok(view) => {
                info.insert(
                    "provider".to_owned(),
                    if view.configured {
                        "configured"
                    } else {
                        "not configured"
                    }
                    .to_owned(),
                );
                info.insert("model".to_owned(), view.model);
                info.insert("base_url".to_owned(), view.base_url);
            }
            Err(error) => {
                info.insert("provider".to_owned(), "invalid".to_owned());
                info.insert("provider_error".to_owned(), error.to_string());
            }
        }
        if let Some(path) = &self.inputs.env_file {
            info.insert("env_file".to_owned(), path.display().to_string());
        }
        info
    }

    /// Build a run, off the async threads.
    ///
    /// # Errors
    /// `invalid_repo`, `invalid_config`, `model_unavailable`, `internal`.
    pub async fn prepare(
        &self,
        params: &RunStartParams,
        context: RuntimeContext,
    ) -> Result<Prepared, ProtocolError> {
        let me = self.clone();
        let params = params.clone();
        tokio::task::spawn_blocking(move || me.prepare_blocking(&params, context))
            .await
            .map_err(|error| internal(format!("building the run failed: {error}")))?
    }

    fn prepare_blocking(
        &self,
        params: &RunStartParams,
        context: RuntimeContext,
    ) -> Result<Prepared, ProtocolError> {
        if !params.repo.is_absolute() {
            return Err(ProtocolError::new(
                ErrorCode::InvalidRepo,
                format!(
                    "repository path {} is not absolute (the client resolves it)",
                    params.repo.display()
                ),
            ));
        }
        if !params.repo.is_dir() {
            return Err(ProtocolError::new(
                ErrorCode::InvalidRepo,
                format!("repository {} is not a directory", params.repo.display()),
            ));
        }

        let config = run_config(params)?;
        let options = backend_options(params, self.human_available)?;
        let mut launch = BTreeMap::new();
        let model = self.build_model(params.simulate_model, &mut launch)?;
        crate::statedir::ensure(&self.data_dir)
            .map_err(|e| internal(format!("preparing the state directory: {e}")))?;
        let worker = build_worker(&config, &options, &self.data_dir)?;
        launch.insert(
            "permissions".to_owned(),
            params
                .permissions
                .clone()
                .unwrap_or_else(|| "sandboxed".to_owned()),
        );
        launch.insert(
            "external_sandbox".to_owned(),
            params.external_sandbox.to_string(),
        );
        launch.insert(
            "steering".to_owned(),
            if worker.supports_steering() {
                "available"
            } else {
                "unsupported"
            }
            .to_owned(),
        );
        launch.insert("worker".to_owned(), config.worker.as_str().to_owned());
        launch.insert(
            "worker_mode".to_owned(),
            if options.prefer_steering {
                "live input"
            } else {
                "one-shot"
            }
            .to_owned(),
        );

        let definitions = self
            .load_definitions()
            .map_err(|e| config_error(e.to_string()))?;
        let evidence = load_evidence_from(self.inputs.evidence_config.as_deref())
            .map_err(|e| config_error(e.to_string()))?;
        let evidence_temp_dir = self.data_dir.join(crate::statedir::EVIDENCE_DIR);

        let runtime = FactoryRuntime::new(RuntimeDeps {
            config,
            store: context.store,
            model,
            worker,
            // Replaced once routing has run; the pre-routing placeholder the runtime
            // needs for its type.
            registry: ResponsibilityRegistry::builtin(),
            definitions,
            evidence,
            evidence_temp_dir,
            clock: context.clock,
            sink: context.sink,
            ensemble_enabled: params.ensemble,
            uncertainty_margin: aftman_daemon::default_margin(),
            steering_grace: std::time::Duration::from_secs(30),
            launch,
        });
        Ok(Prepared {
            built: BuiltRun {
                runtime: Arc::new(runtime),
                job: Job {
                    prompt: params.job.clone(),
                    repo: params.repo.clone(),
                    source: JobSource::Cli,
                },
            },
        })
    }

    /// The bundled responsibility definitions plus this factory's override directory.
    ///
    /// # Errors
    /// A definition that does not parse, or one that would never propose anything.
    pub fn load_definitions(&self) -> Result<DefinitionSet, FactoryError> {
        load_definitions_from(self.inputs.responsibilities_dir.as_deref())
    }

    /// Pick the decision model, or explain what is missing.
    ///
    /// A configured provider is used by default; the simulated model has to be asked
    /// for. That direction matters: a supervised run that quietly used a simulation
    /// would be a lie about what happened.
    fn build_model(
        &self,
        simulate_model: bool,
        launch: &mut BTreeMap<String, String>,
    ) -> Result<Arc<dyn DecisionModel>, ProtocolError> {
        if simulate_model {
            launch.insert("supervisor".to_owned(), "simulated".to_owned());
            return Ok(Arc::new(SimulatedModel::new()));
        }
        let file = self.inputs.env_file_map();
        let settings = aftman_jev::resolve_with(&self.inputs.lookup(), file.as_ref())
            .map_err(|e| config_error(format!("resolving provider settings: {e}")))?;
        if !settings.is_configured() {
            return Err(ProtocolError::new(
                ErrorCode::ModelUnavailable,
                self.model_unavailable_text(),
            ));
        }
        let view = settings.view(aftman_jev::CredentialSource::Environment);
        launch.insert("supervisor".to_owned(), "jev".to_owned());
        launch.insert("model".to_owned(), view.model);
        launch.insert("base_url".to_owned(), view.base_url);
        launch.insert("loopback".to_owned(), view.loopback.to_string());
        let transport = Arc::new(aftman_jev::ReqwestTransport::new());
        let recorder = Arc::new(UsageRecorder::in_state_dir(&self.data_dir));
        Ok(Arc::new(
            aftman_jev::SystemOneClient::new(settings, transport, Arc::new(SystemClock))
                .with_usage(recorder),
        ))
    }

    fn model_unavailable_text(&self) -> String {
        let file = self.inputs.env_file.as_ref().map_or_else(
            || "the aftman config .env file".to_owned(),
            |p| p.display().to_string(),
        );
        let (whose, fix) = match self.mode {
            Mode::Daemon => (
                format!(
                    "This daemon (pid {}) was started without a provider configured: a daemon \
                     keeps the environment it was started with, so a key exported in your \
                     shell now does not reach it.",
                    std::process::id()
                ),
                format!(
                    "To fix it, either put the key in {file} (re-read on every run, no restart \
                     needed), or export it and restart the daemon (`aftman stop`; the next \
                     command starts a new one with your environment)."
                ),
            ),
            Mode::Embedded => (
                "No provider is configured in this process's environment.".to_owned(),
                format!(
                    "Set TYPESAFE_API_KEY (and optionally TYPESAFE_BASE_URL) in the \
                     environment, or in {file}."
                ),
            ),
        };
        format!(
            "no decision-model provider is configured.\n\n{whose}\n{fix}\n\n\
             To run offline instead, pass --simulate-model. A run needs a supervisor;\n\
             silently simulating one would misrepresent what happened."
        )
    }

    fn responsibility_list(&self) -> Result<ResponsibilityListResult, ProtocolError> {
        let set = self
            .load_definitions()
            .map_err(|e| config_error(e.to_string()))?;
        let responsibilities = set
            .enabled()
            .map(|(id, definition)| ResponsibilityInfo {
                id: id.clone(),
                description: definition
                    .title
                    .clone()
                    .or_else(|| definition.routing_instructions.clone())
                    .unwrap_or_default(),
                checks: definition
                    .checks(id)
                    .iter()
                    .map(|check| check.id.as_str().to_owned())
                    .collect(),
            })
            .collect();
        Ok(ResponsibilityListResult { responsibilities })
    }
}

#[async_trait]
impl RuntimeFactory for CliFactory {
    async fn build(
        &self,
        params: &RunStartParams,
        context: RuntimeContext,
    ) -> Result<BuiltRun, ProtocolError> {
        Ok(self.prepare(params, context).await?.built)
    }

    async fn responsibilities(&self) -> Result<ResponsibilityListResult, ProtocolError> {
        let me = self.clone();
        tokio::task::spawn_blocking(move || me.responsibility_list())
            .await
            .map_err(|e| internal(format!("listing responsibilities failed: {e}")))?
    }

    async fn usage(&self, params: &JevUsageParams) -> Result<Value, ProtocolError> {
        let since = match &params.since {
            Some(text) => Some(
                OffsetDateTime::parse(text, &Rfc3339)
                    .map_err(|e| ProtocolError::invalid(format!("since is not RFC 3339: {e}")))?,
            ),
            None => None,
        };
        let dir = self.data_dir.clone();
        let run_id = params.run_id;
        let report = tokio::task::spawn_blocking(move || usage_report(&dir, since, run_id))
            .await
            .map_err(|e| internal(format!("reading usage failed: {e}")))??;
        Ok(report.to_json())
    }
}

// ---------------------------------------------------------------------------
// Parsing the request
// ---------------------------------------------------------------------------

/// Parse a worker name.
///
/// A real backend that silently fell back to a simulation would make a run look
/// supervised when it was not, so an unknown name fails loudly.
pub fn resolve_worker(name: &str) -> Result<WorkerKind, ProtocolError> {
    match name {
        "simulated" => Ok(WorkerKind::Simulated),
        "pi" => Ok(WorkerKind::Pi),
        "agy" => Ok(WorkerKind::Agy),
        "claude" => Ok(WorkerKind::Claude),
        "generic" => Ok(WorkerKind::Generic),
        "herdr:pane" => Err(config_error(
            "`herdr:pane` is driven by the herdr surface (milestone M6), not by a \
             command line. Use --worker pi, agy, claude, or generic for now.",
        )),
        other => Err(config_error(format!(
            "unknown worker `{other}` (try: simulated, pi, agy, claude, generic)"
        ))),
    }
}

fn run_config(params: &RunStartParams) -> Result<RunConfig, ProtocolError> {
    let verify = match params.verify.as_deref().unwrap_or("when-asked") {
        "never" => VerifyPolicy::Never,
        "always" => VerifyPolicy::Always,
        "when-asked" => VerifyPolicy::WhenAsked,
        other => {
            return Err(config_error(format!(
                "unknown verify policy `{other}` (never, when-asked, always)"
            )));
        }
    };
    Ok(RunConfig {
        worker: resolve_worker(params.worker.as_deref().unwrap_or("simulated"))?,
        worker_model: params.model.clone(),
        verify,
        ..RunConfig::default()
    })
}

fn backend_options(
    params: &RunStartParams,
    human_available: bool,
) -> Result<BackendOptions, ProtocolError> {
    let permissions = match params.permissions.as_deref().unwrap_or("sandboxed") {
        "sandboxed" => PermissionMode::Sandboxed,
        // The acknowledgement is the client's to give; see the module documentation.
        "auto-approve" => PermissionMode::AutoApprove,
        "interactive" if human_available => PermissionMode::Interactive,
        "interactive" => {
            return Err(config_error(
                "--permissions interactive needs a human attached to the worker, and none \
                 is attached here: it is available only on the herdr surface. Use \
                 `sandboxed` or `auto-approve`.",
            ));
        }
        other => {
            return Err(config_error(format!(
                "unknown permission posture `{other}` (sandboxed, auto-approve, interactive)"
            )));
        }
    };
    Ok(BackendOptions {
        model: params.model.clone(),
        effort: params.effort.clone(),
        executable: None,
        permissions,
        external_sandbox: params.external_sandbox,
        human_available,
        command_template: params.cmd.clone(),
        prefer_steering: params.steer,
        extra_args: Vec::new(),
    })
}

// ---------------------------------------------------------------------------
// The parts
// ---------------------------------------------------------------------------

/// Load the responsibility definitions: the compiled-in set plus an override directory.
///
/// # Errors
/// A definition that does not parse, or one that would never propose anything.
pub fn load_definitions_from(dir: Option<&Path>) -> Result<DefinitionSet, FactoryError> {
    let mut set = DefinitionSet::bundled().map_err(|e| FactoryError::Bundled(e.to_string()))?;
    if let Some(dir) = dir {
        if dir.is_dir() {
            set.overlay_dir(dir).map_err(|e| FactoryError::Overlay {
                dir: dir.to_owned(),
                message: e.to_string(),
            })?;
        }
    }
    let unknown = set.unknown_ids();
    if !unknown.is_empty() {
        return Err(FactoryError::Unknown(unknown.join(", ")));
    }
    Ok(set)
}

/// Load command evidence providers from a config file, if it exists.
///
/// # Errors
/// A config file that cannot be read or parsed.
pub fn load_evidence_from(path: Option<&Path>) -> Result<EvidencePlan, FactoryError> {
    let Some(path) = path else {
        return Ok(EvidencePlan::default());
    };
    if !path.is_file() {
        return Ok(EvidencePlan::default());
    }
    let source = std::fs::read_to_string(path).map_err(|source| FactoryError::Read {
        path: path.to_owned(),
        source,
    })?;
    EvidencePlan::from_toml(&source, &path.display().to_string())
        .map_err(|e| FactoryError::Evidence(e.to_string()))
}

/// Construct the configured coding-agent backend.
///
/// A backend whose posture it cannot express fails here, at startup, rather than
/// launching something unpinned and discovering it later.
fn build_worker(
    config: &RunConfig,
    options: &BackendOptions,
    data_dir: &Path,
) -> Result<Arc<dyn Worker>, ProtocolError> {
    if config.worker == WorkerKind::Simulated {
        return Ok(Arc::new(SimulatedWorker::new()));
    }
    // Validate the posture *before* any run state exists. A backend that cannot
    // express what was asked for must fail here, not after a worker has been announced
    // and then failed to launch.
    aftman_workers::backends::validate(config.worker, options)
        .map_err(|e| config_error(format!("--worker {}: {e}", config.worker.as_str())))?;
    let mission_dir = data_dir.join(crate::statedir::MISSIONS_DIR);
    let worker = SubprocessWorker::backend(config.worker, options.clone(), mission_dir);
    Ok(Arc::new(worker))
}

// ---------------------------------------------------------------------------
// jev.usage
// ---------------------------------------------------------------------------

/// Recorded decision-model usage, filtered.
#[derive(Debug, Clone)]
pub struct UsageReport {
    pub entries: Vec<UsageEntry>,
    pub totals: UsageTotals,
}

impl UsageReport {
    /// The `jev.usage` result: opaque to the protocol crate, stable here.
    #[must_use]
    pub fn to_json(&self) -> Value {
        json!({
            "entries": self.entries,
            "totals": {
                "requests": self.totals.requests,
                "failed_requests": self.totals.failed_requests,
                "input_tokens": self.totals.input_tokens,
                "output_tokens": self.totals.output_tokens,
                "total_tokens": self.totals.total_tokens(),
                "retried_requests": self.totals.retried_requests,
                "compacted_requests": self.totals.compacted_requests,
            },
        })
    }
}

/// Read the usage log in `data_dir` and keep what matches.
///
/// # Errors
/// `internal` when the log exists and cannot be read.
pub fn usage_report(
    data_dir: &Path,
    since: Option<OffsetDateTime>,
    run_id: Option<RunId>,
) -> Result<UsageReport, ProtocolError> {
    let recorder = UsageRecorder::in_state_dir(data_dir);
    let entries = recorder
        .read()
        .map_err(|e| internal(format!("reading the usage log: {e}")))?;
    let entries: Vec<UsageEntry> = entries
        .into_iter()
        .filter(|entry| since.is_none_or(|t| entry.at >= t))
        .filter(|entry| run_id.is_none_or(|id| entry.run_id == Some(id)))
        .collect();
    let totals = aftman_jev::usage::totals(&entries);
    Ok(UsageReport { entries, totals })
}

#[cfg(test)]
mod tests {
    use super::*;
    use aftman_daemon::{EventSink, VecSink};
    use aftman_store::MemoryRunStore;

    fn context() -> RuntimeContext {
        let sink: Arc<dyn EventSink> = Arc::new(VecSink::default());
        RuntimeContext {
            store: Arc::new(MemoryRunStore::default()),
            sink,
            clock: Arc::new(SystemClock),
        }
    }

    fn factory_with(inputs: FactoryInputs, mode: Mode) -> (tempfile::TempDir, CliFactory) {
        let dir = tempfile::tempdir().unwrap();
        let factory = CliFactory::new(dir.path().join("state"), inputs, mode);
        (dir, factory)
    }

    fn factory() -> (tempfile::TempDir, CliFactory) {
        factory_with(FactoryInputs::empty(), Mode::Embedded)
    }

    fn params(dir: &Path) -> RunStartParams {
        RunStartParams {
            worker: Some("simulated".into()),
            simulate_model: true,
            ..RunStartParams::new(dir, "job")
        }
    }

    async fn code(factory: &CliFactory, params: &RunStartParams) -> Option<ErrorCode> {
        factory
            .prepare(params, context())
            .await
            .err()
            .map(|e| e.code)
    }

    #[tokio::test]
    async fn a_simulated_run_builds_and_records_how_it_was_launched() {
        let (dir, factory) = factory();
        let prepared = factory
            .prepare(&params(dir.path()), context())
            .await
            .unwrap();
        assert_eq!(prepared.built.job.repo, dir.path());
    }

    #[tokio::test]
    async fn the_launch_record_carries_the_posture() {
        let (dir, factory) = factory();
        let store = Arc::new(MemoryRunStore::default());
        let sink = Arc::new(VecSink::default());
        let mut p = params(dir.path());
        p.permissions = Some("auto-approve".into());
        p.external_sandbox = true;
        let prepared = factory
            .prepare(
                &p,
                RuntimeContext {
                    store,
                    sink: sink.clone(),
                    clock: Arc::new(SystemClock),
                },
            )
            .await
            .unwrap();
        let _ = prepared
            .built
            .runtime
            .start(prepared.built.job)
            .await
            .unwrap();
        let created = sink
            .events()
            .into_iter()
            .find(|e| e.kind == aftman_core::event::EventKind::RunCreated)
            .unwrap();
        let launch = &created.payload["launch"];
        assert_eq!(launch["permissions"], "auto-approve");
        assert_eq!(launch["external_sandbox"], "true");
        assert_eq!(launch["steering"], "available");
        assert_eq!(launch["supervisor"], "simulated");
    }

    #[tokio::test]
    async fn a_bad_repo_is_invalid_repo() {
        let (dir, factory) = factory();
        let missing = params(&dir.path().join("nope"));
        assert_eq!(code(&factory, &missing).await, Some(ErrorCode::InvalidRepo));
        let relative = RunStartParams {
            repo: PathBuf::from("relative/path"),
            ..params(dir.path())
        };
        assert_eq!(
            code(&factory, &relative).await,
            Some(ErrorCode::InvalidRepo)
        );
    }

    #[tokio::test]
    async fn bad_configuration_is_invalid_config() {
        let (dir, factory) = factory();
        for tweak in [
            |p: &mut RunStartParams| p.worker = Some("gpt".into()),
            |p: &mut RunStartParams| p.worker = Some("herdr:pane".into()),
            |p: &mut RunStartParams| p.permissions = Some("yolo".into()),
            |p: &mut RunStartParams| p.permissions = Some("interactive".into()),
            |p: &mut RunStartParams| p.verify = Some("sometimes".into()),
            |p: &mut RunStartParams| p.worker = Some("generic".into()),
            |p: &mut RunStartParams| p.worker = Some("pi".into()),
        ] {
            let mut p = params(dir.path());
            tweak(&mut p);
            assert_eq!(
                code(&factory, &p).await,
                Some(ErrorCode::InvalidConfig),
                "{p:?}"
            );
        }
    }

    #[tokio::test]
    async fn interactive_names_herdr_and_auto_approve_is_not_checked_here() {
        let (dir, factory) = factory();
        let mut p = params(dir.path());
        p.permissions = Some("interactive".into());
        let error = factory.prepare(&p, context()).await.err().unwrap();
        assert!(error.message.contains("herdr"));
        p.permissions = Some("auto-approve".into());
        assert!(factory.prepare(&p, context()).await.is_ok());
    }

    #[tokio::test]
    async fn no_provider_is_model_unavailable_whatever_the_machine_has() {
        // Explicit, empty inputs: the developer's own key and .env cannot matter.
        for mode in [Mode::Embedded, Mode::Daemon] {
            let (dir, factory) = factory_with(FactoryInputs::empty(), mode);
            let mut p = params(dir.path());
            p.simulate_model = false;
            let error = factory.prepare(&p, context()).await.err().unwrap();
            assert_eq!(error.code, ErrorCode::ModelUnavailable);
            assert!(error.message.contains("--simulate-model"));
            assert!(error.message.contains("TYPESAFE_API_KEY") || mode == Mode::Daemon);
            if mode == Mode::Daemon {
                assert!(error.message.contains("aftman stop"), "{}", error.message);
                assert!(error.message.contains("started without a provider"));
            }
        }
    }

    #[tokio::test]
    async fn a_configured_provider_builds_and_the_view_is_redacted() {
        let inputs = FactoryInputs {
            provider_env: BTreeMap::from([
                ("TYPESAFE_API_KEY".to_owned(), "sk-very-secret".to_owned()),
                (
                    "TYPESAFE_BASE_URL".to_owned(),
                    "https://user:hunter2@api.example.com/api".to_owned(),
                ),
            ]),
            ..FactoryInputs::empty()
        };
        let (dir, factory) = factory_with(inputs, Mode::Daemon);
        let mut p = params(dir.path());
        p.simulate_model = false;
        assert!(factory.prepare(&p, context()).await.is_ok());
        let info = factory.daemon_info();
        assert_eq!(info["provider"], "configured");
        assert_eq!(info["base_url"], "https://api.example.com/api");
        let everything = format!("{info:?}");
        assert!(!everything.contains("sk-very-secret") && !everything.contains("hunter2"));
    }

    #[tokio::test]
    async fn the_daemon_info_says_not_configured_for_empty_inputs() {
        let (_dir, factory) = factory_with(FactoryInputs::empty(), Mode::Daemon);
        assert_eq!(factory.daemon_info()["provider"], "not configured");
    }

    #[tokio::test]
    async fn evidence_and_overrides_come_only_from_the_injected_paths() {
        // A malformed evidence file at an injected path is an error; with empty inputs
        // nothing is read at all.
        let dir = tempfile::tempdir().unwrap();
        let bad = dir.path().join("evidence.toml");
        std::fs::write(&bad, "this is [not valid").unwrap();
        let (work, broken) = factory_with(
            FactoryInputs {
                evidence_config: Some(bad),
                ..FactoryInputs::empty()
            },
            Mode::Embedded,
        );
        assert_eq!(
            code(&broken, &params(work.path())).await,
            Some(ErrorCode::InvalidConfig)
        );
        let (work, clean) = factory();
        assert!(clean.prepare(&params(work.path()), context()).await.is_ok());
    }

    #[tokio::test]
    async fn the_state_directory_is_private() {
        use std::os::unix::fs::PermissionsExt as _;
        let (dir, factory) = factory();
        factory
            .prepare(&params(dir.path()), context())
            .await
            .unwrap();
        for sub in ["", "missions", "evidence-tmp", "runs"] {
            let path = dir.path().join("state").join(sub);
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o700, "{}", path.display());
        }
    }

    #[tokio::test]
    async fn responsibilities_and_empty_usage() {
        let (_dir, factory) = factory();
        let list = factory.responsibilities().await.unwrap();
        assert!(
            list.responsibilities
                .iter()
                .any(|r| r.id == "core.completion")
        );
        assert!(
            list.responsibilities
                .iter()
                .any(|r| r.checks.iter().any(|c| c == "ready_to_finish"))
        );
        let usage = factory.usage(&JevUsageParams::default()).await.unwrap();
        assert_eq!(usage["totals"]["requests"], 0);
        let bad = JevUsageParams {
            since: Some("last tuesday".into()),
            run_id: None,
        };
        assert_eq!(
            factory.usage(&bad).await.unwrap_err().code,
            ErrorCode::InvalidRequest
        );
    }
}
