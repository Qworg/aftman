//! The subcommands. Each is a client of the daemon, or of the embedded runtime under
//! `--no-daemon`.

pub mod control;
pub mod inspect;
pub mod operate;
pub mod run;
pub mod serve;

use std::sync::Arc;

use aftman_daemon::lifecycle::DaemonPaths;
use aftman_daemon::protocol::DaemonStatusResult;
use aftman_store::FileRunStore;

use crate::autostart::{self, Locations, Starter};
use crate::cli::{Cli, Command, ResponsibilitiesAction};
use crate::client::{Client, ClientConfig};
use crate::errln;
use crate::error::CliError;
use crate::factory::FactoryInputs;
use crate::render::GlobalFlags;

impl From<aftman_daemon::protocol::ProtocolError> for CliError {
    fn from(error: aftman_daemon::protocol::ProtocolError) -> Self {
        Self::Client(crate::client::ClientError::Protocol(error))
    }
}

/// What every command needs to know about how it was invoked.
#[derive(Debug, Clone)]
pub struct Ctx {
    /// `--socket` and `--data-dir`, made absolute: the daemon does not share our
    /// working directory.
    pub locations: Locations,
    pub quiet: bool,
    pub no_daemon: bool,
    pub client: ClientConfig,
    /// Provider environment, override directories and config paths, read once.
    pub inputs: FactoryInputs,
}

impl Ctx {
    /// # Errors
    /// The current directory cannot be determined (to make paths absolute).
    pub fn from_cli(cli: &Cli) -> Result<Self, CliError> {
        let locations = Locations {
            socket_flag: cli.socket.clone(),
            data_dir_flag: cli.data_dir.clone(),
        }
        .absolute()
        .map_err(CliError::Location)?;
        Ok(Self {
            locations,
            quiet: cli.quiet,
            no_daemon: cli.no_daemon,
            client: ClientConfig::default(),
            inputs: FactoryInputs::from_process(),
        })
    }

    /// The global flags to repeat in a command we print for the user to copy.
    #[must_use]
    pub fn flags(&self) -> GlobalFlags {
        GlobalFlags {
            socket: self
                .locations
                .socket_flag
                .as_ref()
                .map(|p| p.display().to_string()),
            data_dir: self
                .locations
                .data_dir_flag
                .as_ref()
                .map(|p| p.display().to_string()),
        }
    }

    /// Where the daemon's socket and pid file are.
    ///
    /// # Errors
    /// No location can be determined, or the path is unusable.
    pub fn paths(&self) -> Result<DaemonPaths, CliError> {
        Ok(autostart::resolve(&self.locations)?)
    }

    /// The state directory.
    ///
    /// # Errors
    /// Neither `--data-dir` nor a default location is available.
    pub fn data_dir(&self) -> Result<std::path::PathBuf, CliError> {
        Ok(autostart::data_dir(&self.locations)?)
    }

    /// The store, read directly (`--no-daemon`).
    ///
    /// # Errors
    /// As [`Ctx::data_dir`].
    pub fn store(&self) -> Result<Arc<FileRunStore>, CliError> {
        Ok(Arc::new(FileRunStore::new(self.data_dir()?)))
    }

    /// A client of the daemon, starting it if it is not running, after checking that
    /// it is the daemon this command means to talk to.
    ///
    /// - A daemon serving a **different state directory** is a hard error: the command
    ///   would silently read and write someone else's store.
    /// - A daemon of a **different version** is a warning.
    ///
    /// # Errors
    /// The daemon could not be started or reached, or serves another state directory.
    pub async fn daemon(&self) -> Result<(Client, DaemonPaths, DaemonStatusResult), CliError> {
        let paths = self.paths()?;
        let mut starter = Starter::for_current_exe(&self.locations)?;
        starter.client = self.client;
        let mut client = autostart::ensure_daemon(&paths, &starter).await?;
        let status = client.status().await?;
        self.check_daemon(&paths, &status)?;
        Ok((client, paths, status))
    }

    /// The checks [`Ctx::daemon`] applies to a daemon's status.
    ///
    /// # Errors
    /// The daemon serves a different state directory than this command asked for.
    pub fn check_daemon(
        &self,
        paths: &DaemonPaths,
        status: &DaemonStatusResult,
    ) -> Result<(), CliError> {
        if let (Some(theirs), Ok(mine)) = (&status.state_dir, self.data_dir()) {
            if !same_dir(theirs, &mine) {
                return Err(CliError::usage(format!(
                    "the daemon at {} (pid {}) serves the state directory {}, but this command \
                     was told to use {}. Running on would read and write a different store \
                     than you asked for. Pass the matching --data-dir, or `aftman stop` and \
                     let the next command start a daemon for the one you want.",
                    paths.socket.display(),
                    status.pid,
                    theirs.display(),
                    mine.display()
                )));
            }
        }
        if status.version != env!("CARGO_PKG_VERSION") {
            errln!(
                "warning: the daemon (pid {}) is aftman {}, this command is {}. \
                 `aftman stop` restarts it with the new version.",
                status.pid,
                status.version,
                env!("CARGO_PKG_VERSION")
            );
        }
        Ok(())
    }

    /// A clear error for a command that has no meaning without a daemon.
    ///
    /// # Errors
    /// Always, under `--no-daemon`.
    pub fn require_daemon(&self, what: &str) -> Result<(), CliError> {
        if self.no_daemon {
            Err(CliError::usage(format!(
                "`{what}` acts on a run supervised by a daemon; --no-daemon runs live in \
                 their own process and cannot be reached from here"
            )))
        } else {
            Ok(())
        }
    }
}

/// Whether two paths name the same directory: equal as written, or the same after
/// resolving symlinks.
fn same_dir(a: &std::path::Path, b: &std::path::Path) -> bool {
    if a == b {
        return true;
    }
    match (a.canonicalize(), b.canonicalize()) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

/// How the daemon's view of the provider differs from this shell's, if it does.
#[must_use]
pub fn provider_disagreement(ctx: &Ctx, status: &DaemonStatusResult) -> Option<String> {
    let theirs = status.info.get("provider").map(String::as_str)?;
    let mine = ctx.inputs.provider_view().ok()?;
    let mine_configured = mine.configured;
    let theirs_configured = theirs == "configured";
    if mine_configured && !theirs_configured {
        return Some(
            "this shell has a provider configured, but the daemon was started without one: \
             runs through the daemon will fail with model_unavailable. A daemon keeps the \
             environment it was started with. Put the key in the config .env file (re-read \
             every run), or `aftman stop` so the next command starts a daemon with this \
             environment."
                .to_owned(),
        );
    }
    if !mine_configured && theirs_configured {
        return Some(
            "the daemon has a provider configured but this shell does not; runs through the \
             daemon will use the daemon's."
                .to_owned(),
        );
    }
    if mine_configured
        && (status.info.get("model") != Some(&mine.model)
            || status.info.get("base_url") != Some(&mine.base_url))
    {
        return Some(
            "the daemon's model or base URL differs from this shell's; runs through the \
             daemon use the daemon's. `aftman stop` restarts it with this environment."
                .to_owned(),
        );
    }
    None
}

/// Lines describing the daemon's provider next to this shell's.
#[must_use]
pub fn provider_comparison(ctx: &Ctx, status: &DaemonStatusResult) -> Vec<String> {
    let mut lines = Vec::new();
    if let Some(provider) = status.info.get("provider") {
        let mut line = format!("  provider   {provider}");
        if let Some(model) = status.info.get("model") {
            line.push_str(&format!(", model {model}"));
        }
        if let Some(url) = status.info.get("base_url") {
            line.push_str(&format!(", {url}"));
        }
        lines.push(format!("{line}  (the daemon's environment)"));
        if let Some(error) = status.info.get("provider_error") {
            lines.push(format!("             problem: {error}"));
        }
    }
    if let Ok(view) = ctx.inputs.provider_view() {
        lines.push(format!(
            "  yours      {}, model {}, {}  (this shell)",
            if view.configured {
                "configured"
            } else {
                "not configured"
            },
            view.model,
            view.base_url
        ));
    }
    if let Some(note) = provider_disagreement(ctx, status) {
        lines.push(format!("  NOTE: {note}"));
    }
    lines
}

/// Run the parsed command line; the value is the process exit code.
///
/// # Errors
/// Whatever the command reports; [`CliError::exit_code`] says how to exit.
pub async fn dispatch(cli: Cli) -> Result<u8, CliError> {
    let ctx = Ctx::from_cli(&cli)?;
    match cli.command {
        Command::Run(args) => run::run(&ctx, args).await,
        Command::Demo(args) => run::demo(&ctx, args).await,
        Command::Ls { status, limit } => inspect::ls(&ctx, &status, limit).await,
        Command::Watch(target) => crate::tui::watch(&ctx, target).await,
        Command::Show(target) => inspect::show(&ctx, target).await,
        Command::Decisions(target) => inspect::decisions(&ctx, target).await,
        Command::Logs { target, follow } => inspect::logs(&ctx, target, follow).await,
        Command::Steer { run, message } => control::steer(&ctx, &run, &message).await,
        Command::Cancel(target) => control::cancel(&ctx, target).await,
        Command::Answer(args) => control::answer(&ctx, args).await,
        Command::Purge { run, all } => control::purge(&ctx, run, all).await,
        Command::Responsibilities { action } => match action {
            ResponsibilitiesAction::List => operate::responsibilities_list(&ctx).await,
            ResponsibilitiesAction::Show { id } => operate::responsibilities_show(&ctx, &id),
        },
        Command::Usage { since, run_id } => operate::usage(&ctx, since, run_id),
        Command::Doctor { probe } => operate::doctor(&ctx, probe).await,
        Command::Serve { detached } => serve::serve(&ctx, detached).await,
        Command::Stop => serve::stop(&ctx).await,
        Command::Status => serve::status(&ctx).await,
    }
}
