//! Read-only commands: `ls`, `show`, `decisions`, `logs`.
//!
//! Through the daemon by default (starting it if needed). With `--no-daemon` they read
//! the store directly and never start anything; `logs -f` has no meaning there.

use std::sync::Arc;

use aftman_core::event::EventEnvelope;
use aftman_core::ids::{EventSeq, RunId};
use aftman_core::job::FactoryStatus;
use aftman_core::run::{RunFilter, RunState, RunSummary};
use aftman_store::{FileRunStore, RunStore, StoreError};

use crate::cli::{RunTarget, parse_status};
use crate::client::Client;
use crate::commands::Ctx;
use crate::error::{CliError, exit};
use crate::follow::Follow;
use crate::render;
use crate::runid::{RunSpec, parse_full, resolve_prefix};
use crate::{errln, outln};

/// Prefix resolution looks at this many of the newest runs.
const PREFIX_SCAN: usize = 500;

/// Where a read-only command gets its data.
pub enum Reader {
    Daemon(Box<Client>),
    Store(Arc<FileRunStore>),
}

fn store_error(error: StoreError) -> CliError {
    match error {
        StoreError::NotFound(id) => CliError::usage(format!("unknown run {id}")),
        other => CliError::Store(other),
    }
}

impl Reader {
    /// A reader for this invocation: the daemon, or the store under `--no-daemon`.
    ///
    /// # Errors
    /// The daemon could not be started or reached, or no state directory exists.
    pub async fn open(ctx: &Ctx) -> Result<Self, CliError> {
        if ctx.no_daemon {
            Ok(Self::Store(ctx.store()?))
        } else {
            Ok(Self::Daemon(Box::new(ctx.daemon().await?.0)))
        }
    }

    /// Newest-first listing.
    ///
    /// # Errors
    /// Daemon or store errors.
    pub async fn list(
        &mut self,
        status: Option<Vec<FactoryStatus>>,
        limit: usize,
    ) -> Result<Vec<RunSummary>, CliError> {
        let limit = limit.max(1);
        match self {
            Self::Daemon(client) => Ok(client.all_runs(status, limit).await?),
            Self::Store(store) => store
                .list(&RunFilter {
                    status,
                    limit: Some(limit),
                    ..RunFilter::default()
                })
                .await
                .map_err(store_error),
        }
    }

    /// One run's state.
    ///
    /// # Errors
    /// `unknown run`, or an unreadable one.
    pub async fn state(&mut self, id: RunId) -> Result<RunState, CliError> {
        match self {
            Self::Daemon(client) => Ok(client.run_get(id).await?),
            Self::Store(store) => store.load(id).await.map_err(store_error),
        }
    }

    /// One run's events from `from`, all of them.
    ///
    /// # Errors
    /// `unknown run`, or a log that is not contiguous.
    pub async fn events(
        &mut self,
        id: RunId,
        from: EventSeq,
    ) -> Result<Vec<EventEnvelope>, CliError> {
        match self {
            Self::Daemon(client) => Ok(client.all_events(id, from).await?),
            Self::Store(store) => store.events(id, from).await.map_err(store_error),
        }
    }

    /// Turn what the user typed into a run id.
    ///
    /// # Errors
    /// No runs, no match, or an ambiguous prefix.
    pub async fn resolve(&mut self, spec: RunSpec) -> Result<RunId, CliError> {
        match spec {
            RunSpec::Last => self
                .list(None, 1)
                .await?
                .first()
                .map(|run| run.id)
                .ok_or_else(|| CliError::usage("there are no runs")),
            RunSpec::Id(text) => {
                if let Some(id) = parse_full(&text) {
                    return Ok(id);
                }
                let known: Vec<RunId> = self
                    .list(None, PREFIX_SCAN)
                    .await?
                    .into_iter()
                    .map(|run| run.id)
                    .collect();
                resolve_prefix(&text, &known)
            }
        }
    }
}

/// Resolve a `RunTarget` against a reader.
pub(crate) async fn target(reader: &mut Reader, target: RunTarget) -> Result<RunId, CliError> {
    let spec = RunSpec::from_args(target.run, target.last)?;
    reader.resolve(spec).await
}

/// `aftman ls`.
///
/// # Errors
/// Daemon or store errors.
pub async fn ls(ctx: &Ctx, status: &[String], limit: usize) -> Result<u8, CliError> {
    let filter = if status.is_empty() {
        None
    } else {
        Some(
            status
                .iter()
                .map(|s| parse_status(s))
                .collect::<Result<Vec<_>, _>>()?,
        )
    };
    let mut reader = Reader::open(ctx).await?;
    let runs = reader.list(filter, limit).await?;
    if runs.is_empty() {
        match &reader {
            Reader::Store(store) => outln!("no runs in {}", store.root().display())?,
            Reader::Daemon(_) => outln!("no runs")?,
        }
        return Ok(exit::FINISHED);
    }
    for line in render::run_table(&runs) {
        crate::out::line(&line)?;
    }
    Ok(exit::FINISHED)
}

/// `aftman show`.
///
/// The status is reconciled with the log: the snapshot is written after the events it
/// summarises, so a reader can catch it behind, and the log is the truth. The
/// `aftman answer` hint is printed only when a daemon is in a position to take the
/// answer (a daemon reader, and a run waiting for one).
///
/// # Errors
/// `unknown run`, daemon or store errors.
pub async fn show(ctx: &Ctx, run: RunTarget) -> Result<u8, CliError> {
    let mut reader = Reader::open(ctx).await?;
    let id = target(&mut reader, run).await?;
    let state = reader.state(id).await?;
    let events = reader.events(id, EventSeq::FIRST).await?;
    let answerable = matches!(reader, Reader::Daemon(_));
    for line in render::show_lines(&state, &events, &ctx.flags(), answerable) {
        crate::out::line(&line)?;
    }
    Ok(exit::FINISHED)
}

/// `aftman decisions`.
///
/// # Errors
/// `unknown run`, daemon or store errors.
pub async fn decisions(ctx: &Ctx, run: RunTarget) -> Result<u8, CliError> {
    let mut reader = Reader::open(ctx).await?;
    let id = target(&mut reader, run).await?;
    let events = reader.events(id, EventSeq::FIRST).await?;
    let lines = render::decision_lines(&events);
    if lines.is_empty() {
        outln!("no decisions recorded yet")?;
    }
    for line in lines {
        crate::out::line(&line)?;
    }
    Ok(exit::FINISHED)
}

/// `aftman logs [-f]`: the text of the run's `WORKER_OUTPUT` events.
///
/// The text is untrusted (a worker wrote it) and is sanitised on the way out: escape
/// sequences, control characters and bidi overrides are removed; tabs and newlines are
/// kept. There is no raw mode.
///
/// # Errors
/// `unknown run`, daemon or store errors, `-f` under `--no-daemon`.
pub async fn logs(ctx: &Ctx, run: RunTarget, follow: bool) -> Result<u8, CliError> {
    if follow && ctx.no_daemon {
        return Err(CliError::usage(
            "`logs -f` follows a run through the daemon; with --no-daemon the run's own \
             terminal is the live view, and `logs` without -f reads what is stored",
        ));
    }
    let mut reader = Reader::open(ctx).await?;
    let id = target(&mut reader, run).await?;
    if !follow {
        let events = reader.events(id, EventSeq::FIRST).await?;
        for event in &events {
            if let Some(text) = render::worker_output_text(event) {
                crate::out::text(text)?;
            }
        }
        return Ok(exit::FINISHED);
    }

    let paths = ctx.paths()?;
    let mut stream = Follow::open(&paths.socket, id, EventSeq::FIRST, ctx.client).await?;
    if stream.is_polling() {
        errln!(
            "run {id} is not being driven by this daemon (it has ended, or another process \
             owns it); following its log until it ends"
        );
    }
    while let Some(event) = stream.next().await? {
        if let Some(text) = render::worker_output_text(&event) {
            crate::out::text(text)?;
        }
    }
    Ok(exit::FINISHED)
}
