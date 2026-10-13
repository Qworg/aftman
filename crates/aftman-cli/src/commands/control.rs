//! Intervening: `steer`, `cancel`, `answer`, `purge`.
//!
//! These act on runs a daemon supervises; under `--no-daemon` they are a clear error,
//! except `purge`, which can delete from the store directly and still refuses a run
//! someone is driving (the ownership claim decides).

use aftman_core::assessment::HumanDecision;
use aftman_core::ids::EscalationId;
use aftman_core::worker::SteerOutcome;
use aftman_daemon::probe_state_dir;
use aftman_daemon::protocol::RunPurgeParams;
use aftman_store::{RunStore, StoreError};

use crate::cli::{AnswerArgs, RunTarget};
use crate::commands::Ctx;
use crate::commands::inspect::{Reader, target};
use crate::error::{CliError, exit};
use crate::outln;
use crate::runid::RunSpec;

/// `aftman steer <run> "<message>"`.
///
/// # Errors
/// Anything the daemon refuses. A message the backend rejects, or a worker with no
/// input channel, is reported as such with a non-zero exit.
pub async fn steer(ctx: &Ctx, run: &str, message: &str) -> Result<u8, CliError> {
    ctx.require_daemon("steer")?;
    let mut reader = Reader::open(ctx).await?;
    let id = reader.resolve(RunSpec::Id(run.to_owned())).await?;
    let Reader::Daemon(client) = &mut reader else {
        return Err(CliError::usage("steer needs the daemon"));
    };
    match client.run_steer(id, message).await? {
        SteerOutcome::Delivered => {
            outln!("steer delivered to run {id}")?;
            Ok(exit::FINISHED)
        }
        SteerOutcome::Queued => {
            outln!("steer accepted by run {id}; delivery to the worker is not confirmed")?;
            Ok(exit::FINISHED)
        }
        SteerOutcome::Rejected { reason } => Err(CliError::Failed(format!(
            "the worker rejected the steer for run {id}: {reason}"
        ))),
        SteerOutcome::Unsupported => Err(CliError::Failed(format!(
            "the worker of run {id} has no live input channel; it cannot be steered"
        ))),
    }
}

/// `aftman cancel <run>`.
///
/// # Errors
/// `unknown_run`, `wrong_state` (not live), daemon errors.
pub async fn cancel(ctx: &Ctx, run: RunTarget) -> Result<u8, CliError> {
    ctx.require_daemon("cancel")?;
    let mut reader = Reader::open(ctx).await?;
    let id = target(&mut reader, run).await?;
    let Reader::Daemon(client) = &mut reader else {
        return Err(CliError::usage("cancel needs the daemon"));
    };
    client.run_cancel(id).await?;
    outln!("cancel requested for run {id}")?;
    Ok(exit::FINISHED)
}

/// `aftman answer <run> <escalation-id> [text] [--accept|--recheck|--stop|--claim-complete]`.
///
/// # Errors
/// A bad escalation id, no decision, or a daemon refusal (`wrong_state`: the
/// escalation was already answered, or the run is not waiting).
pub async fn answer(ctx: &Ctx, args: AnswerArgs) -> Result<u8, CliError> {
    ctx.require_daemon("answer")?;
    let (decision, note) = args.decision()?;
    let escalation: EscalationId = args.escalation_id.trim().parse().map_err(|_| {
        CliError::usage(format!(
            "`{}` is not an escalation id (see `aftman show <run>`)",
            args.escalation_id
        ))
    })?;
    let mut reader = Reader::open(ctx).await?;
    let id = reader.resolve(RunSpec::Id(args.run.clone())).await?;
    let Reader::Daemon(client) = &mut reader else {
        return Err(CliError::usage("answer needs the daemon"));
    };
    client
        .run_answer(id, escalation, decision.clone(), note)
        .await?;
    outln!(
        "answered escalation {escalation} of run {id}: {}",
        describe(&decision)
    )?;
    Ok(exit::FINISHED)
}

fn describe(decision: &HumanDecision) -> &'static str {
    match decision {
        HumanDecision::Accept => "accept",
        HumanDecision::Recheck => "recheck",
        HumanDecision::Stop => "stop",
        HumanDecision::ClaimComplete => "claim complete",
        HumanDecision::Guidance { .. } => "guidance for the worker",
    }
}

/// `aftman purge <run> | --all`.
///
/// # Errors
/// `unknown_run`, or `wrong_state` for a run that is live.
pub async fn purge(ctx: &Ctx, run: Option<String>, all: bool) -> Result<u8, CliError> {
    if ctx.no_daemon {
        // Deleting run files from under a daemon leaves its replay ring serving a run
        // that no longer exists (and `logs -f` printing purged output). Refuse.
        if let Some(pid) = probe_state_dir(&ctx.data_dir()?).await {
            return Err(CliError::usage(format!(
                "a daemon (pid {pid}) is serving this state directory; `purge --no-daemon` \
                 would delete runs out from under it. Run `aftman purge` without --no-daemon, \
                 or `aftman stop` first."
            )));
        }
    }
    let mut reader = Reader::open(ctx).await?;
    let id = match (run, all) {
        (Some(run), false) => Some(reader.resolve(RunSpec::Id(run)).await?),
        (None, true) => None,
        _ => return Err(CliError::usage("name one run, or pass --all")),
    };
    let purged = match &mut reader {
        Reader::Daemon(client) => purge_via_daemon(client, id, all).await?,
        Reader::Store(store) => purge_store(store.as_ref(), id).await?,
    };
    outln!("purged {purged} run(s)")?;
    Ok(exit::FINISHED)
}

/// `run.purge`, tolerating the moment just after a run ends.
///
/// The daemon announces `RUN_FINISHED` slightly before it drops the run from its live
/// table, so a purge fired the instant a `run` returned can be told the run is still
/// live. When the run's stored state is already terminal that refusal is stale, so
/// retry briefly (bounded) rather than make every script sleep.
async fn purge_via_daemon(
    client: &mut crate::client::Client,
    id: Option<aftman_core::ids::RunId>,
    all: bool,
) -> Result<u64, CliError> {
    const RETRIES: u32 = 40;
    let params = RunPurgeParams { run_id: id, all };
    for _ in 0..RETRIES {
        let error = match client.run_purge(params.clone()).await {
            Ok(purged) => return Ok(purged),
            Err(error) => error,
        };
        let live = error.protocol().map(|e| e.code)
            == Some(aftman_daemon::protocol::ErrorCode::WrongState);
        let ended = match id {
            Some(run) if live => client
                .run_get(run)
                .await
                .is_ok_and(|state| state.status.is_terminal()),
            _ => false,
        };
        if !ended {
            return Err(error.into());
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    Ok(client.run_purge(params).await?)
}

/// `purge` under `--no-daemon`: the same rule as the daemon's, taken straight from
/// the store. A run whose ownership claim is held (it is being driven, by a daemon or
/// by another `--no-daemon` process) is never deleted; `--all` skips it, a named one
/// is an error.
async fn purge_store(
    store: &dyn RunStore,
    only: Option<aftman_core::ids::RunId>,
) -> Result<u64, CliError> {
    let known = store.ids().await.map_err(CliError::Store)?;
    let ids = match only {
        // Taking a claim on an id that does not exist would create its directory.
        Some(id) if !known.contains(&id) => {
            return Err(CliError::usage(format!("unknown run {id}")));
        }
        Some(id) => vec![id],
        None => known,
    };
    let mut purged = 0;
    for id in ids {
        let Some(_claim) = store.claim(id).await.map_err(CliError::Store)? else {
            if only.is_some() {
                return Err(CliError::Failed(format!(
                    "run {id} is being driven by another process; cancel it before purging"
                )));
            }
            continue;
        };
        match store.delete(id).await {
            Ok(()) => purged += 1,
            Err(StoreError::NotFound(missing)) => {
                if only.is_some() {
                    return Err(CliError::usage(format!("unknown run {missing}")));
                }
            }
            Err(error) => return Err(CliError::Store(error)),
        }
    }
    Ok(purged)
}
