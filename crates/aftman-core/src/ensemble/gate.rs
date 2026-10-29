//! The irreversible gate: a backstop on what the cycle actually decides.
//!
//! The per-check ladder plans its opinions from the selection *before* it ran. But the
//! ladder itself changes the selection: a refresh can turn `StartWorker` into `Finish`,
//! and a contest on one check can unmask a `Stop` that rests on another. The gate runs
//! after the ladder, on the **resolved** checks:
//!
//! 1. Arbitrate. If the selected directive is not `Finish` or `Stop`, stop.
//! 2. Find the checks that selection rests on (`irreversible_dependencies`: every check
//!    a rule proposing it reads, plus every check whose side changes it) among those that
//!    are decisive and permitted.
//! 3. Each must carry an R2 verdict obtained this cycle under `Irreversible` semantics
//!    (an unusable answer contests). A verdict the ladder obtained under another trigger
//!    is re-read under those semantics; a missing one is bought. With the budget spent
//!    the check is held. With no R2 for the check, it stands, as it would in the ladder.
//! 4. Fold, and go back to 1.
//!
//! Refreshes happen only in the ladder, never here. See `docs/06-ensemble.md` §6.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::assessment::{CheckSuppression, Resolution, ResolvedCheck, Rung, Verdict};
use crate::ids::CheckKey;

use super::resolve::{
    CheckRecord, EnsembleLimit, KeyedOpinion, OpinionKey, OpinionSource, ReframeRequest,
    apply_opinions, opinion_from, reframe_verdict,
};
use super::settings::{EnsembleCounters, MarkVerdict, OpinionMark};
use super::trigger::{EnsembleContext, EnsembleError, EnsemblePlan, EnsembleTrigger};

/// What the gate did for one check an irreversible selection rested on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IrreversibleGuard {
    /// The directive that rested on the check: `Finish` or `Stop`.
    pub directive: String,
    pub outcome: GuardOutcome,
}

/// How the gate guarded a check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GuardOutcome {
    /// The ladder had already obtained its R2 verdict under an `Irreversible` trigger.
    AlreadyGuarded,
    /// The ladder obtained an R2 verdict under another trigger; it was re-read with
    /// `Irreversible` semantics (an unusable answer contests).
    Reinterpreted,
    /// No R2 verdict yet this cycle: one was bought, with `Irreversible` semantics.
    Bought,
    /// R2 is not available for this check; it stands, as it would in the ladder.
    Unavailable,
    /// The opinion budget is spent; the check is held.
    BudgetSpent,
}

/// Run the gate. Returns the number of rounds it took.
pub(super) async fn run(
    plan: &EnsemblePlan,
    ctx: &EnsembleContext<'_>,
    source: &dyn OpinionSource,
    checks: &mut BTreeMap<CheckKey, ResolvedCheck>,
    records: &mut Vec<CheckRecord>,
    counters: &mut EnsembleCounters,
) -> Result<u32, EnsembleError> {
    // Termination. Each round either stops, or guards at least one check not yet guarded
    // this cycle, and a check is guarded at most once, so there are at most
    // `checks.len() + 1` rounds. Guarding can only remove permission (a contest or a
    // hold), never grant it, so no round can make an earlier one's work stale.
    let bound = u32::try_from(checks.len())
        .unwrap_or(u32::MAX)
        .saturating_add(1);
    let mut guarded: BTreeSet<CheckKey> = BTreeSet::new();
    let mut rounds = 0;
    while rounds < bound {
        rounds += 1;
        let selected = super::trigger::select(ctx, checks);
        if !super::trigger::acts_irreversibly(ctx, &selected) {
            break;
        }
        let pending: Vec<CheckKey> = super::trigger::irreversible_dependencies(ctx, checks)
            .into_iter()
            .filter(|key| {
                checks
                    .get(key)
                    .is_some_and(|rc| rc.threshold.is_some() && rc.decisive && rc.permitted)
            })
            .filter(|key| !guarded.contains(key))
            .collect();
        if pending.is_empty() {
            break;
        }
        for key in pending {
            guarded.insert(key.clone());
            guard(
                plan,
                ctx,
                source,
                &key,
                selected.name(),
                checks,
                records,
                counters,
            )
            .await?;
        }
    }
    Ok(rounds)
}

#[allow(clippy::too_many_arguments)]
async fn guard(
    plan: &EnsemblePlan,
    ctx: &EnsembleContext<'_>,
    source: &dyn OpinionSource,
    key: &CheckKey,
    directive: &str,
    checks: &mut BTreeMap<CheckKey, ResolvedCheck>,
    records: &mut Vec<CheckRecord>,
    counters: &mut EnsembleCounters,
) -> Result<(), EnsembleError> {
    let settings = &plan.settings;
    let Some(rc) = checks.get(key).cloned() else {
        return Ok(());
    };
    let (Some(p), Some(threshold)) = (rc.jev_p, rc.threshold) else {
        return Ok(());
    };
    let index = match records.binary_search_by(|record| record.check.cmp(key)) {
        Ok(index) => index,
        Err(index) => {
            records.insert(
                index,
                CheckRecord {
                    check: key.clone(),
                    triggers: BTreeSet::new(),
                    baseline: plan
                        .baseline
                        .get(key)
                        .cloned()
                        .unwrap_or_else(|| rc.clone()),
                    resolved: rc.clone(),
                    rungs_used: Vec::new(),
                    refresh: None,
                    opinions: Vec::new(),
                    limits: Vec::new(),
                    escalation: None,
                    irreversible_guard: None,
                },
            );
            index
        }
    };
    let record = &mut records[index];
    let existing = record.opinions.iter().position(|keyed| {
        keyed.key.cycle == plan.cycle && keyed.key.rung == Rung::ReframeSameModel
    });

    let outcome = if let Some(position) = existing {
        if record.triggers.contains(&EnsembleTrigger::Irreversible) {
            GuardOutcome::AlreadyGuarded
        } else {
            let opinion = &mut record.opinions[position].opinion;
            opinion.verdict = reframe_verdict(
                p,
                opinion.p_reported,
                threshold,
                settings.framing_tolerance,
                true,
            );
            GuardOutcome::Reinterpreted
        }
    } else if !(settings.has(Rung::ReframeSameModel) && ctx.reframe_available.contains(key)) {
        GuardOutcome::Unavailable
    } else if counters.opinions_used.saturating_add(1) > settings.max_opinions_per_run {
        GuardOutcome::BudgetSpent
    } else {
        let opinion_key = OpinionKey {
            cycle: plan.cycle,
            check: key.clone(),
            rung: Rung::ReframeSameModel,
        };
        counters.opinions_used = counters.opinions_used.saturating_add(1);
        record.rungs_used.push(Rung::ReframeSameModel);
        let answer = source
            .reframe_same_model(&ReframeRequest {
                opinion: opinion_key.clone(),
                timeout: Duration::from_secs(settings.opinion_timeout_seconds),
            })
            .await?;
        let verdict = reframe_verdict(
            p,
            answer.p_negated,
            threshold,
            settings.framing_tolerance,
            true,
        );
        record.opinions.push(KeyedOpinion {
            key: opinion_key,
            opinion: opinion_from(Rung::ReframeSameModel, verdict, answer),
        });
        GuardOutcome::Bought
    };

    let resolved = match outcome {
        GuardOutcome::Unavailable => rc,
        GuardOutcome::BudgetSpent => {
            record.limits.push(EnsembleLimit::OpinionBudget {
                used: counters.opinions_used,
                cap: settings.max_opinions_per_run,
            });
            rc.suppress(
                CheckSuppression::Unresolved,
                Resolution::UnresolvedInBand {
                    ask: String::new(),
                    rungs: Vec::new(),
                },
            )
        }
        GuardOutcome::AlreadyGuarded | GuardOutcome::Reinterpreted | GuardOutcome::Bought => {
            let opinions: Vec<_> = record
                .opinions
                .iter()
                .map(|keyed| keyed.opinion.clone())
                .collect();
            let verdict = if opinions
                .iter()
                .any(|o| matches!(o.verdict, Verdict::Contest { .. }))
            {
                MarkVerdict::Contest
            } else if opinions.iter().any(|o| o.verdict == Verdict::Abstain) {
                MarkVerdict::Abstain
            } else {
                MarkVerdict::Confirm
            };
            counters.checks.entry(key.clone()).or_default().last_opinion = Some(OpinionMark {
                cycle: plan.cycle,
                verdict,
                side: rc.side,
                irreversible: true,
            });
            apply_opinions(rc, &opinions)
        }
    };
    record.irreversible_guard = Some(IrreversibleGuard {
        directive: directive.to_owned(),
        outcome,
    });
    record.resolved = resolved.clone();
    checks.insert(key.clone(), resolved);
    Ok(())
}
