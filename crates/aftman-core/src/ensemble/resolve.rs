//! The ladder: R0 (evidence refresh), R2 (same-model reframe), and escalation.
//! See `docs/06-ensemble.md` §4 and §6.
//!
//! The only I/O is behind [`OpinionSource`]. Everything else here is a deterministic
//! function of the [`EnsemblePlan`] and what the source answered, which is what lets a
//! recorded source replay a run exactly.
//!
//! Three properties are structural rather than checked:
//!
//! - `permitted = true` is only ever produced by `ResolvedCheck::from_measurement` (the
//!   baseline, built before this module runs) or `ResolvedCheck::from_refresh` (R0).
//!   Every other step here goes through `ResolvedCheck::suppress`, which has no inverse.
//! - No request carries Jev's value. A source cannot show the model its own earlier
//!   answer, because it is never given it.
//! - No arithmetic is done on a probability. The one subtraction in the module is the
//!   R2 consistency rule, which compares two answers and produces a verdict; its result
//!   is never stored or used as a probability.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::assessment::{
    CheckSuppression, Opinion, RefreshMeasurement, Resolution, ResolvedCheck, Rung, Verdict,
    side_of,
};
use crate::ids::{AssessmentSeq, CheckKey, EvidenceId};
use crate::model::Usage;

use super::gate::IrreversibleGuard;
use super::settings::{EnsembleCounters, EnsembleSettings, MarkVerdict, OpinionMark};
use super::trigger::{
    CheckPlan, EnsembleContext, EnsembleError, EnsemblePlan, EnsembleTrigger, derive_baseline,
};

/// Cap on an opinion's stored excerpt, and on its error text.
pub const OPINION_EXCERPT_CAP_BYTES: usize = 2048;
/// Cap on the identifying strings an opinion carries (`source`, `model`, hashes).
pub const OPINION_LABEL_CAP_BYTES: usize = 256;
/// Cap on a refresh's error text, and on an escalation's question.
pub const REFRESH_ERROR_CAP_BYTES: usize = 2048;

/// Identifies one opinion: the cycle, the check, and the rung.
///
/// Deterministic on purpose, so a replay asks for the same key the live run recorded.
/// At most one opinion per rung per check per cycle is ever requested.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct OpinionKey {
    pub cycle: AssessmentSeq,
    pub check: CheckKey,
    pub rung: Rung,
}

/// R0: run these providers, rebuild the observation, and ask Jev the same question.
///
/// Carries no probability: the refreshed answer must come from the fresh evidence alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshRequest {
    pub cycle: AssessmentSeq,
    pub check: CheckKey,
    /// In configuration order. Never empty.
    pub providers: Vec<EvidenceId>,
}

/// One provider run during a refresh.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderRun {
    pub provider: EvidenceId,
    /// `None` if the command was killed by a signal or could not be launched.
    pub exit_code: Option<i32>,
}

/// What a refresh produced.
///
/// For the source's implementer: report every provider attempted in `runs`, with
/// `exit_code: None` for one that could not be launched or was killed. Return `new_p:
/// None` when no provider exited — re-asking the model about evidence that was never
/// produced is not a refresh. The resolver enforces this anyway: a refresh counts as a
/// measurement only if at least one run has an exit code **and** `new_p` is a finite
/// probability in `0..=1`. Only runs of providers the request named count; any other run
/// is dropped. Anything else is recorded as a rejected attempt, still counts
/// against the cap, and leaves the check held.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RefreshOutcome {
    /// The providers attempted, in the order they ran.
    pub runs: Vec<ProviderRun>,
    /// Jev's answer for this check on the fresh evidence. `None` if nothing ran, the call
    /// failed, or the answer was not a probability; a non-finite or out-of-range value is
    /// treated the same way.
    pub new_p: Option<f32>,
    pub error: Option<String>,
}

/// R2: ask the same model the negated question about the same state.
///
/// Carries no probability, so the model cannot be shown its own first answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReframeRequest {
    pub opinion: OpinionKey,
    /// `opinion_timeout_seconds`. The source enforces it.
    pub timeout: Duration,
}

/// What an R2 call returned, before any verdict is computed.
///
/// This is also the recorded shape a replay answers from.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReframeAnswer {
    /// `p(not X)`. `None` if the call failed or did not produce a probability.
    pub p_negated: Option<f32>,
    pub source: String,
    pub model: String,
    pub excerpt: String,
    pub prompt_hash: String,
    pub state_digest: String,
    pub latency: Duration,
    #[serde(default)]
    pub usage: Usage,
    pub error: Option<String>,
}

impl From<&Opinion> for ReframeAnswer {
    /// The answer a recorded opinion was built from, for a replay to give back.
    ///
    /// Exact, because the opinion stores the answer's fields already capped and with any
    /// non-finite value dropped, and building an opinion from this answer does nothing
    /// further to them.
    fn from(opinion: &Opinion) -> Self {
        Self {
            p_negated: opinion.p_reported,
            source: opinion.source.clone(),
            model: opinion.model.clone(),
            excerpt: opinion.excerpt.clone(),
            prompt_hash: opinion.prompt_hash.clone(),
            state_digest: opinion.state_digest.clone(),
            latency: opinion.latency,
            usage: opinion.usage,
            error: opinion.error.clone(),
        }
    }
}

/// Where the ladder's I/O goes. The runtime implements it against the evidence
/// providers and the decision model; [`crate::testing::RecordedOpinionSource`]
/// implements it from a recording.
///
/// A failed call is an **outcome**, not an error: `new_p: None`, `p_negated: None`, and
/// the resolver decides what it means, so that a replay of a failure resolves the same way
/// the failure did. `Err` is reserved for a source that cannot answer *at all* — a replay
/// asked something its log does not record — and it aborts the resolution rather than
/// being folded into it as a value.
///
/// R1 (`independent_judge`) has no method until ensemble stage S3.
#[async_trait]
pub trait OpinionSource: Send + Sync {
    /// R0. Run the providers, rebuild the observation, re-ask Jev.
    async fn refresh_evidence(
        &self,
        request: &RefreshRequest,
    ) -> Result<RefreshOutcome, SourceError>;

    /// R2. Ask Jev the check's negated question about the same state.
    async fn reframe_same_model(
        &self,
        request: &ReframeRequest,
    ) -> Result<ReframeAnswer, SourceError>;
}

/// A source that cannot answer at all. Never a model or command failure: those are
/// outcomes.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SourceError {
    /// A replay was asked something its log does not record. The replay has diverged
    /// from the run it is replaying.
    #[error("replay diverged: no recorded outcome for {0}")]
    NotRecorded(String),
}

/// A bound that stopped the ladder, recorded so that it is visible in the log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "limit")]
pub enum EnsembleLimit {
    /// `max_evidence_refreshes_per_check` is spent for this check.
    RefreshCap { used: u32, cap: u32 },
    /// `max_opinions_per_run` would be exceeded. The check is held.
    OpinionBudget { used: u32, cap: u32 },
    /// An opinion was bought for this check within `per_check_cooldown_cycles`.
    /// `held` says whether the check was held for it (the last opinion contested).
    Cooldown {
        last_opinion: AssessmentSeq,
        cooldown_cycles: u64,
        held: bool,
    },
}

/// One R0 attempt, as recorded.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RefreshRecord {
    pub providers: Vec<EvidenceId>,
    /// The value it set out to replace. For the log only.
    pub old_p: Option<f32>,
    pub outcome: RefreshOutcome,
    /// Why the attempt produced no measurement, when it did not. `None` means the fresh
    /// value was used.
    #[serde(default)]
    pub rejected: Option<RefreshRejection>,
}

/// Why an evidence refresh did not count as a measurement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefreshRejection {
    /// No provider actually ran: none was launched, or every one was killed before it
    /// exited. A model answer with no command behind it is not fresh evidence.
    NoProviderRan,
    /// A provider ran, but there was no fresh value: the model call failed, or what it
    /// returned was not a probability in `0..=1`.
    NoMeasurement,
}

/// One opinion, with its key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeyedOpinion {
    pub key: OpinionKey,
    pub opinion: Opinion,
}

/// What the ladder did for one triggered check.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CheckRecord {
    pub check: CheckKey,
    pub triggers: BTreeSet<EnsembleTrigger>,
    /// Jev's own value, before the ladder.
    pub baseline: ResolvedCheck,
    /// What policy will read.
    pub resolved: ResolvedCheck,
    /// In the order they were used.
    pub rungs_used: Vec<Rung>,
    pub refresh: Option<RefreshRecord>,
    pub opinions: Vec<KeyedOpinion>,
    pub limits: Vec<EnsembleLimit>,
    /// Set when the check was escalated to a person: the question to ask.
    pub escalation: Option<String>,
    /// Set when the final irreversible gate guarded this check: which directive rested on
    /// it, and what the gate did. A check the gate guarded but the ladder never looked at
    /// has a record with no triggers.
    #[serde(default)]
    pub irreversible_guard: Option<IrreversibleGuard>,
}

/// The ensemble's result for one cycle.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnsembleOutcome {
    pub cycle: AssessmentSeq,
    /// Every check, as policy should read it: the baseline, with each triggered check
    /// replaced by its resolution.
    pub checks: BTreeMap<CheckKey, ResolvedCheck>,
    /// One per triggered check, in key order.
    pub records: Vec<CheckRecord>,
    /// The counters to store for the next cycle.
    pub counters: EnsembleCounters,
    /// How many rounds the irreversible gate took; at most the number of checks plus one.
    #[serde(default)]
    pub gate_rounds: u32,
}

impl EnsembleOutcome {
    /// The checks the ladder escalated, with the question for each.
    pub fn escalations(&self) -> impl Iterator<Item = (&CheckKey, &str)> {
        self.records
            .iter()
            .filter_map(|record| record.escalation.as_deref().map(|ask| (&record.check, ask)))
    }
}

impl EnsemblePlan {
    /// Walk the ladder for every triggered check (`docs/06-ensemble.md` §6).
    ///
    /// Deterministic given the plan and the source's answers: checks are resolved in key
    /// order, and every decision is a function of the plan, the counters, and what the
    /// source returned.
    ///
    /// After the ladder, the irreversible gate (`gate.rs`) checks the selection the
    /// cycle actually arrives at, so `ctx` must be the context the plan was built from.
    ///
    /// # Errors
    ///
    /// [`EnsembleError::Source`] if the source cannot answer at all; the resolution is
    /// then abandoned rather than completed with an invented answer.
    /// [`EnsembleError::ContextMismatch`] if `ctx` is not the plan's.
    pub async fn resolve(
        &self,
        ctx: &EnsembleContext<'_>,
        source: &dyn OpinionSource,
    ) -> Result<EnsembleOutcome, EnsembleError> {
        if ctx.cycle != self.cycle {
            return Err(EnsembleError::ContextMismatch(format!(
                "context cycle {}, plan cycle {}",
                ctx.cycle, self.cycle
            )));
        }
        if *ctx.settings != self.settings || derive_baseline(ctx) != self.baseline {
            return Err(EnsembleError::ContextMismatch(
                "the context derives a different baseline or carries different settings".to_owned(),
            ));
        }
        let mut counters = self.counters.clone();
        let mut checks = self.baseline.clone();
        let mut records = Vec::with_capacity(self.triggered.len());
        for plan in &self.triggered {
            let record =
                resolve_check(plan, self.cycle, &self.settings, &mut counters, source).await?;
            checks.insert(plan.check.clone(), record.resolved.clone());
            records.push(record);
        }
        let gate_rounds = if self.settings.enabled {
            super::gate::run(self, ctx, source, &mut checks, &mut records, &mut counters).await?
        } else {
            0
        };
        if self.settings.enabled {
            for (key, rc) in &checks {
                if rc.threshold.is_none() {
                    continue;
                }
                let contested_now = records.iter().any(|record: &CheckRecord| {
                    record.check == *key
                        && record
                            .opinions
                            .iter()
                            .any(|o| matches!(o.opinion.verdict, Verdict::Contest { .. }))
                });
                let entry = counters.checks.entry(key.clone()).or_default();
                entry.consecutive_in_band = if rc.decisive {
                    0
                } else {
                    entry.consecutive_in_band.saturating_add(1)
                };
                entry.consecutive_contests = if contested_now {
                    entry.consecutive_contests.saturating_add(1)
                } else if rc.permitted || !rc.decisive {
                    0
                } else {
                    entry.consecutive_contests
                };
            }
        }
        Ok(EnsembleOutcome {
            cycle: self.cycle,
            checks,
            records,
            counters,
            gate_rounds,
        })
    }
}

async fn resolve_check(
    plan: &CheckPlan,
    cycle: AssessmentSeq,
    settings: &EnsembleSettings,
    counters: &mut EnsembleCounters,
    source: &dyn OpinionSource,
) -> Result<CheckRecord, SourceError> {
    let mut record = CheckRecord {
        check: plan.check.clone(),
        triggers: plan.triggers.clone(),
        baseline: plan.baseline.clone(),
        resolved: plan.baseline.clone(),
        rungs_used: Vec::new(),
        refresh: None,
        opinions: Vec::new(),
        limits: Vec::new(),
        escalation: None,
        irreversible_guard: None,
    };
    let rc = plan.baseline.clone();

    // ---- In band: no opinion can change `permitted` here (§5.2), so none is asked.
    // Evidence, then escalation, are the only options.
    if !rc.decisive {
        if let Some(refreshed) =
            refresh(plan, cycle, settings, counters, source, &mut record).await?
        {
            record.resolved = refreshed;
            return Ok(record);
        }
        if plan.has(EnsembleTrigger::RepeatContest)
            && plan.has(EnsembleTrigger::HighStakes)
            && settings.has(Rung::Human)
        {
            escalate(plan, settings, counters, &mut record);
            return Ok(record);
        }
        record.resolved = rc.suppress(
            CheckSuppression::InBand,
            Resolution::UnresolvedInBand {
                ask: ask_for(plan, None),
                rungs: Vec::new(),
            },
        );
        return Ok(record);
    }

    // ---- Decisive. Nothing to guard unless a rule is acting on it.
    if !plan.rule_would_fire {
        return Ok(record);
    }

    // Measuring beats second-guessing: a decisive value about to act on a bare claim is
    // re-measured first.
    if plan.has(EnsembleTrigger::ClaimOnlyEvidence)
        && let Some(refreshed) =
            refresh(plan, cycle, settings, counters, source, &mut record).await?
    {
        record.resolved = refreshed;
        return Ok(record);
    }

    if !plan.wants_opinion() {
        return Ok(record);
    }

    let rungs: Vec<Rung> = [Rung::ReframeSameModel]
        .into_iter()
        .filter(|rung| settings.has(*rung))
        .filter(|rung| *rung != Rung::ReframeSameModel || plan.reframe_available)
        .collect();
    // No opinion to buy: a decisive, uncontested value stands, whatever its history.
    if rungs.is_empty() {
        return Ok(record);
    }

    // A value that keeps being contested is a question for a person, not another opinion.
    if plan.has(EnsembleTrigger::RepeatContest) && settings.has(Rung::Human) {
        escalate(plan, settings, counters, &mut record);
        return Ok(record);
    }

    let irreversible = plan.has(EnsembleTrigger::Irreversible);
    let state = counters.check(&plan.check);
    // One opinion per check per cooldown window — except that an opinion an
    // `Irreversible` trigger needs is never skipped or held by the cooldown: it is always
    // bought fresh, on this cycle's value, subject only to the opinion budget below.
    if let Some(last) = state.last_opinion
        && !irreversible
        && cycle.get().saturating_sub(last.cycle.get()) < settings.per_check_cooldown_cycles
    {
        // The earlier opinion stands in for this one only if it covers it: a confirm,
        // about the side Jev is on now. Otherwise the check is held for the window.
        let covered = last.covers(rc.side, irreversible);
        record.limits.push(EnsembleLimit::Cooldown {
            last_opinion: last.cycle,
            cooldown_cycles: settings.per_check_cooldown_cycles,
            held: !covered,
        });
        if !covered {
            record.resolved = rc.suppress(
                CheckSuppression::Unresolved,
                Resolution::UnresolvedInBand {
                    ask: ask_for(plan, None),
                    rungs: Vec::new(),
                },
            );
        }
        return Ok(record);
    }

    let needed = u32::try_from(rungs.len()).unwrap_or(u32::MAX);
    if counters.opinions_used.saturating_add(needed) > settings.max_opinions_per_run {
        // Unable to check a decisive value a rule is about to act on: hold it.
        record.limits.push(EnsembleLimit::OpinionBudget {
            used: counters.opinions_used,
            cap: settings.max_opinions_per_run,
        });
        record.resolved = rc.suppress(
            CheckSuppression::Unresolved,
            Resolution::UnresolvedInBand {
                ask: ask_for(plan, None),
                rungs: Vec::new(),
            },
        );
        return Ok(record);
    }

    // R1 and R2 both run when both are enabled; neither short-circuits the other (§4).
    // Only R2 exists before S3.
    let (Some(p), Some(threshold)) = (rc.jev_p, rc.threshold) else {
        return Ok(record);
    };
    for rung in rungs {
        let key = OpinionKey {
            cycle,
            check: plan.check.clone(),
            rung,
        };
        counters.opinions_used = counters.opinions_used.saturating_add(1);
        record.rungs_used.push(rung);
        let answer = source
            .reframe_same_model(&ReframeRequest {
                opinion: key.clone(),
                timeout: Duration::from_secs(settings.opinion_timeout_seconds),
            })
            .await?;
        let verdict = reframe_verdict(
            p,
            answer.p_negated,
            threshold,
            settings.framing_tolerance,
            irreversible,
        );
        record.opinions.push(KeyedOpinion {
            key,
            opinion: opinion_from(rung, verdict, answer),
        });
    }

    let opinions: Vec<Opinion> = record
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
    let side = rc.side;
    let resolved = apply_opinions(rc, &opinions);
    counters
        .checks
        .entry(plan.check.clone())
        .or_default()
        .last_opinion = Some(OpinionMark {
        cycle,
        verdict,
        side,
        irreversible,
    });
    record.resolved = resolved;
    Ok(record)
}

/// R0, if it applies and its cap allows. `Some` only for a fresh, usable measurement.
async fn refresh(
    plan: &CheckPlan,
    cycle: AssessmentSeq,
    settings: &EnsembleSettings,
    counters: &mut EnsembleCounters,
    source: &dyn OpinionSource,
    record: &mut CheckRecord,
) -> Result<Option<ResolvedCheck>, SourceError> {
    if !settings.has(Rung::RefreshEvidence) || plan.refresh_providers.is_empty() {
        return Ok(None);
    }
    let used = counters.check(&plan.check).refreshes;
    if used >= settings.max_evidence_refreshes_per_check {
        record.limits.push(EnsembleLimit::RefreshCap {
            used,
            cap: settings.max_evidence_refreshes_per_check,
        });
        return Ok(None);
    }
    counters
        .checks
        .entry(plan.check.clone())
        .or_default()
        .refreshes = used.saturating_add(1);
    record.rungs_used.push(Rung::RefreshEvidence);

    let outcome = normalize_refresh(
        source
            .refresh_evidence(&RefreshRequest {
                cycle,
                check: plan.check.clone(),
                providers: plan.refresh_providers.clone(),
            })
            .await?,
        &plan.refresh_providers,
    );
    let old_p = plan.baseline.jev_p;
    // A measurement needs both halves: a command that actually ran (it exited, so it
    // produced evidence), and a fresh probability. The provider reported is the first that
    // exited; none is ever invented.
    let ran = outcome
        .runs
        .iter()
        .find(|run| run.exit_code.is_some())
        .cloned();
    let rejected = match (&ran, outcome.new_p) {
        (None, _) => Some(RefreshRejection::NoProviderRan),
        (Some(_), None) => Some(RefreshRejection::NoMeasurement),
        (Some(_), Some(_)) => None,
    };
    record.refresh = Some(RefreshRecord {
        providers: plan.refresh_providers.clone(),
        old_p,
        outcome: outcome.clone(),
        rejected,
    });
    let (Some(run), Some(new_p)) = (ran, outcome.new_p) else {
        return Ok(None);
    };
    let (provider, exit_code) = (run.provider, run.exit_code);
    Ok(Some(ResolvedCheck::from_refresh(
        plan.check.clone(),
        plan.baseline.threshold,
        plan.baseline.margin,
        RefreshMeasurement {
            new_p,
            old_p,
            provider,
            exit_code,
            supersedes: cycle,
        },
    )))
}

/// Hold the check and ask a person. The hold is what blocks `Finish`; the question is
/// for the runtime to raise.
fn escalate(
    plan: &CheckPlan,
    settings: &EnsembleSettings,
    counters: &EnsembleCounters,
    record: &mut CheckRecord,
) {
    let state = counters.check(&plan.check);
    let repeated = if plan.baseline.decisive {
        Repeated::Contested(state.consecutive_contests)
    } else {
        Repeated::InBand(state.consecutive_in_band)
    };
    let ask = ask_for(
        plan,
        Some((repeated, settings.consecutive_contests_before_human)),
    );
    record.rungs_used.push(Rung::Human);
    record.escalation = Some(ask.clone());
    record.resolved = plan.baseline.clone().suppress(
        CheckSuppression::Unresolved,
        Resolution::UnresolvedInBand {
            ask,
            rungs: vec![Rung::Human],
        },
    );
}

/// The question a held check puts to a person. Built only from the check key and
/// numbers, never from model or repository text.
/// Why a check is being escalated, for the question.
#[derive(Clone, Copy)]
enum Repeated {
    InBand(u32),
    Contested(u32),
}

fn ask_for(plan: &CheckPlan, repeated: Option<(Repeated, u32)>) -> String {
    let rc = &plan.baseline;
    let value = rc
        .jev_p
        .map_or_else(|| "no value".to_owned(), |p| format!("{p:.2}"));
    let threshold = rc
        .threshold
        .map_or_else(|| "no threshold".to_owned(), |t| format!("{t:.2}"));
    let mut ask = format!(
        "`{}` is {value} against a threshold of {threshold} (margin {:.2}), and aftman will \
         not act on it.",
        plan.check, rc.margin
    );
    match repeated {
        Some((Repeated::InBand(cycles), limit)) => ask.push_str(&format!(
            " It has been inside the uncertainty band for {cycles} consecutive cycles (limit \
             {limit}). Is the work it describes actually done?"
        )),
        Some((Repeated::Contested(cycles), limit)) => ask.push_str(&format!(
            " A second opinion has contested it for {cycles} consecutive cycles (limit \
             {limit}). Is the work it describes actually done?"
        )),
        None => {}
    }
    cap_bytes(&ask, REFRESH_ERROR_CAP_BYTES)
}

/// Drop values that are not probabilities, and cap free text, so that what is recorded
/// is exactly what a replay will read back.
///
/// Only runs of providers that were requested are kept, at most one per provider: a run
/// of a provider nobody asked for is not evidence about this check, and must not be what
/// makes a refresh count as a measurement. Provider names are capped as well.
fn normalize_refresh(mut outcome: RefreshOutcome, requested: &[EvidenceId]) -> RefreshOutcome {
    let mut seen: Vec<EvidenceId> = Vec::new();
    outcome.runs.retain(|run| {
        let wanted = requested.contains(&run.provider) && !seen.contains(&run.provider);
        if wanted {
            seen.push(run.provider.clone());
        }
        wanted
    });
    for run in &mut outcome.runs {
        run.provider = EvidenceId::new(cap_bytes(run.provider.as_str(), OPINION_LABEL_CAP_BYTES));
    }
    outcome.new_p = outcome.new_p.filter(|p| is_probability(*p));
    outcome.error = outcome
        .error
        .map(|error| cap_bytes(&error, REFRESH_ERROR_CAP_BYTES));
    outcome
}

fn is_probability(p: f32) -> bool {
    p.is_finite() && (0.0..=1.0).contains(&p)
}

/// The R2 verdict. Pure.
///
/// ```text
/// consistent  ⇔  |p(X) − (1 − p(¬X))| ≤ framing_tolerance
/// ```
///
/// Consistent confirms Jev's side; inconsistent contests, carrying the side the reframed
/// answer points to. Nothing is averaged and no value is produced: `1 − p(¬X)` exists
/// only inside this comparison.
///
/// An unusable answer — missing, non-finite, or outside `0..=1` — abstains, except when
/// the trigger is `Irreversible`, where it contests on Jev's side (§4: an opinion that
/// cannot be had on an irreversible action is not a reason to take it).
#[must_use]
pub fn reframe_verdict(
    p: f32,
    p_negated: Option<f32>,
    threshold: f32,
    framing_tolerance: f32,
    irreversible: bool,
) -> Verdict {
    let Some(q) = p_negated.filter(|q| is_probability(*q)) else {
        return if irreversible {
            Verdict::Contest {
                side: side_of(p, threshold),
            }
        } else {
            Verdict::Abstain
        };
    };
    let reframed = 1.0 - q;
    if (p - reframed).abs() <= framing_tolerance {
        Verdict::Confirm {
            side: side_of(p, threshold),
        }
    } else {
        Verdict::Contest {
            side: side_of(reframed, threshold),
        }
    }
}

/// Fold opinions into a check. **Suppress-only.**
///
/// A `Contest` from any opinion removes permission. `Confirm` and `Abstain` change
/// nothing — whatever their `side`. There is no branch here that can set `permitted`.
#[must_use]
pub fn apply_opinions(rc: ResolvedCheck, opinions: &[Opinion]) -> ResolvedCheck {
    let dissent: Vec<Opinion> = opinions
        .iter()
        .filter(|opinion| matches!(opinion.verdict, Verdict::Contest { .. }))
        .cloned()
        .collect();
    if dissent.is_empty() {
        return rc;
    }
    let resolution = match rc.jev_p {
        Some(jev_p) => Resolution::SuppressedByContest { jev_p, dissent },
        None => Resolution::UnresolvedInBand {
            ask: String::new(),
            rungs: Vec::new(),
        },
    };
    rc.suppress(CheckSuppression::Contested, resolution)
}

/// Build the stored opinion from an answer: bounded, and with no non-finite number, so
/// that what is recorded is exactly what a replay reads back.
pub(super) fn opinion_from(rung: Rung, verdict: Verdict, answer: ReframeAnswer) -> Opinion {
    Opinion {
        rung,
        source: cap_bytes(&answer.source, OPINION_LABEL_CAP_BYTES),
        model: cap_bytes(&answer.model, OPINION_LABEL_CAP_BYTES),
        verdict,
        p_reported: answer.p_negated.filter(|p| p.is_finite()),
        excerpt: cap_bytes(&answer.excerpt, OPINION_EXCERPT_CAP_BYTES),
        prompt_hash: cap_bytes(&answer.prompt_hash, OPINION_LABEL_CAP_BYTES),
        state_digest: cap_bytes(&answer.state_digest, OPINION_LABEL_CAP_BYTES),
        usage: answer.usage,
        latency: answer.latency,
        error: answer
            .error
            .map(|error| cap_bytes(&error, OPINION_EXCERPT_CAP_BYTES)),
    }
}

/// At most `cap` bytes, cut on a character boundary. Idempotent.
pub(crate) fn cap_bytes(text: &str, cap: usize) -> String {
    if text.len() <= cap {
        return text.to_owned();
    }
    let mut end = cap;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_owned()
}
