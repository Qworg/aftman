//! Payloads for the ensemble's events (`docs/06-ensemble.md` §8).
//!
//! The kinds already exist in [`crate::event::EventKind`]; these are their shapes, so that
//! the runtime that emits them and [`crate::testing::RecordedOpinionSource`] that replays
//! from them agree. Emitting them is the runtime's job.
//!
//! An evidence refresh has no kind of its own: it is visible as the provider's
//! `EvidenceCollected` and as the check's entry in `EnsembleResolved`, whose resolution is
//! `evidence_refreshed { old_p, new_p, provider, exit_code, supersedes }` and whose
//! `refresh` field records every provider that ran.
//!
//! Payload types are not `deny_unknown_fields`: the envelope adds `schema_version`, and a
//! reader must tolerate additive change.

use std::collections::BTreeSet;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::assessment::{CheckSuppression, Resolution, Rung, Verdict};
use crate::ids::{AssessmentSeq, CheckKey};
use crate::model::Usage;

use super::gate::IrreversibleGuard;
use super::resolve::{
    CheckRecord, EnsembleLimit, EnsembleOutcome, KeyedOpinion, OpinionKey, ReframeAnswer,
    ReframeRequest, RefreshRecord,
};
use super::trigger::{CheckPlan, DecisiveDirectives, EnsembleTrigger};

/// `ENSEMBLE_TRIGGERED`: one per triggered check, before its ladder runs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnsembleTriggeredPayload {
    pub check: CheckKey,
    pub triggers: Vec<EnsembleTrigger>,
    /// The triggers as one comma-separated string, for surfaces that show a line.
    pub reason: String,
    /// Jev's value, unmodified.
    pub jev_p: Option<f32>,
    pub threshold: Option<f32>,
    pub margin: f32,
    /// The selected directive with this check decisively above and decisively below its
    /// threshold. Names only.
    pub decisive_directives: DecisiveDirectives,
}

impl CheckPlan {
    /// The `ENSEMBLE_TRIGGERED` payload for this check.
    #[must_use]
    pub fn triggered_payload(&self) -> EnsembleTriggeredPayload {
        let triggers: Vec<EnsembleTrigger> = self.triggers.iter().copied().collect();
        EnsembleTriggeredPayload {
            check: self.check.clone(),
            reason: triggers
                .iter()
                .map(|trigger| trigger.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            triggers,
            jev_p: self.baseline.jev_p,
            threshold: self.baseline.threshold,
            margin: self.baseline.margin,
            decisive_directives: self.decisive_directives.clone(),
        }
    }
}

/// `OPINION_REQUESTED`: emitted by the source, before it makes the call, because only the
/// source knows the prompt it is about to send.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpinionRequestedPayload {
    pub opinion_id: OpinionKey,
    pub rung: Rung,
    pub source: String,
    pub model: String,
    pub prompt_hash: String,
    pub state_digest: String,
    pub timeout_seconds: u64,
}

impl OpinionRequestedPayload {
    #[must_use]
    pub fn new(
        request: &ReframeRequest,
        source: &str,
        model: &str,
        prompt_hash: &str,
        state_digest: &str,
    ) -> Self {
        use super::resolve::OPINION_LABEL_CAP_BYTES as CAP;
        Self {
            opinion_id: request.opinion.clone(),
            rung: request.opinion.rung,
            source: super::resolve::cap_bytes(source, CAP),
            model: super::resolve::cap_bytes(model, CAP),
            prompt_hash: super::resolve::cap_bytes(prompt_hash, CAP),
            state_digest: super::resolve::cap_bytes(state_digest, CAP),
            timeout_seconds: request.timeout.as_secs(),
        }
    }
}

/// `OPINION_RECORDED`: one per opinion, after the resolver computed its verdict.
///
/// Carries the raw answer as well as the verdict, so a replay can answer from it and
/// recompute the same verdict. `p_reported` is stored for audit; policy never reads it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpinionRecordedPayload {
    pub opinion_id: OpinionKey,
    pub rung: Rung,
    pub verdict: Verdict,
    pub source: String,
    pub model: String,
    pub p_reported: Option<f32>,
    /// At most 2 KiB.
    pub excerpt: String,
    pub prompt_hash: String,
    pub state_digest: String,
    pub latency: Duration,
    #[serde(default)]
    pub usage: Usage,
    pub error: Option<String>,
}

impl From<&KeyedOpinion> for OpinionRecordedPayload {
    fn from(keyed: &KeyedOpinion) -> Self {
        let opinion = &keyed.opinion;
        Self {
            opinion_id: keyed.key.clone(),
            rung: opinion.rung,
            verdict: opinion.verdict,
            source: opinion.source.clone(),
            model: opinion.model.clone(),
            p_reported: opinion.p_reported,
            excerpt: opinion.excerpt.clone(),
            prompt_hash: opinion.prompt_hash.clone(),
            state_digest: opinion.state_digest.clone(),
            latency: opinion.latency,
            usage: opinion.usage,
            error: opinion.error.clone(),
        }
    }
}

impl OpinionRecordedPayload {
    /// The answer as the source returned it, for a replay to give back.
    #[must_use]
    pub fn answer(&self) -> ReframeAnswer {
        ReframeAnswer {
            p_negated: self.p_reported,
            source: self.source.clone(),
            model: self.model.clone(),
            excerpt: self.excerpt.clone(),
            prompt_hash: self.prompt_hash.clone(),
            state_digest: self.state_digest.clone(),
            latency: self.latency,
            usage: self.usage,
            error: self.error.clone(),
        }
    }
}

impl CheckRecord {
    /// One `OPINION_RECORDED` payload per opinion, in the order they were asked.
    #[must_use]
    pub fn opinion_recorded_payloads(&self) -> Vec<OpinionRecordedPayload> {
        self.opinions
            .iter()
            .map(OpinionRecordedPayload::from)
            .collect()
    }
}

/// `ENSEMBLE_RESOLVED`: one per cycle.
///
/// The first four fields are the ones the S0 runtime already emitted and the surfaces
/// already read; `cycle` and `checks` are additive and default when absent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnsembleResolvedPayload {
    pub enabled: bool,
    pub margin: f32,
    /// Checks policy may not use this cycle.
    pub unresolved: Vec<CheckKey>,
    /// Every rung used this cycle, deduplicated, in ladder order.
    pub rungs: Vec<Rung>,
    #[serde(default)]
    pub cycle: Option<AssessmentSeq>,
    /// One entry per triggered check.
    #[serde(default)]
    pub checks: Vec<CheckResolvedPayload>,
}

/// One triggered check's resolution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CheckResolvedPayload {
    pub check: CheckKey,
    pub triggers: Vec<EnsembleTrigger>,
    /// Jev's value as first assessed, unmodified. A refreshed value is in `resolution`.
    pub jev_p: Option<f32>,
    pub decisive: bool,
    pub contested: bool,
    pub permitted: bool,
    pub suppression: Option<CheckSuppression>,
    pub resolution: Resolution,
    pub rungs_used: Vec<Rung>,
    #[serde(default)]
    pub limits: Vec<EnsembleLimit>,
    #[serde(default)]
    pub refresh: Option<RefreshRecord>,
    #[serde(default)]
    pub escalation: Option<String>,
    /// Set when the final irreversible gate guarded this check.
    #[serde(default)]
    pub irreversible_guard: Option<IrreversibleGuard>,
}

impl From<&CheckRecord> for CheckResolvedPayload {
    fn from(record: &CheckRecord) -> Self {
        Self {
            check: record.check.clone(),
            triggers: record.triggers.iter().copied().collect(),
            jev_p: record.baseline.jev_p,
            decisive: record.resolved.decisive,
            contested: record.resolved.contested,
            permitted: record.resolved.permitted,
            suppression: record.resolved.suppression,
            resolution: record.resolved.resolution.clone(),
            rungs_used: record.rungs_used.clone(),
            limits: record.limits.clone(),
            refresh: record.refresh.clone(),
            escalation: record.escalation.clone(),
            irreversible_guard: record.irreversible_guard.clone(),
        }
    }
}

impl EnsembleOutcome {
    /// The `ENSEMBLE_RESOLVED` payload for this cycle.
    #[must_use]
    pub fn resolved_payload(&self, enabled: bool, margin: f32) -> EnsembleResolvedPayload {
        let rungs: BTreeSet<Rung> = self
            .records
            .iter()
            .flat_map(|record| record.rungs_used.iter().copied())
            .collect();
        EnsembleResolvedPayload {
            enabled,
            margin,
            unresolved: self
                .checks
                .iter()
                .filter(|(_, rc)| rc.is_unresolved())
                .map(|(key, _)| key.clone())
                .collect(),
            rungs: rungs.into_iter().collect(),
            cycle: Some(self.cycle),
            checks: self
                .records
                .iter()
                .map(CheckResolvedPayload::from)
                .collect(),
        }
    }
}
