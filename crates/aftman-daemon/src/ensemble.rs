//! One cycle's ensemble inputs, materialised once (`docs/06-ensemble.md` §6).
//!
//! The ensemble plan fingerprints the policy input it was built over — the clock, the
//! counters, the observation — and `resolve` refuses a context that differs. So the runtime
//! builds every value policy will read **once**, into an [`EnsembleCycle`], and derives the
//! plan's context, the resolution's context and the final arbitration's input from that one
//! value. Nothing is re-read from the run state in between.
//!
//! The same value is what [`crate::runtime::FactoryRuntime::with_ensemble_tap`] hands an
//! observer, which is how a test replays a real run's cycles through
//! `RecordedOpinionSource` and checks that the log reproduces every resolution.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use time::OffsetDateTime;

use aftman_core::assessment::{AnswerValue, CheckSuppression, Resolution, ResolvedCheck};
use aftman_core::directive::Directive;
use aftman_core::ensemble::{
    EnsembleContext, EnsembleCounters, EnsembleError, EnsembleOutcome, EnsemblePlan,
    EnsembleSettings, OpinionSource,
};
use aftman_core::evidence::EvidencePlan;
use aftman_core::ids::{AssessmentSeq, CheckKey};
use aftman_core::job::{FactoryStatus, RunConfig};
use aftman_core::observation::{Counters, Observation};
use aftman_core::policy::PolicyInput;
use aftman_core::responsibility::{Check, ResponsibilityRegistry};
use aftman_core::worker::WorkerRecord;

/// Something that watches each cycle's materialised ensemble inputs.
///
/// An **audit API**: it observes and never influences the run. It is called synchronously
/// on the run loop, once per resolved cycle, so it **must not block** — copy what it needs
/// and return; anything slow belongs on another task. What it is handed includes
/// repository text (the observation: the diff, `AGENTS.md`, untracked files, worker
/// output), so it must not log or forward it anywhere the run log itself would not go
/// (`docs/02-lld.md` §3.3). Used by the replay tests.
pub type EnsembleTap = Arc<dyn Fn(&EnsembleCycle) + Send + Sync>;

/// Everything one cycle's ensemble and policy read, owned.
#[derive(Debug, Clone)]
pub struct EnsembleCycle {
    /// The assessment this cycle resolves: `assessment_seq` of its `ASSESSMENT_COMPLETED`.
    pub cycle: AssessmentSeq,
    pub settings: EnsembleSettings,
    pub status: FactoryStatus,
    pub counters: Counters,
    pub config: RunConfig,
    pub observation: Observation,
    pub now: OffsetDateTime,
    pub active_worker: Option<WorkerRecord>,
    pub last_directive: Option<Directive>,
    pub steering_grace: Duration,
    /// The active checks, with their evidence selections.
    pub definitions: Vec<Check>,
    pub evidence: EvidencePlan,
    /// This cycle's raw answers.
    pub answers: BTreeMap<CheckKey, AnswerValue>,
    /// The ensemble's per-run counters as they stood before this cycle.
    pub ensemble: EnsembleCounters,
    /// Checks R2 can be asked about: `reframable_checks` over `definitions` — every check
    /// with a written negation and a threshold, when `reframe_same_model` is a configured
    /// rung (`docs/06-ensemble.md` §4 R2). Built once per cycle.
    pub reframe_available: BTreeSet<CheckKey>,
    /// A `Finish` selected this cycle will not be executed: `before_finish` providers
    /// exist and have not run, so the runtime runs them and re-assesses instead.
    pub finish_is_provisional: bool,
    /// The baseline: Jev's answers resolved with the settings' margin and `enabled`.
    pub checks: BTreeMap<CheckKey, ResolvedCheck>,
}

impl EnsembleCycle {
    /// The policy input over `checks`, with everything else from this cycle.
    #[must_use]
    pub fn policy_input<'a>(
        &'a self,
        checks: &'a BTreeMap<CheckKey, ResolvedCheck>,
    ) -> PolicyInput<'a> {
        PolicyInput {
            status: self.status,
            counters: &self.counters,
            config: &self.config,
            checks,
            observation: &self.observation,
            now: self.now,
            active_worker: self.active_worker.as_ref(),
            last_directive: self.last_directive.as_ref(),
            ensemble_enabled: self.settings.enabled,
            steering_grace: self.steering_grace,
        }
    }

    /// The ensemble context over `policy`, which must be `self.policy_input(&self.checks)`.
    #[must_use]
    pub fn context<'a>(
        &'a self,
        policy: &'a PolicyInput<'a>,
        registry: &'a ResponsibilityRegistry,
    ) -> EnsembleContext<'a> {
        EnsembleContext {
            settings: &self.settings,
            cycle: self.cycle,
            policy,
            registry,
            definitions: &self.definitions,
            evidence: &self.evidence,
            answers: &self.answers,
            counters: &self.ensemble,
            reframe_available: &self.reframe_available,
            finish_is_provisional: self.finish_is_provisional,
        }
    }

    /// Plan and resolve this cycle against `source`, exactly as the runtime did. For a
    /// replay: `registry` must be the run's (the bundled definitions with what routing
    /// activated).
    ///
    /// # Errors
    /// As [`EnsemblePlan::build`] and [`EnsemblePlan::resolve`].
    pub async fn replay(
        &self,
        registry: &ResponsibilityRegistry,
        source: &dyn OpinionSource,
    ) -> Result<EnsembleOutcome, EnsembleError> {
        let input = self.policy_input(&self.checks);
        let ctx = self.context(&input, registry);
        let plan = EnsemblePlan::build(&ctx)?;
        plan.resolve(&ctx, source).await
    }

    /// The check set a cycle falls back to when the ensemble could not resolve it: every
    /// thresholded check held, so no rule can act on any of them and `Finish` is blocked.
    #[must_use]
    pub fn held(&self) -> BTreeMap<CheckKey, ResolvedCheck> {
        self.checks
            .iter()
            .map(|(key, rc)| {
                let rc = if rc.threshold.is_some() {
                    rc.clone().suppress(
                        CheckSuppression::Unresolved,
                        Resolution::UnresolvedInBand {
                            ask: String::new(),
                            rungs: Vec::new(),
                        },
                    )
                } else {
                    rc.clone()
                };
                (key.clone(), rc)
            })
            .collect()
    }
}
