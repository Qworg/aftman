//! The ensemble's tests (`docs/06-ensemble.md` §14). Offline: every answer comes from a
//! [`RecordedOpinionSource`].

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Duration;

use time::macros::datetime;

use super::{
    CheckCounters, CheckPlan, DecisiveDirectives, EnsembleContext, EnsembleCounters, EnsembleLimit,
    EnsembleOutcome, EnsemblePlan, EnsembleSettings, EnsembleTrigger, MarkVerdict, OpinionKey,
    OpinionMark, ProviderRun, ReframeAnswer, RefreshOutcome, apply_opinions, reframe_verdict,
};
use crate::assessment::{
    AnswerValue, CheckSuppression, Opinion, Resolution, ResolvedCheck, Rung, Side, Verdict,
};
use crate::definitions::DefinitionSet;
use crate::directive::{Directive, Proposal};
use crate::event::{EventEnvelope, EventKind};
use crate::evidence::{CommandProvider, EvidencePlan, ProviderCwd, RunWhen};
use crate::ids::{
    AssessmentSeq, CheckKey, EventSeq, EvidenceId, ResponsibilityId, RunId, WorkerId,
};
use crate::job::{FactoryStatus, JobSummary, RunConfig, TaskKind, WorkerKind};
use crate::log::BoundedLog;
use crate::model::Usage;
use crate::observation::{Counters, GitEvidence, Observation};
use crate::policy::PolicyInput;
use crate::responsibility::{Check, ResponsibilityRegistry};
use crate::testing::RecordedOpinionSource;
use crate::worker::WorkerRecord;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const IMPL: &str = "core.completion__implementation_complete";
const REQS: &str = "core.completion__requirements_satisfied";
const READY: &str = "core.completion__ready_to_finish";
const TESTS: &str = "core.verification__tests_sufficient";
const NEEDS_VER: &str = "core.verification__needs_verification";
const STUCK: &str = "core.worker-health__worker_stuck";
const OFF_TRACK: &str = "core.worker-health__work_off_track";
const PROGRESS: &str = "core.worker-health__meaningful_progress";
const HUMAN: &str = "core.human-escalation__needs_human";
const DRIFT: &str = "repository.instructions__agents_md_drift";
const DOCS: &str = "quality.documentation__documentation_sufficient";

const CYCLE: AssessmentSeq = AssessmentSeq(20);

fn k(key: &str) -> CheckKey {
    CheckKey::from(key)
}

// ---- fixture ---------------------------------------------------------------

/// One cycle's inputs: the same things the runtime would hand to the ensemble.
struct Scenario {
    settings: EnsembleSettings,
    counters: Counters,
    config: RunConfig,
    observation: Observation,
    values: BTreeMap<CheckKey, Option<f32>>,
    worker: Option<WorkerRecord>,
    registry: ResponsibilityRegistry,
    definitions: Vec<Check>,
    evidence: EvidencePlan,
    answers: BTreeMap<CheckKey, AnswerValue>,
    ensemble: EnsembleCounters,
    reframe: BTreeSet<CheckKey>,
    cycle: AssessmentSeq,
    /// The runtime will run `before_finish` providers instead of executing a Finish.
    finish_provisional: bool,
}

impl Scenario {
    /// Every check decisive, no worker running, one worker already run: policy selects
    /// `Finish`.
    fn finish_ready() -> Self {
        let definitions = DefinitionSet::bundled().unwrap_or_default();
        let ids: Vec<ResponsibilityId> = definitions
            .definitions
            .keys()
            .map(ResponsibilityId::new)
            .collect();
        let registry = ResponsibilityRegistry::from_definitions(&definitions, &ids, 30, 1)
            .unwrap_or_else(|_| ResponsibilityRegistry::builtin());
        let checks = registry.checks();
        let values = [
            (IMPL, 0.95),
            (REQS, 0.95),
            (READY, 0.95),
            (TESTS, 0.95),
            (NEEDS_VER, 0.10),
            (STUCK, 0.05),
            (OFF_TRACK, 0.05),
            (PROGRESS, 0.50),
            (HUMAN, 0.05),
            (DRIFT, 0.05),
            (DOCS, 0.95),
        ]
        .into_iter()
        .map(|(key, p)| (k(key), Some(p)))
        .collect();
        Self {
            settings: EnsembleSettings {
                enabled: true,
                ..EnsembleSettings::default()
            },
            counters: Counters {
                workers_run: 1,
                ..Counters::default()
            },
            config: RunConfig::default(),
            observation: observation(),
            values,
            worker: None,
            registry,
            definitions: checks,
            evidence: EvidencePlan::new(vec![
                provider("tests", RunWhen::OnDemand),
                provider("audit", RunWhen::BeforeFinish),
                provider("lint", RunWhen::WorkerCompleted),
            ]),
            answers: BTreeMap::new(),
            // The cooldown's clock starts where the cycle ids do, so a hand-built mark
            // `cycle: CYCLE - 3` is three resolutions ago.
            ensemble: EnsembleCounters {
                resolutions: CYCLE.get() - 1,
                ..EnsembleCounters::default()
            },
            reframe: BTreeSet::new(),
            cycle: CYCLE,
            finish_provisional: false,
        }
    }

    fn set(mut self, key: &str, p: f32) -> Self {
        self.values.insert(k(key), Some(p));
        self
    }

    fn with_worker(mut self) -> Self {
        self.worker = Some(WorkerRecord::new(
            WorkerId::new(),
            WorkerKind::Simulated,
            TaskKind::Code,
            "do the work",
            BoundedLog::new(16, 16),
            false,
        ));
        self
    }

    /// Make `key` select command evidence from `providers`.
    fn selecting(mut self, key: &str, providers: &[&str]) -> Self {
        for check in &mut self.definitions {
            if check.key == k(key) {
                let mut evidence = crate::responsibility::providers_for(check);
                evidence.extend(providers.iter().map(|p| format!("command:{p}")));
                check.evidence = evidence.into_iter().map(EvidenceId::new).collect();
            }
        }
        self
    }

    fn reframable(mut self, keys: &[&str]) -> Self {
        self.reframe.extend(keys.iter().map(|key| k(key)));
        self
    }

    fn reframable_all(mut self) -> Self {
        self.reframe.extend(self.values.keys().cloned());
        self
    }

    fn without_rung(mut self, rung: Rung) -> Self {
        self.settings.rungs.retain(|r| *r != rung);
        self
    }

    fn held_for(mut self, key: &str, cycles: u32) -> Self {
        self.ensemble
            .checks
            .entry(k(key))
            .or_default()
            .consecutive_in_band = cycles;
        self
    }

    fn contested_for(mut self, key: &str, cycles: u32) -> Self {
        self.ensemble
            .checks
            .entry(k(key))
            .or_default()
            .consecutive_contests = cycles;
        self
    }

    fn thresholds(&self) -> BTreeMap<CheckKey, Option<f32>> {
        self.registry.thresholds().into_iter().collect()
    }

    /// This cycle's answers: every value as a probability, overridden by any answer a
    /// test set explicitly.
    fn answer_map(&self) -> BTreeMap<CheckKey, AnswerValue> {
        let mut answers: BTreeMap<CheckKey, AnswerValue> = self
            .values
            .iter()
            .filter_map(|(key, p)| p.map(|p| (key.clone(), AnswerValue::Noul(p))))
            .collect();
        answers.extend(self.answers.clone());
        answers
    }

    /// The baseline, exactly as `resolve_checks` would build it from the answers.
    fn baseline(&self) -> BTreeMap<CheckKey, ResolvedCheck> {
        let thresholds = self.thresholds();
        let answers = self.answer_map();
        self.values
            .keys()
            .map(|key| {
                let threshold = thresholds.get(key).copied().flatten();
                (
                    key.clone(),
                    ResolvedCheck::from_measurement(
                        key.clone(),
                        answers.get(key).and_then(AnswerValue::as_noul),
                        threshold,
                        self.settings.uncertainty_margin,
                        self.settings.enabled,
                    ),
                )
            })
            .collect()
    }

    fn input<'a>(&'a self, checks: &'a BTreeMap<CheckKey, ResolvedCheck>) -> PolicyInput<'a> {
        PolicyInput {
            status: FactoryStatus::Assessing,
            counters: &self.counters,
            config: &self.config,
            checks,
            observation: &self.observation,
            now: datetime!(2026-01-01 00:00:00 UTC),
            active_worker: self.worker.as_ref(),
            last_directive: None,
            ensemble_enabled: self.settings.enabled,
            steering_grace: Duration::from_secs(30),
        }
    }

    /// What the scenario's assessment sent Jev: the default selection plus every command
    /// output the observation holds, so that here a check sees the command evidence its
    /// observation has (the runtime's assessment sends none; `docs/03-jev.md` §5).
    fn assessed_state(&self) -> serde_json::Value {
        let mut selection = crate::observation::default_evidence();
        selection.push("command_evidence".to_owned());
        self.observation.state_for(&selection)
    }

    fn proposals(&self, checks: &BTreeMap<CheckKey, ResolvedCheck>) -> Vec<Proposal> {
        self.registry.propose(&self.input(checks))
    }

    fn select(&self, checks: &BTreeMap<CheckKey, ResolvedCheck>) -> Directive {
        let input = self.input(checks);
        crate::policy::decide(&self.registry, &input, &[]).selected
    }

    fn plan(&self) -> EnsemblePlan {
        self.plan_over(&self.baseline())
            .unwrap_or_else(|e| panic!("{e}"))
    }

    /// Plan with the caller's checks set to `checks`.
    fn plan_over(
        &self,
        checks: &BTreeMap<CheckKey, ResolvedCheck>,
    ) -> Result<EnsemblePlan, super::EnsembleError> {
        let input = self.input(checks);
        let answers = self.answer_map();
        let assessed = self.assessed_state();
        EnsemblePlan::build(&EnsembleContext {
            settings: &self.settings,
            cycle: self.cycle,
            policy: &input,
            registry: &self.registry,
            definitions: &self.definitions,
            evidence: &self.evidence,
            answers: &answers,
            assessed_state: &assessed,
            counters: &self.ensemble,
            reframe_available: &self.reframe,
            finish_is_provisional: self.finish_provisional,
        })
    }

    /// Resolve `plan` with this scenario as the context.
    async fn resolve_plan(
        &self,
        plan: &EnsemblePlan,
        source: &RecordedOpinionSource,
    ) -> Result<EnsembleOutcome, super::EnsembleError> {
        let baseline = self.baseline();
        let input = self.input(&baseline);
        let answers = self.answer_map();
        let assessed = self.assessed_state();
        let ctx = EnsembleContext {
            settings: &self.settings,
            cycle: self.cycle,
            policy: &input,
            registry: &self.registry,
            definitions: &self.definitions,
            evidence: &self.evidence,
            answers: &answers,
            assessed_state: &assessed,
            counters: &self.ensemble,
            reframe_available: &self.reframe,
            finish_is_provisional: self.finish_provisional,
        };
        plan.resolve(&ctx, source).await
    }

    /// The digest `build` would store for this scenario's context.
    fn fingerprint(&self) -> String {
        let baseline = self.baseline();
        let input = self.input(&baseline);
        let answers = self.answer_map();
        let assessed = self.assessed_state();
        super::trigger::context_fingerprint(&EnsembleContext {
            settings: &self.settings,
            cycle: self.cycle,
            policy: &input,
            registry: &self.registry,
            definitions: &self.definitions,
            evidence: &self.evidence,
            answers: &answers,
            assessed_state: &assessed,
            counters: &self.ensemble,
            reframe_available: &self.reframe,
            finish_is_provisional: self.finish_provisional,
        })
    }

    /// Plan and resolve this scenario.
    async fn run_plan(
        &self,
        source: &RecordedOpinionSource,
    ) -> Result<EnsembleOutcome, super::EnsembleError> {
        self.resolve_plan(&self.plan(), source).await
    }

    fn triggers(&self, key: &str) -> BTreeSet<EnsembleTrigger> {
        self.plan()
            .triggered
            .into_iter()
            .find(|plan| plan.check == k(key))
            .map(|plan| plan.triggers)
            .unwrap_or_default()
    }

    fn check_plan(&self, key: &str) -> Option<CheckPlan> {
        self.plan()
            .triggered
            .into_iter()
            .find(|plan| plan.check == k(key))
    }
}

fn provider(id: &str, run_when: RunWhen) -> CommandProvider {
    CommandProvider {
        id: EvidenceId::new(id),
        argv: vec![id.to_owned()],
        cwd: ProviderCwd::Repo,
        timeout_seconds: 600,
        max_output_bytes: 20_000,
        run_when,
        success_exit_codes: Vec::new(),
    }
}

fn observation() -> Observation {
    Observation {
        run_id: RunId::new(),
        status: FactoryStatus::Assessing,
        job: JobSummary {
            prompt_excerpt: "job".into(),
            prompt_truncated: false,
            repo: PathBuf::from("/repo"),
            elapsed_secs: 0,
        },
        active: None,
        history: Vec::new(),
        git: GitEvidence::default(),
        untracked: Vec::new(),
        instructions: None,
        tests: Vec::new(),
        events: Vec::new(),
        prior: None,
        counters: Counters::default(),
        evidence: BTreeMap::new(),
        output: None,
        activity: None,
        observation_note: None,
    }
}

fn refreshed(new_p: Option<f32>, provider: &str, exit_code: i32) -> RefreshOutcome {
    RefreshOutcome {
        runs: vec![ProviderRun {
            provider: EvidenceId::new(provider),
            exit_code: Some(exit_code),
        }],
        new_p,
        error: None,
        state_digest: None,
    }
}

/// A refresh whose command could not be launched.
fn failed_refresh() -> RefreshOutcome {
    RefreshOutcome {
        runs: vec![ProviderRun {
            provider: EvidenceId::new("tests"),
            exit_code: None,
        }],
        new_p: None,
        error: Some("could not launch".into()),
        state_digest: None,
    }
}

fn answer(p_negated: Option<f32>) -> ReframeAnswer {
    ReframeAnswer {
        p_negated,
        source: "jev".into(),
        model: "recorded-model".into(),
        excerpt: "{\"p\": ...}".into(),
        prompt_hash: "prompt-hash".into(),
        state_digest: "state-digest".into(),
        latency: Duration::from_millis(250),
        usage: Usage {
            input_tokens: Some(100),
            output_tokens: Some(5),
        },
        error: None,
    }
}

fn reframe_key(cycle: AssessmentSeq, key: &str) -> OpinionKey {
    OpinionKey {
        cycle,
        check: k(key),
        rung: Rung::ReframeSameModel,
    }
}

/// A source that answers every opinion the plan could ask with the same `p(not X)`.
fn answering_all(plan: &EnsemblePlan, p_negated: Option<f32>) -> RecordedOpinionSource {
    answering_by(plan, |_| p_negated)
}

/// A source that answers every opinion the plan could ask with `p(not X) = answer(p(X))`.
/// Every check, not only the triggered ones: the irreversible gate may ask about any.
fn answering_by(plan: &EnsemblePlan, by: impl Fn(f32) -> Option<f32>) -> RecordedOpinionSource {
    plan.baseline()
        .iter()
        .fold(RecordedOpinionSource::new(), |source, (key, rc)| {
            source.with_reframe(
                OpinionKey {
                    cycle: plan.cycle(),
                    check: key.clone(),
                    rung: Rung::ReframeSameModel,
                },
                answer(rc.jev_p.and_then(&by)),
            )
        })
}

fn permitted_set(checks: &BTreeMap<CheckKey, ResolvedCheck>) -> BTreeSet<CheckKey> {
    checks
        .iter()
        .filter(|(_, rc)| rc.permitted)
        .map(|(key, _)| key.clone())
        .collect()
}

fn record<'a>(outcome: &'a EnsembleOutcome, key: &str) -> Option<&'a super::CheckRecord> {
    outcome.records.iter().find(|r| r.check == k(key))
}

// ---- triggers --------------------------------------------------------------

#[tokio::test]
async fn with_the_ensemble_disabled_nothing_triggers_and_nothing_is_asked() {
    let mut s = Scenario::finish_ready().set(READY, 0.79).reframable_all();
    s.settings.enabled = false;
    let plan = s.plan();
    assert!(plan.triggered.is_empty());
    let source = RecordedOpinionSource::new();
    let outcome = s
        .resolve_plan(&plan, &source)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(outcome.checks, s.baseline());
    assert_eq!(outcome.counters, s.ensemble, "the counters are untouched");
    assert!(source.refresh_calls().is_empty() && source.reframe_calls().is_empty());
}

#[test]
fn the_baseline_is_derived_not_taken_from_the_caller() {
    // Round-2 probe N2: `ready_to_finish = 0.76` is in band, but a caller resolved it with
    // the band switched off and handed it in as decisive and permitted.
    let s = Scenario::finish_ready().set(READY, 0.76);
    let mut forged = s.baseline();
    forged.insert(
        k(READY),
        ResolvedCheck::from_measurement(k(READY), Some(0.76), Some(0.75), 0.15, false),
    );
    assert!(forged[&k(READY)].permitted);
    assert_eq!(
        s.plan_over(&forged).err(),
        Some(super::EnsembleError::BaselineMismatch { check: k(READY) })
    );
    // A different margin is refused too.
    let mut other_margin = s.baseline();
    other_margin.insert(
        k(TESTS),
        ResolvedCheck::from_measurement(k(TESTS), Some(0.95), Some(0.75), 0.10, true),
    );
    assert!(s.plan_over(&other_margin).is_err());
    // The derived baseline is what the plan carries.
    let plan = s.plan();
    assert_eq!(plan.baseline(), &s.baseline());
    assert!(!plan.baseline()[&k(READY)].permitted);
}

#[test]
fn nothing_covers_an_irreversible_opinion() {
    for verdict in [
        MarkVerdict::Confirm,
        MarkVerdict::Contest,
        MarkVerdict::Abstain,
    ] {
        for irreversible_mark in [false, true] {
            let mark = OpinionMark {
                cycle: CYCLE,
                resolution: CYCLE.get(),
                verdict,
                side: Some(Side::Above),
                irreversible: irreversible_mark,
            };
            assert!(!mark.covers(Some(Side::Above), true), "{mark:?}");
            assert_eq!(
                mark.covers(Some(Side::Above), false),
                verdict == MarkVerdict::Confirm,
                "{mark:?}"
            );
            assert!(!mark.covers(Some(Side::Below), false), "{mark:?}");
        }
    }
}

#[tokio::test]
async fn resolve_refuses_different_answers_or_settings_under_the_same_policy_input() {
    // The fingerprint covers the policy input, not the answers or the settings; the
    // derived baseline and the settings are compared separately.
    let s = Scenario::finish_ready()
        .set(TESTS, 0.92)
        .reframable(&[TESTS]);
    let plan = s.plan();
    let mut other_answer = Scenario::finish_ready()
        .set(TESTS, 0.71)
        .reframable(&[TESTS]);
    other_answer.observation = s.observation.clone();
    assert_eq!(other_answer.fingerprint(), s.fingerprint());
    let result = other_answer
        .resolve_plan(&plan, &RecordedOpinionSource::new())
        .await;
    assert!(
        matches!(result, Err(super::EnsembleError::ContextMismatch(_))),
        "{result:?}"
    );
    let mut other_settings = Scenario::finish_ready()
        .set(TESTS, 0.92)
        .reframable(&[TESTS]);
    other_settings.observation = s.observation.clone();
    other_settings.settings.framing_tolerance = 0.30;
    assert_eq!(other_settings.fingerprint(), s.fingerprint());
    let result = other_settings
        .resolve_plan(&plan, &RecordedOpinionSource::new())
        .await;
    assert!(
        matches!(result, Err(super::EnsembleError::ContextMismatch(_))),
        "{result:?}"
    );
}

#[tokio::test]
async fn resolve_refuses_a_different_set_of_reframable_checks() {
    // Round-4 item 1. The gate reads which checks R2 can be asked about. Clearing that set
    // between `build` and `resolve` let a refresh-unmasked Finish through with no R2 call.
    let mut s = Scenario::finish_ready()
        .set(READY, 0.79)
        .selecting(READY, &["audit"])
        .set(TESTS, 0.92)
        .reframable(&[TESTS]);
    let plan = s.plan();
    s.reframe.clear();
    let source =
        RecordedOpinionSource::new().with_refresh(CYCLE, READY, refreshed(Some(0.95), "audit", 0));
    let result = s.resolve_plan(&plan, &source).await;
    assert!(
        matches!(result, Err(super::EnsembleError::ContextMismatch(_))),
        "{result:?}"
    );
    assert!(source.refresh_calls().is_empty() && source.reframe_calls().is_empty());
}

#[tokio::test]
async fn resolve_refuses_a_different_observation() {
    // Round-4 item 3 (mutation RM5): the observation is part of the policy input the gate
    // re-arbitrates over. `observation_note` alone blocks Finish (`UnobservedRepository`);
    // repository instructions arm `repository.instructions:drift`.
    let s = Scenario::finish_ready().reframable_all();
    let plan = s.plan();

    let mut unobserved = Scenario::finish_ready().reframable_all();
    unobserved.observation = s.observation.clone();
    unobserved.observation.observation_note = Some("the repository could not be read".into());
    assert_eq!(unobserved.baseline(), s.baseline());
    let result = unobserved
        .resolve_plan(&plan, &RecordedOpinionSource::new())
        .await;
    assert!(
        matches!(result, Err(super::EnsembleError::ContextMismatch(_))),
        "{result:?}"
    );

    let mut instructed = Scenario::finish_ready().reframable_all();
    instructed.observation = s.observation.clone();
    instructed.observation.instructions = Some(crate::observation::RepoInstructions {
        file: "AGENTS.md".into(),
        excerpt: "x".into(),
        truncated: false,
    });
    let result = instructed
        .resolve_plan(&plan, &RecordedOpinionSource::new())
        .await;
    assert!(
        matches!(result, Err(super::EnsembleError::ContextMismatch(_))),
        "{result:?}"
    );
}

#[tokio::test]
async fn resolve_refuses_a_different_policy_input() {
    // Round-3 probe N2b: a plan built with no worker, resolved with one. The baseline is
    // identical, so only the fingerprint can tell.
    let s = Scenario::finish_ready();
    let plan = s.plan();
    let mut with_worker = Scenario::finish_ready().with_worker();
    with_worker.observation = s.observation.clone();
    assert_eq!(with_worker.baseline(), s.baseline());
    let result = with_worker
        .resolve_plan(&plan, &RecordedOpinionSource::new())
        .await;
    assert!(
        matches!(result, Err(super::EnsembleError::ContextMismatch(_))),
        "{result:?}"
    );
    // So is a change of `finish_is_provisional`, or of the policy counters.
    let mut provisional = Scenario::finish_ready();
    provisional.observation = s.observation.clone();
    provisional.finish_provisional = true;
    let result = provisional
        .resolve_plan(&plan, &RecordedOpinionSource::new())
        .await;
    assert!(
        matches!(result, Err(super::EnsembleError::ContextMismatch(_))),
        "{result:?}"
    );
    let mut counted = Scenario::finish_ready();
    counted.observation = s.observation.clone();
    counted.counters.workers_run = 2;
    let result = counted
        .resolve_plan(&plan, &RecordedOpinionSource::new())
        .await;
    assert!(
        matches!(result, Err(super::EnsembleError::ContextMismatch(_))),
        "{result:?}"
    );
    // The same context resolves.
    let mut same = Scenario::finish_ready();
    same.observation = s.observation.clone();
    let plan = same.plan();
    let source = answering_all(&plan, Some(0.5));
    assert!(same.resolve_plan(&plan, &source).await.is_ok());
}

#[tokio::test]
async fn resolve_refuses_a_context_the_plan_was_not_built_from() {
    let s = Scenario::finish_ready()
        .set(TESTS, 0.92)
        .reframable(&[TESTS]);
    let plan = s.plan();
    // Another value: another baseline.
    let other = Scenario::finish_ready()
        .set(TESTS, 0.71)
        .reframable(&[TESTS]);
    let result = other
        .resolve_plan(&plan, &RecordedOpinionSource::new())
        .await;
    assert!(
        matches!(result, Err(super::EnsembleError::ContextMismatch(_))),
        "{result:?}"
    );
    // Another cycle.
    let mut later = Scenario::finish_ready()
        .set(TESTS, 0.92)
        .reframable(&[TESTS]);
    later.cycle = AssessmentSeq(CYCLE.get() + 1);
    let result = later
        .resolve_plan(&plan, &RecordedOpinionSource::new())
        .await;
    assert!(
        matches!(result, Err(super::EnsembleError::ContextMismatch(_))),
        "{result:?}"
    );
}

#[test]
fn a_check_with_no_threshold_never_triggers() {
    let s = Scenario::finish_ready().set(PROGRESS, 0.5);
    assert!(s.check_plan(PROGRESS).is_none());
    // Even with the most suspicious-looking state around it.
    let s = Scenario::finish_ready()
        .set(PROGRESS, 0.0)
        .held_for(PROGRESS, 9);
    assert!(s.check_plan(PROGRESS).is_none());
}

#[test]
fn an_in_band_finish_conjunct_is_near_threshold_relevant_and_high_stakes() {
    let s = Scenario::finish_ready().set(READY, 0.79);
    let plan = s.check_plan(READY);
    let plan = plan.as_ref();
    let triggers = plan.map(|p| p.triggers.clone()).unwrap_or_default();
    assert!(triggers.contains(&EnsembleTrigger::NearThreshold));
    assert!(triggers.contains(&EnsembleTrigger::OutcomeRelevant));
    assert!(triggers.contains(&EnsembleTrigger::HighStakes));
    assert!(triggers.contains(&EnsembleTrigger::ClaimOnlyEvidence));
    assert!(
        !triggers.contains(&EnsembleTrigger::Irreversible),
        "an in-band value is not what an irreversible directive rests on"
    );
    assert_eq!(plan.map(|p| p.rule_would_fire), Some(false));
    assert_eq!(
        plan.map(|p| p.decisive_directives.clone()),
        Some(DecisiveDirectives {
            if_above: "Finish".into(),
            if_below: "StartWorker".into(),
        })
    );
}

#[test]
fn a_decisive_check_no_rule_reads_is_not_relevant() {
    // No worker, so worker-health proposes nothing either way.
    let s = Scenario::finish_ready();
    let triggers = s.triggers(STUCK);
    assert_eq!(
        triggers,
        BTreeSet::from([EnsembleTrigger::ClaimOnlyEvidence]),
        "only the evidence trigger applies"
    );
    assert_eq!(s.check_plan(STUCK).map(|p| p.rule_would_fire), Some(false));
}

#[test]
fn a_decisive_value_a_finish_rests_on_is_irreversible() {
    let s = Scenario::finish_ready().set(TESTS, 0.92);
    assert_eq!(s.select(&s.baseline()).name(), "Finish");
    let plan = s.check_plan(TESTS);
    let triggers = plan
        .as_ref()
        .map(|p| p.triggers.clone())
        .unwrap_or_default();
    for expected in [
        EnsembleTrigger::Irreversible,
        EnsembleTrigger::OutcomeRelevant,
        EnsembleTrigger::HighStakes,
    ] {
        assert!(triggers.contains(&expected), "{expected:?} in {triggers:?}");
    }
    assert!(!triggers.contains(&EnsembleTrigger::NearThreshold));
    assert_eq!(plan.map(|p| p.rule_would_fire), Some(true));
}

#[test]
fn triggers_agree_with_a_brute_force_oracle() {
    // Review finding 6: the decision table takes triggers as input, so trigger computation
    // needs its own oracle. Here every decisive value on each side is tried for real
    // (through `from_measurement`, not the planner's pinned ends), and the selected
    // directive kinds compared.
    for worker in [false, true] {
        for key in [READY, TESTS, NEEDS_VER, DOCS, STUCK, HUMAN, DRIFT] {
            for step in 0..=20 {
                let p = step as f32 / 20.0;
                let mut s = Scenario::finish_ready();
                if worker {
                    s = s.with_worker();
                }
                let s = s.set(key, p);
                let t = s.thresholds()[&k(key)].unwrap_or(0.75);
                let baseline = s.baseline();
                let rc = &baseline[&k(key)];
                let kinds = |above: bool| -> BTreeSet<&'static str> {
                    (0..=100)
                        .map(|v| v as f32 / 100.0)
                        .filter(|v| (*v >= t) == above)
                        .filter_map(|v| {
                            let probe = ResolvedCheck::from_measurement(
                                k(key),
                                Some(v),
                                Some(t),
                                0.15,
                                true,
                            );
                            probe.decisive.then(|| {
                                let mut checks = baseline.clone();
                                checks.insert(k(key), probe);
                                s.select(&checks).name()
                            })
                        })
                        .collect()
                };
                let (above, below) = (kinds(true), kinds(false));
                let triggers = s.triggers(key);
                let label = format!("{key}={p} worker={worker} above={above:?} below={below:?}");

                assert_eq!(
                    triggers.contains(&EnsembleTrigger::NearThreshold),
                    !rc.decisive,
                    "near threshold: {label}"
                );
                if !above.is_empty() && !below.is_empty() {
                    assert_eq!(
                        triggers.contains(&EnsembleTrigger::OutcomeRelevant),
                        above != below,
                        "outcome relevant: {label}"
                    );
                }
                let selected = s.select(&baseline);
                let opposite = if rc.jev_p.is_some_and(|v| v >= t) {
                    &below
                } else {
                    &above
                };
                let irreversible = rc.decisive
                    && selected.is_irreversible()
                    && opposite.iter().any(|kind| *kind != selected.name());
                assert_eq!(
                    triggers.contains(&EnsembleTrigger::Irreversible),
                    irreversible,
                    "irreversible: {label}"
                );
            }
        }
    }
}

#[test]
fn the_counterfactual_does_not_count_the_uncertainty_invariant_as_relevance() {
    // A held check blocks `Finish` through `UnresolvedUncertainty`, whatever it measures.
    // If "forced not to fire" meant "not permitted", every check would look relevant to
    // every `Finish`. `agents_md_drift` decides nothing without a worker.
    let s = Scenario::finish_ready();
    assert!(
        !s.triggers(DRIFT)
            .contains(&EnsembleTrigger::OutcomeRelevant)
    );
    assert!(!s.triggers(DRIFT).contains(&EnsembleTrigger::Irreversible));
}

#[test]
fn low_confidence_reads_the_choice_or_score_confidence() {
    let mut s = Scenario::finish_ready();
    s.answers.insert(
        k(IMPL),
        AnswerValue::Choice {
            choice: "done".into(),
            probabilities: BTreeMap::new(),
            confidence: 0.40,
        },
    );
    assert!(s.triggers(IMPL).contains(&EnsembleTrigger::LowConfidence));
    s.answers.insert(
        k(IMPL),
        AnswerValue::Score {
            score: 2.0,
            level: Some(2),
            confidence: 0.70,
            probabilities: BTreeMap::new(),
        },
    );
    assert!(!s.triggers(IMPL).contains(&EnsembleTrigger::LowConfidence));
    s.answers.insert(
        k(IMPL),
        AnswerValue::Choice {
            choice: "done".into(),
            probabilities: BTreeMap::new(),
            confidence: f32::NAN,
        },
    );
    assert!(
        s.triggers(IMPL).contains(&EnsembleTrigger::LowConfidence),
        "a confidence that is not a number is not a high one"
    );
}

#[test]
fn claim_only_means_the_check_saw_no_command_evidence() {
    // Selected but not run (on demand): what the check scored was the worker's claim.
    let s = Scenario::finish_ready().selecting(TESTS, &["tests"]);
    assert!(
        s.triggers(TESTS)
            .contains(&EnsembleTrigger::ClaimOnlyEvidence)
    );

    let mut s = s;
    s.observation
        .evidence
        .insert("tests".into(), "16 passed, 1 failed".into());
    assert!(
        !s.triggers(TESTS)
            .contains(&EnsembleTrigger::ClaimOnlyEvidence)
    );
    // Another check that does not select it still saw only claims.
    assert!(
        s.triggers(READY)
            .contains(&EnsembleTrigger::ClaimOnlyEvidence)
    );
}

#[test]
fn a_check_held_for_enough_cycles_repeat_contests() {
    let s = Scenario::finish_ready().set(READY, 0.79).held_for(READY, 1);
    assert!(!s.triggers(READY).contains(&EnsembleTrigger::RepeatContest));
    let s = s.held_for(READY, 2);
    assert!(s.triggers(READY).contains(&EnsembleTrigger::RepeatContest));
}

#[test]
fn refresh_providers_are_the_selected_on_demand_and_before_finish_ones() {
    let plan = EvidencePlan::new(vec![
        provider("tests", RunWhen::OnDemand),
        provider("audit", RunWhen::BeforeFinish),
        provider("lint", RunWhen::WorkerCompleted),
    ]);
    let names = |selection: &[&str]| {
        let selection: Vec<String> = selection.iter().map(|s| (*s).to_owned()).collect();
        super::refresh_providers(&selection, &plan)
            .into_iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        names(&["command:audit", "command:tests"]),
        ["tests", "audit"]
    );
    assert!(
        names(&["command:lint"]).is_empty(),
        "worker_completed is not a refresh"
    );
    assert!(names(&["worker_output"]).is_empty());
    assert_eq!(names(&["command_evidence"]), ["tests", "audit"]);
}

#[test]
fn a_stop_on_a_decisive_stuck_worker_is_irreversible() {
    // 0.97, not 0.95: in `f32`, 0.95 - 0.80 is just under the 0.15 margin.
    let s = Scenario::finish_ready().with_worker().set(STUCK, 0.97);
    assert!(
        s.proposals(&s.baseline())
            .iter()
            .any(|p| p.rule.as_str() == "core.worker-health:stuck")
    );
    assert_eq!(s.select(&s.baseline()).name(), "Stop");
    let plan = s.check_plan(STUCK);
    let triggers = plan
        .as_ref()
        .map(|p| p.triggers.clone())
        .unwrap_or_default();
    assert!(triggers.contains(&EnsembleTrigger::Irreversible));
    assert!(triggers.contains(&EnsembleTrigger::OutcomeRelevant));
    assert_eq!(plan.map(|p| p.rule_would_fire), Some(true));
}

#[test]
fn the_plan_records_what_the_ladder_could_use() {
    let s = Scenario::finish_ready()
        .set(READY, 0.79)
        .set(TESTS, 0.79)
        .reframable(&[TESTS])
        .selecting(TESTS, &["tests", "audit", "lint"]);
    assert_eq!(s.check_plan(TESTS).map(|p| p.reframe_available), Some(true));
    assert_eq!(
        s.check_plan(READY).map(|p| p.reframe_available),
        Some(false)
    );
    assert_eq!(
        s.check_plan(TESTS).map(|p| p.refresh_providers),
        Some(vec![EvidenceId::new("tests"), EvidenceId::new("audit")])
    );
    assert_eq!(
        s.check_plan(READY).map(|p| p.refresh_providers),
        Some(Vec::new())
    );
    let s = s.reframable_all().without_rung(Rung::ReframeSameModel);
    assert_eq!(s.check_plan(READY).map(|p| p.reframe_available), Some(true));
    assert!(!s.settings.has(Rung::ReframeSameModel));
}

// ---- built-in thresholds ----------------------------------------------------

#[test]
fn insufficient_documentation_beats_finish() {
    // docs/05 §6.3: `quality.documentation:insufficient` proposes StartWorker at 750, above
    // Finish at 700. The bundled definition had no `priority`, so it took the
    // `start-worker` default of 500 and lost to Finish.
    for enabled in [false, true] {
        let mut s = Scenario::finish_ready().set(DOCS, 0.30);
        s.settings.enabled = enabled;
        let proposals = s.proposals(&s.baseline());
        let docs = proposals
            .iter()
            .find(|p| p.rule.as_str() == "quality.documentation:insufficient");
        assert_eq!(
            docs.map(|p| p.priority),
            Some(crate::directive::priority::START_WORKER_DOCUMENTATION),
            "enabled={enabled}"
        );
        assert_eq!(
            s.select(&s.baseline()).name(),
            "StartWorker",
            "enabled={enabled}"
        );
    }
}

#[test]
fn the_finish_disjunct_reads_needs_verification_against_its_own_threshold() {
    // docs/05 §6.2: `needs_verification < 0.65`. `core.completion` used to look the
    // threshold up among its own checks, found none, and compared against 0.75. With
    // the ensemble off (no band), 0.70 then satisfied the disjunct and finished.
    let mut s = Scenario::finish_ready().set(NEEDS_VER, 0.70);
    s.settings.enabled = false;
    assert_eq!(s.thresholds()[&k(NEEDS_VER)], Some(0.65));
    assert!(
        !s.proposals(&s.baseline())
            .iter()
            .any(|p| p.rule.as_str() == "core.completion:thresholds-satisfied"),
        "0.70 is not below 0.65"
    );
    let s = s.set(NEEDS_VER, 0.60);
    assert!(
        s.proposals(&s.baseline())
            .iter()
            .any(|p| p.rule.as_str() == "core.completion:thresholds-satisfied")
    );
}

// ---- the R2 consistency rule ----------------------------------------------

#[test]
fn reframe_verdict_is_the_documented_consistency_rule() {
    // p = 0.92, p(not X) = 0.10: 1 - 0.10 = 0.90, within 0.15.
    assert_eq!(
        reframe_verdict(0.92, Some(0.10), 0.75, 0.15, false),
        Verdict::Confirm { side: Side::Above }
    );
    // p = 0.92, p(not X) = 0.75: 1 - 0.75 = 0.25, far outside.
    assert_eq!(
        reframe_verdict(0.92, Some(0.75), 0.75, 0.15, false),
        Verdict::Contest { side: Side::Below }
    );
    // Inconsistent but on the same side still contests.
    assert_eq!(
        reframe_verdict(0.99, Some(0.20), 0.75, 0.15, false),
        Verdict::Contest { side: Side::Above }
    );
    // Exactly at the tolerance is consistent (exactly representable values).
    assert_eq!(
        reframe_verdict(0.75, Some(0.5), 0.5, 0.25, false),
        Verdict::Confirm { side: Side::Above }
    );
    // A below-threshold value confirmed stays below.
    assert_eq!(
        reframe_verdict(0.30, Some(0.70), 0.75, 0.15, false),
        Verdict::Confirm { side: Side::Below }
    );
}

#[test]
fn the_f32_boundary_errs_toward_holding() {
    // Review finding 7. Values are compared as `f32`, so a value whose decimal distance is
    // exactly the margin (or the tolerance) may land on either side. What must never
    // happen is the unsafe direction: a value decimally inside the band being decisive,
    // or a decimally inconsistent R2 pair confirming. Two-decimal grid.
    let mut boundary_held = 0;
    for ti in 0..=100_i32 {
        for pi in 0..=100_i32 {
            let rc = ResolvedCheck::from_measurement(
                k(TESTS),
                Some(pi as f32 / 100.0),
                Some(ti as f32 / 100.0),
                0.15,
                true,
            );
            let distance = (pi - ti).abs();
            if distance < 15 {
                assert!(!rc.decisive, "p={pi} t={ti} is inside the band");
            }
            if distance > 15 {
                assert!(rc.decisive, "p={pi} t={ti} is outside the band");
            }
            if distance == 15 && !rc.decisive {
                boundary_held += 1;
            }
        }
    }
    assert!(
        boundary_held > 0,
        "some exact-margin values are held, as documented"
    );
    for pi in 0..=100_i32 {
        for qi in 0..=100_i32 {
            let distance = (pi - (100 - qi)).abs();
            let verdict = reframe_verdict(
                pi as f32 / 100.0,
                Some(qi as f32 / 100.0),
                0.75,
                0.15,
                false,
            );
            if distance > 15 {
                assert!(
                    matches!(verdict, Verdict::Contest { .. }),
                    "p={pi} q={qi} is inconsistent"
                );
            }
            if distance < 15 {
                assert!(
                    matches!(verdict, Verdict::Confirm { .. }),
                    "p={pi} q={qi} is consistent"
                );
            }
        }
    }
}

#[test]
fn beyond_two_decimals_the_f32_boundary_can_fall_either_side() {
    // Round-2 probe P5: the two-decimal guarantee above does not extend to arbitrary
    // precision. Pinned so that the documentation cannot drift back into claiming it does:
    // these values are decimally inside the band (|p - t| < 0.15), and at least one of them
    // is decisive in f32. The error is of the order of 1e-7.
    let mut decisive = 0;
    for text in ["0.6500000001", "0.65000001", "0.650000001"] {
        let p: f32 = text.parse().unwrap_or(0.0);
        let rc = ResolvedCheck::from_measurement(k(TESTS), Some(p), Some(0.80), 0.15, true);
        if rc.decisive {
            decisive += 1;
            assert!(
                ((0.80 - p) - 0.15).abs() < 1e-6,
                "{text}: off by more than rounding"
            );
        }
    }
    assert!(decisive > 0);
}

#[test]
fn an_unusable_reframe_abstains_except_on_an_irreversible_trigger() {
    for unusable in [
        None,
        Some(f32::NAN),
        Some(-0.1),
        Some(1.5),
        Some(f32::INFINITY),
    ] {
        assert_eq!(
            reframe_verdict(0.92, unusable, 0.75, 0.15, false),
            Verdict::Abstain,
            "{unusable:?}"
        );
        assert_eq!(
            reframe_verdict(0.92, unusable, 0.75, 0.15, true),
            Verdict::Contest { side: Side::Above },
            "{unusable:?}"
        );
    }
}

// ---- worked examples (§9) --------------------------------------------------

#[tokio::test]
async fn worked_example_9_1_thin_evidence_is_measured_not_judged() {
    let s = Scenario::finish_ready()
        .set(TESTS, 0.71)
        .selecting(TESTS, &["tests"])
        .reframable_all();
    let plan = s.plan();
    let triggers = s.triggers(TESTS);
    assert!(triggers.contains(&EnsembleTrigger::OutcomeRelevant));
    assert!(triggers.contains(&EnsembleTrigger::HighStakes));
    assert!(triggers.contains(&EnsembleTrigger::ClaimOnlyEvidence));

    // The command exits 1; the fresh answer is 0.08.
    let source =
        RecordedOpinionSource::new().with_refresh(CYCLE, TESTS, refreshed(Some(0.08), "tests", 1));
    let outcome = s
        .resolve_plan(&plan, &source)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let rc = &outcome.checks[&k(TESTS)];
    assert_eq!(
        rc.resolution,
        Resolution::EvidenceRefreshed {
            new_p: 0.08,
            provider: EvidenceId::new("tests"),
            supersedes: CYCLE,
            old_p: Some(0.71),
            exit_code: Some(1),
        }
    );
    assert!(rc.permitted, "decisively below: usable");
    assert!(!rc.at_least(0.75), "but the `>=` conjunct is false");
    assert_eq!(s.select(&outcome.checks).name(), "StartWorker");
    assert!(
        source.reframe_calls().is_empty(),
        "no second model was involved"
    );
    assert_eq!(source.refresh_calls().len(), 1);
    assert_eq!(
        source.refresh_calls()[0].providers,
        vec![EvidenceId::new("tests")]
    );
}

#[tokio::test]
async fn a_fake_passing_test_run_converts_a_0_71_into_a_decisive_value() {
    // The M5 exit criterion: the same check, and this time the command passes.
    let s = Scenario::finish_ready()
        .set(TESTS, 0.71)
        .selecting(TESTS, &["tests"]);
    assert_ne!(s.select(&s.baseline()).name(), "Finish");
    let source =
        RecordedOpinionSource::new().with_refresh(CYCLE, TESTS, refreshed(Some(0.93), "tests", 0));
    let outcome = s.run_plan(&source).await.unwrap_or_else(|e| panic!("{e}"));
    let rc = &outcome.checks[&k(TESTS)];
    assert!(rc.decisive && rc.permitted);
    assert_eq!(rc.jev_p, Some(0.93));
    assert_eq!(s.select(&outcome.checks).name(), "Finish");
}

#[tokio::test]
async fn worked_example_9_2_in_band_without_evidence_escalates_and_asks_no_model() {
    // No command evidence configured for the check, and R2 available: still not asked.
    let s = Scenario::finish_ready().set(TESTS, 0.71).reframable_all();
    let source = RecordedOpinionSource::new();
    let outcome = s.run_plan(&source).await.unwrap_or_else(|e| panic!("{e}"));
    let rc = &outcome.checks[&k(TESTS)];
    assert!(!rc.permitted);
    assert_eq!(rc.suppression, Some(CheckSuppression::InBand));
    assert!(matches!(
        &rc.resolution,
        Resolution::UnresolvedInBand { rungs, .. } if rungs.is_empty()
    ));
    assert!(source.reframe_calls().is_empty() && source.refresh_calls().is_empty());
    assert_ne!(s.select(&outcome.checks).name(), "Finish");
    assert_eq!(
        outcome.counters.check(&k(TESTS)).consecutive_in_band,
        1,
        "held cycles are counted"
    );

    // After `consecutive_contests_before_human` held cycles: escalate, still no model.
    let s = s.held_for(TESTS, 2);
    let source = RecordedOpinionSource::new();
    let outcome = s.run_plan(&source).await.unwrap_or_else(|e| panic!("{e}"));
    let rc = &outcome.checks[&k(TESTS)];
    assert!(!rc.permitted);
    assert_eq!(rc.suppression, Some(CheckSuppression::Unresolved));
    assert!(matches!(
        &rc.resolution,
        Resolution::UnresolvedInBand { rungs, .. } if rungs == &[Rung::Human]
    ));
    assert_eq!(outcome.escalations().count(), 1);
    assert!(source.reframe_calls().is_empty() && source.refresh_calls().is_empty());
}

#[tokio::test]
async fn worked_example_9_2_a_decisive_finish_is_contested() {
    // The doc's example uses the independent judge; until S3 the same suppression comes
    // from R2, which is what this asserts.
    let s = Scenario::finish_ready()
        .set(TESTS, 0.92)
        .reframable(&[TESTS]);
    assert_eq!(s.select(&s.baseline()).name(), "Finish");
    let source =
        RecordedOpinionSource::new().with_reframe(reframe_key(CYCLE, TESTS), answer(Some(0.75)));
    let outcome = s.run_plan(&source).await.unwrap_or_else(|e| panic!("{e}"));
    let rc = &outcome.checks[&k(TESTS)];
    assert!(!rc.permitted && rc.contested);
    assert_eq!(rc.jev_p, Some(0.92), "Jev's value is untouched");
    match &rc.resolution {
        Resolution::SuppressedByContest { jev_p, dissent } => {
            assert!((jev_p - 0.92).abs() < f32::EPSILON);
            assert_eq!(dissent.len(), 1);
            assert_eq!(dissent[0].p_reported, Some(0.75), "stored for audit");
        }
        other => panic!("expected a contest, got {other:?}"),
    }
    assert_ne!(s.select(&outcome.checks).name(), "Finish");
    assert_eq!(source.reframe_calls().len(), 1);
    assert_eq!(outcome.counters.opinions_used, 1);
}

#[tokio::test]
async fn worked_example_9_3_agreement_below_cannot_stop_a_worker() {
    let s = Scenario::finish_ready()
        .with_worker()
        .set(STUCK, 0.78)
        .reframable_all();
    let source = answering_all(&s.plan(), Some(0.75));
    let outcome = s.run_plan(&source).await.unwrap_or_else(|e| panic!("{e}"));
    let rc = &outcome.checks[&k(STUCK)];
    assert!(!rc.permitted, "in band: held");
    assert!(
        source.reframe_calls().is_empty(),
        "in-band checks never reach an opinion"
    );
    // Even if one had, a confirm on the same side changes nothing.
    let confirmed = apply_opinions(
        rc.clone(),
        &[opinion(Verdict::Confirm { side: Side::Below })],
    );
    assert_eq!(&confirmed, rc);
    let proposals = s.proposals(&outcome.checks);
    assert!(
        !proposals
            .iter()
            .any(|p| p.rule.as_str() == "core.worker-health:stuck"),
        "the worker continues"
    );
}

#[tokio::test]
async fn worked_example_9_4_only_evidence_may_finish() {
    let s = Scenario::finish_ready()
        .set(READY, 0.79)
        .selecting(READY, &["tests", "audit"])
        .reframable_all();
    // Without a refresh: Finish is impossible, and no judge is asked.
    let no_refresh = s
        .run_plan(&RecordedOpinionSource::new().with_refresh(CYCLE, READY, failed_refresh()))
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert_ne!(s.select(&no_refresh.checks).name(), "Finish");

    let s = s.without_rung(Rung::RefreshEvidence);
    let source = answering_all(&s.plan(), Some(0.02));
    let outcome = s.run_plan(&source).await.unwrap_or_else(|e| panic!("{e}"));
    assert!(source.reframe_calls().is_empty());
    assert!(!outcome.checks[&k(READY)].permitted);
    assert_ne!(s.select(&outcome.checks).name(), "Finish");

    // With R0: tests and audit pass, and the fresh answer is decisive.
    let s = Scenario::finish_ready()
        .set(READY, 0.79)
        .selecting(READY, &["tests", "audit"]);
    let source = RecordedOpinionSource::new().with_refresh(
        CYCLE,
        READY,
        RefreshOutcome {
            runs: vec![
                ProviderRun {
                    provider: EvidenceId::new("tests"),
                    exit_code: Some(0),
                },
                ProviderRun {
                    provider: EvidenceId::new("audit"),
                    exit_code: Some(0),
                },
            ],
            new_p: Some(0.96),
            error: None,
            state_digest: None,
        },
    );
    let outcome = s.run_plan(&source).await.unwrap_or_else(|e| panic!("{e}"));
    let rc = &outcome.checks[&k(READY)];
    assert!(rc.permitted && rc.decisive);
    assert_eq!(s.select(&outcome.checks).name(), "Finish");
    assert_eq!(
        record(&outcome, READY)
            .and_then(|r| r.refresh.as_ref())
            .map(|r| r.outcome.runs.len()),
        Some(2),
        "every provider that ran is recorded"
    );
}

#[tokio::test]
async fn worked_example_9_5_a_less_than_rule_is_not_collateral_damage() {
    let s = Scenario::finish_ready().set(DOCS, 0.31).reframable_all();
    let plan = s.check_plan(DOCS);
    assert_eq!(plan.as_ref().map(|p| p.rule_would_fire), Some(true));
    // R2 agrees it is below.
    let source = answering_all(&s.plan(), Some(0.70));
    let outcome = s.run_plan(&source).await.unwrap_or_else(|e| panic!("{e}"));
    let rc = &outcome.checks[&k(DOCS)];
    assert!(
        rc.permitted,
        "the direction is none of `permitted`'s business"
    );
    assert!(rc.less_than(0.75));
    let proposals = s.proposals(&outcome.checks);
    assert!(
        proposals
            .iter()
            .any(|p| p.rule.as_str() == "quality.documentation:insufficient"),
        "the `<` rule fires"
    );
    assert_eq!(s.select(&outcome.checks).name(), "StartWorker");
}

// ---- three-valued conditions ------------------------------------------------

const GUARD: &str = "my.guard__deploy_safe";

/// Review probe 1: a declarative responsibility whose `Stop` reads `not (k >= t)`.
fn guarded() -> Scenario {
    let mut s = Scenario::finish_ready().with_worker();
    let mut definitions = DefinitionSet::bundled().unwrap_or_default();
    let source = r#"enabled = true
always = true
title = "guard"
[checks.deploy_safe]
instructions = "Is the deployment proven safe?"
min_threshold = 0.75

[[rules]]
id = "my.guard:proceed"
when = "my.guard__deploy_safe >= 0.75"
propose = "continue"

[[rules]]
id = "my.guard:halt"
when = "not (my.guard__deploy_safe >= 0.75)"
propose = "stop"
reason = "deployment not proven safe"
"#;
    if let Ok(definition) =
        crate::definitions::ResponsibilityDefinition::parse("my.guard", source, "test")
    {
        definitions
            .definitions
            .insert("my.guard".into(), definition);
    }
    let ids: Vec<ResponsibilityId> = definitions
        .definitions
        .keys()
        .map(ResponsibilityId::new)
        .collect();
    if let Ok(registry) = ResponsibilityRegistry::from_definitions(&definitions, &ids, 30, 1) {
        s.registry = registry;
    }
    s.definitions = s.registry.checks();
    s.values.insert(k(GUARD), Some(0.95));
    s.reframable(&[GUARD])
}

#[tokio::test]
async fn an_r2_contest_cannot_enable_a_not_rule() {
    let s = guarded();
    assert!(s.registry.ids().iter().any(|id| id.as_str() == "my.guard"));
    assert_ne!(s.select(&s.baseline()).name(), "Stop");
    // R2: p(not X) = 0.95 against p(X) = 0.95 is inconsistent, so it contests.
    let source =
        RecordedOpinionSource::new().with_reframe(reframe_key(CYCLE, GUARD), answer(Some(0.95)));
    let outcome = s.run_plan(&source).await.unwrap_or_else(|e| panic!("{e}"));
    assert!(!outcome.checks[&k(GUARD)].permitted, "contested");
    assert!(
        !s.proposals(&outcome.checks)
            .iter()
            .any(|p| p.rule.as_str() == "my.guard:halt"),
        "a contest removed permission; it must not make `not (k >= t)` true"
    );
    assert_ne!(s.select(&outcome.checks).name(), "Stop");
}

/// The rules that may start firing when a check loses permission, and why each is not an
/// opinion enabling an action. Everything else — every declarative rule included — may
/// only stop firing.
fn may_fire_on_lost_permission(rule: &str, held: &BTreeMap<CheckKey, ResolvedCheck>) -> bool {
    match rule {
        // Rust fallbacks of `core.completion`: they fire because the `Finish`
        // precondition is not established, which is the conservative outcome.
        "core.completion:work-remains" | "core.completion:retry-stopped" => true,
        // `core.worker-health` proposes the more probable of two independent findings.
        // Holding the winner lets the runner-up through only if its own check is itself
        // permitted and above its threshold: nothing about the held check enables it.
        "core.worker-health:off-track" => held
            .get(&k(OFF_TRACK))
            .is_some_and(|c| c.at_least(c.threshold.unwrap_or(1.0))),
        "core.worker-health:stuck" => held
            .get(&k(STUCK))
            .is_some_and(|c| c.at_least(c.threshold.unwrap_or(1.0))),
        _ => false,
    }
}

#[test]
fn no_rule_starts_firing_because_a_check_lost_permission() {
    // Review finding 3: monotonic conservatism, stated over rules. For every check in
    // every scenario, holding it may remove proposals but may add only the documented
    // Rust fallbacks.
    let mut exercised = 0;
    for guard in [false, true] {
        for worker in [false, true] {
            for &(key, values) in &[
                (READY, &[0.05_f32, 0.30, 0.95][..]),
                (TESTS, &[0.05, 0.95][..]),
                (NEEDS_VER, &[0.05, 0.90][..]),
                (DOCS, &[0.05, 0.95][..]),
                (STUCK, &[0.05, 0.97][..]),
                (OFF_TRACK, &[0.05, 0.97][..]),
                (HUMAN, &[0.05, 0.97][..]),
                (GUARD, &[0.05, 0.95][..]),
            ] {
                for &p in values {
                    let mut s = if guard {
                        guarded()
                    } else {
                        Scenario::finish_ready()
                    };
                    if worker && s.worker.is_none() {
                        s = s.with_worker();
                    }
                    if !worker {
                        s.worker = None;
                    }
                    let s = s.set(key, p).set(OFF_TRACK, 0.90);
                    let baseline = s.baseline();
                    let before: BTreeSet<String> = s
                        .proposals(&baseline)
                        .iter()
                        .map(|p| p.rule.to_string())
                        .collect();
                    for (held_key, rc) in &baseline {
                        if !rc.permitted {
                            continue;
                        }
                        let mut held = baseline.clone();
                        held.insert(
                            held_key.clone(),
                            apply_opinions(
                                rc.clone(),
                                &[opinion(Verdict::Contest { side: Side::Below })],
                            ),
                        );
                        for proposal in s.proposals(&held) {
                            let rule = proposal.rule.to_string();
                            if before.contains(&rule) {
                                continue;
                            }
                            exercised += 1;
                            assert!(
                                may_fire_on_lost_permission(&rule, &held),
                                "holding {held_key} made `{rule}` fire (guard={guard} \
                                 worker={worker} {key}={p})"
                            );
                        }
                    }
                }
            }
        }
    }
    assert!(exercised > 0);
}

/// Round-3 probe G1: a declarative Stop on `a >= 0.75 or b >= 0.75`.
fn either_stop() -> Scenario {
    let mut s = Scenario::finish_ready().with_worker();
    let mut definitions = DefinitionSet::bundled().unwrap_or_default();
    let source = r#"enabled = true
always = true
title = "either"
[checks.a]
instructions = "?"
min_threshold = 0.75
[checks.b]
instructions = "?"
min_threshold = 0.75
[[rules]]
id = "my.either:halt"
when = "my.either__a >= 0.75 or my.either__b >= 0.75"
propose = "stop"
reason = "either signal says stop"
"#;
    if let Ok(definition) =
        crate::definitions::ResponsibilityDefinition::parse("my.either", source, "test")
    {
        definitions
            .definitions
            .insert("my.either".into(), definition);
    }
    let ids: Vec<ResponsibilityId> = definitions
        .definitions
        .keys()
        .map(ResponsibilityId::new)
        .collect();
    if let Ok(registry) = ResponsibilityRegistry::from_definitions(&definitions, &ids, 30, 1) {
        s.registry = registry;
    }
    s.definitions = s.registry.checks();
    s.values.insert(k("my.either__a"), Some(0.95));
    s.values.insert(k("my.either__b"), Some(0.95));
    s.reframable(&["my.either__a", "my.either__b"])
}

#[tokio::test]
async fn a_stop_resting_on_a_disjunction_is_guarded_on_every_disjunct() {
    // Flipping either check alone leaves the Stop in place, so the single-check
    // counterfactual finds no dependency. The rule reads both, so both are guarded.
    let s = either_stop();
    assert_eq!(s.select(&s.baseline()).name(), "Stop");
    let plan = s.plan();
    // Every R2 answer contests.
    let source = answering_by(&plan, Some);
    let outcome = s
        .resolve_plan(&plan, &source)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let asked: BTreeSet<CheckKey> = source
        .reframe_calls()
        .into_iter()
        .map(|r| r.opinion.check)
        .collect();
    assert!(asked.contains(&k("my.either__a")) && asked.contains(&k("my.either__b")));
    assert_ne!(s.select(&outcome.checks).name(), "Stop");
    // Both confirm: the Stop goes ahead, guarded on both.
    let source = answering_by(&plan, |p| Some(1.0 - p));
    let outcome = s
        .resolve_plan(&plan, &source)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(s.select(&outcome.checks).name(), "Stop");
    assert_irreversible_guarded(&s, &outcome, "either, confirmed");
    for key in ["my.either__a", "my.either__b"] {
        assert!(
            record(&outcome, key).is_some_and(|r| r.irreversible_guard.is_some()),
            "{key}"
        );
    }
}

#[test]
fn builtin_read_lists_match_the_rules() {
    // `RULE_READS` against the code: in every scenario where a built-in rule fires, the
    // checks whose loss of permission removes it are exactly within its read list, and over
    // the scenarios every listed check is seen to matter.
    use crate::responsibility::builtin::{RULE_READS, rule_reads};
    let mut drift = Scenario::finish_ready().with_worker().set(DRIFT, 0.97);
    drift.observation.instructions = Some(crate::observation::RepoInstructions {
        file: "AGENTS.md".into(),
        excerpt: "x".into(),
        truncated: false,
    });
    let scenarios = vec![
        Scenario::finish_ready(),
        Scenario::finish_ready().set(READY, 0.30),
        Scenario::finish_ready().set(NEEDS_VER, 0.90),
        Scenario::finish_ready().set(HUMAN, 0.97),
        Scenario::finish_ready().with_worker().set(STUCK, 0.97),
        Scenario::finish_ready().with_worker().set(OFF_TRACK, 0.97),
        drift,
    ];
    let mut seen: BTreeMap<String, BTreeSet<CheckKey>> = BTreeMap::new();
    let mut fired: BTreeSet<String> = BTreeSet::new();
    for s in &scenarios {
        let baseline = s.baseline();
        let before = s.proposals(&baseline);
        for proposal in &before {
            let rule = proposal.rule.to_string();
            let Some(reads) = rule_reads(&rule) else {
                continue;
            };
            fired.insert(rule.clone());
            for (key, rc) in &baseline {
                if !rc.permitted {
                    continue;
                }
                let mut held = baseline.clone();
                held.insert(
                    key.clone(),
                    apply_opinions(
                        rc.clone(),
                        &[opinion(Verdict::Contest { side: Side::Below })],
                    ),
                );
                if !s.proposals(&held).iter().any(|p| p.rule == proposal.rule) {
                    assert!(
                        reads.contains(key),
                        "{rule} depends on {key}, not in its read list"
                    );
                    seen.entry(rule.clone()).or_default().insert(key.clone());
                }
            }
        }
    }
    for (rule, reads) in RULE_READS {
        if reads.is_empty() {
            continue;
        }
        assert!(fired.contains(*rule), "{rule} never fired in the scenarios");
        let listed: BTreeSet<CheckKey> = rule_reads(rule).unwrap_or_default().into_iter().collect();
        assert_eq!(
            seen.get(*rule),
            Some(&listed),
            "{rule}: listed reads not all seen to matter"
        );
    }
}

#[tokio::test]
async fn a_contest_cannot_unmask_a_stop() {
    // Round-2 probe N1a, as the integrated review settled it. `needs_human` and
    // `worker_stuck` both decisively high, with a worker that cannot be steered: `Escalate`
    // wins. R2 contests `needs_human`. An earlier revision let that unmask the `Stop`
    // resting on `worker_stuck`, guarded it in the gate, and — given a confirm on
    // `worker_stuck` — let it execute: an opinion turned an escalation into a Stop. The Stop
    // rests on `needs_human` (resolving it either way changes the selection), so with
    // `needs_human` held it is refused (`UnresolvedDependency`), whatever `worker_stuck`'s
    // own opinion says, and the gate has no Stop to guard.
    let s = Scenario::finish_ready()
        .with_worker()
        .set(HUMAN, 0.97)
        .set(STUCK, 0.97)
        .reframable(&[HUMAN, STUCK]);
    assert_eq!(s.select(&s.baseline()).name(), "Escalate");
    for stuck in [None, Some(0.03)] {
        let source = RecordedOpinionSource::new()
            .with_reframe(reframe_key(CYCLE, HUMAN), answer(Some(0.97)))
            .with_reframe(reframe_key(CYCLE, STUCK), answer(stuck));
        let outcome = resolved(&s, &source).await;
        assert_ne!(s.select(&outcome.checks).name(), "Stop", "{stuck:?}");
        assert!(
            record(&outcome, STUCK).is_none_or(|r| r.irreversible_guard.is_none()),
            "nothing irreversible was selected, so nothing was guarded: {stuck:?}"
        );
        assert_irreversible_guarded(&s, &outcome, "N1a");
    }
}

#[tokio::test]
async fn a_refresh_cannot_unmask_an_unguarded_finish() {
    // Round-2 probe N1b. `ready_to_finish` is in band, so the ladder plans for
    // StartWorker and buys no opinion on `tests_sufficient`. R0 then refreshes `ready` to
    // a decisive 0.95, and the cycle's selection becomes Finish — resting on
    // `tests_sufficient`, which nobody asked about.
    let s = Scenario::finish_ready()
        .set(READY, 0.79)
        .selecting(READY, &["audit"])
        .set(TESTS, 0.92)
        .reframable(&[TESTS]);
    let source = RecordedOpinionSource::new()
        .with_refresh(CYCLE, READY, refreshed(Some(0.95), "audit", 0))
        .with_reframe(reframe_key(CYCLE, TESTS), answer(Some(0.92)));
    let outcome = resolved(&s, &source).await;
    assert_eq!(
        source.reframe_calls().len(),
        1,
        "the gate bought the irreversible opinion"
    );
    assert_ne!(s.select(&outcome.checks).name(), "Finish");
    assert_eq!(
        record(&outcome, TESTS).and_then(|r| r.irreversible_guard.clone()),
        Some(super::IrreversibleGuard {
            directive: "Finish".into(),
            outcome: super::GuardOutcome::Bought,
        })
    );
    // On a confirm it finishes.
    let source = RecordedOpinionSource::new()
        .with_refresh(CYCLE, READY, refreshed(Some(0.95), "audit", 0))
        .with_reframe(reframe_key(CYCLE, TESTS), answer(Some(0.08)));
    let outcome = resolved(&s, &source).await;
    assert_eq!(s.select(&outcome.checks).name(), "Finish");
    assert_irreversible_guarded(&s, &outcome, "N1b, confirmed");
}

#[tokio::test]
async fn the_gate_holds_when_the_budget_is_spent_and_never_refreshes() {
    let mut s = Scenario::finish_ready()
        .set(READY, 0.79)
        .selecting(READY, &["audit"])
        .set(TESTS, 0.92)
        .reframable(&[TESTS]);
    s.ensemble.opinions_used = s.settings.max_opinions_per_run;
    let source =
        RecordedOpinionSource::new().with_refresh(CYCLE, READY, refreshed(Some(0.95), "audit", 0));
    let outcome = resolved(&s, &source).await;
    assert!(source.reframe_calls().is_empty());
    assert_eq!(source.refresh_calls().len(), 1, "only the ladder refreshes");
    assert!(!outcome.checks[&k(TESTS)].permitted);
    assert_eq!(
        record(&outcome, TESTS)
            .and_then(|r| r.irreversible_guard.clone())
            .map(|g| g.outcome),
        Some(super::GuardOutcome::BudgetSpent)
    );
    assert_ne!(s.select(&outcome.checks).name(), "Finish");
    // And, with the human rung configured, it says why.
    let asks: Vec<&str> = outcome.escalations().map(|(_, ask)| ask).collect();
    assert_eq!(asks.len(), 1, "{asks:?}");
    assert!(
        asks[0].contains("Finish on") && asks[0].contains(TESTS),
        "{}",
        asks[0]
    );
}

#[tokio::test]
async fn the_runner_up_stop_is_guarded_in_its_own_right() {
    // The gate's rounds, with the dependency invariant comparing kinds. The worker-health
    // Stop on `worker_stuck` is selected, and its irreversible opinion contests. The
    // `repository.instructions` Stop on `agents_md_drift`, which lost the tie-break, now
    // wins: the same kind of directive, on a separate, decisive finding, so the contest on
    // `worker_stuck` does not hold it (resolving `worker_stuck` would only change which Stop).
    // It executes only on its own fresh opinion, which the gate buys in the next round.
    let mut s = Scenario::finish_ready()
        .with_worker()
        .set(HUMAN, 0.03)
        .set(STUCK, 0.97)
        .set(DRIFT, 0.97)
        .reframable(&[HUMAN, STUCK, DRIFT]);
    s.observation.instructions = Some(crate::observation::RepoInstructions {
        file: "AGENTS.md".into(),
        excerpt: "do it this way".into(),
        truncated: false,
    });
    assert_eq!(s.select(&s.baseline()).name(), "Stop");
    for (drift, stops) in [(Some(0.03_f32), true), (Some(0.97), false), (None, false)] {
        let source = answering_by(&s.plan(), |p| Some(1.0 - p))
            .with_reframe(reframe_key(CYCLE, STUCK), answer(Some(0.97)))
            .with_reframe(reframe_key(CYCLE, DRIFT), answer(drift));
        let outcome = resolved(&s, &source).await;
        assert!(
            !outcome.checks[&k(STUCK)].permitted,
            "worker_stuck was contested"
        );
        let guard = record(&outcome, DRIFT)
            .and_then(|r| r.irreversible_guard.clone())
            .map(|g| (g.directive, g.outcome));
        assert_eq!(
            guard,
            Some(("Stop".to_owned(), super::GuardOutcome::Bought)),
            "{drift:?}"
        );
        assert_eq!(
            s.select(&outcome.checks).name() == "Stop",
            stops,
            "drift answered {drift:?}"
        );
        assert_irreversible_guarded(&s, &outcome, "rounds");
    }
}

#[tokio::test]
async fn a_stop_on_either_of_two_findings_rests_on_both() {
    // `worker_stuck` and `work_off_track` both decisive: worker-health proposes the more
    // probable, and the Stop rests on each of them.
    let s = Scenario::finish_ready()
        .with_worker()
        .set(STUCK, 0.97)
        .set(OFF_TRACK, 0.96)
        .reframable(&[STUCK, OFF_TRACK]);
    assert_eq!(s.select(&s.baseline()).name(), "Stop");
    let plan = s.plan();
    // Both confirm: the Stop goes ahead, guarded on both.
    let outcome = s
        .resolve_plan(&plan, &answering_by(&plan, |p| Some(1.0 - p)))
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(s.select(&outcome.checks).name(), "Stop");
    assert_irreversible_guarded(&s, &outcome, "both confirm");
    for key in [STUCK, OFF_TRACK] {
        assert!(
            outcome
                .counters
                .check(&k(key))
                .last_opinion
                .is_some_and(|m| m.irreversible),
            "{key} was guarded"
        );
    }
}

#[tokio::test]
async fn a_provisional_finish_buys_no_irreversible_opinion() {
    // Round-3 item 2a. With `before_finish` providers pending the runtime will not execute
    // this Finish; it re-assesses. Nothing is bought to guard it, and nothing is held for
    // it. The re-assessed Finish, which does execute, is guarded as usual.
    let mut provisional = Scenario::finish_ready().reframable_all();
    provisional.finish_provisional = true;
    provisional.cycle = AssessmentSeq(20);
    let plan = provisional.plan();
    assert!(
        plan.triggered()
            .iter()
            .all(|c| !c.has(EnsembleTrigger::Irreversible)),
        "no Irreversible trigger for a provisional Finish"
    );
    let source = answering_by(&plan, |p| Some(1.0 - p));
    let out = provisional
        .resolve_plan(&plan, &source)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(provisional.select(&out.checks).name(), "Finish");
    assert!(
        out.records.iter().all(|r| r.irreversible_guard.is_none()),
        "the gate stays out"
    );
    let provisional_calls = source.reframe_calls().len();

    // The same cycle, executed: the gate guards everything the Finish rests on.
    let mut executed = Scenario::finish_ready().reframable_all();
    executed.cycle = AssessmentSeq(20);
    let plan = executed.plan();
    let source = answering_by(&plan, |p| Some(1.0 - p));
    let out = executed
        .resolve_plan(&plan, &source)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(executed.select(&out.checks).name(), "Finish");
    assert_irreversible_guarded(&executed, &out, "executed");
    assert!(
        source.reframe_calls().len() > provisional_calls,
        "provisional {provisional_calls} vs executed {}",
        source.reframe_calls().len()
    );

    // The re-assessment after the provisional Finish is guarded: nothing it bought covers.
    let mut reassessed = Scenario::finish_ready().reframable_all();
    reassessed.cycle = AssessmentSeq(21);
    reassessed.ensemble = out.counters.clone();
    let plan = reassessed.plan();
    let source = answering_by(&plan, |p| Some(1.0 - p));
    let out = reassessed
        .resolve_plan(&plan, &source)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert_irreversible_guarded(&reassessed, &out, "re-assessed");
}

#[tokio::test]
async fn a_spent_budget_escalates_instead_of_holding_finish_silently() {
    // Round-3 probe G2 (liveness). Every check says done and the opinion budget is spent.
    // Holding the Finish without a word left policy starting a worker every cycle up to
    // `max_workers`, then escalating that the job "does not look complete". Now the first
    // exhausted cycle raises one escalation that says what happened, and no worker is
    // started for nothing.
    let mut counters = EnsembleCounters {
        opinions_used: 20,
        ..EnsembleCounters::default()
    };
    let mut workers = 1;
    for i in 0..8_u64 {
        let mut s = Scenario::finish_ready().reframable_all();
        s.cycle = AssessmentSeq(40 + i);
        s.ensemble = counters.clone();
        s.counters.workers_run = workers;
        let plan = s.plan();
        let outcome = s
            .resolve_plan(&plan, &answering_by(&plan, |p| Some(1.0 - p)))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let escalations: Vec<String> = outcome
            .escalations()
            .map(|(_, ask)| ask.to_owned())
            .collect();
        assert_eq!(i, 0, "escalated on the first exhausted cycle");
        assert_eq!(
            escalations.len(),
            1,
            "one escalation, not one per check: {escalations:?}"
        );
        assert!(
            escalations[0].contains("opinion budget"),
            "{}",
            escalations[0]
        );
        assert!(escalations[0].contains("Finish on"), "{}", escalations[0]);
        assert!(escalations[0].contains(TESTS), "{}", escalations[0]);
        assert_ne!(s.select(&outcome.checks).name(), "Finish");
        assert!(record(&outcome, TESTS).is_some_and(|r| {
            r.limits
                .iter()
                .any(|l| matches!(l, EnsembleLimit::OpinionBudget { .. }))
        }));
        // The runtime turns the escalation into an Escalate; no worker is burned.
        if !escalations.is_empty() {
            break;
        }
        workers += 1;
        counters = outcome.counters;
    }
    assert_eq!(workers, 1);

    // Without the `human` rung there is nobody to tell: hold, as before.
    let mut s = Scenario::finish_ready()
        .reframable_all()
        .without_rung(Rung::Human);
    s.ensemble.opinions_used = 20;
    let plan = s.plan();
    let outcome = s
        .resolve_plan(&plan, &answering_by(&plan, |p| Some(1.0 - p)))
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(outcome.escalations().count(), 0);
    assert_ne!(s.select(&outcome.checks).name(), "Finish");
}

#[tokio::test]
async fn the_cost_of_a_provisional_and_executed_finish_pair() {
    // Round-4 item 2. Every check reframable and confirming, all decisive, Finish selected.
    // Cycle 20 is provisional (before_finish providers pending): nothing is bought. Cycle 21
    // is the executed Finish: the gate guards every check it rests on.
    let mut counters = EnsembleCounters::default();
    let mut calls = Vec::new();
    for (i, provisional) in [true, false].into_iter().enumerate() {
        let mut s = Scenario::finish_ready().reframable_all();
        s.cycle = AssessmentSeq(20 + i as u64);
        s.finish_provisional = provisional;
        s.ensemble = counters.clone();
        let plan = s.plan();
        let source = answering_by(&plan, |p| Some(1.0 - p));
        let outcome = s
            .resolve_plan(&plan, &source)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(s.select(&outcome.checks).name(), "Finish");
        if !provisional {
            assert_irreversible_guarded(&s, &outcome, "executed");
        }
        calls.push(source.reframe_calls().len());
        counters = outcome.counters;
    }
    // The figure stated in docs/06-ensemble.md §6.
    assert_eq!(calls, [0, 6], "provisional, executed");

    // A Verify outranked by the provisional Finish is not about to act either: it is not
    // selected, and nothing is executed this cycle. (An earlier revision counted any
    // proposal that would disappear if its check were held, and bought `needs_verification`
    // an opinion here; the integrated review restored "about to act" to "selected".)
    let mut s = Scenario::finish_ready()
        .set(NEEDS_VER, 0.90)
        .reframable(&[NEEDS_VER, READY]);
    s.counters.verifier_completed = true;
    s.finish_provisional = true;
    assert_eq!(s.select(&s.baseline()).name(), "Finish");
    assert!(
        s.proposals(&s.baseline())
            .iter()
            .any(|p| p.rule.as_str() == "core.verification:warranted")
    );
    let plan = s.plan();
    let source = answering_by(&plan, |p| Some(1.0 - p));
    s.resolve_plan(&plan, &source)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let asked: BTreeSet<CheckKey> = source
        .reframe_calls()
        .into_iter()
        .map(|r| r.opinion.check)
        .collect();
    assert_eq!(
        asked,
        BTreeSet::new(),
        "neither the Verify's check nor the Finish's"
    );
}

#[tokio::test]
async fn the_gate_terminates_within_its_bound() {
    // Every check reframable and decisive, Finish pending, every answer inconsistent: the
    // gate guards, contests, re-arbitrates, and stops within checks + 1 rounds.
    for worker in [false, true] {
        let mut s = Scenario::finish_ready()
            .set(STUCK, 0.97)
            .set(HUMAN, 0.97)
            .reframable_all();
        if worker {
            s = s.with_worker();
        }
        let plan = s.plan();
        for q in [None, Some(0.5), Some(0.97)] {
            let outcome = s
                .resolve_plan(&plan, &answering_all(&plan, q))
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            assert!(outcome.gate_rounds >= 1);
            assert!(
                outcome.gate_rounds as usize <= outcome.checks.len() + 1,
                "{} rounds for {} checks",
                outcome.gate_rounds,
                outcome.checks.len()
            );
            assert_irreversible_guarded(&s, &outcome, &format!("worker={worker} q={q:?}"));
        }
    }
}

// ---- cost ------------------------------------------------------------------

#[tokio::test]
async fn a_triggered_check_in_band_or_inert_requests_zero_opinions() {
    // In band, for every check and every value in the band, with every rung on and R2
    // available everywhere.
    for key in [
        IMPL, REQS, READY, TESTS, NEEDS_VER, STUCK, OFF_TRACK, HUMAN, DRIFT, DOCS,
    ] {
        for worker in [false, true] {
            for offset in [-0.14_f32, -0.05, 0.0, 0.05, 0.14] {
                let mut s = Scenario::finish_ready().reframable_all();
                if worker {
                    s = s.with_worker();
                }
                let t = s.thresholds()[&k(key)].unwrap_or(0.75);
                let s = s.set(key, (t + offset).clamp(0.0, 1.0));
                let plan = s.plan();
                let source = answering_all(&plan, Some(0.5));
                let outcome = s
                    .resolve_plan(&plan, &source)
                    .await
                    .unwrap_or_else(|e| panic!("{e}"));
                let asked: Vec<CheckKey> = source
                    .reframe_calls()
                    .into_iter()
                    .map(|r| r.opinion.check)
                    .collect();
                assert!(
                    !asked.contains(&k(key)),
                    "{key} in band at {offset} was asked about"
                );
                for record in &outcome.records {
                    if !record.baseline.decisive {
                        assert!(record.opinions.is_empty(), "{}", record.check);
                    }
                }
            }
        }
    }

    // Decisive, but no rule acts on it: the ladder buys nothing. "Inert" is established
    // here from the proposals alone — holding the check removes no proposal — not read back
    // from the plan under test. (Not from the selected directive: holding *any* check
    // blocks a Finish through `UnresolvedUncertainty`, which is exactly the confusion the
    // counterfactual construction avoids.) The irreversible gate may still buy for such a
    // check, but only if the selected Finish/Stop rests on it; answers here confirm, so the
    // gate gets to act.
    let mut gate_purchases_for_inert_checks = 0;
    for worker in [false, true] {
        for &(key, p) in &[
            (STUCK, 0.05_f32),
            (DRIFT, 0.05),
            (HUMAN, 0.05),
            (DOCS, 0.95),
            (READY, 0.30),
            (TESTS, 0.05),
            (NEEDS_VER, 0.90),
            (STUCK, 0.97),
        ] {
            let mut s = Scenario::finish_ready().set(key, p).reframable_all();
            if worker {
                s = s.with_worker();
            }
            let baseline = s.baseline();
            let plan = s.plan();
            let source = answering_by(&plan, |v| Some(1.0 - v));
            let outcome = s
                .resolve_plan(&plan, &source)
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            let label = format!("{key}={p} worker={worker}");
            let before = s.proposals(&baseline);
            // The state the gate first saw: every guarded check back as the ladder left it.
            let mut pre_gate = outcome.checks.clone();
            for record in &outcome.records {
                if record.irreversible_guard.is_some() {
                    pre_gate.insert(record.check.clone(), record.baseline.clone());
                }
            }
            let gate_deps = irreversible_dependencies(&s, &pre_gate);
            for (check, rc) in &baseline {
                let asked = source
                    .reframe_calls()
                    .iter()
                    .any(|r| r.opinion.check == *check);
                let guard =
                    record(&outcome, check.as_str()).and_then(|r| r.irreversible_guard.clone());
                if !rc.decisive {
                    assert!(!asked, "{check} is in band but was asked about: {label}");
                    continue;
                }
                let mut held = baseline.clone();
                held.insert(
                    check.clone(),
                    apply_opinions(
                        rc.clone(),
                        &[opinion(Verdict::Contest { side: Side::Below })],
                    ),
                );
                let after = s.proposals(&held);
                let inert = before.iter().all(|proposal| after.contains(proposal));
                if inert && asked {
                    gate_purchases_for_inert_checks += 1;
                    // Only the gate, and only for what the irreversible selection rests on.
                    assert_eq!(
                        guard.as_ref().map(|g| g.outcome),
                        Some(super::GuardOutcome::Bought),
                        "{check} is inert; only the gate may buy for it: {label}"
                    );
                    assert!(
                        gate_deps.contains(check),
                        "the gate bought for {check}, which the selection does not rest on: \
                         {label}"
                    );
                }
            }
        }
    }
    assert!(
        gate_purchases_for_inert_checks > 0,
        "the gate's half was exercised (e.g. needs_human under a Finish)"
    );
}

#[tokio::test]
async fn the_last_opinion_in_the_budget_is_still_bought() {
    // Pins the budget comparison: with one opinion left, it is bought.
    let mut s = Scenario::finish_ready()
        .set(TESTS, 0.92)
        .reframable(&[TESTS]);
    s.ensemble.opinions_used = s.settings.max_opinions_per_run - 1;
    let source =
        RecordedOpinionSource::new().with_reframe(reframe_key(CYCLE, TESTS), answer(Some(0.08)));
    let outcome = s.run_plan(&source).await.unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(source.reframe_calls().len(), 1);
    assert_eq!(
        outcome.counters.opinions_used,
        s.settings.max_opinions_per_run
    );
    assert!(outcome.checks[&k(TESTS)].permitted);
}

#[tokio::test]
async fn repeat_contest_rows_from_the_contract() {
    // Written from docs/06-ensemble.md §3 and §6 and the review's decisions, not from the
    // resolver. Columns: value, in-band count, contest count, human rung, R2 available,
    // expected escalation, expected permitted, expected R2 calls.
    struct Row {
        key: &'static str,
        p: f32,
        in_band: u32,
        contests: u32,
        human: bool,
        reframe: bool,
        escalate: bool,
        permitted: bool,
        calls: usize,
    }
    let rows = [
        // In band, high stakes, held for two cycles: escalate (with a person to ask).
        Row {
            key: TESTS,
            p: 0.71,
            in_band: 2,
            contests: 0,
            human: true,
            reframe: true,
            escalate: true,
            permitted: false,
            calls: 0,
        },
        Row {
            key: TESTS,
            p: 0.71,
            in_band: 2,
            contests: 0,
            human: false,
            reframe: true,
            escalate: false,
            permitted: false,
            calls: 0,
        },
        Row {
            key: TESTS,
            p: 0.71,
            in_band: 1,
            contests: 0,
            human: true,
            reframe: true,
            escalate: false,
            permitted: false,
            calls: 0,
        },
        // In band, but no high-stakes rule depends on it (no worker): hold, do not escalate.
        Row {
            key: DRIFT,
            p: 0.79,
            in_band: 5,
            contests: 0,
            human: true,
            reframe: true,
            escalate: false,
            permitted: false,
            calls: 0,
        },
        // Decisive after in-band history: the history does not count.
        Row {
            key: TESTS,
            p: 0.92,
            in_band: 5,
            contests: 0,
            human: true,
            reframe: true,
            escalate: false,
            permitted: true,
            calls: 1,
        },
        // Decisive, contested twice: escalate instead of asking a third time.
        Row {
            key: TESTS,
            p: 0.92,
            in_band: 0,
            contests: 2,
            human: true,
            reframe: true,
            escalate: true,
            permitted: false,
            calls: 0,
        },
        // ... unless there is no opinion to buy: it stands, whatever its history.
        Row {
            key: TESTS,
            p: 0.92,
            in_band: 0,
            contests: 2,
            human: true,
            reframe: false,
            escalate: false,
            permitted: true,
            calls: 0,
        },
        // ... or nobody to escalate to: ask again.
        Row {
            key: TESTS,
            p: 0.92,
            in_band: 0,
            contests: 2,
            human: false,
            reframe: true,
            escalate: false,
            permitted: true,
            calls: 1,
        },
        // Decisive and inert: nothing acts on it, nothing to escalate.
        Row {
            key: DRIFT,
            p: 0.05,
            in_band: 0,
            contests: 7,
            human: true,
            reframe: true,
            escalate: false,
            permitted: true,
            calls: 0,
        },
    ];
    for row in rows {
        let mut s = Scenario::finish_ready().set(row.key, row.p);
        if row.reframe {
            s = s.reframable(&[row.key]);
        }
        if !row.human {
            s = s.without_rung(Rung::Human);
        }
        let s = s
            .held_for(row.key, row.in_band)
            .contested_for(row.key, row.contests);
        // Confirms if asked.
        let p_negated = 1.0 - row.p;
        let source = RecordedOpinionSource::new()
            .with_reframe(reframe_key(CYCLE, row.key), answer(Some(p_negated)));
        let outcome = s.run_plan(&source).await.unwrap_or_else(|e| panic!("{e}"));
        let label = format!(
            "{} p={} in_band={} contests={} human={} reframe={}",
            row.key, row.p, row.in_band, row.contests, row.human, row.reframe
        );
        assert_eq!(
            outcome.escalations().any(|(check, _)| *check == k(row.key)),
            row.escalate,
            "escalation: {label}"
        );
        assert_eq!(
            outcome.checks[&k(row.key)].permitted,
            row.permitted,
            "permitted: {label}"
        );
        assert_eq!(source.reframe_calls().len(), row.calls, "calls: {label}");
    }
}

#[tokio::test]
async fn a_refresh_whose_command_never_ran_does_not_permit() {
    // Review probe 4: a fresh answer with no command behind it is not a measurement.
    for runs in [
        Vec::new(),
        vec![ProviderRun {
            provider: EvidenceId::new("audit"),
            exit_code: None,
        }],
    ] {
        let s = Scenario::finish_ready()
            .set(READY, 0.79)
            .selecting(READY, &["audit"]);
        let source = RecordedOpinionSource::new().with_refresh(
            CYCLE,
            READY,
            RefreshOutcome {
                runs: runs.clone(),
                new_p: Some(0.95),
                error: Some("command could not be launched".into()),
                state_digest: None,
            },
        );
        let outcome = s.run_plan(&source).await.unwrap_or_else(|e| panic!("{e}"));
        let rc = &outcome.checks[&k(READY)];
        assert!(!rc.permitted, "{runs:?}");
        assert!(
            !matches!(rc.resolution, Resolution::EvidenceRefreshed { .. }),
            "{runs:?}"
        );
        assert_ne!(s.select(&outcome.checks).name(), "Finish", "{runs:?}");
        let refresh = record(&outcome, READY).and_then(|r| r.refresh.clone());
        assert_eq!(
            refresh.and_then(|r| r.rejected),
            Some(super::RefreshRejection::NoProviderRan),
            "the failed refresh is visible"
        );
        assert_eq!(
            outcome.counters.check(&k(READY)).refreshes,
            1,
            "and it counts"
        );
    }
}

#[tokio::test]
async fn a_run_of_a_provider_that_was_not_requested_does_not_count() {
    // Round-2 probe P4b: `lint` exited, but the refresh asked for `audit`.
    let s = Scenario::finish_ready()
        .set(READY, 0.79)
        .selecting(READY, &["audit"]);
    let source = RecordedOpinionSource::new().with_refresh(
        CYCLE,
        READY,
        RefreshOutcome {
            runs: vec![ProviderRun {
                provider: EvidenceId::new("lint"),
                exit_code: Some(0),
            }],
            new_p: Some(0.95),
            error: None,
            state_digest: None,
        },
    );
    let outcome = resolved(&s, &source).await;
    let rc = &outcome.checks[&k(READY)];
    assert!(!rc.permitted);
    assert!(!matches!(
        rc.resolution,
        Resolution::EvidenceRefreshed { .. }
    ));
    let refresh = record(&outcome, READY).and_then(|r| r.refresh.clone());
    assert_eq!(
        refresh.as_ref().and_then(|r| r.rejected),
        Some(super::RefreshRejection::NoProviderRan)
    );
    assert!(
        refresh.is_some_and(|r| r.outcome.runs.is_empty()),
        "the unrequested run is dropped from the record"
    );
}

#[tokio::test]
async fn a_refresh_value_that_is_not_a_probability_does_not_permit() {
    for new_p in [1.5_f32, -0.2, f32::NAN, f32::INFINITY] {
        let s = Scenario::finish_ready()
            .set(READY, 0.79)
            .selecting(READY, &["audit"]);
        let source = RecordedOpinionSource::new().with_refresh(
            CYCLE,
            READY,
            refreshed(Some(new_p), "audit", 0),
        );
        let outcome = s.run_plan(&source).await.unwrap_or_else(|e| panic!("{e}"));
        assert!(!outcome.checks[&k(READY)].permitted, "{new_p}");
        assert_eq!(
            record(&outcome, READY)
                .and_then(|r| r.refresh.clone())
                .and_then(|r| r.rejected),
            Some(super::RefreshRejection::NoMeasurement),
            "{new_p}"
        );
    }
}

#[tokio::test]
async fn refresh_records_are_bounded() {
    // Review finding 9: everything that reaches an event is capped.
    let s = Scenario::finish_ready()
        .set(READY, 0.79)
        .selecting(READY, &["audit"]);
    // A hundred runs of the requested provider, and one of a provider with a huge name
    // that was not requested.
    let mut runs: Vec<ProviderRun> = (0..100)
        .map(|_| ProviderRun {
            provider: EvidenceId::new("audit"),
            exit_code: Some(0),
        })
        .collect();
    runs.push(ProviderRun {
        provider: EvidenceId::new("p".repeat(10_000)),
        exit_code: Some(0),
    });
    let source = RecordedOpinionSource::new().with_refresh(
        CYCLE,
        READY,
        RefreshOutcome {
            runs,
            new_p: Some(0.96),
            error: Some("e".repeat(100_000)),
            state_digest: None,
        },
    );
    let outcome = s.run_plan(&source).await.unwrap_or_else(|e| panic!("{e}"));
    let refresh = record(&outcome, READY).and_then(|r| r.refresh.clone());
    let refresh = refresh.as_ref();
    assert_eq!(
        refresh.map(|r| r.outcome.runs.len()),
        Some(1),
        "one run per provider"
    );
    assert!(refresh.is_some_and(|r| {
        r.outcome
            .runs
            .iter()
            .all(|run| run.provider.as_str().len() <= 256)
    }));
    assert!(refresh.is_some_and(|r| r.outcome.error.as_ref().is_some_and(|e| e.len() <= 2048)));
    match &outcome.checks[&k(READY)].resolution {
        Resolution::EvidenceRefreshed { provider, .. } => assert!(provider.as_str().len() <= 256),
        other => panic!("{other:?}"),
    }
}

// ---- bounds ----------------------------------------------------------------

#[tokio::test]
async fn repeated_refresh_is_capped_and_visible() {
    let mut s = Scenario::finish_ready()
        .set(TESTS, 0.71)
        .selecting(TESTS, &["tests"]);
    let mut source = RecordedOpinionSource::new();
    for cycle in 20..30 {
        source = source.with_refresh(
            AssessmentSeq(cycle),
            TESTS,
            refreshed(Some(0.70), "tests", 0),
        );
    }
    let mut refreshes = 0;
    for cycle in 20..26 {
        s.cycle = AssessmentSeq(cycle);
        let outcome = s.run_plan(&source).await.unwrap_or_else(|e| panic!("{e}"));
        s.ensemble = outcome.counters.clone();
        let record = record(&outcome, TESTS);
        let refreshed_now = record.is_some_and(|r| r.refresh.is_some());
        if refreshed_now {
            refreshes += 1;
        }
        let capped = record.is_some_and(|r| {
            r.limits
                .contains(&EnsembleLimit::RefreshCap { used: 3, cap: 3 })
        });
        assert_eq!(refreshed_now, cycle < 23, "cycle {cycle}");
        assert_eq!(capped, cycle >= 23, "cycle {cycle}: the cap is visible");
        assert!(!outcome.checks[&k(TESTS)].permitted);
    }
    assert_eq!(refreshes, 3);
    assert_eq!(source.refresh_calls().len(), 3);
    assert_eq!(s.ensemble.check(&k(TESTS)).refreshes, 3);
}

/// Cycle A of review probe 3: `needs_verification` decisively high selects `Verify`; the
/// opinion bought on it fails, so it abstains.
fn verify_cycle() -> Scenario {
    let mut a = Scenario::finish_ready()
        .set(NEEDS_VER, 0.90)
        .reframable(&[NEEDS_VER]);
    a.cycle = AssessmentSeq(20);
    a
}

/// Cycle B of probe 3: the verifier ran without completing, and `needs_verification` is
/// now decisively low, which satisfies the `Finish` disjunct. `Finish` rests on it.
fn finish_after_verify_cycle() -> Scenario {
    let mut b = Scenario::finish_ready()
        .set(NEEDS_VER, 0.05)
        .reframable(&[NEEDS_VER]);
    b.counters.verifiers_run = 1;
    b.cycle = AssessmentSeq(21);
    b
}

/// Resolve one scenario against one source, panicking on a replay divergence.
async fn resolved(s: &Scenario, source: &RecordedOpinionSource) -> EnsembleOutcome {
    s.run_plan(source).await.unwrap_or_else(|e| panic!("{e}"))
}

/// Cycle A answered with `p(not X) = q`, then cycle B's scenario with A's counters.
async fn after_verify(q: Option<f32>) -> (EnsembleOutcome, Scenario) {
    let a = verify_cycle();
    assert_eq!(a.select(&a.baseline()).name(), "Verify");
    let source =
        RecordedOpinionSource::new().with_reframe(reframe_key(a.cycle, NEEDS_VER), answer(q));
    let out_a = resolved(&a, &source).await;
    assert_eq!(source.reframe_calls().len(), 1);
    let mut b = finish_after_verify_cycle();
    assert_eq!(b.select(&b.baseline()).name(), "Finish");
    assert!(
        b.triggers(NEEDS_VER)
            .contains(&EnsembleTrigger::Irreversible)
    );
    b.ensemble = out_a.counters.clone();
    (out_a, b)
}

#[tokio::test]
async fn a_cooldown_after_an_abstain_does_not_skip_the_irreversible_guard() {
    // Review probe 3: cycle A's opinion fails and abstains. Cycle B's Finish rests on the
    // same check. The cooldown must not let it through: the irreversible opinion is
    // bought afresh, and when that fails too it contests.
    let (out_a, b) = after_verify(None).await;
    assert!(
        record(&out_a, NEEDS_VER)
            .is_some_and(|r| r.opinions[0].opinion.verdict == Verdict::Abstain)
    );
    let source =
        RecordedOpinionSource::new().with_reframe(reframe_key(b.cycle, NEEDS_VER), answer(None));
    let out_b = resolved(&b, &source).await;
    assert_eq!(
        source.reframe_calls().len(),
        1,
        "a fresh irreversible opinion is bought"
    );
    assert!(!out_b.checks[&k(NEEDS_VER)].permitted);
    assert_ne!(b.select(&out_b.checks).name(), "Finish");
}

#[tokio::test]
async fn a_verify_confirm_does_not_delay_the_next_finish() {
    // Cycle A confirms the value for a Verify. That confirm does not cover the Finish in
    // cycle B, but the cooldown does not hold B either: B buys its own irreversible
    // opinion, and finishes on a confirm.
    let (out_a, b) = after_verify(Some(0.10)).await;
    assert!(
        record(&out_a, NEEDS_VER).is_some_and(
            |r| r.opinions[0].opinion.verdict == Verdict::Confirm { side: Side::Above }
        )
    );
    // p = 0.05, p(not X) = 0.95: consistent.
    let source = RecordedOpinionSource::new()
        .with_reframe(reframe_key(b.cycle, NEEDS_VER), answer(Some(0.95)));
    let out_b = resolved(&b, &source).await;
    assert_eq!(source.reframe_calls().len(), 1);
    assert_eq!(
        b.select(&out_b.checks).name(),
        "Finish",
        "finished on its own confirm"
    );
    assert!(record(&out_b, NEEDS_VER).is_some_and(|r| {
        !r.limits
            .iter()
            .any(|l| matches!(l, EnsembleLimit::Cooldown { .. }))
    }));

    // And the same cycle with a contesting answer does not finish.
    let source = RecordedOpinionSource::new()
        .with_reframe(reframe_key(b.cycle, NEEDS_VER), answer(Some(0.05)));
    let out_b = resolved(&b, &source).await;
    assert_ne!(b.select(&out_b.checks).name(), "Finish");
}

#[tokio::test]
async fn a_spent_budget_holds_an_uncovered_irreversible_opinion() {
    let (_, mut b) = after_verify(Some(0.10)).await;
    b.ensemble.opinions_used = b.settings.max_opinions_per_run;
    let source = RecordedOpinionSource::new();
    let out_b = resolved(&b, &source).await;
    assert!(source.reframe_calls().is_empty());
    let rc = &out_b.checks[&k(NEEDS_VER)];
    assert!(!rc.permitted);
    assert_eq!(rc.suppression, Some(CheckSuppression::Unresolved));
    assert!(record(&out_b, NEEDS_VER).is_some_and(|r| {
        r.limits
            .contains(&EnsembleLimit::OpinionBudget { used: 20, cap: 20 })
    }));
    assert_ne!(b.select(&out_b.checks).name(), "Finish");
}

#[tokio::test]
async fn a_confirm_does_not_cover_the_before_finish_reassessment() {
    // Round-2 probe N4. Cycle A: Finish selected, R2 confirms `tests_sufficient = 0.92`.
    // The runtime runs the `before_finish` providers and re-assesses: cycle A+1, Jev says
    // 0.91 on the fresh evidence and Finish is selected again. That Finish must rest on an
    // opinion about 0.91, bought in A+1, not on A's confirm about 0.92.
    let mut a = Scenario::finish_ready()
        .set(TESTS, 0.92)
        .reframable(&[TESTS]);
    a.cycle = AssessmentSeq(20);
    let confirming =
        RecordedOpinionSource::new().with_reframe(reframe_key(a.cycle, TESTS), answer(Some(0.08)));
    let out_a = resolved(&a, &confirming).await;
    assert_eq!(a.select(&out_a.checks).name(), "Finish");

    let mut b = Scenario::finish_ready()
        .set(TESTS, 0.91)
        .reframable(&[TESTS]);
    b.cycle = AssessmentSeq(21);
    b.ensemble = out_a.counters.clone();
    // On the fresh value R2 contests: p(not X) = 0.91.
    let source =
        RecordedOpinionSource::new().with_reframe(reframe_key(b.cycle, TESTS), answer(Some(0.91)));
    let out_b = resolved(&b, &source).await;
    assert_eq!(
        source.reframe_calls().len(),
        1,
        "a fresh irreversible opinion is bought"
    );
    assert_ne!(b.select(&out_b.checks).name(), "Finish");
}

#[tokio::test]
async fn a_covering_confirm_stands_only_for_a_non_irreversible_trigger() {
    // A confirm for a Verify covers the next Verify on the same side.
    let a = verify_cycle();
    let confirming = RecordedOpinionSource::new()
        .with_reframe(reframe_key(a.cycle, NEEDS_VER), answer(Some(0.10)));
    let out_a = resolved(&a, &confirming).await;
    let mut a2 = verify_cycle();
    a2.cycle = AssessmentSeq(21);
    a2.ensemble = out_a.counters.clone();
    assert!(
        !a2.triggers(NEEDS_VER)
            .contains(&EnsembleTrigger::Irreversible)
    );
    let source = RecordedOpinionSource::new();
    let out_a2 = resolved(&a2, &source).await;
    assert!(source.reframe_calls().is_empty());
    assert!(
        out_a2.checks[&k(NEEDS_VER)].permitted,
        "covered: the confirm stands"
    );
    assert!(record(&out_a2, NEEDS_VER).is_some_and(|r| {
        r.limits
            .iter()
            .any(|l| matches!(l, EnsembleLimit::Cooldown { held: false, .. }))
    }));

    // An irreversible confirm does not cover the next irreversible cycle: the next Finish
    // buys its own opinion, and finishes on it.
    let mut b1 = finish_after_verify_cycle();
    b1.cycle = AssessmentSeq(20);
    let confirming = RecordedOpinionSource::new()
        .with_reframe(reframe_key(b1.cycle, NEEDS_VER), answer(Some(0.95)));
    let out_b1 = resolved(&b1, &confirming).await;
    assert_eq!(b1.select(&out_b1.checks).name(), "Finish");
    let mut b2 = finish_after_verify_cycle();
    b2.ensemble = out_b1.counters.clone();
    let source = RecordedOpinionSource::new()
        .with_reframe(reframe_key(b2.cycle, NEEDS_VER), answer(Some(0.95)));
    let out_b2 = resolved(&b2, &source).await;
    assert_eq!(source.reframe_calls().len(), 1);
    assert_eq!(
        b2.select(&out_b2.checks).name(),
        "Finish",
        "on its own confirm"
    );
}

#[tokio::test]
async fn the_opinion_budget_holds_rather_than_acting_unchecked() {
    let mut s = Scenario::finish_ready()
        .set(TESTS, 0.92)
        .reframable(&[TESTS]);
    s.ensemble.opinions_used = s.settings.max_opinions_per_run;
    let source = RecordedOpinionSource::new();
    let outcome = s.run_plan(&source).await.unwrap_or_else(|e| panic!("{e}"));
    let rc = &outcome.checks[&k(TESTS)];
    assert!(!rc.permitted);
    assert_eq!(rc.suppression, Some(CheckSuppression::Unresolved));
    assert!(record(&outcome, TESTS).is_some_and(|r| {
        r.limits
            .contains(&EnsembleLimit::OpinionBudget { used: 20, cap: 20 })
    }));
    assert!(source.reframe_calls().is_empty());
    assert_ne!(s.select(&outcome.checks).name(), "Finish");
}

#[tokio::test]
async fn the_cooldown_rows_from_the_contract() {
    // Written from the contract, not from the resolver. Inside the window an earlier
    // opinion covers this one only if it was a confirm about the same side, and never if
    // this one is irreversible.
    //
    // Irreversible (`tests_sufficient = 0.92`, Finish pending): never covered; a fresh
    // opinion is bought every time (and here it confirms).
    //
    // Not irreversible (`needs_verification = 0.90`, Verify pending): covered ⇒ it stands;
    // not covered ⇒ held for the window. No call either way.
    let marks = [
        (MarkVerdict::Contest, Some(Side::Above), true),
        (MarkVerdict::Abstain, Some(Side::Above), true),
        (MarkVerdict::Confirm, Some(Side::Above), true),
        (MarkVerdict::Confirm, Some(Side::Above), false),
        (MarkVerdict::Confirm, Some(Side::Below), true),
        (MarkVerdict::Confirm, None, true),
    ];
    // (key, value, p(not X) that confirms, expected cover per mark row)
    let cases = [
        // Nothing covers an irreversible opinion.
        (TESTS, 0.92_f32, 0.08_f32, true, [false; 6]),
        (
            NEEDS_VER,
            0.90,
            0.10,
            false,
            [false, false, true, true, false, false],
        ),
    ];
    for (key, p, confirms, irreversible_now, covers) in cases {
        for ((verdict, side, irreversible), covered) in marks.iter().copied().zip(covers) {
            let mut s = Scenario::finish_ready().set(key, p).reframable(&[key]);
            assert_eq!(
                s.triggers(key).contains(&EnsembleTrigger::Irreversible),
                irreversible_now
            );
            s.ensemble.checks.insert(
                k(key),
                CheckCounters {
                    last_opinion: Some(OpinionMark {
                        cycle: AssessmentSeq(CYCLE.get() - 3),
                        resolution: AssessmentSeq(CYCLE.get() - 3).get(),
                        verdict,
                        side,
                        irreversible,
                    }),
                    ..CheckCounters::default()
                },
            );
            let source = RecordedOpinionSource::new()
                .with_reframe(reframe_key(CYCLE, key), answer(Some(confirms)));
            let outcome = resolved(&s, &source).await;
            let row = format!("{key} {verdict:?} {side:?} irreversible={irreversible}");
            let bought = irreversible_now && !covered;
            assert_eq!(
                source.reframe_calls().len(),
                usize::from(bought),
                "calls: {row}"
            );
            let permitted = covered || bought;
            assert_eq!(
                outcome.checks[&k(key)].permitted,
                permitted,
                "permitted: {row}"
            );
            let cooldown = record(&outcome, key).and_then(|r| {
                r.limits.iter().find_map(|l| match l {
                    EnsembleLimit::Cooldown { held, .. } => Some(*held),
                    _ => None,
                })
            });
            let expected_cooldown = if bought { None } else { Some(!covered) };
            assert_eq!(cooldown, expected_cooldown, "cooldown limit: {row}");
        }
    }

    // Once the window has passed, it is asked again.
    let mut s = Scenario::finish_ready()
        .set(NEEDS_VER, 0.90)
        .reframable(&[NEEDS_VER]);
    s.ensemble.checks.insert(
        k(NEEDS_VER),
        CheckCounters {
            last_opinion: Some(OpinionMark {
                cycle: AssessmentSeq(CYCLE.get() - 10),
                resolution: AssessmentSeq(CYCLE.get() - 10).get(),
                verdict: MarkVerdict::Contest,
                side: Some(Side::Above),
                irreversible: false,
            }),
            ..CheckCounters::default()
        },
    );
    let source = RecordedOpinionSource::new()
        .with_reframe(reframe_key(CYCLE, NEEDS_VER), answer(Some(0.10)));
    let outcome = resolved(&s, &source).await;
    assert_eq!(source.reframe_calls().len(), 1);
    assert!(
        outcome.checks[&k(NEEDS_VER)].permitted,
        "consistent: confirmed"
    );
    assert_eq!(
        outcome.counters.check(&k(NEEDS_VER)).last_opinion,
        Some(OpinionMark {
            cycle: CYCLE,
            resolution: CYCLE.get(),
            verdict: MarkVerdict::Confirm,
            side: Some(Side::Above),
            irreversible: false,
        }),
        "the new mark records what the confirm established"
    );
}

#[tokio::test]
async fn repeat_contest_does_not_latch_from_in_band_holds() {
    // Review probe 2: two in-band cycles, then a decisive value. In-band holds must not
    // escalate a later decisive value, with or without a person to escalate to.
    for human in [true, false] {
        let mut counters = EnsembleCounters::default();
        for (i, ready) in [0.79_f32, 0.79, 0.97, 0.97, 0.97].into_iter().enumerate() {
            let mut s = Scenario::finish_ready().set(READY, ready);
            if !human {
                s = s.without_rung(Rung::Human);
            }
            s.cycle = AssessmentSeq(20 + i as u64);
            s.ensemble = counters.clone();
            let outcome = s
                .run_plan(&RecordedOpinionSource::new())
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            if ready > 0.9 {
                assert_eq!(outcome.escalations().count(), 0, "human={human} cycle {i}");
                assert!(
                    outcome.checks[&k(READY)].permitted,
                    "human={human} cycle {i}"
                );
                assert_eq!(
                    s.select(&outcome.checks).name(),
                    "Finish",
                    "human={human} cycle {i}"
                );
            }
            counters = outcome.counters;
        }
    }
}

/// Run `tests_sufficient` through a sequence of cycles, each with the given value and
/// R2 answer (`None` means R2 is not asked about anything that cycle), carrying counters.
/// Returns, per cycle: (escalated, R2 calls, permitted, counters after).
async fn tests_sequence(
    steps: &[(f32, Option<f32>)],
    cooldown: u64,
) -> Vec<(bool, usize, bool, CheckCounters)> {
    let mut counters = EnsembleCounters::default();
    let mut out = Vec::new();
    for (i, (p, q)) in steps.iter().enumerate() {
        let mut s = Scenario::finish_ready().set(TESTS, *p).reframable(&[TESTS]);
        s.settings.per_check_cooldown_cycles = cooldown;
        s.cycle = AssessmentSeq(20 + i as u64);
        s.ensemble = counters.clone();
        let source =
            RecordedOpinionSource::new().with_reframe(reframe_key(s.cycle, TESTS), answer(*q));
        let outcome = s.run_plan(&source).await.unwrap_or_else(|e| panic!("{e}"));
        out.push((
            outcome.escalations().count() > 0,
            source.reframe_calls().len(),
            outcome.checks[&k(TESTS)].permitted,
            outcome.counters.check(&k(TESTS)),
        ));
        counters = outcome.counters;
    }
    out
}

#[tokio::test]
async fn repeat_contest_counts_consecutive_contests_only() {
    // p(not X) = 0.75 against p = 0.92 contests; 0.08 confirms.
    let contest = Some(0.75);
    let confirm = Some(0.08);
    // Two contests, then escalation instead of a third call; the escalation does not
    // feed its own counter.
    let run = tests_sequence(
        &[
            (0.92, contest),
            (0.92, contest),
            (0.92, contest),
            (0.92, contest),
        ],
        0,
    )
    .await;
    let escalated: Vec<bool> = run.iter().map(|r| r.0).collect();
    let calls: Vec<usize> = run.iter().map(|r| r.1).collect();
    let contests: Vec<u32> = run.iter().map(|r| r.3.consecutive_contests).collect();
    assert_eq!(escalated, [false, false, true, true]);
    assert_eq!(calls, [1, 1, 0, 0]);
    assert_eq!(
        contests,
        [1, 2, 2, 2],
        "an escalation does not feed its own counter"
    );
    assert!(
        run.iter().all(|r| !r.2),
        "contested or escalated: never permitted"
    );

    // A confirm in between resets the count.
    let run = tests_sequence(
        &[
            (0.92, contest),
            (0.92, confirm),
            (0.92, contest),
            (0.92, contest),
        ],
        0,
    )
    .await;
    let contests: Vec<u32> = run.iter().map(|r| r.3.consecutive_contests).collect();
    assert_eq!(contests, [1, 0, 1, 2]);
    assert!(
        run.iter().all(|r| !r.0),
        "never two consecutive contests before the last"
    );

    // So does a cycle in band.
    let run = tests_sequence(
        &[
            (0.92, contest),
            (0.71, None),
            (0.92, contest),
            (0.92, contest),
        ],
        0,
    )
    .await;
    let contests: Vec<u32> = run.iter().map(|r| r.3.consecutive_contests).collect();
    assert_eq!(contests, [1, 0, 1, 2]);
    assert!(run.iter().all(|r| !r.0));
}

#[tokio::test]
async fn consecutive_in_band_cycles_still_escalate() {
    let run = tests_sequence(
        &[(0.71, None), (0.71, None), (0.71, None), (0.92, Some(0.08))],
        0,
    )
    .await;
    let escalated: Vec<bool> = run.iter().map(|r| r.0).collect();
    let in_band: Vec<u32> = run.iter().map(|r| r.3.consecutive_in_band).collect();
    assert_eq!(escalated, [false, false, true, false]);
    assert_eq!(in_band, [1, 2, 3, 0]);
    assert!(
        run[3].2,
        "decisive and confirmed afterwards: permitted, history notwithstanding"
    );
}

#[tokio::test]
async fn a_repeated_contest_escalates_instead_of_asking_again() {
    let s = Scenario::finish_ready()
        .set(TESTS, 0.92)
        .reframable(&[TESTS])
        .contested_for(TESTS, 2);
    let source = RecordedOpinionSource::new();
    let outcome = s.run_plan(&source).await.unwrap_or_else(|e| panic!("{e}"));
    assert!(source.reframe_calls().is_empty());
    let rc = &outcome.checks[&k(TESTS)];
    assert!(!rc.permitted);
    assert_eq!(outcome.escalations().count(), 1);
    assert_ne!(s.select(&outcome.checks).name(), "Finish");

    // Without a human rung there is nobody to escalate to: the opinion is asked.
    let s = s.without_rung(Rung::Human);
    let source =
        RecordedOpinionSource::new().with_reframe(reframe_key(CYCLE, TESTS), answer(Some(0.08)));
    let outcome = s.run_plan(&source).await.unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(source.reframe_calls().len(), 1);
    assert_eq!(outcome.escalations().count(), 0);
}

#[tokio::test]
async fn an_unusable_opinion_on_an_irreversible_action_holds_it() {
    let s = Scenario::finish_ready()
        .set(TESTS, 0.92)
        .reframable(&[TESTS]);
    // The call fails: recorded as an answer with no value.
    let source = RecordedOpinionSource::new().with_reframe(reframe_key(CYCLE, TESTS), answer(None));
    let outcome = s.run_plan(&source).await.unwrap_or_else(|e| panic!("{e}"));
    let rc = &outcome.checks[&k(TESTS)];
    assert!(rc.contested && !rc.permitted);
    assert_ne!(s.select(&outcome.checks).name(), "Finish");
}

#[tokio::test]
async fn opinion_text_is_bounded() {
    let s = Scenario::finish_ready()
        .set(TESTS, 0.92)
        .reframable(&[TESTS]);
    let mut long = answer(Some(0.08));
    long.excerpt = "é".repeat(5000);
    long.model = "m".repeat(5000);
    long.error = Some("x".repeat(9000));
    let source = RecordedOpinionSource::new().with_reframe(reframe_key(CYCLE, TESTS), long);
    let outcome = s.run_plan(&source).await.unwrap_or_else(|e| panic!("{e}"));
    let opinion = &record(&outcome, TESTS).map(|r| r.opinions[0].opinion.clone());
    let opinion = opinion.as_ref();
    assert!(opinion.is_some_and(|o| o.excerpt.len() <= 2048 && o.excerpt.len() > 2000));
    assert!(opinion.is_some_and(|o| o.model.len() <= 256));
    assert!(opinion.is_some_and(|o| o.error.as_ref().is_some_and(|e| e.len() <= 2048)));
}

// ---- the exhaustive table --------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq)]
enum Kind {
    Decisive,
    Refreshed,
    InBand,
    Unresolved,
    Escalated,
    Contested,
}

fn kind_of(rc: &ResolvedCheck) -> Kind {
    match (&rc.resolution, rc.suppression) {
        (Resolution::EvidenceRefreshed { .. }, _) => Kind::Refreshed,
        (Resolution::SuppressedByContest { .. }, _) => Kind::Contested,
        (Resolution::UnresolvedInBand { rungs, .. }, _) if rungs.contains(&Rung::Human) => {
            Kind::Escalated
        }
        (Resolution::UnresolvedInBand { .. }, Some(CheckSuppression::Unresolved)) => {
            Kind::Unresolved
        }
        (Resolution::UnresolvedInBand { .. }, _) => Kind::InBand,
        (Resolution::Decisive, _) => Kind::Decisive,
        (Resolution::HumanDecided { .. }, _) => Kind::Unresolved,
    }
}

#[derive(Debug, Clone, Copy)]
enum CounterState {
    Fresh,
    RefreshCapped,
    CooldownAfterContest,
    CooldownAfterConfirm,
    BudgetSpent,
}

#[derive(Debug, Clone, Copy)]
enum RefreshAnswer {
    Decisive,
    InBand,
    Failed,
}

#[derive(Debug, Clone, Copy)]
enum ReframeKind {
    /// `1 - p(not X)` equals `p`.
    Consistent,
    /// `1 - p(not X)` is `0.14` from `p`: inside the 0.15 tolerance.
    NearlyInconsistent,
    /// `1 - p(not X)` is `0.16` from `p`: just outside it.
    JustInconsistent,
    Inconsistent,
    Unusable,
}

struct Case {
    p: f32,
    triggers: BTreeSet<EnsembleTrigger>,
    rule_would_fire: bool,
    providers: bool,
    rungs: Vec<Rung>,
    reframe_available: bool,
    counters: CounterState,
    refresh: RefreshAnswer,
    reframe: ReframeKind,
}

struct Expected {
    kind: Kind,
    permitted: bool,
    refreshed: bool,
    reframed: bool,
    /// The verdict the new opinion mark records, when an opinion was bought.
    mark: Option<MarkVerdict>,
}

const T: f32 = 0.75;
const M: f32 = 0.15;

/// The ladder of §6, written out as a decision table, independently of the resolver.
fn oracle(c: &Case) -> Expected {
    let has = |t: EnsembleTrigger| c.triggers.contains(&t);
    let rung = |r: Rung| c.rungs.contains(&r);
    let refresh_possible = rung(Rung::RefreshEvidence)
        && c.providers
        && !matches!(c.counters, CounterState::RefreshCapped);
    let refreshed_value = match c.refresh {
        RefreshAnswer::Decisive => Some(0.05_f32),
        RefreshAnswer::InBand => Some(0.70),
        RefreshAnswer::Failed => None,
    };
    let after_refresh = |e: Expected| -> Expected {
        match refreshed_value {
            Some(p) => Expected {
                kind: Kind::Refreshed,
                permitted: (p - T).abs() >= M,
                refreshed: true,
                reframed: false,
                mark: None,
            },
            None => Expected {
                refreshed: true,
                ..e
            },
        }
    };
    let done = |kind, permitted| Expected {
        kind,
        permitted,
        refreshed: false,
        reframed: false,
        mark: None,
    };
    let decisive = (c.p - T).abs() >= M;

    if !decisive {
        let fallback = if has(EnsembleTrigger::RepeatContest)
            && has(EnsembleTrigger::HighStakes)
            && rung(Rung::Human)
        {
            done(Kind::Escalated, false)
        } else {
            done(Kind::InBand, false)
        };
        return if refresh_possible {
            after_refresh(fallback)
        } else {
            fallback
        };
    }
    if !c.rule_would_fire {
        return done(Kind::Decisive, true);
    }
    let mut refreshed = false;
    if has(EnsembleTrigger::ClaimOnlyEvidence) && refresh_possible {
        if refreshed_value.is_some() {
            return after_refresh(done(Kind::Decisive, true));
        }
        refreshed = true;
    }
    let with = |e: Expected| Expected { refreshed, ..e };
    let wants = has(EnsembleTrigger::OutcomeRelevant)
        || has(EnsembleTrigger::HighStakes)
        || has(EnsembleTrigger::Irreversible);
    if !wants {
        return with(done(Kind::Decisive, true));
    }
    // No opinion to buy: the decisive value stands, whatever its history.
    if !(rung(Rung::ReframeSameModel) && c.reframe_available) {
        return with(done(Kind::Decisive, true));
    }
    if has(EnsembleTrigger::RepeatContest) && rung(Rung::Human) {
        return with(done(Kind::Escalated, false));
    }
    match c.counters {
        // An irreversible opinion is never held by the cooldown: it is bought afresh.
        CounterState::CooldownAfterContest if !has(EnsembleTrigger::Irreversible) => {
            return with(done(Kind::Unresolved, false));
        }
        CounterState::CooldownAfterConfirm if !has(EnsembleTrigger::Irreversible) => {
            return with(done(Kind::Decisive, true));
        }
        // A spent budget holds; with a person to ask, it says so (one escalation).
        CounterState::BudgetSpent if rung(Rung::Human) => {
            return with(done(Kind::Escalated, false));
        }
        CounterState::BudgetSpent => return with(done(Kind::Unresolved, false)),
        CounterState::CooldownAfterContest
        | CounterState::CooldownAfterConfirm
        | CounterState::Fresh
        | CounterState::RefreshCapped => {}
    }
    let (contest, mark) = match c.reframe {
        ReframeKind::Consistent | ReframeKind::NearlyInconsistent => (false, MarkVerdict::Confirm),
        ReframeKind::Inconsistent | ReframeKind::JustInconsistent => (true, MarkVerdict::Contest),
        ReframeKind::Unusable if has(EnsembleTrigger::Irreversible) => (true, MarkVerdict::Contest),
        ReframeKind::Unusable => (false, MarkVerdict::Abstain),
    };
    Expected {
        kind: if contest {
            Kind::Contested
        } else {
            Kind::Decisive
        },
        permitted: !contest,
        refreshed,
        reframed: true,
        mark: Some(mark),
    }
}

fn trigger_sets() -> Vec<BTreeSet<EnsembleTrigger>> {
    use EnsembleTrigger as E;
    let all = [
        E::NearThreshold,
        E::LowConfidence,
        E::ClaimOnlyEvidence,
        E::OutcomeRelevant,
        E::HighStakes,
        E::Irreversible,
        E::RepeatContest,
    ];
    let mut sets: Vec<BTreeSet<E>> = all.iter().map(|t| BTreeSet::from([*t])).collect();
    sets.extend([
        BTreeSet::from([E::RepeatContest, E::HighStakes]),
        BTreeSet::from([E::ClaimOnlyEvidence, E::Irreversible]),
        BTreeSet::from([E::ClaimOnlyEvidence, E::OutcomeRelevant, E::RepeatContest]),
        BTreeSet::from([E::NearThreshold, E::OutcomeRelevant, E::HighStakes]),
        BTreeSet::from(all),
    ]);
    sets
}

fn rung_sets() -> Vec<Vec<Rung>> {
    let rungs = EnsembleSettings::available_rungs();
    (0..(1_u32 << rungs.len()))
        .map(|mask| {
            rungs
                .iter()
                .enumerate()
                .filter(|(i, _)| mask & (1 << i) != 0)
                .map(|(_, r)| *r)
                .collect()
        })
        .collect()
}

#[tokio::test]
async fn resolve_matches_the_decision_table_exhaustively() {
    let mut cases = 0_u32;
    for p in [0.71_f32, 0.92, 0.30] {
        // A worker is running and cannot be steered, and nothing proposes anything that
        // rests on `tests_sufficient`: the final selection is never irreversible, so the
        // gate stays out of the way and this table tests the per-check ladder alone. Built
        // once per value: loading the definitions is the expensive part.
        let mut s = Scenario::finish_ready().with_worker().set(TESTS, p);
        s.cycle = CYCLE;
        for triggers in trigger_sets() {
            for rule_would_fire in [false, true] {
                for providers in [false, true] {
                    for rungs in rung_sets() {
                        for reframe_available in [false, true] {
                            for counters in [
                                CounterState::Fresh,
                                CounterState::RefreshCapped,
                                CounterState::CooldownAfterContest,
                                CounterState::CooldownAfterConfirm,
                                CounterState::BudgetSpent,
                            ] {
                                for refresh in [
                                    RefreshAnswer::Decisive,
                                    RefreshAnswer::InBand,
                                    RefreshAnswer::Failed,
                                ] {
                                    for reframe in [
                                        ReframeKind::Consistent,
                                        ReframeKind::NearlyInconsistent,
                                        ReframeKind::JustInconsistent,
                                        ReframeKind::Inconsistent,
                                        ReframeKind::Unusable,
                                    ] {
                                        let case = Case {
                                            p,
                                            triggers: triggers.clone(),
                                            rule_would_fire,
                                            providers,
                                            rungs: rungs.clone(),
                                            reframe_available,
                                            counters,
                                            refresh,
                                            reframe,
                                        };
                                        run_case(&case, &mut s).await;
                                        cases += 1;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    assert!(cases > 100_000, "{cases} cases");
}

async fn run_case(c: &Case, s: &mut Scenario) {
    let key = k(TESTS);
    let baseline = ResolvedCheck::from_measurement(key.clone(), Some(c.p), Some(T), M, true);
    let mut check = CheckCounters::default();
    let mut counters = EnsembleCounters {
        resolutions: CYCLE.get() - 1,
        ..EnsembleCounters::default()
    };
    match c.counters {
        CounterState::Fresh => {}
        CounterState::RefreshCapped => check.refreshes = 3,
        CounterState::CooldownAfterContest | CounterState::CooldownAfterConfirm => {
            check.last_opinion = Some(OpinionMark {
                cycle: AssessmentSeq(CYCLE.get() - 1),
                resolution: AssessmentSeq(CYCLE.get() - 1).get(),
                verdict: if matches!(c.counters, CounterState::CooldownAfterContest) {
                    MarkVerdict::Contest
                } else {
                    MarkVerdict::Confirm
                },
                side: baseline.side,
                irreversible: true,
            });
        }
        CounterState::BudgetSpent => counters.opinions_used = 20,
    }
    counters.checks.insert(key.clone(), check);
    s.settings = EnsembleSettings {
        enabled: true,
        rungs: c.rungs.clone(),
        ..EnsembleSettings::default()
    };
    let plan = EnsemblePlan::scripted(
        CYCLE,
        s.settings.clone(),
        counters,
        s.baseline(),
        vec![CheckPlan::scripted(
            key.clone(),
            c.p,
            T,
            M,
            c.triggers.clone(),
            c.rule_would_fire,
            if c.providers {
                vec![EvidenceId::new("tests")]
            } else {
                Vec::new()
            },
            c.reframe_available,
        )],
        s.fingerprint(),
        s.reframe.clone(),
    );
    let new_p = match c.refresh {
        RefreshAnswer::Decisive => Some(0.05),
        RefreshAnswer::InBand => Some(0.70),
        RefreshAnswer::Failed => None,
    };
    let p_negated = match c.reframe {
        // 1 - p(not X) equals p exactly: consistent.
        ReframeKind::Consistent => Some(1.0 - c.p),
        ReframeKind::NearlyInconsistent => Some(1.0 - c.p + 0.14),
        ReframeKind::JustInconsistent => Some(1.0 - c.p + 0.16),
        ReframeKind::Inconsistent => Some(c.p),
        ReframeKind::Unusable => None,
    };
    let source = RecordedOpinionSource::new()
        .with_refresh(CYCLE, TESTS, refreshed(new_p, "tests", 0))
        .with_reframe(reframe_key(CYCLE, TESTS), answer(p_negated));
    let outcome = s
        .resolve_plan(&plan, &source)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let rc = &outcome.checks[&key];
    let expected = oracle(c);
    let label = format!(
        "p={} triggers={:?} fire={} providers={} rungs={:?} reframe={} counters={:?} refresh={:?} reframe={:?}",
        c.p,
        c.triggers,
        c.rule_would_fire,
        c.providers,
        c.rungs,
        c.reframe_available,
        c.counters,
        c.refresh,
        c.reframe
    );
    assert_eq!(kind_of(rc), expected.kind, "{label}");
    assert_eq!(rc.permitted, expected.permitted, "{label}");
    assert_eq!(
        source.refresh_calls().len(),
        usize::from(expected.refreshed),
        "{label}"
    );
    assert_eq!(
        source.reframe_calls().len(),
        usize::from(expected.reframed),
        "{label}"
    );

    // The counters, row by row.
    let base_opinions = if matches!(c.counters, CounterState::BudgetSpent) {
        20
    } else {
        0
    };
    let base_refreshes = if matches!(c.counters, CounterState::RefreshCapped) {
        3
    } else {
        0
    };
    let after = outcome.counters.check(&key);
    assert_eq!(
        outcome.counters.opinions_used,
        base_opinions + u32::from(expected.reframed),
        "opinions_used: {label}"
    );
    assert_eq!(
        after.refreshes,
        base_refreshes + u32::from(expected.refreshed),
        "refreshes: {label}"
    );
    match expected.mark {
        Some(verdict) => assert_eq!(
            after.last_opinion,
            Some(OpinionMark {
                cycle: CYCLE,
                resolution: CYCLE.get(),
                verdict,
                side: baseline.side,
                irreversible: c.triggers.contains(&EnsembleTrigger::Irreversible),
            }),
            "mark: {label}"
        ),
        None => assert_eq!(
            after.last_opinion, check.last_opinion,
            "mark unchanged: {label}"
        ),
    }
    assert_eq!(
        after.consecutive_in_band,
        u32::from(!rc.decisive),
        "in band: {label}"
    );
    assert_eq!(
        after.consecutive_contests,
        u32::from(expected.mark == Some(MarkVerdict::Contest)),
        "contests: {label}"
    );

    // Invariants that hold in every row.
    if !matches!(rc.resolution, Resolution::EvidenceRefreshed { .. }) {
        assert_eq!(rc.jev_p, Some(c.p), "no fabrication: {label}");
        assert!(
            !rc.permitted || baseline.permitted,
            "only a refresh may raise permission: {label}"
        );
    }
    if !baseline.decisive || !c.rule_would_fire {
        assert!(source.reframe_calls().is_empty(), "cost: {label}");
    }
}

// ---- properties ------------------------------------------------------------

const GRID: [f32; 11] = [
    0.05, 0.30, 0.55, 0.62, 0.70, 0.74, 0.75, 0.80, 0.88, 0.92, 0.99,
];
const NEGATED: [Option<f32>; 8] = [
    None,
    Some(0.0),
    Some(0.08),
    Some(0.25),
    Some(0.5),
    Some(0.75),
    Some(0.92),
    Some(1.0),
];

#[tokio::test]
async fn monotonic_conservatism_removing_r2_never_shrinks_the_permitted_set() {
    for worker in [false, true] {
        for &ready in &GRID {
            for &tests in &GRID {
                for &q in &NEGATED {
                    let mut s = Scenario::finish_ready()
                        .set(READY, ready)
                        .set(TESTS, tests)
                        .set(STUCK, tests)
                        .reframable_all();
                    if worker {
                        s = s.with_worker();
                    }
                    let plan = s.plan();
                    let with = s
                        .resolve_plan(&plan, &answering_all(&plan, q))
                        .await
                        .unwrap_or_else(|e| panic!("{e}"));
                    let s = s.without_rung(Rung::ReframeSameModel);
                    let without = s
                        .run_plan(&RecordedOpinionSource::new())
                        .await
                        .unwrap_or_else(|e| panic!("{e}"));
                    assert!(
                        permitted_set(&with.checks).is_subset(&permitted_set(&without.checks)),
                        "ready={ready} tests={tests} q={q:?} worker={worker}"
                    );
                    // And at the verdict level: every verdict replaced by Abstain.
                    for record in &with.records {
                        let abstained: Vec<Opinion> = record
                            .opinions
                            .iter()
                            .map(|keyed| Opinion {
                                verdict: Verdict::Abstain,
                                ..keyed.opinion.clone()
                            })
                            .collect();
                        let relaxed = apply_opinions(record.baseline.clone(), &abstained);
                        assert!(relaxed.permitted || !record.resolved.permitted);
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum CycleState {
    Verify,
    FinishAfterVerify,
    ReadyInBand,
    FinishReady,
    TestsLow,
    /// `ready_to_finish` in band; its `before_finish` refresh comes back decisive, which
    /// turns the cycle's selection into `Finish` after the ladder planned for `StartWorker`.
    RefreshUnmasksFinish,
    /// A worker that cannot be steered; `needs_human` and `worker_stuck` both decisively
    /// high. A contest on `needs_human` unmasks the `Stop` that rests on `worker_stuck`.
    ContestUnmasksStop,
    /// The cycle after a `before_finish` refresh: a fresh, slightly different value on
    /// which `Finish` is selected again.
    Reassessed,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Answers {
    Fail,
    Consistent,
    Inconsistent,
}

fn cycle_scenario(state: CycleState, cycle: u64, counters: &EnsembleCounters) -> Scenario {
    let mut s = Scenario::finish_ready().reframable_all();
    match state {
        CycleState::Verify => s = s.set(NEEDS_VER, 0.90),
        CycleState::FinishAfterVerify => {
            s = s.set(NEEDS_VER, 0.05);
            s.counters.verifiers_run = 1;
        }
        CycleState::ReadyInBand => s = s.set(READY, 0.79),
        CycleState::FinishReady => {}
        CycleState::TestsLow => s = s.set(TESTS, 0.30),
        CycleState::RefreshUnmasksFinish => s = s.set(READY, 0.79).selecting(READY, &["audit"]),
        CycleState::ContestUnmasksStop => {
            s = s.with_worker().set(HUMAN, 0.97).set(STUCK, 0.97);
        }
        CycleState::Reassessed => s = s.set(TESTS, 0.91),
    }
    s.cycle = AssessmentSeq(cycle);
    s.ensemble = counters.clone();
    s
}

/// The source for one cycle: every opinion it could ask answered by `answers`, and the
/// refresh of the refresh state answered with a decisive value.
fn cycle_source(state: CycleState, plan: &EnsemblePlan, answers: Answers) -> RecordedOpinionSource {
    let source = match answers {
        Answers::Fail => answering_by(plan, |_| None),
        Answers::Consistent => answering_by(plan, |p| Some(1.0 - p)),
        Answers::Inconsistent => answering_by(plan, Some),
    };
    match state {
        CycleState::RefreshUnmasksFinish => {
            source.with_refresh(plan.cycle(), READY, refreshed(Some(0.95), "audit", 0))
        }
        _ => source,
    }
}

/// The checks an irreversible selection rests on, found by brute force: every decisive
/// value on each side of the threshold is tried through `from_measurement`, against the
/// final check set. Independent of the gate's own counterfactual.
fn irreversible_dependencies(
    s: &Scenario,
    checks: &BTreeMap<CheckKey, ResolvedCheck>,
) -> Vec<CheckKey> {
    let selected = s.select(checks);
    if !selected.is_irreversible() {
        return Vec::new();
    }
    // What the rules proposing the selected directive read (the read lists are pinned
    // against the rules' code by `builtin_read_lists_match_the_rules`).
    let read: BTreeSet<CheckKey> = s
        .proposals(checks)
        .iter()
        .filter(|p| p.directive == selected)
        .flat_map(|p| s.registry.reads(&p.rule))
        .collect();
    let mut deps = Vec::new();
    for (key, rc) in checks {
        let Some(t) = rc.threshold else { continue };
        if !(rc.decisive && rc.permitted) {
            continue;
        }
        if read.contains(key) {
            deps.push(key.clone());
            continue;
        }
        // Compared on the whole directive: a Stop for one finding is not a Stop for another.
        let kinds = |above: bool| -> BTreeSet<String> {
            (0..=20)
                .map(|v| v as f32 / 20.0)
                .filter(|v| (*v >= t) == above)
                .filter_map(|v| {
                    let probe = ResolvedCheck::from_measurement(
                        key.clone(),
                        Some(v),
                        Some(t),
                        rc.margin,
                        true,
                    );
                    probe.decisive.then(|| {
                        let mut c = checks.clone();
                        c.insert(key.clone(), probe);
                        format!("{:?}", s.select(&c))
                    })
                })
                .collect()
        };
        if kinds(true) != kinds(false) {
            deps.push(key.clone());
        }
    }
    deps
}

/// Every `Finish` or `Stop` the resolver lets through rests on checks each carrying an R2
/// confirm obtained this cycle under `Irreversible` semantics, wherever R2 could be had.
fn assert_irreversible_guarded(s: &Scenario, outcome: &EnsembleOutcome, label: &str) {
    assert!(
        outcome.gate_rounds as usize <= outcome.checks.len() + 1,
        "the gate's bound: {label}"
    );
    let r2 = s.settings.has(Rung::ReframeSameModel);
    for key in irreversible_dependencies(s, &outcome.checks) {
        if !(r2 && s.reframe.contains(&key)) {
            continue;
        }
        let mark = outcome.counters.check(&key).last_opinion;
        assert!(
            mark.is_some_and(|m| m.cycle == s.cycle
                && m.verdict == MarkVerdict::Confirm
                && m.irreversible
                && m.side == outcome.checks[&key].side),
            "{} lets {key} through without an irreversible confirm this cycle ({mark:?}): {label}",
            s.select(&outcome.checks).name()
        );
        assert!(
            record(outcome, key.as_str()).is_some_and(|r| r
                .opinions
                .iter()
                .any(|o| o.key.cycle == s.cycle
                    && matches!(o.opinion.verdict, Verdict::Confirm { .. }))),
            "{key}: {label}"
        );
    }
}

#[tokio::test]
async fn no_opinion_driven_finish_across_cycles() {
    // Every sequence of three cycles over these states, with four answer patterns, counters
    // carried. The states include the selection changes within a cycle (a refresh that
    // unmasks a Finish, a contest that unmasks a Stop) and the before_finish
    // re-assessment, which the single-cycle properties cannot see.
    use CycleState as C;
    let states = [
        C::Verify,
        C::FinishAfterVerify,
        C::ReadyInBand,
        C::FinishReady,
        C::TestsLow,
        C::RefreshUnmasksFinish,
        C::ContestUnmasksStop,
        C::Reassessed,
    ];
    let patterns = [
        [Answers::Fail; 3],
        [Answers::Consistent; 3],
        [Answers::Inconsistent; 3],
        [Answers::Consistent, Answers::Fail, Answers::Inconsistent],
        [Answers::Fail, Answers::Consistent, Answers::Consistent],
    ];
    let mut irreversible = 0;
    for &s0 in &states {
        for &s1 in &states {
            for &s2 in &states {
                let sequence = [s0, s1, s2];
                // The same sequence with no opinion rung at all.
                let mut without = Vec::new();
                let mut counters = EnsembleCounters::default();
                for (i, state) in sequence.iter().enumerate() {
                    let s = cycle_scenario(*state, 20 + i as u64, &counters)
                        .without_rung(Rung::ReframeSameModel);
                    let plan = s.plan();
                    let outcome = s
                        .resolve_plan(&plan, &cycle_source(*state, &plan, Answers::Fail))
                        .await
                        .unwrap_or_else(|e| panic!("{e}"));
                    without.push(s.select(&outcome.checks).name());
                    counters = outcome.counters;
                }
                for answers in patterns {
                    let mut counters = EnsembleCounters::default();
                    for (i, state) in sequence.iter().enumerate() {
                        let s = cycle_scenario(*state, 20 + i as u64, &counters);
                        let plan = s.plan();
                        let source = cycle_source(*state, &plan, answers[i]);
                        let outcome = s
                            .resolve_plan(&plan, &source)
                            .await
                            .unwrap_or_else(|e| panic!("{e}"));
                        let selected = s.select(&outcome.checks).name();
                        let label = format!("{sequence:?} {answers:?} cycle {i}");
                        if selected == "Finish" {
                            // A contest can never unmask a Finish: it needs every check
                            // permitted.
                            assert_eq!(without[i], "Finish", "{label}");
                        }
                        if selected == "Stop" {
                            // Nor a Stop: an opinion may only suppress, so a Stop with the
                            // opinions is a Stop without them (integrated review, blocker 1).
                            assert_eq!(without[i], "Stop", "{label}");
                        }
                        if selected == "Finish" || selected == "Stop" {
                            irreversible += 1;
                            assert_ne!(
                                answers[i],
                                Answers::Fail,
                                "a source that never answered let {selected} through: {label}"
                            );
                        }
                        assert_irreversible_guarded(&s, &outcome, &label);
                        counters = outcome.counters;
                    }
                }
            }
        }
    }
    assert!(irreversible > 0, "the property was exercised");
}

#[tokio::test]
async fn no_fabrication_jev_p_is_unchanged_unless_refreshed() {
    for &tests in &GRID {
        for &q in &NEGATED {
            for refresh in [None, Some(0.05_f32), Some(0.8)] {
                let s = Scenario::finish_ready()
                    .set(TESTS, tests)
                    .set(READY, tests)
                    .selecting(TESTS, &["tests"])
                    .reframable_all();
                let plan = s.plan();
                let mut source = answering_all(&plan, q);
                source = source.with_refresh(
                    CYCLE,
                    TESTS,
                    refresh.map_or_else(failed_refresh, |p| refreshed(Some(p), "tests", 0)),
                );
                let outcome = s
                    .resolve_plan(&plan, &source)
                    .await
                    .unwrap_or_else(|e| panic!("{e}"));
                let baseline = s.baseline();
                for (key, rc) in &outcome.checks {
                    match &rc.resolution {
                        Resolution::EvidenceRefreshed { new_p, old_p, .. } => {
                            assert_eq!(rc.jev_p, Some(*new_p));
                            assert_eq!(*old_p, baseline[key].jev_p);
                        }
                        _ => {
                            assert_eq!(rc.jev_p, baseline[key].jev_p, "{key} tests={tests} q={q:?}")
                        }
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn no_input_produces_an_opinion_driven_finish() {
    let grid = [0.30_f32, 0.62, 0.70, 0.80, 0.95];
    for &ready in &grid {
        for &reqs in &grid {
            for &tests in &grid {
                for &needs in &[0.10_f32, 0.55, 0.62, 0.80] {
                    for &docs in &[0.30_f32, 0.95] {
                        for &q in &[None, Some(0.05_f32), Some(0.5), Some(0.95)] {
                            for verifier_completed in [false, true] {
                                let mut s = Scenario::finish_ready()
                                    .set(READY, ready)
                                    .set(REQS, reqs)
                                    .set(TESTS, tests)
                                    .set(NEEDS_VER, needs)
                                    .set(DOCS, docs)
                                    // No evidence refresh: an opinion-only resolution.
                                    .without_rung(Rung::RefreshEvidence)
                                    .reframable_all();
                                s.counters.verifier_completed = verifier_completed;
                                let plan = s.plan();
                                let outcome = s
                                    .resolve_plan(&plan, &answering_all(&plan, q))
                                    .await
                                    .unwrap_or_else(|e| panic!("{e}"));
                                assert_irreversible_guarded(
                                    &s,
                                    &outcome,
                                    &format!(
                                        "ready={ready} reqs={reqs} tests={tests} needs={needs} \
                                         docs={docs} q={q:?}"
                                    ),
                                );
                                let baseline_finishes = s.select(&s.baseline()).name() == "Finish";
                                // Whatever verdict any opinion could return, folded into
                                // every check at once, cannot construct a Finish either.
                                for verdict in [
                                    Verdict::Confirm { side: Side::Above },
                                    Verdict::Confirm { side: Side::Below },
                                    Verdict::Abstain,
                                    Verdict::Contest { side: Side::Above },
                                ] {
                                    let folded: BTreeMap<CheckKey, ResolvedCheck> = outcome
                                        .checks
                                        .iter()
                                        .map(|(key, rc)| {
                                            (
                                                key.clone(),
                                                apply_opinions(rc.clone(), &[opinion(verdict)]),
                                            )
                                        })
                                        .collect();
                                    assert!(
                                        s.select(&folded).name() != "Finish" || baseline_finishes,
                                        "{verdict:?} produced a Finish"
                                    );
                                }
                                if s.select(&outcome.checks).name() == "Finish" {
                                    assert_eq!(
                                        s.select(&s.baseline()).name(),
                                        "Finish",
                                        "an opinion produced a Finish: ready={ready} \
                                         reqs={reqs} tests={tests} needs={needs} docs={docs} \
                                         q={q:?}"
                                    );
                                    for key in [READY, REQS, TESTS] {
                                        assert_eq!(
                                            outcome.checks[&k(key)].resolution,
                                            Resolution::Decisive
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn side_guard_a_confirm_below_never_permits_the_rule() {
    // §9.3 as a property: a below-threshold worker_stuck, whatever the opinions say.
    for &p in &[0.0_f32, 0.30, 0.64, 0.66, 0.70, 0.78, 0.79] {
        for &q in &NEGATED {
            let s = Scenario::finish_ready()
                .with_worker()
                .set(STUCK, p)
                .reframable_all();
            let plan = s.plan();
            let outcome = s
                .resolve_plan(&plan, &answering_all(&plan, q))
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            let rc = &outcome.checks[&k(STUCK)];
            for verdict in [
                Verdict::Confirm { side: Side::Below },
                Verdict::Confirm { side: Side::Above },
                Verdict::Abstain,
                Verdict::Contest { side: Side::Below },
            ] {
                let after = apply_opinions(rc.clone(), &[opinion(verdict)]);
                assert!(!after.at_least(0.80), "p={p} {verdict:?}");
                assert!(
                    !after.permitted || rc.permitted,
                    "p={p} {verdict:?} raised permission"
                );
            }
            assert!(
                !s.proposals(&outcome.checks)
                    .iter()
                    .any(|p| p.rule.as_str() == "core.worker-health:stuck"),
                "p={p} q={q:?}"
            );
        }
    }
}

#[tokio::test]
async fn direction_guard_less_than_rules_fire_and_every_finish_comparison_is_gated() {
    // A `<` rule fires on a decisively-below value.
    for &docs in &[0.0_f32, 0.30, 0.59] {
        let s = Scenario::finish_ready().set(DOCS, docs);
        let outcome = s
            .run_plan(&RecordedOpinionSource::new())
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(outcome.checks[&k(DOCS)].permitted, "docs={docs}");
        assert!(
            s.proposals(&outcome.checks)
                .iter()
                .any(|p| p.rule.as_str() == "quality.documentation:insufficient"),
            "docs={docs}"
        );
    }

    // Every comparison in the Finish precondition — the three `>=` conjuncts and the
    // `needs_verification < 0.65` disjunct — is false for an in-band or contested check.
    let finish_rule = "core.completion:thresholds-satisfied";
    let in_band = [
        (READY, 0.79_f32),
        (REQS, 0.71),
        (TESTS, 0.80),
        (NEEDS_VER, 0.55),
    ];
    for (key, value) in in_band {
        let s = Scenario::finish_ready().set(key, value);
        let outcome = s
            .run_plan(&RecordedOpinionSource::new())
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(!outcome.checks[&k(key)].permitted, "{key} in band");
        assert!(
            !s.proposals(&outcome.checks)
                .iter()
                .any(|p| p.rule.as_str() == finish_rule),
            "{key} in band still proposed Finish"
        );
    }
    for key in [READY, REQS, TESTS, NEEDS_VER] {
        let s = Scenario::finish_ready().reframable(&[key]);
        let mut checks = s.baseline();
        assert!(
            s.proposals(&checks)
                .iter()
                .any(|p| p.rule.as_str() == finish_rule)
        );
        let contested = apply_opinions(
            checks[&k(key)].clone(),
            &[opinion(Verdict::Contest { side: Side::Below })],
        );
        checks.insert(k(key), contested);
        assert!(
            !s.proposals(&checks)
                .iter()
                .any(|p| p.rule.as_str() == finish_rule),
            "{key} contested still proposed Finish"
        );
    }
}

// ---- replay ----------------------------------------------------------------

/// The events a runtime would write for one cycle's resolution.
fn events_for(
    outcome: &EnsembleOutcome,
    settings: &EnsembleSettings,
    seq: &mut u64,
) -> Vec<EventEnvelope> {
    let run = RunId::new();
    let at = datetime!(2026-01-01 00:00:00 UTC);
    let mut out = Vec::new();
    let mut push = |kind, payload| {
        *seq += 1;
        out.push(EventEnvelope::new(EventSeq(*seq), run, at, kind, payload));
    };
    for record in &outcome.records {
        for payload in record.opinion_recorded_payloads() {
            push(
                EventKind::OpinionRecorded,
                serde_json::to_value(payload).unwrap_or_default(),
            );
        }
    }
    push(
        EventKind::EnsembleResolved,
        serde_json::to_value(
            outcome.resolved_payload(settings.enabled, settings.uncertainty_margin),
        )
        .unwrap_or_default(),
    );
    out
}

#[tokio::test]
async fn a_replay_from_recorded_events_is_bit_identical() -> TestResult {
    // Four cycles touching every rung: a refresh, a contest, a confirm, a failed call,
    // an escalation, and a cap.
    let cycles: Vec<Scenario> = vec![
        Scenario::finish_ready()
            .set(TESTS, 0.71)
            .selecting(TESTS, &["tests"])
            .reframable_all(),
        Scenario::finish_ready().set(TESTS, 0.92).reframable_all(),
        Scenario::finish_ready()
            .set(DOCS, 0.31)
            .with_worker()
            .set(STUCK, 0.97)
            .reframable_all(),
        Scenario::finish_ready().set(READY, 0.79).reframable_all(),
    ];
    // Each cycle answers every opinion it could ask, with these exceptions; the last
    // cycle's calls all fail.
    let live_source = |i: usize, plan: &EnsemblePlan| match i {
        0 => answering_all(plan, Some(0.5)).with_refresh(
            AssessmentSeq(20),
            TESTS,
            refreshed(Some(0.08), "tests", 1),
        ),
        1 => answering_all(plan, Some(0.5))
            .with_reframe(reframe_key(AssessmentSeq(21), TESTS), answer(Some(0.75))),
        2 => answering_all(plan, Some(0.5))
            .with_reframe(reframe_key(AssessmentSeq(22), STUCK), answer(Some(0.04)))
            .with_reframe(reframe_key(AssessmentSeq(22), DOCS), answer(Some(f32::NAN))),
        _ => answering_all(plan, None),
    };

    let mut counters = EnsembleCounters::default();
    let mut live = Vec::new();
    let mut events = Vec::new();
    let mut live_refreshes = Vec::new();
    let mut live_reframes = Vec::new();
    let mut seq = 0;
    for (i, mut s) in cycles.into_iter().enumerate() {
        s.cycle = AssessmentSeq(20 + i as u64);
        s.ensemble = counters.clone();
        let plan = s.plan();
        let source = live_source(i, &plan);
        let outcome = s
            .resolve_plan(&plan, &source)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        live_refreshes.extend(source.refresh_calls());
        live_reframes.extend(source.reframe_calls());
        events.extend(events_for(&outcome, &s.settings, &mut seq));
        counters = outcome.counters.clone();
        live.push(outcome);
    }
    assert!(
        live.iter()
            .any(|o| o.records.iter().any(|r| r.refresh.is_some()))
    );
    assert!(
        live.iter()
            .any(|o| o.records.iter().any(|r| !r.opinions.is_empty()))
    );

    // Replay: the same inputs, answered only from the log.
    let log = serde_json::to_string(&events)?;
    let events: Vec<EventEnvelope> = serde_json::from_str(&log)?;
    let replay_source = RecordedOpinionSource::from_events(&events)?;
    let cycles: Vec<Scenario> = vec![
        Scenario::finish_ready()
            .set(TESTS, 0.71)
            .selecting(TESTS, &["tests"])
            .reframable_all(),
        Scenario::finish_ready().set(TESTS, 0.92).reframable_all(),
        Scenario::finish_ready()
            .set(DOCS, 0.31)
            .with_worker()
            .set(STUCK, 0.97)
            .reframable_all(),
        Scenario::finish_ready().set(READY, 0.79).reframable_all(),
    ];
    let mut counters = EnsembleCounters::default();
    for (i, mut s) in cycles.into_iter().enumerate() {
        s.cycle = AssessmentSeq(20 + i as u64);
        s.ensemble = counters.clone();
        let replayed = s
            .run_plan(&replay_source)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            replay_source.divergences(&replayed),
            Vec::new(),
            "cycle {i}"
        );
        assert_eq!(replayed, live[i], "cycle {i}");
        assert_eq!(
            serde_json::to_vec(&replayed)?,
            serde_json::to_vec(&live[i])?,
            "cycle {i} is not byte-identical"
        );
        counters = replayed.counters;
    }
    assert_eq!(replay_source.refresh_calls(), live_refreshes);
    assert_eq!(
        replay_source.reframe_calls(),
        live_reframes,
        "the replay asked exactly what the live run asked"
    );
    Ok(())
}

/// One cycle with an opinion and its events, for the replay-refusal tests.
async fn docs_cycle_events() -> (EnsembleOutcome, Vec<EventEnvelope>) {
    let s = Scenario::finish_ready().set(DOCS, 0.31).reframable_all();
    let plan = s.plan();
    let live = s
        .resolve_plan(&plan, &answering_all(&plan, Some(0.70)))
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let mut seq = 0;
    let events = events_for(&live, &s.settings, &mut seq);
    (live, events)
}

#[tokio::test]
async fn a_replay_refuses_a_log_missing_an_opinion_record() {
    // Review probe 6: drop one OPINION_RECORDED. The reader must not answer the missing
    // opinion with an invented abstention.
    let (live, events) = docs_cycle_events().await;
    assert!(record(&live, DOCS).is_some_and(|r| !r.opinions.is_empty()));
    let dropped: Vec<EventEnvelope> = events
        .into_iter()
        .filter(|e| e.kind != EventKind::OpinionRecorded)
        .collect();
    assert!(matches!(
        RecordedOpinionSource::from_events(&dropped),
        Err(super::super::testing::ReplayError::MissingOpinion(_))
    ));
}

/// An `OPINION_REQUESTED` event for `key`, as the daemon's source would emit it.
fn requested_event(seq: u64, key: OpinionKey) -> EventEnvelope {
    let payload = super::OpinionRequestedPayload::new(
        &super::ReframeRequest {
            opinion: key,
            timeout: Duration::from_secs(120),
            refreshed: false,
        },
        "jev",
        "model",
        "prompt-hash",
        "state-digest",
    );
    EventEnvelope::new(
        EventSeq(seq),
        RunId::new(),
        datetime!(2026-01-01 00:00:00 UTC),
        EventKind::OpinionRequested,
        serde_json::to_value(payload).unwrap_or_default(),
    )
}

#[tokio::test]
async fn a_replay_tolerates_a_dangling_request_in_the_trailing_cycle() -> TestResult {
    // Round-2 probe P6c: a complete cycle, then a run cancelled mid-opinion in the next.
    let (_, mut events) = docs_cycle_events().await;
    events.push(requested_event(100, reframe_key(AssessmentSeq(99), DOCS)));
    let source = RecordedOpinionSource::from_events(&events)?;
    assert_eq!(source.incomplete_cycle(), Some(AssessmentSeq(99)));
    // The complete cycle still replays.
    let replayed = resolved(
        &Scenario::finish_ready().set(DOCS, 0.31).reframable_all(),
        &source,
    )
    .await;
    assert_eq!(source.divergences(&replayed), Vec::new());
    Ok(())
}

#[tokio::test]
async fn a_replay_refuses_a_dangling_request_before_the_last_resolved_cycle() {
    // The same dangling request, but a later cycle was resolved: a gap, not a crash.
    let (_, events) = docs_cycle_events().await;
    let mut log = vec![requested_event(0, reframe_key(AssessmentSeq(5), DOCS))];
    log.extend(events);
    assert!(matches!(
        RecordedOpinionSource::from_events(&log),
        Err(super::super::testing::ReplayError::MissingOpinion(_))
    ));
}

#[tokio::test]
async fn a_requested_opinion_in_a_resolved_cycle_needs_its_record() {
    // Pins the OPINION_REQUESTED requirement on its own: the resolved entry no longer
    // names the rung, so only the request says an opinion was asked.
    let (_, events) = docs_cycle_events().await;
    let mut log: Vec<EventEnvelope> = vec![requested_event(0, reframe_key(CYCLE, DOCS))];
    for mut event in events {
        if event.kind == EventKind::OpinionRecorded {
            continue;
        }
        if let Some(checks) = event
            .payload
            .get_mut("checks")
            .and_then(|c| c.as_array_mut())
        {
            for check in checks {
                check["rungs_used"] = serde_json::json!([]);
            }
        }
        log.push(event);
    }
    assert!(matches!(
        RecordedOpinionSource::from_events(&log),
        Err(super::super::testing::ReplayError::MissingOpinion(_))
    ));
}

#[tokio::test]
async fn a_replay_refuses_a_log_missing_a_refresh_record() {
    let s = Scenario::finish_ready()
        .set(TESTS, 0.71)
        .selecting(TESTS, &["tests"]);
    let live = s
        .run_plan(&RecordedOpinionSource::new().with_refresh(
            CYCLE,
            TESTS,
            refreshed(Some(0.08), "tests", 1),
        ))
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let mut seq = 0;
    let mut events = events_for(&live, &s.settings, &mut seq);
    for event in &mut events {
        if let Some(checks) = event
            .payload
            .get_mut("checks")
            .and_then(|c| c.as_array_mut())
        {
            for check in checks {
                check["refresh"] = serde_json::Value::Null;
            }
        }
    }
    assert!(matches!(
        RecordedOpinionSource::from_events(&events),
        Err(super::super::testing::ReplayError::MissingRefresh { .. })
    ));
}

#[tokio::test]
async fn a_replay_reports_a_recomputed_verdict_that_differs_from_the_log() -> TestResult {
    let (_, mut events) = docs_cycle_events().await;
    // Tamper with the recorded verdict; the recorded answer still confirms.
    for event in &mut events {
        if event.kind == EventKind::OpinionRecorded {
            event.payload["verdict"] = serde_json::json!({"kind": "contest", "side": "above"});
        }
    }
    let source = RecordedOpinionSource::from_events(&events)?;
    let replayed = Scenario::finish_ready()
        .set(DOCS, 0.31)
        .reframable_all()
        .run_plan(&source)
        .await?;
    let divergences = source.divergences(&replayed);
    assert!(
        divergences.iter().any(|d| matches!(
            d,
            super::super::testing::ReplayError::VerdictMismatch { .. }
        )),
        "{divergences:?}"
    );
    Ok(())
}

#[tokio::test]
async fn an_unrecorded_call_is_a_divergence_not_an_answer() {
    let s = Scenario::finish_ready()
        .set(TESTS, 0.92)
        .reframable(&[TESTS]);
    let result = s.run_plan(&RecordedOpinionSource::new()).await;
    assert!(
        matches!(
            result,
            Err(super::EnsembleError::Source(
                super::SourceError::NotRecorded(_)
            ))
        ),
        "{result:?}"
    );
}

#[test]
fn an_s0_ensemble_resolved_event_is_skipped_by_the_replay_reader() -> TestResult {
    let event = EventEnvelope::new(
        EventSeq(1),
        RunId::new(),
        datetime!(2026-01-01 00:00:00 UTC),
        EventKind::EnsembleResolved,
        serde_json::json!({"enabled": false, "margin": 0.15, "unresolved": [], "rungs": []}),
    );
    let source = RecordedOpinionSource::from_events(&[event])?;
    assert!(source.refresh_calls().is_empty());
    Ok(())
}

#[test]
fn the_triggered_payload_names_the_triggers_and_records_no_counterfactual_value() {
    let s = Scenario::finish_ready().set(READY, 0.79);
    let payload = s.check_plan(READY).map(|p| p.triggered_payload());
    let payload = payload.as_ref();
    assert_eq!(payload.and_then(|p| p.jev_p), Some(0.79));
    assert!(payload.is_some_and(|p| p.reason.contains("near_threshold")));
    let json = serde_json::to_string(&payload).unwrap_or_default();
    assert!(!json.contains("1.0") && !json.contains(":0.0"), "{json}");
}

fn opinion(verdict: Verdict) -> Opinion {
    Opinion {
        rung: Rung::ReframeSameModel,
        source: "test".into(),
        model: "test".into(),
        verdict,
        p_reported: Some(0.5),
        excerpt: String::new(),
        prompt_hash: String::new(),
        state_digest: String::new(),
        usage: Usage::default(),
        latency: Duration::ZERO,
        error: None,
    }
}

// ---- stage B: escalation through policy, and the human answer ----------------

impl Scenario {
    /// What policy selects over a resolved cycle: every responsibility's proposals plus
    /// the ensemble's escalation, arbitrated together (`super::propose`).
    fn decide(&self, outcome: &EnsembleOutcome) -> crate::policy::PolicyDecision {
        let input = self.input(&outcome.checks);
        let extra: Vec<Proposal> = outcome.escalation_proposal().into_iter().collect();
        crate::policy::decide(&self.registry, &input, &extra)
    }

    fn answered(mut self, keys: &[&str], decision: &crate::assessment::HumanDecision) -> Self {
        let keys: Vec<CheckKey> = keys.iter().map(|key| k(key)).collect();
        self.ensemble.record_human_decision(&keys, decision);
        self
    }
}

#[test]
fn the_ensemble_escalation_priority_sits_between_work_and_worker_protection() {
    use crate::directive::priority;
    for below in [
        priority::RETRY_STOPPED,
        priority::START_WORKER_DOCUMENTATION,
        priority::FINISH,
        priority::VERIFY,
        priority::START_WORKER_COMPLETION,
    ] {
        assert!(priority::ESCALATE_ENSEMBLE > below);
    }
    for above in [
        priority::STOP_OR_STEER_WORKER_HEALTH,
        priority::STOP_OR_STEER_INSTRUCTIONS,
        priority::ESCALATE_BUDGET,
        priority::ESCALATE_NEEDS_HUMAN,
    ] {
        assert!(priority::ESCALATE_ENSEMBLE < above);
    }
}

#[tokio::test]
async fn one_cycles_escalations_coalesce_into_one_escalate_that_preempts_the_selection() {
    // Two worker-health checks held in band for two cycles, each one a `Stop` away: two
    // escalations from the ladder, one `Escalate` for policy.
    let s = Scenario::finish_ready()
        .with_worker()
        .set(STUCK, 0.78)
        .set(OFF_TRACK, 0.78)
        .held_for(STUCK, 2)
        .held_for(OFF_TRACK, 2);
    let outcome = s
        .run_plan(&RecordedOpinionSource::new())
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        outcome.escalations().count(),
        2,
        "the ladder escalates both"
    );
    assert_eq!(outcome.escalated_checks(), vec![k(OFF_TRACK), k(STUCK)]);
    let decision = s.decide(&outcome);
    let Directive::Escalate { reason, ask } = &decision.selected else {
        panic!("expected one Escalate, got {:?}", decision.selected);
    };
    assert!(outcome.is_escalation(&decision.selected));
    assert!(
        reason.starts_with(super::ESCALATION_REASON_PREFIX),
        "{reason}"
    );
    assert!(ask.contains(STUCK) && ask.contains(OFF_TRACK), "{ask}");
    assert!(ask.starts_with("2 checks"), "{ask}");
    let escalates = decision
        .considered
        .iter()
        .filter(|p| matches!(p.directive, Directive::Escalate { .. }))
        .count();
    assert_eq!(escalates, 1, "coalesced: one proposal for the cycle");

    // Pre-emption. One Finish conjunct held in band for two cycles, with nothing to
    // refresh it: without the ensemble's proposal, policy would start another worker for a
    // job whose only problem is that nobody can tell whether it is done.
    let s = Scenario::finish_ready().set(TESTS, 0.71).held_for(TESTS, 2);
    let outcome = s
        .run_plan(&RecordedOpinionSource::new())
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(outcome.escalations().count(), 1);
    assert_eq!(s.select(&outcome.checks).name(), "StartWorker");
    let decision = s.decide(&outcome);
    assert!(outcome.is_escalation(&decision.selected), "{decision:?}");
    assert!(decision.suppressed.iter().any(|s| {
        s.rule.as_str() == "core.completion:work-remains"
            && s.reason == crate::directive::SuppressionReason::HigherPriority
    }));
}

#[tokio::test]
async fn a_cycle_with_no_escalation_proposes_nothing_extra() {
    let s = Scenario::finish_ready();
    let outcome = s
        .run_plan(&RecordedOpinionSource::new())
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert!(outcome.escalation_proposal().is_none());
    let decision = s.decide(&outcome);
    assert_eq!(decision.selected.name(), "Finish");
    assert_eq!(
        decision.considered,
        s.proposals(&outcome.checks),
        "identical to the registry's own proposals"
    );
}

#[tokio::test]
async fn g2_a_spent_budget_selects_one_escalate_and_burns_no_worker() {
    // The reviewer's G2 scenario, through policy: the job is done, every check is
    // decisive, the opinion budget is spent. The gate holds every check the Finish rests
    // on; the cycle must ask a person once, not start a worker.
    let mut s = Scenario::finish_ready().reframable_all();
    s.ensemble.opinions_used = s.settings.max_opinions_per_run;
    let plan = s.plan();
    let outcome = s
        .resolve_plan(&plan, &answering_by(&plan, |p| Some(1.0 - p)))
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(outcome.escalations().count(), 1);
    // The bug: arbitrating the registry's proposals alone starts a worker.
    assert_eq!(s.select(&outcome.checks).name(), "StartWorker");

    let decision = s.decide(&outcome);
    assert!(outcome.is_escalation(&decision.selected), "{decision:?}");
    let Directive::Escalate { ask, .. } = &decision.selected else {
        panic!("expected an Escalate");
    };
    assert!(ask.contains("opinion budget"), "{ask}");
    assert!(decision.suppressed.iter().any(|s| {
        s.rule.as_str() == "core.completion:work-remains"
            && s.reason == crate::directive::SuppressionReason::HigherPriority
    }));
}

#[tokio::test]
async fn an_accepted_check_is_held_as_human_decided_and_never_escalated_again() {
    use crate::assessment::HumanDecision;
    let mut s = Scenario::finish_ready()
        .set(TESTS, 0.71)
        .held_for(TESTS, 2)
        .answered(&[TESTS], &HumanDecision::Accept);
    for cycle in 20..26 {
        s.cycle = AssessmentSeq(cycle);
        let outcome = s
            .run_plan(&RecordedOpinionSource::new())
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(outcome.escalations().count(), 0, "cycle {cycle}");
        let rc = &outcome.checks[&k(TESTS)];
        assert!(!rc.permitted, "acceptance never permits");
        assert_eq!(
            rc.resolution,
            Resolution::HumanDecided {
                decision: HumanDecision::Accept
            }
        );
        assert!(
            record(&outcome, TESTS)
                .is_some_and(|r| r.triggers.contains(&EnsembleTrigger::RepeatContest)),
            "still visibly repeated"
        );
        assert_ne!(s.decide(&outcome).selected.name(), "Finish");
        s.ensemble = outcome.counters;
    }

    // Guidance is Accept plus a steer; its text is not repeated on every cycle.
    let s = Scenario::finish_ready()
        .set(TESTS, 0.71)
        .held_for(TESTS, 2)
        .answered(
            &[TESTS],
            &HumanDecision::Guidance {
                text: "the flaky test is known".into(),
            },
        );
    let outcome = s
        .run_plan(&RecordedOpinionSource::new())
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(outcome.escalations().count(), 0);
    assert_eq!(
        outcome.checks[&k(TESTS)].resolution,
        Resolution::HumanDecided {
            decision: HumanDecision::Guidance {
                text: String::new()
            }
        }
    );

    // A later decisive measurement is Jev's own, and stands as any other would: the
    // acceptance changed nothing about it in either direction.
    let mut s = Scenario::finish_ready()
        .set(TESTS, 0.95)
        .answered(&[TESTS], &HumanDecision::Accept);
    s.ensemble
        .checks
        .entry(k(TESTS))
        .or_default()
        .consecutive_in_band = 4;
    let outcome = s
        .run_plan(&RecordedOpinionSource::new())
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert!(outcome.checks[&k(TESTS)].permitted);
}

#[tokio::test]
async fn an_earlier_accept_does_not_quiet_a_decisive_value_held_by_a_spent_budget() {
    // Review probe SB3. Every check decisive, the job done, the opinion budget spent, and a
    // person accepted every check earlier (when it was in band). An acceptance is about an
    // uncertainty: it must not swallow the budget escalation for a decisive value, which
    // left policy starting workers up to the limit.
    let mut counters = EnsembleCounters {
        opinions_used: 20,
        ..EnsembleCounters::default()
    };
    let keys: Vec<CheckKey> = Scenario::finish_ready().values.keys().cloned().collect();
    counters.record_human_decision(&keys, &crate::assessment::HumanDecision::Accept);
    let mut s = Scenario::finish_ready().reframable_all();
    s.ensemble = counters;
    let plan = s.plan();
    let outcome = s
        .resolve_plan(&plan, &answering_by(&plan, |p| Some(1.0 - p)))
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(outcome.escalations().count(), 1);
    assert!(
        !outcome
            .checks
            .values()
            .any(|c| matches!(c.resolution, Resolution::HumanDecided { .. })),
        "no decisive value is recorded as a person's decision"
    );
    let decision = s.decide(&outcome);
    assert!(outcome.is_escalation(&decision.selected), "{decision:?}");

    // The decisive repeat-contest path ignores it too.
    let s = Scenario::finish_ready()
        .set(TESTS, 0.92)
        .reframable(&[TESTS])
        .contested_for(TESTS, 2)
        .answered(&[TESTS], &crate::assessment::HumanDecision::Accept);
    let outcome = s
        .run_plan(&RecordedOpinionSource::new())
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(outcome.escalations().count(), 1);
}

#[tokio::test]
async fn a_recheck_runs_r0_on_the_next_cycle_within_the_cap() {
    use crate::assessment::HumanDecision;
    // Decisively below with no rule acting on it: the ladder alone would never refresh.
    let s = Scenario::finish_ready()
        .set(TESTS, 0.30)
        .selecting(TESTS, &["tests"]);
    let quiet = RecordedOpinionSource::new();
    let outcome = s.run_plan(&quiet).await.unwrap_or_else(|e| panic!("{e}"));
    assert!(
        quiet.refresh_calls().is_empty(),
        "nothing asked for a refresh"
    );
    assert!(!outcome.records.iter().any(|r| r.refresh.is_some()));

    let s = s.answered(&[TESTS], &HumanDecision::Recheck);
    assert!(s.triggers(TESTS).contains(&EnsembleTrigger::HumanRecheck));
    let source =
        RecordedOpinionSource::new().with_refresh(CYCLE, TESTS, refreshed(Some(0.20), "tests", 1));
    let outcome = s.run_plan(&source).await.unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(source.refresh_calls().len(), 1, "exactly one refresh");
    assert!(matches!(
        outcome.checks[&k(TESTS)].resolution,
        Resolution::EvidenceRefreshed { .. }
    ));
    assert!(!outcome.counters.check(&k(TESTS)).recheck, "answered");
    assert_eq!(outcome.counters.check(&k(TESTS)).refreshes, 1);

    // The cap still applies: with every refresh spent, the recheck refreshes nothing, says
    // so, and is still consumed.
    let mut capped = Scenario::finish_ready()
        .set(TESTS, 0.30)
        .selecting(TESTS, &["tests"])
        .answered(&[TESTS], &HumanDecision::ClaimComplete);
    capped
        .ensemble
        .checks
        .entry(k(TESTS))
        .or_default()
        .refreshes = 3;
    let source = RecordedOpinionSource::new();
    let outcome = capped
        .run_plan(&source)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert!(source.refresh_calls().is_empty());
    assert!(record(&outcome, TESTS).is_some_and(|r| {
        r.limits
            .contains(&EnsembleLimit::RefreshCap { used: 3, cap: 3 })
    }));
    assert!(!outcome.counters.check(&k(TESTS)).recheck);
}

#[tokio::test]
async fn claim_complete_cannot_satisfy_a_finish_conjunct() {
    use crate::assessment::HumanDecision;
    // The person says it is done. The refresh their answer buys lands in band again, so
    // the conjunct is still held: no Finish, and the question comes back.
    let s = Scenario::finish_ready()
        .set(TESTS, 0.71)
        .selecting(TESTS, &["tests"])
        .held_for(TESTS, 2)
        .answered(&[TESTS], &HumanDecision::ClaimComplete);
    let source =
        RecordedOpinionSource::new().with_refresh(CYCLE, TESTS, refreshed(Some(0.72), "tests", 0));
    let outcome = s.run_plan(&source).await.unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(source.refresh_calls().len(), 1, "one refresh, not two");
    let rc = &outcome.checks[&k(TESTS)];
    assert!(!rc.permitted, "a fresh value in band is still held");
    assert!(matches!(
        rc.resolution,
        Resolution::EvidenceRefreshed { new_p, .. } if (new_p - 0.72).abs() < f32::EPSILON
    ));
    assert_ne!(s.decide(&outcome).selected.name(), "Finish");

    // Only a decisive measurement finishes.
    let source =
        RecordedOpinionSource::new().with_refresh(CYCLE, TESTS, refreshed(Some(0.95), "tests", 0));
    let outcome = s.run_plan(&source).await.unwrap_or_else(|e| panic!("{e}"));
    assert!(outcome.checks[&k(TESTS)].permitted);
    assert_eq!(s.decide(&outcome).selected.name(), "Finish");
}

#[test]
fn human_answers_are_recorded_per_check_and_old_counters_decode_without_them() -> TestResult {
    use crate::assessment::HumanDecision;
    let mut counters = EnsembleCounters::default();
    let keys = [k(TESTS), k(READY)];
    counters.record_human_decision(&keys, &HumanDecision::Accept);
    assert!(keys.iter().all(|key| counters.check(key).human.is_some()));
    counters.record_human_decision(&keys[..1], &HumanDecision::Recheck);
    assert_eq!(
        counters.check(&keys[0]).human,
        None,
        "a recheck forgets the accept"
    );
    assert!(counters.check(&keys[0]).recheck);
    counters.record_human_decision(&keys, &HumanDecision::Stop);
    assert!(
        counters.check(&keys[1]).human.is_some(),
        "a stop changes nothing"
    );

    let old: CheckCounters = serde_json::from_str(r#"{"refreshes":1}"#)?;
    assert_eq!(old.human, None);
    assert!(!old.recheck);
    Ok(())
}

#[tokio::test]
async fn two_finish_conjuncts_in_band_escalate_together() {
    // Review probe (stage B, finding 3). With one Finish conjunct in band, the check
    // escalates after `consecutive_contests_before_human` held cycles. With two, neither
    // pinned alone produced a Finish, so neither was high stakes, neither escalated, and
    // policy started a worker every cycle until the worker limit. Both rest on the pending
    // Finish now, and the run asks a person at the same cycle as with one.
    for inband in [vec![(READY, 0.79_f32)], vec![(READY, 0.79), (TESTS, 0.71)]] {
        let mut counters = EnsembleCounters::default();
        let mut escalated_at = None;
        for i in 0..8u64 {
            let mut s = Scenario::finish_ready();
            for (key, p) in &inband {
                s = s.set(key, *p);
            }
            s.cycle = AssessmentSeq(20 + i);
            s.ensemble = counters.clone();
            // One worker run per cycle so far: each earlier cycle started one.
            s.counters.workers_run = u32::try_from(i).unwrap_or(u32::MAX).saturating_add(1);
            for (key, _) in &inband {
                assert!(
                    s.triggers(key).contains(&EnsembleTrigger::HighStakes),
                    "{key} rests on the pending Finish ({} in band)",
                    inband.len()
                );
            }
            let outcome = s
                .run_plan(&RecordedOpinionSource::new())
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            let selected = s.decide(&outcome).selected;
            counters = outcome.counters.clone();
            if outcome.is_escalation(&selected) {
                assert_eq!(outcome.escalations().count(), inband.len());
                escalated_at = Some(i);
                break;
            }
            assert_eq!(selected.name(), "StartWorker");
        }
        assert_eq!(
            escalated_at,
            Some(2),
            "{} in band: asked after two held cycles, not after the worker limit",
            inband.len()
        );
    }
}

// ---- which checks are reframable ------------------------------------------------

#[test]
fn reframe_is_available_for_negated_checks_only_when_its_rung_is_configured() {
    let checks = ResponsibilityRegistry::builtin().checks();
    let on = super::reframable_checks(&EnsembleSettings::default(), &checks);
    // Every thresholded bundled check carries a negation; `meaningful_progress` has no
    // threshold and is not reframable.
    let thresholded: BTreeSet<CheckKey> = checks
        .iter()
        .filter(|c| c.threshold.is_some())
        .map(|c| c.key.clone())
        .collect();
    assert_eq!(on, thresholded);
    assert!(!on.contains(&k(PROGRESS)));
    // Without the rung, nothing is: a run that did not configure R2 never plans it.
    let mut settings = EnsembleSettings::default();
    settings
        .rungs
        .retain(|rung| *rung != Rung::ReframeSameModel);
    assert!(super::reframable_checks(&settings, &checks).is_empty());
    // A check without a negation is not reframable, whatever the rungs.
    let plain = vec![Check::new(
        &ResponsibilityId::new("user.x"),
        "c",
        crate::model::Question::noul("Is it done?"),
        Some(0.7),
    )];
    assert!(super::reframable_checks(&EnsembleSettings::default(), &plain).is_empty());
    let negated = vec![plain[0].clone().with_negation("Does work remain?")];
    assert_eq!(
        super::reframable_checks(&EnsembleSettings::default(), &negated).len(),
        1
    );
}

// ---- an irreversible directive never rests on an unresolved check -------------------

#[tokio::test]
async fn an_r2_contest_alone_never_turns_an_escalation_into_a_stop() {
    // Integrated review, blocker 1 (probe `probe_an_r2_contest_alone_turns_an_escalation_
    // into_an_executed_stop`). A worker that cannot be steered; `needs_human` and
    // `worker_stuck` both decisively high. Without opinions, `Escalate` (1000) wins. R2
    // contests `needs_human` (p(not X) = 0.97), which used to unmask the `Stop` (900) resting
    // on `worker_stuck` — and the `Stop` executed. An opinion may only suppress: the `Stop`
    // rests on `needs_human` (its side changes it), so with `needs_human` unresolved it is
    // refused.
    let s = Scenario::finish_ready()
        .with_worker()
        .set(HUMAN, 0.97)
        .set(STUCK, 0.97)
        .reframable(&[HUMAN, STUCK]);
    assert_eq!(s.select(&s.baseline()).name(), "Escalate");
    let source = RecordedOpinionSource::new()
        .with_reframe(reframe_key(CYCLE, HUMAN), answer(Some(0.97)))
        .with_reframe(reframe_key(CYCLE, STUCK), answer(Some(0.03)));
    let outcome = s.run_plan(&source).await.unwrap_or_else(|e| panic!("{e}"));
    assert!(
        !outcome.checks[&k(HUMAN)].permitted,
        "the contest held needs_human"
    );
    let decision = s.decide(&outcome);
    assert_ne!(decision.selected.name(), "Stop", "{:?}", decision.selected);
    assert!(
        decision
            .invariants
            .contains(&crate::directive::Invariant::UnresolvedDependency),
        "{:?}",
        decision.invariants
    );
    assert!(
        decision
            .suppressed
            .iter()
            .any(|s| s.rule.as_str() == "core.worker-health:stuck"
                && matches!(
                    s.reason,
                    crate::directive::SuppressionReason::Invariant(
                        crate::directive::Invariant::UnresolvedDependency
                    )
                ))
    );
    // With needs_human decisively low, the Stop it does not rest on goes ahead.
    let s = Scenario::finish_ready()
        .with_worker()
        .set(HUMAN, 0.03)
        .set(STUCK, 0.97);
    assert_eq!(s.select(&s.baseline()).name(), "Stop");
}

#[tokio::test]
async fn an_in_band_inhibitor_does_not_unmask_a_stop_and_its_escalation_reaches_a_person() {
    // `needs_human` = 0.75 against 0.80 (in band, held); `worker_stuck` = 0.97 (decisive)
    // on a worker that cannot be steered. A held inhibitor must block, not permit: the Stop
    // rests on `needs_human` and is refused, in the baseline already. After two in-band
    // cycles the high-stakes escalation about `needs_human` is put to a person, and is not
    // outranked by the Stop (900 > 850) it would otherwise lose to.
    let s = Scenario::finish_ready()
        .with_worker()
        .set(HUMAN, 0.75)
        .set(STUCK, 0.97)
        .reframable_all();
    assert_ne!(s.select(&s.baseline()).name(), "Stop");
    let s = s.held_for(HUMAN, 2);
    let outcome = s
        .run_plan(&answering_by(&s.plan(), |p| Some(1.0 - p)))
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert!(
        outcome.escalations().any(|(key, _)| *key == k(HUMAN)),
        "needs_human is escalated"
    );
    let decision = s.decide(&outcome);
    assert!(
        outcome.is_escalation(&decision.selected),
        "{:?}",
        decision.selected
    );
}

#[test]
fn a_stop_with_an_unrelated_check_in_band_still_stops_a_stuck_worker() {
    // Scoped to what the Stop rests on: a check whose resolution would not change the
    // *kind* of directive selected does not hold it. `documentation_sufficient` in band has
    // nothing to do with stopping a stuck worker. (It does block `Finish`, which keeps its
    // global `UnresolvedUncertainty`.)
    let s = Scenario::finish_ready()
        .with_worker()
        .set(HUMAN, 0.03)
        .set(STUCK, 0.97)
        .set(DOCS, 0.72);
    assert!(!s.baseline()[&k(DOCS)].permitted);
    assert_eq!(s.select(&s.baseline()).name(), "Stop");
}

#[test]
fn a_runner_up_finding_in_band_does_not_keep_a_stuck_worker_running() {
    // Re-verification, finding 1 (probe `probe_runner_up_in_band_blocks_stopping_a_stuck_
    // worker`). `worker_stuck` = 0.97 on a worker that cannot be steered; `work_off_track`
    // anywhere in its band. Resolving off-track above only changes *which* Stop
    // (`Stop{"off track"}` instead of `Stop{"stuck"}`): the kind is the same, so it is not
    // something the Stop rests on. Compared as whole directives it was, the Stop was
    // refused, and the worker ran on unchecked with nothing escalated.
    for off_track in [0.70_f32, 0.75, 0.80, 0.90] {
        let s = Scenario::finish_ready()
            .with_worker()
            .set(HUMAN, 0.03)
            .set(STUCK, 0.97)
            .set(OFF_TRACK, off_track);
        assert!(
            !s.baseline()[&k(OFF_TRACK)].permitted,
            "{off_track} is in band"
        );
        let decision = {
            let baseline = s.baseline();
            let input = s.input(&baseline);
            crate::policy::decide(&s.registry, &input, &[])
        };
        assert_eq!(decision.selected.name(), "Stop", "off-track {off_track}");
        assert!(
            !decision
                .invariants
                .contains(&crate::directive::Invariant::UnresolvedDependency)
        );
    }
}

#[test]
fn with_the_ensemble_disabled_the_dependency_invariant_never_fires() {
    let mut s = Scenario::finish_ready()
        .with_worker()
        .set(HUMAN, 0.75)
        .set(STUCK, 0.97);
    s.settings.enabled = false;
    // Disabled, every value is decisive and permitted at `p >= t`: needs_human 0.75 is
    // below 0.80, so the Stop goes ahead, exactly as before the ensemble.
    let decision = {
        let baseline = s.baseline();
        let input = s.input(&baseline);
        crate::policy::decide(&s.registry, &input, &[])
    };
    assert_eq!(decision.selected.name(), "Stop");
    assert!(
        !decision
            .invariants
            .contains(&crate::directive::Invariant::UnresolvedDependency)
    );
}

// ---- a rule about to act is the rule whose proposal is selected ------------------------

#[tokio::test]
async fn no_opinion_is_bought_for_a_finish_that_is_outranked_or_refused() {
    // Integrated review, blocker 2 (probe `probe_opinions_bought_for_a_finish_that_cannot_
    // be_selected`). "A rule about to act" is the rule whose proposal arbitration selects.
    // An earlier revision counted any proposal that would disappear if the check were held,
    // so every cycle with a Finish proposed — outranked, or refused by an invariant — bought
    // an opinion on each of its conjuncts: four or five calls a cycle on a Finish that could
    // not happen.
    //
    // Outranked: documentation at 0.30 starts a worker (750) over the Finish (700). Only
    // documentation's own rule is about to act, so only it is asked about.
    let s = Scenario::finish_ready().set(DOCS, 0.30).reframable_all();
    assert_eq!(s.select(&s.baseline()).name(), "StartWorker");
    assert!(
        s.proposals(&s.baseline())
            .iter()
            .any(|p| matches!(p.directive, Directive::Finish { .. }))
    );
    let plan = s.plan();
    let source = answering_by(&plan, |p| Some(1.0 - p));
    let outcome = s
        .resolve_plan(&plan, &source)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let asked: Vec<CheckKey> = source
        .reframe_calls()
        .into_iter()
        .map(|r| r.opinion.check)
        .collect();
    assert_eq!(asked, vec![k(DOCS)], "only the rule about to act");
    for key in [READY, REQS, TESTS, NEEDS_VER] {
        assert!(
            !s.check_plan(key).is_some_and(|p| p.rule_would_fire()),
            "{key}: the outranked Finish is not about to act"
        );
    }
    assert_eq!(s.decide(&outcome).selected.name(), "StartWorker");

    // Refused: no worker has run, so `NoWorkAttempted` refuses the Finish. Nothing is about
    // to act on its conjuncts, and nothing is bought.
    let mut s = Scenario::finish_ready().reframable_all();
    s.counters.workers_run = 0;
    assert_ne!(s.select(&s.baseline()).name(), "Finish");
    let plan = s.plan();
    let source = answering_by(&plan, |p| Some(1.0 - p));
    s.resolve_plan(&plan, &source)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert!(
        source.reframe_calls().is_empty(),
        "{:?}",
        source.reframe_calls()
    );

    // Refused by uncertainty: `needs_human` in band holds every Finish.
    let s = Scenario::finish_ready().set(HUMAN, 0.75).reframable_all();
    let plan = s.plan();
    let source = answering_by(&plan, |p| Some(1.0 - p));
    s.resolve_plan(&plan, &source)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert!(
        source.reframe_calls().is_empty(),
        "{:?}",
        source.reframe_calls()
    );

    // The selected Finish is about to act, and its conjuncts are asked about.
    let s = Scenario::finish_ready().reframable_all();
    for key in [READY, REQS, TESTS, NEEDS_VER] {
        assert!(
            s.check_plan(key).is_some_and(|p| p.rule_would_fire()),
            "{key}"
        );
    }
}

// ---- a spent budget is not a dead end -------------------------------------------------

/// One cycle of `s` over `counters`, every opinion answered consistently.
async fn budget_cycle(
    s: &mut Scenario,
    cycle: u64,
    counters: &EnsembleCounters,
) -> (EnsembleOutcome, Directive, usize) {
    s.cycle = AssessmentSeq(cycle);
    s.ensemble = counters.clone();
    let plan = s.plan();
    let source = answering_by(&plan, |p| Some(1.0 - p));
    let outcome = s
        .resolve_plan(&plan, &source)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let selected = s.decide(&outcome).selected;
    (outcome, selected, source.reframe_calls().len())
}

#[tokio::test]
async fn recheck_on_a_spent_budget_authorises_enough_opinions_to_check_the_finish() {
    // Integrated review, blocker 2(b). With the budget spent, an executed Finish is held and
    // put to a person. A person may authorise cost, never an outcome: `Recheck` (or
    // `ClaimComplete`) tops the budget up by the number of checks the escalation lists —
    // every check the Finish needs an opinion on — once. The Finish still needs a fresh,
    // confirming opinion on each; §2.2 is untouched. `Accept` keeps holding.
    let mut s = Scenario::finish_ready().reframable_all();
    let spent = EnsembleCounters {
        opinions_used: s.settings.max_opinions_per_run,
        ..EnsembleCounters::default()
    };
    let (outcome, selected, bought) = budget_cycle(&mut s, 20, &spent).await;
    assert_eq!(bought, 0);
    assert!(outcome.is_escalation(&selected), "{selected:?}");
    assert_eq!(outcome.opinion_topup, 6, "every check the Finish rests on");
    let (_, ask) = outcome
        .escalations()
        .next()
        .unwrap_or_else(|| panic!("an ask"));
    assert!(
        ask.contains("Recheck or ClaimComplete authorises 6 more opinions"),
        "{ask}"
    );
    assert!(ask.contains("Accept keeps them held"), "{ask}");

    // Accept: held, asked again, nothing bought, nothing granted.
    let mut accepted = outcome.counters.clone();
    let checks = outcome.escalated_checks();
    accepted.record_human_decision(&checks, &crate::assessment::HumanDecision::Accept);
    assert_eq!(
        accepted.record_budget_answer(
            outcome.opinion_topup,
            &crate::assessment::HumanDecision::Accept
        ),
        0
    );
    let (again, selected, bought) = budget_cycle(&mut s, 21, &accepted).await;
    assert_eq!(bought, 0);
    assert!(again.is_escalation(&selected));

    // Recheck: six more opinions, once; the next cycle checks the Finish and it finishes.
    for decision in [
        crate::assessment::HumanDecision::Recheck,
        crate::assessment::HumanDecision::ClaimComplete,
    ] {
        let mut answered = outcome.counters.clone();
        answered.record_human_decision(&checks, &decision);
        assert_eq!(
            answered.record_budget_answer(outcome.opinion_topup, &decision),
            6
        );
        assert_eq!(
            answered.opinion_budget(&s.settings),
            s.settings.max_opinions_per_run + 6
        );
        let (checked, selected, bought) = budget_cycle(&mut s, 21, &answered).await;
        assert_eq!(bought, 6, "{decision:?}");
        assert_eq!(selected.name(), "Finish", "{decision:?}");
        assert_irreversible_guarded(&s, &checked, "topped up");
    }
}

#[tokio::test]
async fn a_top_up_of_only_the_held_checks_would_never_let_the_finish_through() {
    // Why the top-up is every check the held directive needs, not only those the budget
    // happened to hold. An irreversible opinion is always bought afresh, so a Finish needs
    // all six again on the cycle after the answer. Topping up by the held count alone (4,
    // the read set the ladder held; then 2, the inhibitors the gate held; then 2…) leaves
    // the next attempt short every time: the run would ask a person forever.
    let mut s = Scenario::finish_ready().reframable_all();
    let mut counters = EnsembleCounters {
        opinions_used: s.settings.max_opinions_per_run,
        ..EnsembleCounters::default()
    };
    for cycle in 20..28 {
        let (outcome, selected, _) = budget_cycle(&mut s, cycle, &counters).await;
        assert_ne!(selected.name(), "Finish", "cycle {cycle}");
        let held = outcome
            .records
            .iter()
            .filter(|r| {
                r.limits
                    .iter()
                    .any(|l| matches!(l, EnsembleLimit::OpinionBudget { .. }))
            })
            .count();
        assert!(held > 0 && held < 6, "cycle {cycle}: {held} held");
        counters = outcome.counters;
        counters.opinion_topup += u32::try_from(held).unwrap_or(u32::MAX);
    }
}

#[tokio::test]
async fn the_cooldown_is_counted_in_cycles_not_assessment_ids() {
    // Integrated review, finding 5. Cycle ids are assessment numbers, and a refresh's or a
    // reframe's model call takes one too, so ids advance by two or more a cycle. A cooldown
    // measured in ids lasted half as many cycles as configured. A Verify acting on
    // `needs_verification` buys a (non-irreversible) opinion, which confirms; for the next
    // `per_check_cooldown_cycles` cycles it covers, whatever ids those cycles have.
    let mut counters = EnsembleCounters::default();
    let mut bought = Vec::new();
    for i in 0..11u64 {
        let mut s = Scenario::finish_ready()
            .set(NEEDS_VER, 0.90)
            .reframable(&[NEEDS_VER]);
        // Each cycle's id two past the last: its reframe took an assessment number.
        s.cycle = AssessmentSeq(20 + 2 * i);
        s.ensemble = counters.clone();
        assert_eq!(s.select(&s.baseline()).name(), "Verify");
        let plan = s.plan();
        let source = answering_by(&plan, |p| Some(1.0 - p));
        let outcome = s
            .resolve_plan(&plan, &source)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        if !source.reframe_calls().is_empty() {
            bought.push(i);
        }
        counters = outcome.counters;
    }
    let window = EnsembleSettings::default().per_check_cooldown_cycles;
    assert_eq!(window, 10);
    assert_eq!(bought, vec![0, 10], "once, then again after ten cycles");
}
