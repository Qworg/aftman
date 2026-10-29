//! The ensemble's tests (`docs/06-ensemble.md` §14). Offline: every answer comes from a
//! [`RecordedOpinionSource`].

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Duration;

use time::macros::datetime;

use super::{
    CheckCounters, CheckPlan, DecisiveDirectives, EnsembleContext, EnsembleCounters, EnsembleLimit,
    EnsembleOutcome, EnsemblePlan, EnsembleSettings, EnsembleTrigger, OpinionKey, OpinionMark,
    ProviderRun, ReframeAnswer, RefreshOutcome, apply_opinions, reframe_verdict,
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
use crate::policy::{PolicyInput, arbitrate};
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
            ensemble: EnsembleCounters::default(),
            reframe: BTreeSet::new(),
            cycle: CYCLE,
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
            .consecutive_held = cycles;
        self
    }

    fn thresholds(&self) -> BTreeMap<CheckKey, Option<f32>> {
        self.registry.thresholds().into_iter().collect()
    }

    /// The baseline, exactly as `resolve_checks` would build it.
    fn baseline(&self) -> BTreeMap<CheckKey, ResolvedCheck> {
        let thresholds = self.thresholds();
        self.values
            .iter()
            .map(|(key, p)| {
                let threshold = thresholds.get(key).copied().flatten();
                (
                    key.clone(),
                    ResolvedCheck::from_measurement(
                        key.clone(),
                        *p,
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

    fn proposals(&self, checks: &BTreeMap<CheckKey, ResolvedCheck>) -> Vec<Proposal> {
        self.registry.propose(&self.input(checks))
    }

    fn select(&self, checks: &BTreeMap<CheckKey, ResolvedCheck>) -> Directive {
        let input = self.input(checks);
        arbitrate(&input, &self.registry.propose(&input)).selected
    }

    fn plan(&self) -> EnsemblePlan {
        let baseline = self.baseline();
        let input = self.input(&baseline);
        let propose = |input: &PolicyInput<'_>| self.registry.propose(input);
        EnsemblePlan::build(&EnsembleContext {
            settings: &self.settings,
            cycle: self.cycle,
            policy: &input,
            propose: &propose,
            definitions: &self.definitions,
            evidence: &self.evidence,
            answers: &self.answers,
            counters: &self.ensemble,
            reframe_available: &self.reframe,
        })
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
    plan.triggered
        .iter()
        .fold(RecordedOpinionSource::new(), |source, check| {
            source.with_reframe(
                OpinionKey {
                    cycle: plan.cycle,
                    check: check.check.clone(),
                    rung: Rung::ReframeSameModel,
                },
                answer(p_negated),
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
    let outcome = plan.resolve(&source).await;
    assert_eq!(outcome.checks, s.baseline());
    assert_eq!(outcome.counters, EnsembleCounters::default());
    assert!(source.refresh_calls().is_empty() && source.reframe_calls().is_empty());
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
    let outcome = plan.resolve(&source).await;
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
    let outcome = s.plan().resolve(&source).await;
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
    let outcome = s.plan().resolve(&source).await;
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
        outcome.counters.check(&k(TESTS)).consecutive_held,
        1,
        "held cycles are counted"
    );

    // After `consecutive_contests_before_human` held cycles: escalate, still no model.
    let s = s.held_for(TESTS, 2);
    let source = RecordedOpinionSource::new();
    let outcome = s.plan().resolve(&source).await;
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
    let outcome = s.plan().resolve(&source).await;
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
    let outcome = s.plan().resolve(&source).await;
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
    let no_refresh = s.plan().resolve(&RecordedOpinionSource::new()).await;
    assert_ne!(s.select(&no_refresh.checks).name(), "Finish");

    let s = s.without_rung(Rung::RefreshEvidence);
    let source = answering_all(&s.plan(), Some(0.02));
    let outcome = s.plan().resolve(&source).await;
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
        },
    );
    let outcome = s.plan().resolve(&source).await;
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
    let outcome = s.plan().resolve(&source).await;
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
                let outcome = plan.resolve(&source).await;
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

    // Decisive, triggered, but no rule acts on it: still nothing bought.
    let s = Scenario::finish_ready().reframable_all();
    let plan = s.plan();
    let source = answering_all(&plan, Some(0.5));
    let outcome = plan.resolve(&source).await;
    for record in &outcome.records {
        let would_fire = plan
            .triggered
            .iter()
            .find(|p| p.check == record.check)
            .is_some_and(|p| p.rule_would_fire);
        if !would_fire {
            assert!(record.opinions.is_empty(), "{} is inert", record.check);
        }
    }
    let asked: BTreeSet<CheckKey> = source
        .reframe_calls()
        .into_iter()
        .map(|r| r.opinion.check)
        .collect();
    assert!(!asked.contains(&k(STUCK)) && !asked.contains(&k(DRIFT)));
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
        let outcome = s.plan().resolve(&source).await;
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

#[tokio::test]
async fn the_opinion_budget_holds_rather_than_acting_unchecked() {
    let mut s = Scenario::finish_ready()
        .set(TESTS, 0.92)
        .reframable(&[TESTS]);
    s.ensemble.opinions_used = s.settings.max_opinions_per_run;
    let source = RecordedOpinionSource::new();
    let outcome = s.plan().resolve(&source).await;
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
async fn the_cooldown_holds_after_a_contest_and_does_not_re_ask_after_a_confirm() {
    for contested in [true, false] {
        let mut s = Scenario::finish_ready()
            .set(TESTS, 0.92)
            .reframable(&[TESTS]);
        s.ensemble.checks.insert(
            k(TESTS),
            CheckCounters {
                last_opinion: Some(OpinionMark {
                    cycle: AssessmentSeq(CYCLE.get() - 3),
                    contested,
                }),
                ..CheckCounters::default()
            },
        );
        let source = RecordedOpinionSource::new();
        let outcome = s.plan().resolve(&source).await;
        assert!(source.reframe_calls().is_empty(), "one opinion per window");
        assert!(record(&outcome, TESTS).is_some_and(|r| r.limits.contains(
            &EnsembleLimit::Cooldown {
                last_opinion: AssessmentSeq(CYCLE.get() - 3),
                cooldown_cycles: 10,
                held: contested,
            }
        )));
        assert_eq!(outcome.checks[&k(TESTS)].permitted, !contested);
    }
    // Once the window has passed, it is asked again.
    let mut s = Scenario::finish_ready()
        .set(TESTS, 0.92)
        .reframable(&[TESTS]);
    s.ensemble.checks.insert(
        k(TESTS),
        CheckCounters {
            last_opinion: Some(OpinionMark {
                cycle: AssessmentSeq(CYCLE.get() - 10),
                contested: true,
            }),
            ..CheckCounters::default()
        },
    );
    let source =
        RecordedOpinionSource::new().with_reframe(reframe_key(CYCLE, TESTS), answer(Some(0.08)));
    let outcome = s.plan().resolve(&source).await;
    assert_eq!(source.reframe_calls().len(), 1);
    assert!(outcome.checks[&k(TESTS)].permitted, "consistent: confirmed");
}

#[tokio::test]
async fn a_repeated_contest_escalates_instead_of_asking_again() {
    let s = Scenario::finish_ready()
        .set(TESTS, 0.92)
        .reframable(&[TESTS])
        .held_for(TESTS, 2);
    let source = RecordedOpinionSource::new();
    let outcome = s.plan().resolve(&source).await;
    assert!(source.reframe_calls().is_empty());
    let rc = &outcome.checks[&k(TESTS)];
    assert!(!rc.permitted);
    assert_eq!(outcome.escalations().count(), 1);
    assert_ne!(s.select(&outcome.checks).name(), "Finish");

    // Without a human rung there is nobody to escalate to: the opinion is asked.
    let s = s.without_rung(Rung::Human);
    let source =
        RecordedOpinionSource::new().with_reframe(reframe_key(CYCLE, TESTS), answer(Some(0.08)));
    let outcome = s.plan().resolve(&source).await;
    assert_eq!(source.reframe_calls().len(), 1);
    assert_eq!(outcome.escalations().count(), 0);
}

#[tokio::test]
async fn an_unusable_opinion_on_an_irreversible_action_holds_it() {
    let s = Scenario::finish_ready()
        .set(TESTS, 0.92)
        .reframable(&[TESTS]);
    let outcome = s.plan().resolve(&RecordedOpinionSource::new()).await;
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
    let outcome = s.plan().resolve(&source).await;
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
    Consistent,
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
    if has(EnsembleTrigger::RepeatContest) && rung(Rung::Human) {
        return with(done(Kind::Escalated, false));
    }
    if !(rung(Rung::ReframeSameModel) && c.reframe_available) {
        return with(done(Kind::Decisive, true));
    }
    match c.counters {
        CounterState::CooldownAfterContest => return with(done(Kind::Unresolved, false)),
        CounterState::CooldownAfterConfirm => return with(done(Kind::Decisive, true)),
        CounterState::BudgetSpent => return with(done(Kind::Unresolved, false)),
        CounterState::Fresh | CounterState::RefreshCapped => {}
    }
    let contest = match c.reframe {
        ReframeKind::Consistent => false,
        ReframeKind::Inconsistent => true,
        ReframeKind::Unusable => has(EnsembleTrigger::Irreversible),
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
                                        run_case(&case).await;
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

async fn run_case(c: &Case) {
    let key = k(TESTS);
    let baseline = ResolvedCheck::from_measurement(key.clone(), Some(c.p), Some(T), M, true);
    let mut check = CheckCounters::default();
    let mut counters = EnsembleCounters::default();
    match c.counters {
        CounterState::Fresh => {}
        CounterState::RefreshCapped => check.refreshes = 3,
        CounterState::CooldownAfterContest | CounterState::CooldownAfterConfirm => {
            check.last_opinion = Some(OpinionMark {
                cycle: AssessmentSeq(CYCLE.get() - 1),
                contested: matches!(c.counters, CounterState::CooldownAfterContest),
            });
        }
        CounterState::BudgetSpent => counters.opinions_used = 20,
    }
    counters.checks.insert(key.clone(), check);
    let plan = EnsemblePlan {
        cycle: CYCLE,
        settings: EnsembleSettings {
            enabled: true,
            rungs: c.rungs.clone(),
            ..EnsembleSettings::default()
        },
        counters,
        baseline: BTreeMap::from([(key.clone(), baseline.clone())]),
        triggered: vec![CheckPlan {
            check: key.clone(),
            baseline: baseline.clone(),
            triggers: c.triggers.clone(),
            rule_would_fire: c.rule_would_fire && baseline.permitted,
            refresh_providers: if c.providers {
                vec![EvidenceId::new("tests")]
            } else {
                Vec::new()
            },
            reframe_available: c.reframe_available,
            decisive_directives: DecisiveDirectives {
                if_above: "Finish".into(),
                if_below: "StartWorker".into(),
            },
        }],
    };
    let new_p = match c.refresh {
        RefreshAnswer::Decisive => Some(0.05),
        RefreshAnswer::InBand => Some(0.70),
        RefreshAnswer::Failed => None,
    };
    let p_negated = match c.reframe {
        // 1 - p(not X) equals p exactly: consistent.
        ReframeKind::Consistent => Some(1.0 - c.p),
        ReframeKind::Inconsistent => Some(c.p),
        ReframeKind::Unusable => None,
    };
    let source = RecordedOpinionSource::new()
        .with_refresh(CYCLE, TESTS, refreshed(new_p, "tests", 0))
        .with_reframe(reframe_key(CYCLE, TESTS), answer(p_negated));
    let outcome = plan.resolve(&source).await;
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
                    let with = plan.resolve(&answering_all(&plan, q)).await;
                    let s = s.without_rung(Rung::ReframeSameModel);
                    let without = s.plan().resolve(&RecordedOpinionSource::new()).await;
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
                if let Some(p) = refresh {
                    source = source.with_refresh(CYCLE, TESTS, refreshed(Some(p), "tests", 0));
                }
                let outcome = plan.resolve(&source).await;
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
                                let outcome = plan.resolve(&answering_all(&plan, q)).await;
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
            let outcome = plan.resolve(&answering_all(&plan, q)).await;
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
        let outcome = s.plan().resolve(&RecordedOpinionSource::new()).await;
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
        let outcome = s.plan().resolve(&RecordedOpinionSource::new()).await;
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
    let live_source = RecordedOpinionSource::new()
        .with_refresh(AssessmentSeq(20), TESTS, refreshed(Some(0.08), "tests", 1))
        .with_reframe(reframe_key(AssessmentSeq(21), TESTS), answer(Some(0.75)))
        .with_reframe(reframe_key(AssessmentSeq(22), STUCK), answer(Some(0.04)))
        .with_reframe(reframe_key(AssessmentSeq(22), DOCS), answer(Some(f32::NAN)));

    let mut counters = EnsembleCounters::default();
    let mut live = Vec::new();
    let mut events = Vec::new();
    let mut seq = 0;
    for (i, mut s) in cycles.into_iter().enumerate() {
        s.cycle = AssessmentSeq(20 + i as u64);
        s.ensemble = counters.clone();
        let outcome = s.plan().resolve(&live_source).await;
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
        let replayed = s.plan().resolve(&replay_source).await;
        assert_eq!(replayed, live[i], "cycle {i}");
        assert_eq!(
            serde_json::to_vec(&replayed)?,
            serde_json::to_vec(&live[i])?,
            "cycle {i} is not byte-identical"
        );
        counters = replayed.counters;
    }
    assert_eq!(
        replay_source.refresh_calls().len(),
        live_source.refresh_calls().len()
    );
    assert_eq!(
        replay_source.reframe_calls(),
        live_source.reframe_calls(),
        "the replay asked exactly what the live run asked"
    );
    Ok(())
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
