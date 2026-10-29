//! The ensemble's tests (`docs/06-ensemble.md` §14).

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Duration;

use time::macros::datetime;

use super::{
    CheckPlan, DecisiveDirectives, EnsembleContext, EnsembleCounters, EnsemblePlan,
    EnsembleSettings, EnsembleTrigger,
};
use crate::assessment::{AnswerValue, ResolvedCheck, Rung};
use crate::definitions::DefinitionSet;
use crate::directive::{Directive, Proposal};
use crate::evidence::{CommandProvider, EvidencePlan, ProviderCwd, RunWhen};
use crate::ids::{AssessmentSeq, CheckKey, EvidenceId, ResponsibilityId, RunId, WorkerId};
use crate::job::{FactoryStatus, JobSummary, RunConfig, TaskKind, WorkerKind};
use crate::log::BoundedLog;
use crate::observation::{Counters, GitEvidence, Observation};
use crate::policy::{PolicyInput, arbitrate};
use crate::responsibility::{Check, ResponsibilityRegistry};
use crate::worker::WorkerRecord;

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

// ---- triggers --------------------------------------------------------------

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
