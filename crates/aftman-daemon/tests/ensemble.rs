//! Ensemble stage S1 through the runtime (`docs/06-ensemble.md` §4 R0, §6, §13).
//!
//! Offline: a simulated or scripted worker, a model that answers from the state it was
//! shown, and command providers made of `sh -c` one-liners. Every run is bounded by a
//! timeout, and every run driven with a control channel is aborted if the test fails
//! before it ends (`RunGuard`), so nothing outlives its test.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use tokio::task::JoinHandle;

use aftman_core::assessment::{AnswerValue, HumanDecision, Resolution};
use aftman_core::clock::SystemClock;
use aftman_core::definitions::{DefinitionSet, ResponsibilityDefinition};
use aftman_core::ensemble::{EnsembleResolvedPayload, EnsembleSettings, EnsembleTrigger};
use aftman_core::error::ModelError;
use aftman_core::event::{EventEnvelope, EventKind};
use aftman_core::evidence::EvidencePlan;
use aftman_core::ids::{CheckKey, EscalationId, ResponsibilityId};
use aftman_core::job::{FactoryStatus, Job, JobSource, RunConfig, WorkerKind};
use aftman_core::model::{
    AskContext, Budget, DecisionModel, ModelAnswers, Purpose, Question, Usage,
};
use aftman_core::responsibility::ResponsibilityRegistry;
use aftman_core::testing::{RecordedOpinionSource, ScriptedModel, SimulatedModel};
use aftman_core::worker::Worker;
use aftman_daemon::{
    EnsembleCycle, FactoryRuntime, RunControl, RunOutcome, RuntimeDeps, RuntimeError, VecSink,
};
use aftman_store::{FileRunStore, RunStore};
use aftman_workers::SimulatedWorker;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const TESTS: &str = "core.verification__tests_sufficient";
const STUCK: &str = "core.worker-health__worker_stuck";
const OFF_TRACK: &str = "core.worker-health__work_off_track";

/// The longest any run in this file may take.
const PATIENCE: Duration = Duration::from_secs(60);

// ---- doubles ------------------------------------------------------------------

/// Everything finished and nobody needed, except what `tests` says.
const FINISH_READY: [(&str, f32); 9] = [
    ("core.completion__ready_to_finish", 0.95),
    ("core.completion__implementation_complete", 0.95),
    ("core.completion__requirements_satisfied", 0.95),
    ("core.verification__needs_verification", 0.10),
    ("core.worker-health__worker_stuck", 0.02),
    ("core.worker-health__work_off_track", 0.02),
    ("core.human-escalation__needs_human", 0.01),
    ("repository.instructions__agents_md_drift", 0.01),
    ("core.worker-health__meaningful_progress", 0.60),
];

type Rule = Box<dyn Fn(&str) -> f32 + Send + Sync>;

/// A model that answers `tests_sufficient` from the text of the state it was shown, and
/// everything else from a table. Answering from the state is what proves a refresh
/// re-asked over the *fresh* observation.
struct StateModel {
    table: BTreeMap<CheckKey, f32>,
    tests: Rule,
    calls: Mutex<Vec<(Purpose, Vec<CheckKey>, String)>>,
}

impl StateModel {
    fn new(tests: impl Fn(&str) -> f32 + Send + Sync + 'static) -> Self {
        Self {
            table: FINISH_READY
                .iter()
                .map(|(key, p)| (CheckKey::from(*key), *p))
                .collect(),
            tests: Box::new(tests),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn with(mut self, key: &str, p: f32) -> Self {
        self.table.insert(CheckKey::from(key), p);
        self
    }

    fn calls(&self, purpose: Purpose) -> Vec<(Vec<CheckKey>, String)> {
        self.calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|(p, _, _)| *p == purpose)
            .map(|(_, keys, state)| (keys.clone(), state.clone()))
            .collect()
    }
}

#[async_trait]
impl DecisionModel for StateModel {
    async fn ask(
        &self,
        context: &AskContext,
        state: &Value,
        questions: &BTreeMap<CheckKey, Question>,
        _budget: &Budget,
    ) -> Result<ModelAnswers, ModelError> {
        let text = state.to_string();
        self.calls.lock().unwrap_or_else(|e| e.into_inner()).push((
            context.purpose,
            questions.keys().cloned().collect(),
            text.clone(),
        ));
        let mut answers = BTreeMap::new();
        for key in questions.keys() {
            let p = if *key == CheckKey::from(TESTS) {
                Some((self.tests)(&text))
            } else {
                self.table.get(key).copied()
            };
            if let Some(p) = p {
                answers.insert(key.clone(), AnswerValue::Noul(p));
            }
        }
        Ok(ModelAnswers {
            model: "state-model".into(),
            usage: Usage {
                input_tokens: Some(10),
                output_tokens: Some(1),
            },
            answers,
            request_id: None,
            attempts: 1,
        })
    }
}

/// The worker printed "3 passed" (`SimulatedWorker` does), and nothing has measured it.
fn claim_only(text: &str) -> bool {
    text.contains("3 passed") && !text.contains("PROVIDER")
}

// ---- fixture ------------------------------------------------------------------

/// The bundled definitions, with `tests_sufficient` selecting the `tests` provider.
fn definitions() -> Result<DefinitionSet, Box<dyn std::error::Error>> {
    let mut set = DefinitionSet::bundled()?;
    let verification = ResponsibilityDefinition::parse(
        "core.verification",
        r#"
enabled = true
always = true
title = "Verification"

[checks.tests_sufficient]
instructions = "Does the work have sufficient relevant test coverage and passing verification?"
min_threshold = 0.75
evidence = ["worker_output", "test_summary", "command:tests"]

[checks.needs_verification]
instructions = "Does the current state warrant an independent verification pass before finishing?"
min_threshold = 0.65
"#,
        "test",
    )?;
    set.definitions
        .insert("core.verification".to_owned(), verification);
    Ok(set)
}

fn provider(run_when: &str, script: &str) -> Result<EvidencePlan, Box<dyn std::error::Error>> {
    Ok(EvidencePlan::from_toml(
        &format!(
            r#"
[[evidence.commands]]
id = "tests"
argv = ["sh", "-c", "{script}"]
run_when = "{run_when}"
success_exit_codes = [0]
"#
        ),
        "test",
    )?)
}

fn settings(tweak: impl FnOnce(&mut EnsembleSettings)) -> EnsembleSettings {
    let mut settings = EnsembleSettings {
        enabled: true,
        ..EnsembleSettings::default()
    };
    tweak(&mut settings);
    settings
}

fn config(tweak: impl FnOnce(&mut RunConfig)) -> RunConfig {
    let mut config = RunConfig {
        worker: WorkerKind::Simulated,
        min_assessment_interval: Duration::from_millis(1),
        idle_poll_interval: Duration::from_millis(10),
        overall_timeout: Duration::from_secs(60),
        worker_timeout: Duration::from_secs(10),
        max_workers: 30,
        max_attempts: 30,
        ..RunConfig::default()
    };
    tweak(&mut config);
    config
}

struct Fixture {
    dir: tempfile::TempDir,
    repo: PathBuf,
    store: Arc<dyn RunStore>,
    sink: Arc<VecSink>,
    cycles: Arc<Mutex<Vec<EnsembleCycle>>>,
    definitions: DefinitionSet,
    evidence: EvidencePlan,
}

impl Fixture {
    fn new(
        definitions: DefinitionSet,
        evidence: EvidencePlan,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let repo = dir.path().join("repo");
        std::fs::create_dir_all(&repo)?;
        let store: Arc<dyn RunStore> = Arc::new(FileRunStore::new(dir.path().join("state")));
        Ok(Self {
            dir,
            repo,
            store,
            sink: Arc::new(VecSink::new()),
            cycles: Arc::new(Mutex::new(Vec::new())),
            definitions,
            evidence,
        })
    }

    fn runtime(
        &self,
        config: RunConfig,
        settings: EnsembleSettings,
        model: Arc<dyn DecisionModel>,
        worker: Arc<dyn Worker>,
    ) -> FactoryRuntime {
        let temp = self.dir.path().join("evidence-tmp");
        let _ = std::fs::create_dir_all(&temp);
        let cycles = Arc::clone(&self.cycles);
        FactoryRuntime::new(RuntimeDeps {
            config,
            store: Arc::clone(&self.store),
            model,
            worker,
            registry: ResponsibilityRegistry::builtin(),
            definitions: self.definitions.clone(),
            evidence: self.evidence.clone(),
            evidence_temp_dir: temp,
            clock: Arc::new(SystemClock),
            sink: Arc::clone(&self.sink) as Arc<dyn aftman_daemon::EventSink>,
            ensemble: settings,
            steering_grace: Duration::from_secs(30),
            launch: Default::default(),
            forwarded_env: Default::default(),
            jev_budget: Default::default(),
        })
        .with_ensemble_tap(Arc::new(move |cycle: &EnsembleCycle| {
            cycles
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(cycle.clone());
        }))
    }

    fn job(&self) -> Job {
        Job {
            prompt: "Add request retries with backoff, and cover them with tests.".into(),
            repo: self.repo.clone(),
            source: JobSource::Cli,
        }
    }

    async fn run(&self, rt: FactoryRuntime) -> Result<RunOutcome, Box<dyn std::error::Error>> {
        Ok(tokio::time::timeout(PATIENCE, rt.run(self.job()))
            .await
            .map_err(|_| "the run did not end")??)
    }

    fn events(&self) -> Vec<EventEnvelope> {
        self.sink.events()
    }

    fn of_kind(&self, kind: EventKind) -> Vec<EventEnvelope> {
        self.sink.of_kind(kind)
    }

    fn cycles(&self) -> Vec<EnsembleCycle> {
        self.cycles
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Every `ENSEMBLE_RESOLVED` with a cycle, decoded.
    fn resolutions(&self) -> Vec<(u64, EnsembleResolvedPayload)> {
        self.of_kind(EventKind::EnsembleResolved)
            .iter()
            .filter_map(|event| {
                serde_json::from_value::<EnsembleResolvedPayload>(event.payload.clone())
                    .ok()
                    .map(|payload| (event.seq.get(), payload))
            })
            .filter(|(_, payload)| payload.cycle.is_some())
            .collect()
    }

    /// The refresh records for `check`, in order.
    fn refreshes(&self, check: &str) -> Vec<(u64, aftman_core::ensemble::CheckResolvedPayload)> {
        self.resolutions()
            .into_iter()
            .flat_map(|(seq, payload)| {
                payload
                    .checks
                    .into_iter()
                    .filter(|entry| entry.check == CheckKey::from(check) && entry.refresh.is_some())
                    .map(move |entry| (seq, entry))
            })
            .collect()
    }

    /// The directive selected first after event `seq`.
    fn selected_after(&self, seq: u64) -> Option<String> {
        self.of_kind(EventKind::DirectiveSelected)
            .into_iter()
            .find(|event| event.seq.get() > seq)
            .and_then(|event| event.payload["directive"].as_str().map(ToOwned::to_owned))
    }
}

/// A run driven with a control channel. Aborted on drop, so a failing assertion never
/// leaves it running.
struct RunGuard {
    control: RunControl,
    task: Option<JoinHandle<Result<RunOutcome, RuntimeError>>>,
}

impl Drop for RunGuard {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl RunGuard {
    async fn launch(fx: &Fixture, rt: FactoryRuntime) -> Result<Self, Box<dyn std::error::Error>> {
        let rt = Arc::new(rt);
        let started = rt.start(fx.job()).await?;
        let (control, receiver) = RunControl::channel();
        let task = tokio::spawn(async move { rt.drive(started, Some(receiver)).await });
        Ok(Self {
            control,
            task: Some(task),
        })
    }

    async fn answer(&self, id: EscalationId, decision: HumanDecision) -> TestResult {
        self.control.answer(id, decision, "", "test").await?;
        Ok(())
    }

    async fn join(&mut self) -> Result<RunOutcome, Box<dyn std::error::Error>> {
        let task = self.task.take().ok_or("already joined")?;
        Ok(tokio::time::timeout(PATIENCE, task)
            .await
            .map_err(|_| "the run did not end")???)
    }

    async fn cancel(&mut self) -> Result<RunOutcome, Box<dyn std::error::Error>> {
        // A run that already ended answers `RunEnded`; either way it is joined.
        let _ = self.control.cancel("test done").await;
        self.join().await
    }
}

/// Wait until `condition` holds over the run's events.
async fn until(fx: &Fixture, what: &str, condition: impl Fn(&[EventEnvelope]) -> bool) {
    let deadline = tokio::time::Instant::now() + PATIENCE;
    loop {
        if condition(&fx.events()) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "gave up waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn count(events: &[EventEnvelope], kind: EventKind) -> usize {
    events.iter().filter(|event| event.kind == kind).count()
}

/// The id of the `nth` (1-based) escalation, once the run is parked on it.
async fn escalation(fx: &Fixture, nth: usize) -> Result<EscalationId, Box<dyn std::error::Error>> {
    until(fx, "an escalation", |events| {
        count(events, EventKind::EscalationRaised) >= nth
            && events
                .iter()
                .rev()
                .find(|e| e.kind == EventKind::StateChanged)
                .is_some_and(|e| e.payload["to"] == "needs_human")
    })
    .await;
    let raised = fx.of_kind(EventKind::EscalationRaised);
    Ok(raised[nth - 1].payload["escalation_id"]
        .as_str()
        .ok_or("no escalation id")?
        .parse()?)
}

// ---- disabled: unchanged ------------------------------------------------------

/// A run's decisions, reduced to what policy did: the same projection the fixture
/// `fixtures/disabled_ensemble_trace.txt` was captured with, from the release before the
/// ensemble was wired, by the same code.
fn trace(events: &[EventEnvelope]) -> Vec<String> {
    let mut out = Vec::new();
    for e in events {
        match e.kind {
            EventKind::WorkerOutput | EventKind::AssessmentSkippedUnchanged => {}
            EventKind::DirectiveSelected => out.push(format!(
                "SELECTED {} | {} | {} | {}",
                e.payload["directive"],
                e.payload["reason"],
                e.payload["invariants"],
                e.payload["suppressed"]
            )),
            EventKind::DirectiveProposed => out.push(format!(
                "PROPOSED {} {} {}",
                e.payload["rule"], e.payload["directive"], e.payload["priority"]
            )),
            EventKind::EnsembleResolved => out.push(format!(
                "ENSEMBLE {} {} {} {}",
                e.payload["enabled"],
                e.payload["margin"],
                e.payload["unresolved"],
                e.payload["rungs"]
            )),
            EventKind::AssessmentCompleted => {
                out.push(format!("ASSESSED {}", e.payload["checks"]));
            }
            EventKind::EscalationRaised => out.push(format!(
                "ESCALATION {} {} {}",
                e.payload["kind"], e.payload["reason"], e.payload["ask"]
            )),
            EventKind::StateChanged => out.push(format!(
                "STATE {}->{}",
                e.payload["from"].as_str().unwrap_or("?"),
                e.payload["to"].as_str().unwrap_or("?")
            )),
            other => out.push(other.as_str().to_owned()),
        }
    }
    out
}

async fn disabled_trace(
    model: Arc<dyn DecisionModel>,
    max_workers: u32,
) -> Result<(Vec<String>, Vec<EventEnvelope>), Box<dyn std::error::Error>> {
    let fx = Fixture::new(DefinitionSet::bundled()?, EvidencePlan::default())?;
    let rt = fx.runtime(
        config(|c| {
            c.overall_timeout = Duration::from_secs(30);
            c.max_workers = max_workers;
            c.max_attempts = 2;
        }),
        EnsembleSettings::default(),
        model,
        Arc::new(SimulatedWorker::instant()),
    );
    fx.run(rt).await?;
    Ok((trace(&fx.events()), fx.events()))
}

// Virtual time and an in-process worker, so the trace is the same on every machine.
#[tokio::test(start_paused = true)]
async fn a_disabled_ensemble_selects_exactly_what_it_did_before() -> TestResult {
    let near = ScriptedModel::new("near").with_all(&[
        ("core.completion__ready_to_finish", 0.95),
        ("core.completion__implementation_complete", 0.95),
        ("core.completion__requirements_satisfied", 0.95),
        ("core.verification__tests_sufficient", 0.71),
        ("core.verification__needs_verification", 0.10),
        ("core.worker-health__worker_stuck", 0.02),
        ("core.worker-health__work_off_track", 0.02),
        ("core.human-escalation__needs_human", 0.01),
        ("repository.instructions__agents_md_drift", 0.01),
        ("core.worker-health__meaningful_progress", 0.60),
    ]);
    let (simulated, a) = disabled_trace(Arc::new(SimulatedModel::new()), 5).await?;
    let (near, b) = disabled_trace(Arc::new(near), 3).await?;
    let actual = format!(
        "== simulated\n{}\n== near\n{}\n",
        simulated.join("\n"),
        near.join("\n")
    );
    let expected = include_str!("fixtures/disabled_ensemble_trace.txt");
    assert_eq!(
        actual, expected,
        "a disabled run must decide exactly as before"
    );

    // And it adds no ensemble events: nothing is triggered, refreshed, or asked.
    for events in [&a, &b] {
        for kind in [
            EventKind::EnsembleTriggered,
            EventKind::OpinionRequested,
            EventKind::OpinionRecorded,
            EventKind::EvidenceCollected,
        ] {
            assert_eq!(count(events, kind), 0, "{}", kind.as_str());
        }
        for event in events
            .iter()
            .filter(|e| e.kind == EventKind::EnsembleResolved)
        {
            assert_eq!(event.payload["checks"], serde_json::json!([]));
        }
    }
    Ok(())
}

// ---- the exit criterion: R0 -------------------------------------------------------

#[tokio::test]
async fn the_exit_criterion_a_fake_passing_test_run_is_refreshed_into_a_decisive_value()
-> TestResult {
    // The worker claims "3 passed". Jev, shown only that claim, says 0.71: in band. The
    // configured `tests` provider runs, Jev is asked again over the fresh observation, and
    // says 0.95 — decisive. The run finishes on the measurement, not on the claim.
    let fx = Fixture::new(
        definitions()?,
        provider("on_demand", "echo 'PROVIDER-PASS 17 passed'; exit 0")?,
    )?;
    let model = Arc::new(StateModel::new(|text| {
        if text.contains("PROVIDER-PASS") {
            0.95
        } else if claim_only(text) {
            0.71
        } else {
            0.10
        }
    }));
    let rt = fx.runtime(
        config(|_| {}),
        settings(|_| {}),
        Arc::clone(&model) as Arc<dyn DecisionModel>,
        Arc::new(SimulatedWorker::instant()),
    );
    let outcome = fx.run(rt).await?;
    assert_eq!(outcome.status, FactoryStatus::Finished);
    assert!(
        fx.of_kind(EventKind::Error).is_empty(),
        "the plan and the resolution agreed on one materialised input"
    );

    let refreshes = fx.refreshes(TESTS);
    assert_eq!(refreshes.len(), 1, "one refresh: {refreshes:?}");
    let (seq, entry) = &refreshes[0];
    assert!(entry.triggers.contains(&EnsembleTrigger::NearThreshold));
    assert!(entry.triggers.contains(&EnsembleTrigger::ClaimOnlyEvidence));
    assert_eq!(entry.jev_p, Some(0.71));
    let Resolution::EvidenceRefreshed {
        new_p,
        provider,
        old_p,
        exit_code,
        ..
    } = &entry.resolution
    else {
        panic!("expected evidence_refreshed, got {:?}", entry.resolution);
    };
    assert!((new_p - 0.95).abs() < f32::EPSILON);
    assert_eq!(old_p, &Some(0.71));
    assert_eq!(provider.as_str(), "tests");
    assert_eq!(exit_code, &Some(0));
    assert!(entry.decisive && entry.permitted);
    // The decision follows from the fresh measurement.
    assert_eq!(fx.selected_after(*seq).as_deref(), Some("Finish"));

    // The provider ran exactly once, through the evidence path, and the refresh call was
    // one question, billed under its own purpose, over the fresh evidence.
    let collected = fx.of_kind(EventKind::EvidenceCollected);
    assert_eq!(collected.len(), 1);
    assert_eq!(collected[0].payload["provider"], "tests");
    let refresh_calls = model.calls(Purpose::Refresh);
    assert_eq!(refresh_calls.len(), 1);
    assert_eq!(refresh_calls[0].0, vec![CheckKey::from(TESTS)]);
    assert!(refresh_calls[0].1.contains("PROVIDER-PASS"));
    // ... and it counted against the assessment budget.
    let assessed = fx.of_kind(EventKind::AssessmentCompleted).len() as u32;
    assert_eq!(outcome.assessments, assessed + 1);
    assert_eq!(
        fx.of_kind(EventKind::EnsembleTriggered)
            .iter()
            .filter(|e| e.payload["check"] == TESTS)
            .count(),
        fx.resolutions()
            .iter()
            .filter(|(_, p)| p.checks.iter().any(|c| c.check == CheckKey::from(TESTS)))
            .count(),
        "one ENSEMBLE_TRIGGERED per triggered check per cycle"
    );
    Ok(())
}

#[tokio::test]
async fn worked_example_9_1_a_failing_refresh_keeps_the_run_from_finishing() -> TestResult {
    // The same claim; the command exits 1 with "16 passed, 1 failed"; the fresh value is
    // 0.08, decisive and below. The run must not finish.
    let fx = Fixture::new(
        definitions()?,
        provider(
            "on_demand",
            "echo 'PROVIDER-FAIL 16 passed, 1 failed'; exit 1",
        )?,
    )?;
    let model = Arc::new(StateModel::new(|text| {
        if text.contains("PROVIDER-FAIL") {
            0.08
        } else if claim_only(text) {
            0.71
        } else {
            0.10
        }
    }));
    let rt = fx.runtime(
        config(|c| c.max_workers = 3),
        settings(|_| {}),
        Arc::clone(&model) as Arc<dyn DecisionModel>,
        Arc::new(SimulatedWorker::instant()),
    );
    let outcome = fx.run(rt).await?;
    assert_ne!(outcome.status, FactoryStatus::Finished);
    assert!(
        !fx.of_kind(EventKind::DirectiveSelected)
            .iter()
            .any(|e| e.payload["directive"] == "Finish")
    );

    let refreshes = fx.refreshes(TESTS);
    assert!(!refreshes.is_empty());
    let (seq, entry) = &refreshes[0];
    let Resolution::EvidenceRefreshed {
        new_p, exit_code, ..
    } = &entry.resolution
    else {
        panic!("expected evidence_refreshed, got {:?}", entry.resolution);
    };
    assert!((new_p - 0.08).abs() < f32::EPSILON);
    assert_eq!(exit_code, &Some(1));
    assert!(entry.decisive && entry.permitted, "decisive, and below");
    assert_eq!(fx.selected_after(*seq).as_deref(), Some("StartWorker"));
    Ok(())
}

#[tokio::test]
async fn repeated_refresh_is_capped_and_visible() -> TestResult {
    // A check whose evidence never settles it: every refresh lands in band again. The cap
    // stops the refreshing, the limit is recorded, and the check goes to a person.
    let fx = Fixture::new(
        definitions()?,
        provider("on_demand", "echo 'PROVIDER-PASS 17 passed'; exit 0")?,
    )?;
    let model = Arc::new(StateModel::new(|text| {
        if text.contains("3 passed") || text.contains("PROVIDER") {
            0.71
        } else {
            0.10
        }
    }));
    let rt = fx.runtime(
        config(|_| {}),
        settings(|s| s.max_evidence_refreshes_per_check = 2),
        Arc::clone(&model) as Arc<dyn DecisionModel>,
        Arc::new(SimulatedWorker::instant()),
    );
    let outcome = fx.run(rt).await?;
    assert_eq!(outcome.status, FactoryStatus::NeedsHuman);

    assert_eq!(fx.refreshes(TESTS).len(), 2, "exactly the cap");
    assert_eq!(fx.of_kind(EventKind::EvidenceCollected).len(), 2);
    assert_eq!(model.calls(Purpose::Refresh).len(), 2);
    let capped = fx
        .resolutions()
        .into_iter()
        .flat_map(|(_, p)| p.checks)
        .filter(|c| c.check == CheckKey::from(TESTS))
        .any(|c| {
            c.limits
                .contains(&aftman_core::ensemble::EnsembleLimit::RefreshCap { used: 2, cap: 2 })
        });
    assert!(capped, "the cap is recorded on the check");
    let raised = fx.of_kind(EventKind::EscalationRaised);
    assert_eq!(raised.len(), 1);
    assert_eq!(raised[0].payload["checks"], serde_json::json!([TESTS]));
    Ok(())
}

// ---- finish_is_provisional --------------------------------------------------------

#[tokio::test]
async fn a_finish_is_provisional_exactly_while_before_finish_providers_are_pending() -> TestResult {
    // `before_finish` configured: the first Finish is provisional and is not applied; the
    // provider runs; the re-assessed Finish is not provisional and is applied.
    let fx = Fixture::new(
        DefinitionSet::bundled()?,
        provider("before_finish", "echo audited; exit 0")?,
    )?;
    let rt = fx.runtime(
        config(|_| {}),
        settings(|_| {}),
        Arc::new(SimulatedModel::new()),
        Arc::new(SimulatedWorker::instant()),
    );
    let outcome = fx.run(rt).await?;
    assert_eq!(outcome.status, FactoryStatus::Finished);

    let collected = fx.of_kind(EventKind::EvidenceCollected);
    assert_eq!(collected.len(), 1);
    let ran_at = collected[0].seq.get();
    let finishes: Vec<u64> = fx
        .of_kind(EventKind::DirectiveSelected)
        .iter()
        .filter(|e| e.payload["directive"] == "Finish")
        .map(|e| e.seq.get())
        .collect();
    assert_eq!(finishes.len(), 2, "one provisional, one applied");
    assert!(finishes[0] < ran_at && ran_at < finishes[1]);
    // The provisional Finish was not applied: nothing finished before the provider ran.
    assert!(
        !fx.of_kind(EventKind::StateChanged)
            .iter()
            .any(|e| e.payload["to"] == "finished" && e.seq.get() < ran_at)
    );

    // What the ensemble was told, cycle by cycle: provisional until the provider ran,
    // never after — although the provider is still configured.
    let cycles = fx.cycles();
    let resolved: Vec<(u64, bool)> = fx
        .resolutions()
        .iter()
        .map(|(seq, p)| {
            let cycle = p.cycle.map(|c| c.get());
            let provisional = cycles
                .iter()
                .find(|c| Some(c.cycle.get()) == cycle)
                .is_some_and(|c| c.finish_is_provisional);
            (*seq, provisional)
        })
        .collect();
    assert!(
        resolved.iter().all(|(seq, p)| *p == (*seq < ran_at)),
        "{resolved:?}"
    );
    Ok(())
}

#[tokio::test]
async fn a_provisional_finish_is_not_applied_even_when_its_provider_cannot_run() -> TestResult {
    let fx = Fixture::new(
        DefinitionSet::bundled()?,
        EvidencePlan::from_toml(
            r#"
[[evidence.commands]]
id = "audit"
argv = ["/nonexistent/aftman-test-provider"]
run_when = "before_finish"
"#,
            "test",
        )?,
    )?;
    let rt = fx.runtime(
        config(|_| {}),
        settings(|_| {}),
        Arc::new(SimulatedModel::new()),
        Arc::new(SimulatedWorker::instant()),
    );
    let outcome = fx.run(rt).await?;
    let collected = fx.of_kind(EventKind::EvidenceCollected);
    assert_eq!(collected.len(), 1);
    assert!(collected[0].payload["launch_error"].is_string());
    let ran_at = collected[0].seq.get();
    assert!(
        !fx.of_kind(EventKind::StateChanged)
            .iter()
            .any(|e| e.payload["to"] == "finished" && e.seq.get() < ran_at),
        "the provisional Finish was never applied"
    );
    assert_eq!(outcome.status, FactoryStatus::Finished);
    Ok(())
}

#[tokio::test]
async fn with_no_before_finish_provider_no_finish_is_provisional() -> TestResult {
    // An on-demand provider is configured, but nothing runs before a finish.
    let fx = Fixture::new(DefinitionSet::bundled()?, provider("on_demand", "exit 0")?)?;
    let rt = fx.runtime(
        config(|_| {}),
        settings(|_| {}),
        Arc::new(SimulatedModel::new()),
        Arc::new(SimulatedWorker::instant()),
    );
    let outcome = fx.run(rt).await?;
    assert_eq!(outcome.status, FactoryStatus::Finished);
    let cycles = fx.cycles();
    assert!(!cycles.is_empty());
    assert!(cycles.iter().all(|c| !c.finish_is_provisional));
    assert_eq!(
        fx.of_kind(EventKind::DirectiveSelected)
            .iter()
            .filter(|e| e.payload["directive"] == "Finish")
            .count(),
        1,
        "applied the first time"
    );
    Ok(())
}

// ---- escalation -------------------------------------------------------------------

#[tokio::test]
async fn an_ensemble_escalation_preempts_starting_a_worker() -> TestResult {
    // `tests_sufficient` stays in band and nothing can refresh it: after two held cycles
    // the check goes to a person, and the worker that policy would otherwise start is not.
    let fx = Fixture::new(DefinitionSet::bundled()?, EvidencePlan::default())?;
    let model = Arc::new(StateModel::new(|text| {
        if text.contains("3 passed") {
            0.71
        } else {
            0.10
        }
    }));
    let rt = fx.runtime(
        config(|_| {}),
        settings(|_| {}),
        model as Arc<dyn DecisionModel>,
        Arc::new(SimulatedWorker::instant()),
    );
    let outcome = fx.run(rt).await?;
    assert_eq!(outcome.status, FactoryStatus::NeedsHuman);
    let raised = fx.of_kind(EventKind::EscalationRaised);
    assert_eq!(raised.len(), 1);
    assert_eq!(raised[0].payload["kind"], "check");
    assert_eq!(raised[0].payload["checks"], serde_json::json!([TESTS]));
    assert!(
        raised[0].payload["reason"]
            .as_str()
            .is_some_and(|r| r.starts_with("ensemble:"))
    );
    let selected = fx
        .of_kind(EventKind::DirectiveSelected)
        .into_iter()
        .rev()
        .find(|e| e.payload["directive"] == "Escalate")
        .ok_or("no Escalate selected")?;
    let suppressed = selected.payload["suppressed"].to_string();
    assert!(
        suppressed.contains("core.completion:work-remains")
            && suppressed.contains("higher_priority"),
        "{suppressed}"
    );
    let state = fx.store.load(outcome.run_id).await?;
    assert_eq!(
        state.escalations[0].ensemble_checks,
        vec![CheckKey::from(TESTS)]
    );
    assert_eq!(state.escalations[0].check, Some(CheckKey::from(TESTS)));
    assert_eq!(state.escalations[0].jev_p, Some(0.71));
    // The counters are persisted with the run.
    assert!(
        state
            .ensemble
            .check(&CheckKey::from(TESTS))
            .consecutive_in_band
            >= 3
    );
    Ok(())
}

#[tokio::test]
async fn two_finish_conjuncts_in_band_are_put_to_a_person_not_to_more_workers() -> TestResult {
    // Review probe PD4. `ready_to_finish` and `tests_sufficient` both in band, nothing to
    // refresh them, the default worker limit. Before the fix neither was high stakes, so
    // the run burned every worker and ended on "worker limit reached before completion".
    let fx = Fixture::new(DefinitionSet::bundled()?, EvidencePlan::default())?;
    let model = StateModel::new(|text| {
        if text.contains("3 passed") {
            0.71
        } else {
            0.10
        }
    })
    .with("core.completion__ready_to_finish", 0.79);
    let rt = fx.runtime(
        config(|c| {
            c.max_workers = RunConfig::default().max_workers;
            c.skip_unchanged_assessments = false;
        }),
        settings(|_| {}),
        Arc::new(model),
        Arc::new(SimulatedWorker::instant()),
    );
    let outcome = fx.run(rt).await?;
    assert_eq!(outcome.status, FactoryStatus::NeedsHuman);
    let raised = fx.of_kind(EventKind::EscalationRaised);
    assert_eq!(raised.len(), 1);
    assert!(
        raised[0].payload["reason"]
            .as_str()
            .is_some_and(|r| r.starts_with("ensemble:")),
        "{}",
        raised[0].payload
    );
    // `ready_to_finish` is in band from the first cycle, `tests_sufficient` only once the
    // worker has claimed its tests, so the first is the one that reaches the limit.
    assert!(
        raised[0].payload["checks"]
            .as_array()
            .is_some_and(|c| c.contains(&serde_json::json!("core.completion__ready_to_finish"))),
        "{}",
        raised[0].payload
    );
    assert!(
        (fx.of_kind(EventKind::WorkerStarted).len() as u32) < RunConfig::default().max_workers,
        "the worker limit was not what ended it"
    );
    Ok(())
}
/// A worker that talks until it is stopped.
#[derive(Default)]
struct LongWorker {
    stopped: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl Worker for LongWorker {
    fn kind(&self) -> WorkerKind {
        WorkerKind::Simulated
    }

    fn supports_steering(&self) -> bool {
        false
    }

    async fn run(
        &self,
        ctx: aftman_core::worker::WorkerCtx,
        sink: aftman_core::worker::WorkerEventSink,
    ) -> Result<aftman_core::worker::WorkerOutcome, aftman_core::worker::WorkerError> {
        let mut tick = 0u32;
        while !self.stopped.load(std::sync::atomic::Ordering::SeqCst) {
            {
                let mut output = ctx.output.lock().unwrap_or_else(|e| e.into_inner());
                output.push(
                    aftman_core::log::Stream::Stdout,
                    &format!("working {tick}\n"),
                );
            }
            sink.activity();
            tick += 1;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Ok(aftman_core::worker::WorkerOutcome::Terminated {
            reason: aftman_core::worker::TerminateReason::Stopped,
        })
    }

    async fn steer(
        &self,
        _message: &str,
    ) -> Result<aftman_core::worker::SteerOutcome, aftman_core::worker::WorkerError> {
        Ok(aftman_core::worker::SteerOutcome::Unsupported)
    }

    async fn terminate(
        &self,
        _reason: aftman_core::worker::TerminateReason,
    ) -> Result<(), aftman_core::worker::WorkerError> {
        self.stopped
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn one_cycles_escalations_coalesce_into_one_escalate() -> TestResult {
    // Two worker-health checks in band at once, each a `Stop` away: one question.
    let fx = Fixture::new(DefinitionSet::bundled()?, EvidencePlan::default())?;
    let model = Arc::new(
        StateModel::new(|_| 0.10)
            .with(STUCK, 0.78)
            .with(OFF_TRACK, 0.78),
    );
    let rt = fx.runtime(
        config(|_| {}),
        settings(|_| {}),
        model as Arc<dyn DecisionModel>,
        Arc::new(LongWorker::default()),
    );
    let outcome = fx.run(rt).await?;
    assert_eq!(outcome.status, FactoryStatus::NeedsHuman);
    let raised = fx.of_kind(EventKind::EscalationRaised);
    assert_eq!(raised.len(), 1, "one escalation for the cycle");
    assert_eq!(
        raised[0].payload["checks"],
        serde_json::json!([OFF_TRACK, STUCK])
    );
    let ask = raised[0].payload["ask"].as_str().unwrap_or_default();
    assert!(
        ask.starts_with("2 checks") && ask.contains(STUCK) && ask.contains(OFF_TRACK),
        "{ask}"
    );
    let proposed = fx
        .of_kind(EventKind::DirectiveProposed)
        .iter()
        .filter(|e| e.payload["rule"] == "core.ensemble:escalate")
        .count();
    assert_eq!(proposed, 1);
    Ok(())
}

// ---- the human answer ---------------------------------------------------------------

/// `tests_sufficient` held in band with nothing to refresh it, under a control channel.
async fn held_in_band(
    fx: &Fixture,
    model: StateModel,
) -> Result<(RunGuard, EscalationId), Box<dyn std::error::Error>> {
    let rt = fx.runtime(
        config(|c| c.skip_unchanged_assessments = false),
        settings(|s| s.max_evidence_refreshes_per_check = 10),
        Arc::new(model),
        Arc::new(SimulatedWorker::instant()),
    );
    let guard = RunGuard::launch(fx, rt).await?;
    let id = escalation(fx, 1).await?;
    Ok((guard, id))
}

fn in_band_model() -> StateModel {
    StateModel::new(|text| {
        if text.contains("3 passed") {
            0.71
        } else {
            0.10
        }
    })
}

/// The `tests_sufficient` entries of `ENSEMBLE_RESOLVED` events after `seq`.
fn entries_after(fx: &Fixture, seq: u64) -> Vec<aftman_core::ensemble::CheckResolvedPayload> {
    fx.resolutions()
        .into_iter()
        .filter(|(at, _)| *at > seq)
        .flat_map(|(_, p)| p.checks)
        .filter(|c| c.check == CheckKey::from(TESTS))
        .collect()
}

#[tokio::test]
async fn accept_holds_the_check_and_does_not_ask_again() -> TestResult {
    let fx = Fixture::new(DefinitionSet::bundled()?, EvidencePlan::default())?;
    let (mut run, id) = held_in_band(&fx, in_band_model()).await?;
    run.answer(id, HumanDecision::Accept).await?;
    let answered = fx
        .of_kind(EventKind::EscalationAnswered)
        .last()
        .map(|e| e.seq.get())
        .ok_or("no answer")?;
    // Several more held cycles.
    until(&fx, "five more cycles", |events| {
        events
            .iter()
            .filter(|e| e.kind == EventKind::EnsembleResolved && e.seq.get() > answered)
            .count()
            >= 5
    })
    .await;
    assert_eq!(
        fx.of_kind(EventKind::EscalationRaised).len(),
        1,
        "asked once"
    );
    let after = entries_after(&fx, answered);
    assert!(!after.is_empty());
    for entry in &after {
        assert!(!entry.permitted, "acceptance never permits");
    }
    assert!(after.iter().any(|entry| entry.resolution
        == Resolution::HumanDecided {
            decision: HumanDecision::Accept
        }));
    assert!(
        !fx.of_kind(EventKind::DirectiveSelected)
            .iter()
            .any(|e| e.payload["directive"] == "Finish")
    );
    let outcome = run.cancel().await?;
    assert_eq!(outcome.status, FactoryStatus::Cancelled);
    Ok(())
}

#[tokio::test]
async fn guidance_is_accept_plus_a_steer() -> TestResult {
    let fx = Fixture::new(DefinitionSet::bundled()?, EvidencePlan::default())?;
    let (mut run, id) = held_in_band(&fx, in_band_model()).await?;
    run.answer(
        id,
        HumanDecision::Guidance {
            text: "the suite is known to be thin; carry on".into(),
        },
    )
    .await?;
    let answered = fx
        .of_kind(EventKind::EscalationAnswered)
        .last()
        .map(|e| e.seq.get())
        .ok_or("no answer")?;
    until(&fx, "three more cycles", |events| {
        events
            .iter()
            .filter(|e| e.kind == EventKind::EnsembleResolved && e.seq.get() > answered)
            .count()
            >= 3
    })
    .await;
    assert_eq!(
        fx.of_kind(EventKind::EscalationRaised).len(),
        1,
        "asked once"
    );
    // Delivered to the worker if one was running; recorded as undeliverable otherwise.
    let steered = fx
        .of_kind(EventKind::WorkerSteered)
        .iter()
        .any(|e| e.seq.get() > answered);
    let noted = fx
        .of_kind(EventKind::Notification)
        .iter()
        .any(|e| e.payload["code"] == "guidance_not_delivered");
    assert!(steered || noted);
    assert!(entries_after(&fx, answered).iter().any(|entry| matches!(
        entry.resolution,
        Resolution::HumanDecided {
            decision: HumanDecision::Guidance { .. }
        }
    )));
    run.cancel().await?;
    Ok(())
}

#[tokio::test]
async fn stop_ends_the_run_as_cancelled() -> TestResult {
    let fx = Fixture::new(DefinitionSet::bundled()?, EvidencePlan::default())?;
    let (mut run, id) = held_in_band(&fx, in_band_model()).await?;
    run.answer(id, HumanDecision::Stop).await?;
    let outcome = run.join().await?;
    assert_eq!(outcome.status, FactoryStatus::Cancelled);
    assert_eq!(outcome.reason.as_deref(), Some("stopped by operator"));
    Ok(())
}

/// A provider whose command does not exist until the test writes it: until then every
/// refresh is rejected (nothing ran), so the check is escalated with refreshes left.
fn late_provider(dir: &Path) -> Result<(EvidencePlan, PathBuf), Box<dyn std::error::Error>> {
    let script = dir.join("late-tests.sh");
    let plan = EvidencePlan::from_toml(
        &format!(
            r#"
[[evidence.commands]]
id = "tests"
argv = ["{}"]
run_when = "on_demand"
"#,
            script.display()
        ),
        "test",
    )?;
    Ok((plan, script))
}

fn write_script(path: &Path, body: &str) -> TestResult {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::write(path, format!("#!/bin/sh\n{body}\n"))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))?;
    Ok(())
}

async fn recheck_with(
    decision: HumanDecision,
    fresh: f32,
) -> Result<(Fixture, RunGuard, u64), Box<dyn std::error::Error>> {
    let mut fx = Fixture::new(definitions()?, EvidencePlan::default())?;
    let (plan, script) = late_provider(fx.dir.path())?;
    fx.evidence = plan;
    let model = StateModel::new(move |text| {
        if text.contains("PROVIDER-RAN") {
            fresh
        } else if text.contains("3 passed") {
            0.71
        } else {
            0.10
        }
    });
    let (run, id) = held_in_band(&fx, model).await?;
    let before = fx.of_kind(EventKind::EvidenceCollected).len();
    assert!(before > 0, "refreshes were attempted, and nothing ran");
    write_script(&script, "echo 'PROVIDER-RAN 17 passed'")?;
    run.answer(id, decision).await?;
    let answered = fx
        .of_kind(EventKind::EscalationAnswered)
        .last()
        .map(|e| e.seq.get())
        .ok_or("no answer")?;
    until(&fx, "a refresh after the answer", |_| {
        !fx.refreshes(TESTS)
            .iter()
            .filter(|(seq, entry)| {
                *seq > answered && entry.refresh.as_ref().is_some_and(|r| r.rejected.is_none())
            })
            .collect::<Vec<_>>()
            .is_empty()
    })
    .await;
    Ok((fx, run, answered))
}

#[tokio::test]
async fn recheck_runs_r0_on_the_next_cycle_and_evidence_may_finish() -> TestResult {
    let (fx, mut run, answered) = recheck_with(HumanDecision::Recheck, 0.95).await?;
    let outcome = run.join().await?;
    assert_eq!(outcome.status, FactoryStatus::Finished);
    let (seq, entry) = fx
        .refreshes(TESTS)
        .into_iter()
        .find(|(seq, _)| *seq > answered)
        .ok_or("no refresh after the answer")?;
    assert!(entry.triggers.contains(&EnsembleTrigger::HumanRecheck));
    assert!(matches!(
        entry.resolution,
        Resolution::EvidenceRefreshed { .. }
    ));
    assert!(entry.permitted);
    // The fresh, decisive measurement is what finishes the run: not the person.
    assert_eq!(fx.selected_after(seq).as_deref(), Some("Finish"));
    Ok(())
}

#[tokio::test]
async fn claim_complete_followed_by_an_in_band_refresh_does_not_finish() -> TestResult {
    let (fx, mut run, answered) = recheck_with(HumanDecision::ClaimComplete, 0.72).await?;
    let (seq, entry) = fx
        .refreshes(TESTS)
        .into_iter()
        .find(|(seq, _)| *seq > answered)
        .ok_or("no refresh after the answer")?;
    assert!(entry.triggers.contains(&EnsembleTrigger::HumanRecheck));
    let Resolution::EvidenceRefreshed { new_p, .. } = entry.resolution else {
        panic!("expected a refresh");
    };
    assert!((new_p - 0.72).abs() < f32::EPSILON);
    assert!(!entry.permitted, "in band, so still held");
    assert_ne!(fx.selected_after(seq).as_deref(), Some("Finish"));
    // A few more cycles: it never finishes on the person's word.
    until(&fx, "more cycles", |events| {
        events
            .iter()
            .filter(|e| e.kind == EventKind::EnsembleResolved && e.seq.get() > seq)
            .count()
            >= 3
    })
    .await;
    assert!(
        !fx.of_kind(EventKind::DirectiveSelected)
            .iter()
            .any(|e| e.payload["directive"] == "Finish")
    );
    let outcome = run.cancel().await?;
    assert_ne!(outcome.status, FactoryStatus::Finished);
    Ok(())
}

// ---- replay -----------------------------------------------------------------------

/// The run's registry, rebuilt as the runtime built it: the definitions and what routing
/// activated.
fn registry_of(fx: &Fixture) -> Result<ResponsibilityRegistry, Box<dyn std::error::Error>> {
    let routed = fx
        .of_kind(EventKind::JobRouted)
        .into_iter()
        .next()
        .ok_or("no JOB_ROUTED")?;
    let activated: Vec<ResponsibilityId> = routed.payload["activated"]
        .as_array()
        .ok_or("no activated list")?
        .iter()
        .filter_map(Value::as_str)
        .map(ResponsibilityId::new)
        .collect();
    Ok(ResponsibilityRegistry::from_definitions(
        &fx.definitions,
        &activated,
        30,
        1,
    )?)
}

async fn replay(fx: &Fixture, outcome: &RunOutcome) -> TestResult {
    let log = fx.store.read_log(outcome.run_id).await?;
    let source = RecordedOpinionSource::from_events(&log.events)?;
    assert_eq!(source.incomplete_cycle(), None);
    let registry = registry_of(fx)?;
    let recorded: BTreeMap<u64, EnsembleResolvedPayload> = log
        .events
        .iter()
        .filter(|e| e.kind == EventKind::EnsembleResolved)
        .filter_map(|e| serde_json::from_value::<EnsembleResolvedPayload>(e.payload.clone()).ok())
        .filter_map(|p| p.cycle.map(|c| (c.get(), p)))
        .collect();
    let cycles = fx.cycles();
    assert_eq!(
        cycles.len(),
        recorded.len(),
        "every resolved cycle was observed"
    );
    for cycle in &cycles {
        let replayed = cycle.replay(&registry, &source).await?;
        assert_eq!(source.divergences(&replayed), Vec::new());
        let payload =
            replayed.resolved_payload(cycle.settings.enabled, cycle.settings.uncertainty_margin);
        assert_eq!(
            Some(&payload),
            recorded.get(&cycle.cycle.get()),
            "cycle {} replays to the recorded ENSEMBLE_RESOLVED",
            cycle.cycle
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_real_run_replays_from_its_own_log() -> TestResult {
    // Refreshes that pass, fail and are capped, replayed from the log alone.
    for (script, tests) in [
        (
            "echo 'PROVIDER-PASS 17 passed'; exit 0",
            Box::new(|text: &str| -> f32 {
                if text.contains("PROVIDER-PASS") {
                    0.95
                } else if claim_only(text) {
                    0.71
                } else {
                    0.10
                }
            }) as Box<dyn Fn(&str) -> f32 + Send + Sync>,
        ),
        (
            "echo 'PROVIDER-FAIL 16 passed, 1 failed'; exit 1",
            Box::new(|text: &str| -> f32 {
                if text.contains("PROVIDER-FAIL") {
                    0.08
                } else if claim_only(text) {
                    0.71
                } else {
                    0.10
                }
            }),
        ),
        (
            "echo 'PROVIDER-PASS 17 passed'; exit 0",
            Box::new(|text: &str| -> f32 {
                if text.contains("3 passed") || text.contains("PROVIDER") {
                    0.71
                } else {
                    0.10
                }
            }),
        ),
    ] {
        let fx = Fixture::new(definitions()?, provider("on_demand", script)?)?;
        let model = Arc::new(StateModel::new(tests));
        let rt = fx.runtime(
            config(|c| c.max_workers = 3),
            settings(|s| s.max_evidence_refreshes_per_check = 2),
            model as Arc<dyn DecisionModel>,
            Arc::new(SimulatedWorker::instant()),
        );
        let outcome = fx.run(rt).await?;
        assert!(!fx.refreshes(TESTS).is_empty(), "the run refreshed");
        replay(&fx, &outcome).await?;
    }
    Ok(())
}

// ---- the silent worker ------------------------------------------------------------

/// `SimulatedModel`, counting its calls.
#[derive(Default)]
struct Counting {
    inner: SimulatedModel,
    calls: std::sync::atomic::AtomicUsize,
}

#[async_trait]
impl DecisionModel for Counting {
    async fn ask(
        &self,
        context: &AskContext,
        state: &Value,
        questions: &BTreeMap<CheckKey, Question>,
        budget: &Budget,
    ) -> Result<ModelAnswers, ModelError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.ask(context, state, questions, budget).await
    }
}

/// A worker that says nothing at all for a while, then completes.
struct SilentWorker;

#[async_trait]
impl Worker for SilentWorker {
    fn kind(&self) -> WorkerKind {
        WorkerKind::Simulated
    }

    fn supports_steering(&self) -> bool {
        false
    }

    async fn run(
        &self,
        _ctx: aftman_core::worker::WorkerCtx,
        _sink: aftman_core::worker::WorkerEventSink,
    ) -> Result<aftman_core::worker::WorkerOutcome, aftman_core::worker::WorkerError> {
        tokio::time::sleep(Duration::from_millis(120)).await;
        Ok(aftman_core::worker::WorkerOutcome::Completed)
    }

    async fn steer(
        &self,
        _message: &str,
    ) -> Result<aftman_core::worker::SteerOutcome, aftman_core::worker::WorkerError> {
        Ok(aftman_core::worker::SteerOutcome::Unsupported)
    }

    async fn terminate(
        &self,
        _reason: aftman_core::worker::TerminateReason,
    ) -> Result<(), aftman_core::worker::WorkerError> {
        Ok(())
    }
}

// Virtual time, as in `offline_run.rs`: the ticks inside the worker's lifetime are fixed.
#[tokio::test(start_paused = true)]
async fn with_the_ensemble_on_a_silent_worker_still_costs_nothing_per_idle_tick() -> TestResult {
    // `docs/06-ensemble.md` §14: N idle ticks with unchanged observation content consume
    // zero assessment budget and emit zero empty `WorkerOutput` events — with the
    // ensemble enabled, so a refresh can never be what spends them.
    let fx = Fixture::new(DefinitionSet::bundled()?, EvidencePlan::default())?;
    let model = Arc::new(Counting::default());
    let rt = fx.runtime(
        config(|c| c.idle_poll_interval = Duration::from_millis(5)),
        settings(|_| {}),
        Arc::clone(&model) as Arc<dyn DecisionModel>,
        Arc::new(SilentWorker),
    );
    let outcome = fx.run(rt).await?;
    let skipped = fx.of_kind(EventKind::AssessmentSkippedUnchanged).len();
    assert!(skipped >= 10, "idle ticks were skipped: {skipped}");
    // Every model call is accounted for by an assessment that was not skipped: the skipped
    // ticks cost nothing.
    assert_eq!(
        model.calls.load(std::sync::atomic::Ordering::SeqCst),
        1 + fx.of_kind(EventKind::AssessmentCompleted).len(),
        "routing plus one call per assessed cycle"
    );
    assert_eq!(
        outcome.assessments as usize,
        fx.of_kind(EventKind::AssessmentCompleted).len()
    );
    assert_eq!(
        fx.of_kind(EventKind::WorkerOutput).len(),
        0,
        "no empty output events"
    );
    Ok(())
}
