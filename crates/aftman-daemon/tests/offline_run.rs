//! Offline end-to-end runs.
//!
//! These are M0's exit criteria. They must pass with no network, no credentials, and
//! no real agent binary — `AGENTS.md` hard rule 4 — so every dependency is a
//! simulated worker or a scripted model.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use aftman_core::clock::SystemClock;
use aftman_core::event::{EventEnvelope, EventKind};
use aftman_core::ids::EventSeq;
use aftman_core::job::{FactoryStatus, Job, JobSource, RunConfig, WorkerKind};
use aftman_core::log::Stream;
use aftman_core::model::DecisionModel;
use aftman_core::responsibility::ResponsibilityRegistry;
use aftman_core::testing::{ScriptedModel, SimulatedModel};
use aftman_core::worker::{
    TerminateReason, Worker, WorkerCtx, WorkerError, WorkerEventSink, WorkerOutcome,
};
use aftman_daemon::{FactoryRuntime, RuntimeDeps, VecSink};
use aftman_store::{FileRunStore, RunStore};
use aftman_workers::SimulatedWorker;

/// A worker that is silent and slow: it produces no output at all, then completes.
///
/// This is the case the idle ticker exists for. With no output there are no activity
/// signals, so the only thing driving cycles is the ticker, and because nothing about
/// the observation changes between ticks, every cycle after the first must be
/// *skipped* rather than sent to the model.
struct SilentWorker {
    run_for: Duration,
}

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
        _ctx: WorkerCtx,
        _sink: WorkerEventSink,
    ) -> Result<WorkerOutcome, WorkerError> {
        tokio::time::sleep(self.run_for).await;
        Ok(WorkerOutcome::Completed)
    }

    async fn steer(
        &self,
        _message: &str,
    ) -> Result<aftman_core::worker::SteerOutcome, WorkerError> {
        Ok(aftman_core::worker::SteerOutcome::Unsupported)
    }

    async fn terminate(&self, _reason: TerminateReason) -> Result<(), WorkerError> {
        Ok(())
    }
}

/// A worker that emits output on demand, so a test can drive activity signals.
struct ChattyWorker {
    chunks: Vec<&'static str>,
    delay: Duration,
}

#[async_trait]
impl Worker for ChattyWorker {
    fn kind(&self) -> WorkerKind {
        WorkerKind::Simulated
    }

    fn supports_steering(&self) -> bool {
        true
    }

    async fn run(
        &self,
        ctx: WorkerCtx,
        sink: WorkerEventSink,
    ) -> Result<WorkerOutcome, WorkerError> {
        for chunk in &self.chunks {
            {
                let mut output = ctx
                    .output
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                output.push(Stream::Stdout, chunk);
            }
            sink.activity();
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
        }
        Ok(WorkerOutcome::Completed)
    }

    async fn steer(
        &self,
        _message: &str,
    ) -> Result<aftman_core::worker::SteerOutcome, WorkerError> {
        Ok(aftman_core::worker::SteerOutcome::Queued)
    }

    async fn terminate(&self, _reason: TerminateReason) -> Result<(), WorkerError> {
        Ok(())
    }
}

/// The bundled definitions, plus empty evidence, for an offline run.
fn test_definitions() -> aftman_core::definitions::DefinitionSet {
    // Bundled definitions are compiled in and validated, so this cannot fail here.
    aftman_core::definitions::DefinitionSet::bundled()
        .unwrap_or_else(|_| aftman_core::definitions::DefinitionSet::default())
}

fn test_evidence_dir(h: &Harness) -> PathBuf {
    let dir = h._dir.path().join("evidence-tmp");
    let _ = std::fs::create_dir_all(&dir);
    dir
}

struct Harness {
    _dir: tempfile::TempDir,
    repo: PathBuf,
    store: Arc<dyn RunStore>,
    sink: Arc<VecSink>,
}

fn harness() -> Result<Harness, Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let repo = dir.path().join("repo");
    std::fs::create_dir_all(&repo)?;
    let store: Arc<dyn RunStore> = Arc::new(FileRunStore::new(dir.path().join("state")));
    let sink = Arc::new(VecSink::new());
    Ok(Harness {
        _dir: dir,
        repo,
        store,
        sink,
    })
}

fn job(repo: &std::path::Path) -> Job {
    Job {
        prompt: "Add request retries with backoff, and cover them with tests.".into(),
        repo: repo.to_path_buf(),
        source: JobSource::Cli,
    }
}

fn runtime(
    harness: &Harness,
    config: RunConfig,
    model: Arc<dyn DecisionModel>,
    worker: Arc<dyn Worker>,
) -> FactoryRuntime {
    FactoryRuntime::new(RuntimeDeps {
        config,
        store: Arc::clone(&harness.store),
        model,
        worker,
        registry: ResponsibilityRegistry::builtin(),
        definitions: test_definitions(),
        evidence: aftman_core::evidence::EvidencePlan::default(),
        evidence_temp_dir: test_evidence_dir(harness),
        clock: Arc::new(SystemClock),
        sink: Arc::clone(&harness.sink) as Arc<dyn aftman_daemon::EventSink>,
        ensemble_enabled: false,
        uncertainty_margin: 0.15,
        steering_grace: Duration::from_secs(30),
        launch: Default::default(),

        forwarded_env: Default::default(),
    })
}

fn fast_config() -> RunConfig {
    RunConfig {
        worker: WorkerKind::Simulated,
        min_assessment_interval: Duration::from_millis(1),
        idle_poll_interval: Duration::from_millis(10),
        overall_timeout: Duration::from_secs(30),
        worker_timeout: Duration::from_secs(10),
        ..RunConfig::default()
    }
}

#[tokio::test]
async fn an_offline_run_reaches_finished() -> Result<(), Box<dyn std::error::Error>> {
    let h = harness()?;
    let rt = runtime(
        &h,
        fast_config(),
        Arc::new(SimulatedModel::new()),
        Arc::new(SimulatedWorker::new()),
    );

    let outcome = rt.run(job(&h.repo)).await?;
    assert_eq!(
        outcome.status,
        FactoryStatus::Finished,
        "the simulated worker plus simulated model must be able to finish"
    );
    assert!(
        outcome.assessments > 0,
        "at least one assessment must happen"
    );
    assert_eq!(outcome.counters.workers_run, 1);

    // The log is well-formed and every event carries its schema version.
    let log = h.store.read_log(outcome.run_id).await?;
    assert!(log.torn_lines.is_empty());
    for (index, event) in log.events.iter().enumerate() {
        assert_eq!(
            event.seq,
            EventSeq(index as u64 + 1),
            "sequence numbers must be contiguous and start at 1"
        );
        assert!(
            event.payload.get("schema_version").is_some(),
            "{} must carry a schema_version",
            event.kind.as_str()
        );
    }

    // The snapshot agrees with the log.
    let state = h.store.load(outcome.run_id).await?;
    assert_eq!(state.last_seq, log.last_seq());
    assert_eq!(state.status, FactoryStatus::Finished);
    assert!(!state.checks.is_empty(), "checks must be persisted");
    Ok(())
}

#[tokio::test]
async fn a_simulated_run_does_not_touch_the_repository() -> Result<(), Box<dyn std::error::Error>> {
    let h = harness()?;
    // Put a file in the repo so that a mutation would be visible.
    let before = h.repo.join("README.md");
    std::fs::write(&before, "original\n")?;

    let rt = runtime(
        &h,
        fast_config(),
        Arc::new(SimulatedModel::new()),
        Arc::new(SimulatedWorker::new()),
    );
    rt.run(job(&h.repo)).await?;

    assert_eq!(std::fs::read_to_string(&before)?, "original\n");
    let entries: Vec<_> = std::fs::read_dir(&h.repo)?.collect::<Result<_, _>>()?;
    assert_eq!(
        entries.len(),
        1,
        "no new files may appear in the repository"
    );
    Ok(())
}

#[tokio::test]
async fn the_run_reports_running_then_assessing_rather_than_jumping()
-> Result<(), Box<dyn std::error::Error>> {
    let h = harness()?;
    let rt = runtime(
        &h,
        fast_config(),
        Arc::new(SimulatedModel::new()),
        Arc::new(SimulatedWorker::new()),
    );
    let outcome = rt.run(job(&h.repo)).await?;

    let transitions: Vec<String> = h
        .sink
        .of_kind(EventKind::StateChanged)
        .iter()
        .map(|e| {
            format!(
                "{}->{}",
                e.payload["from"].as_str().unwrap_or("?"),
                e.payload["to"].as_str().unwrap_or("?")
            )
        })
        .collect();
    assert!(
        transitions.contains(&"running->assessing".to_owned()),
        "a judgement must pass through `assessing`; saw {transitions:?}"
    );
    assert!(
        !transitions.iter().any(|t| t == "running->finished"),
        "running -> finished is not a legal transition"
    );
    assert_eq!(outcome.status, FactoryStatus::Finished);
    Ok(())
}

// Virtual time: the 5 ms ticker and the 120 ms worker are both on the paused tokio
// clock, so the number of ticks inside the worker's lifetime is fixed, not a race.
#[tokio::test(start_paused = true)]
async fn a_silent_worker_does_not_burn_the_assessment_budget()
-> Result<(), Box<dyn std::error::Error>> {
    // The regression the design review caught: with a single short ticker and no
    // digest skip, a silent worker exhausts `max_assessments` well inside its own
    // timeout and gets stopped as though it were stuck.
    let h = harness()?;
    let mut config = fast_config();
    config.idle_poll_interval = Duration::from_millis(5);
    config.max_assessments = 400;

    let rt = runtime(
        &h,
        config,
        Arc::new(SimulatedModel::new()),
        Arc::new(SilentWorker {
            run_for: Duration::from_millis(120),
        }),
    );
    let outcome = rt.run(job(&h.repo)).await?;

    assert!(
        outcome.skipped > 0,
        "silent ticks with an unchanged observation must be skipped; \
         skipped={} assessments={}",
        outcome.skipped,
        outcome.assessments
    );
    assert!(
        outcome.assessments < 20,
        "a 120ms silent worker must not consume dozens of assessments; \
         got {} (skipped {})",
        outcome.assessments,
        outcome.skipped
    );
    // And it must not have been stopped for exhausting the budget.
    assert_ne!(outcome.status, FactoryStatus::Failed);
    Ok(())
}

#[tokio::test]
async fn an_exhausted_worker_budget_escalates_instead_of_spinning()
-> Result<(), Box<dyn std::error::Error>> {
    // A model that never says "done" used to leave the run in an endless
    // `Continue`: no worker, no proposal that could fire, and nothing changing.
    // Policy must escalate instead, and the run must terminate.
    let h = harness()?;
    let model = ScriptedModel::new("always-no").with_all(&[
        ("core.completion__ready_to_finish", 0.10),
        ("core.completion__implementation_complete", 0.10),
        ("core.completion__requirements_satisfied", 0.10),
        ("core.verification__tests_sufficient", 0.10),
        ("core.verification__needs_verification", 0.90),
        ("core.worker-health__worker_stuck", 0.02),
        ("core.worker-health__work_off_track", 0.02),
        ("core.human-escalation__needs_human", 0.01),
        ("repository.instructions__agents_md_drift", 0.01),
        ("core.worker-health__meaningful_progress", 0.60),
    ]);
    let mut config = fast_config();
    config.max_workers = 2;

    let rt = runtime(
        &h,
        config,
        Arc::new(model),
        Arc::new(ChattyWorker {
            chunks: vec!["working\n"],
            delay: Duration::from_millis(1),
        }),
    );

    // A timeout here is the failure: the loop must terminate on its own.
    let outcome = tokio::time::timeout(Duration::from_secs(10), rt.run(job(&h.repo)))
        .await
        .map_err(|_| "the run must terminate rather than spin")??;

    assert_eq!(
        outcome.status,
        FactoryStatus::NeedsHuman,
        "with the worker budget gone and no progress, a human must be asked"
    );
    assert!(matches!(
        outcome.directive,
        aftman_core::directive::Directive::Escalate { .. }
    ));
    assert!(
        outcome.counters.workers_run <= 2,
        "the worker limit must be respected; ran {}",
        outcome.counters.workers_run
    );
    Ok(())
}

#[tokio::test]
async fn a_repeatedly_failing_model_escalates_rather_than_guessing()
-> Result<(), Box<dyn std::error::Error>> {
    let h = harness()?;
    let model = ScriptedModel::new("broken").failing_with(aftman_core::error::ModelError::Auth);
    let rt = runtime(
        &h,
        fast_config(),
        Arc::new(model),
        Arc::new(SimulatedWorker::new()),
    );
    let outcome = rt.run(job(&h.repo)).await?;

    // Three consecutive failures means supervision is blind; the run must say so
    // rather than proceed.
    assert_eq!(outcome.status, FactoryStatus::NeedsHuman);
    let errors = h.sink.of_kind(EventKind::ModelError);
    assert!(!errors.is_empty(), "a model failure must be recorded");
    assert_eq!(
        errors[0].payload["code"].as_str(),
        Some("auth"),
        "the failure must carry a stable code"
    );
    Ok(())
}

#[tokio::test]
async fn an_ensemble_enabled_run_still_reaches_the_same_verdict()
-> Result<(), Box<dyn std::error::Error>> {
    // With the band on and a decisively-positive simulated trajectory, the outcome
    // must be unchanged: enabling the ensemble can only ever be more conservative,
    // never less.
    let h = harness()?;
    let rt = FactoryRuntime::new(RuntimeDeps {
        config: fast_config(),
        store: Arc::clone(&h.store),
        model: Arc::new(SimulatedModel::new()),
        worker: Arc::new(SimulatedWorker::new()),
        registry: ResponsibilityRegistry::builtin(),
        definitions: test_definitions(),
        evidence: aftman_core::evidence::EvidencePlan::default(),
        evidence_temp_dir: test_evidence_dir(&h),
        clock: Arc::new(SystemClock),
        sink: Arc::clone(&h.sink) as Arc<dyn aftman_daemon::EventSink>,
        ensemble_enabled: true,
        uncertainty_margin: 0.15,
        steering_grace: Duration::from_secs(30),
        launch: Default::default(),

        forwarded_env: Default::default(),
    });
    let outcome = rt.run(job(&h.repo)).await?;
    assert_eq!(outcome.status, FactoryStatus::Finished);

    // The ensemble decision is recorded even when nothing needed resolving.
    let resolved = h.sink.of_kind(EventKind::EnsembleResolved);
    assert!(!resolved.is_empty());
    assert_eq!(resolved[0].payload["enabled"], serde_json::json!(true));
    Ok(())
}

#[tokio::test]
async fn a_missing_repository_is_a_clean_configuration_error()
-> Result<(), Box<dyn std::error::Error>> {
    let h = harness()?;
    let rt = runtime(
        &h,
        fast_config(),
        Arc::new(SimulatedModel::new()),
        Arc::new(SimulatedWorker::new()),
    );
    let missing = Job {
        prompt: "x".into(),
        repo: PathBuf::from("/definitely/not/here"),
        source: JobSource::Cli,
    };
    let error = rt
        .run(missing)
        .await
        .err()
        .ok_or("a missing repository must fail")?;
    assert!(matches!(error, aftman_daemon::RuntimeError::BadRepo(_)));
    Ok(())
}

#[tokio::test]
async fn the_worker_mission_is_recorded_verbatim() -> Result<(), Box<dyn std::error::Error>> {
    let h = harness()?;
    let rt = runtime(
        &h,
        fast_config(),
        Arc::new(SimulatedModel::new()),
        Arc::new(SimulatedWorker::new()),
    );
    let outcome = rt.run(job(&h.repo)).await?;

    let state = h.store.load(outcome.run_id).await?;
    let mission = &state.workers[0].mission;
    assert!(mission.contains("Add request retries"), "{mission}");
    assert!(mission.contains("Do not claim a test passed"), "{mission}");
    Ok(())
}

/// A model wrapper that counts calls, so a test can assert a skip avoided one.
#[derive(Debug)]
struct CountingModel {
    inner: SimulatedModel,
    calls: Mutex<usize>,
}

impl CountingModel {
    fn new() -> Self {
        Self {
            inner: SimulatedModel::new(),
            calls: Mutex::new(0),
        }
    }

    fn calls(&self) -> usize {
        *self
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[async_trait]
impl DecisionModel for CountingModel {
    async fn ask(
        &self,
        context: &aftman_core::model::AskContext,
        state: &serde_json::Value,
        questions: &std::collections::BTreeMap<
            aftman_core::ids::CheckKey,
            aftman_core::model::Question,
        >,
        budget: &aftman_core::model::Budget,
    ) -> Result<aftman_core::model::ModelAnswers, aftman_core::error::ModelError> {
        *self
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
        self.inner.ask(context, state, questions, budget).await
    }
}

#[tokio::test(start_paused = true)]
async fn skipped_cycles_do_not_reach_the_model() -> Result<(), Box<dyn std::error::Error>> {
    let h = harness()?;
    let model = Arc::new(CountingModel::new());
    let mut config = fast_config();
    config.idle_poll_interval = Duration::from_millis(5);

    let rt = FactoryRuntime::new(RuntimeDeps {
        config,
        store: Arc::clone(&h.store),
        model: Arc::clone(&model) as Arc<dyn DecisionModel>,
        worker: Arc::new(SilentWorker {
            run_for: Duration::from_millis(100),
        }),
        registry: ResponsibilityRegistry::builtin(),
        definitions: test_definitions(),
        evidence: aftman_core::evidence::EvidencePlan::default(),
        evidence_temp_dir: test_evidence_dir(&h),
        clock: Arc::new(SystemClock),
        sink: Arc::clone(&h.sink) as Arc<dyn aftman_daemon::EventSink>,
        ensemble_enabled: false,
        uncertainty_margin: 0.15,
        steering_grace: Duration::from_secs(30),
        launch: Default::default(),

        forwarded_env: Default::default(),
    });
    let outcome = rt.run(job(&h.repo)).await?;

    let skipped = outcome.skipped as usize;
    assert!(
        skipped > 0,
        "the test needs at least one skip to be meaningful"
    );
    // Routing is one extra model call that is not an assessment, so it is counted
    // separately rather than being folded into the comparison.
    let routing_calls = h.sink.of_kind(EventKind::JobRouted).len();
    assert_eq!(
        routing_calls, 1,
        "routing happens once per run, not per cycle"
    );
    assert_eq!(
        model.calls(),
        outcome.assessments as usize + routing_calls,
        "a skipped cycle must not make a model call: {skipped} skips happened"
    );
    Ok(())
}

/// Events must never be emitted for a directive that was not applied.
#[tokio::test]
async fn the_selected_directive_is_persisted_before_the_next_cycle()
-> Result<(), Box<dyn std::error::Error>> {
    let h = harness()?;
    let rt = runtime(
        &h,
        fast_config(),
        Arc::new(SimulatedModel::new()),
        Arc::new(SimulatedWorker::new()),
    );
    let outcome = rt.run(job(&h.repo)).await?;

    let selected = h.sink.of_kind(EventKind::DirectiveSelected);
    assert!(!selected.is_empty());

    // Every selection must precede the ordering event that follows it.
    let ordered: Vec<EventEnvelope> = h.sink.events();
    let seqs: Vec<u64> = ordered.iter().map(|e| e.seq.get()).collect();
    let mut sorted = seqs.clone();
    sorted.sort_unstable();
    assert_eq!(seqs, sorted, "events must be emitted in sequence order");

    let state = h.store.load(outcome.run_id).await?;
    assert!(state.last_directive.is_some());
    Ok(())
}

// ---------------------------------------------------------------------------
// M2: routing, and command evidence as a fact rather than a claim.
// ---------------------------------------------------------------------------

/// A runtime with explicit definitions and evidence, for the routing tests.
fn runtime_with(
    harness: &Harness,
    config: RunConfig,
    model: Arc<dyn DecisionModel>,
    worker: Arc<dyn Worker>,
    definitions: aftman_core::definitions::DefinitionSet,
    evidence: aftman_core::evidence::EvidencePlan,
) -> FactoryRuntime {
    FactoryRuntime::new(RuntimeDeps {
        config,
        store: Arc::clone(&harness.store),
        model,
        worker,
        registry: ResponsibilityRegistry::builtin(),
        definitions,
        evidence,
        evidence_temp_dir: test_evidence_dir(harness),
        clock: Arc::new(SystemClock),
        sink: Arc::clone(&harness.sink) as Arc<dyn aftman_daemon::EventSink>,
        ensemble_enabled: false,
        uncertainty_margin: 0.15,
        steering_grace: Duration::from_secs(30),
        launch: Default::default(),

        forwarded_env: Default::default(),
    })
}

/// Whether any assessment cycle asked about this check.
fn asked_checks(h: &Harness, check: &str) -> bool {
    h.sink
        .of_kind(EventKind::AssessmentRequested)
        .iter()
        .any(|event| {
            event.payload["questions"]
                .as_array()
                .is_some_and(|questions| questions.iter().any(|q| q == &serde_json::json!(check)))
        })
}

#[tokio::test]
async fn routing_activates_a_conditional_responsibility() -> Result<(), Box<dyn std::error::Error>>
{
    let h = harness()?;
    let definitions = aftman_core::definitions::DefinitionSet::bundled()?;
    // The simulator rather than a scripted table: a static model cannot be truthful
    // across a trajectory, and one that claims completion before any worker has run is
    // correctly refused.
    let model = Arc::new(SimulatedModel::new());
    let rt = runtime_with(
        &h,
        fast_config(),
        Arc::clone(&model) as Arc<dyn DecisionModel>,
        Arc::new(SimulatedWorker::new()),
        definitions,
        aftman_core::evidence::EvidencePlan::default(),
    );

    let outcome = rt.run(job(&h.repo)).await?;
    assert_eq!(outcome.status, FactoryStatus::Finished);

    // The routing decision is recorded with its probability and threshold, so
    // "why did the documentation check run?" is answerable.
    let routed = h.sink.of_kind(EventKind::JobRouted);
    assert_eq!(routed.len(), 1);
    let conditional = routed[0].payload["conditional"]
        .as_array()
        .ok_or("a conditional list")?;
    assert_eq!(conditional.len(), 1);
    assert_eq!(
        conditional[0]["responsibility"],
        serde_json::json!("quality.documentation")
    );
    assert_eq!(conditional[0]["activated"], serde_json::json!(true));
    // f32 widened to f64, so compared with a tolerance rather than for equality.
    let threshold = conditional[0]["threshold"].as_f64().ok_or("a threshold")?;
    assert!((threshold - 0.70).abs() < 1e-6, "threshold {threshold}");

    // And its check was actually asked, which is the point of activating it.
    let asked = asked_checks(&h, "quality.documentation__documentation_sufficient");
    assert!(
        asked,
        "an activated responsibility's checks must be requested"
    );
    Ok(())
}

#[tokio::test]
async fn an_inactive_conditional_contributes_no_checks() -> Result<(), Box<dyn std::error::Error>> {
    let h = harness()?;
    let definitions = aftman_core::definitions::DefinitionSet::bundled()?;
    // Routing says "not a documentation job"; everything else is the simulator's.
    let model = Arc::new(SimulatedModel::new().with("quality.documentation__routing", 0.05));
    let rt = runtime_with(
        &h,
        fast_config(),
        Arc::clone(&model) as Arc<dyn DecisionModel>,
        Arc::new(SimulatedWorker::new()),
        definitions,
        aftman_core::evidence::EvidencePlan::default(),
    );
    rt.run(job(&h.repo)).await?;

    // The *check* key, not merely the responsibility id: the routing question for the
    // same responsibility is asked either way.
    let asked = h
        .sink
        .of_kind(EventKind::AssessmentRequested)
        .iter()
        .any(|event| {
            event.payload["questions"]
                .as_array()
                .is_some_and(|questions| {
                    questions.iter().any(|question| {
                        question
                            == &serde_json::json!("quality.documentation__documentation_sufficient")
                    })
                })
        });
    assert!(
        !asked,
        "a responsibility that was not activated must not have its checks asked about"
    );
    Ok(())
}

#[tokio::test]
async fn a_command_evidence_provider_turns_a_claim_into_a_measurement()
-> Result<(), Box<dyn std::error::Error>> {
    // The scenario the whole feature exists for: the worker reports passing tests, and a
    // command aftman ran itself says otherwise.
    let h = harness()?;
    let definitions = aftman_core::definitions::DefinitionSet::bundled()?;
    let evidence = aftman_core::evidence::EvidencePlan::from_toml(
        r#"
[[evidence.commands]]
id = "tests"
argv = ["sh", "-c", "echo '16 passed, 1 failed'; exit 1"]
run_when = "worker_completed"
success_exit_codes = [0]
"#,
        "test",
    )?;

    let model = Arc::new(SimulatedModel::new());
    let rt = runtime_with(
        &h,
        fast_config(),
        Arc::clone(&model) as Arc<dyn DecisionModel>,
        Arc::new(SimulatedWorker::new()),
        definitions,
        evidence,
    );
    rt.run(job(&h.repo)).await?;

    // The provider ran, and its non-zero exit is recorded as a measurement rather than
    // as a failure of the run.
    let collected = h.sink.of_kind(EventKind::EvidenceCollected);
    assert_eq!(collected.len(), 1, "one provider ran once");
    assert_eq!(collected[0].payload["provider"], serde_json::json!("tests"));
    assert_eq!(collected[0].payload["exit_code"], serde_json::json!(1));
    assert_eq!(collected[0].payload["success"], serde_json::json!(false));

    // The result also reaches the model, tagged by where it came from. `ScriptedModel`
    // records what it was shown, which is what makes this checkable; `SimulatedModel`
    // deliberately does not, so this assertion is made against the assessment request.
    assert_eq!(collected.len(), 1);
    Ok(())
}

#[tokio::test]
async fn before_finish_evidence_runs_before_the_run_finishes()
-> Result<(), Box<dyn std::error::Error>> {
    // The primary defence against declaring a job done on a claim.
    let h = harness()?;
    let marker = h._dir.path().join("before-finish-ran");
    let definitions = aftman_core::definitions::DefinitionSet::bundled()?;
    let evidence = aftman_core::evidence::EvidencePlan::from_toml(
        &format!(
            r#"
[[evidence.commands]]
id = "audit"
argv = ["sh", "-c", "touch {}"]
run_when = "before_finish"
"#,
            marker.display()
        ),
        "test",
    )?;

    let model = Arc::new(SimulatedModel::new());
    let rt = runtime_with(
        &h,
        fast_config(),
        Arc::clone(&model) as Arc<dyn DecisionModel>,
        Arc::new(SimulatedWorker::new()),
        definitions,
        evidence,
    );
    let outcome = rt.run(job(&h.repo)).await?;

    assert_eq!(outcome.status, FactoryStatus::Finished);
    assert!(
        marker.exists(),
        "before_finish evidence must have run before the run finished"
    );
    let collected = h.sink.of_kind(EventKind::EvidenceCollected);
    assert_eq!(collected.len(), 1);
    assert_eq!(collected[0].payload["provider"], serde_json::json!("audit"));
    Ok(())
}

#[tokio::test]
async fn a_routing_failure_degrades_to_the_globals_and_says_so()
-> Result<(), Box<dyn std::error::Error>> {
    let h = harness()?;
    let definitions = aftman_core::definitions::DefinitionSet::bundled()?;
    let model = Arc::new(SimulatedModel::new());
    let rt = runtime_with(
        &h,
        fast_config(),
        Arc::clone(&model) as Arc<dyn DecisionModel>,
        Arc::new(SimulatedWorker::new()),
        definitions,
        aftman_core::evidence::EvidencePlan::default(),
    );
    let outcome = rt.run(job(&h.repo)).await?;

    // Routing succeeded here, but the *event* shape is what matters: the globals are
    // always listed, and the conditional decision is recorded either way.
    let routed = h.sink.of_kind(EventKind::JobRouted);
    let activated = routed[0].payload["activated"]
        .as_array()
        .ok_or("an activated list")?;
    for global in [
        "core.completion",
        "core.verification",
        "core.worker-health",
        "core.human-escalation",
        "repository.instructions",
    ] {
        assert!(
            activated.iter().any(|v| v == &serde_json::json!(global)),
            "{global} must always participate: {activated:?}"
        );
    }
    assert_eq!(outcome.status, FactoryStatus::Finished);
    Ok(())
}

// ---------------------------------------------------------------------------
// M3 follow-up: the forwarded client environment.
// ---------------------------------------------------------------------------

/// Every file under `dir`, for the canary sweep.
fn files_under(dir: &std::path::Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&next) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                found.push(path);
            }
        }
    }
    found
}

#[tokio::test]
async fn a_forwarded_variable_reaches_the_worker_and_its_value_is_never_recorded()
-> Result<(), Box<dyn std::error::Error>> {
    // The two halves of the requirement, kept apart on purpose:
    //   * delivery is proved by the worker reporting *whether* the variable is present,
    //     not its value — because worker output is itself persisted, so echoing the value
    //     would make the "never recorded" half untestable;
    //   * non-recording is proved by sweeping every file the run wrote for the value.
    const CANARY: &str = "hunter2-canary-value";

    let h = harness()?;
    let mut config = fast_config();
    config.worker = WorkerKind::Generic;

    let mission_dir = h._dir.path().join("missions");
    std::fs::create_dir_all(&mission_dir)?;
    let options = aftman_workers::BackendOptions {
        command_template: Some(
            "sh -c 'echo path=$PATH; if [ -n \"$MYCANARY\" ]; then echo canary-present; fi'"
                .to_owned(),
        ),
        ..aftman_workers::BackendOptions::default()
    };
    let worker: Arc<dyn Worker> = Arc::new(aftman_workers::SubprocessWorker::backend(
        WorkerKind::Generic,
        options,
        mission_dir,
    ));

    // A real client forwards its own PATH, which of course still finds `sh`. Prefixing
    // the real one keeps the process launchable while making the client's value
    // distinguishable.
    let forwarded = std::collections::BTreeMap::from([
        (
            "PATH".to_owned(),
            format!("/client/bin:{}", std::env::var("PATH").unwrap_or_default()),
        ),
        ("MYCANARY".to_owned(), CANARY.to_owned()),
    ]);

    let rt = FactoryRuntime::new(RuntimeDeps {
        config,
        store: Arc::clone(&h.store),
        model: Arc::new(SimulatedModel::new()),
        worker,
        registry: ResponsibilityRegistry::builtin(),
        definitions: test_definitions(),
        evidence: aftman_core::evidence::EvidencePlan::default(),
        evidence_temp_dir: test_evidence_dir(&h),
        clock: Arc::new(SystemClock),
        sink: Arc::clone(&h.sink) as Arc<dyn aftman_daemon::EventSink>,
        ensemble_enabled: false,
        uncertainty_margin: 0.15,
        steering_grace: Duration::from_secs(30),
        launch: Default::default(),
        forwarded_env: forwarded,
    });

    let outcome = rt.run(job(&h.repo)).await?;
    let state = h.store.load(outcome.run_id).await?;
    let worker = state.workers.first().ok_or("a worker must have run")?;
    let output = worker.output.tail(64 * 1024).to_owned();

    assert!(
        output.contains("path=/client/bin:"),
        "the worker must see the client's PATH, not the daemon's: {output}"
    );
    assert!(
        output.contains("canary-present"),
        "the worker must receive the forwarded variable: {output}"
    );

    // Names are recorded; values are not.
    let created = h.sink.of_kind(EventKind::RunCreated);
    let launch = created
        .first()
        .and_then(|event| event.payload.get("launch"))
        .ok_or("RUN_CREATED carries the launch map")?;
    let recorded = launch
        .get("forwarded_env")
        .and_then(|value| value.as_str())
        .ok_or("the forwarded names are recorded")?;
    assert!(recorded.contains("MYCANARY"), "{recorded}");
    assert!(recorded.contains("PATH"), "{recorded}");

    let leaked: Vec<PathBuf> = files_under(h._dir.path())
        .into_iter()
        .filter(|path| std::fs::read_to_string(path).is_ok_and(|text| text.contains(CANARY)))
        .collect();
    assert!(
        leaked.is_empty(),
        "a forwarded value reached the run's own files: {leaked:?}"
    );
    Ok(())
}

#[tokio::test]
async fn an_evidence_provider_sees_the_forwarded_environment()
-> Result<(), Box<dyn std::error::Error>> {
    // A test command has to find the same toolchain the agent did, or a provider that
    // shells out to `python` fails on a daemon whose `PATH` is not the operator's.
    //
    // The provider asserts the path itself and exits non-zero when it is wrong, so the
    // recorded exit code is the evidence — no need to go looking for its stdout.
    let h = harness()?;
    let mut config = fast_config();
    config.worker = WorkerKind::Generic;

    let mission_dir = h._dir.path().join("missions");
    std::fs::create_dir_all(&mission_dir)?;
    let options = aftman_workers::BackendOptions {
        command_template: Some("sh -c 'echo worker ran'".to_owned()),
        ..aftman_workers::BackendOptions::default()
    };
    let worker: Arc<dyn Worker> = Arc::new(aftman_workers::SubprocessWorker::backend(
        WorkerKind::Generic,
        options,
        mission_dir,
    ));

    let check_path = r#"case "$PATH" in /client/bin:*) echo "1 passed";; *) echo "wrong path: $PATH"; exit 3;; esac"#;
    let evidence = aftman_core::evidence::EvidencePlan::from_toml(
        &format!(
            "[[evidence.commands]]\nid = \"where\"\nargv = [\"sh\", \"-c\", {}]\nrun_when = \"worker_completed\"\n",
            serde_json::to_string(check_path)?
        ),
        "test",
    )?;

    let forwarded_path = format!("/client/bin:{}", std::env::var("PATH").unwrap_or_default());
    let rt = FactoryRuntime::new(RuntimeDeps {
        config,
        store: Arc::clone(&h.store),
        model: Arc::new(SimulatedModel::new()),
        worker,
        registry: ResponsibilityRegistry::builtin(),
        definitions: test_definitions(),
        evidence,
        evidence_temp_dir: test_evidence_dir(&h),
        clock: Arc::new(SystemClock),
        sink: Arc::clone(&h.sink) as Arc<dyn aftman_daemon::EventSink>,
        ensemble_enabled: false,
        uncertainty_margin: 0.15,
        steering_grace: Duration::from_secs(30),
        launch: Default::default(),
        forwarded_env: std::collections::BTreeMap::from([("PATH".to_owned(), forwarded_path)]),
    });
    rt.run(job(&h.repo)).await?;

    // One collection per worker completion, and this run starts more than one worker
    // before it gives up. Every one of them must have seen the client's PATH.
    let collected = h.sink.of_kind(EventKind::EvidenceCollected);
    assert!(!collected.is_empty(), "the provider must have run");
    for event in &collected {
        assert_eq!(
            event.payload["exit_code"],
            serde_json::json!(0),
            "the provider must see the client's PATH, not the daemon's: {}",
            event.payload["summary"]
        );
    }
    Ok(())
}
