//! `aftman decisions` over a real run: a repeated evidence refresh, capped, is visible in
//! what the command prints (`docs/09-plan.md` M5 exit criteria: "repeated refresh is
//! capped and visible").
//!
//! Offline: the simulated worker, a model that answers from the state it is shown, and an
//! `sh -c` one-liner as the command provider.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;

use aftman_cli::render::decision_lines;
use aftman_core::assessment::AnswerValue;
use aftman_core::clock::SystemClock;
use aftman_core::definitions::{DefinitionSet, ResponsibilityDefinition};
use aftman_core::ensemble::EnsembleSettings;
use aftman_core::error::ModelError;
use aftman_core::evidence::EvidencePlan;
use aftman_core::ids::CheckKey;
use aftman_core::job::{FactoryStatus, Job, JobSource, RunConfig, WorkerKind};
use aftman_core::model::{AskContext, Budget, DecisionModel, ModelAnswers, Question, Usage};
use aftman_core::responsibility::ResponsibilityRegistry;
use aftman_daemon::{FactoryRuntime, RuntimeDeps, VecSink};
use aftman_store::{FileRunStore, RunStore};
use aftman_workers::SimulatedWorker;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const TESTS: &str = "core.verification__tests_sufficient";

/// Done in every respect but one: `tests_sufficient` sits in the band whatever it is shown.
struct InBand;

#[async_trait]
impl DecisionModel for InBand {
    async fn ask(
        &self,
        _context: &AskContext,
        state: &Value,
        questions: &BTreeMap<CheckKey, Question>,
        _budget: &Budget,
    ) -> Result<ModelAnswers, ModelError> {
        let text = state.to_string();
        let table: [(&str, f32); 9] = [
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
        let tests = if text.contains("passed") { 0.71 } else { 0.10 };
        let answers = questions
            .keys()
            .filter_map(|key| {
                let p = if *key == CheckKey::from(TESTS) {
                    Some(tests)
                } else {
                    table
                        .iter()
                        .find(|(name, _)| CheckKey::from(*name) == *key)
                        .map(|(_, p)| *p)
                };
                p.map(|p| (key.clone(), AnswerValue::Noul(p)))
            })
            .collect();
        Ok(ModelAnswers {
            model: "in-band".into(),
            usage: Usage::default(),
            answers,
            request_id: None,
            attempts: 1,
        })
    }
}

#[tokio::test]
async fn repeated_refresh_is_capped_and_visible_in_aftman_decisions() -> TestResult {
    let dir = tempfile::tempdir()?;
    let repo = dir.path().join("repo");
    std::fs::create_dir_all(&repo)?;
    let temp = dir.path().join("evidence-tmp");
    std::fs::create_dir_all(&temp)?;
    let store: Arc<dyn RunStore> = Arc::new(FileRunStore::new(dir.path().join("state")));
    let sink = Arc::new(VecSink::new());

    let mut definitions = DefinitionSet::bundled()?;
    definitions.definitions.insert(
        "core.verification".to_owned(),
        ResponsibilityDefinition::parse(
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
        )?,
    );
    let evidence = EvidencePlan::from_toml(
        r#"
[[evidence.commands]]
id = "tests"
argv = ["sh", "-c", "echo '17 passed'; exit 0"]
run_when = "on_demand"
"#,
        "test",
    )?;
    let rt = FactoryRuntime::new(RuntimeDeps {
        config: RunConfig {
            worker: WorkerKind::Simulated,
            min_assessment_interval: Duration::from_millis(1),
            idle_poll_interval: Duration::from_millis(10),
            overall_timeout: Duration::from_secs(60),
            worker_timeout: Duration::from_secs(10),
            max_workers: 30,
            max_attempts: 30,
            ..RunConfig::default()
        },
        store: Arc::clone(&store),
        model: Arc::new(InBand),
        worker: Arc::new(SimulatedWorker::instant()),
        registry: ResponsibilityRegistry::builtin(),
        definitions,
        evidence,
        evidence_temp_dir: temp,
        clock: Arc::new(SystemClock),
        sink: Arc::clone(&sink) as Arc<dyn aftman_daemon::EventSink>,
        ensemble: EnsembleSettings {
            enabled: true,
            max_evidence_refreshes_per_check: 2,
            ..EnsembleSettings::default()
        },
        steering_grace: Duration::from_secs(30),
        launch: Default::default(),
        forwarded_env: Default::default(),
        jev_budget: Default::default(),
    });
    let outcome = tokio::time::timeout(
        Duration::from_secs(60),
        rt.run(Job {
            prompt: "Add retries, with tests.".into(),
            repo,
            source: JobSource::Cli,
        }),
    )
    .await
    .map_err(|_| "the run did not end")??;
    assert_eq!(outcome.status, FactoryStatus::NeedsHuman);

    // What `aftman decisions` prints for the run, from its stored log.
    let log = store.read_log(outcome.run_id).await?;
    let lines = decision_lines(&log.events);
    let text = lines.join("\n");
    let refreshed = lines
        .iter()
        .filter(|line| {
            line.contains("refreshed 0.71 -> 0.71 (tests exit 0); supersedes assessment")
        })
        .count();
    assert_eq!(
        refreshed, 2,
        "both refreshes, each with its provider and exit code:\n{text}"
    );
    assert!(
        text.contains("limit: refresh cap reached (2/2)"),
        "the cap is visible:\n{text}"
    );
    assert!(
        text.contains(&format!("{TESTS} p=0.71")) && text.contains("near_threshold"),
        "the check and its triggers:\n{text}"
    );
    assert!(
        text.contains("escalated: `core.verification__tests_sufficient` is 0.71"),
        "the escalation:\n{text}"
    );
    Ok(())
}
