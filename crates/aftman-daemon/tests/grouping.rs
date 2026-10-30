//! Regression tests from the adversarial review of evidence-selection grouping
//! (`docs/03-jev.md` §5): each began as a probe that failed before its fix. Offline: a model
//! that answers from the state it was shown, `SimulatedWorker`, and `sh` audit scripts.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;

use aftman_core::assessment::AnswerValue;
use aftman_core::clock::SystemClock;
use aftman_core::definitions::{DefinitionSet, ResponsibilityDefinition};
use aftman_core::ensemble::EnsembleSettings;
use aftman_core::error::ModelError;
use aftman_core::event::{EventEnvelope, EventKind};
use aftman_core::evidence::EvidencePlan;
use aftman_core::ids::CheckKey;
use aftman_core::job::{FactoryStatus, Job, JobSource, RunConfig, WorkerKind};
use aftman_core::model::{
    AskContext, Budget, DecisionModel, ModelAnswers, Purpose, Question, Usage,
};
use aftman_core::responsibility::ResponsibilityRegistry;
use aftman_daemon::{FactoryRuntime, RunOutcome, RuntimeDeps, VecSink};
use aftman_store::{FileRunStore, RunStore};
use aftman_workers::SimulatedWorker;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const READY: &str = "core.completion__ready_to_finish";

const DONE: [(&str, f32); 11] = [
    ("core.completion__ready_to_finish", 0.95),
    ("core.completion__implementation_complete", 0.95),
    ("core.completion__requirements_satisfied", 0.95),
    ("core.verification__tests_sufficient", 0.95),
    ("core.verification__needs_verification", 0.10),
    ("core.worker-health__worker_stuck", 0.02),
    ("core.worker-health__work_off_track", 0.02),
    ("core.human-escalation__needs_human", 0.01),
    ("repository.instructions__agents_md_drift", 0.01),
    ("core.worker-health__meaningful_progress", 0.60),
    ("quality.documentation__documentation_sufficient", 0.95),
];
const NOT_YET: [(&str, f32); 11] = [
    ("core.completion__ready_to_finish", 0.10),
    ("core.completion__implementation_complete", 0.35),
    ("core.completion__requirements_satisfied", 0.30),
    ("core.verification__tests_sufficient", 0.30),
    ("core.verification__needs_verification", 0.40),
    ("core.worker-health__worker_stuck", 0.10),
    ("core.worker-health__work_off_track", 0.08),
    ("core.human-escalation__needs_human", 0.02),
    ("repository.instructions__agents_md_drift", 0.0),
    ("core.worker-health__meaningful_progress", 0.90),
    ("quality.documentation__documentation_sufficient", 0.40),
];

type Rule = Box<dyn Fn(&str, &Value, &str) -> Option<f32> + Send + Sync>;
type Fail = Box<dyn Fn(&[CheckKey]) -> bool + Send + Sync>;

struct Call {
    purpose: Purpose,
    keys: Vec<CheckKey>,
    state: String,
}

/// Answers from `rule(key, state, text)`, then from DONE/NOT_YET by `workers_run`.
struct ProbeModel {
    rule: Rule,
    fail: Option<Fail>,
    calls: Mutex<Vec<Call>>,
}

impl ProbeModel {
    fn new(rule: impl Fn(&str, &Value, &str) -> Option<f32> + Send + Sync + 'static) -> Self {
        Self {
            rule: Box::new(rule),
            fail: None,
            calls: Mutex::new(Vec::new()),
        }
    }
    fn failing(mut self, fail: impl Fn(&[CheckKey]) -> bool + Send + Sync + 'static) -> Self {
        self.fail = Some(Box::new(fail));
        self
    }
    fn calls(&self, purpose: Purpose) -> Vec<(Vec<CheckKey>, String)> {
        self.calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|c| c.purpose == purpose)
            .map(|c| (c.keys.clone(), c.state.clone()))
            .collect()
    }
}

fn workers_run(state: &Value) -> Option<u64> {
    state["factory_status"]["workers_run"].as_u64()
}

#[async_trait]
impl DecisionModel for ProbeModel {
    async fn ask(
        &self,
        context: &AskContext,
        state: &Value,
        questions: &BTreeMap<CheckKey, Question>,
        _budget: &Budget,
    ) -> Result<ModelAnswers, ModelError> {
        let text = state.to_string();
        let keys: Vec<CheckKey> = questions.keys().cloned().collect();
        self.calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(Call {
                purpose: context.purpose,
                keys: keys.clone(),
                state: text.clone(),
            });
        if context.purpose == Purpose::Assessment
            && let Some(fail) = &self.fail
            && fail(&keys)
        {
            return Err(ModelError::BadRequest(
                "probe: this question set is rejected".into(),
            ));
        }
        let early = workers_run(state) == Some(0) || !text.contains("3 passed");
        let mut answers = BTreeMap::new();
        for key in &keys {
            let k = key.as_str();
            let p = if k.ends_with("__routing") {
                Some(0.75)
            } else {
                let table: &[(&str, f32)] = if early { &NOT_YET } else { &DONE };
                (self.rule)(k, state, &text)
                    .filter(|_| !early)
                    .or_else(|| table.iter().find(|(t, _)| *t == k).map(|(_, p)| *p))
            };
            let p = if context.purpose == Purpose::Reframe {
                p.map(|p| 1.0 - p)
            } else {
                p
            };
            if let Some(p) = p {
                answers.insert(key.clone(), AnswerValue::Noul(p));
            }
        }
        Ok(ModelAnswers {
            model: "probe".into(),
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

fn audited_definitions() -> Result<DefinitionSet, Box<dyn std::error::Error>> {
    let mut set = DefinitionSet::bundled()?;
    let bundled = aftman_core::definitions::BUNDLED
        .iter()
        .find(|(id, _)| *id == "core.completion")
        .map(|(_, source)| *source)
        .ok_or("completion")?;
    let marker = "[checks.ready_to_finish]\n";
    let at = bundled.find(marker).ok_or("ready")? + marker.len();
    let mut selection = aftman_core::observation::default_evidence();
    selection.push("command:audit".to_owned());
    let source = format!(
        "{}evidence = {}\n{}",
        &bundled[..at],
        serde_json::to_string(&selection)?,
        &bundled[at..]
    );
    let completion = ResponsibilityDefinition::parse("core.completion", &source, "test")?;
    set.definitions
        .insert("core.completion".to_owned(), completion);
    Ok(set)
}

/// An audit that prints "audit clean" until run `fail_from`, and FAILED from that run on.
fn counting_audit(
    dir: &Path,
    run_when: &str,
    fail_from: u32,
) -> Result<EvidencePlan, Box<dyn std::error::Error>> {
    let script = dir.join("audit.sh");
    let count = dir.join("audit-runs");
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\nn=$(cat '{c}' 2>/dev/null || echo 0); n=$((n+1)); echo $n > '{c}'\n\
             if [ $n -ge {fail_from} ]; then echo 'FAILED: 2 critical vulnerabilities'; exit 1; fi\n\
             echo 'audit clean'\n",
            c = count.display()
        ),
    )?;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))?;
    Ok(EvidencePlan::from_toml(
        &format!(
            "[[evidence.commands]]\nid = \"audit\"\nargv = [\"{}\"]\nrun_when = \"{run_when}\"\nsuccess_exit_codes = [0]\n",
            script.display()
        ),
        "test",
    )?)
}

struct Fixture {
    dir: tempfile::TempDir,
    store: Arc<dyn RunStore>,
    sink: Arc<VecSink>,
}

impl Fixture {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        std::fs::create_dir_all(dir.path().join("repo"))?;
        let store: Arc<dyn RunStore> = Arc::new(FileRunStore::new(dir.path().join("state")));
        Ok(Self {
            dir,
            store,
            sink: Arc::new(VecSink::new()),
        })
    }
    fn runtime(
        &self,
        config: RunConfig,
        enabled: bool,
        tweak: impl FnOnce(&mut EnsembleSettings),
        definitions: DefinitionSet,
        evidence: EvidencePlan,
        model: Arc<dyn DecisionModel>,
    ) -> FactoryRuntime {
        let temp = self.dir.path().join("evidence-tmp");
        let _ = std::fs::create_dir_all(&temp);
        let mut settings = EnsembleSettings {
            enabled,
            ..EnsembleSettings::default()
        };
        tweak(&mut settings);
        FactoryRuntime::new(RuntimeDeps {
            config,
            store: Arc::clone(&self.store),
            model,
            worker: Arc::new(SimulatedWorker::instant()),
            registry: ResponsibilityRegistry::builtin(),
            definitions,
            evidence,
            evidence_temp_dir: temp,
            clock: Arc::new(SystemClock),
            sink: Arc::clone(&self.sink) as Arc<dyn aftman_daemon::EventSink>,
            ensemble: settings,
            steering_grace: Duration::from_secs(30),
            launch: Default::default(),
            forwarded_env: Default::default(),
            jev_budget: Default::default(),
        })
    }
    async fn run(&self, rt: FactoryRuntime) -> Result<RunOutcome, Box<dyn std::error::Error>> {
        let job = Job {
            prompt: "Add request retries with backoff, and cover them with tests.".into(),
            repo: self.dir.path().join("repo"),
            source: JobSource::Cli,
        };
        Ok(tokio::time::timeout(Duration::from_secs(60), rt.run(job))
            .await
            .map_err(|_| "hung")??)
    }
    fn of_kind(&self, kind: EventKind) -> Vec<EventEnvelope> {
        self.sink.of_kind(kind)
    }
    fn trace(&self) -> Vec<String> {
        self.sink
            .events()
            .iter()
            .filter_map(|e| match e.kind {
                EventKind::WorkerStarted => Some(format!("worker:{}", e.payload["task"])),
                EventKind::EvidenceCollected => Some(format!("evidence:{}", e.payload["summary"])),
                EventKind::DirectiveSelected => {
                    Some(format!("selected:{}", e.payload["directive"]))
                }
                EventKind::EscalationRaised => Some(format!("escalation:{}", e.payload["reason"])),
                EventKind::StateChanged => Some(format!("state:{}", e.payload["to"])),
                _ => None,
            })
            .collect()
    }
}

fn config(tweak: impl FnOnce(&mut RunConfig)) -> RunConfig {
    let mut config = RunConfig {
        worker: WorkerKind::Simulated,
        min_assessment_interval: Duration::from_millis(1),
        idle_poll_interval: Duration::from_millis(10),
        overall_timeout: Duration::from_secs(60),
        worker_timeout: Duration::from_secs(10),
        max_workers: 6,
        max_attempts: 30,
        ..RunConfig::default()
    };
    tweak(&mut config);
    config
}

// ---- the failure counters are per cycle (review F2, F4) -------------------------------

#[tokio::test]
async fn a_cycle_whose_later_request_keeps_failing_still_escalates_model_unavailable() -> TestResult
{
    // Review F2 (probe B). Once the audit has run, `ready_to_finish` is a request of its own,
    // made second, and the model rejects it every cycle. The first request succeeded and used
    // to reset `consecutive_model_failures`, the second set it to 1, and it never reached 3:
    // supervision was blind and nothing said so. The counters are per cycle. And (F4) a partly
    // failed cycle reports the tokens of the request it did make, not null.
    let fx = Fixture::new()?;
    let model = Arc::new(
        ProbeModel::new(|_, _, _| None).failing(|keys| keys == [CheckKey::from(READY)].as_slice()),
    );
    let rt = fx.runtime(
        config(|c| c.max_workers = 6),
        false,
        |_| {},
        audited_definitions()?,
        counting_audit(fx.dir.path(), "worker_completed", 1000)?,
        Arc::clone(&model) as Arc<dyn DecisionModel>,
    );
    let outcome = fx.run(rt).await?;
    let trace = fx.trace();
    assert!(
        trace.iter().any(|t| t.contains("model_unavailable")),
        "{trace:#?}"
    );
    assert_eq!(outcome.status, FactoryStatus::NeedsHuman);
    let failed: Vec<EventEnvelope> = fx
        .of_kind(EventKind::AssessmentCompleted)
        .into_iter()
        .filter(|e| e.payload["model"] == "unavailable")
        .collect();
    assert_eq!(failed.len(), 3, "three failed cycles, then the escalation");
    let rejected = model
        .calls(Purpose::Assessment)
        .iter()
        .filter(|(keys, _)| keys == &[CheckKey::from(READY)])
        .count();
    assert_eq!(rejected, 3);
    for e in &failed {
        assert_eq!(e.payload["requests"].as_array().map(Vec::len), Some(2));
        assert_eq!(e.payload["input_tokens"], 10, "{}", e.payload);
        assert_eq!(e.payload["output_tokens"], 1, "{}", e.payload);
    }
    Ok(())
}

// ---- the budget is checked up front (review F3a) ---------------------------------------

#[tokio::test]
async fn no_request_is_made_that_the_cycle_cannot_use() -> TestResult {
    // Review F3 (probe C). Once the audit has run every cycle is two requests, and the work is
    // never done. With `max_assessments = 4`, the first cycle is one request (the states are
    // identical until the audit runs) and the second two; the third has one assessment left
    // for two requests. That request used to be made, and billed, though its cycle could never
    // use it. Now a cycle the budget left cannot cover makes no request, and policy escalates
    // as for a spent budget, saying so.
    let fx = Fixture::new()?;
    let model = Arc::new(ProbeModel::new(|key, _, _| (key == READY).then_some(0.10)));
    let rt = fx.runtime(
        config(|c| c.max_assessments = 4),
        false,
        |_| {},
        audited_definitions()?,
        counting_audit(fx.dir.path(), "worker_completed", 1000)?,
        Arc::clone(&model) as Arc<dyn DecisionModel>,
    );
    let outcome = fx.run(rt).await?;
    let completed = fx.of_kind(EventKind::AssessmentCompleted);
    let used: usize = completed
        .iter()
        .filter(|e| e.payload["model"] == "probe")
        .map(|e| e.payload["requests"].as_array().map_or(1, Vec::len))
        .sum();
    let made = model.calls(Purpose::Assessment).len();
    assert_eq!(made, used, "{made} requests made, answers of {used} used");
    assert_eq!(made, 3);
    // The cycle that could not be covered made no call, and said why.
    let last = completed.last().ok_or("a cycle")?;
    assert_eq!(last.payload["model"], "budget_exhausted");
    assert_eq!(outcome.status, FactoryStatus::NeedsHuman);
    let raised = fx.of_kind(EventKind::EscalationRaised);
    let ask = raised
        .last()
        .and_then(|e| e.payload["ask"].as_str())
        .unwrap_or_default()
        .to_owned();
    assert!(
        ask.contains("assessment budget (4 model calls) has too little left"),
        "{ask}"
    );
    assert!(outcome.counters.assessment_budget_short);
    Ok(())
}

// ---- identical states are one request (review F3b) -------------------------------------

#[tokio::test]
async fn groups_whose_states_are_identical_are_asked_once() -> TestResult {
    // Review F3(b). `ready_to_finish` selects the default set and `command:audit`. Until the
    // audit has run, that selection renders exactly the default state, and it was asked a
    // second time, and paid for. Identical states are one request; once the audit has run,
    // the states differ and are two.
    let fx = Fixture::new()?;
    let model = Arc::new(ProbeModel::new(|_, _, _| None));
    let rt = fx.runtime(
        config(|c| c.max_workers = 2),
        false,
        |_| {},
        audited_definitions()?,
        counting_audit(fx.dir.path(), "worker_completed", 1000)?,
        Arc::clone(&model) as Arc<dyn DecisionModel>,
    );
    fx.run(rt).await?;
    let counts: Vec<usize> = fx
        .of_kind(EventKind::AssessmentCompleted)
        .iter()
        .map(|e| e.payload["requests"].as_array().map_or(0, Vec::len))
        .collect();
    assert_eq!(counts.first(), Some(&1), "{counts:?}");
    assert!(counts.iter().skip(1).all(|n| *n == 2), "{counts:?}");
    assert_eq!(
        model.calls(Purpose::Assessment).len(),
        counts.iter().sum::<usize>()
    );
    // The merged request asked every check, `ready_to_finish` included.
    let (first, _) = &model.calls(Purpose::Assessment)[0];
    assert!(first.contains(&CheckKey::from(READY)));
    Ok(())
}
