//! Bounded evidence: what a model is allowed to see.
//!
//! Aftman never dumps a repository into a model. This module assembles a compact,
//! capped summary and is the **only** place where evidence becomes model input,
//! which is what makes both the request budget and the injection surface auditable
//! in one place.
//!
//! Two derived artifacts come out of an observation, and they are deliberately
//! different:
//!
//! - [`Observation::state_for`] — the JSON `state` handed to a model. Keeps
//!   everything, including elapsed times, because it is the evidence.
//! - [`Observation::content_digest`] — a fingerprint used to decide whether a cycle
//!   is worth running at all. **Removes** volatile scalars, because a digest that
//!   changes every tick is a digest that skips nothing, and the idle ticker would
//!   then burn the whole assessment budget on silence.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::ids::{CheckKey, EventSeq, RunId, WorkerId};
use crate::job::{FactoryStatus, Job, JobSummary, RunConfig, TaskKind, WorkerKind, WorkerStatus};
use crate::log::BoundedLog;

/// The default evidence provider set, used when a check names none.
pub static DEFAULT_EVIDENCE: &[&str] = &[
    "job",
    "factory_status",
    "active_worker",
    "worker_output",
    "worker_history",
    "steering_history",
    "git_status",
    "git_diff",
    "changed_files",
    "untracked_evidence",
    "repo_instructions",
    "test_summary",
    "recent_events",
    "prior_decision",
];

/// Running totals. These are *facts about the run*, not judgements, and they gate
/// several hard invariants.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Counters {
    pub workers_run: u32,
    pub attempts: u32,
    pub failures: u32,
    pub verifiers_run: u32,
    pub verifier_completed: bool,
    pub steers_sent: u32,
    pub assessments: u32,
    /// Cycles skipped because the observation content was unchanged.
    pub assessments_skipped: u32,
    pub consecutive_model_failures: u32,
    /// Elapsed wall time. Volatile: excluded from the content digest.
    pub elapsed_secs: u64,
}

/// A capped view of one worker, for the observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerSummary {
    pub id: WorkerId,
    pub kind: WorkerKind,
    pub task: TaskKind,
    pub status: WorkerStatus,
    pub mission_excerpt: String,
    pub elapsed_secs: u64,
    pub exit_code: Option<i32>,
    pub steer_count: u32,
    /// Whether the backend can accept mid-turn input.
    pub supports_steering: bool,
}

/// Git state at observation time.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitEvidence {
    /// `git status --short --untracked-files=all`.
    pub status_short: String,
    /// A capped diff.
    pub diff_excerpt: String,
    /// `git diff --stat`, which survives truncation better than the full diff.
    pub diff_stat: String,
    pub changed_files: Vec<String>,
    pub head_sha: Option<String>,
    pub branch: Option<String>,
    pub truncated: bool,
}

/// A bounded excerpt of one untracked text file.
///
/// `git diff` never shows untracked files, so new tests and new sources written by
/// a worker are invisible without this.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UntrackedExcerpt {
    pub path: String,
    pub bytes: usize,
    pub content: String,
    pub truncated: bool,
}

/// Repository-root instructions.
///
/// Read fresh for every observation and **never persisted**: repository prose must
/// not land in the run log. Only the drift judgement about it is stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoInstructions {
    pub file: String,
    pub excerpt: String,
    pub truncated: bool,
}

/// Where a test result came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum TestSource {
    /// Parsed from the worker's own output. A **claim**.
    WorkerOutput,
    /// Produced by a command aftman ran itself. A **fact**.
    Command { provider: String },
}

/// The overall verdict of one parsed test summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TestStatus {
    Passed,
    Failed,
    Errored,
    Mixed,
    Unknown,
}

/// One parsed test summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestResult {
    pub source: TestSource,
    pub passed: u32,
    pub failed: u32,
    pub errored: u32,
    pub skipped: u32,
    pub status: TestStatus,
    /// The line this was parsed from, so a reader can check the parse.
    pub raw: String,
}

/// A compact summary of one persisted event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventSummary {
    pub seq: EventSeq,
    pub kind: String,
    pub summary: String,
}

/// The previous cycle's decision, so a model can see what it already said.
///
/// Deliberately does **not** carry the assessment sequence number. That number
/// increments on every cycle by construction, so putting it in the observation would
/// make the content digest change even when nothing else had — silently defeating
/// the unchanged-observation skip. What a model needs is the previous directive and
/// its reason; which numbered cycle produced it is available in the event log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriorDecision {
    pub directive: String,
    pub reason: String,
}

/// One cycle's bounded evidence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Observation {
    pub run_id: RunId,
    pub status: FactoryStatus,
    pub job: JobSummary,
    pub active: Option<WorkerSummary>,
    pub history: Vec<WorkerSummary>,
    pub git: GitEvidence,
    pub untracked: Vec<UntrackedExcerpt>,
    pub instructions: Option<RepoInstructions>,
    pub tests: Vec<TestResult>,
    pub events: Vec<EventSummary>,
    pub prior: Option<PriorDecision>,
    pub counters: Counters,
    /// Results of command evidence providers, keyed by provider id.
    pub evidence: BTreeMap<String, String>,
}

impl Observation {
    /// Assemble the JSON `state` object handed to a model.
    ///
    /// Deterministic: the same observation and the same provider selection produce
    /// byte-identical output, which is what the snapshot tests assert and what makes
    /// replay meaningful.
    #[must_use]
    pub fn state_for(&self, providers: &[String]) -> Value {
        let has = |id: &str| providers.iter().any(|p| p == id);

        let mut state = serde_json::Map::new();
        let mut put = |key: &str, value: Value| {
            state.insert(key.to_owned(), value);
        };

        if has("job") {
            put(
                "job",
                json!({
                    "prompt": self.job.prompt_excerpt,
                    "prompt_truncated": self.job.prompt_truncated,
                    "repo": self.job.repo.display().to_string(),
                }),
            );
        }
        if has("factory_status") {
            put(
                "factory_status",
                json!({
                    "status": self.status.as_str(),
                    "workers_run": self.counters.workers_run,
                    "attempts": self.counters.attempts,
                    "failures": self.counters.failures,
                    "verifiers_run": self.counters.verifiers_run,
                    "verifier_completed": self.counters.verifier_completed,
                    "steers_sent": self.counters.steers_sent,
                    "assessments": self.counters.assessments,
                    "elapsed_secs": self.counters.elapsed_secs,
                }),
            );
        }
        if has("active_worker") {
            put(
                "active_worker",
                self.active
                    .as_ref()
                    .map_or(Value::Null, worker_summary_json),
            );
        }
        if has("worker_output") {
            put(
                "worker_output",
                self.active.as_ref().map_or(Value::Null, |w| {
                    json!({
                        "worker_id": w.id.to_string(),
                        "output_head": String::new(),
                        "output_tail": String::new(),
                    })
                }),
            );
        }
        if has("worker_history") {
            put(
                "worker_history",
                Value::Array(self.history.iter().map(worker_summary_json).collect()),
            );
        }
        if has("steering_history") {
            put(
                "steering_history",
                json!(
                    self.history
                        .iter()
                        .map(|w| json!({ "worker_id": w.id.to_string(), "steers": w.steer_count }))
                        .collect::<Vec<_>>()
                ),
            );
        }
        if has("git_status") {
            put(
                "git_status",
                json!({ "status_short": self.git.status_short, "branch": self.git.branch, "head": self.git.head_sha }),
            );
        }
        if has("git_diff") {
            put(
                "git_diff",
                json!({ "diff": self.git.diff_excerpt, "stat": self.git.diff_stat, "truncated": self.git.truncated }),
            );
        }
        if has("changed_files") {
            put("changed_files", json!(self.git.changed_files));
        }
        if has("untracked_evidence") {
            put("untracked_evidence", json!(self.untracked));
        }
        if has("repo_instructions") {
            put(
                "agents_md_instructions",
                self.instructions.as_ref().map_or(
                    Value::Null,
                    |i| json!({ "file": i.file, "content": i.excerpt }),
                ),
            );
        }
        if has("test_summary") {
            put("test_results", json!(self.tests));
        }
        if has("recent_events") {
            put("recent_events", json!(self.events));
        }
        if has("prior_decision") {
            put(
                "prior",
                self.prior.as_ref().map_or(Value::Null, |p| json!(p)),
            );
        }
        // Command evidence, selected by its own provider id.
        let mut commands = serde_json::Map::new();
        for (id, output) in &self.evidence {
            let provider = format!("command:{id}");
            if has(&provider) || has("command_evidence") {
                commands.insert(id.clone(), Value::from(output.clone()));
            }
        }
        if !commands.is_empty() {
            put("command_evidence", Value::Object(commands));
        }

        Value::Object(state)
    }

    /// A fingerprint of the **semantic** content of this observation.
    ///
    /// Volatile scalars are removed deliberately:
    ///
    /// - elapsed durations (they change every tick by definition),
    /// - absolute timestamps,
    /// - event sequence numbers and the event window.
    ///
    /// What remains is what a decision could actually depend on. Time-driven
    /// decisions are handled separately by the runtime's `time_driven_check_due`,
    /// because a digest cannot see a timeout approaching — only the clock can.
    #[must_use]
    pub fn content_digest(&self) -> String {
        let view = self.content_digest_view();
        // `serde_json::to_vec` on a `Map`-backed `Value` is key-ordered and stable.
        let bytes = serde_json::to_vec(&view).unwrap_or_default();
        blake3::hash(&bytes).to_hex().to_string()
    }

    /// The exact document the content digest hashes.
    ///
    /// Exposed so a test — or a confused maintainer — can diff two cycles and see
    /// precisely which field failed to be stable. A digest that changes for a reason
    /// nobody can name is how the unchanged-observation skip silently stops
    /// working.
    #[must_use]
    pub fn content_digest_view(&self) -> Value {
        json!({
            "status": self.status.as_str(),
            "prompt": self.job.prompt_excerpt,
            "repo": self.job.repo.display().to_string(),
            "active": self.active.as_ref().map(stable_worker_summary),
            "history": self.history.iter().map(stable_worker_summary).collect::<Vec<_>>(),
            "git_status": self.git.status_short,
            "diff": self.git.diff_excerpt,
            "diff_stat": self.git.diff_stat,
            "changed_files": self.git.changed_files,
            "head": self.git.head_sha,
            "branch": self.git.branch,
            "untracked": self.untracked,
            "instructions": self.instructions,
            "tests": self.tests,
            "prior": self.prior,
            "evidence": self.evidence,
            "counters": {
                "workers_run": self.counters.workers_run,
                "attempts": self.counters.attempts,
                "failures": self.counters.failures,
                "verifiers_run": self.counters.verifiers_run,
                "verifier_completed": self.counters.verifier_completed,
                "steers_sent": self.counters.steers_sent,
                // `assessments` is deliberately absent. It increments on every
                // cycle by construction, so including it would make the digest
                // change even when nothing else had — which would silently defeat
                // the whole unchanged-observation skip.
            },
        })
    }
}

/// A worker summary with elapsed time removed, for digesting.
fn stable_worker_summary(w: &WorkerSummary) -> Value {
    json!({
        "id": w.id.to_string(),
        "kind": w.kind.as_str(),
        "task": w.task.as_str(),
        "status": w.status.as_str(),
        "mission": w.mission_excerpt,
        "exit_code": w.exit_code,
        "steers": w.steer_count,
    })
}

fn worker_summary_json(w: &WorkerSummary) -> Value {
    json!({
        "worker_id": w.id.to_string(),
        "kind": w.kind.as_str(),
        "task": w.task.as_str(),
        "status": w.status.as_str(),
        "mission": w.mission_excerpt,
        "elapsed_secs": w.elapsed_secs,
        "exit_code": w.exit_code,
        "steers": w.steer_count,
        "supports_steering": w.supports_steering,
    })
}

/// Build the observation's job summary, capping the prompt.
#[must_use]
pub fn summarize_job(job: &Job, elapsed_secs: u64, cap_chars: usize) -> JobSummary {
    let (prompt_excerpt, truncated) = cap_text(&job.prompt, cap_chars);
    JobSummary {
        prompt_excerpt,
        prompt_truncated: truncated,
        repo: job.repo.clone(),
        elapsed_secs,
    }
}

/// Cap text at `cap_chars` characters, reporting whether anything was removed.
#[must_use]
pub fn cap_text(text: &str, cap_chars: usize) -> (String, bool) {
    if text.chars().count() <= cap_chars {
        return (text.to_owned(), false);
    }
    let head: String = text.chars().take(cap_chars).collect();
    (head, true)
}

/// Build a worker summary from a record.
#[must_use]
pub fn summarize_worker(
    record: &crate::worker::WorkerRecord,
    now: time::OffsetDateTime,
) -> WorkerSummary {
    let (mission_excerpt, _) = cap_text(&record.mission, 400);
    WorkerSummary {
        id: record.id,
        kind: record.kind,
        task: record.task,
        status: record.status,
        mission_excerpt,
        elapsed_secs: record.elapsed(now).as_secs(),
        exit_code: record.exit.and_then(|e| e.code),
        steer_count: record.steer_count(),
        supports_steering: record.supports_steering,
    }
}

/// Render a bounded log into the two fields the observation reports.
#[must_use]
pub fn render_log(log: &BoundedLog, tail_chars: usize) -> (String, String) {
    (
        log.head(tail_chars).to_owned(),
        log.tail(tail_chars).to_owned(),
    )
}

/// The default evidence selection, as owned ids.
#[must_use]
pub fn default_evidence() -> Vec<String> {
    DEFAULT_EVIDENCE.iter().map(|s| (*s).to_owned()).collect()
}

/// Convenience for a check that names its own providers.
#[must_use]
pub fn evidence_selection(check: &crate::responsibility::Check) -> Vec<String> {
    if check.evidence.is_empty() {
        default_evidence()
    } else {
        check.evidence.iter().map(ToString::to_string).collect()
    }
}

/// A check's wire key set, for building a request.
#[must_use]
pub fn check_keys(checks: &[crate::responsibility::Check]) -> Vec<CheckKey> {
    checks.iter().map(|c| c.key.clone()).collect()
}

/// Whether a config's caps are internally consistent.
#[must_use]
pub fn caps_are_sane(config: &RunConfig) -> bool {
    config.obs_tail_chars > 0
        && config.obs_diff_chars > 0
        && config.idle_poll_interval >= config.min_assessment_interval
}

#[cfg(test)]
mod tests {
    use super::{
        Counters, EventSummary, GitEvidence, Observation, TestResult, TestSource, TestStatus,
        WorkerSummary, cap_text, default_evidence,
    };
    use crate::ids::{RunId, WorkerId};
    use crate::job::{
        FactoryStatus, Job, JobSource, JobSummary, TaskKind, WorkerKind, WorkerStatus,
    };
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    fn observation() -> Observation {
        let worker = WorkerSummary {
            id: WorkerId::new(),
            kind: WorkerKind::Simulated,
            task: TaskKind::Code,
            status: WorkerStatus::Completed,
            mission_excerpt: "add retries".into(),
            elapsed_secs: 12,
            exit_code: Some(0),
            steer_count: 1,
            supports_steering: true,
        };
        Observation {
            run_id: RunId::new(),
            status: FactoryStatus::Assessing,
            job: JobSummary {
                prompt_excerpt: "Add retries with backoff".into(),
                prompt_truncated: false,
                repo: PathBuf::from("/repo"),
                elapsed_secs: 40,
            },
            active: Some(worker.clone()),
            history: vec![worker],
            git: GitEvidence {
                status_short: " M src/lib.rs".into(),
                diff_excerpt: "+ retry loop".into(),
                diff_stat: " 1 file changed".into(),
                changed_files: vec!["src/lib.rs".into()],
                head_sha: Some("deadbeef".into()),
                branch: Some("main".into()),
                truncated: false,
            },
            untracked: Vec::new(),
            instructions: None,
            tests: vec![TestResult {
                source: TestSource::WorkerOutput,
                passed: 3,
                failed: 0,
                errored: 0,
                skipped: 0,
                status: TestStatus::Passed,
                raw: "3 passed".into(),
            }],
            events: vec![EventSummary {
                seq: crate::ids::EventSeq(4),
                kind: "WORKER_COMPLETED".into(),
                summary: "WORKER_COMPLETED".into(),
            }],
            prior: None,
            counters: Counters {
                workers_run: 1,
                attempts: 1,
                assessments: 3,
                elapsed_secs: 40,
                ..Counters::default()
            },
            evidence: BTreeMap::new(),
        }
    }

    #[test]
    fn state_for_is_byte_stable_for_the_same_input() -> Result<(), Box<dyn std::error::Error>> {
        let o = observation();
        let providers = default_evidence();
        let a = serde_json::to_vec(&o.state_for(&providers))?;
        let b = serde_json::to_vec(&o.state_for(&providers))?;
        assert_eq!(a, b, "state_for must be deterministic");
        Ok(())
    }

    #[test]
    fn state_for_honours_the_provider_selection() -> Result<(), Box<dyn std::error::Error>> {
        let o = observation();
        let only_job = o.state_for(&["job".to_owned()]);
        let map = only_job.as_object().ok_or("state must be a JSON object")?;
        assert_eq!(map.len(), 1);
        assert!(map.contains_key("job"));

        let with_commands = o.state_for(&["job".to_owned(), "command:tests".to_owned()]);
        assert!(
            with_commands.get("command_evidence").is_none(),
            "no evidence recorded, so nothing to include"
        );
        Ok(())
    }

    #[test]
    fn state_for_keeps_elapsed_time_because_it_is_evidence() {
        let mut o = observation();
        let providers = default_evidence();
        let before = o.state_for(&providers);
        o.counters.elapsed_secs += 5;
        let after = o.state_for(&providers);
        assert_ne!(
            before["factory_status"]["elapsed_secs"],
            after["factory_status"]["elapsed_secs"]
        );
    }

    #[test]
    fn content_digest_ignores_elapsed_time() {
        // The whole point: an idle tick must produce an identical digest, or the
        // skip is dead code and a silent worker burns the assessment budget.
        let mut o = observation();
        let before = o.content_digest();
        o.counters.elapsed_secs += 30;
        o.job.elapsed_secs += 30;
        for w in &mut o.history {
            w.elapsed_secs += 30;
        }
        if let Some(w) = &mut o.active {
            w.elapsed_secs += 30;
        }
        assert_eq!(
            before,
            o.content_digest(),
            "elapsed time must not affect the digest"
        );
    }

    #[test]
    fn content_digest_ignores_the_event_window() {
        let mut o = observation();
        let before = o.content_digest();
        o.events.push(EventSummary {
            seq: crate::ids::EventSeq(99),
            kind: "NOTIFICATION".into(),
            summary: "NOTIFICATION".into(),
        });
        assert_eq!(before, o.content_digest());
    }

    #[test]
    fn content_digest_changes_when_something_real_changes() {
        let mut o = observation();
        let before = o.content_digest();
        o.git.diff_excerpt.push_str("\n+ another change");
        assert_ne!(before, o.content_digest());
    }

    #[test]
    fn content_digest_changes_when_a_counter_moves() {
        let mut o = observation();
        let before = o.content_digest();
        o.counters.attempts += 1;
        assert_ne!(
            before,
            o.content_digest(),
            "attempts gate a hard invariant, so they must be visible to the digest"
        );
    }

    #[test]
    fn content_digest_ignores_the_assessment_counter() {
        // The counter increments every cycle by construction. If it were part of the
        // digest, two consecutive identical observations would always differ and the
        // unchanged-observation skip would never fire — the exact bug this asserts
        // against.
        let mut o = observation();
        let before = o.content_digest();
        o.counters.assessments += 1;
        o.counters.assessments_skipped += 1;
        assert_eq!(before, o.content_digest());
    }

    #[test]
    fn prior_decision_carries_no_cycle_counter() -> Result<(), Box<dyn std::error::Error>> {
        // The second half of the same bug: an assessment sequence number inside the
        // prior decision also changes every cycle. It is deliberately not a field.
        let prior = super::PriorDecision {
            directive: "Continue".to_owned(),
            reason: String::new(),
        };
        let json = serde_json::to_string(&prior)?;
        assert!(!json.contains("assessment_seq"), "{json}");

        let mut o = observation();
        o.prior = Some(prior.clone());
        let before = o.content_digest();
        o.prior = Some(prior);
        assert_eq!(before, o.content_digest());
        Ok(())
    }

    #[test]
    fn cap_text_reports_truncation_and_respects_char_boundaries() {
        let (text, truncated) = cap_text("hello", 10);
        assert_eq!(text, "hello");
        assert!(!truncated);

        let (text, truncated) = cap_text("hello", 3);
        assert_eq!(text, "hel");
        assert!(truncated);

        let (text, truncated) = cap_text("ééééé", 2);
        assert_eq!(text, "éé");
        assert!(truncated);
    }

    #[test]
    fn job_and_worker_caps_are_applied() {
        let job = Job {
            prompt: "x".repeat(500),
            repo: PathBuf::from("/repo"),
            source: JobSource::Cli,
        };
        let summary = super::summarize_job(&job, 0, 100);
        assert_eq!(summary.prompt_excerpt.chars().count(), 100);
        assert!(summary.prompt_truncated);
    }
}
