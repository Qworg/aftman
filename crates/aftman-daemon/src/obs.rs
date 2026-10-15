//! Assembling bounded evidence from a run.
//!
//! Every field here has a byte or character cap, and the whole thing is designed so
//! that a repository of any size produces an observation of bounded size. This is
//! the only place repository state becomes model input, which is what makes both
//! the request budget and the injection surface auditable in one place.
//!
//! ## Simulation mode
//!
//! When the worker is [`WorkerKind::Simulated`] the builder produces **synthetic**
//! git and test evidence rather than reading the repository. Two reasons: a
//! simulated run must never dirty a user's working tree, and the runtime's own
//! tests must not depend on `git` being installed or on a fixture repository being
//! present. The synthesis is labelled in `evidence["simulated"]`, so a reader — or a
//! model — can tell that the diff did not come from a real tree.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use time::OffsetDateTime;

use aftman_core::ansi::strip_control;
use aftman_core::job::{Job, WorkerKind, WorkerStatus};
use aftman_core::log::BoundedLog;
use aftman_core::observation::{
    Counters, EventSummary, GitEvidence, Observation, RepoInstructions, TestResult, TestSource,
    TestStatus, UntrackedExcerpt, WorkerActivityExcerpt, WorkerOutputExcerpt, summarize_job,
    summarize_worker,
};
use aftman_core::run::RunState;
use aftman_core::worker::WorkerRecord;

/// How much of the job prompt is shown to a model.
pub const JOB_PROMPT_CAP: usize = 4_000;

/// Caps applied to the assembled observation.
#[derive(Debug, Clone, Copy)]
pub struct ObsCaps {
    pub diff_chars: usize,
    /// Bytes of the worker's output head retained for the model.
    ///
    /// The head shows what the agent was asked to do and how it started, where the tail
    /// shows where it ended up. The tail is the larger of the two because a worker that
    /// has gone wrong has usually gone wrong most recently.
    pub head_chars: usize,
    pub tail_chars: usize,
    pub events_history: usize,
    pub workers_history: usize,
    pub untracked_files: usize,
    pub untracked_bytes: usize,
    pub instruction_chars: usize,
}

impl Default for ObsCaps {
    fn default() -> Self {
        Self {
            diff_chars: 20_000,
            head_chars: 4_000,
            tail_chars: 12_000,
            events_history: 30,
            workers_history: 10,
            untracked_files: 3,
            untracked_bytes: 4_096,
            instruction_chars: 4_000,
        }
    }
}

/// Builds observations.
#[derive(Debug, Clone)]
pub struct ObservationBuilder {
    pub caps: ObsCaps,
    /// Produce synthetic evidence instead of reading the repository.
    pub simulation: bool,
    /// The overall limit on assembling the repository evidence of one observation.
    pub timeout: Duration,
}

impl ObservationBuilder {
    #[must_use]
    pub fn new(simulation: bool) -> Self {
        Self {
            caps: ObsCaps::default(),
            simulation,
            timeout: OBSERVATION_TIMEOUT,
        }
    }

    #[must_use]
    pub fn for_worker(kind: WorkerKind) -> Self {
        Self::new(matches!(kind, WorkerKind::Simulated))
    }

    /// Assemble one observation.
    ///
    /// `events` are the recent persisted event summaries, already capped by the
    /// caller's window.
    ///
    /// Reading the repository (status, diff, untracked files, `AGENTS.md`) is bounded and
    /// never blocks the caller's thread: it runs in-process on a blocking thread under one
    /// deadline ([`OBSERVATION_TIMEOUT`]), and a timeout sets an interrupt the observer
    /// polls. Observing a repository never executes a program the repository names; see
    /// [`crate::repo_observe`]. On any failure or timeout the repository evidence is omitted
    /// and a note says so under `evidence["observation_note"]`.
    pub async fn build(
        &self,
        state: &RunState,
        job: &Job,
        active: Option<&WorkerRecord>,
        event_summaries: Vec<EventSummary>,
        evidence: &[aftman_core::evidence::EvidenceOutcome],
        now: OffsetDateTime,
    ) -> Observation {
        let counters = Counters {
            elapsed_secs: elapsed_secs(state, now),
            ..state.counters.clone()
        };

        let active_summary = active.map(|w| summarize_worker(w, now));
        // History is the *other* workers. `active` already carries the most recent
        // one, whether it is still running or has finished.
        let active_id = active.map(|w| w.id);
        let history: Vec<_> = state
            .workers
            .iter()
            .filter(|w| Some(w.id) != active_id)
            .rev()
            .take(self.caps.workers_history)
            .map(|w| summarize_worker(w, now))
            .collect();

        let mut note = None;
        let (git, untracked, instructions, mut tests) = if self.simulation {
            (
                self.synthetic_git(active),
                Vec::new(),
                None,
                self.synthetic_tests(active),
            )
        } else {
            let tests = active.map_or_else(Vec::new, |w| parse_worker_tests(&w.output));
            match collect_repo(&job.repo, self.caps, self.timeout).await {
                Ok(repo) => {
                    if let Some(why) = repo.note {
                        note = Some(why);
                    }
                    (repo.git, repo.untracked, repo.instructions, tests)
                }
                Err(why) => {
                    tracing::warn!(repo = %job.repo.display(), why, "repository evidence omitted");
                    note = Some(why);
                    (GitEvidence::default(), Vec::new(), None, tests)
                }
            }
        };

        // Command evidence arrives as *facts*, tagged so a check can tell them apart
        // from a worker's claim about the same thing.
        let mut evidence_map = std::collections::BTreeMap::new();
        for outcome in evidence {
            evidence_map.insert(outcome.evidence_key(), outcome.output_excerpt.clone());
            // A provider that printed test summaries is the measurement the
            // test-sufficiency check actually wants, rather than the agent's report of
            // having run something.
            tests.extend(parse_test_summaries(
                &outcome.output_excerpt,
                TestSource::Command {
                    provider: outcome.provider.to_string(),
                },
            ));
        }
        if let Some(why) = note {
            evidence_map.insert("observation_note".to_owned(), why);
        }
        if self.simulation {
            evidence_map.insert(
                "simulated".to_owned(),
                "synthetic diff and test results; the repository was not touched".to_owned(),
            );
        }

        Observation {
            run_id: state.header.id,
            status: state.status,
            job: summarize_job(job, counters.elapsed_secs, JOB_PROMPT_CAP),
            active: active_summary,
            history,
            git,
            untracked,
            instructions,
            tests,
            events: event_summaries
                .into_iter()
                .rev()
                .take(self.caps.events_history)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect(),
            prior: state
                .last_directive
                .as_ref()
                .map(|d| aftman_core::observation::PriorDecision {
                    directive: d.name().to_owned(),
                    reason: d.reason().unwrap_or_default().to_owned(),
                }),
            counters,
            evidence: evidence_map,
            output: Self::output_excerpt(active, self.caps.head_chars, self.caps.tail_chars),
            activity: Self::activity_excerpt(active),
        }
    }

    /// The bounded, decoded output of the most recent worker.
    ///
    /// Decoded, because terminal control sequences cost tokens and mean nothing to a
    /// model; see `aftman_core::ansi`. Both ends are capped and the cap is reported, so a
    /// model is never shown a clipped log as though it were the whole thing.
    fn output_excerpt(
        worker: Option<&WorkerRecord>,
        head_chars: usize,
        tail_chars: usize,
    ) -> Option<WorkerOutputExcerpt> {
        let worker = worker?;
        if worker.output.is_empty() {
            // A worker that has printed nothing has nothing to show. `Some("")` would
            // read as "this worker said nothing", which is true but adds no evidence.
            return None;
        }
        let head_raw = worker.output.head(head_chars);
        let tail_raw = worker.output.tail(tail_chars);
        Some(WorkerOutputExcerpt {
            worker_id: worker.id,
            head_truncated: worker.output.head(usize::MAX).len() > head_chars,
            tail_truncated: worker.output.tail(usize::MAX).len() > tail_chars,
            head: strip_control(head_raw),
            tail: strip_control(tail_raw),
            total_bytes: worker.output.total_bytes(),
            dropped_bytes: worker.output.dropped_bytes(),
        })
    }

    /// The bounded view of what the most recent worker's stream said it was doing.
    ///
    /// `None` when the backend has no structured stream, and when it has one but has not
    /// produced anything yet: reporting "0 tool calls" for a worker that has simply not
    /// spoken would read as a worker that did nothing, which is a different claim.
    fn activity_excerpt(worker: Option<&WorkerRecord>) -> Option<WorkerActivityExcerpt> {
        let activity = worker?.activity.as_ref()?;
        if activity.lines == 0 {
            return None;
        }
        Some(WorkerActivityExcerpt::from_activity(activity))
    }

    /// A diff that only exists in simulation mode.
    fn synthetic_git(&self, active: Option<&WorkerRecord>) -> GitEvidence {
        let produced_output = active.is_some_and(|w| !w.output.is_empty());
        if !produced_output {
            return GitEvidence::default();
        }
        GitEvidence {
            status_short: " M src/http.rs\n?? tests/http_retry.rs".to_owned(),
            diff_excerpt: "+++ b/src/http.rs\n+ fn with_retry(req: Request) -> Request {\n+     req.retries(3)\n+ }\n"
                .to_owned(),
            diff_stat: " 1 file changed, 3 insertions(+)".to_owned(),
            changed_files: vec!["src/http.rs".to_owned(), "tests/http_retry.rs".to_owned()],
            head_sha: Some("0000000000000000000000000000000000000000".to_owned()),
            branch: Some("simulated".to_owned()),
            truncated: false,
        }
    }

    /// Test results derived from the simulated worker's own output.
    fn synthetic_tests(&self, active: Option<&WorkerRecord>) -> Vec<TestResult> {
        let Some(worker) = active else {
            return Vec::new();
        };
        let tail = worker.output.tail(self.caps.tail_chars).to_owned();
        let mut parsed = parse_test_summaries(&tail, TestSource::WorkerOutput);
        if parsed.is_empty() && worker.status == WorkerStatus::Completed {
            // The simulated worker's script may not have reached its summary line
            // yet; a completed simulation is a passing one by construction.
            parsed.push(TestResult {
                source: TestSource::WorkerOutput,
                passed: 3,
                failed: 0,
                errored: 0,
                skipped: 0,
                status: TestStatus::Passed,
                raw: "simulated: 3 passed".to_owned(),
            });
        }
        parsed
    }
}

/// Elapsed supervised time for a run: wall time since creation less any time spent
/// waiting for a human, because this is the figure policy compares to
/// `overall_timeout`.
fn elapsed_secs(state: &RunState, now: OffsetDateTime) -> u64 {
    state.supervised_elapsed(now).as_secs()
}

/// The hard limit on assembling the repository part of one observation.
pub const OBSERVATION_TIMEOUT: Duration = Duration::from_secs(8);

/// Most bytes of rendered observation text (the diff and the `--stat`) that are kept, so a
/// repository with a very large change set cannot make the observation unbounded.
pub const MAX_GIT_OUTPUT_BYTES: usize = 256 * 1024;

/// Repository evidence for one observation.
struct RepoEvidence {
    git: GitEvidence,
    untracked: Vec<UntrackedExcerpt>,
    instructions: Option<RepoInstructions>,
    /// Set when the repository could not be observed (or the observation timed out), in
    /// which case `git` and `untracked` are empty.
    note: Option<String>,
}

/// Collect everything repository-shaped, under one deadline.
///
/// The whole read is in-process ([`crate::repo_observe`]) and runs on a blocking thread, so
/// no blocking I/O occupies an async worker and no program the repository names is ever
/// executed. On timeout the observer's interrupt flag is set, which makes gix give up at its
/// next checkpoint rather than leaving a thread hashing a huge file for minutes.
async fn collect_repo(repo: &Path, caps: ObsCaps, limit: Duration) -> Result<RepoEvidence, String> {
    let deadline = tokio::time::Instant::now() + limit;
    let interrupt = Arc::new(AtomicBool::new(false));
    let repo_path = repo.to_path_buf();
    let observer = {
        let interrupt = interrupt.clone();
        tokio::task::spawn_blocking(move || {
            crate::repo_observe::observe(&repo_path, caps, &interrupt)
        })
    };
    match tokio::time::timeout_at(deadline, observer).await {
        Ok(Ok(observed)) => Ok(RepoEvidence {
            git: observed.git,
            untracked: observed.untracked,
            instructions: observed.instructions,
            note: observed.note,
        }),
        Ok(Err(join_error)) => Err(format!(
            "the repository observation task failed: {join_error}"
        )),
        Err(_) => {
            // A blocking task cannot be cancelled; the flag makes it stop at its next
            // checkpoint.
            interrupt.store(true, Ordering::Relaxed);
            Err(format!(
                "repository observation exceeded {}s; git, untracked-file and AGENTS.md evidence \
                 were omitted for this cycle",
                limit.as_secs()
            ))
        }
    }
}

/// Parse test summaries out of a worker's own captured output.
///
/// This is the "claim" path: the text is whatever the agent chose to print. A
/// command evidence result is tagged differently, and the checks are instructed to
/// weight the two differently.
#[must_use]
pub fn parse_worker_tests(log: &BoundedLog) -> Vec<TestResult> {
    let mut text = String::new();
    text.push_str(log.head(8_000));
    text.push('\n');
    text.push_str(log.tail(8_000));
    parse_test_summaries(&text, TestSource::WorkerOutput)
}

/// Parse pytest-style summary lines.
///
/// Handles `passed`-first and failure-first orderings, the `N error`/`N errors`
/// forms, skipped counts, optional `=== … ===` wrappers, and a trailing `in Ns`.
/// Lines without a recognized count are not summaries — `2 warnings in 0.12s` is
/// explicitly *not* a test result, which is a real false positive to avoid.
#[must_use]
pub fn parse_test_summaries(text: &str, source: TestSource) -> Vec<TestResult> {
    let mut results = Vec::new();
    for raw in text.lines() {
        let line = raw.trim().trim_matches('=').trim();
        if line.is_empty() {
            continue;
        }
        let mut passed = 0;
        let mut failed = 0;
        let mut errored = 0;
        let mut skipped = 0;
        let mut recognized = false;

        let tokens: Vec<&str> = line.split_whitespace().collect();
        for pair in tokens.windows(2) {
            let Ok(count) = pair[0].parse::<u32>() else {
                continue;
            };
            let label = pair[1].trim_end_matches(',');
            match label {
                "passed" => {
                    passed = count;
                    recognized = true;
                }
                "failed" | "failure" | "failures" => {
                    failed = count;
                    recognized = true;
                }
                "error" | "errors" => {
                    errored = count;
                    recognized = true;
                }
                "skipped" | "xfailed" | "xpassed" => {
                    skipped += count;
                    recognized = true;
                }
                _ => {}
            }
        }
        if !recognized {
            continue;
        }

        let status = match (failed > 0, errored > 0) {
            (true, true) => TestStatus::Mixed,
            (true, false) => TestStatus::Failed,
            (false, true) => TestStatus::Errored,
            (false, false) if passed > 0 => TestStatus::Passed,
            (false, false) => TestStatus::Unknown,
        };

        results.push(TestResult {
            source: source.clone(),
            passed,
            failed,
            errored,
            skipped,
            status,
            raw: raw.trim().to_owned(),
        });
    }
    results
}

#[cfg(test)]
mod tests {
    use super::parse_test_summaries;
    use aftman_core::observation::{TestSource, TestStatus};

    fn parse(text: &str) -> Vec<aftman_core::observation::TestResult> {
        parse_test_summaries(text, TestSource::WorkerOutput)
    }

    #[test]
    fn a_passing_summary_is_recognized() {
        let results = parse("3 passed in 0.12s");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].passed, 3);
        assert_eq!(results[0].status, TestStatus::Passed);
    }

    #[test]
    fn failure_first_and_passed_first_both_parse() {
        let a = parse("1 failed, 3 passed in 0.4s");
        assert_eq!(a.len(), 1);
        assert_eq!((a[0].failed, a[0].passed), (1, 3));
        assert_eq!(a[0].status, TestStatus::Failed);

        let b = parse("3 passed, 1 failed");
        assert_eq!((b[0].passed, b[0].failed), (3, 1));
        assert_eq!(b[0].status, TestStatus::Failed);
    }

    #[test]
    fn error_forms_are_handled() {
        for text in ["2 errors in 1.0s", "2 error in 1.0s"] {
            let results = parse(text);
            assert_eq!(results[0].errored, 2, "{text}");
            assert_eq!(results[0].status, TestStatus::Errored, "{text}");
        }
    }

    #[test]
    fn decorated_summary_lines_parse() {
        let results = parse("=== 4 passed, 1 skipped in 0.88s ===");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].passed, 4);
        assert_eq!(results[0].skipped, 1);
        assert_eq!(results[0].status, TestStatus::Passed);
    }

    #[test]
    fn a_warning_line_is_not_a_test_summary() {
        // The documented false positive: this must not read as a test result.
        assert!(parse("2 warnings in 0.12s").is_empty());
    }

    #[test]
    fn unrelated_output_is_not_a_summary() {
        for text in [
            "editing src/http.rs",
            "3 files changed",
            "",
            "running the test suite",
        ] {
            assert!(parse(text).is_empty(), "{text:?} must not parse");
        }
    }

    #[test]
    fn multiple_summaries_are_all_reported() {
        let results = parse("3 passed in 0.1s\nnow running integration\n1 failed, 2 passed");
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].status, TestStatus::Passed);
        assert_eq!(results[1].status, TestStatus::Failed);
    }
}

/// The worker-output excerpt: what a model is shown of what the agent printed.
#[cfg(test)]
mod output_excerpt_tests {
    use super::{ObsCaps, ObservationBuilder};
    use aftman_core::activity::{StreamActivity, StreamUsage, ToolCall};
    use aftman_core::ids::WorkerId;
    use aftman_core::job::{TaskKind, WorkerKind};
    use aftman_core::log::{BoundedLog, Stream};
    use aftman_core::worker::WorkerRecord;

    type TestResult = Result<(), String>;

    fn worker(log: BoundedLog) -> WorkerRecord {
        WorkerRecord::new(
            WorkerId::new(),
            WorkerKind::Pi,
            TaskKind::Code,
            "mission",
            log,
            true,
        )
    }

    #[test]
    fn a_worker_that_printed_nothing_has_no_excerpt() {
        // `Some("")` would read as "this worker said nothing", which is true and adds no
        // evidence; the field's absence says the same thing more cheaply.
        let w = worker(BoundedLog::new(4096, 4096));
        assert!(ObservationBuilder::output_excerpt(Some(&w), 100, 100).is_none());
        assert!(ObservationBuilder::output_excerpt(None, 100, 100).is_none());
    }

    #[test]
    fn a_short_log_appears_in_both_ends_because_it_fits() -> TestResult {
        // `BoundedLog` keeps a head up to its *own* capacity and a tail of the most recent
        // bytes, so a log smaller than the head capacity is wholly present in both.
        let mut log = BoundedLog::new(4096, 4096);
        log.push(Stream::Stdout, "start\n");
        log.push(Stream::Stdout, "end\n");
        let w = worker(log);
        let excerpt = ObservationBuilder::output_excerpt(Some(&w), 100, 100)
            .ok_or("a worker that printed has an excerpt")?;
        assert_eq!(excerpt.head, "start\nend\n");
        assert_eq!(excerpt.tail, "start\nend\n");
        assert!(!excerpt.head_truncated);
        assert!(!excerpt.tail_truncated);
        assert_eq!(
            excerpt.total_bytes, 10,
            "6 bytes of \"start\\n\" plus 4 of \"end\\n\""
        );
        assert_eq!(excerpt.worker_id, w.id);
        Ok(())
    }

    #[test]
    fn terminal_control_sequences_are_stripped_before_a_model_sees_them() -> TestResult {
        // They cost tokens and mean nothing to a model; see `aftman_core::ansi`.
        let mut log = BoundedLog::new(4096, 4096);
        log.push(Stream::Stdout, "\u{1b}[31mred\u{1b}[0m plain\n");
        log.push(Stream::Stdout, "\u{1b}]0;title\u{7}after");
        let w = worker(log);
        let excerpt =
            ObservationBuilder::output_excerpt(Some(&w), 4096, 4096).ok_or("an excerpt")?;
        assert!(excerpt.tail.contains("red plain"), "{:?}", excerpt.tail);
        assert!(excerpt.tail.contains("after"), "{:?}", excerpt.tail);
        assert!(!excerpt.tail.contains('\u{1b}'), "{:?}", excerpt.tail);
        assert!(!excerpt.tail.contains("title"), "{:?}", excerpt.tail);
        Ok(())
    }

    #[test]
    fn a_long_log_is_capped_at_both_ends_and_says_so() -> TestResult {
        let mut log = BoundedLog::new(2048, 2048);
        log.push(Stream::Stdout, &"h".repeat(600));
        log.push(Stream::Stdout, &"t".repeat(600));
        let w = worker(log);
        let excerpt = ObservationBuilder::output_excerpt(Some(&w), 100, 200).ok_or("an excerpt")?;
        assert!(
            excerpt.head.len() <= 100,
            "head is {} bytes",
            excerpt.head.len()
        );
        assert!(
            excerpt.tail.len() <= 200,
            "tail is {} bytes",
            excerpt.tail.len()
        );
        assert!(excerpt.head_truncated, "the cap must be reported");
        assert!(excerpt.tail_truncated);
        assert!(
            excerpt.head.starts_with('h'),
            "the head keeps the beginning"
        );
        assert!(excerpt.tail.ends_with('t'), "the tail keeps the end");
        Ok(())
    }

    #[test]
    fn a_lossy_log_reports_what_it_dropped() -> TestResult {
        // A model must not be shown a clipped log as though it were the whole thing.
        let mut log = BoundedLog::new(64, 64);
        for _ in 0..100 {
            log.push(Stream::Stdout, "a chatty worker says hello\n");
        }
        let w = worker(log);
        let excerpt = ObservationBuilder::output_excerpt(Some(&w), 32, 32).ok_or("an excerpt")?;
        assert!(excerpt.dropped_bytes > 0, "{excerpt:?}");
        assert!(excerpt.total_bytes > excerpt.dropped_bytes);
        // Both ends are capped below the log's own capacities, so both report truncation.
        assert!(excerpt.head_truncated, "{excerpt:?}");
        assert!(excerpt.tail_truncated, "{excerpt:?}");
        Ok(())
    }

    #[test]
    fn the_default_head_cap_is_smaller_than_the_tail_cap() {
        // A worker that has gone wrong has usually gone wrong most recently, so the tail
        // is where the evidence is.
        let caps = ObsCaps::default();
        assert!(caps.head_chars > 0);
        assert!(
            caps.head_chars < caps.tail_chars,
            "head {} tail {}",
            caps.head_chars,
            caps.tail_chars
        );
    }

    #[test]
    fn the_activity_excerpt_is_absent_when_there_is_nothing_to_say() -> TestResult {
        // A generic backend has no structured stream, and a structured one that has not
        // spoken yet is indistinguishable from it. Both must read as absent rather than as
        // "0 tool calls", which is a claim about the agent rather than about the backend.
        let quiet = worker(BoundedLog::new(64, 64));
        assert!(ObservationBuilder::activity_excerpt(Some(&quiet)).is_none());
        assert!(ObservationBuilder::activity_excerpt(None).is_none());
        Ok(())
    }

    #[test]
    fn the_activity_excerpt_projects_the_decoded_summary() -> TestResult {
        let mut w = worker(BoundedLog::new(64, 64));
        w.activity = Some(StreamActivity {
            assistant_text: "done".into(),
            lines: 36,
            tool_calls: vec![ToolCall {
                name: "read".into(),
                failed: false,
            }],
            tool_call_count: 1,
            usage: Some(StreamUsage {
                input_tokens: Some(137),
                output_tokens: Some(2),
                cost_usd: Some(0.000_074_358),
            }),
            terminal_status: Some("success".into()),
            aborted: Some(false),
            ..StreamActivity::default()
        });
        let excerpt = ObservationBuilder::activity_excerpt(Some(&w))
            .ok_or("a worker that decoded a stream has an excerpt")?;
        assert_eq!(excerpt.tool_calls, 1);
        assert_eq!(excerpt.tool_names, vec!["read".to_owned()]);
        assert_eq!(excerpt.failed_tools, 0);
        assert_eq!(excerpt.input_tokens, Some(137));
        assert_eq!(excerpt.cost_usd, Some(0.000_074_358));
        assert_eq!(excerpt.terminal_status.as_deref(), Some("success"));
        assert!(!excerpt.calls_truncated);
        Ok(())
    }
}
