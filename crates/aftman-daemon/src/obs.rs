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
use std::process::Command;

use time::OffsetDateTime;

use aftman_core::job::{Job, WorkerKind, WorkerStatus};
use aftman_core::log::BoundedLog;
use aftman_core::observation::{
    Counters, EventSummary, GitEvidence, Observation, RepoInstructions, TestResult, TestSource,
    TestStatus, UntrackedExcerpt, cap_text, summarize_job, summarize_worker,
};
use aftman_core::run::RunState;
use aftman_core::worker::WorkerRecord;

/// How much of the job prompt is shown to a model.
pub const JOB_PROMPT_CAP: usize = 4_000;

/// Caps applied to the assembled observation.
#[derive(Debug, Clone, Copy)]
pub struct ObsCaps {
    pub diff_chars: usize,
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
}

impl ObservationBuilder {
    #[must_use]
    pub fn new(simulation: bool) -> Self {
        Self {
            caps: ObsCaps::default(),
            simulation,
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
    #[must_use]
    pub fn build(
        &self,
        state: &RunState,
        job: &Job,
        active: Option<&WorkerRecord>,
        event_summaries: Vec<EventSummary>,
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

        let (git, untracked, instructions, tests) = if self.simulation {
            (
                self.synthetic_git(active),
                Vec::new(),
                None,
                self.synthetic_tests(active),
            )
        } else {
            let git = git_evidence(&job.repo, self.caps);
            let untracked = untracked_excerpts(&job.repo, self.caps);
            let instructions = read_instructions(&job.repo, self.caps.instruction_chars);
            let tests = active.map_or_else(Vec::new, |w| parse_worker_tests(&w.output));
            (git, untracked, instructions, tests)
        };

        let mut evidence = std::collections::BTreeMap::new();
        if self.simulation {
            evidence.insert(
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
            evidence,
        }
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

/// Elapsed wall time for a run, from its header.
fn elapsed_secs(state: &RunState, now: OffsetDateTime) -> u64 {
    (now - state.header.created_at).whole_seconds().max(0) as u64
}

/// Collect git state, degrading to an empty record rather than failing a cycle.
///
/// A slow or huge `git diff` must not be able to stall supervision, so every command
/// has a bounded output budget and any failure is reported as absent evidence
/// rather than as an error.
fn git_evidence(repo: &Path, caps: ObsCaps) -> GitEvidence {
    if !repo.is_dir() {
        return GitEvidence::default();
    }
    // `--untracked-files=all` matters: a collapsed `dir/` entry hides the individual
    // new files a worker just wrote.
    let status_short =
        run_git(repo, &["status", "--short", "--untracked-files=all"]).unwrap_or_default();
    let diff_stat = run_git(repo, &["diff", "--stat"]).unwrap_or_default();
    let full_diff = run_git(repo, &["diff"]).unwrap_or_default();
    let (diff_excerpt, truncated) = cap_text(&full_diff, caps.diff_chars);
    let changed_files = status_short
        .lines()
        .filter_map(|line| line.get(3..).map(str::trim))
        .filter(|path| !path.is_empty())
        .map(ToOwned::to_owned)
        .collect();

    GitEvidence {
        status_short,
        diff_excerpt,
        diff_stat,
        changed_files,
        head_sha: run_git(repo, &["rev-parse", "HEAD"]).map(|s| s.trim().to_owned()),
        branch: run_git(repo, &["branch", "--show-current"]).map(|s| s.trim().to_owned()),
        truncated,
    }
}

/// Run one git command, returning `None` on any failure.
fn run_git(repo: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Bounded excerpts of untracked text files.
///
/// `git diff` never shows untracked files, so a new test or source file written by a
/// worker is invisible to the supervisor without this.
fn untracked_excerpts(repo: &Path, caps: ObsCaps) -> Vec<UntrackedExcerpt> {
    let status = run_git(repo, &["status", "--short", "--untracked-files=all"]).unwrap_or_default();
    let mut budget = caps.untracked_bytes;
    let mut excerpts = Vec::new();

    for line in status.lines() {
        if excerpts.len() >= caps.untracked_files || budget == 0 {
            break;
        }
        if !line.starts_with("??") {
            continue;
        }
        let Some(relative) = line.get(3..).map(str::trim) else {
            continue;
        };
        if relative.is_empty() || !is_safe_to_read(relative) {
            continue;
        }
        let path = repo.join(relative);
        let Ok(bytes) = std::fs::read(&path) else {
            continue;
        };
        // Binary-looking files are skipped: a NUL in the first 4 KiB is the same
        // heuristic `git` uses.
        if bytes.iter().take(4096).any(|b| *b == 0) {
            continue;
        }
        let take = bytes.len().min(budget);
        let truncated = bytes.len() > take;
        // Truncate on a raw byte boundary *before* decoding, so multibyte text can
        // never exceed the byte budget.
        let content = String::from_utf8_lossy(&bytes[..take]).into_owned();
        budget = budget.saturating_sub(take);
        excerpts.push(UntrackedExcerpt {
            path: relative.to_owned(),
            bytes: bytes.len(),
            content,
            truncated,
        });
    }
    excerpts
}

/// Names that must never be read into an observation.
fn is_safe_to_read(relative: &str) -> bool {
    let lower = relative.to_ascii_lowercase();
    let name = lower.rsplit('/').next().unwrap_or(&lower);
    const DENY_SUBSTRINGS: &[&str] = &["secret", "token", "credential"];
    const DENY_SUFFIXES: &[&str] = &[".pem", ".key", ".p12", ".keystore"];
    if lower.starts_with("..") || lower.contains("/../") {
        return false;
    }
    if name == ".env" || name.starts_with(".env.") {
        return false;
    }
    if name == "id_rsa" || name == "id_ed25519" || name == "id_ecdsa" {
        return false;
    }
    if DENY_SUBSTRINGS.iter().any(|s| name.contains(s)) {
        return false;
    }
    !DENY_SUFFIXES.iter().any(|s| name.ends_with(s))
}

/// Read repository-root instructions.
///
/// Transient: the contents are included in a request and never persisted, because
/// repository prose must not land in the run log.
fn read_instructions(repo: &Path, cap_chars: usize) -> Option<RepoInstructions> {
    for file in ["AGENTS.override.md", "AGENTS.md"] {
        let path = repo.join(file);
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let (excerpt, truncated) = cap_text(&text, cap_chars);
        return Some(RepoInstructions {
            file: file.to_owned(),
            excerpt,
            truncated,
        });
    }
    None
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
    use super::{is_safe_to_read, parse_test_summaries};
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

    #[test]
    fn sensitive_paths_are_never_read() {
        for path in [
            ".env",
            ".env.local",
            "config/.env",
            "id_rsa",
            "certs/server.pem",
            "deploy.key",
            "src/secrets.rs",
            "lib/token_store.rs",
            "../outside.txt",
            "a/../../outside.txt",
        ] {
            assert!(!is_safe_to_read(path), "{path} must not be read");
        }
    }

    #[test]
    fn ordinary_paths_are_readable() {
        for path in [
            "src/http.rs",
            "tests/http_retry.rs",
            "README.md",
            "docs/notes.md",
        ] {
            assert!(is_safe_to_read(path), "{path} should be readable");
        }
    }
}
