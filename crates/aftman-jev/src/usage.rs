//! Usage accounting.
//!
//! One JSONL line per request, appended to `jev-usage.jsonl`. Two decisions are worth
//! stating because they are not obvious:
//!
//! - **It is written before the answer is validated.** A malformed response still cost
//!   tokens, and a bill that only counts successful calls is a bill that lies.
//! - **The reported model is stored alongside the requested one.** Aliases resolve, so
//!   a stored score is only attributable if the model that actually served it is
//!   recorded.
//!
//! See `docs/03-jev.md` §7.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use aftman_core::ids::{AssessmentSeq, RunId};
use aftman_core::model::{Purpose, Usage};

/// One recorded request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UsageEntry {
    #[serde(with = "aftman_core::timefmt")]
    pub at: OffsetDateTime,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<RunId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assessment_seq: Option<AssessmentSeq>,
    pub purpose: Purpose,
    /// The model that actually answered, as reported.
    pub model: String,
    /// The model that was asked for, which may be an alias.
    pub requested_model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    pub latency_ms: u64,
    /// Transport attempts including retries.
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// Whether the request budget had to reduce the observation.
    #[serde(default)]
    pub compacted: bool,
    /// What the budget did, so "why did Jev not see the test output?" is answerable.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub measurements: BTreeMap<String, usize>,
    /// Set when the call failed. A failed call still consumed whatever it consumed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl UsageEntry {
    #[must_use]
    pub fn new(
        at: OffsetDateTime,
        purpose: Purpose,
        requested_model: impl Into<String>,
        reported_model: impl Into<String>,
        usage: Usage,
    ) -> Self {
        Self {
            at,
            run_id: None,
            assessment_seq: None,
            purpose,
            model: reported_model.into(),
            requested_model: requested_model.into(),
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
            latency_ms: 0,
            attempts: 1,
            request_id: None,
            compacted: false,
            measurements: BTreeMap::new(),
            error: None,
        }
    }

    #[must_use]
    pub fn with_context(mut self, run_id: Option<RunId>, seq: Option<AssessmentSeq>) -> Self {
        self.run_id = run_id;
        self.assessment_seq = seq;
        self
    }

    #[must_use]
    pub const fn with_latency(mut self, latency_ms: u64) -> Self {
        self.latency_ms = latency_ms;
        self
    }

    #[must_use]
    pub const fn with_attempts(mut self, attempts: u32) -> Self {
        self.attempts = attempts;
        self
    }

    #[must_use]
    pub fn with_measurements(
        mut self,
        compacted: bool,
        measurements: BTreeMap<String, usize>,
    ) -> Self {
        self.compacted = compacted;
        self.measurements = measurements;
        self
    }

    #[must_use]
    pub fn with_request_id(mut self, request_id: Option<String>) -> Self {
        self.request_id = request_id;
        self
    }

    #[must_use]
    pub fn with_error(mut self, error: impl Into<String>) -> Self {
        self.error = Some(error.into());
        self
    }
}

/// Aggregated usage.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UsageTotals {
    pub requests: u64,
    pub failed_requests: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    /// Requests where the observation had to be compacted.
    pub compacted_requests: u64,
    /// Requests that needed more than one transport attempt.
    pub retried_requests: u64,
}

impl UsageTotals {
    #[must_use]
    pub const fn total_tokens(&self) -> u64 {
        self.input_tokens + self.output_tokens
    }
}

/// Appends usage records to a file.
///
/// A recorder with no path is a no-op, which is what tests and `--no-daemon` runs use
/// so they never write outside their temp directory.
#[derive(Debug, Clone, Default)]
pub struct UsageRecorder {
    path: Option<PathBuf>,
}

impl UsageRecorder {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: Some(path.into()),
        }
    }

    /// The default location, `<state-dir>/jev-usage.jsonl`.
    #[must_use]
    pub fn in_state_dir(dir: &Path) -> Self {
        Self::new(dir.join("jev-usage.jsonl"))
    }

    #[must_use]
    pub fn disabled() -> Self {
        Self { path: None }
    }

    #[must_use]
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// Append one record.
    ///
    /// Returns an error rather than swallowing it, so a caller can decide whether a
    /// full disk is worth failing a run over. The client logs and continues: an
    /// unbillable request is not a reason to stop supervising.
    pub fn record(&self, entry: &UsageEntry) -> std::io::Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            use std::os::unix::fs::DirBuilderExt as _;
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(parent)?;
        }
        let line = serde_json::to_string(entry).map_err(std::io::Error::other)?;
        // Owner-only, like everything else in the state directory.
        let mut file = {
            use std::os::unix::fs::OpenOptionsExt as _;
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .mode(0o600)
                .open(path)?
        };
        file.write_all(line.as_bytes())?;
        file.write_all(b"\n")?;
        Ok(())
    }

    /// Read every record back.
    ///
    /// A torn final line is skipped rather than failing the read: an interrupted
    /// append must not make the whole history unreadable.
    pub fn read(&self) -> std::io::Result<Vec<UsageEntry>> {
        let Some(path) = &self.path else {
            return Ok(Vec::new());
        };
        let Ok(text) = std::fs::read_to_string(path) else {
            return Ok(Vec::new());
        };
        let mut entries = Vec::new();
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<UsageEntry>(line) {
                Ok(entry) => entries.push(entry),
                Err(error) => {
                    tracing::warn!(%error, "skipping an unreadable usage line");
                }
            }
        }
        Ok(entries)
    }

    /// Aggregate.
    pub fn totals(&self) -> std::io::Result<UsageTotals> {
        Ok(totals(&self.read()?))
    }
}

/// Sum a set of entries.
#[must_use]
pub fn totals(entries: &[UsageEntry]) -> UsageTotals {
    let mut totals = UsageTotals::default();
    for entry in entries {
        totals.requests += 1;
        if entry.error.is_some() {
            totals.failed_requests += 1;
        }
        totals.input_tokens += entry.input_tokens.unwrap_or(0);
        totals.output_tokens += entry.output_tokens.unwrap_or(0);
        if entry.compacted {
            totals.compacted_requests += 1;
        }
        if entry.attempts > 1 {
            totals.retried_requests += 1;
        }
    }
    totals
}

#[cfg(test)]
mod tests {
    use super::{UsageEntry, UsageRecorder, totals};
    use aftman_core::ids::{AssessmentSeq, RunId};
    use aftman_core::model::{Purpose, Usage};
    use time::macros::datetime;

    fn entry(model: &str, input: Option<u64>, output: Option<u64>) -> UsageEntry {
        UsageEntry::new(
            datetime!(2026-01-01 00:00:00 UTC),
            Purpose::Assessment,
            "jev-latest",
            model,
            Usage {
                input_tokens: input,
                output_tokens: output,
            },
        )
    }

    #[test]
    fn a_record_round_trips_through_jsonl() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let recorder = UsageRecorder::in_state_dir(dir.path());
        let record = entry("jev-2026-01", Some(8123), Some(96))
            .with_context(Some(RunId::new()), Some(AssessmentSeq(7)))
            .with_latency(412)
            .with_attempts(2)
            .with_request_id(Some("req-1".to_owned()));
        recorder.record(&record)?;

        let read = recorder.read()?;
        assert_eq!(read.len(), 1);
        assert_eq!(read[0], record);
        assert_eq!(read[0].requested_model, "jev-latest");
        assert_eq!(read[0].model, "jev-2026-01");
        Ok(())
    }

    #[test]
    fn records_append_rather_than_replace() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let recorder = UsageRecorder::in_state_dir(dir.path());
        for n in 0..3 {
            recorder.record(&entry("m", Some(n), Some(1)))?;
        }
        assert_eq!(recorder.read()?.len(), 3);
        Ok(())
    }

    #[test]
    fn a_disabled_recorder_writes_nothing_and_reads_empty() -> Result<(), Box<dyn std::error::Error>>
    {
        let recorder = UsageRecorder::disabled();
        recorder.record(&entry("m", Some(1), Some(1)))?;
        assert!(recorder.read()?.is_empty());
        assert_eq!(recorder.totals()?.requests, 0);
        assert!(recorder.path().is_none());
        Ok(())
    }

    #[test]
    fn a_torn_final_line_is_skipped_not_fatal() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let recorder = UsageRecorder::in_state_dir(dir.path());
        recorder.record(&entry("m", Some(1), Some(1)))?;
        let path = recorder.path().ok_or("a path")?.to_path_buf();
        let mut text = std::fs::read_to_string(&path)?;
        text.push_str("{\"at\":\"2026-01-01T00:00:00Z\",\"purpo");
        std::fs::write(&path, text)?;

        assert_eq!(recorder.read()?.len(), 1, "the complete record survives");
        Ok(())
    }

    #[test]
    fn an_absent_file_reads_as_empty() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let recorder = UsageRecorder::new(dir.path().join("nothing/here.jsonl"));
        assert!(recorder.read()?.is_empty());
        Ok(())
    }

    #[test]
    fn totals_count_failures_retries_and_compaction() -> Result<(), Box<dyn std::error::Error>> {
        let entries = vec![
            entry("m", Some(100), Some(10)),
            entry("m", Some(50), Some(5))
                .with_attempts(3)
                .with_measurements(true, Default::default()),
            entry("m", None, None).with_error("rate limited"),
        ];
        let totals = totals(&entries);
        assert_eq!(totals.requests, 3);
        assert_eq!(totals.failed_requests, 1);
        assert_eq!(totals.input_tokens, 150);
        assert_eq!(totals.output_tokens, 15);
        assert_eq!(totals.total_tokens(), 165);
        assert_eq!(totals.retried_requests, 1);
        assert_eq!(totals.compacted_requests, 1);
        Ok(())
    }

    #[test]
    fn a_failed_call_with_no_token_report_is_still_counted() {
        // A bill that only counts successful calls is a bill that lies.
        let totals = totals(&[entry("m", None, None).with_error("timeout")]);
        assert_eq!(totals.requests, 1);
        assert_eq!(totals.failed_requests, 1);
        assert_eq!(totals.total_tokens(), 0);
    }

    #[test]
    fn absent_optional_fields_are_omitted_from_the_line() -> Result<(), Box<dyn std::error::Error>>
    {
        let record = entry("m", None, None);
        let line = serde_json::to_string(&record)?;
        assert!(!line.contains("run_id"), "{line}");
        assert!(!line.contains("error"), "{line}");
        assert!(!line.contains("request_id"), "{line}");
        assert!(!line.contains("measurements"), "{line}");
        assert!(line.contains("requested_model"), "{line}");
        Ok(())
    }
}
