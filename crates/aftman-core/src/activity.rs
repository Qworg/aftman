//! What a structured agent stream told us.
//!
//! All three real backends emit structured progress by default — `pi --mode json`, `agy
//! --output-format stream-json`, `claude --output-format stream-json` — and the shapes
//! differ enough that guessing is not viable. `pi` and `claude` envelope events under a
//! `type` key; **`agy` uses `event`**. That difference was found by recording real
//! output rather than by reading `--help`, which is why `docs/04-workers.md` §12 insists
//! on it.
//!
//! This type is the summary of such a stream: enough for the observation to judge
//! progress, and for a surface to show what the agent actually did without rendering a
//! wall of JSON.

use serde::{Deserialize, Serialize};

/// One tool invocation the agent made.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCall {
    pub name: String,
    /// Whether the agent's own harness reported the call as failed.
    pub failed: bool,
}

/// Token usage the agent's harness reported.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct StreamUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    /// Reported cost in US dollars, when the backend says so.
    pub cost_usd: Option<f64>,
}

impl StreamUsage {
    #[must_use]
    pub const fn total_tokens(&self) -> Option<u64> {
        match (self.input_tokens, self.output_tokens) {
            (Some(i), Some(o)) => Some(i + o),
            (Some(i), None) => Some(i),
            (None, Some(o)) => Some(o),
            (None, None) => None,
        }
    }
}

/// The most tool calls retained in [`StreamActivity::tool_calls`].
///
/// The counts are what show a loop, and the list is what names the tools; neither needs
/// every call from a long run. Keeping them all would let a chatty agent grow a persisted
/// record without bound, which is the same reason the assistant prose is capped.
pub const MAX_TOOL_CALLS: usize = 256;

/// The summary of one agent stream.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StreamActivity {
    /// Assistant prose, concatenated across deltas and capped.
    pub assistant_text: String,
    pub assistant_text_truncated: bool,
    /// The tool calls kept, in order, at most [`MAX_TOOL_CALLS`] of them. The counts below
    /// are authoritative for how many there were; this is the sample that names them.
    pub tool_calls: Vec<ToolCall>,
    /// Every tool call the stream contained, including ones not retained.
    pub tool_call_count: usize,
    /// Failures across every call seen, including ones not retained.
    pub failed_tool_count: usize,
    pub usage: Option<StreamUsage>,
    /// `pi` reports whether the run was aborted rather than completing.
    pub aborted: Option<bool>,
    /// The backend's own terminal status, where it reports one.
    pub terminal_status: Option<String>,
    /// Lines that did not parse. A stream that is *mostly* unparsed means the backend's
    /// format moved, which is worth surfacing rather than silently showing raw JSON.
    pub unparsed_lines: usize,
    /// Total lines consumed.
    pub lines: usize,
}

impl StreamActivity {
    /// Record a tool call, retaining it only while there is room.
    ///
    /// The counters keep counting past the cap: a run with more calls than are worth
    /// keeping is precisely the run whose call count matters.
    pub fn note_tool_call(&mut self, call: ToolCall) {
        self.tool_call_count += 1;
        if call.failed {
            self.failed_tool_count += 1;
        }
        if self.tool_calls.len() < MAX_TOOL_CALLS {
            self.tool_calls.push(call);
        }
    }

    /// Record that the most recent call with this name failed.
    ///
    /// Counted as a failure *report*, which is what a backend actually emits. A call whose
    /// record fell outside the retained list is still counted: a long, failing run is
    /// exactly the one whose failure count matters, and dropping it would hide it.
    pub fn note_tool_failure(&mut self, name: &str) {
        self.failed_tool_count += 1;
        if let Some(call) = self
            .tool_calls
            .iter_mut()
            .rev()
            .find(|call| call.name == name && !call.failed)
        {
            call.failed = true;
        }
    }

    /// Record a failure for the most recent call, for backends whose result event does not
    /// name the tool that produced it.
    pub fn note_last_tool_failure(&mut self) {
        self.failed_tool_count += 1;
        if let Some(call) = self.tool_calls.iter_mut().rev().find(|call| !call.failed) {
            call.failed = true;
        }
    }

    /// How many tool calls the stream contained, including ones not retained.
    ///
    /// The stored count wins when the decoder filled it; otherwise the retained list is
    /// the answer, so a summary assembled by hand — in a test, or from a fixture — can set
    /// only the list and still count correctly.
    #[must_use]
    pub fn tool_calls(&self) -> usize {
        self.tool_call_count.max(self.tool_calls.len())
    }

    /// How many calls the agent's own harness reported as failed.
    #[must_use]
    pub fn failed_tools(&self) -> usize {
        self.failed_tool_count
            .max(self.tool_calls.iter().filter(|call| call.failed).count())
    }

    /// Whether the retained list holds fewer calls than the stream made.
    #[must_use]
    pub fn calls_truncated(&self) -> bool {
        self.tool_calls() > self.tool_calls.len()
    }

    /// The distinct tool names used, in first-use order.
    #[must_use]
    pub fn tool_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = Vec::new();
        for call in &self.tool_calls {
            if !names.contains(&call.name.as_str()) {
                names.push(&call.name);
            }
        }
        names
    }

    /// Whether the stream said nothing at all: no prose, no tool calls, and no tokens.
    ///
    /// Distinct from "a quiet worker inside a working stream". A `pi` run that picked a model
    /// returning empty content produced a well-formed stream with `content: []` and
    /// `"totalTokens": 0` — the worker ran, exited 0, and did nothing, and every other signal
    /// said success. This is the shape that hid it.
    #[must_use]
    pub fn produced_nothing(&self) -> bool {
        self.assistant_text.trim().is_empty()
            && self.tool_calls() == 0
            && self
                .usage
                .and_then(|usage| usage.total_tokens())
                .unwrap_or(0)
                == 0
    }

    /// Whether the stream looked like the format it was expected to be.
    ///
    /// A backend whose shape changed produces a stream that parses as nothing; a caller
    /// can use this to say so rather than presenting an empty observation as a quiet
    /// worker.
    #[must_use]
    pub fn looks_structured(&self) -> bool {
        self.lines > 0 && self.unparsed_lines * 2 < self.lines
    }

    /// One line for a log or a dashboard.
    #[must_use]
    pub fn summary(&self) -> String {
        let tokens = self
            .usage
            .and_then(|usage| usage.total_tokens())
            .map_or_else(|| "tokens n/a".to_owned(), |t| format!("{t} tokens"));
        format!(
            "{} tool call(s){} · {tokens}",
            self.tool_calls(),
            if self.failed_tools() > 0 {
                format!(", {} failed", self.failed_tools())
            } else {
                String::new()
            }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_TOOL_CALLS, StreamActivity, StreamUsage, ToolCall};

    #[test]
    fn tool_calls_are_counted_and_deduplicated_by_name() {
        let activity = StreamActivity {
            tool_calls: vec![
                ToolCall {
                    name: "read".into(),
                    failed: false,
                },
                ToolCall {
                    name: "bash".into(),
                    failed: true,
                },
                ToolCall {
                    name: "read".into(),
                    failed: false,
                },
            ],
            ..StreamActivity::default()
        };
        assert_eq!(activity.tool_calls(), 3);
        assert_eq!(activity.failed_tools(), 1);
        assert_eq!(activity.tool_names(), vec!["read", "bash"]);
        assert!(activity.summary().contains("3 tool call(s)"));
        assert!(activity.summary().contains("1 failed"));
    }

    #[test]
    fn a_stream_with_no_tools_says_so_plainly() {
        let activity = StreamActivity {
            usage: Some(StreamUsage {
                input_tokens: Some(100),
                output_tokens: Some(5),
                cost_usd: None,
            }),
            ..StreamActivity::default()
        };
        assert_eq!(activity.summary(), "0 tool call(s) · 105 tokens");
    }

    #[test]
    fn usage_totals_only_what_was_reported() {
        assert_eq!(StreamUsage::default().total_tokens(), None);
        assert_eq!(
            StreamUsage {
                input_tokens: Some(10),
                output_tokens: None,
                cost_usd: None
            }
            .total_tokens(),
            Some(10)
        );
        assert_eq!(
            StreamUsage {
                input_tokens: Some(10),
                output_tokens: Some(4),
                cost_usd: Some(0.002)
            }
            .total_tokens(),
            Some(14)
        );
    }

    #[test]
    fn structure_is_judged_by_how_much_parsed() {
        let mostly_parsed = StreamActivity {
            lines: 10,
            unparsed_lines: 1,
            ..StreamActivity::default()
        };
        assert!(mostly_parsed.looks_structured());

        // Half of it unrecognized means the format moved, not that the agent was quiet.
        let half_parsed = StreamActivity {
            lines: 10,
            unparsed_lines: 6,
            ..StreamActivity::default()
        };
        assert!(!half_parsed.looks_structured());

        assert!(
            !StreamActivity::default().looks_structured(),
            "nothing at all"
        );
    }

    #[test]
    fn the_retained_list_is_capped_but_the_count_is_not() {
        let mut activity = StreamActivity::default();
        for i in 0..(MAX_TOOL_CALLS + 10) {
            activity.note_tool_call(ToolCall {
                name: if i % 2 == 0 { "read" } else { "bash" }.to_owned(),
                failed: false,
            });
        }
        assert_eq!(
            activity.tool_calls.len(),
            MAX_TOOL_CALLS,
            "the list is bounded"
        );
        assert_eq!(
            activity.tool_calls(),
            MAX_TOOL_CALLS + 10,
            "the count is not"
        );
        assert!(activity.calls_truncated());
        // The sample still names what the agent used, which is the list's whole purpose.
        assert_eq!(activity.tool_names(), vec!["read", "bash"]);
    }

    #[test]
    fn a_failure_on_a_dropped_call_still_counts() {
        // The case a naive cap would hide: a long run that looks fine until it fails. The
        // retained list cannot show it, so the count has to.
        let mut activity = StreamActivity::default();
        for _ in 0..(MAX_TOOL_CALLS + 5) {
            activity.note_tool_call(ToolCall {
                name: "read".into(),
                failed: false,
            });
        }
        activity.note_tool_failure("read");
        assert_eq!(activity.failed_tools(), 1, "the failure is still counted");
        assert_eq!(activity.tool_calls(), MAX_TOOL_CALLS + 5);
    }

    #[test]
    fn a_failure_marks_the_most_recent_call_of_that_name() {
        let mut activity = StreamActivity::default();
        activity.note_tool_call(ToolCall {
            name: "read".into(),
            failed: false,
        });
        activity.note_tool_call(ToolCall {
            name: "bash".into(),
            failed: false,
        });
        activity.note_tool_failure("bash");
        assert_eq!(activity.failed_tools(), 1);
        assert!(activity.tool_calls[1].failed, "bash is the one that failed");
        assert!(!activity.tool_calls[0].failed);

        // A later report for another name marks that name's most recent call, not the
        // same call twice. There is no call id in these streams, so the most recent match
        // is the best the decoder can do; the earlier `read` is left alone.
        activity.note_tool_call(ToolCall {
            name: "read".into(),
            failed: false,
        });
        activity.note_tool_failure("read");
        assert_eq!(activity.failed_tools(), 2);
        assert!(activity.tool_calls[1].failed, "bash");
        assert!(activity.tool_calls[2].failed, "the most recent read");
        assert!(
            !activity.tool_calls[0].failed,
            "the earlier read is untouched"
        );
    }

    #[test]
    fn a_run_that_said_nothing_is_recognised() {
        // A well-formed stream carrying no prose, no tools and no tokens: the worker ran and
        // did nothing, which every other signal reports as success.
        assert!(StreamActivity::default().produced_nothing());
        let silent = StreamActivity {
            lines: 9,
            usage: Some(StreamUsage {
                input_tokens: Some(0),
                output_tokens: Some(0),
                cost_usd: None,
            }),
            ..StreamActivity::default()
        };
        assert!(silent.produced_nothing());

        // Any one of the three means it did something.
        let spoke = StreamActivity {
            assistant_text: "ok".into(),
            ..StreamActivity::default()
        };
        assert!(!spoke.produced_nothing());
        let worked = StreamActivity {
            tool_calls: vec![ToolCall {
                name: "read".into(),
                failed: false,
            }],
            ..StreamActivity::default()
        };
        assert!(!worked.produced_nothing());
        let spent = StreamActivity {
            usage: Some(StreamUsage {
                input_tokens: Some(12),
                output_tokens: Some(1),
                cost_usd: None,
            }),
            ..StreamActivity::default()
        };
        assert!(!spent.produced_nothing());
    }

    #[test]
    fn a_summary_assembled_by_hand_counts_from_its_list() {
        // Fixtures and tests set the list without the counters. Reporting zero for those
        // would be wrong, and silently so.
        let activity = StreamActivity {
            tool_calls: vec![
                ToolCall {
                    name: "read".into(),
                    failed: false,
                },
                ToolCall {
                    name: "bash".into(),
                    failed: true,
                },
            ],
            ..StreamActivity::default()
        };
        assert_eq!(activity.tool_calls(), 2);
        assert_eq!(activity.failed_tools(), 1);
        assert!(!activity.calls_truncated());
    }
}
