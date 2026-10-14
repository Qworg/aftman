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

/// The summary of one agent stream.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StreamActivity {
    /// Assistant prose, concatenated across deltas and capped.
    pub assistant_text: String,
    pub assistant_text_truncated: bool,
    pub tool_calls: Vec<ToolCall>,
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
    #[must_use]
    pub fn tool_calls(&self) -> usize {
        self.tool_calls.len()
    }

    #[must_use]
    pub fn failed_tools(&self) -> usize {
        self.tool_calls.iter().filter(|call| call.failed).count()
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
    use super::{StreamActivity, StreamUsage, ToolCall};

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
}
