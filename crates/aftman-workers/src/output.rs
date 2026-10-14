//! Turning a structured agent stream into readable log lines and an activity summary.
//!
//! Every real backend emits structured progress by default, and the observation must not
//! be a wall of JSON: a model judging a worker, and a human watching one, both need the
//! same thing — what the agent *said* and what tools it *ran*.
//!
//! The three formats were read off recorded output, not off `--help`, because guessing
//! is wrong in a way that hides. The decisive detail: `pi` and `claude` envelope events
//! under `type`, and **`agy` uses `event`**. A parser that assumed one key would have
//! silently produced an empty activity summary for `agy` and called it a quiet worker.
//!
//! See `crates/aftman-workers/tests/fixtures/` for the recordings and
//! `docs/04-workers.md` §12 for why they exist.

use aftman_core::activity::{StreamActivity, StreamUsage, ToolCall};
use aftman_core::job::WorkerKind;
use serde_json::Value;

/// How much assistant prose is retained. The log tail is separately bounded; this cap is
/// for the *summary*, which reaches a model.
pub const ASSISTANT_TEXT_CAP: usize = 4_000;

/// Which envelope key a backend puts its event kind under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Envelope {
    Type,
    Event,
    /// No structured envelope: pass lines through unchanged.
    Plain,
}

fn envelope_for(kind: WorkerKind) -> Envelope {
    match kind {
        WorkerKind::Pi | WorkerKind::Claude => Envelope::Type,
        // Verified against agy 1.3.3: `{"event":"step_update","step_update":{…}}`.
        WorkerKind::Agy => Envelope::Event,
        WorkerKind::Generic | WorkerKind::HerdrPane | WorkerKind::Simulated => Envelope::Plain,
    }
}

/// Renders a structured stream into readable lines.
///
/// Feed it bytes as they arrive; it emits complete lines. Partial lines are buffered,
/// because a pipe read can split a JSON object across two chunks and decoding half an
/// object would count it as unparsed.
pub struct StreamDecoder {
    kind: WorkerKind,
    envelope: Envelope,
    buffer: String,
    activity: StreamActivity,
}

impl std::fmt::Debug for StreamDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamDecoder")
            .field("kind", &self.kind)
            .field("envelope", &self.envelope)
            .finish()
    }
}

impl StreamDecoder {
    #[must_use]
    pub fn new(kind: WorkerKind) -> Self {
        Self {
            kind,
            envelope: envelope_for(kind),
            buffer: String::new(),
            activity: StreamActivity::default(),
        }
    }

    /// Feed a chunk. Each complete line is rendered through `emit`.
    pub fn push(&mut self, chunk: &str, emit: &mut impl FnMut(&str)) {
        self.buffer.push_str(chunk);
        // A stream is newline-delimited. Anything after the last newline is a partial
        // line and waits for the next chunk.
        while let Some(index) = self.buffer.find('\n') {
            let line: String = self.buffer.drain(..=index).collect();
            let line = line.trim_end_matches(['\n', '\r']);
            if !line.is_empty() {
                self.consume(line, emit);
            }
        }
    }

    /// Flush a trailing partial line, and return what the stream said.
    pub fn finish(&mut self, emit: &mut impl FnMut(&str)) -> StreamActivity {
        let leftover = std::mem::take(&mut self.buffer);
        let leftover = leftover.trim();
        if !leftover.is_empty() {
            // A backend that exits mid-line still said something.
            self.consume(leftover, emit);
        }
        std::mem::take(&mut self.activity)
    }

    /// Decode one line.
    fn consume(&mut self, line: &str, emit: &mut impl FnMut(&str)) {
        self.activity.lines += 1;
        if self.envelope == Envelope::Plain {
            // An unstructured backend's own output is the readable form already.
            emit(line);
            return;
        }

        let Ok(event) = serde_json::from_str::<Value>(line) else {
            // Not JSON. Keep it: a CLI's diagnostics go to stdout in some versions, and
            // dropping them would lose the only explanation of a failure.
            self.activity.unparsed_lines += 1;
            emit(line);
            return;
        };
        let key = match self.envelope {
            Envelope::Type => "type",
            Envelope::Event => "event",
            Envelope::Plain => unreachable!("handled above"),
        };
        let Some(kind) = event.get(key).and_then(Value::as_str) else {
            self.activity.unparsed_lines += 1;
            emit(line);
            return;
        };

        match self.kind {
            WorkerKind::Pi => self.consume_pi(kind, &event, emit),
            WorkerKind::Agy => self.consume_agy(kind, &event, emit),
            WorkerKind::Claude => self.consume_claude(kind, &event, emit),
            _ => {
                self.activity.unparsed_lines += 1;
                emit(line);
            }
        }
    }

    /// `pi --mode json`: `message_update` deltas, `tool_execution_*`, `agent_settled`.
    fn consume_pi(&mut self, kind: &str, event: &Value, emit: &mut impl FnMut(&str)) {
        match kind {
            "message_update" => {
                // The recorded shape is `assistantMessageEvent: {"type": "text_delta",
                // "delta": "done"}` — the discriminator is `type`, and `delta` is its
                // sibling. It is *not* a nested `text_delta` object. Requiring the nested
                // shape is how a decoder ends up producing empty prose for every `pi` run
                // and calling it a quiet worker, which is why the fixture test exists.
                let nested = event.get("assistantMessageEvent");
                let is_delta = nested.and_then(|n| n.get("type")).and_then(Value::as_str)
                    == Some("text_delta");
                if let Some(delta) = nested.and_then(|n| n.get("delta")).and_then(Value::as_str)
                    && is_delta
                {
                    self.append_assistant(delta, emit);
                }
            }
            "tool_execution_start" => {
                let name = event
                    .get("toolName")
                    .and_then(Value::as_str)
                    .unwrap_or("tool");
                self.activity.tool_calls.push(ToolCall {
                    name: name.to_owned(),
                    failed: false,
                });
                emit(&format!("▸ {name}"));
            }
            "tool_execution_end" => {
                let name = event
                    .get("toolName")
                    .and_then(Value::as_str)
                    .unwrap_or("tool");
                let failed = event
                    .get("isError")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let millis = event.get("durationMs").and_then(Value::as_u64);
                if let Some(call) = self
                    .activity
                    .tool_calls
                    .iter_mut()
                    .rev()
                    .find(|call| call.name == name && !call.failed)
                {
                    call.failed = failed;
                }
                let timing = millis.map_or(String::new(), |ms| format!(" ({ms}ms)"));
                emit(&format!(
                    "{} {name}{timing}",
                    if failed { "✗" } else { "✓" }
                ));
            }
            "message_end" => {
                let message = event.get("message");
                if let Some(usage) = message.and_then(|m| m.get("usage")) {
                    self.absorb_pi_usage(usage);
                }
            }
            "agent_settled" => {
                self.activity.aborted = event.get("aborted").and_then(Value::as_bool);
            }
            _ => {}
        }
    }

    fn absorb_pi_usage(&mut self, usage: &Value) {
        // pi reports per-message usage with a `totalTokens` aggregate; the last message's
        // figures are the run's, so they overwrite rather than accumulate.
        self.activity.usage = Some(StreamUsage {
            input_tokens: usage.get("input").and_then(Value::as_u64),
            output_tokens: usage.get("output").and_then(Value::as_u64),
            cost_usd: usage
                .get("cost")
                .and_then(|cost| cost.get("total"))
                .and_then(Value::as_f64),
        });
    }

    /// `agy --output-format stream-json`: `step_update` with a state, plus `result`.
    fn consume_agy(&mut self, kind: &str, event: &Value, emit: &mut impl FnMut(&str)) {
        match kind {
            "step_update" => {
                let Some(step) = event.get("step_update") else {
                    return;
                };
                let state = step.get("state").and_then(Value::as_str).unwrap_or("");
                let step_type = step.get("step_type").and_then(Value::as_str).unwrap_or("");
                match step_type {
                    "tool" => {
                        let name = step
                            .get("tool_name")
                            .and_then(Value::as_str)
                            .unwrap_or("tool");
                        match state {
                            "ACTIVE" => {
                                self.activity.tool_calls.push(ToolCall {
                                    name: name.to_owned(),
                                    failed: false,
                                });
                                emit(&format!("▸ {name}"));
                            }
                            "DONE" => {
                                let seconds = step
                                    .get("duration_seconds")
                                    .and_then(Value::as_f64)
                                    .map_or(String::new(), |s| format!(" ({s:.1}s)"));
                                emit(&format!("✓ {name}{seconds}"));
                            }
                            _ => {}
                        }
                    }
                    _ => {
                        // `ACTIVE` carries a delta; `DONE` repeats the whole text, so
                        // only the delta is appended or the assistant would be doubled.
                        if state == "ACTIVE" {
                            if let Some(delta) = step.get("text_delta").and_then(Value::as_str) {
                                self.append_assistant(delta, emit);
                            }
                        }
                    }
                }
            }
            "result" => {
                let Some(result) = event.get("result") else {
                    return;
                };
                self.activity.terminal_status = result
                    .get("status")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned);
                if let Some(response) = result.get("response").and_then(Value::as_str) {
                    // The result repeats the final response; only use it if no deltas
                    // arrived, so the prose is not duplicated.
                    if self.activity.assistant_text.trim().is_empty() {
                        self.append_assistant(response, emit);
                    }
                }
                if let Some(usage) = result.get("usage") {
                    self.activity.usage = Some(StreamUsage {
                        input_tokens: usage.get("input_tokens").and_then(Value::as_u64),
                        output_tokens: usage.get("output_tokens").and_then(Value::as_u64),
                        cost_usd: None,
                    });
                }
            }
            _ => {}
        }
    }

    /// `claude --output-format stream-json`: whole `assistant` messages, plus `result`.
    fn consume_claude(&mut self, kind: &str, event: &Value, emit: &mut impl FnMut(&str)) {
        match kind {
            "assistant" => {
                let Some(message) = event.get("message") else {
                    return;
                };
                if let Some(usage) = message.get("usage") {
                    // Claude reports cumulative-input per turn, not a run total, so the
                    // last turn's counts are kept rather than summed.
                    self.activity.usage = Some(StreamUsage {
                        input_tokens: usage.get("input_tokens").and_then(Value::as_u64),
                        output_tokens: usage.get("output_tokens").and_then(Value::as_u64),
                        cost_usd: self.activity.usage.and_then(|u| u.cost_usd),
                    });
                }
                let Some(content) = message.get("content").and_then(Value::as_array) else {
                    return;
                };
                for block in content {
                    match block.get("type").and_then(Value::as_str) {
                        Some("text") => {
                            if let Some(text) = block.get("text").and_then(Value::as_str) {
                                self.append_assistant(text, emit);
                            }
                        }
                        Some("tool_use") => {
                            let name = block.get("name").and_then(Value::as_str).unwrap_or("tool");
                            self.activity.tool_calls.push(ToolCall {
                                name: name.to_owned(),
                                failed: false,
                            });
                            emit(&format!("▸ {name}"));
                        }
                        _ => {}
                    }
                }
            }
            "user" => {
                // Tool results come back on the user turn.
                let Some(content) = event
                    .get("message")
                    .and_then(|m| m.get("content"))
                    .and_then(Value::as_array)
                else {
                    return;
                };
                for block in content {
                    if block.get("type").and_then(Value::as_str) != Some("tool_result") {
                        continue;
                    }
                    let failed = block
                        .get("is_error")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    if failed {
                        if let Some(call) = self.activity.tool_calls.last_mut() {
                            call.failed = true;
                        }
                    }
                    emit(if failed {
                        "✗ tool result"
                    } else {
                        "✓ tool result"
                    });
                }
            }
            "result" => {
                let is_error = event
                    .get("is_error")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                self.activity.terminal_status = Some(
                    event
                        .get("subtype")
                        .and_then(Value::as_str)
                        .unwrap_or(if is_error { "error" } else { "success" })
                        .to_owned(),
                );
                if let Some(usage) = event.get("usage") {
                    self.activity.usage = Some(StreamUsage {
                        input_tokens: usage.get("input_tokens").and_then(Value::as_u64),
                        output_tokens: usage.get("output_tokens").and_then(Value::as_u64),
                        cost_usd: event.get("total_cost_usd").and_then(Value::as_f64),
                    });
                }
                if let Some(result) = event.get("result").and_then(Value::as_str) {
                    if self.activity.assistant_text.trim().is_empty() {
                        self.append_assistant(result, emit);
                    }
                }
            }
            // `system` and `rate_limit_event` are harness bookkeeping, not agent activity.
            _ => {}
        }
    }

    /// Append assistant prose.
    ///
    /// The readable log gets the text regardless of the summary cap: the log is a ring
    /// buffer with its own bound, and a human watching a long run should not have the log
    /// fall silent at exactly the point where the agent is most talkative. Only the
    /// summary is capped, because the summary is what reaches a model.
    fn append_assistant(&mut self, text: &str, emit: &mut impl FnMut(&str)) {
        emit(text);
        if self.activity.assistant_text.len() >= ASSISTANT_TEXT_CAP {
            self.activity.assistant_text_truncated = true;
            return;
        }
        self.activity.assistant_text.push_str(text);
        if self.activity.assistant_text.len() > ASSISTANT_TEXT_CAP {
            let mut end = ASSISTANT_TEXT_CAP;
            while end > 0 && !self.activity.assistant_text.is_char_boundary(end) {
                end -= 1;
            }
            self.activity.assistant_text.truncate(end);
            self.activity.assistant_text_truncated = true;
        }
    }
}

/// Decode a whole recorded stream, for tests and for replaying a fixture.
#[must_use]
pub fn decode_all(kind: WorkerKind, stream: &str) -> (Vec<String>, StreamActivity) {
    let mut decoder = StreamDecoder::new(kind);
    let mut lines = Vec::new();
    decoder.push(stream, &mut |line| lines.push(line.to_owned()));
    let activity = decoder.finish(&mut |line| lines.push(line.to_owned()));
    (lines, activity)
}

#[cfg(test)]
mod tests {
    use super::{ASSISTANT_TEXT_CAP, StreamDecoder, decode_all};
    use aftman_core::job::WorkerKind;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    /// The recorded `pi --mode json` run: one `read` call, then "done".
    const PI: &str = include_str!("../tests/fixtures/pi/one-shot.jsonl");
    /// The recorded `agy --print --output-format stream-json` run.
    const AGY: &str = include_str!("../tests/fixtures/agy/print.jsonl");
    /// The recorded `claude -p --output-format stream-json` run.
    const CLAUDE: &str = include_str!("../tests/fixtures/claude/print.jsonl");

    // ---- pi ----------------------------------------------------------------

    #[test]
    fn pi_recorded_output_yields_prose_tools_and_usage() -> TestResult {
        let (lines, activity) = decode_all(WorkerKind::Pi, PI);
        assert_eq!(activity.assistant_text.trim(), "done");
        assert_eq!(activity.tool_calls(), 1);
        assert_eq!(activity.tool_names(), vec!["read"]);
        assert_eq!(activity.failed_tools(), 0);
        assert_eq!(activity.aborted, Some(false));
        let usage = activity.usage.ok_or("pi reports usage")?;
        assert!(usage.input_tokens.is_some());
        assert!(usage.cost_usd.is_some(), "pi reports a cost");
        // The readable log names the tool and shows the prose, not the JSON.
        assert!(lines.iter().any(|l| l == "▸ read"), "{lines:?}");
        assert!(lines.iter().any(|l| l == "done"), "{lines:?}");
        assert!(
            !lines.iter().any(|l| l.contains("\"type\"")),
            "no raw JSON should reach the log: {lines:?}"
        );
        Ok(())
    }

    #[test]
    fn pi_output_is_fully_recognized() {
        // A stream that mostly fails to parse means the backend's format moved.
        let (_, activity) = decode_all(WorkerKind::Pi, PI);
        assert!(activity.looks_structured(), "{activity:?}");
        assert_eq!(activity.unparsed_lines, 0, "{activity:?}");
    }

    // ---- agy ---------------------------------------------------------------

    #[test]
    fn agy_recorded_output_yields_prose_tools_and_status() {
        // `agy` envelopes under `event`, not `type`. A parser assuming `type` would
        // report a quiet worker here, which is exactly the failure the fixture prevents.
        let (lines, activity) = decode_all(WorkerKind::Agy, AGY);
        assert_eq!(activity.assistant_text.trim(), "done");
        assert_eq!(activity.tool_calls(), 1);
        assert_eq!(activity.tool_names(), vec!["view_file"]);
        assert_eq!(activity.terminal_status.as_deref(), Some("SUCCESS"));
        assert!(activity.looks_structured(), "{activity:?}");
        assert!(lines.iter().any(|l| l == "▸ view_file"), "{lines:?}");
        assert!(
            lines.iter().any(|l| l.starts_with("✓ view_file")),
            "{lines:?}"
        );
    }

    #[test]
    fn agy_prose_is_not_duplicated_by_its_result_event() {
        // `agy` repeats the whole response in its `result` event after having already
        // streamed it as a delta. Appending both would double the prose.
        let (_, activity) = decode_all(WorkerKind::Agy, AGY);
        assert_eq!(
            activity.assistant_text.matches("done").count(),
            1,
            "prose arrived twice: {:?}",
            activity.assistant_text
        );
    }

    // ---- claude ------------------------------------------------------------

    #[test]
    fn claude_recorded_output_yields_prose_tools_and_cost() {
        let (lines, activity) = decode_all(WorkerKind::Claude, CLAUDE);
        assert_eq!(activity.assistant_text.trim(), "done");
        assert_eq!(activity.tool_calls(), 1);
        assert_eq!(activity.tool_names(), vec!["Read"]);
        assert_eq!(activity.failed_tools(), 0);
        assert!(activity.terminal_status.is_some());
        assert!(activity.looks_structured(), "{activity:?}");
        assert!(lines.iter().any(|l| l == "▸ Read"), "{lines:?}");
        assert!(lines.iter().any(|l| l == "✓ tool result"), "{lines:?}");
    }

    #[test]
    fn claude_harness_bookkeeping_is_not_shown_as_agent_activity() {
        // `system` and `rate_limit_event` are the harness talking, not the agent.
        let (lines, activity) = decode_all(WorkerKind::Claude, CLAUDE);
        assert!(!lines.iter().any(|l| l.contains("rate_limit")), "{lines:?}");
        assert_eq!(
            activity.tool_calls(),
            1,
            "only the real tool call is counted"
        );
    }

    // ---- general behaviour -------------------------------------------------

    #[test]
    fn an_unstructured_backend_passes_lines_through_unchanged() {
        let (lines, activity) = decode_all(WorkerKind::Generic, "hello\nworld\n");
        assert_eq!(lines, vec!["hello", "world"]);
        assert_eq!(activity.lines, 2);
        assert_eq!(
            activity.unparsed_lines, 0,
            "plain output is not a parse failure"
        );
        assert_eq!(activity.assistant_text, "");
    }

    #[test]
    fn a_partial_line_waits_for_the_rest_of_its_chunk() {
        // A pipe read can split a JSON object in half; decoding an object at a time
        // would count it as unparsed and lose the tool call.
        let payload = r#"{"type":"tool_execution_start","toolName":"bash"}"#;
        let (first, second) = payload.split_at(20);
        let mut decoder = StreamDecoder::new(WorkerKind::Pi);
        let mut lines = Vec::new();
        decoder.push(first, &mut |l| lines.push(l.to_owned()));
        assert!(lines.is_empty(), "nothing complete yet");
        decoder.push(&format!("{second}\n"), &mut |l| lines.push(l.to_owned()));
        assert_eq!(lines, vec!["▸ bash"]);
    }

    #[test]
    fn a_trailing_partial_line_is_still_consumed() {
        let mut decoder = StreamDecoder::new(WorkerKind::Generic);
        let mut lines = Vec::new();
        decoder.push("no trailing newline", &mut |l| lines.push(l.to_owned()));
        assert!(lines.is_empty());
        let activity = decoder.finish(&mut |l| lines.push(l.to_owned()));
        assert_eq!(lines, vec!["no trailing newline"]);
        assert_eq!(activity.lines, 1);
    }

    #[test]
    fn non_json_output_is_kept_rather_than_dropped() {
        // A CLI's diagnostics can land on stdout, and dropping them loses the only
        // explanation of a failure.
        let mut decoder = StreamDecoder::new(WorkerKind::Pi);
        let mut lines = Vec::new();
        decoder.push("Warning: something happened\n", &mut |l| {
            lines.push(l.to_owned())
        });
        decoder.finish(&mut |l| lines.push(l.to_owned()));
        assert_eq!(lines, vec!["Warning: something happened"]);
    }

    #[test]
    fn a_known_envelope_with_an_unknown_kind_is_ignored_not_echoed() {
        // Forward compatibility: the backend adding an event kind must not put raw JSON
        // in front of a model.
        let (lines, activity) = decode_all(
            WorkerKind::Pi,
            "{\"type\":\"something_new\",\"payload\":{\"a\":1}}\n",
        );
        assert!(lines.is_empty(), "{lines:?}");
        assert_eq!(
            activity.unparsed_lines, 0,
            "a known envelope is not a parse failure"
        );
        assert_eq!(activity.lines, 1);
    }

    #[test]
    fn assistant_text_is_capped_for_the_summary_but_the_log_keeps_it_all() {
        let mut stream = String::new();
        for _ in 0..200 {
            let chunk = "x".repeat(100);
            // Written to the recorded shape, not to the decoder's assumption.
            stream.push_str(&format!(
                "{{\"type\":\"message_update\",\"assistantMessageEvent\":{{\"type\":\"text_delta\",\"delta\":\"{chunk}\"}}}}\n"
            ));
        }
        let (lines, activity) = decode_all(WorkerKind::Pi, &stream);
        assert!(activity.assistant_text.len() <= ASSISTANT_TEXT_CAP);
        assert!(activity.assistant_text_truncated);
        // The log is separately bounded, so the prose is still there in full.
        let logged: usize = lines.iter().map(String::len).sum();
        assert!(
            logged > ASSISTANT_TEXT_CAP * 2,
            "the log kept {logged} bytes"
        );
    }

    #[test]
    fn a_failed_tool_is_marked_as_failed() {
        let stream = "\
{\"type\":\"tool_execution_start\",\"toolName\":\"bash\"}
{\"type\":\"tool_execution_end\",\"toolName\":\"bash\",\"isError\":true}
";
        let (lines, activity) = decode_all(WorkerKind::Pi, stream);
        assert_eq!(activity.tool_calls(), 1);
        assert_eq!(activity.failed_tools(), 1);
        assert!(lines.iter().any(|l| l.starts_with('✗')), "{lines:?}");
    }

    #[test]
    fn an_aborted_run_is_reported_as_aborted() {
        let (_, activity) = decode_all(
            WorkerKind::Pi,
            "{\"type\":\"agent_settled\",\"aborted\":true}\n",
        );
        assert_eq!(activity.aborted, Some(true));
    }
}
