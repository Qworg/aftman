//! Terminal formatting. Pure functions from protocol and domain values to text, so
//! the daemon-backed and the embedded paths print the same thing, and the TUI can
//! reuse the pieces it wants.

use aftman_core::event::{EventEnvelope, EventKind};
use aftman_core::ids::RunId;
use aftman_core::run::{RunState, RunSummary};
use serde_json::Value;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::error::CliError;

/// A leading marker makes the decision path skimmable: `>` is a state or worker
/// transition, `*` is a decision, `!` is something going wrong, `?` is a question
/// for a human, `=` is the end.
#[must_use]
pub const fn marker(kind: EventKind) -> &'static str {
    match kind {
        EventKind::DirectiveSelected => "*",
        EventKind::ModelError | EventKind::Error | EventKind::RateLimited => "!",
        EventKind::EscalationRaised => "?",
        EventKind::RunFinished => "=",
        _ => ">",
    }
}

/// One event as the terminal shows it while a run is followed: a summary line, and for
/// a decision a second line with its reason.
#[must_use]
pub fn event_lines(event: &EventEnvelope) -> Vec<String> {
    let mut lines = vec![format!(
        "{} [{:>4}] {}",
        marker(event.kind),
        event.seq.get(),
        event.summary()
    )];
    if event.kind == EventKind::DirectiveSelected {
        if let Some(reason) = event.payload.get("reason").and_then(Value::as_str) {
            if !reason.is_empty() {
                lines.push(format!("        reason: {reason}"));
            }
        }
    }
    lines
}

/// The text of a `WORKER_OUTPUT` event, if that is what this is.
#[must_use]
pub fn worker_output_text(event: &EventEnvelope) -> Option<&str> {
    if event.kind != EventKind::WorkerOutput {
        return None;
    }
    event.payload.get("text").and_then(Value::as_str)
}

/// The status a `RUN_FINISHED` event reports.
#[must_use]
pub fn finished_status(event: &EventEnvelope) -> Option<&str> {
    if event.kind != EventKind::RunFinished {
        return None;
    }
    event.payload.get("status").and_then(Value::as_str)
}

/// An escalation a `STATE_CHANGED -> needs_human` is about, as raised on the stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaisedEscalation {
    pub id: String,
    pub ask: String,
}

/// Read an `ESCALATION_RAISED` event.
#[must_use]
pub fn raised_escalation(event: &EventEnvelope) -> Option<RaisedEscalation> {
    if event.kind != EventKind::EscalationRaised {
        return None;
    }
    Some(RaisedEscalation {
        id: event.payload.get("escalation_id")?.as_str()?.to_owned(),
        ask: event
            .payload
            .get("ask")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
    })
}

/// Whether `event` moves the run into `needs_human`.
#[must_use]
pub fn enters_needs_human(event: &EventEnvelope) -> bool {
    event.kind == EventKind::StateChanged
        && event.payload.get("to").and_then(Value::as_str) == Some("needs_human")
}

/// How to reach the daemon again from a printed command: the global flags the user
/// gave, so a copy-pasted command talks to the same daemon.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GlobalFlags {
    pub socket: Option<String>,
    pub data_dir: Option<String>,
}

impl GlobalFlags {
    /// `aftman` plus the location flags, as a command to copy.
    #[must_use]
    pub fn prefix(&self) -> String {
        let mut out = String::from("aftman");
        if let Some(socket) = &self.socket {
            out.push_str(&format!(" --socket {}", shell_word(socket)));
        }
        if let Some(dir) = &self.data_dir {
            out.push_str(&format!(" --data-dir {}", shell_word(dir)));
        }
        out
    }
}

fn shell_word(text: &str) -> String {
    let plain = !text.is_empty()
        && text
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "/._-:@%+=,".contains(c));
    if plain {
        text.to_owned()
    } else {
        format!("'{}'", text.replace('\'', r"'\''"))
    }
}

/// What `aftman run` prints when the run needs a human and the command exits 3.
#[must_use]
pub fn needs_human_message(
    flags: &GlobalFlags,
    run_id: RunId,
    escalation_id: &str,
    ask: &str,
) -> String {
    let aftman = flags.prefix();
    let mut out = String::new();
    out.push_str(&format!("run {run_id} needs a human\n"));
    out.push_str(&format!("  escalation  {escalation_id}\n"));
    for (i, line) in ask.lines().enumerate() {
        let label = if i == 0 {
            "  ask         "
        } else {
            "              "
        };
        out.push_str(&format!("{label}{line}\n"));
    }
    out.push_str("\nanswer with guidance for the worker:\n");
    out.push_str(&format!(
        "  {aftman} answer {run_id} {escalation_id} \"<what the worker should do>\"\n"
    ));
    out.push_str("or with a decision:\n");
    out.push_str(&format!(
        "  {aftman} answer {run_id} {escalation_id} --accept | --recheck | --stop | --claim-complete\n"
    ));
    out.push_str("\nthe run keeps waiting in the daemon.");
    out
}

/// What `aftman run` prints when the user detaches with Ctrl-C.
#[must_use]
pub fn detached_message(flags: &GlobalFlags, run_id: RunId) -> String {
    let aftman = flags.prefix();
    format!(
        "detached: run {run_id} keeps running in the daemon\n  \
         watch it   {aftman} logs {run_id} -f\n  \
         inspect    {aftman} show {run_id}\n  \
         stop it    {aftman} cancel {run_id}"
    )
}

/// The closing summary of a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunReport {
    pub run_id: RunId,
    pub status: String,
    pub directive: String,
    pub assessments: u32,
    pub skipped: u32,
    pub workers: u32,
    pub attempts: u32,
    pub verifiers: u32,
}

impl RunReport {
    /// From a stored run state.
    #[must_use]
    pub fn from_state(state: &RunState) -> Self {
        Self {
            run_id: state.header.id,
            status: state.status.as_str().to_owned(),
            directive: state
                .last_directive
                .as_ref()
                .map_or("-", |d| d.name())
                .to_owned(),
            assessments: state.counters.assessments,
            skipped: state.counters.assessments_skipped,
            workers: state.counters.workers_run,
            attempts: state.counters.attempts,
            verifiers: state.counters.verifiers_run,
        }
    }

    #[must_use]
    pub fn lines(&self) -> Vec<String> {
        vec![
            format!("run id      {}", self.run_id),
            format!("status      {}", self.status),
            format!("directive   {}", self.directive),
            format!(
                "assessments {} ({} skipped as unchanged)",
                self.assessments, self.skipped
            ),
            format!(
                "workers     {} ({} attempts, {} verifiers)",
                self.workers, self.attempts, self.verifiers
            ),
        ]
    }
}

#[must_use]
pub fn truncate(text: &str, max: usize) -> String {
    let flattened = text.replace('\n', " ");
    if flattened.chars().count() <= max {
        return flattened;
    }
    let mut out: String = flattened.chars().take(max).collect();
    out.push('…');
    out
}

/// `aftman ls`: the header and one line per run.
#[must_use]
pub fn run_table(runs: &[RunSummary]) -> Vec<String> {
    let mut lines = vec![format!(
        "{:<38} {:<12} {:>8}  JOB",
        "RUN ID", "STATUS", "ELAPSED"
    )];
    for run in runs {
        lines.push(format!(
            "{:<38} {:<12} {:>7}s  {}",
            run.id,
            run.status.as_str(),
            run.elapsed_secs,
            truncate(&run.job_excerpt, 52)
        ));
    }
    lines
}

/// An escalation that is still waiting for an answer, as the event log shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenEscalation {
    pub id: String,
    pub kind: String,
    pub ask: String,
}

/// The escalations the log says are open: raised and not yet answered. The log is the
/// source of truth; the snapshot may trail it.
#[must_use]
pub fn open_escalations(events: &[EventEnvelope]) -> Vec<OpenEscalation> {
    let mut open: Vec<OpenEscalation> = Vec::new();
    for event in events {
        match event.kind {
            EventKind::EscalationRaised => {
                if let Some(raised) = raised_escalation(event) {
                    open.push(OpenEscalation {
                        id: raised.id,
                        kind: event
                            .payload
                            .get("kind")
                            .and_then(Value::as_str)
                            .unwrap_or("check")
                            .to_owned(),
                        ask: raised.ask,
                    });
                }
            }
            EventKind::EscalationAnswered => {
                if let Some(id) = event.payload.get("escalation_id").and_then(Value::as_str) {
                    open.retain(|o| o.id != id);
                }
            }
            _ => {}
        }
    }
    open
}

/// The status the log says the run is in: the last `STATE_CHANGED` wins when the log is
/// ahead of the snapshot (the snapshot is written after the events it summarises, so a
/// reader can catch it behind).
#[must_use]
pub fn reconciled_status(state: &RunState, events: &[EventEnvelope]) -> String {
    events
        .iter()
        .rev()
        .find(|e| e.kind == EventKind::StateChanged && e.seq > state.last_seq)
        .and_then(|e| e.payload.get("to").and_then(Value::as_str))
        .map_or_else(|| state.status.as_str().to_owned(), ToOwned::to_owned)
}

/// The lines describing how a run was launched, from a `RUN_CREATED` payload's
/// `launch` map: the supervisor, the worker, and the permission posture.
#[must_use]
pub fn launch_lines(payload: &Value) -> Vec<String> {
    let Some(launch) = payload.get("launch").and_then(Value::as_object) else {
        return Vec::new();
    };
    let get = |key: &str| launch.get(key).and_then(Value::as_str).unwrap_or("");
    let mut lines = Vec::new();
    match get("supervisor") {
        "simulated" => {
            lines.push("using the simulated decision model (--simulate-model)".to_owned())
        }
        "jev" => lines.push(format!(
            "supervising with {} at {}{}",
            get("model"),
            get("base_url"),
            if get("loopback") == "true" {
                " (loopback)"
            } else {
                ""
            }
        )),
        _ => {}
    }
    let worker = get("worker");
    if !worker.is_empty() && worker != "simulated" {
        lines.push(format!(
            "worker      {worker} ({}), steering {}",
            get("worker_mode"),
            get("steering")
        ));
    }
    if get("permissions") == "auto-approve" {
        lines.push(auto_approve_warning());
    }
    lines
}

/// The warning printed on every `auto-approve` run, in both modes.
#[must_use]
pub fn auto_approve_warning() -> String {
    "WARNING: --permissions auto-approve: the agent runs with its approval prompts \
     suppressed, and aftman does not sandbox it. Isolation is yours to provide."
        .to_owned()
}

/// `aftman show`: the summary, how it was launched, open escalations (with the command
/// to answer them when `answer_hint` says it would work), then the timeline.
#[must_use]
pub fn show_lines(
    state: &RunState,
    events: &[EventEnvelope],
    flags: &GlobalFlags,
    answer_hint: bool,
) -> Vec<String> {
    let status = reconciled_status(state, events);
    let mut lines = vec![
        format!("run id      {}", state.header.id),
        format!("created     {}", state.header.created_at),
        format!("repo        {}", state.header.job.repo.display()),
        format!("status      {status}"),
        format!("worker      {}", state.header.config.worker.as_str()),
        format!(
            "last seq    {}",
            events.last().map_or(state.last_seq, |e| e.seq)
        ),
        format!("job         {}", truncate(&state.header.job.prompt, 200)),
    ];
    if let Some(created) = events.iter().find(|e| e.kind == EventKind::RunCreated) {
        if let Some(launch) = created.payload.get("launch").and_then(Value::as_object) {
            let get = |key: &str| launch.get(key).and_then(Value::as_str).unwrap_or("?");
            lines.push(format!(
                "posture     permissions {}, external sandbox {}, steering {}",
                get("permissions"),
                get("external_sandbox"),
                get("steering")
            ));
            if get("permissions") == "auto-approve" {
                lines.push(format!("            {}", auto_approve_warning()));
            }
        }
    }
    if let Some(directive) = &state.last_directive {
        lines.push(format!(
            "directive   {} ({})",
            directive.name(),
            directive.reason().unwrap_or("")
        ));
    }
    let aftman = flags.prefix();
    let waiting = status == "needs_human";
    for escalation in open_escalations(events) {
        lines.push(String::new());
        lines.push(format!(
            "open escalation {} ({})",
            escalation.id, escalation.kind
        ));
        lines.extend(escalation.ask.lines().map(|l| format!("  {l}")));
        if answer_hint && waiting {
            lines.push(format!(
                "  answer: {aftman} answer {} {} \"<guidance>\"   (or --accept --recheck --stop --claim-complete)",
                state.header.id, escalation.id
            ));
        }
    }
    lines.push(String::new());
    lines.push("timeline".to_owned());
    let timeline = timeline_lines(events);
    if timeline.is_empty() {
        lines.push("  (no decisions recorded yet)".to_owned());
    } else {
        lines.extend(timeline.iter().map(|l| format!("  {l}")));
    }
    lines
}

/// The decision timeline `show` prints: state changes, decisions, and escalations.
#[must_use]
pub fn timeline_lines(events: &[EventEnvelope]) -> Vec<String> {
    let mut out = Vec::new();
    for event in events {
        match event.kind {
            EventKind::StateChanged
            | EventKind::DirectiveSelected
            | EventKind::EscalationRaised
            | EventKind::EscalationAnswered
            | EventKind::RunFinished => out.extend(event_lines(event)),
            _ => {}
        }
    }
    out
}

/// `aftman decisions`: every selection, suppression, and ensemble resolution.
#[must_use]
pub fn decision_lines(events: &[EventEnvelope]) -> Vec<String> {
    let mut out = Vec::new();
    for event in events {
        match event.kind {
            EventKind::DirectiveSelected => {
                let directive = event.payload["directive"].as_str().unwrap_or("?");
                let invariants = event.payload["invariants"].as_array().map_or(0, Vec::len);
                out.push(format!(
                    "[{:>4}] selected {directive} ({invariants} invariants)",
                    event.seq.get()
                ));
                if let Some(suppressed) = event.payload["suppressed"].as_array() {
                    for entry in suppressed {
                        out.push(format!(
                            "        suppressed {:<34} {}",
                            entry["rule"].as_str().unwrap_or("?"),
                            entry["reason"].as_str().unwrap_or("?")
                        ));
                    }
                }
            }
            EventKind::EnsembleResolved => {
                let unresolved = event.payload["unresolved"].as_array().map_or(0, Vec::len);
                out.push(format!(
                    "[{:>4}] ensemble enabled={} margin={} unresolved={unresolved}",
                    event.seq.get(),
                    event.payload["enabled"],
                    event.payload["margin"]
                ));
            }
            EventKind::AssessmentSkippedUnchanged => {
                out.push(format!(
                    "[{:>4}] skipped: observation unchanged",
                    event.seq.get()
                ));
            }
            EventKind::ModelError => {
                out.push(format!(
                    "[{:>4}] model error: {}",
                    event.seq.get(),
                    event.payload["code"].as_str().unwrap_or("?")
                ));
            }
            _ => {}
        }
    }
    out
}

/// Turn a `--since` value into an absolute time: a duration back from `now`
/// (`90s`, `30m`, `12h`, `7d`, `2w`) or an RFC 3339 timestamp.
///
/// # Errors
/// Anything else.
pub fn parse_since(text: &str, now: OffsetDateTime) -> Result<OffsetDateTime, CliError> {
    let text = text.trim();
    if let Ok(at) = OffsetDateTime::parse(text, &Rfc3339) {
        return Ok(at);
    }
    let bad = || {
        CliError::usage(format!(
            "`{text}` is not a time: use a duration like 90s, 30m, 12h, 7d, 2w, or an RFC 3339 timestamp"
        ))
    };
    let split = text.find(|c: char| !c.is_ascii_digit()).ok_or_else(bad)?;
    let (digits, unit) = text.split_at(split);
    let count: i64 = digits.parse().map_err(|_| bad())?;
    let seconds = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 3_600,
        "d" => 86_400,
        "w" => 604_800,
        _ => return Err(bad()),
    };
    let span = time::Duration::seconds(count.checked_mul(seconds).ok_or_else(bad)?);
    now.checked_sub(span).ok_or_else(bad)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aftman_core::ids::EventSeq;
    use serde_json::json;
    use time::macros::datetime;

    fn event(seq: u64, kind: EventKind, payload: Value) -> EventEnvelope {
        EventEnvelope::new(
            EventSeq(seq),
            RunId::new(),
            datetime!(2026-01-01 00:00:00 UTC),
            kind,
            payload,
        )
    }

    #[test]
    fn a_decision_renders_with_its_reason_and_the_right_marker() {
        let lines = event_lines(&event(
            7,
            EventKind::DirectiveSelected,
            json!({"directive": "Continue", "reason": "worker is making progress"}),
        ));
        assert_eq!(lines[0], "* [   7] DIRECTIVE_SELECTED: Continue");
        assert_eq!(lines[1], "        reason: worker is making progress");
        assert_eq!(marker(EventKind::RunFinished), "=");
        assert_eq!(marker(EventKind::ModelError), "!");
        assert_eq!(marker(EventKind::EscalationRaised), "?");
        assert_eq!(marker(EventKind::WorkerStarted), ">");
    }

    #[test]
    fn an_empty_reason_prints_no_second_line() {
        let lines = event_lines(&event(
            1,
            EventKind::DirectiveSelected,
            json!({"directive": "Stop", "reason": ""}),
        ));
        assert_eq!(lines.len(), 1);
    }

    #[test]
    fn worker_output_and_finish_are_readable_from_payloads() {
        let out = event(2, EventKind::WorkerOutput, json!({"text": "hello\n"}));
        assert_eq!(worker_output_text(&out), Some("hello\n"));
        assert_eq!(finished_status(&out), None);
        let done = event(3, EventKind::RunFinished, json!({"status": "finished"}));
        assert_eq!(finished_status(&done), Some("finished"));
        assert_eq!(worker_output_text(&done), None);
    }

    #[test]
    fn an_escalation_is_read_from_the_stream() {
        let raised = event(
            4,
            EventKind::EscalationRaised,
            json!({"escalation_id": "e-1", "ask": "is it done?"}),
        );
        assert_eq!(
            raised_escalation(&raised),
            Some(RaisedEscalation {
                id: "e-1".into(),
                ask: "is it done?".into()
            })
        );
        let changed = event(5, EventKind::StateChanged, json!({"to": "needs_human"}));
        assert!(enters_needs_human(&changed));
        let other = event(5, EventKind::StateChanged, json!({"to": "running"}));
        assert!(!enters_needs_human(&other));
    }

    #[test]
    fn the_needs_human_message_has_the_exact_answer_command() {
        let run = RunId::new();
        let flags = GlobalFlags {
            socket: Some("/tmp/a b/s.sock".into()),
            data_dir: None,
        };
        let text = needs_human_message(&flags, run, "esc-1", "Is it done?\nPlease check.");
        assert!(text.contains(&format!("run {run} needs a human")));
        assert!(text.contains("escalation  esc-1"));
        assert!(text.contains("Is it done?") && text.contains("Please check."));
        assert!(text.contains(&format!(
            "aftman --socket '/tmp/a b/s.sock' answer {run} esc-1 \"<what the worker should do>\""
        )));
        assert!(text.contains("--accept | --recheck | --stop | --claim-complete"));
        assert!(text.contains("keeps waiting"));
    }

    #[test]
    fn the_detach_message_says_how_to_watch_and_cancel() {
        let run = RunId::new();
        let text = detached_message(&GlobalFlags::default(), run);
        assert!(text.contains(&format!("aftman logs {run} -f")));
        assert!(text.contains(&format!("aftman cancel {run}")));
    }

    #[test]
    fn since_accepts_durations_and_timestamps() {
        let now = datetime!(2026-03-10 12:00:00 UTC);
        assert_eq!(
            parse_since("7d", now).unwrap(),
            datetime!(2026-03-03 12:00:00 UTC)
        );
        assert_eq!(
            parse_since("90s", now).unwrap(),
            datetime!(2026-03-10 11:58:30 UTC)
        );
        assert_eq!(
            parse_since("2026-01-01T00:00:00Z", now).unwrap(),
            datetime!(2026-01-01 00:00:00 UTC)
        );
        for bad in ["", "d", "7", "7x", "-3d", "yesterday"] {
            assert!(parse_since(bad, now).is_err(), "{bad}");
        }
    }

    #[test]
    fn truncation_is_on_characters_and_flattens_newlines() {
        assert_eq!(truncate("a\nb", 10), "a b");
        assert_eq!(truncate("héllo wörld", 5), "héllo…");
    }

    #[test]
    fn the_log_overrides_a_snapshot_that_trails_it() {
        use aftman_core::job::{FactoryStatus, Job, JobSource, RunConfig};
        use aftman_core::run::RunHeader;
        let id = RunId::new();
        let header = RunHeader {
            id,
            created_at: datetime!(2026-01-01 00:00:00 UTC),
            job: Job {
                prompt: "j".into(),
                repo: "/r".into(),
                source: JobSource::Cli,
            },
            config: RunConfig::default(),
            aftman_version: "x".into(),
            worker_versions: Default::default(),
            granted_key_names: vec![],
        };
        let mut state = RunState::new(header, FactoryStatus::Running);
        state.last_seq = EventSeq(2);
        let mk = |seq, kind, payload| {
            EventEnvelope::new(
                EventSeq(seq),
                id,
                datetime!(2026-01-01 00:00:00 UTC),
                kind,
                payload,
            )
        };
        let events = vec![
            mk(2, EventKind::StateChanged, json!({"to": "running"})),
            mk(
                3,
                EventKind::EscalationRaised,
                json!({"escalation_id": "e1", "kind": "check", "ask": "?"}),
            ),
            mk(4, EventKind::StateChanged, json!({"to": "needs_human"})),
        ];
        assert_eq!(reconciled_status(&state, &events), "needs_human");
        assert_eq!(reconciled_status(&state, &events[..1]), "running");
        let lines = show_lines(&state, &events, &GlobalFlags::default(), true);
        assert!(lines.iter().any(|l| l == "status      needs_human"));
        assert!(lines.iter().any(|l| l.contains("answer: aftman answer")));
        let no_hint = show_lines(&state, &events, &GlobalFlags::default(), false);
        assert!(!no_hint.iter().any(|l| l.contains("answer: ")));
        let answered = [
            events.clone(),
            vec![mk(
                5,
                EventKind::EscalationAnswered,
                json!({"escalation_id": "e1"}),
            )],
        ]
        .concat();
        assert!(open_escalations(&answered).is_empty());
    }

    #[test]
    fn the_launch_lines_warn_about_auto_approve_and_describe_the_supervisor() {
        let payload = json!({"launch": {"supervisor": "jev", "model": "m", "base_url": "https://h", "loopback": "false",
            "worker": "pi", "worker_mode": "one-shot", "steering": "unsupported", "permissions": "auto-approve"}});
        let lines = launch_lines(&payload);
        assert_eq!(lines[0], "supervising with m at https://h");
        assert_eq!(lines[1], "worker      pi (one-shot), steering unsupported");
        assert!(lines[2].starts_with("WARNING"));
        let sim = launch_lines(
            &json!({"launch": {"supervisor": "simulated", "worker": "simulated", "permissions": "sandboxed"}}),
        );
        assert_eq!(
            sim,
            ["using the simulated decision model (--simulate-model)"]
        );
        assert!(launch_lines(&json!({})).is_empty());
    }

    #[test]
    fn the_timeline_keeps_only_the_story() {
        let events = vec![
            event(1, EventKind::RunCreated, json!({})),
            event(2, EventKind::WorkerOutput, json!({"text": "x"})),
            event(3, EventKind::StateChanged, json!({"to": "running"})),
            event(4, EventKind::RunFinished, json!({"status": "finished"})),
        ];
        let lines = timeline_lines(&events);
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("STATE_CHANGED") && lines[1].contains("RUN_FINISHED"));
    }
}
