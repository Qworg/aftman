//! The pure state model behind `aftman watch`.
//!
//! [`WatchState`] is a fold over a run's event stream: `apply` takes one
//! [`EventEnvelope`] and updates what the screen shows. There is exactly one way in.
//! The driver replays the run from `seq` 1 through [`crate::client::Follow`], so
//! "catching up" and "live" are the same code path, and a reconnect (which resumes the
//! stream from `last_seq + 1`) cannot desynchronise it. Applying the same events one by
//! one, in a batch, or in two halves yields equal states (tested), and applying an event
//! whose `seq` was already seen is a no-op.
//!
//! # Tolerance
//!
//! The daemon may be newer than this TUI. Payloads are read field by field with `get`;
//! a missing field, a field of an unexpected type, or an extra field is ignored, never
//! an error and never a panic. A kind this model has no use for is only counted.
//!
//! An event kind this build has never heard of decodes as `EventKind::Unknown` (the
//! client does not reject the line, so the stream does not die), and the reducer's final
//! `_` arm counts it and changes nothing else: no timeline row, no alarm. A test feeds a
//! `"kind":"SOME_FUTURE_KIND"` push through the real wire decode.
//!
//! # Bounds (hard rule 5)
//!
//! Every collection is capped by a named constant below and evicts oldest-first, so a
//! week-long chatty run cannot grow the TUI's memory. Every string taken from an event
//! is capped *and* sanitised ([`super::sanitize`]) on the way in; nothing downstream
//! needs to defend against control characters.

use std::collections::VecDeque;

use serde_json::Value;
use time::OffsetDateTime;

use aftman_core::event::{EventEnvelope, EventKind};
use aftman_core::ids::RunId;
use aftman_core::job::FactoryStatus;
use aftman_core::run::RunState;

use super::sanitize::{clean_field, sanitize, truncate_chars};

/// Timeline entries kept (decisions and notable events together).
pub const MAX_TIMELINE_ENTRIES: usize = 400;
/// Worker-tail lines kept.
pub const MAX_TAIL_LINES: usize = 1_000;
/// Worker-tail bytes kept (sum of line lengths).
pub const MAX_TAIL_BYTES: usize = 128 * 1024;
/// Characters kept of one tail line; the rest is replaced by `…`.
pub const MAX_LINE_CHARS: usize = 500;
/// Characters taken from one `WORKER_OUTPUT` event before sanitising.
pub const MAX_OUTPUT_EVENT_CHARS: usize = 256 * 1024;
/// Characters kept of a reason, an ask, a message.
pub const MAX_FIELD_CHARS: usize = 1_000;
/// Characters kept of a short label (a key, a rule, a code).
pub const MAX_LABEL_CHARS: usize = 120;
/// Proposals shown per decision.
pub const MAX_PROPOSALS: usize = 24;
/// Suppressions shown per decision.
pub const MAX_SUPPRESSIONS: usize = 24;
/// Invariants shown per decision.
pub const MAX_INVARIANTS: usize = 16;
/// Checks shown in the current assessment.
pub const MAX_CHECKS: usize = 64;
/// Escalations remembered (answered ones are evicted first).
pub const MAX_ESCALATIONS: usize = 32;
/// Workers remembered.
pub const MAX_WORKERS: usize = 64;
/// Routing activations remembered.
pub const MAX_ACTIVATED: usize = 32;
/// `launch` entries shown from `RUN_CREATED`.
pub const MAX_LAUNCH: usize = 16;

/// How serious a timeline note is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    Info,
    /// Something to look at: a rate limit, a budget, a rejected steer.
    Warn,
    /// Something went wrong.
    Error,
    /// A person is needed or has just acted.
    Human,
}

/// One proposal a responsibility made in an assessment cycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal {
    pub responsibility: String,
    pub rule: String,
    pub directive: String,
    pub priority: Option<i64>,
}

/// One proposal that policy did not select, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Suppression {
    pub rule: String,
    pub reason: String,
}

/// A selected directive: the "why did it do that?" record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decision {
    pub seq: u64,
    pub at: OffsetDateTime,
    pub directive: String,
    pub reason: String,
    pub invariants: Vec<String>,
    pub suppressed: Vec<Suppression>,
    pub proposals: Vec<Proposal>,
    /// Proposals beyond [`MAX_PROPOSALS`].
    pub proposals_omitted: usize,
    pub assessment_seq: Option<u64>,
}

/// A notable event that is not a decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Note {
    pub seq: u64,
    pub at: OffsetDateTime,
    pub level: Level,
    pub text: String,
}

/// One row of the decision timeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entry {
    Decision(Decision),
    Note(Note),
}

impl Entry {
    #[must_use]
    pub fn seq(&self) -> u64 {
        match self {
            Self::Decision(d) => d.seq,
            Self::Note(n) => n.seq,
        }
    }
}

/// One check of the current assessment, with the numbers that decided it.
#[derive(Debug, Clone, PartialEq)]
pub struct CheckView {
    pub key: String,
    /// The decision model's probability, when it gave one.
    pub value: Option<f64>,
    pub threshold: Option<f64>,
    pub decisive: bool,
    pub contested: bool,
    pub permitted: bool,
    pub unresolved: bool,
}

impl CheckView {
    /// `value - threshold`: positive means the value clears the threshold.
    #[must_use]
    pub fn margin(&self) -> Option<f64> {
        Some(self.value? - self.threshold?)
    }
}

/// The latest completed assessment.
#[derive(Debug, Clone, PartialEq)]
pub struct Assessment {
    pub seq: u64,
    pub at: OffsetDateTime,
    pub model: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub checks: Vec<CheckView>,
    /// Checks beyond [`MAX_CHECKS`].
    pub checks_omitted: usize,
}

/// A question for a human.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EscalationView {
    pub id: String,
    pub kind: String,
    pub reason: String,
    pub ask: String,
    pub raised_seq: u64,
    /// How it was answered, once it was.
    pub answered: Option<String>,
}

/// What a worker is doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerPhase {
    Running,
    Completed { success: bool },
    Terminated,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerInfo {
    pub id: String,
    pub kind: String,
    pub task: String,
    pub supports_steering: bool,
    pub phase: WorkerPhase,
}

/// The latest thing that went wrong with the decision model or the runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trouble {
    pub seq: u64,
    pub at: OffsetDateTime,
    /// `model_error`, `rate_limited`, `budget_exceeded`, `error`.
    pub kind: &'static str,
    pub code: String,
    pub message: String,
}

/// How a run ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finish {
    pub status: String,
    pub reason: Option<String>,
}

/// Counters shown in the header.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    /// The highest `assessment_seq` seen (what the daemon counts).
    pub assessments: u32,
    /// Cycles that found the assessment budget spent and asked no one.
    pub budget_cycles: u32,
    pub skipped: u32,
    pub workers: u32,
    pub attempts: u32,
    pub verifiers: u32,
    pub steers: u32,
    pub events: u64,
}

/// Where a tail line came from. The view styles by this, never by what the text looks
/// like, so nothing a worker prints can pass for the TUI's own markers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TailKind {
    /// Text the worker printed.
    Worker,
    /// A line the TUI added (a worker boundary, a truncation notice).
    Marker,
}

/// The worker's output, as a bounded ring of sanitised lines.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Tail {
    lines: VecDeque<(TailKind, String)>,
    bytes: usize,
    /// The unfinished last line (always worker text).
    partial: String,
    partial_clipped: bool,
    worker: Option<String>,
    /// Lines evicted since the start.
    evicted: u64,
}

impl Tail {
    /// Finished lines, oldest first, then the unfinished one if there is one.
    pub fn lines(&self) -> impl Iterator<Item = (TailKind, &str)> {
        self.lines
            .iter()
            .map(|(kind, text)| (*kind, text.as_str()))
            .chain((!self.partial.is_empty()).then_some((TailKind::Worker, self.partial.as_str())))
    }

    /// How many lines [`Tail::lines`] yields.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lines.len() + usize::from(!self.partial.is_empty())
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The absolute index of the oldest line held. Indices count every line since the
    /// start, so a position stays valid as old lines are evicted.
    #[must_use]
    pub fn first_index(&self) -> u64 {
        self.evicted
    }

    /// The absolute index of the newest line, if there is one.
    #[must_use]
    pub fn last_index(&self) -> Option<u64> {
        (!self.is_empty()).then(|| self.evicted + self.len() as u64 - 1)
    }

    /// The line at an absolute index.
    #[must_use]
    pub fn line_at(&self, index: u64) -> Option<(TailKind, &str)> {
        let offset = usize::try_from(index.checked_sub(self.evicted)?).ok()?;
        self.lines().nth(offset)
    }

    /// Bytes held in finished lines.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Lines dropped to stay within the bounds.
    #[must_use]
    pub fn evicted(&self) -> u64 {
        self.evicted
    }

    fn push_text(&mut self, text: &str) {
        let capped: String = text.chars().take(MAX_OUTPUT_EVENT_CHARS).collect();
        let clean = sanitize(&capped);
        for (index, piece) in clean.split('\n').enumerate() {
            if index > 0 {
                self.finish_line();
            }
            self.append_to_partial(piece);
        }
    }

    fn append_to_partial(&mut self, piece: &str) {
        if self.partial_clipped {
            return;
        }
        let have = self.partial.chars().count();
        let room = MAX_LINE_CHARS.saturating_sub(have);
        if piece.chars().count() > room {
            self.partial
                .extend(piece.chars().take(room.saturating_sub(1)));
            self.partial.push('…');
            self.partial_clipped = true;
        } else {
            self.partial.push_str(piece);
        }
    }

    fn finish_line(&mut self) {
        let line = std::mem::take(&mut self.partial);
        self.partial_clipped = false;
        self.push_line(TailKind::Worker, line);
    }

    fn push_line(&mut self, kind: TailKind, line: String) {
        self.bytes += line.len() + 1;
        self.lines.push_back((kind, line));
        while self.lines.len() > MAX_TAIL_LINES || self.bytes > MAX_TAIL_BYTES {
            let Some((_, old)) = self.lines.pop_front() else {
                break;
            };
            self.bytes = self.bytes.saturating_sub(old.len() + 1);
            self.evicted += 1;
        }
    }

    /// A one-line marker (not worker text).
    fn marker(&mut self, text: &str) {
        if !self.partial.is_empty() {
            self.finish_line();
        }
        self.push_line(TailKind::Marker, text.to_owned());
    }
}

/// Everything `aftman watch` knows about one run.
#[derive(Debug, Clone, PartialEq)]
pub struct WatchState {
    pub run_id: Option<RunId>,
    pub job_excerpt: String,
    pub repo: String,
    /// The configured worker backend (from the header or the first worker event).
    pub worker_kind: String,
    /// The decision model named at routing.
    pub model: String,
    pub activated: Vec<String>,
    /// How the run was launched (`RUN_CREATED.launch`): permissions, sandbox, steering,
    /// worker mode, supervisor. A description, never an input to a decision.
    pub launch: Vec<(String, String)>,
    pub status: Option<FactoryStatus>,
    /// The status as the stream spelled it, for a status this build does not know.
    pub status_text: String,
    pub finished: Option<Finish>,
    pub first_at: Option<OffsetDateTime>,
    pub last_at: Option<OffsetDateTime>,
    /// The highest `seq` applied.
    pub last_seq: u64,
    pub counts: Counts,
    pub timeline: VecDeque<Entry>,
    pub assessment: Option<Assessment>,
    /// An assessment was requested and has not completed.
    pub assessing: bool,
    pub tail: Tail,
    pub workers: VecDeque<WorkerInfo>,
    pub escalations: VecDeque<EscalationView>,
    pub trouble: Option<Trouble>,
    /// Proposals seen since the last selection, waiting to be attached to it.
    pending_proposals: Vec<Proposal>,
    pending_omitted: usize,
    last_assessment_seq: Option<u64>,
}

impl Default for WatchState {
    fn default() -> Self {
        Self::new()
    }
}

fn s<'a>(payload: &'a Value, key: &str) -> Option<&'a str> {
    payload.get(key).and_then(Value::as_str)
}

fn label(payload: &Value, key: &str) -> String {
    s(payload, key).map_or_else(String::new, |v| clean_field(v, MAX_LABEL_CHARS))
}

fn field(payload: &Value, key: &str) -> String {
    s(payload, key).map_or_else(String::new, |v| clean_field(v, MAX_FIELD_CHARS))
}

fn parse_status(text: &str) -> Option<FactoryStatus> {
    serde_json::from_value(Value::String(text.to_owned())).ok()
}

/// Keep `items` within `max`, oldest first.
fn push_bounded<T>(items: &mut VecDeque<T>, item: T, max: usize) {
    items.push_back(item);
    while items.len() > max {
        items.pop_front();
    }
}

impl WatchState {
    #[must_use]
    pub fn new() -> Self {
        Self {
            run_id: None,
            job_excerpt: String::new(),
            repo: String::new(),
            worker_kind: String::new(),
            model: String::new(),
            activated: Vec::new(),
            launch: Vec::new(),
            status: None,
            status_text: String::new(),
            finished: None,
            first_at: None,
            last_at: None,
            last_seq: 0,
            counts: Counts::default(),
            timeline: VecDeque::new(),
            assessment: None,
            assessing: false,
            tail: Tail::default(),
            workers: VecDeque::new(),
            escalations: VecDeque::new(),
            trouble: None,
            pending_proposals: Vec::new(),
            pending_omitted: 0,
            last_assessment_seq: None,
        }
    }

    /// Fill in what the event stream does not carry (the job text, the repo, the
    /// worker backend) from the run's stored header. Optional: the screen works
    /// without it.
    pub fn set_header(&mut self, run: &RunState) {
        self.run_id = Some(run.header.id);
        self.job_excerpt = clean_field(&run.header.job.prompt, MAX_FIELD_CHARS);
        self.repo = clean_field(&run.header.job.repo.display().to_string(), MAX_FIELD_CHARS);
        self.worker_kind = clean_field(run.header.config.worker.as_str(), MAX_LABEL_CHARS);
    }

    /// A launch entry by key.
    #[must_use]
    pub fn launch_value(&self, key: &str) -> Option<&str> {
        self.launch
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// Whether the worker runs with its approval prompts suppressed.
    #[must_use]
    pub fn auto_approve(&self) -> bool {
        self.launch_value("permissions") == Some("auto-approve")
    }

    /// The launch posture in a few words: `permissions sandboxed, steering available`.
    #[must_use]
    pub fn posture(&self) -> String {
        let mut parts = Vec::new();
        if let Some(p) = self.launch_value("permissions") {
            parts.push(format!("permissions {p}"));
        }
        if self.launch_value("external_sandbox") == Some("true") {
            parts.push("external sandbox".to_owned());
        }
        if let Some(st) = self.launch_value("steering") {
            parts.push(format!("steering {st}"));
        }
        parts.join(", ")
    }

    /// Whether the run has ended.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.finished.is_some()
    }

    /// Whether the run is waiting for a person.
    #[must_use]
    pub fn needs_human(&self) -> bool {
        !self.is_finished() && self.status == Some(FactoryStatus::NeedsHuman)
    }

    /// Escalations nobody has answered, oldest first.
    pub fn open_escalations(&self) -> impl Iterator<Item = &EscalationView> {
        self.escalations.iter().filter(|e| e.answered.is_none())
    }

    /// The worker that is running now, if any.
    #[must_use]
    pub fn active_worker(&self) -> Option<&WorkerInfo> {
        self.workers
            .iter()
            .rev()
            .find(|w| w.phase == WorkerPhase::Running)
    }

    /// The most recent decision.
    #[must_use]
    pub fn last_decision(&self) -> Option<&Decision> {
        self.timeline.iter().rev().find_map(|e| match e {
            Entry::Decision(d) => Some(d),
            Entry::Note(_) => None,
        })
    }

    /// The status to show: the final one when the run ended.
    #[must_use]
    pub fn status_label(&self) -> String {
        if let Some(finish) = &self.finished {
            return finish.status.clone();
        }
        match self.status {
            Some(status) => status.as_str().to_owned(),
            None if self.status_text.is_empty() => "waiting".to_owned(),
            None => self.status_text.clone(),
        }
    }

    /// How long the run has been going: up to the last event once it ended, up to `now`
    /// while it is live. `None` before the first event.
    #[must_use]
    pub fn elapsed(&self, now: OffsetDateTime) -> Option<std::time::Duration> {
        let start = self.first_at?;
        let end = if self.is_finished() {
            self.last_at?
        } else {
            now.max(self.last_at?)
        };
        let seconds = (end - start).whole_seconds().max(0);
        Some(std::time::Duration::from_secs(
            u64::try_from(seconds).unwrap_or(0),
        ))
    }

    /// Apply a sequence of events.
    pub fn apply_all<'a>(&mut self, events: impl IntoIterator<Item = &'a EventEnvelope>) {
        for event in events {
            self.apply(event);
        }
    }

    /// Fold one event in. An event at or below `last_seq` has been seen and is ignored.
    pub fn apply(&mut self, event: &EventEnvelope) {
        let seq = event.seq.get();
        if seq <= self.last_seq {
            return;
        }
        self.last_seq = seq;
        self.counts.events += 1;
        self.run_id.get_or_insert(event.run_id);
        self.first_at.get_or_insert(event.at);
        self.last_at = Some(event.at);
        let p = &event.payload;

        match event.kind {
            EventKind::RunCreated => {
                if self.repo.is_empty() {
                    self.repo = field(p, "repo");
                }
                if let Some(launch) = p.get("launch").and_then(Value::as_object) {
                    self.launch = launch
                        .iter()
                        .take(MAX_LAUNCH)
                        .filter_map(|(k, v)| {
                            Some((
                                clean_field(k, MAX_LABEL_CHARS),
                                clean_field(v.as_str()?, MAX_LABEL_CHARS),
                            ))
                        })
                        .collect();
                }
                self.set_status_text("queued");
            }
            EventKind::JobRouted => self.on_routed(p),
            EventKind::StateChanged => {
                if let Some(to) = s(p, "to") {
                    self.set_status_text(to);
                    let from = label(p, "from");
                    // A wait for a human is the screen's loudest state: it is also a
                    // timeline row, so the history shows when it began.
                    if self.status == Some(FactoryStatus::NeedsHuman) {
                        self.note(event, Level::Human, format!("needs a human (from {from})"));
                    } else {
                        let to = label(p, "to");
                        self.note(event, Level::Info, format!("{from} -> {to}"));
                    }
                }
            }
            EventKind::WorkerStarted => self.on_worker_started(event),
            EventKind::WorkerOutput => self.on_output(p),
            EventKind::WorkerCompleted => self.on_worker_completed(event),
            EventKind::WorkerTerminated => {
                self.set_worker_phase(s(p, "worker_id"), WorkerPhase::Terminated);
                let cause = label(p, "cause");
                let reason = label(p, "reason");
                self.note(
                    event,
                    Level::Info,
                    format!("worker terminated: {cause} {reason}")
                        .trim()
                        .to_owned(),
                );
            }
            EventKind::WorkerSteered => self.on_steered(event),
            EventKind::EvidenceCollected => {
                let provider = label(p, "provider");
                let summary = field(p, "summary");
                let ok = p.get("success").and_then(Value::as_bool).unwrap_or(false);
                let level = if ok { Level::Info } else { Level::Warn };
                self.note(event, level, format!("evidence {provider}: {summary}"));
            }
            EventKind::AssessmentRequested => self.assessing = true,
            EventKind::AssessmentCompleted => self.on_assessment(event),
            EventKind::AssessmentSkippedUnchanged => {
                self.counts.skipped = self.counts.skipped.saturating_add(1);
            }
            EventKind::DirectiveProposed => self.on_proposed(p),
            EventKind::DirectiveSelected => self.on_selected(event),
            EventKind::EscalationRaised => self.on_escalation_raised(event),
            EventKind::EscalationAnswered => self.on_escalation_answered(event),
            EventKind::ModelError => self.on_trouble(event, "model_error", Level::Error),
            EventKind::RateLimited => self.on_trouble(event, "rate_limited", Level::Warn),
            EventKind::Error => self.on_trouble(event, "error", Level::Error),
            EventKind::BudgetExceeded => {
                let max = p.get("max_assessments").and_then(Value::as_u64);
                let message = max.map_or_else(
                    || "assessment budget exhausted".to_owned(),
                    |m| format!("assessment budget of {m} exhausted"),
                );
                self.trouble = Some(Trouble {
                    seq,
                    at: event.at,
                    kind: "budget_exceeded",
                    code: "budget_exceeded".to_owned(),
                    message: message.clone(),
                });
                self.note(event, Level::Warn, message);
            }
            EventKind::Notification => {
                let code = label(p, "code");
                let message = field(p, "message");
                self.note(event, Level::Info, format!("{code}: {message}"));
            }
            EventKind::RunFinished => {
                let status = s(p, "status").map_or_else(
                    || "finished".to_owned(),
                    |v| clean_field(v, MAX_LABEL_CHARS),
                );
                let reason = s(p, "reason")
                    .map(|v| clean_field(v, MAX_FIELD_CHARS))
                    .filter(|v| !v.is_empty());
                self.status = parse_status(&status);
                self.status_text.clone_from(&status);
                self.assessing = false;
                self.note(
                    event,
                    if status == "finished" {
                        Level::Info
                    } else {
                        Level::Warn
                    },
                    match &reason {
                        Some(reason) => format!("run {status}: {reason}"),
                        None => format!("run {status}"),
                    },
                );
                self.finished = Some(Finish { status, reason });
            }
            // Kinds this view has no use for yet (the ensemble ones arrive with M5).
            // They are counted above and otherwise ignored.
            // (and any kind added later: see the module documentation).
            _ => {}
        }
    }

    fn set_status_text(&mut self, text: &str) {
        self.status = parse_status(text);
        self.status_text = clean_field(text, MAX_LABEL_CHARS);
    }

    fn note(&mut self, event: &EventEnvelope, level: Level, text: String) {
        push_bounded(
            &mut self.timeline,
            Entry::Note(Note {
                seq: event.seq.get(),
                at: event.at,
                level,
                text: truncate_chars(&text, MAX_FIELD_CHARS),
            }),
            MAX_TIMELINE_ENTRIES,
        );
    }

    fn on_routed(&mut self, p: &Value) {
        self.activated = p
            .get("activated")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .take(MAX_ACTIVATED)
                    .map(|v| clean_field(v, MAX_LABEL_CHARS))
                    .collect()
            })
            .unwrap_or_default();
        self.model = label(p, "model");
    }

    fn on_worker_started(&mut self, event: &EventEnvelope) {
        let p = &event.payload;
        let kind = label(p, "kind");
        if self.worker_kind.is_empty() {
            self.worker_kind.clone_from(&kind);
        }
        let task = label(p, "task");
        self.counts.workers = self.counts.workers.saturating_add(1);
        if task.eq_ignore_ascii_case("code") {
            self.counts.attempts = self.counts.attempts.saturating_add(1);
        } else if !task.is_empty() {
            self.counts.verifiers = self.counts.verifiers.saturating_add(1);
        }
        let supports_steering = p
            .get("supports_steering")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let mission = field(p, "mission_excerpt");
        push_bounded(
            &mut self.workers,
            WorkerInfo {
                id: label(p, "worker_id"),
                kind: kind.clone(),
                task: task.clone(),
                supports_steering,
                phase: WorkerPhase::Running,
            },
            MAX_WORKERS,
        );
        self.note(
            event,
            Level::Info,
            format!("worker started ({kind} {task}): {mission}"),
        );
    }

    fn on_output(&mut self, p: &Value) {
        if let Some(id) = s(p, "worker_id") {
            let id = clean_field(id, MAX_LABEL_CHARS);
            if self.tail.worker.as_deref() != Some(id.as_str()) {
                let short: String = id.chars().take(8).collect();
                self.tail.worker = Some(id);
                self.tail.marker(&format!("-- worker {short} --"));
            }
        }
        if let Some(text) = s(p, "text") {
            self.tail.push_text(text);
        }
        if p.get("truncated").and_then(Value::as_bool) == Some(true) {
            self.tail.marker("-- output truncated by the daemon --");
        }
        if p.get("exit").and_then(Value::as_bool) == Some(true) && !self.tail.partial.is_empty() {
            self.tail.finish_line();
        }
    }

    fn set_worker_phase(&mut self, id: Option<&str>, phase: WorkerPhase) {
        let id = id.map(|v| clean_field(v, MAX_LABEL_CHARS));
        if let Some(worker) = self
            .workers
            .iter_mut()
            .rev()
            .find(|w| id.as_deref().is_none_or(|id| w.id == id))
        {
            worker.phase = phase;
        }
    }

    fn on_worker_completed(&mut self, event: &EventEnvelope) {
        let p = &event.payload;
        let success = p.get("success").and_then(Value::as_bool).unwrap_or(false);
        self.set_worker_phase(s(p, "worker_id"), WorkerPhase::Completed { success });
        let outcome = label(p, "outcome");
        self.note(
            event,
            if success { Level::Info } else { Level::Warn },
            format!("worker completed: {outcome}"),
        );
    }

    fn on_steered(&mut self, event: &EventEnvelope) {
        let p = &event.payload;
        let accepted = p.get("accepted").and_then(Value::as_bool).unwrap_or(false);
        if accepted {
            self.counts.steers = self.counts.steers.saturating_add(1);
        }
        let source = label(p, "source");
        let outcome = label(p, "outcome");
        let excerpt = field(p, "excerpt");
        let mut text = format!("steer ({source}): {outcome}");
        if !excerpt.is_empty() {
            text.push_str(": ");
            text.push_str(&excerpt);
        }
        self.note(
            event,
            if accepted { Level::Info } else { Level::Warn },
            text,
        );
    }

    fn on_assessment(&mut self, event: &EventEnvelope) {
        let p = &event.payload;
        self.assessing = false;
        let seq = p.get("assessment_seq").and_then(Value::as_u64);
        let model = label(p, "model");
        if model == "budget_exhausted" {
            // The runtime emits one of these per cycle once the budget is spent, with
            // no model call and without advancing `assessment_seq`.
            self.counts.budget_cycles = self.counts.budget_cycles.saturating_add(1);
        } else {
            self.counts.assessments = match seq {
                Some(seq) => self
                    .counts
                    .assessments
                    .max(u32::try_from(seq).unwrap_or(u32::MAX)),
                None => self.counts.assessments.saturating_add(1),
            };
        }
        self.last_assessment_seq = seq;
        let all: Vec<&Value> = p
            .get("checks")
            .and_then(Value::as_array)
            .map(|items| items.iter().collect())
            .unwrap_or_default();
        let checks: Vec<CheckView> = all
            .iter()
            .take(MAX_CHECKS)
            .map(|c| {
                let flag = |key: &str| c.get(key).and_then(Value::as_bool).unwrap_or(false);
                CheckView {
                    key: label(c, "key"),
                    value: c.get("jev_p").and_then(Value::as_f64),
                    threshold: c.get("threshold").and_then(Value::as_f64),
                    decisive: flag("decisive"),
                    contested: flag("contested"),
                    permitted: flag("permitted"),
                    unresolved: flag("unresolved"),
                }
            })
            .collect();
        if checks.is_empty() {
            // A cycle that produced no answers (the model failed, the budget is spent)
            // does not replace the last real assessment with an empty one; the trouble
            // line says why.
            return;
        }
        // A real answer after a model error or a rate limit: that trouble is over.
        if self
            .trouble
            .as_ref()
            .is_some_and(|t| matches!(t.kind, "model_error" | "rate_limited"))
        {
            self.trouble = None;
        }
        self.assessment = Some(Assessment {
            seq: seq.unwrap_or_else(|| u64::from(self.counts.assessments)),
            at: event.at,
            model,
            input_tokens: p.get("input_tokens").and_then(Value::as_u64).unwrap_or(0),
            output_tokens: p.get("output_tokens").and_then(Value::as_u64).unwrap_or(0),
            checks_omitted: all.len().saturating_sub(checks.len()),
            checks,
        });
    }

    fn on_proposed(&mut self, p: &Value) {
        if self.pending_proposals.len() >= MAX_PROPOSALS {
            self.pending_omitted += 1;
            return;
        }
        self.pending_proposals.push(Proposal {
            responsibility: label(p, "responsibility"),
            rule: label(p, "rule"),
            directive: label(p, "directive"),
            priority: p.get("priority").and_then(Value::as_i64),
        });
    }

    fn on_selected(&mut self, event: &EventEnvelope) {
        let p = &event.payload;
        let invariants = p
            .get("invariants")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .take(MAX_INVARIANTS)
                    .map(|v| clean_field(v, MAX_LABEL_CHARS))
                    .collect()
            })
            .unwrap_or_default();
        let suppressed = p
            .get("suppressed")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .take(MAX_SUPPRESSIONS)
                    .map(|item| Suppression {
                        rule: label(item, "rule"),
                        reason: label(item, "reason"),
                    })
                    .collect()
            })
            .unwrap_or_default();
        let decision = Decision {
            seq: event.seq.get(),
            at: event.at,
            directive: label(p, "directive"),
            reason: field(p, "reason"),
            invariants,
            suppressed,
            proposals: std::mem::take(&mut self.pending_proposals),
            proposals_omitted: std::mem::take(&mut self.pending_omitted),
            assessment_seq: self.last_assessment_seq,
        };
        push_bounded(
            &mut self.timeline,
            Entry::Decision(decision),
            MAX_TIMELINE_ENTRIES,
        );
    }

    fn on_escalation_raised(&mut self, event: &EventEnvelope) {
        let p = &event.payload;
        let view = EscalationView {
            id: label(p, "escalation_id"),
            kind: label(p, "kind"),
            reason: field(p, "reason"),
            ask: field(p, "ask"),
            raised_seq: event.seq.get(),
            answered: None,
        };
        let text = format!("escalation {}: {}", view.id, view.ask);
        self.escalations.push_back(view);
        while self.escalations.len() > MAX_ESCALATIONS {
            // Answered ones first; only if every one is open does an open one go.
            let index = self
                .escalations
                .iter()
                .position(|e| e.answered.is_some())
                .unwrap_or(0);
            self.escalations.remove(index);
        }
        self.note(event, Level::Human, text);
    }

    fn on_escalation_answered(&mut self, event: &EventEnvelope) {
        let p = &event.payload;
        let id = label(p, "escalation_id");
        let decision = p
            .get("decision")
            .and_then(|d| d.get("decision"))
            .and_then(Value::as_str)
            .map_or_else(
                || "answered".to_owned(),
                |d| clean_field(d, MAX_LABEL_CHARS),
            );
        let surface = label(p, "surface");
        let note = field(p, "note");
        if let Some(escalation) = self.escalations.iter_mut().find(|e| e.id == id) {
            escalation.answered = Some(decision.clone());
        }
        let mut text = format!("answered {id}: {decision}");
        if !surface.is_empty() {
            text.push_str(&format!(" (via {surface})"));
        }
        if !note.is_empty() {
            text.push_str(&format!(": {note}"));
        }
        self.note(event, Level::Human, text);
    }

    fn on_trouble(&mut self, event: &EventEnvelope, kind: &'static str, level: Level) {
        let p = &event.payload;
        let code = label(p, "code");
        let message = field(p, "message");
        self.trouble = Some(Trouble {
            seq: event.seq.get(),
            at: event.at,
            kind,
            code: code.clone(),
            message: message.clone(),
        });
        self.note(event, level, format!("{kind} {code}: {message}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aftman_core::ids::EventSeq;
    use serde_json::json;
    use time::macros::datetime;

    const RUN: &str = "0193a000-0000-7000-8000-000000000001";

    fn run_id() -> RunId {
        RUN.parse().expect("a valid run id")
    }

    fn at(second: i64) -> OffsetDateTime {
        datetime!(2026-01-01 00:00:00 UTC) + time::Duration::seconds(second)
    }

    fn ev(seq: u64, kind: EventKind, payload: Value) -> EventEnvelope {
        EventEnvelope::new(EventSeq(seq), run_id(), at(seq as i64), kind, payload)
    }

    /// A plausible stream, in the shapes the runtime emits.
    fn stream() -> Vec<EventEnvelope> {
        vec![
            ev(1, EventKind::RunCreated, json!({"repo": "/work/repo"})),
            ev(
                2,
                EventKind::JobRouted,
                json!({"activated": ["core.completion"], "degraded": false, "conditional": [], "model": "sim-1"}),
            ),
            ev(
                3,
                EventKind::StateChanged,
                json!({"from": "routing", "to": "running"}),
            ),
            ev(
                4,
                EventKind::WorkerStarted,
                json!({"worker_id": "w-123456789", "kind": "simulated", "task": "Code", "mission_excerpt": "add retries", "supports_steering": true}),
            ),
            ev(
                5,
                EventKind::WorkerOutput,
                json!({"worker_id": "w-123456789", "text": "editing src/a.rs\nrunning te", "bytes": 27, "truncated": false, "exit": false}),
            ),
            ev(
                6,
                EventKind::AssessmentRequested,
                json!({"questions": ["k"]}),
            ),
            ev(
                7,
                EventKind::AssessmentCompleted,
                json!({"assessment_seq": 1, "model": "sim-1", "input_tokens": 10, "output_tokens": 2,
                "checks": [
                  {"key": "core.completion.tests_pass", "jev_p": 0.82, "threshold": 0.7, "decisive": true, "contested": false, "permitted": true, "unresolved": false},
                  {"key": "core.completion.no_todo", "jev_p": 0.4, "threshold": 0.5, "decisive": false, "contested": true, "permitted": false, "unresolved": true}
                ]}),
            ),
            ev(8, EventKind::EnsembleResolved, json!({"enabled": false})),
            ev(
                9,
                EventKind::DirectiveProposed,
                json!({"responsibility": "core.completion", "rule": "core.completion.finish", "directive": "finish", "priority": 5}),
            ),
            ev(
                10,
                EventKind::DirectiveSelected,
                json!({"directive": "continue", "reason": "tests not yet green", "invariants": ["MinWorkTime"],
                       "suppressed": [{"rule": "core.completion.finish", "reason": "not_permitted"}]}),
            ),
            ev(
                11,
                EventKind::AssessmentSkippedUnchanged,
                json!({"digest": "abc"}),
            ),
            ev(
                12,
                EventKind::WorkerOutput,
                json!({"worker_id": "w-123456789", "text": "st result: ok\n", "bytes": 14, "truncated": false, "exit": true}),
            ),
            ev(
                13,
                EventKind::WorkerCompleted,
                json!({"worker_id": "w-123456789", "outcome": "Completed", "success": true}),
            ),
            ev(
                14,
                EventKind::RunFinished,
                json!({"status": "finished", "assessments": 1, "assessments_skipped": 1, "workers_run": 1}),
            ),
        ]
    }

    #[test]
    fn the_launch_posture_is_read_from_run_created_and_sanitised() {
        let mut state = WatchState::new();
        state.apply(&ev(
            1,
            EventKind::RunCreated,
            json!({"repo": "/r", "launch": {"permissions": "auto-approve", "external_sandbox": "true", "steering": "avail\u{1b}[31mable", "n": 5}}),
        ));
        assert!(state.auto_approve());
        assert_eq!(
            state.posture(),
            "permissions auto-approve, external sandbox, steering available"
        );
        let plain = state_with_launch(json!({"permissions": "sandboxed"}));
        assert!(!plain.auto_approve());
        assert_eq!(plain.posture(), "permissions sandboxed");
        // No launch map (an older daemon): nothing to say, nothing wrong.
        assert_eq!(WatchState::new().posture(), "");
    }

    fn state_with_launch(launch: Value) -> WatchState {
        let mut state = WatchState::new();
        state.apply(&ev(1, EventKind::RunCreated, json!({"launch": launch})));
        state
    }

    #[test]
    fn a_stream_folds_into_what_the_screen_shows() {
        let mut state = WatchState::new();
        state.apply_all(&stream());

        assert_eq!(state.run_id, Some(run_id()));
        assert_eq!(state.repo, "/work/repo");
        assert_eq!(state.worker_kind, "simulated");
        assert_eq!(state.model, "sim-1");
        assert_eq!(state.activated, vec!["core.completion"]);
        assert_eq!(state.status_label(), "finished");
        assert!(state.is_finished());
        assert!(!state.needs_human());
        assert_eq!(state.last_seq, 14);
        assert_eq!(
            state.counts,
            Counts {
                assessments: 1,
                skipped: 1,
                workers: 1,
                attempts: 1,
                verifiers: 0,
                steers: 0,
                events: 14,
                budget_cycles: 0,
            }
        );

        let decision = state.last_decision().expect("a decision");
        assert_eq!(decision.directive, "continue");
        assert_eq!(decision.reason, "tests not yet green");
        assert_eq!(decision.invariants, vec!["MinWorkTime"]);
        assert_eq!(decision.suppressed[0].reason, "not_permitted");
        assert_eq!(decision.proposals.len(), 1);
        assert_eq!(decision.proposals[0].priority, Some(5));
        assert_eq!(decision.assessment_seq, Some(1));

        let assessment = state.assessment.as_ref().expect("an assessment");
        assert_eq!(assessment.checks.len(), 2);
        let first = &assessment.checks[0];
        assert_eq!(first.key, "core.completion.tests_pass");
        assert!((first.margin().expect("margin") - 0.12).abs() < 1e-9);
        assert!(first.decisive && first.permitted && !first.contested);
        let second = &assessment.checks[1];
        assert!(second.contested && second.unresolved && !second.permitted);
        assert!(second.margin().expect("margin") < 0.0);
        assert_eq!((assessment.input_tokens, assessment.output_tokens), (10, 2));

        // Output split mid-line across events is joined.
        let tail: Vec<&str> = state.tail.lines().map(|(_, t)| t).collect();
        assert_eq!(
            tail,
            vec![
                "-- worker w-123456 --",
                "editing src/a.rs",
                "running test result: ok"
            ]
        );
        assert_eq!(
            state.workers[0].phase,
            WorkerPhase::Completed { success: true }
        );
        assert_eq!(
            state.elapsed(at(1_000)),
            Some(std::time::Duration::from_secs(13))
        );
    }

    #[test]
    fn replay_one_by_one_in_a_batch_or_in_halves_is_the_same_state() {
        let events = stream();
        let mut one_by_one = WatchState::new();
        for event in &events {
            one_by_one.apply(event);
        }
        let mut batch = WatchState::new();
        batch.apply_all(&events);
        let mut halves = WatchState::new();
        halves.apply_all(&events[..7]);
        halves.apply_all(&events[7..]);
        assert_eq!(one_by_one, batch);
        assert_eq!(one_by_one, halves);
    }

    #[test]
    fn a_replayed_event_is_ignored() {
        let events = stream();
        let mut state = WatchState::new();
        state.apply_all(&events);
        let before = state.clone();
        state.apply_all(&events);
        state.apply(&events[3]);
        assert_eq!(state, before);
    }

    #[test]
    fn an_escalation_is_raised_shown_and_answered() {
        let mut state = WatchState::new();
        state.apply(&ev(
            1,
            EventKind::StateChanged,
            json!({"from": "assessing", "to": "needs_human"}),
        ));
        assert!(state.needs_human());
        state.apply(&ev(
            2,
            EventKind::EscalationRaised,
            json!({"escalation_id": "e-1", "kind": "check", "reason": "unsure", "ask": "Is the migration safe?"}),
        ));
        let open: Vec<_> = state.open_escalations().collect();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].id, "e-1");
        assert_eq!(open[0].ask, "Is the migration safe?");
        assert_eq!(open[0].kind, "check");

        state.apply(&ev(
            3,
            EventKind::EscalationAnswered,
            json!({"escalation_id": "e-1", "surface": "tui", "decision": {"decision": "guidance", "text": "yes"}, "note": ""}),
        ));
        assert_eq!(state.open_escalations().count(), 0);
        assert_eq!(state.escalations[0].answered.as_deref(), Some("guidance"));
        state.apply(&ev(
            4,
            EventKind::StateChanged,
            json!({"from": "needs_human", "to": "running"}),
        ));
        assert!(!state.needs_human());
    }

    #[test]
    fn model_errors_rate_limits_budget_and_termination_are_recorded() {
        let mut state = WatchState::new();
        state.apply(&ev(
            1,
            EventKind::ModelError,
            json!({"code": "bad_json", "message": "no", "retryable": true}),
        ));
        assert_eq!(state.trouble.as_ref().map(|t| t.kind), Some("model_error"));
        state.apply(&ev(
            2,
            EventKind::RateLimited,
            json!({"code": "rate_limited", "message": "slow down", "retryable": true}),
        ));
        let trouble = state.trouble.clone().expect("trouble");
        assert_eq!(
            (trouble.kind, trouble.code.as_str()),
            ("rate_limited", "rate_limited")
        );
        state.apply(&ev(
            3,
            EventKind::BudgetExceeded,
            json!({"max_assessments": 40}),
        ));
        assert!(
            state
                .trouble
                .as_ref()
                .is_some_and(|t| t.message.contains("40"))
        );
        state.apply(&ev(
            4,
            EventKind::Error,
            json!({"code": "internal", "message": "oops"}),
        ));
        assert_eq!(state.trouble.as_ref().map(|t| t.kind), Some("error"));

        state.apply(&ev(
            5,
            EventKind::WorkerStarted,
            json!({"worker_id": "w1", "kind": "pi", "task": "Verify", "mission_excerpt": "m"}),
        ));
        assert_eq!(state.counts.verifiers, 1);
        assert!(state.active_worker().is_some());
        state.apply(&ev(
            6,
            EventKind::WorkerTerminated,
            json!({"worker_id": "w1", "cause": "stopped", "reason": "run ended"}),
        ));
        assert!(state.active_worker().is_none());
        assert_eq!(state.workers[0].phase, WorkerPhase::Terminated);

        state.apply(&ev(7, EventKind::WorkerSteered, json!({"source": "human", "worker_id": "w1", "outcome": "Queued", "accepted": true, "excerpt": "left"})));
        state.apply(&ev(
            8,
            EventKind::WorkerSteered,
            json!({"source": "human", "outcome": "Rejected { reason: \"x\" }", "accepted": false}),
        ));
        assert_eq!(state.counts.steers, 1);
        let kinds: Vec<Level> = state
            .timeline
            .iter()
            .filter_map(|e| match e {
                Entry::Note(n) => Some(n.level),
                Entry::Decision(_) => None,
            })
            .collect();
        assert!(kinds.contains(&Level::Error) && kinds.contains(&Level::Warn));
    }

    #[test]
    fn a_cancelled_run_ends_as_cancelled_with_its_reason() {
        let mut state = WatchState::new();
        state.apply(&ev(
            1,
            EventKind::RunFinished,
            json!({"status": "cancelled", "reason": "stopped by operator"}),
        ));
        assert_eq!(state.status_label(), "cancelled");
        assert_eq!(
            state.finished,
            Some(Finish {
                status: "cancelled".into(),
                reason: Some("stopped by operator".into())
            })
        );
        assert_eq!(state.status, Some(FactoryStatus::Cancelled));
    }

    #[test]
    fn payloads_with_missing_wrong_or_extra_fields_are_tolerated() {
        let kinds = [
            EventKind::RunCreated,
            EventKind::JobRouted,
            EventKind::WorkerStarted,
            EventKind::WorkerOutput,
            EventKind::WorkerCompleted,
            EventKind::WorkerSteered,
            EventKind::WorkerTerminated,
            EventKind::EvidenceCollected,
            EventKind::AssessmentRequested,
            EventKind::AssessmentCompleted,
            EventKind::AssessmentSkippedUnchanged,
            EventKind::DirectiveProposed,
            EventKind::DirectiveSelected,
            EventKind::EnsembleTriggered,
            EventKind::OpinionRequested,
            EventKind::OpinionRecorded,
            EventKind::EnsembleResolved,
            EventKind::StateChanged,
            EventKind::EscalationRaised,
            EventKind::EscalationAnswered,
            EventKind::ModelError,
            EventKind::RateLimited,
            EventKind::BudgetExceeded,
            EventKind::RunFinished,
            EventKind::Error,
            EventKind::Notification,
        ];
        let payloads = [
            json!({}),
            json!(null),
            json!("a string"),
            json!([1, 2, 3]),
            json!(42),
            json!({"text": 5, "worker_id": [], "status": {"a": 1}, "checks": "none", "to": 7,
                   "decision": "x", "escalation_id": null, "invariants": [1, {"a": 1}],
                   "suppressed": ["x", 3, {"rule": 4}], "unknown_future_field": {"deep": [1, 2]}}),
            json!({"checks": [1, "x", null, {"jev_p": "high", "threshold": null, "key": 5}]}),
            json!({"text": "\u{1b}]0;title\u{7}fine", "worker_id": "\u{1b}[31mw", "truncated": "yes"}),
        ];
        let mut state = WatchState::new();
        let mut seq = 0;
        for _ in 0..3 {
            for kind in kinds {
                for payload in &payloads {
                    seq += 1;
                    state.apply(&ev(seq, kind, payload.clone()));
                }
            }
        }
        assert_eq!(state.last_seq, seq);
        // Nothing hostile survived into any string the state holds.
        let dump = format!("{state:?}");
        assert!(!dump.contains("\\u{1b}"), "an escape reached the state");
    }

    #[test]
    fn untrusted_text_is_sanitised_on_the_way_in() {
        let mut state = WatchState::new();
        state.apply(&ev(
            1,
            EventKind::WorkerOutput,
            json!({"worker_id": "w", "text": "ok\u{1b}[2J\u{1b}]52;c;AAAA\u{7}\rEVIL\u{202e}\n"}),
        ));
        state.apply(&ev(
            2,
            EventKind::EscalationRaised,
            json!({"escalation_id": "e\u{1b}[1m", "kind": "check", "reason": "r\u{7}", "ask": "do\nit\u{9b}31m now"}),
        ));
        let tail: Vec<&str> = state.tail.lines().map(|(_, t)| t).collect();
        assert_eq!(tail, vec!["-- worker w --", "ok", "EVIL"]);
        let escalation = &state.escalations[0];
        assert_eq!(escalation.id, "e");
        assert_eq!(escalation.ask, "do it now");
        assert_eq!(escalation.reason, "r");
    }

    #[test]
    fn the_timeline_evicts_oldest_first() {
        let mut state = WatchState::new();
        for seq in 1..=(MAX_TIMELINE_ENTRIES as u64 + 50) {
            state.apply(&ev(
                seq,
                EventKind::DirectiveSelected,
                json!({"directive": "continue", "reason": format!("r{seq}")}),
            ));
        }
        assert_eq!(state.timeline.len(), MAX_TIMELINE_ENTRIES);
        assert_eq!(state.timeline.front().map(Entry::seq), Some(51));
        assert_eq!(
            state.timeline.back().map(Entry::seq),
            Some(MAX_TIMELINE_ENTRIES as u64 + 50)
        );
    }

    #[test]
    fn the_tail_is_bounded_in_lines_bytes_and_line_length() {
        let mut state = WatchState::new();
        let mut seq = 0;
        for i in 0..(MAX_TAIL_LINES + 300) {
            seq += 1;
            state.apply(&ev(
                seq,
                EventKind::WorkerOutput,
                json!({"worker_id": "w", "text": format!("line {i}\n")}),
            ));
        }
        assert!(state.tail.len() <= MAX_TAIL_LINES + 1);
        assert!(state.tail.evicted() >= 300);
        let last = state.tail.lines().last().map(|(_, t)| t.to_owned());
        assert_eq!(last, Some(format!("line {}", MAX_TAIL_LINES + 299)));

        // Bytes: long lines hit the byte cap long before the line cap.
        let mut state = WatchState::new();
        for _ in 0..2_000 {
            seq += 1;
            state.apply(&ev(
                seq,
                EventKind::WorkerOutput,
                json!({"worker_id": "w", "text": format!("{}\n", "y".repeat(400))}),
            ));
        }
        assert!(state.tail.bytes() <= MAX_TAIL_BYTES);

        // Line length: one enormous line is clipped, even when it arrives in pieces.
        let mut state = WatchState::new();
        for _ in 0..20 {
            seq += 1;
            state.apply(&ev(
                seq,
                EventKind::WorkerOutput,
                json!({"worker_id": "w", "text": "z".repeat(100_000)}),
            ));
        }
        let longest = state
            .tail
            .lines()
            .map(|(_, l)| l.chars().count())
            .max()
            .unwrap_or(0);
        assert!(longest <= MAX_LINE_CHARS, "{longest}");
        assert!(
            state
                .tail
                .lines()
                .last()
                .is_some_and(|(_, l)| l.ends_with('…'))
        );
    }

    #[test]
    fn proposals_checks_escalations_and_workers_are_bounded() {
        let mut state = WatchState::new();
        let mut seq = 0;
        for i in 0..(MAX_PROPOSALS + 10) {
            seq += 1;
            state.apply(&ev(
                seq,
                EventKind::DirectiveProposed,
                json!({"rule": format!("r{i}")}),
            ));
        }
        seq += 1;
        state.apply(&ev(
            seq,
            EventKind::DirectiveSelected,
            json!({"directive": "d", "suppressed": (0..100).map(|i| json!({"rule": format!("s{i}"), "reason": "x"})).collect::<Vec<_>>(),
                   "invariants": (0..100).map(|i| format!("i{i}")).collect::<Vec<_>>()}),
        ));
        let decision = state.last_decision().expect("decision");
        assert_eq!(decision.proposals.len(), MAX_PROPOSALS);
        assert_eq!(decision.proposals_omitted, 10);
        assert_eq!(decision.suppressed.len(), MAX_SUPPRESSIONS);
        assert_eq!(decision.invariants.len(), MAX_INVARIANTS);

        seq += 1;
        state.apply(&ev(
            seq,
            EventKind::AssessmentCompleted,
            json!({"checks": (0..200).map(|i| json!({"key": format!("c{i}"), "jev_p": 0.5, "threshold": 0.5})).collect::<Vec<_>>()}),
        ));
        let assessment = state.assessment.as_ref().expect("assessment");
        assert_eq!(assessment.checks.len(), MAX_CHECKS);
        assert_eq!(assessment.checks_omitted, 200 - MAX_CHECKS);

        for i in 0..(MAX_ESCALATIONS + 10) {
            seq += 1;
            state.apply(&ev(
                seq,
                EventKind::EscalationRaised,
                json!({"escalation_id": format!("e{i}")}),
            ));
        }
        assert_eq!(state.escalations.len(), MAX_ESCALATIONS);
        for i in 0..(MAX_WORKERS + 10) {
            seq += 1;
            state.apply(&ev(
                seq,
                EventKind::WorkerStarted,
                json!({"worker_id": format!("w{i}"), "task": "Code"}),
            ));
        }
        assert_eq!(state.workers.len(), MAX_WORKERS);
        assert!(state.timeline.len() <= MAX_TIMELINE_ENTRIES);
    }

    #[test]
    fn an_unknown_status_string_is_shown_as_it_came() {
        let mut state = WatchState::new();
        state.apply(&ev(
            1,
            EventKind::StateChanged,
            json!({"from": "running", "to": "hibernating"}),
        ));
        assert_eq!(state.status, None);
        assert_eq!(state.status_label(), "hibernating");
        assert!(!state.needs_human());
    }

    #[test]
    fn elapsed_runs_to_now_while_live_and_stops_when_finished() {
        let mut state = WatchState::new();
        assert_eq!(state.elapsed(at(100)), None);
        state.apply(&ev(1, EventKind::RunCreated, json!({})));
        state.apply(&ev(5, EventKind::StateChanged, json!({"to": "running"})));
        assert_eq!(
            state.elapsed(at(60)),
            Some(std::time::Duration::from_secs(59))
        );
        // A clock behind the last event does not go negative.
        assert_eq!(
            state.elapsed(at(0)),
            Some(std::time::Duration::from_secs(4))
        );
        state.apply(&ev(6, EventKind::RunFinished, json!({"status": "failed"})));
        assert_eq!(
            state.elapsed(at(9_999)),
            Some(std::time::Duration::from_secs(5))
        );
    }

    fn sample_header(prompt: &str, repo: &str) -> RunState {
        use aftman_core::job::{Job, JobSource, RunConfig};
        use aftman_core::run::RunHeader;
        RunState::new(
            RunHeader {
                id: run_id(),
                created_at: at(0),
                job: Job {
                    prompt: prompt.to_owned(),
                    repo: std::path::PathBuf::from(repo),
                    source: JobSource::Cli,
                },
                config: RunConfig::default(),
                aftman_version: "0".into(),
                worker_versions: Default::default(),
                granted_key_names: Vec::new(),
            },
            FactoryStatus::Queued,
        )
    }

    #[test]
    fn the_header_text_is_sanitised_like_everything_else() {
        let mut state = WatchState::new();
        state.set_header(&sample_header(
            "fix it\u{1b}]0;pwned\u{7}\nnow\u{202e}",
            "/tmp/re\u{1b}[31mpo\u{7}",
        ));
        assert_eq!(state.job_excerpt, "fix it now");
        assert_eq!(state.repo, "/tmp/repo");
        assert_eq!(state.run_id, Some(run_id()));
    }

    #[test]
    fn markers_are_a_different_kind_from_worker_text_whatever_it_says() {
        let mut state = WatchState::new();
        state.apply(&ev(
            1,
            EventKind::WorkerOutput,
            json!({"worker_id": "w1", "text": "-- output truncated by the daemon --\n-- worker deadbeef --\n", "truncated": true}),
        ));
        let kinds: Vec<(TailKind, &str)> = state.tail.lines().collect();
        assert_eq!(kinds[0], (TailKind::Marker, "-- worker w1 --"));
        assert_eq!(
            kinds[1],
            (TailKind::Worker, "-- output truncated by the daemon --")
        );
        assert_eq!(kinds[2], (TailKind::Worker, "-- worker deadbeef --"));
        assert_eq!(
            kinds[3],
            (TailKind::Marker, "-- output truncated by the daemon --")
        );
    }

    #[test]
    fn tail_indices_stay_valid_as_old_lines_are_evicted() {
        let mut state = WatchState::new();
        for i in 0..(MAX_TAIL_LINES + 10) {
            state.apply(&ev(
                i as u64 + 1,
                EventKind::WorkerOutput,
                json!({"worker_id": "w", "text": format!("line {i}\n")}),
            ));
        }
        let first = state.tail.first_index();
        assert!(first >= 10);
        assert_eq!(state.tail.line_at(first - 1), None);
        assert!(state.tail.line_at(first).is_some());
        let last = state.tail.last_index().expect("last");
        assert_eq!(
            state.tail.line_at(last).map(|(_, t)| t),
            Some(format!("line {}", MAX_TAIL_LINES + 9).as_str())
        );
        assert_eq!(state.tail.line_at(last + 1), None);
    }

    #[test]
    fn the_assessment_counter_follows_the_daemons_sequence_and_budget_cycles_are_separate() {
        let mut state = WatchState::new();
        let check = json!([{"key": "k", "jev_p": 0.5, "threshold": 0.5}]);
        state.apply(&ev(
            1,
            EventKind::AssessmentCompleted,
            json!({"assessment_seq": 1, "model": "m", "checks": check}),
        ));
        state.apply(&ev(
            2,
            EventKind::AssessmentCompleted,
            json!({"assessment_seq": 2, "model": "m", "checks": check}),
        ));
        for seq in 3..8 {
            state.apply(&ev(
                seq,
                EventKind::AssessmentCompleted,
                json!({"assessment_seq": 2, "model": "budget_exhausted", "checks": []}),
            ));
        }
        assert_eq!(state.counts.assessments, 2);
        assert_eq!(state.counts.budget_cycles, 5);
        // The last real assessment is still the one shown.
        assert_eq!(state.assessment.as_ref().map(|a| a.checks.len()), Some(1));
    }

    #[test]
    fn a_model_error_is_cleared_by_the_next_real_assessment_but_budget_stays() {
        let mut state = WatchState::new();
        state.apply(&ev(
            1,
            EventKind::RateLimited,
            json!({"code": "rate_limited", "message": "slow"}),
        ));
        state.apply(&ev(
            2,
            EventKind::AssessmentCompleted,
            json!({"assessment_seq": 1, "model": "unavailable", "checks": []}),
        ));
        assert!(state.trouble.is_some(), "an empty cycle is not recovery");
        state.apply(&ev(
            3,
            EventKind::AssessmentCompleted,
            json!({"assessment_seq": 2, "model": "m", "checks": [{"key": "k"}]}),
        ));
        assert!(state.trouble.is_none());
        state.apply(&ev(
            4,
            EventKind::BudgetExceeded,
            json!({"max_assessments": 3}),
        ));
        state.apply(&ev(
            5,
            EventKind::AssessmentCompleted,
            json!({"assessment_seq": 3, "model": "m", "checks": [{"key": "k"}]}),
        ));
        assert_eq!(
            state.trouble.as_ref().map(|t| t.kind),
            Some("budget_exceeded")
        );
        assert_eq!(state.trouble.as_ref().map(|t| t.at), Some(at(4)));
    }

    #[test]
    fn fifty_thousand_events_fold_quickly_and_stay_bounded() {
        let mut state = WatchState::new();
        let started = std::time::Instant::now();
        for seq in 1..=50_000u64 {
            let (kind, payload) = if seq % 3 == 0 {
                (
                    EventKind::DirectiveSelected,
                    json!({"directive": "continue", "reason": "r"}),
                )
            } else {
                (
                    EventKind::WorkerOutput,
                    json!({"worker_id": "w", "text": "some output line\n"}),
                )
            };
            state.apply(&ev(seq, kind, payload));
        }
        assert_eq!(state.last_seq, 50_000);
        assert!(state.timeline.len() <= MAX_TIMELINE_ENTRIES);
        assert!(state.tail.len() <= MAX_TAIL_LINES + 1);
        assert!(
            started.elapsed() < std::time::Duration::from_secs(20),
            "{:?}",
            started.elapsed()
        );
    }

    #[test]
    fn a_future_event_kind_survives_the_real_decode_path_and_is_counted_quietly() {
        use aftman_daemon::protocol::{ServerMessage, encode_line};
        let known = ev(2, EventKind::RunCreated, json!({"repo": "/r"}));
        let line = encode_line(&ServerMessage::event(known)).expect("encodes");
        assert!(line.contains("\"RUN_CREATED\""), "{line}");
        let future = line.replace("RUN_CREATED", "SOME_FUTURE_KIND");
        let ServerMessage::Event(push) = ServerMessage::decode(future.trim()).expect("decodes")
        else {
            panic!("an event push");
        };
        assert_eq!(push.event.kind, EventKind::Unknown);
        let mut state = WatchState::new();
        state.apply(&ev(
            1,
            EventKind::StateChanged,
            json!({"from": "queued", "to": "running"}),
        ));
        let timeline_before = state.timeline.clone();
        state.apply(&push.event);
        assert_eq!(state.last_seq, 2);
        assert_eq!(state.counts.events, 2);
        assert_eq!(
            state.timeline, timeline_before,
            "an unknown kind shows nothing"
        );
        assert!(state.trouble.is_none());
    }

    #[test]
    fn an_event_kind_the_reducer_does_not_handle_is_counted_and_changes_nothing_else() {
        let mut before = WatchState::new();
        before.apply(&ev(1, EventKind::RunCreated, json!({"repo": "/r"})));
        let mut after = before.clone();
        after.apply(&ev(2, EventKind::OpinionRecorded, json!({"anything": 1})));
        assert_eq!(after.counts.events, 2);
        assert_eq!(after.last_seq, 2);
        assert_eq!(after.timeline, before.timeline);
        assert_eq!(after.status, before.status);
    }
}
