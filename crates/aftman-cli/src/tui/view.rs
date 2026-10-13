//! The pure view: `draw(frame, state, ui)`.
//!
//! No I/O, no clock, no mutation: the same `WatchState` and `UiState` always draw the
//! same screen, which is what lets the tests assert on a `TestBackend` buffer.
//!
//! # Layout
//!
//! ```text
//! +- header: status badge, run, elapsed, counters ----------------+
//! | worker, model, repo (middle-elided), job                      |
//! +- banner: NEEDS A HUMAN + escalation id + ask  (or run ended) -+
//! | 1 Decisions        | 3 Worker output                          |
//! |  (timeline; the    |  (every line behind a "| " gutter;       |
//! |  selected one is   |   long lines wrap, "+ " continues)       |
//! |  expanded: why,    |                                          |
//! |  proposals, ...)   |                                          |
//! +--------------------+                                          |
//! | 2 Assessment       |                                          |
//! |  every check: p,   |                                          |
//! |  threshold, margin |                                          |
//! +- footer: message and key hints (or the input box) ------------+
//! ```
//!
//! Three panes at 110+ columns sit in two columns (timeline over assessment, worker
//! tail on the right); at 50+ columns and 15+ body rows they stack; below that only the
//! focused pane is shown (`Tab` switches). Under 12 columns or 3 rows the screen is one
//! status line. Nothing here can panic at any size: every rectangle is derived with
//! saturating arithmetic and clipped to the frame.
//!
//! `NeedsHuman` is never drawn like `Failed`: a black-on-yellow badge and a banner
//! (the words "NEEDS HUMAN" appear in both, so it survives a monochrome terminal),
//! where `Failed` is white-on-red and carries no banner.

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};

use super::input::{
    CONFIRM_MIN, Choice, Confirm, DECIDE_MIN, Link, Mode, Pane, Purpose, Tone, UiState, hints,
};
use super::sanitize::{clip, elide_middle, hard_wrap, width, wrap};
use super::state::{
    CheckView, Decision, Entry, EscalationView, Level, Note, TailKind, WatchState, WorkerPhase,
};

const MAX_DETAIL_LINES: usize = 24;

fn bold() -> Style {
    Style::new().add_modifier(Modifier::BOLD)
}

fn dim() -> Style {
    Style::new().fg(Color::DarkGray)
}

/// Draw the whole screen.
pub fn draw(frame: &mut Frame, state: &WatchState, ui: &UiState) {
    let area = frame.area();
    if area.width == 0 || area.height == 0 {
        return;
    }
    if area.height < 3 || area.width < 12 {
        draw_tiny(frame, area, state, ui);
        return;
    }

    let typing_box = ui.is_typing() && area.height >= 12;
    let footer_h: u16 = if typing_box { 3 } else { 1 };
    let header_h: u16 = match (area.height, state.auto_approve()) {
        (14.., true) => 3,
        (12.., _) => 2,
        _ => 1,
    };
    let banner_h = banner_height(area, state);
    let body_h = area
        .height
        .saturating_sub(footer_h)
        .saturating_sub(header_h)
        .saturating_sub(banner_h);

    let [header, banner, body, footer] = Layout::vertical([
        Constraint::Length(header_h),
        Constraint::Length(banner_h),
        Constraint::Length(body_h),
        Constraint::Length(footer_h),
    ])
    .areas(area);

    draw_header(frame, header, state, ui);
    if banner_h > 0 {
        draw_banner(frame, banner, state);
    }
    draw_body(frame, body, state, ui);
    if typing_box {
        draw_input_box(frame, footer, ui);
    } else if let Mode::Input { purpose, editor } = &ui.mode {
        // Too short for a box: one line, `title> text`.
        draw_input_line(frame, footer, purpose, editor);
    } else {
        draw_footer(frame, footer, state, ui);
    }

    match &ui.mode {
        Mode::Decide {
            escalation,
            position,
            selected,
        } => draw_decide(
            frame,
            area,
            state,
            &escalation.to_string(),
            *position,
            *selected,
        ),
        Mode::Confirm { what, typed } => draw_confirm(frame, area, what, typed),
        Mode::Help => draw_help(frame, area),
        Mode::Normal | Mode::Input { .. } => {}
    }
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// The badge text and style for the run's status. Distinct for every state that
/// matters: a person must never mistake "needs a human" for "failed".
fn badge(state: &WatchState) -> (String, Style) {
    let label = state.status_label();
    let finished_as = state.finished.as_ref().map(|f| f.status.as_str());
    let status = finished_as.unwrap_or(label.as_str());
    let shown = status.replace('_', " ").to_uppercase();
    let style = match status {
        "needs_human" => Style::new()
            .fg(Color::Black)
            .bg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
        "failed" => Style::new()
            .fg(Color::White)
            .bg(Color::Red)
            .add_modifier(Modifier::BOLD),
        "cancelled" | "stopped" => Style::new()
            .fg(Color::White)
            .bg(Color::DarkGray)
            .add_modifier(Modifier::BOLD),
        "finished" => Style::new()
            .fg(Color::Black)
            .bg(Color::Green)
            .add_modifier(Modifier::BOLD),
        _ => Style::new()
            .fg(Color::Black)
            .bg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    };
    (format!(" {shown} "), style)
}

fn clock(duration: std::time::Duration) -> String {
    let total = duration.as_secs();
    format!(
        "{}:{:02}:{:02}",
        total / 3600,
        (total / 60) % 60,
        total % 60
    )
}

fn short(id: &str) -> String {
    id.chars().take(8).collect()
}

fn draw_tiny(frame: &mut Frame, area: Rect, state: &WatchState, ui: &UiState) {
    let (text, style) = badge(state);
    let mut spans = vec![Span::styled(text.trim().to_owned(), style)];
    if state.open_escalations().next().is_some() {
        spans.push(Span::styled(" ASK", bold()));
    }
    if ui.busy.is_some() {
        spans.push(Span::raw(" ..."));
    }
    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

fn draw_header(frame: &mut Frame, area: Rect, state: &WatchState, ui: &UiState) {
    let (text, style) = badge(state);
    let mut first = Vec::new();
    if state.auto_approve() {
        // Never subtle: the agent's approval prompts are off and nothing sandboxes it.
        first.push(Span::styled(
            " AUTO-APPROVE ",
            Style::new()
                .fg(Color::White)
                .bg(Color::Red)
                .add_modifier(Modifier::BOLD | Modifier::REVERSED),
        ));
        first.push(Span::raw(" "));
    }
    if let Link::Lost { failures } = ui.link {
        // What follows is the last thing the daemon said, not what is true now.
        first.push(Span::styled(
            format!(" NO DAEMON ({failures}) "),
            Style::new()
                .fg(Color::White)
                .bg(Color::Magenta)
                .add_modifier(Modifier::BOLD),
        ));
        first.push(Span::raw(" "));
    }
    first.push(Span::styled(text, style));
    first.push(Span::raw(" "));
    if let Some(id) = state.run_id {
        first.push(Span::styled(
            format!("run {}", short(&id.to_string())),
            bold(),
        ));
        first.push(Span::raw("  "));
    }
    if let Some(elapsed) = state.elapsed(ui.now) {
        first.push(Span::raw(clock(elapsed)));
        first.push(Span::raw("  "));
    }
    let c = state.counts;
    let mut counters = format!("assessments {}", c.assessments);
    if c.skipped > 0 {
        counters.push_str(&format!(" (+{} skipped)", c.skipped));
    }
    if c.budget_cycles > 0 {
        counters.push_str(&format!(" (+{} budget-exhausted)", c.budget_cycles));
    }
    counters.push_str(&format!("  workers {}  attempts {}", c.workers, c.attempts));
    if c.verifiers > 0 {
        counters.push_str(&format!("  verifiers {}", c.verifiers));
    }
    counters.push_str(&format!("  steers {}", c.steers));
    first.push(Span::styled(counters, dim()));
    if state.assessing {
        first.push(Span::raw("  "));
        first.push(Span::styled("assessing...", Style::new().fg(Color::Cyan)));
    }
    let mut lines = vec![clip_line(first, area.width)];

    if area.height >= 2 {
        let cols = usize::from(area.width);
        if let Link::Lost { .. } = ui.link {
            lines.push(Line::styled(
                clip(
                    "the daemon is not answering; this screen is stale and reconnecting (q quits, the run is not touched)",
                    cols,
                ),
                Style::new().fg(Color::Magenta),
            ));
        } else if ui.polling && !state.is_finished() {
            lines.push(Line::styled(
                clip(
                    "not live in this daemon (another process, or an orphan): following its log by polling; steering and cancelling may be refused",
                    cols,
                ),
                Style::new().fg(Color::Yellow),
            ));
        } else {
            lines.push(Line::styled(header_details(state, cols), dim()));
        }
    }
    if area.height >= 3 && state.auto_approve() {
        lines.push(Line::styled(
            clip(
                "AUTO-APPROVE: the agent runs with its approval prompts suppressed, and aftman does not sandbox it",
                usize::from(area.width),
            ),
            Style::new()
                .fg(Color::White)
                .bg(Color::Red)
                .add_modifier(Modifier::BOLD),
        ));
    }
    frame.render_widget(Paragraph::new(lines), area);
}

/// Worker, model, repo and job on one line. The worker and the model are short and
/// always kept; the repo path loses its middle and the job its end before either is
/// dropped.
fn header_details(state: &WatchState, cols: usize) -> String {
    let mut out = String::new();
    for (name, value) in [("worker", &state.worker_kind), ("model", &state.model)] {
        if !value.is_empty() {
            if !out.is_empty() {
                out.push_str("  ");
            }
            out.push_str(&format!("{name}: {value}"));
        }
    }
    // The launch posture is the least important thing on this line (an unsafe one has
    // its own red line), so it is shown only when the repo and the job still get room.
    let posture = state.posture();
    if !posture.is_empty() {
        let extra = 2 + "launch: ".len() + width(&posture);
        if cols.saturating_sub(width(&out) + extra) >= 60 {
            out.push_str(&format!("  launch: {posture}"));
        }
    }
    let sep = if out.is_empty() { 0 } else { 2 };
    let room = cols.saturating_sub(width(&out) + sep);
    let repo_label = "repo: ";
    let job_label = "job: ";
    let repo_full = if state.repo.is_empty() {
        0
    } else {
        sep + repo_label.len() + width(&state.repo)
    };
    let job_full = if state.job_excerpt.is_empty() {
        0
    } else {
        2 + job_label.len() + width(&state.job_excerpt)
    };
    let (repo_room, job_room) = if repo_full + job_full <= room {
        (repo_full, job_full)
    } else if job_full == 0 {
        (room, 0)
    } else if repo_full == 0 {
        (0, room)
    } else {
        let repo_room = repo_full.min(room / 2);
        (repo_room, room - repo_room)
    };
    if repo_room > sep + repo_label.len() + 4 {
        out.push_str(&" ".repeat(sep));
        out.push_str(repo_label);
        out.push_str(&elide_middle(
            &state.repo,
            repo_room - sep - repo_label.len(),
        ));
    }
    if job_room > 2 + job_label.len() + 4 {
        out.push_str("  ");
        out.push_str(job_label);
        out.push_str(&clip(&state.job_excerpt, job_room - 2 - job_label.len()));
    }
    clip(out.trim_start(), cols)
}

/// Clip a line of spans to `cols` columns (by cutting the last span that overflows).
fn clip_line(spans: Vec<Span<'static>>, cols: u16) -> Line<'static> {
    let mut left = usize::from(cols);
    let mut out = Vec::new();
    for span in spans {
        if left == 0 {
            break;
        }
        let w = width(&span.content);
        if w <= left {
            left -= w;
            out.push(span);
        } else {
            out.push(Span::styled(clip(&span.content, left), span.style));
            break;
        }
    }
    Line::from(out)
}

// ---------------------------------------------------------------------------
// Banner
// ---------------------------------------------------------------------------

fn banner_height(area: Rect, state: &WatchState) -> u16 {
    let wants = state.needs_human() || state.is_finished();
    if !wants || area.height < 8 {
        return 0;
    }
    if state.needs_human() && area.height >= 18 {
        4
    } else {
        1
    }
}

fn draw_banner(frame: &mut Frame, area: Rect, state: &WatchState) {
    if state.is_finished() {
        let (text, style) = badge(state);
        let reason = state
            .finished
            .as_ref()
            .and_then(|f| f.reason.clone())
            .unwrap_or_default();
        let line = format!(
            "{}  run ended{}{}   q to quit",
            text.trim(),
            if reason.is_empty() { "" } else { ": " },
            reason
        );
        frame.render_widget(
            Paragraph::new(Line::styled(clip(&line, usize::from(area.width)), style)),
            area,
        );
        return;
    }
    let style = Style::new()
        .fg(Color::Black)
        .bg(Color::Yellow)
        .add_modifier(Modifier::BOLD);
    let open: Vec<&EscalationView> = state.open_escalations().collect();
    let first = open.first().copied();
    let id = first.map_or("(not yet reported)", |e| e.id.as_str());
    let more = if open.len() > 1 {
        format!("  (+{} more)", open.len() - 1)
    } else {
        String::new()
    };
    if area.height >= 4 {
        let block = Block::new()
            .borders(Borders::ALL)
            .border_style(style)
            .style(style)
            .title(" NEEDS A HUMAN ");
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let cols = usize::from(inner.width);
        let ask = first.map_or_else(String::new, |e| e.ask.clone());
        let lines = vec![
            Line::styled(
                clip(&format!("escalation {id}{more}   press a to answer"), cols),
                style,
            ),
            Line::styled(clip(&format!("ask: {ask}"), cols), style),
        ];
        frame.render_widget(Paragraph::new(lines).style(style), inner);
    } else {
        let line = format!("NEEDS HUMAN  escalation {id}{more}  [a] answer");
        frame.render_widget(
            Paragraph::new(Line::styled(clip(&line, usize::from(area.width)), style)).style(style),
            area,
        );
    }
}

// ---------------------------------------------------------------------------
// Panes
// ---------------------------------------------------------------------------

fn pane_block(title: String, focused: bool) -> Block<'static> {
    let (border, title_style) = if focused {
        (
            Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD),
            Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        )
    } else {
        (dim(), Style::new())
    };
    let marker = if focused { ">" } else { " " };
    Block::new()
        .borders(Borders::ALL)
        .border_style(border)
        .title(Span::styled(format!("{marker}{title} "), title_style))
}

fn draw_body(frame: &mut Frame, area: Rect, state: &WatchState, ui: &UiState) {
    if area.width == 0 || area.height == 0 {
        return;
    }
    if area.width >= 110 && area.height >= 10 {
        let [left, right] =
            Layout::horizontal([Constraint::Percentage(56), Constraint::Percentage(44)])
                .areas(area);
        let [timeline, assessment] =
            Layout::vertical([Constraint::Percentage(56), Constraint::Percentage(44)]).areas(left);
        draw_timeline(frame, timeline, state, ui);
        draw_assessment(frame, assessment, state, ui);
        draw_tail(frame, right, state, ui);
    } else if area.width >= 50 && area.height >= 15 {
        let areas = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Percentage(36),
                Constraint::Percentage(34),
                Constraint::Percentage(30),
            ])
            .split(area);
        draw_timeline(frame, areas[0], state, ui);
        draw_assessment(frame, areas[1], state, ui);
        draw_tail(frame, areas[2], state, ui);
    } else {
        match ui.focus {
            Pane::Timeline => draw_timeline(frame, area, state, ui),
            Pane::Assessment => draw_assessment(frame, area, state, ui),
            Pane::Tail => draw_tail(frame, area, state, ui),
        }
    }
}

fn level_style(level: Level) -> Style {
    match level {
        Level::Info => dim(),
        Level::Warn => Style::new().fg(Color::Yellow),
        Level::Error => Style::new().fg(Color::Red),
        Level::Human => Style::new().fg(Color::Magenta).add_modifier(Modifier::BOLD),
    }
}

fn level_marker(level: Level) -> &'static str {
    match level {
        Level::Info => ">",
        Level::Warn | Level::Error => "!",
        Level::Human => "?",
    }
}

fn draw_timeline(frame: &mut Frame, area: Rect, state: &WatchState, ui: &UiState) {
    let block = pane_block(
        format!("1 Decisions ({})", state.timeline.len()),
        ui.focus == Pane::Timeline,
    );
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.width == 0 || inner.height == 0 {
        return;
    }
    let cols = usize::from(inner.width);
    let rows = usize::from(inner.height);
    if state.timeline.is_empty() {
        frame.render_widget(
            Paragraph::new(Line::styled("(no events yet)", dim())),
            inner,
        );
        return;
    }

    let selected = ui.selected_index(state);
    let mut lines: Vec<Line<'static>> = Vec::new();
    let mut sel_range = (0, 0);
    for (index, entry) in state.timeline.iter().enumerate() {
        let is_selected = selected == Some(index);
        let start = lines.len();
        lines.push(timeline_row(entry, is_selected, cols));
        if is_selected {
            lines.extend(detail_lines(entry, cols));
            sel_range = (start, lines.len());
        }
    }
    // Keep the selected block in view; following the newest pins the bottom.
    let mut first = sel_range.1.saturating_sub(rows);
    if sel_range.0 < first {
        first = sel_range.0;
    }
    let visible: Vec<Line<'static>> = lines.into_iter().skip(first).take(rows).collect();
    frame.render_widget(Paragraph::new(visible), inner);
}

fn timeline_row(entry: &Entry, selected: bool, cols: usize) -> Line<'static> {
    let cursor = if selected { "> " } else { "  " };
    let cursor_style = if selected {
        Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD)
    } else {
        Style::new()
    };
    let mut spans = vec![Span::styled(cursor, cursor_style)];
    match entry {
        Entry::Decision(d) => {
            spans.push(Span::styled(format!("{:>5} ", d.seq), dim()));
            spans.push(Span::styled("* ", bold()));
            spans.push(Span::styled(d.directive.clone(), bold().fg(Color::Green)));
            if !d.reason.is_empty() {
                spans.push(Span::styled(format!("  {}", d.reason), Style::new()));
            }
        }
        Entry::Note(n) => {
            let style = level_style(n.level);
            spans.push(Span::styled(format!("{:>5} ", n.seq), dim()));
            spans.push(Span::styled(format!("{} ", level_marker(n.level)), style));
            spans.push(Span::styled(n.text.clone(), style));
        }
    }
    clip_line(spans, u16::try_from(cols).unwrap_or(u16::MAX))
}

fn detail_lines(entry: &Entry, cols: usize) -> Vec<Line<'static>> {
    let indent = "      ";
    let room = cols.saturating_sub(indent.len() + 2).max(1);
    let mut out: Vec<Line<'static>> = Vec::new();
    let mut push = |label: &str, text: &str, style: Style| {
        for (i, piece) in wrap(text, room.saturating_sub(label.len()).max(1), 6)
            .into_iter()
            .enumerate()
        {
            let lead = if i == 0 {
                format!("{indent}{label}")
            } else {
                format!("{indent}{}", " ".repeat(label.len()))
            };
            out.push(Line::from(vec![
                Span::styled(lead, dim()),
                Span::styled(piece, style),
            ]));
        }
    };
    match entry {
        Entry::Decision(d) => decision_detail(d, &mut push),
        Entry::Note(Note { text, level, .. }) => push("", text, level_style(*level)),
    }
    out.truncate(MAX_DETAIL_LINES);
    out
}

fn decision_detail(d: &Decision, push: &mut impl FnMut(&str, &str, Style)) {
    if d.reason.is_empty() {
        push("why: ", "(no reason given)", dim());
    } else {
        push("why: ", &d.reason, Style::new());
    }
    if let Some(seq) = d.assessment_seq {
        push("from: ", &format!("assessment #{seq}"), dim());
    }
    for p in &d.proposals {
        let priority = p
            .priority
            .map_or_else(String::new, |n| format!(" (priority {n})"));
        push(
            "proposed: ",
            &format!("{} -> {}{priority}", p.rule, p.directive),
            Style::new(),
        );
    }
    if d.proposals_omitted > 0 {
        push(
            "proposed: ",
            &format!("... and {} more", d.proposals_omitted),
            dim(),
        );
    }
    for s in &d.suppressed {
        push(
            "suppressed: ",
            &format!("{} ({})", s.rule, s.reason),
            Style::new().fg(Color::Yellow),
        );
    }
    if !d.invariants.is_empty() {
        push(
            "invariants: ",
            &d.invariants.join(", "),
            Style::new().fg(Color::Magenta),
        );
    }
}

fn fmt_p(value: Option<f64>) -> String {
    value.map_or_else(|| "  -  ".to_owned(), |v| format!("{v:.2}"))
}

/// The margin from the unrounded values, never `-0.00`: a value just under its
/// threshold says so with more digits instead of a rounded zero that looks like a tie.
fn fmt_margin(check: &CheckView) -> String {
    check.margin().map_or_else(
        || "  -  ".to_owned(),
        |m| {
            if m == 0.0 {
                "0.00".to_owned()
            } else if m.abs() < 0.0005 {
                if m > 0.0 { "+<0.001" } else { "-<0.001" }.to_owned()
            } else if m.abs() < 0.005 {
                format!("{m:+.3}")
            } else {
                format!("{m:+.2}")
            }
        },
    )
}

/// Which side of its threshold a value is on, in words, so that direction does not
/// depend on colour or on how a flag happens to read.
fn side(check: &CheckView) -> &'static str {
    match check.margin() {
        None => "",
        Some(m) if m > 0.0 => "above",
        Some(m) if m < 0.0 => "below",
        Some(_) => "at",
    }
}

/// The flags, verbatim. Deliberately no green or cyan: `decisive` means the model's
/// value was far enough from the threshold to act on, in either direction, and
/// `permitted` means a rule may evaluate it. Neither says the check is good.
fn flags(check: &CheckView) -> Vec<(&'static str, Style)> {
    let mut out = Vec::new();
    if check.decisive {
        out.push(("decisive", bold()));
    }
    if check.contested {
        out.push((
            "contested",
            Style::new().fg(Color::Yellow).add_modifier(Modifier::BOLD),
        ));
    }
    if check.unresolved {
        out.push((
            "unresolved",
            Style::new().fg(Color::Magenta).add_modifier(Modifier::BOLD),
        ));
    }
    if check.permitted {
        out.push(("permitted", Style::new()));
    } else {
        out.push(("not permitted", dim()));
    }
    out
}

fn age(duration: time::Duration) -> String {
    let seconds = duration.whole_seconds().max(0);
    if seconds < 90 {
        format!("{seconds}s")
    } else if seconds < 5400 {
        format!("{}m", seconds / 60)
    } else {
        format!("{}h", seconds / 3600)
    }
}

fn draw_assessment(frame: &mut Frame, area: Rect, state: &WatchState, ui: &UiState) {
    let title = match &state.assessment {
        Some(a) => format!(
            "2 Assessment #{} ({} checks)",
            a.seq,
            a.checks.len() + a.checks_omitted
        ),
        None => "2 Assessment".to_owned(),
    };
    let block = pane_block(title, ui.focus == Pane::Assessment);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.width == 0 || inner.height == 0 {
        return;
    }
    let cols = usize::from(inner.width);
    let mut lines: Vec<Line<'static>> = Vec::new();

    if let Some(t) = &state.trouble {
        let style = match t.kind {
            "rate_limited" | "budget_exceeded" => Style::new().fg(Color::Yellow),
            _ => Style::new().fg(Color::Red),
        };
        lines.push(Line::styled(
            clip(
                &format!(
                    "! {} {}: {}  (event {}, {} ago)",
                    t.kind,
                    t.code,
                    t.message,
                    t.seq,
                    age(ui.now - t.at)
                ),
                cols,
            ),
            style.add_modifier(Modifier::BOLD),
        ));
    }
    match &state.assessment {
        None => lines.push(Line::styled(
            if state.assessing {
                "assessing..."
            } else {
                "(no assessment yet)"
            },
            dim(),
        )),
        Some(a) => {
            let table = cols >= 84;
            let key_w = cols.saturating_sub(64).clamp(12, 40);
            if table {
                lines.push(Line::styled(
                    clip(
                        &format!(
                            "{:<key_w$} {:>5}  {:>5}  {:>7} {:<5}  flags",
                            "check", "p", "thr", "margin", "side"
                        ),
                        cols,
                    ),
                    dim(),
                ));
            }
            for check in &a.checks {
                if table {
                    let mut spans = vec![Span::raw(format!(
                        "{:<key_w$} {:>5}  {:>5}  {:>7} {:<5}  ",
                        clip(&check.key, key_w),
                        fmt_p(check.value),
                        fmt_p(check.threshold),
                        fmt_margin(check),
                        side(check),
                    ))];
                    spans.extend(flag_spans(check));
                    lines.push(clip_line(spans, u16::try_from(cols).unwrap_or(u16::MAX)));
                } else {
                    lines.push(Line::styled(clip(&check.key, cols), bold()));
                    let mut spans = vec![Span::raw(format!(
                        "  p {}  thr {}  margin {} {}  ",
                        fmt_p(check.value).trim(),
                        fmt_p(check.threshold).trim(),
                        fmt_margin(check).trim(),
                        side(check),
                    ))];
                    spans.extend(flag_spans(check));
                    lines.push(clip_line(spans, u16::try_from(cols).unwrap_or(u16::MAX)));
                }
            }
            if a.checks_omitted > 0 {
                lines.push(Line::styled(
                    format!("... {} more checks", a.checks_omitted),
                    dim(),
                ));
            }
            lines.push(Line::styled(
                clip(
                    &format!(
                        "model {}  tokens in {} out {}",
                        if a.model.is_empty() { "-" } else { &a.model },
                        a.input_tokens,
                        a.output_tokens
                    ),
                    cols,
                ),
                dim(),
            ));
        }
    }
    let rows = usize::from(inner.height);
    let max_scroll = lines.len().saturating_sub(rows);
    // The one clamp: the keys use the same number.
    ui.assess_max.set(max_scroll);
    let skip = ui.assess_scroll.min(max_scroll);
    let visible: Vec<Line<'static>> = lines.into_iter().skip(skip).take(rows).collect();
    frame.render_widget(Paragraph::new(visible), inner);
}

fn flag_spans(check: &CheckView) -> Vec<Span<'static>> {
    let mut out = Vec::new();
    for (i, (text, style)) in flags(check).into_iter().enumerate() {
        if i > 0 {
            out.push(Span::raw(" "));
        }
        out.push(Span::styled(text, style));
    }
    out
}

/// Rows one tail line may take once wrapped.
const MAX_TAIL_ROWS_PER_LINE: usize = 6;

/// The rows one tail line takes. Every worker line has a `| ` gutter (`+ ` on its
/// continuation rows) and every TUI marker a `~ ` gutter in dim text, so nothing a
/// worker prints can be read as the TUI's own chrome: styling follows the line's
/// kind, never what its text looks like.
fn tail_rows(kind: TailKind, text: &str, cols: usize) -> Vec<Line<'static>> {
    let gutter = cols >= 6;
    let text_cols = if gutter { cols - 2 } else { cols };
    hard_wrap(text, text_cols, MAX_TAIL_ROWS_PER_LINE)
        .into_iter()
        .enumerate()
        .map(|(i, row)| {
            let (lead, lead_style, body_style) = match (kind, i) {
                (TailKind::Worker, 0) => ("| ", dim(), Style::new()),
                (TailKind::Worker, _) => ("+ ", dim(), Style::new()),
                (TailKind::Marker, _) => ("~ ", dim(), dim().add_modifier(Modifier::ITALIC)),
            };
            if gutter {
                Line::from(vec![
                    Span::styled(lead, lead_style),
                    Span::styled(row, body_style),
                ])
            } else {
                Line::styled(row, body_style)
            }
        })
        .collect()
}

fn draw_tail(frame: &mut Frame, area: Rect, state: &WatchState, ui: &UiState) {
    let mut title = "3 Worker output".to_owned();
    if let Some(worker) = state.active_worker() {
        title.push_str(&format!(" ({} {})", worker.kind, worker.task));
    } else if let Some(worker) = state.workers.back() {
        let how = match worker.phase {
            WorkerPhase::Running => "running",
            WorkerPhase::Completed { success: true } => "completed",
            WorkerPhase::Completed { success: false } => "failed",
            WorkerPhase::Terminated => "terminated",
        };
        title.push_str(&format!(" ({} {})", worker.kind, how));
    }
    let bounds = state
        .tail
        .last_index()
        .map(|last| (state.tail.first_index(), last));
    let anchor = match (ui.tail_anchor, bounds) {
        (Some(anchor), Some((first, last))) if anchor < last => Some(anchor.max(first)),
        _ => None,
    };
    if let (Some(anchor), Some((_, last))) = (anchor, bounds) {
        title.push_str(&format!(
            " - reading back; {} newer lines, End follows",
            last - anchor
        ));
    }
    let block = pane_block(title, ui.focus == Pane::Tail);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.width == 0 || inner.height == 0 {
        return;
    }
    let cols = usize::from(inner.width);
    let rows = usize::from(inner.height);
    let Some((first, last)) = bounds else {
        frame.render_widget(
            Paragraph::new(Line::styled("(no worker output yet)", dim())),
            inner,
        );
        return;
    };

    // Rows ending at the line being read (the newest, when following), filled upwards;
    // if the top of the output is reached first, the rest of the pane is filled with
    // the lines after it, so scrolling to the top never leaves the pane blank.
    let at = anchor.unwrap_or(last);
    let mut groups: Vec<Vec<Line<'static>>> = Vec::new();
    let mut used = 0;
    let mut index = at;
    loop {
        if let Some((kind, text)) = state.tail.line_at(index) {
            let rows_of_line = tail_rows(kind, text, cols);
            used += rows_of_line.len();
            groups.push(rows_of_line);
        }
        if used >= rows || index <= first {
            break;
        }
        index -= 1;
    }
    let mut lines: Vec<Line<'static>> = groups.into_iter().rev().flatten().collect();
    let mut next = at + 1;
    while lines.len() < rows && next <= last {
        if let Some((kind, text)) = state.tail.line_at(next) {
            lines.extend(tail_rows(kind, text, cols));
        }
        next += 1;
    }
    // Over-full: keep the bottom when following, the top when at the start.
    if lines.len() > rows {
        if next > at + 1 {
            lines.truncate(rows);
        } else {
            lines.drain(..lines.len() - rows);
        }
    }
    frame.render_widget(Paragraph::new(lines), inner);
}

// ---------------------------------------------------------------------------
// Footer and input
// ---------------------------------------------------------------------------

fn draw_footer(frame: &mut Frame, area: Rect, state: &WatchState, ui: &UiState) {
    let mut spans: Vec<Span<'static>> = Vec::new();
    if let Some(what) = &ui.busy {
        spans.push(Span::styled(
            format!(" working: {what} "),
            Style::new().fg(Color::Black).bg(Color::Cyan),
        ));
        spans.push(Span::raw("  "));
    }
    if let Some(message) = &ui.message {
        let style = match message.tone {
            Tone::Info => Style::new().fg(Color::Cyan),
            Tone::Ok => Style::new().fg(Color::Green).add_modifier(Modifier::BOLD),
            Tone::Error => Style::new()
                .fg(Color::White)
                .bg(Color::Red)
                .add_modifier(Modifier::BOLD),
        };
        spans.push(Span::styled(format!(" {} ", message.text), style));
        spans.push(Span::raw("  "));
    }
    spans.push(Span::styled(
        hints(&ui.mode, state.is_finished(), state.needs_human()),
        dim(),
    ));
    frame.render_widget(Paragraph::new(clip_line(spans, area.width)), area);
}

/// The visible slice of `text` for a field `cols` wide, and the cursor's column in it.
fn text_window(text: &str, cursor_col: usize, cols: usize) -> (String, usize) {
    if cols == 0 {
        return (String::new(), 0);
    }
    let skip_cols = cursor_col.saturating_sub(cols - 1);
    let mut skipped = 0;
    let mut chars = text.chars().peekable();
    while skipped < skip_cols {
        let Some(c) = chars.next() else { break };
        skipped += unicode_width::UnicodeWidthChar::width(c).unwrap_or(0);
    }
    let mut out = String::new();
    let mut used = 0;
    for c in chars {
        let w = unicode_width::UnicodeWidthChar::width(c).unwrap_or(0);
        if used + w > cols {
            break;
        }
        used += w;
        out.push(c);
    }
    (out, cursor_col.saturating_sub(skipped).min(cols - 1))
}

fn counts_label(editor: &super::input::LineEditor) -> String {
    format!(
        "{}/{} bytes, {}/{} chars",
        editor.bytes(),
        editor.max_bytes(),
        editor.chars(),
        editor.max_chars()
    )
}

fn draw_input_box(frame: &mut Frame, area: Rect, ui: &UiState) {
    let Mode::Input { purpose, editor } = &ui.mode else {
        return;
    };
    let block = Block::new()
        .borders(Borders::ALL)
        .border_style(Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD))
        .title(format!(
            " {} - Enter send, Esc cancel - {} ",
            purpose.title(),
            counts_label(editor)
        ));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.width == 0 || inner.height == 0 {
        return;
    }
    let (visible, cursor_x) =
        text_window(editor.text(), editor.cursor_col(), usize::from(inner.width));
    frame.render_widget(Paragraph::new(visible), inner);
    let x = inner.x.saturating_add(u16::try_from(cursor_x).unwrap_or(0));
    frame.set_cursor_position((x.min(inner.right().saturating_sub(1)), inner.y));
}

fn draw_input_line(
    frame: &mut Frame,
    area: Rect,
    purpose: &Purpose,
    editor: &super::input::LineEditor,
) {
    let prompt = match purpose {
        Purpose::Steer => "steer> ",
        Purpose::Guidance { .. } => "guide> ",
        Purpose::Note { .. } => "note> ",
    };
    let cols = usize::from(area.width).saturating_sub(prompt.len());
    let (visible, cursor_x) = text_window(editor.text(), editor.cursor_col(), cols);
    frame.render_widget(Paragraph::new(format!("{prompt}{visible}")), area);
    if cols > 0 {
        let x = area
            .x
            .saturating_add(u16::try_from(prompt.len() + cursor_x).unwrap_or(0));
        frame.set_cursor_position((x.min(area.right().saturating_sub(1)), area.y));
    }
}

// ---------------------------------------------------------------------------
// Popups
// ---------------------------------------------------------------------------

fn centered(area: Rect, w: u16, h: u16) -> Rect {
    let w = w.min(area.width);
    let h = h.min(area.height);
    Rect {
        x: area.x + (area.width - w) / 2,
        y: area.y + (area.height - h) / 2,
        width: w,
        height: h,
    }
}

fn popup(frame: &mut Frame, area: Rect, title: &str, lines: Vec<Line<'static>>, border: Color) {
    let h = u16::try_from(lines.len().saturating_add(2)).unwrap_or(u16::MAX);
    let rect = centered(area, 78, h);
    frame.render_widget(Clear, rect);
    let block = Block::new()
        .borders(Borders::ALL)
        .border_style(Style::new().fg(border).add_modifier(Modifier::BOLD))
        .title(format!(" {title} "));
    let inner = block.inner(rect);
    frame.render_widget(block, rect);
    frame.render_widget(Paragraph::new(lines), inner);
}

/// What a prompt shows instead of itself when the terminal cannot hold it. Its keys
/// do nothing but Esc while this is up (`input.rs` checks the same sizes).
fn draw_too_small(frame: &mut Frame, area: Rect, min: (u16, u16)) {
    let text = format!(
        "terminal too small for this prompt (need {}x{}): resize, or press Esc",
        min.0, min.1
    );
    let lines: Vec<Line<'static>> = wrap(
        &text,
        usize::from(area.width.max(1)),
        usize::from(area.height),
    )
    .into_iter()
    .map(|l| Line::styled(l, Style::new().fg(Color::Black).bg(Color::Yellow)))
    .collect();
    frame.render_widget(Clear, area);
    frame.render_widget(Paragraph::new(lines), area);
}

fn draw_decide(
    frame: &mut Frame,
    area: Rect,
    state: &WatchState,
    escalation: &str,
    position: (usize, usize),
    selected: usize,
) {
    if area.width < DECIDE_MIN.0 || area.height < DECIDE_MIN.1 {
        draw_too_small(frame, area, DECIDE_MIN);
        return;
    }
    let inner_cols = usize::from(area.width.min(78)).saturating_sub(2).max(1);
    let mut lines = vec![Line::styled(
        clip(
            &format!("escalation {escalation} ({} of {})", position.0, position.1),
            inner_cols,
        ),
        bold(),
    )];
    for (i, choice) in Choice::ALL.iter().enumerate() {
        let on = i == selected;
        let style = if on {
            Style::new()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::new()
        };
        lines.push(Line::styled(
            clip(
                &format!(
                    "{} {}  {}",
                    if on { ">" } else { " " },
                    i + 1,
                    choice.name()
                ),
                inner_cols,
            ),
            style,
        ));
    }
    lines.push(Line::raw(""));
    let choice = Choice::ALL[selected.min(Choice::ALL.len() - 1)];
    for piece in wrap(choice.explanation(), inner_cols, 4) {
        lines.push(Line::styled(piece, Style::new().fg(Color::Cyan)));
    }
    let ask = state
        .escalations
        .iter()
        .find(|e| e.id == escalation)
        .map(|e| e.ask.clone())
        .unwrap_or_default();
    if !ask.is_empty() {
        lines.push(Line::raw(""));
        for piece in wrap(&format!("ask: {ask}"), inner_cols, 3) {
            lines.push(Line::styled(piece, dim()));
        }
    }
    popup(frame, area, "Answer the escalation", lines, Color::Yellow);
}

fn draw_confirm(frame: &mut Frame, area: Rect, confirm: &Confirm, typed: &str) {
    if area.width < CONFIRM_MIN.0 || area.height < CONFIRM_MIN.1 {
        draw_too_small(frame, area, CONFIRM_MIN);
        return;
    }
    let inner_cols = usize::from(area.width.min(78)).saturating_sub(2).max(1);
    let (title, text) = match confirm {
        Confirm::CancelRun => (
            "Cancel this run?",
            "The worker is stopped and the run ends as cancelled. This cannot be undone.",
        ),
        Confirm::StopAnswer { .. } => (
            "Answer: stop?",
            "The run is stopped now and ends as cancelled. This cannot be undone.",
        ),
    };
    let mut lines: Vec<Line<'static>> = wrap(text, inner_cols, 3)
        .into_iter()
        .map(Line::raw)
        .collect();
    lines.push(Line::raw(""));
    lines.push(Line::styled(
        format!("Type {} and press Enter. Esc goes back.", confirm.word()),
        bold(),
    ));
    lines.push(Line::styled(
        clip(&format!("> {typed}_"), inner_cols),
        Style::new().fg(Color::Red).add_modifier(Modifier::BOLD),
    ));
    popup(frame, area, title, lines, Color::Red);
}

fn draw_help(frame: &mut Frame, area: Rect) {
    let rows = [
        (
            "q  Esc  Ctrl-C  Ctrl-D",
            "quit the TUI; the run keeps going in the daemon",
        ),
        ("Tab  Shift-Tab  1 2 3", "focus a pane"),
        (
            "j k  Up Down  PgUp PgDn",
            "scroll the focused pane; Home g / End G jump",
        ),
        ("s", "steer: send guidance to the worker"),
        ("a", "answer an open escalation"),
        ("c", "cancel the run (asks first)"),
        ("?  F1", "this help"),
        ("cancel / stop", "need the word typed, then Enter"),
        ("", ""),
        (
            "typing",
            "Enter send, Esc cancel, Ctrl-U clear, Ctrl-A/E ends",
        ),
    ];
    let lines: Vec<Line<'static>> = rows
        .iter()
        .map(|(keys, what)| {
            Line::from(vec![
                Span::styled(format!("{keys:<26}"), bold()),
                Span::raw((*what).to_owned()),
            ])
        })
        .collect();
    popup(frame, area, "Keys", lines, Color::Cyan);
}
