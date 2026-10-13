//! `TestBackend` tests for the pure view.

use ratatui::Terminal;
use ratatui::backend::TestBackend;
use ratatui::buffer::Buffer;
use ratatui::style::Color;
use serde_json::{Value, json};
use time::OffsetDateTime;
use time::macros::datetime;

use aftman_core::event::{EventEnvelope, EventKind};
use aftman_core::ids::{EscalationId, EventSeq, RunId};

use super::input::{Confirm, LineEditor, Mode, Pane, Purpose, Tone, UiState};
use super::state::WatchState;
use super::view::draw;

fn start() -> OffsetDateTime {
    datetime!(2026-01-01 00:00:00 UTC)
}

fn ev(seq: u64, kind: EventKind, payload: Value) -> EventEnvelope {
    EventEnvelope::new(
        EventSeq(seq),
        RunId::new(),
        start() + time::Duration::seconds(seq as i64),
        kind,
        payload,
    )
}

fn running_events() -> Vec<EventEnvelope> {
    vec![
        ev(1, EventKind::RunCreated, json!({"repo": "/work/repo"})),
        ev(
            2,
            EventKind::JobRouted,
            json!({"activated": ["core.completion"], "model": "sim-1"}),
        ),
        ev(
            3,
            EventKind::StateChanged,
            json!({"from": "routing", "to": "running"}),
        ),
        ev(
            4,
            EventKind::WorkerStarted,
            json!({"worker_id": "abcdef012345", "kind": "simulated", "task": "Code", "mission_excerpt": "add retries", "supports_steering": true}),
        ),
        ev(
            5,
            EventKind::WorkerOutput,
            json!({"worker_id": "abcdef012345", "text": "editing src/http.rs\nrunning the test suite\ntest result: ok. 3 passed\n"}),
        ),
        ev(
            6,
            EventKind::AssessmentCompleted,
            json!({"assessment_seq": 2, "model": "sim-1", "input_tokens": 120, "output_tokens": 8, "checks": [
                {"key": "core.completion.tests_pass", "jev_p": 0.82, "threshold": 0.7, "decisive": true, "contested": false, "permitted": true, "unresolved": false},
                {"key": "core.completion.no_todo", "jev_p": 0.4, "threshold": 0.5, "decisive": false, "contested": true, "permitted": false, "unresolved": true}
            ]}),
        ),
        ev(
            7,
            EventKind::DirectiveProposed,
            json!({"responsibility": "core.completion", "rule": "core.completion.finish", "directive": "finish", "priority": 5}),
        ),
        ev(
            8,
            EventKind::DirectiveSelected,
            json!({"directive": "continue", "reason": "tests are not yet green on the retry path",
                   "invariants": ["MinWorkTime"],
                   "suppressed": [{"rule": "core.completion.finish", "reason": "not_permitted"}]}),
        ),
    ]
}

fn state_of(events: &[EventEnvelope]) -> WatchState {
    let mut state = WatchState::new();
    state.apply_all(events);
    state
}

fn ui() -> UiState {
    UiState::new(start() + time::Duration::seconds(75))
}

fn escalation_id() -> EscalationId {
    "0193a000-0000-7000-8000-00000000abcd"
        .parse()
        .expect("an escalation id")
}

fn waiting() -> WatchState {
    let mut events = running_events();
    events.push(ev(
        9,
        EventKind::EscalationRaised,
        json!({"escalation_id": escalation_id().to_string(), "kind": "check", "reason": "unsure", "ask": "Is it safe to drop the legacy table?"}),
    ));
    events.push(ev(
        10,
        EventKind::StateChanged,
        json!({"from": "assessing", "to": "needs_human"}),
    ));
    state_of(&events)
}

fn failed() -> WatchState {
    let mut events = running_events();
    events.push(ev(
        9,
        EventKind::StateChanged,
        json!({"from": "running", "to": "failed"}),
    ));
    events.push(ev(
        10,
        EventKind::RunFinished,
        json!({"status": "failed", "reason": "worker crashed"}),
    ));
    state_of(&events)
}

fn render_with(width: u16, height: u16, state: &WatchState, ui: &UiState) -> Buffer {
    let mut terminal = Terminal::new(TestBackend::new(width, height)).expect("terminal");
    terminal.draw(|frame| draw(frame, state, ui)).expect("draw");
    terminal.backend().buffer().clone()
}

fn text(buffer: &Buffer) -> String {
    let area = buffer.area;
    let mut out = String::new();
    for y in 0..area.height {
        let mut skip = 0;
        for x in 0..area.width {
            let symbol = buffer[(x, y)].symbol();
            if skip > 0 {
                // The cell after a wide character is its other half.
                skip -= 1;
                continue;
            }
            skip = unicode_width::UnicodeWidthStr::width(symbol).saturating_sub(1);
            out.push_str(symbol);
        }
        out.push('\n');
    }
    out
}

fn screen(width: u16, height: u16, state: &WatchState, ui: &UiState) -> String {
    text(&render_with(width, height, state, ui))
}

fn assert_has(screen: &str, needles: &[&str]) {
    for needle in needles {
        assert!(screen.contains(needle), "missing {needle:?} in:\n{screen}");
    }
}

#[test]
fn a_running_run_shows_status_decision_checks_and_tail_at_80x24() {
    let s = screen(80, 24, &state_of(&running_events()), &ui());
    assert_has(
        &s,
        &[
            "RUNNING",
            "run ",
            "0:01:14",
            "assessments 2",
            "attempts 1",
            "continue",
            "tests are not yet green",
            "core.completion.tests_pass",
            "0.82",
            "0.70",
            "+0.12",
            "decisive",
            "permitted",
            "core.completion.no_todo",
            "-0.10",
            "contested",
            "unresolved",
            "not permitted",
            "editing src/http.rs",
            "test result: ok. 3 passed",
            "q quit",
        ],
    );
    assert!(!s.contains("NEEDS HUMAN"));
}

#[test]
fn the_selected_decision_is_expanded_with_its_proposals_suppressions_and_invariants() {
    let s = screen(120, 40, &state_of(&running_events()), &ui());
    assert_has(
        &s,
        &[
            "why: tests are not yet green on the retry path",
            "proposed: core.completion.finish -> finish",
            "priority 5",
            "suppressed: core.completion.finish (not_permitted)",
            "invariants: MinWorkTime",
            "assessment #2",
            "worker: simulated",
            "model: sim-1",
            "repo: /work/repo",
            "Assessment #2",
            "tokens in 120 out 8",
        ],
    );
}

#[test]
fn the_two_column_layout_shows_all_three_panes_at_120x40() {
    let s = screen(120, 40, &state_of(&running_events()), &ui());
    assert_has(
        &s,
        &[
            "1 Decisions",
            "2 Assessment",
            "3 Worker output",
            "check",
            "margin",
        ],
    );
}

#[test]
fn needs_human_has_a_banner_with_the_escalation_id_and_is_not_drawn_like_failed() {
    let id = escalation_id().to_string();
    for (w, h) in [(80, 24), (120, 40)] {
        let s = screen(w, h, &waiting(), &ui());
        assert_has(
            &s,
            &[
                "NEEDS HUMAN",
                "NEEDS A HUMAN",
                &id,
                "Is it safe to drop the legacy table?",
                "press a to answer",
                "a ANSWER",
            ],
        );
        assert!(!s.contains("FAILED"));
    }
    // Short terminals get the one-line banner.
    let s = screen(80, 10, &waiting(), &ui());
    assert_has(&s, &["NEEDS HUMAN", &id]);

    let f = screen(80, 24, &failed(), &ui());
    assert_has(&f, &["FAILED", "run ended: worker crashed"]);
    assert!(!f.contains("NEEDS HUMAN"));
    assert!(!f.contains("s steer"), "no steering hint once it ended");
}

#[test]
fn needs_human_and_failed_use_different_colours() {
    let waiting_buf = render_with(80, 24, &waiting(), &ui());
    let failed_buf = render_with(80, 24, &failed(), &ui());
    let badge_bg = |buf: &Buffer| buf[(1, 0)].style().bg;
    assert_eq!(badge_bg(&waiting_buf), Some(Color::Yellow));
    assert_eq!(badge_bg(&failed_buf), Some(Color::Red));
}

#[test]
fn a_finished_run_says_so_and_stops_the_clock() {
    let mut events = running_events();
    events.push(ev(9, EventKind::RunFinished, json!({"status": "finished"})));
    let state = state_of(&events);
    let mut later = ui();
    later.now = start() + time::Duration::seconds(10_000);
    let s = screen(80, 24, &state, &later);
    assert_has(&s, &["FINISHED", "run ended", "0:00:08"]);
}

#[test]
fn trouble_is_shown_in_the_assessment_pane() {
    let mut events = running_events();
    events.push(ev(
        9,
        EventKind::RateLimited,
        json!({"code": "rate_limited", "message": "slow down", "retryable": true}),
    ));
    let s = screen(100, 30, &state_of(&events), &ui());
    assert_has(&s, &["rate_limited", "slow down"]);
}

#[test]
fn the_tail_scrolled_back_says_so() {
    let mut u = ui();
    u.focus = Pane::Tail;
    // The tail has a marker and three worker lines (absolute 0..=3): read line 1.
    u.tail_anchor = Some(1);
    let s = screen(80, 24, &state_of(&running_events()), &u);
    assert_has(&s, &["reading back; 2 newer lines", "End follows"]);
    // The pane is taller than the whole output, so it is filled forward from the top
    // rather than left blank.
    assert_has(&s, &["editing src/http.rs"]);
}

#[test]
fn the_footer_shows_messages_and_in_flight_work() {
    let mut u = ui();
    u.busy = Some("cancelling the run...".into());
    u.set_message(Tone::Error, "wrong_state: the run is not live");
    let s = screen(100, 24, &state_of(&running_events()), &u);
    assert_has(
        &s,
        &[
            "working: cancelling the run...",
            "wrong_state: the run is not live",
        ],
    );
}

#[test]
fn the_input_box_shows_the_text_the_counts_and_places_the_cursor() {
    let mut u = ui();
    u.mode = Mode::Input {
        purpose: Purpose::Steer,
        editor: LineEditor::with_text(16_384, 8_000, "héllo 日本"),
    };
    let mut terminal = Terminal::new(TestBackend::new(80, 24)).expect("terminal");
    terminal
        .draw(|frame| draw(frame, &state_of(&running_events()), &u))
        .expect("draw");
    let s = text(terminal.backend().buffer());
    assert_has(
        &s,
        &[
            "Steer the worker",
            "héllo 日本",
            "/16384 bytes",
            "/8000 chars",
        ],
    );
    let cursor = terminal.get_cursor_position().expect("cursor");
    assert_eq!(cursor.y, 22);
    // Border, then "héllo " (6 columns) and "日本" (4).
    assert_eq!(cursor.x, 1 + 6 + 4);
}

#[test]
fn a_long_input_scrolls_horizontally_to_keep_the_cursor_visible() {
    let mut u = ui();
    let long = "x".repeat(300) + "END";
    u.mode = Mode::Input {
        purpose: Purpose::Steer,
        editor: LineEditor::with_text(16_384, 8_000, &long),
    };
    let mut terminal = Terminal::new(TestBackend::new(40, 24)).expect("terminal");
    terminal
        .draw(|frame| draw(frame, &state_of(&running_events()), &u))
        .expect("draw");
    let s = text(terminal.backend().buffer());
    assert_has(&s, &["xxxEND"]);
    let cursor = terminal.get_cursor_position().expect("cursor");
    assert!(cursor.x < 40);
}

#[test]
fn the_decision_popup_lists_the_choices_and_explains_the_selected_one() {
    let id = escalation_id();
    let mut u = ui();
    u.mode = Mode::Decide {
        escalation: id,
        position: (1, 1),
        selected: 3,
    };
    let s = screen(100, 30, &waiting(), &u);
    assert_has(
        &s,
        &[
            "accept",
            "recheck",
            "stop",
            "claim-complete",
            "guidance",
            "> 4  claim-complete",
            "re-measures",
            "finish the run by itself",
            &id.to_string(),
            "Is it safe to drop the legacy table?",
        ],
    );
}

#[test]
fn the_confirm_and_help_popups_draw() {
    let mut u = ui();
    u.mode = Mode::Confirm {
        what: Confirm::CancelRun,
        typed: "can".into(),
    };
    assert_has(
        &screen(80, 24, &state_of(&running_events()), &u),
        &["Cancel this run?", "Type cancel and press Enter", "> can_"],
    );
    u.mode = Mode::Help;
    assert_has(
        &screen(80, 24, &state_of(&running_events()), &u),
        &["Keys", "steer", "cancel the run"],
    );
}

#[test]
fn nothing_hostile_reaches_the_buffer() {
    let mut events = running_events();
    events.push(ev(
        9,
        EventKind::WorkerOutput,
        json!({"worker_id": "abcdef012345", "text": "\u{1b}[2J\u{1b}]0;pwned\u{7}\u{1b}[31mred\u{202e}rtl\r\u{0}\u{8}after\n"}),
    ));
    events.push(ev(
        10,
        EventKind::DirectiveSelected,
        json!({"directive": "continue\u{1b}[H", "reason": "r\u{9b}2Jeason"}),
    ));
    let s = screen(120, 40, &state_of(&events), &ui());
    assert!(
        s.chars().all(|c| c == '\n' || !c.is_control()),
        "a control character reached the screen"
    );
    assert!(!s.contains("pwned"));
    assert_has(&s, &["red", "after"]);
}

#[test]
fn it_never_panics_at_any_size_in_any_mode() {
    let id = escalation_id();
    let states = [
        WatchState::new(),
        state_of(&running_events()),
        waiting(),
        failed(),
    ];
    let modes = [
        Mode::Normal,
        Mode::Help,
        Mode::Confirm {
            what: Confirm::CancelRun,
            typed: "can".into(),
        },
        Mode::Decide {
            escalation: id,
            position: (1, 2),
            selected: 4,
        },
        Mode::Input {
            purpose: Purpose::Steer,
            editor: LineEditor::with_text(100, 100, "日本語 text"),
        },
        Mode::Input {
            purpose: Purpose::Guidance { escalation: id },
            editor: LineEditor::new(100, 100),
        },
    ];
    for state in &states {
        for mode in &modes {
            for focus in [Pane::Timeline, Pane::Assessment, Pane::Tail] {
                let mut u = ui();
                u.mode = mode.clone();
                u.focus = focus;
                u.busy = Some("working".into());
                u.set_message(Tone::Error, "a failure with 日本語 in it");
                u.assess_scroll = 7;
                u.tail_anchor = Some(1);
                u.timeline_sel = Some(2);
                for width in 1..=40u16 {
                    for height in 1..=12u16 {
                        let _ = render_with(width, height, state, &u);
                    }
                }
                for (w, h) in [
                    (200, 60),
                    (300, 3),
                    (3, 100),
                    (80, 24),
                    (120, 40),
                    (110, 10),
                    (50, 15),
                ] {
                    let _ = render_with(w, h, state, &u);
                }
            }
        }
    }
}

#[test]
fn tiny_screens_still_say_what_matters() {
    let buf = render_with(1, 1, &waiting(), &ui());
    assert_eq!(buf.area.width, 1);
    let s = screen(20, 2, &waiting(), &ui());
    assert!(s.contains("NEEDS"), "{s}");
}

fn with_extra(extra: Vec<EventEnvelope>) -> WatchState {
    let mut events = running_events();
    events.extend(extra);
    state_of(&events)
}

#[test]
fn the_header_keeps_worker_and_model_whatever_the_repo_path_is() {
    let mut events = running_events();
    events[0] = ev(
        1,
        EventKind::RunCreated,
        json!({"repo": "/home/someone/Development/company/projects/very/deep/monorepo/services/billing-backend"}),
    );
    let s = screen(80, 24, &state_of(&events), &ui());
    assert_has(&s, &["worker: simulated", "model: sim-1", "repo: /home/"]);
    assert!(s.contains('…'), "the middle of the path is elided");
    assert_has(&s, &["billing-backend"]);
}

#[test]
fn margins_are_computed_from_unrounded_values_and_never_print_negative_zero() {
    let s = screen(
        120,
        40,
        &with_extra(vec![ev(
            9,
            EventKind::AssessmentCompleted,
            json!({"assessment_seq": 3, "model": "m", "checks": [
                {"key": "just.below", "jev_p": 0.6996, "threshold": 0.7, "decisive": true, "permitted": true},
                {"key": "exactly.at", "jev_p": 0.7, "threshold": 0.7, "permitted": false},
                {"key": "far.below", "jev_p": 0.05, "threshold": 0.9, "decisive": true, "permitted": true}
            ]}),
        )]),
        &ui(),
    );
    assert!(!s.contains("-0.00"), "{s}");
    assert_has(&s, &["-<0.001", "0.00", "below", "at", "-0.85"]);
}

#[test]
fn direction_is_in_words_and_flags_are_not_coloured_as_good() {
    let buf = render_with(120, 40, &state_of(&running_events()), &ui());
    // No cell in the assessment area is green or cyan because a check is "decisive".
    let s = text(&buf);
    assert_has(&s, &["above", "below"]);
    for y in 0..buf.area.height {
        for x in 0..buf.area.width {
            let cell = &buf[(x, y)];
            if cell.symbol() == "d" || cell.symbol() == "p" {
                assert_ne!(cell.style().fg, Some(Color::Green));
            }
        }
    }
}

#[test]
fn a_stale_model_error_shows_its_age_and_goes_when_a_real_assessment_follows() {
    let with_error = with_extra(vec![ev(
        9,
        EventKind::ModelError,
        json!({"code": "bad_json", "message": "no", "retryable": true}),
    )]);
    let s = screen(100, 30, &with_error, &ui());
    assert_has(&s, &["bad_json", "(event 9, 66s ago)"]);
    let mut healed = with_error.clone();
    healed.apply(&ev(
        10,
        EventKind::AssessmentCompleted,
        json!({"assessment_seq": 3, "model": "m", "checks": [{"key": "k", "jev_p": 0.5, "threshold": 0.5}]}),
    ));
    assert!(!screen(100, 30, &healed, &ui()).contains("(event 9"));
}

#[test]
fn budget_exhausted_cycles_are_shown_as_such() {
    let mut extra = Vec::new();
    for seq in 9..12 {
        extra.push(ev(
            seq,
            EventKind::AssessmentCompleted,
            json!({"assessment_seq": 2, "model": "budget_exhausted", "checks": []}),
        ));
    }
    let s = screen(120, 40, &with_extra(extra), &ui());
    assert_has(&s, &["assessments 2", "(+3 budget-exhausted)"]);
}

#[test]
fn a_lost_daemon_makes_the_screen_look_stale() {
    let mut u = ui();
    u.link = super::input::Link::Lost { failures: 3 };
    let s = screen(100, 30, &state_of(&running_events()), &u);
    assert_has(&s, &["NO DAEMON (3)", "stale and reconnecting"]);
    assert!(!screen(100, 30, &state_of(&running_events()), &ui()).contains("NO DAEMON"));
}

#[test]
fn a_run_the_daemon_is_not_driving_says_so_until_it_has_ended() {
    let mut u = ui();
    u.polling = true;
    assert_has(
        &screen(120, 30, &state_of(&running_events()), &u),
        &["not live in this daemon", "polling"],
    );
    let mut ended = running_events();
    ended.push(ev(9, EventKind::RunFinished, json!({"status": "finished"})));
    let s = screen(120, 30, &state_of(&ended), &u);
    assert!(!s.contains("not live in this daemon"), "{s}");
    assert_has(&s, &["worker: simulated"]);
}

#[test]
fn an_auto_approve_run_is_unmistakable() {
    let mut events = running_events();
    events[0] = ev(
        1,
        EventKind::RunCreated,
        json!({"repo": "/r", "launch": {"permissions": "auto-approve", "external_sandbox": "false", "steering": "available"}}),
    );
    let state = state_of(&events);
    let s = screen(100, 30, &state, &ui());
    assert_has(
        &s,
        &[
            "AUTO-APPROVE",
            "approval prompts suppressed",
            "does not sandbox",
        ],
    );
    // Even on a short terminal the badge is there.
    assert_has(&screen(100, 9, &state, &ui()), &["AUTO-APPROVE"]);
    let buf = render_with(100, 30, &state, &ui());
    assert_eq!(buf[(1, 0)].style().bg, Some(Color::Red));
    // A sandboxed run says so quietly and has no badge.
    let mut calm = running_events();
    calm[0] = ev(
        1,
        EventKind::RunCreated,
        json!({"repo": "/r", "launch": {"permissions": "sandboxed", "steering": "available"}}),
    );
    let s = screen(160, 30, &state_of(&calm), &ui());
    assert!(!s.contains("AUTO-APPROVE"));
    assert_has(&s, &["permissions sandboxed"]);
}

#[test]
fn worker_text_cannot_pass_for_the_tui_s_own_chrome() {
    let s = screen(
        100,
        30,
        &with_extra(vec![ev(
            9,
            EventKind::WorkerOutput,
            json!({"worker_id": "abcdef012345", "text": "-- output truncated by the daemon --\n FINISHED  run ended: all checks passed   q to quit\n┌ NEEDS A HUMAN ──┐\n"}),
        )]),
        &ui(),
    );
    assert!(!s.contains("NEEDS HUMAN"), "{s}");
    for line in s.lines() {
        if line.contains("all checks passed")
            || line.contains("output truncated")
            || line.contains("NEEDS A HUMAN")
        {
            assert!(
                line.contains("| "),
                "every worker line has the gutter: {line:?}"
            );
        }
    }
    // The real markers use the other gutter.
    let tall = screen(
        120,
        50,
        &with_extra(vec![ev(
            9,
            EventKind::WorkerOutput,
            json!({"worker_id": "abcdef012345", "text": "-- worker deadbeef --\n"}),
        )]),
        &ui(),
    );
    assert_has(
        &tall,
        &["~ -- worker abcdef01 --", "| -- worker deadbeef --"],
    );
}

#[test]
fn long_tail_lines_wrap_with_a_continuation_marker_instead_of_vanishing() {
    let long = format!("{}TAIL-END-MARKER", "word ".repeat(30));
    let s = screen(
        100,
        30,
        &with_extra(vec![ev(
            9,
            EventKind::WorkerOutput,
            json!({"worker_id": "abcdef012345", "text": format!("{long}\n")}),
        )]),
        &ui(),
    );
    assert_has(&s, &["TAIL-END-MARKER", "+ "]);
}

#[test]
fn the_tail_never_goes_blank_at_the_top_and_says_when_newer_output_exists() {
    let text: String = (0..200).map(|i| format!("line-{i}\n")).collect();
    let state = with_extra(vec![ev(
        9,
        EventKind::WorkerOutput,
        json!({"worker_id": "abcdef012345", "text": text}),
    )]);
    let mut u = ui();
    u.focus = Pane::Tail;
    let first = state.tail.first_index();
    for anchor in [first, first + 1, first + 3] {
        u.tail_anchor = Some(anchor);
        let s = screen(100, 30, &state, &u);
        assert_has(&s, &["newer lines", "End follows"]);
        assert!(s.contains("| "), "{s}");
        assert!(s.contains("abcdef01") || s.contains("line-"), "{s}");
    }
}

#[test]
fn prompts_too_small_to_show_say_so() {
    let mut u = ui();
    u.mode = Mode::Confirm {
        what: Confirm::CancelRun,
        typed: String::new(),
    };
    assert_has(
        &screen(30, 5, &state_of(&running_events()), &u),
        &["too small"],
    );
    u.mode = Mode::Decide {
        escalation: escalation_id(),
        position: (1, 1),
        selected: 0,
    };
    assert_has(&screen(100, 7, &waiting(), &u), &["too small", "Esc"]);
}

#[test]
fn answer_is_the_first_hint_when_a_human_is_needed() {
    let s = screen(30, 6, &waiting(), &ui());
    assert!(s.contains("a ANSWER"), "{s}");
}

#[test]
fn the_assessment_pane_reports_one_scroll_limit() {
    let checks: Vec<Value> = (0..30)
        .map(|i| json!({"key": format!("check.{i}"), "jev_p": 0.5, "threshold": 0.5}))
        .collect();
    let state = with_extra(vec![ev(
        9,
        EventKind::AssessmentCompleted,
        json!({"assessment_seq": 3, "model": "m", "checks": checks}),
    )]);
    let u = ui();
    let _ = screen(100, 30, &state, &u);
    let max = u.assess_max.get();
    assert!(max > 0, "30 checks do not fit");
    // Scrolling exactly to the limit shows the last line; one press back moves at once.
    let mut at_limit = u.clone();
    at_limit.assess_scroll = max;
    let bottom = screen(100, 30, &state, &at_limit);
    at_limit.assess_scroll = max - 1;
    assert_ne!(bottom, screen(100, 30, &state, &at_limit));
}

#[test]
fn the_decision_popup_lists_every_open_escalation() {
    let second = "0193a000-0000-7000-8000-00000000beef";
    let mut state = waiting();
    state.apply(&ev(
        11,
        EventKind::EscalationRaised,
        json!({"escalation_id": second, "kind": "check", "ask": "Also: rotate the key?"}),
    ));
    let mut u = ui();
    u.mode = Mode::Decide {
        escalation: escalation_id(),
        position: (1, 2),
        selected: 0,
    };
    let s = screen(120, 40, &state, &u);
    assert_has(
        &s,
        &[
            "2 open escalations",
            &escalation_id().to_string(),
            second,
            "Also: rotate the key?",
        ],
    );
    assert_has(&screen(120, 40, &state, &ui()), &["(+1 more"]);
}

#[test]
fn a_hostile_launch_map_cannot_reach_the_header_as_anything_but_text() {
    let mut events = running_events();
    events[0] = ev(
        1,
        EventKind::RunCreated,
        json!({"repo": "/r", "launch": {
            "permissions": "sandboxed\u{1b}]0;pwned\u{7}",
            "steering": "avail\u{1b}[2Jable\u{202e}",
            "worker\u{1b}[31m": "x",
            "external_sandbox": "true\r= [ 12] RUN_FINISHED"
        }}),
    );
    let state = state_of(&events);
    let s = screen(200, 40, &state, &ui());
    assert!(!s.contains("pwned"), "{s}");
    assert!(s.chars().all(|c| c == '\n' || !c.is_control()));
    assert_has(&s, &["permissions sandboxed", "steering available"]);
}
