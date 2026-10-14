//! Removing terminal control sequences from text destined for a model.
//!
//! A worker's output arrives from a terminal-oriented program, so it is full of things
//! that exist for a terminal and mean nothing to a model: colour codes, cursor movement,
//! progress-bar redraws, window titles. They cost tokens and they get in the way of the
//! text being judged.
//!
//! # Why this is not the CLI's `sanitize`
//!
//! `aftman-cli/src/sanitize.rs` also removes escape sequences, and the overlap is real.
//! The two are nevertheless separate, because they are answering different questions and
//! one of them would be wrong to reuse here:
//!
//! - The CLI is making text safe to **print on a terminal**. It must defeat OSC 52
//!   clipboard writes, window retitling, and bidirectional-override spoofing of a file
//!   name or a verdict, and it caps zero-width marks so one cell cannot carry hundreds of
//!   code points.
//! - This module is making text **useful to a model**. It removes control sequences
//!   because they are noise, and it deliberately leaves bidirectional marks and other
//!   invisible formatting alone: a bidi override inside a string literal is *content* that
//!   a coding model may need to see verbatim, and a model is not a terminal that can be
//!   tricked into reordering a line for a human. Stripping it here would silently change
//!   the code being judged.
//!
//! Core also cannot depend on the CLI, and moving `sanitize` down would drag
//! `unicode_width` and the spoofing rules into the domain layer for no benefit.
//!
//! # What it does
//!
//! - Escape sequences are removed whole: CSI (`ESC [ … final`), the string forms
//!   (OSC, DCS, SOS, PM, APC) up to `BEL` or `ST`, and the two-character forms
//!   (`ESC c`, `ESC 7`, `ESC ( B`). A sequence that never terminates consumes the rest of
//!   the input — dropping too much is the safe error, and a model must never be handed
//!   half a control sequence as though it were prose.
//! - C1 controls (`U+0080`–`U+009F`) are removed, with the single-character CSI/OSC/DCS
//!   forms handled like their `ESC` equivalents so their payload does not survive as
//!   visible text.
//! - Other C0 controls are removed, except newline and tab, which carry the structure an
//!   indented log or a diff needs.
//! - A carriage return becomes a newline: a progress bar that redraws with `\r` should
//!   read as successive lines rather than as one overwritten line.

/// Remove terminal control sequences, keeping newline and tab.
#[must_use]
pub fn strip_control(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '\u{1b}' => skip_escape(&mut chars),
            // A single-character C1 control is the same sequence with its introducer
            // folded into one code point.
            '\u{9b}' => skip_csi(&mut chars),
            '\u{9d}' | '\u{90}' | '\u{98}' | '\u{9e}' | '\u{9f}' => skip_string(&mut chars),
            '\n' | '\t' => out.push(c),
            '\r' => out.push('\n'),
            // Remaining C0 and all C1.
            c if (c as u32) < 0x20 || (0x7f..=0x9f).contains(&(c as u32)) => {}
            c => out.push(c),
        }
    }
    out
}

/// Skip the body of an `ESC`-introduced sequence.
fn skip_escape(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    match chars.peek().copied() {
        // CSI: parameter and intermediate bytes, then a final byte in 0x40..=0x7E.
        Some('[') => {
            chars.next();
            skip_csi(chars);
        }
        // OSC, DCS, SOS, PM, APC: a string terminated by BEL or ST.
        Some(']') | Some('P') | Some('X') | Some('^') | Some('_') => {
            chars.next();
            skip_string(chars);
        }
        // A two-character form (`ESC c`, `ESC 7`, `ESC =`, …).
        Some(_) => {
            chars.next();
        }
        None => {}
    }
}

/// Skip a CSI body, stopping after its final byte.
fn skip_csi(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    for c in chars.by_ref() {
        // Parameter bytes 0x30..=0x3F, intermediate bytes 0x20..=0x2F, then a final byte.
        if (0x40..=0x7e).contains(&(c as u32)) {
            return;
        }
    }
}

/// Skip a control string, stopping at BEL or ST (`ESC \`).
fn skip_string(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    while let Some(c) = chars.next() {
        match c {
            '\u{7}' | '\u{9c}' => return,
            '\u{1b}' if chars.peek() == Some(&'\\') => {
                chars.next();
                return;
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::strip_control;

    #[test]
    fn colour_codes_are_removed_but_the_text_survives() {
        assert_eq!(strip_control("\u{1b}[31mred\u{1b}[0m"), "red");
        assert_eq!(strip_control("\u{1b}[1;32mok\u{1b}[m"), "ok");
    }

    #[test]
    fn cursor_movement_and_clear_screen_are_removed() {
        assert_eq!(strip_control("\u{1b}[2J\u{1b}[Hvisible"), "visible");
        assert_eq!(strip_control("\u{1b}[10;20Hmoved"), "moved");
    }

    #[test]
    fn a_window_title_does_not_become_text() {
        // OSC 0 with a BEL terminator.
        assert_eq!(strip_control("\u{1b}]0;hacked\u{7}after"), "after");
        // And with an ST terminator.
        assert_eq!(strip_control("\u{1b}]0;hacked\u{1b}\\after"), "after");
    }

    #[test]
    fn a_clipboard_write_does_not_become_text() {
        assert_eq!(strip_control("\u{1b}]52;c;cm0gLXJm\u{7}after"), "after");
    }

    #[test]
    fn the_single_character_c1_forms_are_handled_like_their_esc_equivalents() {
        // U+009B is CSI in one code point; leaving its payload behind would put `31m`
        // in front of a model as though it were prose.
        assert_eq!(strip_control("\u{9b}31mred"), "red");
        assert_eq!(strip_control("\u{9d}0;title\u{9c}after"), "after");
    }

    #[test]
    fn an_unterminated_sequence_swallows_the_rest() {
        // Dropping too much is the safe error: half a control sequence must never reach a
        // model as prose.
        assert_eq!(strip_control("\u{1b}]0;never ends"), "");
        assert_eq!(strip_control("before\u{1b}"), "before");
    }

    #[test]
    fn carriage_returns_become_newlines() {
        // A progress bar redrawn with CR should read as successive lines.
        assert_eq!(strip_control("10%\r50%\r100%"), "10%\n50%\n100%");
        assert_eq!(strip_control("line\r\nnext"), "line\n\nnext");
    }

    #[test]
    fn newline_and_tab_are_kept() {
        // A diff's indentation and a log's line structure are the text.
        assert_eq!(strip_control("a\n\tb"), "a\n\tb");
    }

    #[test]
    fn other_control_characters_are_removed() {
        assert_eq!(strip_control("a\u{0}b\u{7}c\u{8}d\u{7f}e"), "abcde");
    }

    #[test]
    fn bidirectional_and_zero_width_marks_are_left_alone() {
        // Deliberately different from the CLI's terminal sanitizer: these are *content* a
        // coding model may need to see, not terminal control, so stripping them would
        // silently change the code being judged.
        for mark in ['\u{202e}', '\u{200e}', '\u{200b}', '\u{feff}'] {
            let text = format!("a{mark}b");
            assert_eq!(strip_control(&text), text, "{mark:?} must survive");
        }
    }

    #[test]
    fn ordinary_text_including_non_ascii_is_untouched() {
        for text in [
            "plain",
            "path/to/file.rs",
            "3 passed, 1 failed",
            "émoji 🎉 and ünïcode",
            "{\"json\": true}",
        ] {
            assert_eq!(strip_control(text), text, "{text:?}");
        }
    }

    #[test]
    fn jsonl_output_keeps_its_structure() {
        // The observation carries a structured backend's stream, so newlines matter even
        // though the escapes do not.
        let stream = "{\"type\":\"a\"}\n{\"type\":\"b\"}\n";
        assert_eq!(strip_control(stream), stream);
    }

    #[test]
    fn stripping_is_idempotent() {
        let dirty = "\u{1b}[31mred\u{1b}[0m\r\n\t\u{1b}]0;t\u{7}done";
        let once = strip_control(dirty);
        assert_eq!(strip_control(&once), once);
        assert_eq!(once, "red\n\n\tdone");
    }

    #[test]
    fn an_empty_string_is_empty() {
        assert_eq!(strip_control(""), "");
    }
}
