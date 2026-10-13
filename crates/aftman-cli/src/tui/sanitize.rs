//! Untrusted text.
//!
//! Worker output, job text, escalation asks and model reasons come from a coding
//! agent and from repository content. None of it may reach the terminal as anything
//! but inert, printable text: a worker must not be able to move the cursor, retitle
//! the window, write the clipboard (OSC 52), hide text behind a carriage return, flip
//! the reading order (bidi overrides) or draw something that looks like our own UI.
//!
//! [`sanitize`] is the one function everything passes through. The reducer applies it
//! to every string it takes from an event (`state.rs`), the input editor applies it to
//! pasted text, and error messages from the daemon pass through it before the footer.
//! The view therefore only ever sees clean strings; there is deliberately no second,
//! slightly different sanitiser anywhere else.
//!
//! What it does, in order of how dangerous the input is:
//!
//! - **Escape sequences are removed whole**, not just their ESC byte: CSI
//!   (`ESC [ ... final`), OSC / DCS / SOS / PM / APC strings (up to BEL or ST, or to
//!   the end of the text when unterminated), and two-character escapes. The 8-bit C1
//!   forms (U+009B CSI, U+009D OSC, ...) are treated the same way.
//! - **Controls are dropped**: every other C0 control, DEL and the remaining C1
//!   controls. Tab becomes four spaces. NEL, U+2028 and U+2029 become newlines.
//! - **Carriage return becomes a line break.** A terminal would let `ok\rFAIL` show only
//!   `FAIL`; here both parts stay visible on their own lines. `\r\n` is one break.
//! - **Format characters are dropped**: bidi embeddings, overrides and isolates,
//!   zero-width space/joiners, word joiner, BOM, soft hyphen and the Unicode tag block.
//!
//! Everything else, including wide (CJK, emoji) and combining characters, is kept;
//! [`clip`] and [`wrap`] measure by terminal columns, not by chars.

use unicode_width::{UnicodeWidthChar as _, UnicodeWidthStr as _};

/// Columns a tab expands to.
const TAB_COLS: usize = 4;

/// The most characters [`clean_field`] will look at in one field. Escapes are removed
/// *before* the display cap applies, so a field cannot hide its content behind a long
/// run of them; this bound only keeps the work finite.
const MAX_RAW_FIELD_CHARS: usize = 1 << 20;

/// Strip control characters and escape sequences from `text` (the shared
/// [`crate::sanitize::sanitize`]), and expand tabs to four spaces, which the panes
/// cannot align. The result contains printable characters and `\n` only.
#[must_use]
pub fn sanitize(text: &str) -> String {
    let clean = crate::sanitize::sanitize(text);
    if clean.contains('\t') {
        clean.replace('\t', &" ".repeat(TAB_COLS))
    } else {
        clean
    }
}

/// [`sanitize`], then fold line breaks into single spaces: for a value shown on one
/// line (a reason, a job excerpt, an error message).
#[must_use]
pub fn sanitize_inline(text: &str) -> String {
    let clean = sanitize(text);
    let mut out = String::with_capacity(clean.len());
    for (i, line) in clean.split('\n').enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if i > 0 && !out.is_empty() {
            out.push(' ');
        }
        out.push_str(line);
    }
    out
}

/// [`sanitize`] for bytes that may not be UTF-8: invalid sequences become U+FFFD.
#[must_use]
pub fn sanitize_bytes(bytes: &[u8]) -> String {
    sanitize(&String::from_utf8_lossy(bytes))
}

/// Keep at most `max_chars` characters, ending in `…` when anything was cut. Applied
/// before [`sanitize`] on untrusted input so the work is bounded by the cap, not by the
/// size of what a worker printed.
#[must_use]
pub fn truncate_chars(text: &str, max_chars: usize) -> String {
    if text.chars().nth(max_chars).is_none() {
        return text.to_owned();
    }
    let mut out: String = text.chars().take(max_chars.saturating_sub(1)).collect();
    out.push('…');
    out
}

/// Sanitise `text` after capping it at `max_chars` characters (inline form).
#[must_use]
pub fn clean_field(text: &str, max_chars: usize) -> String {
    // Sanitise first, then cap: the cap is on what is shown, and escapes must not be
    // able to use it up. Only the (very generous) raw bound is applied beforehand.
    let bounded: String = text.chars().take(MAX_RAW_FIELD_CHARS).collect();
    truncate_chars(&sanitize_inline(&bounded), max_chars)
}

/// Terminal columns `text` occupies. Control characters count as zero.
#[must_use]
pub fn width(text: &str) -> usize {
    text.width()
}

/// Cut `text` to at most `max_cols` terminal columns, ending in `…` when cut. Never
/// splits a wide character.
#[must_use]
pub fn clip(text: &str, max_cols: usize) -> String {
    if max_cols == 0 {
        return String::new();
    }
    if text.width() <= max_cols {
        return text.to_owned();
    }
    let budget = max_cols - 1;
    let mut out = String::new();
    let mut used = 0;
    for c in text.chars() {
        let w = c.width().unwrap_or(0);
        if used + w > budget {
            break;
        }
        used += w;
        out.push(c);
    }
    out.push('…');
    out
}

/// Break `text` into lines of at most `max_cols` columns, at spaces where possible and
/// inside a word when one is longer than a line. Existing line breaks are kept. At most
/// `max_lines` lines come back; if there was more, the last one ends in `…`.
#[must_use]
pub fn wrap(text: &str, max_cols: usize, max_lines: usize) -> Vec<String> {
    if max_cols == 0 || max_lines == 0 {
        return Vec::new();
    }
    let mut lines: Vec<String> = Vec::new();
    for paragraph in text.split('\n') {
        let mut current = String::new();
        let mut current_cols = 0;
        for word in paragraph.split(' ').filter(|w| !w.is_empty()) {
            let mut word = word;
            loop {
                let word_cols = word.width();
                let sep = usize::from(current_cols > 0);
                if current_cols + sep + word_cols <= max_cols {
                    if sep == 1 {
                        current.push(' ');
                    }
                    current.push_str(word);
                    current_cols += sep + word_cols;
                    break;
                }
                if current_cols > 0 {
                    lines.push(std::mem::take(&mut current));
                    current_cols = 0;
                    continue;
                }
                // A word wider than a whole line: hard-split it.
                let (head, tail) = split_cols(word, max_cols);
                if head.is_empty() {
                    // A single character wider than the line (width 1 line, wide char):
                    // drop it rather than loop forever.
                    word = tail
                        .split_at(tail.chars().next().map_or(0, char::len_utf8))
                        .1;
                    if word.is_empty() {
                        break;
                    }
                    continue;
                }
                lines.push(head.to_owned());
                word = tail;
                if word.is_empty() {
                    break;
                }
            }
            if lines.len() > max_lines {
                break;
            }
        }
        lines.push(current);
        if lines.len() > max_lines {
            break;
        }
    }
    if lines.len() > max_lines {
        lines.truncate(max_lines);
        if let Some(last) = lines.last_mut() {
            *last = clip(&format!("{last} …"), max_cols);
            if !last.ends_with('…') {
                last.push('…');
            }
        }
    }
    lines
}

/// Break `text` into rows of exactly `cols` columns each (the last may be shorter),
/// keeping every space: for code and logs, where indentation means something. At most
/// `max_rows` rows come back; if there was more, the last one ends in `…`.
#[must_use]
pub fn hard_wrap(text: &str, cols: usize, max_rows: usize) -> Vec<String> {
    if cols == 0 || max_rows == 0 {
        return Vec::new();
    }
    let mut rows = Vec::new();
    let mut rest = text;
    loop {
        if rest.width() <= cols {
            rows.push(rest.to_owned());
            break;
        }
        let (head, tail) = split_cols(rest, cols);
        if head.is_empty() {
            // A wide character in a one-column row: drop it, never loop.
            let skip = tail.chars().next().map_or(0, char::len_utf8);
            rest = &tail[skip..];
            if rest.is_empty() {
                break;
            }
            continue;
        }
        rows.push(head.to_owned());
        rest = tail;
        if rows.len() >= max_rows {
            if let Some(last) = rows.last_mut() {
                *last = clip(&format!("{last}…"), cols);
                if !last.ends_with('…') {
                    last.pop();
                    last.push('…');
                }
            }
            break;
        }
    }
    rows
}

/// Shorten `text` to `cols` columns by replacing the middle with `…`, keeping both
/// ends (a path is recognisable by its start and its file name).
#[must_use]
pub fn elide_middle(text: &str, cols: usize) -> String {
    if text.width() <= cols {
        return text.to_owned();
    }
    if cols <= 1 {
        return clip(text, cols);
    }
    let budget = cols - 1;
    let tail_cols = budget / 2;
    let head_cols = budget - tail_cols;
    let (head, _) = split_cols(text, head_cols);
    let mut tail_chars: Vec<char> = Vec::new();
    let mut used = 0;
    for c in text.chars().rev() {
        let w = c.width().unwrap_or(0);
        if used + w > tail_cols {
            break;
        }
        used += w;
        tail_chars.push(c);
    }
    let tail: String = tail_chars.into_iter().rev().collect();
    format!("{head}…{tail}")
}

/// Split `text` after the longest prefix that fits in `cols` columns.
fn split_cols(text: &str, cols: usize) -> (&str, &str) {
    let mut used = 0;
    for (index, c) in text.char_indices() {
        let w = c.width().unwrap_or(0);
        if used + w > cols {
            return text.split_at(index);
        }
        used += w;
    }
    (text, "")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_field_cannot_hide_behind_a_run_of_escapes() {
        let hostile = "\u{1b}[m".repeat(100_000) + "REAL ASK";
        assert_eq!(clean_field(&hostile, 100), "REAL ASK");
        assert_eq!(clean_field(&hostile, 4), "REA…");
    }

    #[test]
    fn arabic_letter_mark_and_other_invisibles_are_dropped() {
        assert_eq!(sanitize("a\u{61c}b\u{34f}c\u{180e}d"), "abcd");
    }

    #[test]
    fn combining_marks_are_capped_per_base_character() {
        let zalgo = format!("e{}x", "\u{301}".repeat(500));
        let clean = sanitize(&zalgo);
        assert_eq!(clean.chars().count(), 2 + 4);
        assert!(clean.ends_with('x'));
        // Variation selectors count the same way, and a new base resets the count.
        let selectors = format!("a{}b\u{301}", "\u{fe0f}".repeat(50));
        assert_eq!(sanitize(&selectors).chars().count(), 2 + 4 + 1);
        assert_eq!(
            sanitize("e\u{301}e\u{301}e\u{301}"),
            "e\u{301}e\u{301}e\u{301}"
        );
    }

    #[test]
    fn hard_wrap_keeps_spaces_and_bounds_the_rows() {
        assert_eq!(
            hard_wrap("    indented code", 8, 10),
            vec!["    inde", "nted cod", "e"]
        );
        assert_eq!(hard_wrap("short", 10, 10), vec!["short"]);
        assert_eq!(hard_wrap("", 10, 10), vec![""]);
        let rows = hard_wrap(&"x".repeat(1000), 10, 3);
        assert_eq!(rows.len(), 3);
        assert!(rows[2].ends_with('…') && width(&rows[2]) <= 10);
        assert!(hard_wrap("x", 0, 3).is_empty());
        for cols in 1..6 {
            for row in hard_wrap("日本語 abc 日", cols, 50) {
                assert!(width(&row) <= cols.max(2), "{row:?} in {cols}");
            }
        }
    }

    #[test]
    fn elide_middle_keeps_both_ends() {
        assert_eq!(
            elide_middle("/home/user/projects/repo", 40),
            "/home/user/projects/repo"
        );
        let short = elide_middle("/home/user/projects/very/deep/repo-name", 20);
        assert_eq!(width(&short), 20);
        assert!(short.starts_with("/home/") && short.ends_with("repo-name") && short.contains('…'));
        assert_eq!(elide_middle("abcdef", 1), "…");
        assert_eq!(elide_middle("abcdef", 0), "");
    }

    #[test]
    fn plain_text_is_unchanged() {
        assert_eq!(
            sanitize("hello, wörld\nsecond line"),
            "hello, wörld\nsecond line"
        );
        assert_eq!(sanitize(""), "");
    }

    #[test]
    fn csi_sequences_are_removed_whole() {
        assert_eq!(sanitize("a\u{1b}[31mred\u{1b}[0mb"), "aredb");
        assert_eq!(sanitize("\u{1b}[2J\u{1b}[H gone"), " gone");
        assert_eq!(sanitize("x\u{1b}[?1049hy"), "xy");
        assert_eq!(sanitize("x\u{1b}[38;2;1;2;3;4mq"), "xq");
        // 8-bit CSI.
        assert_eq!(sanitize("x\u{9b}31my"), "xy");
        // A CSI cut off by the end of the text leaves nothing behind.
        assert_eq!(sanitize("x\u{1b}[31"), "x");
        // A CSI aborted by a control character resumes at that character.
        assert_eq!(sanitize("x\u{1b}[3\ny"), "x\ny");
    }

    #[test]
    fn osc_sequences_cannot_retitle_or_write_the_clipboard() {
        assert_eq!(sanitize("a\u{1b}]0;pwned\u{7}b"), "ab");
        assert_eq!(sanitize("a\u{1b}]52;c;ZXZpbA==\u{1b}\\b"), "ab");
        assert_eq!(sanitize("a\u{9d}0;title\u{9c}b"), "ab");
        // Unterminated: the rest of the text is part of the string, none of it shows.
        assert_eq!(sanitize("a\u{1b}]0;never ends"), "a");
        // Hyperlinks (OSC 8) lose the link and keep the label.
        assert_eq!(
            sanitize("\u{1b}]8;;http://evil\u{7}label\u{1b}]8;;\u{7}"),
            "label"
        );
    }

    #[test]
    fn dcs_apc_pm_sos_strings_are_removed() {
        assert_eq!(sanitize("a\u{1b}Pq data\u{1b}\\b"), "ab");
        assert_eq!(sanitize("a\u{1b}_Gi=1;AAAA\u{1b}\\b"), "ab");
        assert_eq!(sanitize("a\u{1b}^pm\u{7}b"), "ab");
        assert_eq!(sanitize("a\u{1b}Xsos\u{7}b"), "ab");
    }

    #[test]
    fn short_escapes_are_removed() {
        assert_eq!(sanitize("a\u{1b}cb"), "ab"); // RIS, a full terminal reset
        assert_eq!(sanitize("a\u{1b}(Bb"), "ab"); // charset designation
        assert_eq!(sanitize("a\u{1b}#8b"), "ab"); // DECALN screen fill
        assert_eq!(sanitize("a\u{1b}7b\u{1b}8"), "ab");
        // A lone trailing ESC, and ESC ESC.
        assert_eq!(sanitize("a\u{1b}"), "a");
        assert_eq!(sanitize("a\u{1b}\u{1b}[31mb"), "ab");
    }

    #[test]
    fn c0_and_c1_controls_are_dropped() {
        assert_eq!(
            sanitize("a\u{0}b\u{7}c\u{8}d\u{b}e\u{c}f\u{7f}g"),
            "abcdefg"
        );
        assert_eq!(sanitize("a\u{80}b\u{91}c\u{99}d"), "abcd");
        // Every control character except \n is gone from the output.
        let everything: String = (0u32..0xa0).filter_map(char::from_u32).collect();
        let clean = sanitize(&everything);
        assert!(
            clean.chars().all(|c| !c.is_control() || c == '\n'),
            "{clean:?}"
        );
    }

    #[test]
    fn tabs_become_spaces_and_unicode_line_breaks_become_newlines() {
        assert_eq!(sanitize("a\tb"), "a    b");
        assert_eq!(sanitize("a\u{85}b\u{2028}c\u{2029}d"), "ab\nc\nd");
    }

    #[test]
    fn carriage_return_cannot_overwrite_earlier_text() {
        // On a terminal this shows only "FAIL".
        assert_eq!(sanitize("all tests passed\rFAIL"), "all tests passed\nFAIL");
        assert_eq!(sanitize("a\r\nb"), "a\nb");
        assert_eq!(
            sanitize("progress 10%\rprogress 20%\r"),
            "progress 10%\nprogress 20%\n"
        );
    }

    #[test]
    fn bidi_overrides_and_invisible_format_characters_are_dropped() {
        // The classic "trojan source" reordering and an RLO filename trick.
        assert_eq!(sanitize("a\u{202e}txt.exe\u{202c}b"), "atxt.exeb");
        assert_eq!(sanitize("a\u{2066}b\u{2069}c\u{200e}d\u{200f}"), "abcd");
        assert_eq!(
            sanitize("a\u{200b}b\u{200d}c\u{2060}d\u{feff}e\u{ad}f"),
            "abcdef"
        );
        // Tag characters spelling an invisible instruction.
        assert_eq!(sanitize("hi\u{e0049}\u{e0067}\u{e006e}"), "hi");
    }

    #[test]
    fn wide_and_combining_characters_survive() {
        assert_eq!(sanitize("日本語 ✓ e\u{301}"), "日本語 ✓ e\u{301}");
        assert_eq!(width("日本語"), 6);
        assert_eq!(width("e\u{301}"), 1);
    }

    #[test]
    fn invalid_utf8_becomes_the_replacement_character() {
        assert_eq!(
            sanitize_bytes(b"ok \xff\xfe bad \x1b[31m"),
            "ok \u{fffd}\u{fffd} bad "
        );
    }

    #[test]
    fn inline_form_is_one_line() {
        assert_eq!(sanitize_inline("a\n\n  b \r c"), "a b c");
        assert_eq!(sanitize_inline("\u{1b}[1m\n\n"), "");
    }

    #[test]
    fn clip_counts_columns_and_never_splits_a_wide_character() {
        assert_eq!(clip("hello", 10), "hello");
        assert_eq!(clip("hello world", 6), "hello…");
        assert_eq!(clip("日本語日本語", 5), "日本…");
        assert_eq!(clip("日本語", 6), "日本語");
        assert_eq!(clip("abc", 0), "");
        assert_eq!(clip("abc", 1), "…");
        assert_eq!(clip("日本", 1), "…");
    }

    #[test]
    fn a_very_long_line_is_bounded_by_the_cap_not_by_its_length() {
        let huge = "x".repeat(5_000_000);
        let field = clean_field(&huge, 100);
        assert_eq!(field.chars().count(), 100);
        assert!(field.ends_with('…'));
        assert_eq!(truncate_chars("short", 100), "short");
        assert_eq!(truncate_chars("abcdef", 4), "abc…");
    }

    #[test]
    fn wrap_breaks_at_spaces_and_inside_long_words() {
        assert_eq!(
            wrap("the quick brown fox", 9, 10),
            vec!["the quick", "brown fox"]
        );
        assert_eq!(wrap("abcdefghij", 4, 10), vec!["abcd", "efgh", "ij"]);
        assert_eq!(wrap("a\n\nb", 10, 10), vec!["a", "", "b"]);
        assert_eq!(wrap("日本語日本語", 4, 10), vec!["日本", "語日", "本語"]);
        for line in wrap("x ".repeat(100).as_str(), 7, 100) {
            assert!(width(&line) <= 7);
        }
    }

    #[test]
    fn wrap_is_bounded_and_survives_degenerate_widths() {
        let lines = wrap(&"word ".repeat(100), 10, 3);
        assert_eq!(lines.len(), 3);
        assert!(lines[2].ends_with('…'));
        assert!(wrap("anything", 0, 5).is_empty());
        assert!(wrap("anything", 5, 0).is_empty());
        // A wide character in a one-column line cannot fit: no infinite loop.
        let _ = wrap("日本語", 1, 10);
        for cols in 1..6 {
            for line in wrap("日本語 abc 日", cols, 50) {
                assert!(
                    width(&line) <= cols.max(1) || cols < 2,
                    "{line:?} in {cols}"
                );
            }
        }
    }

    #[test]
    fn the_output_of_sanitize_is_always_clean() {
        // A deterministic mixed bag standing in for fuzzing.
        let fragments = [
            "\u{1b}[",
            "\u{1b}]0;",
            "\u{7}",
            "\u{1b}\\",
            "\r",
            "\u{9b}",
            "\u{202e}",
            "ok",
            "日",
            "\u{1b}",
            "\t",
            "\u{0}",
            "[31m",
            "\n",
        ];
        for seed in 0..2000u32 {
            let mut s = String::new();
            let mut n = seed;
            for _ in 0..12 {
                s.push_str(fragments[(n % fragments.len() as u32) as usize]);
                n = n.wrapping_mul(1_103_515_245).wrapping_add(12_345) >> 3;
            }
            let clean = sanitize(&s);
            assert!(
                clean.chars().all(|c| c == '\n' || !c.is_control()),
                "{s:?} -> {clean:?}"
            );
            assert!(!clean.contains('\u{1b}') && !clean.contains('\r'));
        }
    }
}
