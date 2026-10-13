//! Making untrusted text safe to print on an operator's terminal.
//!
//! Worker output, model reasons, escalation asks, job text and daemon error messages
//! all come from somewhere that is not this process, and a terminal obeys what it is
//! sent: OSC 0 retitles the window, OSC 52 writes the clipboard, `ESC[2J` clears the
//! screen, a bare CR overwrites the line a person just read. [`sanitize`] is the one
//! entry point; everything that reaches the terminal goes through it (see `out`).
//!
//! What it does, deliberately blunt:
//!
//! - **Escape sequences are removed whole**, not just their introducer: CSI
//!   (`ESC [ ... final`), OSC / DCS / SOS / PM / APC strings (up to BEL or ST), and the
//!   two-character forms (`ESC c`, `ESC 7`, `ESC ( B`). A sequence that never
//!   terminates swallows the rest of the text: dropping too much is the safe error.
//! - **C1 controls (U+0080-U+009F) are removed**; the single-character CSI (U+009B),
//!   OSC (U+009D), DCS (U+0090) and friends are treated like their ESC forms first, so
//!   `U+009D 0;title U+009C` does not leave `0;title` behind as visible text.
//! - **Bidirectional overrides and isolates** (U+202A-U+202E, U+2066-U+2069, plus the
//!   marks U+200E, U+200F, U+061C) are removed: they reorder what is displayed
//!   ("Trojan Source" style spoofing of a file name or a verdict).
//! - **Carriage return**: `CR LF` becomes `LF`; a lone `CR` becomes `LF`, so progress
//!   bars that redraw with `\r` print as successive lines instead of overwriting.
//! - **Other C0 controls are removed**: NUL, BEL, backspace, vertical tab, form feed,
//!   shift in/out, DEL.
//! - **Newline and tab are kept.** `logs` prints worker text, and its line structure and
//!   indentation are the text. There is no raw mode.
//!
//! Printable text, including non-ASCII, is left untouched.

/// Remove everything from `input` that could drive a terminal rather than be read.
#[must_use]
pub fn sanitize(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\u{1b}' => skip_escape(&mut chars),
            // C1 introducers behave like their ESC forms.
            '\u{9b}' => skip_csi(&mut chars),
            '\u{9d}' | '\u{90}' | '\u{98}' | '\u{9e}' | '\u{9f}' => skip_string(&mut chars),
            '\r' => {
                if chars.peek() == Some(&'\n') {
                    chars.next();
                }
                out.push('\n');
            }
            '\n' | '\t' => out.push(c),
            c if c.is_control() => {}
            '\u{202a}'..='\u{202e}'
            | '\u{2066}'..='\u{2069}'
            | '\u{200e}'
            | '\u{200f}'
            | '\u{061c}' => {}
            c => out.push(c),
        }
    }
    out
}

type Chars<'a> = std::iter::Peekable<std::str::Chars<'a>>;

/// After an `ESC`: consume the rest of the sequence it introduces.
fn skip_escape(chars: &mut Chars<'_>) {
    match chars.peek().copied() {
        Some('[') => {
            chars.next();
            skip_csi(chars);
        }
        // OSC, DCS, SOS, PM, APC: a string, ended by BEL or ST.
        Some(']' | 'P' | 'X' | '^' | '_') => {
            chars.next();
            skip_string(chars);
        }
        Some(c) if ('\u{20}'..='\u{2f}').contains(&c) => {
            // Intermediates (`ESC ( B`, `ESC # 8`): then one final byte.
            while chars
                .peek()
                .is_some_and(|c| ('\u{20}'..='\u{2f}').contains(c))
            {
                chars.next();
            }
            chars.next();
        }
        Some(_) => {
            // `ESC c`, `ESC 7`, `ESC =`, ...: one more character.
            chars.next();
        }
        None => {}
    }
}

/// Inside a CSI: parameters, intermediates, one final byte (`@`..`~`). A control
/// character inside aborts it (terminals do the same); the rest of the input is kept.
fn skip_csi(chars: &mut Chars<'_>) {
    while let Some(&c) = chars.peek() {
        match c {
            '\u{30}'..='\u{3f}' | '\u{20}'..='\u{2f}' => {
                chars.next();
            }
            '\u{40}'..='\u{7e}' => {
                chars.next();
                return;
            }
            _ => return,
        }
    }
}

/// Inside OSC / DCS / ...: skip to BEL, ST (`ESC \` or U+009C). Unterminated: to the end.
fn skip_string(chars: &mut Chars<'_>) {
    while let Some(c) = chars.next() {
        match c {
            '\u{7}' | '\u{9c}' => return,
            '\u{1b}' => {
                if chars.peek() == Some(&'\\') {
                    chars.next();
                }
                // Any ESC ends the string (a new sequence, or ST).
                return;
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::sanitize;

    #[test]
    fn plain_text_newlines_tabs_and_unicode_pass_through() {
        let text = "fn main() {\n\tprintln!(\"héllo, 世界 🦀\");\n}\n";
        assert_eq!(sanitize(text), text);
    }

    #[test]
    fn csi_sequences_are_removed_whole() {
        assert_eq!(sanitize("a\u{1b}[2Jb"), "ab");
        assert_eq!(sanitize("\u{1b}[31;1mred\u{1b}[0m"), "red");
        assert_eq!(sanitize("x\u{1b}[?1049hy"), "xy");
        assert_eq!(sanitize("x\u{1b}[1;2;3;4Hy"), "xy");
        // Unterminated at the end of input: nothing left over.
        assert_eq!(sanitize("ok\u{1b}[12;"), "ok");
        // A control character aborts it; the text after survives.
        assert_eq!(sanitize("a\u{1b}[1\nb"), "a\nb");
    }

    #[test]
    fn osc_title_and_clipboard_are_removed() {
        assert_eq!(sanitize("a\u{1b}]0;pwned\u{7}b"), "ab");
        assert_eq!(sanitize("a\u{1b}]52;c;SGVsbG8=\u{1b}\\b"), "ab");
        assert_eq!(
            sanitize("a\u{1b}]8;;http://evil\u{7}link\u{1b}]8;;\u{7}"),
            "alink"
        );
    }

    #[test]
    fn unterminated_strings_swallow_the_rest() {
        assert_eq!(sanitize("safe\u{1b}]0;never ends"), "safe");
        assert_eq!(sanitize("safe\u{1b}Pq#0;2;0;0;0 never ends"), "safe");
        assert_eq!(sanitize("safe\u{1b}_Gi=1;payload"), "safe");
        assert_eq!(sanitize("safe\u{1b}^private"), "safe");
        assert_eq!(sanitize("safe\u{1b}Xsos"), "safe");
    }

    #[test]
    fn dcs_with_st_terminator() {
        assert_eq!(sanitize("a\u{1b}P1$r0m\u{1b}\\b"), "ab");
    }

    #[test]
    fn short_escapes_are_removed() {
        assert_eq!(sanitize("a\u{1b}cb"), "ab");
        assert_eq!(sanitize("a\u{1b}7b\u{1b}8c"), "abc");
        assert_eq!(sanitize("a\u{1b}(Bb"), "ab");
        assert_eq!(sanitize("a\u{1b}#8b"), "ab");
        assert_eq!(sanitize("trailing\u{1b}"), "trailing");
    }

    #[test]
    fn c1_controls_are_removed_and_their_introducers_obeyed() {
        assert_eq!(sanitize("a\u{85}b\u{80}c\u{9a}d"), "abcd");
        assert_eq!(
            sanitize("a\u{9f}apc payload"),
            "a",
            "APC swallows what follows"
        );
        assert_eq!(sanitize("a\u{9b}2Jb"), "ab");
        assert_eq!(sanitize("a\u{9d}0;title\u{9c}b"), "ab");
        assert_eq!(sanitize("a\u{90}payload\u{9c}b"), "ab");
        assert_eq!(sanitize("a\u{9d}0;never ends"), "a");
    }

    #[test]
    fn bidi_overrides_and_isolates_are_removed() {
        for c in [
            '\u{202a}', '\u{202b}', '\u{202c}', '\u{202d}', '\u{202e}', '\u{2066}', '\u{2067}',
            '\u{2068}', '\u{2069}', '\u{200e}', '\u{200f}', '\u{061c}',
        ] {
            assert_eq!(sanitize(&format!("a{c}b")), "ab", "U+{:04X}", c as u32);
        }
        assert_eq!(sanitize("exe\u{202e}txt.pdf"), "exetxt.pdf");
    }

    #[test]
    fn carriage_return_cannot_overwrite_the_line() {
        assert_eq!(sanitize("PASS\rFAIL"), "PASS\nFAIL");
        assert_eq!(sanitize("a\r\nb"), "a\nb");
        assert_eq!(sanitize("a\r\r\nb"), "a\n\nb");
        assert_eq!(sanitize("50%\r100%\r"), "50%\n100%\n");
    }

    #[test]
    fn backspace_nul_del_and_other_c0_are_removed() {
        assert_eq!(sanitize("ab\u{8}\u{8}cd"), "abcd");
        assert_eq!(
            sanitize("a\0b\u{7f}c\u{7}d\u{b}e\u{c}f\u{e}g\u{f}h"),
            "abcdefgh"
        );
    }

    #[test]
    fn the_reviewers_attack_leaves_only_text() {
        let attack =
            "\u{1b}]0;hacked\u{7}\u{1b}]52;c;cm0gLXJmIH4=\u{7}\u{1b}[2J\u{1b}[Hall tests passed";
        assert_eq!(sanitize(attack), "all tests passed");
    }

    #[test]
    fn the_result_never_contains_a_control_character_but_newline_and_tab() {
        let nasty: String = (0u32..0x200)
            .filter_map(char::from_u32)
            .chain("\u{1b}[\u{1b}]\u{1b}P".chars())
            .collect();
        for c in sanitize(&nasty).chars() {
            assert!(
                !c.is_control() || c == '\n' || c == '\t',
                "U+{:04X}",
                c as u32
            );
        }
    }
}
