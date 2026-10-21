//! HTML output, and the one function every untrusted string must pass through.
//!
//! The rule from [04-workers.md](../../../docs/04-workers.md) applies to the browser exactly
//! as it applies to the terminal: worker output, job text, repository content, event
//! payloads and daemon errors reach the page **only** through [`escape`]. There is no
//! template engine, so there is no second path and no `|safe` to forget to omit.
//!
//! Deliberately no dependencies: the front end is server-rendered HTML with a little
//! vanilla JS, so there is no bundler and no Node in the runtime path.

use std::fmt::Write as _;

use time::macros::format_description;

/// Escape a string for HTML text and quoted-attribute contexts.
///
/// Escapes `&`, `<`, `>`, `"` and `'`. Not a sanitiser: a string that *is* HTML stays
/// escaped, which is what "there is no second path" means.
#[must_use]
pub fn escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 8);
    for character in value.chars() {
        match character {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#x27;"),
            // A NUL and other control characters have no meaning in HTML and would
            // survive into the DOM; drop the renderings that break parsing.
            '\u{0}' => out.push('\u{FFFD}'),
            other => out.push(other),
        }
    }
    out
}

/// Escape for an attribute value. Same mapping as [`escape`]; separate name so call sites
/// read as their context.
#[must_use]
pub fn attr(value: &str) -> String {
    escape(value)
}

/// A probability, to three decimals. `None` is an em dash rather than a blank, because a
/// missing measurement is a fact worth seeing.
#[must_use]
pub fn probability(value: Option<f32>) -> String {
    match value {
        Some(value) => format!("{value:.3}"),
        None => "\u{2014}".to_owned(),
    }
}

/// A threshold, or `none` for a check that cannot drive a threshold rule.
#[must_use]
pub fn threshold(value: Option<f32>) -> String {
    match value {
        Some(value) => format!("{value:.3}"),
        None => "none".to_owned(),
    }
}

/// `|p - t|`, the distance that `decisive` is measured against.
#[must_use]
pub fn distance(probability: Option<f32>, threshold: Option<f32>) -> String {
    match (probability, threshold) {
        (Some(p), Some(t)) => format!("{:.3}", (p - t).abs()),
        _ => "\u{2014}".to_owned(),
    }
}

/// A duration for a human: `1.2s`, `3m04s`, `2h07m`.
#[must_use]
pub fn duration(millis: u64) -> String {
    let seconds = millis / 1000;
    if seconds < 60 {
        format!("{}.{}s", seconds, (millis % 1000) / 100)
    } else if seconds < 3600 {
        format!("{}m{:02}s", seconds / 60, seconds % 60)
    } else {
        format!("{}h{:02}m", seconds / 3600, (seconds % 3600) / 60)
    }
}

/// A timestamp in UTC, to the second. The event log is the precise record; a page shows
/// enough to line events up.
#[must_use]
pub fn timestamp(at: time::OffsetDateTime) -> String {
    // A constant format, so this cannot fail for a reason a caller could act on; the
    // fallback exists because `format` returns a `Result` and a page must still render.
    at.format(&format_description!(
        "[year]-[month]-[day] [hour]:[minute]:[second]Z"
    ))
    .unwrap_or_else(|_| "?".to_owned())
}

/// The CSS class for a run status.
///
/// `NeedsHuman` and `Failed` are deliberately *not* the same class. Conflating "it broke"
/// with "a person must decide" is how a UI teaches an operator to ignore it.
#[must_use]
pub fn status_class(status: aftman_core::job::FactoryStatus) -> &'static str {
    use aftman_core::job::FactoryStatus;
    match status {
        FactoryStatus::NeedsHuman => "status status-needs-human",
        FactoryStatus::Failed => "status status-failed",
        FactoryStatus::Finished => "status status-finished",
        FactoryStatus::Running | FactoryStatus::Assessing | FactoryStatus::Verifying => {
            "status status-live"
        }
        FactoryStatus::Stopped => "status status-stopped",
        FactoryStatus::Queued | FactoryStatus::Routing => "status status-pending",
        // Cancelled is an ending a person asked for: neither a failure nor a finish.
        FactoryStatus::Cancelled => "status status-stopped",
    }
}

/// A short, human label for a status.
#[must_use]
pub fn status_label(status: aftman_core::job::FactoryStatus) -> &'static str {
    use aftman_core::job::FactoryStatus;
    match status {
        FactoryStatus::NeedsHuman => "needs a human",
        FactoryStatus::Failed => "failed",
        FactoryStatus::Finished => "finished",
        FactoryStatus::Running => "running",
        FactoryStatus::Assessing => "assessing",
        FactoryStatus::Verifying => "verifying",
        FactoryStatus::Stopped => "stopped",
        FactoryStatus::Queued => "queued",
        FactoryStatus::Routing => "routing",
        FactoryStatus::Cancelled => "cancelled",
    }
}

/// The full document. One shell for every page, so a change to the nav is one change.
#[must_use]
pub fn page(title: &str, body: &str, token_mode: bool) -> String {
    let mut out = String::with_capacity(body.len() + 2048);
    out.push_str("<!doctype html>\n<html lang=\"en\">\n<head>\n");
    out.push_str("<meta charset=\"utf-8\">\n");
    out.push_str("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n");
    out.push_str("<title>");
    out.push_str(&escape(title));
    out.push_str(" \u{2014} aftman</title>\n");
    out.push_str("<link rel=\"stylesheet\" href=\"/assets/app.css\">\n");
    out.push_str("</head>\n<body>\n");
    out.push_str("<header class=\"bar\">\n<nav>\n");
    out.push_str("<a class=\"brand\" href=\"/\">aftman</a>\n");
    out.push_str("<a href=\"/\">runs</a>\n");
    out.push_str("<a href=\"/new\">new job</a>\n");
    out.push_str("<a href=\"/usage\">usage</a>\n");
    out.push_str("<a href=\"/config\">config</a>\n");
    out.push_str("</nav>\n</header>\n<main>\n");
    out.push_str(body);
    out.push_str("\n</main>\n");
    if token_mode {
        out.push_str(
            "<footer class=\"note\">This surface requires a token. \
             <code>?token=</code> in a URL logs a browser in with a cookie; a URL \
             with a token can be written to a proxy log, so prefer the cookie once \
             you have it.</footer>\n",
        );
    }
    out.push_str("</body>\n</html>\n");
    out
}

/// A key/value table. Values are already-escaped HTML; keys are escaped here.
#[must_use]
pub fn kv_rows(rows: &[(&str, String)]) -> String {
    let mut out = String::new();
    for (key, value) in rows {
        let _ = writeln!(out, "<tr><th>{}</th><td>{value}</td></tr>", escape(key));
    }
    out
}

/// The banner for an error the page wants to report (a refused form, say).
#[must_use]
pub fn error_banner(message: &str) -> String {
    format!("<p class=\"banner banner-error\">{}</p>\n", escape(message))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_dangerous_character_is_escaped() {
        // The one function every untrusted string goes through. `<script>` is the case
        // that matters; the quote characters matter for attribute injection.
        assert_eq!(
            escape("<script>alert(\"x\")</script>"),
            "&lt;script&gt;alert(&quot;x&quot;)&lt;/script&gt;"
        );
        assert_eq!(escape("a & b"), "a &amp; b");
        assert_eq!(escape("' onload='x"), "&#x27; onload=&#x27;x");
    }

    #[test]
    fn an_attribute_break_out_is_neutralised() {
        // A value placed in a quoted attribute cannot close the quote.
        let hostile = "\" onclick=\"steal()";
        let rendered = format!("<input value=\"{}\">", attr(hostile));
        assert!(!rendered.contains("onclick=\"steal"), "{rendered}");
    }

    #[test]
    fn a_nul_does_not_survive() {
        assert!(!escape("a\u{0}b").contains('\u{0}'));
    }

    #[test]
    fn a_missing_measurement_is_visible_not_blank() {
        assert_eq!(probability(None), "\u{2014}");
        assert_eq!(probability(Some(0.5)), "0.500");
        assert_eq!(distance(None, Some(0.5)), "\u{2014}");
        assert_eq!(distance(Some(0.72), Some(0.5)), "0.220");
    }

    #[test]
    fn needs_human_and_failed_are_visually_distinct() {
        use aftman_core::job::FactoryStatus;
        assert_ne!(
            status_class(FactoryStatus::NeedsHuman),
            status_class(FactoryStatus::Failed)
        );
        assert_ne!(
            status_label(FactoryStatus::NeedsHuman),
            status_label(FactoryStatus::Failed)
        );
    }

    #[test]
    fn durations_read_like_durations() {
        assert_eq!(duration(0), "0.0s");
        assert_eq!(duration(1200), "1.2s");
        assert_eq!(duration(184_000), "3m04s");
        assert_eq!(duration(7_620_000), "2h07m");
    }
}
