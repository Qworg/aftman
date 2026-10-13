//! Resolving the `<run>` argument: a full id or an unambiguous prefix.

use aftman_core::ids::RunId;

use crate::error::CliError;

/// Prefixes shorter than this are refused: a UUIDv7 prefix of one or two characters
/// names half the store, and an accidental match is worse than a clear error.
pub const MIN_PREFIX_LEN: usize = 4;

/// How many candidates an ambiguity message lists.
const MAX_LISTED: usize = 8;

/// What the user typed for `<run>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunSpec {
    /// A full id or a prefix.
    Id(String),
    /// `--last`: the newest run.
    Last,
}

impl RunSpec {
    /// Build from the `<run>` positional and the `--last` flag.
    ///
    /// # Errors
    /// Neither given, or both.
    pub fn from_args(run: Option<String>, last: bool) -> Result<Self, CliError> {
        match (run, last) {
            (Some(_), true) => Err(CliError::usage("give a run id or --last, not both")),
            (Some(run), false) => Ok(Self::Id(run)),
            (None, true) => Ok(Self::Last),
            (None, false) => Err(CliError::usage(
                "name a run (an id or a prefix), or pass --last",
            )),
        }
    }
}

/// Whether `text` parses as a complete run id.
#[must_use]
pub fn parse_full(text: &str) -> Option<RunId> {
    text.trim().parse().ok()
}

/// Pick the one id in `known` that starts with `prefix` (case-insensitive).
///
/// `known` is whatever the caller could list; a prefix cannot be resolved against
/// runs nobody listed, so callers list generously.
///
/// # Errors
/// Too short, no match, or more than one match (the message lists them).
pub fn resolve_prefix(prefix: &str, known: &[RunId]) -> Result<RunId, CliError> {
    let wanted = prefix.trim().to_ascii_lowercase();
    if wanted.len() < MIN_PREFIX_LEN {
        return Err(CliError::usage(format!(
            "`{prefix}` is too short to name a run; give at least {MIN_PREFIX_LEN} characters"
        )));
    }
    if !wanted.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
        return Err(CliError::usage(format!(
            "`{prefix}` is not a run id: ids are hexadecimal"
        )));
    }
    let matches: Vec<RunId> = known
        .iter()
        .copied()
        .filter(|id| id.to_string().starts_with(&wanted))
        .collect();
    match matches.as_slice() {
        [] => Err(CliError::usage(format!("no run matches `{prefix}`"))),
        [only] => Ok(*only),
        many => {
            let mut listed: Vec<String> = many
                .iter()
                .take(MAX_LISTED)
                .map(ToString::to_string)
                .collect();
            if many.len() > MAX_LISTED {
                listed.push(format!("... and {} more", many.len() - MAX_LISTED));
            }
            Err(CliError::usage(format!(
                "`{prefix}` matches {} runs; be more specific:\n  {}",
                many.len(),
                listed.join("\n  ")
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids() -> Vec<RunId> {
        [
            "0193a000-0000-7000-8000-000000000001",
            "0193a000-0000-7000-8000-000000000002",
            "0193b111-0000-7000-8000-000000000003",
        ]
        .iter()
        .filter_map(|s| s.parse().ok())
        .collect()
    }

    #[test]
    fn a_unique_prefix_resolves() {
        let known = ids();
        let got = resolve_prefix("0193b", &known);
        assert_eq!(got.ok(), Some(known[2]));
        let got = resolve_prefix("0193A000-0000-7000-8000-000000000002", &known);
        assert_eq!(got.ok(), Some(known[1]), "case-insensitive, full id");
    }

    #[test]
    fn an_ambiguous_prefix_lists_the_candidates() {
        let Err(error) = resolve_prefix("0193a", &ids()) else {
            panic!("should be ambiguous");
        };
        let text = error.to_string();
        assert!(text.contains("matches 2 runs"), "{text}");
        assert!(text.contains("000000000001") && text.contains("000000000002"));
        assert_eq!(error.exit_code(), 2);
    }

    #[test]
    fn short_unknown_and_non_hex_prefixes_are_usage_errors() {
        for bad in ["01", "ffff", "zzzzzz", ""] {
            let Err(error) = resolve_prefix(bad, &ids()) else {
                panic!("{bad} should fail");
            };
            assert_eq!(error.exit_code(), 2, "{bad}");
        }
    }

    #[test]
    fn run_or_last_but_not_both_or_neither() {
        assert_eq!(
            RunSpec::from_args(Some("abcd".into()), false).ok(),
            Some(RunSpec::Id("abcd".into()))
        );
        assert_eq!(RunSpec::from_args(None, true).ok(), Some(RunSpec::Last));
        assert!(RunSpec::from_args(None, false).is_err());
        assert!(RunSpec::from_args(Some("abcd".into()), true).is_err());
    }

    #[test]
    fn a_full_id_parses_without_a_listing() {
        assert!(parse_full("0193a000-0000-7000-8000-000000000001").is_some());
        assert!(parse_full("0193a000").is_none());
    }
}
