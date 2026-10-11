//! The `when` grammar for declarative rules.
//!
//! Deliberately tiny, so that evaluation is total and unambiguous:
//!
//! ```text
//! condition   := conjunction ("or" conjunction)*
//! conjunction := term ("and" term)*
//! term        := check_key comparator number | "not" "(" condition ")"
//! comparator  := ">=" | ">" | "<=" | "<"
//! check_key   := "<responsibility_id>__<check_id>"
//! ```
//!
//! Two properties matter more than the grammar itself:
//!
//! 1. **A comparison reads `permitted` first.** A term is true only when the check's
//!    measurement is decisive and uncontested *and* the comparison holds. That is what
//!    makes an in-band value fail both directions, so an uncertain check can never
//!    drive a rule — in either direction. See `docs/06-ensemble.md` §5.2.
//! 2. **Mixing `and` with `or` at one level without parentheses is a parse error.**
//!    Precedence subtleties are how a rule ends up meaning something other than what
//!    its author read.

use std::collections::BTreeMap;

use crate::assessment::ResolvedCheck;
use crate::ids::CheckKey;

/// How a value is compared.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Comparator {
    GreaterOrEqual,
    Greater,
    LessOrEqual,
    Less,
}

impl Comparator {
    fn parse(text: &str) -> Option<Self> {
        match text {
            ">=" => Some(Self::GreaterOrEqual),
            ">" => Some(Self::Greater),
            "<=" => Some(Self::LessOrEqual),
            "<" => Some(Self::Less),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GreaterOrEqual => ">=",
            Self::Greater => ">",
            Self::LessOrEqual => "<=",
            Self::Less => "<",
        }
    }

    fn holds(self, value: f32, bound: f32) -> bool {
        match self {
            Self::GreaterOrEqual => value >= bound,
            Self::Greater => value > bound,
            Self::LessOrEqual => value <= bound,
            Self::Less => value < bound,
        }
    }
}

/// A parsed condition.
///
/// Deliberately **not** `serde`-derived. A condition is parsed from a rule's `when`
/// string and evaluated; it never crosses a boundary, so it needs no representation on
/// one. Deriving it matters more than it looks: the type is recursive, and serde's
/// internally-tagged form generates code the compiler cannot resolve a bound for —
/// `overflow evaluating the requirement` — which raising `recursion_limit` does not fix
/// because the cycle is real rather than deep.
#[derive(Debug, Clone, PartialEq)]
pub enum Condition {
    Comparison {
        key: CheckKey,
        op: Comparator,
        value: f32,
    },
    Not(Box<Condition>),
    /// Both operands, in source order.
    Conjunction(Vec<Condition>),
    Disjunction(Vec<Condition>),
}

impl Condition {
    /// Parse a `when` expression.
    pub fn parse(text: &str) -> Result<Self, ConditionError> {
        let tokens = tokenize(text)?;
        if tokens.is_empty() {
            return Err(ConditionError::Empty);
        }
        let mut parser = Parser { tokens, at: 0 };
        let condition = parser.parse_level()?;
        if parser.at != parser.tokens.len() {
            return Err(ConditionError::Unexpected(parser.tokens[parser.at].clone()));
        }
        Ok(condition)
    }

    /// Evaluate against this cycle's resolved checks.
    ///
    /// A term whose check is missing, or is not permitted, is **false**. Missing means
    /// the rule does not fire, which is the conservative reading: a rule that cannot
    /// see its evidence has not earned the right to act.
    #[must_use]
    pub fn evaluate(&self, checks: &BTreeMap<CheckKey, ResolvedCheck>) -> bool {
        match self {
            Self::Comparison { key, op, value } => checks.get(key).is_some_and(|check| {
                // Permission first, then the comparison. This ordering is the whole
                // point: an in-band or contested measurement permits nothing.
                check.permitted_value().is_some_and(|p| op.holds(p, *value))
            }),
            Self::Not(inner) => !inner.evaluate(checks),
            Self::Conjunction(all) => all.iter().all(|c| c.evaluate(checks)),
            Self::Disjunction(any) => any.iter().any(|c| c.evaluate(checks)),
        }
    }

    /// Every check key this condition reads.
    #[must_use]
    pub fn keys(&self) -> Vec<CheckKey> {
        let mut out = Vec::new();
        self.collect_keys(&mut out);
        out
    }

    fn collect_keys(&self, out: &mut Vec<CheckKey>) {
        match self {
            Self::Comparison { key, .. } => {
                if !out.contains(key) {
                    out.push(key.clone());
                }
            }
            Self::Not(inner) => inner.collect_keys(out),
            Self::Conjunction(all) | Self::Disjunction(all) => {
                for condition in all {
                    condition.collect_keys(out);
                }
            }
        }
    }

    /// Whether the condition reads a threshold-literal that disagrees with the check's
    /// own declared threshold.
    ///
    /// The band is defined relative to a check's `min_threshold`, so a rule comparing
    /// against a *different* number is comparing against a line the band does not
    /// describe. Load time reports it rather than silently choosing one.
    #[must_use]
    pub fn conflicting_thresholds(
        &self,
        thresholds: &BTreeMap<CheckKey, f32>,
    ) -> Vec<(CheckKey, f32, f32)> {
        let mut conflicts = Vec::new();
        self.walk_comparisons(&mut |key, value| {
            if let Some(declared) = thresholds.get(key) {
                if (*declared - *value).abs() > f32::EPSILON {
                    conflicts.push((key.clone(), *value, *declared));
                }
            }
        });
        conflicts
    }

    fn walk_comparisons(&self, visit: &mut impl FnMut(&CheckKey, &f32)) {
        match self {
            Self::Comparison { key, value, .. } => visit(key, value),
            Self::Not(inner) => inner.walk_comparisons(visit),
            Self::Conjunction(all) | Self::Disjunction(all) => {
                for condition in all {
                    condition.walk_comparisons(visit);
                }
            }
        }
    }
}

/// A `when` expression that could not be parsed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConditionError {
    #[error("the condition is empty")]
    Empty,

    #[error("unbalanced parentheses")]
    Unbalanced,

    #[error("expected a check key, comparator, or number but found `{0}`")]
    Unexpected(String),

    #[error("`{0}` is not a comparator (expected one of >=, >, <=, <)")]
    BadComparator(String),

    #[error("`{0}` is not a number")]
    BadNumber(String),

    #[error(
        "`{0}` mixes `and` with `or` at the same level; add parentheses to say which binds first"
    )]
    MixedOperators(String),

    #[error("a `not` must be followed by a parenthesised condition")]
    BareNot,
}

fn tokenize(text: &str) -> Result<Vec<String>, ConditionError> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut chars = text.chars().peekable();

    let flush = |current: &mut String, tokens: &mut Vec<String>| {
        if !current.is_empty() {
            tokens.push(std::mem::take(current));
        }
    };

    while let Some(c) = chars.next() {
        match c {
            '(' | ')' => {
                flush(&mut current, &mut tokens);
                tokens.push(c.to_string());
            }
            // A comparator may be two characters, so peek before deciding.
            '>' | '<' | '=' => {
                flush(&mut current, &mut tokens);
                let mut op = c.to_string();
                if let Some(next) = chars.peek() {
                    if *next == '=' || (c == '<' && *next == '>') {
                        op.push(*next);
                        chars.next();
                    }
                }
                tokens.push(op);
            }
            c if c.is_whitespace() => flush(&mut current, &mut tokens),
            c => current.push(c),
        }
    }
    flush(&mut current, &mut tokens);
    if tokens.iter().filter(|t| *t == "(").count() != tokens.iter().filter(|t| *t == ")").count() {
        return Err(ConditionError::Unbalanced);
    }
    Ok(tokens)
}

struct Parser {
    tokens: Vec<String>,
    at: usize,
}

impl Parser {
    fn peek(&self) -> Option<&str> {
        self.tokens.get(self.at).map(String::as_str)
    }

    fn next(&mut self) -> Option<String> {
        let token = self.tokens.get(self.at).cloned();
        if token.is_some() {
            self.at += 1;
        }
        token
    }

    /// Parse one level: a run of operands joined by a single operator kind.
    ///
    /// Rejecting a mix at one level is the documented rule, and it is easier to read
    /// than a precedence table.
    fn parse_level(&mut self) -> Result<Condition, ConditionError> {
        let mut operands = vec![self.parse_term()?];
        let mut operator: Option<&'static str> = None;

        while let Some(token) = self.peek() {
            let next_operator = match token {
                "and" => "and",
                "or" => "or",
                _ => break,
            };
            match operator {
                None => operator = Some(next_operator),
                Some(existing) if existing == next_operator => {}
                Some(existing) => {
                    return Err(ConditionError::MixedOperators(format!(
                        "{existing} then {next_operator}"
                    )));
                }
            }
            self.next();
            operands.push(self.parse_term()?);
        }

        Ok(match operator {
            Some("and") => Condition::Conjunction(operands),
            Some("or") => Condition::Disjunction(operands),
            // A single operand needs no wrapper.
            _ => operands.remove(0),
        })
    }

    fn parse_term(&mut self) -> Result<Condition, ConditionError> {
        let token = self
            .next()
            .ok_or_else(|| ConditionError::Unexpected("end of expression".to_owned()))?;

        if token == "not" {
            if self.next().as_deref() != Some("(") {
                return Err(ConditionError::BareNot);
            }
            let inner = self.parse_level()?;
            if self.next().as_deref() != Some(")") {
                return Err(ConditionError::Unbalanced);
            }
            return Ok(Condition::Not(Box::new(inner)));
        }

        if token == "(" {
            let inner = self.parse_level()?;
            if self.next().as_deref() != Some(")") {
                return Err(ConditionError::Unbalanced);
            }
            return Ok(inner);
        }

        // `check_key comparator number`
        let key = CheckKey::from(token.as_str());
        if key.split().is_none() {
            return Err(ConditionError::Unexpected(format!(
                "{token} (a check key is `responsibility__check`)"
            )));
        }
        let operator_token = self
            .next()
            .ok_or_else(|| ConditionError::Unexpected("end of expression".to_owned()))?;
        let op = Comparator::parse(&operator_token)
            .ok_or_else(|| ConditionError::BadComparator(operator_token.clone()))?;
        let number_token = self
            .next()
            .ok_or_else(|| ConditionError::Unexpected("end of expression".to_owned()))?;
        let value: f32 = number_token
            .parse()
            .map_err(|_| ConditionError::BadNumber(number_token.clone()))?;
        Ok(Condition::Comparison { key, op, value })
    }
}

#[cfg(test)]
mod tests {
    use super::{Comparator, Condition, ConditionError};
    use crate::assessment::ResolvedCheck;
    use crate::ids::CheckKey;
    use std::collections::BTreeMap;

    fn checks(entries: &[(&str, f32)]) -> BTreeMap<CheckKey, ResolvedCheck> {
        entries
            .iter()
            .map(|(key, p)| {
                let key = CheckKey::from(*key);
                let resolved =
                    ResolvedCheck::from_measurement(key.clone(), Some(*p), Some(0.75), 0.15, true);
                (key, resolved)
            })
            .collect()
    }

    fn evaluate(text: &str, entries: &[(&str, f32)]) -> Result<bool, ConditionError> {
        Ok(Condition::parse(text)?.evaluate(&checks(entries)))
    }

    #[test]
    fn a_simple_comparison_parses_and_evaluates() -> Result<(), Box<dyn std::error::Error>> {
        let condition = Condition::parse("core.worker-health__worker_stuck >= 0.80")?;
        match &condition {
            Condition::Comparison { op, value, .. } => {
                assert_eq!(*op, Comparator::GreaterOrEqual);
                assert!((value - 0.80).abs() < f32::EPSILON);
            }
            other => panic!("expected a comparison, got {other:?}"),
        }
        assert!(evaluate(
            "core.worker-health__worker_stuck >= 0.80",
            &[("core.worker-health__worker_stuck", 0.91)]
        )?);
        assert!(!evaluate(
            "core.worker-health__worker_stuck >= 0.80",
            &[("core.worker-health__worker_stuck", 0.20)]
        )?);
        Ok(())
    }

    #[test]
    fn every_comparator_is_supported() -> Result<(), Box<dyn std::error::Error>> {
        let key = "core.verification__needs_verification";
        assert!(evaluate(&format!("{key} < 0.65"), &[(key, 0.10)])?);
        assert!(evaluate(&format!("{key} <= 0.65"), &[(key, 0.10)])?);
        assert!(evaluate(&format!("{key} > 0.10"), &[(key, 0.40)])?);
        assert!(evaluate(&format!("{key} >= 0.40"), &[(key, 0.40)])?);
        // The boundary behaves as written.
        assert!(!evaluate(&format!("{key} < 0.40"), &[(key, 0.40)])?);
        assert!(evaluate(&format!("{key} <= 0.40"), &[(key, 0.40)])?);
        Ok(())
    }

    #[test]
    fn an_in_band_check_fails_both_directions() -> Result<(), Box<dyn std::error::Error>> {
        // The single most important property of evaluation. `0.70` is in the band
        // around `0.75`, so it satisfies neither a `>= 0.65` nor a `< 0.75` term.
        let key = "core.verification__needs_verification";
        let mut conditions = BTreeMap::new();
        let k = CheckKey::from(key);
        conditions.insert(
            k.clone(),
            ResolvedCheck::from_measurement(k, Some(0.70), Some(0.65), 0.15, true),
        );
        assert!(!Condition::parse(&format!("{key} >= 0.65"))?.evaluate(&conditions));
        assert!(!Condition::parse(&format!("{key} < 0.90"))?.evaluate(&conditions));
        Ok(())
    }

    #[test]
    fn a_contested_check_fails_both_directions() -> Result<(), Box<dyn std::error::Error>> {
        let key = CheckKey::from("core.completion__ready_to_finish");
        let mut conditions = BTreeMap::new();
        conditions.insert(
            key.clone(),
            ResolvedCheck::from_measurement(key, Some(0.95), Some(0.75), 0.15, true).suppress(
                crate::assessment::CheckSuppression::Contested,
                crate::assessment::Resolution::Decisive,
            ),
        );
        assert!(
            !Condition::parse("core.completion__ready_to_finish >= 0.75")?.evaluate(&conditions)
        );
        assert!(
            !Condition::parse("core.completion__ready_to_finish < 0.75")?.evaluate(&conditions)
        );
        Ok(())
    }

    #[test]
    fn a_missing_check_makes_the_rule_false_not_true() -> Result<(), Box<dyn std::error::Error>> {
        // A rule that cannot see its evidence has not earned the right to act.
        assert!(!evaluate("core.completion__ready_to_finish >= 0.75", &[])?);
        assert!(!evaluate("core.completion__ready_to_finish < 0.75", &[])?);
        Ok(())
    }

    #[test]
    fn conjunction_and_disjunction_work() -> Result<(), Box<dyn std::error::Error>> {
        let entries = [
            ("core.completion__implementation_complete", 0.95),
            ("core.verification__needs_verification", 0.10),
        ];
        assert!(evaluate(
            "core.completion__implementation_complete >= 0.75 and core.verification__needs_verification < 0.65",
            &entries
        )?);
        assert!(evaluate(
            "core.completion__implementation_complete < 0.10 or core.verification__needs_verification < 0.65",
            &entries
        )?);
        assert!(!evaluate(
            "core.completion__implementation_complete < 0.10 and core.verification__needs_verification < 0.65",
            &entries
        )?);
        Ok(())
    }

    #[test]
    fn not_and_parentheses_work() -> Result<(), Box<dyn std::error::Error>> {
        let entries = [("core.verification__needs_verification", 0.10)];
        assert!(evaluate(
            "not (core.verification__needs_verification >= 0.65)",
            &entries
        )?);
        // Parentheses let the two operators mix, which is the documented escape hatch.
        assert!(evaluate(
            "core.verification__needs_verification < 0.05 or (core.verification__needs_verification >= 0.08 and core.verification__needs_verification < 0.20)",
            &entries
        )?);
        Ok(())
    }

    #[test]
    fn mixing_and_with_or_at_one_level_is_refused() {
        let error = Condition::parse(
            "core.completion__ready_to_finish >= 0.75 and core.verification__needs_verification < 0.65 or core.completion__ready_to_finish < 0.10",
        )
        .expect_err("must refuse");
        assert!(
            matches!(error, ConditionError::MixedOperators(_)),
            "{error:?}"
        );
        assert!(error.to_string().contains("parentheses"), "{error}");
    }

    #[test]
    fn malformed_expressions_are_reported_with_the_offending_text() {
        assert_eq!(
            Condition::parse("").expect_err("empty"),
            ConditionError::Empty
        );
        assert!(matches!(
            Condition::parse("core.completion__ready_to_finish ~= 0.5").expect_err("bad op"),
            ConditionError::BadComparator(_)
        ));
        assert!(matches!(
            Condition::parse("core.completion__ready_to_finish >= high").expect_err("bad number"),
            ConditionError::BadNumber(_)
        ));
        assert!(matches!(
            Condition::parse("not core.completion__ready_to_finish >= 0.5").expect_err("bare not"),
            ConditionError::BareNot
        ));
        assert!(matches!(
            Condition::parse("(core.completion__ready_to_finish >= 0.5").expect_err("unbalanced"),
            ConditionError::Unbalanced
        ));
        // A bare word with no separator is not a check key.
        assert!(matches!(
            Condition::parse("nonsense >= 0.5").expect_err("bad key"),
            ConditionError::Unexpected(_)
        ));
    }

    #[test]
    fn a_key_must_use_the_double_underscore_form() -> Result<(), Box<dyn std::error::Error>> {
        // Dots cannot be split back into their parts, which is why the wire form is `__`.
        assert!(Condition::parse("core.completion.ready_to_finish >= 0.5").is_err());
        assert!(Condition::parse("core.completion__ready_to_finish >= 0.5").is_ok());
        Ok(())
    }

    #[test]
    fn keys_lists_every_check_the_condition_reads() -> Result<(), Box<dyn std::error::Error>> {
        let condition = Condition::parse("a__one >= 0.5 and not (b__two < 0.2 or a__one < 0.1)")?;
        let keys: Vec<String> = condition.keys().iter().map(ToString::to_string).collect();
        assert_eq!(keys.len(), 2, "duplicates are collapsed: {keys:?}");
        assert!(keys.contains(&"a__one".to_owned()));
        assert!(keys.contains(&"b__two".to_owned()));
        Ok(())
    }

    #[test]
    fn a_threshold_that_contradicts_the_check_is_reported() -> Result<(), Box<dyn std::error::Error>>
    {
        // The band is defined relative to a check's own threshold, so a rule comparing
        // against a different number is comparing against a line the band does not
        // describe.
        let condition = Condition::parse("core.completion__ready_to_finish >= 0.90")?;
        let thresholds =
            BTreeMap::from([(CheckKey::from("core.completion__ready_to_finish"), 0.75_f32)]);
        let conflicts = condition.conflicting_thresholds(&thresholds);
        assert_eq!(conflicts.len(), 1);
        let (key, used, declared) = &conflicts[0];
        assert_eq!(key.to_string(), "core.completion__ready_to_finish");
        assert!((*used - 0.90).abs() < f32::EPSILON);
        assert!((*declared - 0.75).abs() < f32::EPSILON);
        Ok(())
    }

    #[test]
    fn a_matching_threshold_is_not_a_conflict() -> Result<(), Box<dyn std::error::Error>> {
        let condition = Condition::parse("core.completion__ready_to_finish >= 0.75")?;
        let thresholds =
            BTreeMap::from([(CheckKey::from("core.completion__ready_to_finish"), 0.75_f32)]);
        assert!(condition.conflicting_thresholds(&thresholds).is_empty());
        Ok(())
    }

    #[test]
    fn multiple_operands_of_one_kind_are_all_required() -> Result<(), Box<dyn std::error::Error>> {
        // Values chosen clear of the band edge. A value exactly `margin` from the
        // threshold is at the mercy of f32 representation: `0.9 - 0.75` is
        // `0.14999998`, which is below a margin of `0.15`, so the check reads as in
        // band. That is a real property of the design, not a bug — but a test that
        // sits on the boundary is testing the float, not the rule.
        let entries = [("a__one", 0.95), ("a__two", 0.95), ("a__three", 0.05)];
        assert!(evaluate("a__one >= 0.5 and a__two >= 0.5", &entries)?);
        assert!(!evaluate(
            "a__one >= 0.5 and a__two >= 0.5 and a__three >= 0.5",
            &entries
        )?);
        Ok(())
    }
}
