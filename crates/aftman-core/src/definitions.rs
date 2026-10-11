//! Declarative responsibility definitions.
//!
//! A responsibility's **checks** live in TOML, because that is where operator
//! customisation actually matters: the instructions a model reads and the thresholds
//! its answers are compared against. A responsibility's **rule logic** stays in Rust
//! where it depends on runtime facts a condition cannot see — whether a verifier has
//! already run, whether a steer is still available, whether the attempt budget is spent.
//! Pretending TOML could express those would produce rules that silently mean something
//! weaker than they read.
//!
//! What TOML *can* express is a self-contained rule over check results, and that is
//! offered as [`DeclarativeResponsibility`] for responsibilities an operator adds.
//!
//! See `docs/05-responsibilities.md` §1 and §2.

use std::collections::BTreeMap;
use std::path::Path;

use serde::Deserialize;

use crate::condition::Condition;
use crate::directive::{Directive, Proposal, Stakes, SteerKind};
use crate::ids::{CheckId, CheckKey, EvidenceId, ResponsibilityId, RuleId};
use crate::model::Question;
use crate::policy::PolicyInput;
use crate::responsibility::{Check, Responsibility, Routing};

/// The bundled definitions, compiled in.
///
/// Compiled in rather than read from disk so that a binary always has a complete set and
/// cannot be broken by a missing file. An operator overrides them by placing a file of
/// the same name in the config directory.
pub const BUNDLED: &[(&str, &str)] = &[
    (
        "core.completion",
        include_str!("../definitions/core.completion.toml"),
    ),
    (
        "core.verification",
        include_str!("../definitions/core.verification.toml"),
    ),
    (
        "core.worker-health",
        include_str!("../definitions/core.worker-health.toml"),
    ),
    (
        "core.human-escalation",
        include_str!("../definitions/core.human-escalation.toml"),
    ),
    (
        "repository.instructions",
        include_str!("../definitions/repository.instructions.toml"),
    ),
    (
        "quality.documentation",
        include_str!("../definitions/quality.documentation.toml"),
    ),
];

/// The responsibility ids the code provides rule logic for.
///
/// A definition whose id is not here is either a user-authored declarative
/// responsibility (fine, if it declares `[[rules]]`) or a typo (reported).
pub const KNOWN_BUILTIN_IDS: &[&str] = &[
    "core.completion",
    "core.verification",
    "core.worker-health",
    "core.human-escalation",
    "repository.instructions",
    "quality.documentation",
];

/// A definition file that could not be loaded.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DefinitionError {
    #[error("could not read {path}: {message}")]
    Read { path: String, message: String },

    #[error("{path} is not valid TOML: {message}")]
    Parse { path: String, message: String },

    #[error("{path}: {message}")]
    Invalid { path: String, message: String },
}

/// A value as written in a definition file.
///
/// An explicit enum rather than `toml::Value`. Routing a TOML value through the derive
/// machinery alongside these types makes the compiler evaluate an unbounded generic
/// requirement — `overflow evaluating the requirement` — which raising
/// `recursion_limit` does not fix, because the cycle is real rather than deep. Naming the
/// shapes we accept also means an unexpected one is reported instead of half-supported.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum DeclaredValue {
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Array(Vec<DeclaredValue>),
    Table(BTreeMap<String, DeclaredValue>),
}

impl DeclaredValue {
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(text) => Some(text),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Self::Integer(number) => Some(*number),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_table(&self) -> Option<&BTreeMap<String, DeclaredValue>> {
        match self {
            Self::Table(table) => Some(table),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_array(&self) -> Option<&[DeclaredValue]> {
        match self {
            Self::Array(items) => Some(items),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Boolean(flag) => Some(*flag),
            _ => None,
        }
    }

    fn to_json(&self) -> serde_json::Value {
        match self {
            Self::Boolean(flag) => serde_json::Value::Bool(*flag),
            Self::Integer(number) => serde_json::Value::from(*number),
            Self::Float(number) => serde_json::Number::from_f64(*number)
                .map_or(serde_json::Value::Null, serde_json::Value::from),
            Self::String(text) => serde_json::Value::String(text.clone()),
            Self::Array(items) => {
                serde_json::Value::Array(items.iter().map(DeclaredValue::to_json).collect())
            }
            Self::Table(table) => serde_json::Value::Object(
                table
                    .iter()
                    .map(|(key, entry)| (key.clone(), entry.to_json()))
                    .collect(),
            ),
        }
    }
}

/// One check, as declared.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckDefinition {
    /// The question, as text or as structured JSON. Optional for `choice`/`score`,
    /// which may be described entirely by their criteria.
    #[serde(default)]
    instructions: Option<DeclaredValue>,
    /// The threshold this check's band is measured against. Absent means the check
    /// cannot drive a threshold rule.
    #[serde(default)]
    min_threshold: Option<f32>,
    /// `noul` (default), `choice`, or `score`.
    #[serde(default)]
    primitive: Option<String>,
    #[serde(default)]
    criteria: Option<DeclaredValue>,
    /// Evidence providers this check selects. Empty means the default set.
    #[serde(default)]
    evidence: Option<Vec<String>>,
}

/// One rule, as declared.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleDefinition {
    pub id: String,
    /// A condition over check results. See [`Condition`].
    pub when: String,
    /// `continue`, `start-worker`, `steer`, `retry`, `verify`, `stop`, `finish`, or
    /// `escalate`.
    pub propose: String,
    #[serde(default)]
    pub reason: Option<String>,
    /// `low` or `high`. High opts a rule into the second-opinion path.
    #[serde(default)]
    pub stakes: Option<String>,
    /// `nudge`, `refocus`, `warn`, or `ask`. Only meaningful for `propose = "steer"`.
    #[serde(default)]
    pub steer_kind: Option<String>,
    /// Overrides the priority from the documented table for this directive.
    #[serde(default)]
    pub priority: Option<u16>,
}

/// A whole definition file.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResponsibilityDefinition {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Whether the responsibility participates without router involvement.
    #[serde(default = "default_true")]
    pub always: bool,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub routing_instructions: Option<String>,
    #[serde(default)]
    pub routing_threshold: Option<f32>,
    #[serde(default)]
    pub checks: BTreeMap<String, CheckDefinition>,
    /// Responsibility-specific values, validated against a known key set below.
    #[serde(default)]
    pub settings: BTreeMap<String, DeclaredValue>,
    #[serde(default)]
    pub rules: Vec<RuleDefinition>,
}

fn default_true() -> bool {
    true
}

impl ResponsibilityDefinition {
    /// Parse and validate one definition.
    pub fn parse(id: &str, source: &str, path: &str) -> Result<Self, DefinitionError> {
        let definition: Self = toml::from_str(source).map_err(|error| DefinitionError::Parse {
            path: path.to_owned(),
            message: error.message().to_owned(),
        })?;
        definition.validate(id, path)?;
        Ok(definition)
    }

    /// Reject anything that would misbehave at assessment time.
    fn validate(&self, id: &str, path: &str) -> Result<(), DefinitionError> {
        let invalid = |message: String| DefinitionError::Invalid {
            path: path.to_owned(),
            message,
        };

        if !self.always
            && self
                .routing_instructions
                .as_deref()
                .is_none_or(|text| text.trim().is_empty())
        {
            return Err(invalid(
                "`always = false` requires non-empty `routing_instructions`, or the router \
                 has nothing to decide with"
                    .to_owned(),
            ));
        }
        if let Some(threshold) = self.routing_threshold {
            if !(0.0..=1.0).contains(&threshold) {
                return Err(invalid(format!(
                    "routing_threshold {threshold} is outside 0..=1"
                )));
            }
        }
        if self.checks.is_empty() && self.rules.is_empty() {
            return Err(invalid(
                "a definition must declare at least one check or one rule".to_owned(),
            ));
        }

        for (check_id, check) in &self.checks {
            if check_id.trim().is_empty() {
                return Err(invalid("a check id is empty".to_owned()));
            }
            if let Some(threshold) = check.min_threshold {
                if !(0.0..=1.0).contains(&threshold) {
                    return Err(invalid(format!(
                        "check `{check_id}`: min_threshold {threshold} is outside 0..=1"
                    )));
                }
            }
            let primitive = check.primitive.as_deref().unwrap_or("noul");
            match primitive {
                "noul" => {}
                "choice" => {
                    let criteria = check
                        .criteria
                        .as_ref()
                        .and_then(DeclaredValue::as_table)
                        .map_or(0, BTreeMap::len);
                    if criteria == 0 {
                        return Err(invalid(format!(
                            "check `{check_id}` is a choice and needs a non-empty `criteria` table"
                        )));
                    }
                }
                "score" => {
                    let levels = check
                        .criteria
                        .as_ref()
                        .and_then(DeclaredValue::as_array)
                        .map_or(0, <[DeclaredValue]>::len);
                    if levels == 0 {
                        return Err(invalid(format!(
                            "check `{check_id}` is a score and needs at least one ordered criterion"
                        )));
                    }
                }
                other => {
                    return Err(invalid(format!(
                        "check `{check_id}`: unknown primitive `{other}` (noul, choice, score)"
                    )));
                }
            }
            if let Some(evidence) = &check.evidence {
                if evidence.is_empty() || evidence.iter().any(|e| e.trim().is_empty()) {
                    return Err(invalid(format!(
                        "check `{check_id}`: an explicit evidence list must be non-empty and \
                         contain no blank ids"
                    )));
                }
                let mut seen = evidence.clone();
                seen.sort();
                seen.dedup();
                if seen.len() != evidence.len() {
                    return Err(invalid(format!(
                        "check `{check_id}`: evidence provider ids must be unique"
                    )));
                }
            }
        }

        let mut rule_ids: Vec<&str> = self.rules.iter().map(|r| r.id.as_str()).collect();
        let rule_count = rule_ids.len();
        rule_ids.sort_unstable();
        rule_ids.dedup();
        if rule_ids.len() != rule_count {
            return Err(invalid(
                "rule ids must be unique; the policy tie-break depends on it".to_owned(),
            ));
        }

        // Thresholds declared here, for cross-checking rule literals.
        let thresholds: BTreeMap<CheckKey, f32> = self
            .checks
            .iter()
            .filter_map(|(check_id, check)| {
                check.min_threshold.map(|threshold| {
                    (
                        CheckKey::new(&ResponsibilityId::new(id), &CheckId::new(check_id)),
                        threshold,
                    )
                })
            })
            .collect();

        for rule in &self.rules {
            if rule.id.trim().is_empty() {
                return Err(invalid("a rule id is empty".to_owned()));
            }
            let condition = Condition::parse(&rule.when)
                .map_err(|error| invalid(format!("rule `{}`: {error}", rule.id)))?;
            // A rule reading a check nobody declares is almost always a typo, and it
            // would sit permanently false without saying so.
            for key in condition.keys() {
                if !thresholds.contains_key(&key) {
                    return Err(invalid(format!(
                        "rule `{}` reads `{key}`, which no check in this definition declares",
                        rule.id
                    )));
                }
            }
            // The band is defined against a check's own threshold, so a rule comparing
            // against a different number compares against a line the band does not
            // describe.
            if let Some((key, used, declared)) =
                condition.conflicting_thresholds(&thresholds).first()
            {
                return Err(invalid(format!(
                    "rule `{}` compares `{key}` against {used}, but the check declares \
                     {declared}. The uncertainty band is defined against the check's own \
                     threshold, so these must agree.",
                    rule.id
                )));
            }
            parse_directive(rule).map_err(invalid)?;
        }

        validate_settings(id, &self.settings).map_err(invalid)?;
        Ok(())
    }

    /// The checks this definition declares, as the runtime uses them.
    pub fn checks(&self, id: &str) -> Vec<Check> {
        let responsibility = ResponsibilityId::new(id);
        self.checks
            .iter()
            .map(|(check_id, definition)| {
                let mut check = Check::new(
                    &responsibility,
                    check_id,
                    build_question(check_id, definition),
                    definition.min_threshold,
                );
                if let Some(evidence) = &definition.evidence {
                    check.evidence = evidence.iter().map(EvidenceId::new).collect();
                }
                check
            })
            .collect()
    }

    /// The routing rule this definition declares.
    #[must_use]
    pub fn routing(&self) -> Routing {
        if self.always {
            Routing::global()
        } else {
            Routing::conditional(
                self.routing_instructions.clone().unwrap_or_default(),
                self.routing_threshold.unwrap_or(0.5),
            )
        }
    }

    /// A `u64` setting, if present and of the right type.
    #[must_use]
    pub fn setting_u64(&self, key: &str) -> Option<u64> {
        self.settings
            .get(key)
            .and_then(DeclaredValue::as_integer)
            .and_then(|value| u64::try_from(value).ok())
    }

    /// A list-of-strings setting, if present and of the right shape.
    #[must_use]
    pub fn setting_strings(&self, key: &str) -> Option<Vec<String>> {
        self.settings
            .get(key)
            .and_then(DeclaredValue::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(DeclaredValue::as_str)
                    .map(ToOwned::to_owned)
                    .collect()
            })
    }
}

/// Settings each built-in understands. An unknown key is an error, so a typo does not
/// silently do nothing.
fn validate_settings(id: &str, settings: &BTreeMap<String, DeclaredValue>) -> Result<(), String> {
    const KNOWN: &[(&str, &[&str])] = &[
        (
            "core.worker-health",
            &["steering_grace_seconds", "max_steers_per_worker"],
        ),
        ("repository.instructions", &["instruction_files"]),
    ];
    let allowed = KNOWN
        .iter()
        .find(|(known_id, _)| *known_id == id)
        .map(|(_, keys)| *keys)
        .unwrap_or(&[]);
    for key in settings.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(format!(
                "unknown setting `{key}` for `{id}` (known: {})",
                if allowed.is_empty() {
                    "none".to_owned()
                } else {
                    allowed.join(", ")
                }
            ));
        }
    }
    Ok(())
}

/// Turn a declared check into a question.
fn build_question(check_id: &str, definition: &CheckDefinition) -> Question {
    let instructions = definition
        .instructions
        .as_ref()
        .map(instruction_text)
        .unwrap_or_else(|| check_id.to_owned());
    match definition.primitive.as_deref().unwrap_or("noul") {
        "choice" => {
            let criteria = definition
                .criteria
                .as_ref()
                .and_then(DeclaredValue::as_table)
                .map(|table| {
                    table
                        .iter()
                        .map(|(name, description)| (name.clone(), instruction_text(description)))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            Question::choice(instructions, criteria)
        }
        "score" => {
            let criteria = definition
                .criteria
                .as_ref()
                .and_then(DeclaredValue::as_array)
                .map(|levels| levels.iter().map(instruction_text).collect::<Vec<_>>())
                .unwrap_or_default();
            Question::score(instructions, criteria)
        }
        _ => Question::noul(instructions),
    }
}

/// Render a declared value as instruction text.
///
/// A structured value becomes a JSON string, which the wire layer parses back into a
/// structured `instructions` field. That keeps one representation through the domain and
/// the provider's structured form at the edge.
fn instruction_text(value: &DeclaredValue) -> String {
    match value {
        DeclaredValue::String(text) => normalize_instruction(text),
        other => other.to_json().to_string(),
    }
}

/// Collapse the hard wrapping a multi-line TOML string introduces.
///
/// A TOML `"""` string keeps its line breaks, so a wrapped definition would reach the
/// model as text broken mid-sentence. Paragraph breaks are preserved, because a check
/// that states its not-applicable case in a second paragraph means it to be read that
/// way.
#[must_use]
pub fn normalize_instruction(text: &str) -> String {
    let paragraphs: Vec<String> = text
        .split("\n\n")
        .map(|paragraph| paragraph.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|paragraph| !paragraph.is_empty())
        .collect();
    paragraphs.join("\n\n")
}

/// Build the directive a rule proposes.
fn parse_directive(rule: &RuleDefinition) -> Result<(Directive, u16, Stakes), String> {
    let reason = rule
        .reason
        .clone()
        .unwrap_or_else(|| format!("rule `{}` matched", rule.id));
    let default_priority = |name: &str| match name {
        "escalate" => crate::directive::priority::ESCALATE_NEEDS_HUMAN,
        "stop" => crate::directive::priority::STOP_OR_STEER_WORKER_HEALTH,
        "steer" => crate::directive::priority::STOP_OR_STEER_WORKER_HEALTH,
        "retry" => crate::directive::priority::RETRY_STOPPED,
        "verify" => crate::directive::priority::VERIFY,
        "finish" => crate::directive::priority::FINISH,
        "start-worker" => crate::directive::priority::START_WORKER_COMPLETION,
        _ => crate::directive::priority::CONTINUE,
    };
    let steer_kind = match rule.steer_kind.as_deref() {
        None | Some("nudge") => SteerKind::Nudge,
        Some("refocus") => SteerKind::Refocus,
        Some("warn") => SteerKind::Warn,
        Some("ask") => SteerKind::Ask,
        Some(other) => return Err(format!("unknown steer_kind `{other}`")),
    };
    let directive = match rule.propose.as_str() {
        "continue" => Directive::Continue,
        "start-worker" => Directive::StartWorker { reason },
        "steer" => Directive::Steer {
            kind: steer_kind,
            reason,
        },
        "retry" => Directive::Retry { reason },
        "verify" => Directive::Verify { reason },
        "stop" => Directive::Stop { reason },
        "finish" => Directive::Finish { reason },
        "escalate" => Directive::Escalate {
            reason,
            ask: reason_ask(rule),
        },
        other => {
            return Err(format!(
                "unknown propose `{other}` (continue, start-worker, steer, retry, verify, \
                 stop, finish, escalate)"
            ));
        }
    };
    let priority = rule
        .priority
        .unwrap_or_else(|| default_priority(rule.propose.as_str()));
    let stakes = match rule.stakes.as_deref() {
        None | Some("low") => Stakes::Low,
        Some("high") => Stakes::High,
        Some(other) => return Err(format!("unknown stakes `{other}` (low, high)")),
    };
    Ok((directive, priority, stakes))
}

fn reason_ask(rule: &RuleDefinition) -> String {
    rule.reason
        .clone()
        .unwrap_or_else(|| format!("rule `{}` asks for a human decision", rule.id))
}

/// A responsibility whose whole behaviour is declared.
///
/// Only for definitions an operator adds: the bundled responsibilities keep their
/// runtime-aware logic in Rust. A declarative rule can express everything a condition
/// over check results can express, and nothing more.
#[derive(Debug)]
pub struct DeclarativeResponsibility {
    id: ResponsibilityId,
    checks: Vec<Check>,
    routing: Routing,
    rules: Vec<(RuleId, Condition, Directive, u16, Stakes)>,
}

impl DeclarativeResponsibility {
    /// Build from a parsed definition, or explain why it cannot be.
    pub fn new(id: &str, definition: &ResponsibilityDefinition) -> Result<Self, String> {
        if definition.rules.is_empty() {
            return Err(format!(
                "`{id}` is not a built-in id and declares no `[[rules]]`, so nothing would \
                 ever propose a directive from it"
            ));
        }
        let mut rules = Vec::new();
        for rule in &definition.rules {
            let condition = Condition::parse(&rule.when)
                .map_err(|error| format!("rule `{}`: {error}", rule.id))?;
            let (directive, priority, stakes) = parse_directive(rule)?;
            rules.push((
                RuleId::new(&rule.id),
                condition,
                directive,
                priority,
                stakes,
            ));
        }
        Ok(Self {
            id: ResponsibilityId::new(id),
            checks: definition.checks(id),
            routing: definition.routing(),
            rules,
        })
    }

    /// The declared rules, for inspection.
    #[must_use]
    pub fn rules(&self) -> usize {
        self.rules.len()
    }
}

impl Responsibility for DeclarativeResponsibility {
    fn id(&self) -> &ResponsibilityId {
        &self.id
    }

    fn checks(&self) -> &[Check] {
        &self.checks
    }

    fn routing(&self) -> &Routing {
        &self.routing
    }

    fn propose(&self, input: &PolicyInput<'_>) -> Vec<Proposal> {
        self.rules
            .iter()
            .filter(|(_, condition, _, _, _)| condition.evaluate(input.checks))
            .map(|(rule, _, directive, priority, stakes)| {
                Proposal::new(
                    self.id.clone(),
                    rule.clone(),
                    directive.clone(),
                    *priority,
                    *stakes,
                )
            })
            .collect()
    }
}

/// Every definition the runtime knows about.
#[derive(Debug, Clone, Default)]
pub struct DefinitionSet {
    /// Keyed by responsibility id, in a deterministic order.
    pub definitions: BTreeMap<String, ResponsibilityDefinition>,
    /// Where each came from, for `doctor` and for error messages.
    pub sources: BTreeMap<String, String>,
}

impl DefinitionSet {
    /// The compiled-in definitions.
    pub fn bundled() -> Result<Self, DefinitionError> {
        let mut set = Self::default();
        for (id, source) in BUNDLED {
            let definition = ResponsibilityDefinition::parse(id, source, "bundled")?;
            set.sources.insert((*id).to_owned(), "bundled".to_owned());
            set.definitions.insert((*id).to_owned(), definition);
        }
        Ok(set)
    }

    /// Overlay a directory of `.toml` files.
    ///
    /// A file replaces the definition of the same name, which is how an operator changes
    /// an instruction or a threshold without patching the binary. A file whose id is
    /// neither a built-in nor rule-bearing is refused rather than ignored.
    pub fn overlay_dir(&mut self, dir: &Path) -> Result<(), DefinitionError> {
        let entries = std::fs::read_dir(dir).map_err(|error| DefinitionError::Read {
            path: dir.display().to_string(),
            message: error.to_string(),
        })?;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(std::ffi::OsStr::to_str) != Some("toml") {
                continue;
            }
            let id = path
                .file_stem()
                .and_then(std::ffi::OsStr::to_str)
                .unwrap_or_default()
                .to_owned();
            let source = std::fs::read_to_string(&path).map_err(|error| DefinitionError::Read {
                path: path.display().to_string(),
                message: error.to_string(),
            })?;
            let definition =
                ResponsibilityDefinition::parse(&id, &source, &path.display().to_string())?;
            self.sources.insert(id.clone(), path.display().to_string());
            self.definitions.insert(id, definition);
        }
        Ok(())
    }

    /// Ids that are neither built-in nor declarative, which is almost always a typo.
    #[must_use]
    pub fn unknown_ids(&self) -> Vec<String> {
        self.definitions
            .iter()
            .filter(|(id, definition)| {
                !KNOWN_BUILTIN_IDS.contains(&id.as_str()) && definition.rules.is_empty()
            })
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Only the enabled definitions.
    pub fn enabled(&self) -> impl Iterator<Item = (&String, &ResponsibilityDefinition)> {
        self.definitions
            .iter()
            .filter(|(_, definition)| definition.enabled)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DeclarativeResponsibility, DefinitionError, DefinitionSet, ResponsibilityDefinition,
    };
    use crate::condition::Condition;
    use crate::responsibility::Responsibility;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn parse(id: &str, source: &str) -> Result<ResponsibilityDefinition, DefinitionError> {
        ResponsibilityDefinition::parse(id, source, "test")
    }

    #[test]
    fn every_bundled_definition_loads_and_validates() -> TestResult {
        let set = DefinitionSet::bundled()?;
        assert_eq!(set.definitions.len(), 6);
        // Every bundled id is one the code has logic for.
        assert!(set.unknown_ids().is_empty(), "{:?}", set.unknown_ids());
        for (id, definition) in &set.definitions {
            assert!(!definition.checks.is_empty(), "{id} declares no checks");
        }
        Ok(())
    }

    #[test]
    fn the_bundled_thresholds_match_the_documented_ones() -> TestResult {
        // Parity with docs/05-responsibilities.md §1. A silent drift here would change
        // what the supervisor does without changing what it says it does.
        let set = DefinitionSet::bundled()?;
        let expected: &[(&str, &str, f32)] = &[
            ("core.completion", "implementation_complete", 0.75),
            ("core.completion", "requirements_satisfied", 0.75),
            ("core.completion", "ready_to_finish", 0.75),
            ("core.verification", "tests_sufficient", 0.75),
            ("core.verification", "needs_verification", 0.65),
            ("core.worker-health", "worker_stuck", 0.80),
            ("core.worker-health", "work_off_track", 0.80),
            ("core.human-escalation", "needs_human", 0.80),
            ("repository.instructions", "agents_md_drift", 0.80),
            ("quality.documentation", "documentation_sufficient", 0.75),
        ];
        for (id, check, threshold) in expected {
            let definition = set.definitions.get(*id).ok_or("missing definition")?;
            let declared = definition
                .checks
                .get(*check)
                .ok_or("missing check")?
                .min_threshold
                .ok_or("missing threshold")?;
            assert!(
                (declared - threshold).abs() < f32::EPSILON,
                "{id}.{check} is {declared}, documented as {threshold}"
            );
        }
        // `meaningful_progress` deliberately has no threshold.
        assert!(
            set.definitions["core.worker-health"].checks["meaningful_progress"]
                .min_threshold
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn the_bundled_check_keys_use_the_double_underscore_form() -> TestResult {
        let set = DefinitionSet::bundled()?;
        for (id, definition) in &set.definitions {
            for check in definition.checks(id) {
                assert!(check.key.as_str().contains("__"), "{}", check.key);
                let (responsibility, check_id) = check.key.split().ok_or("splittable")?;
                assert_eq!(responsibility, id);
                assert!(!check_id.is_empty());
            }
        }
        Ok(())
    }

    #[test]
    fn the_documentation_responsibility_is_conditional_and_has_routing_text() -> TestResult {
        let set = DefinitionSet::bundled()?;
        let documentation = &set.definitions["quality.documentation"];
        assert!(!documentation.always, "it must be routed in");
        let routing = documentation.routing();
        assert!(!routing.always);
        assert!(routing.instructions.is_some_and(|t| !t.is_empty()));
        assert!((routing.threshold - 0.70).abs() < f32::EPSILON);
        Ok(())
    }

    #[test]
    fn the_other_bundled_responsibilities_are_global() -> TestResult {
        let set = DefinitionSet::bundled()?;
        for id in [
            "core.completion",
            "core.verification",
            "core.worker-health",
            "core.human-escalation",
            "repository.instructions",
        ] {
            assert!(
                set.definitions[id].routing().always,
                "{id} should be global"
            );
        }
        Ok(())
    }

    #[test]
    fn the_drift_check_spells_out_the_not_applicable_case() -> TestResult {
        let set = DefinitionSet::bundled()?;
        let checks = set.definitions["repository.instructions"].checks("repository.instructions");
        let question = &checks[0].question;
        let text = match question {
            crate::model::Question::Noul { instructions } => instructions.clone(),
            other => panic!("expected a noul, got {other:?}"),
        };
        assert!(
            text.contains("Answer no when no AGENTS.md instructions are present"),
            "{text}"
        );
        Ok(())
    }

    #[test]
    fn instructions_lose_their_hard_wrapping_but_keep_their_paragraphs() {
        let wrapped = "Answer no when no AGENTS.md instructions are present\n                       or the evidence is insufficient.\n\n                       A second paragraph matters too.";
        let normalized = super::normalize_instruction(wrapped);
        assert!(
            normalized
                .contains("Answer no when no AGENTS.md instructions are present or the evidence"),
            "{normalized}"
        );
        assert!(
            normalized.contains("\n\n"),
            "a paragraph break is preserved: {normalized}"
        );
        assert!(
            !normalized.contains("  "),
            "no runs of spaces: {normalized}"
        );
        assert!(!normalized.ends_with('\n'));
    }

    #[test]
    fn the_bundled_instructions_reach_the_model_unwrapped() -> TestResult {
        // The drift check's last sentence is load-bearing, and a hard-wrapped newline
        // mid-sentence is how it would silently stop matching a downstream consumer.
        let set = DefinitionSet::bundled()?;
        let checks = set.definitions["repository.instructions"].checks("repository.instructions");
        let text = match &checks[0].question {
            crate::model::Question::Noul { instructions } => instructions.clone(),
            other => panic!("expected a noul, got {other:?}"),
        };
        assert!(
            text.contains("Answer no when no AGENTS.md instructions are present"),
            "{text}"
        );
        Ok(())
    }

    #[test]
    fn worker_health_settings_are_readable() -> TestResult {
        let set = DefinitionSet::bundled()?;
        let health = &set.definitions["core.worker-health"];
        assert_eq!(health.setting_u64("steering_grace_seconds"), Some(30));
        assert_eq!(health.setting_u64("max_steers_per_worker"), Some(1));
        Ok(())
    }

    // ---- validation --------------------------------------------------------

    #[test]
    fn an_unknown_key_anywhere_is_refused() {
        // `deny_unknown_fields` means a typo fails loudly instead of silently doing
        // nothing.
        let error = parse("x", "enabled = true\nmystery = 1\n").expect_err("must refuse");
        assert!(matches!(error, DefinitionError::Parse { .. }), "{error:?}");
    }

    #[test]
    fn a_conditional_responsibility_needs_routing_text() {
        let error = parse("x", "always = false\n\n[checks.a]\ninstructions = \"?\"\n")
            .expect_err("must refuse");
        assert!(
            error.to_string().contains("routing_instructions"),
            "{error}"
        );
    }

    #[test]
    fn a_threshold_outside_zero_to_one_is_refused() {
        let error = parse(
            "x",
            "[checks.a]\ninstructions = \"?\"\nmin_threshold = 1.5\n",
        )
        .expect_err("must refuse");
        assert!(error.to_string().contains("outside 0..=1"), "{error}");
    }

    #[test]
    fn a_choice_check_needs_criteria() {
        let error = parse(
            "x",
            "[checks.a]\nprimitive = \"choice\"\ninstructions = \"?\"\n",
        )
        .expect_err("must refuse");
        assert!(error.to_string().contains("criteria"), "{error}");
    }

    #[test]
    fn a_score_check_needs_ordered_levels() {
        let error = parse(
            "x",
            "[checks.a]\nprimitive = \"score\"\ninstructions = \"?\"\n",
        )
        .expect_err("must refuse");
        assert!(error.to_string().contains("ordered criterion"), "{error}");
        // With levels it is accepted.
        assert!(parse(
            "x",
            "[checks.a]\nprimitive = \"score\"\ninstructions = \"?\"\ncriteria = [\"none\", \"some\"]\n"
        )
        .is_ok());
    }

    #[test]
    fn an_unknown_primitive_is_refused() {
        let error = parse("x", "[checks.a]\nprimitive = \"vibe\"\n").expect_err("must refuse");
        assert!(error.to_string().contains("unknown primitive"), "{error}");
    }

    #[test]
    fn duplicate_evidence_providers_are_refused() {
        let error = parse(
            "x",
            "[checks.a]\ninstructions = \"?\"\nevidence = [\"git_diff\", \"git_diff\"]\n",
        )
        .expect_err("must refuse");
        assert!(error.to_string().contains("unique"), "{error}");
    }

    #[test]
    fn an_empty_definition_is_refused() {
        let error = parse("x", "enabled = true\n").expect_err("must refuse");
        assert!(error.to_string().contains("at least one check"), "{error}");
    }

    #[test]
    fn an_unknown_setting_for_a_known_id_is_refused() {
        let error = parse(
            "core.worker-health",
            "[checks.a]\ninstructions = \"?\"\n\n[settings]\nnonsense = 1\n",
        )
        .expect_err("must refuse");
        assert!(error.to_string().contains("unknown setting"), "{error}");
    }

    #[test]
    fn a_rule_reading_an_undeclared_check_is_refused() {
        let error = parse(
            "x",
            "[checks.a]\ninstructions = \"?\"\nmin_threshold = 0.5\n\n[[rules]]\nid = \"r\"\nwhen = \"x__b >= 0.5\"\npropose = \"stop\"\n",
        )
        .expect_err("must refuse");
        assert!(
            error.to_string().contains("no check in this definition"),
            "{error}"
        );
    }

    #[test]
    fn a_rule_comparing_against_a_different_threshold_is_refused() {
        // The band is defined against the check's own threshold, so these must agree.
        let error = parse(
            "x",
            "[checks.a]\ninstructions = \"?\"\nmin_threshold = 0.75\n\n[[rules]]\nid = \"r\"\nwhen = \"x__a >= 0.9\"\npropose = \"stop\"\n",
        )
        .expect_err("must refuse");
        assert!(error.to_string().contains("must agree"), "{error}");
    }

    #[test]
    fn a_rule_matching_the_declared_threshold_is_accepted() {
        assert!(parse(
            "x",
            "[checks.a]\ninstructions = \"?\"\nmin_threshold = 0.75\n\n[[rules]]\nid = \"r\"\nwhen = \"x__a >= 0.75\"\npropose = \"stop\"\n"
        )
        .is_ok());
    }

    #[test]
    fn duplicate_rule_ids_are_refused() {
        let error = parse(
            "x",
            "[checks.a]\ninstructions = \"?\"\nmin_threshold = 0.5\n\n[[rules]]\nid = \"r\"\nwhen = \"x__a >= 0.5\"\npropose = \"stop\"\n\n[[rules]]\nid = \"r\"\nwhen = \"x__a < 0.5\"\npropose = \"retry\"\n",
        )
        .expect_err("must refuse");
        assert!(error.to_string().contains("unique"), "{error}");
    }

    #[test]
    fn an_unknown_directive_or_stakes_is_refused() {
        let unknown_propose = parse(
            "x",
            "[checks.a]\ninstructions = \"?\"\nmin_threshold = 0.5\n\n[[rules]]\nid = \"r\"\nwhen = \"x__a >= 0.5\"\npropose = \"vibe\"\n",
        )
        .expect_err("must refuse");
        assert!(
            unknown_propose.to_string().contains("unknown propose"),
            "{unknown_propose}"
        );

        let unknown_stakes = parse(
            "x",
            "[checks.a]\ninstructions = \"?\"\nmin_threshold = 0.5\n\n[[rules]]\nid = \"r\"\nwhen = \"x__a >= 0.5\"\npropose = \"stop\"\nstakes = \"enormous\"\n",
        )
        .expect_err("must refuse");
        assert!(
            unknown_stakes.to_string().contains("unknown stakes"),
            "{unknown_stakes}"
        );
    }

    // ---- declarative responsibilities --------------------------------------

    #[test]
    fn a_declarative_responsibility_proposes_from_its_rules() -> TestResult {
        let definition = parse(
            "quality.docs",
            "[checks.sufficient]\ninstructions = \"enough docs?\"\nmin_threshold = 0.75\n\n[[rules]]\nid = \"quality.docs:insufficient\"\nwhen = \"quality.docs__sufficient < 0.75\"\npropose = \"start-worker\"\nreason = \"documentation is incomplete\"\nstakes = \"high\"\n",
        )?;
        let responsibility = DeclarativeResponsibility::new("quality.docs", &definition)
            .map_err(|e| e.to_string())?;
        assert_eq!(responsibility.rules(), 1);
        assert_eq!(responsibility.id().as_str(), "quality.docs");
        assert_eq!(responsibility.checks().len(), 1);
        Ok(())
    }

    #[test]
    fn a_declarative_responsibility_without_rules_is_refused() -> TestResult {
        let definition = parse("x", "[checks.a]\ninstructions = \"?\"\n")?;
        let error = DeclarativeResponsibility::new("x", &definition)
            .err()
            .ok_or("must refuse")?;
        assert!(error.contains("nothing would ever propose"), "{error}");
        Ok(())
    }

    #[test]
    fn a_declarative_rule_evaluates_against_permitted_checks_only() -> TestResult {
        let definition = parse(
            "quality.docs",
            "[checks.sufficient]\ninstructions = \"?\"\nmin_threshold = 0.75\n\n[[rules]]\nid = \"r\"\nwhen = \"quality.docs__sufficient < 0.75\"\npropose = \"start-worker\"\n",
        )?;
        let responsibility = DeclarativeResponsibility::new("quality.docs", &definition)
            .map_err(|e| e.to_string())?;
        // The rule's condition is reachable through the responsibility's own checks.
        let key = responsibility.checks()[0].key.clone();
        assert_eq!(key.to_string(), "quality.docs__sufficient");
        // And the same condition parses and reads that key.
        let condition = Condition::parse("quality.docs__sufficient < 0.75")?;
        assert_eq!(condition.keys(), vec![key]);
        Ok(())
    }

    // ---- overlay ----------------------------------------------------------

    #[test]
    fn an_overlay_file_replaces_a_bundled_definition() -> TestResult {
        let dir = tempfile::tempdir()?;
        std::fs::write(
            dir.path().join("core.completion.toml"),
            "[checks.ready_to_finish]\ninstructions = \"changed\"\nmin_threshold = 0.90\n",
        )?;
        let mut set = DefinitionSet::bundled()?;
        set.overlay_dir(dir.path())?;

        let definition = &set.definitions["core.completion"];
        assert_eq!(
            definition.checks.len(),
            1,
            "the file replaces, it does not merge"
        );
        let checks = definition.checks("core.completion");
        assert!((checks[0].threshold.unwrap_or_default() - 0.90).abs() < f32::EPSILON);
        assert_eq!(
            set.sources["core.completion"],
            dir.path()
                .join("core.completion.toml")
                .display()
                .to_string()
        );
        Ok(())
    }

    #[test]
    fn a_broken_overlay_file_is_reported_with_its_path() -> TestResult {
        let dir = tempfile::tempdir()?;
        std::fs::write(
            dir.path().join("core.completion.toml"),
            "min_threshold = \"not a number\"\n",
        )?;
        let mut set = DefinitionSet::bundled()?;
        let error = set.overlay_dir(dir.path()).err().ok_or("must refuse")?;
        assert!(
            error.to_string().contains("core.completion.toml"),
            "{error}"
        );
        Ok(())
    }

    #[test]
    fn a_non_toml_file_in_the_overlay_directory_is_ignored() -> TestResult {
        let dir = tempfile::tempdir()?;
        std::fs::write(dir.path().join("notes.md"), "hello")?;
        let mut set = DefinitionSet::bundled()?;
        set.overlay_dir(dir.path())?;
        assert_eq!(set.definitions.len(), 6);
        Ok(())
    }

    #[test]
    fn a_user_definition_with_rules_is_not_flagged_as_unknown() -> TestResult {
        let dir = tempfile::tempdir()?;
        std::fs::write(
            dir.path().join("my.quality.toml"),
            "[checks.a]\ninstructions = \"?\"\nmin_threshold = 0.5\n\n[[rules]]\nid = \"my.quality:low\"\nwhen = \"my.quality__a < 0.5\"\npropose = \"start-worker\"\n",
        )?;
        let mut set = DefinitionSet::bundled()?;
        set.overlay_dir(dir.path())?;
        assert!(set.unknown_ids().is_empty(), "{:?}", set.unknown_ids());
        Ok(())
    }

    #[test]
    fn a_user_definition_without_rules_is_flagged_instead_of_ignored() -> TestResult {
        let dir = tempfile::tempdir()?;
        std::fs::write(
            dir.path().join("my.typo.toml"),
            "[checks.a]\ninstructions = \"?\"\nmin_threshold = 0.5\n",
        )?;
        let mut set = DefinitionSet::bundled()?;
        set.overlay_dir(dir.path())?;
        assert_eq!(set.unknown_ids(), vec!["my.typo".to_owned()]);
        Ok(())
    }

    #[test]
    fn enabled_filters_disabled_definitions() -> TestResult {
        let dir = tempfile::tempdir()?;
        std::fs::write(
            dir.path().join("core.verification.toml"),
            "enabled = false\n\n[checks.tests_sufficient]\ninstructions = \"?\"\nmin_threshold = 0.5\n",
        )?;
        let mut set = DefinitionSet::bundled()?;
        set.overlay_dir(dir.path())?;
        assert_eq!(set.definitions.len(), 6);
        assert_eq!(set.enabled().count(), 5);
        assert!(set.enabled().all(|(id, _)| id != "core.verification"));
        Ok(())
    }
}
