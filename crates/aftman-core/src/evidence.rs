//! Command evidence providers.
//!
//! A worker's output about a test run is a **claim**: it is text the agent chose to
//! print. A command aftman ran itself is a **fact**. This module defines the commands an
//! operator configures to turn the first into the second, and when each runs.
//!
//! Three properties are non-negotiable:
//!
//! - **`argv` is a literal list, never a shell string.** Nothing user-supplied is
//!   interpolated, so a job title containing `;` cannot become a command.
//! - **The environment is scrubbed**, exactly as a worker's is, because a provider runs
//!   repository code and repository code must not be able to read the supervisor's key.
//! - **Output and time are bounded**, so an evidence run cannot stall a cycle or grow the
//!   supervisor's memory.
//!
//! See `docs/05-responsibilities.md` §3.

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::ids::EvidenceId;

/// When a provider runs.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum RunWhen {
    /// After a coding worker finishes and before the cycle that judges it. The common
    /// case, and the one that turns "3 passed" into a measurement.
    #[default]
    WorkerCompleted,
    /// Only when a trigger asks for it — the ensemble's evidence refresh.
    OnDemand,
    /// Before `Finish` is allowed to take effect. The primary defence against
    /// declaring a job done on a claim.
    BeforeFinish,
}

/// Where a provider runs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderCwd {
    /// The repository under supervision.
    #[default]
    Repo,
    /// A scratch directory, for a probe that must not touch the repository.
    Temp,
}

/// One configured command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandProvider {
    pub id: EvidenceId,
    /// The literal argv. Never passed through a shell.
    pub argv: Vec<String>,
    #[serde(default)]
    pub cwd: ProviderCwd,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(default = "default_max_output_bytes")]
    pub max_output_bytes: usize,
    #[serde(default)]
    pub run_when: RunWhen,
    /// Exit codes that mean success. A test command that exits 1 has still *run*.
    #[serde(default)]
    pub success_exit_codes: Vec<i32>,
}

fn default_timeout_seconds() -> u64 {
    600
}

fn default_max_output_bytes() -> usize {
    20_000
}

impl CommandProvider {
    #[must_use]
    pub fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout_seconds)
    }

    /// Whether an exit code counts as success.
    ///
    /// An empty list means only `0` does, which is the least surprising reading.
    #[must_use]
    pub fn exit_was_success(&self, code: Option<i32>) -> bool {
        match code {
            Some(code) if self.success_exit_codes.is_empty() => code == 0,
            Some(code) => self.success_exit_codes.contains(&code),
            // Killed by a signal: not a success by any reading.
            None => false,
        }
    }

    /// A string for the event log. Never contains the environment.
    #[must_use]
    pub fn display(&self) -> String {
        self.argv.join(" ")
    }
}

/// The `[evidence]` table as an operator writes it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceConfig {
    #[serde(default)]
    pub commands: Vec<CommandProvider>,
}

/// A whole configuration file, whose providers live under `[evidence]`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvidenceFile {
    #[serde(default)]
    evidence: EvidenceConfig,
}

/// A configuration problem that must be fixed before a run starts.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EvidenceConfigError {
    #[error("{path} is not valid TOML: {message}")]
    Parse { path: String, message: String },

    #[error("{path}: {message}")]
    Invalid { path: String, message: String },
}

/// The central evidence configuration path, `$XDG_CONFIG_HOME/aftman/evidence.toml`.
#[must_use]
pub fn default_config_path() -> Option<PathBuf> {
    if let Some(config) = std::env::var_os("XDG_CONFIG_HOME") {
        return Some(PathBuf::from(config).join("aftman/evidence.toml"));
    }
    std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config/aftman/evidence.toml"))
}

/// The providers, validated.
#[derive(Debug, Clone, Default)]
pub struct EvidencePlan {
    providers: Vec<CommandProvider>,
}

impl EvidencePlan {
    #[must_use]
    pub fn new(providers: Vec<CommandProvider>) -> Self {
        Self { providers }
    }

    /// Parse and validate an evidence configuration.
    pub fn from_toml(source: &str, path: &str) -> Result<Self, EvidenceConfigError> {
        let file: EvidenceFile =
            toml::from_str(source).map_err(|error| EvidenceConfigError::Parse {
                path: path.to_owned(),
                message: error.message().to_owned(),
            })?;
        Self::validate(file.evidence.commands, path)
    }

    fn validate(providers: Vec<CommandProvider>, path: &str) -> Result<Self, EvidenceConfigError> {
        let invalid = |message: String| EvidenceConfigError::Invalid {
            path: path.to_owned(),
            message,
        };
        for provider in &providers {
            if provider.id.as_str().trim().is_empty() {
                return Err(invalid("a provider id is empty".to_owned()));
            }
            if provider.argv.is_empty() {
                return Err(invalid(format!(
                    "provider `{}` has an empty argv",
                    provider.id
                )));
            }
            if provider.argv.iter().any(|arg| arg.is_empty()) {
                return Err(invalid(format!(
                    "provider `{}` has an empty argv element",
                    provider.id
                )));
            }
            if provider.timeout_seconds == 0 {
                return Err(invalid(format!(
                    "provider `{}` has a zero timeout, which would always fail",
                    provider.id
                )));
            }
            if provider.max_output_bytes == 0 {
                return Err(invalid(format!(
                    "provider `{}` has a zero output budget, so its result could never be read",
                    provider.id
                )));
            }
        }
        let mut ids: Vec<&str> = providers.iter().map(|p| p.id.as_str()).collect();
        let count = ids.len();
        ids.sort_unstable();
        ids.dedup();
        if ids.len() != count {
            return Err(invalid("evidence provider ids must be unique".to_owned()));
        }
        Ok(Self { providers })
    }

    /// The providers that should run for this trigger, in configuration order.
    #[must_use]
    pub fn due(&self, trigger: RunWhen) -> Vec<&CommandProvider> {
        self.providers
            .iter()
            .filter(|provider| provider.run_when == trigger)
            .collect()
    }

    /// Every provider referenced by a check's evidence selection.
    ///
    /// A check names a provider as `command:<id>`; an unknown id is reported rather than
    /// treated as "no evidence", because a check expecting a measurement and silently
    /// getting nothing is how a run finishes on a claim.
    pub fn unknown_references<'a>(
        &self,
        selections: impl IntoIterator<Item = &'a str>,
    ) -> Vec<String> {
        let mut unknown = Vec::new();
        for selection in selections {
            let Some(id) = selection.strip_prefix("command:") else {
                continue;
            };
            if !self.providers.iter().any(|p| p.id.as_str() == id) {
                unknown.push(id.to_owned());
            }
        }
        unknown.sort();
        unknown.dedup();
        unknown
    }

    #[must_use]
    pub fn providers(&self) -> &[CommandProvider] {
        &self.providers
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.providers.len()
    }
}

/// The result of running one provider.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvidenceOutcome {
    pub provider: EvidenceId,
    pub argv: Vec<String>,
    pub cwd: PathBuf,
    pub exit_code: Option<i32>,
    /// Whether the exit code was among the provider's accepted ones.
    pub success: bool,
    pub duration: Duration,
    pub output_excerpt: String,
    pub truncated: bool,
    #[serde(with = "crate::timefmt")]
    pub at: OffsetDateTime,
    /// Set when the command could not be launched at all, which is different from
    /// running and failing.
    pub launch_error: Option<String>,
}

impl EvidenceOutcome {
    /// A one-line summary for the event log.
    #[must_use]
    pub fn summary(&self) -> String {
        if let Some(error) = &self.launch_error {
            return format!("{}: could not launch ({error})", self.provider);
        }
        match self.exit_code {
            Some(code) => format!(
                "{}: exit {code} ({}) in {:?}",
                self.provider,
                if self.success { "ok" } else { "failed" },
                self.duration
            ),
            None => format!(
                "{}: killed by a signal after {:?}",
                self.provider, self.duration
            ),
        }
    }

    /// The provider key an observation uses.
    #[must_use]
    pub fn evidence_key(&self) -> String {
        self.provider.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{CommandProvider, EvidencePlan, ProviderCwd, RunWhen};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    const SAMPLE: &str = r#"
[[evidence.commands]]
id = "tests"
argv = ["pytest", "-q"]
cwd = "repo"
timeout_seconds = 600
max_output_bytes = 20000
run_when = "worker_completed"
success_exit_codes = [0]

[[evidence.commands]]
id = "audit"
argv = ["cargo", "audit"]
run_when = "before_finish"
"#;

    #[test]
    fn a_provider_configuration_parses() -> TestResult {
        let plan = EvidencePlan::from_toml(SAMPLE, "test")?;
        assert_eq!(plan.len(), 2);
        let tests = &plan.providers()[0];
        assert_eq!(tests.id.as_str(), "tests");
        assert_eq!(tests.argv, vec!["pytest", "-q"]);
        assert_eq!(tests.cwd, ProviderCwd::Repo);
        assert_eq!(tests.timeout_seconds, 600);
        assert_eq!(tests.run_when, RunWhen::WorkerCompleted);
        assert!(tests.exit_was_success(Some(0)));
        assert!(!tests.exit_was_success(Some(1)));
        Ok(())
    }

    #[test]
    fn defaults_apply_when_fields_are_omitted() -> TestResult {
        let plan = EvidencePlan::from_toml(
            "[[evidence.commands]]\nid = \"x\"\nargv = [\"true\"]\n",
            "test",
        )?;
        let provider = &plan.providers()[0];
        assert_eq!(provider.cwd, ProviderCwd::Repo);
        assert_eq!(provider.timeout_seconds, 600);
        assert_eq!(provider.max_output_bytes, 20_000);
        assert_eq!(provider.run_when, RunWhen::WorkerCompleted);
        // An empty success list means only zero.
        assert!(provider.exit_was_success(Some(0)));
        assert!(
            !provider.exit_was_success(None),
            "a signal is never success"
        );
        Ok(())
    }

    #[test]
    fn a_declared_success_code_is_honoured() -> TestResult {
        // A test command that exits 1 has still run, and the runner needs to be able to
        // say so without calling the whole evidence collection a failure.
        let plan = EvidencePlan::from_toml(
            "[[evidence.commands]]\nid = \"x\"\nargv = [\"sh\"]\nsuccess_exit_codes = [0, 1]\n",
            "test",
        )?;
        let provider = &plan.providers()[0];
        assert!(provider.exit_was_success(Some(1)));
        assert!(!provider.exit_was_success(Some(2)));
        Ok(())
    }

    #[test]
    fn triggers_select_the_right_providers() -> TestResult {
        let plan = EvidencePlan::from_toml(SAMPLE, "test")?;
        let after = plan.due(RunWhen::WorkerCompleted);
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].id.as_str(), "tests");

        let before_finish = plan.due(RunWhen::BeforeFinish);
        assert_eq!(before_finish.len(), 1);
        assert_eq!(before_finish[0].id.as_str(), "audit");

        assert!(plan.due(RunWhen::OnDemand).is_empty());
        Ok(())
    }

    #[test]
    fn an_unknown_placeholder_is_caught_by_the_schema() {
        // `deny_unknown_fields` means a misspelled key fails loudly.
        let error = EvidencePlan::from_toml(
            "[[evidence.commands]]\nid = \"x\"\nargv = [\"true\"]\nnonsense = 1\n",
            "test",
        )
        .expect_err("must refuse");
        assert!(error.to_string().contains("nonsense"), "{error}");
    }

    #[test]
    fn an_empty_argv_is_refused() {
        let error =
            EvidencePlan::from_toml("[[evidence.commands]]\nid = \"x\"\nargv = []\n", "test")
                .expect_err("must refuse");
        assert!(error.to_string().contains("empty argv"), "{error}");
    }

    #[test]
    fn a_zero_timeout_or_budget_is_refused() {
        let no_timeout = EvidencePlan::from_toml(
            "[[evidence.commands]]\nid = \"x\"\nargv = [\"true\"]\ntimeout_seconds = 0\n",
            "test",
        )
        .expect_err("must refuse");
        assert!(
            no_timeout.to_string().contains("zero timeout"),
            "{no_timeout}"
        );

        let no_budget = EvidencePlan::from_toml(
            "[[evidence.commands]]\nid = \"x\"\nargv = [\"true\"]\nmax_output_bytes = 0\n",
            "test",
        )
        .expect_err("must refuse");
        assert!(
            no_budget.to_string().contains("zero output budget"),
            "{no_budget}"
        );
    }

    #[test]
    fn duplicate_provider_ids_are_refused() -> TestResult {
        let error = EvidencePlan::from_toml(
            "[[evidence.commands]]\nid = \"x\"\nargv = [\"true\"]\n\n[[evidence.commands]]\nid = \"x\"\nargv = [\"false\"]\n",
            "test",
        )
        .err()
        .ok_or("must refuse")?;
        assert!(error.to_string().contains("unique"), "{error}");
        Ok(())
    }

    #[test]
    fn an_empty_configuration_is_valid_and_empty() -> TestResult {
        let plan = EvidencePlan::from_toml("", "test")?;
        assert!(plan.is_empty());
        assert!(plan.due(RunWhen::WorkerCompleted).is_empty());
        Ok(())
    }

    #[test]
    fn a_check_referencing_an_unknown_provider_is_reported() -> TestResult {
        // Silently treating this as "no evidence" is how a check that expects a
        // measurement ends up judging a claim.
        let plan = EvidencePlan::from_toml(SAMPLE, "test")?;
        let unknown = plan.unknown_references(["command:tests", "command:typo", "git_diff"]);
        assert_eq!(unknown, vec!["typo".to_owned()]);
        assert!(
            plan.unknown_references(["command:tests", "git_diff"])
                .is_empty()
        );
        Ok(())
    }

    #[test]
    fn the_display_only_shows_argv() {
        let provider = CommandProvider {
            id: "x".into(),
            argv: vec!["cargo".into(), "test".into()],
            cwd: ProviderCwd::Repo,
            timeout_seconds: 60,
            max_output_bytes: 100,
            run_when: RunWhen::WorkerCompleted,
            success_exit_codes: vec![],
        };
        assert_eq!(provider.display(), "cargo test");
    }

    #[test]
    fn an_outcome_summarises_itself_readably() -> TestResult {
        use super::EvidenceOutcome;
        use std::path::PathBuf;
        use time::macros::datetime;

        let mut outcome = EvidenceOutcome {
            provider: "tests".into(),
            argv: vec!["pytest".into()],
            cwd: PathBuf::from("/repo"),
            exit_code: Some(1),
            success: false,
            duration: std::time::Duration::from_millis(420),
            output_excerpt: "1 failed".into(),
            truncated: false,
            at: datetime!(2026-01-01 00:00:00 UTC),
            launch_error: None,
        };
        assert!(
            outcome.summary().contains("exit 1"),
            "{}",
            outcome.summary()
        );
        assert!(outcome.summary().contains("failed"));
        assert_eq!(outcome.evidence_key(), "tests");

        outcome.launch_error = Some("not found".into());
        assert!(outcome.summary().contains("could not launch"));
        Ok(())
    }
}
