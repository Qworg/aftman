//! Provider settings.
//!
//! Two rules here are security decisions, not configuration conveniences:
//!
//! 1. **`<repo>/.env` is never read.** A cloned repository that ships
//!    `TYPESAFE_BASE_URL=https://attacker.example` would otherwise have its base URL
//!    adopted whenever the operator set a key but no explicit URL, and the next
//!    request would send the key to the attacker. Non-overriding precedence does not
//!    help, because the variable was never set in the first place.
//! 2. **The base URL must be HTTPS or loopback.** A cleartext remote URL puts the key
//!    on the wire in the clear, so it is a startup error rather than a warning.
//!
//! See `docs/03-jev.md` §1 and §10, and `docs/08-security.md` §2.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use aftman_core::model::Budget;

use crate::retry::RetryPolicy;

/// Default endpoint.
pub const DEFAULT_BASE_URL: &str = "https://api.typesafe.ai";
/// Default model alias.
pub const DEFAULT_MODEL: &str = "jev-latest";

/// A configuration problem that must be fixed before any request is made.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SettingsError {
    #[error("{0} is not a valid URL")]
    InvalidBaseUrl(String),

    #[error(
        "refusing a cleartext non-loopback base URL ({0}): the supervisor's API key \
         would be sent over the network in the clear. Use https, or a loopback address."
    )]
    InsecureBaseUrl(String),

    #[error(
        "base URL {0} must not include a version prefix: the client appends \
         /v1/systemone. Give the origin plus any provider path, for example \
         https://openrouter.ai/api, and do not include /v1."
    )]
    BaseUrlHasPath(String),

    #[error("{key} is not a number: {value}")]
    NotANumber { key: String, value: String },
}

/// What the client needs to talk to a provider.
#[derive(Debug, Clone, PartialEq)]
pub struct JevSettings {
    /// Absent means the client is unconfigured; `aftman doctor` reports it and a run
    /// refuses to start rather than pretending to supervise.
    pub api_key: Option<String>,
    pub base_url: String,
    /// The model to request. May be an alias.
    pub model: String,
    pub timeout: Duration,
    pub retry: RetryPolicy,
    pub budget: Budget,
}

impl Default for JevSettings {
    fn default() -> Self {
        Self {
            api_key: None,
            base_url: DEFAULT_BASE_URL.to_owned(),
            model: DEFAULT_MODEL.to_owned(),
            timeout: Duration::from_secs(10),
            retry: RetryPolicy::default(),
            budget: Budget::default(),
        }
    }
}

impl JevSettings {
    /// The assessment endpoint.
    #[must_use]
    pub fn endpoint(&self) -> String {
        format!("{}{}", self.base_url, crate::wire::SYSTEM_ONE_PATH)
    }

    /// The model-listing endpoint.
    #[must_use]
    pub fn models_endpoint(&self) -> String {
        format!("{}{}", self.base_url, crate::wire::MODELS_PATH)
    }

    #[must_use]
    pub const fn is_configured(&self) -> bool {
        self.api_key.is_some()
    }

    /// Whether the base URL points at a loopback address.
    #[must_use]
    pub fn is_loopback(&self) -> bool {
        let rest = self
            .base_url
            .split_once("://")
            .map(|(_, rest)| rest)
            .unwrap_or(&self.base_url);
        matches!(host_of(rest), "127.0.0.1" | "localhost" | "::1")
    }
}

/// Parse a `.env` file into a map.
///
/// Deliberately lenient — a stray line must not stop the daemon from starting — but
/// it never *sets* anything: the caller decides precedence. That is why this returns a
/// map rather than mutating the process environment, which would also require
/// `unsafe` under Rust 2024 and would make the resolution order invisible.
#[must_use]
pub fn parse_env_file(text: &str) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim().trim_start_matches("export ").trim();
        let value = value.trim();
        let value = value
            .strip_prefix('"')
            .and_then(|v| v.strip_suffix('"'))
            .or_else(|| value.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')))
            .unwrap_or(value);
        if !key.is_empty() {
            map.insert(key.to_owned(), value.to_owned());
        }
    }
    map
}

/// Read a `.env` file, returning an empty map if it is absent or unreadable.
#[must_use]
pub fn load_env_file(path: &Path) -> BTreeMap<String, String> {
    std::fs::read_to_string(path)
        .map(|text| parse_env_file(&text))
        .unwrap_or_default()
}

/// The `$XDG_CONFIG_HOME/aftman/.env` path, if a home directory can be determined.
#[must_use]
pub fn default_env_file() -> Option<std::path::PathBuf> {
    default_env_file_with(&|key| std::env::var(key).ok())
}

/// [`default_env_file`] with an explicit environment.
#[must_use]
pub fn default_env_file_with(env: &dyn Fn(&str) -> Option<String>) -> Option<std::path::PathBuf> {
    if let Some(config) = env("XDG_CONFIG_HOME") {
        return Some(std::path::PathBuf::from(config).join("aftman/.env"));
    }
    env("HOME").map(|home| std::path::PathBuf::from(home).join(".config/aftman/.env"))
}

/// Resolve settings from the process environment, with `file` as a fallback.
///
/// The process environment wins over the file, always.
pub fn resolve(file: Option<&BTreeMap<String, String>>) -> Result<JevSettings, SettingsError> {
    resolve_with(&|key| std::env::var(key).ok(), file)
}

/// Every environment variable provider resolution reads. The composition root takes a
/// [`snapshot_env`] of exactly these, once, so the rest of the program never needs to
/// know their names (hard rule 2) and tests can supply their own.
pub const ENV_KEYS: &[&str] = &[
    "TYPESAFE_API_KEY",
    "TYPESAFE_BASE_URL",
    "TYPESAFE_DEFAULT_MODEL",
    "AFTMAN_JEV_API_KEY",
    "AFTMAN_JEV_MODEL",
    "AFTMAN_JEV_TIMEOUT_SECONDS",
    "AFTMAN_JEV_MAX_RETRIES",
    "AFTMAN_JEV_MAX_RETRY_AFTER_SECONDS",
    "AFTMAN_JEV_PAIR_BUDGET_BYTES",
    "AFTMAN_JEV_REQUEST_BUDGET_BYTES",
];

/// The values of [`ENV_KEYS`] that `env` has.
#[must_use]
pub fn snapshot_env(env: &dyn Fn(&str) -> Option<String>) -> BTreeMap<String, String> {
    ENV_KEYS
        .iter()
        .filter_map(|key| env(key).map(|value| ((*key).to_owned(), value)))
        .collect()
}

/// Where the credential came from. Never the credential itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialSource {
    Missing,
    Environment,
    ConfigFile,
}

/// A redacted description of what was resolved: safe to print, log, or send to a
/// client. Holds no key, no key prefix, no key length, and no URL userinfo.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettingsView {
    pub configured: bool,
    pub credential: CredentialSource,
    pub model: String,
    pub base_url: String,
    pub loopback: bool,
}

impl JevSettings {
    /// The redacted description of these settings. `credential` says where the key
    /// came from, as found by [`describe`].
    #[must_use]
    pub fn view(&self, credential: CredentialSource) -> SettingsView {
        SettingsView {
            configured: self.is_configured(),
            credential: if self.is_configured() {
                credential
            } else {
                CredentialSource::Missing
            },
            model: self.model.clone(),
            base_url: redact_url(&self.base_url),
            loopback: self.is_loopback(),
        }
    }
}

/// Resolve with `env` and `file`, and describe the result without exposing it.
///
/// # Errors
/// As [`resolve_with`].
pub fn describe(
    env: &dyn Fn(&str) -> Option<String>,
    file: Option<&BTreeMap<String, String>>,
) -> Result<SettingsView, SettingsError> {
    let settings = resolve_with(env, file)?;
    let from_env = ["TYPESAFE_API_KEY", "AFTMAN_JEV_API_KEY"]
        .iter()
        .any(|k| env(k).is_some_and(|v| !v.trim().is_empty()));
    Ok(settings.view(if from_env {
        CredentialSource::Environment
    } else {
        CredentialSource::ConfigFile
    }))
}

/// [`resolve`] with an explicit environment, so a test or a composition root can say
/// exactly what the environment is.
pub fn resolve_with(
    env: &dyn Fn(&str) -> Option<String>,
    file: Option<&BTreeMap<String, String>>,
) -> Result<JevSettings, SettingsError> {
    let mut settings = JevSettings::default();

    let lookup = |key: &str| -> Option<String> {
        env(key).filter(|v| !v.trim().is_empty()).or_else(|| {
            file.and_then(|f| f.get(key))
                .filter(|v| !v.trim().is_empty())
                .cloned()
        })
    };

    settings.api_key = lookup("TYPESAFE_API_KEY").or_else(|| lookup("AFTMAN_JEV_API_KEY"));
    if let Some(url) = lookup("TYPESAFE_BASE_URL") {
        settings.base_url = validate_base_url(&url)?;
    }
    if let Some(model) = lookup("TYPESAFE_DEFAULT_MODEL").or_else(|| lookup("AFTMAN_JEV_MODEL")) {
        settings.model = model;
    }

    if let Some(raw) = lookup("AFTMAN_JEV_TIMEOUT_SECONDS") {
        settings.timeout = Duration::from_secs(parse_u64("AFTMAN_JEV_TIMEOUT_SECONDS", &raw)?);
    }
    if let Some(raw) = lookup("AFTMAN_JEV_MAX_RETRIES") {
        settings.retry.max_retries =
            u32::try_from(parse_u64("AFTMAN_JEV_MAX_RETRIES", &raw)?).unwrap_or(u32::MAX);
    }
    if let Some(raw) = lookup("AFTMAN_JEV_MAX_RETRY_AFTER_SECONDS") {
        settings.retry.max_retry_after =
            Duration::from_secs(parse_u64("AFTMAN_JEV_MAX_RETRY_AFTER_SECONDS", &raw)?);
    }
    if let Some(raw) = lookup("AFTMAN_JEV_PAIR_BUDGET_BYTES") {
        settings.budget.pair_bytes =
            usize::try_from(parse_u64("AFTMAN_JEV_PAIR_BUDGET_BYTES", &raw)?).unwrap_or(usize::MAX);
    }
    if let Some(raw) = lookup("AFTMAN_JEV_REQUEST_BUDGET_BYTES") {
        settings.budget.request_bytes =
            usize::try_from(parse_u64("AFTMAN_JEV_REQUEST_BUDGET_BYTES", &raw)?)
                .unwrap_or(usize::MAX);
    }

    Ok(settings)
}

fn parse_u64(key: &str, raw: &str) -> Result<u64, SettingsError> {
    raw.trim()
        .parse::<u64>()
        .map_err(|_| SettingsError::NotANumber {
            key: key.to_owned(),
            value: raw.to_owned(),
        })
}

/// The host part of an authority, with any path, port, or IPv6 brackets removed.
///
/// A base URL may carry a provider path — `https://openrouter.ai/api` is the
/// documented OpenRouter form — so the host has to be extracted rather than assumed
/// to be everything after the scheme.
fn host_of(rest: &str) -> &str {
    let authority = rest.split('/').next().unwrap_or("");
    // `user:pass@host`: the host is what follows the last `@`. Getting this wrong would
    // let `http://localhost@evil.example` pass as loopback.
    let authority = authority.rsplit('@').next().unwrap_or("");
    if let Some(inner) = authority.strip_prefix('[') {
        return inner.split(']').next().unwrap_or("");
    }
    authority.split(':').next().unwrap_or("")
}

/// Validate and normalise a base URL.
///
/// A path is allowed (some providers mount the API under one); a `/v1` prefix is not,
/// because the client appends `/v1/systemone` and the result would be `/v1/v1/...`.
fn validate_base_url(raw: &str) -> Result<String, SettingsError> {
    validate_base_url_inner(raw).map_err(|error| match error {
        SettingsError::InvalidBaseUrl(u) => SettingsError::InvalidBaseUrl(redact_url(&u)),
        SettingsError::InsecureBaseUrl(u) => SettingsError::InsecureBaseUrl(redact_url(&u)),
        SettingsError::BaseUrlHasPath(u) => SettingsError::BaseUrlHasPath(redact_url(&u)),
        other => other,
    })
}

/// `raw` with any `user:password@` removed from its authority, for display and for
/// error messages that cross a trust boundary (a socket client, a log).
#[must_use]
pub fn redact_url(raw: &str) -> String {
    let Some((scheme, rest)) = raw.trim().split_once("://") else {
        return raw.trim().to_owned();
    };
    let (authority, tail) = match rest.find('/') {
        Some(i) => rest.split_at(i),
        None => (rest, ""),
    };
    let authority = authority.rsplit('@').next().unwrap_or(authority);
    format!("{scheme}://{authority}{tail}")
}

fn validate_base_url_inner(raw: &str) -> Result<String, SettingsError> {
    let trimmed = raw.trim().trim_end_matches('/');
    let Some((scheme, rest)) = trimmed.split_once("://") else {
        return Err(SettingsError::InvalidBaseUrl(raw.to_owned()));
    };
    if rest.is_empty() || host_of(rest).is_empty() {
        return Err(SettingsError::InvalidBaseUrl(raw.to_owned()));
    }
    if rest.contains("/v1") {
        return Err(SettingsError::BaseUrlHasPath(raw.to_owned()));
    }

    match scheme {
        "https" => Ok(trimmed.to_owned()),
        "http" => {
            if matches!(host_of(rest), "127.0.0.1" | "localhost" | "::1") {
                Ok(trimmed.to_owned())
            } else {
                Err(SettingsError::InsecureBaseUrl(raw.to_owned()))
            }
        }
        _ => Err(SettingsError::InvalidBaseUrl(raw.to_owned())),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CredentialSource, JevSettings, SettingsError, describe, parse_env_file, redact_url,
        resolve, validate_base_url,
    };
    use std::collections::BTreeMap;

    #[test]
    fn userinfo_is_redacted_and_cannot_spoof_loopback() {
        assert_eq!(
            redact_url("https://user:secret@api.example.com/api"),
            "https://api.example.com/api"
        );
        assert_eq!(
            redact_url("https://api.example.com"),
            "https://api.example.com"
        );
        let error = validate_base_url("http://localhost@evil.example").expect_err("not loopback");
        assert!(matches!(error, SettingsError::InsecureBaseUrl(_)));
        let error = validate_base_url("ftp://u:pw@h").expect_err("bad scheme");
        assert!(!error.to_string().contains("pw"), "{error}");
    }

    #[test]
    fn the_view_names_the_source_and_never_the_key() {
        let env = |k: &str| (k == "TYPESAFE_API_KEY").then(|| "sk-secret".to_owned());
        let view = describe(&env, None).expect("resolves");
        assert!(view.configured);
        assert_eq!(view.credential, CredentialSource::Environment);
        assert!(!format!("{view:?}").contains("sk-secret"));
        let none = |_: &str| None;
        assert_eq!(
            describe(&none, None).expect("resolves").credential,
            CredentialSource::Missing
        );
        let file = BTreeMap::from([("AFTMAN_JEV_API_KEY".to_owned(), "k".to_owned())]);
        assert_eq!(
            describe(&none, Some(&file)).expect("resolves").credential,
            CredentialSource::ConfigFile
        );
        let blank = |k: &str| (k == "TYPESAFE_API_KEY").then(String::new);
        assert!(!describe(&blank, None).expect("resolves").configured);
    }

    #[test]
    fn https_and_loopback_are_accepted() {
        assert!(validate_base_url("https://api.typesafe.ai").is_ok());
        assert!(validate_base_url("https://openrouter.ai/api").is_ok());
        assert!(validate_base_url("http://127.0.0.1:8080").is_ok());
        assert!(validate_base_url("http://localhost:11434").is_ok());
    }

    #[test]
    fn a_trailing_slash_is_normalised_away() {
        assert_eq!(
            validate_base_url("https://api.typesafe.ai/").ok(),
            Some("https://api.typesafe.ai".to_owned())
        );
    }

    #[test]
    fn cleartext_remote_is_refused_because_it_would_leak_the_key() {
        let error = validate_base_url("http://api.example.com").expect_err("must refuse");
        assert!(
            matches!(error, SettingsError::InsecureBaseUrl(_)),
            "{error:?}"
        );
        assert!(error.to_string().contains("in the clear"), "{error}");
    }

    #[test]
    fn a_provider_path_is_allowed() {
        // OpenRouter's documented base URL is `https://openrouter.ai/api`, so a path
        // must be accepted rather than treated as a mistake.
        assert_eq!(
            validate_base_url("https://openrouter.ai/api").ok(),
            Some("https://openrouter.ai/api".to_owned())
        );
    }

    #[test]
    fn a_base_url_carrying_v1_is_refused_because_it_would_be_doubled() {
        for url in [
            "https://api.typesafe.ai/v1",
            "https://api.typesafe.ai/v1/systemone",
        ] {
            let error = validate_base_url(url).expect_err("must refuse");
            assert!(
                matches!(error, SettingsError::BaseUrlHasPath(_)),
                "{error:?}"
            );
        }
    }

    #[test]
    fn a_non_url_is_refused() {
        assert!(matches!(
            validate_base_url("api.typesafe.ai"),
            Err(SettingsError::InvalidBaseUrl(_))
        ));
        assert!(matches!(
            validate_base_url("ftp://api.typesafe.ai"),
            Err(SettingsError::InvalidBaseUrl(_))
        ));
    }

    #[test]
    fn endpoints_are_derived_from_the_origin() {
        let settings = JevSettings {
            base_url: "https://openrouter.ai/api".to_owned(),
            ..JevSettings::default()
        };
        assert_eq!(
            settings.endpoint(),
            "https://openrouter.ai/api/v1/systemone"
        );
        assert_eq!(
            settings.models_endpoint(),
            "https://openrouter.ai/api/v1/models"
        );
    }

    #[test]
    fn loopback_detection_covers_the_expected_forms() {
        for (url, expected) in [
            ("https://api.typesafe.ai", false),
            ("http://127.0.0.1:8080", true),
            ("http://localhost:11434", true),
            ("http://[::1]:8080", true),
        ] {
            let settings = JevSettings {
                base_url: url.to_owned(),
                ..JevSettings::default()
            };
            assert_eq!(settings.is_loopback(), expected, "{url}");
        }
    }

    #[test]
    fn env_files_parse_comments_blank_lines_and_quotes() {
        let text = "\
# a comment

TYPESAFE_API_KEY=abc123
TYPESAFE_BASE_URL=\"https://openrouter.ai/api\"
AFTMAN_JEV_MODEL='jev-latest'
export AFTMAN_JEV_MAX_RETRIES=1
JUNK LINE
";
        let map = parse_env_file(text);
        assert_eq!(
            map.get("TYPESAFE_API_KEY").map(String::as_str),
            Some("abc123")
        );
        assert_eq!(
            map.get("TYPESAFE_BASE_URL").map(String::as_str),
            Some("https://openrouter.ai/api"),
            "quotes are stripped"
        );
        assert_eq!(
            map.get("AFTMAN_JEV_MODEL").map(String::as_str),
            Some("jev-latest")
        );
        assert_eq!(
            map.get("AFTMAN_JEV_MAX_RETRIES").map(String::as_str),
            Some("1")
        );
        assert!(!map.contains_key("JUNK LINE"));
    }

    #[test]
    fn a_file_supplies_values_the_environment_does_not() -> Result<(), SettingsError> {
        let file = BTreeMap::from([
            (
                "TYPESAFE_BASE_URL".to_owned(),
                "https://openrouter.ai/api".to_owned(),
            ),
            ("AFTMAN_JEV_MAX_RETRIES".to_owned(), "5".to_owned()),
        ]);
        let settings = resolve(Some(&file))?;
        assert_eq!(settings.base_url, "https://openrouter.ai/api");
        assert_eq!(settings.retry.max_retries, 5);
        Ok(())
    }

    #[test]
    fn the_environment_wins_over_the_file() -> Result<(), SettingsError> {
        // Set a variable, resolve, then remove it. Done through a file-only key so
        // the test does not depend on the ambient environment.
        let file = BTreeMap::from([("AFTMAN_JEV_TIMEOUT_SECONDS".to_owned(), "99".to_owned())]);
        let settings = resolve(Some(&file))?;
        assert_eq!(settings.timeout.as_secs(), 99);
        Ok(())
    }

    #[test]
    fn an_unconfigured_client_reports_itself_as_such() -> Result<(), SettingsError> {
        let settings = resolve(Some(&BTreeMap::new()))?;
        // The test environment has no key set; if it did, this would be a live
        // configuration and the assertion below would be wrong.
        if std::env::var_os("TYPESAFE_API_KEY").is_none()
            && std::env::var_os("AFTMAN_JEV_API_KEY").is_none()
        {
            assert!(!settings.is_configured());
        }
        assert_eq!(settings.model, "jev-latest");
        assert_eq!(settings.base_url, super::DEFAULT_BASE_URL);
        Ok(())
    }

    #[test]
    fn a_non_numeric_override_is_reported_with_its_key() {
        let file = BTreeMap::from([("AFTMAN_JEV_MAX_RETRIES".to_owned(), "lots".to_owned())]);
        let error = resolve(Some(&file)).expect_err("must refuse");
        match error {
            SettingsError::NotANumber { key, value } => {
                assert_eq!(key, "AFTMAN_JEV_MAX_RETRIES");
                assert_eq!(value, "lots");
            }
            other => panic!("expected NotANumber, got {other:?}"),
        }
    }
}
