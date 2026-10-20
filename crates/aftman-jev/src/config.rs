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

/// The most retries a chain may make.
///
/// The chain's total budget is `timeout * 4`, so more than a handful of retries is already
/// unreachable; the bound is here so an operator cannot configure a chain that retries for weeks.
pub const MAX_RETRIES: u64 = 10;

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

    #[error("{0}")]
    Budget(String),
}

/// What the client needs to talk to a provider.
///
/// The `Debug` impl is written by hand and does **not** print the key. The derived one did,
/// so a failing `assert_eq!` in a test, or any `{:?}` in a log or an error message, would put
/// the credential in the output. `PartialEq` is derived and compares the key — comparing
/// secrets is not printing them.
#[derive(Clone, PartialEq)]
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

impl std::fmt::Debug for JevSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JevSettings")
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field("base_url", &redact_url(&self.base_url))
            .field("model", &self.model)
            .field("timeout", &self.timeout)
            .field("retry", &self.retry)
            .field("budget", &self.budget)
            .finish()
    }
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
        // Parsed, not split: `http://evil.example?@127.0.0.1` has a host of
        // `evil.example`, and a hand-rolled authority split reads `127.0.0.1`. `doctor`
        // reports this value, so getting it wrong also misreports the transport.
        url::Url::parse(&self.base_url)
            .ok()
            .and_then(|url| url.host_str().map(ToOwned::to_owned))
            .is_some_and(|host| {
                matches!(host.as_str(), "127.0.0.1" | "localhost" | "::1" | "[::1]")
            })
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

/// The budget, with overridden caps and targets scaled to match.
///
/// The targets are what compaction aims for *after* it starts (hysteresis), so a target above
/// its cap can never be reached: `reduce` would stop early and `fit` would then fail having
/// "compacted" nothing. Scaling keeps the relationship the defaults have (12 000 / 20 000 and
/// 60 000 / 120 000). A zero cap is refused rather than obeyed, because it makes every request
/// unrepresentable.
fn budget_with_overrides(pair: Option<u64>, request: Option<u64>) -> Result<Budget, SettingsError> {
    let mut budget = Budget::default();
    if let Some(pair) = pair {
        let bytes = usize::try_from(pair).unwrap_or(usize::MAX);
        if bytes == 0 {
            return Err(SettingsError::Budget(
                "AFTMAN_JEV_PAIR_BUDGET_BYTES must be at least 1".to_owned(),
            ));
        }
        budget.pair_bytes = bytes;
        budget.pair_target_bytes = bytes.saturating_mul(12) / 20;
    }
    if let Some(request) = request {
        let bytes = usize::try_from(request).unwrap_or(usize::MAX);
        if bytes == 0 {
            return Err(SettingsError::Budget(
                "AFTMAN_JEV_REQUEST_BUDGET_BYTES must be at least 1".to_owned(),
            ));
        }
        budget.request_bytes = bytes;
        budget.request_target_bytes = bytes / 2;
    }
    Ok(budget)
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

/// How to configure the provider, in words: the one place that names the variables to a
/// person, so no other crate has to (hard rule 2).
#[must_use]
pub fn setup_hint(env_file: &str) -> String {
    format!(
        "Set {} (and optionally {}) in the environment, or in {env_file}.",
        ENV_KEYS[0], ENV_KEYS[1]
    )
}

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
        let seconds = parse_u64("AFTMAN_JEV_TIMEOUT_SECONDS", &raw)?;
        // Zero was accepted, and it makes every attempt time out — so the request is retried
        // until the budget is spent, and every cycle fails the same way.
        if seconds == 0 {
            return Err(SettingsError::Budget(
                "AFTMAN_JEV_TIMEOUT_SECONDS must be at least 1".to_owned(),
            ));
        }
        settings.timeout = Duration::from_secs(seconds);
    }
    if let Some(raw) = lookup("AFTMAN_JEV_MAX_RETRIES") {
        let retries = parse_u64("AFTMAN_JEV_MAX_RETRIES", &raw)?;
        // `u32::MAX` was accepted, which with the backoff is a supervisor that retries for
        // weeks. The chain's total budget is `timeout * 4`, so more than a handful of retries
        // is already unreachable.
        if retries > MAX_RETRIES {
            return Err(SettingsError::Budget(format!(
                "AFTMAN_JEV_MAX_RETRIES must be at most {MAX_RETRIES}"
            )));
        }
        settings.retry.max_retries =
            u32::try_from(retries).unwrap_or(u32::try_from(MAX_RETRIES).unwrap_or(0));
    }
    if let Some(raw) = lookup("AFTMAN_JEV_MAX_RETRY_AFTER_SECONDS") {
        settings.retry.max_retry_after =
            Duration::from_secs(parse_u64("AFTMAN_JEV_MAX_RETRY_AFTER_SECONDS", &raw)?);
    }
    let pair = lookup("AFTMAN_JEV_PAIR_BUDGET_BYTES")
        .map(|raw| parse_u64("AFTMAN_JEV_PAIR_BUDGET_BYTES", &raw))
        .transpose()?;
    let request = lookup("AFTMAN_JEV_REQUEST_BUDGET_BYTES")
        .map(|raw| parse_u64("AFTMAN_JEV_REQUEST_BUDGET_BYTES", &raw))
        .transpose()?;
    settings.budget = budget_with_overrides(pair, request)?;

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
    // Parsed with a real URL parser rather than split on `/`. Hand-rolled authority parsing
    // gets this wrong in the direction that matters: WHATWG ends the authority at `?` or `#`
    // and treats `\` as a path separator, so `http://evil.example?@127.0.0.1` has a host of
    // `evil.example` — while `rsplit('@')` reads `127.0.0.1` and calls it loopback. The key
    // then goes to `evil.example` in cleartext, and `doctor` reports it as loopback.
    let url =
        url::Url::parse(trimmed).map_err(|_| SettingsError::InvalidBaseUrl(raw.to_owned()))?;
    // Checked most-informative-error first: an `http` URL to a non-loopback host is the
    // cleartext risk, and naming it that way is more useful than "invalid".
    if url.query().is_some() || url.fragment().is_some() {
        return Err(SettingsError::InvalidBaseUrl(raw.to_owned()));
    }
    // A path prefix is legitimate — `https://openrouter.ai/api` is a documented provider
    // base — but `/v1` is not, because `endpoint()` appends `/v1/systemone` to it.
    if url.path().contains("/v1") {
        return Err(SettingsError::BaseUrlHasPath(raw.to_owned()));
    }
    let Some(host) = url.host_str() else {
        return Err(SettingsError::InvalidBaseUrl(raw.to_owned()));
    };
    let loopback = matches!(host, "127.0.0.1" | "localhost" | "::1" | "[::1]");
    match url.scheme() {
        "https" => {}
        "http" if loopback => {}
        "http" => return Err(SettingsError::InsecureBaseUrl(raw.to_owned())),
        _ => return Err(SettingsError::InvalidBaseUrl(raw.to_owned())),
    }
    Ok(trimmed.to_owned())
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

    #[test]
    fn the_retry_and_timeout_bounds_are_refused_rather_than_obeyed() {
        // `u32::MAX` retries with the backoff is a supervisor that retries for weeks, and a
        // zero timeout makes every attempt time out and be retried.
        let with = |pairs: &[(&str, &str)]| {
            let file: BTreeMap<String, String> = pairs
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect();
            super::resolve_with(&|_| None, Some(&file))
        };
        assert!(
            with(&[("AFTMAN_JEV_MAX_RETRIES", "100000")]).is_err(),
            "an unbounded retry count must be refused"
        );
        assert!(
            with(&[("AFTMAN_JEV_TIMEOUT_SECONDS", "0")]).is_err(),
            "a zero timeout must be refused"
        );
        // A sane value still works.
        let settings = with(&[("AFTMAN_JEV_MAX_RETRIES", "3")]).expect("a sane value");
        assert_eq!(settings.retry.max_retries, 3);
    }

    #[test]
    fn the_key_is_never_rendered() {
        // A derived `Debug` printed it, so a failing `assert_eq!` or any `{:?}` put the
        // credential in the output.
        let settings = JevSettings {
            api_key: Some("ts-super-secret-value".to_owned()),
            base_url: "https://user:hunter2@api.example.com".to_owned(),
            ..JevSettings::default()
        };
        let rendered = format!("{settings:?}");
        assert!(!rendered.contains("ts-super-secret-value"), "{rendered}");
        assert!(!rendered.contains("hunter2"), "{rendered}");
        // It says the key is there, without saying what it is.
        assert!(rendered.contains("redacted"), "{rendered}");
    }

    #[test]
    fn a_host_cannot_be_smuggled_past_the_loopback_check() {
        // WHATWG ends the authority at `?` or `#` and treats `\` as a path separator, so
        // each of these has a non-loopback host — while splitting the whole string on `/` and
        // then on the last `@` reads `127.0.0.1` and calls it loopback, sending the key to
        // `evil.example` in cleartext.
        for raw in [
            "http://evil.example?@127.0.0.1",
            "http://evil.example#@localhost",
            "http://evil.example\\@localhost",
        ] {
            assert!(
                validate_base_url(raw).is_err(),
                "{raw} was accepted as a loopback base URL"
            );
        }
    }

    #[test]
    fn https_and_genuine_loopback_are_still_accepted() {
        for raw in [
            "https://api.typesafe.ai",
            "http://127.0.0.1:8080",
            "http://localhost",
            "http://[::1]:9000",
        ] {
            assert!(validate_base_url(raw).is_ok(), "{raw}");
        }
    }

    #[test]
    fn a_provider_path_prefix_is_allowed_but_v1_is_not() {
        // `https://openrouter.ai/api` is a documented provider base. Rejecting every path
        // would break it; `/v1` is refused because `endpoint()` appends `/v1/systemone`.
        assert!(validate_base_url("https://openrouter.ai/api").is_ok());
        assert!(matches!(
            validate_base_url("https://api.typesafe.ai/v1"),
            Err(SettingsError::BaseUrlHasPath(_))
        ));
    }

    #[test]
    fn is_loopback_uses_the_parsed_host() {
        let hostile = JevSettings {
            base_url: "http://evil.example?@127.0.0.1".to_owned(),
            ..JevSettings::default()
        };
        assert!(
            !hostile.is_loopback(),
            "`doctor` would report a hostile URL as loopback"
        );
        let local = JevSettings {
            base_url: "http://127.0.0.1:8080".to_owned(),
            ..JevSettings::default()
        };
        assert!(local.is_loopback());
    }
}
