//! The `[web]` configuration: where the daemon binds, who may talk to it, and which
//! directories a run may be pointed at.
//!
//! The contract this file enforces is [08-security.md §4](../../../docs/08-security.md):
//! **any non-loopback bind with `auth != "token"` is a startup error, not a warning** —
//! including `auth = "loopback"`, which an earlier revision left undefined for a
//! `--bind 0.0.0.0`. Aftman's web API starts agents and steers them, so a bind on
//! `0.0.0.0` without a token is a remote-code-execution surface by construction.

use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::net::{AddrParseError, SocketAddr};
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// The default bind. Loopback, so the default posture needs no token.
pub const DEFAULT_BIND: &str = "127.0.0.1:8787";

/// The environment variable that supplies the token under `auth = "token"`.
pub const TOKEN_ENV: &str = "AFTMAN_WEB_TOKEN";

/// Who may use the web surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthMode {
    /// No authentication at all. Only meaningful on loopback, and refused elsewhere.
    None,
    /// No token; anything that arrives from a loopback peer is trusted, and a non-loopback
    /// peer is refused as defence in depth. This is what `tailscale serve` relies on: it
    /// proxies from loopback, so the port is never opened on a physical interface.
    Loopback,
    /// `Authorization: Bearer <token>`, compared in constant time.
    Token,
}

impl AuthMode {
    /// The string used in configuration and in error messages.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Loopback => "loopback",
            Self::Token => "token",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "none" => Some(Self::None),
            "loopback" => Some(Self::Loopback),
            "token" => Some(Self::Token),
            _ => None,
        }
    }
}

/// Everything [`WebConfig::from_parts`] needs, as a value.
///
/// A struct rather than seven parameters: a call site that mixes up `allowed_origins` and
/// `allowed_hosts` would compile, and one of those is a network-traffic allow-list.
#[derive(Debug, Clone)]
pub struct WebParts {
    pub bind: SocketAddr,
    pub auth: AuthMode,
    pub token: Option<String>,
    pub allowed_repo_roots: Vec<PathBuf>,
    pub allow_non_git_repos: bool,
    pub allowed_origins: Vec<String>,
    pub allowed_hosts: Vec<String>,
}

impl Default for WebParts {
    fn default() -> Self {
        Self {
            bind: DEFAULT_BIND.parse().expect("the default bind parses"),
            auth: AuthMode::Loopback,
            token: None,
            allowed_repo_roots: Vec::new(),
            allow_non_git_repos: false,
            allowed_origins: Vec::new(),
            allowed_hosts: Vec::new(),
        }
    }
}

impl WebParts {
    /// The default posture at `bind`: `auth = "loopback"`, no token, no extra hosts.
    #[must_use]
    pub fn loopback(bind: SocketAddr) -> Self {
        Self {
            bind,
            auth: AuthMode::Loopback,
            ..Self::default()
        }
    }
}

/// A validated web configuration. Construct one through [`WebConfig::load`] or
/// [`WebConfig::from_parts`]; a validated value is the only thing [`crate::serve`] accepts,
/// so "refuse to start" cannot be skipped by a caller that forgot to check.
#[derive(Clone)]
pub struct WebConfig {
    enabled: bool,
    bind: SocketAddr,
    auth: AuthMode,
    /// The token, when one was configured. `None` under `token` auth means "generate one at
    /// startup and print it once", which is what `serve` does.
    token: Option<String>,
    allowed_repo_roots: Vec<PathBuf>,
    allow_non_git_repos: bool,
    /// Origins allowed to make cross-origin requests. Empty by default: the UI is
    /// same-origin, so no CORS headers are sent at all.
    allowed_origins: Vec<String>,
    /// `Host` header values accepted besides the loopback literals.
    ///
    /// `tailscale serve` forwards the tailnet name, and a rebinding page forwards its own
    /// name, so the two are indistinguishable except by this list. Empty by default: a
    /// loopback `Host` is the only one accepted.
    allowed_hosts: Vec<String>,
    /// Where each value came from, for `/config` and for `doctor`.
    sources: BTreeMap<String, String>,
}

impl fmt::Debug for WebConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The token is a credential. `{:?}` on the configuration must never print it.
        f.debug_struct("WebConfig")
            .field("enabled", &self.enabled)
            .field("bind", &self.bind)
            .field("auth", &self.auth)
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .field("allowed_repo_roots", &self.allowed_repo_roots)
            .field("allow_non_git_repos", &self.allow_non_git_repos)
            .field("allowed_origins", &self.allowed_origins)
            .field("allowed_hosts", &self.allowed_hosts)
            .finish()
    }
}

impl Default for WebConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: DEFAULT_BIND.parse().expect("the default bind parses"),
            auth: AuthMode::Loopback,
            token: None,
            allowed_repo_roots: Vec::new(),
            allow_non_git_repos: false,
            allowed_origins: Vec::new(),
            allowed_hosts: Vec::new(),
            sources: BTreeMap::new(),
        }
    }
}

impl WebConfig {
    /// Where the `[web]` table lives: `<config home>/aftman/config.toml`.
    ///
    /// The provider `.env` already lives in `<config home>/aftman/`, so this is the
    /// directory the user already knows.
    #[must_use]
    pub fn default_path(config_home: Option<&Path>) -> Option<PathBuf> {
        config_home.map(|dir| dir.join("aftman/config.toml"))
    }

    /// Resolve the configuration from `<config home>/aftman/config.toml` and the
    /// `AFTMAN_WEB_*` environment.
    pub fn load(
        config_home: Option<&Path>,
        get: &dyn Fn(&str) -> Option<String>,
    ) -> Result<Self, WebConfigError> {
        let path = Self::default_path(config_home);
        let mut raw = match path.as_deref() {
            Some(path) => read_file(path)?,
            None => RawFile::default(),
        };
        let mut sources = raw.sources(path.as_deref());

        // The environment overrides the file, so a one-off `AFTMAN_WEB_BIND` does not
        // require editing a file, and a token never has to be written to disk.
        if let Some(value) = get("AFTMAN_WEB_ENABLED") {
            raw.web.enabled = Some(parse_bool("AFTMAN_WEB_ENABLED", &value)?);
            sources.insert("web.enabled".to_owned(), "AFTMAN_WEB_ENABLED".to_owned());
        }
        if let Some(value) = get("AFTMAN_WEB_BIND") {
            raw.web.bind = Some(value);
            sources.insert("web.bind".to_owned(), "AFTMAN_WEB_BIND".to_owned());
        }
        if let Some(value) = get("AFTMAN_WEB_AUTH") {
            raw.web.auth = Some(value);
            sources.insert("web.auth".to_owned(), "AFTMAN_WEB_AUTH".to_owned());
        }
        if let Some(value) = get(TOKEN_ENV) {
            raw.web.token = Some(value);
            sources.insert("web.token".to_owned(), TOKEN_ENV.to_owned());
        }
        if let Some(value) = get("AFTMAN_WEB_ALLOWED_REPO_ROOTS") {
            raw.web.allowed_repo_roots = Some(
                value
                    .split(':')
                    .filter(|part| !part.is_empty())
                    .map(PathBuf::from)
                    .collect(),
            );
            sources.insert(
                "web.allowed_repo_roots".to_owned(),
                "AFTMAN_WEB_ALLOWED_REPO_ROOTS".to_owned(),
            );
        }

        let home = get("HOME").map(PathBuf::from);
        let config = Self::from_raw(&raw.web, &sources, home.as_deref(), get)?;
        Ok(config)
    }

    /// Build and validate from parts. This is the single place the startup rules live, and
    /// the only constructor that skips reading a file, so tests and the CLI can use it.
    pub fn from_parts(parts: WebParts, home: Option<&Path>) -> Result<Self, WebConfigError> {
        let WebParts {
            bind,
            auth,
            token,
            allowed_repo_roots,
            allow_non_git_repos,
            allowed_origins,
            allowed_hosts,
        } = parts;
        let mut sources = BTreeMap::new();
        sources.insert("web.bind".to_owned(), "argument".to_owned());
        sources.insert("web.auth".to_owned(), "argument".to_owned());
        let mut config = Self {
            enabled: true,
            bind,
            auth,
            token,
            allowed_repo_roots,
            allow_non_git_repos,
            allowed_origins,
            allowed_hosts,
            sources,
        };
        config.apply_defaults(home);
        config.validate(bind, auth, home)?;
        Ok(config)
    }

    fn from_raw(
        raw: &RawWeb,
        sources: &BTreeMap<String, String>,
        home: Option<&Path>,
        get: &dyn Fn(&str) -> Option<String>,
    ) -> Result<Self, WebConfigError> {
        let enabled = raw.enabled.unwrap_or(false);
        let bind_text = raw.bind.clone().unwrap_or_else(|| DEFAULT_BIND.to_owned());
        let bind: SocketAddr =
            bind_text
                .parse()
                .map_err(|source: AddrParseError| WebConfigError::Bind {
                    value: bind_text.clone(),
                    source,
                })?;
        let auth = match raw.auth.as_deref() {
            Some(value) => AuthMode::parse(value).ok_or_else(|| WebConfigError::AuthMode {
                value: value.to_owned(),
            })?,
            None => AuthMode::Loopback,
        };
        let token = raw
            .token
            .clone()
            .or_else(|| get(TOKEN_ENV))
            .filter(|value| !value.is_empty());
        let mut config = Self {
            enabled,
            bind,
            auth,
            token,
            allowed_repo_roots: raw.allowed_repo_roots.clone().unwrap_or_default(),
            allow_non_git_repos: raw.allow_non_git_repos.unwrap_or(false),
            allowed_origins: raw.allowed_origins.clone().unwrap_or_default(),
            allowed_hosts: raw.allowed_hosts.clone().unwrap_or_default(),
            sources: sources.clone(),
        };
        config.apply_defaults(home);
        config.validate(bind, auth, home)?;
        Ok(config)
    }

    /// Under `auth = "loopback"` the default repo root is `$HOME`: a local user is already
    /// trusted with everything that user can read. Under `auth = "token"` there is
    /// deliberately no default — see [`Self::validate`].
    fn apply_defaults(&mut self, home: Option<&Path>) {
        if self.allowed_repo_roots.is_empty()
            && self.auth == AuthMode::Loopback
            && let Some(home) = home
        {
            self.allowed_repo_roots.push(home.to_path_buf());
            self.sources
                .insert("web.allowed_repo_roots".to_owned(), "$HOME".to_owned());
        }
        self.sources
            .entry("web.enabled".to_owned())
            .or_insert_with(|| "default".to_owned());
        self.sources
            .entry("web.bind".to_owned())
            .or_insert_with(|| "default".to_owned());
        self.sources
            .entry("web.auth".to_owned())
            .or_insert_with(|| "default".to_owned());
        if self.allowed_repo_roots.is_empty() {
            self.sources
                .insert("web.allowed_repo_roots".to_owned(), "unset".to_owned());
        }
    }

    fn validate(
        &self,
        bind: SocketAddr,
        auth: AuthMode,
        home: Option<&Path>,
    ) -> Result<(), WebConfigError> {
        // A surface that is off serves nothing, so its posture cannot be unsafe and it must
        // not be able to stop the daemon from starting. This matters: `aftman run` starts a
        // daemon, and a stale `[web]` stanza — a bind, an auth mode, a missing `$HOME` for
        // the default repo root — must never turn into "the supervisor will not run".
        if !self.enabled {
            return Ok(());
        }
        // The whole point of the module. Note this refuses `loopback` too, not only `none`:
        // a `--bind 0.0.0.0` with the default auth must not serve LAN peers.
        if !is_loopback(bind) && auth != AuthMode::Token {
            return Err(WebConfigError::NonLoopbackRequiresToken { bind, auth });
        }
        if auth == AuthMode::Token && self.allowed_repo_roots.is_empty() {
            // A token holder can start a run and read its observation back, so the token is
            // not a substitute for deciding which directories runs may read.
            return Err(WebConfigError::RepoRootsRequired);
        }
        if auth == AuthMode::Loopback && self.allowed_repo_roots.is_empty() && home.is_none() {
            return Err(WebConfigError::NoHome);
        }
        for root in &self.allowed_repo_roots {
            if !root.is_absolute() {
                return Err(WebConfigError::RepoRootNotAbsolute { path: root.clone() });
            }
        }
        if let Some(token) = &self.token
            && !token.chars().all(is_safe_token_char)
        {
            // The token is written into a `Set-Cookie` header and a URL. A token containing
            // `;` could add cookie attributes, and one containing a newline could split the
            // header. Refusing at startup is better than silently not setting the cookie.
            return Err(WebConfigError::TokenCharset);
        }
        Ok(())
    }

    #[must_use]
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    #[must_use]
    pub fn bind(&self) -> SocketAddr {
        self.bind
    }

    #[must_use]
    pub fn auth(&self) -> AuthMode {
        self.auth
    }

    #[must_use]
    pub fn allowed_repo_roots(&self) -> &[PathBuf] {
        &self.allowed_repo_roots
    }

    #[must_use]
    pub fn allow_non_git_repos(&self) -> bool {
        self.allow_non_git_repos
    }

    #[must_use]
    pub fn allowed_origins(&self) -> &[String] {
        &self.allowed_origins
    }

    #[must_use]
    pub fn allowed_hosts(&self) -> &[String] {
        &self.allowed_hosts
    }

    /// The configured token, if one was supplied. Under `token` auth a `None` means the
    /// caller must generate one and print it once.
    #[must_use]
    pub fn configured_token(&self) -> Option<&str> {
        self.token.as_deref()
    }

    /// The resolved view, for `/v1/web` and for `doctor`. The token is reported as present
    /// or absent, never printed.
    #[must_use]
    pub fn describe(&self) -> serde_json::Value {
        serde_json::json!({
            "enabled": self.enabled,
            "bind": self.bind.to_string(),
            "loopback": is_loopback(self.bind),
            "auth": self.auth.as_str(),
            "token": if self.token.is_some() { "configured" } else { "none" },
            "allowed_repo_roots": self
                .allowed_repo_roots
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>(),
            "allow_non_git_repos": self.allow_non_git_repos,
            "allowed_origins": self.allowed_origins,
            "allowed_hosts": self.allowed_hosts,
            "sources": self.sources,
        })
    }

    /// A URL a person can paste into a browser. The token rides in the query string only
    /// when there is one, and this string is printed exactly once by `serve`.
    #[must_use]
    pub fn url(&self, token: Option<&str>) -> String {
        // A wildcard bind has no address a person can type, so the URL points at loopback —
        // which is where the surface is reachable on this machine. A LAN bind is printed with
        // the address it was given, because that is what a peer would use.
        let host = match self.bind.ip() {
            std::net::IpAddr::V4(v4) if v4.is_unspecified() => "127.0.0.1".to_owned(),
            std::net::IpAddr::V4(v4) => v4.to_string(),
            std::net::IpAddr::V6(v6) if v6.is_unspecified() => "[::1]".to_owned(),
            std::net::IpAddr::V6(v6) => format!("[{v6}]"),
        };
        match token {
            Some(token) => format!("http://{host}:{}/?token={token}", self.bind.port()),
            None => format!("http://{host}:{}/", self.bind.port()),
        }
    }
}

/// Whether a bind address is loopback. `0.0.0.0` and `::` are *not*: they accept traffic
/// from every interface, which is exactly the case that must carry a token.
pub fn is_loopback(addr: SocketAddr) -> bool {
    addr.ip().is_loopback()
}

/// Generate a token when `auth = "token"` was configured without one. Returned with
/// `true` so the caller knows to print it once rather than on every start.
pub fn generate_token() -> Result<String, WebConfigError> {
    // 32 bytes of OS randomness, hex. `getrandom` is already in the tree via `uuid`.
    let mut bytes = [0u8; 32];
    getrandom::getrandom(&mut bytes).map_err(|error| WebConfigError::Random {
        message: error.to_string(),
    })?;
    let mut token = String::with_capacity(64);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(token, "{byte:02x}");
    }
    Ok(token)
}

/// The characters a token may contain: the URL-safe base64 and hex alphabets, plus the
/// padding character. Anything else is refused rather than escaped: a token is a secret, and
/// a secret that needs escaping to travel is a configuration mistake.
fn is_safe_token_char(character: char) -> bool {
    character.is_ascii_alphanumeric()
        || matches!(character, '-' | '_' | '.' | '~' | '+' | '/' | '=')
}

fn read_file(path: &Path) -> Result<RawFile, WebConfigError> {
    match std::fs::read_to_string(path) {
        Ok(source) => parse(&source).map_err(|source| WebConfigError::Parse {
            path: path.to_path_buf(),
            source,
        }),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(RawFile::default()),
        Err(source) => Err(WebConfigError::Read {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn parse(source: &str) -> Result<RawFile, toml::de::Error> {
    toml::from_str(source)
}

fn parse_bool(name: &str, value: &str) -> Result<bool, WebConfigError> {
    match value {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(WebConfigError::Boolean {
            name: name.to_owned(),
            value: value.to_owned(),
        }),
    }
}

/// The file shape. `deny_unknown_fields` so a typo in a key that grants network access is a
/// startup error rather than a silently ignored line.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFile {
    #[serde(default)]
    web: RawWeb,
}

impl RawFile {
    fn sources(&self, path: Option<&Path>) -> BTreeMap<String, String> {
        let mut sources = BTreeMap::new();
        if let Some(path) = path {
            for key in ["enabled", "bind", "auth", "token", "allowed_repo_roots"] {
                sources.insert(format!("web.{key}"), path.display().to_string());
            }
        }
        sources
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawWeb {
    enabled: Option<bool>,
    bind: Option<String>,
    auth: Option<String>,
    token: Option<String>,
    allowed_repo_roots: Option<Vec<PathBuf>>,
    #[serde(default)]
    allow_non_git_repos: Option<bool>,
    #[serde(default)]
    allowed_origins: Option<Vec<String>>,
    #[serde(default)]
    allowed_hosts: Option<Vec<String>>,
}

/// Everything that can stop the web surface from starting.
#[derive(Debug, thiserror::Error)]
pub enum WebConfigError {
    #[error("cannot read {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("cannot parse {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("web.bind {value:?} is not an address and port: {source}")]
    Bind {
        value: String,
        #[source]
        source: AddrParseError,
    },
    #[error("web.auth {value:?} is not one of none, loopback, token")]
    AuthMode { value: String },
    #[error("{name} wants a boolean, not {value:?}")]
    Boolean { name: String, value: String },
    #[error(
        "refusing to bind {bind}: a bind that is not loopback requires web.auth = \"token\", \
         and this configuration has auth = \"{auth}\". Aftman's web API starts agents and \
         steers them; exposing it without a token is a remote-code-execution surface. Set \
         web.auth = \"token\" and provide {TOKEN_ENV}, or bind 127.0.0.1",
        auth = .auth.as_str()
    )]
    NonLoopbackRequiresToken { bind: SocketAddr, auth: AuthMode },
    #[error(
        "web.auth = \"token\" requires web.allowed_repo_roots: a token holder can start a run \
         and read its observation back, so the token is not a substitute for deciding which \
         directories a run may read"
    )]
    RepoRootsRequired,
    #[error("web.allowed_repo_roots must be an absolute path, and {path:?} is not")]
    RepoRootNotAbsolute { path: PathBuf },
    #[error(
        "web.auth = \"loopback\" defaults web.allowed_repo_roots to $HOME, and HOME is not set"
    )]
    NoHome,
    #[error("cannot generate a web token: {message}")]
    Random { message: String },
    #[error(
        "the web token contains a character outside A-Z a-z 0-9 - _ . ~ + / =: it is written \
         into a Set-Cookie header and a URL, so a `;` or a newline would corrupt both"
    )]
    TokenCharset,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |name| {
            pairs
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| (*value).to_owned())
        }
    }

    // Exit criterion: binding 0.0.0.0 without a token fails to start.
    #[test]
    fn a_non_loopback_bind_without_a_token_is_a_startup_error() {
        let bind: SocketAddr = "0.0.0.0:8787".parse().expect("parses");
        let error = WebConfig::from_parts(
            WebParts {
                bind,
                auth: AuthMode::Loopback,
                token: None,
                allowed_repo_roots: vec![PathBuf::from("/home/me")],
                allow_non_git_repos: false,
                ..WebParts::default()
            },
            Some(Path::new("/home/me")),
        )
        .expect_err("must refuse");
        assert!(
            matches!(error, WebConfigError::NonLoopbackRequiresToken { .. }),
            "got {error:?}"
        );
        // The message has to say what to do, because it is printed at startup.
        let message = error.to_string();
        assert!(message.contains("web.auth = \"token\""), "{message}");
        assert!(message.contains(TOKEN_ENV), "{message}");
    }

    // The review's earlier revision only refused `auth = "none"`, which left it undefined
    // whether the default auth served LAN peers. `loopback` must be refused too.
    #[test]
    fn the_default_auth_is_refused_on_a_non_loopback_bind() {
        for auth in [AuthMode::None, AuthMode::Loopback] {
            let error = WebConfig::from_parts(
                WebParts {
                    bind: "0.0.0.0:8787".parse().expect("parses"),
                    auth,
                    allowed_repo_roots: vec![PathBuf::from("/home/me")],
                    ..WebParts::default()
                },
                Some(Path::new("/home/me")),
            )
            .expect_err("must refuse");
            assert!(
                matches!(error, WebConfigError::NonLoopbackRequiresToken { .. }),
                "auth {} got {error:?}",
                auth.as_str()
            );
        }
    }

    #[test]
    fn an_ipv6_unspecified_bind_is_not_loopback() {
        let config = WebConfig::from_parts(
            WebParts {
                bind: "[::]:8787".parse().expect("parses"),
                auth: AuthMode::Token,
                token: Some("t".to_owned()),
                allowed_repo_roots: vec![PathBuf::from("/home/me")],
                allow_non_git_repos: false,
                ..WebParts::default()
            },
            Some(Path::new("/home/me")),
        );
        assert!(config.is_ok(), "a token makes it legal: {config:?}");
        assert!(!is_loopback("[::]:8787".parse().expect("parses")));
    }

    #[test]
    fn a_non_loopback_bind_with_a_token_is_allowed() {
        let config = WebConfig::from_parts(
            WebParts {
                bind: "0.0.0.0:8787".parse().expect("parses"),
                auth: AuthMode::Token,
                token: Some("secret".to_owned()),
                allowed_repo_roots: vec![PathBuf::from("/home/me/Development")],
                allow_non_git_repos: false,
                ..WebParts::default()
            },
            Some(Path::new("/home/me")),
        )
        .expect("a token makes a LAN bind legal");
        assert_eq!(config.auth(), AuthMode::Token);
    }

    #[test]
    fn token_auth_requires_explicit_repo_roots() {
        // $HOME must NOT be used as a default here: with a token the user is not
        // necessarily on this machine.
        let error = WebConfig::from_parts(
            WebParts {
                bind: "127.0.0.1:8787".parse().expect("parses"),
                auth: AuthMode::Token,
                token: Some("secret".to_owned()),
                ..WebParts::default()
            },
            Some(Path::new("/home/me")),
        )
        .expect_err("must refuse");
        assert!(
            matches!(error, WebConfigError::RepoRootsRequired),
            "got {error:?}"
        );
    }

    #[test]
    fn loopback_auth_defaults_repo_roots_to_home() {
        let config = WebConfig::load(None, &env(&[("HOME", "/home/me")])).expect("resolves");
        assert_eq!(config.allowed_repo_roots(), [PathBuf::from("/home/me")]);
        assert_eq!(
            config.describe()["sources"]["web.allowed_repo_roots"],
            "$HOME"
        );
    }

    #[test]
    fn an_enabled_loopback_surface_without_home_is_an_error_rather_than_an_empty_allow_list() {
        // An empty allow-list would reject every repo at request time, which reads as
        // "aftman is broken" instead of "your environment is". Only when the surface is on:
        // a disabled surface accepts every repo because it accepts no requests.
        let error = WebConfig::load(None, &env(&[("AFTMAN_WEB_ENABLED", "true")]))
            .expect_err("must refuse");
        assert!(matches!(error, WebConfigError::NoHome), "got {error:?}");

        // With the surface off, the same environment is fine: nothing is listening.
        let config = WebConfig::load(None, &env(&[])).expect("a disabled surface loads");
        assert!(!config.enabled());
    }

    #[test]
    fn a_relative_repo_root_is_refused() {
        let error = WebConfig::from_parts(
            WebParts {
                bind: "127.0.0.1:8787".parse().expect("parses"),
                auth: AuthMode::Loopback,
                token: None,
                allowed_repo_roots: vec![PathBuf::from("Development")],
                allow_non_git_repos: false,
                ..WebParts::default()
            },
            Some(Path::new("/home/me")),
        )
        .expect_err("must refuse");
        assert!(
            matches!(error, WebConfigError::RepoRootNotAbsolute { .. }),
            "got {error:?}"
        );
    }

    #[test]
    fn the_environment_overrides_the_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = WebConfig::default_path(Some(dir.path())).expect("path");
        std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        std::fs::write(&path, "[web]\nenabled = true\nbind = \"127.0.0.1:9000\"\n").expect("write");
        let config = WebConfig::load(
            Some(dir.path()),
            &env(&[("HOME", "/home/me"), ("AFTMAN_WEB_BIND", "127.0.0.1:9100")]),
        )
        .expect("resolves");
        assert!(config.enabled());
        assert_eq!(config.bind().port(), 9100);
        assert_eq!(config.describe()["sources"]["web.bind"], "AFTMAN_WEB_BIND");
    }

    #[test]
    fn a_missing_config_file_means_the_defaults() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config =
            WebConfig::load(Some(dir.path()), &env(&[("HOME", "/home/me")])).expect("resolves");
        assert!(!config.enabled(), "the web surface is opt-in");
        assert_eq!(config.bind(), DEFAULT_BIND.parse().expect("parses"));
        assert_eq!(config.auth(), AuthMode::Loopback);
    }

    #[test]
    fn a_typo_in_the_file_is_an_error_not_a_default() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = WebConfig::default_path(Some(dir.path())).expect("path");
        std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        std::fs::write(&path, "[web]\nbnid = \"0.0.0.0:8787\"\n").expect("write");
        let error = WebConfig::load(Some(dir.path()), &env(&[("HOME", "/home/me")]))
            .expect_err("must refuse");
        assert!(
            matches!(error, WebConfigError::Parse { .. }),
            "got {error:?}"
        );
    }

    #[test]
    fn a_token_is_never_rendered() {
        let config = WebConfig::from_parts(
            WebParts {
                bind: "127.0.0.1:8787".parse().expect("parses"),
                auth: AuthMode::Token,
                token: Some("s3cret-token".to_owned()),
                allowed_repo_roots: vec![PathBuf::from("/home/me")],
                allow_non_git_repos: false,
                ..WebParts::default()
            },
            Some(Path::new("/home/me")),
        )
        .expect("resolves");
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("s3cret-token"), "{rendered}");
        assert!(rendered.contains("<redacted>"), "{rendered}");
        // ...and neither is it in the JSON the /config page reports.
        let described = config.describe().to_string();
        assert!(!described.contains("s3cret-token"), "{described}");
        assert!(described.contains("configured"), "{described}");
    }

    // A disabled surface must not veto the daemon. `aftman run` starts one, so a stale
    // `[web]` stanza would otherwise stop the supervisor from running at all.
    #[test]
    fn a_disabled_surface_cannot_stop_the_daemon_from_starting() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = WebConfig::default_path(Some(dir.path())).expect("path");
        std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        // Every posture rule this file has, all violated at once, with the surface off.
        std::fs::write(
            &path,
            "[web]\nenabled = false\nbind = \"0.0.0.0:8787\"\nauth = \"none\"\n\
             allowed_repo_roots = [\"relative/path\"]\n",
        )
        .expect("write");
        // ...and no HOME, so the loopback default has nowhere to come from either.
        let config = WebConfig::load(Some(dir.path()), &env(&[]))
            .expect("a disabled surface must load whatever the posture says");
        assert!(!config.enabled());

        // The same stanza with the surface on is refused.
        std::fs::write(
            &path,
            "[web]\nenabled = true\nbind = \"0.0.0.0:8787\"\nauth = \"none\"\n",
        )
        .expect("write");
        let error = WebConfig::load(Some(dir.path()), &env(&[("HOME", "/home/me")]))
            .expect_err("enabled must refuse");
        assert!(
            matches!(error, WebConfigError::NonLoopbackRequiresToken { .. }),
            "got {error:?}"
        );
    }

    #[test]
    fn a_token_that_would_corrupt_a_header_or_url_is_refused() {
        // The token is written into `Set-Cookie` and into a URL. `;` would add cookie
        // attributes and a newline would split the header, so this cannot be escaped away:
        // it is a configuration mistake and startup is where it belongs.
        for hostile in [
            "abc; Domain=evil.example",
            "abc\r\nX-Injected: 1",
            "abc def",
        ] {
            let error = WebConfig::from_parts(
                WebParts {
                    bind: "127.0.0.1:8787".parse().expect("parses"),
                    auth: AuthMode::Token,
                    token: Some(hostile.to_owned()),
                    allowed_repo_roots: vec![PathBuf::from("/home/me")],
                    ..WebParts::default()
                },
                Some(Path::new("/home/me")),
            )
            .expect_err("must refuse");
            assert!(
                matches!(error, WebConfigError::TokenCharset),
                "{hostile:?} got {error:?}"
            );
        }
        // The alphabets a real token uses are accepted: hex, base64url, base64.
        for good in ["deadbeef", "a_b-c.d~e", "ab+/cd=="] {
            assert!(
                WebConfig::from_parts(
                    WebParts {
                        bind: "127.0.0.1:8787".parse().expect("parses"),
                        auth: AuthMode::Token,
                        token: Some(good.to_owned()),
                        allowed_repo_roots: vec![PathBuf::from("/home/me")],
                        ..WebParts::default()
                    },
                    Some(Path::new("/home/me")),
                )
                .is_ok(),
                "{good:?} must be accepted"
            );
        }
    }

    #[test]
    fn a_generated_token_is_long_and_unique() {
        let first = generate_token().expect("generates");
        let second = generate_token().expect("generates");
        assert_eq!(first.len(), 64);
        assert_ne!(first, second);
        assert!(first.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn a_wildcard_bind_prints_a_url_a_person_can_open() {
        // `http://0.0.0.0:8787/` is not something to paste into a browser.
        let mut config = WebConfig::from_parts(
            WebParts {
                bind: "0.0.0.0:8787".parse().expect("parses"),
                auth: AuthMode::Token,
                token: Some("t".to_owned()),
                allowed_repo_roots: vec![PathBuf::from("/home/me")],
                ..WebParts::default()
            },
            Some(Path::new("/home/me")),
        )
        .expect("resolves");
        assert_eq!(config.url(None), "http://127.0.0.1:8787/");
        config = WebConfig::from_parts(
            WebParts {
                bind: "[::]:8787".parse().expect("parses"),
                auth: AuthMode::Token,
                token: Some("t".to_owned()),
                allowed_repo_roots: vec![PathBuf::from("/home/me")],
                ..WebParts::default()
            },
            Some(Path::new("/home/me")),
        )
        .expect("resolves");
        assert_eq!(config.url(None), "http://[::1]:8787/");
    }

    #[test]
    fn the_url_carries_the_token_only_when_given_one() {
        let config = WebConfig::from_parts(
            WebParts {
                bind: "127.0.0.1:8787".parse().expect("parses"),
                auth: AuthMode::Loopback,
                token: None,
                allowed_repo_roots: vec![PathBuf::from("/home/me")],
                allow_non_git_repos: false,
                ..WebParts::default()
            },
            Some(Path::new("/home/me")),
        )
        .expect("resolves");
        assert_eq!(config.url(None), "http://127.0.0.1:8787/");
        assert_eq!(config.url(Some("abc")), "http://127.0.0.1:8787/?token=abc");
    }
}
