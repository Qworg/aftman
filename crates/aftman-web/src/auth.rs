//! Who may talk to the web surface.
//!
//! Three modes ([08-security.md §4](../../../docs/08-security.md)):
//!
//! - `loopback` — no token; a loopback peer is trusted and a non-loopback peer is refused as
//!   defence in depth. `tailscale serve` proxies from loopback, so this posture survives a
//!   reverse proxy without the port ever being opened on a physical interface.
//! - `token` — `Authorization: Bearer <token>`, compared in constant time. The `?token=`
//!   query parameter is accepted on the SSE endpoint *only* because `EventSource` cannot set
//!   headers; the cookie-login flow is the alternative, and the query-parameter caveat (proxy
//!   and server logs) is stated in the UI's own help text.
//! - `none` — only reachable on loopback, because [`crate::config`] refuses anything else.
//!
//! Two more guards protect a loopback service from a *browser*, which is the part that is
//! easy to forget: the peer address is loopback for a DNS-rebinding request too.
//!
//! - **Host.** A rebinding page makes the browser send `Host: evil.example` to `127.0.0.1`.
//!   Only loopback literals, `localhost`, or a configured `allowed_hosts` entry are accepted.
//! - **Origin.** A state-changing request from another origin is refused even when the peer
//!   is loopback, which blocks the cross-site form post.

use std::net::IpAddr;
use std::sync::Arc;

use axum::http::HeaderMap;

use crate::config::AuthMode;

/// The cookie the browser carries after the `?token=` login flow.
pub const COOKIE_NAME: &str = "aftman_token";

/// Query parameter accepted on the SSE endpoint, because `EventSource` cannot set headers.
pub const QUERY_PARAM: &str = "token";

/// How a request proved who it was, recorded on the answer so the escalation shows which
/// surface answered it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Principal {
    /// A loopback peer, trusted by `auth = "loopback"` or `auth = "none"`.
    Loopback,
    /// A bearer token, or the cookie that carried one.
    Token,
}

impl Principal {
    /// The surface name reported to the runtime. `run.answer` validates it against a closed
    /// set, so this is not free text.
    #[must_use]
    pub const fn surface(self) -> &'static str {
        "web"
    }
}

/// Why a request was refused. The variants exist because the *messages* have to differ: an
/// API client gets a JSON error, a browser gets a page that tells it what to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Denied {
    /// The `Authorization`/cookie was absent or wrong, under `auth = "token"`.
    MissingToken,
    /// Under `auth = "loopback"`, the peer was not loopback.
    NotLoopback,
    /// The `Host` header is not one this surface answers to. Rebinding defence.
    UnknownHost,
    /// A cross-origin state-changing request.
    CrossOrigin,
}

impl Denied {
    #[must_use]
    pub const fn status(self) -> u16 {
        match self {
            Self::MissingToken => 401,
            Self::NotLoopback | Self::CrossOrigin => 403,
            Self::UnknownHost => 421,
        }
    }

    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::MissingToken => {
                "this surface requires a token: send Authorization: Bearer <token>, or open \
                 the URL printed by `aftman serve`, which logs you in with a cookie"
            }
            Self::NotLoopback => "auth = \"loopback\" refuses a request from a non-loopback peer",
            Self::UnknownHost => {
                "this surface does not answer to that Host header; if a reverse proxy is \
                 forwarding here, add its hostname to [web] allowed_hosts"
            }
            Self::CrossOrigin => "a state-changing request from another origin is refused",
        }
    }

    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::MissingToken => "unauthorized",
            Self::NotLoopback => "not_loopback",
            Self::UnknownHost => "unknown_host",
            Self::CrossOrigin => "cross_origin",
        }
    }
}

/// The policy, resolved once at startup.
#[derive(Clone)]
pub struct AuthPolicy {
    mode: AuthMode,
    /// The token, when `auth = "token"`. `serve` generates one when the configuration did
    /// not carry one, so this is always `Some` in token mode.
    token: Option<Arc<str>>,
    /// Host header values accepted besides the loopback literals.
    allowed_hosts: Vec<String>,
    /// Origins allowed to make cross-origin requests.
    allowed_origins: Vec<String>,
}

impl std::fmt::Debug for AuthPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // A token must never be printed, not even by `{:?}`.
        f.debug_struct("AuthPolicy")
            .field("mode", &self.mode)
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .field("allowed_hosts", &self.allowed_hosts)
            .field("allowed_origins", &self.allowed_origins)
            .finish()
    }
}

impl AuthPolicy {
    #[must_use]
    pub fn new(
        mode: AuthMode,
        token: Option<String>,
        allowed_hosts: Vec<String>,
        allowed_origins: Vec<String>,
    ) -> Self {
        Self {
            mode,
            token: token.map(Arc::from),
            allowed_hosts,
            allowed_origins,
        }
    }

    #[must_use]
    pub fn mode(&self) -> AuthMode {
        self.mode
    }

    /// Check a request: the bearer header, or a cookie.
    ///
    /// The `Host` and `Origin` guards run for every method. A `GET` from another origin is
    /// refused too, because it can read a run.
    pub fn check(&self, peer: IpAddr, headers: &HeaderMap) -> Result<Principal, Denied> {
        self.check_host(headers)?;
        self.check_origin(headers)?;
        match self.mode {
            AuthMode::None => Ok(Principal::Loopback),
            AuthMode::Loopback => {
                if peer.is_loopback() {
                    Ok(Principal::Loopback)
                } else {
                    Err(Denied::NotLoopback)
                }
            }
            AuthMode::Token => {
                if self.token_matches(headers) {
                    Ok(Principal::Token)
                } else {
                    Err(Denied::MissingToken)
                }
            }
        }
    }

    /// Check the SSE endpoint, which may carry the token in `?token=` because `EventSource`
    /// cannot set a header.
    pub fn check_sse(
        &self,
        peer: IpAddr,
        headers: &HeaderMap,
        query: Option<&str>,
    ) -> Result<Principal, Denied> {
        self.check_host(headers)?;
        // `EventSource` sends no `Origin`, so this passes for a real SSE client and refuses
        // a cross-origin `fetch` to the same endpoint.
        self.check_origin(headers)?;
        match self.mode {
            AuthMode::None => Ok(Principal::Loopback),
            AuthMode::Loopback => {
                if peer.is_loopback() {
                    Ok(Principal::Loopback)
                } else {
                    Err(Denied::NotLoopback)
                }
            }
            AuthMode::Token => {
                if let Some(presented) = query
                    && let Some(expected) = self.token.as_deref()
                    && constant_time_eq(presented.as_bytes(), expected.as_bytes())
                {
                    return Ok(Principal::Token);
                }
                if self.token_matches(headers) {
                    Ok(Principal::Token)
                } else {
                    Err(Denied::MissingToken)
                }
            }
        }
    }

    /// Whether the `?token=` on a page request should log the browser in.
    ///
    /// Called only for a `GET` page with a token in the query, and only in token mode.
    #[must_use]
    pub fn token_accepts(&self, presented: &str) -> bool {
        match self.token.as_deref() {
            Some(expected) => constant_time_eq(presented.as_bytes(), expected.as_bytes()),
            None => false,
        }
    }

    /// Whether the presented bearer header or cookie matches.
    fn token_matches(&self, headers: &HeaderMap) -> bool {
        let Some(expected) = self.token.as_deref() else {
            // Token mode without a token cannot happen after `serve` resolves one. Refusing
            // is the safe reading of a state that should be unreachable.
            return false;
        };
        if let Some(bearer) = bearer(headers)
            && constant_time_eq(bearer.as_bytes(), expected.as_bytes())
        {
            return true;
        }
        if let Some(cookie) = cookie(headers, COOKIE_NAME)
            && constant_time_eq(cookie.as_bytes(), expected.as_bytes())
        {
            return true;
        }
        false
    }

    fn check_host(&self, headers: &HeaderMap) -> Result<(), Denied> {
        let Some(host) = headers
            .get(axum::http::header::HOST)
            .and_then(|value| value.to_str().ok())
        else {
            // HTTP/1.1 requires Host. A request without one is not a browser, and could only
            // be a client that had to work at it; refuse rather than guess.
            return Err(Denied::UnknownHost);
        };
        if host_is_local(host) || self.allowed_hosts.iter().any(|allowed| allowed == host) {
            return Ok(());
        }
        Err(Denied::UnknownHost)
    }

    fn check_origin(&self, headers: &HeaderMap) -> Result<(), Denied> {
        let Some(origin) = headers
            .get(axum::http::header::ORIGIN)
            .and_then(|value| value.to_str().ok())
        else {
            // A non-browser client sends no Origin. It is already constrained by the mode
            // (a token, or a loopback peer), and requiring one would break `curl`.
            return Ok(());
        };
        if self.allowed_origins.iter().any(|allowed| allowed == origin) {
            return Ok(());
        }
        // Same-origin: the Origin's authority must equal the Host we are answering on.
        let Some(host) = headers
            .get(axum::http::header::HOST)
            .and_then(|value| value.to_str().ok())
        else {
            return Err(Denied::UnknownHost);
        };
        if origin_authority(origin).is_some_and(|authority| authority == host) {
            return Ok(());
        }
        Err(Denied::CrossOrigin)
    }
}

/// Compare two byte strings without an early return, so the time taken does not depend on
/// where the first difference is.
///
/// The *length* is not secret — a token's length is visible in the header of every request
/// that carries it — so returning early on a length mismatch leaks nothing further.
#[must_use]
pub fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut difference = 0u8;
    for (a, b) in left.iter().zip(right) {
        difference |= a ^ b;
    }
    difference == 0
}

/// The `Bearer`/`Basic` value from `Authorization`, if it is a bearer token.
fn bearer(headers: &HeaderMap) -> Option<String> {
    let value = headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    let (scheme, token) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = token.trim();
    (!token.is_empty()).then(|| token.to_owned())
}

/// One cookie's value by name. A hand-rolled parser because the format is four lines and the
/// alternative is another dependency in the network path.
fn cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    let header = headers.get(axum::http::header::COOKIE)?.to_str().ok()?;
    for pair in header.split(';') {
        let pair = pair.trim();
        if let Some((key, value)) = pair.split_once('=')
            && key.trim() == name
        {
            return Some(value.trim().to_owned());
        }
    }
    None
}

/// Whether a `Host` header names this machine. Port is ignored: a proxy may rewrite it, and
/// the port is not what rebinding attacks.
#[must_use]
pub fn host_is_local(host: &str) -> bool {
    let host = host.trim();
    let name = if let Some(rest) = host.strip_prefix('[') {
        // `[::1]:8787`
        match rest.split_once(']') {
            Some((inside, _)) => inside,
            None => rest,
        }
    } else if host.matches(':').count() > 1 {
        // A bare IPv6 literal has more than one colon and so cannot be `host:port`; treating
        // the last group as a port is how `::1` gets mistaken for the host `:`.
        host
    } else {
        match host.rsplit_once(':') {
            Some((name, port)) if !port.is_empty() && port.chars().all(|c| c.is_ascii_digit()) => {
                name
            }
            _ => host,
        }
    };
    matches!(
        name.to_ascii_lowercase().as_str(),
        "localhost" | "127.0.0.1" | "::1"
    )
}

/// The `host:port` (or bare host) of an `Origin` value.
fn origin_authority(origin: &str) -> Option<&str> {
    let rest = origin
        .strip_prefix("http://")
        .or_else(|| origin.strip_prefix("https://"))?;
    // An Origin never carries a path, but be tolerant of a trailing slash.
    Some(rest.trim_end_matches('/'))
}

/// The `Set-Cookie` value for a logged-in browser.
///
/// `HttpOnly` so script cannot read it and `SameSite=Strict` so another site cannot make the
/// browser send it. `Secure` is set only when the request arrived over a proxy that says it
/// terminated TLS: on plain loopback HTTP a `Secure` cookie is dropped by the browser, which
/// would silently break the login flow on the default posture.
#[must_use]
pub fn login_cookie(token: &str, secure: bool) -> String {
    let mut cookie = format!("{COOKIE_NAME}={token}; Path=/; HttpOnly; SameSite=Strict");
    if secure {
        cookie.push_str("; Secure");
    }
    cookie
}

/// Whether a proxy in front of us terminated TLS.
#[must_use]
pub fn request_is_https(headers: &HeaderMap) -> bool {
    headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("https"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(
                axum::http::HeaderName::from_bytes(name.as_bytes()).expect("name"),
                HeaderValue::from_str(value).expect("value"),
            );
        }
        map
    }

    fn loopback() -> IpAddr {
        "127.0.0.1".parse().expect("parses")
    }

    fn policy(mode: AuthMode, token: Option<&str>) -> AuthPolicy {
        AuthPolicy::new(mode, token.map(ToOwned::to_owned), Vec::new(), Vec::new())
    }

    #[test]
    fn a_loopback_peer_is_trusted_under_loopback_auth() {
        let policy = policy(AuthMode::Loopback, None);
        let result = policy.check(loopback(), &headers(&[("host", "127.0.0.1:8787")]));
        assert_eq!(result, Ok(Principal::Loopback));
    }

    #[test]
    fn a_non_loopback_peer_is_refused_even_under_loopback_auth() {
        // Defence in depth: `tailscale serve` proxies from loopback, so this only fires for
        // something that reached the socket from elsewhere.
        let policy = policy(AuthMode::Loopback, None);
        let remote: IpAddr = "192.168.1.5".parse().expect("parses");
        let result = policy.check(remote, &headers(&[("host", "127.0.0.1:8787")]));
        assert_eq!(result, Err(Denied::NotLoopback));
    }

    #[test]
    fn a_token_is_required_and_must_be_exactly_right() {
        let policy = policy(AuthMode::Token, Some("correct-horse"));
        let host = [("host", "127.0.0.1:8787")];
        assert_eq!(
            policy.check(loopback(), &headers(&host)),
            Err(Denied::MissingToken)
        );
        let wrong = [
            ("host", "127.0.0.1:8787"),
            ("authorization", "Bearer correct-hors"),
        ];
        assert_eq!(
            policy.check(loopback(), &headers(&wrong)),
            Err(Denied::MissingToken)
        );
        let right = [
            ("host", "127.0.0.1:8787"),
            ("authorization", "Bearer correct-horse"),
        ];
        assert_eq!(
            policy.check(loopback(), &headers(&right)),
            Ok(Principal::Token)
        );
    }

    #[test]
    fn the_cookie_logs_a_browser_in() {
        let policy = policy(AuthMode::Token, Some("correct-horse"));
        let request = [
            ("host", "127.0.0.1:8787"),
            ("cookie", "other=1; aftman_token=correct-horse; another=2"),
        ];
        assert_eq!(
            policy.check(loopback(), &headers(&request)),
            Ok(Principal::Token)
        );
    }

    #[test]
    fn a_wrong_cookie_does_not_log_a_browser_in() {
        let policy = policy(AuthMode::Token, Some("correct-horse"));
        let request = [("host", "127.0.0.1:8787"), ("cookie", "aftman_token=wrong")];
        assert_eq!(
            policy.check(loopback(), &headers(&request)),
            Err(Denied::MissingToken)
        );
    }

    #[test]
    fn a_dns_rebinding_host_is_refused() {
        // The peer is loopback in a rebinding attack too, which is why the Host header is
        // the guard that catches it.
        let policy = policy(AuthMode::Loopback, None);
        let result = policy.check(loopback(), &headers(&[("host", "evil.example:8787")]));
        assert_eq!(result, Err(Denied::UnknownHost));
    }

    #[test]
    fn a_host_the_operator_allowed_is_accepted_for_a_reverse_proxy() {
        let policy = AuthPolicy::new(
            AuthMode::Loopback,
            None,
            vec!["box.tailnet.ts.net".to_owned()],
            Vec::new(),
        );
        let result = policy.check(loopback(), &headers(&[("host", "box.tailnet.ts.net")]));
        assert_eq!(result, Ok(Principal::Loopback));
    }

    #[test]
    fn a_cross_origin_state_changing_request_is_refused() {
        let policy = policy(AuthMode::Loopback, None);
        let request = [
            ("host", "127.0.0.1:8787"),
            ("origin", "http://evil.example"),
        ];
        assert_eq!(
            policy.check(loopback(), &headers(&request)),
            Err(Denied::CrossOrigin)
        );
        // ...and the same-origin one is not.
        let same = [
            ("host", "127.0.0.1:8787"),
            ("origin", "http://127.0.0.1:8787"),
        ];
        assert_eq!(
            policy.check(loopback(), &headers(&same)),
            Ok(Principal::Loopback)
        );
    }

    #[test]
    fn a_curl_request_without_an_origin_is_allowed() {
        // Requiring an Origin would break every non-browser client, which is exactly what
        // the JSON API is for.
        let policy = policy(AuthMode::Loopback, None);
        assert_eq!(
            policy.check(loopback(), &headers(&[("host", "127.0.0.1:8787")])),
            Ok(Principal::Loopback)
        );
    }

    #[test]
    fn the_sse_endpoint_accepts_a_query_token_because_eventsource_cannot_set_headers() {
        let policy = policy(AuthMode::Token, Some("correct-horse"));
        let request = [("host", "127.0.0.1:8787")];
        assert_eq!(
            policy.check_sse(loopback(), &headers(&request), Some("correct-horse")),
            Ok(Principal::Token)
        );
        assert_eq!(
            policy.check_sse(loopback(), &headers(&request), Some("wrong")),
            Err(Denied::MissingToken)
        );
        // No query token, no header: refused.
        assert_eq!(
            policy.check_sse(loopback(), &headers(&request), None),
            Err(Denied::MissingToken)
        );
    }

    #[test]
    fn the_comparison_is_correct_at_every_position() {
        // Not a timing test — a timing test would be flaky. It pins that no early return
        // skips a byte, by mutating each position in turn.
        let expected = b"0123456789abcdef";
        assert!(constant_time_eq(expected, expected));
        for index in 0..expected.len() {
            let mut candidate = *expected;
            candidate[index] ^= 0x01;
            assert!(
                !constant_time_eq(expected, &candidate),
                "a difference at {index} must be found"
            );
        }
        assert!(!constant_time_eq(expected, &expected[..expected.len() - 1]));
        assert!(!constant_time_eq(b"", expected));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn host_parsing_handles_ports_and_ipv6() {
        for local in [
            "127.0.0.1",
            "127.0.0.1:8787",
            "localhost",
            "localhost:8787",
            "LocalHost:8787",
            "[::1]:8787",
            "[::1]",
            "::1",
        ] {
            assert!(host_is_local(local), "{local} must be local");
        }
        for remote in [
            "0.0.0.0:8787",
            "evil.example",
            "192.168.1.5:8787",
            "[::]:8787",
        ] {
            assert!(!host_is_local(remote), "{remote} must not be local");
        }
    }

    #[test]
    fn a_missing_host_header_is_refused() {
        let policy = policy(AuthMode::Loopback, None);
        assert_eq!(
            policy.check(loopback(), &HeaderMap::new()),
            Err(Denied::UnknownHost)
        );
    }

    #[test]
    fn the_login_cookie_is_httponly_and_same_site() {
        let cookie = login_cookie("abc", false);
        assert!(cookie.contains("HttpOnly"), "{cookie}");
        assert!(cookie.contains("SameSite=Strict"), "{cookie}");
        assert!(
            !cookie.contains("Secure"),
            "loopback HTTP must not set Secure, or the browser drops it"
        );
        assert!(
            login_cookie("abc", true).contains("Secure"),
            "a TLS-terminating proxy gets a Secure cookie"
        );
    }

    #[test]
    fn a_token_is_never_rendered_by_the_policy_either() {
        let policy = policy(AuthMode::Token, Some("s3cret"));
        let rendered = format!("{policy:?}");
        assert!(!rendered.contains("s3cret"), "{rendered}");
        assert!(rendered.contains("<redacted>"), "{rendered}");
    }
}
