//! The HTTP boundary.
//!
//! The client talks to a [`Transport`], not to `reqwest`. Two reasons, and both
//! matter:
//!
//! 1. **`AGENTS.md` hard rule 4: tests are offline.** A fixture transport that
//!    answers from memory keeps the retry, timeout, and error-mapping tests entirely
//!    free of sockets, rather than binding a loopback port and calling it offline.
//! 2. **Timing becomes assertable.** Because a transport is a plain async function,
//!    a test can make it report a `429` and then succeed, and the retry policy can be
//!    driven with a deterministic seed.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use aftman_core::error::ModelError;
use async_trait::async_trait;

/// One request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequest {
    pub url: String,
    /// Ordered so a test can assert exactly what was sent.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub timeout: Duration,
}

impl HttpRequest {
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

/// One response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    #[must_use]
    pub fn json(status: u16, body: serde_json::Value) -> Self {
        Self {
            status,
            headers: vec![("content-type".to_owned(), "application/json".to_owned())],
            body: serde_json::to_vec(&body).unwrap_or_default(),
        }
    }

    #[must_use]
    pub fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_owned(), value.to_owned()));
        self
    }
}

/// Sends one request.
#[async_trait]
pub trait Transport: Send + Sync {
    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, ModelError>;
}

/// The most of a provider response this client will read.
///
/// A supervision answer is a few kilobytes. Anything near this is a proxy error page or a
/// provider bug, and allocating for it unbounded is a denial of service against the daemon.
pub const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

/// The real transport.
pub struct ReqwestTransport {
    client: reqwest::Client,
}

impl ReqwestTransport {
    /// Build a client with no implicit retries of its own.
    ///
    /// `reqwest`'s retry behaviour would double up with [`crate::retry::RetryPolicy`]
    /// and make the `X-TypeSafe-Retry-Count` header a lie, so it is not enabled.
    #[must_use]
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .user_agent(concat!("aftman/", env!("CARGO_PKG_VERSION")))
                // `docs/03-jev.md` §10.2: no proxying. `reqwest` enables the system proxy by
                // default and its matcher has no loopback bypass, so with `HTTP_PROXY` set a
                // legitimate `http://127.0.0.1` base would send the key in cleartext *to the
                // proxy*. The key goes to the configured endpoint, nowhere else.
                .no_proxy()
                // An API endpoint that redirects is a misconfiguration, and a 307/308 would
                // re-POST the whole state — including repository contents — to whatever host
                // the redirect names, with the `Authorization` header attached.
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap_or_default(),
        }
    }

    #[must_use]
    pub fn from_client(client: reqwest::Client) -> Self {
        Self { client }
    }
}

impl Default for ReqwestTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Transport for ReqwestTransport {
    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, ModelError> {
        let method = reqwest::Method::POST;
        let mut builder = self
            .client
            .request(method, &request.url)
            .timeout(request.timeout)
            .body(request.body.clone());
        for (name, value) in &request.headers {
            builder = builder.header(name.as_str(), value.as_str());
        }
        let response = builder.send().await.map_err(|error| {
            if error.is_timeout() {
                ModelError::Timeout(request.timeout)
            } else {
                ModelError::Transport(error.to_string())
            }
        })?;
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_owned(),
                    value.to_str().unwrap_or_default().to_owned(),
                )
            })
            .collect();
        // Bounded: a provider (or a proxy that answered instead of it) must not be able to
        // make the supervisor allocate without limit. A supervision response is kilobytes.
        let mut response = response;
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| ModelError::Transport(error.to_string()))?
        {
            if body.len() + chunk.len() > MAX_RESPONSE_BYTES {
                return Err(ModelError::Transport(format!(
                    "response body exceeded {MAX_RESPONSE_BYTES} bytes; refusing to read it"
                )));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }
}

/// What a fixture transport should do for one call.
#[derive(Debug, Clone)]
pub enum FixtureResponse {
    /// Reply with this response.
    Reply(HttpResponse),
    /// Fail before a response, as a connection error or a timeout would.
    Fail(ModelError),
}

/// A transport that answers from a script, and records what it was asked.
///
/// The recording is the point as much as the answering: the retry tests assert the
/// `X-TypeSafe-Retry-Count` header, and the credential test asserts that a base URL
/// change is never silently adopted.
#[derive(Debug, Default)]
pub struct FixtureTransport {
    script: Mutex<std::collections::VecDeque<FixtureResponse>>,
    requests: Mutex<Vec<HttpRequest>>,
    /// Used once the script runs out, so a test only has to describe the interesting
    /// prefix of a conversation.
    fallback: Mutex<Option<FixtureResponse>>,
}

impl FixtureTransport {
    #[must_use]
    pub fn new(script: Vec<FixtureResponse>) -> Self {
        Self {
            script: Mutex::new(script.into()),
            requests: Mutex::new(Vec::new()),
            fallback: Mutex::new(None),
        }
    }

    /// A transport that always replies with the same thing.
    #[must_use]
    pub fn always(response: HttpResponse) -> Self {
        Self::new(Vec::new()).with_fallback(FixtureResponse::Reply(response))
    }

    #[must_use]
    pub fn with_fallback(self, response: FixtureResponse) -> Self {
        *self
            .fallback
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(response);
        self
    }

    /// Every request seen, in order.
    #[must_use]
    pub fn requests(&self) -> Vec<HttpRequest> {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    #[must_use]
    pub fn call_count(&self) -> usize {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// The parsed request body of call `index`.
    #[must_use]
    pub fn body(&self, index: usize) -> Option<serde_json::Value> {
        let requests = self.requests();
        let request = requests.get(index)?;
        serde_json::from_slice(&request.body).ok()
    }
}

#[async_trait]
impl Transport for FixtureTransport {
    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, ModelError> {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(request);
        let next = self
            .script
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front()
            .or_else(|| {
                self.fallback
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
            });
        match next {
            Some(FixtureResponse::Reply(response)) => Ok(response),
            Some(FixtureResponse::Fail(error)) => Err(error),
            None => Err(ModelError::Transport("fixture script exhausted".to_owned())),
        }
    }
}

/// A transport shared behind an `Arc`, for the client's field type.
pub type SharedTransport = Arc<dyn Transport>;

#[cfg(test)]
mod tests {
    use super::{FixtureResponse, FixtureTransport, HttpRequest, HttpResponse, Transport};
    use aftman_core::error::ModelError;
    use std::time::Duration;

    fn request() -> HttpRequest {
        HttpRequest {
            url: "https://api.example/v1/systemone".to_owned(),
            headers: vec![("X-Test".to_owned(), "yes".to_owned())],
            body: br#"{"state":{}}"#.to_vec(),
            timeout: Duration::from_secs(5),
        }
    }

    #[tokio::test]
    async fn a_fixture_answers_in_script_order() -> Result<(), Box<dyn std::error::Error>> {
        let transport = FixtureTransport::new(vec![
            FixtureResponse::Reply(HttpResponse::json(429, serde_json::json!({}))),
            FixtureResponse::Reply(HttpResponse::json(200, serde_json::json!({"ok": true}))),
        ]);
        let first = transport.send(request()).await?;
        assert_eq!(first.status, 429);
        let second = transport.send(request()).await?;
        assert_eq!(second.status, 200);
        assert_eq!(transport.call_count(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn a_fixture_can_fail_like_a_connection_error() -> Result<(), Box<dyn std::error::Error>>
    {
        let transport = FixtureTransport::new(vec![FixtureResponse::Fail(ModelError::Transport(
            "connection refused".to_owned(),
        ))]);
        let result = transport.send(request()).await;
        assert!(matches!(result, Err(ModelError::Transport(_))));
        Ok(())
    }

    #[tokio::test]
    async fn a_fallback_serves_every_call_after_the_script()
    -> Result<(), Box<dyn std::error::Error>> {
        let transport =
            FixtureTransport::always(HttpResponse::json(200, serde_json::json!({"n": 1})));
        for _ in 0..3 {
            assert_eq!(transport.send(request()).await?.status, 200);
        }
        assert_eq!(transport.call_count(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn an_exhausted_script_is_an_error_not_a_hang() {
        let transport = FixtureTransport::new(Vec::new());
        let result = transport.send(request()).await;
        assert!(matches!(result, Err(ModelError::Transport(_))));
    }

    #[tokio::test]
    async fn requests_are_recorded_for_assertion() -> Result<(), Box<dyn std::error::Error>> {
        let transport = FixtureTransport::always(HttpResponse::json(200, serde_json::json!({})));
        transport.send(request()).await?;
        let requests = transport.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].header("x-test"),
            Some("yes"),
            "header lookup is case-insensitive"
        );
        assert!(transport.body(0).is_some());
        Ok(())
    }

    #[test]
    fn headers_are_added_not_replaced() {
        let response =
            HttpResponse::json(200, serde_json::json!({})).with_header("retry-after-ms", "100");
        assert!(
            response
                .headers
                .iter()
                .any(|(k, v)| k == "retry-after-ms" && v == "100")
        );
    }
}
