//! The web surface: HTTP and SSE in front of the *same* protocol the Unix socket speaks.
//!
//! This is a second *transport*, not a second implementation. The handlers call the same
//! [`aftman_daemon::RunManager`] methods the socket dispatch calls, with the same parameter
//! types, so a rule enforced for one surface is enforced for both. The SSE endpoint
//! subscribes through the same [`aftman_daemon::EventHub`], with sequence numbers as event
//! ids, so resumption is exact rather than approximate: a browser that reconnects mid-run
//! asks for the first event it has not seen and receives it.
//!
//! Three behaviours are worth knowing before reading the code:
//!
//! - **The surface refuses to start rather than degrade.** A non-loopback bind without a
//!   token is a [`WebConfigError`], not a warning ([`config`]).
//! - **Every untrusted string goes through one escaper** ([`html::escape`]); there is no
//!   template engine, so there is no second path and no `|safe` to omit.
//! - **A run may only read a repository an operator allowed** ([`repo::resolve`]).

pub mod api;
pub mod auth;
pub mod config;
pub mod html;
pub mod pages;
pub mod repo;
pub mod routes;

use std::future::Future;
use std::sync::Arc;

use aftman_daemon::manager::RunManager;

pub use auth::{AuthPolicy, Denied, Principal};
pub use config::{AuthMode, WebConfig, WebConfigError};

/// A prepared web surface: a validated configuration and the resolved token.
pub struct Web {
    config: WebConfig,
    auth: AuthPolicy,
    token: Option<String>,
    token_generated: bool,
}

impl std::fmt::Debug for Web {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Web")
            .field("config", &self.config)
            .field("auth", &self.auth)
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .field("token_generated", &self.token_generated)
            .finish()
    }
}

/// Why the web surface could not be prepared. Only configuration: binding is the caller's
/// job, so a caller can prepare, then bind, then report both failures distinctly.
#[derive(Debug, thiserror::Error)]
pub enum WebError {
    #[error("{0}")]
    Config(#[from] WebConfigError),
}

impl Web {
    /// Validate the configuration and resolve the token.
    ///
    /// Under `auth = "token"` with no configured token, one is generated **here** and
    /// [`Self::url`] carries it, so `serve` prints a URL that logs a browser in. That is
    /// the "generated token printed once by `serve`" the design describes.
    pub fn prepare(config: WebConfig) -> Result<Self, WebError> {
        let (token, token_generated) = match config.auth() {
            AuthMode::Token => match config.configured_token() {
                Some(token) => (Some(token.to_owned()), false),
                None => (Some(config::generate_token()?), true),
            },
            AuthMode::None | AuthMode::Loopback => (None, false),
        };
        let auth = AuthPolicy::new(
            config.auth(),
            token.clone(),
            config.bind(),
            config.allowed_hosts().to_vec(),
            config.allowed_origins().to_vec(),
        );
        Ok(Self {
            config,
            auth,
            token,
            token_generated,
        })
    }

    #[must_use]
    pub fn config(&self) -> &WebConfig {
        &self.config
    }

    #[must_use]
    pub fn auth(&self) -> &AuthPolicy {
        &self.auth
    }

    /// Whether the token was generated rather than configured, so `serve` knows to print it
    /// once instead of on every start.
    #[must_use]
    pub fn token_was_generated(&self) -> bool {
        self.token_generated
    }

    /// A URL a person can open. Carries the token only when one is in use.
    #[must_use]
    pub fn url(&self) -> String {
        match self.config.auth() {
            AuthMode::Token => self.config.url(self.token.as_deref()),
            AuthMode::None | AuthMode::Loopback => self.config.url(None),
        }
    }

    /// The router, bound to a run manager. `Arc<Web>` so the state is shared by every
    /// request without cloning the configuration.
    pub fn router(self: &Arc<Self>, manager: Arc<RunManager>) -> axum::Router {
        routes::router(routes::AppState {
            web: Arc::clone(self),
            manager,
        })
    }
}

/// Serve until `shutdown` resolves.
///
/// # Errors
/// Whatever the listener reports. A bind failure is deliberately the caller's error type so
/// the daemon can report "port in use" distinctly from "configuration refused".
pub async fn serve(
    web: Arc<Web>,
    manager: Arc<RunManager>,
    listener: tokio::net::TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let router = web.router(manager);
    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown)
    .await
}
