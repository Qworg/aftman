//! Routing, the auth guard, and the embedded assets.
//!
//! The router is the only place that knows the URL shapes, so a change to a path is one
//! change. It is also the only place that decides what a refusal *looks* like: a browser
//! gets a page that says what to do, an API client gets a JSON body.

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::extract::{ConnectInfo, Path};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};

use aftman_daemon::manager::RunManager;
use rust_embed::RustEmbed;

use crate::Web;
use crate::auth::{Denied, Principal};
use crate::pages;
use crate::repo::{self, RepoError};

/// State every handler shares.
#[derive(Clone)]
pub struct AppState {
    pub web: Arc<Web>,
    pub manager: Arc<RunManager>,
}

impl AppState {
    /// Validate a repository path against the configured roots.
    ///
    /// # Errors
    /// Whatever [`repo::resolve`] refuses, which the caller maps to a 400.
    pub fn resolve_repo(&self, path: &std::path::Path) -> Result<PathBuf, RepoError> {
        let config = self.web.config();
        repo::resolve(
            path,
            config.allowed_repo_roots(),
            config.allow_non_git_repos(),
        )
    }
}

/// The embedded assets: compiled in, so there is no Node step at runtime and no separate
/// deploy.
#[derive(RustEmbed)]
#[folder = "assets/"]
pub struct Assets;

pub fn router(state: AppState) -> Router {
    Router::new()
        // ---- pages ----------------------------------------------------------
        .route("/", get(pages::index))
        .route("/new", get(pages::new_form).post(pages::new_submit))
        .route("/runs/{id}", get(pages::run_detail))
        .route("/runs/{id}/decisions", get(pages::run_decisions))
        .route("/runs/{id}/evidence", get(pages::run_evidence))
        .route("/runs/{id}/answer", post(pages::answer))
        .route("/runs/{id}/steer", post(pages::steer))
        .route("/runs/{id}/cancel", post(pages::cancel))
        .route("/usage", get(pages::usage))
        .route("/config", get(pages::config))
        .route("/assets/{*path}", get(asset))
        // ---- JSON API -------------------------------------------------------
        .route("/v1/status", get(crate::api::status))
        .route(
            "/v1/runs",
            get(crate::api::list_runs).post(crate::api::start_run),
        )
        .route("/v1/runs/{id}", get(crate::api::get_run))
        .route("/v1/runs/{id}/events", get(crate::api::events))
        .route("/v1/runs/{id}/steer", post(crate::api::steer))
        .route("/v1/runs/{id}/cancel", post(crate::api::cancel))
        .route("/v1/runs/{id}/answer", post(crate::api::answer))
        .route("/v1/responsibilities", get(crate::api::responsibilities))
        .route("/v1/usage", get(crate::api::usage))
        .route("/v1/web", get(crate::api::web_config))
        .with_state(state)
}

/// The peer address of a request.
///
/// `ConnectInfo` is a required extractor rather than an `Option`, so a router served without
/// connection info answers 500 instead of silently treating every caller as local. That is
/// the safe direction: forgetting `into_make_service_with_connect_info` must not open the
/// surface.
#[must_use]
pub fn peer_ip(peer: &ConnectInfo<SocketAddr>) -> IpAddr {
    peer.0.ip()
}

/// The auth check for a request that carries no `?token=`.
pub fn guard(
    state: &AppState,
    headers: &HeaderMap,
    peer: &ConnectInfo<SocketAddr>,
) -> Result<Principal, Denied> {
    state.web.auth().check(peer_ip(peer), headers)
}

/// What a refusal looks like. A browser gets a page; anything else gets JSON.
///
/// `WWW-Authenticate` is set on a 401 so a `curl -v` shows the same instruction the page does.
pub fn refusal(error: Denied, state: &AppState, headers: &HeaderMap) -> Response {
    let status = StatusCode::from_u16(error.status()).unwrap_or(StatusCode::FORBIDDEN);
    let mut response = if wants_html(headers) {
        let page = pages::denied(error, state.web.config().auth());
        (status, axum::response::Html(page)).into_response()
    } else {
        let body = serde_json::json!({
            "error": { "code": error.code(), "message": error.message() }
        });
        (status, axum::Json(body)).into_response()
    };
    if error == Denied::MissingToken {
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_static("Bearer realm=\"aftman\""),
        );
    }
    response
}

/// Whether the client would rather have HTML. `EventSource` sends `text/event-stream`, so an
/// SSE failure is reported as JSON, which the script surfaces.
fn wants_html(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|accept| accept.contains("text/html"))
}

/// Serve one embedded asset.
async fn asset(Path(path): Path<String>) -> Response {
    let Some(file) = Assets::get(&path) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mime = match path.rsplit_once('.').map(|(_, extension)| extension) {
        Some("css") => "text/css; charset=utf-8",
        Some("js") => "text/javascript; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("woff2") => "font/woff2",
        _ => "application/octet-stream",
    };
    let mut response = file.data.into_response();
    if let Ok(value) = HeaderValue::from_str(mime) {
        response.headers_mut().insert(header::CONTENT_TYPE, value);
    }
    // The body is compiled in, so it changes only when the binary does.
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=300"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn the_unspecified_address_is_not_loopback() {
        // If a peer address ever defaults instead of being required, `0.0.0.0` must not
        // satisfy a loopback check.
        assert!(!IpAddr::V4(Ipv4Addr::UNSPECIFIED).is_loopback());
    }

    #[test]
    fn the_assets_are_embedded() {
        // A missing asset is a 404 at runtime, which is easy to miss until someone opens
        // the page. This makes it a test failure.
        assert!(Assets::get("app.css").is_some(), "app.css must be embedded");
        assert!(Assets::get("app.js").is_some(), "app.js must be embedded");
        assert!(Assets::get("nope.css").is_none());
    }
}
