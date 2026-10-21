//! The server-rendered pages.
//!
//! Design rules from [07-surfaces.md §6](../../../docs/07-surfaces.md) that shape the code:
//!
//! - The run detail page is a **decision log with a diff**, not a chat UI. Aftman has no chat.
//! - An escalation is a first-class, answerable form, and it appears even when the run is
//!   otherwise quiet.
//! - [`FactoryStatus::NeedsHuman`] is visually distinct from [`FactoryStatus::Failed`]:
//!   conflating "it broke" with "a person must decide" teaches an operator to ignore both.
//!
//! Every untrusted string — job text, worker output, repository paths, daemon errors — goes
//! through [`crate::html::escape`]. There is no template engine, so there is no second path.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::extract::{ConnectInfo, Form, Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{Html, IntoResponse, Redirect, Response};

use aftman_core::assessment::{Band, HumanDecision, ResolvedCheck};
use aftman_core::event::{EventEnvelope, EventKind};
use aftman_core::ids::{EscalationId, RunId};
use aftman_core::job::FactoryStatus;
use aftman_core::run::{Escalation, RunState};
use aftman_daemon::obs::ObsCaps;
use aftman_daemon::protocol::{RunAnswerParams, RunEventsParams, RunIdParams, RunStartParams};
use serde::Deserialize;

use crate::auth::Denied;
use crate::config::AuthMode;
use crate::html::{self, attr, escape};
use crate::routes::{AppState, guard, refusal};

/// How many events the detail page renders. The log itself is unbounded and the page is not;
/// "see all of it" is what `/runs/{id}/decisions` and the CLI are for.
const TIMELINE_EVENTS: usize = 200;

/// A page-sized diff. The model's cap is 20 000 characters because it has to fit a request
/// budget; a person can scroll, so the page may show more — but not without a bound.
const PAGE_DIFF_CHARS: usize = 60_000;

/// How long a page waits for the in-process git walk before giving up on the diff. The walk
/// is interruptible, so this is a real bound rather than a hope.
const GIT_SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(5);

fn html_response(body: String) -> Response {
    ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], body).into_response()
}

/// A logged-out browser: the token goes into a cookie and the URL loses the query.
fn login_redirect(web: &crate::Web, token: &str, headers: &HeaderMap) -> Response {
    let secure = crate::auth::request_is_https(headers);
    let cookie = crate::auth::login_cookie(token, secure);
    let mut response = Redirect::to("/").into_response();
    if let Ok(value) = HeaderValue::from_str(&cookie) {
        response.headers_mut().insert(header::SET_COOKIE, value);
    }
    let _ = web;
    response
}

/// The page a refused browser gets. Says what to do rather than only that it was refused.
#[must_use]
pub fn denied(error: Denied, mode: AuthMode) -> String {
    let mut body = String::new();
    body.push_str("<h1>Not allowed</h1>\n");
    body.push_str(&html::error_banner(error.message()));
    if error == Denied::MissingToken {
        body.push_str(
            "<form method=\"get\" action=\"/\">\
             <label>Token <input type=\"password\" name=\"token\" autocomplete=\"off\"></label>\
             <button type=\"submit\">Log in</button></form>\n",
        );
        body.push_str(
            "<p class=\"note\">The token is printed once by <code>aftman serve</code>, \
             or supplied by <code>AFTMAN_WEB_TOKEN</code>.</p>\n",
        );
    }
    if mode == AuthMode::Loopback {
        body.push_str(
            "<p class=\"note\">This daemon serves <code>auth = \"loopback\"</code>: only a \
             loopback peer is accepted. A reverse proxy must forward from loopback, and its \
             hostname must be listed in <code>[web] allowed_hosts</code>.</p>\n",
        );
    }
    html::page("Not allowed", &body, mode == AuthMode::Token)
}

// ---- pages ------------------------------------------------------------------

/// `GET /` — the run list, and the token login.
pub async fn index(
    State(state): State<AppState>,
    peer: ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    // The cookie-login flow: `?token=` logs the browser in, then redirects to a URL with no
    // token in it, so the token does not sit in the address bar or in history.
    if let Some(token) = query.get(crate::auth::QUERY_PARAM)
        && state.web.auth().token_accepts(token)
    {
        return login_redirect(&state.web, token, &headers);
    }
    if let Err(error) = guard(&state, &headers, &peer) {
        return refusal(error, &state, &headers);
    }
    let status = query
        .get("status")
        .and_then(|name| FactoryStatus::parse(name));
    let params = aftman_daemon::protocol::RunListParams {
        status: status.map(|s| vec![s]),
        limit: Some(100),
        cursor: None,
    };
    let list = match state.manager.run_list(params).await {
        Ok(list) => list,
        Err(error) => {
            return html_response(html::page(
                "runs",
                &format!("<h1>Runs</h1>{}", html::error_banner(&error.to_string())),
                state.web.config().auth() == AuthMode::Token,
            ));
        }
    };

    let mut body = String::new();
    body.push_str("<h1>Runs</h1>\n");
    body.push_str("<p class=\"filters\">");
    body.push_str("<a href=\"/\">all</a>");
    for status in [
        FactoryStatus::Running,
        FactoryStatus::NeedsHuman,
        FactoryStatus::Finished,
        FactoryStatus::Failed,
    ] {
        body.push_str(&format!(
            " <a href=\"/?status={}\">{}</a>",
            attr(status.as_str()),
            escape(name_of(status))
        ));
    }
    body.push_str("</p>\n");

    if list.runs.is_empty() {
        body.push_str("<p class=\"empty\">No runs yet. <a href=\"/new\">Start one.</a></p>\n");
    } else {
        body.push_str("<table class=\"runs\">\n<thead><tr>");
        body.push_str(
            "<th>run</th><th>status</th><th>repository</th><th>job</th>\
             <th>created</th><th>directive</th></tr></thead>\n<tbody>\n",
        );
        for summary in &list.runs {
            let id = summary.id.to_string();
            body.push_str(&format!(
                "<tr>\
                 <td><a class=\"mono\" href=\"/runs/{id}\">{short}</a></td>\
                 <td><span class=\"{class}\">{label}</span></td>\
                 <td class=\"mono\">{repo}</td>\
                 <td>{job}</td>\
                 <td class=\"mono\">{created}</td>\
                 <td class=\"mono\">{directive}</td>\
                 </tr>\n",
                short = escape(&short_id(&id)),
                class = html::status_class(summary.status),
                label = escape(html::status_label(summary.status)),
                repo = escape(&summary.repo.display().to_string()),
                // The job excerpt is untrusted text: escaped like everything else.
                job = escape(&excerpt(&summary.job_excerpt, 140)),
                created = escape(&html::timestamp(summary.created_at)),
                directive = escape(summary.last_directive.as_deref().unwrap_or("—")),
            ));
        }
        body.push_str("</tbody></table>\n");
    }
    html_response(html::page(
        "runs",
        &body,
        state.web.config().auth() == AuthMode::Token,
    ))
}

/// `GET /runs/{id}` — the decision log with a diff.
pub async fn run_detail(
    State(state): State<AppState>,
    peer: ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err(error) = guard(&state, &headers, &peer) {
        return refusal(error, &state, &headers);
    }
    let Some(run_id) = parse_id(&id) else {
        return not_found(&state, &id);
    };
    let run = match state.manager.run_get(RunIdParams { run_id }).await {
        Ok(run) => run,
        Err(error) => return not_found_error(&state, &error.to_string()),
    };
    let events = load_events(&state, run_id).await;

    let mut body = String::new();
    body.push_str(&run_header(&run));

    // The escalation form comes before the timeline: it is the thing that needs a person.
    body.push_str(&escalation_panel(&run));

    // The assessment: every value with its threshold and margin.
    body.push_str(&check_table(&run));

    // Controls.
    body.push_str(&controls(run_id, &run));

    // The diff, from the same in-process walk the supervisor runs.
    body.push_str(&diff_panel(&state, &run).await);

    body.push_str(&timeline(&events, &run, true));

    html_response(html::page(
        &format!("run {}", short_id(&id)),
        &body,
        state.web.config().auth() == AuthMode::Token,
    ))
}

/// `GET /runs/{id}/decisions` — every proposal, suppression, and ensemble step.
pub async fn run_decisions(
    State(state): State<AppState>,
    peer: ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err(error) = guard(&state, &headers, &peer) {
        return refusal(error, &state, &headers);
    }
    let Some(run_id) = parse_id(&id) else {
        return not_found(&state, &id);
    };
    let Ok(run) = state.manager.run_get(RunIdParams { run_id }).await else {
        return not_found(&state, &id);
    };
    let events = load_events(&state, run_id).await;

    let mut body = String::new();
    body.push_str(&format!(
        "<h1>Decisions <span class=\"mono dim\">{}</span></h1>\n",
        escape(&short_id(&id))
    ));
    body.push_str(&format!(
        "<p><a href=\"/runs/{id}\">&larr; back to the run</a></p>\n"
    ));
    let interesting: Vec<&EventEnvelope> = events
        .iter()
        .filter(|event| {
            matches!(
                event.kind,
                EventKind::DirectiveProposed
                    | EventKind::DirectiveSelected
                    | EventKind::EnsembleTriggered
                    | EventKind::OpinionRequested
                    | EventKind::OpinionRecorded
                    | EventKind::EnsembleResolved
                    | EventKind::AssessmentCompleted
                    | EventKind::AssessmentSkippedUnchanged
                    | EventKind::EscalationRaised
                    | EventKind::EscalationAnswered
                    | EventKind::JobRouted
            )
        })
        .collect();
    if interesting.is_empty() {
        body.push_str("<p class=\"empty\">No decisions recorded yet.</p>\n");
    }
    // Newest first: the last decision is the one an operator is looking for.
    body.push_str("<table class=\"events\">\n<thead><tr><th>seq</th><th>at</th><th>kind</th><th>detail</th></tr></thead>\n<tbody>\n");
    for event in interesting.iter().rev() {
        body.push_str(&event_row(event));
    }
    body.push_str("</tbody></table>\n");
    body.push_str(&format!(
        "<p class=\"note\">Margin in force: <code>{:.3}</code>. A value within the margin of \
         its threshold is {}.</p>\n",
        check_margin(&run).unwrap_or(0.0),
        escape("held, and policy may not act on it"),
    ));
    html_response(html::page(
        &format!("decisions {}", short_id(&id)),
        &body,
        state.web.config().auth() == AuthMode::Token,
    ))
}

/// `GET /runs/{id}/evidence` — command evidence with argv, exit code and duration.
pub async fn run_evidence(
    State(state): State<AppState>,
    peer: ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err(error) = guard(&state, &headers, &peer) {
        return refusal(error, &state, &headers);
    }
    let Some(run_id) = parse_id(&id) else {
        return not_found(&state, &id);
    };
    let events = load_events(&state, run_id).await;

    let mut body = String::new();
    body.push_str(&format!(
        "<h1>Evidence <span class=\"mono dim\">{}</span></h1>\n",
        escape(&short_id(&id))
    ));
    body.push_str(&format!(
        "<p><a href=\"/runs/{id}\">&larr; back to the run</a></p>\n"
    ));
    let collected: Vec<&EventEnvelope> = events
        .iter()
        .filter(|event| event.kind == EventKind::EvidenceCollected)
        .collect();
    if collected.is_empty() {
        body.push_str(
            "<p class=\"empty\">No command evidence has been collected for this run.</p>\n",
        );
    }
    for event in collected.iter().rev() {
        body.push_str("<section class=\"evidence\">\n");
        body.push_str(&format!(
            "<h2>seq {} &middot; {}</h2>\n",
            event.seq.get(),
            escape(&html::timestamp(event.at))
        ));
        body.push_str(&payload_table(
            &event.payload,
            &[
                "provider",
                "argv",
                "exit_code",
                "duration_ms",
                "timed_out",
                "output",
            ],
        ));
        body.push_str("</section>\n");
    }
    html_response(html::page(
        &format!("evidence {}", short_id(&id)),
        &body,
        state.web.config().auth() == AuthMode::Token,
    ))
}

/// `GET /new` — the start-a-job form.
pub async fn new_form(
    State(state): State<AppState>,
    peer: ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    if let Err(error) = guard(&state, &headers, &peer) {
        return refusal(error, &state, &headers);
    }
    html_response(html::page(
        "new job",
        &new_job_body(&state, None),
        state.web.config().auth() == AuthMode::Token,
    ))
}

/// `POST /new` — start a run, then redirect to it.
pub async fn new_submit(
    State(state): State<AppState>,
    peer: ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Form(form): Form<NewJobForm>,
) -> Response {
    if let Err(error) = guard(&state, &headers, &peer) {
        return refusal(error, &state, &headers);
    }
    let repo = match state.resolve_repo(std::path::Path::new(&form.repo)) {
        Ok(repo) => repo,
        Err(error) => {
            let body = new_job_body(&state, Some(&error.to_string()));
            return (
                StatusCode::BAD_REQUEST,
                Html(html::page(
                    "new job",
                    &body,
                    state.web.config().auth() == AuthMode::Token,
                )),
            )
                .into_response();
        }
    };
    let mut params = RunStartParams::new(repo, form.job);
    params.worker = non_empty(form.worker);
    params.model = non_empty(form.model);
    params.verify = non_empty(form.verify);
    params.permissions = non_empty(form.permissions);
    match state.manager.run_start(params).await {
        Ok(started) => Redirect::to(&format!("/runs/{}", started.run_id)).into_response(),
        Err(error) => {
            let body = new_job_body(&state, Some(&error.to_string()));
            (
                StatusCode::BAD_REQUEST,
                Html(html::page(
                    "new job",
                    &body,
                    state.web.config().auth() == AuthMode::Token,
                )),
            )
                .into_response()
        }
    }
}

/// `POST /runs/{id}/answer` — the escalation form.
pub async fn answer(
    State(state): State<AppState>,
    peer: ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Form(form): Form<AnswerForm>,
) -> Response {
    let principal = match guard(&state, &headers, &peer) {
        Ok(principal) => principal,
        Err(error) => return refusal(error, &state, &headers),
    };
    let Some(run_id) = parse_id(&id) else {
        return not_found(&state, &id);
    };
    let Some(escalation_id) = parse_escalation(&form.escalation_id) else {
        return not_found(&state, "that escalation id is not an id");
    };
    let decision = match form.decision.as_str() {
        "accept" => HumanDecision::Accept,
        "recheck" => HumanDecision::Recheck,
        "stop" => HumanDecision::Stop,
        "claim_complete" => HumanDecision::ClaimComplete,
        // Anything else is guidance, which is the only decision that carries text. A form
        // cannot type a wrong variant into existence, because this is a closed match.
        _ => HumanDecision::Guidance {
            text: form.note.clone(),
        },
    };
    let params = RunAnswerParams {
        run_id,
        escalation_id,
        decision,
        note: form.note,
        surface: Some(principal.surface().to_owned()),
    };
    match state.manager.run_answer(params, principal.surface()).await {
        Ok(_) => Redirect::to(&format!("/runs/{run_id}")).into_response(),
        Err(error) => {
            let body = format!(
                "<h1>Answer refused</h1>{}<p><a href=\"/runs/{run_id}\">&larr; back to the run</a></p>",
                html::error_banner(&error.to_string())
            );
            (
                StatusCode::CONFLICT,
                Html(html::page(
                    "answer refused",
                    &body,
                    state.web.config().auth() == AuthMode::Token,
                )),
            )
                .into_response()
        }
    }
}

/// `POST /runs/{id}/steer`.
pub async fn steer(
    State(state): State<AppState>,
    peer: ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Form(form): Form<SteerForm>,
) -> Response {
    let principal = match guard(&state, &headers, &peer) {
        Ok(principal) => principal,
        Err(error) => return refusal(error, &state, &headers),
    };
    let _ = principal;
    let Some(run_id) = parse_id(&id) else {
        return not_found(&state, &id);
    };
    let params = aftman_daemon::protocol::RunSteerParams {
        run_id,
        message: form.text,
    };
    match state.manager.run_steer(params).await {
        Ok(_) => Redirect::to(&format!("/runs/{run_id}")).into_response(),
        Err(error) => (
            StatusCode::CONFLICT,
            Html(html::page(
                "steer refused",
                &format!(
                    "<h1>Steer refused</h1>{}<p><a href=\"/runs/{run_id}\">&larr; back</a></p>",
                    html::error_banner(&error.to_string())
                ),
                state.web.config().auth() == AuthMode::Token,
            )),
        )
            .into_response(),
    }
}

/// `POST /runs/{id}/cancel`.
pub async fn cancel(
    State(state): State<AppState>,
    peer: ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err(error) = guard(&state, &headers, &peer) {
        return refusal(error, &state, &headers);
    }
    let Some(run_id) = parse_id(&id) else {
        return not_found(&state, &id);
    };
    match state.manager.run_cancel(RunIdParams { run_id }).await {
        Ok(_) => Redirect::to(&format!("/runs/{run_id}")).into_response(),
        Err(error) => (
            StatusCode::CONFLICT,
            Html(html::page(
                "cancel refused",
                &format!(
                    "<h1>Cancel refused</h1>{}<p><a href=\"/runs/{run_id}\">&larr; back</a></p>",
                    html::error_banner(&error.to_string())
                ),
                state.web.config().auth() == AuthMode::Token,
            )),
        )
            .into_response(),
    }
}

/// `GET /usage` — Jev tokens and request counts.
pub async fn usage(
    State(state): State<AppState>,
    peer: ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    if let Err(error) = guard(&state, &headers, &peer) {
        return refusal(error, &state, &headers);
    }
    let usage = state
        .manager
        .jev_usage(aftman_daemon::protocol::JevUsageParams::default())
        .await;
    let mut body = String::new();
    body.push_str("<h1>Decision-model usage</h1>\n");
    match usage {
        Ok(value) => body.push_str(&format!(
            "<pre class=\"mono\">{}</pre>\n",
            escape(&serde_json::to_string_pretty(&value).unwrap_or_default())
        )),
        Err(error) => body.push_str(&html::error_banner(&error.to_string())),
    }
    html_response(html::page(
        "usage",
        &body,
        state.web.config().auth() == AuthMode::Token,
    ))
}

/// `GET /config` — resolved configuration and where each value came from.
pub async fn config(
    State(state): State<AppState>,
    peer: ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    if let Err(error) = guard(&state, &headers, &peer) {
        return refusal(error, &state, &headers);
    }
    let described = state.web.config().describe();
    let mut body = String::new();
    body.push_str("<h1>Web configuration</h1>\n");
    body.push_str("<table class=\"kv\">\n<tbody>\n");
    if let Some(map) = described.as_object() {
        for (key, value) in map {
            body.push_str(&format!(
                "<tr><th>{}</th><td class=\"mono\">{}</td></tr>\n",
                escape(key),
                escape(&render_value(value))
            ));
        }
    }
    body.push_str("</tbody></table>\n");
    body.push_str(
        "<h2>Other settings</h2>\n\
         <p class=\"note\">The provider, worker and threshold settings are reported by \
         <code>aftman doctor</code> and are not duplicated here: one resolver, shown in one \
         place, so the two cannot disagree.</p>\n",
    );
    html_response(html::page(
        "config",
        &body,
        state.web.config().auth() == AuthMode::Token,
    ))
}

// ---- pieces -----------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct NewJobForm {
    pub repo: String,
    pub job: String,
    #[serde(default)]
    pub worker: String,
    #[serde(default)]
    pub model: String,
    #[serde(default)]
    pub verify: String,
    #[serde(default)]
    pub permissions: String,
}

#[derive(Debug, Deserialize)]
pub struct AnswerForm {
    pub escalation_id: String,
    #[serde(default)]
    pub decision: String,
    #[serde(default)]
    pub note: String,
}

#[derive(Debug, Deserialize)]
pub struct SteerForm {
    #[serde(default)]
    pub text: String,
}

fn non_empty(value: String) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

fn new_job_body(state: &AppState, error: Option<&str>) -> String {
    let mut body = String::new();
    body.push_str("<h1>New job</h1>\n");
    if let Some(error) = error {
        body.push_str(&html::error_banner(error));
    }
    body.push_str("<form method=\"post\" action=\"/new\" class=\"stack\">\n");
    body.push_str("<label>Repository <input name=\"repo\" required placeholder=\"/home/me/project\"></label>\n");
    body.push_str("<p class=\"note\">Allowed roots: ");
    let roots: Vec<String> = state
        .web
        .config()
        .allowed_repo_roots()
        .iter()
        .map(|root| escape(&root.display().to_string()))
        .collect();
    body.push_str(&roots.join(", "));
    body.push_str("</p>\n");
    body.push_str("<label>Job <textarea name=\"job\" rows=\"6\" required></textarea></label>\n");
    body.push_str("<label>Worker <select name=\"worker\">");
    for (value, label) in [
        ("", "daemon default"),
        ("simulated", "simulated"),
        ("pi", "pi"),
        ("agy", "agy"),
        ("claude", "claude"),
    ] {
        body.push_str(&format!(
            "<option value=\"{}\">{}</option>",
            attr(value),
            escape(label)
        ));
    }
    body.push_str("</select></label>\n");
    body.push_str(
        "<label>Model <input name=\"model\" placeholder=\"(backend default)\"></label>\n",
    );
    body.push_str("<label>Permissions <select name=\"permissions\">");
    for (value, label) in [
        ("", "daemon default"),
        ("sandboxed", "sandboxed"),
        ("auto-approve", "auto-approve (aftman does not sandbox it)"),
    ] {
        body.push_str(&format!(
            "<option value=\"{}\">{}</option>",
            attr(value),
            escape(label)
        ));
    }
    body.push_str("</select></label>\n");
    body.push_str("<button type=\"submit\">Start the run</button>\n");
    body.push_str("</form>\n");
    body
}

fn run_header(run: &RunState) -> String {
    let mut body = String::new();
    let id = run.header.id.to_string();
    body.push_str(&format!(
        "<h1>Run <span class=\"mono\">{}</span> <span class=\"{}\">{}</span></h1>\n",
        escape(&short_id(&id)),
        html::status_class(run.status),
        escape(html::status_label(run.status)),
    ));
    body.push_str("<table class=\"kv\">\n<tbody>\n");
    body.push_str(&html::kv_rows(&[
        (
            "repository",
            escape(&run.header.job.repo.display().to_string()),
        ),
        ("job", escape(&run.header.job.prompt)),
        (
            "worker",
            escape(&format!(
                "{}{}",
                run.header.config.worker.as_str(),
                run.header
                    .config
                    .worker_model
                    .as_deref()
                    .map(|model| format!(" ({model})"))
                    .unwrap_or_default()
            )),
        ),
        (
            "last directive",
            escape(
                &run.last_directive
                    .as_ref()
                    .map(|directive| match directive.reason() {
                        Some(reason) => format!("{}: {reason}", directive.name()),
                        None => directive.name().to_owned(),
                    })
                    .unwrap_or_else(|| "—".to_owned()),
            ),
        ),
        ("created", escape(&html::timestamp(run.header.created_at))),
        ("last seq", escape(&run.last_seq.get().to_string())),
        (
            "waiting on a human",
            escape(&html::duration(run.human_wait_ms)),
        ),
        ("aftman", escape(&run.header.aftman_version)),
    ]));
    body.push_str("</tbody></table>\n");
    body
}

/// The escalation panel. Rendered even when there is nothing to answer, because "no
/// escalation" is information too — but it is only prominent when one is open.
fn escalation_panel(run: &RunState) -> String {
    let open: Vec<&Escalation> = run
        .escalations
        .iter()
        .filter(|escalation| escalation.answered.is_none())
        .collect();
    let mut body = String::new();
    if open.is_empty() {
        body.push_str("<section class=\"escalations none\">\n");
        body.push_str("<h2>No open escalation</h2>\n");
        if !run.escalations.is_empty() {
            body.push_str(&format!(
                "<p class=\"note\">{} answered. <a href=\"/runs/{}/decisions\">See the \
                 decisions.</a></p>\n",
                run.escalations.len(),
                escape(&run.header.id.to_string())
            ));
        }
        body.push_str("</section>\n");
        return body;
    }
    body.push_str("<section class=\"escalations open\">\n");
    body.push_str("<h2>Needs a human</h2>\n");
    for escalation in open {
        let escalation_id = escalation.id.to_string();
        body.push_str(&format!(
            "<article class=\"escalation\">\
             <p class=\"ask\">{ask}</p>\
             <p class=\"note\">kind <code>{kind}</code>{check}{p}</p>",
            ask = escape(&escalation.ask),
            kind = escape(&format!("{:?}", escalation.kind).to_lowercase()),
            check = escalation
                .check
                .as_ref()
                .map(|key| format!(" &middot; check <code>{}</code>", escape(key.as_str())))
                .unwrap_or_default(),
            p = escalation
                .jev_p
                .map(|p| format!(" &middot; model said <code>{p:.3}</code>"))
                .unwrap_or_default(),
        ));
        body.push_str(&format!(
            "<form method=\"post\" action=\"/runs/{}/answer\" class=\"stack\">\
             <input type=\"hidden\" name=\"escalation_id\" value=\"{}\">\
             <label>Decision \
             <select name=\"decision\">\
             <option value=\"accept\">accept the uncertainty and continue</option>\
             <option value=\"recheck\">look again with fresh evidence</option>\
             <option value=\"guidance\">give the worker guidance</option>\
             <option value=\"claim_complete\">claim the job is complete</option>\
             <option value=\"stop\">stop the run</option>\
             </select></label>\
             <label>Note <textarea name=\"note\" rows=\"3\"></textarea></label>\
             <button type=\"submit\">Answer</button>\
             </form>\n",
            escape(&run.header.id.to_string()),
            attr(&escalation_id),
        ));
        if !escalation.opinions.is_empty() {
            body.push_str("<details><summary>opinions</summary><ul>");
            for opinion in &escalation.opinions {
                body.push_str(&format!("<li>{}</li>", escape(&format!("{opinion:?}"))));
            }
            body.push_str("</ul></details>\n");
        }
        body.push_str("</article>\n");
    }
    body.push_str("</section>\n");
    body
}

/// Every check, with its value, threshold, margin and the distance between them.
///
/// This is the exit criterion made visible: an operator can see *why* a value was or was not
/// permitted, rather than only what policy decided.
fn check_table(run: &RunState) -> String {
    let mut body = String::new();
    body.push_str("<section class=\"checks\">\n<h2>Assessment</h2>\n");
    if run.checks.is_empty() {
        body.push_str(
            "<p class=\"empty\">No assessment yet: nothing has been measured for this run.</p>\n",
        );
        body.push_str("</section>\n");
        return body;
    }
    body.push_str(
        "<table class=\"checks\">\n<thead><tr>\
         <th>check</th><th>value</th><th>threshold</th><th>margin</th><th>|p&minus;t|</th>\
         <th>band</th><th>permitted</th><th>why</th>\
         </tr></thead>\n<tbody>\n",
    );
    for check in run.checks.values() {
        body.push_str(&check_row(check));
    }
    body.push_str("</tbody></table>\n");
    body.push_str(
        "<p class=\"note\">A value inside the margin of its threshold is <em>held</em>: policy \
         may not act on it, so it cannot finish or stop the run. <code>permitted</code> is \
         computed from the value, the threshold and the margin — never from which side of the \
         threshold it fell.</p>\n",
    );
    body.push_str("</section>\n");
    body
}

fn check_row(check: &ResolvedCheck) -> String {
    let band = match check.band {
        Band::Decisive => "decisive",
        Band::InBand => "in-band",
    };
    format!(
        "<tr class=\"check {band}\">\
         <td class=\"mono\">{key}</td>\
         <td class=\"mono num\">{value}</td>\
         <td class=\"mono num\">{threshold}</td>\
         <td class=\"mono num\">{margin:.3}</td>\
         <td class=\"mono num\">{distance}</td>\
         <td>{band_label}</td>\
         <td>{permitted}</td>\
         <td>{why}</td>\
         </tr>\n",
        key = escape(check.check.as_str()),
        value = escape(&html::probability(check.jev_p)),
        threshold = escape(&html::threshold(check.threshold)),
        margin = check.margin,
        distance = escape(&html::distance(check.jev_p, check.threshold)),
        band_label = escape(band),
        permitted = if check.permitted { "yes" } else { "no" },
        why = escape(&why(check)),
    )
}

/// The reason a check is or is not permitted, in one phrase.
fn why(check: &ResolvedCheck) -> String {
    if check.permitted {
        return "decisive".to_owned();
    }
    if check.contested {
        return "contested: two opinions disagree".to_owned();
    }
    if check.threshold.is_none() {
        return "no threshold: nothing to be uncertain about".to_owned();
    }
    if check.jev_p.is_none() {
        return "no measurement".to_owned();
    }
    "in band: closer to the threshold than the margin".to_owned()
}

/// Steer and cancel. Both are forms, because a page that can answer an escalation should be
/// able to do the other two things a human does.
fn controls(run_id: RunId, run: &RunState) -> String {
    let mut body = String::new();
    if run.status.is_terminal() {
        body.push_str("<section class=\"controls\">\n");
        body.push_str("<p class=\"note\">This run has ended; there is nothing to steer.</p>\n");
        body.push_str("</section>\n");
        return body;
    }
    body.push_str("<section class=\"controls\">\n");
    body.push_str("<h2>Controls</h2>\n");
    body.push_str(&format!(
        "<form method=\"post\" action=\"/runs/{run_id}/steer\" class=\"stack\">\
         <label>Steer the worker <input name=\"text\" placeholder=\"guidance for the agent\"></label>\
         <button type=\"submit\">Send</button></form>\n"
    ));
    body.push_str(&format!(
        "<form method=\"post\" action=\"/runs/{run_id}/cancel\" class=\"inline\">\
         <button type=\"submit\" class=\"danger\">Cancel the run</button></form>\n"
    ));
    body.push_str("</section>\n");
    body
}

/// The diff, read from the same in-process walk the supervisor runs.
async fn diff_panel(state: &AppState, run: &RunState) -> String {
    let repo = run.header.job.repo.clone();
    let interrupt = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&interrupt);
    let caps = ObsCaps {
        diff_chars: PAGE_DIFF_CHARS,
        ..ObsCaps::default()
    };
    let walked = tokio::task::spawn_blocking(move || {
        aftman_daemon::repo_observe::git_snapshot(&repo, caps, &flag)
    });
    let mut body = String::new();
    body.push_str("<section class=\"diff\">\n<h2>The tree as it is now</h2>\n");
    let outcome = tokio::time::timeout(GIT_SNAPSHOT_TIMEOUT, walked).await;
    match outcome {
        Ok(Ok(Ok(git))) => {
            if !git.diff_stat.is_empty() {
                body.push_str(&format!(
                    "<pre class=\"mono stat\">{}</pre>\n",
                    escape(&git.diff_stat)
                ));
            }
            if !git.status_short.is_empty() {
                body.push_str(&format!(
                    "<pre class=\"mono status\">{}</pre>\n",
                    escape(&git.status_short)
                ));
            }
            if git.diff_excerpt.is_empty() {
                body.push_str("<p class=\"empty\">No tracked changes.</p>\n");
            } else {
                if git.truncated {
                    body.push_str(
                        "<p class=\"note\">Truncated: the diff is larger than this page shows.</p>\n",
                    );
                }
                body.push_str(&format!(
                    "<pre class=\"mono diff\">{}</pre>\n",
                    escape(&git.diff_excerpt)
                ));
            }
        }
        Ok(Ok(Err(note))) => body.push_str(&html::error_banner(&note)),
        Ok(Err(error)) => body.push_str(&html::error_banner(&error.to_string())),
        Err(_) => {
            // Stop the walk rather than leaving a blocking thread on a huge repository.
            interrupt.store(true, Ordering::Relaxed);
            body.push_str(&html::error_banner(
                "the repository did not finish being read in time; the diff is omitted",
            ));
        }
    }
    body.push_str("</section>\n");
    let _ = state;
    body
}

fn timeline(events: &[EventEnvelope], run: &RunState, full: bool) -> String {
    let mut body = String::new();
    body.push_str("<section class=\"timeline\">\n");
    body.push_str(&format!(
        "<h2>Timeline <span class=\"dim\">(last {} of {})</span></h2>\n",
        events.len(),
        run.last_seq.get()
    ));
    if events.is_empty() {
        body.push_str("<p class=\"empty\">No events.</p>\n");
    } else {
        body.push_str("<table class=\"events\">\n<thead><tr><th>seq</th><th>at</th><th>kind</th><th>detail</th></tr></thead>\n<tbody id=\"events\">\n");
        for event in events.iter().rev() {
            body.push_str(&event_row(event));
        }
        body.push_str("</tbody></table>\n");
    }
    if full {
        body.push_str(&format!(
            "<p class=\"note\"><a href=\"/runs/{}/decisions\">Decisions</a> &middot; \
             <a href=\"/runs/{}/evidence\">Evidence</a></p>\n",
            escape(&run.header.id.to_string()),
            escape(&run.header.id.to_string())
        ));
    }
    body.push_str("</section>\n");
    body
}

fn event_row(event: &EventEnvelope) -> String {
    format!(
        "<tr class=\"event kind-{kind}\">\
         <td class=\"mono num\">{seq}</td>\
         <td class=\"mono\">{at}</td>\
         <td class=\"mono\">{kind_name}</td>\
         <td>{detail}</td>\
         </tr>\n",
        seq = event.seq.get(),
        at = escape(&html::timestamp(event.at)),
        kind = escape(&crate::api::kind_name(event.kind).to_ascii_lowercase()),
        kind_name = escape(crate::api::kind_name(event.kind)),
        detail = escape(&summarise(event.kind, &event.payload)),
    )
}

/// A one-line summary of an event payload. Deliberately conservative: it prints the keys it
/// understands and never tries to render arbitrary JSON into the page.
fn summarise(kind: EventKind, payload: &serde_json::Value) -> String {
    let strings = |key: &str| {
        payload
            .get(key)
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_owned()
    };
    let numbers = |key: &str| {
        payload
            .get(key)
            .and_then(serde_json::Value::as_u64)
            .map(|value| value.to_string())
            .unwrap_or_default()
    };
    match kind {
        EventKind::WorkerStarted => {
            let what = strings("worker_id");
            format!("worker {}", short_id(&what))
        }
        EventKind::WorkerOutput => {
            let text = strings("text");
            excerpt(&text, 120)
        }
        EventKind::WorkerCompleted => format!(
            "outcome {} · success {}",
            strings("outcome"),
            payload
                .get("success")
                .and_then(serde_json::Value::as_bool)
                .map_or("?", |v| if v { "yes" } else { "no" })
        ),
        EventKind::DirectiveSelected | EventKind::DirectiveProposed => {
            format!("{} — {}", strings("directive"), strings("reason"))
        }
        EventKind::AssessmentRequested => {
            let count = payload
                .get("questions")
                .and_then(serde_json::Value::as_array)
                .map_or(0, Vec::len);
            format!("asked {count} check(s)")
        }
        EventKind::AssessmentCompleted => format!(
            "assessment {} by {}",
            numbers("assessment_seq"),
            strings("model")
        ),
        EventKind::AssessmentSkippedUnchanged => "the observation was unchanged".to_owned(),
        EventKind::EscalationRaised => strings("ask"),
        EventKind::EscalationAnswered => format!(
            "answered by {}: {}",
            strings("surface"),
            strings("decision")
        ),
        EventKind::StateChanged => format!("{} → {}", strings("from"), strings("to")),
        EventKind::ModelError | EventKind::Error => {
            format!("{}: {}", strings("code"), strings("message"))
        }
        EventKind::RateLimited => format!("retry after {} ms", numbers("retry_after_ms")),
        EventKind::BudgetExceeded => strings("message").to_string(),
        EventKind::RunFinished => strings("status").to_string(),
        EventKind::EvidenceCollected => strings("provider"),
        EventKind::JobRouted => "routing decided which responsibilities apply".to_owned(),
        EventKind::EnsembleTriggered => strings("reason").to_string(),
        EventKind::EnsembleResolved => strings("resolution").to_string(),
        EventKind::OpinionRequested | EventKind::OpinionRecorded => strings("model"),
        EventKind::WorkerSteered => excerpt(&strings("message"), 120),
        EventKind::WorkerTerminated => strings("reason"),
        EventKind::RunCreated | EventKind::Notification | EventKind::Unknown => String::new(),
    }
}

/// A small table of the payload's scalar fields, for a page that wants the raw detail.
fn payload_table(payload: &serde_json::Value, keys: &[&str]) -> String {
    let mut body = String::from("<table class=\"kv\">\n<tbody>\n");
    for key in keys {
        if let Some(value) = payload.get(*key) {
            body.push_str(&format!(
                "<tr><th>{}</th><td><pre class=\"mono\">{}</pre></td></tr>\n",
                escape(key),
                escape(&render_value(value))
            ));
        }
    }
    body.push_str("</tbody></table>\n");
    body
}

fn render_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

fn excerpt(text: &str, limit: usize) -> String {
    let mut out = String::new();
    for (index, character) in text.chars().enumerate() {
        if index >= limit {
            out.push('…');
            break;
        }
        out.push(character);
    }
    out
}

fn short_id(id: &str) -> String {
    id.chars().take(8).collect()
}

fn name_of(status: FactoryStatus) -> &'static str {
    match status {
        FactoryStatus::NeedsHuman => "needs a human",
        other => other.as_str(),
    }
}

fn parse_id(raw: &str) -> Option<RunId> {
    raw.trim().parse::<RunId>().ok()
}

fn parse_escalation(raw: &str) -> Option<EscalationId> {
    raw.trim().parse::<EscalationId>().ok()
}

async fn load_events(state: &AppState, run_id: RunId) -> Vec<EventEnvelope> {
    let params = RunEventsParams {
        run_id,
        from_seq: None,
        limit: Some(TIMELINE_EVENTS),
    };
    match state.manager.run_events(params).await {
        Ok(page) => page.events,
        Err(_) => Vec::new(),
    }
}

fn not_found(state: &AppState, what: &str) -> Response {
    not_found_error(state, &format!("{what:?} is not a run id"))
}

fn not_found_error(state: &AppState, message: &str) -> Response {
    let body = format!(
        "<h1>Not found</h1>{}<p><a href=\"/\">&larr; all runs</a></p>",
        html::error_banner(message)
    );
    (
        StatusCode::NOT_FOUND,
        Html(html::page(
            "not found",
            &body,
            state.web.config().auth() == AuthMode::Token,
        )),
    )
        .into_response()
}

/// The margin the run's checks were resolved against, read from the checks themselves.
///
/// The margin is persisted with each resolved check precisely so a page does not have to guess
/// it from the daemon's current configuration, which may have changed since.
fn check_margin(run: &RunState) -> Option<f32> {
    run.checks.values().next().map(|check| check.margin)
}
