//! Coding-agent backends.
//!
//! Each backend implements [`aftman_core::worker::Worker`]. The trait is the whole
//! contract: kind, whether the backend can be steered mid-turn, a run, a steer, and
//! a terminate. Nothing about `pi`, `agy`, `claude`, or herdr leaks into the
//! runtime.
//!
//! | Backend | Steering | Verified against |
//! | --- | --- | --- |
//! | [`backends::pi_oneshot`] / [`backends::pi_rpc`] | RPC mode only | pi 1.1.0 |
//! | [`backends::agy`] | no | agy 1.3.3 |
//! | [`backends::claude`] | no | claude 2.1.293 |
//! | [`backends::generic`] | no | operator-supplied |
//! | [`simulated::SimulatedWorker`] | yes (synthetic) | — |
//!
//! [`subprocess::SubprocessWorker`] runs any of them, and is where the requirements
//! that are the same for all of them live: an own process group so termination reaches
//! grandchildren, bounded output, interrupt-before-kill, and a scrubbed environment.

#![forbid(unsafe_code)]

pub mod backends;
pub mod evidence;
pub mod process;
pub mod simulated;
pub mod subprocess;

pub use backends::{BackendOptions, PermissionMode, SpecError, SteerProtocol};
pub use evidence::run_provider;
pub use simulated::SimulatedWorker;
pub use subprocess::{CommandSpec, SubprocessWorker};

/// Names the supervisor may contribute to a worker's environment.
///
/// The daemon keeps whichever shell started it, for life. Inheriting that environment
/// wholesale is what let a `GITHUB_TOKEN` exported in one shell reach every later worker
/// from any other shell, so inheritance is now this list and nothing more: what the
/// operator wishes to add, they name with `--pass-env` (the client's allowlist), and what
/// the daemon contributes is fixed and reviewable here.
///
/// The distinction is the whole point. `PATH` is here as a fallback for a client that
/// forwards nothing (a raw socket client); the CLI always forwards its own, which
/// overrides this. `SSH_AUTH_SOCK` is deliberately **not** here: it is a live signing
/// capability, so it is something the operator grants with the client's allowlist rather
/// than something the daemon hands out by sitting in the right shell.
pub fn is_worker_baseline(key: &str) -> bool {
    const EXACT: &[&str] = &[
        "HOME", "USER", "LOGNAME", "SHELL", "TERM", "TMPDIR", "TMP", "TEMP", "TZ", "LANG",
        "LANGUAGE", "PATH",
    ];
    const PREFIXES: &[&str] = &["LC_", "XDG_"];
    EXACT.contains(&key) || PREFIXES.iter().any(|prefix| key.starts_with(prefix))
}

/// The worker environment, with supervisor credentials removed.
///
/// This is the single most important function in this crate. A coding agent is a
/// general-purpose program with shell access that reads files; if it can read the
/// supervisor's key, then any text it reads from anywhere can leak that key, and the
/// key is billable.
///
/// Every code path that spawns a subprocess must build its environment through this
/// function. Never `std::env::inherit`.
#[must_use]
pub fn worker_environment(extra: &[(String, String)]) -> Vec<(String, String)> {
    // `vars_os`, not `vars`: `vars` panics on an entry that is not valid UTF-8. Such an
    // entry is dropped, deliberately: the scrubbing below works on names and values as
    // text, and an agent gains nothing it needs from a variable that is not text.
    build_worker_environment(
        std::env::vars_os()
            .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?))),
        extra,
    )
}

/// The pure half of [`worker_environment`].
///
/// Split out so the scrubbing rule can be tested against a synthetic environment
/// instead of by mutating the test process's own environment (which would be
/// `unsafe`, racy across threads, and forbidden workspace-wide). All of the
/// interesting behaviour lives here; the public function only supplies the source
/// variables.
#[must_use]
pub fn build_worker_environment(
    base: impl IntoIterator<Item = (String, String)>,
    extra: &[(String, String)],
) -> Vec<(String, String)> {
    // The daemon contributes only the baseline; the client contributes its allowlist.
    // Nothing else crosses, which is what makes "what may influence a run?" answerable by
    // reading [`is_worker_baseline`] and the client's flag.
    let mut env: Vec<(String, String)> = base
        .into_iter()
        .filter(|(key, _)| is_worker_baseline(key) && !is_supervisor_secret(key))
        .collect();
    // Extras are applied last so a caller can grant something specific.
    //
    // They are **also** scrubbed. An earlier version of this comment claimed an extra
    // "cannot re-introduce a secret it was not deliberately given", which was simply
    // false: because extras are appended after the filter above, an extra was the one
    // way a `TYPESAFE_*` name could reach a worker. The protocol layer rejects such a
    // name outright; this is the second layer, so a future caller that forgets cannot
    // reopen it.
    env.extend(
        extra
            .iter()
            .filter(|(key, _)| !is_supervisor_secret(key))
            .cloned(),
    );
    env
}

/// The supervisor's own environment, for a subprocess the *supervisor* runs.
///
/// The supervisor's `git` is the only caller: it is not an agent, it needs the daemon's
/// environment to be usable at all, and it must not see a secret. It is a separate
/// function from [`worker_environment`] because the two want different policies, and a
/// single helper serving both is how a worker ended up inheriting the daemon's shell.
#[must_use]
pub fn supervisor_environment() -> Vec<(String, String)> {
    build_supervisor_environment(
        std::env::vars_os()
            .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?))),
        &[],
    )
}

/// The pure half of [`supervisor_environment`], over an explicit base.
#[must_use]
pub fn build_supervisor_environment(
    base: impl IntoIterator<Item = (String, String)>,
    extra: &[(String, String)],
) -> Vec<(String, String)> {
    let mut env: Vec<(String, String)> = base
        .into_iter()
        .filter(|(key, _)| !is_supervisor_secret(key))
        .collect();
    env.extend(
        extra
            .iter()
            .filter(|(key, _)| !is_supervisor_secret(key))
            .cloned(),
    );
    env
}

/// Whether an environment variable belongs to the supervisor rather than the agent.
///
/// Prefix matching is deliberate: a future `TYPESAFE_*` or `AFTMAN_*` variable must
/// be scrubbed by default rather than by being remembered here.
#[must_use]
pub fn is_supervisor_secret(key: &str) -> bool {
    const PREFIXES: &[&str] = &[
        "TYPESAFE_",
        "AFTMAN_",
        // Provider keys are not granted to a worker by default; an agent normally
        // authenticates itself through its own login. `inherit_provider_keys` in the
        // run config opts in explicitly, and every run that does so records which
        // names were granted.
        "OPENROUTER_API_KEY",
        "ANTHROPIC_API_KEY",
        "OPENAI_API_KEY",
    ];
    PREFIXES
        .iter()
        .any(|prefix| key.eq_ignore_ascii_case(prefix) || key.starts_with(prefix))
        || key.eq_ignore_ascii_case("TYPESAFE_API_KEY")
}

#[cfg(test)]
mod tests {
    use super::{
        build_supervisor_environment, build_worker_environment, is_supervisor_secret,
        is_worker_baseline,
    };

    #[test]
    fn supervisor_credentials_are_never_inherited() {
        for key in [
            "TYPESAFE_API_KEY",
            "TYPESAFE_BASE_URL",
            "AFTMAN_WEB_TOKEN",
            "AFTMAN_JEV_API_KEY",
            "OPENROUTER_API_KEY",
            "ANTHROPIC_API_KEY",
            "OPENAI_API_KEY",
        ] {
            assert!(is_supervisor_secret(key), "{key} must be scrubbed");
        }
    }

    #[test]
    fn future_prefixed_variables_are_scrubbed_by_default() {
        // A variable that does not exist yet must be scrubbed without anyone
        // remembering to add it to a list.
        assert!(is_supervisor_secret("TYPESAFE_SOMETHING_NEW"));
        assert!(is_supervisor_secret("AFTMAN_FUTURE_FLAG"));
    }

    #[test]
    fn ordinary_variables_are_not_secrets() {
        for key in ["PATH", "HOME", "LANG", "CARGO_HOME", "TERM"] {
            assert!(!is_supervisor_secret(key), "{key} is not a secret");
        }
    }

    #[test]
    fn the_baseline_is_what_the_daemon_may_contribute() {
        // What an agent needs to be usable at all.
        for key in ["HOME", "PATH", "SHELL", "TERM", "TMPDIR", "LANG", "USER"] {
            assert!(is_worker_baseline(key), "{key} must be in the baseline");
        }
        // Prefix families, so a locale or XDG variable does not have to be listed.
        for key in ["LC_ALL", "LC_CTYPE", "XDG_RUNTIME_DIR", "XDG_CONFIG_HOME"] {
            assert!(is_worker_baseline(key), "{key} must be in the baseline");
        }
        // Everything else the daemon happens to hold is *not* inherited. This is the list
        // that decides, and `SSH_AUTH_SOCK` is deliberately absent: it is a live signing
        // capability, so the operator grants it with the client's allowlist rather than
        // the daemon handing it out because it sat in the right shell.
        for key in [
            "GITHUB_TOKEN",
            "AWS_SECRET_ACCESS_KEY",
            "CARGO_HOME",
            "SSH_AUTH_SOCK",
            "EDITOR",
        ] {
            assert!(!is_worker_baseline(key), "{key} must not be inherited");
        }
    }

    #[test]
    fn the_built_environment_contains_no_supervisor_secret() {
        // The regression that matters most: whatever the process happens to have
        // set, none of it leaks. Driven by a synthetic source environment so the
        // test does not mutate this process's own variables.
        let base = vec![
            ("PATH".to_owned(), "/usr/bin".to_owned()),
            ("TYPESAFE_API_KEY".to_owned(), "should-not-leak".to_owned()),
            (
                "TYPESAFE_BASE_URL".to_owned(),
                "https://evil.example".to_owned(),
            ),
            ("AFTMAN_WEB_TOKEN".to_owned(), "should-not-leak".to_owned()),
            ("ANTHROPIC_API_KEY".to_owned(), "should-not-leak".to_owned()),
            ("CARGO_HOME".to_owned(), "/cargo".to_owned()),
        ];
        let env = build_worker_environment(base, &[]);

        assert!(
            env.iter().any(|(k, _)| k == "PATH"),
            "a baseline variable must survive"
        );
        assert!(
            !env.iter().any(|(k, _)| k == "CARGO_HOME"),
            "a non-baseline variable must not be inherited from the daemon: {env:?}"
        );
        for (key, value) in &env {
            assert!(
                !is_supervisor_secret(key),
                "scrubbed key {key} leaked into the worker environment"
            );
            assert_ne!(value, "should-not-leak", "value for {key} leaked");
        }
        assert!(
            !env.iter().any(|(_, v)| v == "https://evil.example"),
            "a repository-supplied base URL must not reach the worker either"
        );
    }

    #[test]
    fn the_daemons_own_variables_do_not_reach_a_worker() {
        // The symptom this closes: the daemon is started by the first client that needs
        // it and keeps that shell's environment for life, so a token exported in one
        // shell used to reach every later worker from any other shell.
        let base = vec![
            ("PATH".to_owned(), "/usr/bin".to_owned()),
            ("GITHUB_TOKEN".to_owned(), "canary-token-value".to_owned()),
            ("VIRTUAL_ENV".to_owned(), "/first-shell/venv".to_owned()),
        ];
        let env = build_worker_environment(base, &[]);
        assert!(env.iter().any(|(k, _)| k == "PATH"));
        assert!(
            !env.iter().any(|(k, _)| k == "GITHUB_TOKEN"),
            "the daemon's shell must not leak: {env:?}"
        );
        assert!(
            !env.iter().any(|(k, _)| k == "VIRTUAL_ENV"),
            "the client forwards its own virtualenv, so the daemon's must not appear: {env:?}"
        );
        assert!(!env.iter().any(|(_, v)| v == "canary-token-value"));
    }

    #[test]
    fn the_supervisor_environment_still_scrubs_but_inherits() {
        // The supervisor's own `git` is not an agent: it needs the daemon's environment to
        // run, and must still never see a secret.
        let base = vec![
            ("PATH".to_owned(), "/usr/bin".to_owned()),
            ("CARGO_HOME".to_owned(), "/cargo".to_owned()),
            ("GITHUB_TOKEN".to_owned(), "still-inherited".to_owned()),
            ("TYPESAFE_API_KEY".to_owned(), "must-not-leak".to_owned()),
        ];
        let env = build_supervisor_environment(base, &[]);
        assert!(env.iter().any(|(k, _)| k == "CARGO_HOME"));
        assert!(env.iter().any(|(k, _)| k == "GITHUB_TOKEN"));
        assert!(!env.iter().any(|(k, _)| is_supervisor_secret(k)));
        assert!(!env.iter().any(|(_, v)| v == "must-not-leak"));
    }

    #[test]
    fn extras_are_applied_after_scrubbing_and_are_themselves_scrubbed() {
        let base = vec![("TYPESAFE_API_KEY".to_owned(), "secret".to_owned())];
        let env = build_worker_environment(
            base,
            &[
                ("MY_TEST_MARKER".to_owned(), "1".to_owned()),
                // The one way a secret name could reach a worker, before the fix.
                ("TYPESAFE_API_KEY".to_owned(), "smuggled".to_owned()),
                // Note this is *also* the supervisor's namespace, so an operator cannot
                // forward it either: `AFTMAN_*` names are reserved, not merely checked.
                ("AFTMAN_WEB_TOKEN".to_owned(), "smuggled".to_owned()),
            ],
        );
        assert!(
            env.iter().any(|(k, v)| k == "MY_TEST_MARKER" && v == "1"),
            "an ordinary extra must survive"
        );
        assert!(
            !env.iter().any(|(k, _)| is_supervisor_secret(k)),
            "an extra must not be able to re-introduce a supervisor secret: {env:?}"
        );
        assert!(
            !env.iter().any(|(_, v)| v == "smuggled"),
            "the smuggled value must not appear at all"
        );
    }
}
