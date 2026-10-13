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
    let mut env: Vec<(String, String)> = base
        .into_iter()
        .filter(|(key, _)| !is_supervisor_secret(key))
        .collect();
    // Extras are applied last so a caller can grant something specific; the
    // scrub happens before, not after, so an extra cannot re-introduce a secret it
    // was not deliberately given.
    env.extend(extra.iter().cloned());
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
    use super::is_supervisor_secret;

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
    fn ordinary_variables_are_left_alone() {
        for key in ["PATH", "HOME", "LANG", "CARGO_HOME", "TERM"] {
            assert!(!is_supervisor_secret(key), "{key} must survive");
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
        let env = super::build_worker_environment(base, &[]);

        assert!(
            env.iter().any(|(k, _)| k == "PATH"),
            "ordinary variables must survive"
        );
        assert!(env.iter().any(|(k, _)| k == "CARGO_HOME"));
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
    fn extras_are_applied_after_scrubbing() {
        let base = vec![("TYPESAFE_API_KEY".to_owned(), "secret".to_owned())];
        let env = super::build_worker_environment(
            base,
            &[("AFTMAN_TEST_MARKER".to_owned(), "1".to_owned())],
        );
        assert!(
            env.iter()
                .any(|(k, v)| k == "AFTMAN_TEST_MARKER" && v == "1"),
            "the caller's extras must survive even though the prefix is scrubbed"
        );
        assert!(!env.iter().any(|(k, _)| k == "TYPESAFE_API_KEY"));
    }
}
