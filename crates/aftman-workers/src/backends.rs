//! Per-CLI command construction.
//!
//! These are pure functions from options to a [`CommandSpec`], which is deliberate:
//! argv construction is where a backend silently breaks, and a pure function can be
//! tested against the documented command line without the binary being installed.
//!
//! Three facts were established by reading the installed CLIs and are encoded here:
//!
//! - **`agy` requires the mission attached to the flag.** A bare `--print` consumes the
//!   next token as its prompt, so `agy --print --model x` fails immediately with
//!   *"print took `--model` as its prompt"*. The form is `--print="<mission>"`.
//! - **`pi` has no sandbox flag and no tool-permission flag.** Its `--approve` /
//!   `--no-approve` govern trust of *project-local files*, not tool permissions, so
//!   `sandboxed` cannot be expressed and is refused rather than silently downgraded.
//! - **`claude` has real non-interactive permission control**:
//!   `--permission-mode` and `--permission-prompts`, which is what keeps a headless
//!   run from blocking on a prompt nobody can answer.
//!
//! See `docs/04-workers.md` §4, §5, and §8.1.

use serde::{Deserialize, Serialize};

use aftman_core::job::WorkerKind;

use crate::subprocess::CommandSpec;

/// The most bytes allowed in a single argv element.
///
/// Linux caps one argument at 128 KiB (`MAX_ARG_STRLEN`); aftman refuses at half that
/// rather than letting the kernel fail the launch opaquely.
pub const MAX_ARG_BYTES: usize = 64 * 1024;

/// How a session accepts a mission and further guidance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SteerProtocol {
    /// No live input channel. `steer` is honestly unsupported.
    None,
    /// `pi --mode rpc`: one JSON object per line on stdin.
    PiRpc,
}

impl SteerProtocol {
    #[must_use]
    pub const fn supports_steering(self) -> bool {
        matches!(self, Self::PiRpc)
    }

    /// The first line to write, when the mission is delivered over stdin.
    #[must_use]
    pub fn initial_prompt(self, mission: &str) -> Option<String> {
        match self {
            Self::None => None,
            Self::PiRpc => Some(pi_rpc_message("prompt", mission, Some("mission"))),
        }
    }

    /// The line that delivers guidance into a running turn.
    #[must_use]
    pub fn steer_command(self, message: &str) -> Option<String> {
        match self {
            Self::None => None,
            Self::PiRpc => Some(pi_rpc_message("steer", message, None)),
        }
    }
}

/// Build one pi RPC JSONL command.
///
/// `serde_json` does the escaping: a mission contains newlines, quotes, and braces, and
/// hand-rolling that is how a steering channel silently corrupts.
fn pi_rpc_message(kind: &str, message: &str, id: Option<&str>) -> String {
    let mut object = serde_json::Map::new();
    if let Some(id) = id {
        object.insert("id".to_owned(), serde_json::Value::String(id.to_owned()));
    }
    object.insert(
        "type".to_owned(),
        serde_json::Value::String(kind.to_owned()),
    );
    object.insert(
        "message".to_owned(),
        serde_json::Value::String(message.to_owned()),
    );
    // Cannot fail: the value is a flat object of strings.
    serde_json::to_string(&object).unwrap_or_default()
}

/// What a backend is allowed to do without asking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionMode {
    /// Apply the strongest non-interactive restriction the backend offers.
    #[default]
    Sandboxed,
    /// Suppress approvals entirely. Requires an explicit acknowledgement.
    AutoApprove,
    /// Leave approvals in place. Only valid where a human can answer.
    Interactive,
}

/// Per-run backend options.
#[derive(Debug, Clone, Default)]
pub struct BackendOptions {
    pub model: Option<String>,
    /// Reasoning effort, where the backend accepts one.
    pub effort: Option<String>,
    /// Override the executable name or path.
    pub executable: Option<String>,
    pub permissions: PermissionMode,
    /// Assert that isolation is provided externally (a container, a VM, or a
    /// throwaway checkout), which is what makes `Sandboxed` honest for `pi`.
    pub external_sandbox: bool,
    /// Whether a human can answer an approval prompt. True only on the herdr surface.
    pub human_available: bool,
    /// The argv template for `WorkerKind::Generic`, with `{mission_file}`,
    /// `{repo}`, and `{model}` placeholders.
    pub command_template: Option<String>,
    /// Prefer a live-input mode where one exists. Only `pi` has one, and it changes the
    /// entire launch shape (`--mode rpc` versus `--print --mode json`).
    pub prefer_steering: bool,
    pub extra_args: Vec<String>,
}

impl BackendOptions {
    fn program(&self, default: &str, kind: WorkerKind) -> String {
        let _ = kind;
        self.executable
            .clone()
            .unwrap_or_else(|| default.to_owned())
    }
}

/// Why a backend cannot be launched as configured.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SpecError {
    #[error(
        "`{kind}` cannot express `sandboxed`: it has no sandbox flag and no \
         tool-permission flag, so aftman will not claim to have restricted it. Either \
         provide isolation externally and set workers.{kind}.sandbox = 'container', or \
         accept permissions = 'auto-approve' (and set AFTMAN_ACK_UNSANDBOXED=1)."
    )]
    SandboxNotExpressible { kind: String },

    #[error(
        "`interactive` permissions are only valid where a human can answer an approval \
         prompt, which means the herdr surface. A headless daemon would hang."
    )]
    InteractiveNeedsHuman,

    #[error("the mission is {bytes} bytes, over the {limit} byte single-argument limit")]
    MissionTooLong { bytes: usize, limit: usize },

    #[error("unknown placeholder `{{{0}}}` in the generic command template")]
    UnknownPlaceholder(String),

    #[error("the generic command template is empty")]
    EmptyTemplate,

    // The message rather than the `io::Error`: this enum is compared in tests, and
    // `io::Error` is neither `Clone` nor `Eq`.
    #[error("could not write the mission file at {path}: {message}")]
    MissionFile { path: String, message: String },

    #[error("`{kind}` cannot be built here: {reason}")]
    UnsupportedBackend { kind: String, reason: String },
}

/// Check the mission fits in one argv element.
fn check_mission_len(mission: &str) -> Result<(), SpecError> {
    if mission.len() > MAX_ARG_BYTES {
        return Err(SpecError::MissionTooLong {
            bytes: mission.len(),
            limit: MAX_ARG_BYTES,
        });
    }
    Ok(())
}

/// `pi` in one-shot JSON mode: structured progress, then exit.
#[must_use]
pub fn pi_oneshot(mission: &str, options: &BackendOptions) -> CommandSpec {
    let mut args = vec![
        "--print".to_owned(),
        "--mode".to_owned(),
        "json".to_owned(),
        // A supervised run must not pollute or resume a human's session.
        "--no-session".to_owned(),
        // Project-local files include extensions and skills that execute code, so a
        // cloned repository does not get to load them implicitly.
        "--no-approve".to_owned(),
    ];
    if let Some(model) = &options.model {
        args.push("--model".to_owned());
        args.push(model.clone());
    }
    if let Some(effort) = &options.effort {
        args.push("--thinking".to_owned());
        args.push(effort.clone());
    }
    args.extend(options.extra_args.iter().cloned());
    args.push("--".to_owned());
    args.push(mission.to_owned());

    CommandSpec {
        program: options.program("pi", WorkerKind::Pi),
        args,
        env: Vec::new(),
        steer: SteerProtocol::None,
        working_dir_is_repo: true,
    }
}

/// `pi` in RPC mode: a long-lived session with a real steering channel.
///
/// The mission is *not* in argv; it is the first JSONL line on stdin, which is why this
/// returns a spec rather than taking a mission.
#[must_use]
pub fn pi_rpc(options: &BackendOptions) -> CommandSpec {
    let mut args = vec![
        "--mode".to_owned(),
        "rpc".to_owned(),
        "--no-session".to_owned(),
        "--no-approve".to_owned(),
    ];
    if let Some(model) = &options.model {
        args.push("--model".to_owned());
        args.push(model.clone());
    }
    if let Some(effort) = &options.effort {
        args.push("--thinking".to_owned());
        args.push(effort.clone());
    }
    args.extend(options.extra_args.iter().cloned());

    CommandSpec {
        program: options.program("pi", WorkerKind::Pi),
        args,
        env: Vec::new(),
        steer: SteerProtocol::PiRpc,
        working_dir_is_repo: true,
    }
}

/// `agy` in print mode.
///
/// The mission is attached to `--print=`, which is not cosmetic: a bare `--print`
/// consumes the following token as its prompt.
pub fn agy(mission: &str, options: &BackendOptions) -> Result<CommandSpec, SpecError> {
    check_mission_len(mission)?;
    let mut args = vec![
        format!("--print={mission}"),
        "--output-format".to_owned(),
        "stream-json".to_owned(),
    ];
    if let Some(model) = &options.model {
        args.push("--model".to_owned());
        args.push(model.clone());
    }
    if let Some(effort) = &options.effort {
        args.push("--effort".to_owned());
        args.push(effort.clone());
    }
    match options.permissions {
        PermissionMode::Sandboxed => args.push("--sandbox".to_owned()),
        PermissionMode::AutoApprove => args.push("--dangerously-skip-permissions".to_owned()),
        PermissionMode::Interactive => {}
    }
    args.extend(options.extra_args.iter().cloned());

    Ok(CommandSpec {
        program: options.program("agy", WorkerKind::Agy),
        args,
        env: Vec::new(),
        steer: SteerProtocol::None,
        working_dir_is_repo: true,
    })
}

/// `claude` in print mode with real permission control.
pub fn claude(mission: &str, options: &BackendOptions) -> Result<CommandSpec, SpecError> {
    check_mission_len(mission)?;
    let mut args = vec![
        "-p".to_owned(),
        "--output-format".to_owned(),
        "stream-json".to_owned(),
        "--verbose".to_owned(),
    ];
    match options.permissions {
        // Edits are the point; anything else that would prompt is denied rather than
        // blocking a daemon that nobody can answer.
        PermissionMode::Sandboxed => {
            args.push("--permission-mode".to_owned());
            args.push("acceptEdits".to_owned());
            args.push("--permission-prompts".to_owned());
            args.push("none".to_owned());
        }
        PermissionMode::AutoApprove => {
            args.push("--permission-mode".to_owned());
            args.push("bypassPermissions".to_owned());
        }
        PermissionMode::Interactive => {}
    }
    if let Some(model) = &options.model {
        args.push("--model".to_owned());
        args.push(model.clone());
    }
    if let Some(effort) = &options.effort {
        args.push("--effort".to_owned());
        args.push(effort.clone());
    }
    args.extend(options.extra_args.iter().cloned());
    args.push("--".to_owned());
    args.push(mission.to_owned());

    Ok(CommandSpec {
        program: options.program("claude", WorkerKind::Claude),
        args,
        env: Vec::new(),
        steer: SteerProtocol::None,
        working_dir_is_repo: true,
    })
}

/// Write a mission file readable by this user only, in a directory only this user can
/// enter. The mission is the job text, which is as sensitive as the run's own directory
/// and must not outlive the run (the worker removes the file when it ends).
fn write_mission_file(path: &std::path::Path, mission: &str) -> std::io::Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _, PermissionsExt as _};
    if let Some(parent) = path.parent() {
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(parent)?;
        // A directory left by an older version may be wider; tighten what is ours.
        let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
    }
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(mission.as_bytes())
}

/// An operator-supplied command, with the mission written to a file.
///
/// The mission goes to a file rather than into the command line so that a long prompt
/// never lands in `argv` or in a process listing, and so the template cannot be made to
/// run a shell.
pub fn generic(
    template: &str,
    mission: &str,
    repo: &std::path::Path,
    model: Option<&str>,
    mission_file: &std::path::Path,
) -> Result<CommandSpec, SpecError> {
    if template.trim().is_empty() {
        return Err(SpecError::EmptyTemplate);
    }
    write_mission_file(mission_file, mission).map_err(|error| SpecError::MissionFile {
        path: mission_file.display().to_string(),
        message: error.to_string(),
    })?;

    let mut args = Vec::new();
    for token in tokenize(template) {
        let replaced = token
            .replace("{mission_file}", &mission_file.display().to_string())
            .replace("{repo}", &repo.display().to_string())
            .replace("{model}", model.unwrap_or(""))
            // `{mission}` is offered but discouraged: it can exceed an argv limit.
            .replace("{mission}", mission);
        if let Some(start) = replaced.find('{') {
            let rest = &replaced[start..];
            if let Some(end) = rest.find('}') {
                return Err(SpecError::UnknownPlaceholder(rest[1..end].to_owned()));
            }
        }
        args.push(replaced);
    }

    Ok(CommandSpec {
        program: args.first().cloned().unwrap_or_default(),
        args: args.into_iter().skip(1).collect(),
        env: Vec::new(),
        steer: SteerProtocol::None,
        working_dir_is_repo: true,
    })
}

/// Check a backend/options combination without needing a mission.
///
/// This is deliberately separate from [`build`] so the runtime can validate at
/// **startup**. A posture the backend cannot express must fail before any run state
/// exists, not after a worker has been announced and then failed to launch.
pub fn validate(kind: WorkerKind, options: &BackendOptions) -> Result<(), SpecError> {
    // `interactive` needs a human, which a headless daemon does not have.
    if options.permissions == PermissionMode::Interactive && !options.human_available {
        return Err(SpecError::InteractiveNeedsHuman);
    }
    match kind {
        WorkerKind::Pi => {
            if options.permissions == PermissionMode::Sandboxed && !options.external_sandbox {
                // Honest failure. Passing a flag `pi` does not have would crash on
                // launch; passing nothing would leave an unpinned posture in a headless
                // daemon with no record of it.
                Err(SpecError::SandboxNotExpressible {
                    kind: "pi".to_owned(),
                })
            } else {
                Ok(())
            }
        }
        WorkerKind::Generic => {
            if options
                .command_template
                .as_deref()
                .is_none_or(|t| t.trim().is_empty())
            {
                Err(SpecError::EmptyTemplate)
            } else {
                Ok(())
            }
        }
        WorkerKind::HerdrPane => Err(SpecError::UnsupportedBackend {
            kind: "herdr:pane".to_owned(),
            reason: "herdr panes are driven by the herdr surface, not by a command line".to_owned(),
        }),
        WorkerKind::Agy | WorkerKind::Claude | WorkerKind::Simulated => Ok(()),
    }
}

/// Whether a backend will have a live input channel as configured.
///
/// Needed before a run starts, because policy decides whether to propose `Steer` or
/// degrade to `stop`/`retry` based on it. Only `pi` has such a channel today.
#[must_use]
pub fn supports_steering(kind: WorkerKind, options: &BackendOptions) -> bool {
    matches!(kind, WorkerKind::Pi) && options.prefer_steering
}

/// Split a command template into argv, honouring quotes.
///
/// A plain `split_whitespace` would turn `sh -c 'echo hi'` into four arguments and
/// quietly run something different from what was typed. This does not invoke a shell —
/// aftman never does — it only groups the tokens a shell *would* have grouped.
#[must_use]
pub fn tokenize(template: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut chars = template.chars().peekable();
    let mut in_token = false;

    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                // Single quotes: literal until the closing quote.
                in_token = true;
                for inner in chars.by_ref() {
                    if inner == '\'' {
                        break;
                    }
                    current.push(inner);
                }
            }
            '"' => {
                in_token = true;
                // Double quotes: an escaped character is literal.
                while let Some(inner) = chars.next() {
                    if inner == '"' {
                        break;
                    }
                    if inner == '\\' {
                        if let Some(escaped) = chars.next() {
                            current.push(escaped);
                        }
                        continue;
                    }
                    current.push(inner);
                }
            }
            c if c.is_whitespace() => {
                if in_token {
                    tokens.push(std::mem::take(&mut current));
                    in_token = false;
                }
            }
            c => {
                in_token = true;
                current.push(c);
            }
        }
    }
    if in_token {
        tokens.push(current);
    }
    tokens
}

/// Validate a backend/options combination, and build its spec.
///
/// This is the single entry point the runtime uses, so a backend that cannot express
/// the requested posture fails at startup rather than launching something unpinned.
pub fn build(
    kind: WorkerKind,
    mission: &str,
    options: &BackendOptions,
    mission_file: &std::path::Path,
    repo: &std::path::Path,
) -> Result<CommandSpec, SpecError> {
    validate(kind, options)?;

    match kind {
        WorkerKind::Pi => {
            if options.prefer_steering {
                Ok(pi_rpc(options))
            } else {
                Ok(pi_oneshot(mission, options))
            }
        }
        WorkerKind::Agy => agy(mission, options),
        WorkerKind::Claude => claude(mission, options),
        WorkerKind::Generic => {
            // `validate` has already established that a template is present.
            let template = options.command_template.as_deref().unwrap_or_default();
            generic(
                template,
                mission,
                repo,
                options.model.as_deref(),
                mission_file,
            )
        }
        WorkerKind::HerdrPane => Err(SpecError::UnsupportedBackend {
            kind: "herdr:pane".to_owned(),
            reason: "herdr panes are driven by the herdr surface, not by a command line".to_owned(),
        }),
        WorkerKind::Simulated => Ok(CommandSpec {
            program: String::new(),
            args: Vec::new(),
            env: Vec::new(),
            steer: SteerProtocol::None,
            working_dir_is_repo: false,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BackendOptions, MAX_ARG_BYTES, PermissionMode, SpecError, SteerProtocol, agy, build,
        claude, generic, pi_oneshot, pi_rpc,
    };
    use aftman_core::job::WorkerKind;

    fn opts() -> BackendOptions {
        BackendOptions::default()
    }

    fn auto() -> BackendOptions {
        BackendOptions {
            permissions: PermissionMode::AutoApprove,
            ..BackendOptions::default()
        }
    }

    // ---- agy ---------------------------------------------------------------

    #[test]
    fn agy_attaches_the_mission_to_the_print_flag() -> Result<(), SpecError> {
        // Verified against agy 1.3.3: a bare `--print` consumes the *next* token as its
        // prompt, so `agy --print --model m` fails with
        // "print took \"--model\" as its prompt".
        let spec = agy("do the thing", &opts())?;
        let first = spec.args.first().ok_or(SpecError::EmptyTemplate)?;
        assert_eq!(first, "--print=do the thing");
        assert!(
            !spec.args.iter().any(|a| a == "--print"),
            "a bare --print would swallow the next flag: {:?}",
            spec.args
        );
        Ok(())
    }

    #[test]
    fn agy_translates_the_permission_posture() -> Result<(), SpecError> {
        let sandboxed = agy("m", &opts())?;
        assert!(sandboxed.args.iter().any(|a| a == "--sandbox"));

        let auto = agy("m", &auto())?;
        assert!(
            auto.args
                .iter()
                .any(|a| a == "--dangerously-skip-permissions")
        );

        let interactive = agy(
            "m",
            &BackendOptions {
                permissions: PermissionMode::Interactive,
                human_available: true,
                ..BackendOptions::default()
            },
        )?;
        assert!(!interactive.args.iter().any(|a| a.starts_with("--sandbox")));
        assert!(!interactive.args.iter().any(|a| a.contains("dangerously")));
        Ok(())
    }

    #[test]
    fn agy_streams_json_and_pins_model_and_effort() -> Result<(), SpecError> {
        let spec = agy(
            "m",
            &BackendOptions {
                model: Some("fast".to_owned()),
                effort: Some("high".to_owned()),
                ..auto()
            },
        )?;
        assert!(
            spec.args
                .windows(2)
                .any(|w| w == ["--output-format", "stream-json"])
        );
        assert!(spec.args.windows(2).any(|w| w == ["--model", "fast"]));
        assert!(spec.args.windows(2).any(|w| w == ["--effort", "high"]));
        Ok(())
    }

    #[test]
    fn a_mission_over_the_argv_limit_is_refused() {
        let huge = "x".repeat(MAX_ARG_BYTES + 1);
        let error = agy(&huge, &auto()).expect_err("must refuse");
        assert!(
            matches!(error, SpecError::MissionTooLong { .. }),
            "{error:?}"
        );
        assert!(error.to_string().contains(&MAX_ARG_BYTES.to_string()));
    }

    // ---- claude ------------------------------------------------------------

    #[test]
    fn claude_denies_by_default_rather_than_blocking() -> Result<(), SpecError> {
        // Without these, `-p` still routes approvals to a prompt nobody can answer and
        // the worker hangs in a state indistinguishable from being stuck.
        let spec = claude("do it", &opts())?;
        assert!(
            spec.args
                .windows(2)
                .any(|w| w == ["--permission-mode", "acceptEdits"])
        );
        assert!(
            spec.args
                .windows(2)
                .any(|w| w == ["--permission-prompts", "none"])
        );
        Ok(())
    }

    #[test]
    fn claude_auto_approve_uses_the_bypass_mode() -> Result<(), SpecError> {
        let spec = claude("do it", &auto())?;
        assert!(
            spec.args
                .windows(2)
                .any(|w| w == ["--permission-mode", "bypassPermissions"])
        );
        assert!(!spec.args.iter().any(|a| a == "--permission-prompts"));
        Ok(())
    }

    #[test]
    fn claude_passes_the_mission_after_a_separator() -> Result<(), SpecError> {
        let spec = claude("--not-a-flag", &opts())?;
        let separator = spec.args.iter().position(|a| a == "--");
        assert_eq!(separator, Some(spec.args.len() - 2));
        assert_eq!(spec.args.last().map(String::as_str), Some("--not-a-flag"));
        Ok(())
    }

    // ---- pi ----------------------------------------------------------------

    #[test]
    fn pi_oneshot_requests_structured_progress_without_a_session() -> Result<(), SpecError> {
        let spec = pi_oneshot("do it", &opts());
        assert!(spec.args.windows(2).any(|w| w == ["--mode", "json"]));
        assert!(spec.args.iter().any(|a| a == "--no-session"));
        assert!(spec.args.iter().any(|a| a == "--print"));
        assert_eq!(spec.args.last().map(String::as_str), Some("do it"));
        assert!(!spec.steer.supports_steering());
        Ok(())
    }

    #[test]
    fn pi_does_not_trust_project_local_files_by_default() {
        // Extensions and skills from a cloned repository execute code.
        assert!(
            pi_oneshot("m", &opts())
                .args
                .iter()
                .any(|a| a == "--no-approve")
        );
        assert!(pi_rpc(&opts()).args.iter().any(|a| a == "--no-approve"));
    }

    #[test]
    fn pi_rpc_is_steerable_and_takes_its_mission_on_stdin() {
        let spec = pi_rpc(&opts());
        assert!(spec.steer.supports_steering());
        assert!(spec.args.windows(2).any(|w| w == ["--mode", "rpc"]));
        assert!(
            !spec.args.iter().any(|a| a == "--print"),
            "RPC mode is not print mode"
        );
        // The mission arrives as the first JSONL line instead.
        let initial = spec.steer.initial_prompt("do it");
        assert!(initial.is_some_and(|line| line.contains("\"type\":\"prompt\"")));
    }

    #[test]
    fn the_steer_command_is_valid_jsonl_that_survives_hostile_text()
    -> Result<(), Box<dyn std::error::Error>> {
        // A steering message contains quotes, braces, and newlines; hand-rolled
        // escaping is how this channel silently corrupts.
        let hostile = "stop\n{\"type\":\"steer\",\"message\":\"injected\"}\n\"quoted\"";
        let line = SteerProtocol::PiRpc
            .steer_command(hostile)
            .ok_or("pi rpc supports steering")?;
        assert!(!line.contains('\n'), "one line per command: {line}");

        let parsed: serde_json::Value = serde_json::from_str(&line)?;
        assert_eq!(parsed["type"], serde_json::json!("steer"));
        assert_eq!(
            parsed["message"],
            serde_json::json!(hostile),
            "the payload survives the round trip intact"
        );
        Ok(())
    }

    #[test]
    fn the_initial_prompt_carries_an_id_and_the_steer_does_not()
    -> Result<(), Box<dyn std::error::Error>> {
        let initial: serde_json::Value =
            serde_json::from_str(&SteerProtocol::PiRpc.initial_prompt("m").ok_or("a prompt")?)?;
        assert_eq!(initial["id"], serde_json::json!("mission"));
        assert_eq!(initial["type"], serde_json::json!("prompt"));

        let steer: serde_json::Value =
            serde_json::from_str(&SteerProtocol::PiRpc.steer_command("m").ok_or("a steer")?)?;
        assert!(steer.get("id").is_none());
        Ok(())
    }

    #[test]
    fn a_non_steerable_protocol_refuses_both() {
        assert!(SteerProtocol::None.initial_prompt("m").is_none());
        assert!(SteerProtocol::None.steer_command("m").is_none());
        assert!(!SteerProtocol::None.supports_steering());
    }

    // ---- generic -----------------------------------------------------------

    #[test]
    fn the_generic_template_substitutes_every_placeholder() -> Result<(), Box<dyn std::error::Error>>
    {
        let dir = tempfile::tempdir()?;
        let mission_file = dir.path().join("mission.md");
        let spec = generic(
            "mytool run --prompt {mission_file} --cwd {repo} --model {model}",
            "do the thing",
            std::path::Path::new("/repo"),
            Some("m1"),
            &mission_file,
        )?;
        assert_eq!(spec.program, "mytool");
        assert!(spec.args.contains(&"--prompt".to_owned()));
        assert!(
            spec.args
                .iter()
                .any(|a| a == &mission_file.display().to_string())
        );
        assert!(spec.args.iter().any(|a| a == "/repo"));
        assert!(spec.args.iter().any(|a| a == "m1"));
        // The mission is on disk, not in argv.
        assert_eq!(std::fs::read_to_string(&mission_file)?, "do the thing");
        assert!(!spec.args.iter().any(|a| a.contains("do the thing")));
        Ok(())
    }

    #[test]
    fn an_unknown_placeholder_is_refused_not_ignored() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let error = generic(
            "tool --x {nonsense}",
            "m",
            std::path::Path::new("/repo"),
            None,
            &dir.path().join("m.md"),
        )
        .expect_err("must refuse");
        assert_eq!(error, SpecError::UnknownPlaceholder("nonsense".to_owned()));
        Ok(())
    }

    #[test]
    fn the_template_tokenizer_honours_quotes() {
        assert_eq!(super::tokenize("a b c"), vec!["a", "b", "c"]);
        assert_eq!(
            super::tokenize("sh -c 'echo hi there'"),
            vec!["sh", "-c", "echo hi there"],
            "a quoted argument stays one argument"
        );
        assert_eq!(
            super::tokenize("tool \"one arg\" plain"),
            vec!["tool", "one arg", "plain"]
        );
        assert_eq!(
            super::tokenize("tool \"escaped \\\"quote\\\"\""),
            vec!["tool", "escaped \"quote\""]
        );
        // Collapsing runs of whitespace, and an empty template, are not errors here.
        assert_eq!(super::tokenize("  a   b  "), vec!["a", "b"]);
        assert!(super::tokenize("   ").is_empty());
    }

    #[test]
    fn a_quoted_generic_template_produces_the_intended_argv()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let spec = generic(
            "sh -c 'cat {mission_file}'",
            "hello",
            std::path::Path::new("/repo"),
            None,
            &dir.path().join("m.md"),
        )?;
        assert_eq!(spec.program, "sh");
        assert_eq!(spec.args[0], "-c");
        assert!(spec.args[1].starts_with("cat "), "{:?}", spec.args);
        assert_eq!(spec.args.len(), 2, "the quoted command is one argument");
        Ok(())
    }

    #[test]
    fn an_empty_template_is_refused() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let error = generic(
            "   ",
            "m",
            std::path::Path::new("/repo"),
            None,
            &dir.path().join("m.md"),
        )
        .expect_err("must refuse");
        assert_eq!(error, SpecError::EmptyTemplate);
        Ok(())
    }

    // ---- posture validation ------------------------------------------------

    #[test]
    fn pi_with_sandboxed_is_refused_rather_than_silently_downgraded()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let error = build(
            WorkerKind::Pi,
            "m",
            &opts(),
            &dir.path().join("m.md"),
            dir.path(),
        )
        .err()
        .ok_or("sandboxed is not expressible for pi, so this must fail")?;
        assert!(
            matches!(error, SpecError::SandboxNotExpressible { .. }),
            "{error:?}"
        );
        // The message tells the operator exactly what to do instead.
        let message = error.to_string();
        assert!(message.contains("sandbox = 'container'"), "{message}");
        assert!(message.contains("auto-approve"), "{message}");
        Ok(())
    }

    #[test]
    fn pi_with_an_asserted_external_sandbox_is_allowed() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let options = BackendOptions {
            external_sandbox: true,
            ..BackendOptions::default()
        };
        let spec = build(
            WorkerKind::Pi,
            "m",
            &options,
            &dir.path().join("m.md"),
            dir.path(),
        )?;
        assert_eq!(spec.program, "pi");
        Ok(())
    }

    #[test]
    fn interactive_permissions_are_refused_without_a_human()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let options = BackendOptions {
            permissions: PermissionMode::Interactive,
            human_available: false,
            ..BackendOptions::default()
        };
        let error = build(
            WorkerKind::Claude,
            "m",
            &options,
            &dir.path().join("m.md"),
            dir.path(),
        )
        .err()
        .ok_or("interactive permissions need a human, so this must fail")?;
        assert_eq!(error, SpecError::InteractiveNeedsHuman);
        assert!(error.to_string().contains("herdr"), "{error}");
        Ok(())
    }

    #[test]
    fn interactive_permissions_are_allowed_on_a_surface_with_a_human()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let options = BackendOptions {
            permissions: PermissionMode::Interactive,
            human_available: true,
            ..BackendOptions::default()
        };
        let spec = build(
            WorkerKind::Claude,
            "m",
            &options,
            &dir.path().join("m.md"),
            dir.path(),
        )?;
        assert!(!spec.args.iter().any(|a| a == "--permission-mode"));
        Ok(())
    }

    #[test]
    fn an_executable_override_is_honoured() -> Result<(), SpecError> {
        let spec = agy(
            "m",
            &BackendOptions {
                executable: Some("/opt/agy".to_owned()),
                ..auto()
            },
        )?;
        assert_eq!(spec.program, "/opt/agy");
        Ok(())
    }
}
