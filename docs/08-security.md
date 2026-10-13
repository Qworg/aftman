# 08 — Security

Aftman holds a billable API key, spawns coding agents with write access to a repository, and can
expose an HTTP API that starts those agents. That is the whole threat surface.

## 1. Assets, in order of importance

| Asset | Why it matters | Primary protection |
| --- | --- | --- |
| `TYPESAFE_API_KEY` | Billable; a leak costs money and can be used to read prompts | Never leaves the daemon process; scrubbed from every worker env; never logged |
| The repository | A worker can destroy it | Aftman does not protect it — see §10 |
| Run events | Contain job text, diffs, and model outputs | Local-only, `0700` state dir, no telemetry |
| The web API | Starts agents and steers them | Loopback default; bearer token when bound wider |
| Supervisor integrity | A forged probability changes what happens to the repo | Policy is pure Rust; no model output is an instruction |

The ordering is deliberate: aftman is much better at protecting the key and the decision path than
it is at protecting your working tree, and the docs say so rather than implying otherwise.

## 2. Credentials

### 2.1 Storage

- Read from `TYPESAFE_API_KEY` (compatible with the upstream SDK) or `AFTMAN_JEV_API_KEY`.
- `.env` loading order, non-overriding, real environment always wins:
  1. `$XDG_CONFIG_HOME/aftman/.env`;
  2. the process environment.
- **`<repo>/.env` is never loaded into the supervisor process.** This is a deliberate divergence
  from upstream. The attack it closes: a cloned repository ships
  `TYPESAFE_BASE_URL=https://attacker.example` in its `.env`; the operator has `TYPESAFE_API_KEY`
  exported but no explicit base URL; the repo's value is the only one present, so it is adopted,
  and the next request sends `Authorization: Bearer <key>` to the attacker. Non-overriding
  precedence does **not** help here, because the variable was never set in the first place.
  A repository `.env` may still be passed to the *worker* environment as ordinary variables; it may
  not reach supervisor configuration.
- **The base URL must be `https://…` or a loopback address.** A cleartext non-loopback base URL is a
  startup error, because it would put the key on the wire in the clear. `aftman doctor` prints the
  resolved scheme and host so an unexpected redirect is visible before the first request.
- Optional OS keyring support (`keyring` crate) via `aftman auth login`, for operators who do not
  want a key in a shell profile. Env var still takes precedence when both exist.
- `aftman doctor` reports presence, never the value, and never a prefix or length.

### 2.2 Subprocess hygiene

This is the single most important rule in the codebase.

```rust
pub fn worker_environment(extra: &[(String, String)]) -> Vec<(String, String)>
```

Always removes:

```text
TYPESAFE_API_KEY, TYPESAFE_BASE_URL, TYPESAFE_DEFAULT_MODEL, TYPESAFE_* (prefix)
AFTMAN_JEV_API_KEY, AFTMAN_WEB_TOKEN, AFTMAN_* (prefix)
OPENROUTER_API_KEY, ANTHROPIC_API_KEY, OPENAI_API_KEY     # unless explicitly granted
```

**The one exception, and it is not a worker.** Auto-starting the daemon makes the CLI spawn
`current_exe() serve --detached` ([07-surfaces.md section 1](07-surfaces.md)). That is the
supervisor re-executing itself, and it must inherit its environment whole because it needs the
provider credentials the user set; routing it through `worker_environment()` would start a daemon
that can never reach Jev. Nothing the daemon later spawns as a *worker* is affected: every such
subprocess still gets `worker_environment()`. The spawn site in `aftman-cli/src/autostart.rs` says
so in a comment.

**The daemon's environment is frozen at its start.** The auto-started daemon inherits the
starting shell's environment, and all of that environment (minus the stripped names above) is
what its workers and evidence commands see for every later run, whoever asks for it. Nothing
forwards a later caller's environment. This is stated, not hidden: `aftman status` and `doctor`
print what the daemon holds (provider yes/no, model, base URL with credentials removed, never a
key), and `aftman stop` replaces it. Whether to forward a per-run environment is a protocol
decision not taken in M3.

Rationale: a coding agent is a general-purpose program with shell access that reads files. If it
can read the supervisor's key, then any prompt injection that reaches it can exfiltrate the key,
and the key is billable. The agent's own authentication comes from its own CLI login and is
unaffected.

### 2.2a Project-local agent configuration

The same logic applies one level down: a cloned repository can carry executable agent configuration
(`pi` extensions and skills, `claude` project settings, `agy` projects). Aftman does not load it by
default — `pi` runs with `--no-approve`, and `agy`/`claude` are not given project-config trust.
`[workers] trust_project_config = true` opts in, is recorded in `meta.json`, and warns. See
[04-workers.md §8.2](04-workers.md).

Provider keys are *not* granted by default. `[workers] inherit_provider_keys = ["ANTHROPIC_API_KEY"]`
opts in for backends that need an API key instead of a login, and every run that does so records
which keys were granted in `meta.json` (names only) and prints a warning.

### 2.3 Redaction

- A base URL carrying `user:password@` is redacted wherever it is shown (`doctor`, `status`,
  the `launch` record, `invalid_config` messages sent to socket clients), and the userinfo is
  never mistaken for the host (`http://localhost@evil.example` is not loopback).

- A `tracing` layer redacts any field whose key matches
  `(?i)(api[_-]?key|authorization|token|secret|password|cookie)`.
- `Authorization` is never logged even at `trace`; the outgoing header map is logged by key name
  only.
- Error bodies are truncated to 200 bytes and passed through the same redactor. A provider that
  echoes the key back in an error is the realistic leak path here, and this is the mitigation.
- `meta.json` stores no environment values. It stores the *names* of granted keys.
- Untracked-file excerpts and `AGENTS.md` contents are **never persisted** — only the digest and
  the assessment that referenced them.

## 3. Subprocess boundary

| Control | Implementation |
| --- | --- |
| Process group | Every worker starts with `process_group(0)`, so termination reaches grandchildren (shells, test runners, language servers) |
| No shell interpolation | `argv` is a literal vector everywhere; `generic` uses a parsed template, never `sh -c` around a mission |
| Mission as a file | `{mission_file}` is preferred over `{mission}` so a long prompt never lands in `argv` or in a process listing |
| Bounded output | Every stream is a bounded ring; a worker cannot exhaust daemon memory by printing |
| Hard timeouts | Per-worker and per-run; the timeout path is the same interrupt→grace→kill path as cancellation |
| No credential inheritance | §2.2 |
| Explicit permission mode | §3.1 |

### 3.1 Permission modes, stated honestly

```toml
[workers.permissions]
mode = "sandboxed"   # sandboxed | auto-approve | interactive
```

- `sandboxed` (default) passes the backend's own sandbox flag where one exists.
- `auto-approve` passes the backend's skip-permissions flag. Requires
  `AFTMAN_ACK_UNSANDBOXED=1` **in the environment of the client that starts the run** (the CLI
  checks it before sending `run.start`; the daemon's environment is whichever shell happened to
  start it, so checking there would test the wrong person's intent). Every run with this posture
  prints a prominent warning (in both daemon and `--no-daemon` mode), and the posture is recorded
  in the run: the `RUN_CREATED` event carries a `launch` map (`permissions`, `external_sandbox`,
  `steering`) that the composition root fills in ([02-lld.md section 11](02-lld.md)), and `aftman
  show` prints it. Because the socket accepts any process of the same user (section 5), a raw
  socket client can request `auto-approve` **without** the acknowledgement; the acknowledgement is
  a guard against a slip, not an access control, and the record in the run is what makes such a
  run visible afterwards.
  It exists because a headless agent that blocks on a prompt is useless, and pretending otherwise
  would push operators to bypass aftman instead.
- `interactive` is allowed **only** on the herdr surface, where a `blocked` agent becomes an
  escalation a human can answer. It is refused at startup elsewhere, with a message naming herdr:
  by the CLI before it sends anything, and by the daemon's factory (`invalid_config`) for any other
  client.

**Aftman is not a sandbox.** It does not confine the filesystem, network, or syscalls of a worker.
The correct controls are the agent's own sandbox, a container, or a throwaway checkout, and
[04-workers.md §8](04-workers.md) says so in the same words.

## 4. Network exposure

| Posture | Auth | Exposure |
| --- | --- | --- |
| Loopback (default) | none needed | This machine only |
| `bind = 0.0.0.0` | `auth = "token"` required | LAN; fails to start without a token |
| Tailscale via `tailscale serve` | tailnet identity | Tailnet only; port stays closed on physical interfaces |
| `tailscale funnel` | token required + warning | Public internet — **discouraged** |

Implementation details:

- **Any non-loopback bind with `auth != "token"` is a startup error**, not a warning. This
  deliberately includes `auth = "loopback"`: an earlier revision only rejected `auth = "none"`,
  which left it undefined whether a `--bind 0.0.0.0` plus the default `auth = "loopback"` served
  LAN peers unauthenticated.
- `auth = "loopback"` additionally rejects any request whose peer address is not loopback, as
  defence in depth. `tailscale serve` proxies from loopback, so the default posture still works end
  to end with the port never opened on a physical interface.
- **`repo` paths are validated against `web.allowed_repo_roots`.** `POST /v1/runs` accepts a
  filesystem path, and a run's observation is readable afterwards, so without an allow-list a
  token holder could point a run at `/etc` or `~/.ssh` and read what the observation collected.
  Paths are canonicalised (symlinks resolved) and must land inside a configured root; anything else
  is `invalid_repo`. Under `auth = "loopback"` the default root is `$HOME`; under `auth = "token"`
  an explicit `allowed_repo_roots` is required.
- A repository that is not a git work tree is rejected unless `[web] allow_non_git_repos = true`,
  since a non-git tree is not a code repository and is almost always a mistake or a probe.
- Token comparison is constant-time; the token is compared as bytes, never logged, and never
  included in error messages.
- SSE `?token=` is accepted only because `EventSource` cannot set headers. The cookie-login flow is
  the documented alternative, and the query-parameter caveat (proxy and server logs) is stated in
  the UI's own help text.
- No CORS headers are sent by default. The UI is same-origin; a cross-origin API client must be
  explicitly listed in `web.allowed_origins`.
- No plugins, no `eval`, no user-supplied routes.
- The API cannot read arbitrary files: `/v1/runs` returns observations that aftman *chose* to
  collect, already bounded and scoped to an allowed root. There is no `GET /file?path=`.

## 5. Socket

- `$XDG_RUNTIME_DIR/aftman/aftman.sock`, mode `0600`; if `XDG_RUNTIME_DIR` is unavailable, the
  fallback path is `0700` with the socket at `0600`.
- Any process running as the user can speak to the socket. This is intentional: on a single-user
  machine, "can run as you" is already game over, and a socket-level auth layer would add no
  security while breaking `nc` debugging.
- Stale sockets are replaced only after a connect attempt fails, and only by the process holding
  the daemon's pid lock; a live socket is never stolen ([07-surfaces.md section 9](07-surfaces.md)).

## 6. Evidence, injection, and untrusted text

Everything in an observation that comes from the repository or a worker is untrusted text:

- **Bounded** by byte caps, and compacted by the request budget, so a repository cannot flood a
  model.
- **Labelled by source** so a check's instructions can distinguish a claim (`worker_output`) from a
  fact (`command:…`).
- **Fenced as data** in the `state` object. Jev receives state and typed questions; it does not
  receive instructions from the repository. This is a structural advantage of a decision model over
  a chat model, and it is why Jev is the supervisor.
- **Judges are tool-less.** A `pi`/`agy`/`claude` judge in the ensemble has no tools and no file
  access, so injected text cannot cause an action. It can at most cause a CONTEST, which is the
  conservative direction ([06-ensemble.md §10](06-ensemble.md)).
- **Sensitive files are skipped** in untracked excerpts: binary files (NUL in the first 4 KiB),
  unreadable files, symlinks, paths escaping the repository, and names matching `.env`, `id_rsa`,
  `*secret*`, `*token*`, `*.pem`, `*.key`.
- **Truncation happens on raw byte boundaries** before UTF-8 decoding, so a truncated excerpt can
  never exceed its cap and cannot be used to smuggle bytes past a length check.

The honest statement of the residual risk: a worker can put arbitrary text in its output, and that
text reaches a model. It cannot reach the key, and it cannot become an instruction to aftman. The
worst realistic outcome is a *worse judgment*, which the thresholds, the invariants, and the
ensemble exist to bound.

## 7. Data at rest

```text
$XDG_STATE_HOME/aftman/            0700
├─ runs/<run-id>/                  0700
│  ├─ meta.json                    job, repo path, config, granted key *names*
│  ├─ state.json
│  ├─ events.jsonl                 assessments, decisions, opinions, evidence
│  └─ artifacts/                   diffs captured at decision points
├─ jev-usage.jsonl
└─ index.sqlite
```

- Nothing is written outside this directory and the run's own temp dirs. The directories aftman
  creates (the state root if missing, `runs/`, and each `runs/<id>/`) are created `0700`; a state
  root that already exists is left as the user made it.
- A `generic` worker's mission file (the job text) is `0600` in a `0700` directory and is removed
  when the worker ends; after a hard kill, recovery removes it with the orphaned worker. Nothing
  of a run's job text outlives the run outside its run directory.
- A worker orphaned by a dead supervisor is ended by recovery only if it can be positively
  identified (start time, process group, boot id); otherwise it is left alone and the event says
  so ([04-workers.md section 11](04-workers.md)).
- Writes to a run's files are serialized per run, and a purge renames the run away before
  deleting it, so concurrent purges and a late write from a dead task cannot corrupt or resurrect
  a run.
- Logs contain: job text, diffs, model probabilities, decisions, and command evidence output. They
  do **not** contain: the API key, `AGENTS.md` contents, untracked file contents, or full model
  prompts.
- A diff is a copy of source code. The state directory is as sensitive as the repository, and the
  docs say so in the README rather than hiding it.
- Everything aftman creates here is `0700` (directories) or `0600` (files, including
  `daemon.log` and mission files), on every path, and a directory the user made is not chmodded.
  The daemon log is rotated at 4 MiB.
- Anything printed to a terminal is sanitised first (escape sequences, control characters, bidi
  overrides removed), because worker output, model reasons and job text are untrusted text and a
  terminal obeys what it is sent ([07-surfaces.md section 4](07-surfaces.md)).
- `aftman purge <run-id>` deletes a run's directory; `aftman purge --all` clears the store. No
  soft-delete, no hidden backups. A run that is being driven, by this daemon or by another process
  holding its ownership claim, is never purged; `purge --all` skips it.

## 8. Supply chain

- Workers are external binaries. Aftman records the resolved path and version in `meta.json` and in
  `WorkerStarted`, so a run's outcome is attributable to a specific agent build.
- Aftman never auto-updates anything. `aftman doctor` reports versions; the operator upgrades.
- No telemetry, no crash reporting, no license check, no outbound connection other than the
  configured Jev base URL.
- Dependencies are pinned with a committed `Cargo.lock`; `cargo audit` runs in CI.
- `cargo deny` bans duplicate crate versions and unknown licenses.

## 9. Auditability

- Every Jev request records `x-typesafe-request-id`, the model, token usage, and the latency.
- Every decision records all proposals, all suppressions with reasons, and the selected directive.
- Every ensemble trigger records the `state_digest` and `prompt_hash` of each opinion requested.
- Every evidence run records the resolved `argv`, exit code, duration, and output excerpt.
- A human answer records the surface and the free text.

The result: for any run, "why did this happen?" is answerable from `events.jsonl` alone, offline,
without re-calling a model.

## 10. What aftman does not protect against

Stated plainly, because a security section that only lists strengths is marketing:

1. **A destructive worker.** Aftman does not sandbox, snapshot, or roll back your repository. Run
   workers in a container, on a throwaway checkout, or with an agent in a restricted permission
   mode. `git` is your undo.
2. **A malicious repository.** Cloned code can contain an `AGENTS.md` designed to manipulate a
   coding agent, and the agent may act on it. Aftman can *notice* drift
   (`agents_md_drift`) and *stop* the run, but the agent already had your shell.
3. **A compromised worker binary.** Aftman trusts `pi`, `agy`, `claude`, and anything on `PATH`.
   Verified installs are the operator's job.
4. **Multi-user isolation.** Aftman has no user model. It is a single-user tool. Two operators
   sharing a daemon share the key and each other's run history.
5. **A wrong judgment.** Calibration is not correctness. A confident `Finish` on broken code is the
   failure mode this whole system is built to reduce, not to eliminate.
6. **A hostile local process.** Anything running as the user can talk to the socket and read the
   state directory.

## 11. Operator checklist

```bash
aftman doctor                      # config, key presence, versions, permissions
stat -c '%a %n' "${XDG_RUNTIME_DIR:-$HOME/.aftman}/aftman/aftman.sock"
ls -ld ~/.local/state/aftman
```

- [ ] `TYPESAFE_API_KEY` is in the environment or a `0600` `.env`, not in the repo.
- [ ] `.env` and the state directory are in `.gitignore` (aftman ships a `.gitignore` entry).
- [ ] Remote access goes through `tailscale serve`, not a LAN bind and not funnel.
- [ ] If a LAN bind is genuinely needed, `auth = "token"` is set and the port is firewalled.
- [ ] Workers run in a container or throwaway checkout for untrusted jobs.
- [ ] `AFTMAN_ACK_UNSANDBOXED` is set only where you mean it.
