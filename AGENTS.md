# AGENTS.md — working in this repository

Aftman is an agent supervisor written in Rust. This file is for any coding agent working **on**
the repo (not on a job aftman is supervising).

## Read the design first

`docs/` is the contract. Before changing behavior, read:

- `docs/01-hld.md` — what the system is and why
- `docs/02-lld.md` — crates, traits, types, protocol
- the specific doc for the area you are touching

If code and docs disagree, that is a bug. Fix one of them in the same change. Update the doc when
you change a trait, a wire format, a config key, or a threshold.

## Hard rules

1. **Jev decides nothing.** Only `aftman-core` policy may turn check results into a directive.
   Never let a model output become an action without passing through policy.
2. **One module knows Jev.** All System One HTTP lives in `crates/aftman-jev`. No other crate may
   reference `systemone`, `TYPESAFE_*`, or the answer types.
3. **Workers never see supervisor credentials.** Any code that spawns a subprocess must build its
   environment through `worker_environment()`, which removes `TYPESAFE_*`.
4. **Tests are offline.** No network, no real `pi`/`agy`/`claude` binary, no credentials. Use
   `SimulatedWorker`, `SimulatedModel`, and recorded fixtures.
5. **Bounds are explicit.** Every field that reaches a model has a byte/char cap, and the request
   budget module must be able to shrink the request without dropping required evidence silently.
6. **herdr is optional.** Core crates must not depend on herdr, `HERDR_ENV`, or the herdr binary.
   Only `aftman-workers::herdr` and the surface layer may.

## Conventions

- Rust 2024 edition. `cargo fmt` before every commit. `cargo clippy --all-targets --all-features --
  -D warnings`.
- Errors: `thiserror` in libraries, `anyhow` only in the binary/crate boundaries.
- Async: `tokio`. Nothing blocking in an async task; use `spawn_blocking` for filesystem walks.
- Time: always `tokio::time::Instant` for elapsed measurement. Wall-clock stamps come from one
  injected clock so tests can control them.
- IDs: `uuid::Uuid` v7 (time-ordered) for runs and workers. Sequence numbers for events.
- Serialization: `serde` with `#[serde(deny_unknown_fields)]` on inputs, and additive change
  tolerated on model responses: fields and answers we did not ask for are ignored. A **requested**
  key answered with a type this client does not model is *not* additive — it is the provider renaming
  a primitive — and is fatal, because the alternative is a run that looks supervised while every
  check has no probability.
- No `.unwrap()` on I/O in library crates.

## Commands

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace            # offline; must pass with no env vars set
cargo run -p aftman-cli -- run --repo ./some-repo --job "..." --dry-run
```

These are the same three commands CI runs, with the same flags. Keep them in step: a check that
passes locally and fails in CI, or the reverse, is a bug in this file.

## Working in this repository — things that cost time

- **The suite must pass with an empty environment.** Run it as
  `env -i PATH="$PATH" HOME="$HOME" cargo test --workspace`. Quote `PATH`: on some machines it
  contains spaces, and an unquoted inline `env -i` splits it.
- **Every test that starts a daemon must stop it on every path**, including the failing ones (a
  `Drop` guard). Leftovers make unrelated tests time out and the failure looks like a regression.
  Check with `pgrep -fa 'aftman serve'` after a loop, and kill by pid: `pkill -f 'aftman ...'`
  matches the shell running it. After a rebuild `/proc/<pid>/exe` reads `(deleted)`, so match
  leftovers by pid or command line, never by exe path.
- **Unix socket paths are capped at ~103 bytes** (`sun_path`), less the daemon's staging prefix.
  Test sandboxes need sockets under short `/tmp/...` paths, not under a deep scratch directory.
- **Every writer of a run's events appends to the store before emitting to the hub, in `seq`
  order** (`docs/02-lld.md` §11). A writer that breaks this produces gaps no client can detect.
- **Untrusted text** — worker output, job text, repository content, event payloads, daemon errors —
  reaches the terminal only through `crates/aftman-cli/src/out.rs` and `sanitize.rs`.
- **The two hard rules that are easiest to break by accident.** Rule 2: no crate but `aftman-jev`
  may even *name* `TYPESAFE_*`; ask that crate for the text (for example `setup_hint()`). Rule 3:
  every subprocess the supervisor spawns goes through `worker_environment()`; the one exception is
  the daemon self-spawn in `crates/aftman-cli/src/autostart.rs`, listed in `docs/08-security.md`
  §2.2. Add to that list deliberately or not at all.
- **The repository observation is in-process `gix`, never a `git` child**
  (`crates/aftman-daemon/src/repo_observe.rs`). Do not reintroduce a CLI path or a fallback: the
  repository is worker-controlled, and a `git` subprocess runs clean/smudge/process filters,
  fsmonitor and hooks it names, with no switch to disable them
  (`docs/08-security.md` §3.2). The filter and diff-driver lists must stay empty, and
  `prepare_diff_command` / `prepare_binary_to_text_cmd` must never be called. The observation
  tests must not require the `git` binary on `PATH` — fixtures may be created with `git`, but the
  observation itself must run without it.
- **The advisory snapshot is pinned on purpose.** The `audit` job in
  `.github/workflows/test.yml` audits against the `advisories/` submodule with `--no-fetch`,
  so the gate is a function of this repository and a red result means something here
  changed. New advisories arrive through the weekly `audit-live` job, not by drifting the
  pin. Move it deliberately with `scripts/refresh-advisories.sh`, which prints what the
  refresh brings in. After cloning, `git submodule update --init` if you want the audit to
  work locally.
- **`unsafe` is forbidden workspace-wide** (that is why there is no double fork). `rust-version` is
  1.88 — the floor set by a `time` security fix, not a preference — which is also why the TUI uses
  ratatui 0.30 rather than 0.29.
- **How a milestone is run, if you are repeating it:** each stage implemented by one agent in its
  own git worktree; reviewed by a separate adversarial reviewer given the code and the contract but
  *not* the implementer's reasoning, who must prove findings with probes; fixed; then the
  integrated milestone reviewed against its exit criteria and the fixes verified once more. The
  final verification pass is not ceremony — it has caught a hardening fix that was incomplete.

## Commit style

`area: imperative summary` — e.g. `jev: honor retry-after-ms on 429`, `policy: refuse finish
without a completed verifier`. One behavior change per commit. Reference the doc section you
changed.
