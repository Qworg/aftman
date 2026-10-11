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

- Rust 2024 edition. `cargo fmt` before every commit. `cargo clippy --all-targets -- -D warnings`.
- Errors: `thiserror` in libraries, `anyhow` only in the binary/crate boundaries.
- Async: `tokio`. Nothing blocking in an async task; use `spawn_blocking` for filesystem walks.
- Time: always `tokio::time::Instant` for elapsed measurement. Wall-clock stamps come from one
  injected clock so tests can control them.
- IDs: `uuid::Uuid` v7 (time-ordered) for runs and workers. Sequence numbers for events.
- Serialization: `serde` with `#[serde(deny_unknown_fields)]` on inputs, `extra` tolerated on
  model responses (Jev adds answer types over time — ignore, don't fail).
- No `.unwrap()` on I/O in library crates.

## Commands

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test --workspace            # offline; must pass with no env vars set
cargo run -p aftman-cli -- run --repo ./some-repo --job "..." --dry-run
```

## Commit style

`area: imperative summary` — e.g. `jev: honor retry-after-ms on 429`, `policy: refuse finish
without a completed verifier`. One behavior change per commit. Reference the doc section you
changed.
