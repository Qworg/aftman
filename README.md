# aftman

A software-factory foreman. `aftman` puts a fast, **calibrated decision model** (TypeSafe's Jev)
above slower coding agents (**pi**, **agy**, **claude**) and supervises their work against the
original job.

Give it a ticket, a specification, a bug report, or any free-form software job. A coding agent
does the engineering. Jev independently assesses completion, requirement satisfaction, test
sufficiency, worker health, and drift — and asks for a human when it should. Deterministic code,
not the model, decides what happens next.

```text
                        SOFTWARE FACTORY
   pi / agy / claude          pi / agy / claude            tests
        worker                      worker                   │
           │                           │                     │
           └───────────────┬───────────┴─────────────────────┘
                           │  bounded factory evidence
                           ▼
                        AFTMAN
                          Jev  (System One)
                           │  probabilities, one parallel request
                           ▼
                 responsibilities
                   completion        implementation_complete  .91
                                     requirements_satisfied   .79
                                     ready_to_finish          .21
                   verification      tests_sufficient         .34
                                     needs_verification       .82
                   worker health     worker_stuck             .02
                                     work_off_track           .06
                                     meaningful_progress      .94
                   human escalation  needs_human              .01
                           │
                           ▼
                    continue / steer / stop / retry / verify / finish
```

**Generative models work. Aftman watches the work.**

## Status

**Milestones M0-M3 are built.** There is a runtime that supervises a coding agent against a job
(deterministic policy over a decision model's probabilities, with an offline simulated model and
worker for tests and `aftman demo`), a file-backed append-only run store, a daemon that serves a
Unix-socket protocol, and a command-line client and terminal UI for it. The HTTP/web surface (M4)
and the herdr integration (M6) are not built; the docs describe them as design.

```bash
cargo build --release                     # the binary is target/release/aftman
aftman demo                               # simulated worker and model, no network, no credentials
aftman run --repo . --job "Add retries"   # starts the daemon on first use; needs a provider,
                                          #   or add --simulate-model to run offline
aftman run --repo . --job "..." --no-daemon   # embed the runtime in this process (CI, scripts)
aftman ls | show <run> | logs <run> -f | decisions <run>
aftman steer <run> "..." | cancel <run> | answer <run> <escalation> --accept
aftman watch <run>                        # terminal UI
aftman status | stop | doctor
```

Exit codes: `0` finished, `1` failed or cancelled, `2` configuration or usage error, `3` a person
must look (an escalation). The full command surface, flags and behaviour are in
[docs/07-surfaces.md](docs/07-surfaces.md). A daemon keeps the environment it was started with;
see there before relying on a key exported in your shell.

- **New here?** Read the [visual explainer](explainer/index.html) first — one self-contained HTML
  page, no build step, no network. Open the file directly, or serve it over a tailnet. It is a
  narrative rendering of the docs below; where the two disagree, the docs win.
- **Working on the code?** The design is written down before the code, and the docs are the
  contract:

| Doc | What it covers |
| --- | --- |
| [docs/01-hld.md](docs/01-hld.md) | Goals, non-goals, components, the two loops, surfaces |
| [docs/02-lld.md](docs/02-lld.md) | Crates, traits, types, state machine, events, protocol |
| [docs/03-jev.md](docs/03-jev.md) | The System One wire contract, budgeting, errors, usage |
| [docs/04-workers.md](docs/04-workers.md) | `Worker` trait, pi/agy/claude/herdr backends, steering |
| [docs/05-responsibilities.md](docs/05-responsibilities.md) | Checks, TOML definitions, routing, policy |
| [docs/06-ensemble.md](docs/06-ensemble.md) | The ensemble / second-opinion path |
| [docs/07-surfaces.md](docs/07-surfaces.md) | Standalone CLI, herdr, web + Tailscale surfaces |
| [docs/08-security.md](docs/08-security.md) | Credentials, subprocess hygiene, web auth |
| [docs/09-plan.md](docs/09-plan.md) | Milestones and work breakdown |

Start with [docs/README.md](docs/README.md).

## Design commitments

1. **Jev supervises; it never decides.** The model returns probabilities. Rust returns directives.
   Every action depends on non-semantic invariants (attempts left, has a verifier run, are finish
   thresholds jointly satisfied) that belong in deterministic policy.
2. **One protocol, three surfaces.** A daemon owns runs and emits one event stream. The CLI, herdr,
   and the web UI are all clients of the same protocol over different transports. herdr is never
   required.
3. **The supervisor key never reaches a worker.** `TYPESAFE_*` is stripped from every worker
   subprocess environment.
4. **Bounded observation.** Aftman never dumps a repository into a model. Every field has a byte
   cap and a byte budget shrinks the request before it is sent.
5. **Append-only, replayable runs.** `events.jsonl` is the source of truth. A run can be replayed
   offline, including every model call and every second opinion.
6. **Offline tests.** The test suite never touches the network and never invokes a real agent
   binary. Simulated workers and a recorded decision model cover the runtime.

## Credits

Aftman is an independent Rust implementation inspired by
[thruwire/foreman](https://github.com/thruwire/foreman) (MIT). The responsibility decomposition,
the observation boundaries, and the control-plane discipline are derived from that project. The
worker backends, the surfaces, and the ensemble path are aftman's own.

License: MIT.
