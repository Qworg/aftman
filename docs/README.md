# aftman design documentation

Read in order the first time. Each doc stands alone after that.

| # | Doc | Read it when |
| --- | --- | --- |
| 01 | [High-level design](01-hld.md) | You want to know what aftman is, what it refuses to be, and how the pieces fit |
| 02 | [Low-level design](02-lld.md) | You are writing code: crates, traits, types, state machine, event schema, protocol |
| 03 | [Jev integration](03-jev.md) | You are touching the decision model, the request budget, or usage accounting |
| 04 | [Workers](04-workers.md) | You are adding or changing a coding-agent backend, or steering |
| 05 | [Responsibilities and policy](05-responsibilities.md) | You are changing checks, thresholds, routing, or directive arbitration |
| 06 | [Ensemble and second opinions](06-ensemble.md) | You are changing how aftman resolves a decision it is unsure about |
| 07 | [Surfaces](07-surfaces.md) | You are touching the CLI, the daemon protocol, herdr, or the web UI |
| 08 | [Security](08-security.md) | You are touching credentials, subprocess environments, or network exposure |
| 09 | [Plan](09-plan.md) | You want to know what gets built in what order |

## The one-paragraph version

Aftman supervises a coding agent. It runs a coding agent (`pi`, `agy`, `claude`, or a generic
command) against a repository and a free-form job, and concurrently — without stopping that agent
— it gathers bounded evidence (git state, diffs, untracked files, output tails, test summaries,
repository instructions) and asks **Jev**, a System One decision model, a set of narrow yes/no and
multiple-choice questions. Jev returns probabilities. Pluggable **responsibilities** turn those
probabilities into directive proposals. A deterministic **policy** validates the proposals against
hard invariants and selects exactly one: `continue`, `steer`, `stop`, `retry`, `verify`, `finish`,
or `escalate`. Everything is persisted as an append-only event log and is visible through three
interchangeable surfaces: a CLI, herdr, and a local web UI.

## The three invariants

If you remember nothing else about this system:

1. **The model estimates; code decides.** A probability is never an action.
2. **The supervisor's credential is invisible to the worker.** Always.
3. **Uncertainty escalates, never accelerates.** When aftman is unsure, it does the more
   conservative thing. See [06-ensemble.md](06-ensemble.md).
