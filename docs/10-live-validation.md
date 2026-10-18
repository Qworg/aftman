# 10 — Live validation runbook

Everything aftman does is tested against fixtures and simulations, and that is deliberate
(`AGENTS.md` hard rule 4). But it leaves exactly two seams that no test can close:

- **The decision model has never been asked a real question.** `AFTMAN_LIVE=1` tests exist and have
  never run. The architecture's central hypothesis — that Jev is fast enough to supervise in flight
  — is still a hypothesis, and the risk register says so.
- **The agent CLIs have never actually been driven.** Their argv is tested against `--help`, and
  their *output* parsing is fixture-driven, but "the recording says the decoder is right" is not
  "the installed CLI still emits that".

This runbook exercises both, in the order that fails cheapest first, and records the evidence.
Everything except steps 1 and 2 is free.

## What you need

- A Jev credential. `aftman doctor` reports whether one is configured and where from — never the
  value, and never a prefix of it.
- The agent CLI(s) you intend to supervise, installed and already logged in. `doctor` reports
  versions; a CLI that is installed but not authenticated will pass step 2's version check and fail
  the probe, which is the correct outcome.
- **A repository and a job you do not mind an agent modifying.** It will modify the working tree. Use
  a throwaway clone or a scratch branch, not your only copy.
- About 15 minutes and a little token budget.

## Step 0 — preflight (free)

```bash
aftman doctor --repo /path/to/repo
```

Pass = it ends with `ready for a live run` and exits 0. It checks, in one pass:

| Check | Why |
| --- | --- |
| provider resolved | A run without one refuses to start rather than silently simulating |
| each installed CLI's `--version` | You need to know *which* build is about to be driven |
| each CLI's `--help` still lists every flag aftman passes | Flag drift is the risk-register entry. The list is derived from the argv the backend actually builds, so it cannot disagree with what a run passes |
| the path is a git working tree | aftman reads the repository in-process; a bare repo or a plain directory cannot be observed, and `Invariant::UnobservedRepository` means such a run will never finish |

`DRIFT: --help does not mention …` means the CLI moved. Fix
`crates/aftman-workers/src/backends.rs`, re-record the backend's output into
`crates/aftman-workers/tests/fixtures/`, and run the suite — do not run a live job against a CLI
aftman no longer knows how to drive.

## Step 1 — the decision model (one billable question)

```bash
aftman doctor --probe
```

Reports reachable, the answering model, and **latency**. Record the latency: it is the first real
measurement of the hypothesis the whole architecture rests on. If it is slower than the debounce
interval, that is a finding about the design, not a bug to work around.

## Step 2 — each agent CLI (one trivial turn each; costs the agent's tokens)

```bash
aftman doctor --probe-workers
```

Runs each installed CLI once with the mission `Reply with exactly: ok`, decodes stdout with the
same `StreamDecoder` a supervised run uses, and reports whether the stream parsed.

- `stream parsed` — the installed CLI still speaks the format aftman decodes. This is the check
  fixtures cannot make.
- `no stream on stdout` — the CLI refused the turn (not logged in, no quota, wrong account). Not
  drift. The first line of its stderr is printed.
- `DRIFT: N line(s), M unparsed` — it produced output aftman cannot read. Same treatment as step 0.

## Step 3 — a real supervised run

```bash
aftman run \
  --repo /path/to/repo \
  --job "The smallest real change you can describe precisely." \
  --worker pi \
  --permissions sandboxed --external-sandbox \
  --effort medium
```

- `--worker pi|agy|claude|generic`. For `generic`, also pass `--cmd`.
- `--permissions sandboxed` is the default and the only posture that is honest without a human
  attached, except in a herdr pane. `auto-approve` means what it says.
- `--external-sandbox` asserts that isolation comes from a container, VM, or throwaway checkout.
  **For `pi` this is what makes `sandboxed` true rather than aspirational** — without it a
  sandboxed `pi` run is refused at startup, which is the fail-closed behaviour working.
- `--simulate-model` runs the *worker* half offline with a fake decision model. Use it if you want
  to exercise a backend without spending Jev tokens; it does not exercise Jev.
- `--pass-env KEY` forwards one variable from your shell to the worker (repeatable). A small default
  set is always forwarded; anything else is not, and the supervisor's own credential never is.

Watch it: `aftman watch <run>` (TUI) or `aftman logs -f <run>`.

## Step 4 — did it actually supervise?

```bash
aftman show <run-id>
aftman decisions <run-id>
```

`decisions` is the point of the whole system: every proposal, every suppression, every invariant, and
why the selected directive won. Read it, do not just check the exit status. Specifically:

- **Every check value appears with its threshold and margin.** A check that never fired and a check
  that was never measured are different things, and the output should let you tell them apart.
- **The selected directive follows from something.** If `Finish` was chosen, the completion and
  verification checks should show it.
- **No invariant did the real work.** A run that finished under `Invariant::NoWorkAttempted` or
  `Invariant::UnobservedRepository` did not finish — it was refused finish and something else ended
  the loop. That is a validation failure, not a pass.

## Step 5 — record the evidence

The point of the exercise is the recorded result, not the good feeling. Put it where the next person
will find it:

```bash
day=$(date -u +%F)
out=docs/live/$day
mkdir -p "$out"

aftman doctor --repo /path/to/repo > "$out/doctor.txt" 2>&1
aftman doctor --probe >> "$out/doctor.txt" 2>&1
aftman doctor --probe-workers >> "$out/doctor.txt" 2>&1
aftman show "$run" > "$out/show.txt" 2>&1
aftman decisions "$run" > "$out/decisions.txt" 2>&1
# The state directory is the one `aftman doctor` prints; the run's log is under it.
cp "<state-dir>/runs/$run/events.jsonl" "$out/" 2>/dev/null || true
```

Then, and this is the part that rots if skipped:

- update the version matrix in [04-workers.md](04-workers.md) §1 with the versions you verified;
- update the M8 row in [09-plan.md](09-plan.md) with what the run proved and what it found;
- if the latency was surprising, say so in [03-jev.md](03-jev.md) rather than leaving it in a
  terminal.

`docs/live/` is ordinary tracked evidence. If a bundle is large, keep `doctor.txt`, `show.txt` and
`decisions.txt` and drop `events.jsonl` — but say in the commit that you did.

## Pass criteria

This is definition-of-done item 2 (see [09-plan.md](09-plan.md)):

1. `aftman doctor --repo <real repo>` says ready.
2. `aftman doctor --probe` reaches Jev, and the latency is recorded.
3. `aftman doctor --probe-workers` reports a parsed stream for the backend you intend to use.
4. `aftman run --repo <real repo> --job <real job> --worker <real backend>` either finishes or stops
   correctly, with `events.jsonl` explaining every decision.
5. `aftman decisions` answers "why did it do that?" without re-running anything.

## Failure modes

| Symptom | Meaning | Do |
| --- | --- | --- |
| `doctor` ends `NOT READY` | One or more checks above failed | Fix the marked item; it lists all of them, not the first |
| `DRIFT: --help does not mention …` | The CLI changed its flags | Update `backends.rs`, re-record fixtures |
| `probe-workers` says `no stream on stdout` | The CLI refused the turn | Log in / check quota; this is not a format problem |
| `run` refuses to start, mentions a provider | No credential configured | Configure it, or pass `--simulate-model` for the worker-only check |
| `run` refuses a sandboxed `pi` | `pi` has no sandbox flag of its own | Add `--external-sandbox` only if isolation really is external |
| The run ends `Escalate` with no worker started | Nothing was attempted and no proposal arrived | Check `decisions`; this is fail-safe, not a crash |
| The run never finishes | Watch `decisions` for the invariant holding it | Often `UnobservedRepository` — see step 0 |

## Safety, briefly

The agent gets a real working tree and the supervisor's environment is scrubbed before it starts
([08-security.md](08-security.md) §2.2); `doctor` prints only whether a credential is present. The
repository is read in-process, so observing it cannot run a program the repository names — but the
*agent* certainly can run programs, which is what it is for. Use a checkout you are willing to lose.
