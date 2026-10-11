# 04 — Workers

A worker is a coding agent that aftman supervises. Aftman never edits code; it starts an agent,
watches it, and intervenes.

All backends implement one small trait. There is no inheritance, no adapter hierarchy, and no
per-agent types leaking into the runtime.

```rust
#[async_trait]
pub trait Worker: Send + Sync {
    fn kind(&self) -> WorkerKind;
    fn supports_steering(&self) -> bool;
    fn artifact_ref(&self) -> Option<String> { None }

    async fn run(&self, ctx: WorkerCtx, emit: WorkerEventSink) -> Result<WorkerOutcome>;
    async fn steer(&self, message: &str) -> Result<SteerOutcome>;
    async fn terminate(&self, reason: TerminateReason) -> Result<()>;
}
```

## 1. Backends at a glance

| Backend | Selection | Transport | Live steering | Verified against |
| --- | --- | --- | --- | --- |
| `pi` | `--worker pi` | `pi --print --mode json` (one-shot) or `pi --mode rpc` (steerable) | **Yes** (RPC) | pi 1.1.0 |
| `agy` | `--worker agy` | `agy --print --output-format stream-json` | No | agy 1.3.3 |
| `claude` | `--worker claude` | `claude -p --output-format stream-json` | No (resume only) | claude 2.1.293 |
| `generic` | `--worker generic --cmd …` | operator-supplied argv template | No | — |
| `herdr-pane` | `--worker herdr:pane` | drives a herdr pane + recognized agent | **Yes** (`agent prompt`) | herdr 0.8.2 |
| `simulated` | tests / `aftman demo` | in-process | Yes (synthetic) | — |

`aftman doctor` reports each backend's availability and version. A backend whose binary is missing
is a startup error with the exact binary name and an install hint, not a mid-run surprise.

## 2. One-shot vs. steerable

The distinction that matters is whether the agent can accept mid-turn input.

- **Steerable** backends keep a live input channel. Policy may choose `Steer`, and the runtime
  delivers a message into the running turn.
- **Non-steerable** backends declare `supports_steering = false`. Policy *never attempts delivery*
  and degrades to `stop`/`retry`. This is not a limitation to be papered over: an honest `False`
  is better than a `Steer` that silently does nothing.

`aftman doctor` prints the matrix above for the configured backend so the operator knows what
supervision they actually have.

## 3. `pi`

Two modes, chosen automatically by whether steering is enabled for the run.

### 3.1 One-shot

```bash
pi --print --mode json --no-session -- <mission>
```

- `--mode json` writes **JSONL session events** on stdout (session header, `agent_start`,
  `message_update`, `tool_execution_*`, `turn_end`, `agent_end`, `agent_settled`).
- This is structured *progress*, not a constrained model output. Aftman parses it for tool
  activity, token usage, and the final assistant text; it does **not** treat any field as trusted
  evidence. See §7.
- `--mode json` reserves stdout for protocol records, so anything not JSON on stdout is a
  diagnostic and is logged, not parsed.

### 3.2 Steerable (RPC)

```bash
pi --mode rpc --no-session
```

Long-lived; stdin is a JSONL command channel, stdout is JSONL responses and events. Verified
commands relevant to supervision:

| Command | Purpose |
| --- | --- |
| `{"id":"1","type":"prompt","message":"<mission>"}` | Start the work |
| `{"type":"steer","message":"<msg>"}` | Queue guidance into the running agent |
| `{"type":"follow_up","message":"<msg>"}` | Queue work for after the current run |
| `{"type":"abort"}` | Stop the current turn |
| `{"type":"get_state"}` | Session id, model, working directory |
| `{"type":"get_messages"}` | Transcript for evidence |

Steering semantics, quoted from the RPC reference: a steer is *"delivered after the current
assistant turn finishes executing its tool calls, before the next LLM call."* The response is
`{"success":true,"data":{"disposition":"queued"}}`.

**Honesty rule:** `disposition: queued` means *accepted*, not *delivered*, and the reference
explicitly does not guarantee the message stays queued. Therefore:

```rust
SteerOutcome::Queued          // pi accepted it; delivery is not confirmed
SteerOutcome::Delivered       // only for backends that acknowledge delivery
SteerOutcome::Rejected(reason)
SteerOutcome::Unsupported
```

`SteerRecord` stores the outcome, the raw response, and the queue depth at send time. Policy may
steer the same worker at most `max_steers_per_worker` times (default 1), because a queue that
never drains is indistinguishable from a stuck worker.

`abort` is used for `TerminateReason::Timeout | Cancelled`; `terminate()` sends `abort`, waits
`terminate_grace`, then kills the process group.

### 3.3 Mission construction

The mission is assembled deterministically and stored verbatim:

```text
<repo preamble>
  You are working in the repository at <abs path>. All commands run there.
  Do not ask for confirmation; complete the work and run the tests.

<job>
  <the free-form job, verbatim>

<worker contract>
  You are being supervised. Report progress through normal tool use and output.
  Do not claim a test passed unless you ran it in this session.
```

The third block exists because the checks read worker output as evidence. Making the contract
explicit does not make output trustworthy (§7) but it makes an honest worker's output more useful.

## 4. `agy`

```bash
agy --print \
    --output-format stream-json \
    --model <model> \
    --effort <low|medium|high|xhigh|max> \
    [--sandbox] [--add-dir <dir>] \
    -- "<mission>"
```

- `stream-json` emits one JSON object per event; `json` emits a single final object. Aftman uses
  `stream-json` so the output tail exists while the worker is still running.
- `--print` runs a single non-interactive turn. `--print-timeout` bounds it; aftman sets this to
  `worker_timeout` so the CLI and the supervisor agree on the clock rather than fighting.
- `--json-schema <schema|file>` constrains the **final** result only, and is used by aftman for the
  *verifier* mission (§6), not for coding missions.
- `--conversation <id>` / `--continue` support resume, which is how a `Retry` reuses context when
  the operator enables it (`--resume-on-retry`). Default off: a retry with full context tends to
  repeat the same mistake.
- `--input-format stream-json` accepts NDJSON turns on stdin. It allows *follow-up* turns but does
  not deliver input mid-turn, so `supports_steering = false`.

## 5. `claude`

```bash
claude -p \
    --output-format stream-json --verbose \
    --model <model> --effort <level> \
    [--append-system-prompt <text>] [--add-dir <dir>] \
    -- "<mission>"
```

- `-p/--print` is non-interactive; `--output-format stream-json` emits NDJSON.
- `--json-schema <schema>` is available **only with `--print`** and is how the verifier mission is
  constrained (§6).
- `--resume <session-id>`, `--continue`, `--fork-session`, and `--session-id` provide session
  continuity. `--bg/--background` starts a detached session and returns an id that can be resumed
  later.
- `--no-session-persistence` is used for verifier missions, which must not pollute the coding
  session.
- No mid-turn input channel: `supports_steering = false`. A `Steer` degrades to
  stop-then-resume-with-guidance only if `--resume-on-retry` is enabled; otherwise stop/retry.

The exact stdout schema of `stream-json` is **not yet fixture-verified**. The parser must be built
against recorded real output (see §9) before this backend is considered supported, and until then
`aftman doctor --worker claude --probe` is required to enable it.

## 6. The verifier is a worker

Verification is not a special code path. It is the same `Worker` implementations running a
different, deterministic mission:

```text
You are an independent verifier. You did not write this code and must not trust
the previous agent's reports.

Repository: <path>
Original job: <job>

1. Inspect the working tree and the change (git status, git diff).
2. Determine what the job actually required.
3. Run the repository's own checks. Record the exact commands and their results.
4. Try to falsify the claim that the job is complete. Prefer a small failing case
   over a broad re-read.
5. Report: PASS or FAIL, the exact evidence, and any requirement you could not evaluate.

Do not modify the repository. Do not fix anything. Report only.
```

Rules:

- the verifier **must not** be the same backend as the coding worker; the runtime enforces this
  (configurable, and the config records the override);
- the verifier runs at most once per run;
- a verifier that modifies the repository is a policy violation: the runtime diffs the tree before
  and after and records `verifier_mutated_repo` as an event, and the verification is treated as
  inconclusive rather than as a pass;
- when the backend supports `--json-schema`, the verifier is asked for
  `{"verdict": "pass"|"fail", "evidence": [...], "unevaluated": [...], "confidence": 0..1}` and
  that answer becomes an `EvidenceResult`, not a probability fed to Jev. Jev still decides whether
  the *run* is finished.

That last point is the architectural rule: a verifier produces **evidence**. Jev produces
**probabilities**. Policy produces **actions**. A verifier verdict never short-circuits policy.

## 7. Worker output is not evidence of truth

Everything a worker writes, including `git diff`, `AGENTS.md`, and untracked files, is
model-reachable text and is therefore:

1. **bounded** (byte caps per field, budget compaction per request);
2. **labelled by source** — `worker_output`, `git_diff`, `untracked_file`, `command_evidence`,
   `repo_instructions` — so a check's instructions can say what it is looking at;
3. **never executed** as an instruction by aftman itself;
4. **cross-checked where possible**: a "tests passed" line in worker output is a claim; the same
   test run through a command evidence provider is a fact. This is why command evidence exists and
   why the *ensemble* path prefers re-derivation over opinion (doc 06).

## 8. Permissions: the honest version

Headless agents block on approval prompts. A blocked worker in a daemon looks exactly like a stuck
worker, so this must be decided up front, not discovered at runtime.

```toml
[workers.permissions]
mode = "sandboxed"     # sandboxed | auto-approve | interactive
```

| Mode | Behaviour | When to use |
| --- | --- | --- |
| `sandboxed` | Pass the backend's sandbox flag (e.g. `agy --sandbox`). No approval prompts. | **Default.** Least privilege that still completes work. |
| `auto-approve` | Pass the backend's skip-permissions flag. | Trusted repos, containers, throwaway checkouts. Requires `AFTMAN_ACK_UNSANDBOXED=1` and prints a warning on every run. |
| `interactive` | Do not pass any approval-suppressing flag. | **herdr surface only**, where a human can approve. |

On the herdr surface, `interactive` is genuinely useful: herdr classifies a waiting agent as
`blocked`, aftman turns that into an `EscalationRaised`, and the human answers in the pane. On the
daemon/headless surface there is no human, so `interactive` is refused at startup with a message
pointing at herdr.

Aftman does **not** sandbox by itself. It passes flags and documents this; it never claims to
contain a worker it did not contain.

## 9. `generic`

```bash
aftman run --repo . --job "..." --worker generic \
  --cmd 'mytool run --prompt {mission_file} --cwd {repo}'
```

Placeholders: `{mission_file}` (a temp file containing the mission — preferred over `{mission}`
so a long prompt never hits argv limits), `{mission}`, `{repo}`, `{model}`. The command runs with
the scrubbed environment, in a fresh process group, with the same streaming and termination rules.

This is the escape hatch for any agent aftman does not ship a backend for.

## 10. `herdr-pane`

The broadest integration: it works with **any** agent herdr recognizes, not just the three above.
herdr is optional and this backend is the only place herdr is referenced outside the surface layer.

Sequence, using only verified herdr commands:

```bash
herdr pane split --current --direction right --cwd "$REPO" --no-focus
#   → read .result.pane.pane_id
herdr agent start <unique-name> --kind <kind> --pane <pane-id>
#   → returns when herdr detects the agent and it is ready
herdr agent prompt <unique-name> "<mission>" --wait --timeout <ms>
herdr agent get <unique-name>        # working | idle | blocked | done | unknown
herdr agent read <unique-name> --source recent-unwrapped --lines 200
```

Mapping to the trait:

| Trait call | herdr operation |
| --- | --- |
| `run()` | split → `agent start` → `agent prompt --wait`; poll `agent get` for state; read output tails with `agent read` |
| `steer()` | `agent prompt <name> "<message>"`; reject if state is `blocked` |
| `terminate()` | `agent send-keys <name> ctrl+c`, then `esc`; close only panes aftman created |
| `artifact_ref()` | the agent name and pane id, so the CLI/web surface can deep-link |

Rules copied from the herdr contract, because violating them is user-hostile:

- require `HERDR_ENV=1`; otherwise `run()` refuses and the runtime falls back to the subprocess
  backend with a recorded reason;
- never create a workspace, tab, or worktree unless the operator asked for that topology;
- always `--no-focus`, so a supervising run never steals the human's focus;
- prefer a sibling pane in the current tab and the current working directory;
- never close a pane aftman did not create;
- treat `blocked` as an escalation, never as `stuck`. herdr's `unknown` does not prove completion
  and is never mapped to success.

Because the pane's agent is a live interactive session, this backend is **steerable** and is the
only backend where the `interactive` permission mode is allowed.

## 11. Termination

Identical for every backend:

```text
interrupt → wait terminate_grace (5s) → kill process group
```

- Unix: children are started with `process_group(0)`, so `kill(-pgid, SIGKILL)` reaches the whole
  tree. This matters: coding agents spawn shells, test runners, and language servers.
- `TerminateReason::Timeout | Cancelled | Stopped | Superseded` is recorded on the worker record
  and in a `WorkerTerminated` event with the number of signals sent and the elapsed grace.
- Termination is idempotent; calling it twice is not an error.
- On daemon shutdown, every live worker is terminated with the same path, and the run is
  persisted as `Cancelled` with a `daemon shutdown` reason.

## 12. Adding a backend

1. Implement `Worker` in `crates/aftman-workers/src/<name>.rs`.
2. Return `supports_steering` accurately. If you cannot confirm delivery, return `Queued`.
3. Build the environment through `worker_environment()`; never `std::env::inherit`.
4. Stream both pipes concurrently; bound the log; never accumulate unbounded output.
5. Cover it offline with a fake executable: argv construction, streaming, launch failure, signal
   termination, and `steer()` while running and while finished.
6. Record real CLI output into `crates/aftman-workers/tests/fixtures/<name>/` and parse *that*.
   Hand-written fixtures for a third-party CLI format are how parsers rot.
7. Add the backend row to the matrix in §1 with the version you verified against.

## 13. What is deliberately not supported

- **Multiple concurrent coding workers on one run.** The checks assume a single active worker.
  Concurrency is a future design, not a config flag.
- **Mid-turn steering of `agy` and `claude`.** Their CLIs have no input channel that lands inside
  a running turn. Aftman says `Unsupported` rather than pretending.
- **Attaching to an agent session aftman did not start**, other than via a herdr pane it created.
  Attached-session supervision through native hooks (upstream's `foreman hook`) is out of scope
  for v1; see [09-plan.md](09-plan.md).
- **A transcript-independent "the worker says it is done" fast path.** A worker's own claim is one
  input among many, never a terminal state.
