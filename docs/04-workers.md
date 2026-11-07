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

| Backend | Selection | Transport | Live steering | Non-interactive permissions | Verified against |
| --- | --- | --- | --- | --- | --- |
| `pi` | `--worker pi` | `pi --print --mode json` (one-shot) or `pi --mode rpc` (steerable) | **Yes** (RPC), gated behind `doctor --probe-workers` — see §12 | no sandbox flag — see §8.1 | pi 1.3.0 (one-shot); RPC unrecorded |
| `agy` | `--worker agy` | `agy --print="…" --output-format stream-json` | No | `--sandbox` | agy 1.4.2 |
| `claude` | `--worker claude` | `claude -p --output-format stream-json` | No (resume only) | `--permission-mode` / `--permission-prompts` | claude 2.1.299 |
| `generic` | `--worker generic --cmd …` | operator-supplied argv template | No | operator's responsibility | — |
| `herdr-pane` | `--worker herdr:pane` | drives a herdr pane + recognized agent | **Yes** (`agent prompt`) | inherits the pane's agent | herdr 0.8.2 |
| `simulated` | tests / `aftman demo` | in-process | Yes (synthetic) | n/a | — |

`aftman doctor` reports each backend's availability and version. A backend whose binary is missing
is a startup error with the exact binary name and an install hint, not a mid-run surprise.

Versions verified on 2026-10-20 by `aftman doctor --probe-workers` and a supervised live run
(`docs/live/2026-10-20/`), one-shot modes only. `pi --mode rpc` has no recording and is verified
per machine by the probe instead (§12).
`aftman doctor` re-checks every flag these backends pass against the installed CLI's `--help`,
deriving the list from the argv the backend builds, so drift shows up before a run does.

**`pi`'s model matters as much as its version.** One model answered with `content: []` and
`"totalTokens": 0`: a well-formed stream containing no prose and no tool calls, so the worker "ran"
and did nothing while exiting 0. If a `pi` run produces no output, check the model before the
backend — `--model deepseek/deepseek-flash` was verified working against pi 1.3.0, and the
default model worked too on 2026-10-20, so it is the *model* rather than the version that
decides.

**The agent's own tooling can write to the tree.** `pi`'s extension leaves a
`.pi-lens-probe-home/` directory behind, so a job that says "change nothing else" can never
complete in an environment where that extension is installed — the tree really did change, and
the agent usually says so itself. This is a fact about the job, not a supervisor defect.

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
  activity, token usage, and the final assistant text
  (`aftman_workers::output::StreamDecoder`), and the log gets the agent's words and tool names
  rather than the envelopes; it does **not** treat any field as trusted evidence. See §7.
- The three structured backends agree on almost nothing. `pi` and `claude` envelope events under
  `type`; **`agy` uses `event`**. A decoder that assumed one key produces an empty activity
  summary for the other and reports a busy worker as quiet — a failure that looks like good news,
  which is why it is checked against recorded output rather than `--help` (§12 item 6).
- The decoded summary is recorded on the worker (`WorkerRecord::activity`) and reaches the model as
  `worker_activity`: tool-call and failure counts, the distinct tool names, tokens, cost, and the
  backend's own terminal status. The **counts** are what make a loop visible — a worker repeating one
  tool with identical results changes the count and nothing else — and they keep counting past the
  retention cap, because a long failing run is exactly the one whose count matters. The summary is
  published while the worker is still running, so a loop can be judged before the worker exits.
- `--mode json` reserves stdout for protocol records, so anything not JSON on stdout is a
  diagnostic and is logged, not parsed.

### 3.2 Steerable (RPC)

```bash
pi --mode rpc --no-session
```

**Gated** until `aftman doctor --probe-workers` has verified it on the machine: this shape has
no recorded fixture yet (§12).

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

**Ending a session.** An RPC session does not exit when its work is done; it waits for stdin to
close, which is pi's documented orderly shutdown. The stdout decoder watches for `agent_settled`
("Pi will not continue automatically") and the run then closes stdin, so the worker exits and is
reaped like a one-shot one. `agent_end` is deliberately not the signal: pi documents that a retry,
compaction, a steer or follow-up work can follow it. The session also ends when pi's `response`
to the mission's `prompt` is `success: false` or `disposition: "handled"`: pi documents that no
run follows either, so no `agent_settled` will come. That worker ends as a `StreamError` carrying
pi's reason (a bad `--model` is enough to cause it), never as `Completed`. A steer sent after the
session settled is `Rejected` ("the agent has settled"), not `Unsupported` — the backend is still
steerable. Before
this, nothing closed stdin and an RPC run could end only at `worker_timeout` or by a policy stop.

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
agy --print="<mission>" \
    --output-format stream-json \
    --model <model> \
    --effort <low|medium|high|xhigh|max> \
    [--sandbox] [--add-dir <dir>]
```

**The prompt must be attached to the flag.** Verified against `agy 1.3.3`: a bare `--print`
consumes the *next* token as its prompt, so the natural-looking
`agy --print --model x -- "<mission>"` fails immediately with

```text
Error: --print took "--model" as its prompt, so the intended prompt was left as an argument
```

The `--print="<mission>"` form may appear anywhere on the command line; flags may precede or follow
it. This exact failure was caught in design review, and a test asserts that the constructed argv
attaches the mission to `--print`.

Because the mission is a single argv element, it is bounded by the platform's per-argument limit
(128 KiB `MAX_ARG_STRLEN` on Linux). Aftman rejects a mission over 64 KiB rather than letting the
kernel truncate it or the launch fail opaquely.

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
    --permission-mode <acceptEdits|auto|bypassPermissions|manual|dontAsk|plan> \
    --permission-prompts <host|none> \
    --model <model> --effort <level> \
    [--append-system-prompt <text>] [--add-dir <dir>] \
    -- "<mission>"
```

`--permission-mode` and `--permission-prompts` are what make `claude` safe to run headless. Without
them, `-p` mode still routes tool approvals to a prompt that nobody can answer in a daemon, and
the worker hangs in a state indistinguishable from being stuck. Aftman sets
`--permission-mode acceptEdits --permission-prompts none` under the default `sandboxed` posture:
edits are accepted (the work is the point), and anything else that would prompt is **denied**
rather than blocking. See §8.1 for the full posture mapping.

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

The stdout schema of `stream-json` is verified against a recording
(`crates/aftman-workers/tests/fixtures/claude/print.jsonl`, claude 2.1.293): `system`,
`assistant`, `user`, `result`, with content blocks. `system` and `rate_limit_event` are harness
bookkeeping rather than agent activity and are deliberately not rendered, so a log does not read as
though the agent did something when the CLI was merely signing in. Claude reports
cumulative-input usage per turn rather than a run total, so the last turn's counts are kept and not
summed. There is no recorded fixture for a *failed* claude run, so that path is still only gated by
`aftman doctor --worker claude --probe`.

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
- no verifier runs before a code worker has: there is no work to verify, and a verifier is not
  an attempt at the job (`Invariant::NoWorkAttempted` counts code workers only), so it cannot
  lift that guard on a job no code worker touched (`a_run_never_finishes_on_a_verifier_alone`);
- the verifier runs at most once per state of the code: once a verifier has started, `Verify` is
  not proposed again (`Invariant::VerificationAlreadyRun`) until a later code worker has started
  (`counters.verified_since_code`). Then the verification rule may propose it again under its
  normal conditions, bounded by `max_workers`, so a FAIL, a fix and a PASS can finish. An
  earlier revision allowed one verifier per run: after a FAIL and a fix nothing proposed `Verify`
  again, and the run spent its workers and escalated
  (`after_a_failing_verdict_and_a_fix_the_verifier_runs_again_and_may_finish`). The one
  exception is `verify = "always"`, which refuses every `Finish` until a verifier *completes*:
  a verifier that did not complete may be retried once on the same code
  (`counters.verifiers_since_code`), and a retry that does not complete either is put to a
  person, with the question saying so
  (`a_verifier_that_never_completes_is_retried_once_and_the_run_says_why`), whose `Recheck`
  or `ClaimComplete` to that question authorises one more verifier and whose `Accept` does not
  (an answer to any other question authorises nothing)
  (`recheck_authorises_one_more_verifier_after_its_retry`);
- a verifier that modifies the repository is a policy violation: the runtime fingerprints the
  working tree before the verifier starts and again when it ends
  (`repo_observe::tree_fingerprint`), and a difference is recorded as a `Notification` with
  `code: "verifier_mutated_repo"` naming what changed. The verification is then inconclusive
  rather than a pass: the verifier has **not completed**, whatever it reported
  (`a_verifier_that_modifies_the_repository_is_inconclusive_not_a_pass`). And it is **not
  retried**: the run is put to a person at once (`Invariant::VerifierModifiedRepository`, whatever
  the `verify` policy), with a question naming what the verifier changed. A retry would start
  on the tree as the first verifier left it, so a clean retry would verify, and the run could
  finish over, an edit no code worker made and nobody approved
  (`a_verifiers_edit_is_never_finished_over_by_a_clean_retry`). `Stop` ends the run. Any other
  answer takes the tree as it now is, the verifier's changes included, as the work: a new state
  of the code, so the verifier counters reset as when a code worker starts, and it is verified
  again from the start before the run can finish (`policy::accept_verifier_edit`, `Notification`
  `verifier_edit_accepted`). To reject the changes, revert them before answering. Only an answer
  to that question does this (`Escalation::verifier_edit`). A tree that could not be
  fingerprinted, before or after, also leaves the verifier not completed, recorded as
  `verifier_tree_unverified` with the reason — "nothing changed" is not concluded from "nothing
  could be seen", as for `Invariant::UnobservedRepository` — but it is not known to have been
  written, so it is retried like any verifier that did not complete. Under simulation the repository is
  never read and no fingerprint is taken.

  The fingerprint covers what the observation does — `HEAD` and its branch, the index's entries
  (path, object id, mode, stage and the `assume-unchanged` / `skip-worktree` / `intent-to-add`
  flags; not their stat data, which a verifier's `git status` refreshes, and which a verifier
  could forge), every tracked path, every untracked path that is not ignored, and each path
  inside an untracked directory the walk reports whole (a nested repository) that the
  repository's ignore rules do not exclude, leaving out that repository's own `.git` — and
  records each of those paths' **own metadata from the worktree**, including its change time,
  which a process cannot set back. So any write to a tracked or untracked file is a change,
  whether or not it changed the bytes and whatever the index says: a further edit to a file that
  was already modified, an edit to a clean file whose index entry was rewritten to match, or to
  one flagged `assume-unchanged` / `skip-worktree`, which the walk never compares. A directory is
  compared by its type, mode and identity only, since its times move whenever anything inside it
  is created, ignored output included; its contents are compared entry by entry. The status walk
  itself also runs with **every** stat field on, whatever the repository's `core.trustctime` or
  `core.checkStat` say. A path that cannot be examined — too long, or in a directory that cannot
  be listed — makes the fingerprint fail rather than be skipped, since it would read the same
  before and after any write. What decides how the tree is read is part of
  the fingerprint as well: the resolved configuration (every file it came from, includes and
  `config.worktree` among them) and `.git/info/exclude` / `info/attributes`, so a verifier that
  adds an exclude pattern or turns a setting off has changed the tree as judged. Not covered:
  paths already ignored when the verifier starts (a verifier's build output belongs there;
  ignores come from the committed `.gitignore` and `info/exclude`), refs other than `HEAD`, and
  submodules. Anything that writes the tree while the verifier runs counts, including a command
  provider run in the repository meanwhile. **Caches that ignore themselves are not ignored
  here:** pytest's `.pytest_cache/`, and mypy's and ruff's caches, mark themselves ignored with an
  untracked `.gitignore` of their own, which `git status` honours and the fingerprint does not —
  an uncommitted ignore file is a place a verifier could hide a write. In a repository whose
  committed `.gitignore` does not list them, every verifier that runs such a tool is
  inconclusive; commit the ignore line (`.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`), as
  GitHub's Python template does.

  The comparison is made when a verifier ends on its own (it exited, failed or timed out) **and
  when it is stopped**, cancelled or superseded, after the reap: a stopped verifier has not
  completed, but what it wrote is still in the tree a retry would start on
  (`a_stopped_verifiers_edit_is_put_to_a_person_too`); a `Retry` or `Verify` that supersedes such
  a verifier starts nothing that cycle, so the edit is put to a person before any worker can
  start over it. A verifier killed mid-run may leave files it would have cleaned up (a lock, a
  temporary file in the tree); those count as its edit, and the run is put to a person rather
  than retried — fail-closed, by design. An accepted edit is verified as under
  `verify = "always"` whatever the policy (`Counters::verification_owed`,
  `policy::requires_verifier`), so the question's promise holds under `when_asked` too. A helper process a verifier
  leaves running cannot write after the comparison: the worker's process group is ended when the
  worker exits (§ termination below). The fingerprint is the same in-process read as an observation, under the
  same bounds: one of the run's observation slots and `OBSERVATION_TIMEOUT`; an interrupted walk,
  or one recording more than 200 000 paths beyond the index's own, is refused rather than
  compared partially
  ([08-security.md §3.2](08-security.md)), as is an index of more than 2 000 000 entries;
- when the backend supports `--json-schema`, the verifier is asked for
  `{"verdict": "pass"|"fail", "evidence": [...], "unevaluated": [...], "confidence": 0..1}` and
  that answer becomes an `EvidenceResult`, not a probability fed to Jev. Jev still decides whether
  the *run* is finished.

A verifier that exits successfully and produces output that is not only whitespace has
**completed** (`counters.verifier_completed`, until a code worker starts again): that satisfies the
`verification_completed` disjunct of the `Finish` precondition, and nothing more. Completed is not
passed — the verdict reaches policy only through the `Finish` conjuncts, scored over the
verifier's report ([05-responsibilities.md §6.2](05-responsibilities.md)).

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

### 8.1 Per-backend permission mapping

There is no single `--sandbox` flag the three CLIs share. `mode` is a *posture* that each backend
maps onto its own flags, and where a backend cannot express the posture, aftman says so rather than
pretending:

| Mode | `pi` | `agy` | `claude` |
| --- | --- | --- | --- |
| `sandboxed` (**default**) | *not expressible by a flag* | `--sandbox` | `--permission-mode acceptEdits --permission-prompts none` |
| `auto-approve` | pi's modes are already non-interactive | `--dangerously-skip-permissions` | `--permission-mode bypassPermissions` |
| `interactive` | no flag | no flag | no flag |

`pi` has neither a sandbox flag nor a tool-permission flag. Its `--approve` / `--no-approve` govern
trust of *project-local files* (AGENTS.md, extensions, skills), not tool permissions. So `sandboxed`
cannot be honoured for `pi` by a CLI flag, and aftman does not silently degrade:

- with `mode = "sandboxed"` and `--worker pi`, startup **fails** with guidance, unless the operator
  either sets `[workers.pi] sandbox = "container"` to assert that isolation is provided externally
  (container, VM, or throwaway checkout), or explicitly moves to `mode = "auto-approve"` and sets
  `AFTMAN_ACK_UNSANDBOXED=1`;
- this is the honest failure mode. Passing a flag `pi` does not have would crash on launch; passing
  nothing would leave a headless daemon with an unpinned permission posture and no record of it.

### 8.2 Project-local agent configuration is not trusted by default

A cloned repository can carry executable agent configuration: `pi` extensions and skills, `claude`
project settings, `agy` projects. Aftman does not load it:

- `pi` runs with `--no-approve`;
- `agy` and `claude` are not passed `--add-dir` for paths outside the repository, and any
  project-config trust flag is left off.

`[workers] trust_project_config = true` opts back in, is recorded in `meta.json`, and prints a
warning. This addresses the same threat as [08-security.md §10](08-security.md) item 2 (a malicious
repository) one step earlier: instead of noticing drift after the agent has acted, the agent's
project-local extensions are never loaded.

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
**its working directory set to the repository** (like every other backend; `{repo}` is the same
path, for tools that want it explicitly), with the scrubbed environment, in a fresh process
group, with the same streaming and termination rules. The mission file is created `0600` in a
`0700` directory and removed when the worker ends (see section 11); it never outlives the run.

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
- **The group is ended whenever the worker exits**, not only when it is terminated: a helper it
  started and left running (a dev server, `(…) >/dev/null &`) is part of the worker, and the
  worker is over. Without this, such a helper wrote to the repository after the worker ended —
  after the supervisor compared a verifier's tree (§6). The group is killed **before** the
  leader is reaped: once the leader is reaped and no member is left, its pid (the group id) can
  be reused, and the kill could reach an unrelated process group. On Linux the exit is awaited
  with `waitid(WNOWAIT)`, which leaves the leader an unreaped zombie whose pid cannot be reused;
  elsewhere the kill follows the reap immediately, a window of two system calls that would need
  the pid space to wrap within it (`subprocess::end_group_on_exit`). Every other signal to the
  group — `terminate`'s interrupt and its kill after `terminate_grace`, the drop guard's — is sent
  only while the group is still live (`signal_live`), and `run` retires the group under the same
  lock before it reaps: a worker that exits on the interrupt during the grace period is not
  followed by a kill to an id that may since have been reused. Output a helper had not yet
  written when the group is ended is lost; what an agent reports, its leader writes. A process
  that leaves the group with `setsid` escapes all of this, as it escapes termination, and the
  output drain gives up on it after `DRAIN_GRACE` rather than signal anything
  ([08-security.md §3](08-security.md)).
- `TerminateReason::Timeout | Cancelled | Stopped | Superseded` is recorded on the worker record
  and in a `WorkerTerminated` event with the number of signals sent and the elapsed grace.
- Termination is idempotent; calling it twice is not an error.
- **Every worker runs with its working directory set to the run's repository** (the daemon's own
  working directory is whatever a client happened to start it from and is never inherited), and
  the repository path the daemon accepts must be absolute.
- **The mission file** (`generic`) is created `0600` in a `0700` directory and removed when the
  worker ends by any route: completion, failure, timeout, termination, the `run` future being
  dropped, a failed launch.
- **Orphans.** A worker is its own process group, so it survives a supervisor that dies abruptly
  (`kill -9` of the daemon). When a subprocess worker starts, its process identity is written to
  `runs/<id>/processes.json` (process-group id, the leader's start time, the boot id, the
  mission-file path) and cleared when the worker is reaped. Restart recovery, for a run whose
  claim it holds, reads those records and ends each group (`SIGINT`, 5 s grace, `SIGKILL`) **only
  if it positively identifies the process**: the process now at that pid must have the recorded
  start time and the recorded process group, on the same boot. It then records `WORKER_TERMINATED`
  with cause `orphaned` and result `killed`, `already_gone` or `unverified`, removes the record and
  the mission file, and lists the outcome in the recovery report. Anything it cannot identify is
  **not signalled**, and the event/report say so.
  - Linux: start time is `/proc/<pid>/stat` field 22, the group is field 5, the boot id is
    `/proc/sys/kernel/random/boot_id`.
  - macOS and other Unix: no `/proc`; the start token is `ps -o lstart=` and the group is
    `ps -o pgid=` (second resolution, no boot id), shelling out to `ps`. This path is not
    exercised by the test suite.
  - Limits that cannot be removed: if the group leader has already exited while other members
    live on, those members cannot be told from an unrelated new group and are reported
    `unverified`; a worker spawned and killed between `spawn` and the record being written (a few
    microseconds to milliseconds) is not recorded; a crash of the *whole machine* needs no cleanup.
  - The same primitive serves the embedded (`--no-daemon`) path through `mark_run_failed`.
- On daemon shutdown, every live worker is terminated with the same path, and the run is
  persisted as `Cancelled` with a `daemon shutdown` reason. A run that does not stop within
  the shutdown grace is aborted: dropping the run's task drops its worker handle, which aborts
  the worker task (the backend then kills its process group when its `run` future is dropped),
  and the run is recorded `Failed` (`run task aborted`) with its worker records closed out. The
  same holds when a run task panics or errors: no way of ending a run task leaves a worker
  alive.

## 12. Adding a backend

1. Implement `Worker` in `crates/aftman-workers/src/<name>.rs`.
2. Return `supports_steering` accurately. If you cannot confirm delivery, return `Queued`.
3. Build the environment through `worker_environment()`; never `std::env::inherit`.
4. Stream both pipes concurrently; bound the log; never accumulate unbounded output.
5. Cover it offline with a fake executable: argv construction, streaming, launch failure, signal
   termination, and `steer()` while running and while finished.
6. Record real CLI output into `crates/aftman-workers/tests/fixtures/<name>/` and parse *that*.
   Hand-written fixtures for a third-party CLI format are how parsers rot. (Doc 09 M8 sequences
   this work; the red line here is that a backend without a recorded fixture stays gated behind
   `aftman doctor --probe-workers`.)
   *Gated today:* `pi --mode rpc`, the shape `--worker pi --steer` selects. `run` refuses it
   (`invalid_config`, naming the probe) unless `<state dir>/verified-streams.json` records a probe
   turn by this aftman of **the executable this run would start** — `pi` resolved against the
   run's own worker environment, which is the client's forwarded `PATH`, not the daemon's — and
   that executable still reports the recorded `--version`. The path is canonical (symlinks
   resolved), so the same `pi` reached through a version manager's per-shell symlink directory
   (fnm's `fnm_multishells/…/bin`) is one file from every shell. The run then starts it by that
   canonical path, so the file checked is the file run. The probe records a turn only if it
   decoded, said `agent_settled` without `aborted`, and exited 0 once its input closed
   (`crates/aftman-cli/src/verified.rs`). With no record, nothing is looked up or run. Run the
   probe from the shell you start runs from: another shell whose `PATH` finds another `pi` is
   refused. The record is per state directory; a daemon with a different state directory than the
   shell that ran `doctor` will not see it, which `doctor` already reports.
   *Recorded so far:* `pi/one-shot.jsonl` (with a tool call, and the `message_update` text-delta
   shape that a hand-written input had got wrong), `agy/print.jsonl`, `claude/print.jsonl`. A
   **failed** run per backend, and a `pi --mode rpc` session, are still unrecorded.
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
