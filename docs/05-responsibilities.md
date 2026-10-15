# 05 — Responsibilities, evidence, and policy

A **responsibility** is one logical area of supervision. It owns checks (questions for Jev) and
rules that propose directives. It does not have a score of its own and does not act. All active
responsibilities match at once and their checks go to Jev in as few requests as their evidence
selections allow.

```text
responsibility ──owns──► checks ──► one Jev request (grouped by evidence)
                              │
                              ▼
                        check results
                              │
                              ▼
        responsibility ──proposes──► directives (with priority)
                              │
                              ▼
                        FactoryPolicy ──selects──► one directive
```

## 1. Bundled responsibilities

Ported from the upstream project with the same ids, check ids, and thresholds, so behaviour and
operator expectations carry over. Definitions are TOML compiled in with `include_str!` and
overridable from `$XDG_CONFIG_HOME/aftman/responsibilities/`.

| Responsibility | `always` | Checks | Threshold | Rule ids and directives (priority) |
| --- | --- | --- | --- | --- |
| `core.completion` | yes | `implementation_complete`, `requirements_satisfied`, `ready_to_finish` | 0.75 | `:retry-stopped` → `Retry` (800); `:thresholds-satisfied` → `Finish` (700); `:work-remains` → `StartWorker` (500) |
| `core.verification` | yes | `tests_sufficient`, `needs_verification` | 0.75 / 0.65 | `:warranted` → `Verify` (600) |
| `core.worker-health` | yes | `meaningful_progress`, `worker_stuck`, `work_off_track` | — / 0.80 / 0.80 | `:stuck`, `:off-track` → `Steer` or `Stop` (900) |
| `repository.instructions` | yes | `agents_md_drift` | 0.80 | `:drift` → `Steer` or `Stop` (900) |
| `core.human-escalation` | yes | `needs_human` | 0.80 | `:needs-human` → `Escalate` (1000) |
| `quality.documentation` | **no** | `documentation_sufficient` | 0.75 | `:insufficient` → `StartWorker` (750) |

**Every rule has a unique id, including the built-ins.** This is not cosmetic: the tie-break
ladder in §6.3 ends at the rule id, and two built-in worker-health proposals (`:stuck` and
`:off-track`) could otherwise be indistinguishable when both fire — the same responsibility, the
same priority, the same stakes — leaving the winner dependent on slice order and breaking replay.

`quality.documentation` is *conditional*: it participates only if the router decides the incoming
job requires documentation (routing threshold 0.70). Routing sees the job text, never the
repository.

### 1.1 Check definitions

```toml
# core.completion.toml
enabled = true
always  = true

[checks.implementation_complete]
instructions = """
Is the implementation work required by the original job complete?
"""
min_threshold = 0.75

[checks.requirements_satisfied]
instructions = """
Does the current repository satisfy the original free-form job as a whole?
"""
min_threshold = 0.75

[checks.ready_to_finish]
instructions = """
Given all evidence, is the factory job ready to be declared complete?
"""
min_threshold = 0.75
```

```toml
# core.verification.toml
enabled = true
always  = true

[checks.tests_sufficient]
instructions = """
Does the work have sufficient relevant test coverage and passing verification?
"""
min_threshold = 0.75

[checks.needs_verification]
instructions = """
Does the current state warrant an independent verification pass before finishing?
"""
min_threshold = 0.65
```

```toml
# core.worker-health.toml
enabled = true
always  = true

[checks.meaningful_progress]
instructions = """
Is the active or most recent worker making meaningful progress toward the job?
"""

[checks.worker_stuck]
instructions = """
Does the active or most recent worker appear stuck, looping, or unable to advance?
"""
min_threshold = 0.80

[checks.work_off_track]
instructions = """
Is the current work drifting from the original job or making unrelated changes?
"""
min_threshold = 0.80

[settings]
steering_enabled       = true
max_steers_per_worker  = 1
steering_grace_seconds = 30
```

```toml
# core.human-escalation.toml
enabled = true
always  = true

[checks.needs_human]
instructions = """
Does this situation require human judgment, credentials, clarification, or permission?
"""
min_threshold = 0.80
```

```toml
# repository.instructions.toml
enabled = true
always  = true

[checks.agents_md_drift]
instructions = """
When agents_md_instructions is present, is the active or most recent worker's behavior or
repository work materially inconsistent with those repository instructions? Answer no when no
AGENTS.md instructions are present or the evidence is insufficient.
"""
min_threshold = 0.80

[settings]
instruction_files = ["AGENTS.override.md", "AGENTS.md"]
```

```toml
# quality.documentation.toml
enabled = true
always  = false
routing_instructions = """
Does the incoming work explicitly require creating or updating README content, user guides,
API documentation, release notes, or other documentation delivered with the repository?
"""
routing_threshold = 0.70

[checks.documentation_sufficient]
instructions = """
When documentation is required by the original job, is the relevant documentation complete,
accurate, and consistent with the implemented behavior?
"""
min_threshold = 0.75
```

### 1.2 Why `agents_md_drift` is phrased that way

The last sentence is load-bearing. Without *"answer no when no AGENTS.md instructions are present
or the evidence is insufficient"*, a model with no instructions to compare against has no choice
but to emit an uninformative middling probability, which then sits near the 0.80 threshold and
produces spurious steering. Phrasing a check so that "not applicable" resolves decisively is a
general rule for this system: **every check must have a well-defined answer when its evidence is
absent.**

## 2. TOML schema

```toml
enabled = true            # present in the registry (default true)
always  = true            # participates without routing; false ⇒ routing_instructions required
routing_instructions = "" # required when always = false
routing_threshold = 0.70  # required when always = false

[settings]                # responsibility-specific, validated against a known key set
key = value

[checks.<check_id>]
instructions = "..."      # string | table | array  (structured instructions are supported)
min_threshold = 0.75      # optional; omitted ⇒ check cannot drive a threshold rule
primitive = "noul"        # noul (default) | choice | score
criteria = { ... }        # required for choice; ordered array for score
evidence = ["git_diff"]   # optional; empty ⇒ default provider set

[[rules]]                 # optional; advanced responsibilities
id = "stop-when-stuck"
when = "core.worker-health__worker_stuck >= 0.80"
propose = "stop"
stakes = "high"

[[rules]]
id = "verify-before-finish"
when = "core.verification__needs_verification >= 0.65 and core.completion__implementation_complete >= 0.75"
propose = "verify"
```

### 2.1 `when` grammar

Deliberately tiny, so that evaluation is total and unambiguous:

```text
condition  := disjunction
 disjunction := conjunction ("or" conjunction)*
 conjunction := term ("and" term)*
 term       := check_key comparator number
             | "not" "(" condition ")"
 comparator := ">=" | ">" | "<=" | "<"
 check_key  := "<responsibility_id>__<check_id>"      (the wire key, §2)
 number     := decimal literal
```

`and` binds tighter than `or`, evaluation is left-to-right, and **mixing `and` and `or` at the same
level without parentheses is a parse error** rather than a precedence subtlety. A reference to an
unknown check key is a load-time error. A condition term evaluates
`permitted(check) && comparator(p, t)`. `permitted` is direction-agnostic (`decisive &&
!contested`), so `<` and `>=` behave identically, and an ensemble-suppressed check cannot drive a
rule in either direction (doc 06 §5.2).

Validation rules (all fatal at load, never at assessment time):

- `min_threshold` ∈ [0,1]; a check without one may be referenced by an explicit `[[rules]]` but
  cannot participate in a declarative failure rule;
- `always = false` requires non-empty `routing_instructions`;
- `evidence` entries must be unique, non-empty, and must name a known provider;
- a `choice` check requires at least one criterion; a `score` check requires at least one level;
- `propose` must be one of the directive names; `stakes = "high"` opts the rule into
  [06-ensemble.md](06-ensemble.md);
- unknown keys anywhere are errors (`deny_unknown_fields`), so a typo fails loudly;
- check keys are `"{responsibility_id}__{check_id}"` on the wire.

## 3. Evidence providers

Evidence is gathered before an assessment and selected per check. A check receives either its
explicit `evidence` list or the default set.

| Provider | Content | Default |
| --- | --- | --- |
| `job` | The original job verbatim (capped) | always included |
| `factory_status` | Status, counters, elapsed, attempts | always included |
| `active_worker` | Kind, mission, model, elapsed, status, exit | included |
| `worker_output` | Head and tail of the bounded log, per stream | included |
| `worker_activity` | Decoded stream: tool-call and failure counts, distinct tool names (capped at 32), tokens, cost, the backend's own terminal status | included |
| `worker_history` | Summaries of ended workers | included |
| `steering_history` | Steers sent, outcomes, queue depth | included |
| `git_status` | `git status --short --untracked-files=all` | included |
| `git_diff` | Bounded diff, plus `--stat` | included |
| `changed_files` | Names only | included |
| `untracked_evidence` | Bounded excerpts of untracked text files | included |
| `repo_instructions` | `AGENTS.override.md` / `AGENTS.md`, transient | included |
| `test_summary` | Parsed test results from output and command evidence | included |
| `recent_events` | Last N event summaries | included |
| `prior_decision` | Previous assessment and intervention | included |
| `command:<id>` | Output of a configured command evidence provider | opt-in |

**Command evidence providers** are configured centrally and are the only way a claim becomes a
fact:

```toml
[[evidence.commands]]
id = "tests"
argv = ["pytest", "-q"]
cwd = "repo"                 # repo | temp
timeout_seconds = 600
max_output_bytes = 20000
run_when = "worker_completed"   # worker_completed | on_demand | before_finish
success_exit_codes = [0]
```

Rules:

- commands run with the scrubbed worker environment, in the repo, in their own process group, with
  a hard timeout and bounded output;
- an evidence command is never a worker instruction and never runs user-supplied shell
  interpolation: `argv` is a literal list, never a shell string;
- evidence results are persisted with exit code, duration, output excerpt, and the resolved argv,
  so a model's conclusion about a test run is auditable down to the command;
- `run_when = "before_finish"` means the check is re-evaluated with fresh evidence before `Finish`
  is allowed — the primary defense against a worker that reports tests it never ran.

## 4. Observation construction

Deterministic, bounded, and concurrent:

```rust
// Repository evidence: one deadline (8 s) for all of it, git as a killed-on-timeout child
// process, file reads on blocking threads and only of regular files, byte-capped while
// reading (docs/02-lld.md section 15). On timeout it is omitted and a note is recorded.
let (git, untracked, instructions) = collect_repo(&repo, &cfg, deadline).await;
let events = recent_events(&store, run_id, cfg).await;
```

Byte caps (defaults, all configurable): diff 20,000 chars; the most recent worker's output
head 4,000 chars and tail 12,000 chars (terminal control sequences stripped);
3 untracked files totalling 4,096 bytes; 30 events; 10 workers of history.

Untracked excerpts skip: binary-looking files (NUL in the first 4 KiB), unreadable files, symlinks,
paths escaping the repository, and names matching sensitive patterns (`.env`, `id_rsa`, `*secret*`,
`*token*`, `*.pem`, `*.key`). Truncation happens on raw byte boundaries before UTF-8 decoding, so
multibyte text cannot exceed the byte cap.

**Test summaries** are parsed from worker output independent of any structured event, because most
agents do not emit test events. The parser handles `passed`-first and failure-first orderings
(`1 failed, 3 passed`), `N error`/`N errors`, skipped counts, optional `=== … ===` wrappers, and a
trailing `in Ns`. Lines with no recognized counts (for example `2 warnings in 0.12s`) are not test
summaries. Parsed results are tagged `source: worker_output` and are treated as a **claim**; a
command evidence result is tagged `source: command` and treated as a **fact**. Checks can see the
tag and should be instructed to weight them differently.

## 5. Routing

Before the first worker starts:

1. global responsibilities are included directly;
2. every conditional responsibility is evaluated **together in one Jev request** against the job;
3. every responsibility whose `routing_probability >= routing_threshold` is activated;
4. the decision is persisted as `JOB_ROUTED` with each probability and threshold, so "why didn't
   the documentation check run?" is answerable.

Routing chooses *which checks participate*. It never chooses the directive. A routing failure
(model error) activates only the global responsibilities and records the degradation.

## 6. Arbitration

Policy is pure: no clock, no I/O, no randomness. Same input ⇒ same decision.

### 6.1 Hard invariants

Applied before any priority comparison. They cannot be overridden by a score:

| Invariant | Effect |
| --- | --- |
| Overall timeout exceeded | `Stop` |
| **`permitted == false` for a check** | Every rule that reads that check is suppressed (`SuppressionReason::NotPermitted`), in both comparison directions, regardless of how far the value is from the threshold. This is how the ensemble's guarantee is enforced (doc 06 §2.2). |
| `attempts >= max_attempts` | `Retry` suppressed |
| `workers >= max_workers` | `StartWorker` / `Verify` / `Retry` suppressed → `Escalate` |
| `verification_started` | `Verify` suppressed |
| Assessment budget exhausted | `Stop`, or `Escalate` when verification is pending |
| Model error this cycle | only conservative directives are permitted (see §7) |
| **Any active check unresolved** (`UnresolvedInBand`, `SuppressedByContest`, `Unresolved`) | `Finish` suppressed (`Invariant::UnresolvedUncertainty`). A high-priority inhibitor rule that cannot fire because its own measurement is uncertain must not silently disarm: uncertainty about a blocker blocks, it does not permit. |
| **No worker has ever run** | `Finish` suppressed (`Invariant::NoWorkAttempted`). A model that answers optimistically on the first cycle — before any work exists — would otherwise end a job that never started. Added during M2, after a test model did exactly that. |
| **No proposal survived and no worker is running** | `Escalate`, not `Continue`. `Continue` with nothing to continue with is a stall, and the run would spin on the idle ticker until the overall timeout. |

### 6.2 The `Finish` precondition

`Finish` is proposed only when **all** of these hold:

```text
ready_to_finish        >= 0.75  and  permitted
requirements_satisfied >= 0.75  and  permitted
tests_sufficient       >= 0.75  and  permitted
and ( verification_completed  or  ( needs_verification < 0.65  and  permitted ) )
and no worker is active
```

`permitted` is `true` only when the value is decisively outside the uncertainty band (or the
ensemble is disabled). So with `ensemble.enabled = true`, a `ready_to_finish` of `0.79` against a
`0.75` threshold is **in band** and cannot finish: the run either refreshes evidence (R0) or
continues. This is the mechanism, not the prose, behind "only evidence can finish work".

Note that the `needs_verification` disjunct — the one comparison that reads a *less-than* — carries
`permitted` like every other term. An earlier revision omitted it there, which let a raw, in-band,
or even actively contested `needs_verification` satisfy a `Finish` conjunct.

And the whole disjunction is additionally gated by the last row of §6.1: **if any active check is
unresolved for any reason, `Finish` is suppressed.** The precondition above names the checks
`Finish` directly depends on; that invariant covers every *other* active check, including
`documentation_sufficient`, whose `<` rule inhibits `Finish` by priority and would otherwise be
disarmed the moment its own measurement fell in the band.

Any single failure falls through to `StartWorker`, so an unmet requirement can never be silently
finished. This is the joint-threshold rule that keeps `ready_to_finish` from being trusted alone.

### 6.3 Priority order

Proposals carry a priority; the highest wins, with a stable tie-break:

```text
priority  directive      rule id                             source
1000      Escalate       core.human-escalation:needs-human    needs_human >= 0.80
 950      Escalate       runtime:budget-exhausted             assessment/iteration cap
 900      Stop           core.worker-health:stuck             worker_stuck >= 0.80
 900      Stop           core.worker-health:off-track         work_off_track >= 0.80
 900      Stop           repository.instructions:drift        agents_md_drift >= 0.80
 900      Steer          (the same three rules, when a steer is permitted)
 800      Retry          core.completion:retry-stopped         no worker, last directive was Stop
 800      Steer          (declarative rule with `propose = "steer"`)
 750      StartWorker    quality.documentation:insufficient   documentation_sufficient < 0.75
 700      Finish         core.completion:thresholds-satisfied §6.2 holds
 600      Verify         core.verification:warranted          needs_verification >= 0.65
 500      StartWorker    core.completion:work-remains         no worker, thresholds unmet
   0      Continue       runtime:default
```

Tie-break order: priority desc → stakes (High first) → responsibility id → **rule id**. Because
every rule — built-in included — has a unique id (§1), this is a total order with no ties, which is
what makes `arbitrate()` reproducible from the event log. `docs/09-plan.md` M2 requires a test that
asserts it: no two proposals in any built-in scenario compare equal.

### 6.4 Steering grace

After a steer is delivered to a worker, the worker-health and repository-instruction
responsibilities suppress further warnings for `steering_grace_seconds` (default 30) and propose
`Continue` at priority 900 instead. Without this, a supervisor that steers once will steer again on
the very next cycle, because the worker has not had time to change anything. A supervisor that
steers every five seconds is just noise in the worker's context window.

### 6.5 Steer or stop

Worker-health and instruction-drift produce a warning. Whether the warning is a `Steer` or a `Stop`
is decided deterministically:

```text
if steering_enabled
   and worker.supports_steering
   and worker.steer_count < max_steers_per_worker
   and within_grace == false
then Steer
else Stop
```

A non-steerable backend therefore degrades to stop/retry automatically, with the same thresholds.
This is why `supports_steering` must be honest (see [04-workers.md §2](04-workers.md)).

**This decision is pure, and that requires the inputs to be in `PolicyInput`.** `within_grace`
depends on the current time and on `worker.last_steered_at`; `steer_count` lives on the worker
record. Both `now` and `active_worker` are therefore fields of `PolicyInput`
([02-lld.md §5](02-lld.md)), materialised by the runtime before either `propose()` or `arbitrate()`
runs. An earlier revision asserted policy purity while leaving these inputs unreachable, which would
have forced an implementer to read the clock inside a function advertised as pure and would have
broken replay of the steering-grace path.

### 6.6 Recording

Every cycle persists `DirectiveProposed` for **all** proposals and `DirectiveSelected` for the
winner, including every suppression with its reason. The UI answers "why wasn't it stopped?" from
the log alone, without re-running anything.

## 7. When the model fails

A model error is not a reason to guess. The runtime builds a `PolicyInput` with an empty
assessment, and the policy permits only:

| Situation | Permitted directive |
| --- | --- |
| Worker still active, budget remains | `Continue` |
| Worker exited, budget remains | `Retry` **only if** a previous cycle established the work was incomplete; otherwise `Escalate` |
| Budget exhausted, or repeated model failure (≥3 consecutive) | `Escalate` with the error attached |
| Overall timeout exceeded | `Stop` |

`Escalate` here is a distinct reason (`model_unavailable`) so the UI can say *"supervision is
blind"* rather than *"the model thinks a human is needed"*. Those are very different messages and
conflating them is how a supervisor loses an operator's trust.

## 8. Extension points

- **New check in an existing responsibility**: add a `[checks.*]` block. No code.
- **New responsibility**: add a definition file with `always`/routing and checks. No code.
- **New evidence provider**: implement `EvidenceProvider` (in-tree) or configure a
  `[[evidence.commands]]` entry (no code).
- **New directive source**: needs code, and needs a priority. Adding a priority means editing the
  table in §6.3, which is intentional friction: directive sources are the part of this system where
  a mistake stops someone else's work.

## 9. Testing

- Every built-in responsibility has rule tests: below/at/above threshold, and the degenerate cases
  (no active worker, no AGENTS.md, no test output).
- The `Finish` precondition has a truth table test over all five conjuncts.
- Priority arbitration has a test per priority pair to pin the ordering, plus a test asserting the
  ordering is a total order (no two proposals compare equal).
- The steering-grace path is tested with `TestClock` so the 30-second window is exercised in
  microseconds.
- `state_for` has a byte-exact snapshot test per provider selection.
- The test-summary parser has a table of real agent output lines, including the lines that must
  *not* parse as a summary.
