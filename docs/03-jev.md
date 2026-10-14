# 03 — Jev integration (System One)

`crates/aftman-jev` is the **only** crate that may speak `systemone`, read `TYPESAFE_*`, or name
an answer type. Everything else depends on the `DecisionModel` trait and aftman's own types.

This document is the wire contract. It was derived by reading
`typesafe-sdk` 0.7.4 (`_core/endpoints.py`, `_core/question_types.py`,
`_core/response_types.py`, `_core/transport.py`, `_schemas/models.py`) and the provider docs. If
the provider changes, this doc and the client's fixtures change together.

## 1. Endpoint and authentication

```text
POST {base_url}/v1/systemone
GET  {base_url}/v1/models          # listing only; not an auth probe on all providers
```

| Setting | Env var | Default |
| --- | --- | --- |
| API key | `TYPESAFE_API_KEY` | — (required) |
| Base URL | `TYPESAFE_BASE_URL` | `https://api.typesafe.ai` |
| Model | `TYPESAFE_DEFAULT_MODEL` / `AFTMAN_JEV_MODEL` | `jev-latest` |

Provider selection is by base URL only. That is the whole of "or an equivalent model":

| Provider | Base URL | Notes |
| --- | --- | --- |
| TypeSafe direct | `https://api.typesafe.ai` | default |
| OpenRouter | `https://openrouter.ai/api` | maps `jev-latest` → `~typesafe/jev-latest`; key belongs to OpenRouter |
| Any compatible endpoint | operator-supplied | must implement `POST /v1/systemone` |

Set the matching key and URL together. Never include `/v1` or the full endpoint in the base URL.

**The base URL must be `https://…` or loopback.** Aftman refuses to start with a cleartext
non-loopback base URL, because the very next request would put `Authorization: Bearer
<TYPESAFE_API_KEY>` on the wire in the clear. `http://127.0.0.1:…` and `http://localhost:…` are
allowed for a local proxy. `aftman doctor` prints the resolved base URL scheme and host, so an
unexpected redirect is visible before the first assessment rather than after the first bill.

**Headers sent:**

```http
Authorization: Bearer <TYPESAFE_API_KEY>
Content-Type: application/json
Accept: application/json
User-Agent: aftman/<version>
X-TypeSafe-SDK: aftman/<version>
X-TypeSafe-Runtime: rust/<rustc-version> (<os>; <arch>)
X-TypeSafe-Retry-Count: <n>          # present only on retries
```

`X-TypeSafe-SDK` is set honestly to identify this client. Do not impersonate the Python SDK.

## 2. Request

```jsonc
{
  "state": { /* string | object | array */ },
  "model": "jev-latest",
  "questions": {
    "core.completion__implementation_complete": {
      "type": "noul",
      "instructions": "The repository now contains the complete implementation the job asked for."
    },
    "core.completion__ready_to_finish": {
      "type": "noul",
      "instructions": { "question": "Is the job finished?", "context": "..." }
    },
    "core.verification__next_step": {
      "type": "choice",
      "instructions": "What should supervision do next?",
      "criteria": {
        "continue": "The worker is advancing the job; let it continue.",
        "verify":   "Independent verification is warranted before finishing.",
        "stop":     "The worker is not advancing the job; stop it."
      }
    },
    "core.worker-health__progress_level": {
      "type": "score",
      "instructions": "How much meaningful progress has the active worker made?",
      "criteria": ["None", "Some", "Most", "Essentially all"]
    }
  }
}
```

Constraints enforced locally before sending:

- at least one question;
- `state` is always a JSON **object** when we build it (`state_for`), never a bare string;
- a `noul` has `instructions` or at least one non-null `criteria.{true,false}`;
- a `choice` has a non-empty `criteria` map;
- a `score` has at least one criterion;
- `instructions` and `criteria` values may be strings, objects, or arrays;
- `null` values are omitted, not sent.

`instructions` accepting structured JSON is important: it lets a check carry a small structured
rubric without turning it into prose.

## 3. Response

```jsonc
{
  "model": "jev-2025-xx-xx",
  "usage": { "input_tokens": 8123, "output_tokens": 96 },
  "answers": {
    "core.completion__implementation_complete": { "type": "noul",   "noul": 0.91 },
    "core.verification__next_step": {
      "type": "choice", "choice": "verify", "confidence": 0.77,
      "probabilities": { "continue": 0.12, "verify": 0.77, "stop": 0.11 }
    },
    "core.worker-health__progress_level": {
      "type": "score", "score": 2.4, "confidence": 0.81,
      "legend": { "0": "None", "1": "Some", "2": "Most", "3": "Essentially all" },
      "probabilities": { "0": 0.03, "1": 0.11, "2": 0.46, "3": 0.40 }
    }
  }
}
```

Parsing rules (all in `aftman-jev::wire`):

- tolerate and ignore unknown fields anywhere (`#[serde(default)]`, no `deny_unknown_fields`);
- an answer whose `type` is not one of `noul|choice|score` is dropped with a `warn`, not an error
  — the provider adds primitives over time;
- an answer key we did not ask for is dropped with a `debug`;
- a requested key with no answer is a **fatal** `ModelError::MissingAnswer`;
- a non-finite or out-of-range `noul` is a `ModelError::InvalidAnswer` (a tiny float overshoot
  past `1.0` is clamped, with a `warn`);
- a `choice` whose selection is not in the requested criteria is a `ModelError::InvalidAnswer`;
- **`ScoreAnswer.probabilities` and `.legend` arrive with *string* keys**
  (`{"0": 0.03, "1": 0.11}`). The wire type uses `BTreeMap<String, f32>`; `aftman-jev` converts
  to `BTreeMap<u32, f32>` and raises `ModelError::InvalidAnswer` on a non-numeric key. Do not try
  to deserialize directly into `BTreeMap<u32, _>` — `serde_json` rejects it with
  `invalid type: string "0", expected u32`;
- `score` is a float (the probability-weighted mean of the levels). `level` is **derived** by
  `aftman-jev` as the argmax of `probabilities`, is never sent by the provider, and is `None` when
  the keys do not parse;
- `usage` is recorded even when parsing later fails, if the body was readable.

`model` in the response may differ from the request (aliases resolve). Persist **the reported
model**, not the requested one, in the assessment event. That is what makes a stored score
attributable to a real model version later.

## 4. Error handling

| Condition | Behaviour | Retry? |
| --- | --- | --- |
| Connection error / DNS | `ModelError::Transport` | yes |
| Timeout (default 10s) | `ModelError::Timeout` | yes |
| `429` | `ModelError::RateLimited { retry_after }` | yes, honoring the header |
| `500, 502, 503, 504` | `ModelError::Server` | yes |
| `400` / `422` | `ModelError::BadRequest(body.message)` | no |
| `401` / `403` | `ModelError::Auth` | no |
| `404` | `ModelError::Endpoint` | no |
| Body does not parse | `ModelError::Malformed(body_excerpt)` | no |
| Missing/invalid answer | `ModelError::MissingAnswer` / `InvalidAnswer` | no |

Retry policy (mirrors upstream defaults, tightened):

- `max_retries = 2` (override with `AFTMAN_JEV_MAX_RETRIES`);
- backoff: 250ms, 1s, with ±30% jitter;
- `retry-after-ms` (milliseconds) takes precedence over `retry-after` (seconds *or* HTTP-date);
- a `retry-after` longer than `AFTMAN_JEV_MAX_RETRY_AFTER` (default 30s) aborts rather than
  sleeping — a supervisor that blocks for minutes is worse than one that reports uncertainty;
- total wall-clock budget per attempt chain: `timeout * 4`;
- `X-TypeSafe-Retry-Count` is incremented per retry.

Error body message extraction follows the provider's shapes in order: `error` (string or
`{message}`), `message`, `detail` (string, `{message}`, or a validation list rendered as
`field.path: msg; …`). Truncate the stored body to 200 bytes.

**On any model error the runtime does not guess.** It records the failure and applies
`PolicyInput` with an empty assessment, which by construction can only produce a conservative
directive (`Continue` if the worker is still running and the budget allows, otherwise
`Escalate`). See [05-responsibilities.md §6](05-responsibilities.md).

## 5. Grouping and parallelism

Checks are grouped by the evidence providers they select. Each group is one request, and groups
run concurrently with `futures::future::try_join_all` bounded by `AFTMAN_JEV_MAX_CONCURRENCY`
(default 4).

Why group at all, rather than one request per check?

- one request per check multiplies fixed input cost by the number of checks;
- one request for *everything* forces unrelated checks to share a single evidence selection;
- grouping by provider selection is the coarsest split that never sends a check evidence it did
  not ask for.

In practice the built-in responsibilities collapse to one or two groups.

The router is the same call shape: all *conditional* responsibilities are evaluated in one request
against the incoming job, and every threshold match activates that responsibility. Global
responsibilities skip routing.

## 6. Request budget

Jev input tokens are billable, so the observation is compacted before it is sent.

The unit is a **pair**: one leaf `(path, value)` inside the `state` object. Two budgets apply:

| Budget | Field | Default | Meaning |
| --- | --- | --- | --- |
| Pair | `jev.budget.pair_bytes` | 20,000 | one pair's serialized size |
| Request | `jev.budget.request_bytes` | 120,000 | the whole `state` object |

Compaction is deterministic and ordered, and it records every action it takes:

1. Drop low-value evidence first, in a fixed priority order (oldest events → oldest worker
   history → extra untracked files → diff middle → diffs → output heads).
2. Truncate long strings **from the middle**, keeping both ends, and mark them
   `"…[truncated N bytes]…"` so a model is never shown a silently clipped fact.
   Per-pair truncation runs **before** the required-evidence check, not after. A required
   field can itself be enormous — the active worker's output tail is protected, and a chatty
   worker produces megabytes of it — so measuring it first would exceed the budget and refuse
   the request, stopping supervision altogether. Bounding it first keeps the evidence, shrunk.
3. Never drop: the job prompt, the current status, the active worker's tail, the prior decision,
   or `counters`. If those alone exceed the budget the request is refused
   (`ModelError::BudgetExceeded`) and becomes an `Escalate`, because supervising with the
   job removed is meaningless.
4. Target sizes (`pair_target_bytes`, `request_target_bytes`) are hysteresis: once compaction
   starts it continues down to the target, so consecutive cycles do not oscillate between
   compacted and uncompacted observations.

The compaction plan (`measurements`) is logged and stored on the assessment event, so a
"why did Jev not see the test output?" question is answerable.

## 7. Usage accounting

Every request appends one line to `$XDG_STATE_HOME/aftman/jev-usage.jsonl`:

```jsonc
{"at":"2026-01-01T00:00:00Z","run_id":"0193...","assessment_seq":7,
 "purpose":"assessment","model":"jev-2025-xx-xx","requested_model":"jev-latest",
 "input_tokens":8123,"output_tokens":96,"latency_ms":412,"attempts":1,
 "request_id":"...","budget":{"compacted":true,"measurements":{...}}}
```

`purpose` is `assessment` or `routing`. `aftman usage` aggregates this file; the web UI shows the
same numbers. Usage records are written **before** the answer is validated, so a malformed
response still shows up in the bill.

## 8. Rate limiting and cost safety

- A per-run ceiling: `jev.max_assessments_per_run` (default 400). Exceeding it is a `Stop` with a
  distinct reason, never a silent stall.
- A per-run token ceiling is optional (`jev.max_input_tokens_per_run`, default off). When set, the
  budget module clamps harder before the ceiling is reached rather than failing at it.
- A soft rate limiter (token bucket, default 5 req/s) protects against a worker that emits a burst
  of events. The debounce interval is the primary control; the bucket is a backstop.
- On `429` aftman backs off for the *whole run*, not just the one request, and records
  `RateLimited` as an event so the UI can say so.

## 9. The `DecisionModel` trait

```rust
#[async_trait]
pub trait DecisionModel: Send + Sync {
    /// One System One call. `questions` is already grouped by evidence selection.
    async fn ask(
        &self,
        purpose: Purpose,                 // Assessment | Routing
        state: &serde_json::Value,
        questions: &BTreeMap<CheckKey, Question>,
        budget: &Budget,
    ) -> Result<ModelAnswers, ModelError>;

    /// Optional listing for `aftman doctor` and the UI.
    async fn models(&self) -> Result<Vec<ModelInfo>, ModelError> { Ok(Vec::new()) }

    async fn close(&self) -> Result<()> { Ok(()) }
}

pub struct ModelAnswers {
    pub model: String,
    pub usage: Usage,
    pub answers: BTreeMap<CheckKey, AnswerValue>,
    pub request_id: Option<String>,
    pub attempts: u32,
    pub compacted: bool,
}
```

Implementations:

| Impl | Crate | Purpose |
| --- | --- | --- |
| `SystemOneClient` | `aftman-jev` | Real HTTP client |
| `ScriptedModel` | `aftman-core::testing` | Returns canned answers; drives runtime tests |
| `RecordedModel` | `aftman-core::testing` | Replays answers recorded from a real run |
| `SimulatedModel` | `aftman-core::testing` | Derives plausible answers from observation content (for `aftman demo`) |

`ScriptedModel` and `RecordedModel` are what make the whole runtime testable offline and what make
[06-ensemble.md](06-ensemble.md) replayable.

## 10. Compatibility rules

1. Base URL is the only provider switch. Never branch on hostname.
2. Never send the API key anywhere except the configured base URL. No telemetry, no proxying, no
   fallback endpoints.
3. Keep the upstream env var names (`TYPESAFE_*`) so existing setups and `.env` files work.
4. `.env` loading order, non-overriding, real environment always wins:
   1. `$XDG_CONFIG_HOME/aftman/.env`;
   2. the process environment.

   **`<repo>/.env` is never loaded into the supervisor process.** An earlier revision loaded it
   first, mirroring upstream. That is a credential-exfiltration vector: a cloned repository that
   ships `TYPESAFE_BASE_URL=https://attacker.example` would have its base URL picked up whenever
   the operator had set `TYPESAFE_API_KEY` but not an explicit base URL, and the very next request
   would send the key to the attacker. Repository-supplied `.env` files may be passed to the
   *worker* environment as ordinary variables; they may not reach supervisor configuration. See
   [08-security.md §2.1](08-security.md).
5. Persist the *reported* model, keep the *requested* model alongside it.
6. Tolerate additive API change (ignore), never tolerate subtractive change silently (missing
   requested answers are fatal).
7. Record `x-typesafe-request-id` for every request so an answer can be reported to the provider.

## 11. Failure-mode summary

| Failure | What the user sees | What aftman does |
| --- | --- | --- |
| No API key | `doctor` fails, `run` refuses to start | Exit 2 with the exact env var name |
| Auth rejected | `model_unavailable`, run marked `Failed` before a worker starts | No worker spawned; no wasted tokens on coding |
| Model unreachable mid-run | Event + UI banner; supervision degrades | Conservative directives only; escalates rather than guessing |
| Repeated 429 | Backoff event, longer assessment interval | Slows supervision, keeps the worker running |
| Malformed answer | `ModelError` event with body excerpt | No directive from that answer; conservative fallback |
| Budget exceeded on required evidence | `BudgetExceeded` event | `Escalate` with the compaction plan attached |
