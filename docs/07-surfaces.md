# 07 — Surfaces: CLI, herdr, and the web

The design rule is that **herdr is never required, the browser is never required, and the CLI is
never required.** One daemon owns runs and emits one event stream. Every surface is a client.

```text
                          ┌───────────────────────────┐
                          │        aftman-daemon      │
                          │  FactoryRuntime · RunStore │
                          │  ProtocolServer            │
                          └───┬───────────┬────────┬───┘
                              │           │        │
             Unix socket      │           │        │      HTTP + SSE
             NDJSON           │           │        │      (loopback / LAN / tailnet)
                              ▼           ▼        ▼
                        ┌─────────┐  ┌─────────┐  ┌──────────────┐
                        │ aftman  │  │  web UI │  │  herdr       │
                        │ CLI+TUI │  │ browser │  │  worker pane │
                        └─────────┘  └─────────┘  └──────────────┘
                             ▲                            │
                             └──── herdr CLI used by the daemon ────┘
                                   (only for herdr:pane workers)
```

Note the two directions: aftman is a *client* of herdr (to run workers in panes), and herdr users
are *clients* of aftman (via the CLI, the web UI, or by watching panes).

## 1. The daemon

```bash
aftman serve [--socket <path>] [--bind <addr>] [--print-url]
```

- **Socket:** `$XDG_RUNTIME_DIR/aftman/aftman.sock` (fallback `~/.aftman/aftman.sock`), created
  with mode `0600`. This is the real API.
- **HTTP:** disabled unless `--bind` or `web.enabled = true`. Default bind `127.0.0.1:8787`.
- **Auto-start:** the first `aftman` subcommand that needs run state starts the daemon if the
  socket is absent, waits for readiness (500 ms poll, 5 s timeout), and then speaks to it. The
  daemon is a child of nothing: it double-forks on Unix and writes its pid to
  `$XDG_RUNTIME_DIR/aftman/aftman.pid`, so closing the terminal does not kill a running job.
- **Single instance:** a lock on the pid file. A second `serve` prints the existing pid and exits 0.
- **One-shot:** `--no-daemon` embeds the runtime in the CLI process, streams events to the
  terminal, and exits with the run's status. For CI and scripts. No socket, no web, no herdr.
- **Shutdown:** `aftman stop` (or SIGTERM) cancels live workers via the normal termination path,
  persists each run as `Cancelled` with reason `daemon shutdown`, flushes the store, then closes
  the socket.

The daemon is deliberately small: it owns the runtime and the transports, and it holds no
surface-specific logic.

## 2. Transport A — Unix socket, NDJSON

Request/response and event streaming are defined in [02-lld.md §11](02-lld.md). Properties that
matter for every surface:

- newline-delimited JSON, one object per line, so it is debuggable with `nc`/`socat` and testable
  without a client library;
- requests carry an `id`, responses echo it, so a client can pipeline;
- `subscription.open` streams events with `seq` values, so **any** client reconnects with
  `from_seq` and loses nothing — a dropped SSH session, a browser reload, or a herdr pane restart;
- the same message schema is used over HTTP, so there is exactly one protocol to document and one
  set of client types to maintain.

## 3. Transport B — HTTP + SSE

| Endpoint | Purpose |
| --- | --- |
| `GET /v1/status` | daemon version, uptime, live run count |
| `POST /v1/runs` | start a run |
| `GET /v1/runs` | list, filterable by status |
| `GET /v1/runs/{id}` | full run state snapshot |
| `GET /v1/runs/{id}/events` | **SSE**; supports `Last-Event-ID` for resume |
| `POST /v1/runs/{id}/steer` | deliver guidance |
| `POST /v1/runs/{id}/cancel` | cancel |
| `POST /v1/runs/{id}/answer` | answer an escalation |
| `GET /v1/responsibilities` | active responsibilities and checks |
| `GET /v1/usage` | aggregated Jev usage |
| `GET /` | embedded UI |

SSE event ids are the run's `EventSeq`, which makes resume exact rather than approximate. A browser
that reconnects mid-run resumes at the first unseen event; no polling, no dedupe, no gaps.

HTTP is a *second transport in front of the same protocol*, not a second implementation. The
handler layer translates HTTP → protocol messages → the same runtime calls the socket uses.

## 4. Surface 1 — CLI and TUI

```bash
# Supervise a job
aftman run --repo ./my-project --job "Add request retries with backoff and tests"

# Backends
aftman run --repo . --job "..." --worker pi
aftman run --repo . --job "..." --worker agy  --model <m> --effort high
aftman run --repo . --job "..." --worker claude
aftman run --repo . --job "..." --worker generic --cmd 'mytool --prompt {mission_file}'
aftman run --repo . --job "..." --worker herdr:pane --kind claude

# Watch one run live (TUI dashboard)
aftman watch <run-id>
aftman watch --last

# Inspect
aftman ls [--status running|finished|needs-human]
aftman show <run-id>          # summary, decision timeline, final diff
aftman logs <run-id> -f
aftman decisions <run-id>     # every proposal, every suppression, every opinion

# Intervene
aftman steer <run-id> "the migration should not touch the public API"
aftman answer <run-id> <escalation-id> "yes, use the staging credentials"
aftman cancel <run-id>

# Operate
aftman responsibilities list
aftman responsibilities show core.completion
aftman usage [--since 7d]
aftman doctor [--probe]
aftman serve | stop | status
aftman demo                    # simulated worker + simulated model, no network
```

`aftman watch` is the TUI: a decision timeline, the live worker tail, the current assessment, and
a key binding for steer and for answering escalations. It renders correctly in any terminal,
including inside a herdr pane, with no herdr awareness at all.

Exit codes: `0` finished, `1` failed/stopped, `2` configuration or credential error, `3`
`NeedsHuman` (so a script can distinguish "a person must look" from "it broke").

## 5. Surface 2 — herdr

herdr is a terminal workspace manager for coding agents. Aftman works at **three graduated levels
of integration**, and each level is independently useful.

### Level 0 — aftman inside a herdr pane (zero integration)

Run `aftman watch <run-id>` in a pane. That is the whole integration. Aftman does not detect
anything, and this is the level most users need.

Because workers are subprocesses in the daemon's process group, not children of the pane, a worker
keeps running if the pane is closed. The daemon does not care where its client lives.

### Level 1 — detection, notifications, labels

When `HERDR_ENV=1` (and `herdr.enabled = auto`):

| Event | herdr action |
| --- | --- |
| Escalation raised | `herdr notification show` |
| Run finished / failed | `herdr notification show` |
| A pane aftman created is working on run `<id>` | `herdr pane report-metadata --source aftman <pane-id> --title "aftman: <run-id>"` |

Rules the integration follows, taken from herdr's own agent contract:

- `HERDR_ENV` unset ⇒ no herdr calls at all, ever;
- notifications are rate-limited per run (one per escalation, one per terminal transition), because
  a notification per assessment cycle is spam;
- **the daemon never closes, moves, or focuses a pane it did not create**, and never runs
  `herdr server stop`;
- `herdr agent ...` commands are only used for panes aftman created.

### Level 2 — `--worker herdr:pane` (the interesting one)

This runs the *worker* as a herdr agent in a sibling pane instead of a hidden subprocess. It is the
only backend that is both steerable **and** human-visible, and it works with any agent herdr
recognizes — not just `pi`/`agy`/`claude`.

```bash
aftman run --repo . --job "..." --worker herdr:pane --kind claude
```

Sequence (verified commands only):

```bash
herdr pane split --current --direction right --cwd "$REPO" --no-focus
#  → .result.pane.pane_id
herdr agent start aftman-<run>-1 --kind claude --pane <pane-id>
herdr agent prompt aftman-<run>-1 "<mission>" --wait --timeout <ms>
herdr agent get  aftman-<run>-1          # working|idle|blocked|done|unknown
herdr agent read aftman-<run>-1 --source recent-unwrapped --lines 200
```

What this buys:

- **Real steering.** `Steer` becomes `herdr agent prompt <name> "<message>"`, which lands in the
  live session. The trait reports `SteerOutcome::Delivered`/`Queued` honestly (doc 04 §3.2).
- **Approval prompts become an escalation instead of a hang.** herdr classifies a waiting agent as
  `blocked`. Aftman maps `blocked` → `EscalationRaised` rather than `worker_stuck`, so the human
  answers in the pane and work resumes. This is the one configuration where the `interactive`
  permission mode is allowed, and it is the safest way to run an agent that needs approvals.
- **The human can see the worker.** The pane is the worker's real terminal; the human can scroll,
  type, and take over without going through aftman.

Non-negotiable mapping rules:

- `unknown` never means done, and `done` alone never means a run is finished — only Jev plus policy
  finishes a run;
- `blocked` is never `stuck`;
- `idle`/`done` are *lifecycle* states, and the run's outcome comes from the assessment, exactly as
  with a subprocess backend.

The pane id and agent name are recorded on the worker as `artifact_ref`, so the CLI and web UI can
deep-link to the pane.

### Level 3 — a herdr integration entry (future)

`herdr integration` exists as a command group, and `herdr api snapshot` / `herdr api schema` expose
live state and the socket API schema. A future plugin could surface aftman's current assessment in
herdr's sidebar. **Not in v1.** It is listed in [09-plan.md](09-plan.md) as explicitly deferred,
because it is the only herdr work that adds a maintenance dependency on herdr's internals, and the
Level 0–2 design already covers standalone, herdr, and web use.

### Which level to choose

| Situation | Level |
| --- | --- |
| You just want supervision and a dashboard | 0 |
| You want to know when aftman needs you, without watching | 1 |
| You want to watch, and be able to steer, the actual agent | **2** |
| You want aftman state inside herdr's own UI | 3 (future) |

## 6. Surface 3 — Web UI

Served by the daemon from embedded assets (`rust-embed`), so there is no Node build step at
runtime and no separate deploy. The front end is server-rendered HTML with a small amount of
vanilla JS for the SSE stream and the forms. No SPA framework, no bundler in the critical path.

Pages:

| Page | Shows |
| --- | --- |
| `/` run list | Status, repo, job excerpt, elapsed, last directive; filter by status |
| `/runs/{id}` run detail | Decision timeline; live worker tail; current assessment with every check value, threshold, and margin; the diff; evidence results with commands and exit codes; steer and cancel |
| `/runs/{id}/decisions` | Every proposal, every suppression with its reason, and every ensemble trigger, opinion, and resolution |
| `/runs/{id}/evidence` | Command evidence runs: resolved argv, exit code, duration, output excerpt |
| `/new` | Start a job: repo path, job text, backend, model |
| `/usage` | Jev tokens and request counts over time |
| `/config` | Resolved configuration and where each value came from |

Design notes:

- The run detail page is a **decision log with a diff**, not a chat UI. Aftman has no chat.
- Escalations are a first-class, answerable form, and they appear even when the run is otherwise
  quiet.
- `NeedsHuman` is visually distinct from `Failed`. Conflating "it broke" with "a person must
  decide" is how a UI teaches an operator to ignore it.
- The event stream is resumable, so a laptop sleeping mid-run resumes without gaps.
- Live updates are SSE, not WebSocket: the traffic is one-directional server→client, resumption is
  built in via `Last-Event-ID`, and it survives proxies that mishandle upgrades.

## 7. Local, LAN, and Tailscale

Default posture is loopback-only:

```toml
[web]
enabled = true
bind = "127.0.0.1:8787"
auth = "loopback"     # loopback | token | none
# Required when auth = "token". Canonicalised, symlink-resolved prefixes that
# POST /v1/runs may use as `repo`. Defaults to $HOME under auth = "loopback".
allowed_repo_roots = ["/home/me/Development"]
```

`POST /v1/runs` takes a `repo` path, and a run's observation can be read back through
`GET /v1/runs/{id}`. Without a root allow-list, a client that can reach the API could point a run
at any directory the *daemon user* can read and read the observation. Every `repo` is therefore
canonicalised (symlinks resolved) and required to be under one of `allowed_repo_roots`; a path that
escapes every root is rejected with `invalid_repo` before any worker starts. See
[08-security.md §4](08-security.md).

| Posture | Config | Notes |
| --- | --- | --- |
| Loopback only | `bind = "127.0.0.1:8787"`, `auth = "loopback"` | Default. No token needed; anything on this machine is trusted. |
| LAN / another machine | `bind = "0.0.0.0:8787"`, `auth = "token"` | Requires `AFTMAN_WEB_TOKEN` or a generated token printed once by `serve`. |
| **Tailscale (recommended remote)** | keep `bind = "127.0.0.1:8787"`, then `tailscale serve --bg 8787` | HTTPS with a tailnet certificate, reachable only inside the tailnet, **no app token required and no port exposed to the LAN**. |
| Public internet | `tailscale funnel` | Discouraged. Requires `auth = "token"` and prints a warning; aftman's web API can start jobs and steer agents, so a public bind is a remote-code-execution surface by construction. |

Tailscale is the recommended remote path because it moves authentication to the network layer,
where Tailscale is good at it, instead of asking a small Rust server to reimplement it. `tailscale
serve` proxies to the loopback bind, so the default `auth = "loopback"` posture is preserved end to
end and the port is never opened on a physical interface.

When `auth = "token"`: `Authorization: Bearer <token>`, constant-time comparison, and the SSE
endpoint accepts the token via a `?token=` query parameter *only* because `EventSource` cannot set
headers — with the caveat documented (it can land in proxy logs), and a short-lived
cookie-login flow offered as the alternative. See [08-security.md](08-security.md).

## 8. Capability matrix

What each surface can do. Blank means "not applicable", not "not implemented by accident".

| Capability | CLI | TUI | Web | herdr L0/L1 | herdr L2 |
| --- | --- | --- | --- | --- | --- |
| Start a run | ✅ | — | ✅ | — | ✅ (via CLI starts the pane) |
| Live event stream | ✅ | ✅ | ✅ SSE | ✅ (`watch` in a pane) | ✅ |
| Steer a worker | ✅ | ✅ | ✅ | ✅ | ✅ (`agent prompt`) |
| Cancel a run | ✅ | ✅ | ✅ | ✅ | ✅ |
| Answer an escalation | ✅ | ✅ | ✅ | ✅ | ✅ (in the pane) |
| Browse history | ✅ | ✅ | ✅ | — | — |
| See the diff | ✅ | ✅ | ✅ | — | — |
| Inspect decisions | ✅ | ✅ | ✅ | — | — |
| Watch the raw agent | — | — | — | — | ✅ (the pane **is** the agent) |
| Approve a permission prompt | — | — | — | — | ✅ |
| Works with no daemon | ✅ (`--no-daemon`) | — | — | — | — |
| Works with no network | ✅ | ✅ | ✅ (loopback) | ✅ | ✅ |
| Works with no herdr | ✅ | ✅ | ✅ | — | — |

## 9. Daemon lifecycle and resilience

- **Socket permissions:** `0600`, inside `$XDG_RUNTIME_DIR` where the OS already isolates per-user
  state. A stale socket is detected by connect-and-fail, removed, and recreated.
- **Client reconnect:** any client can reconnect and resume from its last `seq`. The daemon keeps a
  bounded replay buffer (default 10,000 events in total, oldest evicted first) plus the on-disk
  log, so resume works for old runs too. Each connection holds one subscription.
- **Daemon restart:** on start, runs with no terminal event are loaded, their state is
  reconstructed, and each is either resumed (if configured) or marked `Failed` with reason
  `daemon restarted`. There is no silent zombie run.
- **Backpressure:** a slow client cannot stall a run. Each subscriber has a bounded channel; on
  overflow the client is dropped with a `subscriber_lagged` error push (carrying `resume_from`) and
  must resume by `seq`; what it already received is a correct gap-free prefix. Runs never block on
  a slow UI. See [02-lld.md section 11](02-lld.md).
- **Multiple clients:** any number, of any kind, simultaneously. The daemon fans out; it does not
  arbitrate between surfaces, because there is only one run state to read.

## 10. What each surface deliberately cannot do

- **The web UI cannot answer a Jev check directly.** It can answer an *escalation*, which policy
  then consumes. There is no path from a browser click to a probability.
- **The CLI cannot bypass policy.** `aftman steer` sends a message to a worker; it is recorded as a
  human steer and does not skip the assessment loop. Forcing a directive is not a feature.
- **herdr integration cannot start agents aftman did not spawn.** No adopting arbitrary panes.
- **No surface can enable `Finish`.** Only the `Finish` precondition in
  [05-responsibilities.md §6.2](05-responsibilities.md) can, and it requires a decisive value that
  only evidence can produce ([06-ensemble.md §2.2](06-ensemble.md)). A human may always **stop**
  work from any surface; a human answer of "this is complete" runs the evidence refresh and lets
  the resulting value decide. Operator words are an authorization to re-measure, never a way to
  satisfy a completion conjunct by fiat.
