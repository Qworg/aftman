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
aftman serve [--socket <path>] [--bind <addr>] [--print-url]   # foreground
```

- **Socket:** `--socket <path>` (env `AFTMAN_SOCKET`), else `$XDG_RUNTIME_DIR/aftman/aftman.sock`
  (fallback `~/.aftman/aftman.sock`), created with mode `0600`. This is the real API. The pid file
  is the socket path with a `.pid` extension. `serve` prints the socket path and what restart
  recovery did (which orphaned runs it marked `Failed`).
- **HTTP:** disabled unless `--bind` or `web.enabled = true`. Default bind `127.0.0.1:8787`.
  `--bind` overrides the configured address and implies enabling the surface; `--print-url`
  prints the URL and starts nothing. The surface is built in [`aftman-web`](../../crates/aftman-web)
  and is a second transport in front of the same `RunManager` — see §6.
- **Auto-start:** the first `aftman` subcommand that needs run state (`run`, `ls`, `show`,
  `decisions`, `logs`, `watch`, `steer`, `cancel`, `answer`, `purge`) connects to the socket; if it is absent
  or stale (`ENOENT`, `ECONNREFUSED`) it starts the daemon, polls `connect` until it succeeds
  (15 ms first, growing to 150 ms; **5 s total** timeout), and then speaks to it. `status`, `stop`,
  `usage`, `doctor`, `responsibilities` and `demo` never start a daemon.
  - **How it detaches.** A classic daemon double-forks, but `fork` is `unsafe` and this workspace
    forbids `unsafe`. Instead the CLI spawns `current_exe() serve --detached` (a hidden flag) with
    stdin closed and stdout/stderr appended to **`<state dir>/daemon.log`**, and does not wait for
    it. The child, as the first thing `main` does and **before any tokio runtime or thread
    exists**, calls `setsid()` (the safe `nix` wrapper) to leave the terminal's session, so
    closing the terminal sends it no SIGHUP and a running job survives. `main` therefore builds
    the tokio runtime by hand instead of using `#[tokio::main]`. The daemon also ignores SIGHUP
    (foreground or detached) and writes its pid to the pid file as below. The spawn passes the
    user's `--socket` / `--data-dir` through and inherits the environment whole: this is the
    supervisor re-executing itself, not a worker, so it needs the provider credentials
    ([08-security.md section 2.2](08-security.md) records the exception to the worker-environment
    rule).
  - **Paths are made absolute by the client.** The detached daemon does `chdir("/")` after
    `setsid()`, so it never keeps the first CLI's working directory (it would otherwise give
    that directory to every later job). `--socket`, `--data-dir`, the repository and a job file
    are therefore resolved to absolute paths in the client before anything is sent or passed on.
    A worker runs with its working directory set to the repository (every backend, `generic`
    included).
  - **The daemon freezes its own environment, and a run brings its own.** An auto-started
    daemon inherits the environment of the CLI that started it, and keeps it for life — so
    without help, every later run's workers, evidence commands, and provider key would use
    *that* shell's environment rather than the caller's. Two things follow:
    - **A run forwards an allowlist of the caller's environment.** The client sends the names
      it chooses — a small default set (`PATH`, `VIRTUAL_ENV`, `CONDA_PREFIX`, `SSH_AUTH_SOCK`,
      `LANG`, `LC_ALL`, `LC_CTYPE`) plus every `--pass-env KEY` — and the worker and the run's
      command evidence providers see those values. A name that is unset in the calling shell is
      an error rather than a silent no-op, and a name from the supervisor's namespace is
      refused. `show` and the TUI header list the forwarded *names*; values are never recorded.
    - **The daemon contributes only a fixed baseline** ([08-security.md section 2.2](08-security.md)
      lists it): `HOME`, `PATH`, `SHELL`, `TERM`, `TMPDIR`, locale and `XDG_*` variables. Anything
      else the daemon happens to hold — a `GITHUB_TOKEN` exported in the shell that started it,
      for example — does **not** cross. A run that forwards nothing (a raw socket client today,
      the web UI in M4) gets the baseline and nothing more.
    Provider settings are the exception to all of this: the provider `.env` file is re-read on
    every run, so putting the key there needs no restart. `aftman status` and `aftman doctor`
    print the daemon's view (provider configured or not, model, base URL without credentials,
    state directory, version) next to the caller's, and flag any disagreement; a
    `model_unavailable` from a daemon says it was started without a provider and how to fix it.
  - **One daemon per state directory.** The daemon takes a lock in the state directory
    (`daemon.lock`) as well as its pid-file lock, so two daemons on different sockets cannot
    share runs. Auto-start reports which pid owns the directory instead of looping. A client
    refuses (exit `2`) to use a daemon that serves a *different* state directory than its
    `--data-dir`, and warns when the daemon's version differs from its own.
  - **A daemon that is starting or shutting down** holds its lock without answering on the
    socket. `status` says so (with the pid, exit `1`); `stop` waits for it to be gone (up to 75 s)
    instead of saying "not running"; auto-start waits for it (up to 75 s) rather than racing it.
  - **Files.** The state directory, `runs/`, `missions/` and `evidence-tmp/` are created `0700`
    and `daemon.log` `0600` on every path (embedded, foreground `serve`, auto-start); a directory
    the user already made is never chmodded (what aftman puts inside it is still private). The
    log is rotated to `daemon.log.old` (one generation) when a daemon is auto-started and it
    exceeds 4 MiB.
  - **Racing starts.** Two CLIs auto-starting at once each spawn a `serve --detached`; the pid-file
    lock lets exactly one win, the other gets `AlreadyRunning`, prints the winner's pid to the log
    and exits `0`, and both CLIs connect to the winner. A child that exits non-zero before the
    socket appears is reported (with the log path) instead of waiting out the timeout.
  - **Data directory.** `--data-dir` (env `AFTMAN_DATA_DIR`, default `$XDG_STATE_HOME/aftman`) is a
    property of the *daemon*: a client talking to a running daemon uses that daemon's store. It is
    what an auto-started daemon is given, and what `--no-daemon`, `usage` and `doctor` read.
- **Single instance:** an exclusive `flock` on the pid file, with the pid written into it. A
  second `serve` gets `AlreadyRunning { pid }`, prints that pid, and exits 0. The lock, not the pid
  number, is the arbiter, so a stale pid file left by a crash is harmless. It is taken before
  recovery and before the socket is touched, so two daemons starting at once cannot both win.
- **One daemon per state directory:** a second exclusive lock, `<state-dir>/daemon.lock`, with the
  same discipline (pid written in, inode re-checked), is taken right after the pid lock when the
  daemon is configured with a state directory. A second daemon on a different socket but the same
  state directory gets `StateDirInUse { pid, state_dir }`. `lifecycle::probe_state_dir` is the
  non-destructive check a `--no-daemon` command uses (it takes the lock shared for an instant,
  creates nothing, and a daemon starting at that moment retries rather than failing): `aftman
  --no-daemon purge` must refuse while it reports a holder, because deleting run files under a
  live daemon leaves its replay ring serving a run that no longer exists.
- **Presence probe:** `lifecycle::probe(&DaemonPaths)` returns `NotRunning`, `Running { pid }`
  (the pid lock is held and the socket answers) or `Busy { pid }` (lock held, socket not
  answering). Graceful shutdown stops listening first and then holds the lock for up to the
  grace period while runs are cancelled, so a CLI that only tries to connect sees "refused" and
  wrongly concludes "not running"; `Busy` is how it knows to wait instead of auto-starting a
  second daemon or misreporting. The probe takes no lock for longer than an instant, creates
  nothing, and makes one connection that it drops at once.
- **One-shot:** `--no-daemon` embeds the runtime in the CLI process, streams events to the
  terminal, and exits with the run's status. For CI and scripts. No socket, no web, no herdr. It
  builds the run through the same `CliFactory` the daemon uses, so the two cannot drift.
- **Shutdown:** `aftman stop` (`daemon.stop`, then waits until the socket and pid file are gone,
  up to 75 s) or SIGTERM/SIGINT. In order: stop accepting
  connections; cancel every live run with reason `daemon shutdown` (workers are terminated via the
  normal path and each run is persisted `Cancelled`), waiting at most `shutdown_grace` (60 s), after
  which stragglers are aborted and recorded `Failed`; close every subscription (each subscriber is
  first sent what it was owed, including the final `RUN_FINISHED`); let connections flush and finish
  (bounded, 5 s); remove the socket file and the pid file, but only if they are still the files
  this daemon created (a path that now names something else belongs to someone else). A
  `daemon.stop` caller gets its `ok` reply before any of this starts.
  Outcomes worth stating: a `run.start` that began before the stop is allowed to finish (shutdown
  waits for it, bounded by the grace) and the run it creates is then cancelled like any other,
  with its events delivered; a `run.start` that begins after is refused with `wrong_state` before
  anything is created. If a factory outlasts the whole wait, the run it finally creates is
  recorded `Failed` (`daemon shutdown`) without being announced (the hub is closed), and the
  caller gets `wrong_state`. A run aborted at the end of the grace has its worker aborted with it.

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
aftman run --repo . --job "..." --worker herdr:pane --kind claude   # not built: herdr surface, M6

# Watch one run live (TUI dashboard)
aftman watch <run-id>
aftman watch --last

# Inspect
aftman ls [--status running,needs-human] [--limit N]
aftman show <run>             # summary, decision timeline, open escalations (with ids)
aftman logs <run> [-f]        # the worker's output; -f follows until the run ends
aftman decisions <run>        # every proposal, every suppression, every opinion

# Intervene
aftman steer <run> "the migration should not touch the public API"
aftman answer <run> <escalation-id> "yes, use the staging credentials"   # guidance
aftman answer <run> <escalation-id> --accept|--recheck|--stop|--claim-complete ["note"]
aftman cancel <run>
aftman purge <run> | --all    # delete a run's data (docs/08 section 7)

# Operate
aftman responsibilities list
aftman responsibilities show core.completion
aftman usage [--since 7d] [--run-id <id>]
aftman doctor [--probe]
aftman serve | stop | status
aftman demo                    # simulated worker + simulated model, no network
```

Global flags: `--data-dir`, `--socket` (env `AFTMAN_SOCKET`), `--quiet`, `--no-daemon`.

`aftman watch` is the TUI: a decision timeline, the live worker tail, the current assessment, and
a key binding for steer and for answering escalations. It renders correctly in any terminal,
including inside a herdr pane, with no herdr awareness at all.

### As built (M3)

`aftman serve` runs the daemon; every other subcommand is a client of it (a typed NDJSON client,
`aftman_cli::client`) or, under `--no-daemon`, of the embedded runtime.

- **`<run>`** is a full id or an unambiguous prefix (at least 4 hex characters; an ambiguous prefix
  lists the candidates). `show`, `decisions`, `logs` and `cancel` also take `--last` (the newest
  run).
- **`run`** goes through the daemon: `run.start`, then it follows the run's events from `seq 1`
  (a dedicated connection that re-opens itself after a `subscriber_lagged` push or a dropped
  connection, resuming from the last `seq` seen, and treats any gap or duplicate as an error) and
  prints them as the embedded runtime does. It exits with the run's code when `RUN_FINISHED`
  arrives. The client, not the daemon, canonicalizes `--repo` and reads `--job-file`: the daemon's
  working directory and terminal are not the user's.
  - **`NeedsHuman`:** when the run enters `needs_human`, `run` prints the escalation id, the ask and
    the exact `aftman answer ...` command (repeating any `--socket` / `--data-dir` the user gave),
    and exits `3`. The run keeps waiting in the daemon.
  - **Ctrl-C detaches**: the run continues in the daemon, `run` prints how to watch
    (`logs <id> -f`) and cancel (`cancel <id>`) it, and exits `130`. It never cancels.
  - **Permissions.** `--permissions interactive` is refused (no human is attached to a worker the
    daemon starts; it exists only on the herdr surface). `auto-approve` requires
    `AFTMAN_ACK_UNSANDBOXED` in the **client's** environment, checked before anything is sent: the
    daemon's environment is whichever shell happened to start it. The socket is same-user-only
    ([08-security.md section 5](08-security.md)), so this is a guard against a slip, not a defence
    against another principal.
  - **`--no-daemon`** embeds the runtime in this process: no socket, no auto-start. It is the CI /
    script mode and is unchanged from M0.
- **Read-only commands** (`ls`, `show`, `decisions`, `logs`) go through the daemon, starting it if
  needed. With `--no-daemon` they read the store directly and never start anything; `logs -f` is an
  error there (a `--no-daemon` run's own terminal is its live view).
- **`steer`, `cancel`, `answer`** act on runs a daemon supervises; with `--no-daemon` they are a
  clear usage error. Outcomes are reported truthfully: a steer the backend *rejects*, or a worker with
  no input channel (`steering_unsupported`), prints that and exits `1`; `queued` is reported as
  "accepted; delivery not confirmed". `answer` text alone is guidance for the worker
  (`HumanDecision::Guidance`); with a decision flag the text is a note recorded with that decision.
  At most one of the four flags may be given.
- **`purge`** deletes through the daemon, or directly from the store under `--no-daemon`; a run
  being driven by anyone is never purged (`purge --all` skips it, a named one is `wrong_state`). A
  purge fired the instant `run` returned is retried briefly, because the daemon announces
  `RUN_FINISHED` slightly before it drops the run from its live table.
- **`status`** prints the daemon's pid, version, uptime and run counts and exits `0`; when no daemon
  answers it prints "not running" and exits **`1`**, so `aftman status && ...` reads naturally.
  **`stop`** asks the daemon to stop and waits until its socket and pid file are gone; when none is
  running it says so and exits **`0`** (stopping is idempotent). Neither starts a daemon.
- **`usage`, `responsibilities`, `doctor`, `demo`** never need or start a daemon. `usage --since`
  takes a duration (`90s`, `30m`, `12h`, `7d`, `2w`) or an RFC 3339 time. `doctor` also reports
  whether the daemon is reachable, and the socket path and permissions; it exits `2` when no
  provider is configured, as before.
- **Errors from the daemon** print with their code and message (`error: invalid_repo: ...`).
- **Untrusted text is sanitised.** Everything printed - worker output, reasons, asks, job text,
  daemon error messages - passes through `sanitize` first: escape sequences (CSI, OSC, DCS, APC,
  PM, SOS, and the short forms; an unterminated one swallows the rest), C1 controls, bidi
  overrides and isolates, NUL, DEL, backspace and other C0 controls are removed, and CR becomes
  LF (so a progress bar cannot overwrite a line you read). Tabs and newlines are kept, since
  they are the structure of worker output. There is no raw mode: a worker can emit OSC 0, OSC 52
  and `ESC[2J` and none of it reaches your terminal.
- **Validation is shared.** `--no-daemon` and the daemon path apply the same request validation
  (an empty job, a job over 256 KiB, a job file read past its limit, are `invalid_request`,
  exit `2`); a job file is read at most the limit plus one byte.
- **`--no-daemon` runs are cleaned up.** A worker that cannot launch records the run `failed`
  (exit `1`), not `running`. SIGINT or SIGTERM cancels the run through its control channel,
  waits for the worker to be terminated and the run recorded `cancelled`, and exits `130` or
  `143`.
- **`logs -f` on a run the daemon is not driving** (it ended, belongs to a `--no-daemon`
  process, or is an orphan) says so and follows by polling the run's log until `RUN_FINISHED`
  or a terminal status; the polling lives in the client's `Follow` stream, so the TUI gets it.
- **`purge --no-daemon` refuses while a daemon serves the state directory** (deleting files from
  under it would leave its replay ring serving a purged run). `purge` also removes the mission
  files; after `purge --all` the job text is nowhere under the state directory. To make that true
  for a run whose supervisor died, `purge` (the daemon's and the `--no-daemon` one alike) first
  does what restart recovery would for it: it ends the run's orphaned worker (only if it can be
  positively identified as that worker) and removes its mission file, and only then deletes the
  directory. A worker that cannot be verified is not touched, and the purge is refused with the
  reason - deleting the run would destroy the only record of a process that may still be
  running (`purge --all` skips such a run and says so).
- **`show` reconciles with the log**: the snapshot trails the event log, so when the log is
  ahead the last `STATE_CHANGED` decides the status, and open escalations are read from the log.
  It prints how the run was launched (permissions, external sandbox, steering). The
  `aftman answer ...` hint is printed only for a daemon-backed run that is waiting, with the
  user's `--socket` / `--data-dir` repeated.
- **A followed run that nobody drives ends the stream.** `Follow` (so `run`, `logs -f`, `watch`)
  treats a non-retryable error push as final and exits non-zero with its reason. Independently,
  after 30 s of silence it asks the daemon whether the run is still driven (one `run.get`, then
  a fresh subscription, whose `live` flag answers and which replays anything missed). A run that
  is neither driven nor finished and whose log has not moved for three such intervals ends the
  stream with "no longer being driven" instead of waiting forever. While the daemon holds its
  lock but does not answer (restart recovery), a stream and auto-start wait for it up to 75 s
  without spending their reconnect budget; recovery itself reaps orphaned workers concurrently,
  so it takes about one 5 s grace period however many there are, before the socket is bound.
- **Output.** All output goes through one writer: a closed pipe (`aftman ls | head`) ends the
  process quietly with exit `0`.
- **Ctrl-C while `run.start` is pending** is effective at once; it (and a `run.start` timeout)
  says the run may have started and to look with `aftman ls`.
- **After `RUN_FINISHED` the event decides** the summary and the exit code; the final `run.get`
  is best effort, on a fresh connection (the daemon closes idle connections after 60 s).

Exit codes: `0` finished, `1` failed/stopped/cancelled (or an operation that did not take effect:
a refused steer, a cancel of an ended run, `status` with no daemon), `2` configuration, credential
or usage error (a bad flag, an unknown run id, `invalid_repo`, `invalid_config`,
`model_unavailable`, a daemon that cannot be reached or started), `3` `NeedsHuman` (so a script can
distinguish "a person must look" from "it broke"), `130` detached with Ctrl-C (or an embedded run
cancelled by SIGINT), `143` an embedded run cancelled by SIGTERM.

### `aftman watch` as built (M3)

`aftman watch <run> | --last` is a client like the others (`aftman_cli::tui`): it replays the run
from `seq 1` through the same self-recovering `Follow` stream the CLI uses, so catching up and
following live are one code path and a reconnect cannot desynchronise the screen. It sends
`run.steer`, `run.cancel` and `run.answer` on a connection of its own, and decides nothing: every
action is a request, and the daemon's answer (`wrong_state`, `steering_unsupported`, a rejected
steer) is shown in the footer. It has no herdr awareness and works unchanged inside a herdr pane.

**The daemon.** `watch` goes through the same daemon checks as the other commands: it starts a daemon
if none is running (then, having no such run, says so: `unknown_run`, or "there are no runs" for
`--last`, exit `2`), refuses a daemon that serves another state directory, and warns on a version
skew. It needs a daemon: `--no-daemon` is a usage error (exit `2`). A run the daemon is not driving
(another process owns it, or it is an orphan) is followed by polling its log; the header says so
("not live in this daemon") and the stream ends when the log does.

**Replay.** The fold starts at `seq 1`, applies events in batches (up to 2,048 between looks at the
keyboard) and draws at the frame rate (50 ms), never once per event; the stream is read on a task of
its own, because a read abandoned half-way (for a run the daemon is not driving it is a request for
a page of up to 5,000 events) is restarted and its response still read and thrown away, which made
replay time quadratic in the length of the log. Until the fold has reached the `last_seq` the run's
snapshot held when the watch began, the header says `REPLAYING history (seq N of M)` instead of a
status (the status at some past `seq` is not the run's), and the NEEDS-A-HUMAN banner is withheld.
"Caught up" is therefore as honest as that snapshot: if it lags the log, the banner appears when the
fold passes it, a few events early at worst; if the snapshot cannot be read, there is no replay
indicator.

**Structure.** A pure state model (`WatchState::apply(&EventEnvelope)`: a fold, idempotent on a
repeated `seq`, tolerant of unknown or malformed payloads; an event kind this build does not know
decodes as `EventKind::Unknown`, is counted and shows nothing), a pure view
(`draw(frame, state, ui)`), pure key handling (`handle_key(ui, state, key) -> Option<Action>`), and an
async driver generic over the ratatui `Backend` and over a channel of terminal events. Only
`tui::terminal` touches the real terminal.

**Layout.** Header (status badge, run, elapsed, counters; worker, model, repo with its middle
elided, job; the launch posture when there is room), a banner only when the run needs a human or has
ended, three panes, a footer.

```text
 RUNNING  run 0193a0f1  0:01:14  assessments 3 (+2 skipped)  workers 1  attempts 1  steers 0
 worker: pi  model: ...  repo: /home/me/.../service  job: ...
+- NEEDS A HUMAN ----------------------------------------+-------------------------------+   only when needed
| escalation <id>   press a to answer                     |                               |
| ask: ...                                                |                               |
+- 1 Decisions -----------------------+ +- 3 Worker output ---------------------------+
| 41 * continue  <reason>             | | ~ -- worker 0193a0f1 --                     |
|> 44 * finish    <reason>            | | | text the worker printed                   |
|     why: ...                        | | + a long line continues here                |
|     proposed: rule -> directive     | |                                             |
|     suppressed: rule (reason)       | |                                             |
|     invariants: ...                 | |                                             |
+- 2 Assessment #3 (6 checks) --------+ |                                             |
| check        p   thr  margin side flags                                             |
| core.x     0.82 0.70  +0.12 above decisive permitted                                |
+-------------------------------------+---------------------------------------------+
 message ......................  a ANSWER  q quit  Tab pane  j/k scroll  s steer  c cancel  ? help
```

- **Decisions** lists every selected directive with its reason, interleaved with the notable events
  around it (state changes, workers, steers, evidence, escalations, errors). The selected entry is
  expanded into its "why": the reason, the assessment it came from, every proposal (rule, directive,
  priority), every suppression with its reason, and the invariants that applied. The newest entry
  is followed until the user moves the selection.
- **Assessment** shows every check of the latest real assessment with its value, threshold, and
  margin computed from the unrounded values (never `-0.00`; a value just under its threshold prints
  `-<0.001`), the side of the threshold in words (`above` / `below` / `at`), and the `decisive` /
  `contested` / `unresolved` / `permitted` flags verbatim and uncoloured as good or bad (`decisive`
  means far enough from the threshold to act on, in either direction). With fewer than 84 columns
  *in the pane* each check takes two lines. The last model error, rate limit or budget event shows
  above the checks with its event number and age, and goes when a later assessment gets real answers
  (a spent budget stays). The header counts `assessment_seq` as the daemon does and counts cycles
  that found the budget spent separately (`+N budget-exhausted`).
- **Worker output** is the sanitised `WORKER_OUTPUT` text. Every worker line has a `| ` gutter; a long
  line wraps (at most six rows, ending in `…`) and its continuation rows start with `+ `; lines the
  TUI itself adds (worker boundaries, truncation notices) have a `~ ` gutter and a different style.
  Styling follows the line's kind, never what its text looks like, so nothing a worker prints can pass
  for the TUI's chrome. Scrolling back anchors to the line being read (new output does not move it),
  the title says how many newer lines exist, and the pane is never left blank at the top.
- **Banner.** `needs_human` only: black on yellow, with the words "NEEDS A HUMAN", the escalation id and
  the ask; `NeedsHuman` is never drawn like `Failed` (white on red, no banner), in colour or without
  it. Once a run ends a one-line summary replaces it.
- **Launch posture** (`RUN_CREATED.launch`): shown in the header when there is room. An `auto-approve`
  run additionally carries a red `AUTO-APPROVE` badge on the first line and, from 14 rows up, a red
  line saying the agent's approval prompts are suppressed and aftman does not sandbox it.
- **Link.** If the stream loses its connection and cannot re-establish it at once, the header shows
  `NO DAEMON (n)` and says the screen is stale and reconnecting. If it cannot be re-established
  within the client's attempts, the last frame says "lost the daemon", the TUI leaves the alternate
  screen and prints the error (see exit codes).
- Three panes at 110+ columns sit in two columns (timeline over assessment on the left, worker
  output on the right); from 50 columns and 15 body rows they stack; below that only the focused pane
  shows (`Tab` switches). Under 12 columns or 3 rows the screen is one status line. It does not panic
  at any size.

**Keys.**

| Keys | |
| --- | --- |
| `q`, `Esc`, `Ctrl-C`, `Ctrl-D` | quit the TUI (the run keeps going) |
| `Tab`, `Shift-Tab`, `1` `2` `3` | focus a pane |
| `j` `k`, arrows, `PgUp` `PgDn`, `Home` `g`, `End` `G` | scroll the focused pane (`End` follows again) |
| `s` | steer the worker |
| `a` | answer the oldest open escalation (refused once the run has ended) |
| `c` | cancel the run |
| `?`, `F1` | help |

- **Steer** (`s`) opens a one-line editor (multi-byte aware: cursor by character,
  `Ctrl-A`/`E`/`U`/`K`/`W`, `Alt-←/→`, bracketed paste). Enter sends. A message that did not go
  through is back in the editor the next time. The limits are enforced as you type
  (`MAX_STEER_BYTES` 16 KiB and the runtime's 8,000 characters). Pasted newlines become spaces.
- **Answer** (`a`) lists the five decisions of `docs/06-ensemble.md` "R3 - Human" with a plain
  sentence each: `accept`, `recheck`, `stop`, `claim-complete` (it re-measures with fresh evidence
  and cannot finish the run by itself), and `guidance` (your words go to the worker; required text).
  Choosing one opens the editor for a note (optional, 2,000 characters; required for guidance).
- **Destructive actions need a word, not a letter.** `c` (cancel) and an answer of `stop` open a
  confirmation where you type `cancel` / `stop` and press Enter; anything else backs out, as does
  Esc. Characters typed in the confirmation are just text, however fast. What a stream of text
  (type-ahead, a paste that arrives without bracketed paste) cannot do is *confirm*: keys closer
  together than 25 ms are a burst, and (1) commands from the main screen are ignored inside one,
  (2) the rest of a burst that opened a modal is swallowed, and (3) an Enter in the same burst as the
  keys before it never confirms (it leaves the prompt open with its text and says "press Enter
  again"; in the text editors it is a space, never a submit). Keys are stamped when read, with the
  injected clock. Prompts that cannot be shown in full (under 44x9 for the confirmation, 44x10 for
  the decision list) say "terminal too small" and accept nothing but Esc.
- With several open escalations, `a` opens the oldest; **Tab / Shift-Tab in the answer dialog** choose
  which one the answer is for, and the dialog lists them all (the banner says "+N more"). An answer
  from the TUI is recorded with surface `tui` (`run.answer`'s optional `surface`,
  `docs/02-lld.md` section 11).
- Only one steer or answer is in flight at a time: a second is refused before its prompt closes, so
  nothing typed is lost. A cancel can always be issued. While an action is in flight the screen keeps
  updating; quitting waits up to two seconds for it (saying so; any key leaves at once).

**Quitting never cancels the run.** When the run ends the TUI stays up on the final state (scroll
the tail and the timeline) until the user quits. It shows **one run**, chosen by id or `--last`; the
list of runs is `aftman ls`. The exit code is the CLI's: `0` / `1` / `3` for a run that ended
finished / failed or cancelled / needing a human, `130` for a detach from a live run, `3` when
quitting while the run waits for a human, and `1` if the daemon was lost and could not be reached
again. That last case prints, on the normal screen, that the TUI cannot tell what happened to the
run (a crashed daemon's runs are marked failed when a daemon next starts) and which commands find
out. The commands it prints for re-attaching, answering and cancelling carry `--socket` /
`--data-dir` when they were given.

**Not a terminal.** When stdout or stdin is not a terminal, `watch` draws nothing and emits no escape
sequence: it prints one line per event through the CLI's sanitising output (the format of
`aftman run`, one line per event whatever the text contains, plus the launch lines) until the run
ends, and Ctrl-C detaches: `130`, or `3` if the run is waiting for a human at that moment (the
same codes as quitting the TUI). `needs_human` is printed as it happens but does not end the
follow.

**Terminal hygiene.** Raw mode, the alternate screen and bracketed paste are entered once and undone
on every exit path: normal return, error, a panic on the TUI's thread (the hook restores first), and
SIGINT / SIGQUIT / SIGTERM / SIGHUP. A panic in an action task is reported in the footer and does not
touch the terminal. Keys are read on a dedicated thread and handed to the async loop through a bounded
channel; if that thread ends the TUI quits.

**Untrusted text.** Worker output, the job, the repo path, escalation asks, model reasons and daemon
error messages all pass through the shared `sanitize` (`crates/aftman-cli/src/sanitize.rs`, the same
function the plain CLI output uses) before they are stored: ESC / CSI / OSC / DCS / APC sequences
(and their 8-bit C1 forms) are removed whole, other C0/C1 controls are dropped, a lone carriage return
becomes a line break, bidi controls, zero-width and tag characters, the combining grapheme joiner and
U+180E are dropped, and at most four zero-width marks are kept per base character. The TUI layers
tab expansion, one-line folding, field caps (applied after sanitising, so escapes cannot use the cap
up), and column-aware clipping and wrapping on top. Wide and combining characters are measured by
terminal column, never split.

**Bounds** (named constants in `tui::state`, oldest-first eviction): 400 timeline entries, 1,000 tail
lines / 128 KiB, 500 characters per tail line (and 256 K characters taken from one event), 1,000
characters per reason or ask, 24 proposals / 24 suppressions / 16 invariants per decision, 64 checks,
32 escalations, 64 workers, 16 launch entries.

Not built: the diff view (the event stream carries none in M3).

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

### As built (M4)

`crates/aftman-web`: an axum router over the same `RunManager` the socket dispatches to, with
assets compiled in by `rust-embed` (no Node in the runtime path) and one hand-written escaper
(`html::escape`) — there is no template engine, so there is no second path for untrusted text and
no `|safe` to forget.

Three details are worth recording because they are easy to get wrong:

- **`Last-Event-ID` is not `from_seq`.** The SSE id is the last event the client *processed*, so
  the resume starts at the one after it; `from_seq` (on `run.events` and `?from_seq=`) is
  inclusive. Passing one into the other re-delivers an event — invisible in a browser that
  dedupes by id, and a duplicated sequence number to anything that counts. The handler translates;
  the reconnect test asserts contiguity across the cut.
- **SSE event bodies carry the kind** (`{"seq", "at", "kind", "payload"}`) rather than using the
  `event:` field, so a client has one message shape to parse. The only named event is `lagged`,
  which is a control signal: the stream then closes and the browser reconnects, and the daemon
  replays the gap from the store. A gap is never silent.
- **`ConnectInfo` is a required extractor, not an `Option`.** A router served without connection
  info answers 500 rather than treating every caller as local, so forgetting
  `into_make_service_with_connect_info` cannot open the surface.

The pages are `/`, `/runs/{id}`, `/runs/{id}/decisions`, `/runs/{id}/evidence`, `/new`, `/usage`
and `/config`, plus the JSON API of §3. The run detail page shows every check with its value,
threshold, **margin**, `|p−t|`, band and permission, which is what makes "held" legible instead of
mysterious; the diff comes from the same in-process git walk the supervisor uses
(`repo_observe::git_snapshot`), bounded and interruptible so a page load cannot pin a thread.

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
| Browse history | ✅ | one run, chosen by id or `--last` (scroll its timeline and output; no run list) | ✅ | — | — |
| See the diff | ✅ | not yet (M4) | ✅ | — | — |
| Inspect decisions | ✅ | ✅ | ✅ | — | — |
| Watch the raw agent | — | — | — | — | ✅ (the pane **is** the agent) |
| Approve a permission prompt | — | — | — | — | ✅ |
| Works with no daemon | ✅ (`--no-daemon`) | — | — | — | — |
| Works with no network | ✅ | ✅ | ✅ (loopback) | ✅ | ✅ |
| Works with no herdr | ✅ | ✅ | ✅ | — | — |

## 9. Daemon lifecycle and resilience

- **Socket permissions:** `0600`, inside `$XDG_RUNTIME_DIR` where the OS already isolates per-user
  state. The daemon's own directory is created `0700`; an existing one owned by someone else is
  refused, and one with group/other bits is tightened to `0700` (logged). For an explicit
  `--socket` path only missing parents are created (`0700`); an existing parent is left alone. A
  path too long for `sun_path` is a clear error up front. The limit applies to the path that is
  actually bound, which is the final path plus the staging directory `/.st<pid>` (103 bytes in
  all; `lifecycle::max_socket_path_bytes()`). The socket is bound inside that private `0700`
  staging directory, made `0600`, and renamed into place, so it is never visible at its final
  path with a wider mode. The directory policy follows where the socket lives: moving only the
  pid file does not switch it off.
- **Stale sockets:** examined only while holding the pid lock. A socket file is replaced only after a
  connect attempt is refused; one that answers is never stolen (`SocketInUse`), and a path that is
  not a socket is never removed (`NotASocket`). A client distinguishes the cases by trying to
  connect: success means a daemon is running; refusal on an existing socket file means stale.
- **Client reconnect:** any client can reconnect and resume from its last `seq`. The daemon keeps a
  bounded replay buffer (default 10,000 events in total, oldest evicted first) plus the on-disk
  log, so resume works for old runs too. Each connection holds one subscription.
- **Daemon restart:** after taking the pid lock and before binding the socket, recovery examines
  every run. A run is *orphaned* if its log has no `RunFinished`, its status (the last
  `STATE_CHANGED` in the log; the snapshot may trail it) is not terminal, and its ownership claim
  ([02-lld.md section 8](02-lld.md)) can be taken, i.e. nobody is driving it. A run whose snapshot
  is terminal and whose log ends with `RUN_FINISHED` (read from the end of the file) is skipped
  without reading its log, so startup cost does not grow with the size of finished logs. Each orphan is marked
  `Failed` with reason `daemon restarted`: a `STATE_CHANGED` and a `RUN_FINISHED` continuing the
  run's `seq`, plus a snapshot. **Resuming a run is not implemented in M3**; the only outcomes are
  "driven by someone" and `Failed`, so there is no silent zombie. Left alone: runs whose log already
  has `RunFinished` (including a `--no-daemon` run that ended in `NeedsHuman`), runs with a terminal
  status, and runs claimed by another process (an `aftman run --no-daemon` in a second terminal
  sharing the state directory: appending to its log from here would corrupt it). A damaged run
  directory is logged and skipped and does not stop the daemon from starting. Runs that are
  already over in the log but whose files disagree are *repaired*: a snapshot trailing a terminal
  log is rewritten from the log, and a terminal status with no `RUN_FINISHED` gets one appended
  (a client waiting for it would otherwise wait forever). A run directory holding nothing but a
  lock file is removed. A log with a torn final line is cut back before anything is appended.
- **Connection bounds:** at most 64 clients, each with a 256-line outbound queue, a 5 s write
  timeout, 32 in-flight requests (the reader stops reading a connection that has that many
  executing; the figure does not multiply across connections or bound queued responses), a 1 MiB
  request-line cap, and a 60 s idle timeout for connections with no subscription and nothing in
  flight. A client that exceeds a bound is dropped; nothing a client does can stall a run, the
  accept loop, or another client.
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
  human steer and does not skip the assessment loop (it marks a cycle pending, subject to the same
  `min_assessment_interval` debounce as output). An accepted human steer also opens the
  steering-grace window for that worker, so the supervisor's own stuck/off-track stops are deferred
  for the window (`steering_grace`, default 30 s). That is intended: an operator who just redirected
  the worker should not have it killed as stuck before it can react. It does not consume the
  supervisor's `max_steers_per_worker` budget. Forcing a directive is not a feature.
- **herdr integration cannot start agents aftman did not spawn.** No adopting arbitrary panes.
- **No surface can enable `Finish`.** Only the `Finish` precondition in
  [05-responsibilities.md §6.2](05-responsibilities.md) can, and it requires a decisive value that
  only evidence can produce ([06-ensemble.md §2.2](06-ensemble.md)). A human may always **stop**
  work from any surface; a human answer of "this is complete" runs the evidence refresh and lets
  the resulting value decide. Operator words are an authorization to re-measure, never a way to
  satisfy a completion conjunct by fiat.
