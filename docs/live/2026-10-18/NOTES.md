# 2026-10-18 — first live validation

Collected by hand following [10-live-validation.md](../../10-live-validation.md).

- **Jev**: reachable, answered by `jev-1.13.0` for a request of `jev-latest`. Latency was **206 ms**
  on the first probe and **284 ms** on the second (this file). Both are here because the first is
  the number that matters and the second shows the variance is small.
- **Worker probe**: all three CLIs parsed. `pi` reports **0 tokens**, and that is not a formatting
  artifact — it is the observation that led to the model finding. On its default model `pi` emits a
  well-formed stream with `content: []` and `"totalTokens": 0`, so the worker runs, exits 0, and
  does nothing. With `--model deepseek/deepseek-flash` the same probe reported usage, and the run
  below produced output.
- **The run** (`show.txt`, `decisions.txt`, `events.jsonl`): the supervised `pi` run that created
  `HELLO.txt`. It ended `needs_human`, and that is the correct outcome rather than a failure — the
  file was created, the supervisor ran a verifier, and it escalated on
  `core.completion:work-remains` because the working tree no longer matched "change nothing else":
  `pi` leaves a `.pi-lens-probe-home/` directory behind. A supervisor that called that tree complete
  would be the bug.
