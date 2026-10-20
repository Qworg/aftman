# 2026-10-20 — second live validation

Collected by hand following [10-live-validation.md](../../10-live-validation.md). This is the run
that **found a real bug in the decoder**, and the first with a working `pi` default model.

## What was proved

- **Jev**: reachable, answered by `jev-1.13.0` for a request of `jev-latest`, in **260 ms**
  (**260 ms** in the `doctor.txt` beside this note; 206 ms and 284 ms on the earlier probes —
  the latency is comfortably inside the debounce interval, which is the assumption the whole
  design rests on).
- **Every worker CLI**: `pi` 1.3.0, `agy` 1.4.2, `claude` 2.1.299 — all three parsed, with
  token counts (17 655 / 7 650 / 6). `pi`'s default model now answers with content; the
  zero-token model of 2026-10-18 was specific to that model, and the `EMPTY` verdict added
  for it is what would have said so.
- **The documented refusals work**: `--worker pi --permissions sandboxed` without an external
  sandbox is refused at startup with the guidance, rather than silently degrading.
- **A supervised run completed the work**: `pi` created `HELLO.txt` containing `hello`, and a
  verifier ran on the result.

## The bug this run found

**The worker's prose was one word per line.** A backend sends its prose in *deltas* — `pi`
sends them a character at a time — and the decoder emitted each delta as a line of its own. The
readable log, which is what the model judges the worker by and what a human watches, looked
like this:

```text
Created
 `
HE
LL
O
.txt
`
```

The activity summary was unaffected (it accumulated the text separately), which is why
`doctor --probe-workers` reported a healthy 17 655 tokens while the log was unreadable. Fixed
by assembling the prose in the decoder and emitting it a line at a time.

## What still ends in an escalation, and why

The run ended `Escalate (worker limit reached before completion)`, and the cause is not the
agent. `pi`'s own extension leaves a `.pi-lens-probe-home/` directory in the working tree, so
the tree does not match "change nothing else" — and the agent says so itself: *"plus a
`.pi-lens-probe-home/` directory created by the tooling itself, not by me"*.

So the supervisor is being confused by the **agent's tooling**, not by the agent. That is
correct behaviour on aftman's part (the tree really did change) and a real operational fact
for whoever writes jobs: a strict "change nothing else" job cannot complete in an environment
where the agent's tooling writes to the tree. Ignore it, or write the job to allow for it.

## The numbers

- Jev latency: **260 ms** in the recorded probe (and 206 / 284 ms on the earlier ones).
- `pi`: 1.3.0, `agy`: 1.4.2, `claude`: 2.1.299. `agy` has moved from 1.3.5 since the last
  validation and every flag still holds.
- Three workers, then a verifier, then the escalation above.
