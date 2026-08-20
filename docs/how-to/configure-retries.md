---
icon: lucide/refresh-cw
---

# How to configure retries for a task

This guide shows you how to control what happens when a task's handler
raises an exception: how many times it's retried, and how long `dura`
waits between attempts.

## Set a retry limit

By default a failed run is retried forever. To cap the number of attempts,
pass `max_attempts` to `spawn_task`:

```python
engine.spawn_task(name="charge_card", params={...}, max_attempts=5)
```

Once the 5th attempt fails, the task's state becomes `failed` and it stops
being retried. Call `engine.get_task(task_id)` to read the stored
`failure_reason` from the last attempt.

## Add backoff between attempts

Without a retry strategy, a failed run becomes claimable again immediately.
To back off, pass a `RetryStrategy`:

```python
from dura import RetryStrategy

engine.spawn_task(
    name="charge_card",
    params={...},
    max_attempts=5,
    retry=RetryStrategy(kind="exponential", base_seconds=1, factor=2, max_seconds=60),
)
```

- Use `kind="fixed"` to always wait `base_seconds` between attempts.
- Use `kind="exponential"` to wait `base_seconds * factor ** (attempt - 1)`,
  capped at `max_seconds` if you set it.
- Leave `retry` unset (or use `kind="none"`, the default) for no delay.

By default, some jitter is added on top of the computed delay, to avoid
many failed tasks retrying in lockstep. Tune it with `jitter_factor`, or
set it to `0` to disable it. See the [`RetryStrategy`
reference](../reference/engine.md) for every field and its default.

## Give retries a lower priority than fresh work, or not

A retried run inherits the `priority` its task was spawned with; you don't
need to do anything for that to happen. If you want retries to be
deprioritized relative to fresh work instead, see
[How to prioritize tasks](prioritize-tasks.md).

## Inspect why a task failed

```python
info = engine.get_task(task_id)
if info.state == "failed":
    print(info.failure_reason)  # {"type": "ValueError", "message": "..."}
```

`failure_reason` is whatever `process_task` recorded from the exception the
handler raised the last time it ran: the exception's type name and message.
