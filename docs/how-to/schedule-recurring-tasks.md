---
icon: lucide/repeat
---

# How to make a task reschedule itself

`dura` has no built-in cron or scheduler. This guide shows you how to
build a recurring task out of a handler that spawns the next occurrence of
itself before returning, using an idempotency key to keep the chain from
forking or stalling.

## Spawn the next occurrence from the handler

Have the handler spawn its own next run before returning, with
`available_after` controlling the delay. Give each occurrence a unique
`idempotency_key`, computed from something that changes every time, so a
retry of the current run cannot spawn two next-occurrences:

```python
from datetime import timedelta

def poll_orders(engine, task):
    run_number = task.params.get("run_number", 0) + 1

    new_orders = fetch_new_orders()
    handle(new_orders)

    engine.spawn_task(
        name="poll_orders",
        params={"run_number": run_number},
        available_after=timedelta(seconds=30),
        idempotency_key=f"poll_orders:{run_number}",
    )
    return {"processed": len(new_orders)}
```

Seed the first occurrence once, at startup, the same way:

```python
engine.spawn_task(name="poll_orders", params={"run_number": 0}, idempotency_key="poll_orders:0")
```

## Derive the key from something that never repeats

Don't derive the key from data that might repeat between runs (a cursor
that can go a whole cycle without moving, for instance): if the next
occurrence's key ever matches the *current* task's own key, `spawn_task`
returns the current task itself instead of creating a new one, and the
chain silently stops. `run_number` (or any strictly-incrementing counter)
avoids that.

## Clean up duplicates from before you had a key

If a chain like this was ever seeded more than once, for example by
application code that called `spawn_task` unconditionally on every
deploy, collapse the duplicates once with `cancel_duplicate_tasks`; see
[How to avoid enqueuing the same task
twice](idempotent-enqueue.md#collapse-duplicates-created-before-you-added-a-key).
