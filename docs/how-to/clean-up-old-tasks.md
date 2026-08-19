---
icon: lucide/trash-2
---

# How to clean up old tasks

This guide shows you how to bound the growth of `engine.db` by removing
finished tasks you no longer need to query.

## Run `cleanup()` on a schedule

```python
from datetime import timedelta

removed = engine.cleanup(ttl=timedelta(days=30))
print(f"removed {removed} terminal tasks")
```

`cleanup()` deletes tasks (and their runs, checkpoints, and waits) that are
in a terminal state (`completed`, `failed`, or `cancelled`) and reached
that state longer ago than `ttl`. It runs a `VACUUM` afterwards if
anything was deleted, so the database file actually shrinks on disk rather
than just marking space free.

Call it periodically from wherever fits your deployment: a dedicated
low-priority task that re-enqueues itself, a cron job that opens the same
`engine.db` and calls `cleanup()` once, or a maintenance script run by your
supervisor.

## Pick a `ttl` you can still debug against

Anything still within `ttl` remains queryable via `get_task`, which is
often the only record you have of what a task did. Set `ttl` to the
shortest window that still covers your debugging and audit needs, not the
shortest window that keeps the file small: the file staying small is a
side effect, not the goal.

## Durable state is never touched

`cleanup()` only removes terminal tasks and the rows keyed to them; the
durable state store (`get_state`/`set_state`, see [How to use durable
state](use-durable-state.md)) is deliberately left alone, since it's
meant to outlive any task that wrote to it. If you need to expire state
too, do it explicitly with `delete_state`.
