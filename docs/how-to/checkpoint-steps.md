---
icon: lucide/save
---

# How to checkpoint the steps of a handler

This guide shows you how to make an individual step inside a task handler
durable, so that a retry or a crash-recovery re-run doesn't repeat it.

## Wrap a step in `checkpoint`

```python
def import_report(engine, task):
    rows = engine.checkpoint(
        task_id=task.task_id,
        step_name="download",
        fn=lambda: fetch_from_sftp(task.params["uri"]),
    )
    engine.checkpoint(
        task_id=task.task_id,
        step_name="load",
        fn=lambda: load_into_db(rows),
    )
    return {"rows": len(rows)}
```

The first time `checkpoint` runs for a given `(task_id, step_name)`, it
calls `fn`, stores the result, and returns it. Every later call for that
same task and step name returns the stored result immediately, without
calling `fn` again. That's true whether the call comes from a retry after
`fail_run`, or from a worker reclaiming an abandoned run after a crash.

## Choose step names carefully

Checkpoints are keyed on `(task_id, step_name)`, not on where in the code
the call appears. If a handler runs the same logical step twice with the
same `step_name` (say, inside a loop), the second call just gets the
first call's result.

Give each logically distinct step its own name. For a step inside a
loop, fold the iteration index into the step name, like `f"row-{i}"`.

## Keep `fn` itself fast to retry, slow to run

`fn` executes outside `dura`'s write transaction, so a slow step (an SFTP
fetch, an S3 upload) doesn't hold the database lock. But `fn` isn't
guarded against running concurrently with itself. If two workers somehow
claimed the same run at once, which `dura`'s leases otherwise prevent,
whichever commits first wins, and the other's result is discarded.

In practice, a single run is only ever claimed by one worker at a time.
This only matters if you're calling `checkpoint` directly, outside the
normal claim/complete lifecycle.

## Read a checkpoint without risking a computation

To check whether a step has already run, without triggering it:

```python
downloaded = engine.get_checkpoint(task_id=task.task_id, step_name="download")
if downloaded is None:
    ...  # hasn't run yet
```

## Difference from durable state

Checkpoints belong to a task and are deleted when that task is cleaned up
by `cleanup()`. If you need a value that outlives the task that computed
it (a cursor, a watermark, a dedup record), use durable state instead; see
[How to use durable state](use-durable-state.md).
