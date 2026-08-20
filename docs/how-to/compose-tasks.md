---
icon: lucide/git-branch
---

# How to chain tasks and fan them out

`dura` has no built-in concept of a "workflow" or a parent task. This
guide shows you how to build both a chain and a fan-out/fan-in out of the
same two primitives: a handler that calls `spawn_task`, and an
idempotency key that makes calling it twice safe.

## Chain one task into another

To run task B once task A finishes, have A's handler spawn B as its last
step. Wrap the spawn in a checkpoint so a retry of A (after a crash, or
after `fail_run`) does not spawn a second B:

```python
def fetch_report(engine, task):
    rows = fetch_rows_from_somewhere()

    engine.checkpoint(
        task_id=task.task_id,
        step_name="spawn_next",
        fn=lambda: engine.spawn_task(
            name="summarize_report",
            params={"rows": rows},
        ).task_id,
    )
    return {"fetched": len(rows)}


def summarize_report(engine, task):
    return {"summary": summarize(task.params["rows"])}
```

The checkpoint stores the child's `task_id` (a plain string, so it's
JSON-serializable). Any later call for the same `(task_id, "spawn_next")`
returns that same id instead of calling `spawn_task` again. See [How to
checkpoint steps](checkpoint-steps.md) for the underlying mechanism.

## Fan a task out into many sub-tasks

To have one task create several independent sub-tasks, spawn them in a
loop. Give each an idempotency key derived from the parent's `task_id`
plus something that varies per child, so a retry of the parent doesn't
spawn the batch twice:

```python
def start_batch_import(engine, task):
    uris = list_files(task.params["bucket"])
    batch_id = task.task_id

    for uri in uris:
        engine.spawn_task(
            name="import_file",
            params={"uri": uri, "batch_id": batch_id},
            idempotency_key=f"import:{batch_id}:{uri}",
        )
    return {"queued": len(uris)}
```

`dura` doesn't track a parent/child relationship between these tasks
itself. Passing `batch_id` through `params`, the way this example does,
is how the sub-tasks find their way back to the batch they belong to.

## Fan back in: know when every sub-task is done

`dura` has no built-in join or wait-group. Build one out of durable state
(as a countdown) and an event (to signal completion), the same primitives
as the rest of this guide:

```python
def start_batch_import(engine, task):
    uris = list_files(task.params["bucket"])
    batch_id = task.task_id

    engine.set_state(namespace=f"batch:{batch_id}", key="remaining", value=len(uris))
    for uri in uris:
        engine.spawn_task(
            name="import_file",
            params={"uri": uri, "batch_id": batch_id},
            idempotency_key=f"import:{batch_id}:{uri}",
        )
    return {"queued": len(uris)}


def import_file(engine, task):
    batch_id = task.params["batch_id"]
    do_import(task.params["uri"])

    remaining = engine.update_state(
        namespace=f"batch:{batch_id}", key="remaining", fn=lambda n: n - 1
    )
    if remaining == 0:
        engine.emit_event(event_name=f"batch_done:{batch_id}")
    return {"done": True}
```

Set the count before spawning any child, not after, so a child that
finishes unusually fast never decrements a counter that hasn't been
written yet. Any other task can then wait on the batch the normal way:

```python
def notify_batch_complete(engine, task):
    engine.wait_for_event(
        run_id=task.run_id,
        task_id=task.task_id,
        step_name="wait",
        event_name=f"batch_done:{task.params['batch_id']}",
        timeout_secs=3600,
    )
    send_notification(task.params["batch_id"])
```

Because `emit_event` is first-write-wins, it doesn't matter whether the
waiter registers before or after the last `import_file` finishes, or
which task gets claimed first. See [How to wait for
events](wait-for-events.md) and [How to use durable
state](use-durable-state.md) for the primitives this pattern is built
from.
