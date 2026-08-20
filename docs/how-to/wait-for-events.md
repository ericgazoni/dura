---
icon: lucide/bell
---

# How to suspend a task until an external event arrives

This guide shows you how to pause a running task until something outside
`dura` happens: a human approval, a webhook, another task finishing. It
resumes from where it left off, without polling or holding a worker
thread the whole time.

## Wait on a named event

```python
from dura import WorkflowSuspended

def await_approval(engine, task):
    payload = engine.wait_for_event(
        run_id=task.run_id,
        task_id=task.task_id,
        step_name="approval",
        event_name=f"order_approved:{task.params['order_id']}",
        timeout_secs=3600,
    )
    if payload is None:
        return {"status": "timed_out"}
    return {"status": "approved", "by": payload["approver"]}
```

Give the event a name unique to what you're waiting for: here, one order's
approval, not "approved" in general, since events are global and
first-write-wins.

## Let `WorkflowSuspended` propagate

`wait_for_event` raises `WorkflowSuspended` when the run needs to park.
Don't catch it. Let it unwind out of your handler.

A worker pool built on `process_task`/`run_workers` catches it for you
and simply leaves the run alone. It's neither completed nor failed, just
parked, and gets reclaimed automatically when the event fires or the
timeout elapses.

If you've written your own claim loop instead of using `dura.workers`,
catch `WorkflowSuspended` around the handler call and skip settling the
run when you see it.

## Emit the event

From wherever the approval actually happens (another task, a web request
handler, anything with access to the same `DurableEngine`):

```python
engine.emit_event(
    event_name=f"order_approved:{order_id}",
    payload={"approver": "alice"},
)
```

The first `emit_event` call for a given `event_name` wins. Later calls
for the same name are ignored. If nothing is waiting yet, the event is
still recorded: a later call to `wait_for_event` with that name resolves
immediately instead of parking.

## Handle the timeout case

If you passed `timeout_secs`, `wait_for_event` returns `None` once the
deadline passes without the event firing. It doesn't raise. Always
handle that case explicitly, as in `await_approval` above. Otherwise a
task that never gets approved hangs forever, since `timeout_secs`
defaults to `None`.

## Reference

See [`await_event`, `wait_for_event`, and
`emit_event`](../reference/engine.md) for the exact return shapes,
including the lower-level `await_event` if you need the raw
`(should_suspend, payload)` tuple instead of the exception-based flow.
