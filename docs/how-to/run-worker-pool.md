---
icon: lucide/server
---

# How to run and shut down a worker pool

This guide shows you how to start a pool of workers against a
`DurableEngine`, shut it down cleanly under a process supervisor, extend a
claim for a long-running step, and cancel a task in flight.

## Start the pool

```python
from dura import DurableEngine, run_workers

engine = DurableEngine("engine.db")
run_workers(engine, handlers={"send_file": send_file}, worker_count=4)
```

`run_workers` starts `worker_count` threads, each running its own claim
loop against `handlers`, and blocks the calling thread. Call it from your
main thread: it installs `SIGINT`/`SIGTERM` handlers, which only works on
the main thread.

## Let it shut down gracefully

Send `SIGINT` (Ctrl-C) or `SIGTERM` (what `docker stop` and Kubernetes send
by default). `run_workers` then:

1. stops claiming new work,
2. gives in-flight handlers up to `dura.workers.SHUTDOWN_GRACE_SECONDS`
   (10 seconds) to finish,
3. closes every connection so the database is left without `-wal`/`-shm`
   sidecar files.

A handler still running when the grace period expires is abandoned, not
killed; that's safe, since its run's lease will simply expire and get
reclaimed on the next start, the same as a crash. If your handlers can
legitimately take longer than that to reach a safe stopping point, design
them around checkpoints (see [How to checkpoint
steps](checkpoint-steps.md)) rather than relying on a longer grace period.

## Extend a claim for a long step

A run's claim (lease) expires after `claim_timeout_secs` (120 seconds by
default when going through `run_workers`). If a handler is about to run a
step you know will take longer than that, extend it first so another
worker doesn't reclaim the run out from under you:

```python
def handler(engine, task):
    engine.extend_claim(run_id=task.run_id, by_secs=600)
    do_the_slow_thing()
    ...
```

## Cancel a task in flight

```python
engine.cancel_task(task_id)
```

This marks the task and any of its non-terminal runs `cancelled` and
clears any pending waits. A worker that later tries to settle a cancelled
run gets `TaskCancelledError`; `process_task` already handles that for you
and simply drops the run.

## Drive the engine yourself instead

If you don't want threads, signal handling, or `dura.workers`' opinions at
all, write your own loop around the same primitives it uses:

```python
while True:
    task = engine.claim_task(worker_id="my-worker")
    if task is None:
        time.sleep(1)
        continue
    try:
        result = handlers[task.name](engine, task)
    except WorkflowSuspended:
        continue
    except Exception as exc:
        engine.fail_run(run_id=task.run_id, reason={"type": type(exc).__name__, "message": str(exc)})
        continue
    engine.complete_run(run_id=task.run_id, result=result)
```

See [`dura.workers`' reference](../reference/workers.md) for the exact
behavior `process_task` and `run_worker` add on top of this (settle-race
handling, logging, heartbeats) that you'd otherwise need to reimplement.
