# dura

[![PyPI](https://img.shields.io/pypi/v/dura.svg)](https://pypi.org/project/dura/)
[![Documentation](https://img.shields.io/badge/docs-ericgazoni.github.io%2Fdura-blue)](https://ericgazoni.github.io/dura/)
[![License](https://img.shields.io/pypi/l/dura.svg)](https://github.com/ericgazoni/dura/blob/main/LICENSE)

A small, embeddable, SQLite-backed durable execution engine and worker pool for Python.

Full documentation: **https://ericgazoni.github.io/dura/**

`dura` runs entirely inside your process: one SQLite database holds every
task, run, checkpoint, event, wait and durable key-value entry, so scheduled
work survives crashes and restarts without a separate workflow server,
message broker, or database cluster to operate. It ships with a reference
worker-pool driver (`dura.workers`) and a liveness heartbeat / health-check
HTTP server (`dura.health`) for running under a process supervisor such as
Kubernetes or systemd.

## Key features

- **Durable tasks and runs** - a task is the logical job; each execution
  attempt is a run. Crashes reclaim in-flight runs automatically (leases
  expire and are picked back up) without consuming a retry, since a crash is
  not a logical failure.
- **Retries with backoff** - `none`, `fixed`, or `exponential` retry
  strategies with jitter, configured per task.
- **Checkpoints** - durable memoization of a step's result, keyed to the
  task. A checkpointed step runs at most once, even across a crash and
  retry of the surrounding handler.
- **Durable key-value state** - a namespaced store for cross-task memory
  (cursors, watermarks, dedup records) that outlives the tasks that wrote
  it and is never touched by cleanup.
- **Events and suspend/resume** - a handler can wait on a named event with
  an optional timeout; the run parks itself (freeing the worker) and is
  woken by `emit_event` or by the timeout, without polling.
- **Priorities and lanes** - tasks carry a priority; worker "lanes" can be
  restricted to claim only up to a given priority ceiling, so a backlog of
  low-priority work never starves latency-sensitive tasks.
- **Idempotent enqueue** - `spawn_task(..., idempotency_key=...)` returns
  the existing task instead of creating a duplicate.
- **Graceful worker pool** - a threaded pool with signal handling
  (SIGINT/SIGTERM), a bounded shutdown grace period, and clean WAL
  checkpointing on close.
- **Health, no dependencies imposed** - a `Heartbeat` for pool-wide
  liveness that opens no sockets and starts no threads; expose it however
  fits your app, and query the SQLite database directly for metrics.

## Installation

```bash
pip install dura
```

`dura` has zero runtime dependencies.

## Quick start

```python
from dura import DurableEngine, run_workers

engine = DurableEngine("engine.db")

engine.spawn_task(name="send_file", params={"uri": "/a.csv"})

def send_file(engine, task):
    return {"sent": task.params["uri"]}

run_workers(engine, handlers={"send_file": send_file}, worker_count=4)
```

Run the same script again after a crash or `SIGKILL` mid-task: `dura`
reclaims the in-flight run from the SQLite file and retries or resumes it,
no external state to reconcile.

## Checkpoints

```python
def import_report(engine, task):
    rows = engine.checkpoint(
        task_id=task.task_id,
        step_name="download",
        fn=lambda: fetch_from_sftp(task.params["uri"]),
    )
    engine.checkpoint(task_id=task.task_id, step_name="load", fn=lambda: load_into_db(rows))
    return {"rows": len(rows)}
```

If the process dies after `download` completes but before `load` runs, a
retry re-executes the handler but `download` returns its stored result
immediately: the SFTP fetch does not happen twice.

## Learn more

This README only covers the basics. See the [full documentation](https://ericgazoni.github.io/dura/):

- [Tutorial](https://ericgazoni.github.io/dura/tutorial/): build a task, run it, crash it on purpose, watch it recover.
- How-to guides:
  - [Retries](https://ericgazoni.github.io/dura/how-to/configure-retries/)
  - [Durable state](https://ericgazoni.github.io/dura/how-to/use-durable-state/)
  - [Events and waiting](https://ericgazoni.github.io/dura/how-to/wait-for-events/)
  - [Composing and chaining tasks](https://ericgazoni.github.io/dura/how-to/compose-tasks/)
  - [Schedule recurring tasks](https://ericgazoni.github.io/dura/how-to/schedule-recurring-tasks/)
  - [Priorities and lanes](https://ericgazoni.github.io/dura/how-to/prioritize-tasks/)
  - [Running a worker pool](https://ericgazoni.github.io/dura/how-to/run-worker-pool/)
  - [Health checks and metrics](https://ericgazoni.github.io/dura/how-to/expose-health-and-metrics/)
- [API reference](https://ericgazoni.github.io/dura/reference/engine/): the full `DurableEngine`, worker pool, and heartbeat API.
- [Explanation](https://ericgazoni.github.io/dura/explanation/durability-model/): how the durability model works, and when `dura` is (and isn't) the right tool.

## Development

```bash
uv sync
uv run pytest
```
