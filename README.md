# dura

A small, embeddable, SQLite-backed durable execution engine and worker pool for Python.

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

## Core concepts

### Tasks, runs, and retries

```python
from dura import DurableEngine, RetryStrategy

engine = DurableEngine("engine.db")

engine.spawn_task(
    name="charge_card",
    params={"customer_id": "c_123", "amount_cents": 4200},
    max_attempts=5,
    retry=RetryStrategy(kind="exponential", base_seconds=1, factor=2, max_seconds=60),
    priority=10,
)
```

A worker claims a run, executes the handler, and calls back into the engine
to settle it - `complete_run` on success, `fail_run` on an unhandled
exception. `run_workers`/`process_task` do this for you; see
`dura/workers.py` if you want to drive the engine with your own loop.

### Checkpoints

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
immediately - the SFTP fetch does not happen twice.

### Durable state

```python
last_seen = engine.get_state(namespace="poller:orders", key="cursor", default=0)
# ... fetch orders newer than last_seen ...
engine.set_state(namespace="poller:orders", key="cursor", value=new_cursor)
```

Unlike checkpoints, state is not tied to a task and is never removed by
`cleanup()` - it is meant to outlive the work that wrote it.

### Events and waiting

```python
from dura import WorkflowSuspended

def await_approval(engine, task):
    payload = engine.wait_for_event(
        run_id=task.run_id,
        task_id=task.task_id,
        step_name="approval",
        event_name="order_approved",
        timeout_secs=3600,
    )
    if payload is None:
        return {"status": "timed_out"}
    return {"status": "approved", "by": payload["approver"]}

# elsewhere, when the approval arrives:
engine.emit_event(event_name="order_approved", payload={"approver": "alice"})
```

`wait_for_event` raises `WorkflowSuspended` to unwind the handler when the
run needs to park; a worker pool built on `dura.workers` handles that
transparently, so the run is simply left alone until it is woken.

### Composing tasks

`dura` has no built-in notion of a workflow, a parent task, or a schedule.
All three are built from the same two primitives: a handler that calls
`spawn_task`, and an idempotency key that makes calling it twice safe.

Chain one task into another by spawning the next step as the last thing a
handler does, wrapped in a checkpoint so a retry can't spawn it twice:

```python
def fetch_report(engine, task):
    rows = fetch_rows_from_somewhere()
    engine.checkpoint(
        task_id=task.task_id,
        step_name="spawn_next",
        fn=lambda: engine.spawn_task(name="summarize_report", params={"rows": rows}).task_id,
    )
    return {"fetched": len(rows)}
```

A task can reschedule itself the same way, to build a recurring poller,
by spawning its own next occurrence with a fresh `idempotency_key` before
returning. See [the recurring-tasks how-to
guide](docs/how-to/schedule-recurring-tasks.md) for the gotchas around
choosing that key:

```python
from datetime import timedelta

def poll_orders(engine, task):
    run_number = task.params.get("run_number", 0) + 1
    handle(fetch_new_orders())
    engine.spawn_task(
        name="poll_orders",
        params={"run_number": run_number},
        available_after=timedelta(seconds=30),
        idempotency_key=f"poll_orders:{run_number}",
    )
    return {"processed": True}
```

A task can also fan out into many sub-tasks (spawn one per item, tagging
each with a shared `batch_id`), and fan back in by pairing durable state
(a countdown of how many remain) with an event that fires when it hits
zero. See [the chain-and-fan-out how-to
guide](docs/how-to/compose-tasks.md) for the full chaining and
fan-out/fan-in patterns.

### Concurrency model

SQLite allows a single writer at a time. Every mutating operation runs
inside a `BEGIN IMMEDIATE` transaction, so concurrent writers serialize
(they wait, they do not deadlock) instead of failing late with
`SQLITE_BUSY`. Connections are per-thread and coordinate through SQLite's
own file locking, with WAL mode enabled for concurrent readers. This is
built and tested for multiple threads inside a single process - the model
`dura.workers` implements - not for coordinating multiple separate
processes or machines against the same database file.

### Health and metrics

`run_workers` takes an optional `heartbeat: Heartbeat | None = None`.
When you do need liveness (e.g. under Kubernetes), `Heartbeat` is a plain
in-memory object: it opens no sockets and starts no threads - so `dura`
never imposes an HTTP server or a metrics library on your app; you expose
`seconds_since_beat()` however fits your app. 

For metrics, `DurableEngine` exposes the common queries directly -
`ready_run_count()` for queue depth, `task_counts_by_state()` and
`task_counts_by_name_and_state()` for a breakdown, `recent_failures(limit=...)`
for the last N failures and their reason - or query the `tasks`/`runs`
tables yourself for anything else, safely from another process since the
database is opened in WAL mode. See [how to expose health checks and
metrics](docs/how-to/expose-health-and-metrics.md) for the full recipe.

See the docstrings in `dura/engine.py` and `dura/workers.py` for the full
API, including cancellation, claim extension, and lane-based worker pools.

## What dura is for

`dura` is deliberately small in scope. It targets local or isolated
applications: a single process (or a small number of independent,
non-clustered instances) that needs to survive crashes and restarts
cleanly and stay easy to reason about and debug - one file you can open
with the `sqlite3` CLI to see exactly what's queued, running, or stuck. There
is no cluster to stand up, no distributed lock service, and no plan to add
one: SQLite is the first-class (and only) storage backend, on purpose.

If you need workflows coordinated across multiple processes, pods, or
machines against a shared database - Postgres/MySQL-backed locking, saga
compensation, CloudEvents ingestion, multi-worker fan-out - look at
[Edda](https://github.com/i2y/edda), which is built for exactly that: a
durable execution framework designed to scale out across a cluster.

If you want `dura`'s single SQLite file to survive the loss of the machine
it runs on, pair it with [Litestream](https://litestream.io/), which
replicates a SQLite database continuously to object storage for point-in-time
recovery - without turning `dura` itself into a client of a remote database.

## Development

```bash
uv sync
uv run pytest
```
