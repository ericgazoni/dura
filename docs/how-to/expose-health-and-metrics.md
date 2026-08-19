---
icon: lucide/heart-pulse
---

# How to expose health checks and metrics

`dura` has zero runtime dependencies and starts no servers or background
threads on your behalf. Both liveness and metrics are things you wire up
yourself, using pieces `dura` gives you: a `Heartbeat` object for liveness,
and the SQLite database itself for everything else.

## Detect a wedged worker pool with `Heartbeat`

```python
from dura import DurableEngine, Heartbeat, run_workers

heartbeat = Heartbeat()

engine = DurableEngine("engine.db")
run_workers(engine, handlers=handlers, worker_count=4, heartbeat=heartbeat)
```

Every worker in the pool beats `heartbeat` once per loop iteration, whether
it just claimed a task or found none, so it reflects the health of the pool
as a whole, not any single worker. A pool that's simply idle, with no work
to claim, keeps beating; only a fully wedged pool (every worker blocked,
e.g. all stuck on a dead downstream dependency) goes silent.

`Heartbeat` is a plain in-memory object (it opens no sockets and starts no
threads) so exposing it is entirely up to you and your app's existing
supervision. For example, with your own HTTP server:

```python
@app.get("/healthz")
def healthz():
    silence = heartbeat.seconds_since_beat()
    max_silence_seconds = 30
    if silence < max_silence_seconds:
        return Response("alive", status_code=200)
    return Response("stuck", status_code=503)
```

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 8080
  periodSeconds: 15
```

Set your threshold comfortably above the worker loop's normal cadence (poll
interval plus typical handler duration), so a probe restart isn't triggered
by ordinary idle periods. A script or desktop app that doesn't need a
liveness probe can just skip `heartbeat` entirely (it defaults to `None`).

## Metrics: read the built-in queries, or query the database yourself

`dura` doesn't ship a metrics registry, exporter, or HTTP endpoint, and
doesn't register anything with any observability library you use, that
would be one more opinion imposed on an app that may already have its own.
Instead, `DurableEngine` exposes the handful of queries most people end up
writing by hand:

```python
engine.ready_run_count()
# -> 7  (claimable right now, across all priorities: the queue-depth gauge)

engine.task_counts_by_state()
# -> {"pending": 5, "running": 2, "completed": 140, "failed": 3}

engine.task_counts_by_name_and_state()
# -> {"send_file": {"pending": 3, "completed": 90}, "charge_card": {"failed": 3, "completed": 50}}

engine.recent_failures(limit=20)
# -> [FailureInfo(run_id=..., task_id=..., task_name="charge_card", attempt=3,
#                  failed_at="2026-08-19T10:03:11+00:00",
#                  failure_reason={"type": "TimeoutError", "message": "..."}), ...]
```

These run as plain `SELECT`s on the engine's own connection: no new
process, no special setup. Feed whatever you sample into your own metrics
system (Prometheus, StatsD, logs, whatever your app already uses) on
whatever schedule you like.

For anything these don't cover, every task, run, checkpoint, and event
lives in one SQLite file opened in WAL mode, so a separate read-only
process (or a thread in the same process) can query it directly while the
workers run:

```python
import sqlite3
from datetime import datetime, timedelta, timezone

conn = sqlite3.connect("file:engine.db?mode=ro", uri=True)
one_hour_ago = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
failed_last_hour = conn.execute(
    "SELECT COUNT(*) FROM runs WHERE state = 'failed' AND failed_at > ?",
    (one_hour_ago,),
).fetchone()[0]
```
