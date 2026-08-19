---
icon: lucide/heart-pulse
---

# How to expose health checks and metrics

This guide shows you how to wire up a liveness probe and Prometheus
metrics for a `dura` worker pool running under a supervisor like
Kubernetes or systemd.

## Start the combined server

```python
from dura import DurableEngine, Heartbeat, run_workers, start_metrics_and_health_server

heartbeat = Heartbeat()
start_metrics_and_health_server(port=8080, heartbeat=heartbeat, max_silence_seconds=30)

engine = DurableEngine("engine.db")
run_workers(engine, handlers=handlers, worker_count=4, heartbeat=heartbeat)
```

Pass the same `Heartbeat` instance to both calls. Every worker in the pool
beats it once per loop iteration, whether it just claimed a task or found
none, so it reflects the health of the pool as a whole, not any single
worker.

## Point your liveness probe at `/healthz`

`/healthz` (alias `/livez`) returns `200 alive` as long as at least one
worker has beaten within `max_silence_seconds`, and `503 stuck` otherwise.
A pool that's simply idle, with no work to claim, keeps beating and stays
healthy; only a fully wedged pool (every worker blocked, e.g. all stuck on
a dead downstream dependency) reports unhealthy.

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 8080
  periodSeconds: 15
```

Set `max_silence_seconds` comfortably above your worker loop's normal
cadence (poll interval plus typical handler duration), so a probe restart
isn't triggered by ordinary idle periods.

## Scrape `/metrics`

The same server serves Prometheus text-format metrics at `/metrics` (and
at `/`), from `prometheus_client`'s default registry: anything you or a
library you use has registered there is exposed automatically. `dura`
itself doesn't register any metrics of its own; if you want queue-depth or
task-outcome metrics, register your own counters/gauges and update them
around your calls to `spawn_task`, `complete_run`, and `fail_run`, or by
sampling `engine.ready_run_count()` periodically.

```yaml
scrape_configs:
  - job_name: dura-worker-pool
    static_configs:
      - targets: ["localhost:8080"]
```
