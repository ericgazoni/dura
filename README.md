# dura

A SQLite-backed durable execution engine and worker pool for Python.

`dura` persists tasks, runs, checkpoints, events, waits and durable
key-value state in a single SQLite database, so scheduled work survives
process restarts and crashes. It ships with a reference worker-pool driver
(`dura.workers`) and a liveness heartbeat/health-check HTTP server
(`dura.health`) for running it under a process supervisor such as
Kubernetes.

## Installation

```bash
pip install dura
```

## Usage

```python
from dura import DurableEngine, run_workers

engine = DurableEngine("engine.db")

engine.spawn_task("send_file", {"uri": "/a.csv"})

def send_file(engine, task):
    return {"sent": task.params["uri"]}

run_workers(engine, handlers={"send_file": send_file}, worker_count=4)
```

See the docstrings in `dura/engine.py` and `dura/workers.py` for the full
API: retries, checkpoints, durable state, events/waits, priorities and
graceful shutdown.

## Development

```bash
cd packages/dura
poetry install
poetry run pytest
```
