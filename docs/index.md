---
icon: lucide/house
---

# dura

`dura` is a small, SQLite-backed durable execution engine and worker pool
for Python. One database file holds every task, run, checkpoint, event,
wait, and durable key-value entry. Scheduled work survives crashes and
restarts without a separate workflow server, message broker, or database
cluster.


## Installation

```bash
pip install dura
```

`dura` requires Python 3.13 or later and has zero runtime dependencies.

## Where to start

- **[Tutorial: your first durable task](tutorial.md)**: new to `dura`?
  Build a task, run it, crash it on purpose, and watch it pick up where it
  left off.
- **[How-to guides](how-to/configure-retries.md)**: already have a `dura`
  app running? Recipes for retries, checkpoints, events, durable state,
  priorities, worker pools, and health checks.
- **[Reference](reference/engine.md)**: the full API for `DurableEngine`,
  the worker pool, and the liveness heartbeat.
- **[Explanation](explanation/durability-model.md)**: how the durability
  model actually works, and when `dura` is (and isn't) the right tool.
