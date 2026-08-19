---
icon: lucide/arrow-up-narrow-wide
---

# How to stop a low-priority backlog from starving urgent work

This guide shows you how to make sure latency-sensitive tasks get claimed
ahead of a backlog of bulk or maintenance work, using priorities and, if
that's not enough on its own, dedicated worker lanes.

## Give urgent tasks a higher priority

Every task has a `priority` (default `0`, the lowest band). Workers claim
the highest-priority claimable run first, breaking ties by availability
and then insertion order:

```python
engine.spawn_task(name="send_password_reset", params={...}, priority=10)
engine.spawn_task(name="reindex_catalog", params={...})  # priority=0
```

With a shared worker pool, `send_password_reset` is claimed first even if
`reindex_catalog` was enqueued earlier and has a huge backlog. Retried runs
inherit their task's original priority automatically: you don't need to
set it again in `fail_run`.

## When priority alone isn't enough

If every worker is currently busy running long low-priority tasks, a
higher-priority task still has to wait for one to free up: priority only
affects claim order, not preemption. If that's a problem, dedicate some
workers to a lane that only claims low-priority work, so the rest of the
pool is always available for urgent tasks:

```python
from dura import run_workers

run_workers(
    engine,
    handlers=handlers,
    worker_count=6,
    lanes=[(0, 2)],  # 2 workers only claim priority <= 0
)
```

The remaining 4 workers here are unrestricted and claim from the full
queue, urgent or not. Workers in a lane pass `max_priority` to
`claim_task` under the hood; you can do the same directly if you're
driving your own claim loop:

```python
engine.claim_task(worker_id="bulk-worker-1", max_priority=0)
```

## Check how deep the backlog is

```python
engine.ready_run_count()  # number of runs claimable right now, across all priorities
```

Useful as a queue-depth gauge to decide whether to add lanes or workers at
all, before you reach for either.
