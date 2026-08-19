# dura

[![PyPI](https://img.shields.io/pypi/v/dura.svg)](https://pypi.org/project/dura/)
[![Documentation](https://img.shields.io/badge/docs-ericgazoni.github.io%2Fdura-blue)](https://ericgazoni.github.io/dura/)
[![License](https://img.shields.io/pypi/l/dura.svg)](https://github.com/ericgazoni/dura/blob/main/LICENSE)

`dura` is built to be tough as nails: a laptop that goes to sleep mid-script,
a process that gets OOM-killed mid-charge, a server that loses power
mid-order, a device that drops off the network for an hour - none of it
should be able to take your work down with it. Without a plan for that,
interrupted work either happens twice (a customer charged twice, a
duplicate email) or silently never resumes (a stuck order nobody notices
until a customer complains).

`dura` picks interrupted work back up exactly where it left off - never
redoing what already succeeded, never losing track of what's left - and
it earns that toughness by depending on nothing: no message broker, no
workflow server, no database cluster, not even a network connection.
Everything lives in one SQLite file inside your own process, so there is
nothing else that can go down, drift out of sync with reality, or need
its own on-call rotation. Fewer moving parts, fewer ways to break.

That's also what makes it the hands-on alternative to heavier setups like
[Edda](https://github.com/i2y/edda) or
[Absurd](https://earendil-works.github.io/absurd/), both of which
inspired it: wrap a script in a durable task in five minutes, then reuse
the same primitives to grow it into a small daemon that shrugs off
crashes, restarts, and dropped connections.

It's built for a single process, or a handful of independent ones: a
script you want to be able to kill and re-run safely, a simple
application that can't afford to fail outright, or a device that can't
rely on networked resources at all (embedded hardware, intermittent
connectivity, air-gapped environments) - and it's already running in
production doing exactly that.

It's not built to coordinate work
across many machines or services sharing one queue; see [scope and
alternatives](https://ericgazoni.github.io/dura/explanation/scope-and-alternatives/)
for that boundary and what to reach for instead.

Full documentation: **https://ericgazoni.github.io/dura/**

## Key features

- **Depends on nothing** - the entire engine is one SQLite file: no
  broker, server, or cluster to run alongside your app, and no network
  connection required at all. There's nothing else that can be down.
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
import time
from dataclasses import dataclass

from dura import DurableEngine, run_workers


@dataclass
class Order:
    id: str
    customer_id: str
    amount_cents: int


def fetch_pending_orders():
    # Stand-in for wherever your orders actually come from.
    for n in range(1, 21):
        yield Order(id=f"ord_{n}", customer_id=f"cust_{n}", amount_cents=1000 + n * 100)


def charge_card(*, customer_id, amount_cents, idempotency_key):
    # Stand-in for a real payment gateway call.
    time.sleep(1)
    print(f"charged {customer_id} {amount_cents}c ({idempotency_key})")


def send_receipt(customer_id, order_id):
    # Stand-in for a real email/notification call.
    time.sleep(1)
    print(f"receipt sent to {customer_id} for {order_id}")


def charge_order(engine, task):
    engine.checkpoint(
        task_id=task.task_id,
        step_name="charge",
        fn=lambda: charge_card(
            customer_id=task.params["customer_id"],
            amount_cents=task.params["amount_cents"],
            idempotency_key=f"charge:{task.task_id}",
        ),
    )
    engine.checkpoint(
        task_id=task.task_id,
        step_name="receipt",
        fn=lambda: send_receipt(task.params["customer_id"], task.params["order_id"]),
    )
    return {"charged": task.params["order_id"]}


engine = DurableEngine("engine.db")

for order in fetch_pending_orders():
    engine.spawn_task(
        name="charge_order",
        params={"order_id": order.id, "customer_id": order.customer_id, "amount_cents": order.amount_cents},
        idempotency_key=f"charge_order:{order.id}",
    )

run_workers(engine, handlers={"charge_order": charge_order}, worker_count=4)
```

Save this as `quickstart.py` and run it with `python quickstart.py`. It
starts charging its way through 20 orders, four at a time; press
**Ctrl+C** partway through, well before it reaches the last one. `dura`
stops claiming new work, lets whatever's currently in flight finish, and
exits.

Run the script again: the orders that were already charged don't get
charged twice - `idempotency_key` on `spawn_task` skips re-queuing them,
and the `charge` checkpoint means a charge that already succeeded is
never retried - while the ones the batch hadn't reached yet pick up right
where it left off. Reach for `kill -9` instead of Ctrl+C and it recovers
exactly the same way: `dura` doesn't depend on a graceful shutdown to
stay correct, only to be tidy about it.

## Learn more

This README only covers the basics. See the [full documentation](https://ericgazoni.github.io/dura/):

- [Tutorial](https://ericgazoni.github.io/dura/tutorial/): build a task, run it, crash it on purpose, watch it recover.
- How-to guides:
  - [Retries](https://ericgazoni.github.io/dura/how-to/configure-retries/)
  - [Checkpointing steps](https://ericgazoni.github.io/dura/how-to/checkpoint-steps/)
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
