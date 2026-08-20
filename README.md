# dura

[![PyPI](https://img.shields.io/pypi/v/dura.svg)](https://pypi.org/project/dura/)
[![Documentation](https://img.shields.io/badge/docs-ericgazoni.github.io%2Fdura-blue)](https://ericgazoni.github.io/dura/)
[![License](https://img.shields.io/pypi/l/dura.svg)](https://github.com/ericgazoni/dura/blob/main/LICENSE)

Long jobs get interrupted: a laptop sleeps, a process gets killed, a
server loses power, a connection drops. Without a plan for that, the work
either runs twice (a customer charged twice, a duplicate email) or
silently stalls (a stuck order nobody notices until a customer
complains).

`dura` picks that work back up exactly where it left off, without
redoing anything that already succeeded. It needs nothing else to do
that: no message broker, no workflow server, no database cluster, not
even a network connection. Everything lives in one SQLite file. 
Fewer moving parts, fewer ways to break.

You can turn a script into a durable workflow in five minutes, 
or make a small app crash-proof with a few lines of code.

It is designed for workflows where interruptions or breaks are expected, but side-effects are a problem.
It's already tested and running in production doing exactly that.

It's a lighter, hands-on alternative to setups like
[Edda](https://github.com/i2y/edda) and
[Absurd](https://earendil-works.github.io/absurd/), both of which
inspired served as inspiration.
See [scope and
alternatives](https://ericgazoni.github.io/dura/explanation/scope-and-alternatives/)
for that boundary, and what to reach for instead.

Full documentation: **https://ericgazoni.github.io/dura/**

## Key features

- **Depends on nothing**: one SQLite file. No broker, server, or cluster
  to run, and no network connection required.
- **Durable tasks and runs**: a task is the job, each attempt is a run.
- **Retries with backoff**: `none`, `fixed`, or `exponential` strategies
  with jitter, configured per task.
- **Checkpoints**: memoize a step's result, keyed to the task. A
  checkpointed step runs _at most once_, even across a crash and retry.
- **Durable key-value state**: a namespaced key-value store for cross-task memory
  (cursors, watermarks, dedup records).
- **Events and suspend/resume**: a handler waits on a named event, with
  an optional timeout. The run parks itself and wakes on `emit_event` or
  the timeout, no polling.
- **Priorities and lanes**: tasks carry a priority, and worker "lanes"
  can be restricted to a priority ceiling, so bulk work never starves
  urgent tasks.
- **Idempotent enqueue**: `spawn_task(..., idempotency_key=...)` returns
  the existing task instead of creating a duplicate.
- **Graceful worker pool**: signal handling (SIGINT/SIGTERM), a bounded
  shutdown grace period, and clean WAL checkpointing on close.
- **Health, no dependencies imposed**: a `Heartbeat` for pool-wide
  liveness that opens no sockets and starts no threads. Expose it
  however fits your app, and query the SQLite database directly for
  metrics.

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

run_workers(engine, handlers={"charge_order": charge_order}, worker_count=2)
```

Save this as `quickstart.py` and run
it with `python quickstart.py`. It
charges its way through 20 orders, two at a time. Press Ctrl+C partway
through, before it reaches the last one. `dura` stops claiming new work,
lets what's in flight finish, and exits.

Run the script again. The orders already charged don't get charged
twice: `idempotency_key` skips re-queuing them, and the `charge`
checkpoint means a successful charge is never retried. The orders the
batch hadn't reached yet just pick up where it left off.

Note: a `kill -9` instead of Ctrl+C recovers the same way. `dura` doesn't need
a graceful shutdown to stay correct, only to be tidy about it.

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
- [A complete example](https://ericgazoni.github.io/dura/examples/poll-hacker-news/): polling an API, fanning out, checkpointing, and rescheduling in one script.
- [`examples/`](examples/): this quick start and every script above, runnable as-is.

## Development

```bash
uv sync
uv run pytest
```
