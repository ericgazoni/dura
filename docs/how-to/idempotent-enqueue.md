---
icon: lucide/copy-check
---

# How to avoid enqueuing the same task twice

This guide shows you how to make `spawn_task` safe to call more than once
for what is logically the same piece of work, for example from a
retried API request, a redelivered webhook, or a script that might be run
twice by mistake.

## Pass an idempotency key

```python
engine.spawn_task(
    name="charge_card",
    params={"order_id": order_id, "amount_cents": 4200},
    idempotency_key=f"charge:{order_id}",
)
```

Choose a key that uniquely identifies the logical operation, not the call
site: here, the order being charged, not a random UUID generated per
call (that would defeat the purpose).

## Check whether it was actually created

`spawn_task` always returns a `TaskRef`, whether or not it created a new
task:

```python
ref = engine.spawn_task(name="charge_card", params={...}, idempotency_key=key)
if not ref.created:
    print(f"already enqueued as {ref.task_id}")
```

When a task already exists for that key, no new task or run is created;
`ref.task_id` and `ref.run_id` point at the existing ones instead.

## Collapse duplicates created before you added a key

If duplicate tasks with the same name were already enqueued, for example
before you introduced an `idempotency_key`, clean them up once at
startup:

```python
surviving_id, cancelled_count = engine.cancel_duplicate_tasks("charge_card")
```

This keeps the oldest non-terminal task with that name and cancels the
rest. It's meant for a one-off cleanup, not routine use; going forward,
prevent duplicates at the source with `idempotency_key` instead.
