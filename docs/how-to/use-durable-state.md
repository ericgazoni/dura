---
icon: lucide/database-zap
---

# How to store cross-task durable state

This guide shows you how to keep values that outlive any single task (a
poller's cursor, a high-water mark, a "have I already seen this?" record)
in `dura`'s durable key-value store.

## Read and write a single key

State is scoped by a `namespace` you choose, plus a `key` within it:

```python
last_seen = engine.get_state(namespace="poller:orders", key="cursor", default=0)
# ... fetch orders newer than last_seen ...
engine.set_state(namespace="poller:orders", key="cursor", value=new_cursor)
```

`set_state` is an upsert: the latest write for a `(namespace, key)` wins.
`get_state` returns `default` (defaulting to `None`) if the key has never
been written.

## Update a value atomically

If two workers might update the same key concurrently (incrementing a
counter, appending to a dedup set), don't do a `get_state` followed by a
`set_state`: there's a race between them. Use `update_state` instead. It
runs your function inside the write transaction.

```python
def bump(current):
    return current + 1

engine.update_state(namespace="stats", key="processed_count", fn=bump, default=0)
```

Keep the function pure and fast, with no I/O, since it runs while the
database write lock is held.

## Write many keys at once

Loop-calling `set_state` for a bulk import commits once per key. Use
`set_state_many` to write a whole batch in a single transaction:

```python
engine.set_state_many(namespace="dedup:orders", items={order_id: True for order_id in seen})
```

## List or check a namespace

```python
engine.list_state("poller:orders")     # {"cursor": 42, ...}
engine.has_state("poller:orders")      # True, without loading any values
```

## Delete a key

```python
engine.delete_state(namespace="poller:orders", key="cursor")  # True if it existed
```

## Remember: state is not cleaned up

Unlike checkpoints, durable state is never touched by `cleanup()`. It's
meant to survive the tasks that wrote it. If a value really is scoped to
one task's lifetime, use a [checkpoint](checkpoint-steps.md) instead.
