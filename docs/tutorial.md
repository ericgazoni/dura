---
icon: lucide/graduation-cap
---

# Your first durable task

In this tutorial we'll build a tiny `dura` task, run it, and watch it
complete. Then we'll make one of its steps durable, kill the process
mid-run, and restart it. You'll see it pick up exactly where it left off,
without redoing work or losing the task. Along the way we'll meet the
engine, the worker pool, and checkpoints.

You'll need Python 3.13 or later. Install `dura` into a fresh project:

```bash
pip install dura
```

## 1. Write a task

Create a file called `charge_order.py`:

```python
--8<-- "examples/charge_order_once.py"
```

`DurableEngine("engine.db")` opens a SQLite file (creating it on the first
run) that holds everything `dura` needs to track. `spawn_task` records the
intent to run `charge_order` with those params.

We also give it an `idempotency_key`: a name we choose for this task, so
we can look it back up later without noting down some ID `dura` generated
for it. `run_workers` starts a small pool of worker threads that claim
and run tasks against `handlers`, and blocks until you stop it.

## 2. Run it

```bash
python charge_order.py
```

You should see:

```
charged cust_9 4200c
```

The task ran and completed almost instantly. `run_workers` is still
running though, waiting for more work. Press ++ctrl+c++: `dura` stops
claiming new tasks, lets any in-flight handler finish, and exits.

## 3. Look at what happened

Because everything is recorded in `engine.db`, we can inspect the task from
a completely separate script. Create `check_charge_order.py`:

```python
--8<-- "examples/check_charge_order.py"
```

Run it:

```bash
python check_charge_order.py
```

```
completed
{'order_id': 'ord_42'}
```

Two things are worth noticing here.

First, we didn't need the running process to answer this question. The
task's outcome lives in the database file, not in memory.

Second, calling `spawn_task` again with the same `idempotency_key` didn't
create a second task. It returned the existing one, which is exactly what
lets `check_charge_order.py` find the right task without ever handling a
`task_id` by hand. You'll reuse this same script again in step 5, pointed
at a different task.

## 4. Make a step durable

A real charge usually has more than one step, and some of them are worth
protecting individually. Replace the contents of `charge_order.py` with:

```python
--8<-- "examples/charge_order_durable.py"
```

A few things changed.

Each step of the handler is now wrapped in `engine.checkpoint(...)`. Once
a step's result is stored, calling `checkpoint` again for the same task
and step name returns that result instead of running `fn` again.

We also swapped `run_workers` (a pool of several workers) for
`run_worker` (one worker, run directly on this thread). That lets us set
a short 5-second `claim_timeout_secs`, so we don't have to wait out the
default 2-minute lease in the next step.

And we gave `spawn_task` a new `idempotency_key`, `"charge-2026-02"`
instead of `"charge-2026-01"`. This is a different task from the one in
steps 1 to 3, which already completed.

Run it:

```bash
python charge_order.py
```

You'll see:

```
Attempt 1
charged cust_9 4200c (this only happens once)
```

Then it pauses for 8 seconds, simulating a slow step, sending the
receipt. While it's paused, open another terminal in the same directory
and kill it hard, the way a crash or an out-of-memory kill would:

```bash
pkill -9 -f charge_order.py
```

The process is gone. No exception, no cleanup, nothing. As far as `dura`
is concerned, the run is still `running`, and its worker still holds a
claim on it. `engine.db` shows a completed `charge` checkpoint and a run
that never finished.

## 5. Restart and recover

Wait about 5 seconds (long enough for the claim's lease to expire), then
run the exact same command again:

```bash
python charge_order.py
```

```
Attempt 1
receipt sent to cust_9 for ord_42
Done
```

Notice two things:

- It says `Attempt 1`, not attempt 2. A crash isn't a logical failure, so
  reclaiming an abandoned run doesn't consume a retry.
- It didn't charge the card again. The `charge` checkpoint was already
  stored, so `dura` returned that result straight away and moved on to
  `receipt`.

Once you see `Done`, stop the script the same way as before:
`pkill -9 -f charge_order.py`, since it's still waiting for more work.

Then confirm the outcome. Open `check_charge_order.py` from step 3,
change its `idempotency_key` to `"charge-2026-02"` to match the task this
section built, and run it again:

```bash
python check_charge_order.py
```

```
completed
{'charged': True}
```

You've now built a task that survives a hard crash mid-step, without
losing progress or re-running from scratch. That's the core guarantee
`dura` is built around.

## Next steps

- Configure what happens when a handler raises instead of crashing: see
  [How to configure retries](how-to/configure-retries.md).
- See the full checkpoint and state APIs in the
  [`DurableEngine` reference](reference/engine.md).
- Read [About the durability model](explanation/durability-model.md) for
  how leases, checkpoints, and SQLite's locking fit together.
- The scripts in this tutorial, and the batch-processing example from
  the README, are runnable as-is in the
  [`examples/`](https://github.com/ericgazoni/dura/tree/main/examples)
  folder.
