---
icon: lucide/graduation-cap
---

# Your first durable task

In this tutorial we'll build a tiny `dura` task, run it, and watch it
complete. Then we'll make one of its steps durable, kill the process while
it's in the middle of running, and restart it, and see it pick up exactly
where it left off, without redoing work and without losing the task. Along
the way we'll meet the engine, the worker pool, and checkpoints.

You'll need Python 3.13 or later. Install `dura` into a fresh project:

```bash
pip install dura
```

## 1. Write a task

Create a file called `import_report.py`:

```python
from dura import DurableEngine, run_workers


def import_report(engine, task):
    print(f"Importing {task.params['uri']}")
    return {"rows": 3}


engine = DurableEngine("engine.db")
engine.spawn_task(
    name="import_report",
    params={"uri": "/report.csv"},
    idempotency_key="report-2026-01",
)

run_workers(engine, handlers={"import_report": import_report}, worker_count=2)
```

`DurableEngine("engine.db")` opens (and, on the first run, creates) a SQLite
file that will hold everything `dura` needs to track. `spawn_task` records
the intent to run `import_report` with those params. We also give it an
`idempotency_key`, a name *we* choose for this task, so that in the next
step we can look this exact task back up without having to note down some
ID `dura` generated for it. `run_workers` starts a small pool of worker
threads that claim and run tasks against `handlers`, and blocks until you
stop it.

## 2. Run it

```bash
python import_report.py
```

You should see:

```
Importing /report.csv
```

The task ran and completed almost instantly, but `run_workers` is still
running, waiting for more work. Press ++ctrl+c++. `dura` stops claiming
new tasks, lets any in-flight handler finish, and exits.

## 3. Look at what happened

Because everything is recorded in `engine.db`, we can inspect the task from
a completely separate script. Create `check_task.py`:

```python
from dura import DurableEngine

engine = DurableEngine("engine.db")
ref = engine.spawn_task(
    name="import_report",
    params={"uri": "/report.csv"},
    idempotency_key="report-2026-01",
)
task = engine.get_task(ref.task_id)
print(task.state)
print(task.result)
```

Run it:

```bash
python check_task.py
```

```
completed
{'rows': 3}
```

Two things are worth noticing here. First, we didn't need the running
process to ask this question: the task's outcome lives in the database
file, not in memory. Second, calling `spawn_task` again with the same
`idempotency_key` didn't create a second task: it returned the existing
one, which is exactly what lets `check_task.py` find the right task
without ever handling a `task_id` by hand. You'll reuse this same script
again in step 5, pointed at a different task.

## 4. Make a step durable

A real import usually has more than one step, and some of them are worth
protecting individually. Replace the contents of `import_report.py` with:

```python
import threading
import time
from dura import DurableEngine, run_worker


def fetch_rows(uri):
    print(f"Downloading {uri} (this only happens once)")
    return [1, 2, 3]


def load_rows(rows):
    print(f"Loading {len(rows)} rows")
    return True


def import_report(engine, task):
    print(f"Attempt {task.attempt}")
    rows = engine.checkpoint(
        task_id=task.task_id,
        step_name="download",
        fn=lambda: fetch_rows(task.params["uri"]),
    )
    time.sleep(8)  # pretend loading takes a while
    engine.checkpoint(
        task_id=task.task_id, step_name="load", fn=lambda: load_rows(rows)
    )
    print("Done")
    return {"rows": len(rows)}


engine = DurableEngine("engine.db")
engine.spawn_task(
    name="import_report",
    params={"uri": "/report.csv"},
    idempotency_key="report-2026-02",
)

run_worker(
    engine,
    handlers={"import_report": import_report},
    worker_id="tutorial-worker",
    stop_event=threading.Event(),
    claim_timeout_secs=5,
)
```

A few things changed. Each step of the handler is now wrapped in
`engine.checkpoint(...)`: once a step's result is stored, calling
`checkpoint` again for the same task and step name returns that stored
result instead of running `fn` again. We swapped `run_workers` (a pool of
several workers) for `run_worker` (one worker, run directly on this thread
instead of a background one), so we can set a short 5-second
`claim_timeout_secs` and not have to wait out the default 2-minute lease
in the next step. And we gave `spawn_task` a new `idempotency_key`,
`"report-2026-02"` instead of `"report-2026-01"`, since this is a
different task from the one in steps 1 to 3, which has already completed.

Run it:

```bash
python import_report.py
```

You'll see:

```
Attempt 1
Downloading /report.csv (this only happens once)
```

Then it pauses for 8 seconds, simulating a slow load step. **While it's
paused**, open another terminal in the same directory and kill it hard,
the way a crash or an out-of-memory kill would:

```bash
pkill -9 -f import_report.py
```

The process is gone: no exception, no cleanup, nothing. As far as `dura`
is concerned, the run is still `running` and its worker is still holding a
claim on it. `engine.db` shows a completed `download` checkpoint and a run
that never finished.

## 5. Restart and recover

Wait about 5 seconds (long enough for the claim's lease to expire), then
run the exact same command again:

```bash
python import_report.py
```

```
Attempt 1
Loading 3 rows
Done
```

Notice two things:

- It says `Attempt 1`, not attempt 2. A crash isn't a logical failure, so
  reclaiming an abandoned run doesn't consume a retry.
- It did **not** print "Downloading..." again. The `download` checkpoint
  was already stored, so `dura` returned that result straight away and
  moved on to `load`.

Once you see `Done`, stop the script the same way as before, with
`pkill -9 -f import_report.py`, since it's still waiting for more work.
Then confirm the outcome. Open `check_task.py` from step 3 and change its
`idempotency_key` to `"report-2026-02"`, to match the task this section
built, then run it again:

```bash
python check_task.py
```

```
completed
{'rows': 3}
```

You have now built a task that survives a hard crash mid-step without
losing progress or being re-run from scratch: the core guarantee `dura`
is built around.

## Next steps

- Configure what happens when a handler raises instead of crashing: see
  [How to configure retries](how-to/configure-retries.md).
- See the full checkpoint and state APIs in the
  [`DurableEngine` reference](reference/engine.md).
- Read [About the durability model](explanation/durability-model.md) for
  how leases, checkpoints, and SQLite's locking fit together.
