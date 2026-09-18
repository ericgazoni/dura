---
icon: lucide/shield-check
---

# About the durability model

Running background work reliably usually means operating something extra
alongside your app: a message broker, a workflow server, a process that
reconciles state if the two disagree. `dura`'s bet is that a single
SQLite file can carry that whole burden by itself, with nothing else to
keep in sync. This page walks through the problems that bet has to
solve, and how `dura` solves each one.

## What dura needs to track, and where it lives

To be a task queue's entire source of truth, `dura` has to answer
several different questions reliably: what work is there to do, has this
exact step already run, has something happened yet, who's waiting on
it. It answers each with its own table:

- `tasks`: one row per logical job, the intent.
- `runs`: one row per execution attempt of a task.
- `checkpoints`: the persisted result of each completed step, keyed to
  the task.
- `events`: named, first-write-wins signals.
- `waits`: which run is waiting for which event, with an optional
  timeout.

A sixth table, `state`, is a general-purpose durable key-value store,
deliberately kept outside this lifecycle. See [How dura keeps long-lived
data from being deleted by
accident](#how-dura-keeps-long-lived-data-from-being-deleted-by-accident)
below.

One more distinction matters before we get to failure handling: a task
is not the same thing as a run. A task is what you asked for ("import
this report"). A run is one attempt at it. Without that split, `dura`
would have no way to say a crash and a logical failure are different
things, which is exactly the problem the next section solves.

## How dura handles different kinds of failure

A run can stop making progress in three different ways, and `dura`
reacts to each one differently: a handler that raises an exception, a
process that dies outright, and a handler that just hangs.

### The handler raises an exception

This is the case `dura` is designed around. The handler runs, hits a
real problem, and raises. `dura` catches that, inside `process_task` if
you're using `run_workers`, and calls `fail_run`. That marks the current
run `failed`, creates a new run with `attempt + 1`, and schedules it
according to the task's retry strategy: immediately, after a fixed
delay, or after an exponential backoff with jitter. Once `max_attempts`
is reached, the task's state becomes `failed` for good.

### The process dies outright

This covers a real crash: the Linux OOM killer sending `SIGKILL` when a
container exceeds its memory limit, a `kill -9`, a deploy that
force-kills the old process, or a segfault in a native library. None of
these are Python exceptions. They're the kernel tearing the process
down, so no code in that process runs afterward, no `except`, no
`finally`, nothing. That's true for every thread in the process at once,
since it's the whole process being killed, not one thread being picked
off.

`dura` doesn't find out this happened, at least not right away. What
protects you instead is the lease every claimed run holds
(`claim_expires_at`), set when it was claimed and good for
`claim_timeout_secs`, 120 seconds by default. Once that deadline passes,
the following `claim_task` call, whether from a restarted instance of
the same process or a completely different one, notices the stale lease
and resets the run from `running` back to `pending`. It keeps its
original attempt number, so the crash doesn't cost the task a retry. A
crash and a logical failure are different things to `dura`, and only the
second one should count against `max_attempts`.

### The handler hangs, without crashing

This is the case `dura` can't fully protect you from on its own. An
infinite loop, a network call that never times out, a deadlock: none of
these raise, and none of them kill the process. To `dura`, a hung
handler looks exactly like a crashed one, since both just stop renewing
anything, and the same lease mechanism kicks in once
`claim_timeout_secs` passes.

The difference is that the original handler is still actually running.
When another worker claims the run once its lease looks expired and
starts executing the same handler again, the first one hasn't
necessarily stopped. You can end up with two workers running the same
logical step at once. If a step's side effects aren't safe to run
twice, see [how dura avoids repeating
work](#how-dura-avoids-repeating-work-you-already-did) below, that's a
real risk, not a hypothetical one.

The fix is to tell `dura` about it before it becomes a problem. If a
step legitimately needs more than the default lease, call
`extend_claim` to push the deadline forward yourself, so another worker
doesn't reclaim the run while you're still legitimately working on it.

## How dura avoids repeating work you already did

A handler that gets retried, whether because it raised an exception or
because its run was reclaimed after a crash, starts over from the top.
For a handler with more than one step, say downloading a file and then
loading it into a database, that's a real problem: you don't want the
download to happen twice just because the load step failed.

`engine.checkpoint(task_id=..., step_name=..., fn=...)` is `dura`'s
answer. The first time it runs for a given `(task_id, step_name)`, it
calls `fn`, stores the result, and returns it. Every later call for that
same task and step name returns the stored result instead, without
calling `fn` again, whether that later call comes from a retry after
`fail_run` or from a worker reclaiming an abandoned run after a crash.

The key is `(task_id, step_name)`, deliberately not `run_id`. That's
what lets a step run at most once across every attempt at the same
task: attempt 2 sees the checkpoints attempt 1 wrote, because they share
a `task_id`. If checkpoints were keyed to the run instead, every retry
would start from a clean slate, and `dura` would only be memoizing
within a single attempt, which defeats the purpose: the whole point is
to survive the next attempt after a crash.

Checkpoints don't cover everything, though. A checkpoint's `fn` isn't
itself transactional with the rest of your system. If `fn` has an
external side effect (an email sent, a charge made), and the process
crashes after that side effect happens but before `dura` commits the
checkpoint, the side effect happens again on the reclaimed retry.
Checkpointing gives you at-most-once from `dura`'s point of view, not
exactly-once through to every external system your handler touches.
Making the side effect itself idempotent is still on you, the same as
with any retry-based system.

## How dura lets a task wait for something else, without polling

Sometimes a task can't finish until something outside it happens: a
human approval, a webhook, another task finishing. The obvious way to
wait for that is to poll a flag in a loop, but that ties up a worker
thread, and a database query, for however long the wait takes.

`wait_for_event` is `dura`'s answer. It raises `WorkflowSuspended`, the
run is marked `sleeping`, and the worker moves on to other work. Nothing
polls. The parked run is later woken by the same claim query every other
run uses, either when `emit_event` resets it to `pending`, or when its
timeout is reached. From the scheduler's point of view, a parked run is
just another row with a future `available_at`.

That still leaves a race to solve: what if `emit_event` fires before
anyone is waiting for it? In most pub/sub systems, a message published
before anyone subscribed is simply lost. `emit_event` avoids that by
being first-write-wins: the first call for a given `event_name` is the
one that's recorded, and it stays recorded whether a waiter shows up
before or after. It doesn't matter which one runs first, the outcome is
the same either way.

## How dura lets many workers share one SQLite file safely

SQLite allows exactly one writer at a time. Left alone, that would mean
a busy worker pool hitting `SQLITE_BUSY` errors whenever two threads
tried to write at once. `dura` avoids that by embracing the limit rather
than working around it: every mutating operation runs inside a `BEGIN
IMMEDIATE` transaction, which acquires SQLite's write lock up front.
Concurrent writers from different threads then serialize. They queue and
wait for their turn, instead of failing late the way they would under
SQLite's default deferred locking.

Reads aren't held up by this. WAL mode is enabled so readers aren't
blocked by an in-progress write. Connections are per-thread and
coordinate purely through SQLite's own file locking, so there's no
separate lock manager to reason about either.

This is built and tested for many threads inside one process, which is
the model `dura.workers` implements. It is not built for multiple separate
processes or machines writing to the same file concurrently. See [About
scope and alternatives](scope-and-alternatives.md) for what to reach for
when you outgrow that.

Put `engine.db` on local or block storage, not a network filesystem
(NFS, CephFS, etc.). WAL mode depends on shared-memory-backed locking
between connections (the `-shm` file), and most network filesystems
don't implement that correctly, so lock waits become unpredictable and
you'll see `database is locked` errors regardless of `busy_timeout_ms`.
If you must run against network-backed storage, raise
`busy_timeout_ms` (default `30_000`) enough to ride out its latency,
but treat that as a mitigation, not a fix.

## How dura keeps long-lived data from being deleted by accident

**Checkpoints** and durable state look similar: both are persisted,
key-addressed values. Treating them as one generic store would create a
real problem, though, since `cleanup()` needs to know which rows are
safe to delete once a task finishes, and which ones have to survive it.

A checkpoint answers "did this task already do this step?" and is
supposed to disappear with the task, so `cleanup()` reclaims it once the
task becomes terminal. 

**State** answers a different question entirely,
"what do I already know, independent of any one task?": things like a
poller's cursor, or a dedup set spanning thousands of tasks. That's
exactly the data you don't want `cleanup()` to touch just because the
task that last wrote it became terminal.

`dura` keeps them as separate tables, rather than one generic store with
a "don't delete this one" flag, which makes that distinction structural
instead of a convention someone can forget to follow.
