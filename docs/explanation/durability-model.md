---
icon: lucide/shield-check
---

# About the durability model

`dura`'s central idea is that a single SQLite file can be the entire source
of truth for a task queue, without a separate broker, workflow server, or
scanner process alongside it to keep in sync. This is a discussion of how
that works and why it's built the way it is.

## One file, five tables

Everything lives in one database, organized into five tables: `tasks` (one
row per logical job, the intent), `runs` (one row per execution attempt
of a task), `checkpoints` (the persisted result of each completed step,
keyed to the task), `events` (named, first-write-wins signals), and
`waits` (which run is waiting for which event, with an optional timeout).
A sixth table, `state`, is a general-purpose durable key-value store that
is deliberately outside this lifecycle entirely; see [Why state is
separate](#why-state-is-kept-separate) below.

The task/run split matters: a task is what you asked for ("import this
report"), a run is one attempt at it. This is what makes it possible to
say a crash is not the same thing as a logical failure, which the next
section depends on.

## Why a crash doesn't consume a retry

When a worker claims a run, it takes out a time-bounded lease
(`claim_expires_at`), not a permanent lock. If the worker dies mid-handler,
say an OOM kill, a hard crash, or a `SIGKILL`, nothing tells `dura` the
worker is gone; there's no heartbeat *per run*. Instead, the next
`claim_task` call notices the lease has expired and resets that run from
`running` back to `pending`, keeping its original attempt number. Only
`fail_run`, an explicit, in-process decision that the handler raised,
advances the attempt counter and applies backoff.

This is a real design commitment, not an incidental detail: it means a
flaky *process* (one that gets OOM-killed under load, say) doesn't
silently eat through a task's retry budget the way it would in a queue
that conflates "attempt didn't finish" with "attempt failed." The cost is
that a genuinely stuck handler, one that hangs forever rather than
crashing, looks identical to a crashed one until its lease expires;
`extend_claim` exists so a handler that legitimately needs longer than the
default lease can say so explicitly, rather than being reclaimed out from
under itself.

## Why checkpoints are keyed to the task, not the run

A checkpoint's key is `(task_id, step_name)`, not `run_id`. That's what
lets a step "run at most once" *across* retries and crash-recovery
re-runs of the same task: attempt 2 of a task sees the checkpoints
attempt 1 wrote, because they share a `task_id`. If checkpoints were keyed
to the run instead, every retry would start from a clean slate and
`dura` would only be memoizing within a single attempt, which is much
less useful, since the whole point is to survive the *next* attempt after
a crash.

The trade-off is that a checkpoint's `fn` is not itself transactional with
the rest of your system: if `fn` has an external side effect (an email
sent, a charge made) and the process crashes after that side effect
happens but before `dura` commits the checkpoint, the side effect will
happen again on the reclaimed retry. Checkpointing gives you *at-most-once
from dura's point of view*, not exactly-once through to every external
system your handler touches; that still requires making the side effect
itself idempotent, the same as with any retry-based system.

## Why events are first-write-wins, and waits don't poll

`emit_event` intentionally ignores later calls for a name that's already
been emitted. This makes emission commutative with waiting: it doesn't
matter whether `wait_for_event` or `emit_event` runs first, the outcome
is the same either way, which avoids an entire class of races common to
pub/sub systems where a message published before anyone subscribed is
simply lost. A run that's waiting doesn't hold a worker thread or poll on
an interval; `wait_for_event` raises `WorkflowSuspended`, the run is
marked `sleeping`, and it's woken by the same claim query every other run
uses, either when `emit_event` resets it to `pending` or when its timeout
is reached. From the scheduler's point of view, a parked run is just
another row with a future `available_at`.

## Why the concurrency model is "one writer, many readers, one process"

SQLite allows exactly one writer at a time. `dura` embraces this rather
than working around it: every mutating operation runs inside a `BEGIN
IMMEDIATE` transaction, which acquires SQLite's write lock up front, so
concurrent writers from different threads serialize: they queue and
wait, they don't fail with a late `SQLITE_BUSY` the way they would under
SQLite's default deferred locking. WAL mode is enabled so readers aren't
blocked by an in-progress write. Connections are per-thread and coordinate
purely through SQLite's own file locking; there's no separate lock
manager to reason about.

This is built and tested for many threads inside *one* process, which is
the model `dura.workers` implements; it is not built for multiple separate
processes or machines writing to the same file concurrently. See [About
scope and alternatives](scope-and-alternatives.md) for what to reach for
when you outgrow that.

## Why state is kept separate

Checkpoints and durable state look similar, both are persisted
key-addressed values, but they answer different questions. A checkpoint
answers "did this task already do this step?" and disappears with the
task. State answers "what do I already know, independent of any one
task?", things like a poller's cursor or a dedup set spanning thousands of
tasks, and is exactly the data you don't want `cleanup()` to touch just
because the task that last wrote it became terminal. Keeping them as
separate tables, rather than one generic store with a "don't delete this
one" flag, makes that distinction structural instead of a convention
someone can forget to follow.
