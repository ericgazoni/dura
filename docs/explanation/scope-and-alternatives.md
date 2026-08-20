---
icon: lucide/compass
---

# About scope and alternatives

`dura` is deliberately small, and that's a scope decision, not a
limitation to be worked around. This is a discussion of what it's for,
what it isn't for, and what to reach for instead when it isn't the right
fit.

## What it's for

`dura` targets local or isolated applications: a single process, or a
small number of independent, non-clustered instances, that needs to
survive crashes and restarts cleanly without a separate piece of
infrastructure to operate.

The payoff of that narrowness is legibility. At any point, `engine.db`
is the queue. You can open it with the plain `sqlite3` CLI and see, in
ordinary SQL, exactly what's queued, running, or stuck: no broker state,
no distributed lock table, no second system whose view of the world
might disagree with the database's.

For a single-process worker pool, that's a genuine advantage over
reaching for Celery-plus-Redis or a hosted workflow engine: there's
nothing else to keep running, version, or lose network connectivity to.

## What it isn't for

That same narrowness is a real ceiling, not a temporary gap. SQLite
allows one writer at a time, and `dura` is built around that constraint
rather than against it (see [About the durability
model](durability-model.md#how-dura-lets-many-workers-share-one-sqlite-file-safely)).

It has no answer for coordinating work across multiple machines or pods
sharing one queue, or for saga-style compensation across services, and no
plan to grow one: SQLite is the first-class, and only, storage backend,
on purpose. Reaching for `dura` in a use case that actually needs
cross-process, cross-machine coordination means fighting the tool instead
of using it.

## If you need distributed coordination: Edda

If workflows need to be coordinated across multiple processes, pods, or
machines against a shared database, say Postgres/MySQL-backed locking,
saga compensation, CloudEvents ingestion, or multi-worker fan-out across
a cluster, that's a different problem with a different shape.
[Edda](https://github.com/i2y/edda) is built for exactly that.

Choosing between them is really a question of topology: one process (or
a handful of independent ones) versus a cluster that needs to agree on
shared state.

## If you need the file to survive losing the machine: Litestream

`dura`'s durability is against process and application crashes on one
machine, not against losing that machine's disk. If you want the single
SQLite file to survive that too, [Litestream](https://litestream.io/)
replicates it continuously to object storage for point-in-time recovery.

Pairing the two keeps the operational shape the same: still one file,
still no distributed lock service, while extending what "durable"
covers. It doesn't replace `dura`'s model with a client/server database.
