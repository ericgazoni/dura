"""SQLite-backed durable execution engine.

This is the persistence + scheduling core that replaces the disk queue, the
disk event bus, the WAL and the scanner memory with a single database.

Everything lives in one ``engine.db`` organised into five tables:

* ``tasks``       - one row per logical job (the intent)
* ``runs``        - one row per execution attempt of a task
* ``checkpoints`` - persisted result of each completed step, keyed to the task
* ``events``      - named, first-write-wins signals
* ``waits``       - which run is waiting for which event (with optional timeout)
* ``state``       - generic durable key-value store, scoped by namespace, that
  outlives tasks (never touched by cleanup); for cross-task memory such as
  cursors, watermarks or "what have I already seen"

The engine owns no threads. Callers drive it: a worker calls ``claim_task``
in a loop, runs the handler, then calls ``complete_run`` or
``fail_run``. Steps inside a handler are made durable with
``checkpoint``.

SQLite allows a single writer at a time. Every mutating operation runs inside a
``BEGIN IMMEDIATE`` transaction, which acquires the write lock up front so
concurrent writers serialise cleanly (they wait, they do not deadlock) rather
than failing late with ``SQLITE_BUSY``. Connections are per-thread, so each
worker thread coordinates with the others through SQLite's own file locking.
"""

from __future__ import annotations

import json
import random
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, TypeVar

T = TypeVar("T")

# A timestamp far enough in the future that an un-timed wait is never picked up
# by the scheduler. Stored in the same fixed format as every other timestamp so
# lexicographic comparison in SQL matches chronological order.
_INFINITY = "9999-12-31T23:59:59.999999+00:00"

_TERMINAL_STATES = ("completed", "failed", "cancelled")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id           TEXT PRIMARY KEY,
    task_name         TEXT NOT NULL,
    params            TEXT NOT NULL,
    state             TEXT NOT NULL,
    attempts          INTEGER NOT NULL DEFAULT 0,
    max_attempts      INTEGER,
    retry_strategy    TEXT,
    enqueue_at        TEXT NOT NULL,
    first_started_at  TEXT,
    last_attempt_run  TEXT,
    completed_payload TEXT,
    terminal_at       TEXT,
    idempotency_key   TEXT UNIQUE
) STRICT;

CREATE TABLE IF NOT EXISTS runs (
    run_id           TEXT PRIMARY KEY,
    task_id          TEXT NOT NULL,
    attempt          INTEGER NOT NULL,
    state            TEXT NOT NULL,
    claimed_by       TEXT,
    claim_expires_at TEXT,
    available_at     TEXT NOT NULL,
    wake_event       TEXT,
    event_payload    TEXT,
    started_at       TEXT,
    completed_at     TEXT,
    failed_at        TEXT,
    result           TEXT,
    failure_reason   TEXT,
    priority         INTEGER NOT NULL DEFAULT 0
) STRICT;

CREATE TABLE IF NOT EXISTS checkpoints (
    task_id         TEXT NOT NULL,
    checkpoint_name TEXT NOT NULL,
    state           TEXT,
    owner_run_id    TEXT,
    updated_at      TEXT NOT NULL,
    PRIMARY KEY (task_id, checkpoint_name)
) STRICT;

CREATE TABLE IF NOT EXISTS events (
    event_name TEXT PRIMARY KEY,
    payload    TEXT,
    emitted_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS waits (
    run_id     TEXT NOT NULL,
    step_name  TEXT NOT NULL,
    task_id    TEXT NOT NULL,
    event_name TEXT NOT NULL,
    timeout_at TEXT,
    PRIMARY KEY (run_id, step_name)
) STRICT;

CREATE TABLE IF NOT EXISTS state (
    namespace  TEXT NOT NULL,
    key        TEXT NOT NULL,
    value      TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (namespace, key)
) STRICT;

CREATE INDEX IF NOT EXISTS runs_state_available ON runs (state, available_at);
CREATE INDEX IF NOT EXISTS runs_task_id ON runs (task_id);
CREATE INDEX IF NOT EXISTS waits_event_name ON waits (event_name);
CREATE INDEX IF NOT EXISTS tasks_state ON tasks (state);

-- Serves claim_task's "highest-priority, earliest-available" pick directly:
-- the (priority DESC, available_at) order matches its ORDER BY, and the partial
-- predicate keeps the index to only claimable runs.
CREATE INDEX IF NOT EXISTS runs_claimable
    ON runs (priority DESC, available_at)
    WHERE state IN ('pending', 'sleeping');
"""


class EngineError(Exception):
    """Base class for engine errors."""


class TaskNotFound(EngineError):
    """Raised when a referenced task or run does not exist."""


class TaskCancelledError(EngineError):
    """Raised when an operation is attempted on a cancelled task."""


class InvalidRunState(EngineError):
    """Raised when a run is not in a state that permits the operation."""


class WorkflowSuspended(EngineError):
    """Raised by ``DurableEngine.wait_for_event`` when a run parks itself.

    The worker loop catches this and leaves the run alone: it is now
    ``sleeping`` and will be re-claimed when the awaited event fires or the
    timeout elapses. A handler must let this propagate (do not catch it).
    """


@dataclass(frozen=True, kw_only=True)
class RetryStrategy:
    """How a failed task should be retried.

    * ``none``        - no delay between attempts
    * ``fixed``       - always wait ``base_seconds``
    * ``exponential`` - ``base_seconds * factor ** (attempt - 1)``, capped at
      ``max_seconds`` when set
    """

    kind: str = "none"
    base_seconds: float = 30.0
    factor: float = 2.0
    max_seconds: float | None = None
    jitter_factor: float = 0.2

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "base_seconds": self.base_seconds,
            "factor": self.factor,
            "max_seconds": self.max_seconds,
            "jitter_factor": self.jitter_factor,
        }


@dataclass(frozen=True, kw_only=True)
class TaskRef:
    """Returned by ``DurableEngine.spawn_task``."""

    task_id: str
    run_id: str
    attempt: int
    created: bool


@dataclass(frozen=True, kw_only=True)
class ClaimedTask:
    """A task handed to a worker by ``DurableEngine.claim_task``."""

    run_id: str
    task_id: str
    attempt: int
    name: str
    params: Any


@dataclass(frozen=True, kw_only=True)
class TaskInfo:
    """Read model returned by ``DurableEngine.get_task``."""

    task_id: str
    state: str
    attempts: int
    result: Any = None
    failure_reason: Any = None


@dataclass(frozen=True, kw_only=True)
class FailureInfo:
    """One failed run, as returned by ``DurableEngine.recent_failures``."""

    run_id: str
    task_id: str
    task_name: str
    attempt: int
    failed_at: str
    failure_reason: Any


def _new_id() -> str:
    return uuid.uuid4().hex


def _fmt(dt: datetime) -> str:
    """Format an aware datetime as a fixed-width UTC ISO string.

    Fixed width + always-UTC guarantees that string ordering in SQL matches
    chronological ordering.
    """
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")


def _retry_delay(strategy: dict[str, Any] | None, failed_attempt: int) -> float:
    """Seconds to wait before the next attempt after ``failed_attempt`` failed."""
    if not strategy:
        return 0.0
    kind = strategy.get("kind", "none")
    if kind == "fixed":
        delay = float(strategy.get("base_seconds", 60))
    elif kind == "exponential":
        base = float(strategy.get("base_seconds", 30))
        factor = float(strategy.get("factor", 2.0))
        delay = base * (factor ** max(failed_attempt - 1, 0))
        max_seconds = strategy.get("max_seconds")
        if max_seconds is not None:
            delay = min(delay, float(max_seconds))
    else:
        return 0.0
    jitter_factor = float(strategy.get("jitter_factor", 0.2))
    if jitter_factor > 0 and delay > 0:
        delay += random.uniform(0, jitter_factor * delay)  # nosec
    return delay


class DurableEngine:
    def __init__(
        self,
        db_path: str | Path,
        *,
        clock: Callable[[], datetime] | None = None,
        busy_timeout_ms: int = 5000,
    ) -> None:
        self._db_path = str(db_path)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._busy_timeout_ms = busy_timeout_ms
        self._local = threading.local()
        self._conn().executescript(_SCHEMA)

    # -- infrastructure ----------------------------------------------------

    @property
    def db_path(self) -> str:
        """Filesystem path of the engine database."""
        return self._db_path

    def _now(self) -> datetime:
        now = self._clock()
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        return now

    def _conn(self) -> sqlite3.Connection:
        conn: sqlite3.Connection | None = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(
                self._db_path,
                isolation_level=None,  # autocommit; we issue BEGIN IMMEDIATE
                timeout=self._busy_timeout_ms / 1000,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute(f"PRAGMA busy_timeout={self._busy_timeout_ms}")
            self._local.conn = conn
        return conn

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")

    def close(self) -> None:
        """Close the connection for the calling thread, if any."""
        conn: sqlite3.Connection | None = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    # -- task lifecycle ----------------------------------------------------

    def spawn_task(
        self,
        *,
        name: str,
        params: Any,
        idempotency_key: str | None = None,
        retry: RetryStrategy | None = None,
        max_attempts: int | None = None,
        available_after: timedelta | None = None,
        priority: int = 0,
    ) -> TaskRef:
        """Create a task and its first run.

        ``priority`` orders claiming: higher is claimed first (ties broken by
        availability, then insertion order). The default 0 is the lowest band;
        give maintenance and other latency-sensitive work a higher value so it
        is not starved behind a backlog of low-priority work. Retries inherit
        the run's priority.

        If ``idempotency_key`` is supplied and a task already exists for it, no
        new task is created and the existing one is returned with
        ``created=False``.
        """
        now = self._now()
        available_at = now + (available_after or timedelta(0))
        delayed = available_at > now
        state = "sleeping" if delayed else "pending"
        task_id = _new_id()
        run_id = _new_id()

        with self._tx() as conn:
            if idempotency_key is not None:
                existing = conn.execute(
                    "SELECT task_id, last_attempt_run, attempts "
                    "FROM tasks WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
                if existing is not None:
                    return TaskRef(
                        task_id=existing["task_id"],
                        run_id=existing["last_attempt_run"],
                        attempt=existing["attempts"],
                        created=False,
                    )

            conn.execute(
                "INSERT INTO tasks (task_id, task_name, params, state, attempts, "
                "max_attempts, retry_strategy, enqueue_at, last_attempt_run, "
                "idempotency_key) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    name,
                    _dumps(params),
                    state,
                    max_attempts,
                    _dumps(retry.to_dict()) if retry else None,
                    _fmt(now),
                    run_id,
                    idempotency_key,
                ),
            )
            conn.execute(
                "INSERT INTO runs (run_id, task_id, attempt, state, available_at, "
                "priority) VALUES (?, ?, 1, ?, ?, ?)",
                (run_id, task_id, state, _fmt(available_at), priority),
            )
        return TaskRef(task_id=task_id, run_id=run_id, attempt=1, created=True)

    def claim_task(
        self,
        *,
        worker_id: str,
        timeout_secs: int = 120,
        max_priority: int | None = None,
    ) -> ClaimedTask | None:
        """Atomically reserve the next available run, or return ``None``.

        Before claiming, runs whose lease has expired (a worker took them but
        never finished) are reset to ``pending`` so they can be picked up again.
        A reclaimed run keeps its attempt number: a crash is not a logical
        failure, so it does not consume a retry.

        ``max_priority``: when set, only claim runs whose priority is at or
        below this value. Used by lane workers dedicated to low-priority tasks.
        """
        now = self._now()
        now_str = _fmt(now)
        expires_str = _fmt(now + timedelta(seconds=timeout_secs))
        # A sentinel well above any real priority lets the general (unrestricted)
        # case share the same query as the lane case.
        priority_ceil = max_priority if max_priority is not None else 10_000

        with self._tx() as conn:
            conn.execute(
                "UPDATE runs SET state = 'pending', claimed_by = NULL, "
                "claim_expires_at = NULL "
                "WHERE state = 'running' AND claim_expires_at IS NOT NULL "
                "AND claim_expires_at <= ?",
                (now_str,),
            )

            # Pin runs_claimable: its (priority DESC, available_at) order matches
            # the ORDER BY, so the highest-priority due run is the first index
            # row. Without the hint the planner sorts the whole candidate set
            # (a 60k-row temp B-tree under a storm) unless ANALYZE has been run.
            row = conn.execute(
                "SELECT r.run_id, r.task_id, r.attempt, t.task_name, t.params "
                "FROM runs r INDEXED BY runs_claimable "
                "JOIN tasks t ON t.task_id = r.task_id "
                "WHERE r.state IN ('pending', 'sleeping') "
                "AND t.state IN ('pending', 'sleeping', 'running') "
                "AND r.available_at <= ? "
                "AND r.priority <= ? "
                "ORDER BY r.priority DESC, r.available_at, r.rowid LIMIT 1",
                (now_str, priority_ceil),
            ).fetchone()
            if row is None:
                return None

            conn.execute(
                "UPDATE runs SET state = 'running', claimed_by = ?, "
                "claim_expires_at = ?, started_at = COALESCE(started_at, ?) "
                "WHERE run_id = ?",
                (worker_id, expires_str, now_str, row["run_id"]),
            )
            conn.execute(
                "UPDATE tasks SET state = 'running', attempts = MAX(attempts, ?), "
                "first_started_at = COALESCE(first_started_at, ?), "
                "last_attempt_run = ? WHERE task_id = ?",
                (row["attempt"], now_str, row["run_id"], row["task_id"]),
            )

        return ClaimedTask(
            run_id=row["run_id"],
            task_id=row["task_id"],
            attempt=row["attempt"],
            name=row["task_name"],
            params=_loads(row["params"]),
        )

    def complete_run(self, *, run_id: str, result: Any = None) -> None:
        """Mark a run and its task as completed."""
        now_str = _fmt(self._now())
        with self._tx() as conn:
            row = conn.execute(
                "SELECT task_id, state FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            self._guard_running(row, run_id)

            conn.execute(
                "UPDATE runs SET state = 'completed', completed_at = ?, result = ? "
                "WHERE run_id = ?",
                (now_str, _dumps(result), run_id),
            )
            conn.execute(
                "UPDATE tasks SET state = 'completed', completed_payload = ?, "
                "terminal_at = ?, last_attempt_run = ? WHERE task_id = ?",
                (_dumps(result), now_str, run_id, row["task_id"]),
            )
            conn.execute("DELETE FROM waits WHERE run_id = ?", (run_id,))

    def fail_run(self, *, run_id: str, reason: dict[str, Any]) -> None:
        """Mark a run as failed and schedule a retry if attempts remain."""
        now = self._now()
        now_str = _fmt(now)
        with self._tx() as conn:
            row = conn.execute(
                "SELECT task_id, attempt, state, priority FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            self._guard_running(row, run_id)
            task_id = row["task_id"]
            attempt = row["attempt"]

            task = conn.execute(
                "SELECT retry_strategy, max_attempts FROM tasks WHERE task_id = ?",
                (task_id,),
            ).fetchone()

            conn.execute(
                "UPDATE runs SET state = 'failed', failed_at = ?, failure_reason = ? "
                "WHERE run_id = ?",
                (now_str, _dumps(reason), run_id),
            )

            next_attempt = attempt + 1
            max_attempts = task["max_attempts"]
            if max_attempts is None or next_attempt <= max_attempts:
                strategy = (
                    _loads(task["retry_strategy"]) if task["retry_strategy"] else None
                )
                delay = _retry_delay(strategy, attempt)
                available_at = now + timedelta(seconds=delay)
                run_state = "pending" if delay <= 0 else "sleeping"
                new_run_id = _new_id()
                conn.execute(
                    "INSERT INTO runs (run_id, task_id, attempt, state, available_at, "
                    "priority) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        new_run_id,
                        task_id,
                        next_attempt,
                        run_state,
                        _fmt(available_at),
                        row["priority"],
                    ),
                )
                conn.execute(
                    "UPDATE tasks SET state = ?, attempts = MAX(attempts, ?), "
                    "last_attempt_run = ? WHERE task_id = ?",
                    (run_state, next_attempt, new_run_id, task_id),
                )
            else:
                conn.execute(
                    "UPDATE tasks SET state = 'failed', attempts = MAX(attempts, ?), "
                    "terminal_at = ?, last_attempt_run = ? WHERE task_id = ?",
                    (attempt, now_str, run_id, task_id),
                )
            conn.execute("DELETE FROM waits WHERE run_id = ?", (run_id,))

    def cancel_task(self, task_id: str) -> None:
        """Cancel a task and any of its non-terminal runs."""
        now_str = _fmt(self._now())
        with self._tx() as conn:
            row = conn.execute(
                "SELECT state FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
            if row is None:
                raise TaskNotFound(f"Task {task_id} not found")
            if row["state"] in _TERMINAL_STATES:
                return
            conn.execute(
                "UPDATE tasks SET state = 'cancelled', terminal_at = ? "
                "WHERE task_id = ?",
                (now_str, task_id),
            )
            conn.execute(
                "UPDATE runs SET state = 'cancelled', claimed_by = NULL, "
                "claim_expires_at = NULL "
                "WHERE task_id = ? AND state NOT IN ('completed', 'failed', 'cancelled')",
                (task_id,),
            )
            conn.execute("DELETE FROM waits WHERE task_id = ?", (task_id,))

    def cancel_duplicate_tasks(self, task_name: str) -> tuple[str | None, int]:
        """Cancel all but the oldest active task with the given name.

        Returns ``(surviving_task_id, count_cancelled)``.  ``surviving_task_id``
        is ``None`` when no non-terminal tasks with this name exist.  Intended
        for use at startup to collapse duplicate self-perpetuating chains that
        were seeded across multiple restarts before the state-table guard was in
        place.
        """
        rows = (
            self._conn()
            .execute(
                "SELECT task_id FROM tasks "
                "WHERE task_name = ? AND state NOT IN ('completed', 'failed', 'cancelled') "
                "ORDER BY enqueue_at ASC",
                (task_name,),
            )
            .fetchall()
        )
        if not rows:
            return None, 0
        ids = [r["task_id"] for r in rows]
        for dup_id in ids[1:]:
            self.cancel_task(task_id=dup_id)
        return ids[0], len(ids) - 1

    def extend_claim(self, *, run_id: str, by_secs: int) -> None:
        """Push a running lease forward (heartbeat for long steps)."""
        if by_secs <= 0:
            raise ValueError("by_secs must be > 0")
        new_expiry = _fmt(self._now() + timedelta(seconds=by_secs))
        with self._tx() as conn:
            row = conn.execute(
                "SELECT state FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None:
                raise TaskNotFound(f"Run {run_id} not found")
            if row["state"] != "running":
                raise InvalidRunState(f"Run {run_id} is not running")
            conn.execute(
                "UPDATE runs SET claim_expires_at = ? WHERE run_id = ?",
                (new_expiry, run_id),
            )

    # -- checkpoints -------------------------------------------------------

    def checkpoint(
        self,
        *,
        task_id: str,
        step_name: str,
        fn: Callable[[], T],
        owner_run_id: str | None = None,
    ) -> T:
        """Run ``fn`` once and persist its result, keyed to the task.

        On the first call the result is computed and stored. On any later call
        for the same ``(task_id, step_name)`` - including a retry after a crash -
        the stored result is returned and ``fn`` is not invoked again.

        ``fn`` runs outside the write transaction so a slow step (FTP, S3) does
        not hold the database lock.
        """
        conn = self._conn()
        existing = conn.execute(
            "SELECT state FROM checkpoints WHERE task_id = ? AND checkpoint_name = ?",
            (task_id, step_name),
        ).fetchone()
        if existing is not None:
            return _loads(existing["state"])

        result = fn()

        with self._tx() as conn:
            conn.execute(
                "INSERT INTO checkpoints (task_id, checkpoint_name, state, "
                "owner_run_id, updated_at) VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT (task_id, checkpoint_name) DO NOTHING",
                (task_id, step_name, _dumps(result), owner_run_id, _fmt(self._now())),
            )
            # Read back the authoritative value: if a concurrent run committed
            # first, everyone agrees on the same stored result.
            stored = conn.execute(
                "SELECT state FROM checkpoints "
                "WHERE task_id = ? AND checkpoint_name = ?",
                (task_id, step_name),
            ).fetchone()
        return _loads(stored["state"])

    def get_checkpoint(self, *, task_id: str, step_name: str) -> Any:
        """Return a stored checkpoint payload, or ``None`` if absent."""
        row = (
            self._conn()
            .execute(
                "SELECT state FROM checkpoints WHERE task_id = ? AND checkpoint_name = ?",
                (task_id, step_name),
            )
            .fetchone()
        )
        return _loads(row["state"]) if row is not None else None

    # -- durable state -----------------------------------------------------
    #
    # A generic, namespaced key-value store. Unlike checkpoints (which belong to
    # a task and are removed when that task is cleaned up), state is independent
    # of any task and survives cleanup(). Use it for durable cross-task memory:
    # cursors, watermarks, "have I already processed this?" records, etc.

    def get_state(self, *, namespace: str, key: str, default: Any = None) -> Any:
        """Return the value stored at ``(namespace, key)``, or ``default``."""
        row = (
            self._conn()
            .execute(
                "SELECT value FROM state WHERE namespace = ? AND key = ?",
                (namespace, key),
            )
            .fetchone()
        )
        return _loads(row["value"]) if row is not None else default

    def has_state(self, namespace: str) -> bool:
        """Whether any key exists under ``namespace`` (cheap emptiness check)."""
        row = (
            self._conn()
            .execute("SELECT 1 FROM state WHERE namespace = ? LIMIT 1", (namespace,))
            .fetchone()
        )
        return row is not None

    def set_state(self, *, namespace: str, key: str, value: Any) -> None:
        """Store ``value`` at ``(namespace, key)`` (upsert, last write wins)."""
        with self._tx() as conn:
            conn.execute(
                "INSERT INTO state (namespace, key, value, updated_at) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT (namespace, key) DO UPDATE SET "
                "value = excluded.value, updated_at = excluded.updated_at",
                (namespace, key, _dumps(value), _fmt(self._now())),
            )

    def set_state_many(self, *, namespace: str, items: Mapping[str, Any]) -> int:
        """Upsert many ``key -> value`` entries under ``namespace`` in one
        transaction. Much faster than a loop of ``set_state`` for bulk
        imports (one commit, not one per key). Returns the number written.
        """
        now = _fmt(self._now())
        rows = [(namespace, key, _dumps(value), now) for key, value in items.items()]
        with self._tx() as conn:
            conn.executemany(
                "INSERT INTO state (namespace, key, value, updated_at) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT (namespace, key) DO UPDATE SET "
                "value = excluded.value, updated_at = excluded.updated_at",
                rows,
            )
        return len(rows)

    def delete_state(self, *, namespace: str, key: str) -> bool:
        """Delete ``(namespace, key)``. Returns ``True`` if it existed."""
        with self._tx() as conn:
            cur = conn.execute(
                "DELETE FROM state WHERE namespace = ? AND key = ?",
                (namespace, key),
            )
            return cur.rowcount > 0

    def list_state(self, namespace: str) -> dict[str, Any]:
        """Return all ``key -> value`` pairs in ``namespace`` (ordered by key)."""
        rows = (
            self._conn()
            .execute(
                "SELECT key, value FROM state WHERE namespace = ? ORDER BY key",
                (namespace,),
            )
            .fetchall()
        )
        return {row["key"]: _loads(row["value"]) for row in rows}

    def update_state(
        self,
        *,
        namespace: str,
        key: str,
        fn: Callable[[Any], Any],
        default: Any = None,
    ) -> Any:
        """Atomic read-modify-write of ``(namespace, key)``.

        ``fn`` receives the current value (or ``default`` if absent) and returns
        the new value, all inside the write transaction so concurrent workers
        cannot interleave. Keep ``fn`` pure and fast - no I/O - since it runs
        while the database write lock is held. Returns the new value.
        """
        with self._tx() as conn:
            row = conn.execute(
                "SELECT value FROM state WHERE namespace = ? AND key = ?",
                (namespace, key),
            ).fetchone()
            current = _loads(row["value"]) if row is not None else default
            new_value = fn(current)
            conn.execute(
                "INSERT INTO state (namespace, key, value, updated_at) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT (namespace, key) DO UPDATE SET "
                "value = excluded.value, updated_at = excluded.updated_at",
                (namespace, key, _dumps(new_value), _fmt(self._now())),
            )
        return new_value

    # -- events ------------------------------------------------------------

    def emit_event(self, *, event_name: str, payload: Any = None) -> None:
        """Emit a named signal (first write wins) and wake its waiters."""
        now = self._now()
        now_str = _fmt(now)
        with self._tx() as conn:
            already = conn.execute(
                "SELECT 1 FROM events WHERE event_name = ?", (event_name,)
            ).fetchone()
            if already is not None:
                return  # first emit wins; ignore later ones

            conn.execute(
                "INSERT INTO events (event_name, payload, emitted_at) VALUES (?, ?, ?)",
                (event_name, _dumps(payload), now_str),
            )

            waiters = conn.execute(
                "SELECT run_id, task_id FROM waits "
                "WHERE event_name = ? AND (timeout_at IS NULL OR timeout_at > ?)",
                (event_name, now_str),
            ).fetchall()
            for waiter in waiters:
                conn.execute(
                    "UPDATE runs SET state = 'pending', available_at = ?, "
                    "event_payload = ?, wake_event = NULL, claimed_by = NULL, "
                    "claim_expires_at = NULL "
                    "WHERE run_id = ? AND state = 'sleeping'",
                    (now_str, _dumps(payload), waiter["run_id"]),
                )
                conn.execute(
                    "UPDATE tasks SET state = 'pending' WHERE task_id = ?",
                    (waiter["task_id"],),
                )
            conn.execute("DELETE FROM waits WHERE event_name = ?", (event_name,))

    def await_event(
        self,
        *,
        run_id: str,
        task_id: str,
        step_name: str,
        event_name: str,
        timeout_secs: int | None = None,
    ) -> tuple[bool, Any]:
        """Suspend the current run until ``event_name`` fires or the timeout.

        Returns ``(should_suspend, payload)``:

        * ``(False, payload)`` - the event already fired (or fired while we
          slept); the worker continues.
        * ``(False, None)``    - the wait timed out; the worker continues.
        * ``(True, None)``     - the run has been parked; the worker must return
          without completing or failing the run. It will be re-claimed when the
          event fires or the timeout elapses.

        The outcome is checkpointed, so re-execution after a resume resolves the
        same step without parking again.
        """
        now = self._now()
        with self._tx() as conn:
            # 1. Resolved on a previous pass?
            checkpoint = conn.execute(
                "SELECT state FROM checkpoints "
                "WHERE task_id = ? AND checkpoint_name = ?",
                (task_id, step_name),
            ).fetchone()
            if checkpoint is not None:
                return False, _loads(checkpoint["state"])

            run = conn.execute(
                "SELECT wake_event, event_payload FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if run is None:
                raise TaskNotFound(f"Run {run_id} not found")

            # 2. Woken by emit_event: payload was delivered onto our run.
            if run["event_payload"] is not None:
                payload = _loads(run["event_payload"])
                self._write_checkpoint(conn, task_id, step_name, payload, run_id, now)
                conn.execute(
                    "UPDATE runs SET event_payload = NULL, wake_event = NULL "
                    "WHERE run_id = ?",
                    (run_id,),
                )
                return False, payload

            # 3. Event already emitted before we asked.
            event = conn.execute(
                "SELECT payload FROM events WHERE event_name = ?", (event_name,)
            ).fetchone()
            if event is not None:
                payload = _loads(event["payload"])
                self._write_checkpoint(conn, task_id, step_name, payload, run_id, now)
                return False, payload

            # 4. Woken by timeout (we were already waiting on this event).
            if run["wake_event"] == event_name:
                conn.execute(
                    "UPDATE runs SET wake_event = NULL WHERE run_id = ?", (run_id,)
                )
                conn.execute(
                    "DELETE FROM waits WHERE run_id = ? AND step_name = ?",
                    (run_id, step_name),
                )
                self._write_checkpoint(conn, task_id, step_name, None, run_id, now)
                return False, None

            # 5. First encounter: register the wait and park the run.
            if timeout_secs is None:
                timeout_at = None
                available_at = _INFINITY
            else:
                deadline = now + timedelta(seconds=timeout_secs)
                timeout_at = _fmt(deadline)
                available_at = timeout_at
            conn.execute(
                "INSERT INTO waits (run_id, step_name, task_id, event_name, timeout_at) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT (run_id, step_name) DO UPDATE SET "
                "event_name = excluded.event_name, timeout_at = excluded.timeout_at",
                (run_id, step_name, task_id, event_name, timeout_at),
            )
            conn.execute(
                "UPDATE runs SET state = 'sleeping', wake_event = ?, "
                "available_at = ?, claimed_by = NULL, claim_expires_at = NULL "
                "WHERE run_id = ?",
                (event_name, available_at, run_id),
            )
            conn.execute(
                "UPDATE tasks SET state = 'sleeping' WHERE task_id = ?", (task_id,)
            )
            return True, None

    def wait_for_event(
        self,
        *,
        run_id: str,
        task_id: str,
        step_name: str,
        event_name: str,
        timeout_secs: int | None = None,
    ) -> Any:
        """Ergonomic wrapper over ``await_event`` for workflow handlers.

        Returns the event payload (or ``None`` on timeout) when the run may
        proceed. When the run is parked, it raises ``WorkflowSuspended``,
        which unwinds the handler so the worker loop skips completion. Use this
        inside handlers; use ``await_event`` when you need the raw tuple.
        """
        should_suspend, payload = self.await_event(
            run_id=run_id,
            task_id=task_id,
            step_name=step_name,
            event_name=event_name,
            timeout_secs=timeout_secs,
        )
        if should_suspend:
            raise WorkflowSuspended(task_id)
        return payload

    # -- read models / maintenance ----------------------------------------

    def get_task(self, task_id: str) -> TaskInfo:
        """Return the current state of a task."""
        conn = self._conn()
        row = conn.execute(
            "SELECT state, attempts, completed_payload, last_attempt_run "
            "FROM tasks WHERE task_id = ?",
            (task_id,),
        ).fetchone()
        if row is None:
            raise TaskNotFound(f"Task {task_id} not found")

        result = None
        failure_reason = None
        if row["state"] == "completed":
            result = _loads(row["completed_payload"])
        elif row["state"] == "failed" and row["last_attempt_run"]:
            run = conn.execute(
                "SELECT failure_reason FROM runs WHERE run_id = ?",
                (row["last_attempt_run"],),
            ).fetchone()
            if run is not None:
                failure_reason = _loads(run["failure_reason"])

        return TaskInfo(
            task_id=task_id,
            state=row["state"],
            attempts=row["attempts"],
            result=result,
            failure_reason=failure_reason,
        )

    def ready_run_count(self) -> int:
        """Number of runs claimable right now (the work backlog).

        Mirrors claim_task's candidacy: runs in ``pending``/``sleeping`` whose
        task is not terminal and whose ``available_at`` has arrived. Useful as a
        queue-depth gauge.
        """
        now = _fmt(self._now())
        row = (
            self._conn()
            .execute(
                "SELECT COUNT(*) AS n FROM runs r JOIN tasks t ON t.task_id = r.task_id "
                "WHERE r.state IN ('pending', 'sleeping') "
                "AND t.state IN ('pending', 'sleeping', 'running') "
                "AND r.available_at <= ?",
                (now,),
            )
            .fetchone()
        )
        return row["n"]

    def task_counts_by_state(self) -> dict[str, int]:
        """Number of tasks in each state, e.g. ``{"pending": 3, "failed": 1}``.

        States with no tasks are omitted rather than reported as zero.
        """
        rows = self._conn().execute(
            "SELECT state, COUNT(*) AS n FROM tasks GROUP BY state"
        )
        return {row["state"]: row["n"] for row in rows}

    def task_counts_by_name_and_state(self) -> dict[str, dict[str, int]]:
        """Task counts grouped by ``task_name``, then by ``state``.

        e.g. ``{"send_file": {"pending": 2, "completed": 5}}``. Names and
        states with no matching tasks are omitted.
        """
        rows = self._conn().execute(
            "SELECT task_name, state, COUNT(*) AS n FROM tasks "
            "GROUP BY task_name, state"
        )
        counts: dict[str, dict[str, int]] = {}
        for row in rows:
            counts.setdefault(row["task_name"], {})[row["state"]] = row["n"]
        return counts

    def recent_failures(self, *, limit: int = 20) -> list[FailureInfo]:
        """The most recent failed runs, newest first, with their reason.

        Includes every failed attempt, not just a task's latest one: a task
        retried three times and failed each time contributes three entries.
        """
        rows = self._conn().execute(
            "SELECT r.run_id, r.task_id, t.task_name, r.attempt, r.failed_at, "
            "r.failure_reason FROM runs r JOIN tasks t ON t.task_id = r.task_id "
            "WHERE r.state = 'failed' ORDER BY r.failed_at DESC LIMIT ?",
            (limit,),
        )
        return [
            FailureInfo(
                run_id=row["run_id"],
                task_id=row["task_id"],
                task_name=row["task_name"],
                attempt=row["attempt"],
                failed_at=row["failed_at"],
                failure_reason=_loads(row["failure_reason"]),
            )
            for row in rows
        ]

    def cleanup(self, ttl: timedelta = timedelta(days=30)) -> int:
        """Delete terminal tasks (and their rows) older than ``ttl``.

        Durable ``state`` is deliberately left untouched: it is meant to outlive
        the tasks that wrote it. Returns the number of tasks removed.
        """
        cutoff = _fmt(self._now() - ttl)
        with self._tx() as conn:
            ids = [
                r["task_id"]
                for r in conn.execute(
                    "SELECT task_id FROM tasks "
                    "WHERE state IN ('completed', 'failed', 'cancelled') "
                    "AND terminal_at IS NOT NULL AND terminal_at < ?",
                    (cutoff,),
                ).fetchall()
            ]
            for task_id in ids:
                conn.execute("DELETE FROM waits WHERE task_id = ?", (task_id,))
                conn.execute("DELETE FROM checkpoints WHERE task_id = ?", (task_id,))
                conn.execute("DELETE FROM runs WHERE task_id = ?", (task_id,))
                conn.execute("DELETE FROM tasks WHERE task_id = ?", (task_id,))
        # VACUUM cannot run inside a transaction; run it once afterwards.
        if ids:
            self._conn().execute("VACUUM")
        return len(ids)

    # -- helpers -----------------------------------------------------------

    @staticmethod
    def _guard_running(row: sqlite3.Row | None, run_id: str) -> None:
        if row is None:
            raise TaskNotFound(f"Run {run_id} not found")
        if row["state"] == "cancelled":
            raise TaskCancelledError(f"Run {run_id} belongs to a cancelled task")
        if row["state"] != "running":
            raise InvalidRunState(f"Run {run_id} is {row['state']}, not running")

    @staticmethod
    def _write_checkpoint(
        conn: sqlite3.Connection,
        task_id: str,
        step_name: str,
        payload: Any,
        run_id: str,
        now: datetime,
    ) -> None:
        conn.execute(
            "INSERT INTO checkpoints (task_id, checkpoint_name, state, "
            "owner_run_id, updated_at) VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT (task_id, checkpoint_name) DO NOTHING",
            (task_id, step_name, _dumps(payload), run_id, _fmt(now)),
        )


def _dumps(value: Any) -> str:
    return json.dumps(value)


def _loads(value: str | None) -> Any:
    return json.loads(value) if value is not None else None
