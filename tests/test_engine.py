"""Unit tests for the SQLite durable execution engine.

Each test exercises a single behaviour in isolation. Time is driven by a
controllable clock so retries, delays, leases and timeouts are deterministic
without sleeping.
"""

from datetime import timedelta

import pytest

from dura.engine import (
    DurableEngine,
    InvalidRunState,
    RetryStrategy,
    TaskCancelledError,
    TaskNotFound,
    WorkflowSuspended,
)


# -- spawn -----------------------------------------------------------------


def test_spawn_creates_pending_task(engine):
    ref = engine.spawn_task("send_file", {"uri": "/a.csv"})

    assert ref.created is True
    assert engine.get_task(ref.task_id).state == "pending"


def test_spawn_with_idempotency_key_is_deduplicated(engine):
    first = engine.spawn_task("send_file", {"uri": "/a.csv"}, idempotency_key="k1")
    second = engine.spawn_task("send_file", {"uri": "/a.csv"}, idempotency_key="k1")

    assert first.created is True
    assert second.created is False
    assert second.task_id == first.task_id


def test_spawn_different_keys_create_distinct_tasks(engine):
    a = engine.spawn_task("send_file", {}, idempotency_key="a")
    b = engine.spawn_task("send_file", {}, idempotency_key="b")

    assert a.task_id != b.task_id


# -- claim / complete ------------------------------------------------------


def test_claim_returns_spawned_task(engine):
    engine.spawn_task("send_file", {"uri": "/a.csv"})

    claimed = engine.claim_task("worker-1")

    assert claimed is not None
    assert claimed.name == "send_file"
    assert claimed.params == {"uri": "/a.csv"}


def test_claim_returns_none_when_nothing_available(engine):
    assert engine.claim_task("worker-1") is None


def test_claimed_task_is_not_claimed_again(engine):
    engine.spawn_task("send_file", {})
    engine.claim_task("worker-1")

    assert engine.claim_task("worker-2") is None


def test_complete_run_marks_task_completed_with_result(engine):
    engine.spawn_task("send_file", {})
    claimed = engine.claim_task("worker-1")

    engine.complete_run(claimed.run_id, {"s3_key": "bucket/a.csv"})

    info = engine.get_task(claimed.task_id)
    assert info.state == "completed"
    assert info.result == {"s3_key": "bucket/a.csv"}


def test_claim_is_fifo_by_availability(engine):
    engine.spawn_task("t", {"n": 1})
    engine.spawn_task("t", {"n": 2})

    assert engine.claim_task("w").params == {"n": 1}
    assert engine.claim_task("w").params == {"n": 2}


# -- checkpoints -----------------------------------------------------------


def test_checkpoint_runs_function_once(engine):
    ref = engine.spawn_task("t", {})
    calls = []

    def step():
        calls.append(1)
        return "result"

    first = engine.checkpoint(ref.task_id, "fetch", step)
    second = engine.checkpoint(ref.task_id, "fetch", step)

    assert first == second == "result"
    assert len(calls) == 1


def test_checkpoint_persists_across_retry(engine):
    """A checkpoint from one run is visible to a later run of the same task."""
    ref = engine.spawn_task("t", {}, max_attempts=3)
    calls = []

    def step():
        calls.append(1)
        return "fetched"

    # First attempt records the checkpoint, then fails.
    first_run = engine.claim_task("w")
    engine.checkpoint(ref.task_id, "fetch", step, owner_run_id=first_run.run_id)
    engine.fail_run(first_run.run_id, {"error": "boom"})

    # Second attempt: same checkpoint resolves without re-running the step.
    second_run = engine.claim_task("w")
    value = engine.checkpoint(
        ref.task_id, "fetch", step, owner_run_id=second_run.run_id
    )

    assert value == "fetched"
    assert len(calls) == 1


def test_get_checkpoint_returns_none_when_absent(engine):
    ref = engine.spawn_task("t", {})
    assert engine.get_checkpoint(ref.task_id, "missing") is None


# -- retries ---------------------------------------------------------------


def test_fail_run_retries_until_max_attempts(engine):
    ref = engine.spawn_task("t", {}, max_attempts=2)

    run = engine.claim_task("w")
    engine.fail_run(run.run_id, {"error": "1"})
    # A new attempt is immediately available (no retry strategy = no delay).
    assert engine.get_task(ref.task_id).state == "pending"

    run = engine.claim_task("w")
    assert run.attempt == 2
    engine.fail_run(run.run_id, {"error": "2"})

    info = engine.get_task(ref.task_id)
    assert info.state == "failed"
    assert info.failure_reason == {"error": "2"}


def test_exponential_backoff_delays_next_attempt(engine, clock):
    ref = engine.spawn_task(
        "t",
        {},
        retry=RetryStrategy(kind="exponential", base_seconds=30, jitter_factor=0.0),
        max_attempts=5,
    )

    run = engine.claim_task("w")
    engine.fail_run(run.run_id, {"error": "x"})

    # The retry is parked for 30s; not claimable yet.
    assert engine.get_task(ref.task_id).state == "sleeping"
    assert engine.claim_task("w") is None

    clock.advance(30)
    retried = engine.claim_task("w")
    assert retried is not None
    assert retried.attempt == 2


def test_no_retry_when_max_attempts_is_one(engine):
    ref = engine.spawn_task("t", {}, max_attempts=1)

    run = engine.claim_task("w")
    engine.fail_run(run.run_id, {"error": "fatal"})

    assert engine.get_task(ref.task_id).state == "failed"


# -- delayed tasks ---------------------------------------------------------


def test_available_after_delays_first_claim(engine, clock):
    engine.spawn_task("t", {}, available_after=timedelta(seconds=60))

    assert engine.claim_task("w") is None

    clock.advance(60)
    assert engine.claim_task("w") is not None


# -- lease expiry ----------------------------------------------------------


def test_expired_lease_is_reclaimed(engine, clock):
    engine.spawn_task("t", {})
    first = engine.claim_task("worker-1", timeout_secs=30)

    # Worker-1 "crashes" and never completes; lease expires.
    clock.advance(31)
    second = engine.claim_task("worker-2", timeout_secs=30)

    assert second is not None
    assert second.run_id == first.run_id
    assert second.attempt == first.attempt  # a crash does not consume a retry


def test_extend_claim_prevents_reclaim(engine, clock):
    engine.spawn_task("t", {})
    first = engine.claim_task("worker-1", timeout_secs=30)

    clock.advance(20)
    # Heartbeat pushes the lease out to t=50 before the original (t=30) expires.
    engine.extend_claim(first.run_id, by_secs=30)

    clock.advance(11)  # t=31: original lease would have expired, extended one holds.
    assert engine.claim_task("worker-2", timeout_secs=30) is None


# -- events ----------------------------------------------------------------


def test_await_event_resolves_when_already_emitted(engine):
    ref = engine.spawn_task("t", {})
    run = engine.claim_task("w")
    engine.emit_event("config_changed", {"version": 2})

    suspend, payload = engine.await_event(
        run.run_id, ref.task_id, "wait", "config_changed"
    )

    assert suspend is False
    assert payload == {"version": 2}


def test_await_event_parks_then_resumes_on_emit(engine):
    ref = engine.spawn_task("t", {})
    run = engine.claim_task("w")

    # No event yet: the run is parked.
    suspend, payload = engine.await_event(
        run.run_id, ref.task_id, "wait", "config_changed"
    )
    assert suspend is True
    assert engine.get_task(ref.task_id).state == "sleeping"

    # Emit wakes it; it becomes claimable again.
    engine.emit_event("config_changed", {"version": 3})
    resumed = engine.claim_task("w")
    assert resumed.run_id == run.run_id

    # Re-running the step now returns the payload without parking.
    suspend, payload = engine.await_event(
        resumed.run_id, ref.task_id, "wait", "config_changed"
    )
    assert suspend is False
    assert payload == {"version": 3}


def test_await_event_times_out(engine, clock):
    ref = engine.spawn_task("t", {})
    run = engine.claim_task("w")

    suspend, _ = engine.await_event(
        run.run_id, ref.task_id, "wait", "never", timeout_secs=60
    )
    assert suspend is True

    clock.advance(61)
    resumed = engine.claim_task("w")
    assert resumed.run_id == run.run_id

    suspend, payload = engine.await_event(
        resumed.run_id, ref.task_id, "wait", "never", timeout_secs=60
    )
    assert suspend is False
    assert payload is None


def test_emit_event_first_write_wins(engine):
    ref = engine.spawn_task("t", {})
    run = engine.claim_task("w")

    engine.emit_event("e", {"v": 1})
    engine.emit_event("e", {"v": 2})  # ignored

    _, payload = engine.await_event(run.run_id, ref.task_id, "wait", "e")
    assert payload == {"v": 1}


def test_wait_for_event_raises_when_parked(engine):
    ref = engine.spawn_task("t", {})
    run = engine.claim_task("w")

    with pytest.raises(WorkflowSuspended):
        engine.wait_for_event(run.run_id, ref.task_id, "wait", "later")

    assert engine.get_task(ref.task_id).state == "sleeping"


def test_wait_for_event_returns_payload_when_available(engine):
    ref = engine.spawn_task("t", {})
    run = engine.claim_task("w")
    engine.emit_event("ready", {"v": 7})

    payload = engine.wait_for_event(run.run_id, ref.task_id, "wait", "ready")

    assert payload == {"v": 7}


def test_completing_a_parked_run_is_rejected(engine):
    """A handler that suspends must not also be completed by the worker."""
    ref = engine.spawn_task("t", {})
    run = engine.claim_task("w")
    with pytest.raises(WorkflowSuspended):
        engine.wait_for_event(run.run_id, ref.task_id, "wait", "later")

    with pytest.raises(InvalidRunState):
        engine.complete_run(run.run_id, {})


# -- cancellation ----------------------------------------------------------


def test_cancel_task_makes_completion_fail(engine):
    ref = engine.spawn_task("t", {})
    run = engine.claim_task("w")

    engine.cancel_task(ref.task_id)

    assert engine.get_task(ref.task_id).state == "cancelled"
    with pytest.raises(TaskCancelledError):
        engine.complete_run(run.run_id, {})


# -- read models / errors --------------------------------------------------


def test_get_task_raises_for_unknown_task(engine):
    with pytest.raises(TaskNotFound):
        engine.get_task("does-not-exist")


def test_complete_unknown_run_raises(engine):
    with pytest.raises(TaskNotFound):
        engine.complete_run("nope")


def test_double_complete_raises(engine):
    engine.spawn_task("t", {})
    run = engine.claim_task("w")
    engine.complete_run(run.run_id)

    with pytest.raises(InvalidRunState):
        engine.complete_run(run.run_id)


# -- durability ------------------------------------------------------------


def test_state_survives_engine_restart(tmp_path, clock):
    path = tmp_path / "engine.db"
    engine1 = DurableEngine(path, clock=clock)
    ref = engine1.spawn_task("send_file", {"uri": "/a.csv"})
    engine1.close()

    # A fresh engine on the same file sees the pending task.
    engine2 = DurableEngine(path, clock=clock)
    claimed = engine2.claim_task("w")
    assert claimed is not None
    assert claimed.task_id == ref.task_id


# -- durable state ---------------------------------------------------------


def test_set_and_get_state(engine):
    engine.set_state("files", "/a.csv", {"mtime": 1, "size": 10})
    assert engine.get_state("files", "/a.csv") == {"mtime": 1, "size": 10}


def test_get_state_returns_default_when_absent(engine):
    assert engine.get_state("files", "/missing") is None
    assert engine.get_state("files", "/missing", default={}) == {}


def test_set_state_overwrites(engine):
    engine.set_state("ns", "k", "first")
    engine.set_state("ns", "k", "second")
    assert engine.get_state("ns", "k") == "second"


def test_delete_state_reports_existence(engine):
    engine.set_state("ns", "k", 1)
    assert engine.delete_state("ns", "k") is True
    assert engine.delete_state("ns", "k") is False
    assert engine.get_state("ns", "k") is None


def test_namespaces_are_isolated(engine):
    engine.set_state("a", "k", "from-a")
    engine.set_state("b", "k", "from-b")
    assert engine.get_state("a", "k") == "from-a"
    assert engine.get_state("b", "k") == "from-b"


def test_list_state_returns_namespace_contents(engine):
    engine.set_state("files", "/a", 1)
    engine.set_state("files", "/b", 2)
    engine.set_state("other", "/c", 3)
    assert engine.list_state("files") == {"/a": 1, "/b": 2}


def test_set_state_many_bulk_upserts(engine):
    written = engine.set_state_many("files", {"/a": {"mtime": 1}, "/b": {"mtime": 2}})
    assert written == 2
    assert engine.get_state("files", "/a") == {"mtime": 1}
    assert engine.get_state("files", "/b") == {"mtime": 2}

    # Re-running overwrites existing keys (upsert).
    engine.set_state_many("files", {"/a": {"mtime": 9}})
    assert engine.get_state("files", "/a") == {"mtime": 9}


def test_set_state_many_empty_is_noop(engine):
    assert engine.set_state_many("files", {}) == 0


def test_has_state_reports_namespace_emptiness(engine):
    assert engine.has_state("files") is False
    engine.set_state("files", "/a", {"mtime": 1})
    assert engine.has_state("files") is True
    assert engine.has_state("other") is False


def test_update_state_is_atomic_read_modify_write(engine):
    engine.update_state("counters", "n", lambda cur: (cur or 0) + 1, default=0)
    engine.update_state("counters", "n", lambda cur: cur + 1)
    assert engine.get_state("counters", "n") == 2


def test_state_survives_cleanup(engine, clock):
    ref = engine.spawn_task("t", {})
    run = engine.claim_task("w")
    engine.complete_run(run.run_id)
    engine.set_state("files", "/a.csv", {"mtime": 1})

    clock.advance(timedelta(days=31).total_seconds())
    removed = engine.cleanup(ttl=timedelta(days=30))

    assert removed == 1  # the task is gone
    with pytest.raises(TaskNotFound):
        engine.get_task(ref.task_id)
    assert engine.get_state("files", "/a.csv") == {"mtime": 1}  # state remains


# -- cleanup ---------------------------------------------------------------


def test_cleanup_removes_old_terminal_tasks(engine, clock):
    ref = engine.spawn_task("t", {})
    run = engine.claim_task("w")
    engine.complete_run(run.run_id)

    # Not old enough yet.
    assert engine.cleanup(ttl=timedelta(days=30)) == 0

    clock.advance(timedelta(days=31).total_seconds())
    assert engine.cleanup(ttl=timedelta(days=30)) == 1
    with pytest.raises(TaskNotFound):
        engine.get_task(ref.task_id)


# -- cancel_duplicate_tasks ------------------------------------------------


def test_cancel_duplicate_tasks_no_tasks(engine):
    survivor, cancelled = engine.cancel_duplicate_tasks("backup")
    assert survivor is None
    assert cancelled == 0


def test_cancel_duplicate_tasks_single_task_untouched(engine):
    ref = engine.spawn_task("backup", {})
    survivor, cancelled = engine.cancel_duplicate_tasks("backup")
    assert survivor == ref.task_id
    assert cancelled == 0
    assert engine.get_task(ref.task_id).state == "pending"


def test_cancel_duplicate_tasks_keeps_oldest_cancels_rest(engine, clock):
    first = engine.spawn_task("backup", {})
    clock.advance(1)
    second = engine.spawn_task("backup", {})
    clock.advance(1)
    third = engine.spawn_task("backup", {})

    survivor, cancelled = engine.cancel_duplicate_tasks("backup")

    assert survivor == first.task_id
    assert cancelled == 2
    assert engine.get_task(first.task_id).state == "pending"
    assert engine.get_task(second.task_id).state == "cancelled"
    assert engine.get_task(third.task_id).state == "cancelled"


def test_cancel_duplicate_tasks_ignores_terminal_tasks(engine, clock):
    completed = engine.spawn_task("backup", {})
    engine.complete_run(engine.claim_task("w").run_id, {})
    clock.advance(1)
    active = engine.spawn_task("backup", {})

    survivor, cancelled = engine.cancel_duplicate_tasks("backup")

    assert survivor == active.task_id
    assert cancelled == 0
    assert engine.get_task(completed.task_id).state == "completed"
