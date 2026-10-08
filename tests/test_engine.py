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
    _retry_delay,
)

# -- spawn -----------------------------------------------------------------


def test_spawn_creates_pending_task(engine):
    ref = engine.spawn_task(name="send_file", params={"uri": "/a.csv"})

    assert ref.created is True
    assert engine.get_task(ref.task_id).state == "pending"


def test_spawn_with_idempotency_key_is_deduplicated(engine):
    first = engine.spawn_task(
        name="send_file", params={"uri": "/a.csv"}, idempotency_key="k1"
    )
    second = engine.spawn_task(
        name="send_file", params={"uri": "/a.csv"}, idempotency_key="k1"
    )

    assert first.created is True
    assert second.created is False
    assert second.task_id == first.task_id


def test_spawn_different_keys_create_distinct_tasks(engine):
    a = engine.spawn_task(name="send_file", params={}, idempotency_key="a")
    b = engine.spawn_task(name="send_file", params={}, idempotency_key="b")

    assert a.task_id != b.task_id


# -- claim / complete ------------------------------------------------------


def test_claim_returns_spawned_task(engine):
    engine.spawn_task(name="send_file", params={"uri": "/a.csv"})

    claimed = engine.claim_task(worker_id="worker-1")

    assert claimed is not None
    assert claimed.name == "send_file"
    assert claimed.params == {"uri": "/a.csv"}


def test_claim_returns_none_when_nothing_available(engine):
    assert engine.claim_task(worker_id="worker-1") is None


def test_claimed_task_is_not_claimed_again(engine):
    engine.spawn_task(name="send_file", params={})
    engine.claim_task(worker_id="worker-1")

    assert engine.claim_task(worker_id="worker-2") is None


def test_complete_run_marks_task_completed_with_result(engine):
    engine.spawn_task(name="send_file", params={})
    claimed = engine.claim_task(worker_id="worker-1")

    engine.complete_run(run_id=claimed.run_id, result={"s3_key": "bucket/a.csv"})

    info = engine.get_task(claimed.task_id)
    assert info.state == "completed"
    assert info.result == {"s3_key": "bucket/a.csv"}


def test_claim_is_fifo_by_availability(engine):
    engine.spawn_task(name="t", params={"n": 1})
    engine.spawn_task(name="t", params={"n": 2})

    assert engine.claim_task(worker_id="w").params == {"n": 1}
    assert engine.claim_task(worker_id="w").params == {"n": 2}


# -- checkpoints -----------------------------------------------------------


def test_checkpoint_runs_function_once(engine):
    ref = engine.spawn_task(name="t", params={})
    calls = []

    def step():
        calls.append(1)
        return "result"

    first = engine.checkpoint(task_id=ref.task_id, step_name="fetch", fn=step)
    second = engine.checkpoint(task_id=ref.task_id, step_name="fetch", fn=step)

    assert first == second == "result"
    assert len(calls) == 1


def test_checkpoint_persists_across_retry(engine):
    """A checkpoint from one run is visible to a later run of the same task."""
    ref = engine.spawn_task(name="t", params={}, max_attempts=3)
    calls = []

    def step():
        calls.append(1)
        return "fetched"

    # First attempt records the checkpoint, then fails.
    first_run = engine.claim_task(worker_id="w")
    engine.checkpoint(
        task_id=ref.task_id, step_name="fetch", fn=step, owner_run_id=first_run.run_id
    )
    engine.fail_run(run_id=first_run.run_id, reason={"error": "boom"})

    # Second attempt: same checkpoint resolves without re-running the step.
    second_run = engine.claim_task(worker_id="w")
    value = engine.checkpoint(
        task_id=ref.task_id,
        step_name="fetch",
        fn=step,
        owner_run_id=second_run.run_id,
    )

    assert value == "fetched"
    assert len(calls) == 1


def test_get_checkpoint_returns_none_when_absent(engine):
    ref = engine.spawn_task(name="t", params={})
    assert engine.get_checkpoint(task_id=ref.task_id, step_name="missing") is None


# -- retries ---------------------------------------------------------------


def test_fail_run_retries_until_max_attempts(engine):
    ref = engine.spawn_task(name="t", params={}, max_attempts=2)

    run = engine.claim_task(worker_id="w")
    engine.fail_run(run_id=run.run_id, reason={"error": "1"})
    # A new attempt is immediately available (no retry strategy = no delay).
    assert engine.get_task(ref.task_id).state == "pending"

    run = engine.claim_task(worker_id="w")
    assert run.attempt == 2
    engine.fail_run(run_id=run.run_id, reason={"error": "2"})

    info = engine.get_task(ref.task_id)
    assert info.state == "failed"
    assert info.failure_reason == {"error": "2"}


def test_exponential_backoff_delays_next_attempt(engine, clock):
    ref = engine.spawn_task(
        name="t",
        params={},
        retry=RetryStrategy(kind="exponential", base_seconds=30, jitter_factor=0.0),
        max_attempts=5,
    )

    run = engine.claim_task(worker_id="w")
    engine.fail_run(run_id=run.run_id, reason={"error": "x"})

    # The retry is parked for 30s; not claimable yet.
    assert engine.get_task(ref.task_id).state == "sleeping"
    assert engine.claim_task(worker_id="w") is None

    clock.advance(30)
    retried = engine.claim_task(worker_id="w")
    assert retried is not None
    assert retried.attempt == 2


def test_backoff_is_capped_at_max_seconds_for_unbounded_attempts():
    """A task with no max_attempts retries forever; the delay must not.

    ``factor ** (attempt - 1)`` overflows a float once the exponent passes
    ~1024, which used to crash the worker in ``fail_run``.
    """
    strategy = RetryStrategy(
        kind="exponential", base_seconds=30, max_seconds=3600, jitter_factor=0.0
    ).to_dict()

    assert _retry_delay(strategy, 1) == 30
    assert _retry_delay(strategy, 8) == 3600
    assert _retry_delay(strategy, 5_000) == 3600


def test_backoff_without_max_seconds_falls_back_to_a_ceiling():
    strategy = RetryStrategy(
        kind="exponential", base_seconds=30, jitter_factor=0.0
    ).to_dict()

    assert _retry_delay(strategy, 5_000) == 3000


# -- RetryStrategy validation -----------------------------------------------


def test_retry_strategy_accepts_sane_configs():
    RetryStrategy(kind="none")
    RetryStrategy(kind="fixed", base_seconds=60)
    RetryStrategy(kind="exponential", base_seconds=30, factor=2.0, max_seconds=3600)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"kind": "linear"},  # not a supported kind
        {"base_seconds": -1},
        {"factor": 0},
        {"factor": -2.0},
        {"max_seconds": 0},
        {"max_seconds": -10},
        {"base_seconds": 100, "max_seconds": 10},  # cap below the base delay
        {"jitter_factor": -0.1},
        {"kind": "exponential", "base_seconds": 0},  # the OverflowError trigger
        {"kind": "exponential", "factor": 1.0},  # never grows: not backoff
        {"kind": "exponential", "factor": 0.5},  # shrinks: not backoff
    ],
)
def test_retry_strategy_rejects_bad_values(kwargs):
    with pytest.raises(ValueError):
        RetryStrategy(**kwargs)


def test_no_retry_when_max_attempts_is_one(engine):
    ref = engine.spawn_task(name="t", params={}, max_attempts=1)

    run = engine.claim_task(worker_id="w")
    engine.fail_run(run_id=run.run_id, reason={"error": "fatal"})

    assert engine.get_task(ref.task_id).state == "failed"


def test_fail_run_not_retryable_fails_immediately_despite_attempts_remaining(engine):
    # max_attempts=None would normally retry forever; retryable=False overrides
    # that for failures a retry can never fix (e.g. no handler registered).
    ref = engine.spawn_task(name="t", params={})

    run = engine.claim_task(worker_id="w")
    engine.fail_run(run_id=run.run_id, reason={"error": "fatal"}, retryable=False)

    info = engine.get_task(ref.task_id)
    assert info.state == "failed"
    assert info.failure_reason == {"error": "fatal"}
    assert engine.claim_task(worker_id="w") is None


# -- delayed tasks ---------------------------------------------------------


def test_available_after_delays_first_claim(engine, clock):
    engine.spawn_task(name="t", params={}, available_after=timedelta(seconds=60))

    assert engine.claim_task(worker_id="w") is None

    clock.advance(60)
    assert engine.claim_task(worker_id="w") is not None


# -- lease expiry ----------------------------------------------------------


def test_expired_lease_is_reclaimed(engine, clock):
    engine.spawn_task(name="t", params={})
    first = engine.claim_task(worker_id="worker-1", timeout_secs=30)

    # Worker-1 "crashes" and never completes; lease expires.
    clock.advance(31)
    second = engine.claim_task(worker_id="worker-2", timeout_secs=30)

    assert second is not None
    assert second.run_id == first.run_id
    assert second.attempt == first.attempt  # a crash does not consume a retry


def test_extend_claim_prevents_reclaim(engine, clock):
    engine.spawn_task(name="t", params={})
    first = engine.claim_task(worker_id="worker-1", timeout_secs=30)

    clock.advance(20)
    # Heartbeat pushes the lease out to t=50 before the original (t=30) expires.
    engine.extend_claim(run_id=first.run_id, by_secs=30)

    clock.advance(11)  # t=31: original lease would have expired, extended one holds.
    assert engine.claim_task(worker_id="worker-2", timeout_secs=30) is None


# -- events ----------------------------------------------------------------


def test_await_event_resolves_when_already_emitted(engine):
    ref = engine.spawn_task(name="t", params={})
    run = engine.claim_task(worker_id="w")
    engine.emit_event(event_name="config_changed", payload={"version": 2})

    suspend, payload = engine.await_event(
        run_id=run.run_id,
        task_id=ref.task_id,
        step_name="wait",
        event_name="config_changed",
    )

    assert suspend is False
    assert payload == {"version": 2}


def test_await_event_parks_then_resumes_on_emit(engine):
    ref = engine.spawn_task(name="t", params={})
    run = engine.claim_task(worker_id="w")

    # No event yet: the run is parked.
    suspend, payload = engine.await_event(
        run_id=run.run_id,
        task_id=ref.task_id,
        step_name="wait",
        event_name="config_changed",
    )
    assert suspend is True
    assert engine.get_task(ref.task_id).state == "sleeping"

    # Emit wakes it; it becomes claimable again.
    engine.emit_event(event_name="config_changed", payload={"version": 3})
    resumed = engine.claim_task(worker_id="w")
    assert resumed.run_id == run.run_id

    # Re-running the step now returns the payload without parking.
    suspend, payload = engine.await_event(
        run_id=resumed.run_id,
        task_id=ref.task_id,
        step_name="wait",
        event_name="config_changed",
    )
    assert suspend is False
    assert payload == {"version": 3}


def test_await_event_times_out(engine, clock):
    ref = engine.spawn_task(name="t", params={})
    run = engine.claim_task(worker_id="w")

    suspend, _ = engine.await_event(
        run_id=run.run_id,
        task_id=ref.task_id,
        step_name="wait",
        event_name="never",
        timeout_secs=60,
    )
    assert suspend is True

    clock.advance(61)
    resumed = engine.claim_task(worker_id="w")
    assert resumed.run_id == run.run_id

    suspend, payload = engine.await_event(
        run_id=resumed.run_id,
        task_id=ref.task_id,
        step_name="wait",
        event_name="never",
        timeout_secs=60,
    )
    assert suspend is False
    assert payload is None


def test_emit_event_first_write_wins(engine):
    ref = engine.spawn_task(name="t", params={})
    run = engine.claim_task(worker_id="w")

    engine.emit_event(event_name="e", payload={"v": 1})
    engine.emit_event(event_name="e", payload={"v": 2})  # ignored

    _, payload = engine.await_event(
        run_id=run.run_id, task_id=ref.task_id, step_name="wait", event_name="e"
    )
    assert payload == {"v": 1}


def test_wait_for_event_raises_when_parked(engine):
    ref = engine.spawn_task(name="t", params={})
    run = engine.claim_task(worker_id="w")

    with pytest.raises(WorkflowSuspended):
        engine.wait_for_event(
            run_id=run.run_id, task_id=ref.task_id, step_name="wait", event_name="later"
        )

    assert engine.get_task(ref.task_id).state == "sleeping"


def test_wait_for_event_returns_payload_when_available(engine):
    ref = engine.spawn_task(name="t", params={})
    run = engine.claim_task(worker_id="w")
    engine.emit_event(event_name="ready", payload={"v": 7})

    payload = engine.wait_for_event(
        run_id=run.run_id, task_id=ref.task_id, step_name="wait", event_name="ready"
    )

    assert payload == {"v": 7}


def test_completing_a_parked_run_is_rejected(engine):
    """A handler that suspends must not also be completed by the worker."""
    ref = engine.spawn_task(name="t", params={})
    run = engine.claim_task(worker_id="w")
    with pytest.raises(WorkflowSuspended):
        engine.wait_for_event(
            run_id=run.run_id, task_id=ref.task_id, step_name="wait", event_name="later"
        )

    with pytest.raises(InvalidRunState):
        engine.complete_run(run_id=run.run_id, result={})


# -- cancellation ----------------------------------------------------------


def test_cancel_task_makes_completion_fail(engine):
    ref = engine.spawn_task(name="t", params={})
    run = engine.claim_task(worker_id="w")

    engine.cancel_task(ref.task_id)

    assert engine.get_task(ref.task_id).state == "cancelled"
    with pytest.raises(TaskCancelledError):
        engine.complete_run(run_id=run.run_id, result={})


# -- read models / errors --------------------------------------------------


def test_get_task_raises_for_unknown_task(engine):
    with pytest.raises(TaskNotFound):
        engine.get_task("does-not-exist")


def test_complete_unknown_run_raises(engine):
    with pytest.raises(TaskNotFound):
        engine.complete_run(run_id="nope")


def test_double_complete_raises(engine):
    engine.spawn_task(name="t", params={})
    run = engine.claim_task(worker_id="w")
    engine.complete_run(run_id=run.run_id)

    with pytest.raises(InvalidRunState):
        engine.complete_run(run_id=run.run_id)


# -- durability ------------------------------------------------------------


def test_state_survives_engine_restart(tmp_path, clock):
    path = tmp_path / "engine.db"
    engine1 = DurableEngine(path, clock=clock)
    ref = engine1.spawn_task(name="send_file", params={"uri": "/a.csv"})
    engine1.close()

    # A fresh engine on the same file sees the pending task.
    engine2 = DurableEngine(path, clock=clock)
    claimed = engine2.claim_task(worker_id="w")
    assert claimed is not None
    assert claimed.task_id == ref.task_id


# -- durable state ---------------------------------------------------------


def test_set_and_get_state(engine):
    engine.set_state(namespace="files", key="/a.csv", value={"mtime": 1, "size": 10})
    assert engine.get_state(namespace="files", key="/a.csv") == {
        "mtime": 1,
        "size": 10,
    }


def test_get_state_returns_default_when_absent(engine):
    assert engine.get_state(namespace="files", key="/missing") is None
    assert engine.get_state(namespace="files", key="/missing", default={}) == {}


def test_set_state_overwrites(engine):
    engine.set_state(namespace="ns", key="k", value="first")
    engine.set_state(namespace="ns", key="k", value="second")
    assert engine.get_state(namespace="ns", key="k") == "second"


def test_delete_state_reports_existence(engine):
    engine.set_state(namespace="ns", key="k", value=1)
    assert engine.delete_state(namespace="ns", key="k") is True
    assert engine.delete_state(namespace="ns", key="k") is False
    assert engine.get_state(namespace="ns", key="k") is None


def test_namespaces_are_isolated(engine):
    engine.set_state(namespace="a", key="k", value="from-a")
    engine.set_state(namespace="b", key="k", value="from-b")
    assert engine.get_state(namespace="a", key="k") == "from-a"
    assert engine.get_state(namespace="b", key="k") == "from-b"


def test_list_state_returns_namespace_contents(engine):
    engine.set_state(namespace="files", key="/a", value=1)
    engine.set_state(namespace="files", key="/b", value=2)
    engine.set_state(namespace="other", key="/c", value=3)
    assert engine.list_state("files") == {"/a": 1, "/b": 2}


def test_set_state_many_bulk_upserts(engine):
    written = engine.set_state_many(
        namespace="files", items={"/a": {"mtime": 1}, "/b": {"mtime": 2}}
    )
    assert written == 2
    assert engine.get_state(namespace="files", key="/a") == {"mtime": 1}
    assert engine.get_state(namespace="files", key="/b") == {"mtime": 2}

    # Re-running overwrites existing keys (upsert).
    engine.set_state_many(namespace="files", items={"/a": {"mtime": 9}})
    assert engine.get_state(namespace="files", key="/a") == {"mtime": 9}


def test_set_state_many_empty_is_noop(engine):
    assert engine.set_state_many(namespace="files", items={}) == 0


def test_has_state_reports_namespace_emptiness(engine):
    assert engine.has_state("files") is False
    engine.set_state(namespace="files", key="/a", value={"mtime": 1})
    assert engine.has_state("files") is True
    assert engine.has_state("other") is False


def test_update_state_is_atomic_read_modify_write(engine):
    engine.update_state(
        namespace="counters", key="n", fn=lambda cur: (cur or 0) + 1, default=0
    )
    engine.update_state(namespace="counters", key="n", fn=lambda cur: cur + 1)
    assert engine.get_state(namespace="counters", key="n") == 2


def test_state_survives_cleanup(engine, clock):
    ref = engine.spawn_task(name="t", params={})
    run = engine.claim_task(worker_id="w")
    engine.complete_run(run_id=run.run_id)
    engine.set_state(namespace="files", key="/a.csv", value={"mtime": 1})

    clock.advance(timedelta(days=31).total_seconds())
    removed = engine.cleanup(ttl=timedelta(days=30))

    assert removed == 1  # the task is gone
    with pytest.raises(TaskNotFound):
        engine.get_task(ref.task_id)
    assert engine.get_state(namespace="files", key="/a.csv") == {
        "mtime": 1
    }  # state remains


# -- cleanup ---------------------------------------------------------------


def test_cleanup_removes_old_terminal_tasks(engine, clock):
    ref = engine.spawn_task(name="t", params={})
    run = engine.claim_task(worker_id="w")
    engine.complete_run(run_id=run.run_id)

    # Not old enough yet.
    assert engine.cleanup(ttl=timedelta(days=30)) == 0

    clock.advance(timedelta(days=31).total_seconds())
    assert engine.cleanup(ttl=timedelta(days=30)) == 1
    with pytest.raises(TaskNotFound):
        engine.get_task(ref.task_id)


def test_cleanup_spans_multiple_batches(engine, clock):
    # Regression: cleanup() used to delete everything in one transaction;
    # it now commits in batches of _CLEANUP_BATCH_SIZE. Exercise a backlog
    # bigger than one batch and check every task is still removed.
    from dura.engine import _CLEANUP_BATCH_SIZE

    refs = []
    for _ in range(_CLEANUP_BATCH_SIZE + 1):
        ref = engine.spawn_task(name="t", params={})
        engine.complete_run(run_id=engine.claim_task(worker_id="w").run_id)
        refs.append(ref)

    clock.advance(timedelta(days=31).total_seconds())
    assert engine.cleanup(ttl=timedelta(days=30)) == len(refs)
    for ref in refs:
        with pytest.raises(TaskNotFound):
            engine.get_task(ref.task_id)


# -- cancel_duplicate_tasks ------------------------------------------------


def test_cancel_duplicate_tasks_no_tasks(engine):
    survivor, cancelled = engine.cancel_duplicate_tasks("backup")
    assert survivor is None
    assert cancelled == 0


def test_cancel_duplicate_tasks_single_task_untouched(engine):
    ref = engine.spawn_task(name="backup", params={})
    survivor, cancelled = engine.cancel_duplicate_tasks("backup")
    assert survivor == ref.task_id
    assert cancelled == 0
    assert engine.get_task(ref.task_id).state == "pending"


def test_cancel_duplicate_tasks_keeps_oldest_cancels_rest(engine, clock):
    first = engine.spawn_task(name="backup", params={})
    clock.advance(1)
    second = engine.spawn_task(name="backup", params={})
    clock.advance(1)
    third = engine.spawn_task(name="backup", params={})

    survivor, cancelled = engine.cancel_duplicate_tasks("backup")

    assert survivor == first.task_id
    assert cancelled == 2
    assert engine.get_task(first.task_id).state == "pending"
    assert engine.get_task(second.task_id).state == "cancelled"
    assert engine.get_task(third.task_id).state == "cancelled"


def test_cancel_duplicate_tasks_ignores_terminal_tasks(engine, clock):
    completed = engine.spawn_task(name="backup", params={})
    engine.complete_run(run_id=engine.claim_task(worker_id="w").run_id, result={})
    clock.advance(1)
    active = engine.spawn_task(name="backup", params={})

    survivor, cancelled = engine.cancel_duplicate_tasks("backup")

    assert survivor == active.task_id
    assert cancelled == 0
    assert engine.get_task(completed.task_id).state == "completed"


# -- observability -----------------------------------------------------------


def test_task_counts_by_state(engine):
    engine.spawn_task(name="a", params={})
    engine.spawn_task(name="b", params={})
    run = engine.claim_task(worker_id="w")
    engine.complete_run(run_id=run.run_id, result={})

    assert engine.task_counts_by_state() == {"pending": 1, "completed": 1}


def test_task_counts_by_state_omits_empty_states(engine):
    engine.spawn_task(name="a", params={})

    assert engine.task_counts_by_state() == {"pending": 1}


def test_task_counts_by_name_and_state(engine):
    engine.spawn_task(name="a", params={})
    engine.spawn_task(name="a", params={})
    run = engine.claim_task(worker_id="w")
    engine.complete_run(run_id=run.run_id, result={})
    engine.spawn_task(name="b", params={})

    assert engine.task_counts_by_name_and_state() == {
        "a": {"pending": 1, "completed": 1},
        "b": {"pending": 1},
    }


def test_recent_failures_reports_newest_first_with_reason(engine, clock):
    engine.spawn_task(name="a", params={}, max_attempts=1)
    run_a = engine.claim_task(worker_id="w")
    engine.fail_run(
        run_id=run_a.run_id, reason={"type": "ValueError", "message": "bad a"}
    )

    clock.advance(1)
    engine.spawn_task(name="b", params={}, max_attempts=1)
    run_b = engine.claim_task(worker_id="w")
    engine.fail_run(
        run_id=run_b.run_id, reason={"type": "ValueError", "message": "bad b"}
    )

    failures = engine.recent_failures(limit=10)

    assert [f.task_name for f in failures] == ["b", "a"]
    assert failures[0].failure_reason == {"type": "ValueError", "message": "bad b"}
    assert failures[0].run_id == run_b.run_id


def test_recent_failures_respects_limit(engine):
    for _ in range(3):
        ref = engine.spawn_task(name="a", params={}, max_attempts=1)
        run = engine.claim_task(worker_id="w")
        engine.fail_run(run_id=run.run_id, reason={"type": "E", "message": ref.task_id})

    assert len(engine.recent_failures(limit=2)) == 2


def test_recent_failures_empty_when_nothing_failed(engine):
    engine.spawn_task(name="a", params={})

    assert engine.recent_failures() == []
