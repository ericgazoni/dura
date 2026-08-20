"""Tests for the worker's per-task settling logic (process_task).

These avoid the infinite run_worker loop by driving process_task directly:
claim a task, then hand it to process_task with a chosen handler and assert how
the run was settled.
"""

from dura.workers import process_task


def _claim(engine, name="t", params=None):
    engine.spawn_task(name=name, params=params or {})
    return engine.claim_task(worker_id="w")


def test_normal_handler_completes_the_run(engine):
    task = _claim(engine)

    process_task(engine, handlers={"t": lambda e, tk: {"ok": True}}, task=task)

    info = engine.get_task(task.task_id)
    assert info.state == "completed"
    assert info.result == {"ok": True}


def test_suspended_handler_leaves_run_parked(engine):
    task = _claim(engine)

    def handler(e, tk):
        # Parks the run; raises WorkflowSuspended, which process_task swallows.
        return e.wait_for_event(
            run_id=tk.run_id, task_id=tk.task_id, step_name="wait", event_name="later"
        )

    process_task(engine, handlers={"t": handler}, task=task)

    # The run is parked, NOT completed.
    assert engine.get_task(task.task_id).state == "sleeping"


def test_failing_handler_fails_the_run(engine):
    task = _claim(engine, params={}, name="t")

    def handler(e, tk):
        raise RuntimeError("boom")

    process_task(engine, handlers={"t": handler}, task=task)

    # max_attempts defaults to unlimited, so it is queued for another attempt.
    assert engine.get_task(task.task_id).state == "pending"


def test_missing_handler_fails_without_retry(engine):
    # No handler for "t" (e.g. code changed, or the queue is shared with
    # another app). Retrying would just repeat the same failure forever, so
    # this must fail the run outright instead of leaving it pending.
    task = _claim(engine)

    process_task(engine, handlers={}, task=task)

    info = engine.get_task(task.task_id)
    assert info.state == "failed"
    assert info.failure_reason["type"] == "UnknownTaskName"
    assert engine.claim_task(worker_id="w") is None


def test_suspended_then_resumed_completes(engine):
    task = _claim(engine)

    def handler(e, tk):
        payload = e.wait_for_event(
            run_id=tk.run_id, task_id=tk.task_id, step_name="wait", event_name="go"
        )
        return {"woke_with": payload}

    # First pass: parks.
    process_task(engine, handlers={"t": handler}, task=task)
    assert engine.get_task(task.task_id).state == "sleeping"

    # Event fires; the run becomes claimable again.
    engine.emit_event(event_name="go", payload={"v": 1})
    resumed = engine.claim_task(worker_id="w")
    process_task(engine, handlers={"t": handler}, task=resumed)

    info = engine.get_task(task.task_id)
    assert info.state == "completed"
    assert info.result == {"woke_with": {"v": 1}}


def test_completion_tolerates_run_settled_underneath(engine):
    # Reproduces the lease-expiry race: the run was reclaimed and completed by
    # another worker; this worker's complete_run must not crash the thread.
    task = _claim(engine)
    engine.complete_run(run_id=task.run_id, result={"by": "other-worker"})

    # Must not raise (it should log and move on).
    process_task(engine, handlers={"t": lambda e, tk: {"by": "me"}}, task=task)

    assert engine.get_task(task.task_id).state == "completed"


def test_failure_tolerates_run_settled_underneath(engine):
    # Same race, but this worker's handler also failed: fail_run must tolerate
    # the run already being settled rather than raising again.
    task = _claim(engine)
    engine.complete_run(run_id=task.run_id)

    def boom(e, tk):
        raise RuntimeError("handler failed")

    process_task(engine, handlers={"t": boom}, task=task)  # must not raise

    assert engine.get_task(task.task_id).state == "completed"
