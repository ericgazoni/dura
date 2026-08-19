"""Tests for the worker's per-task settling logic (process_task).

These avoid the infinite run_worker loop by driving process_task directly:
claim a task, then hand it to process_task with a chosen handler and assert how
the run was settled.
"""

from dura.workers import process_task


def _claim(engine, name="t", params=None):
    engine.spawn_task(name, params or {})
    return engine.claim_task("w")


def test_normal_handler_completes_the_run(engine):
    task = _claim(engine)

    process_task(engine, {"t": lambda e, tk: {"ok": True}}, task)

    info = engine.get_task(task.task_id)
    assert info.state == "completed"
    assert info.result == {"ok": True}


def test_suspended_handler_leaves_run_parked(engine):
    task = _claim(engine)

    def handler(e, tk):
        # Parks the run; raises WorkflowSuspended, which process_task swallows.
        return e.wait_for_event(tk.run_id, tk.task_id, "wait", "later")

    process_task(engine, {"t": handler}, task)

    # The run is parked, NOT completed.
    assert engine.get_task(task.task_id).state == "sleeping"


def test_failing_handler_fails_the_run(engine):
    task = _claim(engine, params={}, name="t")

    def handler(e, tk):
        raise RuntimeError("boom")

    process_task(engine, {"t": handler}, task)

    # max_attempts defaults to unlimited, so it is queued for another attempt.
    assert engine.get_task(task.task_id).state == "pending"


def test_suspended_then_resumed_completes(engine):
    task = _claim(engine)

    def handler(e, tk):
        payload = e.wait_for_event(tk.run_id, tk.task_id, "wait", "go")
        return {"woke_with": payload}

    # First pass: parks.
    process_task(engine, {"t": handler}, task)
    assert engine.get_task(task.task_id).state == "sleeping"

    # Event fires; the run becomes claimable again.
    engine.emit_event("go", {"v": 1})
    resumed = engine.claim_task("w")
    process_task(engine, {"t": handler}, resumed)

    info = engine.get_task(task.task_id)
    assert info.state == "completed"
    assert info.result == {"woke_with": {"v": 1}}


def test_completion_tolerates_run_settled_underneath(engine):
    # Reproduces the lease-expiry race: the run was reclaimed and completed by
    # another worker; this worker's complete_run must not crash the thread.
    task = _claim(engine)
    engine.complete_run(task.run_id, {"by": "other-worker"})

    # Must not raise (it should log and move on).
    process_task(engine, {"t": lambda e, tk: {"by": "me"}}, task)

    assert engine.get_task(task.task_id).state == "completed"


def test_failure_tolerates_run_settled_underneath(engine):
    # Same race, but this worker's handler also failed: fail_run must tolerate
    # the run already being settled rather than raising again.
    task = _claim(engine)
    engine.complete_run(task.run_id)

    def boom(e, tk):
        raise RuntimeError("handler failed")

    process_task(engine, {"t": boom}, task)  # must not raise

    assert engine.get_task(task.task_id).state == "completed"
