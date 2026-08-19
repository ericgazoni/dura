"""Tests for task priority: higher priority is claimed first and retries
inherit it. Also covers max_priority lane filtering."""


def test_default_priority_is_low(engine):
    engine.spawn_task("default", {})
    engine.spawn_task("high", {}, priority=1)
    assert engine.claim_task("w").name == "high"


def test_higher_priority_is_claimed_first(engine):
    # "low" is spawned first (earlier availability and smaller rowid), yet the
    # later, higher-priority task is claimed before it.
    engine.spawn_task("low", {}, priority=0)
    engine.spawn_task("high", {}, priority=10)

    assert engine.claim_task("w").name == "high"
    assert engine.claim_task("w").name == "low"


def test_same_priority_is_fifo_by_availability(engine):
    engine.spawn_task("t", {"n": 1})
    engine.spawn_task("t", {"n": 2})
    assert engine.claim_task("w").params == {"n": 1}
    assert engine.claim_task("w").params == {"n": 2}


def test_retry_inherits_priority(engine):
    engine.spawn_task("hi", {}, priority=10, max_attempts=3)
    engine.spawn_task("lo", {}, priority=0)

    run = engine.claim_task("w")  # claims the high-priority task
    assert run.name == "hi"
    engine.fail_run(run.run_id, {"error": "boom"})  # immediate retry (no backoff)

    # The retry still outranks the waiting low-priority task.
    retried = engine.claim_task("w")
    assert retried.name == "hi"
    assert retried.attempt == 2


# --- max_priority (lane filtering) ---


def test_max_priority_skips_higher_priority_tasks(engine):
    engine.spawn_task("hi", {}, priority=10)
    engine.spawn_task("lo", {}, priority=0)

    # A lane worker capped at priority 5 must skip "hi" (p=10) and claim "lo" (p=0).
    task = engine.claim_task("w", max_priority=5)
    assert task is not None
    assert task.name == "lo"


def test_max_priority_returns_none_when_no_eligible_task(engine):
    engine.spawn_task("hi", {}, priority=10)

    # No tasks at or below priority 5 → None.
    assert engine.claim_task("w", max_priority=5) is None


def test_max_priority_none_claims_highest_first(engine):
    engine.spawn_task("lo", {}, priority=0)
    engine.spawn_task("hi", {}, priority=10)

    # Unrestricted claim still picks the highest-priority task.
    task = engine.claim_task("w", max_priority=None)
    assert task.name == "hi"
