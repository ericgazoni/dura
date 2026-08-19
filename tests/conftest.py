import logging
from datetime import datetime, timedelta, timezone

import pytest

from dura.engine import ClaimedTask, DurableEngine

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s | %(levelname)-8s | %(message)s"
)


class FakeClock:
    """A controllable clock for deterministic engine tests.

    Calling it returns the current time; ``advance`` moves it forward. Tests
    drive retries, leases, delays and reschedules without sleeping.
    """

    def __init__(self, start: datetime) -> None:
        self._t = start

    def __call__(self) -> datetime:
        return self._t

    def advance(self, seconds: float) -> None:
        self._t += timedelta(seconds=seconds)


@pytest.fixture
def clock_start() -> datetime:
    """The instant ``clock`` starts at. Override in a module that needs a
    specific 'now' (e.g. business-date filtering) without redefining ``clock``
    or ``engine``."""
    return datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture
def clock(clock_start) -> FakeClock:
    """A clock frozen at ``clock_start``; advanceable for time-driven tests."""
    return FakeClock(clock_start)


@pytest.fixture
def engine(tmp_path, clock) -> DurableEngine:
    """A fresh durable engine on a temp database, driven by ``clock``."""
    return DurableEngine(tmp_path / "engine.db", clock=clock)


@pytest.fixture
def make_task():
    """Factory for a ClaimedTask, as a worker would hand one to a workflow."""

    def _make(name, params=None, *, run_id="r", task_id="t", attempt=1) -> ClaimedTask:
        return ClaimedTask(
            run_id=run_id,
            task_id=task_id,
            attempt=attempt,
            name=name,
            params=params or {},
        )

    return _make
