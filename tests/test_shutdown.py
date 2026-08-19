"""Tests for graceful shutdown: workers stop on a stop event and the engine
closes without leaving WAL sidecar files behind."""

import threading

import pytest

from dura.engine import DurableEngine
from dura.workers import run_worker


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "engine.db"


def test_close_removes_wal_sidecars(db_path):
    engine = DurableEngine(db_path)
    engine.spawn_task(name="t", params={})  # forces a WAL write -> -wal/-shm appear

    engine.close()

    assert db_path.exists()
    assert not (db_path.parent / "engine.db-wal").exists()
    assert not (db_path.parent / "engine.db-shm").exists()


def test_worker_stops_on_event_when_idle(db_path):
    engine = DurableEngine(db_path)
    stop = threading.Event()
    worker = threading.Thread(
        target=run_worker,
        args=(engine,),
        kwargs={
            "handlers": {},
            "worker_id": "w",
            "stop_event": stop,
            "poll_interval": 0.05,
        },
    )
    worker.start()
    stop.set()
    worker.join(timeout=5)

    assert not worker.is_alive()


def test_worker_finishes_in_flight_task_before_stopping(db_path):
    engine = DurableEngine(db_path)
    ran = threading.Event()

    def handler(_engine, _task):
        ran.set()

    engine.spawn_task(name="job", params={})
    stop = threading.Event()
    worker = threading.Thread(
        target=run_worker,
        args=(engine,),
        kwargs={
            "handlers": {"job": handler},
            "worker_id": "w",
            "stop_event": stop,
            "poll_interval": 0.02,
        },
    )
    worker.start()

    assert ran.wait(timeout=5)  # the claimed task's handler actually ran
    stop.set()
    worker.join(timeout=5)
    assert not worker.is_alive()
