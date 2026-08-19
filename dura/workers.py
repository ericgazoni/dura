import logging
import signal
import threading
import time
from typing import Callable

from dura.engine import (
    DurableEngine,
    ClaimedTask,
    InvalidRunState,
    TaskCancelledError,
    TaskNotFound,
    WorkflowSuspended,
)
from dura.health import Heartbeat

logger = logging.getLogger(__name__)

# How long to wait for in-flight handlers to finish on shutdown before
# abandoning them. Abandoned work is safe: the run's lease expires and it is
# reclaimed on the next start.
SHUTDOWN_GRACE_SECONDS = 10.0

# A run can be settled by another worker between the time this one claimed it
# and the time it tries to settle it (e.g. if this worker stalled past its
# lease, the run is reclaimed and may be completed elsewhere). These are the
# states that signals "this run is no longer ours"; we drop it rather than crash.
_SETTLE_RACE = (InvalidRunState, TaskNotFound, TaskCancelledError)


def process_task(
    engine: DurableEngine,
    *,
    handlers: dict[str, Callable],
    task: ClaimedTask,
    worker_id: str = "worker",
) -> None:
    """Run one claimed task's handler and settle its run.

    Outcomes:
    * handler returns normally -> the run is completed with its result
    * handler raises WorkflowSuspended -> the run parked itself awaiting an
      event; we leave it alone (neither completed nor failed)
    * handler raises TaskCancelledError -> the engine already marked it
      cancelled; nothing to do
    * any other exception -> the run is failed (and retried per its strategy)

    Settling (complete/fail) tolerates the run having been reclaimed and settled
    by another worker while this one was busy: that is logged, not raised, so a
    lease-expiry race never crashes the worker thread.
    """
    try:
        handler = handlers[task.name]
        result = handler(engine, task)
    except WorkflowSuspended:
        # The handler parked the run via engine.wait_for_event(). It will be
        # re-claimed when the awaited event fires or its timeout elapses.
        logger.debug(
            "Suspended %r task_id=%s, awaiting event", task.name, task.task_id[:8]
        )
        return
    except TaskCancelledError:
        # Engine already updated the state, nothing to do
        logger.debug("Task %s was cancelled", task.task_id[:8])
        return
    except Exception as exc:
        logger.exception("Failed %r task_id=%s: %s", task.name, task.task_id[:8], exc)
        reason = {"type": type(exc).__name__, "message": str(exc)}
        try:
            engine.fail_run(run_id=task.run_id, reason=reason)
        except _SETTLE_RACE as settle_exc:
            logger.warning(
                "Run %s no longer ours; not failing it: %s",
                task.task_id[:8],
                settle_exc,
            )
        return

    try:
        engine.complete_run(run_id=task.run_id, result=result)
        logger.debug(
            "Completed %r task_id=%s worker=%s", task.name, task.task_id[:8], worker_id
        )
    except _SETTLE_RACE as settle_exc:
        logger.warning(
            "Run %s no longer ours; not completing it: %s",
            task.task_id[:8],
            settle_exc,
        )


def run_worker(
    engine: DurableEngine,
    *,
    handlers: dict[str, Callable],
    worker_id: str,
    stop_event: threading.Event,
    heartbeat: Heartbeat | None = None,
    claim_timeout_secs: int = 120,
    poll_interval: float = 1.0,
    max_priority: int | None = None,
) -> None:
    label = f"Worker {worker_id}" + (
        f" (pmax={max_priority})" if max_priority is not None else ""
    )
    logger.info("%s started", label)
    try:
        while not stop_event.is_set():
            # Heartbeat at the top of every loop: liveness sees the pool as
            # alive as long as any worker keeps cycling (claiming or idling).
            if heartbeat is not None:
                heartbeat.beat()

            try:
                task = engine.claim_task(
                    worker_id=worker_id,
                    timeout_secs=claim_timeout_secs,
                    max_priority=max_priority,
                )

                if task is None:
                    # No work right now: wait, but wake immediately on shutdown.
                    stop_event.wait(poll_interval)
                    continue

                logger.debug(
                    "Claimed %r task_id=%s attempt=%s",
                    task.name,
                    task.task_id[:8],
                    task.attempt,
                )
                process_task(engine, handlers=handlers, task=task, worker_id=worker_id)
            except Exception:
                # Anything escaping claim/process (e.g. a transient "database is
                # locked" under write contention) must not kill this thread,
                # that would silently shrink the pool with no health-check
                # signal. Log it and keep cycling; poll_interval avoids a tight
                # spin if the error is persistent.
                logger.exception("%s hit an error; continuing", label)
                stop_event.wait(poll_interval)
    finally:
        # Close this thread's connection so its WAL is checkpointed; the last
        # connection to close removes the -wal/-shm sidecar files.
        engine.close()
        logger.info("%s stopped", label)


def run_workers(
    engine: DurableEngine,
    *,
    handlers: dict,
    worker_count: int,
    heartbeat: Heartbeat | None = None,
    handle_signals: bool = True,
    lanes: list[tuple[int, int]] | None = None,
) -> None:
    """Start the worker pool and block until SIGINT/SIGTERM, then drain.

    On shutdown: stop claiming new work, let in-flight handlers finish (up to
    ``SHUTDOWN_GRACE_SECONDS``), and close every connection so the database is
    left without -wal/-shm sidecars. Must run on the main thread when
    ``handle_signals`` is True, since signal handlers install there only.

    ``heartbeat`` (if given) is beaten by every worker each loop iteration so a
    liveness probe can detect a fully wedged pool.

    ``lanes`` is a list of ``(max_priority, count)`` pairs. Workers in a lane
    only claim tasks at or below ``max_priority``; the remaining workers are
    unrestricted and claim from the full queue.
    """
    stop_event = threading.Event()
    workers = []
    worker_id = 0

    for max_priority, count in lanes or []:
        for _ in range(count):
            workers.append(
                threading.Thread(
                    target=run_worker,
                    name=f"worker_{worker_id}",
                    args=(engine,),
                    kwargs={
                        "handlers": handlers,
                        "worker_id": f"worker_{worker_id}",
                        "stop_event": stop_event,
                        "heartbeat": heartbeat,
                        "max_priority": max_priority,
                    },
                    daemon=True,
                )
            )
            worker_id += 1

    lane_workers = sum(count for _, count in (lanes or []))
    for _ in range(worker_count - lane_workers):
        workers.append(
            threading.Thread(
                target=run_worker,
                name=f"worker_{worker_id}",
                args=(engine,),
                kwargs={
                    "handlers": handlers,
                    "worker_id": f"worker_{worker_id}",
                    "stop_event": stop_event,
                    "heartbeat": heartbeat,
                },
                daemon=True,
            )
        )
        worker_id += 1
    for worker in workers:
        worker.start()
    logger.info("Started %d workers", len(workers))

    if handle_signals:

        def _request_stop(signum, _frame):
            logger.info(
                "Received %s; shutting down gracefully", signal.Signals(signum).name
            )
            stop_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, _request_stop)

    try:
        # Block the main thread until shutdown is requested. The 1s timeout lets
        # pending signals be delivered between waits.
        while not stop_event.wait(timeout=1.0):
            pass

        logger.info("Draining workers (finishing in-flight tasks)...")
        deadline = time.monotonic() + SHUTDOWN_GRACE_SECONDS
        for worker in workers:
            worker.join(timeout=max(0.0, deadline - time.monotonic()))
        stragglers = [w.name for w in workers if w.is_alive()]
        if stragglers:
            logger.warning(
                "Abandoning workers still busy after %ss: %s",
                SHUTDOWN_GRACE_SECONDS,
                stragglers,
            )
    finally:
        # Close the main thread's connection last; with the workers' connections
        # already closed this is the final close and clears the WAL sidecars.
        engine.close()
        logger.info("Engine closed; shutdown complete")
