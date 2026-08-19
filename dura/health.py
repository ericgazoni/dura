"""Liveness heartbeat for detecting a wedged worker pool.

Workers beat a shared ``Heartbeat`` every loop iteration. A pool that's
simply idle keeps beating, so it stays "alive"; only a fully wedged pool
(every worker blocked, e.g. all stuck on a dead downstream dependency)
goes silent. ``Heartbeat`` is a plain in-memory object -- it opens no
sockets and starts no threads -- so using it never surprises a caller
embedding ``dura`` in a script, desktop app, or existing server: pass one
in if you want liveness, and expose ``seconds_since_beat()`` however suits
your app (an HTTP route, a CLI command, a log line, a test).
"""

import time
from typing import Callable


class Heartbeat:
    """Records the time of the most recent worker activity, for liveness."""

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._last = clock()  # assume healthy at startup

    def beat(self) -> None:
        # A bare float store is atomic under the GIL; no lock needed for a value
        # where only "most recent wins" matters.
        self._last = self._clock()

    def seconds_since_beat(self) -> float:
        return self._clock() - self._last
