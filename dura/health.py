"""Liveness heartbeat and the HTTP server that exposes /metrics and /healthz.

Workers beat a shared :class:`Heartbeat` every loop iteration. ``/healthz``
reports the process unhealthy (503) when no worker has beaten within a
configured window -- i.e. the whole pool is wedged (e.g. every worker blocked on
a dead source) -- so a Kubernetes liveness probe restarts it. A pool that is
merely idle keeps beating, so it stays healthy.

The same server also serves Prometheus ``/metrics`` so metrics and health share
one port.
"""

import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable

from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, generate_latest


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


def _make_handler(heartbeat: Heartbeat, max_silence_seconds: float):
    class _Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):  # don't spam stderr per request
            pass

        def _write(self, status: int, content_type: str, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            path = self.path.split("?", 1)[0].rstrip("/") or "/"
            if path in ("/healthz", "/livez"):
                silence = heartbeat.seconds_since_beat()
                alive = silence < max_silence_seconds
                status = 200 if alive else 503
                state = "alive" if alive else "stuck"
                self._write(
                    status, "text/plain", f"{state} silence={silence:.1f}s\n".encode()
                )
            elif path in ("/", "/metrics"):
                self._write(200, CONTENT_TYPE_LATEST, generate_latest(REGISTRY))
            else:
                self._write(404, "text/plain", b"not found\n")

    return _Handler


def start_metrics_and_health_server(
    *,
    port: int,
    heartbeat: Heartbeat,
    max_silence_seconds: float,
) -> ThreadingHTTPServer:
    """Start a background HTTP server serving /metrics and /healthz on ``port``.

    Returns the server; call ``.shutdown()`` to stop it.
    """
    httpd = ThreadingHTTPServer(
        ("", port), _make_handler(heartbeat, max_silence_seconds)
    )
    thread = threading.Thread(
        target=httpd.serve_forever, name="http-server", daemon=True
    )
    thread.start()
    return httpd
