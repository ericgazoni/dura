"""Tests for the liveness heartbeat and the /healthz + /metrics HTTP server."""

from urllib.error import HTTPError
from urllib.request import urlopen

from dura.health import (
    Heartbeat,
    start_metrics_and_health_server,
)


def test_heartbeat_tracks_silence():
    now = [100.0]
    hb = Heartbeat(clock=lambda: now[0])

    assert hb.seconds_since_beat() == 0.0
    now[0] = 130.0
    assert hb.seconds_since_beat() == 30.0
    hb.beat()
    assert hb.seconds_since_beat() == 0.0


def test_healthz_flips_to_503_when_pool_is_stuck():
    now = [1000.0]
    hb = Heartbeat(clock=lambda: now[0])
    httpd = start_metrics_and_health_server(0, hb, max_silence_seconds=60)
    try:
        port = httpd.server_address[1]

        # Fresh heartbeat -> alive.
        with urlopen(f"http://127.0.0.1:{port}/healthz") as resp:
            assert resp.status == 200

        # No beat for longer than the window -> unhealthy (probe would restart).
        now[0] = 1100.0
        try:
            urlopen(f"http://127.0.0.1:{port}/healthz")
            assert False, "expected a 503"
        except HTTPError as err:
            assert err.code == 503

        # A fresh beat brings it back.
        hb.beat()
        with urlopen(f"http://127.0.0.1:{port}/healthz") as resp:
            assert resp.status == 200

        # Metrics are served on the same port.
        with urlopen(f"http://127.0.0.1:{port}/metrics") as resp:
            assert resp.status == 200
    finally:
        httpd.shutdown()
