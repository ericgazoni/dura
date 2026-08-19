"""Tests for the liveness heartbeat."""

from dura.health import Heartbeat


def test_heartbeat_tracks_silence():
    now = [100.0]
    hb = Heartbeat(clock=lambda: now[0])

    assert hb.seconds_since_beat() == 0.0
    now[0] = 130.0
    assert hb.seconds_since_beat() == 30.0
    hb.beat()
    assert hb.seconds_since_beat() == 0.0
