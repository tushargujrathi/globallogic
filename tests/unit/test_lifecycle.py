"""SIGTERM takes the same clean-shutdown path as Ctrl-C."""

import os
import signal

import pytest

from control_plane.core.lifecycle import run_until_stopped


def test_sigterm_stops_the_target_cleanly(caplog: pytest.LogCaptureFixture) -> None:
    previous = signal.getsignal(signal.SIGTERM)
    cleaned_up = []

    def target() -> None:
        try:
            os.kill(os.getpid(), signal.SIGTERM)
        finally:
            cleaned_up.append(True)

    try:
        with caplog.at_level("INFO"):
            run_until_stopped("demo", target)  # must not raise
    finally:
        signal.signal(signal.SIGTERM, previous)
    assert cleaned_up == [True]
    assert "demo stopped" in caplog.text
