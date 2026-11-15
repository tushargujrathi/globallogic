import pytest

from control_plane.messaging.connection import backoff_delay


@pytest.mark.parametrize(
    ("attempt", "low", "high"),
    [(0, 0.25, 0.5), (1, 0.5, 1.0), (3, 2.0, 4.0), (10, 15.0, 30.0), (100, 15.0, 30.0)],
)
def test_backoff_grows_exponentially_with_jitter_and_a_cap(
    attempt: int, low: float, high: float
) -> None:
    for _ in range(50):
        assert low <= backoff_delay(attempt) <= high


def test_backoff_custom_cap() -> None:
    assert all(backoff_delay(50, cap=10.0) <= 10.0 for _ in range(50))
