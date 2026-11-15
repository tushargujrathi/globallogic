"""Worker simulator: update planning and configuration."""

import random
from uuid import uuid4

import pytest

from control_plane.domain.enums import TaskStatus
from worker_simulator.config import SimulationConfig, parse_args
from worker_simulator.simulator import SIMULATED_ERROR, plan_updates


def _plan(**config: object) -> list[tuple[float, TaskStatus]]:
    steps = plan_updates(uuid4(), SimulationConfig(**config), random.Random(7))  # type: ignore[arg-type]
    return [(step.delay_seconds, step.update.status) for step in steps]


def test_happy_path_is_in_progress_then_done_after_the_work_delay() -> None:
    task_id = uuid4()
    steps = plan_updates(
        task_id, SimulationConfig(min_delay_ms=100, max_delay_ms=200), random.Random(1)
    )
    assert [s.update.status for s in steps] == [TaskStatus.IN_PROGRESS, TaskStatus.DONE]
    assert steps[0].delay_seconds == 0
    assert 0.1 <= steps[1].delay_seconds <= 0.2
    assert all(s.update.task_id == task_id for s in steps)
    assert steps[0].update.message_id != steps[1].update.message_id
    assert steps[1].update.occurred_at > steps[0].update.occurred_at


def test_fail_rate_one_always_fails_with_an_error() -> None:
    for seed in range(20):
        steps = plan_updates(uuid4(), SimulationConfig(fail_rate=1.0), random.Random(seed))
        assert steps[-1].update.status == TaskStatus.FAILED
        assert steps[-1].update.error == SIMULATED_ERROR


def test_fail_rate_zero_never_fails() -> None:
    for seed in range(20):
        steps = plan_updates(uuid4(), SimulationConfig(fail_rate=0.0), random.Random(seed))
        assert steps[-1].update.status == TaskStatus.DONE
        assert steps[-1].update.error is None


def test_out_of_order_publishes_terminal_before_in_progress() -> None:
    assert [s for _, s in _plan(out_of_order_rate=1.0)] == [
        TaskStatus.DONE,
        TaskStatus.IN_PROGRESS,
    ]


def test_duplicates_repeat_each_update_with_the_same_message_id() -> None:
    steps = plan_updates(uuid4(), SimulationConfig(duplicate_rate=1.0), random.Random(3))
    assert len(steps) == 4
    assert steps[0].update == steps[1].update
    assert steps[2].update == steps[3].update
    assert steps[1].delay_seconds == steps[3].delay_seconds == 0


def test_same_seed_gives_the_same_outcomes_and_timing() -> None:
    config = SimulationConfig(fail_rate=0.5, duplicate_rate=0.5, out_of_order_rate=0.5)
    rng_a, rng_b = random.Random(42), random.Random(42)
    for _ in range(20):
        first = plan_updates(uuid4(), config, rng_a)
        second = plan_updates(uuid4(), config, rng_b)
        assert [(s.delay_seconds, s.update.status) for s in first] == [
            (s.delay_seconds, s.update.status) for s in second
        ]


def test_message_ids_are_unique_even_with_the_same_seed() -> None:
    """Two workers (or a restarted one) with one seed must not reuse idempotency keys."""
    task_id = uuid4()
    first = plan_updates(task_id, SimulationConfig(), random.Random(42))
    second = plan_updates(task_id, SimulationConfig(), random.Random(42))
    ids = [s.update.message_id for s in first + second]
    assert len(set(ids)) == len(ids)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"fail_rate": -0.1},
        {"fail_rate": 1.1},
        {"duplicate_rate": 2.0},
        {"out_of_order_rate": -1.0},
        {"min_delay_ms": 10, "max_delay_ms": 5},
        {"min_delay_ms": -1},
    ],
)
def test_invalid_config_is_rejected(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError):  # noqa: PT011
        SimulationConfig(**kwargs)  # type: ignore[arg-type]


def test_cli_flags() -> None:
    settings = parse_args(
        [
            "--fail-rate=1",
            "--min-delay-ms=0",
            "--max-delay-ms=10",
            "--duplicate-rate=0.5",
            "--out-of-order-rate=0.25",
            "--seed=9",
            "--rabbitmq-url=amqp://broker/%2F",
        ]
    )
    assert settings.rabbitmq_url == "amqp://broker/%2F"
    assert settings.simulation == SimulationConfig(
        fail_rate=1.0,
        min_delay_ms=0,
        max_delay_ms=10,
        duplicate_rate=0.5,
        out_of_order_rate=0.25,
        seed=9,
    )


def test_env_vars_provide_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORKER_FAIL_RATE", "1")
    monkeypatch.setenv("WORKER_MIN_DELAY_MS", "5")
    monkeypatch.setenv("WORKER_MAX_DELAY_MS", "6")
    simulation = parse_args([]).simulation
    assert (simulation.fail_rate, simulation.min_delay_ms, simulation.max_delay_ms) == (1.0, 5, 6)
    # CLI flags win over the environment.
    assert parse_args(["--fail-rate=0"]).simulation.fail_rate == 0.0


def test_cli_rejects_invalid_values() -> None:
    with pytest.raises(SystemExit):
        parse_args(["--fail-rate=3"])
