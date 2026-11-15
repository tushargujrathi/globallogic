"""Simulator configuration: CLI flags, each defaulting to an environment variable."""

import argparse
import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SimulationConfig:
    fail_rate: float = 0.0
    min_delay_ms: int = 500
    max_delay_ms: int = 2000
    duplicate_rate: float = 0.0
    out_of_order_rate: float = 0.0
    seed: int | None = None

    def __post_init__(self) -> None:
        for name in ("fail_rate", "duplicate_rate", "out_of_order_rate"):
            if not 0.0 <= getattr(self, name) <= 1.0:
                raise ValueError(f"{name} must be between 0 and 1")
        if not 0 <= self.min_delay_ms <= self.max_delay_ms:
            raise ValueError("require 0 <= min_delay_ms <= max_delay_ms")


@dataclass(frozen=True, slots=True)
class WorkerSettings:
    rabbitmq_url: str
    log_level: str
    simulation: SimulationConfig


def _env[T](name: str, cast: Callable[[str], T], default: T) -> T:
    raw = os.environ.get(name)
    return cast(raw) if raw not in (None, "") else default


def parse_args(argv: Sequence[str] | None = None) -> WorkerSettings:
    defaults = SimulationConfig()
    parser = argparse.ArgumentParser(
        prog="worker-simulator",
        description="Consume provisioning tasks and publish simulated progress updates.",
    )
    parser.add_argument(
        "--rabbitmq-url",
        default=os.environ.get("RABBITMQ_URL", "amqp://localhost:5672/%2F"),
        help="env: RABBITMQ_URL",
    )
    parser.add_argument(
        "--fail-rate",
        type=float,
        default=_env("WORKER_FAIL_RATE", float, defaults.fail_rate),
        help="Probability [0-1] that a task ends 'failed'. env: WORKER_FAIL_RATE",
    )
    parser.add_argument(
        "--min-delay-ms",
        type=int,
        default=_env("WORKER_MIN_DELAY_MS", int, defaults.min_delay_ms),
        help="Minimum simulated work time. env: WORKER_MIN_DELAY_MS",
    )
    parser.add_argument(
        "--max-delay-ms",
        type=int,
        default=_env("WORKER_MAX_DELAY_MS", int, defaults.max_delay_ms),
        help="Maximum simulated work time. env: WORKER_MAX_DELAY_MS",
    )
    parser.add_argument(
        "--duplicate-rate",
        type=float,
        default=_env("WORKER_DUPLICATE_RATE", float, defaults.duplicate_rate),
        help="Probability [0-1] that every update of a task is published twice "
        "(same message_id). env: WORKER_DUPLICATE_RATE",
    )
    parser.add_argument(
        "--out-of-order-rate",
        type=float,
        default=_env("WORKER_OUT_OF_ORDER_RATE", float, defaults.out_of_order_rate),
        help="Probability [0-1] that the terminal update is published before "
        "'in_progress'. env: WORKER_OUT_OF_ORDER_RATE",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=_env("WORKER_SEED", int, None),
        help="Random seed for reproducible runs. env: WORKER_SEED",
    )
    parser.add_argument(
        "--log-level", default=os.environ.get("LOG_LEVEL", "INFO"), help="env: LOG_LEVEL"
    )
    args = parser.parse_args(argv)
    try:
        simulation = SimulationConfig(
            fail_rate=args.fail_rate,
            min_delay_ms=args.min_delay_ms,
            max_delay_ms=args.max_delay_ms,
            duplicate_rate=args.duplicate_rate,
            out_of_order_rate=args.out_of_order_rate,
            seed=args.seed,
        )
    except ValueError as exc:
        parser.error(str(exc))
    return WorkerSettings(
        rabbitmq_url=args.rabbitmq_url, log_level=args.log_level, simulation=simulation
    )
