"""``worker-simulator`` entrypoint."""

from collections.abc import Sequence

from control_plane.core.lifecycle import run_until_stopped
from control_plane.core.logging import configure_logging
from worker_simulator.config import parse_args
from worker_simulator.simulator import WorkerSimulator


def main(argv: Sequence[str] | None = None) -> None:
    settings = parse_args(argv)
    configure_logging(settings.log_level)
    run_until_stopped(
        "worker simulator", WorkerSimulator(settings.rabbitmq_url, settings.simulation).run
    )


if __name__ == "__main__":
    main()
