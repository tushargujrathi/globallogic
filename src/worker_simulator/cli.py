"""``worker-simulator`` entrypoint."""

import logging
import signal
from collections.abc import Sequence
from types import FrameType

from control_plane.core.logging import configure_logging
from worker_simulator.config import parse_args
from worker_simulator.simulator import WorkerSimulator


def main(argv: Sequence[str] | None = None) -> None:
    settings = parse_args(argv)
    configure_logging(settings.log_level)

    def _raise(_signum: int, _frame: FrameType | None) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _raise)
    try:
        WorkerSimulator(settings.rabbitmq_url, settings.simulation).run()
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("worker simulator stopped")


if __name__ == "__main__":
    main()
