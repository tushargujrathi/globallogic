"""``control-plane`` entrypoint: ``api`` | ``relay`` | ``consumer``.

Each subcommand is one container in docker-compose, built from the same image.
How each process is wired lives in :mod:`control_plane.bootstrap`.
"""

import argparse
import logging
import signal
from types import FrameType

from control_plane.bootstrap import PROCESSES
from control_plane.core.config import get_settings
from control_plane.core.logging import configure_logging


def _stop_on_sigterm() -> None:
    """Turn SIGTERM (docker stop) into KeyboardInterrupt for a clean shutdown."""

    def _raise(_signum: int, _frame: FrameType | None) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _raise)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="control-plane", description=__doc__)
    parser.add_argument("command", choices=sorted(PROCESSES))
    args = parser.parse_args(argv)
    configure_logging(get_settings().log_level)
    _stop_on_sigterm()
    try:
        PROCESSES[args.command]()
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("%s stopped", args.command)


if __name__ == "__main__":
    main()
