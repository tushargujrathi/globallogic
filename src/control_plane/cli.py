"""``control-plane`` entrypoint: ``api`` | ``relay`` | ``consumer``.

Each subcommand is one container in docker-compose, built from the same image.
How each process is wired lives in :mod:`control_plane.bootstrap`.
"""

import argparse

from control_plane.bootstrap import PROCESSES
from control_plane.core.config import get_settings
from control_plane.core.lifecycle import run_until_stopped
from control_plane.core.logging import configure_logging


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="control-plane", description=__doc__)
    parser.add_argument("command", choices=sorted(PROCESSES))
    args = parser.parse_args(argv)
    configure_logging(get_settings().log_level)
    run_until_stopped(args.command, PROCESSES[args.command])


if __name__ == "__main__":
    main()
