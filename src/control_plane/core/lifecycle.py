"""Process lifecycle shared by every long-running entrypoint."""

import logging
import signal
from collections.abc import Callable
from types import FrameType

logger = logging.getLogger(__name__)


def run_until_stopped(name: str, target: Callable[[], None]) -> None:
    """Run ``target`` until it returns or the process is stopped (Ctrl-C or SIGTERM).

    SIGTERM (``docker stop``) is turned into ``KeyboardInterrupt`` so both
    signals take the same clean-shutdown path inside ``target``.
    """

    def _raise(_signum: int, _frame: FrameType | None) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _raise)
    try:
        target()
    except KeyboardInterrupt:
        logger.info("%s stopped", name)
