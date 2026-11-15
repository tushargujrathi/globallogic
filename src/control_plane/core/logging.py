"""Process-wide logging setup."""

import logging


def configure_logging(level: str = "INFO") -> None:
    """Configure a single, consistent log format for every entrypoint."""
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)-7s %(name)s - %(message)s",
    )
    # pika logs every failed connection attempt at ERROR with a full trace; our
    # retry loops already log one concise warning per attempt.
    logging.getLogger("pika").setLevel(logging.CRITICAL)
