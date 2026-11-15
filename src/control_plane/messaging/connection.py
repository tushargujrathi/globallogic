"""Broker connection with retry, and the backoff policy shared by every retry loop."""

import logging
import random
import time

import pika
from pika.adapters.blocking_connection import BlockingConnection
from pika.exceptions import AMQPConnectionError

logger = logging.getLogger(__name__)


def backoff_delay(attempt: int, *, base: float = 0.5, cap: float = 30.0) -> float:
    """Exponential backoff with "equal jitter": between half and all of ``base * 2**attempt``."""
    ceiling = min(cap, base * 2.0 ** max(attempt, 0))
    # Jitter only, not security-sensitive.
    return ceiling / 2 + random.uniform(0, ceiling / 2)  # noqa: S311  # nosec B311


def connect(url: str) -> BlockingConnection:
    """Open a connection, retrying with backoff until the broker is reachable."""
    attempt = 0
    while True:
        try:
            connection = pika.BlockingConnection(pika.URLParameters(url))
        except AMQPConnectionError as exc:
            delay = backoff_delay(attempt)
            logger.warning("Broker unreachable (%r); retrying in %.1fs", exc, delay)
            time.sleep(delay)
            attempt += 1
        else:
            logger.info("Connected to broker")
            return connection
