"""Broker connection with retry, the shared consume loop, and the backoff policy."""

import logging
import random
import time
from collections.abc import Callable
from functools import partial
from typing import Any

import pika
from pika.adapters.blocking_connection import BlockingConnection
from pika.exceptions import AMQPConnectionError, AMQPError

from control_plane.messaging.topology import declare_topology

MessageCallback = Callable[[BlockingConnection, Any, Any, Any, bytes], None]
"""``(connection, channel, method, properties, body)``: pika's callback plus the connection."""

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


def consume_forever(
    url: str,
    queue: str,
    on_message: MessageCallback,
    *,
    prefetch_count: int,
    confirm_delivery: bool = False,
) -> None:
    """Consume ``queue`` until interrupted, reconnecting whenever the connection is lost.

    ``confirm_delivery`` puts the channel in confirm mode, for callbacks that
    also publish on it.
    """
    while True:
        connection = connect(url)
        try:
            channel = connection.channel()
            declare_topology(channel)
            if confirm_delivery:
                channel.confirm_delivery()
            channel.basic_qos(prefetch_count=prefetch_count)
            channel.basic_consume(queue, on_message_callback=partial(on_message, connection))
            logger.info("Consuming %s", queue)
            channel.start_consuming()
        except AMQPError as exc:
            logger.warning("Lost broker connection (%r); reconnecting", exc)
        except KeyboardInterrupt:
            if connection.is_open:
                connection.close()
            return
