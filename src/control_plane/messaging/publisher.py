"""Publishing with publisher confirms."""

import logging
import time
from typing import Any

import pika
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.exceptions import AMQPError

from control_plane.messaging.connection import connect
from control_plane.messaging.topology import declare_topology

logger = logging.getLogger(__name__)


def publish_json(
    channel: Any, *, exchange: str, routing_key: str, body: bytes, message_id: str
) -> None:
    """Publish a persistent JSON message.

    On a channel in confirm mode this blocks until the broker confirms, and
    raises (``NackError`` / ``UnroutableError``) if it refuses or cannot route it.
    """
    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=body,
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=pika.DeliveryMode.Persistent,
            message_id=message_id,
            type=routing_key,
            timestamp=int(time.time()),
        ),
        mandatory=True,
    )


class RabbitMQPublisher:
    """Long-lived confirmed publisher that (re)connects lazily."""

    def __init__(self, url: str) -> None:
        self._url = url
        self._connection: BlockingConnection | None = None
        self._channel: BlockingChannel | None = None

    def publish(self, *, exchange: str, routing_key: str, body: bytes, message_id: str) -> None:
        publish_json(
            self._ensure_channel(),
            exchange=exchange,
            routing_key=routing_key,
            body=body,
            message_id=message_id,
        )

    def close(self) -> None:
        """Drop the connection; the next publish reconnects."""
        if self._connection is not None and self._connection.is_open:
            try:
                self._connection.close()
            except AMQPError:
                logger.debug("Ignoring error while closing broker connection", exc_info=True)
        self._connection = None
        self._channel = None

    def _ensure_channel(self) -> BlockingChannel:
        if self._channel is None or self._channel.is_closed:
            self.close()
            self._connection = connect(self._url)
            channel = self._connection.channel()
            declare_topology(channel)
            channel.confirm_delivery()
            self._channel = channel
        return self._channel
