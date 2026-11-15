"""Transactional-outbox relay: publishes committed outbox rows to the broker.

Guarantees:

* **After commit only.** Rows are written in the API's transaction; the relay
  can only see committed rows, so nothing is published for a rolled-back write.
* **At least once.** A row is marked published only after the broker confirmed
  it. A crash between confirm and mark republishes it; consumers dedupe on the
  task id (the event id).
"""

import json
import logging
import time
from typing import Protocol

from pika.exceptions import AMQPError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from control_plane.core.clock import utcnow
from control_plane.messaging.connection import backoff_delay
from control_plane.messaging.topology import TASKS_EXCHANGE
from control_plane.repositories.outbox import OutboxRepository

logger = logging.getLogger(__name__)


class Publisher(Protocol):
    def publish(self, *, exchange: str, routing_key: str, body: bytes, message_id: str) -> None: ...

    def close(self) -> None: ...


class OutboxRelay:
    def __init__(
        self,
        session_factory: sessionmaker[Session],
        publisher: Publisher,
        *,
        batch_size: int = 100,
        poll_interval_seconds: float = 0.5,
    ) -> None:
        self._session_factory = session_factory
        self._publisher = publisher
        self._batch_size = batch_size
        self._poll_interval = poll_interval_seconds

    def publish_pending(self) -> int:
        """Publish one batch of pending messages. Returns how many were published."""
        published = 0
        with self._session_factory() as session:
            for message in OutboxRepository(session).lock_unpublished(self._batch_size):
                try:
                    self._publisher.publish(
                        exchange=TASKS_EXCHANGE,
                        routing_key=message.routing_key,
                        body=json.dumps(message.payload).encode(),
                        message_id=str(message.message_id),
                    )
                except Exception:
                    session.commit()  # keep the marks of messages the broker already confirmed
                    raise
                message.published_at = utcnow()
                published += 1
            session.commit()
        if published:
            logger.info("Published %d outbox message(s)", published)
        return published

    def run(self) -> None:
        """Relay forever; broker and database failures are retried with backoff."""
        logger.info("Outbox relay started")
        attempt = 0
        while True:
            try:
                published = self.publish_pending()
            except (AMQPError, SQLAlchemyError) as exc:
                delay = backoff_delay(attempt)
                logger.warning("Outbox relay failed (%r); retrying in %.1fs", exc, delay)
                self._publisher.close()
                time.sleep(delay)
                attempt += 1
                continue
            attempt = 0
            if published < self._batch_size:
                time.sleep(self._poll_interval)
