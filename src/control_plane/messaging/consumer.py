"""Consumer of worker progress updates.

:class:`TaskUpdateHandler` decides what to do with one message (pure of any
broker API, so it is tested without RabbitMQ). :class:`TaskUpdateConsumer` is
the thin pika adapter that turns that decision into ack / reject / nack.

Failure policy:

* malformed message, unknown task -> poison: dead-lettered at once
* anything else (DB down, lock conflict, bug) -> retried with backoff; the
  queue's delivery limit dead-letters it if it never succeeds
* the consumer itself never dies on a message; broker drops reconnect
"""

import logging
from enum import StrEnum
from typing import Any

from pika.adapters.blocking_connection import BlockingConnection
from pydantic import ValidationError
from sqlalchemy.orm import Session, sessionmaker

from control_plane.domain.errors import TaskNotFoundError
from control_plane.messaging.connection import backoff_delay, consume_forever
from control_plane.messaging.contracts import TaskUpdate
from control_plane.messaging.topology import TASK_UPDATES_QUEUE
from control_plane.services.task_updates import TaskUpdateService

logger = logging.getLogger(__name__)


class Disposition(StrEnum):
    ACK = "ack"
    DEAD_LETTER = "dead_letter"
    RETRY = "retry"


class TaskUpdateHandler:
    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def handle(self, body: bytes) -> Disposition:
        try:
            update = TaskUpdate.model_validate_json(body)
        except ValidationError as exc:
            logger.warning(
                "Dead-lettering malformed task update: %s", exc.errors(include_input=False)
            )
            return Disposition.DEAD_LETTER

        try:
            with self._session_factory() as session:
                result = TaskUpdateService(session).apply(update)
        except TaskNotFoundError:
            logger.warning(
                "Dead-lettering update %s for unknown task %s", update.message_id, update.task_id
            )
            return Disposition.DEAD_LETTER
        except Exception:
            logger.exception("Failed to apply update %s; will retry", update.message_id)
            return Disposition.RETRY

        logger.info(
            "Update %s (task %s -> %s): %s",
            update.message_id,
            update.task_id,
            update.status,
            result,
        )
        return Disposition.ACK


class TaskUpdateConsumer:
    def __init__(self, url: str, handler: TaskUpdateHandler, *, prefetch_count: int = 10) -> None:
        self._url = url
        self._handler = handler
        self._prefetch_count = prefetch_count

    def run(self) -> None:
        """Consume forever, reconnecting whenever the broker connection is lost."""
        consume_forever(
            self._url, TASK_UPDATES_QUEUE, self.on_message, prefetch_count=self._prefetch_count
        )

    def on_message(
        self,
        connection: BlockingConnection,
        channel: Any,
        method: Any,
        properties: Any,
        body: bytes,
    ) -> None:
        disposition = self._handler.handle(body)
        if disposition is Disposition.ACK:
            channel.basic_ack(method.delivery_tag)
        elif disposition is Disposition.DEAD_LETTER:
            channel.basic_reject(method.delivery_tag, requeue=False)
        else:
            # Back off before handing the message back (pika keeps heartbeats
            # flowing during connection.sleep and does not re-enter this callback).
            delivery_count = int((properties.headers or {}).get("x-delivery-count", 0))
            connection.sleep(backoff_delay(delivery_count, cap=10.0))
            channel.basic_nack(method.delivery_tag, requeue=True)
