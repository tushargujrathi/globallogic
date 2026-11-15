"""Simulated provisioning.

For every task event the worker publishes ``in_progress``, "works" for a random
delay, then publishes ``done`` or ``failed``. Optional chaos knobs publish
updates twice or out of order, to exercise the control plane's consumer.
"""

import logging
import random
import uuid
from dataclasses import dataclass
from datetime import timedelta
from functools import partial
from typing import Any

from pika.adapters.blocking_connection import BlockingConnection
from pika.exceptions import AMQPError
from pydantic import ValidationError

from control_plane.core.clock import utcnow
from control_plane.domain.enums import TaskStatus
from control_plane.messaging.connection import connect
from control_plane.messaging.contracts import TaskEvent, TaskUpdate
from control_plane.messaging.publisher import publish_json
from control_plane.messaging.topology import (
    TASK_UPDATES_EXCHANGE,
    TASKS_QUEUE,
    declare_topology,
    task_update_routing_key,
)
from worker_simulator.config import SimulationConfig

logger = logging.getLogger(__name__)

SIMULATED_ERROR = "simulated provisioning failure"


@dataclass(frozen=True, slots=True)
class Step:
    """Wait ``delay_seconds``, then publish ``update``."""

    delay_seconds: float
    update: TaskUpdate


def plan_updates(task_id: uuid.UUID, config: SimulationConfig, rng: random.Random) -> list[Step]:
    """Decide, up front, which updates to publish for a task and when.

    ``rng`` drives outcome, timing and chaos (seedable for reproducible runs).
    """
    work_seconds = rng.uniform(config.min_delay_ms, config.max_delay_ms) / 1000
    failed = rng.random() < config.fail_rate
    started_at = utcnow()

    def update(status: TaskStatus, offset: float = 0.0, error: str | None = None) -> TaskUpdate:
        return TaskUpdate(
            # Never derived from the seeded rng: a restarted or second worker with
            # the same seed would reuse idempotency keys and its updates would be
            # dropped as duplicates.
            message_id=uuid.uuid4(),
            task_id=task_id,
            status=status,
            occurred_at=started_at + timedelta(seconds=offset),
            error=error,
        )

    in_progress = update(TaskStatus.IN_PROGRESS)
    terminal = (
        update(TaskStatus.FAILED, work_seconds, SIMULATED_ERROR)
        if failed
        else update(TaskStatus.DONE, work_seconds)
    )

    steps = [Step(0.0, in_progress), Step(work_seconds, terminal)]
    if rng.random() < config.out_of_order_rate:
        steps = [Step(work_seconds, terminal), Step(0.0, in_progress)]
    if rng.random() < config.duplicate_rate:
        steps = [s for step in steps for s in (step, Step(0.0, step.update))]
    return steps


class WorkerSimulator:
    def __init__(self, rabbitmq_url: str, config: SimulationConfig) -> None:
        self._url = rabbitmq_url
        self._config = config
        # Simulation only, not security-sensitive; seedable for reproducible runs.
        self._rng = random.Random(config.seed)  # noqa: S311  # nosec B311

    def run(self) -> None:
        """Consume tasks forever, reconnecting whenever the broker connection is lost."""
        logger.info("Worker simulator started with %s", self._config)
        while True:
            connection = connect(self._url)
            try:
                channel = connection.channel()
                declare_topology(channel)
                channel.confirm_delivery()
                channel.basic_qos(prefetch_count=1)
                channel.basic_consume(
                    TASKS_QUEUE, on_message_callback=partial(self._on_task, connection)
                )
                channel.start_consuming()
            except AMQPError as exc:
                logger.warning("Lost broker connection (%r); reconnecting", exc)
            except KeyboardInterrupt:
                if connection.is_open:
                    connection.close()
                return

    def _on_task(
        self, connection: BlockingConnection, channel: Any, method: Any, _props: Any, body: bytes
    ) -> None:
        try:
            event = TaskEvent.model_validate_json(body)
        except ValidationError:
            logger.warning("Rejecting malformed task event: %r", body[:200])
            channel.basic_reject(method.delivery_tag, requeue=False)
            return

        logger.info("Processing %s task %s for tenant %s", event.type, event.id, event.tenant_id)
        for step in plan_updates(event.id, self._config, self._rng):
            if step.delay_seconds:
                connection.sleep(step.delay_seconds)  # keeps heartbeats flowing
            publish_json(
                channel,
                exchange=TASK_UPDATES_EXCHANGE,
                routing_key=task_update_routing_key(step.update.status),
                body=step.update.model_dump_json().encode(),
                message_id=str(step.update.message_id),
            )
            logger.info("Task %s -> %s", event.id, step.update.status)
        # Ack only after every update is confirmed: a crash mid-task redelivers it.
        channel.basic_ack(method.delivery_tag)
