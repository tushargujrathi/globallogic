"""Exchanges, queues and routing keys. Declared idempotently by every process on connect.

::

    control_plane.tasks (topic) --task.*--------> worker.tasks
    control_plane.task_updates (topic) --task_update.*--> control_plane.task_updates
    control_plane.dead_letter (direct) ---------> <queue>.dead_letter

Work queues are quorum queues with a delivery limit: a message that keeps
failing is dead-lettered by the broker instead of looping forever.
"""

from typing import Any

from control_plane.domain.enums import TaskStatus, TaskType

TASKS_EXCHANGE = "control_plane.tasks"
TASK_UPDATES_EXCHANGE = "control_plane.task_updates"
DEAD_LETTER_EXCHANGE = "control_plane.dead_letter"

TASKS_QUEUE = "worker.tasks"
TASK_UPDATES_QUEUE = "control_plane.task_updates"

DELIVERY_LIMIT = 5


def dead_letter_queue(queue: str) -> str:
    return f"{queue}.dead_letter"


def task_routing_key(task_type: TaskType) -> str:
    return f"task.{task_type}"


def task_update_routing_key(status: TaskStatus) -> str:
    return f"task_update.{status}"


def declare_topology(channel: Any) -> None:
    """Declare everything both sides rely on (safe to call repeatedly).

    The control plane declares the worker's queue too, so tasks published
    before any worker has started are queued rather than dropped.
    """
    channel.exchange_declare(TASKS_EXCHANGE, exchange_type="topic", durable=True)
    channel.exchange_declare(TASK_UPDATES_EXCHANGE, exchange_type="topic", durable=True)
    channel.exchange_declare(DEAD_LETTER_EXCHANGE, exchange_type="direct", durable=True)
    _declare_work_queue(channel, TASKS_QUEUE, TASKS_EXCHANGE, "task.*")
    _declare_work_queue(channel, TASK_UPDATES_QUEUE, TASK_UPDATES_EXCHANGE, "task_update.*")


def _declare_work_queue(channel: Any, queue: str, exchange: str, binding_key: str) -> None:
    dlq = dead_letter_queue(queue)
    channel.queue_declare(dlq, durable=True, arguments={"x-queue-type": "quorum"})
    channel.queue_bind(dlq, DEAD_LETTER_EXCHANGE, routing_key=queue)
    channel.queue_declare(
        queue,
        durable=True,
        arguments={
            "x-queue-type": "quorum",
            "x-delivery-limit": DELIVERY_LIMIT,
            "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE,
            "x-dead-letter-routing-key": queue,
        },
    )
    channel.queue_bind(queue, exchange, routing_key=binding_key)
