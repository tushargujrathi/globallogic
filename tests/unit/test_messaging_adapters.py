"""Broker-facing adapters, against mocked pika channels."""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, call
from uuid import uuid4

from control_plane.core.clock import utcnow
from control_plane.domain.enums import TaskStatus, TaskType
from control_plane.messaging.contracts import TaskEvent
from control_plane.messaging.publisher import publish_json
from control_plane.messaging.topology import (
    DEAD_LETTER_EXCHANGE,
    DELIVERY_LIMIT,
    TASK_UPDATES_EXCHANGE,
    TASK_UPDATES_QUEUE,
    TASKS_QUEUE,
    declare_topology,
)
from worker_simulator.config import SimulationConfig
from worker_simulator.simulator import WorkerSimulator


def test_work_queues_are_quorum_queues_with_delivery_limit_and_dead_lettering() -> None:
    channel = MagicMock()
    declare_topology(channel)

    declared = {c.args[0]: c.kwargs for c in channel.queue_declare.call_args_list}
    for queue in (TASKS_QUEUE, TASK_UPDATES_QUEUE):
        assert declared[queue]["durable"] is True
        assert declared[queue]["arguments"] == {
            "x-queue-type": "quorum",
            "x-delivery-limit": DELIVERY_LIMIT,
            "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE,
            "x-dead-letter-routing-key": queue,
        }
        assert f"{queue}.dead_letter" in declared
    channel.queue_bind.assert_any_call(TASKS_QUEUE, "control_plane.tasks", routing_key="task.*")


def test_publish_json_sends_persistent_mandatory_messages() -> None:
    channel = MagicMock()
    publish_json(channel, exchange="ex", routing_key="rk", body=b"{}", message_id="m-1")

    kwargs = channel.basic_publish.call_args.kwargs
    assert (kwargs["exchange"], kwargs["routing_key"], kwargs["body"]) == ("ex", "rk", b"{}")
    assert kwargs["mandatory"] is True
    assert kwargs["properties"].delivery_mode == 2
    assert kwargs["properties"].message_id == "m-1"
    assert kwargs["properties"].content_type == "application/json"


def _deliver_task(body: bytes, config: SimulationConfig) -> tuple[MagicMock, MagicMock]:
    worker = WorkerSimulator("amqp://unused", config)
    connection, channel = MagicMock(), MagicMock()
    worker._on_task(connection, channel, SimpleNamespace(delivery_tag=5), None, body)
    return connection, channel


def test_worker_publishes_progress_then_acks_the_task() -> None:
    event = TaskEvent(
        id=uuid4(),
        type=TaskType.DEPLOY,
        tenant_id=uuid4(),
        status=TaskStatus.ACCEPTED,
        created_at=utcnow(),
    )
    connection, channel = _deliver_task(
        event.model_dump_json().encode(), SimulationConfig(min_delay_ms=10, max_delay_ms=10)
    )

    published = [c.kwargs for c in channel.basic_publish.call_args_list]
    assert [p["routing_key"] for p in published] == [
        "task_update.in_progress",
        "task_update.done",
    ]
    assert all(p["exchange"] == TASK_UPDATES_EXCHANGE for p in published)
    bodies = [json.loads(p["body"]) for p in published]
    assert {b["task_id"] for b in bodies} == {str(event.id)}
    assert [p["properties"].message_id for p in published] == [b["message_id"] for b in bodies]
    connection.sleep.assert_called_once_with(0.01)
    assert channel.method_calls[-1] == call.basic_ack(5)  # ack only after publishing


def test_worker_rejects_malformed_task_without_requeue() -> None:
    _, channel = _deliver_task(b"garbage", SimulationConfig())
    channel.basic_reject.assert_called_once_with(5, requeue=False)
    channel.basic_publish.assert_not_called()
