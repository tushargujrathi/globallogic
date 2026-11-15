"""Wire contracts between control plane and worker."""

import json
from uuid import uuid4

import pytest
from pydantic import ValidationError

from control_plane.core.clock import utcnow
from control_plane.domain.enums import TaskStatus, TaskType
from control_plane.messaging.contracts import TaskEvent, TaskUpdate
from control_plane.messaging.topology import task_routing_key, task_update_routing_key


def _update(**overrides: object) -> dict[str, object]:
    body: dict[str, object] = {
        "message_id": str(uuid4()),
        "task_id": str(uuid4()),
        "status": "done",
        "occurred_at": "2026-01-01T00:00:00.000Z",
    }
    body.update(overrides)
    return body


def test_task_event_round_trip_exposes_id_type_tenant_and_status() -> None:
    event = TaskEvent(
        id=uuid4(),
        type=TaskType.DEPLOY,
        tenant_id=uuid4(),
        status=TaskStatus.ACCEPTED,
        created_at=utcnow(),
    )
    wire = json.loads(event.model_dump_json())
    assert set(wire) == {"id", "type", "tenant_id", "status", "created_at"}
    assert wire["created_at"].endswith("Z")
    assert TaskEvent.model_validate(wire) == event


@pytest.mark.parametrize("status", ["in_progress", "done", "failed"])
def test_task_update_accepts_reportable_statuses(status: str) -> None:
    assert TaskUpdate.model_validate(_update(status=status)).status == status


@pytest.mark.parametrize(
    "body",
    [
        _update(status="accepted"),  # workers cannot report "accepted"
        _update(status="exploded"),
        _update(message_id="not-a-uuid"),
        {k: v for k, v in _update().items() if k != "message_id"},  # no idempotency key
        {k: v for k, v in _update().items() if k != "task_id"},
    ],
)
def test_task_update_rejects_invalid_bodies(body: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        TaskUpdate.model_validate(body)


def test_task_update_ignores_unknown_fields_for_forward_compatibility() -> None:
    assert TaskUpdate.model_validate(_update(attempt=3)).status == TaskStatus.DONE


def test_routing_keys() -> None:
    assert task_routing_key(TaskType.DESTROY) == "task.destroy"
    assert task_update_routing_key(TaskStatus.IN_PROGRESS) == "task_update.in_progress"
