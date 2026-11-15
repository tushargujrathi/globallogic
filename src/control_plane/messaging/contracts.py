"""Wire contracts between the control plane and workers (JSON message bodies).

Outbound, ``control_plane.tasks`` exchange -> :class:`TaskEvent`
    The task itself. ``id`` is both the task id and the event id (it is also
    sent as the AMQP ``message_id``).

Inbound, ``control_plane.task_updates`` exchange -> :class:`TaskUpdate`
    A progress report for one task. ``message_id`` is generated once by the
    worker per update and kept across redeliveries/re-publishes, so it is the
    idempotency key.
"""

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from control_plane.core.clock import Timestamp
from control_plane.domain.enums import TaskStatus, TaskType


class TaskEvent(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore")

    id: UUID
    type: TaskType
    tenant_id: UUID
    status: TaskStatus
    created_at: Timestamp


ReportableStatus = Literal[TaskStatus.IN_PROGRESS, TaskStatus.DONE, TaskStatus.FAILED]


class TaskUpdate(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore")

    message_id: UUID = Field(description="Idempotency key, stable across redeliveries.")
    task_id: UUID
    status: ReportableStatus
    occurred_at: Timestamp
    error: str | None = Field(default=None, max_length=2000)
