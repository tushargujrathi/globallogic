"""Read-only task queries. Tasks are created by TenantService and driven by worker updates."""

from collections.abc import Sequence
from uuid import UUID

from sqlalchemy.orm import Session

from control_plane.db.models import Task
from control_plane.domain.enums import TaskStatus, TaskType
from control_plane.domain.errors import TaskNotFoundError
from control_plane.repositories.tasks import TaskRepository


class TaskService:
    def __init__(self, session: Session) -> None:
        self._tasks = TaskRepository(session)

    def get(self, task_id: UUID) -> Task:
        task = self._tasks.get(task_id)
        if task is None:
            raise TaskNotFoundError(task_id)
        return task

    def list(
        self,
        *,
        tenant_id: UUID | None,
        status: TaskStatus | None,
        task_type: TaskType | None,
        limit: int,
        offset: int,
    ) -> tuple[Sequence[Task], int]:
        return self._tasks.list(
            tenant_id=tenant_id, status=status, task_type=task_type, limit=limit, offset=offset
        )
