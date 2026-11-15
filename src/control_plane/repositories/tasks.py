from collections.abc import Sequence
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from control_plane.db.models import Task
from control_plane.domain.enums import TaskStatus, TaskType
from control_plane.repositories.pagination import paginate


class TaskRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add(self, task: Task) -> None:
        self._session.add(task)

    def get(self, task_id: UUID, *, for_update: bool = False) -> Task | None:
        """Load a task; ``for_update`` takes a row lock until the transaction ends."""
        return self._session.get(
            Task, task_id, with_for_update=for_update, populate_existing=for_update
        )

    def list(
        self,
        *,
        tenant_id: UUID | None,
        status: TaskStatus | None,
        task_type: TaskType | None,
        limit: int,
        offset: int,
    ) -> tuple[Sequence[Task], int]:
        query = select(Task)
        if tenant_id is not None:
            query = query.where(Task.tenant_id == tenant_id)
        if status is not None:
            query = query.where(Task.status == status)
        if task_type is not None:
            query = query.where(Task.type == task_type)
        return paginate(
            self._session,
            query,
            order_by=(Task.created_at, Task.id),
            limit=limit,
            offset=offset,
        )
