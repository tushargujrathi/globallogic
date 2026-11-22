"""Task resource: read-only. Tasks are created via tenant calls and driven by workers."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query

from control_plane.api.dependencies import PaginationDep, TaskServiceDep
from control_plane.api.errors import error_responses
from control_plane.api.schemas import Page, TaskOut
from control_plane.domain.enums import TaskStatus, TaskType

router = APIRouter(prefix="/tasks", tags=["tasks"])


@router.get("", summary="List tasks")
def list_tasks(
    service: TaskServiceDep,
    pagination: PaginationDep,
    tenant_id: UUID | None = None,
    status_filter: Annotated[TaskStatus | None, Query(alias="status")] = None,
    type_filter: Annotated[TaskType | None, Query(alias="type")] = None,
) -> Page[TaskOut]:
    items, total = service.list(
        tenant_id=tenant_id,
        status=status_filter,
        task_type=type_filter,
        limit=pagination.limit,
        offset=pagination.offset,
    )
    return Page[TaskOut].build(items, total, limit=pagination.limit, offset=pagination.offset)


@router.get("/{task_id}", responses=error_responses(404), summary="Get a task")
def get_task(task_id: UUID, service: TaskServiceDep) -> TaskOut:
    return TaskOut.model_validate(service.get(task_id))
