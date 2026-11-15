"""FastAPI dependencies: one DB session per request, services built on top of it."""

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, Query, Request
from sqlalchemy.orm import Session

from control_plane.services.tasks import TaskService
from control_plane.services.tenants import TenantService


def get_session(request: Request) -> Iterator[Session]:
    with request.app.state.session_factory() as session:
        yield session


SessionDep = Annotated[Session, Depends(get_session)]


def get_tenant_service(session: SessionDep) -> TenantService:
    return TenantService(session)


def get_task_service(session: SessionDep) -> TaskService:
    return TaskService(session)


TenantServiceDep = Annotated[TenantService, Depends(get_tenant_service)]
TaskServiceDep = Annotated[TaskService, Depends(get_task_service)]


@dataclass(frozen=True)
class Pagination:
    limit: int
    offset: int


def get_pagination(
    limit: Annotated[int, Query(ge=1, le=100, description="Page size.")] = 20,
    offset: Annotated[int, Query(ge=0, description="Items to skip.")] = 0,
) -> Pagination:
    return Pagination(limit=limit, offset=offset)


PaginationDep = Annotated[Pagination, Depends(get_pagination)]
