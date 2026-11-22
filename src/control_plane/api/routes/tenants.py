"""Tenant resource. Mutations are asynchronous: they return 201/202 with the task they started."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query, Request, Response, status

from control_plane.api.dependencies import PaginationDep, TenantServiceDep
from control_plane.api.errors import error_responses
from control_plane.api.schemas import (
    Page,
    TaskOut,
    TenantCreate,
    TenantOperationOut,
    TenantOut,
    TenantUpdate,
)
from control_plane.db.models import Task, Tenant
from control_plane.domain.enums import TenantStatus

router = APIRouter(prefix="/tenants", tags=["tenants"])


def _operation(tenant: Tenant, task: Task) -> TenantOperationOut:
    return TenantOperationOut(
        tenant=TenantOut.model_validate(tenant), task=TaskOut.model_validate(task)
    )


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    responses=error_responses(409, 422),
    summary="Create a tenant and start its deploy task",
)
def create_tenant(
    payload: TenantCreate, service: TenantServiceDep, request: Request, response: Response
) -> TenantOperationOut:
    tenant, task = service.create(slug=payload.slug, name=payload.name)
    response.headers["Location"] = str(request.url_for("get_tenant", tenant_id=tenant.id))
    return _operation(tenant, task)


@router.get("", summary="List tenants")
def list_tenants(
    service: TenantServiceDep,
    pagination: PaginationDep,
    status_filter: Annotated[TenantStatus | None, Query(alias="status")] = None,
) -> Page[TenantOut]:
    items, total = service.list(
        status=status_filter, limit=pagination.limit, offset=pagination.offset
    )
    return Page[TenantOut].build(items, total, limit=pagination.limit, offset=pagination.offset)


@router.get("/{tenant_id}", responses=error_responses(404), summary="Get a tenant")
def get_tenant(tenant_id: UUID, service: TenantServiceDep) -> TenantOut:
    return TenantOut.model_validate(service.get(tenant_id))


@router.patch(
    "/{tenant_id}",
    status_code=status.HTTP_202_ACCEPTED,
    responses=error_responses(404, 409, 422),
    summary="Partially update a tenant (optimistic locking on `version`)",
)
def update_tenant(
    tenant_id: UUID, payload: TenantUpdate, service: TenantServiceDep
) -> TenantOperationOut:
    tenant, task = service.update(tenant_id, expected_version=payload.version, name=payload.name)
    return _operation(tenant, task)


@router.delete(
    "/{tenant_id}",
    status_code=status.HTTP_202_ACCEPTED,
    responses=error_responses(404, 409),
    summary="Destroy a tenant (asynchronous)",
)
def delete_tenant(tenant_id: UUID, service: TenantServiceDep) -> TenantOperationOut:
    tenant, task = service.delete(tenant_id)
    return _operation(tenant, task)
