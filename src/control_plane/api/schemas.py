"""Request and response bodies of the HTTP API."""

from collections.abc import Iterable
from typing import Annotated, Any, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

from control_plane.core.clock import Timestamp
from control_plane.domain.enums import TaskStatus, TaskType, TenantStatus
from control_plane.domain.rules import NAME_MAX_LENGTH, SLUG_PATTERN

Slug = Annotated[str, StringConstraints(pattern=SLUG_PATTERN)]
TenantName = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=NAME_MAX_LENGTH)
]

# ----------------------------------------------------------------------------- requests


class TenantCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    slug: Slug = Field(description="3-28 chars, `^[a-z][-a-z0-9]{1,26}[a-z0-9]$`. Immutable.")
    name: TenantName


class TenantUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int = Field(ge=1, description="The tenant version this change is based on.")
    name: TenantName | None = None

    @model_validator(mode="after")
    def _require_a_change(self) -> Self:
        if self.name is None:
            raise ValueError("at least one mutable field (name) must be provided")
        return self


# ----------------------------------------------------------------------------- responses


class TenantOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    slug: str
    name: str
    status: TenantStatus
    version: int
    created_at: Timestamp
    updated_at: Timestamp


class TaskOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    type: TaskType
    tenant_id: UUID
    status: TaskStatus
    error: str | None
    created_at: Timestamp
    updated_at: Timestamp


class TenantOperationOut(BaseModel):
    """Returned by every mutating tenant call: the tenant and the task it started."""

    tenant: TenantOut
    task: TaskOut


class Page[T](BaseModel):
    items: list[T]
    total: int
    limit: int
    offset: int

    @classmethod
    def build(cls, items: Iterable[object], total: int, *, limit: int, offset: int) -> Self:
        """Build a page from ORM rows, e.g. ``Page[TenantOut].build(rows, total, ...)``."""
        return cls.model_validate(
            {"items": items, "total": total, "limit": limit, "offset": offset},
            from_attributes=True,
        )


class Error(BaseModel):
    code: str = Field(description="Stable, machine-readable error code.")
    message: str
    details: Any = None


class ErrorResponse(BaseModel):
    error: Error
