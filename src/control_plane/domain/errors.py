"""Domain errors. Each carries a stable, machine-readable ``code`` that is part of the API contract.

These are transport-agnostic; the HTTP layer maps them to status codes.
"""

from typing import Any, ClassVar
from uuid import UUID

from control_plane.domain.enums import TenantStatus


class DomainError(Exception):
    code: ClassVar[str]

    def __init__(self, message: str, **details: Any) -> None:
        super().__init__(message)
        self.message = message
        self.details = details


class TenantNotFoundError(DomainError):
    code = "tenant_not_found"

    def __init__(self, tenant_id: UUID) -> None:
        super().__init__(f"Tenant {tenant_id} does not exist.", tenant_id=str(tenant_id))


class TenantAlreadyExistsError(DomainError):
    code = "tenant_already_exists"

    def __init__(self, slug: str) -> None:
        super().__init__(f"A tenant with slug '{slug}' already exists.", slug=slug)


class TenantUpdateNotAllowedError(DomainError):
    code = "tenant_update_not_allowed"

    def __init__(self, tenant_id: UUID, status: TenantStatus, operation: str) -> None:
        super().__init__(
            f"Cannot {operation} tenant {tenant_id} while it is '{status}'.",
            tenant_id=str(tenant_id),
            status=str(status),
            operation=operation,
        )


class TenantVersionConflictError(DomainError):
    code = "tenant_version_conflict"

    def __init__(self, tenant_id: UUID, expected_version: int, current_version: int) -> None:
        super().__init__(
            f"Tenant {tenant_id} is at version {current_version}, not {expected_version}.",
            tenant_id=str(tenant_id),
            expected_version=expected_version,
            current_version=current_version,
        )


class TaskNotFoundError(DomainError):
    code = "task_not_found"

    def __init__(self, task_id: UUID) -> None:
        super().__init__(f"Task {task_id} does not exist.", task_id=str(task_id))
