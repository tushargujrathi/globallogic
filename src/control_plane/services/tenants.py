"""Tenant use cases. Every mutation writes tenant + task + outbox row in ONE transaction."""

from collections.abc import Sequence
from datetime import datetime
from uuid import UUID, uuid4

from psycopg.errors import UniqueViolation
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import StaleDataError

from control_plane.core.clock import utcnow
from control_plane.db.models import OutboxMessage, Task, Tenant
from control_plane.domain.enums import TaskStatus, TaskType, TenantStatus
from control_plane.domain.errors import (
    TenantAlreadyExistsError,
    TenantNotFoundError,
    TenantUpdateNotAllowedError,
    TenantVersionConflictError,
)
from control_plane.domain.rules import DESTROYABLE_FROM, UPDATABLE_FROM
from control_plane.messaging.contracts import TaskEvent
from control_plane.messaging.topology import task_routing_key
from control_plane.repositories.outbox import OutboxRepository
from control_plane.repositories.tasks import TaskRepository
from control_plane.repositories.tenants import TenantRepository

_SLUG_CONSTRAINT = "uq_tenants_slug"


class TenantService:
    def __init__(self, session: Session) -> None:
        self._session = session
        self._tenants = TenantRepository(session)
        self._tasks = TaskRepository(session)
        self._outbox = OutboxRepository(session)

    # ------------------------------------------------------------------ queries

    def get(self, tenant_id: UUID) -> Tenant:
        tenant = self._tenants.get(tenant_id)
        if tenant is None:
            raise TenantNotFoundError(tenant_id)
        return tenant

    def list(
        self, *, status: TenantStatus | None, limit: int, offset: int
    ) -> tuple[Sequence[Tenant], int]:
        return self._tenants.list(status=status, limit=limit, offset=offset)

    # ---------------------------------------------------------------- commands

    def create(self, *, slug: str, name: str) -> tuple[Tenant, Task]:
        """Create a tenant in ``provisioning`` and start its ``deploy`` task.

        Slug uniqueness is enforced by the database's unique constraint, so of
        two concurrent creates exactly one commits; the other blocks on the
        index until then and gets a unique violation.
        """
        now = utcnow()
        tenant = Tenant(
            id=uuid4(),
            slug=slug,
            name=name,
            status=TenantStatus.PROVISIONING,
            created_at=now,
            updated_at=now,
        )
        self._tenants.add(tenant)
        try:
            self._session.flush()
        except IntegrityError as exc:
            self._session.rollback()
            if _violates(exc, _SLUG_CONSTRAINT):
                raise TenantAlreadyExistsError(slug) from exc
            raise
        task = self._start_task(tenant, TaskType.DEPLOY, now)
        self._session.commit()
        return tenant, task

    def update(
        self, tenant_id: UUID, *, expected_version: int, name: str | None
    ) -> tuple[Tenant, Task]:
        """Apply a partial update (only allowed from ``active``) and start an ``update`` task.

        The caller's ``expected_version`` must match the current version
        (optimistic locking). The tenant row is locked while the precondition
        is checked, so two concurrent PATCHes carrying the same version are
        serialised: the second one sees the bumped version and gets a conflict.
        """
        tenant = self._get_for_update(tenant_id)
        if tenant.version != expected_version:
            raise TenantVersionConflictError(tenant_id, expected_version, tenant.version)
        if tenant.status not in UPDATABLE_FROM:
            raise TenantUpdateNotAllowedError(tenant_id, tenant.status, "update")

        now = utcnow()
        if name is not None:
            tenant.name = name
        tenant.status = TenantStatus.UPDATING
        tenant.updated_at = now
        task = self._start_task(tenant, TaskType.UPDATE, now)
        self._commit(tenant_id, expected_version)
        return tenant, task

    def delete(self, tenant_id: UUID) -> tuple[Tenant, Task]:
        """Start destroying a tenant (allowed from ``active`` or ``failed``).

        The row is kept with status ``destroyed`` once the task completes, so
        the slug stays reserved.
        """
        tenant = self._get_for_update(tenant_id)
        if tenant.status not in DESTROYABLE_FROM:
            raise TenantUpdateNotAllowedError(tenant_id, tenant.status, "delete")

        now = utcnow()
        tenant.status = TenantStatus.DESTROYING
        tenant.updated_at = now
        task = self._start_task(tenant, TaskType.DESTROY, now)
        self._commit(tenant_id, tenant.version)
        return tenant, task

    # ----------------------------------------------------------------- helpers

    def _get_for_update(self, tenant_id: UUID) -> Tenant:
        tenant = self._tenants.get(tenant_id, for_update=True)
        if tenant is None:
            raise TenantNotFoundError(tenant_id)
        return tenant

    def _start_task(self, tenant: Tenant, task_type: TaskType, now: datetime) -> Task:
        """Create the task and its outbox event; they commit (or roll back) with the tenant."""
        task = Task(
            id=uuid4(),
            tenant_id=tenant.id,
            type=task_type,
            status=TaskStatus.ACCEPTED,
            created_at=now,
            updated_at=now,
        )
        self._tasks.add(task)
        event = TaskEvent.model_validate(task, from_attributes=True)
        self._outbox.add(
            OutboxMessage(
                message_id=task.id,
                routing_key=task_routing_key(task_type),
                payload=event.model_dump(mode="json"),
                created_at=now,
            )
        )
        return task

    def _commit(self, tenant_id: UUID, expected_version: int) -> None:
        try:
            self._session.commit()
        except StaleDataError as exc:
            # Safety net: the row lock makes this unreachable for API writers,
            # but the versioned UPDATE still refuses to overwrite a concurrent change.
            self._session.rollback()
            current = self.get(tenant_id).version
            raise TenantVersionConflictError(tenant_id, expected_version, current) from exc


def _violates(exc: IntegrityError, constraint: str) -> bool:
    origin = exc.orig
    return isinstance(origin, UniqueViolation) and origin.diag.constraint_name == constraint
