"""Applies worker progress updates to tasks and, on terminal outcomes, to tenants."""

import logging
from enum import StrEnum

from sqlalchemy.orm import Session

from control_plane.core.clock import utcnow
from control_plane.domain.errors import TaskNotFoundError
from control_plane.domain.rules import can_transition_task, tenant_transition_for
from control_plane.messaging.contracts import TaskUpdate
from control_plane.repositories.inbox import InboxRepository
from control_plane.repositories.tasks import TaskRepository
from control_plane.repositories.tenants import TenantRepository

logger = logging.getLogger(__name__)


class ApplyResult(StrEnum):
    APPLIED = "applied"
    STALE = "stale"  # valid message, but it would not move the task forward
    DUPLICATE = "duplicate"  # this message_id was already processed


class TaskUpdateService:
    def __init__(self, session: Session) -> None:
        self._session = session
        self._tasks = TaskRepository(session)
        self._tenants = TenantRepository(session)
        self._inbox = InboxRepository(session)

    def apply(self, update: TaskUpdate) -> ApplyResult:
        """Apply ``update`` exactly once, never moving state backwards.

        1. Lock the task row: concurrent updates for one task are serialised.
        2. Record the message id in the inbox, in the same transaction. If it is
           already there, this is a redelivery: change nothing.
        3. Apply the transition only if it moves the task forward; a terminal
           outcome also moves the tenant.
        """
        task = self._tasks.get(update.task_id, for_update=True)
        if task is None:
            raise TaskNotFoundError(update.task_id)

        applies = can_transition_task(task.status, update.status)
        is_new = self._inbox.record(
            message_id=update.message_id,
            task_id=task.id,
            reported_status=update.status,
            outcome=ApplyResult.APPLIED if applies else ApplyResult.STALE,
        )
        if not is_new:
            self._session.rollback()
            return ApplyResult.DUPLICATE
        if not applies:
            self._session.commit()  # keep the inbox record so redeliveries short-circuit
            return ApplyResult.STALE

        now = utcnow()
        task.status = update.status
        task.error = update.error
        task.updated_at = now

        if update.status.is_terminal:
            tenant = self._tenants.get(task.tenant_id, for_update=True)
            transition = tenant_transition_for(task.type, update.status)
            if tenant is not None and tenant.status == transition.from_status:
                tenant.status = transition.to_status
                tenant.updated_at = now
            else:
                # Unreachable while "one open task per tenant" holds; never force it.
                logger.error(
                    "Tenant %s not in %s for terminal %s of task %s; tenant left unchanged",
                    task.tenant_id,
                    transition.from_status,
                    update.status,
                    task.id,
                )

        self._session.commit()
        return ApplyResult.APPLIED
