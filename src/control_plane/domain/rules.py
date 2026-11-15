"""Tenant and task state machines, and the validation rules of the domain.

Tenant transitions are driven by two things only:

* API calls start a task and move the tenant into a transitional status
  (``provisioning`` / ``updating`` / ``destroying``), guarded by the allowed
  source statuses below.
* A **terminal** task outcome moves the tenant out of that transitional status.

Because a task can only be started from a non-transitional status, and a task
leaves the tenant in a transitional status until it is terminal, there is at
most one open task per tenant by construction.
"""

from dataclasses import dataclass

from control_plane.domain.enums import TaskStatus, TaskType, TenantStatus

SLUG_PATTERN = r"^[a-z][-a-z0-9]{1,26}[a-z0-9]$"
"""3-28 chars, Kubernetes-namespace compatible."""
SLUG_MAX_LENGTH = 28
NAME_MAX_LENGTH = 255

# ----------------------------------------------------------------------------- API guards

UPDATABLE_FROM: frozenset[TenantStatus] = frozenset({TenantStatus.ACTIVE})
DESTROYABLE_FROM: frozenset[TenantStatus] = frozenset({TenantStatus.ACTIVE, TenantStatus.FAILED})

# ----------------------------------------------------------------------------- task machine

_TASK_STATUS_RANK: dict[TaskStatus, int] = {
    TaskStatus.ACCEPTED: 0,
    TaskStatus.IN_PROGRESS: 1,
    TaskStatus.DONE: 2,
    TaskStatus.FAILED: 2,
}


def can_transition_task(current: TaskStatus, new: TaskStatus) -> bool:
    """Whether a task in ``current`` may move to ``new``.

    Transitions are forward-only and terminal statuses are final, which is what
    makes the consumer order-tolerant: a stale update (``in_progress`` after
    ``done``, anything after a terminal status, a repeat of the current status)
    is simply not a valid transition.

    ``accepted -> done|failed`` is allowed on purpose: if the terminal update
    overtakes the ``in_progress`` one, rejecting it would lose the outcome and
    leave the task stuck forever.
    """
    return _TASK_STATUS_RANK[new] > _TASK_STATUS_RANK[current]


# ----------------------------------------------------------------------------- tenant machine


@dataclass(frozen=True, slots=True)
class TenantTransition:
    from_status: TenantStatus
    to_status: TenantStatus


_TERMINAL_OUTCOME_TRANSITIONS: dict[tuple[TaskType, TaskStatus], TenantTransition] = {
    (TaskType.DEPLOY, TaskStatus.DONE): TenantTransition(
        TenantStatus.PROVISIONING, TenantStatus.ACTIVE
    ),
    (TaskType.DEPLOY, TaskStatus.FAILED): TenantTransition(
        TenantStatus.PROVISIONING, TenantStatus.FAILED
    ),
    (TaskType.UPDATE, TaskStatus.DONE): TenantTransition(
        TenantStatus.UPDATING, TenantStatus.ACTIVE
    ),
    (TaskType.UPDATE, TaskStatus.FAILED): TenantTransition(
        TenantStatus.UPDATING, TenantStatus.FAILED
    ),
    (TaskType.DESTROY, TaskStatus.DONE): TenantTransition(
        TenantStatus.DESTROYING, TenantStatus.DESTROYED
    ),
    (TaskType.DESTROY, TaskStatus.FAILED): TenantTransition(
        TenantStatus.DESTROYING, TenantStatus.FAILED
    ),
}


def tenant_transition_for(task_type: TaskType, outcome: TaskStatus) -> TenantTransition:
    """The tenant transition caused by a terminal task ``outcome``."""
    try:
        return _TERMINAL_OUTCOME_TRANSITIONS[(task_type, outcome)]
    except KeyError:
        raise ValueError(
            f"{outcome!s} is not a terminal outcome for a {task_type!s} task"
        ) from None
