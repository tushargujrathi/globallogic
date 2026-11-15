"""State machines and guards (pure domain logic)."""

import itertools

import pytest

from control_plane.domain.enums import TaskStatus, TaskType, TenantStatus
from control_plane.domain.rules import (
    DESTROYABLE_FROM,
    UPDATABLE_FROM,
    TenantTransition,
    can_transition_task,
    tenant_transition_for,
)

S = TaskStatus

ALLOWED_TASK_TRANSITIONS = {
    (S.ACCEPTED, S.IN_PROGRESS),
    (S.ACCEPTED, S.DONE),  # terminal overtook in_progress: must not be lost
    (S.ACCEPTED, S.FAILED),
    (S.IN_PROGRESS, S.DONE),
    (S.IN_PROGRESS, S.FAILED),
}


@pytest.mark.parametrize(("current", "new"), list(itertools.product(S, S)))
def test_task_transitions_are_forward_only(current: TaskStatus, new: TaskStatus) -> None:
    assert can_transition_task(current, new) == ((current, new) in ALLOWED_TASK_TRANSITIONS)


@pytest.mark.parametrize("terminal", [S.DONE, S.FAILED])
def test_terminal_task_statuses_are_final(terminal: TaskStatus) -> None:
    assert terminal.is_terminal
    assert not any(can_transition_task(terminal, new) for new in S)


def test_non_terminal_statuses() -> None:
    assert not S.ACCEPTED.is_terminal
    assert not S.IN_PROGRESS.is_terminal


@pytest.mark.parametrize(
    ("task_type", "outcome", "expected"),
    [
        (TaskType.DEPLOY, S.DONE, (TenantStatus.PROVISIONING, TenantStatus.ACTIVE)),
        (TaskType.DEPLOY, S.FAILED, (TenantStatus.PROVISIONING, TenantStatus.FAILED)),
        (TaskType.UPDATE, S.DONE, (TenantStatus.UPDATING, TenantStatus.ACTIVE)),
        (TaskType.UPDATE, S.FAILED, (TenantStatus.UPDATING, TenantStatus.FAILED)),
        (TaskType.DESTROY, S.DONE, (TenantStatus.DESTROYING, TenantStatus.DESTROYED)),
        (TaskType.DESTROY, S.FAILED, (TenantStatus.DESTROYING, TenantStatus.FAILED)),
    ],
)
def test_terminal_outcomes_move_the_tenant(
    task_type: TaskType, outcome: TaskStatus, expected: tuple[TenantStatus, TenantStatus]
) -> None:
    assert tenant_transition_for(task_type, outcome) == TenantTransition(*expected)


@pytest.mark.parametrize("task_type", list(TaskType))
@pytest.mark.parametrize("status", [S.ACCEPTED, S.IN_PROGRESS])
def test_non_terminal_outcomes_do_not_move_the_tenant(
    task_type: TaskType, status: TaskStatus
) -> None:
    with pytest.raises(ValueError, match="not a terminal outcome"):
        tenant_transition_for(task_type, status)


def test_api_guards() -> None:
    assert frozenset({TenantStatus.ACTIVE}) == UPDATABLE_FROM
    assert frozenset({TenantStatus.ACTIVE, TenantStatus.FAILED}) == DESTROYABLE_FROM
