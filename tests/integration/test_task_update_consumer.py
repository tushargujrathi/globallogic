"""Inbound messaging: idempotency, order tolerance and poison/transient handling."""

import json
import threading
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker

from control_plane.db.models import ProcessedMessage, Task, Tenant
from control_plane.domain.enums import TaskStatus, TaskType, TenantStatus
from control_plane.messaging.consumer import Disposition, TaskUpdateHandler
from tests.integration.conftest import FakeWorker, TenantFactory

S = TaskStatus


def snapshot(session_factory: sessionmaker[Session], task_id: object) -> tuple[object, ...]:
    """Everything an update may change: task status/updated_at, tenant status/version."""
    with session_factory() as session:
        task = session.get_one(Task, UUID(str(task_id)))
        tenant = session.get_one(Tenant, task.tenant_id)
        return task.status, task.updated_at, tenant.status, tenant.version, tenant.updated_at


def processed_count(session_factory: sessionmaker[Session]) -> int:
    with session_factory() as session:
        return session.scalar(select(func.count()).select_from(ProcessedMessage)) or 0


# ----------------------------------------------------------------------------- happy path


@pytest.mark.parametrize(
    ("task_type", "outcome", "expected_tenant_status"),
    [
        (TaskType.DEPLOY, S.DONE, TenantStatus.ACTIVE),
        (TaskType.DEPLOY, S.FAILED, TenantStatus.FAILED),
        (TaskType.UPDATE, S.DONE, TenantStatus.ACTIVE),
        (TaskType.UPDATE, S.FAILED, TenantStatus.FAILED),
        (TaskType.DESTROY, S.DONE, TenantStatus.DESTROYED),
        (TaskType.DESTROY, S.FAILED, TenantStatus.FAILED),
    ],
)
def test_terminal_outcome_moves_task_and_tenant(
    tenants: TenantFactory,
    worker: FakeWorker,
    session_factory: sessionmaker[Session],
    task_type: TaskType,
    outcome: TaskStatus,
    expected_tenant_status: TenantStatus,
) -> None:
    if task_type is TaskType.DEPLOY:
        task_id = tenants.create()["task"]["id"]  # type: ignore[index]
    else:
        tenant = tenants.active()
        path = f"/api/v1/tenants/{tenant['id']}"
        response = (
            tenants.client.patch(path, json={"version": 2, "name": "n"})
            if task_type is TaskType.UPDATE
            else tenants.client.delete(path)
        )
        task_id = response.json()["task"]["id"]
    version_before = snapshot(session_factory, task_id)[3]

    worker.complete(task_id, outcome)

    task_status, _, tenant_status, version, _ = snapshot(session_factory, task_id)
    assert task_status == outcome
    assert tenant_status == expected_tenant_status
    assert version == version_before + 1  # type: ignore[operator]


def test_in_progress_changes_the_task_but_not_the_tenant(
    tenants: TenantFactory, worker: FakeWorker, session_factory: sessionmaker[Session]
) -> None:
    task_id = tenants.create()["task"]["id"]  # type: ignore[index]

    assert worker.report(task_id, S.IN_PROGRESS) is Disposition.ACK

    task_status, _, tenant_status, version, _ = snapshot(session_factory, task_id)
    assert (task_status, tenant_status, version) == (S.IN_PROGRESS, TenantStatus.PROVISIONING, 1)


def test_failure_reason_is_stored(
    tenants: TenantFactory, worker: FakeWorker, session_factory: sessionmaker[Session]
) -> None:
    task_id = tenants.create()["task"]["id"]  # type: ignore[index]
    worker.report(task_id, S.FAILED, error="quota exceeded")
    with session_factory() as session:
        assert session.get_one(Task, UUID(task_id)).error == "quota exceeded"


# ----------------------------------------------------------------------------- idempotency


@pytest.mark.parametrize("status", [S.IN_PROGRESS, S.DONE, S.FAILED])
def test_redelivered_update_changes_nothing_and_fails_nothing(
    tenants: TenantFactory,
    worker: FakeWorker,
    session_factory: sessionmaker[Session],
    status: TaskStatus,
) -> None:
    task_id = tenants.create()["task"]["id"]  # type: ignore[index]
    message_id = uuid4()
    assert worker.report(task_id, status, message_id=message_id) is Disposition.ACK
    before = snapshot(session_factory, task_id)

    for _ in range(3):
        assert worker.report(task_id, status, message_id=message_id) is Disposition.ACK

    assert snapshot(session_factory, task_id) == before
    assert processed_count(session_factory) == 1


def test_byte_identical_redelivery_is_a_noop(
    tenants: TenantFactory, handler: TaskUpdateHandler, session_factory: sessionmaker[Session]
) -> None:
    task_id = tenants.create()["task"]["id"]  # type: ignore[index]
    body = json.dumps(
        {
            "message_id": str(uuid4()),
            "task_id": task_id,
            "status": "done",
            "occurred_at": "2026-01-01T00:00:00.000Z",
        }
    ).encode()
    assert handler.handle(body) is Disposition.ACK
    before = snapshot(session_factory, task_id)
    assert handler.handle(body) is Disposition.ACK
    assert snapshot(session_factory, task_id) == before


def test_concurrent_duplicate_deliveries_apply_once(
    tenants: TenantFactory, worker: FakeWorker, session_factory: sessionmaker[Session]
) -> None:
    """Same update consumed by several consumer replicas at the same instant."""
    task_id = tenants.create()["task"]["id"]  # type: ignore[index]
    message_id = uuid4()
    barrier = threading.Barrier(8)
    results: list[Disposition] = []

    def deliver() -> None:
        barrier.wait()
        results.append(worker.report(task_id, S.DONE, message_id=message_id))

    threads = [threading.Thread(target=deliver) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert results == [Disposition.ACK] * 8
    task_status, _, tenant_status, version, _ = snapshot(session_factory, task_id)
    assert (task_status, tenant_status, version) == (S.DONE, TenantStatus.ACTIVE, 2)
    assert processed_count(session_factory) == 1


# ----------------------------------------------------------------------------- ordering


@pytest.mark.parametrize(
    ("terminal", "stale"),
    [
        (S.DONE, S.IN_PROGRESS),  # in_progress arriving after done
        (S.FAILED, S.IN_PROGRESS),
        (S.DONE, S.FAILED),  # any update for a terminal task
        (S.FAILED, S.DONE),
        (S.DONE, S.DONE),  # a different message repeating the outcome
    ],
)
def test_stale_update_does_not_regress_state(
    tenants: TenantFactory,
    worker: FakeWorker,
    session_factory: sessionmaker[Session],
    terminal: TaskStatus,
    stale: TaskStatus,
) -> None:
    task_id = tenants.create()["task"]["id"]  # type: ignore[index]
    worker.complete(task_id, terminal)
    before = snapshot(session_factory, task_id)

    assert worker.report(task_id, stale) is Disposition.ACK

    assert snapshot(session_factory, task_id) == before


def test_terminal_update_overtaking_in_progress_is_applied(
    tenants: TenantFactory, worker: FakeWorker, session_factory: sessionmaker[Session]
) -> None:
    task_id = tenants.create()["task"]["id"]  # type: ignore[index]

    worker.report(task_id, S.DONE)
    worker.report(task_id, S.IN_PROGRESS)  # late

    task_status, _, tenant_status, version, _ = snapshot(session_factory, task_id)
    assert (task_status, tenant_status, version) == (S.DONE, TenantStatus.ACTIVE, 2)


def test_stale_update_for_an_old_task_does_not_touch_the_tenant_current_task(
    tenants: TenantFactory, worker: FakeWorker, session_factory: sessionmaker[Session]
) -> None:
    """A late 'failed' for the finished deploy must not fail the tenant during its update."""
    tenant = tenants.active()
    deploy_task = tenants.client.get("/api/v1/tasks", params={"tenant_id": tenant["id"]}).json()[
        "items"
    ][0]
    update_task = tenants.client.patch(
        f"/api/v1/tenants/{tenant['id']}", json={"version": 2, "name": "n"}
    ).json()["task"]
    before = snapshot(session_factory, update_task["id"])

    worker.report(deploy_task["id"], S.FAILED)

    assert snapshot(session_factory, update_task["id"]) == before


# ----------------------------------------------------------------------------- resilience


@pytest.mark.parametrize(
    "body",
    [
        b"",
        b"not json",
        b"[]",
        b'{"task_id": "x"}',
        json.dumps(
            {
                "message_id": str(uuid4()),
                "task_id": str(uuid4()),
                "status": "accepted",
                "occurred_at": "2026-01-01T00:00:00Z",
            }
        ).encode(),
        "\udcff".encode("utf-8", "surrogateescape"),
    ],
)
def test_malformed_messages_are_dead_lettered(handler: TaskUpdateHandler, body: bytes) -> None:
    assert handler.handle(body) is Disposition.DEAD_LETTER


def test_update_for_unknown_task_is_dead_lettered(
    worker: FakeWorker, session_factory: sessionmaker[Session]
) -> None:
    assert worker.report(uuid4(), S.DONE) is Disposition.DEAD_LETTER
    assert processed_count(session_factory) == 0


def test_transient_database_failure_is_retried_not_dropped(
    tenants: TenantFactory, session_factory: sessionmaker[Session]
) -> None:
    task_id = tenants.create()["task"]["id"]  # type: ignore[index]

    def broken_session() -> Session:
        raise OperationalError("SELECT 1", {}, Exception("connection refused"))

    broken = FakeWorker(TaskUpdateHandler(broken_session))  # type: ignore[arg-type]
    message_id = uuid4()
    assert broken.report(task_id, S.DONE, message_id=message_id) is Disposition.RETRY

    # Once the database is back, the redelivered message is applied.
    healthy = FakeWorker(TaskUpdateHandler(session_factory))
    assert healthy.report(task_id, S.DONE, message_id=message_id) is Disposition.ACK
    assert snapshot(session_factory, task_id)[2] == TenantStatus.ACTIVE


def test_consumer_keeps_working_after_poison_messages(
    tenants: TenantFactory, handler: TaskUpdateHandler, worker: FakeWorker
) -> None:
    task_id = tenants.create()["task"]["id"]  # type: ignore[index]
    for poison in (b"garbage", b"{}", b"\x00\x01"):
        assert handler.handle(poison) is Disposition.DEAD_LETTER
    worker.complete(task_id)
    assert tenants.get(tenants.client.get(f"/api/v1/tasks/{task_id}").json()["tenant_id"])[
        "status"
    ] == ("active")
