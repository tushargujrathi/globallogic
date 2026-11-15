"""Race conditions, exercised with real concurrent transactions against PostgreSQL.

Each test releases N threads at once through a barrier; every thread uses its
own session (= its own connection and transaction), like concurrent requests.
"""

import threading
from collections import Counter
from collections.abc import Callable
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from control_plane.db.models import OutboxMessage, Task, Tenant
from control_plane.domain.enums import TaskStatus, TaskType
from control_plane.domain.errors import DomainError
from control_plane.services.tenants import TenantService
from tests.integration.conftest import TenantFactory

N = 16


def race(
    session_factory: sessionmaker[Session], action: Callable[[TenantService], object]
) -> Counter[str]:
    """Run ``action`` from N threads simultaneously; count outcomes by error code."""
    barrier = threading.Barrier(N)
    outcomes: Counter[str] = Counter()
    lock = threading.Lock()

    def run() -> None:
        with session_factory() as session:
            service = TenantService(session)
            barrier.wait()
            try:
                action(service)
                outcome = "ok"
            except DomainError as exc:
                outcome = exc.code
        with lock:
            outcomes[outcome] += 1

    threads = [threading.Thread(target=run) for _ in range(N)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return outcomes


def count(session_factory: sessionmaker[Session], model: type, *where: object) -> int:
    with session_factory() as session:
        query = select(func.count()).select_from(model).where(*where)  # type: ignore[arg-type]
        return session.scalar(query) or 0


def test_concurrent_creates_with_the_same_slug_yield_exactly_one_tenant(
    session_factory: sessionmaker[Session],
) -> None:
    outcomes = race(session_factory, lambda s: s.create(slug="contested", name="Racer"))

    assert outcomes == Counter({"ok": 1, "tenant_already_exists": N - 1})
    assert count(session_factory, Tenant) == 1
    assert count(session_factory, Task) == 1
    assert count(session_factory, OutboxMessage) == 1  # losers published nothing


def test_concurrent_patches_with_the_same_version_yield_exactly_one_update(
    tenants: TenantFactory, session_factory: sessionmaker[Session]
) -> None:
    tenant_id = UUID(str(tenants.active()["id"]))

    outcomes = race(
        session_factory, lambda s: s.update(tenant_id, expected_version=2, name="winner")
    )

    assert outcomes == Counter({"ok": 1, "tenant_version_conflict": N - 1})
    with session_factory() as session:
        tenant = session.get_one(Tenant, tenant_id)
        assert (tenant.version, tenant.name) == (3, "winner")
    assert count(session_factory, Task, Task.type == TaskType.UPDATE) == 1
    assert count(session_factory, OutboxMessage) == 2  # deploy + the single update


def test_concurrent_deletes_start_exactly_one_destroy(
    tenants: TenantFactory, session_factory: sessionmaker[Session]
) -> None:
    tenant_id = UUID(str(tenants.active()["id"]))

    outcomes = race(session_factory, lambda s: s.delete(tenant_id))

    assert outcomes == Counter({"ok": 1, "tenant_update_not_allowed": N - 1})
    assert count(session_factory, Task, Task.type == TaskType.DESTROY) == 1


def test_mixed_concurrent_mutations_keep_one_open_task(
    tenants: TenantFactory, session_factory: sessionmaker[Session]
) -> None:
    tenant_id = UUID(str(tenants.active()["id"]))
    calls = iter(range(N))
    pick = threading.Lock()

    def mutate(service: TenantService) -> object:
        with pick:
            i = next(calls)
        if i % 2:
            return service.delete(tenant_id)
        return service.update(tenant_id, expected_version=2, name=f"n{i}")

    outcomes = race(session_factory, mutate)

    assert outcomes["ok"] == 1
    open_statuses = (TaskStatus.ACCEPTED, TaskStatus.IN_PROGRESS)
    assert count(session_factory, Task, Task.status.in_(open_statuses)) == 1
    with session_factory() as session:
        assert session.get_one(Tenant, tenant_id).version == 3
