"""Tenant endpoints: CRUD, state guards, optimistic locking, error contract."""

import re
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from control_plane.db.models import OutboxMessage, Task
from control_plane.domain.enums import TaskStatus
from tests.integration.conftest import FakeWorker, TenantFactory

TIMESTAMP = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$")
TENANTS = "/api/v1/tenants"


def assert_error(response: object, status: int, code: str) -> dict[str, object]:
    assert response.status_code == status, response.text  # type: ignore[attr-defined]
    body = response.json()  # type: ignore[attr-defined]
    assert set(body) == {"error"}
    assert body["error"]["code"] == code
    assert body["error"]["message"]
    error: dict[str, object] = body["error"]
    return error


def outbox(session_factory: sessionmaker[Session]) -> list[OutboxMessage]:
    with session_factory() as session:
        return list(session.scalars(select(OutboxMessage).order_by(OutboxMessage.id)))


# ----------------------------------------------------------------------------- create


def test_create_tenant(client: TestClient) -> None:
    response = client.post(TENANTS, json={"slug": "acme", "name": "Acme Corp"})

    assert response.status_code == 201
    body = response.json()
    tenant, task = body["tenant"], body["task"]
    assert response.headers["location"].endswith(f"{TENANTS}/{tenant['id']}")
    assert tenant["slug"] == "acme"
    assert tenant["name"] == "Acme Corp"
    assert tenant["status"] == "provisioning"
    assert tenant["version"] == 1
    assert TIMESTAMP.match(tenant["created_at"])
    assert TIMESTAMP.match(tenant["updated_at"])
    assert task["type"] == "deploy"
    assert task["status"] == "accepted"
    assert task["tenant_id"] == tenant["id"]
    assert TIMESTAMP.match(task["created_at"])


def test_create_writes_exactly_one_outbox_event_carrying_the_task(
    client: TestClient, session_factory: sessionmaker[Session]
) -> None:
    task = client.post(TENANTS, json={"slug": "acme", "name": "Acme"}).json()["task"]

    (message,) = outbox(session_factory)
    assert str(message.message_id) == task["id"]
    assert message.routing_key == "task.deploy"
    assert message.published_at is None
    assert message.payload == {
        "id": task["id"],
        "type": "deploy",
        "tenant_id": task["tenant_id"],
        "status": "accepted",
        "created_at": task["created_at"],
    }


@pytest.mark.parametrize("slug", ["ab", "Acme", "1acme", "acme-", "a" * 29, "ac_me"])
def test_create_rejects_invalid_slug(client: TestClient, slug: str) -> None:
    error = assert_error(
        client.post(TENANTS, json={"slug": slug, "name": "Acme"}), 422, "validation_error"
    )
    assert error["details"][0]["field"] == "body.slug"  # type: ignore[index]


@pytest.mark.parametrize("body", [{"slug": "acme"}, {"slug": "acme", "name": " "}, {}])
def test_create_rejects_invalid_body(client: TestClient, body: dict[str, str]) -> None:
    assert_error(client.post(TENANTS, json=body), 422, "validation_error")


def test_create_rejects_malformed_json(client: TestClient) -> None:
    response = client.post(
        TENANTS, content=b"{not json", headers={"content-type": "application/json"}
    )
    assert_error(response, 422, "validation_error")


def test_duplicate_slug_is_rejected_without_side_effects(
    client: TestClient, session_factory: sessionmaker[Session]
) -> None:
    client.post(TENANTS, json={"slug": "acme", "name": "Acme"})

    error = assert_error(
        client.post(TENANTS, json={"slug": "acme", "name": "Other"}), 409, "tenant_already_exists"
    )
    assert error["details"] == {"slug": "acme"}
    assert len(outbox(session_factory)) == 1  # the rolled-back create emitted nothing


def test_slug_stays_reserved_after_the_tenant_is_destroyed(
    client: TestClient, tenants: TenantFactory, worker: FakeWorker
) -> None:
    tenant = tenants.active("acme")
    task = client.delete(f"{TENANTS}/{tenant['id']}").json()["task"]
    worker.complete(task["id"])
    assert tenants.get(tenant["id"])["status"] == "destroyed"

    assert_error(
        client.post(TENANTS, json={"slug": "acme", "name": "Again"}), 409, "tenant_already_exists"
    )


# ----------------------------------------------------------------------------- read


def test_get_tenant(client: TestClient, tenants: TenantFactory) -> None:
    created = tenants.create()["tenant"]
    assert client.get(f"{TENANTS}/{created['id']}").json() == created  # type: ignore[index]


def test_get_unknown_tenant(client: TestClient) -> None:
    tenant_id = uuid4()
    error = assert_error(client.get(f"{TENANTS}/{tenant_id}"), 404, "tenant_not_found")
    assert error["details"] == {"tenant_id": str(tenant_id)}


def test_get_with_malformed_id(client: TestClient) -> None:
    assert_error(client.get(f"{TENANTS}/not-a-uuid"), 422, "validation_error")


def test_list_tenants_paginates_in_creation_order(
    client: TestClient, tenants: TenantFactory
) -> None:
    slugs = [f"tenant-{i}" for i in range(5)]
    for slug in slugs:
        tenants.create(slug)

    first = client.get(TENANTS, params={"limit": 2}).json()
    second = client.get(TENANTS, params={"limit": 2, "offset": 2}).json()
    last = client.get(TENANTS, params={"limit": 2, "offset": 4}).json()

    assert (first["total"], first["limit"], first["offset"]) == (5, 2, 0)
    seen = [t["slug"] for page in (first, second, last) for t in page["items"]]
    assert seen == slugs


def test_list_tenants_filters_by_status(client: TestClient, tenants: TenantFactory) -> None:
    tenants.create("pending")
    active = tenants.active("live")

    body = client.get(TENANTS, params={"status": "active"}).json()

    assert body["total"] == 1
    assert [t["id"] for t in body["items"]] == [active["id"]]


@pytest.mark.parametrize(
    "params", [{"limit": 0}, {"limit": 101}, {"offset": -1}, {"status": "bogus"}]
)
def test_list_tenants_rejects_invalid_query(client: TestClient, params: dict[str, object]) -> None:
    assert_error(client.get(TENANTS, params=params), 422, "validation_error")


# ----------------------------------------------------------------------------- update


def test_update_active_tenant(
    client: TestClient, tenants: TenantFactory, session_factory: sessionmaker[Session]
) -> None:
    tenant = tenants.active()
    assert tenant["version"] == 2  # created (1) -> deploy done (2)

    response = client.patch(
        f"{TENANTS}/{tenant['id']}", json={"version": 2, "name": "Acme Renamed"}
    )

    assert response.status_code == 202
    body = response.json()
    assert body["tenant"]["status"] == "updating"
    assert body["tenant"]["name"] == "Acme Renamed"
    assert body["tenant"]["version"] == 3
    assert body["tenant"]["slug"] == "acme"
    assert body["task"]["type"] == "update"
    assert body["task"]["status"] == "accepted"
    assert [m.routing_key for m in outbox(session_factory)] == ["task.deploy", "task.update"]


def test_update_with_stale_version_conflicts(client: TestClient, tenants: TenantFactory) -> None:
    tenant = tenants.active()

    error = assert_error(
        client.patch(f"{TENANTS}/{tenant['id']}", json={"version": 1, "name": "x"}),
        409,
        "tenant_version_conflict",
    )
    assert error["details"] == {
        "tenant_id": tenant["id"],
        "expected_version": 1,
        "current_version": 2,
    }
    assert tenants.get(tenant["id"]) == tenant  # unchanged


@pytest.mark.parametrize("setup", ["create", "failed", "updating", "destroying", "destroyed"])
def test_update_is_only_allowed_from_active(
    client: TestClient, tenants: TenantFactory, worker: FakeWorker, setup: str
) -> None:
    if setup == "create":
        tenant = tenants.create()["tenant"]
    elif setup == "failed":
        tenant = tenants.failed()
    else:
        tenant = tenants.active()
        if setup == "updating":
            client.patch(f"{TENANTS}/{tenant['id']}", json={"version": 2, "name": "y"})
        else:
            task = client.delete(f"{TENANTS}/{tenant['id']}").json()["task"]
            if setup == "destroyed":
                worker.complete(task["id"])
        tenant = tenants.get(tenant["id"])

    response = client.patch(
        f"{TENANTS}/{tenant['id']}",  # type: ignore[index]
        json={"version": tenant["version"], "name": "z"},  # type: ignore[index]
    )
    error = assert_error(response, 409, "tenant_update_not_allowed")
    assert error["details"]["operation"] == "update"  # type: ignore[index]


def test_update_unknown_tenant(client: TestClient) -> None:
    assert_error(
        client.patch(f"{TENANTS}/{uuid4()}", json={"version": 1, "name": "x"}),
        404,
        "tenant_not_found",
    )


@pytest.mark.parametrize(
    "body",
    [
        {"name": "x"},  # no version
        {"version": 1},  # nothing to change
        {"version": 1, "slug": "other"},  # slug is immutable
        {"version": 1, "status": "active"},  # status is server-controlled
        {"version": "one", "name": "x"},
    ],
)
def test_update_rejects_invalid_body(
    client: TestClient, tenants: TenantFactory, body: dict[str, object]
) -> None:
    tenant = tenants.active()
    assert_error(client.patch(f"{TENANTS}/{tenant['id']}", json=body), 422, "validation_error")


# ----------------------------------------------------------------------------- delete


@pytest.mark.parametrize("setup", ["active", "failed"])
def test_delete_from_active_or_failed(
    client: TestClient, tenants: TenantFactory, setup: str
) -> None:
    tenant = tenants.active() if setup == "active" else tenants.failed()

    response = client.delete(f"{TENANTS}/{tenant['id']}")

    assert response.status_code == 202
    body = response.json()
    assert body["tenant"]["status"] == "destroying"
    assert body["tenant"]["version"] == tenant["version"] + 1  # type: ignore[operator]
    assert body["task"]["type"] == "destroy"


@pytest.mark.parametrize("setup", ["create", "updating", "destroying", "destroyed"])
def test_delete_is_rejected_from_other_states(
    client: TestClient, tenants: TenantFactory, worker: FakeWorker, setup: str
) -> None:
    if setup == "create":
        tenant_id = tenants.create()["tenant"]["id"]  # type: ignore[index]
    else:
        tenant_id = tenants.active()["id"]
        if setup == "updating":
            client.patch(f"{TENANTS}/{tenant_id}", json={"version": 2, "name": "y"})
        else:
            task = client.delete(f"{TENANTS}/{tenant_id}").json()["task"]
            if setup == "destroyed":
                worker.complete(task["id"])

    error = assert_error(client.delete(f"{TENANTS}/{tenant_id}"), 409, "tenant_update_not_allowed")
    assert error["details"]["operation"] == "delete"  # type: ignore[index]


def test_delete_unknown_tenant(client: TestClient) -> None:
    assert_error(client.delete(f"{TENANTS}/{uuid4()}"), 404, "tenant_not_found")


# ----------------------------------------------------------------------------- lifecycle


def test_full_lifecycle_and_version_history(
    client: TestClient, tenants: TenantFactory, worker: FakeWorker
) -> None:
    created = tenants.create()
    tenant_id = created["tenant"]["id"]  # type: ignore[index]
    history = [(created["tenant"]["status"], created["tenant"]["version"])]  # type: ignore[index]

    worker.report(created["task"]["id"], TaskStatus.IN_PROGRESS)  # type: ignore[index]
    t = tenants.get(tenant_id)
    history.append((t["status"], t["version"]))  # in_progress does not touch the tenant
    worker.report(created["task"]["id"], TaskStatus.DONE)  # type: ignore[index]
    t = tenants.get(tenant_id)
    history.append((t["status"], t["version"]))

    update = client.patch(f"{TENANTS}/{tenant_id}", json={"version": 2, "name": "B"}).json()
    history.append((update["tenant"]["status"], update["tenant"]["version"]))
    worker.complete(update["task"]["id"])
    t = tenants.get(tenant_id)
    history.append((t["status"], t["version"]))

    delete = client.delete(f"{TENANTS}/{tenant_id}").json()
    history.append((delete["tenant"]["status"], delete["tenant"]["version"]))
    worker.complete(delete["task"]["id"])
    t = tenants.get(tenant_id)
    history.append((t["status"], t["version"]))

    assert history == [
        ("provisioning", 1),
        ("provisioning", 1),
        ("active", 2),
        ("updating", 3),
        ("active", 4),
        ("destroying", 5),
        ("destroyed", 6),
    ]
    tasks = client.get("/api/v1/tasks", params={"tenant_id": tenant_id}).json()["items"]
    assert [(t["type"], t["status"]) for t in tasks] == [
        ("deploy", "done"),
        ("update", "done"),
        ("destroy", "done"),
    ]


def test_at_most_one_open_task_per_tenant(
    client: TestClient, tenants: TenantFactory, session_factory: sessionmaker[Session]
) -> None:
    tenant = tenants.active()
    client.patch(f"{TENANTS}/{tenant['id']}", json={"version": 2, "name": "a"})
    client.patch(f"{TENANTS}/{tenant['id']}", json={"version": 3, "name": "b"})
    client.delete(f"{TENANTS}/{tenant['id']}")

    with session_factory() as session:
        open_tasks = session.scalars(
            select(Task).where(
                Task.status.in_([TaskStatus.ACCEPTED, TaskStatus.IN_PROGRESS]),
            )
        ).all()
    assert len(open_tasks) == 1


# ----------------------------------------------------------------------------- misc


def test_unknown_route_returns_structured_error(client: TestClient) -> None:
    assert_error(client.get("/api/v1/nope"), 404, "resource_not_found")


def test_unsupported_method_returns_structured_error(client: TestClient) -> None:
    assert_error(client.put(f"{TENANTS}/{uuid4()}", json={}), 405, "method_not_allowed")


def test_health(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_openapi_documents_the_error_body(client: TestClient) -> None:
    spec = client.get("/openapi.json").json()
    responses = spec["paths"]["/api/v1/tenants/{tenant_id}"]["patch"]["responses"]
    assert {"202", "404", "409", "422"} <= set(responses)
