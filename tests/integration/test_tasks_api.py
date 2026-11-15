"""Task endpoints: read-only list/get with filters."""

from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from control_plane.domain.enums import TaskStatus
from tests.integration.conftest import FakeWorker, TenantFactory

TASKS = "/api/v1/tasks"


def test_get_task(client: TestClient, tenants: TenantFactory) -> None:
    task = tenants.create()["task"]
    response = client.get(f"{TASKS}/{task['id']}")  # type: ignore[index]
    assert response.status_code == 200
    assert response.json() == task


def test_get_unknown_task(client: TestClient) -> None:
    task_id = uuid4()
    response = client.get(f"{TASKS}/{task_id}")
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "task_not_found"
    assert response.json()["error"]["details"] == {"task_id": str(task_id)}


def test_task_reflects_worker_progress(
    client: TestClient, tenants: TenantFactory, worker: FakeWorker
) -> None:
    task_id = tenants.create()["task"]["id"]  # type: ignore[index]
    seen = [client.get(f"{TASKS}/{task_id}").json()["status"]]
    worker.report(task_id, TaskStatus.IN_PROGRESS)
    seen.append(client.get(f"{TASKS}/{task_id}").json()["status"])
    worker.report(task_id, TaskStatus.FAILED, error="disk full")
    final = client.get(f"{TASKS}/{task_id}").json()
    seen.append(final["status"])

    assert seen == ["accepted", "in_progress", "failed"]
    assert final["error"] == "disk full"


def test_list_tasks_with_filters(client: TestClient, tenants: TenantFactory) -> None:
    first = tenants.active("first")
    second = tenants.create("second")["tenant"]
    client.patch(f"/api/v1/tenants/{first['id']}", json={"version": 2, "name": "x"})

    def query(**params: object) -> list[tuple[str, str]]:
        body = client.get(TASKS, params=params).json()
        assert body["total"] == len(body["items"])
        return [(t["type"], t["status"]) for t in body["items"]]

    assert query() == [("deploy", "done"), ("deploy", "accepted"), ("update", "accepted")]
    assert query(tenant_id=first["id"]) == [("deploy", "done"), ("update", "accepted")]
    assert query(tenant_id=second["id"]) == [("deploy", "accepted")]  # type: ignore[index]
    assert query(status="accepted") == [("deploy", "accepted"), ("update", "accepted")]
    assert query(type="update") == [("update", "accepted")]
    assert query(tenant_id=first["id"], type="deploy", status="done") == [("deploy", "done")]
    assert query(tenant_id=str(uuid4())) == []


def test_list_tasks_paginates(client: TestClient, tenants: TenantFactory) -> None:
    for i in range(3):
        tenants.create(f"tenant-{i}")
    body = client.get(TASKS, params={"limit": 2, "offset": 1}).json()
    assert (body["total"], len(body["items"]), body["limit"], body["offset"]) == (3, 2, 2, 1)


@pytest.mark.parametrize(
    "params", [{"status": "bogus"}, {"type": "bogus"}, {"tenant_id": "nope"}, {"limit": 0}]
)
def test_list_tasks_rejects_invalid_query(client: TestClient, params: dict[str, object]) -> None:
    response = client.get(TASKS, params=params)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


@pytest.mark.parametrize(
    ("method", "path"),
    [("post", TASKS), ("patch", f"{TASKS}/{uuid4()}"), ("delete", f"{TASKS}/{uuid4()}")],
)
def test_tasks_are_read_only(client: TestClient, method: str, path: str) -> None:
    response = client.request(method.upper(), path, json={})
    assert response.status_code == 405
    assert response.json()["error"]["code"] == "method_not_allowed"
