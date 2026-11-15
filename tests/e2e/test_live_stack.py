"""End-to-end tests against the running stack (``make up``): real API, broker, relay,
consumer and worker simulator. Run by ``make test``; skipped when E2E_API_URL is unset.
"""

import json
import os
import time
from collections import Counter
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from uuid import uuid4

import httpx
import pika
import pytest

from control_plane.core.clock import isoformat_ms, utcnow
from control_plane.messaging.topology import (
    TASK_UPDATES_EXCHANGE,
    TASK_UPDATES_QUEUE,
    dead_letter_queue,
)

API_URL = os.environ.get("E2E_API_URL")
RABBITMQ_URL = os.environ.get("E2E_RABBITMQ_URL")

pytestmark = pytest.mark.skipif(
    not (API_URL and RABBITMQ_URL), reason="E2E_API_URL / E2E_RABBITMQ_URL not set"
)

TIMEOUT_S = 30.0


@pytest.fixture(scope="module")
def api() -> Iterator[httpx.Client]:
    with httpx.Client(base_url=f"{API_URL}/api/v1", timeout=10) as client:
        yield client


@pytest.fixture(scope="module")
def broker() -> Iterator[Any]:
    connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
    channel = connection.channel()
    channel.confirm_delivery()
    yield channel
    connection.close()


def unique_slug() -> str:
    return f"e2e-{uuid4().hex[:12]}"


def wait_for(probe: Callable[[], Any], predicate: Callable[[Any], bool]) -> Any:
    deadline = time.monotonic() + TIMEOUT_S
    while True:
        value = probe()
        if predicate(value):
            return value
        if time.monotonic() > deadline:
            pytest.fail(f"timed out waiting; last value: {value}")
        time.sleep(0.2)


def tenant(api: httpx.Client, tenant_id: str) -> dict[str, Any]:
    response = api.get(f"/tenants/{tenant_id}")
    response.raise_for_status()
    body: dict[str, Any] = response.json()
    return body


def wait_status(api: httpx.Client, tenant_id: str, status: str) -> dict[str, Any]:
    result: dict[str, Any] = wait_for(
        lambda: tenant(api, tenant_id), lambda t: t["status"] == status
    )
    return result


def create_active(api: httpx.Client) -> dict[str, Any]:
    created = api.post("/tenants", json={"slug": unique_slug(), "name": "E2E"})
    assert created.status_code == 201, created.text
    return wait_status(api, created.json()["tenant"]["id"], "active")


def publish_update(broker: Any, task_id: str, status: str, message_id: str | None = None) -> None:
    message_id = message_id or str(uuid4())
    body = {
        "message_id": message_id,
        "task_id": task_id,
        "status": status,
        "occurred_at": isoformat_ms(utcnow()),
    }
    broker.basic_publish(
        TASK_UPDATES_EXCHANGE,
        f"task_update.{status}",
        json.dumps(body).encode(),
        pika.BasicProperties(message_id=message_id, delivery_mode=2),
    )


def settle() -> None:
    """Give the consumer time to drain messages we just published."""
    time.sleep(2)


# ----------------------------------------------------------------------------- happy path


def test_create_walks_the_deploy_lifecycle(api: httpx.Client) -> None:
    created = api.post("/tenants", json={"slug": unique_slug(), "name": "E2E"}).json()
    tenant_id, task_id = created["tenant"]["id"], created["task"]["id"]

    observed_task_statuses = set()

    def probe() -> dict[str, Any]:
        observed_task_statuses.add(api.get(f"/tasks/{task_id}").json()["status"])
        return tenant(api, tenant_id)

    final = wait_for(probe, lambda t: t["status"] == "active")

    assert final["version"] == 2
    assert api.get(f"/tasks/{task_id}").json()["status"] == "done"
    assert observed_task_statuses <= {"accepted", "in_progress", "done"}


def test_update_and_destroy_lifecycle(api: httpx.Client) -> None:
    active = create_active(api)

    patched = api.patch(f"/tenants/{active['id']}", json={"version": 2, "name": "Renamed"})
    assert patched.status_code == 202
    wait_status(api, active["id"], "active")

    assert api.delete(f"/tenants/{active['id']}").status_code == 202
    destroyed = wait_status(api, active["id"], "destroyed")

    assert destroyed["name"] == "Renamed"
    assert destroyed["version"] == 6
    tasks = api.get("/tasks", params={"tenant_id": active["id"]}).json()["items"]
    assert [(t["type"], t["status"]) for t in tasks] == [
        ("deploy", "done"),
        ("update", "done"),
        ("destroy", "done"),
    ]
    reuse = api.post("/tenants", json={"slug": active["slug"], "name": "again"})
    assert reuse.status_code == 409
    assert reuse.json()["error"]["code"] == "tenant_already_exists"


# ----------------------------------------------------------------------------- messaging


def test_duplicate_and_stale_updates_published_to_the_broker_change_nothing(
    api: httpx.Client, broker: Any
) -> None:
    active = create_active(api)
    deploy_task = api.get("/tasks", params={"tenant_id": active["id"]}).json()["items"][0]

    message_id = str(uuid4())
    for _ in range(3):  # duplicate deliveries of one update
        publish_update(broker, deploy_task["id"], "in_progress", message_id)
    publish_update(broker, deploy_task["id"], "failed")  # update for a terminal task
    publish_update(broker, deploy_task["id"], "in_progress")  # out of order
    settle()

    assert tenant(api, active["id"]) == active
    assert api.get(f"/tasks/{deploy_task['id']}").json()["status"] == "done"


def test_poison_messages_are_dead_lettered_and_do_not_stop_the_consumer(
    api: httpx.Client, broker: Any
) -> None:
    dlq = dead_letter_queue(TASK_UPDATES_QUEUE)
    before = broker.queue_declare(dlq, passive=True).method.message_count

    broker.basic_publish(TASK_UPDATES_EXCHANGE, "task_update.done", b"this is not json")
    publish_update(broker, str(uuid4()), "done")  # unknown task
    settle()

    after = broker.queue_declare(dlq, passive=True).method.message_count
    assert after >= before + 2
    create_active(api)  # consumer still alive and processing


# ----------------------------------------------------------------------------- races over HTTP


def test_concurrent_http_patches_with_the_same_version(api: httpx.Client) -> None:
    active = create_active(api)

    def patch(i: int) -> httpx.Response:
        with httpx.Client(base_url=f"{API_URL}/api/v1", timeout=10) as client:
            return client.patch(f"/tenants/{active['id']}", json={"version": 2, "name": f"n{i}"})

    with ThreadPoolExecutor(max_workers=20) as pool:
        responses = list(pool.map(patch, range(20)))

    outcomes = Counter(
        r.status_code if r.status_code == 202 else r.json()["error"]["code"] for r in responses
    )
    assert outcomes == Counter({202: 1, "tenant_version_conflict": 19})


def test_concurrent_http_creates_with_the_same_slug() -> None:
    slug = unique_slug()

    def create(_: int) -> httpx.Response:
        with httpx.Client(base_url=f"{API_URL}/api/v1", timeout=10) as client:
            return client.post("/tenants", json={"slug": slug, "name": "racer"})

    with ThreadPoolExecutor(max_workers=20) as pool:
        responses = list(pool.map(create, range(20)))

    outcomes = Counter(
        r.status_code if r.status_code == 201 else r.json()["error"]["code"] for r in responses
    )
    assert outcomes == Counter({201: 1, "tenant_already_exists": 19})
