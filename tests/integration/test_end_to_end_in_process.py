"""The whole pipeline in-process, without a broker:

API -> outbox -> relay -> (worker simulator's plan) -> consumer handler -> API.

The in-memory "broker" hands each published task event to the real simulator
planning code and feeds its updates to the real consumer handler, including
the duplicate and out-of-order chaos modes.
"""

import json
import random
from dataclasses import dataclass, field

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session, sessionmaker

from control_plane.messaging.consumer import Disposition, TaskUpdateHandler
from control_plane.messaging.contracts import TaskEvent
from control_plane.messaging.outbox_relay import OutboxRelay
from worker_simulator.config import SimulationConfig
from worker_simulator.simulator import plan_updates


@dataclass
class InMemoryBroker:
    handler: TaskUpdateHandler
    config: SimulationConfig
    rng: random.Random = field(default_factory=lambda: random.Random(1234))
    dispositions: list[Disposition] = field(default_factory=list)

    def publish(self, *, exchange: str, routing_key: str, body: bytes, message_id: str) -> None:
        event = TaskEvent.model_validate_json(body)
        assert message_id == str(event.id)
        for step in plan_updates(event.id, self.config, self.rng):
            self.dispositions.append(self.handler.handle(step.update.model_dump_json().encode()))

    def close(self) -> None:
        pass


def run_pipeline(session_factory: sessionmaker[Session], broker: InMemoryBroker) -> None:
    OutboxRelay(session_factory, broker).publish_pending()


@pytest.mark.parametrize(
    "config",
    [
        SimulationConfig(min_delay_ms=0, max_delay_ms=0),
        SimulationConfig(min_delay_ms=0, max_delay_ms=0, duplicate_rate=1.0),
        SimulationConfig(min_delay_ms=0, max_delay_ms=0, out_of_order_rate=1.0),
        SimulationConfig(min_delay_ms=0, max_delay_ms=0, duplicate_rate=1.0, out_of_order_rate=1.0),
    ],
    ids=["normal", "duplicates", "out-of-order", "duplicates+out-of-order"],
)
def test_lifecycle_converges_under_chaos(
    client: TestClient,
    session_factory: sessionmaker[Session],
    handler: TaskUpdateHandler,
    config: SimulationConfig,
) -> None:
    broker = InMemoryBroker(handler, config)

    tenant = client.post("/api/v1/tenants", json={"slug": "acme", "name": "A"}).json()["tenant"]
    run_pipeline(session_factory, broker)
    tenant = client.get(f"/api/v1/tenants/{tenant['id']}").json()
    assert (tenant["status"], tenant["version"]) == ("active", 2)

    client.patch(f"/api/v1/tenants/{tenant['id']}", json={"version": 2, "name": "B"})
    run_pipeline(session_factory, broker)
    tenant = client.get(f"/api/v1/tenants/{tenant['id']}").json()
    assert (tenant["status"], tenant["version"], tenant["name"]) == ("active", 4, "B")

    client.delete(f"/api/v1/tenants/{tenant['id']}")
    run_pipeline(session_factory, broker)
    tenant = client.get(f"/api/v1/tenants/{tenant['id']}").json()
    assert (tenant["status"], tenant["version"]) == ("destroyed", 6)

    assert set(broker.dispositions) == {Disposition.ACK}
    tasks = client.get("/api/v1/tasks", params={"tenant_id": tenant["id"]}).json()["items"]
    assert [t["status"] for t in tasks] == ["done", "done", "done"]


def test_fail_rate_one_fails_the_tenant_which_can_then_be_destroyed(
    client: TestClient, session_factory: sessionmaker[Session], handler: TaskUpdateHandler
) -> None:
    failing = InMemoryBroker(
        handler, SimulationConfig(fail_rate=1.0, min_delay_ms=0, max_delay_ms=0)
    )
    tenant_id = client.post("/api/v1/tenants", json={"slug": "acme", "name": "A"}).json()["tenant"][
        "id"
    ]
    run_pipeline(session_factory, failing)
    assert client.get(f"/api/v1/tenants/{tenant_id}").json()["status"] == "failed"

    healthy = InMemoryBroker(handler, SimulationConfig(min_delay_ms=0, max_delay_ms=0))
    assert client.delete(f"/api/v1/tenants/{tenant_id}").status_code == 202
    run_pipeline(session_factory, healthy)
    assert client.get(f"/api/v1/tenants/{tenant_id}").json()["status"] == "destroyed"


def test_relay_republish_after_crash_is_harmless(
    client: TestClient, handler: TaskUpdateHandler
) -> None:
    """At-least-once: the same task event delivered twice leaves state correct."""
    broker = InMemoryBroker(handler, SimulationConfig(min_delay_ms=0, max_delay_ms=0))
    task = client.post("/api/v1/tenants", json={"slug": "acme", "name": "A"}).json()["task"]
    event = json.dumps(TaskEvent.model_validate(task).model_dump(mode="json")).encode()

    broker.publish(exchange="", routing_key="task.deploy", body=event, message_id=task["id"])
    broker.publish(exchange="", routing_key="task.deploy", body=event, message_id=task["id"])

    tenant = client.get(f"/api/v1/tenants/{task['tenant_id']}").json()
    assert (tenant["status"], tenant["version"]) == ("active", 2)
