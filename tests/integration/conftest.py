"""Fixtures for tests against a real PostgreSQL (``TEST_DATABASE_URL``).

The schema is built by running the Alembic migrations, so the migrations are
tested too. Tables are truncated before every test.
"""

import os
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from control_plane.api.app import create_app
from control_plane.core.clock import utcnow
from control_plane.domain.enums import TaskStatus
from control_plane.messaging.consumer import Disposition, TaskUpdateHandler
from control_plane.messaging.contracts import TaskUpdate

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def engine() -> Iterator[Engine]:
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is not set")
    engine = create_engine(url)
    with engine.begin() as connection:
        connection.execute(text("DROP SCHEMA public CASCADE; CREATE SCHEMA public"))
    config = Config(str(ROOT / "alembic.ini"))
    config.attributes["database_url"] = url
    config.attributes["configure_logger"] = False
    command.upgrade(config, "head")
    yield engine
    engine.dispose()


@pytest.fixture(autouse=True)
def _clean_database(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            text("TRUNCATE processed_messages, outbox_messages, tasks, tenants RESTART IDENTITY")
        )


@pytest.fixture
def session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(engine, expire_on_commit=False)


@pytest.fixture
def client(session_factory: sessionmaker[Session]) -> Iterator[TestClient]:
    with TestClient(create_app(session_factory), raise_server_exceptions=False) as test_client:
        yield test_client


@dataclass
class FakeWorker:
    """Plays the worker's side of the protocol by feeding updates to the real handler."""

    handler: TaskUpdateHandler
    sent: list[TaskUpdate] = field(default_factory=list)

    def report(
        self,
        task_id: UUID | str,
        status: TaskStatus,
        *,
        message_id: UUID | None = None,
        error: str | None = None,
    ) -> Disposition:
        update = TaskUpdate(
            message_id=message_id or uuid4(),
            task_id=UUID(str(task_id)),
            status=status,
            occurred_at=utcnow(),
            error=error,
        )
        self.sent.append(update)
        return self.handler.handle(update.model_dump_json().encode())

    def complete(self, task_id: UUID | str, outcome: TaskStatus = TaskStatus.DONE) -> None:
        assert self.report(task_id, TaskStatus.IN_PROGRESS) is Disposition.ACK
        assert self.report(task_id, outcome) is Disposition.ACK


@pytest.fixture
def handler(session_factory: sessionmaker[Session]) -> TaskUpdateHandler:
    return TaskUpdateHandler(session_factory)


@pytest.fixture
def worker(handler: TaskUpdateHandler) -> FakeWorker:
    return FakeWorker(handler)


@dataclass
class TenantFactory:
    """Drives tenants into a given status through the public API and the worker protocol."""

    client: TestClient
    worker: FakeWorker

    def create(self, slug: str = "acme", name: str = "Acme") -> dict[str, object]:
        response = self.client.post("/api/v1/tenants", json={"slug": slug, "name": name})
        assert response.status_code == 201, response.text
        body: dict[str, object] = response.json()
        return body

    def active(self, slug: str = "acme") -> dict[str, object]:
        created = self.create(slug)
        self.worker.complete(created["task"]["id"])  # type: ignore[index]
        return self.get(created["tenant"]["id"])  # type: ignore[index]

    def failed(self, slug: str = "acme") -> dict[str, object]:
        created = self.create(slug)
        self.worker.complete(created["task"]["id"], TaskStatus.FAILED)  # type: ignore[index]
        return self.get(created["tenant"]["id"])  # type: ignore[index]

    def get(self, tenant_id: object) -> dict[str, object]:
        response = self.client.get(f"/api/v1/tenants/{tenant_id}")
        assert response.status_code == 200, response.text
        body: dict[str, object] = response.json()
        return body


@pytest.fixture
def tenants(client: TestClient, worker: FakeWorker) -> TenantFactory:
    return TenantFactory(client, worker)
