"""Outbound messaging: publish exactly the committed tasks, after commit, at least once."""

import json
from dataclasses import dataclass, field
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from control_plane.core.clock import utcnow
from control_plane.db.models import OutboxMessage
from control_plane.messaging.contracts import TaskEvent
from control_plane.messaging.outbox_relay import OutboxRelay
from control_plane.messaging.topology import TASKS_EXCHANGE
from tests.integration.conftest import TenantFactory


@dataclass
class RecordingPublisher:
    fail_after: int | None = None
    published: list[dict[str, object]] = field(default_factory=list)

    def publish(self, *, exchange: str, routing_key: str, body: bytes, message_id: str) -> None:
        if self.fail_after is not None and len(self.published) >= self.fail_after:
            raise ConnectionError("broker went away")
        self.published.append(
            {
                "exchange": exchange,
                "routing_key": routing_key,
                "body": json.loads(body),
                "message_id": message_id,
            }
        )

    def close(self) -> None:
        pass


def relay(session_factory: sessionmaker[Session], publisher: RecordingPublisher) -> OutboxRelay:
    return OutboxRelay(session_factory, publisher, batch_size=10)


def test_publishes_each_committed_task_once(
    tenants: TenantFactory, session_factory: sessionmaker[Session]
) -> None:
    tasks = [tenants.create(f"tenant-{i}")["task"] for i in range(3)]
    publisher = RecordingPublisher()

    assert relay(session_factory, publisher).publish_pending() == 3
    assert relay(session_factory, publisher).publish_pending() == 0

    assert [m["message_id"] for m in publisher.published] == [t["id"] for t in tasks]  # type: ignore[index]
    first = publisher.published[0]
    assert first["exchange"] == TASKS_EXCHANGE
    assert first["routing_key"] == "task.deploy"
    event = TaskEvent.model_validate(first["body"])
    assert str(event.id) == tasks[0]["id"]  # type: ignore[index]
    assert (event.type, event.status) == ("deploy", "accepted")
    assert str(event.tenant_id) == tasks[0]["tenant_id"]  # type: ignore[index]
    with session_factory() as session:
        assert all(m.published_at for m in session.scalars(select(OutboxMessage)))


def test_uncommitted_writes_are_never_published(session_factory: sessionmaker[Session]) -> None:
    publisher = RecordingPublisher()
    with session_factory() as writer:
        writer.add(
            OutboxMessage(
                message_id=uuid4(), routing_key="task.deploy", payload={}, created_at=utcnow()
            )
        )
        writer.flush()  # written, but the transaction is still open

        assert relay(session_factory, publisher).publish_pending() == 0
        writer.rollback()

    assert relay(session_factory, publisher).publish_pending() == 0
    assert publisher.published == []


def test_failed_create_publishes_nothing(
    tenants: TenantFactory, session_factory: sessionmaker[Session]
) -> None:
    tenants.create("taken")
    tenants.client.post("/api/v1/tenants", json={"slug": "taken", "name": "dup"})
    tenants.client.patch(f"/api/v1/tenants/{uuid4()}", json={"version": 1, "name": "x"})

    publisher = RecordingPublisher()
    assert relay(session_factory, publisher).publish_pending() == 1


def test_broker_failure_keeps_confirmed_marks_and_retries_the_rest(
    tenants: TenantFactory, session_factory: sessionmaker[Session]
) -> None:
    for i in range(3):
        tenants.create(f"tenant-{i}")
    flaky = RecordingPublisher(fail_after=1)

    with pytest.raises(ConnectionError):
        relay(session_factory, flaky).publish_pending()

    recovered = RecordingPublisher()
    assert relay(session_factory, recovered).publish_pending() == 2
    published_ids = {m["message_id"] for m in flaky.published + recovered.published}
    assert len(published_ids) == 3


def test_rows_locked_by_one_relay_are_skipped_by_another(
    tenants: TenantFactory, session_factory: sessionmaker[Session]
) -> None:
    tenants.create()
    with session_factory() as holding:
        # Relay A is mid-batch, holding the row lock.
        locked = holding.scalars(select(OutboxMessage).with_for_update()).all()
        assert len(locked) == 1
        assert relay(session_factory, RecordingPublisher()).publish_pending() == 0  # relay B
    assert relay(session_factory, RecordingPublisher()).publish_pending() == 1


def test_messages_are_published_in_commit_order(session_factory: sessionmaker[Session]) -> None:
    with session_factory() as session:
        for i in range(3):
            session.add(
                OutboxMessage(
                    message_id=uuid4(),
                    routing_key=f"task.k{i}",
                    payload={"n": i},
                    created_at=utcnow(),
                )
            )
        session.commit()
    publisher = RecordingPublisher()
    relay(session_factory, publisher).publish_pending()
    assert [m["body"] for m in publisher.published] == [{"n": 0}, {"n": 1}, {"n": 2}]
