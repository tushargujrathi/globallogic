"""ORM models.

* ``tenants`` / ``tasks``: the domain state.
* ``outbox_messages``: transactional outbox. Written in the same transaction as the
  task it announces; a relay publishes it only after that transaction committed.
* ``processed_messages``: inbox of worker updates already handled, keyed by the
  update's idempotency key.
"""

import enum
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    DateTime,
    Enum,
    ForeignKey,
    Identity,
    Index,
    MetaData,
    String,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from control_plane.domain.enums import TaskStatus, TaskType, TenantStatus
from control_plane.domain.rules import NAME_MAX_LENGTH, SLUG_MAX_LENGTH

NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_name)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


def _str_enum(enum_cls: type[enum.StrEnum], name: str) -> Enum:
    """Store enums as their lowercase values in a VARCHAR guarded by a CHECK constraint."""
    return Enum(
        enum_cls,
        name=name,
        native_enum=False,
        create_constraint=True,
        length=32,
        values_callable=lambda members: [member.value for member in members],
    )


class Tenant(Base):
    __tablename__ = "tenants"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    # Unique across *all* tenants, destroyed ones included (rows are never deleted).
    slug: Mapped[str] = mapped_column(String(SLUG_MAX_LENGTH), unique=True)
    name: Mapped[str] = mapped_column(String(NAME_MAX_LENGTH))
    status: Mapped[TenantStatus] = mapped_column(_str_enum(TenantStatus, "tenant_status"))
    version: Mapped[int] = mapped_column()
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    # SQLAlchemy increments `version` on every flushed UPDATE and adds
    # `WHERE version = <loaded>`; a lost race raises StaleDataError.
    __mapper_args__ = {"version_id_col": version}  # noqa: RUF012


class Task(Base):
    __tablename__ = "tasks"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), index=True)
    type: Mapped[TaskType] = mapped_column(_str_enum(TaskType, "task_type"))
    status: Mapped[TaskStatus] = mapped_column(_str_enum(TaskStatus, "task_status"))
    error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class OutboxMessage(Base):
    __tablename__ = "outbox_messages"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    message_id: Mapped[uuid.UUID] = mapped_column(unique=True)
    routing_key: Mapped[str] = mapped_column(String(255))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        Index(
            "ix_outbox_messages_unpublished",
            "id",
            postgresql_where=text("published_at IS NULL"),
        ),
    )


class ProcessedMessage(Base):
    __tablename__ = "processed_messages"

    message_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    task_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tasks.id"), index=True)
    reported_status: Mapped[TaskStatus] = mapped_column(_str_enum(TaskStatus, "reported_status"))
    outcome: Mapped[str] = mapped_column(String(16))  # "applied" | "stale", for auditing
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
