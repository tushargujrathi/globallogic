"""Initial schema: tenants, tasks, outbox and inbox.

Revision ID: 0001
Revises:
Create Date: 2026-11-14
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TENANT_STATUSES = ("provisioning", "active", "updating", "destroying", "destroyed", "failed")
TASK_TYPES = ("deploy", "update", "destroy")
TASK_STATUSES = ("accepted", "in_progress", "done", "failed")


def _in(column: str, values: tuple[str, ...]) -> str:
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


def upgrade() -> None:
    op.create_table(
        "tenants",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("slug", sa.String(28), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_tenants"),
        sa.UniqueConstraint("slug", name="uq_tenants_slug"),
        sa.CheckConstraint(_in("status", TENANT_STATUSES), name="ck_tenants_tenant_status"),
    )
    op.create_table(
        "tasks",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("type", sa.String(32), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_tasks"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], name="fk_tasks_tenant_id_tenants"),
        sa.CheckConstraint(_in("type", TASK_TYPES), name="ck_tasks_task_type"),
        sa.CheckConstraint(_in("status", TASK_STATUSES), name="ck_tasks_task_status"),
    )
    op.create_index("ix_tasks_tenant_id", "tasks", ["tenant_id"])
    op.create_table(
        "outbox_messages",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("message_id", sa.Uuid(), nullable=False),
        sa.Column("routing_key", sa.String(255), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_outbox_messages"),
        sa.UniqueConstraint("message_id", name="uq_outbox_messages_message_id"),
    )
    op.create_index(
        "ix_outbox_messages_unpublished",
        "outbox_messages",
        ["id"],
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.create_table(
        "processed_messages",
        sa.Column("message_id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("reported_status", sa.String(32), nullable=False),
        sa.Column("outcome", sa.String(16), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("message_id", name="pk_processed_messages"),
        sa.ForeignKeyConstraint(
            ["task_id"], ["tasks.id"], name="fk_processed_messages_task_id_tasks"
        ),
        sa.CheckConstraint(
            _in("reported_status", TASK_STATUSES), name="ck_processed_messages_reported_status"
        ),
    )
    op.create_index("ix_processed_messages_task_id", "processed_messages", ["task_id"])


def downgrade() -> None:
    op.drop_table("processed_messages")
    op.drop_table("outbox_messages")
    op.drop_table("tasks")
    op.drop_table("tenants")
