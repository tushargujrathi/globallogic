from uuid import UUID

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from control_plane.core.clock import utcnow
from control_plane.db.models import ProcessedMessage
from control_plane.domain.enums import TaskStatus


class InboxRepository:
    """Idempotency store for inbound messages."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def record(
        self, *, message_id: UUID, task_id: UUID, reported_status: TaskStatus, outcome: str
    ) -> bool:
        """Record a message as processed. Returns ``False`` if it was already recorded.

        ``ON CONFLICT DO NOTHING`` makes this atomic: a concurrent insert of the
        same id waits for the other transaction, then inserts nothing.
        """
        statement = (
            insert(ProcessedMessage)
            .values(
                message_id=message_id,
                task_id=task_id,
                reported_status=reported_status,
                outcome=outcome,
                processed_at=utcnow(),
            )
            .on_conflict_do_nothing(index_elements=[ProcessedMessage.message_id])
            .returning(ProcessedMessage.message_id)
        )
        return self._session.execute(statement).scalar_one_or_none() is not None
