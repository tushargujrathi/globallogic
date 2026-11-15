from collections.abc import Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from control_plane.db.models import OutboxMessage


class OutboxRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add(self, message: OutboxMessage) -> None:
        self._session.add(message)

    def lock_unpublished(self, limit: int) -> Sequence[OutboxMessage]:
        """Oldest unpublished messages, row-locked.

        ``SKIP LOCKED`` lets several relay replicas run without publishing the
        same rows concurrently. Rows of uncommitted transactions are invisible,
        which is what guarantees "publish only after commit".
        """
        query = (
            select(OutboxMessage)
            .where(OutboxMessage.published_at.is_(None))
            .order_by(OutboxMessage.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return self._session.scalars(query).all()
