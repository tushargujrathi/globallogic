"""Offset pagination shared by the list queries."""

from collections.abc import Sequence
from typing import Any

from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session


def paginate[T](
    session: Session, query: Select[T], *, order_by: Sequence[Any], limit: int, offset: int
) -> tuple[Sequence[T], int]:
    """Return one page of ``query`` and the total number of matching rows."""
    total = session.scalar(select(func.count()).select_from(query.order_by(None).subquery()))
    items = session.scalars(query.order_by(*order_by).limit(limit).offset(offset)).all()
    return items, total or 0
