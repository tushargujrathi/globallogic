"""Time helpers. All timestamps are UTC, millisecond precision, ISO 8601 with a ``Z`` suffix."""

from datetime import UTC, datetime
from typing import Annotated

from pydantic import PlainSerializer


def utcnow() -> datetime:
    """Current UTC time truncated to milliseconds (the precision we expose)."""
    now = datetime.now(UTC)
    return now.replace(microsecond=now.microsecond // 1000 * 1000)


def isoformat_ms(value: datetime) -> str:
    """Render ``value`` as e.g. ``2026-01-31T12:00:00.123Z``. Naive values are treated as UTC."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


Timestamp = Annotated[datetime, PlainSerializer(isoformat_ms, return_type=str, when_used="json")]
"""A datetime that serialises to JSON in the contract format."""
