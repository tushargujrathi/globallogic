"""Request validation and response serialisation of the HTTP schemas."""

import re
from datetime import UTC, datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from pydantic import ValidationError

from control_plane.api.schemas import Page, TenantCreate, TenantOut, TenantUpdate
from control_plane.core.clock import isoformat_ms, utcnow

VALID_SLUGS = ["abc", "a-b", "a1b", "team-42", "a" * 28, "a" + "-" * 26 + "b"]
INVALID_SLUGS = [
    "ab",  # too short
    "a" * 29,  # too long
    "Abc",  # uppercase
    "1abc",  # starts with a digit
    "-abc",  # starts with a hyphen
    "abc-",  # ends with a hyphen
    "ab_c",  # underscore
    "ab c",  # space
    "",
]


@pytest.mark.parametrize("slug", VALID_SLUGS)
def test_valid_slugs(slug: str) -> None:
    assert TenantCreate(slug=slug, name="Acme").slug == slug


@pytest.mark.parametrize("slug", INVALID_SLUGS)
def test_invalid_slugs(slug: str) -> None:
    with pytest.raises(ValidationError):
        TenantCreate(slug=slug, name="Acme")


@pytest.mark.parametrize("name", ["", "   ", "x" * 256])
def test_invalid_names(name: str) -> None:
    with pytest.raises(ValidationError):
        TenantCreate(slug="acme", name=name)


def test_name_is_stripped() -> None:
    assert TenantCreate(slug="acme", name="  Acme  ").name == "Acme"


def test_create_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        TenantCreate.model_validate({"slug": "acme", "name": "Acme", "status": "active"})


def test_update_requires_version_and_a_change() -> None:
    assert TenantUpdate(version=3, name="New").name == "New"
    with pytest.raises(ValidationError):
        TenantUpdate.model_validate({"name": "New"})
    with pytest.raises(ValidationError, match="at least one mutable field"):
        TenantUpdate(version=1)
    with pytest.raises(ValidationError):
        TenantUpdate(version=0, name="New")


def test_update_rejects_immutable_fields() -> None:
    with pytest.raises(ValidationError):
        TenantUpdate.model_validate({"version": 1, "name": "x", "slug": "other"})


def test_utcnow_is_utc_with_millisecond_precision() -> None:
    now = utcnow()
    assert now.tzinfo is UTC
    assert now.microsecond % 1000 == 0


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (datetime(2026, 1, 2, 3, 4, 5, 678901, tzinfo=UTC), "2026-01-02T03:04:05.678Z"),
        (datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC), "2026-01-02T03:04:05.000Z"),
        (datetime(2026, 1, 2, 3, 4, 5), "2026-01-02T03:04:05.000Z"),  # naive = UTC
        (
            datetime(2026, 1, 2, 5, 4, 5, tzinfo=timezone(timedelta(hours=2))),
            "2026-01-02T03:04:05.000Z",
        ),
    ],
)
def test_isoformat_ms(value: datetime, expected: str) -> None:
    assert isoformat_ms(value) == expected
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z", isoformat_ms(value))


def test_page_build_converts_orm_rows() -> None:
    now = utcnow()
    row = SimpleNamespace(
        id=uuid4(),
        slug="acme",
        name="Acme",
        status="active",
        version=1,
        created_at=now,
        updated_at=now,
    )
    page = Page[TenantOut].build([row], 7, limit=1, offset=2)
    assert page.items == [TenantOut.model_validate(row)]
    assert (page.total, page.limit, page.offset) == (7, 1, 2)
