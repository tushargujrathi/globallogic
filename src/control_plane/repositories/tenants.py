from collections.abc import Sequence
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from control_plane.db.models import Tenant
from control_plane.domain.enums import TenantStatus
from control_plane.repositories.pagination import paginate


class TenantRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add(self, tenant: Tenant) -> None:
        self._session.add(tenant)

    def get(self, tenant_id: UUID, *, for_update: bool = False) -> Tenant | None:
        """Load a tenant; ``for_update`` takes a row lock until the transaction ends."""
        return self._session.get(
            Tenant, tenant_id, with_for_update=for_update, populate_existing=for_update
        )

    def list(
        self, *, status: TenantStatus | None, limit: int, offset: int
    ) -> tuple[Sequence[Tenant], int]:
        query = select(Tenant)
        if status is not None:
            query = query.where(Tenant.status == status)
        return paginate(
            self._session,
            query,
            order_by=(Tenant.created_at, Tenant.id),
            limit=limit,
            offset=offset,
        )
