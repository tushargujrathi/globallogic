"""Application factory."""

from fastapi import FastAPI
from sqlalchemy.orm import Session, sessionmaker

from control_plane.api.errors import register_exception_handlers
from control_plane.api.routes import health, tasks, tenants
from control_plane.core.config import get_settings
from control_plane.db.session import create_session_factory

API_PREFIX = "/api/v1"


def create_app(session_factory: sessionmaker[Session] | None = None) -> FastAPI:
    app = FastAPI(
        title="Tenant Provisioning Control Plane",
        version="1.0.0",
        description="Creates and mutates tenants through asynchronous provisioning tasks.",
    )
    app.state.session_factory = session_factory or create_session_factory(
        get_settings().database_url
    )
    register_exception_handlers(app)
    app.include_router(health.router)
    app.include_router(tenants.router, prefix=API_PREFIX)
    app.include_router(tasks.router, prefix=API_PREFIX)
    return app
