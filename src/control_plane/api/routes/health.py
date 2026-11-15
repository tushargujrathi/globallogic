from fastapi import APIRouter
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from control_plane.api.dependencies import SessionDep
from control_plane.api.errors import error_response

router = APIRouter(tags=["health"])


@router.get("/health", summary="Liveness and database readiness")
def health(session: SessionDep) -> JSONResponse:
    try:
        session.execute(text("SELECT 1"))
    except SQLAlchemyError:
        return error_response(503, "database_unavailable", "The database is not reachable.")
    return JSONResponse({"status": "ok"})
