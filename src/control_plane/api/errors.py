"""Every error leaves the API as ``{"error": {"code", "message", "details"}}``.

=========================== ======
code                        status
=========================== ======
validation_error            422
tenant_not_found            404
task_not_found              404
tenant_already_exists       409
tenant_update_not_allowed   409
tenant_version_conflict     409
resource_not_found          404  (unknown route)
method_not_allowed          405
internal_error              500
=========================== ======
"""

import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from control_plane.api.schemas import Error, ErrorResponse
from control_plane.domain.errors import (
    DomainError,
    TaskNotFoundError,
    TenantAlreadyExistsError,
    TenantNotFoundError,
    TenantUpdateNotAllowedError,
    TenantVersionConflictError,
)

logger = logging.getLogger(__name__)

DOMAIN_ERROR_STATUS: dict[type[DomainError], int] = {
    TenantNotFoundError: 404,
    TaskNotFoundError: 404,
    TenantAlreadyExistsError: 409,
    TenantUpdateNotAllowedError: 409,
    TenantVersionConflictError: 409,
}

_HTTP_ERROR_CODES = {404: "resource_not_found", 405: "method_not_allowed"}


def error_response(status: int, code: str, message: str, details: Any = None) -> JSONResponse:
    body = ErrorResponse(error=Error(code=code, message=message, details=details))
    return JSONResponse(status_code=status, content=body.model_dump(mode="json"))


def error_responses(*statuses: int) -> dict[int | str, dict[str, Any]]:
    """OpenAPI ``responses=`` entry documenting the error body for ``statuses``."""
    return {status: {"model": ErrorResponse} for status in statuses}


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(DomainError)
    async def _domain_error(_: Request, exc: DomainError) -> JSONResponse:
        status = DOMAIN_ERROR_STATUS.get(type(exc), 400)
        return error_response(status, exc.code, exc.message, exc.details)

    @app.exception_handler(RequestValidationError)
    async def _validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
        details = [
            {
                "field": ".".join(str(part) for part in error["loc"]),
                "message": error["msg"],
                "type": error["type"],
            }
            for error in exc.errors()
        ]
        return error_response(422, "validation_error", "The request is invalid.", details)

    @app.exception_handler(StarletteHTTPException)
    async def _http_error(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = _HTTP_ERROR_CODES.get(exc.status_code, "http_error")
        return error_response(exc.status_code, code, str(exc.detail))

    @app.exception_handler(Exception)
    async def _unhandled_error(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("Unhandled error on %s %s", request.method, request.url.path)
        return error_response(500, "internal_error", "An unexpected error occurred.")
