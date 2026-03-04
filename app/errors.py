from __future__ import annotations

from typing import Any, Dict

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from app.generated.models import ErrorResponse


class AppError(Exception):
    def __init__(
        self,
        status_code: int,
        error_code: str,
        message: str,
        details: Dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code
        self.message = message
        self.details = details


def to_json_response(error: AppError) -> JSONResponse:
    payload = ErrorResponse(
        error_code=error.error_code,
        message=error.message,
        details=error.details,
    )
    return JSONResponse(status_code=error.status_code, content=payload.model_dump(mode="json"))


def register_exception_handlers(app) -> None:
    @app.exception_handler(AppError)
    async def app_error_handler(_: Request, exc: AppError) -> JSONResponse:
        return to_json_response(exc)

    @app.exception_handler(RequestValidationError)
    async def request_validation_error_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
        errors = []
        for item in exc.errors():
            location = [str(value) for value in item.get("loc", []) if value != "body"]
            field = ".".join(location) if location else "request"
            errors.append({"field": field, "message": item.get("msg", "invalid value")})

        return to_json_response(
            AppError(
                status_code=400,
                error_code="VALIDATION_ERROR",
                message="Validation failed",
                details={"errors": errors},
            )
        )

    @app.exception_handler(ValidationError)
    async def pydantic_validation_error_handler(_: Request, exc: ValidationError) -> JSONResponse:
        errors = []
        for item in exc.errors():
            location = [str(value) for value in item.get("loc", [])]
            field = ".".join(location) if location else "request"
            errors.append({"field": field, "message": item.get("msg", "invalid value")})

        return to_json_response(
            AppError(
                status_code=400,
                error_code="VALIDATION_ERROR",
                message="Validation failed",
                details={"errors": errors},
            )
        )
