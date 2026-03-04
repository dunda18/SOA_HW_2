from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, Request

from app.config import Settings
from app.context import get_current_user, set_current_user, set_request_id
from app.errors import AppError, to_json_response
from app.security import decode_access_token

logger = logging.getLogger("marketplace.api")
SENSITIVE_KEYS = {"password", "access_token", "refresh_token", "token"}
PUBLIC_PREFIXES = ("/auth/", "/docs", "/redoc", "/openapi.json")
MUTATING_METHODS = {"POST", "PUT", "DELETE"}


def _mask_payload(payload: Any) -> Any:
    if isinstance(payload, dict):
        result: dict[str, Any] = {}
        for key, value in payload.items():
            if key.lower() in SENSITIVE_KEYS:
                result[key] = "***"
            else:
                result[key] = _mask_payload(value)
        return result

    if isinstance(payload, list):
        return [_mask_payload(item) for item in payload]

    return payload


def _read_body_for_log(request: Request) -> dict[str, Any] | None:
    try:
        body_raw = request._body.decode("utf-8") if hasattr(request, "_body") else ""
        if not body_raw:
            return None
        data = json.loads(body_raw)
        return _mask_payload(data)
    except Exception:  # noqa: BLE001
        return None


def _is_public_path(path: str) -> bool:
    return any(path.startswith(prefix) for prefix in PUBLIC_PREFIXES)


def configure_middlewares(app: FastAPI, settings: Settings) -> None:
    @app.middleware("http")
    async def auth_and_logging_middleware(request: Request, call_next):
        request_id = str(uuid4())
        set_request_id(request_id)
        set_current_user(None)

        start = time.perf_counter()
        request_body = None

        if request.method in MUTATING_METHODS:
            body_bytes = await request.body()
            request._body = body_bytes

            async def receive() -> dict[str, Any]:
                return {"type": "http.request", "body": body_bytes, "more_body": False}

            request._receive = receive
            request_body = _read_body_for_log(request)

        try:
            if not _is_public_path(request.url.path):
                auth_header = request.headers.get("Authorization", "")
                if not auth_header.startswith("Bearer "):
                    raise AppError(401, "TOKEN_INVALID", "Access token is invalid")

                token = auth_header.split(" ", 1)[1].strip()
                if not token:
                    raise AppError(401, "TOKEN_INVALID", "Access token is invalid")

                set_current_user(decode_access_token(token, settings))

            response = await call_next(request)
        except AppError as exc:
            response = to_json_response(exc)
        except Exception:  # noqa: BLE001
            logger.exception("Unhandled exception while processing request")
            response = to_json_response(
                AppError(500, "INTERNAL_ERROR", "Internal server error")
            )

        duration_ms = round((time.perf_counter() - start) * 1000, 3)
        user = get_current_user()
        response.headers["X-Request-Id"] = request_id

        log_payload: dict[str, Any] = {
            "request_id": request_id,
            "method": request.method,
            "endpoint": request.url.path,
            "status_code": response.status_code,
            "duration_ms": duration_ms,
            "user_id": str(user.user_id) if user is not None else None,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if request_body is not None and request.method in MUTATING_METHODS:
            log_payload["body"] = request_body

        logger.info(json.dumps(log_payload, ensure_ascii=True, default=str))
        return response
