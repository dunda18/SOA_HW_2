from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass
from typing import Optional
from uuid import UUID

from app.generated.models import Role


@dataclass(frozen=True)
class CurrentUser:
    user_id: UUID
    role: Role
    email: str


_current_user: ContextVar[Optional[CurrentUser]] = ContextVar("current_user", default=None)
_request_id: ContextVar[Optional[str]] = ContextVar("request_id", default=None)


def set_current_user(user: Optional[CurrentUser]) -> None:
    _current_user.set(user)


def get_current_user() -> Optional[CurrentUser]:
    return _current_user.get()


def set_request_id(request_id: str) -> None:
    _request_id.set(request_id)


def get_request_id() -> Optional[str]:
    return _request_id.get()
