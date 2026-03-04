from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

import jwt
from passlib.context import CryptContext
from psycopg import Connection

from app.config import Settings
from app.context import CurrentUser
from app.errors import AppError
from app.generated.models import Role

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    return pwd_context.verify(password, password_hash)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _encode_token(payload: dict[str, Any], settings: Settings) -> str:
    return jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)


def create_access_token(user: CurrentUser, settings: Settings) -> tuple[str, int]:
    issued_at = _now()
    expires_at = issued_at + timedelta(minutes=settings.access_ttl_minutes)
    payload = {
        "sub": str(user.user_id),
        "email": user.email,
        "role": user.role.value,
        "typ": "access",
        "iat": int(issued_at.timestamp()),
        "exp": int(expires_at.timestamp()),
    }
    return _encode_token(payload, settings), int((expires_at - issued_at).total_seconds())


def create_refresh_token(user: CurrentUser, settings: Settings) -> tuple[str, UUID, datetime]:
    issued_at = _now()
    expires_at = issued_at + timedelta(days=settings.refresh_ttl_days)
    token_jti = uuid4()
    payload = {
        "sub": str(user.user_id),
        "email": user.email,
        "role": user.role.value,
        "typ": "refresh",
        "jti": str(token_jti),
        "iat": int(issued_at.timestamp()),
        "exp": int(expires_at.timestamp()),
    }
    return _encode_token(payload, settings), token_jti, expires_at


def persist_refresh_token(
    connection: Connection,
    user_id: UUID,
    token_jti: UUID,
    refresh_token: str,
    expires_at: datetime,
) -> None:
    token_hash = _hash_token(refresh_token)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO refresh_tokens (token_jti, user_id, token_hash, expires_at)
            VALUES (%s, %s, %s, %s)
            """,
            (token_jti, user_id, token_hash, expires_at),
        )


def issue_token_pair(connection: Connection, user: CurrentUser, settings: Settings) -> dict[str, Any]:
    access_token, expires_in = create_access_token(user, settings)
    refresh_token, refresh_jti, refresh_expires_at = create_refresh_token(user, settings)
    persist_refresh_token(connection, user.user_id, refresh_jti, refresh_token, refresh_expires_at)
    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "expires_in": expires_in,
    }


def decode_access_token(token: str, settings: Settings) -> CurrentUser:
    try:
        payload = jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])
    except jwt.ExpiredSignatureError as exc:
        raise AppError(401, "TOKEN_EXPIRED", "Access token expired") from exc
    except jwt.InvalidTokenError as exc:
        raise AppError(401, "TOKEN_INVALID", "Access token is invalid") from exc

    if payload.get("typ") != "access":
        raise AppError(401, "TOKEN_INVALID", "Access token is invalid")

    try:
        user_id = UUID(str(payload["sub"]))
        role = Role(str(payload["role"]))
        email = str(payload.get("email", ""))
    except Exception as exc:  # noqa: BLE001
        raise AppError(401, "TOKEN_INVALID", "Access token is invalid") from exc

    return CurrentUser(user_id=user_id, role=role, email=email)


def decode_refresh_token(token: str, settings: Settings) -> tuple[CurrentUser, UUID]:
    try:
        payload = jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])
    except jwt.ExpiredSignatureError as exc:
        raise AppError(401, "REFRESH_TOKEN_INVALID", "Refresh token is invalid") from exc
    except jwt.InvalidTokenError as exc:
        raise AppError(401, "REFRESH_TOKEN_INVALID", "Refresh token is invalid") from exc

    if payload.get("typ") != "refresh":
        raise AppError(401, "REFRESH_TOKEN_INVALID", "Refresh token is invalid")

    try:
        user_id = UUID(str(payload["sub"]))
        role = Role(str(payload["role"]))
        email = str(payload.get("email", ""))
        token_jti = UUID(str(payload["jti"]))
    except Exception as exc:  # noqa: BLE001
        raise AppError(401, "REFRESH_TOKEN_INVALID", "Refresh token is invalid") from exc

    return CurrentUser(user_id=user_id, role=role, email=email), token_jti


def validate_and_rotate_refresh_token(
    connection: Connection,
    refresh_token: str,
    settings: Settings,
) -> CurrentUser:
    user, token_jti = decode_refresh_token(refresh_token, settings)
    token_hash = _hash_token(refresh_token)

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT id, revoked, expires_at
            FROM refresh_tokens
            WHERE token_jti = %s AND user_id = %s AND token_hash = %s
            """,
            (token_jti, user.user_id, token_hash),
        )
        row = cursor.fetchone()

        if row is None:
            raise AppError(401, "REFRESH_TOKEN_INVALID", "Refresh token is invalid")

        if row["revoked"] or row["expires_at"] <= _now():
            raise AppError(401, "REFRESH_TOKEN_INVALID", "Refresh token is invalid")

        cursor.execute(
            """
            UPDATE refresh_tokens
            SET revoked = TRUE
            WHERE token_jti = %s
            """,
            (token_jti,),
        )

    return user
