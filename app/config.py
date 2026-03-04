from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str
    jwt_secret: str
    jwt_algorithm: str
    access_ttl_minutes: int
    refresh_ttl_days: int
    order_rate_limit_minutes: int
    host: str
    port: int
    log_level: str


def load_settings() -> Settings:
    return Settings(
        database_url=os.getenv(
            "DATABASE_URL",
            "postgresql://marketplace:marketplace@localhost:5432/marketplace",
        ),
        jwt_secret=os.getenv("JWT_SECRET", "change-me-in-env"),
        jwt_algorithm=os.getenv("JWT_ALGORITHM", "HS256"),
        access_ttl_minutes=int(os.getenv("JWT_ACCESS_TTL_MINUTES", "15")),
        refresh_ttl_days=int(os.getenv("JWT_REFRESH_TTL_DAYS", "14")),
        order_rate_limit_minutes=int(os.getenv("ORDER_RATE_LIMIT_MINUTES", "3")),
        host=os.getenv("APP_HOST", "0.0.0.0"),
        port=int(os.getenv("APP_PORT", "8080")),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )
