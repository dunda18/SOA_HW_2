from __future__ import annotations

from contextlib import contextmanager

from psycopg import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from app.config import Settings

_pool: ConnectionPool | None = None


def init_pool(settings: Settings) -> None:
    global _pool
    if _pool is not None:
        return

    _pool = ConnectionPool(
        conninfo=settings.database_url,
        min_size=1,
        max_size=10,
        kwargs={"autocommit": False, "row_factory": dict_row},
        open=True,
    )


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None


@contextmanager
def get_connection() -> Connection:
    if _pool is None:
        raise RuntimeError("Database pool is not initialized")

    with _pool.connection() as connection:
        yield connection
