from __future__ import annotations

import logging

from dotenv import load_dotenv

from app.config import load_settings
from app.db import close_pool, init_pool
from app.errors import register_exception_handlers
from app.generated.main import app
from app.middleware import configure_middlewares

load_dotenv()
settings = load_settings()

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(message)s",
)

configure_middlewares(app, settings)
register_exception_handlers(app)


@app.on_event("startup")
def startup() -> None:
    init_pool(settings)


@app.on_event("shutdown")
def shutdown() -> None:
    close_pool()
