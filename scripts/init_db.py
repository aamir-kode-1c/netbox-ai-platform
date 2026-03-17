#!/usr/bin/env python3
"""
scripts/init_db.py — Initialise the PostgreSQL database schema.
Run once after PostgreSQL is up and credentials are set in .env
"""
import sys

import structlog

from core.database import init_db, engine
from core.settings import settings
from core.utils import setup_logging

setup_logging()
log = structlog.get_logger(__name__)


def main():
    log.info("Initialising database", dsn=settings.postgres_dsn.replace(settings.postgres_password, "***"))
    try:
        init_db()
        log.info("✅ Database initialised successfully")

        # Verify tables
        from sqlalchemy import inspect
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        log.info("Tables created", tables=tables)

    except Exception as exc:
        log.error("Database initialisation failed", error=str(exc))
        log.error("Ensure PostgreSQL is running and credentials in config/.env are correct")
        sys.exit(1)


if __name__ == "__main__":
    main()
