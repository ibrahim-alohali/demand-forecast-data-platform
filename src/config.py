"""Database configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class DBConfig:
    """Immutable database connection parameters."""

    user: str
    password: str
    host: str
    port: int
    dbname: str

    @property
    def conninfo(self) -> str:
        """Return a libpq-style connection string."""
        return (
            f"host={self.host} port={self.port} "
            f"dbname={self.dbname} user={self.user} password={self.password}"
        )


def get_config() -> DBConfig:
    """Load database config from environment variables.

    Reads from a .env file if present, then falls back to
    actual environment variables.
    """
    load_dotenv()

    return DBConfig(
        user=os.environ.get("POSTGRES_USER", "forecast"),
        password=os.environ.get("POSTGRES_PASSWORD", "forecast"),
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "demand_forecast"),
    )
