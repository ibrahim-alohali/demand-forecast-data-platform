"""Thin database connection helper using psycopg (v3)."""

from __future__ import annotations

import psycopg

from src.config import DBConfig, get_config


def get_connection(config: DBConfig | None = None) -> psycopg.Connection:
    """Open a new connection to PostgreSQL.

    Args:
        config: Optional DBConfig. If not provided, loads from environment.

    Returns:
        A psycopg Connection. Caller is responsible for closing it.
    """
    if config is None:
        config = get_config()
    return psycopg.connect(config.conninfo)
