"""Refuse destructive integration fixtures unless a test database is explicit."""

import os

import pytest


def pytest_collection_finish(session):
    if not any(item.get_closest_marker("integration") for item in session.items):
        return
    required = ("POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB",
                "POSTGRES_USER", "POSTGRES_PASSWORD")
    if (any(not os.environ.get(name) for name in required)
            or not os.environ.get("POSTGRES_DB", "").endswith("_test")):
        raise pytest.UsageError(
            "Integration tests truncate tables. Explicitly set POSTGRES_HOST, "
            "POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, and POSTGRES_DB "
            "to an isolated database whose name ends with _test."
        )
