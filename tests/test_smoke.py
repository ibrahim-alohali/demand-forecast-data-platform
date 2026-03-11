"""Smoke tests to verify the project scaffold works."""

import os


def test_src_package_imports():
    """Verify the src package is importable."""
    from src import (
        config,  # noqa: F401
        db,  # noqa: F401
    )


def test_config_loads_defaults():
    """Verify get_config() returns sensible defaults without a .env file."""
    # Clear any env vars that might interfere
    env_vars = [
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
    ]
    original = {k: os.environ.pop(k, None) for k in env_vars}

    try:
        from src.config import get_config

        cfg = get_config()
        assert cfg.user == "forecast"
        assert cfg.password == "forecast"
        assert cfg.host == "localhost"
        assert cfg.port == 5432
        assert cfg.dbname == "demand_forecast"
        assert "host=" in cfg.conninfo
    finally:
        # Restore original env vars
        for k, v in original.items():
            if v is not None:
                os.environ[k] = v
