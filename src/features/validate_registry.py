"""Validate that every feature in registry.yml has a matching column in the database.

Loads the YAML registry, connects to PostgreSQL, and checks that each feature name
appears as a column in features.product_daily_features.

Usage:
    python -m src.features.validate_registry
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

from src.db import get_connection

REGISTRY_PATH = Path(__file__).resolve().parent / "registry.yml"


def get_table_columns(conn) -> set[str]:
    """Return column names for features.product_daily_features."""
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'features' AND table_name = 'product_daily_features'"
    ).fetchall()
    return {row[0] for row in rows}


def load_registry() -> list[dict]:
    """Load and return the features list from registry.yml."""
    with open(REGISTRY_PATH) as f:
        data = yaml.safe_load(f)
    return data["features"]


def validate() -> int:
    """Validate registry against database columns.

    Returns:
        0 if all features match, 1 if any mismatch.
    """
    features = load_registry()
    registry_names = [f["name"] for f in features]

    conn = get_connection()
    try:
        columns = get_table_columns(conn)
    finally:
        conn.close()

    missing = [name for name in registry_names if name not in columns]

    print()
    print(f"Registry features: {len(registry_names)}")
    print(f"Table columns:     {len(columns)}")

    if missing:
        print(f"Missing columns:   {', '.join(missing)}")
        print("FAIL: registry/table mismatch")
        return 1

    print("All registry features have matching columns.")
    return 0


def main() -> None:
    sys.exit(validate())


if __name__ == "__main__":
    main()
