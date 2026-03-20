"""Build feature tables from marts.

Full refresh: truncates features and re-inserts derived data.

Usage:
    python -m src.features.build_features
"""

from __future__ import annotations

import sys
from pathlib import Path

from src.config import DBConfig
from src.db import get_connection

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

SQL_FILES = {
    "ddl": PROJECT_ROOT / "sql" / "features_product_daily.sql",
    "insert": PROJECT_ROOT / "sql" / "features_product_daily_insert.sql",
}


def _get_row_count(conn, table: str) -> int:
    result = conn.execute(f"SELECT COUNT(*) FROM {table}")  # noqa: S608
    return result.fetchone()[0]


def build_features(config: DBConfig | None = None) -> int:
    """Build feature tables from marts.

    Returns:
        Row count inserted into features.product_daily_features.
    """
    conn = get_connection(config)
    try:
        # Check marts have data
        fct_count = _get_row_count(conn, "marts.fct_daily_product_sales")
        dim_count = _get_row_count(conn, "marts.dim_product")

        if fct_count == 0 or dim_count == 0:
            print(
                "Error: marts tables are empty. "
                "Run staging and marts first (make build-all)."
            )
            sys.exit(1)

        # Create table
        conn.execute(SQL_FILES["ddl"].read_text())
        conn.commit()

        # Truncate for full refresh
        conn.execute("TRUNCATE features.product_daily_features")

        # Insert features
        conn.execute(SQL_FILES["insert"].read_text())
        conn.commit()

        feature_count = _get_row_count(conn, "features.product_daily_features")

        print(f"fct_daily_product_sales:   {fct_count:,}")
        print(f"dim_product:               {dim_count:,}")
        print(f"product_daily_features:    {feature_count:,}")

        return feature_count
    finally:
        conn.close()


def main() -> None:
    build_features()


if __name__ == "__main__":
    main()
