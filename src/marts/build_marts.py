"""Build mart tables from staging.stg_online_retail.

Full refresh: truncates marts and re-inserts aggregated data.

Usage:
    python -m src.marts.build_marts
"""

from __future__ import annotations

import sys
from pathlib import Path

from src.config import DBConfig
from src.db import get_connection

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

SQL_FILES = {
    "fct_ddl": PROJECT_ROOT / "sql" / "marts_fct_daily_product_sales.sql",
    "fct_insert": PROJECT_ROOT / "sql" / "marts_fct_daily_product_sales_insert.sql",
    "dim_ddl": PROJECT_ROOT / "sql" / "marts_dim_product.sql",
    "dim_insert": PROJECT_ROOT / "sql" / "marts_dim_product_insert.sql",
}


def _get_row_count(conn, table: str) -> int:
    result = conn.execute(f"SELECT COUNT(*) FROM {table}")  # noqa: S608
    return result.fetchone()[0]


def build_marts(config: DBConfig | None = None) -> dict[str, int]:
    """Build all mart tables from staging.

    Returns:
        Dict mapping table name to row count inserted.
    """
    conn = get_connection(config)
    try:
        # Check staging has data
        staging_count = _get_row_count(conn, "staging.stg_online_retail")
        if staging_count == 0:
            print(
                "Error: staging.stg_online_retail is empty. "
                "Run staging first (make staging)."
            )
            sys.exit(1)

        # Create tables
        for key in ("fct_ddl", "dim_ddl"):
            conn.execute(SQL_FILES[key].read_text())
        conn.commit()

        # Truncate for full refresh
        conn.execute("TRUNCATE marts.fct_daily_product_sales")
        conn.execute("TRUNCATE marts.dim_product")

        # Insert fact table
        conn.execute(SQL_FILES["fct_insert"].read_text())
        conn.commit()

        # Insert dimension table
        conn.execute(SQL_FILES["dim_insert"].read_text())
        conn.commit()

        counts = {
            "fct_daily_product_sales": _get_row_count(
                conn, "marts.fct_daily_product_sales"
            ),
            "dim_product": _get_row_count(conn, "marts.dim_product"),
        }

        print(f"Staging rows:              {staging_count:,}")
        print(f"fct_daily_product_sales:   {counts['fct_daily_product_sales']:,}")
        print(f"dim_product:               {counts['dim_product']:,}")

        return counts
    finally:
        conn.close()


def main() -> None:
    build_marts()


if __name__ == "__main__":
    main()
