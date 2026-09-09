"""Build staging.stg_online_retail from raw.online_retail.

Full refresh: truncates staging and re-inserts cleaned data from raw.

Usage:
    python -m src.staging.build_staging
"""

from __future__ import annotations

from pathlib import Path

from src.config import DBConfig
from src.db import get_connection

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DDL_PATH = PROJECT_ROOT / "sql" / "staging_online_retail.sql"
INSERT_PATH = PROJECT_ROOT / "sql" / "staging_online_retail_insert.sql"


def build_staging(config: DBConfig | None = None) -> int:
    """Build staging.stg_online_retail from raw.online_retail.

    Steps:
        1. Create staging table if it does not exist.
        2. Truncate staging table (full refresh).
        3. Insert cleaned, deduplicated data from raw.

    Returns:
        Number of rows inserted into staging.
    """
    conn = get_connection(config)
    try:
        # Create table
        ddl = DDL_PATH.read_text()
        conn.execute(ddl)

        # Truncate for full refresh
        conn.execute("TRUNCATE staging.stg_online_retail")

        # Run transformation
        insert_sql = INSERT_PATH.read_text()
        audit = conn.execute(insert_sql).fetchone()
        conn.commit()

        raw_count, missing, negative, duplicates, invalid_ids, staging_count = audit
        print(f"Raw rows:              {raw_count:,}")
        print(f"Excluded missing core: {missing:,}")
        print(f"Excluded negative price (complete core): {negative:,}")
        print(f"Exact eligible duplicates removed: {duplicates:,}")
        print(f"Malformed customer IDs set to NULL (retained rows): {invalid_ids:,}")
        print(f"Staging rows:          {staging_count:,}")

        return staging_count
    finally:
        conn.close()


def main() -> None:
    build_staging()


if __name__ == "__main__":
    main()
