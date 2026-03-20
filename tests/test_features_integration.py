"""Integration tests for features layer. Require a running PostgreSQL instance.

Run with: python -m pytest tests/ -m integration
"""

import pytest

from src.db import get_connection
from src.features.build_features import build_features
from src.features.validate_registry import get_table_columns, load_registry
from src.ingestion.load_online_retail import (
    SAMPLE_PATH,
    load_online_retail,
)
from src.ingestion.load_online_retail import (
    _create_table as create_raw_table,
)
from src.marts.build_marts import build_marts
from src.staging.build_staging import build_staging

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def _load_pipeline():
    """Load sample into raw, build staging, marts, and features, then clean up."""
    conn = get_connection()
    try:
        create_raw_table(conn)
        conn.execute("TRUNCATE raw.online_retail")
        conn.commit()
    finally:
        conn.close()

    load_online_retail(SAMPLE_PATH, mode="safe")
    build_staging()
    build_marts()
    build_features()

    yield

    conn = get_connection()
    try:
        conn.execute("TRUNCATE features.product_daily_features")
        conn.execute("TRUNCATE marts.fct_daily_product_sales")
        conn.execute("TRUNCATE marts.dim_product")
        conn.execute("TRUNCATE staging.stg_online_retail")
        conn.execute("TRUNCATE raw.online_retail")
        conn.commit()
    finally:
        conn.close()


def _query(sql: str):
    conn = get_connection()
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


def _count(table: str) -> int:
    return _query(f"SELECT COUNT(*) FROM {table}")[0][0]


# --- row count ---


def test_feature_row_count():
    assert _count("features.product_daily_features") > 0


# --- grain uniqueness ---


def test_grain_unique():
    dupes = _query(
        "SELECT stock_code, sale_date, country, COUNT(*) "
        "FROM features.product_daily_features "
        "GROUP BY stock_code, sale_date, country "
        "HAVING COUNT(*) > 1"
    )
    assert len(dupes) == 0, f"Duplicate grain rows: {dupes}"


# --- no nulls in grain columns ---


def test_grain_columns_not_null():
    for col in ["stock_code", "sale_date", "country"]:
        nulls = _query(
            f"SELECT COUNT(*) FROM features.product_daily_features "
            f"WHERE {col} IS NULL"
        )[0][0]
        assert nulls == 0, f"{col} has {nulls} NULL values"


# --- non-negative quantities ---


def test_total_quantity_non_negative():
    bad = _query(
        "SELECT COUNT(*) FROM features.product_daily_features "
        "WHERE total_quantity < 0"
    )[0][0]
    assert bad == 0, f"{bad} rows with negative total_quantity"


# --- rolling window not null ---


def test_rolling_7d_quantity_not_null():
    nulls = _query(
        "SELECT COUNT(*) FROM features.product_daily_features "
        "WHERE rolling_7d_quantity IS NULL"
    )[0][0]
    assert nulls == 0, f"{nulls} rows with NULL rolling_7d_quantity"


# --- days_since_first_seen >= 0 ---


def test_days_since_first_seen_non_negative():
    bad = _query(
        "SELECT COUNT(*) FROM features.product_daily_features "
        "WHERE days_since_first_seen < 0"
    )[0][0]
    assert bad == 0, f"{bad} rows with negative days_since_first_seen"


# --- registry validation ---


def test_registry_matches_table():
    features = load_registry()
    registry_names = [f["name"] for f in features]
    conn = get_connection()
    try:
        columns = get_table_columns(conn)
    finally:
        conn.close()
    missing = [name for name in registry_names if name not in columns]
    assert not missing, f"Registry features missing from table: {missing}"
