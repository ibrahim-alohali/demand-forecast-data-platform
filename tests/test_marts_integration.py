"""Integration tests for marts layer. Require a running PostgreSQL instance.

Run with: python -m pytest tests/ -m integration
"""

import pytest

from src.db import get_connection
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

NON_STOCK_CODES = {"POST", "DOT", "D", "M", "BANK CHARGES",
                   "PADS", "AMAZONFEE", "C2", "CRUK"}


@pytest.fixture(autouse=True)
def _load_pipeline():
    """Load sample into raw, build staging, build marts, then clean up."""
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

    yield

    conn = get_connection()
    try:
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


# --- fct_daily_product_sales tests ---


def test_fct_row_count():
    assert _count("marts.fct_daily_product_sales") > 0


def test_fct_grain_unique():
    """No duplicate (stock_code, sale_date, country) rows."""
    dupes = _query(
        "SELECT stock_code, sale_date, country, COUNT(*) "
        "FROM marts.fct_daily_product_sales "
        "GROUP BY stock_code, sale_date, country "
        "HAVING COUNT(*) > 1"
    )
    assert len(dupes) == 0, f"Duplicate grain rows: {dupes}"


def test_fct_no_nulls():
    for col in ["stock_code", "sale_date", "country", "total_quantity",
                "total_revenue", "transaction_count", "return_quantity",
                "return_revenue", "return_count"]:
        nulls = _query(
            f"SELECT COUNT(*) FROM marts.fct_daily_product_sales "
            f"WHERE {col} IS NULL"
        )[0][0]
        assert nulls == 0, f"{col} has {nulls} NULL values"


def test_fct_quantities_non_negative():
    bad = _query(
        "SELECT COUNT(*) FROM marts.fct_daily_product_sales "
        "WHERE total_quantity < 0 OR return_quantity < 0"
    )[0][0]
    assert bad == 0, f"{bad} rows with negative quantities"


def test_fct_revenue_non_negative():
    bad = _query(
        "SELECT COUNT(*) FROM marts.fct_daily_product_sales "
        "WHERE total_revenue < 0 OR return_revenue < 0"
    )[0][0]
    assert bad == 0, f"{bad} rows with negative revenue"


def test_fct_has_returns():
    """Sample data includes returns, so at least one row should have them."""
    rows_with_returns = _query(
        "SELECT COUNT(*) FROM marts.fct_daily_product_sales "
        "WHERE return_quantity > 0"
    )[0][0]
    assert rows_with_returns > 0


def test_fct_transaction_count_positive_when_sales_exist():
    bad = _query(
        "SELECT COUNT(*) FROM marts.fct_daily_product_sales "
        "WHERE total_quantity > 0 AND transaction_count < 1"
    )[0][0]
    assert bad == 0


# --- dim_product tests ---


def test_dim_row_count():
    assert _count("marts.dim_product") > 0


def test_dim_stock_code_unique():
    dupes = _query(
        "SELECT stock_code, COUNT(*) FROM marts.dim_product "
        "GROUP BY stock_code HAVING COUNT(*) > 1"
    )
    assert len(dupes) == 0, f"Duplicate stock_codes: {dupes}"


def test_dim_first_seen_before_last_seen():
    bad = _query(
        "SELECT COUNT(*) FROM marts.dim_product "
        "WHERE first_seen > last_seen"
    )[0][0]
    assert bad == 0


def test_dim_no_non_stock_items():
    """Non-product codes should not appear in dim_product."""
    rows = _query("SELECT stock_code FROM marts.dim_product")
    found_codes = {row[0].strip().upper() for row in rows}
    overlap = found_codes & NON_STOCK_CODES
    assert len(overlap) == 0, f"Non-stock items in dim_product: {overlap}"
