"""Integration tests for staging layer. Require a running PostgreSQL instance.

Run with: python -m pytest tests/ -m integration
Skip in CI: python -m pytest tests/ -m "not integration"
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
from src.staging.build_staging import build_staging

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def _load_sample_and_build_staging():
    """Load sample into raw, build staging, then clean up both."""
    conn = get_connection()
    try:
        create_raw_table(conn)
        conn.execute("TRUNCATE raw.online_retail")
        conn.commit()
    finally:
        conn.close()

    load_online_retail(SAMPLE_PATH, mode="safe")
    build_staging()

    yield

    conn = get_connection()
    try:
        conn.execute("TRUNCATE staging.stg_online_retail")
        conn.execute("TRUNCATE raw.online_retail")
        conn.commit()
    finally:
        conn.close()


def _query(sql: str):
    """Execute a query and return all rows."""
    conn = get_connection()
    try:
        result = conn.execute(sql)
        return result.fetchall()
    finally:
        conn.close()


def _count(table: str) -> int:
    rows = _query(f"SELECT COUNT(*) FROM {table}")
    return rows[0][0]


def test_staging_table_creation():
    """Staging table exists with expected columns."""
    rows = _query(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'staging' "
        "AND table_name = 'stg_online_retail' "
        "ORDER BY ordinal_position"
    )
    columns = [row[0] for row in rows]
    expected = [
        "invoice",
        "stock_code",
        "description",
        "quantity",
        "invoice_date",
        "price",
        "customer_id",
        "country",
        "is_return",
        "is_stock_item",
        "staged_at",
    ]
    assert columns == expected


def test_staging_row_count():
    """100 raw rows become 99 staging rows (1 duplicate removed)."""
    assert _count("raw.online_retail") == 100
    assert _count("staging.stg_online_retail") == 99


def test_customer_id_is_integer():
    """customer_id values are NULL or valid integers, no float strings."""
    rows = _query(
        "SELECT customer_id FROM staging.stg_online_retail "
        "WHERE customer_id IS NOT NULL"
    )
    for row in rows:
        assert isinstance(row[0], int), f"Expected int, got {type(row[0])}: {row[0]}"


def test_is_return_flag():
    """C-prefixed invoices have is_return = TRUE; others FALSE."""
    returns = _query(
        "SELECT invoice FROM staging.stg_online_retail WHERE is_return"
    )
    return_invoices = {row[0] for row in returns}
    # Sample has C489439 (deduped to 1) and C489445
    assert all(inv.startswith("C") for inv in return_invoices)
    assert "C489439" in return_invoices
    assert "C489445" in return_invoices

    non_returns = _query(
        "SELECT invoice FROM staging.stg_online_retail WHERE NOT is_return"
    )
    assert all(not row[0].startswith("C") for row in non_returns)


def test_is_stock_item_flag():
    """POST and DOT have is_stock_item = FALSE; regular codes TRUE."""
    non_stock = _query(
        "SELECT stock_code FROM staging.stg_online_retail "
        "WHERE NOT is_stock_item"
    )
    non_stock_codes = {row[0] for row in non_stock}
    assert "POST" in non_stock_codes
    assert "DOT" in non_stock_codes

    stock_items = _query(
        "SELECT DISTINCT stock_code FROM staging.stg_online_retail "
        "WHERE is_stock_item"
    )
    stock_codes = {row[0] for row in stock_items}
    assert "POST" not in stock_codes
    assert "DOT" not in stock_codes


def test_description_cleaned():
    """Double spaces are collapsed to single space."""
    rows = _query(
        "SELECT description FROM staging.stg_online_retail "
        "WHERE stock_code = '22349'"
    )
    assert len(rows) == 1
    assert rows[0][0] == "DOG BOWL CHASING BALL DESIGN"


def test_null_customer_preserved():
    """Rows with empty customer_id in raw have NULL in staging."""
    rows = _query(
        "SELECT COUNT(*) FROM staging.stg_online_retail "
        "WHERE customer_id IS NULL"
    )
    null_count = rows[0][0]
    # Sample has 7 rows with empty customer_id
    assert null_count == 7, f"Expected 7 NULL customers, got {null_count}"


def test_idempotent_rerun():
    """Running staging twice produces the same row count."""
    first_count = _count("staging.stg_online_retail")
    build_staging()
    second_count = _count("staging.stg_online_retail")
    assert first_count == second_count == 99


def test_not_null_constraints():
    """Required columns are never NULL in staging."""
    for col in ["invoice", "stock_code", "quantity", "invoice_date",
                "price", "country"]:
        rows = _query(
            f"SELECT COUNT(*) FROM staging.stg_online_retail "
            f"WHERE {col} IS NULL"
        )
        assert rows[0][0] == 0, f"{col} has NULL values in staging"
