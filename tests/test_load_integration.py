"""Integration tests for raw data loading. Require a running PostgreSQL instance.

Run with: python -m pytest tests/ -m integration
Skip in CI: python -m pytest tests/ -m "not integration"
"""

import pytest

from src.db import get_connection
from src.ingestion.load_online_retail import (
    SAMPLE_PATH,
    _create_table,
    load_online_retail,
)

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def _clean_raw_table():
    """Truncate raw.online_retail before and after each test."""
    conn = get_connection()
    try:
        _create_table(conn)
        conn.execute("TRUNCATE raw.online_retail")
        conn.commit()
    finally:
        conn.close()

    yield

    conn = get_connection()
    try:
        conn.execute("TRUNCATE raw.online_retail")
        conn.commit()
    finally:
        conn.close()


def _count_rows() -> int:
    conn = get_connection()
    try:
        result = conn.execute("SELECT COUNT(*) FROM raw.online_retail")
        return result.fetchone()[0]
    finally:
        conn.close()


def test_raw_table_creation():
    """SQL creates raw.online_retail table successfully."""
    conn = get_connection()
    try:
        result = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'raw' AND table_name = 'online_retail' "
            "ORDER BY ordinal_position"
        )
        columns = [row[0] for row in result.fetchall()]
        expected = [
            "invoice",
            "stock_code",
            "description",
            "quantity",
            "invoice_date",
            "price",
            "customer_id",
            "country",
            "source_file",
            "source_sheet",
            "loaded_at",
        ]
        assert columns == expected
    finally:
        conn.close()


def test_load_sample_data():
    """Sample CSV loads into raw table with correct row count."""
    import pandas as pd

    expected_rows = len(pd.read_csv(SAMPLE_PATH))
    loaded = load_online_retail(SAMPLE_PATH, mode="safe")
    assert loaded == expected_rows
    assert _count_rows() == expected_rows


def test_loaded_at_populated():
    """All loaded rows have a non-null loaded_at timestamp."""
    load_online_retail(SAMPLE_PATH, mode="safe")
    conn = get_connection()
    try:
        result = conn.execute(
            "SELECT COUNT(*) FROM raw.online_retail WHERE loaded_at IS NULL"
        )
        null_count = result.fetchone()[0]
        assert null_count == 0, f"{null_count} rows have NULL loaded_at"
    finally:
        conn.close()


def test_source_provenance_populated():
    """All rows have non-null source_file and source_sheet."""
    load_online_retail(SAMPLE_PATH, mode="safe")
    conn = get_connection()
    try:
        result = conn.execute(
            "SELECT COUNT(*) FROM raw.online_retail "
            "WHERE source_file IS NULL OR source_sheet IS NULL"
        )
        null_count = result.fetchone()[0]
        assert null_count == 0, f"{null_count} rows have NULL provenance"

        # Verify the values are sensible
        result = conn.execute(
            "SELECT DISTINCT source_file, source_sheet FROM raw.online_retail"
        )
        rows = result.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "sample_online_retail.csv"
        assert rows[0][1] == "csv"
    finally:
        conn.close()


def test_rerun_without_flag_fails():
    """Loading twice without --replace or --append raises an error."""
    load_online_retail(SAMPLE_PATH, mode="safe")
    rows_after_first = _count_rows()

    with pytest.raises(RuntimeError, match="already has"):
        load_online_retail(SAMPLE_PATH, mode="safe")

    # Verify no extra rows were inserted
    assert _count_rows() == rows_after_first


def test_rerun_with_replace_succeeds():
    """Loading with --replace truncates and reloads without doubling."""
    load_online_retail(SAMPLE_PATH, mode="safe")
    rows_after_first = _count_rows()

    load_online_retail(SAMPLE_PATH, mode="replace")
    rows_after_second = _count_rows()

    assert rows_after_second == rows_after_first, (
        f"Expected {rows_after_first} rows after replace, got {rows_after_second}"
    )
