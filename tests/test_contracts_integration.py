"""Integration tests for data quality contracts. Require a running PostgreSQL instance.

Run with: python -m pytest tests/ -m integration
"""

import psycopg
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
from src.quality.contracts import (
    ALL_CONTRACTS,
    dim_date_ordering,
    dim_primary_key_unique,
    fct_grain_uniqueness,
    fct_non_negative_quantities,
    fct_non_negative_revenue,
    fct_row_count,
    stg_boolean_flags_not_null,
    stg_not_null_core,
    stg_positive_price,
    stg_row_count,
    stg_valid_customer_id,
)
from src.staging.build_staging import build_staging

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def _load_pipeline():
    """Load sample into raw, build staging and marts, then clean up."""
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


@pytest.fixture()
def conn():
    c = get_connection()
    yield c
    c.close()


# --- staging contracts ---


def test_stg_row_count(conn):
    result = stg_row_count(conn)
    assert result.passed, result.message


def test_stg_not_null_core(conn):
    result = stg_not_null_core(conn)
    assert result.passed, result.message


def test_stg_positive_price(conn):
    result = stg_positive_price(conn)
    assert result.passed, result.message


def test_stg_valid_customer_id(conn):
    result = stg_valid_customer_id(conn)
    assert result.passed, result.message


def test_stg_boolean_flags_not_null(conn):
    result = stg_boolean_flags_not_null(conn)
    assert result.passed, result.message


# --- fct contracts ---


def test_fct_row_count(conn):
    result = fct_row_count(conn)
    assert result.passed, result.message


def test_fct_grain_uniqueness(conn):
    result = fct_grain_uniqueness(conn)
    assert result.passed, result.message


def test_fct_non_negative_quantities(conn):
    result = fct_non_negative_quantities(conn)
    assert result.passed, result.message


def test_fct_non_negative_revenue(conn):
    result = fct_non_negative_revenue(conn)
    assert result.passed, result.message


# --- dim contracts ---


def test_dim_primary_key_unique(conn):
    result = dim_primary_key_unique(conn)
    assert result.passed, result.message


def test_dim_date_ordering(conn):
    result = dim_date_ordering(conn)
    assert result.passed, result.message


# --- runner coverage ---


def test_all_contracts_pass(conn):
    """All 11 contracts pass on sample data."""
    for contract in ALL_CONTRACTS:
        result = contract(conn)
        assert result.passed, f"{result.table}.{result.check_name}: {result.message}"


def test_contract_count():
    """ALL_CONTRACTS contains exactly 11 contracts."""
    assert len(ALL_CONTRACTS) == 11


@pytest.mark.parametrize("contract,statement", [
    (stg_row_count, "DELETE FROM staging.stg_online_retail"),
    (stg_positive_price, "UPDATE staging.stg_online_retail SET price=-1"),
    (stg_positive_price, "UPDATE staging.stg_online_retail SET price='NaN'"),
    (stg_valid_customer_id, "UPDATE staging.stg_online_retail SET customer_id=0"),
    (fct_row_count, "DELETE FROM marts.fct_daily_product_sales"),
    (fct_grain_uniqueness,
     "INSERT INTO marts.fct_daily_product_sales "
     "SELECT * FROM marts.fct_daily_product_sales LIMIT 1"),
    (fct_non_negative_quantities,
     "UPDATE marts.fct_daily_product_sales SET total_quantity=-1"),
    (fct_non_negative_revenue,
     "UPDATE marts.fct_daily_product_sales SET return_revenue=-1"),
    (fct_non_negative_revenue,
     "UPDATE marts.fct_daily_product_sales SET total_revenue='NaN'"),
    (dim_date_ordering,
     "UPDATE marts.dim_product SET first_seen=last_seen+1"),
])
def test_contract_detects_invalid_data(conn, contract, statement):
    assert contract(conn).passed
    conn.execute(statement)
    result = contract(conn)
    assert not result.passed
    assert result.message
    conn.rollback()
    assert contract(conn).passed


@pytest.mark.parametrize("statement,contract", [
    ("UPDATE staging.stg_online_retail SET invoice=NULL", stg_not_null_core),
    ("UPDATE staging.stg_online_retail SET is_return=NULL", stg_boolean_flags_not_null),
    ("INSERT INTO marts.dim_product SELECT * FROM marts.dim_product LIMIT 1",
     dim_primary_key_unique),
])
def test_database_rejects_constraint_violations_first(conn, statement, contract):
    with pytest.raises(psycopg.IntegrityError), conn.transaction():
        conn.execute(statement)
    assert contract(conn).passed
