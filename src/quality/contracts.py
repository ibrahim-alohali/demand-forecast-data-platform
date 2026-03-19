"""Data quality contracts for staging and marts tables.

Each contract function takes a psycopg connection, runs a validation query,
and returns a ContractResult indicating pass or fail.
"""

from __future__ import annotations

from dataclasses import dataclass

import psycopg


@dataclass(frozen=True)
class ContractResult:
    """Result of a single data quality contract check."""

    table: str
    check_name: str
    passed: bool
    message: str


def _scalar(conn: psycopg.Connection, sql: str) -> int:
    return conn.execute(sql).fetchone()[0]  # type: ignore[index]


# --- staging.stg_online_retail contracts ---


def stg_row_count(conn: psycopg.Connection) -> ContractResult:
    count = _scalar(conn, "SELECT COUNT(*) FROM staging.stg_online_retail")
    return ContractResult(
        table="staging.stg_online_retail",
        check_name="row_count",
        passed=count > 0,
        message=f"row count = {count}" if count > 0 else "table is empty",
    )


def stg_not_null_core(conn: psycopg.Connection) -> ContractResult:
    cols = ["invoice", "stock_code", "quantity", "invoice_date", "price", "country"]
    failures = []
    for col in cols:
        nulls = _scalar(
            conn,
            f"SELECT COUNT(*) FROM staging.stg_online_retail WHERE {col} IS NULL",
        )
        if nulls > 0:
            failures.append(f"{col}: {nulls} NULLs")
    return ContractResult(
        table="staging.stg_online_retail",
        check_name="not_null_core",
        passed=len(failures) == 0,
        message="all core columns non-null" if not failures else "; ".join(failures),
    )


def stg_positive_price(conn: psycopg.Connection) -> ContractResult:
    bad = _scalar(
        conn,
        "SELECT COUNT(*) FROM staging.stg_online_retail WHERE price < 0",
    )
    return ContractResult(
        table="staging.stg_online_retail",
        check_name="positive_price",
        passed=bad == 0,
        message="all prices >= 0" if bad == 0 else f"{bad} rows with negative price",
    )


def stg_valid_customer_id(conn: psycopg.Connection) -> ContractResult:
    bad = _scalar(
        conn,
        "SELECT COUNT(*) FROM staging.stg_online_retail "
        "WHERE customer_id IS NOT NULL AND customer_id <= 0",
    )
    return ContractResult(
        table="staging.stg_online_retail",
        check_name="valid_customer_id",
        passed=bad == 0,
        message=(
            "all non-null customer_ids > 0"
            if bad == 0
            else f"{bad} rows with customer_id <= 0"
        ),
    )


def stg_boolean_flags_not_null(conn: psycopg.Connection) -> ContractResult:
    failures = []
    for col in ["is_return", "is_stock_item"]:
        nulls = _scalar(
            conn,
            f"SELECT COUNT(*) FROM staging.stg_online_retail WHERE {col} IS NULL",
        )
        if nulls > 0:
            failures.append(f"{col}: {nulls} NULLs")
    return ContractResult(
        table="staging.stg_online_retail",
        check_name="boolean_flags_not_null",
        passed=len(failures) == 0,
        message="boolean flags non-null" if not failures else "; ".join(failures),
    )


# --- marts.fct_daily_product_sales contracts ---


def fct_row_count(conn: psycopg.Connection) -> ContractResult:
    count = _scalar(conn, "SELECT COUNT(*) FROM marts.fct_daily_product_sales")
    return ContractResult(
        table="marts.fct_daily_product_sales",
        check_name="row_count",
        passed=count > 0,
        message=f"row count = {count}" if count > 0 else "table is empty",
    )


def fct_grain_uniqueness(conn: psycopg.Connection) -> ContractResult:
    total = _scalar(conn, "SELECT COUNT(*) FROM marts.fct_daily_product_sales")
    distinct = _scalar(
        conn,
        "SELECT COUNT(*) FROM ("
        "  SELECT DISTINCT stock_code, sale_date, country"
        "  FROM marts.fct_daily_product_sales"
        ") t",
    )
    return ContractResult(
        table="marts.fct_daily_product_sales",
        check_name="grain_uniqueness",
        passed=total == distinct,
        message=(
            f"grain is unique ({total} rows)"
            if total == distinct
            else f"{total - distinct} duplicate grain rows"
        ),
    )


def fct_non_negative_quantities(conn: psycopg.Connection) -> ContractResult:
    bad = _scalar(
        conn,
        "SELECT COUNT(*) FROM marts.fct_daily_product_sales "
        "WHERE total_quantity < 0 OR return_quantity < 0",
    )
    return ContractResult(
        table="marts.fct_daily_product_sales",
        check_name="non_negative_quantities",
        passed=bad == 0,
        message=(
            "all quantities >= 0"
            if bad == 0
            else f"{bad} rows with negative quantities"
        ),
    )


def fct_non_negative_revenue(conn: psycopg.Connection) -> ContractResult:
    bad = _scalar(
        conn,
        "SELECT COUNT(*) FROM marts.fct_daily_product_sales "
        "WHERE total_revenue < 0 OR return_revenue < 0",
    )
    return ContractResult(
        table="marts.fct_daily_product_sales",
        check_name="non_negative_revenue",
        passed=bad == 0,
        message=(
            "all revenue >= 0" if bad == 0 else f"{bad} rows with negative revenue"
        ),
    )


# --- marts.dim_product contracts ---


def dim_primary_key_unique(conn: psycopg.Connection) -> ContractResult:
    total = _scalar(conn, "SELECT COUNT(*) FROM marts.dim_product")
    distinct = _scalar(
        conn, "SELECT COUNT(DISTINCT stock_code) FROM marts.dim_product"
    )
    return ContractResult(
        table="marts.dim_product",
        check_name="primary_key_unique",
        passed=total == distinct,
        message=(
            f"stock_code is unique ({total} rows)"
            if total == distinct
            else f"{total - distinct} duplicate stock_codes"
        ),
    )


def dim_date_ordering(conn: psycopg.Connection) -> ContractResult:
    bad = _scalar(
        conn,
        "SELECT COUNT(*) FROM marts.dim_product WHERE first_seen > last_seen",
    )
    return ContractResult(
        table="marts.dim_product",
        check_name="date_ordering",
        passed=bad == 0,
        message=(
            "all first_seen <= last_seen"
            if bad == 0
            else f"{bad} rows with inverted dates"
        ),
    )


ALL_CONTRACTS = [
    stg_row_count,
    stg_not_null_core,
    stg_positive_price,
    stg_valid_customer_id,
    stg_boolean_flags_not_null,
    fct_row_count,
    fct_grain_uniqueness,
    fct_non_negative_quantities,
    fct_non_negative_revenue,
    dim_primary_key_unique,
    dim_date_ordering,
]
