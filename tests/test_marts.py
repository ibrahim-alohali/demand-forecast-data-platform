"""Unit tests for marts layer. No database required."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = PROJECT_ROOT / "sql"

FCT_DDL = SQL_DIR / "marts_fct_daily_product_sales.sql"
FCT_INSERT = SQL_DIR / "marts_fct_daily_product_sales_insert.sql"
DIM_DDL = SQL_DIR / "marts_dim_product.sql"
DIM_INSERT = SQL_DIR / "marts_dim_product_insert.sql"


def test_fct_ddl_file_exists():
    assert FCT_DDL.exists()
    assert FCT_DDL.stat().st_size > 0


def test_fct_insert_file_exists():
    assert FCT_INSERT.exists()
    assert FCT_INSERT.stat().st_size > 0


def test_dim_ddl_file_exists():
    assert DIM_DDL.exists()
    assert DIM_DDL.stat().st_size > 0


def test_dim_insert_file_exists():
    assert DIM_INSERT.exists()
    assert DIM_INSERT.stat().st_size > 0


def test_fct_insert_reads_from_staging():
    sql = FCT_INSERT.read_text()
    assert "staging.stg_online_retail" in sql


def test_fct_insert_writes_to_marts():
    sql = FCT_INSERT.read_text()
    assert "marts.fct_daily_product_sales" in sql


def test_dim_insert_reads_from_staging():
    sql = DIM_INSERT.read_text()
    assert "staging.stg_online_retail" in sql


def test_dim_insert_writes_to_marts():
    sql = DIM_INSERT.read_text()
    assert "marts.dim_product" in sql
