"""Unit tests for staging layer. No database required."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DDL_PATH = PROJECT_ROOT / "sql" / "staging_online_retail.sql"
INSERT_PATH = PROJECT_ROOT / "sql" / "staging_online_retail_insert.sql"


def test_staging_ddl_file_exists():
    """DDL file exists and is non-empty."""
    assert DDL_PATH.exists()
    assert DDL_PATH.stat().st_size > 0


def test_staging_insert_file_exists():
    """Insert SQL file exists and is non-empty."""
    assert INSERT_PATH.exists()
    assert INSERT_PATH.stat().st_size > 0


def test_insert_reads_from_raw():
    """Insert SQL references raw.online_retail as source."""
    sql = INSERT_PATH.read_text()
    assert "raw.online_retail" in sql


def test_insert_writes_to_staging():
    """Insert SQL references staging.stg_online_retail as target."""
    sql = INSERT_PATH.read_text()
    assert "staging.stg_online_retail" in sql
