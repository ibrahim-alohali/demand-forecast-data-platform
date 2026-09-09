"""Regression examples for ingestion fidelity and atomic warehouse refreshes."""

import importlib

import pandas as pd
import psycopg
import pytest

from src.db import get_connection
from src.ingestion.load_online_retail import SAMPLE_PATH, load_online_retail
from src.marts.build_marts import build_marts
from src.staging.build_staging import build_staging

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def sample_pipeline():
    load_online_retail(SAMPLE_PATH, mode="replace")
    build_staging()
    build_marts()
    yield


def _rows(table):
    with get_connection() as conn:
        return conn.execute(f"SELECT * FROM {table} ORDER BY 1,2,3").fetchall()


def test_copy_round_trip(tmp_path):
    frame = pd.read_csv(SAMPLE_PATH, dtype=str).iloc[:8].copy()
    values = ["tab\ttext", "line\ntext", 'a "quote"', r"C:\new\thing",
              "العربية café", r"\N", "NA", None]
    frame["description"] = values
    path = tmp_path / "special.csv"
    frame.to_csv(path, index=False)
    load_online_retail(path, mode="replace")
    with get_connection() as conn:
        result = conn.execute("SELECT description FROM raw.online_retail").fetchall()
    assert [row[0] for row in result] == values


def test_invalid_mode_rejected_without_changes():
    before = _rows("raw.online_retail")
    with pytest.raises(ValueError, match="mode"):
        load_online_retail(SAMPLE_PATH, mode="typo")
    assert _rows("raw.online_retail") == before


def test_failed_replace_preserves_rows(tmp_path):
    before = _rows("raw.online_retail")
    frame = pd.read_csv(SAMPLE_PATH, dtype=str)
    frame.loc[50, "quantity"] = "invalid integer"
    path = tmp_path / "bad.csv"
    frame.to_csv(path, index=False)
    with pytest.raises((ValueError, psycopg.Error)):
        load_online_retail(path, mode="replace")
    assert _rows("raw.online_retail") == before


def test_append_keeps_all_source_rows():
    assert load_online_retail(SAMPLE_PATH, mode="append") == 100
    assert len(_rows("raw.online_retail")) == 200
    assert build_staging() == 99


def test_dimension_failure_rolls_back_both_marts(tmp_path, monkeypatch):
    before_fact = _rows("marts.fct_daily_product_sales")
    before_dim = _rows("marts.dim_product")
    with get_connection() as conn:
        conn.execute("UPDATE staging.stg_online_retail SET quantity=quantity*2")
    bad_sql = tmp_path / "failure.sql"
    bad_sql.write_text("SELECT 1/0;", encoding="utf-8")
    module = importlib.import_module("src.marts.build_marts")
    monkeypatch.setitem(module.SQL_FILES, "dim_insert", bad_sql)
    with pytest.raises(psycopg.errors.DivisionByZero):
        build_marts()
    assert _rows("marts.fct_daily_product_sales") == before_fact
    assert _rows("marts.dim_product") == before_dim


def test_mart_rebuild_repeats_values():
    before = (_rows("marts.fct_daily_product_sales"), _rows("marts.dim_product"))
    build_marts()
    after = (_rows("marts.fct_daily_product_sales"), _rows("marts.dim_product"))
    assert after == before


def test_multi_sheet_load_is_atomic(tmp_path):
    before = _rows("raw.online_retail")
    frame = pd.read_csv(SAMPLE_PATH, dtype=str).iloc[:2].copy()
    path = tmp_path / "sheets.xlsx"
    with pd.ExcelWriter(path) as writer:
        frame.to_excel(writer, sheet_name="First", index=False)
        frame.to_excel(writer, sheet_name="Second", index=False)
    assert load_online_retail(path, mode="replace") == 4
    with get_connection() as conn:
        assert conn.execute(
            "SELECT source_file,source_sheet,COUNT(*) FROM raw.online_retail "
            "GROUP BY 1,2 ORDER BY 2"
        ).fetchall() == [(path.name, "First", 2), (path.name, "Second", 2)]
    before = _rows("raw.online_retail")
    with pd.ExcelWriter(path) as writer:
        frame.to_excel(writer, sheet_name="First", index=False)
        frame["quantity"] = "invalid"
        frame.to_excel(writer, sheet_name="Second", index=False)
    with pytest.raises(psycopg.Error):
        load_online_retail(path, mode="replace")
    assert _rows("raw.online_retail") == before


def test_staging_handles_invalid_customers_and_blank_keys(tmp_path):
    frame = pd.read_csv(SAMPLE_PATH, dtype=str).iloc[:5].copy()
    frame["invoice"] = [" A ", "B", "C", "D", "   "]
    frame["customer_id"] = ["123.0", "not an id", "9999999999999999", "-1", "1"]
    frame["stock_code"] = " X "
    path = tmp_path / "customers.csv"
    frame.to_csv(path, index=False)
    load_online_retail(path, mode="replace")
    assert build_staging() == 4
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT invoice,stock_code,customer_id FROM staging.stg_online_retail "
            "ORDER BY invoice"
        ).fetchall()
    assert rows == [("A", "X", 123), ("B", "X", None),
                    ("C", "X", None), ("D", "X", None)]


def test_marts_exclude_unclassified_adjustments(tmp_path):
    frame = pd.read_csv(SAMPLE_PATH, dtype=str).iloc[:8].copy()
    frame["invoice"] = ["S1", "C1", "A1", "S2", "C2", "S3", "S4", "C3"]
    frame["quantity"] = ["3", "-2", "-4", "5", "1", "1", "0", "-3"]
    frame["price"] = ["2", "2", "2", "0", "2", "-1", "2", "1.25"]
    frame["stock_code"] = ["X"] * 7 + ["Y"]
    frame["country"] = "UK"
    path = tmp_path / "adjustments.csv"
    frame.to_csv(path, index=False)
    load_online_retail(path, mode="replace")
    build_staging()
    build_marts()
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT total_quantity,total_revenue,transaction_count,"
            "return_quantity,return_revenue,return_count "
            "FROM marts.fct_daily_product_sales ORDER BY stock_code"
        ).fetchall()
    assert rows == [(3, 6, 1, 2, 4, 1), (0, 0, 0, 3, 3.75, 1)]
