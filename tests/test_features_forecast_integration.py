"""Forecast regressions against calendar gaps and information leakage."""

from datetime import date

import pandas as pd
import pytest

from src.db import get_connection
from src.features.build_features import SQL_FILES, build_features
from src.model.train_baseline import FEATURE_COLUMNS

pytestmark = pytest.mark.integration


@pytest.fixture
def calendar_facts():
    with get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS marts")
        conn.execute("CREATE SCHEMA IF NOT EXISTS features")
        for name in ["marts_fct_daily_product_sales.sql", "marts_dim_product.sql"]:
            conn.execute((SQL_FILES["ddl"].parent / name).read_text())
        conn.execute("TRUNCATE marts.fct_daily_product_sales, marts.dim_product")
        conn.execute("""
            INSERT INTO marts.dim_product VALUES
            ('A', 'A', '2020-01-01', '2020-01-15', 2),
            ('B', 'B', '2020-01-08', '2020-01-08', 1)
        """)
        conn.execute("""
            INSERT INTO marts.fct_daily_product_sales VALUES
            ('A', '2020-01-01', 'UK', 7, 14, 1, 0, 0, 0),
            ('A', '2020-01-08', 'UK', 14, 28, 1, 0, 0, 0),
            ('A', '2020-01-15', 'UK', 21, 42, 1, 0, 0, 0),
            ('A', '2020-01-01', 'France', 70, 140, 1, 0, 0, 0),
            ('B', '2020-01-08', 'UK', 700, 1400, 1, 0, 0, 0)
        """)
    build_features()


def _features():
    with get_connection() as conn:
        cursor = conn.execute("""
            SELECT * FROM features.product_daily_features
            ORDER BY stock_code, country, sale_date
        """)
        return pd.DataFrame(
            cursor.fetchall(), columns=[d.name for d in cursor.description]
        )


def test_calendar_gaps_and_series_partition(calendar_facts):
    df = _features().set_index(["stock_code", "country", "sale_date"])
    assert len(df) == 38  # 15 + 15 + 8; only observed product-country pairs.
    uk = df.loc[("A", "UK", date(2020, 1, 8))]
    assert uk["lag_1d_quantity"] == 0
    assert uk["lag_7d_quantity"] == 7
    assert uk["rolling_7d_quantity"] == 1
    assert df.loc[("A", "France", date(2020, 1, 8)), "lag_7d_quantity"] == 70
    assert pd.isna(df.loc[("B", "UK", date(2020, 1, 8)), "lag_7d_quantity"])
    assert df.loc[("B", "UK", date(2020, 1, 15)), "lag_7d_quantity"] == 700
    assert df.loc[("A", "France", date(2020, 1, 15)), "total_quantity"] == 0
    assert df.loc[("A", "UK", date(2020, 1, 15)), "rolling_7d_quantity"] == 2


@pytest.mark.parametrize("change_date", ["2020-01-08", "2020-01-15"])
def test_target_and_future_changes_leave_earlier_predictors_unchanged(
    calendar_facts, change_date
):
    before = _features()
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE marts.fct_daily_product_sales
            SET total_quantity = total_quantity * 100,
                total_revenue = total_revenue * 100
            WHERE sale_date = %s
        """,
            (change_date,),
        )
    build_features()
    after = _features()
    mask = before["sale_date"] <= date(2020, 1, 8)
    pd.testing.assert_frame_equal(
        before.loc[mask, FEATURE_COLUMNS], after.loc[mask, FEATURE_COLUMNS]
    )


def test_feature_refresh_rolls_back_schema_and_data(
    calendar_facts, monkeypatch, tmp_path
):
    before = _features()
    failing_sql = tmp_path / "fail.sql"
    failing_sql.write_text("SELECT 1 / 0;")
    monkeypatch.setitem(SQL_FILES, "insert", failing_sql)
    with pytest.raises(Exception, match="division by zero"):
        build_features()
    pd.testing.assert_frame_equal(before, _features())


def test_appending_future_dates_preserves_prior_predictors(calendar_facts):
    before = _features().set_index(["stock_code", "country", "sale_date"])
    with get_connection() as conn:
        conn.execute("""
            INSERT INTO marts.fct_daily_product_sales VALUES
            ('A', '2020-01-20', 'UK', 100, 200, 1, 0, 0, 0)
        """)
    build_features()
    after = _features().set_index(["stock_code", "country", "sale_date"])
    pd.testing.assert_frame_equal(
        before[FEATURE_COLUMNS], after.loc[before.index, FEATURE_COLUMNS]
    )


def test_rebuild_migrates_old_feature_schema_and_preserves_sources(calendar_facts):
    with get_connection() as conn:
        old_facts = conn.execute(
            "SELECT * FROM marts.fct_daily_product_sales "
            "ORDER BY stock_code, country, sale_date"
        ).fetchall()
        conn.execute("""
            ALTER TABLE features.product_daily_features
            DROP COLUMN lag_1d_quantity, DROP COLUMN lag_7d_quantity,
            DROP COLUMN weekday
        """)
    build_features()
    assert set(FEATURE_COLUMNS) <= set(_features().columns)
    with get_connection() as conn:
        assert (
            old_facts
            == conn.execute(
                "SELECT * FROM marts.fct_daily_product_sales "
                "ORDER BY stock_code, country, sale_date"
            ).fetchall()
        )
