"""Sample pipeline writes honest, isolated non-evaluation reports."""

import json

import pytest

from src.db import get_connection
from src.features.build_features import build_features
from src.ingestion.load_online_retail import SAMPLE_PATH, load_online_retail
from src.ingestion.load_online_retail import _create_table as create_raw_table
from src.marts.build_marts import build_marts
from src.model.train_baseline import train_baseline
from src.staging.build_staging import build_staging

pytestmark = pytest.mark.integration


@pytest.fixture
def pipeline_outputs(tmp_path):
    with get_connection() as conn:
        create_raw_table(conn)
        conn.execute("TRUNCATE raw.online_retail")
    load_online_retail(SAMPLE_PATH, mode="safe")
    build_staging()
    build_marts()
    build_features()
    result = train_baseline(output_dir=tmp_path)
    yield tmp_path, result
    with get_connection() as conn:
        conn.execute("TRUNCATE features.product_daily_features")
        conn.execute("TRUNCATE marts.fct_daily_product_sales, marts.dim_product")
        conn.execute("TRUNCATE staging.stg_online_retail, raw.online_retail")


def test_evaluation_json_exists(pipeline_outputs):
    directory, _ = pipeline_outputs
    assert (directory / "model_evaluation.json").exists()


def test_evaluation_json_valid(pipeline_outputs):
    directory, result = pipeline_outputs
    saved = json.loads((directory / "model_evaluation.json").read_text())
    assert saved == result


def test_evaluation_has_required_keys(pipeline_outputs):
    _, result = pipeline_outputs
    assert result["status"] == "not_evaluable"
    assert result["distinct_dates"] == 1
    assert result["forecast_horizon_days"] == 1
    assert result["split_method"] == "time-based"
    assert result["source"]["raw_rows"] > 0


def test_no_fabricated_sample_metrics(pipeline_outputs):
    _, result = pipeline_outputs
    for method in [result, *result["baselines"].values()]:
        assert all(method[key] is None for key in ["mae", "rmse", "r2"])
        assert method["n_test"] == 0


def test_all_sample_rows_excluded_for_history(pipeline_outputs):
    _, result = pipeline_outputs
    coverage = result["coverage"]
    assert coverage["eligible_rows"] == 0
    assert coverage["excluded_history_rows"] == coverage["total_rows"]


def test_report_exists(pipeline_outputs):
    directory, _ = pipeline_outputs
    assert (directory / "model_report.md").exists()


def test_report_has_limitations(pipeline_outputs):
    directory, _ = pipeline_outputs
    text = (directory / "model_report.md").read_text(encoding="utf-8")
    assert "## Limitations" in text
    assert "not_evaluable" in text
