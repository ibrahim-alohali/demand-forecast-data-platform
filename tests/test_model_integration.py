"""Integration tests for baseline model. Require a running PostgreSQL instance.

Run with: python -m pytest tests/ -m integration
"""

import json
from pathlib import Path

import pytest

from src.db import get_connection
from src.features.build_features import build_features
from src.ingestion.load_online_retail import (
    SAMPLE_PATH,
    load_online_retail,
)
from src.ingestion.load_online_retail import (
    _create_table as create_raw_table,
)
from src.marts.build_marts import build_marts
from src.model.train_baseline import train_baseline
from src.staging.build_staging import build_staging

pytestmark = pytest.mark.integration

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
EVAL_PATH = DATA_DIR / "model_evaluation.json"
REPORT_PATH = DATA_DIR / "model_report.md"


@pytest.fixture(autouse=True)
def _load_pipeline():
    """Load sample data, build full pipeline, train model, then clean up."""
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
    build_features()
    train_baseline()

    yield

    conn = get_connection()
    try:
        conn.execute("TRUNCATE features.product_daily_features")
        conn.execute("TRUNCATE marts.fct_daily_product_sales")
        conn.execute("TRUNCATE marts.dim_product")
        conn.execute("TRUNCATE staging.stg_online_retail")
        conn.execute("TRUNCATE raw.online_retail")
        conn.commit()
    finally:
        conn.close()

    # Clean up generated files
    for path in [EVAL_PATH, REPORT_PATH]:
        if path.exists():
            path.unlink()


def test_evaluation_json_exists():
    assert EVAL_PATH.exists(), "model_evaluation.json not created"


def test_evaluation_json_valid():
    with open(EVAL_PATH) as f:
        data = json.load(f)
    assert isinstance(data, dict)


def test_evaluation_has_required_keys():
    with open(EVAL_PATH) as f:
        data = json.load(f)
    for key in ["mae", "rmse", "r2"]:
        assert key in data, f"Missing key: {key}"
        assert isinstance(data[key], (int, float)), f"{key} is not numeric"


def test_mae_non_negative():
    with open(EVAL_PATH) as f:
        data = json.load(f)
    assert data["mae"] >= 0


def test_rmse_non_negative():
    with open(EVAL_PATH) as f:
        data = json.load(f)
    assert data["rmse"] >= 0


def test_report_exists():
    assert REPORT_PATH.exists(), "model_report.md not created"


def test_report_has_limitations():
    text = REPORT_PATH.read_text()
    assert "## Limitations" in text
