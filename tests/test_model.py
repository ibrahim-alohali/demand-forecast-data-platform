"""Unit tests for baseline model. No database required."""

import importlib
import json

import numpy as np
import pandas as pd
import pytest

from src.model.train_baseline import FEATURE_COLUMNS, TARGET, _evaluate, _metrics


def test_module_importable():
    from src.model import train_baseline  # noqa: F401


def test_feature_columns_defined():
    assert isinstance(FEATURE_COLUMNS, list)
    assert len(FEATURE_COLUMNS) > 0


def test_target_defined():
    assert isinstance(TARGET, str)
    assert TARGET == "total_quantity"


def test_feature_columns_are_strings():
    for col in FEATURE_COLUMNS:
        assert isinstance(col, str)


def test_target_not_in_features():
    assert TARGET not in FEATURE_COLUMNS


def test_only_predictors_available_before_the_target_day():
    assert FEATURE_COLUMNS == [
        "lag_1d_quantity",
        "lag_7d_quantity",
        "rolling_7d_quantity",
        "weekday",
    ]


def _daily_frame(n_dates=20):
    dates = pd.date_range("2020-01-01", periods=n_dates)
    # Independent small fixture; first seven rows deliberately lack history.
    frame = pd.DataFrame(
        {
            "sale_date": dates,
            "stock_code": "A",
            "country": "UK",
            "total_quantity": np.arange(n_dates) % 5,
            "lag_1d_quantity": [np.nan] + list(np.arange(n_dates - 1) % 5),
            "lag_7d_quantity": [np.nan] * min(7, n_dates)
            + list(np.arange(max(0, n_dates - 7)) % 5),
            "rolling_7d_quantity": [np.nan] * min(7, n_dates)
            + [2.0] * max(0, n_dates - 7),
            "weekday": dates.weekday,
        }
    )
    return frame


@pytest.mark.parametrize("n_dates", [0, 1, 7, 8])
def test_insufficient_history_never_produces_a_score(n_dates, monkeypatch, tmp_path):
    module = importlib.import_module("src.model.train_baseline")
    frame = _daily_frame(n_dates) if n_dates else _daily_frame().iloc[:0]
    monkeypatch.setattr(module, "_load_features", lambda: frame)
    monkeypatch.setattr(module, "DATA_DIR", tmp_path)
    monkeypatch.setattr(module, "PROJECT_ROOT", tmp_path)
    result = module.train_baseline()
    assert result["status"] == "not_evaluable"
    assert all(result[key] is None for key in ("mae", "rmse", "r2"))
    assert result["split_method"] == "time-based"


def test_chronological_cutoff_and_identical_baseline_observations():
    result = _evaluate(_daily_frame())
    assert result["status"] == "evaluated"
    assert result["cutoff_date"] == "2020-01-18"
    assert result["train_date_start"] == "2020-01-08"
    assert result["train_date_end"] == "2020-01-17"
    assert result["test_date_start"] == "2020-01-18"
    assert result["test_date_end"] == "2020-01-20"
    assert result["n_train"] == 10
    assert result["n_test"] == 3
    assert result["coverage"]["excluded_history_rows"] == 7
    assert result["baselines"]["previous_day"] == _metrics([2, 3, 4], [1, 2, 3])
    assert result["baselines"]["previous_weekday"] == _metrics([2, 3, 4], [0, 1, 2])
    assert all(b["n_test"] == result["n_test"] for b in result["baselines"].values())


@pytest.mark.parametrize("actual", [[5], [5, 5], [0, 0]])
def test_undefined_r_squared_is_null(actual):
    assert _metrics(actual, np.zeros(len(actual)))["r2"] is None


def test_one_evaluation_observation_has_no_r_squared():
    result = _evaluate(_daily_frame(9))
    assert result["status"] == "evaluated"
    assert result["n_test"] == 1
    assert result["r2"] is None


def test_predictions_have_nonnegative_floor(monkeypatch):
    module = importlib.import_module("src.model.train_baseline")

    class NegativeModel:
        coef_ = np.zeros(4)
        intercept_ = -10

        def fit(self, x, y):
            return self

        def predict(self, x):
            return np.full(len(x), -10)

    monkeypatch.setattr(module, "LinearRegression", NegativeModel)
    result = module._evaluate(_daily_frame())
    assert result["mae"] == 3
    assert all(example["prediction"] == 0 for example in result["error_examples"])


def test_reports_use_requested_directory_and_strict_json(monkeypatch, tmp_path):
    module = importlib.import_module("src.model.train_baseline")
    monkeypatch.setattr(module, "_load_features", lambda: _daily_frame(9))
    result = module.train_baseline(tmp_path, source_identity={"sha256": "fixture"})
    saved = json.loads((tmp_path / "model_evaluation.json").read_text())
    assert saved == result
    assert saved["source"]["sha256"] == "fixture"
    assert saved["r2"] is None
    assert "## Limitations" in (tmp_path / "model_report.md").read_text(
        encoding="utf-8"
    )


@pytest.mark.parametrize("fault", ["schema", "duplicate", "infinite", "negative"])
def test_invalid_features_fail_explicitly(fault):
    df = _daily_frame()
    if fault == "schema":
        df = df.drop(columns=["lag_1d_quantity"])
    elif fault == "duplicate":
        df = pd.concat([df, df.iloc[:1]], ignore_index=True)
    elif fault == "infinite":
        df.loc[8, "rolling_7d_quantity"] = np.inf
    else:
        df.loc[8, "total_quantity"] = -1
    with pytest.raises(ValueError):
        _evaluate(df)


def test_new_series_and_excluded_test_history_are_disclosed():
    old = _daily_frame(20)
    new = _daily_frame(10)
    new["sale_date"] += pd.Timedelta(days=10)
    new["weekday"] = new["sale_date"].dt.weekday
    new["stock_code"] = "B"
    cold = _daily_frame(3)
    cold["sale_date"] += pd.Timedelta(days=17)
    cold["weekday"] = cold["sale_date"].dt.weekday
    cold["stock_code"] = "C"
    result = _evaluate(pd.concat([old, new, cold], ignore_index=True))
    assert result["coverage"]["test_series_absent_from_training"] == 1
    assert result["coverage"]["excluded_history_rows_in_test_period"] == 3
