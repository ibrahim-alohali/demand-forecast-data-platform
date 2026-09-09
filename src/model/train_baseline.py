"""Evaluate one-calendar-day-ahead recorded paid gross sales forecasts.

Usage: python -m src.model.train_baseline --output-dir data/sample
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from src.db import get_connection

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
TARGET = "total_quantity"
FEATURE_COLUMNS = [
    "lag_1d_quantity",
    "lag_7d_quantity",
    "rolling_7d_quantity",
    "weekday",
]
KEY_COLUMNS = ["stock_code", "country", "sale_date"]


def _load_features() -> pd.DataFrame:
    """Stream only model columns; categorical keys limit full-workbook memory use."""
    columns = KEY_COLUMNS + [TARGET] + FEATURE_COLUMNS
    chunks = []
    with get_connection() as conn:
        stocks = [
            r[0]
            for r in conn.execute(
                "SELECT stock_code FROM marts.dim_product ORDER BY stock_code"
            )
        ]
        countries = [
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT country FROM marts.fct_daily_product_sales "
                "ORDER BY country"
            )
        ]
        sources = conn.execute("""
            SELECT source_file, source_sheet, COUNT(*) FROM raw.online_retail
            GROUP BY source_file, source_sheet ORDER BY source_file, source_sheet
        """).fetchall()
        with conn.cursor(name="forecast_features") as cursor:
            cursor.execute(
                "SELECT " + ", ".join(columns) + " FROM features.product_daily_features"
            )
            while rows := cursor.fetchmany(100_000):
                chunk = pd.DataFrame.from_records(rows, columns=columns)
                chunk["stock_code"] = pd.Categorical(chunk["stock_code"], stocks)
                chunk["country"] = pd.Categorical(chunk["country"], countries)
                chunk["sale_date"] = pd.to_datetime(chunk["sale_date"])
                chunk[FEATURE_COLUMNS] = chunk[FEATURE_COLUMNS].astype(float)
                chunks.append(chunk)
    df = (
        pd.concat(chunks, ignore_index=True)
        if chunks
        else pd.DataFrame(columns=columns)
    )
    df.attrs["source"] = {
        "raw_rows": sum(row[2] for row in sources),
        "files": [{"file": f, "sheet": s, "rows": n} for f, s, n in sources],
    }
    return df


def _json_serializable(obj: object) -> object:
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, date):
        return obj.isoformat()
    raise TypeError(f"Cannot serialize {type(obj).__name__}")


def _date(value) -> str | None:
    return None if pd.isna(value) else pd.Timestamp(value).date().isoformat()


def _metrics(actual, prediction) -> dict:
    """R-squared is undefined for fewer than two or constant observed targets."""
    r2 = None
    if len(actual) >= 2 and np.ptp(np.asarray(actual)) > 0:
        score = float(r2_score(actual, prediction, force_finite=False))
        r2 = round(score, 4) if np.isfinite(score) else None
    return {
        "mae": round(float(mean_absolute_error(actual, prediction)), 4),
        "rmse": round(float(np.sqrt(mean_squared_error(actual, prediction))), 4),
        "r2": r2,
        "n_test": int(len(actual)),
    }


def _evaluate(df: pd.DataFrame) -> dict:
    required = KEY_COLUMNS + [TARGET] + FEATURE_COLUMNS
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise ValueError(
            f"Missing feature columns: {', '.join(missing)}; rebuild features."
        )
    if df[KEY_COLUMNS + [TARGET, "weekday"]].isna().any().any():
        raise ValueError("Feature keys, weekday and target must not be null.")
    if df.duplicated(KEY_COLUMNS).any():
        raise ValueError("Duplicate product-country-date rows in feature table.")
    numeric = df[[TARGET] + FEATURE_COLUMNS].astype(float)
    if np.isinf(numeric.to_numpy()).any() or (numeric[TARGET] < 0).any():
        raise ValueError("Forecast values must be finite and target nonnegative.")

    eligible_mask = df[FEATURE_COLUMNS].notna().all(axis=1)
    eligible = df.loc[eligible_mask].sort_values(["sale_date", "stock_code", "country"])
    dates = sorted(eligible["sale_date"].unique())
    empty_metrics = {"mae": None, "rmse": None, "r2": None, "n_test": 0}
    evaluation = {
        **empty_metrics,
        "schema_version": 2,
        "status": "not_evaluable",
        "reason": None,
        "target": "next-day recorded paid gross sales quantity",
        "forecast_horizon_days": 1,
        "n_train": 0,
        "n_features": len(FEATURE_COLUMNS),
        "feature_columns": FEATURE_COLUMNS,
        "split_method": "time-based",
        "cutoff_date": None,
        "train_date_start": None,
        "train_date_end": None,
        "test_date_start": None,
        "test_date_end": None,
        "date_range_start": _date(df["sale_date"].min()),
        "date_range_end": _date(df["sale_date"].max()),
        "distinct_dates": int(df["sale_date"].nunique()),
        "coverage": {
            "total_rows": len(df),
            "eligible_rows": len(eligible),
            "excluded_history_rows": int((~eligible_mask).sum()),
            "eligible_dates": len(dates),
            "series": len(df[["stock_code", "country"]].drop_duplicates()),
            "excluded_history_rows_in_test_period": None,
            "test_series_absent_from_training": None,
        },
        "baselines": {
            "previous_day": dict(empty_metrics),
            "previous_weekday": dict(empty_metrics),
        },
        "source": df.attrs.get("source"),
        "prediction_floor": 0,
        "error_examples": [],
        "warning": None,
    }
    if len(dates) < 2:
        evaluation["reason"] = (
            "Need at least two eligible target dates after seven full calendar days "
            "of series history. Empty, single-day and short-history samples cannot "
            "support forecasting evaluation."
        )
        return evaluation

    cutoff = dates[max(1, int(len(dates) * 0.8))]
    train = eligible.loc[eligible["sale_date"] < cutoff]
    test = eligible.loc[eligible["sale_date"] >= cutoff]
    model = LinearRegression()
    model.fit(train[FEATURE_COLUMNS], train[TARGET])
    prediction = np.maximum(model.predict(test[FEATURE_COLUMNS]), 0)
    actual = test[TARGET].to_numpy()
    evaluation.update(_metrics(actual, prediction))
    evaluation.update(
        {
            "status": "evaluated",
            "n_train": len(train),
            "cutoff_date": _date(cutoff),
            "train_date_start": _date(train["sale_date"].min()),
            "train_date_end": _date(train["sale_date"].max()),
            "test_date_start": _date(test["sale_date"].min()),
            "test_date_end": _date(test["sale_date"].max()),
            "baselines": {
                "previous_day": _metrics(actual, test["lag_1d_quantity"].to_numpy()),
                "previous_weekday": _metrics(
                    actual, test["lag_7d_quantity"].to_numpy()
                ),
            },
            "coefficients": dict(
                zip(FEATURE_COLUMNS, model.coef_.tolist(), strict=True)
            ),
            "intercept": float(model.intercept_),
        }
    )
    train_series = pd.MultiIndex.from_frame(train[["stock_code", "country"]])
    test_series = pd.MultiIndex.from_frame(test[["stock_code", "country"]])
    evaluation["coverage"].update(
        {
            "excluded_history_rows_in_test_period": int(
                ((~eligible_mask) & (df["sale_date"] >= cutoff)).sum()
            ),
            "test_series_absent_from_training": len(
                test_series.unique().difference(train_series.unique())
            ),
        }
    )
    errors = np.abs(actual - prediction)
    examples = [
        ("smallest absolute error", int(np.argmin(errors))),
        (
            "nearest median absolute error",
            int(np.argmin(np.abs(errors - np.median(errors)))),
        ),
        ("largest absolute error", int(np.argmax(errors))),
    ]
    for label, position in examples:
        row = test.iloc[position]
        evaluation["error_examples"].append(
            {
                "selection": label,
                "stock_code": row["stock_code"],
                "country": row["country"],
                "sale_date": _date(row["sale_date"]),
                "actual": float(actual[position]),
                "prediction": float(prediction[position]),
                "absolute_error": float(errors[position]),
                "previous_day": float(row["lag_1d_quantity"]),
                "previous_weekday": float(row["lag_7d_quantity"]),
            }
        )
    return evaluation


def _generate_report(df: pd.DataFrame, evaluation: dict) -> str:
    e = evaluation
    lines = [
        "# One-day forecasting evaluation",
        "",
        f"Status: **{e['status']}**. {e['reason'] or ''}".rstrip(),
        "",
        "Target: next calendar day's recorded paid gross sales quantity per "
        "product and country. Returns are reported separately; this is not net sales.",
        "",
        "## Method",
        "",
        "- LinearRegression with a nonnegative prediction floor of zero.",
        f"- Inputs: {', '.join(FEATURE_COLUMNS)}.",
        "- Every predictor for date t uses information available through t−1, "
        "plus the known weekday of t.",
        "- Earliest 80% of eligible dates train the model; latest 20% evaluate it. "
        "The fitted model stays fixed; prior observed evaluation days update lags.",
        f"- Train: {e['train_date_start']} to {e['train_date_end']} "
        f"({e['n_train']:,} rows).",
        f"- Test: {e['test_date_start']} to {e['test_date_end']} "
        f"({e['n_test']:,} rows).",
        "",
        "## Coverage",
        "",
        f"- Calendar rows: {len(df):,}; eligible: {e['coverage']['eligible_rows']:,}.",
        f"- Excluded for missing seven-day history: "
        f"{e['coverage']['excluded_history_rows']:,}.",
        f"- Eligible target dates: {e['coverage']['eligible_dates']}.",
        "",
        "## Metrics",
        "",
        "All methods use the same evaluation observations. null means undefined "
        "or not evaluable. MAE and RMSE are in units per product-country-day.",
        "",
        "| Method | MAE | RMSE | R-squared | Evaluation rows |",
        "|---|---:|---:|---:|---:|",
    ]
    for name, result in [("LinearRegression", e), *e["baselines"].items()]:
        values = [
            "null" if result[k] is None else str(result[k])
            for k in ("mae", "rmse", "r2", "n_test")
        ]
        lines.append(f"| {name} | " + " | ".join(values) + " |")
    lines += [
        "",
        "## Limitations",
        "",
        "- Missing transaction days are zero recorded sales, not proof of zero "
        "demand. Inventory availability and stockouts are unknown.",
        "- Series begin at first recorded eligible sale/return and continue to "
        "the shared dataset end. New series need seven days of observations.",
        "- Pooled linear baseline, fixed split, no tuning or external validation. "
        "Global metrics can obscure differences between products and countries.",
        "- R-squared is null for fewer than two observations or constant targets.",
        "- Source files/sheets, coverage, coefficients and explicitly selected "
        "error examples are available in the companion evaluation JSON.",
        "",
    ]
    return "\n".join(lines)


def train_baseline(
    output_dir: Path | str | None = None, source_identity: dict | None = None
) -> dict:
    """Evaluate and write reports; insufficient history is an explicit result."""
    df = _load_features()
    evaluation = _evaluate(df)
    if source_identity is not None:
        evaluation["source"] = {**(evaluation["source"] or {}), **source_identity}
    destination = Path(output_dir) if output_dir is not None else DATA_DIR
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "model_evaluation.json").write_text(
        json.dumps(evaluation, indent=2, default=_json_serializable, allow_nan=False)
        + "\n",
        encoding="utf-8",
    )
    (destination / "model_report.md").write_text(
        _generate_report(df, evaluation),
        encoding="utf-8",
    )
    print(f"Forecast evaluation: {evaluation['status']}; reports: {destination}")
    return evaluation


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DATA_DIR)
    parser.add_argument(
        "--source-file",
        type=Path,
        help="Attach this local input file's name and SHA-256 to results",
    )
    args = parser.parse_args()
    identity = None
    if args.source_file is not None:
        with args.source_file.open("rb") as source:
            digest = hashlib.file_digest(source, "sha256").hexdigest()
        identity = {"input_file": args.source_file.name, "sha256": digest}
    train_baseline(args.output_dir, source_identity=identity)


if __name__ == "__main__":
    main()
