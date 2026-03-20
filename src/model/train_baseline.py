"""Train a baseline linear regression on the feature table.

Reads from features.product_daily_features, trains a LinearRegression,
evaluates on a held-out set, and saves results to data/.

Usage:
    python -m src.model.train_baseline
"""

from __future__ import annotations

import json
import sys
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
    "total_revenue",
    "return_quantity",
    "transaction_count",
    "avg_unit_price",
    "rolling_7d_quantity",
    "rolling_7d_revenue",
    "days_since_first_seen",
    "distinct_countries",
]


def _load_features() -> pd.DataFrame:
    """Read all rows from the feature table into a DataFrame."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM features.product_daily_features"
        ).fetchall()
        columns = [
            desc.name
            for desc in conn.execute(
                "SELECT * FROM features.product_daily_features LIMIT 0"
            ).description
        ]
    finally:
        conn.close()
    return pd.DataFrame(rows, columns=columns)


def _json_serializable(obj: object) -> object:
    """Convert numpy/date types for JSON serialization."""
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, date):
        return obj.isoformat()
    return obj


def train_baseline() -> dict:
    """Train baseline model and return evaluation metrics.

    Returns:
        Dictionary with evaluation results.
    """
    df = _load_features()

    if len(df) == 0:
        print("Error: feature table is empty. Run the pipeline first.")
        sys.exit(1)

    distinct_dates = df["sale_date"].nunique()
    warnings: list[str] = []

    if distinct_dates < 7:
        msg = (
            f"Warning: only {distinct_dates} distinct sale date(s). "
            "Results are not meaningful due to limited temporal data."
        )
        print(msg)
        warnings.append(msg)

    # Fill nulls in avg_unit_price (NULL when total_quantity is 0)
    df["avg_unit_price"] = df["avg_unit_price"].fillna(0)

    X = df[FEATURE_COLUMNS]
    y = df[TARGET]

    # Train/test split
    if distinct_dates > 1:
        sorted_dates = sorted(df["sale_date"].unique())
        split_idx = max(1, int(len(sorted_dates) * 0.8))
        cutoff_date = sorted_dates[split_idx]
        train_mask = df["sale_date"] < cutoff_date
        test_mask = df["sale_date"] >= cutoff_date
        split_method = "time-based"
    else:
        rng = np.random.default_rng(42)
        n = len(df)
        indices = rng.permutation(n)
        split_point = int(n * 0.8)
        train_mask = pd.Series(False, index=df.index)
        test_mask = pd.Series(False, index=df.index)
        train_mask.iloc[indices[:split_point]] = True
        test_mask.iloc[indices[split_point:]] = True
        split_method = "random"
        warnings.append(
            "Single sale date: used random split instead of time-based."
        )

    X_train, X_test = X[train_mask], X[test_mask]
    y_train, y_test = y[train_mask], y[test_mask]

    model = LinearRegression()
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)

    mae = mean_absolute_error(y_test, y_pred)
    rmse = float(np.sqrt(mean_squared_error(y_test, y_pred)))
    r2 = r2_score(y_test, y_pred)

    date_range_start = df["sale_date"].min()
    date_range_end = df["sale_date"].max()

    evaluation = {
        "mae": round(mae, 4),
        "rmse": round(rmse, 4),
        "r2": round(r2, 4),
        "n_train": int(len(X_train)),
        "n_test": int(len(X_test)),
        "n_features": len(FEATURE_COLUMNS),
        "feature_columns": FEATURE_COLUMNS,
        "split_method": split_method,
        "date_range_start": date_range_start,
        "date_range_end": date_range_end,
        "distinct_dates": int(distinct_dates),
        "warning": warnings if warnings else None,
    }

    # Save evaluation JSON
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    eval_path = DATA_DIR / "model_evaluation.json"
    with open(eval_path, "w") as f:
        json.dump(evaluation, f, indent=2, default=_json_serializable)
    print(f"Saved evaluation to {eval_path.relative_to(PROJECT_ROOT)}")

    # Generate report
    report = _generate_report(df, evaluation)
    report_path = DATA_DIR / "model_report.md"
    with open(report_path, "w") as f:
        f.write(report)
    print(f"Saved report to {report_path.relative_to(PROJECT_ROOT)}")

    # Print summary
    print()
    print("Model:          LinearRegression")
    print(f"Target:         {TARGET}")
    print(f"Features:       {len(FEATURE_COLUMNS)}")
    print(f"Train rows:     {len(X_train)}")
    print(f"Test rows:      {len(X_test)}")
    print(f"Split method:   {split_method}")
    print(f"MAE:            {mae:.4f}")
    print(f"RMSE:           {rmse:.4f}")
    print(f"R-squared:      {r2:.4f}")

    return evaluation


def _generate_report(df: pd.DataFrame, evaluation: dict) -> str:
    """Generate a markdown report summarizing the baseline model."""
    lines = [
        "# Baseline Model Report",
        "",
        "## Model",
        "",
        "- Type: LinearRegression (scikit-learn)",
        f"- Target variable: {TARGET}",
        "",
        "## Features",
        "",
    ]
    for feat in FEATURE_COLUMNS:
        lines.append(f"- {feat}")

    lines += [
        "",
        "## Data",
        "",
        f"- Total rows: {len(df)}",
        f"- Date range: {evaluation['date_range_start']} to "
        f"{evaluation['date_range_end']}",
        f"- Distinct dates: {evaluation['distinct_dates']}",
        f"- Distinct products: {df['stock_code'].nunique()}",
        f"- Distinct countries: {df['country'].nunique()}",
        "",
        "## Train/test split",
        "",
        f"- Method: {evaluation['split_method']}",
        f"- Train rows: {evaluation['n_train']}",
        f"- Test rows: {evaluation['n_test']}",
        "",
        "## Metrics",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| MAE | {evaluation['mae']} |",
        f"| RMSE | {evaluation['rmse']} |",
        f"| R-squared | {evaluation['r2']} |",
        "",
        "## Limitations",
        "",
    ]

    limitations = [
        "- Baseline linear model with no hyperparameter tuning.",
        "- Feature set is minimal (8 features).",
        "- Model exists to validate the pipeline, not for production forecasting.",
    ]

    if evaluation["distinct_dates"] < 7:
        limitations.insert(
            1,
            "- Limited temporal data: temporal patterns are not captured.",
        )

    if evaluation["split_method"] == "random":
        limitations.insert(
            1,
            "- Single-day data: time-based split was not possible.",
        )

    lines += limitations
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    train_baseline()


if __name__ == "__main__":
    main()
