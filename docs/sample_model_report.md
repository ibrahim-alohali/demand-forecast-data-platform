# Sample forecasting report

The checked-in sample contains only one calendar date. It is useful for demonstrating ingestion, cleaning, analytical tables and report generation. It cannot support a one-day-ahead forecasting evaluation.

After the sample pipeline is built, generate its separate reports with:

```bash
python -m src.model.train_baseline --output-dir data/sample --source-file data/sample_online_retail.csv
```

The evaluation JSON must report `status: "not_evaluable"`, `split_method: "time-based"`, zero training/evaluation rows and null `mae`, `rmse` and `r2`. Both naive comparators have null scores too. Every sample feature row lacks the required seven prior calendar days and is counted in `coverage.excluded_history_rows`. The code does not fall back to splitting one day's products randomly.

The verified 100-row sample produces 89 product-country-date feature rows. All 89 are excluded for insufficient history; eligible rows, training rows and evaluation rows are zero. Those counts describe the sample pipeline, not forecasting accuracy.

## Full-data evaluation

Run the complete workbook pipeline before evaluating the full period, then use a different output directory:

```bash
python -m src.model.train_baseline --output-dir data/full --source-file data/online_retail_ii.xlsx
```

These commands evaluate the feature table currently loaded in PostgreSQL. `--source-file` attaches the file's name and SHA-256; it does not load that file or prove that the current tables were built from it. The JSON also records the current raw files, worksheet names and row counts. Match those with the pipeline run's reconciliation before citing results.

Evaluation uses the four predictors defined in the [feature registry](feature_registry.md), an 80/20 chronological split of eligible dates, and identical evaluation rows for linear regression, previous-day and previous-weekday forecasts. It records date boundaries, history exclusions, new test series, coefficients and the smallest, nearest-median and largest absolute-error examples. R-squared is null for fewer than two evaluation observations or a constant target.

The old same-day model report has been removed because its predictors included information from the day being predicted. Its scores must not appear as forecasting results in the CV, portfolio or README. Use only the corrected full-run report and its matching verification evidence.

## Limitations

The target is recorded **paid gross sales quantity**, not unconstrained demand or net sales after returns. Missing transaction days are zeros in the observed record; closures and stockouts cannot be distinguished. A fixed pooled linear model and one holdout period do not establish performance on other retailers, periods or product ranges. Better performance than either naive comparator is an empirical question, not a requirement for publishing an honest result.
