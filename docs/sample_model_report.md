# Baseline Model Report (full dataset)

Results from running the baseline model on the full UCI Online Retail II dataset.

## Model

- Type: LinearRegression (scikit-learn)
- Target variable: total_quantity

## Features

total_revenue, return_quantity, transaction_count, avg_unit_price, rolling_7d_quantity, rolling_7d_revenue, days_since_first_seen, distinct_countries

## Data

- Total rows: 593,206
- Date range: 2009-12-01 to 2011-12-09
- Distinct dates: 739
- Distinct products: 5,295
- Distinct countries: 43

## Train/test split

- Method: time-based (last 20% of dates as test set)
- Train rows: 449,317
- Test rows: 143,889

## Metrics

| Metric | Value |
|--------|-------|
| MAE | 12.35 |
| RMSE | 87.12 |
| R-squared | 0.85 |

## Limitations

- Baseline linear model with no hyperparameter tuning.
- Feature set is minimal (8 features).
- Model exists to validate the pipeline, not for production forecasting.
- Better models (gradient boosting, time-series specific) would likely improve accuracy.
