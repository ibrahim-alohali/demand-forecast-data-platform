# One-day forecasting evaluation

Status: **evaluated**.

Target: next calendar day's recorded paid gross sales quantity per product and country. Returns are reported separately; this is not net sales.

## Method

- LinearRegression with a nonnegative prediction floor of zero.
- Inputs: lag_1d_quantity, lag_7d_quantity, rolling_7d_quantity, weekday.
- Every predictor for date t uses information available through t−1, plus the known weekday of t.
- Earliest 80% of eligible dates train the model; latest 20% evaluate it. The fitted model stays fixed; prior observed evaluation days update lags.
- Train: 2009-12-08 to 2011-07-15 (8,333,271 rows).
- Test: 2011-07-16 to 2011-12-09 (3,754,801 rows).

## Coverage

- Calendar rows: 12,286,085; eligible: 12,088,072.
- Excluded for missing seven-day history: 198,013.
- Eligible target dates: 732.

## Metrics

All methods use the same evaluation observations. null means undefined or not evaluable. MAE and RMSE are in units per product-country-day.

| Method | MAE | RMSE | R-squared | Evaluation rows |
|---|---:|---:|---:|---:|
| LinearRegression | 1.3264 | 11.144 | 0.0609 | 3754801 |
| previous_day | 1.0718 | 14.9024 | -0.6794 | 3754801 |
| previous_weekday | 1.0182 | 14.3743 | -0.5625 | 3754801 |

## Limitations

- Missing transaction days are zero recorded sales, not proof of zero demand. Inventory availability and stockouts are unknown.
- Series begin at first recorded eligible sale/return and continue to the shared dataset end. New series need seven days of observations.
- Pooled linear baseline, fixed split, no tuning or external validation. Global metrics can obscure differences between products and countries.
- R-squared is null for fewer than two observations or constant targets.
- Source files/sheets, coverage, coefficients and explicitly selected error examples are available in the companion evaluation JSON.
