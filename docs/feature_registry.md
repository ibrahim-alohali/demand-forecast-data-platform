# Forecast features and calendar rules

The feature table has one row per product (`stock_code`), country and calendar date. It supports a one-day-ahead forecast of **recorded paid gross sales quantity**. This is the positive quantity on eligible paid sales lines; returns remain separate. It does not estimate demand that could not be fulfilled, free units, stock availability or net sales after returns.

## Calendar and observation timing

For each product-country pair present in the sales/returns mart, create every calendar date from its first eligible record through the shared end of the mart's observation period. Do not stop at that pair's last recorded sale: that would use future knowledge to decide when to stop forecasting. Do not create combinations of products and countries that were never observed.

A missing transaction day contributes zero recorded quantity and revenue. This includes days the retailer may have been closed or unable to supply an item; the dataset does not identify those causes.

For a forecast dated `t`, use observations through `t−1` and the known weekday of `t`. Each series needs seven prior calendar days. Rows without that history remain in the feature table with null lag/mean values and are excluded from model fitting and evaluation. Evaluation output counts these exclusions, including those within the test period.

## Model input contract

Only these four columns enter the linear model:

| Column | Definition at target date `t` |
|---|---|
| `lag_1d_quantity` | Recorded paid gross quantity on `t−1`. |
| `lag_7d_quantity` | Recorded paid gross quantity on `t−7`. |
| `rolling_7d_quantity` | Mean quantity on `t−7` through `t−1`, including zero transaction days; null unless all seven calendar days exist. |
| `weekday` | Known target weekday, Monday `0` through Sunday `6`. |

`total_quantity` is the target, never a predictor. The previous-day and previous-weekday comparators use the two lag columns directly on exactly the model's evaluation rows. The linear model's predictions are floored at zero; quantities can otherwise remain fractional.

The earliest 80% of eligible target dates train the model and the latest 20% evaluate it. The split rounds the training date count down, with at least one training date. All rows on a date stay together. The model remains fixed during evaluation; each one-day forecast can use earlier observed evaluation days in its lags. This does not represent an unobserved multi-day forecast made at the cutoff.

## Descriptive columns retained

| Column | Use and limitation |
|---|---|
| `total_revenue` | Target-day paid gross sales amount; unavailable before the day and excluded from the model. |
| `return_quantity` | Target-day absolute units on eligible cancellation lines; excluded from the model. |
| `transaction_count` | Target-day distinct paid-sales invoices; excluded from the model. |
| `avg_unit_price` | Target-day gross revenue / gross quantity; null for zero quantity. Derived from target-day results and excluded. |
| `rolling_7d_revenue` | Mean gross revenue on the seven prior calendar days; null during warmup. Available historically, but not used by this model. |
| `days_since_first_seen` | Days since the product's first eligible record across countries. Descriptive context, not a model input. |
| `distinct_countries` | Country count across the entire loaded period. Contains future information for earlier dates and is excluded from forecasting. |

Same-day information is leakage when predicting that day before it happens. A chronological split alone cannot fix that. The previous implementation used same-day revenue and rolling windows that included the target day, and its reported scores are not valid forecasting evidence.

## Registry and schema changes

[src/features/registry.yml](../src/features/registry.yml) defines all 12 target, predictor and descriptive fields. Each entry records its name, description, source table, grain, transformation, leakage risk, `model_input` flag and downstream use. The validator checks that registry names exist in the database; tests also check that the declared model inputs match the explicit model allowlist.

```bash
python -m src.features.build_features
python -m src.features.validate_registry
```

The rebuild replaces only `features.product_daily_features` inside one PostgreSQL transaction. This migrates an existing older table as well as its values. If creation or insertion fails, the previous table and data remain available. Source and mart tables are not changed. External views depending on the derived table must be handled explicitly; the rebuild does not use `CASCADE`.

Schema version 2 adds `lag_1d_quantity`, `lag_7d_quantity` and `weekday`. It changes both `rolling_7d_*` columns from windows including the current observed row to seven **prior calendar days**, stored as double precision without rounding their means to two decimals. Downstream readers must accept null values during the first seven days of each series.

## Verification

Regression tests exercise calendar gaps, separate product-country histories, target-day and future changes, extension of the observation period, migration and rollback. Model tests cover chronological boundaries, identical-row comparators, nonnegative predictions, cold-start coverage, strict JSON and undefined metrics. The single-day sample produces `not_evaluable` with null scores; it cannot establish forecasting accuracy.
