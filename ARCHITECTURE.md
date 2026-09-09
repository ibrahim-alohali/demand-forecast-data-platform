# Architecture

The pipeline prepares retail history for SQL analysis and evaluates a one-day forecast of recorded paid gross sales quantity. It runs locally with Python 3.11, PostgreSQL 16, pandas, psycopg and scikit-learn.

```text
CSV / Excel workbook
  -> raw.online_retail
  -> staging.stg_online_retail
  -> marts.fct_daily_product_sales + marts.dim_product
  -> data quality contracts
  -> features.product_daily_features + registry validation
  -> chronological model/comparator evaluation
```

Each command is explicit. No scheduler or whole-pipeline transaction coordinates the stages; the [README](README.md) runs them in order and stops on a failed command.

## Table grains and responsibilities

| Table | Meaning of one row | Responsibility |
|---|---|---|
| `raw.online_retail` | One loaded input row, including duplicates. | Store typed business fields, source filename, sheet and load time. |
| `staging.stg_online_retail` | One retained source-business line after exact duplicate removal. | Normalize text and customer IDs, classify invoice/product conventions and report exclusions. |
| `marts.fct_daily_product_sales` | One stock code, date and country with an eligible paid sale or cancellation. | Keep gross quantities, amounts and invoice counts separate from cancellation magnitudes. |
| `marts.dim_product` | One eligible stock code. | Describe the product and its observed date range/country coverage. |
| `features.product_daily_features` | One observed product-country pair on each calendar day from its first eligible record to the shared data end. | Construct prior-day/prior-week inputs, including zero recorded sales on intervening dates. |

The product dimension joins to facts by stock code, many-to-one. Country remains part of the fact grain. Summing a daily group's `transaction_count` across products does not yield the number of unique invoices: the same invoice can contain several products.

## Source ingestion

CSV and all Excel worksheets use the same eight-field source mapping. Validate the mode, format, columns and nonempty sheets before opening a database connection. Unknown or duplicate columns and invalid/non-finite prices are rejected. Empty fields become null; text labels such as `NA`, `NULL` and `\N` remain literal.

The raw table makes minimal, typed changes: quantities are integers, dates are timestamps and prices are numeric with four decimal places. It is not a byte-for-byte archive. Retain the original workbook and its SHA-256 alongside the source identity recorded in evaluation output. The checked workbook had no price changes at that numeric scale; see [DATA_SOURCE.md](DATA_SOURCE.md).

Native psycopg `COPY.write_row` handles escaping and nulls. Database type errors also abort the transaction. The supported modes are:

- `safe`: refuse a table that already contains rows.
- `replace`: replace raw contents in one transaction.
- `append`: retain existing raw rows and add all input rows, including duplicates.

A table lock serializes loads. A failed load, including failure in a later worksheet, leaves previously committed raw data intact. Source files are read, not modified.

Existing environment variables take precedence over `.env`. Connection strings use psycopg's quoting helper so spaces, quotes and backslashes in credentials do not change their meaning.

## Staging decisions

Staging excludes missing invoice/product/country identifiers, dates, quantities or prices, and excludes negative prices. The source's five negative-price rows are bad-debt adjustments; exclusion is a sales-analysis scope decision, not a claim that such accounting entries cannot be valid.

Exact duplicates are identified from the original eight business fields before text normalization, independently of filename, sheet and load time. This removes 34,335 rows from the checked workbook, including 22,523 cross-sheet matches. Without a source line identifier, identical fields cannot prove whether two lines represent an accidental duplicate or a legitimate repetition; that limitation belongs with the result.

The retained rows have trimmed invoice, stock code and country boundaries, normalized description whitespace, and optional integer customer IDs. Missing IDs remain null. Nonempty IDs that are malformed, nonpositive or outside the supported integer range become null and are counted. No customer dimension is built; customer-level completeness is not an objective of this analysis.

The `is_return` flag follows the source's case-insensitive C invoice prefix. `is_stock_item` excludes the documented non-stock codes. Zero-price lines and invoice/quantity disagreements remain inspectable in staging. Console counts reconcile raw rows, missing-core exclusions, negative prices, eligible duplicates and inserted rows; malformed customer IDs are a separate retained-row count.

## Sales and cancellation marts

Both marts require a stock item and positive price, plus one of these combinations:

| Invoice convention | Quantity | Meaning used in the mart |
|---|---:|---|
| No C prefix | Positive | Recorded paid gross sale. |
| C prefix | Negative | Cancellation; expose its supported magnitude as returned units/amount. |

Zero-price stock rows have an uncertain meaning and are excluded from paid-sales analysis. Non-stock lines, zero quantities, negative non-C adjustments and positive-quantity C lines are also excluded. The program prints mutually exclusive exclusion counts.

Returns are not subtracted from the forecasting target. A return-only product-day remains a valid fact row with zero gross sales. Positive gross sales and equal cancellations on the same day also remain visible: product 23843 in the United Kingdom records 80,995 units on each side on 2011-12-09.

Amounts are aggregated by the fact grain and stored at two decimal places in sterling. The latest nonempty eligible product description is selected; same-timestamp ties use lexical ordering. First/last dates and distinct-country counts describe the whole loaded period. These descriptive fields do not enter the forecast.

## Calendar features and prediction timing

For an observed product-country pair, build calendar rows from its first eligible sale or return through the global last eligible date. Do not stop a series at its last sale: that endpoint would use future knowledge. Do not create a cross-product of every product and every country.

A missing transaction day means zero recorded paid sales in this dataset. It does not establish stock availability, zero customer demand or whether the product was still offered.

For target day `t`, the model uses exactly:

| Input | Information used |
|---|---|
| `lag_1d_quantity` | Recorded gross paid quantity on `t−1`. |
| `lag_7d_quantity` | Quantity on `t−7`, seven calendar days earlier. |
| `rolling_7d_quantity` | Mean quantity from `t−7` through `t−1`. |
| `weekday` | Known weekday of `t`, Monday 0 through Sunday 6. |

The first seven days of each series lack the required history. They retain null lag/window fields and are excluded from fitting and scoring. The full run has 12,286,085 calendar rows, 28,353 series and 198,013 excluded history rows.

The table also contains descriptive amounts, product metadata and a prior-seven-calendar-day revenue mean. An explicit model-input list prevents these fields, target-day revenue, target-day quantities or whole-period metadata from entering the model. The [feature registry](docs/feature_registry.md) documents column meanings and compatibility.

## Evaluation and output

One pooled linear regression trains on the earliest 80% of eligible target dates, rounded down; all rows sharing a date stay together. The latest 20% evaluate it. At least two eligible dates are required. There is no random-split fallback for a one-day sample.

The fitted model stays fixed during evaluation. Each later one-day forecast can use observations already available from earlier evaluation days. Predictions are floored at zero. Previous-day and previous-weekday comparators use exactly the same evaluation observations.

The published split trains on 8,333,271 rows through 2011-07-15 and evaluates 3,754,801 rows from 2011-07-16 to 2011-12-09. The [full report](docs/full_model_report.md) shows lower RMSE but higher MAE for the linear model than for the comparators. Aggregate scores can hide country/product differences and large misses.

Evaluation schema version 2 records status/reason, source identity, horizon, feature list, date boundaries, history coverage, model/comparator metrics, coefficients and labeled error examples. Insufficient history produces `not_evaluable` and null scores. R² is null for fewer than two observations or a constant target.

Use separate output directories for sample and full runs. `--source-file` attaches the supplied file's name and SHA-256; it does not itself prove that a manually changed database still corresponds to that file. Preserve the run sequence and reconcile the loaded rows.

## Refresh and verification boundaries

Raw ingestion, staging refresh, the combined fact/dimension refresh and feature refresh each have their own transaction. Feature refresh replaces its derived table definition and data together, applying the current schema rather than retaining obsolete columns.

The entire pipeline is not atomic. A later failure can leave newer upstream data beside older downstream data. Stop on errors and rerun the affected downstream stages before using their outputs.

The [eleven data contracts](docs/data_quality_contracts.md) check specific staging and mart assumptions. Unit and PostgreSQL integration tests cover validation, rollback, aggregation, calendar gaps, series isolation and information leakage. The full source was independently reconciled; current revisions, environments, repeated runs and hosted CI evidence are recorded in [VERIFICATION.md](docs/VERIFICATION.md).

This remains a historical-data project with one fixed evaluation split. Inventory recommendations, stockout estimation, deployment, orchestration and external validation are outside the implemented scope.

