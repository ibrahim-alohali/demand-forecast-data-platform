# Feature Registry

## What is the feature registry?

The feature registry is a structured document that describes every forecasting feature in the pipeline. It exists so that:

- features are reproducible (anyone can rebuild them from the description)
- features are explainable (you can describe each one in an interview)
- leakage risks are visible (future data doesn't leak into training)
- downstream consumers know what each feature means

## Registry location

The registry lives at `src/features/registry.yml`. A validation script (`src/features/validate_registry.py`) checks that every feature in the YAML has a matching column in the database table.

## Feature table

`features.product_daily_features` — grain: (stock_code, sale_date, country).

Built from a JOIN of `marts.fct_daily_product_sales` and `marts.dim_product`.

## Feature list

| # | Feature | Description | Source | Transform | Leakage risk |
|---|---------|-------------|--------|-----------|--------------|
| 1 | total_quantity | Total units sold for this product-day-country | fct_daily_product_sales | passthrough | none |
| 2 | total_revenue | Total revenue for this product-day-country | fct_daily_product_sales | passthrough | none |
| 3 | return_quantity | Total units returned for this product-day-country | fct_daily_product_sales | passthrough | none |
| 4 | transaction_count | Number of distinct invoices for this product-day-country | fct_daily_product_sales | passthrough | none |
| 5 | avg_unit_price | Revenue per unit sold (NULL when quantity is 0) | fct_daily_product_sales | total_revenue / NULLIF(total_quantity, 0) | none |
| 6 | rolling_7d_quantity | 7-day rolling average of daily quantity sold | fct_daily_product_sales | AVG() OVER ROWS 6 PRECEDING | none |
| 7 | rolling_7d_revenue | 7-day rolling average of daily revenue | fct_daily_product_sales | AVG() OVER ROWS 6 PRECEDING | none |
| 8 | days_since_first_seen | Days between sale_date and product first appearance | dim_product | sale_date - first_seen | none |
| 9 | distinct_countries | Number of countries the product has been sold in | dim_product | passthrough | low |

## Field definitions

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Unique feature name, snake_case |
| `description` | Yes | Plain English explanation of what the feature captures |
| `source_table` | Yes | Which mart table the feature is derived from |
| `grain` | Yes | The grouping level (stock_code, sale_date, country) |
| `transform` | Yes | SQL expression used to compute the feature |
| `leakage_risk` | Yes | `none`, `low`, `medium`, or `high` |
| `downstream_use` | No | What model or analysis consumes this feature |

## Why YAML?

- Human-readable without running code
- Machine-parseable for validation
- Standard format in data engineering (dbt schema.yml, Great Expectations)
- Reviewable in pull requests
- Separates feature documentation from feature implementation

## Leakage notes

- `distinct_countries` has low leakage risk: it uses the full-history country count from `dim_product`, which could include countries seen after the sale_date. For a proper train/test split, this would need to be recalculated using only data up to the split point.
- All other features use only same-day or past data and have no leakage risk.
- Rolling window features use `ROWS 6 PRECEDING`, which looks backward only.
