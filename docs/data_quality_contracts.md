# Data Quality Contracts

## What are data quality contracts?

Data quality contracts are explicit, testable rules that validate critical assumptions about important tables. They catch problems before bad data reaches downstream models or feature tables.

This project uses lightweight Python-based checks rather than a heavy framework. Each contract is a function that queries a table and returns a pass/fail result with a human-readable message.

## When contracts are applied

Contracts validate data at layer boundaries after staging and marts are built. They run before any downstream feature generation or modelling.

Run contracts with:

```bash
make quality
# or: python -m src.quality.run_contracts
```

The runner exits 0 if all contracts pass, 1 if any fail.

## Contract registry

| # | Table | Check | Rule | Rationale |
|---|-------|-------|------|-----------|
| 1 | staging.stg_online_retail | row_count | `COUNT(*) > 0` | Empty staging breaks everything downstream |
| 2 | staging.stg_online_retail | not_null_core | `invoice, stock_code, quantity, invoice_date, price, country IS NOT NULL` | These feed NOT NULL constraints in marts; catch issues before constraint errors |
| 3 | staging.stg_online_retail | positive_price | `price >= 0` for all rows | Negative prices are nonsensical; returns use negative quantity, not negative price |
| 4 | staging.stg_online_retail | valid_customer_id | `customer_id > 0` where not NULL | customer_id should be a positive integer when present |
| 5 | staging.stg_online_retail | boolean_flags_not_null | `is_return IS NOT NULL AND is_stock_item IS NOT NULL` | Downstream filtering depends on these flags |
| 6 | marts.fct_daily_product_sales | row_count | `COUNT(*) > 0` | Empty fact table means features have nothing to work with |
| 7 | marts.fct_daily_product_sales | grain_uniqueness | `COUNT(*) = COUNT(DISTINCT (stock_code, sale_date, country))` | Duplicate grains break aggregations and forecasting |
| 8 | marts.fct_daily_product_sales | non_negative_quantities | `total_quantity >= 0 AND return_quantity >= 0` | Negative aggregated quantities indicate a transformation bug |
| 9 | marts.fct_daily_product_sales | non_negative_revenue | `total_revenue >= 0 AND return_revenue >= 0` | Negative aggregated revenue indicates a transformation bug |
| 10 | marts.dim_product | primary_key_unique | `COUNT(*) = COUNT(DISTINCT stock_code)` | Dimension must have one row per product |
| 11 | marts.dim_product | date_ordering | `first_seen <= last_seen` for all rows | Inverted dates indicate a transformation bug |

## What is not checked

- **Raw table contracts**: Raw preserves source data as-is. Contracts belong at layer boundaries, not on raw.
- **Freshness checks**: This is a local full-refresh pipeline. Freshness is meaningless in this context.
- **Feature table contracts**: Will be added in Phase 6 when feature tables are built.

## Implementation

Contracts live in `src/quality/contracts.py`. Each is a function that takes a psycopg connection and returns a `ContractResult` dataclass with: table, check_name, passed, message.

The CLI runner (`src/quality/run_contracts.py`) executes all contracts and prints a summary table. Integration tests in `tests/test_contracts_integration.py` verify all contracts pass on sample data.
