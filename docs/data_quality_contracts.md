# Data quality contracts

The project uses eleven Python functions that query staging and mart tables and return a named pass/fail result. They check specific assumptions; passing them is one part of verification.

The [README](../README.md) runs these checks after staging and marts, before building features:

~~~powershell
./.venv/Scripts/python.exe -m src.quality.run_contracts
~~~

The runner exits 0 when all checks pass and 1 when any check fails. A database/query error also stops the command. Calling the feature builder directly does not automatically run the contracts; keep the documented command order and stop on failures.

## Contract registry

| Table | Check identifier | Rule |
|---|---|---|
| `staging.stg_online_retail` | `row_count` | At least one row exists. |
| `staging.stg_online_retail` | `not_null_core` | Invoice, stock code, quantity, invoice date, price and country are nonnull. |
| `staging.stg_online_retail` | `positive_price` | Price is finite and nonnegative. The historical identifier includes zero-price staging rows. |
| `staging.stg_online_retail` | `valid_customer_id` | Every present customer ID is positive. The column's integer type is enforced by PostgreSQL. |
| `staging.stg_online_retail` | `boolean_flags_not_null` | Both cancellation and stock-item flags are present. |
| `marts.fct_daily_product_sales` | `row_count` | At least one row exists. |
| `marts.fct_daily_product_sales` | `grain_uniqueness` | Stock code, date and country identify one fact row. |
| `marts.fct_daily_product_sales` | `non_negative_quantities` | Gross and returned quantities are nonnegative. |
| `marts.fct_daily_product_sales` | `non_negative_revenue` | Gross and returned amounts are finite and nonnegative. |
| `marts.dim_product` | `primary_key_unique` | One row exists per stock code. |
| `marts.dim_product` | `date_ordering` | First observed eligible date does not follow the last. |

Checks live in [contracts.py](../src/quality/contracts.py). Each accepts a psycopg connection and returns `ContractResult(table, check_name, passed, message)`. [run_contracts.py](../src/quality/run_contracts.py) prints the results and determines the exit status.

## Input validation and database constraints

Ingestion validates load modes, supported files, source columns, nonempty input and finite numeric prices before connecting. PostgreSQL enforces raw quantity/date/price types. The raw table still retains source duplicates and negative-price accounting rows; staging applies the analytical exclusions. See [the source record](../DATA_SOURCE.md) and [architecture](../ARCHITECTURE.md).

Staging has `NOT NULL` constraints on its core fields and flags. The product dimension has a primary key. These constraints reject invalid writes before a contract could inspect them. Contract tests explicitly distinguish that protection from detecting already-stored invalid data.

Zero-price rows are allowed in staging for inspection, but excluded from paid-sales marts. The five negative-price rows in the full source are bad-debt adjustments, excluded from this analysis; negative prices are not described as universally meaningless.

Numeric NaN needs an explicit check: in PostgreSQL, simply testing whether a numeric value is below zero does not reject NaN. The price and revenue contracts include that case.

## What the tests establish

[Contract integration tests](../tests/test_contracts_integration.py) verify the sample's passing results and deliberately introduce empty tables, invalid prices/customers, duplicate fact grains, negative quantities or amounts, and inverted dimension dates. After rollback, the same checks pass again.

Separate cases attempt null core/flag values and a duplicate dimension key and confirm that PostgreSQL rejects them first. The tests use an explicitly configured isolated database ending in `_test`; they truncate tables and must not target a development or full-source database.

[Core regressions](../tests/test_core_regressions.py) also cover input round-trips, load-mode behavior, failed replacement, multiple worksheets, customer conversion, paid-sale/cancellation eligibility, return-only groups and atomic mart refresh. Feature/model tests check calendar windows, chronology and leakage separately.

## Limits of these checks

- They do not prove that a source value is factually correct or that the chosen duplicate/non-stock rules capture every business case.
- Eleven passing contracts do not independently reconcile totals with the workbook. Full-source reconciliation is separate evidence.
- Registry validation checks documented columns; it does not replace tests of feature timing and meaning.
- A historical dataset has no configured freshness service-level target. File identity and period coverage are recorded, rather than claiming a live freshness guarantee.
- The checks do not establish forecast usefulness, subgroup performance, stock availability or unconstrained demand.
- Transactions protect each refresh boundary; they do not make the complete multi-command pipeline atomic.

See [VERIFICATION.md](VERIFICATION.md) for the checked revision and local/hosted evidence, and the [full evaluation](full_model_report.md) for the forecasting result.
