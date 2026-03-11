# Data Quality Contracts

## What are data quality contracts?

Data quality contracts are explicit, testable rules that validate critical assumptions about important tables. They catch problems before bad data reaches downstream models or feature tables.

This project uses lightweight Python-based checks rather than a heavy framework. Each contract is a function that queries a table and raises an error if the data violates expectations.

## When contracts are applied

Contracts validate data at layer boundaries:

- **raw → staging**: Did we load what we expected? Are primary keys present?
- **staging → marts**: Are types correct? Are values within expected ranges?
- **marts → features**: Are grains unique? Are timestamps non-null?

## Contract types

| Check type | What it validates | Example |
|------------|-------------------|---------|
| Not null | Required columns are never null | `order_date IS NOT NULL` |
| Uniqueness | Primary keys have no duplicates | `COUNT(*) = COUNT(DISTINCT id)` |
| Accepted values | Categorical columns contain only valid values | `status IN ('pending', 'shipped', 'delivered')` |
| Freshness | Recently loaded data exists | `MAX(loaded_at) >= NOW() - INTERVAL '24 hours'` |
| Row count | Table is not suspiciously empty | `COUNT(*) > 0` |

## Implementation plan

Contracts will be implemented in Phase 5 as Python functions in `src/quality/`. Each contract:

- targets a specific table and schema
- returns a pass/fail result with a human-readable message
- can be run as part of the test suite via pytest
- is documented here with its rationale

## Contract registry

*To be populated in Phase 5. Each entry will include:*

| Table | Check | Rule | Rationale |
|-------|-------|------|-----------|
| *(example)* staging.orders | not_null | `order_date IS NOT NULL` | Downstream features require valid dates |
