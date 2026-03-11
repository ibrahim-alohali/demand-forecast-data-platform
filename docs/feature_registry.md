# Feature Registry

## What is the feature registry?

The feature registry is a structured document that describes every forecasting feature in the pipeline. It exists so that:

- features are reproducible (anyone can rebuild them from the description)
- features are explainable (you can describe each one in an interview)
- leakage risks are visible (future data doesn't leak into training)
- downstream consumers know what each feature means

## Registry format

The registry will be stored as a YAML file (`src/features/registry.yml` — created in Phase 6) with the following structure per feature:

```yaml
features:
  - name: rolling_7d_sales
    description: "7-day rolling average of daily unit sales"
    source_table: marts.daily_sales
    grain: product_id, date
    transform: "AVG(units_sold) OVER (PARTITION BY product_id ORDER BY date ROWS 6 PRECEDING)"
    leakage_risk: low
    downstream_use: baseline demand forecast
```

## Field definitions

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Unique feature name, snake_case |
| `description` | Yes | Plain English explanation of what the feature captures |
| `source_table` | Yes | Which mart or staging table the feature is derived from |
| `grain` | Yes | The grouping level (e.g., product + date) |
| `transform` | Yes | SQL or Python expression used to compute the feature |
| `leakage_risk` | Yes | `low`, `medium`, or `high` — does this feature risk using future data? |
| `downstream_use` | No | What model or analysis consumes this feature |

## Why YAML?

- Human-readable without running code
- Machine-parseable for validation and tooling
- Standard format in data engineering (dbt schema.yml, Great Expectations)
- Reviewable in pull requests
- Separates feature documentation from feature implementation

## Implementation plan

- Phase 6 creates `src/features/registry.yml` with actual feature entries
- Phase 6 creates `src/features/` Python package to build feature tables
- A validation script will check that every feature in the registry has a matching column in the feature table

## Feature list

*To be populated in Phase 6.*
