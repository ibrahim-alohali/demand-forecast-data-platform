# Architecture



## Architecture goal

Keep the system simple, layered, reproducible, and easy to explain.



## High-level flow

1. Source data enters the system as raw input
2. Raw input is loaded into PostgreSQL raw tables
3. Raw data is cleaned and standardized into staging tables
4. Staging data is transformed into marts for analytics
5. Data quality contracts validate critical assumptions on important tables
6. Forecasting features are documented in a lightweight feature registry
7. ML-ready feature tables are generated from earlier layers
8. A baseline model reads the feature table and produces simple evaluation output


## Layers



### Raw

Purpose:

Store source data with minimal change.



Rules:

- preserve source columns when possible

- track load timestamp

- do not apply business logic here



### Staging

Purpose:

Clean and standardize raw data.



Rules:

- fix types

- normalize names

- handle missing values

- remove obvious bad records carefully

- keep transformations transparent



### Marts

Purpose:

Create analytical tables for reporting and downstream use.



Rules:

- define clean business entities

- use clear grains

- make joins understandable

- avoid hidden logic

Tables:
- `marts.fct_daily_product_sales` — grain: (stock_code, sale_date, country). Aggregates sales and returns separately.
- `marts.dim_product` — grain: stock_code. Tracks description, date range, and country reach.

No customer dimension is built. About 25% of the dataset has null customer_id, and demand forecasting focuses on product-by-time patterns rather than customer segmentation. A customer dimension would be mostly incomplete and not useful for the downstream feature tables.

### Data Quality Contracts
Purpose:
Make critical assumptions explicit and testable.

Rules:
- validate important tables, not everything blindly
- enforce checks that matter for downstream modelling
- keep validations lightweight and understandable
- prefer simple checks over a fake enterprise framework

Examples:
- primary keys should be unique
- core timestamps should not be null
- target categorical values should be constrained where appropriate
- freshness should be checked for newly loaded data

### Feature Registry
Purpose:
Document forecasting features so they are reproducible, explainable, and interview-safe.

Rules:
- each feature must have a clear definition
- each feature must name its source layer
- each feature must describe its grain
- each feature should mention leakage risk if relevant
- registry should stay lightweight, not become a fake platform product

### Features
Purpose:
Prepare forecasting-ready feature tables from warehouse data.

Rules:
- features must be derived from earlier layers
- feature logic must be documented in the registry
- features should support demand forecasting and inventory insight use cases
- do not leak future information into training data



### Model

Purpose:

Train one simple baseline model to prove pipeline usefulness.



Rules:

- keep the model simple

- focus on pipeline credibility, not model hype

- document assumptions and limitations



## Recommended repo shape

- src/ for Python code

- sql/ for schema and transformation SQL when useful

- tests/ for unit and integration tests

- docs/ for architecture and notes

- data/ only for small sample files or download scripts, not huge datasets

- notebooks/ optional and limited, only if truly useful

- .github/workflows/ for CI



## Design principles

- local-first

- explicit is better than hidden

- each file must have a reason to exist

- each dependency must be justified

- every stage should be verifiable

- docs should match implementation



## Not approved yet

These are deferred until the project works:

- dbt

- Prefect

- Airflow

- cloud deployment

- web UI

- advanced monitoring

- streaming



## Likely technical building blocks

- Python

- PostgreSQL

- Docker Compose

- psycopg (v3)

- Pandas for data preparation where appropriate

- Pytest

- Ruff

- basic CI



## What makes this architecture good for a portfolio

It is:

- realistic

- structured

- explainable

- close to real data engineering work

- still manageable for one person

