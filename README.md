[![CI](https://github.com/ibrahim-alohali/demand-forecast-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/ibrahim-alohali/demand-forecast-data-platform/actions/workflows/ci.yml)

# demand-forecast-data-platform

A local-first data engineering platform for demand forecasting and inventory intelligence.


## What this project does

Loads the [UCI Online Retail II](https://archive.ics.uci.edu/dataset/502/online+retail+ii) dataset (~1M UK e-commerce transactions) into a layered PostgreSQL warehouse. It cleans and models the data through raw, staging, and marts layers, then builds ML-ready feature tables and trains a baseline forecasting model.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full design.

## Architecture overview

```
Source data
  → raw schema       (preserve source data as-is)
  → staging schema   (clean types, normalize names, handle nulls)
  → marts schema     (analytical tables with clear grains)
  → features schema  (ML-ready feature tables, documented in registry)
  → baseline model   (simple forecast to prove pipeline usefulness)
```

## Sample pipeline run

Output from running the full pipeline on the 100-row sample dataset:

```
Raw:      100 rows loaded
Staging:  99 rows (1 duplicate removed)
Marts:    89 fact rows, 83 product dimension rows
Features: 89 feature rows
Quality:  11/11 contracts passed
Model:    MAE=0.0, RMSE=0.0, R²=1.0 (single-day sample)
```

On the full UCI dataset (1,067,371 rows), the pipeline produces 1,033,036 staging rows, 593,206 daily fact rows, and 5,295 products. The baseline model scores R²=0.85 with a time-based train/test split across 739 days of transaction data.

## Prerequisites

- Python 3.11
- Docker Desktop (includes Docker Compose)
- Git

Optional: `make` (available via Git Bash on Windows, or install via `choco install make`)

## Quickstart

### 1. Clone and configure

```bash
git clone https://github.com/ibrahim-alohali/demand-forecast-data-platform.git
cd demand-forecast-data-platform
cp .env.example .env          # Linux / Mac / Git Bash
# Copy-Item .env.example .env  # PowerShell
```

### 2. Create a virtual environment and install dependencies

```bash
python -m venv .venv

# Activate:
source .venv/bin/activate       # Linux / Mac / Git Bash
# .venv\Scripts\activate        # PowerShell

pip install -e ".[dev]"
```

### 3. Start PostgreSQL

```bash
# With Make:
make up

# Without Make:
docker compose up -d
```

The first run creates the `raw`, `staging`, `marts`, and `features` schemas automatically via `sql/init.sql`.

### 4. Load data

**Option A: Load sample data (quick, no download needed)**

```bash
# With Make:
make ingest-sample

# Without Make (PowerShell):
python -m src.ingestion.load_online_retail --sample
```

**Option B: Download and load the full dataset**

```bash
# With Make:
make download
make ingest

# Without Make (PowerShell):
python -m src.ingestion.download
python -m src.ingestion.load_online_retail --file data/online_retail_ii.xlsx
```

To reload data from scratch, use `--replace`:

```bash
make ingest-replace
# or: python -m src.ingestion.load_online_retail --file data/online_retail_ii.xlsx --replace
```

### 5. Build staging layer

Cleans raw data into `staging.stg_online_retail`: deduplicates, casts types, normalizes text, and adds `is_return` / `is_stock_item` flags.

```bash
# With Make:
make staging

# Without Make (PowerShell):
python -m src.staging.build_staging
```

This is a full refresh — it truncates and rebuilds staging from raw each time.

### 6. Build marts layer

Aggregates staging data into analytical tables: `marts.fct_daily_product_sales` (daily sales and returns per product per country) and `marts.dim_product` (one row per product).

```bash
# With Make:
make marts

# Without Make (PowerShell):
python -m src.marts.build_marts

# Or build staging + marts + features together:
make build-all
```

### 7. Build feature tables

Joins marts tables into `features.product_daily_features`: passthrough columns, derived price, rolling averages, and product metadata. See [docs/feature_registry.md](docs/feature_registry.md) for the full feature list.

```bash
# With Make:
make features

# Without Make (PowerShell):
python -m src.features.build_features

# Validate that registry.yml matches the table columns:
make validate-registry
# or: python -m src.features.validate_registry
```

### 8. Train baseline model

Trains a linear regression on the feature table, evaluates on a held-out set, and saves results to `data/model_evaluation.json` and `data/model_report.md`.

```bash
# With Make:
make train

# Without Make (PowerShell):
python -m src.model.train_baseline
```

### 9. Run data quality contracts

Validates critical assumptions about staging and marts tables (11 contracts). See [docs/data_quality_contracts.md](docs/data_quality_contracts.md) for the full registry.

```bash
# With Make:
make quality

# Without Make (PowerShell):
python -m src.quality.run_contracts
```

Exits 0 if all contracts pass, 1 if any fail.

### 10. Run tests

```bash
# Unit tests only (no database needed):
make test
# PowerShell: python -m pytest tests/ -m "not integration"

# Integration tests (requires running PostgreSQL):
make test-integration
# PowerShell: python -m pytest tests/ -m integration
```

### 11. Run linter

```bash
# With Make:
make lint

# Without Make:
ruff check src/ tests/
```

## Project structure

```
├── .github/workflows/   CI pipeline
├── data/                Sample data and download target
├── docs/                Design docs
├── sql/                 Schema definitions and transformations
├── src/
│   ├── ingestion/       Download and load scripts
│   ├── staging/         Staging layer build script
│   ├── marts/           Mart table build script
│   ├── quality/         Data quality contracts
│   ├── features/        Feature build, registry, and validation
│   ├── model/           Baseline model training and evaluation
│   ├── config.py        Database configuration
│   └── db.py            Connection helper
├── tests/               Unit and integration tests
├── docker-compose.yml   PostgreSQL service
├── pyproject.toml       Python project config
└── Makefile             Common command shortcuts
```

## Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md) — system design and layer definitions
- [ROADMAP.md](ROADMAP.md) — phased delivery plan
- [docs/data_quality_contracts.md](docs/data_quality_contracts.md) — data quality approach
- [docs/feature_registry.md](docs/feature_registry.md) — feature registry approach

## Roadmap

See [ROADMAP.md](ROADMAP.md) for the full plan. Current progress:

- [x] Phase 1: Scaffold
- [x] Phase 2: Raw ingestion
- [x] Phase 3: Staging
- [x] Phase 4: Marts
- [x] Phase 5: Data quality contracts
- [x] Phase 6: Feature registry and feature tables
- [x] Phase 7: Baseline model
- [x] Phase 8: Polish
