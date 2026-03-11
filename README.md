# demand-forecast-data-platform

A local-first data engineering platform for demand forecasting and inventory intelligence.

**Status:** Phase 1 — scaffold complete, no business logic yet.

## What this project does

Ingests retail-like data, models it in layered PostgreSQL schemas, enforces data quality contracts, builds ML-ready feature tables, and trains a simple baseline forecasting model.

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

## Prerequisites

- Python 3.11
- Docker Desktop (includes Docker Compose)
- Git

Optional: `make` (available via Git Bash on Windows, or install via `choco install make`)

## Quickstart

### 1. Clone and configure

```bash
git clone <repo-url>
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

### 4. Run tests

```bash
# With Make:
make test

# Without Make:
python -m pytest tests/
```

### 5. Run linter

```bash
# With Make:
make lint

# Without Make:
ruff check src/ tests/
```

## Project structure

```
├── .github/workflows/   CI pipeline
├── data/                Sample data files (Phase 2)
├── docs/                Design docs and stubs
├── sql/                 Schema definitions
├── src/                 Python source code
├── tests/               Test suite
├── docker-compose.yml   PostgreSQL service
├── pyproject.toml       Python project config
└── Makefile             Common command shortcuts
```

## Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md) — system design and layer definitions
- [PROJECT_BRIEF.md](PROJECT_BRIEF.md) — project goals and constraints
- [ROADMAP.md](ROADMAP.md) — phased delivery plan
- [docs/data_quality_contracts.md](docs/data_quality_contracts.md) — data quality approach
- [docs/feature_registry.md](docs/feature_registry.md) — feature registry approach

## Roadmap

See [ROADMAP.md](ROADMAP.md) for the full plan. Current progress:

- [x] Phase 1: Scaffold
- [ ] Phase 2: Raw ingestion
- [ ] Phase 3: Staging
- [ ] Phase 4: Marts
- [ ] Phase 5: Data quality contracts
- [ ] Phase 6: Feature registry and feature tables
- [ ] Phase 7: Baseline model
- [ ] Phase 8: Polish
