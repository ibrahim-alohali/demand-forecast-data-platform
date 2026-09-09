[![CI](https://github.com/ibrahim-alohali/demand-forecast-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/ibrahim-alohali/demand-forecast-data-platform/actions/workflows/ci.yml)

# Demand Forecast Data Platform

A Python and PostgreSQL project that turns retail transaction files into analytical tables and evaluates next-day **recorded paid gross sales quantity** for each product and country.

The work covers source validation, cleaning decisions, SQL aggregation, data quality checks and forecasting evaluation. It uses the public [UCI Online Retail II dataset](DATA_SOURCE.md). Recorded sales and cancellations remain separate; the project does not estimate stock availability or unmet demand.

[Project walkthrough](docs/WALKTHROUGH.md) · [Architecture](ARCHITECTURE.md) · [Verification](docs/VERIFICATION.md)

## Verified full-workbook results

| Stage | Result |
|---|---:|
| Raw source rows across two worksheets | 1,067,371 |
| Staging rows after disclosed exclusions and exact duplicate removal | 1,033,031 |
| Eligible paid sale/cancellation lines | 1,021,517 |
| Daily product-country fact rows | 589,055 |
| Products in the dimension | 4,920 |
| Product-country series | 28,353 |
| Calendar feature rows | 12,286,085 |

An independent read-only reconstruction from the original workbook matched every fact group's quantities, sterling amounts and invoice counts, plus each product's date range and country count. The final marts contain **11,188,217 gross paid units (£19,655,609.62)** and **467,874 cancellation units (£724,708.68)**. See [source reconciliation and limitations](DATA_SOURCE.md).

The model uses the previous day, the previous weekday, the preceding seven-day mean and the target day's known weekday. It excludes 198,013 rows without sufficient history.

| Method | MAE | RMSE | R² |
|---|---:|---:|---:|
| Linear regression | 1.3264 | 11.1440 | 0.0609 |
| Previous day | 1.0718 | 14.9024 | -0.6794 |
| Previous weekday | 1.0182 | 14.3743 | -0.5625 |

All three methods use the same **3,754,801 evaluation rows**, from **2011-07-16 to 2011-12-09**. Training uses 8,333,271 rows from 2009-12-08 to 2011-07-15. MAE and RMSE are units per product-country-day. Linear regression has lower RMSE but worse MAE than both simple comparators; this is one chronological evaluation, not evidence of consistent superiority.

Read the [full model report](docs/full_model_report.md) and [evaluation JSON](docs/full_model_evaluation.json) for coverage, coefficients and error examples.

The included 100-row, single-day sample produces 99 staging rows, 89 fact/feature rows and 83 products. Its forecasting status is **`not_evaluable`**, with null scores: one day cannot support a seven-day-history forecast evaluation.

## Run locally with PowerShell 7

Requirements: Python 3.11, Git, Docker Desktop with Docker Compose, and PowerShell 7. The full run materializes over 12 million feature rows; start with the sample to check your setup.

The `&&` chains below stop when a command fails. Fix that failure before continuing to another block. These instructions start from a fresh checkout; keep your existing `.env` if you already configured the project.

### 1. Install and start PostgreSQL

~~~powershell
git clone https://github.com/ibrahim-alohali/demand-forecast-data-platform.git &&
Set-Location demand-forecast-data-platform &&
py -3.11 -m venv .venv &&
./.venv/Scripts/python.exe -m pip install -e ".[dev]" &&
Copy-Item .env.example .env
~~~

Review `.env` before starting Docker if you need different connection settings. Its defaults are for a local demonstration. Existing `POSTGRES_*` environment variables override `.env`; clear any previous test overrides before a normal run.

~~~powershell
docker compose up -d --wait
~~~

On the first start of a new database volume, `sql/init.sql` creates the raw, staging, marts and features schemas.

A fresh editable install passed all **142 tests** and Ruff. To reproduce the package versions used for the published full-run metrics, install the [recorded requirements](docs/full_run_requirements.txt) into your own virtual environment after the editable install:

~~~powershell
./.venv/Scripts/python.exe -m pip install -r docs/full_run_requirements.txt
~~~

### 2. Run the sample

~~~powershell
./.venv/Scripts/python.exe -m src.ingestion.load_online_retail --sample &&
./.venv/Scripts/python.exe -m src.staging.build_staging &&
./.venv/Scripts/python.exe -m src.marts.build_marts &&
./.venv/Scripts/python.exe -m src.quality.run_contracts &&
./.venv/Scripts/python.exe -m src.features.build_features &&
./.venv/Scripts/python.exe -m src.features.validate_registry &&
./.venv/Scripts/python.exe -m src.model.train_baseline --output-dir data/sample --source-file data/sample_online_retail.csv
~~~

Open `data/sample/model_evaluation.json` and `data/sample/model_report.md`. Expect `not_evaluable`, not a numerical forecasting score.

Ingestion defaults to safe mode and refuses a nonempty raw table. For an intentional sample reload, change the first command to `--sample --replace`. Replacement changes that database's raw contents; `--append` deliberately keeps existing rows and adds every input row.

### 3. Run the full workbook in a separate database

Create this empty database once in the same PostgreSQL container, keeping the sample database intact:

~~~powershell
docker compose exec db sh -c 'createdb -U "$POSTGRES_USER" demand_forecast_full'
~~~

If it already exists, inspect the existing data before choosing a reload. The first full load below uses safe mode.

~~~powershell
$env:POSTGRES_DB = "demand_forecast_full"
try {
    @'
from pathlib import Path
from src.db import get_connection
with get_connection() as conn:
    conn.execute(Path("sql/init.sql").read_text())
'@ | ./.venv/Scripts/python.exe -
    if ($LASTEXITCODE -ne 0) { throw "Database initialization failed." }

    ./.venv/Scripts/python.exe -m src.ingestion.download &&
    ./.venv/Scripts/python.exe -m src.ingestion.load_online_retail --file data/online_retail_ii.xlsx &&
    ./.venv/Scripts/python.exe -m src.staging.build_staging &&
    ./.venv/Scripts/python.exe -m src.marts.build_marts &&
    ./.venv/Scripts/python.exe -m src.quality.run_contracts &&
    ./.venv/Scripts/python.exe -m src.features.build_features &&
    ./.venv/Scripts/python.exe -m src.features.validate_registry &&
    ./.venv/Scripts/python.exe -m src.model.train_baseline --output-dir data/full --source-file data/online_retail_ii.xlsx
} finally {
    Remove-Item Env:POSTGRES_DB -ErrorAction SilentlyContinue
}
~~~

The downloader retains an existing workbook. The [source record](DATA_SOURCE.md) gives the expected checksum and explains the canonical download filename. Keep the input workbook unchanged; each evaluation records its checksum and the loaded source sheets.

An intentional rerun can use `--replace` on the ingestion command. Each layer refresh is transactional, and both mart tables publish together. The whole pipeline is not one transaction: after a later failure, earlier completed layers may already be updated. Stop, resolve the cause and rebuild downstream layers before interpreting their results.

### 4. Run checks in an isolated test database

Unit tests and lint do not require PostgreSQL:

~~~powershell
./.venv/Scripts/python.exe -m pytest tests/ -m "not integration" &&
./.venv/Scripts/python.exe -m ruff check src/ tests/
~~~

Integration tests truncate tables. Create a disposable PostgreSQL 16 container with its own data storage and localhost port:

~~~powershell
docker run --detach --rm --name forecast-integration-tests `
  --publish 127.0.0.1:55433:5432 `
  --env POSTGRES_USER=forecast_test `
  --env POSTGRES_PASSWORD=forecast_test_local `
  --env POSTGRES_DB=demand_forecast_test postgres:16
~~~

Before continuing, run this readiness check until it reports that the server is accepting connections:

~~~powershell
docker exec forecast-integration-tests pg_isready -U forecast_test -d demand_forecast_test
~~~

Set all five connection variables explicitly. The test guard rejects missing settings and database names that do not end in `_test`.

~~~powershell
$env:POSTGRES_HOST = "127.0.0.1"
$env:POSTGRES_PORT = "55433"
$env:POSTGRES_USER = "forecast_test"
$env:POSTGRES_PASSWORD = "forecast_test_local"
$env:POSTGRES_DB = "demand_forecast_test"
try {
    @'
from pathlib import Path
from src.db import get_connection
with get_connection() as conn:
    conn.execute(Path("sql/init.sql").read_text())
'@ | ./.venv/Scripts/python.exe -
    if ($LASTEXITCODE -ne 0) { throw "Test database initialization failed." }

    ./.venv/Scripts/python.exe -m pytest tests/ &&
    ./.venv/Scripts/python.exe -m ruff check src/ tests/
} finally {
    Remove-Item Env:POSTGRES_HOST, Env:POSTGRES_PORT, Env:POSTGRES_USER, Env:POSTGRES_PASSWORD, Env:POSTGRES_DB -ErrorAction SilentlyContinue
    docker stop forecast-integration-tests
}
~~~

The final block clears the current shell's test overrides and removes the disposable container when it stops. If you previously used environment variables for another connection, set those intended values again before using it. The full-workbook database is excluded by the test naming guard.

CI configures its own PostgreSQL 16 service and runs lint plus all unit and integration tests. The [verification record](docs/VERIFICATION.md) distinguishes local results from observed hosted runs.

## Documentation

- [Architecture](ARCHITECTURE.md): table grains, refresh boundaries and prediction timing.
- [Data source](DATA_SOURCE.md): attribution, checksum, reconciliation and analytical limits.
- [Data quality contracts](docs/data_quality_contracts.md): implemented checks and negative test cases.
- [Feature registry](docs/feature_registry.md): fields, definitions and permitted model inputs.
- [Walkthrough](docs/WALKTHROUGH.md): sample demonstration and interview explanations.
- [Verification](docs/VERIFICATION.md): checked revisions, environments and repeatability evidence.
- [Roadmap](ROADMAP.md): implemented work and evidence needed for further development.
