.PHONY: up down test test-integration lint format db-init download ingest ingest-replace ingest-sample staging

up:
	docker compose up -d

down:
	docker compose down

test:
	python -m pytest tests/ -m "not integration"

test-integration:
	python -m pytest tests/ -m integration

lint:
	ruff check src/ tests/

format:
	ruff format src/ tests/

db-init:
	docker compose exec db psql -U $${POSTGRES_USER:-forecast} -d $${POSTGRES_DB:-demand_forecast} -f /docker-entrypoint-initdb.d/init.sql

download:
	python -m src.ingestion.download

ingest:
	python -m src.ingestion.load_online_retail --file data/online_retail_ii.xlsx

ingest-replace:
	python -m src.ingestion.load_online_retail --file data/online_retail_ii.xlsx --replace

ingest-sample:
	python -m src.ingestion.load_online_retail --sample

staging:
	python -m src.staging.build_staging
