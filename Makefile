.PHONY: up down test lint format db-init

up:
	docker compose up -d

down:
	docker compose down

test:
	python -m pytest tests/

lint:
	ruff check src/ tests/

format:
	ruff format src/ tests/

db-init:
	docker compose exec db psql -U $${POSTGRES_USER:-forecast} -d $${POSTGRES_DB:-demand_forecast} -f /docker-entrypoint-initdb.d/init.sql
