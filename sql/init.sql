-- Initialize database schemas for the demand forecast data platform.
-- Each schema maps to a pipeline layer defined in ARCHITECTURE.md.
--
-- Run automatically on first `docker compose up` via the
-- /docker-entrypoint-initdb.d/ mount, or manually with:
--   make db-init

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS features;
