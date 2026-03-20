# Roadmap

All 8 phases are complete.

## Phase 1. Scaffold (done)

Goal: Create the repo structure and local development foundation.

Done means:
- repo structure exists
- Docker setup exists
- PostgreSQL service is configured
- Python project setup exists
- linting and test setup exist
- README skeleton exists

## Phase 2. Raw ingestion (done)

Goal: Load source data into raw tables.

Done means:
- source data is defined
- ingestion script works locally
- raw table schema exists
- load process is documented
- basic tests exist

## Phase 3. Staging (done)

Goal: Clean and standardize raw data.

Done means:
- staging transformations exist
- key columns have correct types
- bad/null handling is documented
- tests cover core transformation logic

## Phase 4. Marts (done)

Goal: Create useful analytical tables.

Done means:
- marts have clear grains
- transformations are understandable
- marts support downstream feature creation
- schema decisions are documented

## Phase 5. Data quality contracts (done)

Goal: Make critical table assumptions explicit and enforceable.

Done means:
- key validations exist for important tables
- checks are runnable locally
- quality rules are documented
- failures are understandable

## Phase 6. Feature registry and feature tables (done)

Goal: Create ML-ready feature tables and document them properly.

Done means:
- feature table exists
- feature registry exists
- each important feature is documented
- feature logic is reproducible
- leakage risk is noted where relevant

## Phase 7. Baseline model (done)

Goal: Train one simple forecasting model on the feature table.

Done means:
- training script runs
- baseline evaluation is logged or saved
- assumptions and limitations are documented
- the model supports the pipeline story, not hype

## Phase 8. Polish (done)

Goal: Make the repo interview-ready.

Done means:
- README is strong
- docs are consistent
- code is cleaned up
- tests pass
- CI works
- project can be explained clearly in interviews
