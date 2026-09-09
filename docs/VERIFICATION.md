# Verification record

Local verification: 9 September 2026. The checked source is UCI Online Retail II,
SHA-256 `bcbe73b35f5b7babf197fb0cb983a11f5d9ff929078d4aa53d171b1f2df2e980`.
The full workbook remained unchanged throughout verification. This record describes
the repaired implementation, not the earlier same-day model evaluation.

## Checks and evidence

| Check | Result |
|---|---|
| Original unit/integration baseline | 86 tests passed before repairs; new regressions then reproduced the defects |
| Clean editable installation | `python -m pip install -e ".[dev]"` succeeded in a new Python 3.11 environment |
| Final unit and PostgreSQL integration suite | 142 tests passed; Ruff passed |
| Integration isolation guard | Missing connection variables and a database name without `_test` are rejected before fixtures run |
| Complete sample pipeline | 100 raw, 99 staging, 97 eligible lines, 89 daily facts/features and 83 products; 11 contracts pass |
| Sample forecast | `not_evaluable`, zero eligible rows/dates, null scores; all 89 rows lack required history |
| Full workbook pipeline | Both sheets loaded; staging, marts, contracts, features, registry and model completed |
| Full-source independent arithmetic | All 589,055 fact groups and 4,920 product date/country summaries matched a separate source census |
| Repeatability | Two complete loads/builds produced identical ordered business-data hashes for all five tables |
| Model repeatability | Evaluation JSON was byte-identical across both full runs in the same environment |
| Hosted CI for the repaired revision | Not yet observed; local test success is not a hosted CI result |

The test PostgreSQL service used a separate database, container, port and volume from
the original development database. Full-data runs and each integration-test group also
used separate databases. Tests write model reports to temporary directories.

## Reconciliation and independent method

The source census streamed the original workbook using openpyxl, collections and
Decimal. It did not import the application transformations or their SQL. It independently
applied the stated inclusion policy and computed quantities, amounts, invoice counts
and product date/country coverage. Database comparisons used read-only transactions.

| Quantity | Checked value |
|---|---:|
| Raw source rows | 1,067,371 |
| Missing-core exclusions | 0 |
| Negative-price exclusions | 5 |
| Exact eligible duplicates removed | 34,335 |
| Staging rows | 1,033,031 |
| Staged rows without a customer ID, retained | 235,146 |
| Malformed retained customer IDs converted to null | 0 |
| Non-stock lines excluded from marts | 5,525 |
| Zero-price stock lines excluded from marts | 5,989 |
| Remaining quantity/sign disagreements excluded | 0 |
| Eligible paid sale/return lines | 1,021,517 |
| Distinct product-country-date fact groups | 589,055 |
| Product dimension rows | 4,920 |
| Return-only fact groups | 7,033 |
| Recorded paid gross quantity | 11,188,217 |
| Recorded paid gross amount | GBP 19,655,609.62 |
| Return quantity magnitude | 467,874 |
| Return amount magnitude | GBP 724,708.68 |

The duplicate policy removes identical imported business fields across sheets as well
as within a sheet. Of the 34,335 excluded duplicates, 22,523 match a record already seen
in the other sheet. Identical fields alone cannot prove a real transaction was erroneous;
this is an explicit analytical policy. Raw retains all source rows and sheet attribution.

Every fact group's quantities, amounts and distinct-invoice count matched the independent
census. Product first/last dates and country counts also matched. Description selection
was covered by deterministic SQL and rebuild tests, rather than independently recomputed
by this census. There was no source price loss at the raw four-decimal scale, and no
difference in grand totals from per-group penny rounding for this workbook.

For example, product `23843` in the United Kingdom on 9 December 2011 has 80,995 gross
units and 80,995 cancellation units on the same day. Both are retained. Gross recorded
sales is different from net retained sales, inventory need or unconstrained demand.

## Forecast evaluation

See [the full report](full_model_report.md) and [machine-readable results](full_model_evaluation.json).
The model has lower RMSE but higher MAE than both naive comparators on the same test
observations. No score threshold was used as an acceptance condition.

- Calendar: 12,286,085 observations across 28,353 product-country series and 739 dates.
- History-eligible observations: 12,088,072; excluded for insufficient history: 198,013.
- Training: 8,333,271 observations, 8 December 2009 through 15 July 2011.
- Evaluation: 3,754,801 observations, 16 July through 9 December 2011.
- During the evaluation period, 35,930 calendar rows lack required history; 5,195
  evaluated series were absent from training. These series are part of the pooled
  model evaluation, not evidence of a separately validated cold-start model.

The four predictors contain only observations preceding the target day plus its known
weekday. The fitted model is fixed; earlier evaluation observations can update later
one-day lags. Calendar zeros mean zero recorded sales, not known zero demand.

## Repeatability fingerprints

Each table was streamed in a deterministic business-column order through SHA-256.
Load/staging timestamps were excluded; all other stored columns were included. The
following hashes matched across both complete runs:

| Table | SHA-256 |
|---|---|
| `raw.online_retail` | `f3f8ed1b7a254979eb09191c645234490f5a67c0c163ecf958ec5ffbeffc49b3` |
| `staging.stg_online_retail` | `23c66e5f3898d5b3660204fb81db1ac0a41dac5c67e49fd4438e29afdbd49e2f` |
| `marts.fct_daily_product_sales` | `1dbd673d984be2821b2fbd7701fbbfe2450bc92cd7e900f7867ec16593914c90` |
| `marts.dim_product` | `6ecf515bcfd28934b434ca6759c03ce381b000862c9131c9198055c51126cc1a` |
| `features.product_daily_features` | `4a56b612b94bfa7150164711689098e3dbe07937cac86f747367d621c0ede488` |

Evaluation JSON SHA-256: `a6003bd280b995cd5254423ffb8687f7179c0406214d6f3ee5340050ae343062`.
Hashes provide evidence of repeated results under the recorded environment; they are
not a guarantee that future library versions use identical text or floating serialization.

## Environment and remaining limits

Full evaluation and repetition used Python 3.11, PostgreSQL 16, pandas 3.0.1, psycopg
3.3.3, NumPy 2.4.3 and scikit-learn 1.8.0 on Windows. The exact package snapshot is in
[full_run_requirements.txt](full_run_requirements.txt). To reproduce that environment in
a separate virtual environment, install those requirements before the editable project.

A clean current install also passed all 142 tests using pandas 3.0.5, psycopg 3.3.5,
NumPy 2.4.6, scikit-learn 1.9.0, pytest 9.1.1 and Ruff 0.16.6. This additional suite run
does not claim bit-identical full-model coefficients across dependency versions.

The data is historical, one temporal split is used, inventory is unknown, and aggregate
errors can hide variation across products and countries. There is no live forecasting
deployment or measured business impact. Hosted CI remains open until its actual result
is observed after publication.
