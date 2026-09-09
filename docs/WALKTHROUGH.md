# Project walkthrough and interview refresher

This project turns retail transaction files into a documented PostgreSQL pipeline and a one-day forecasting evaluation. Its main contribution is a traceable path from source rows to analytical results, with explicit cleaning decisions, reproducible features and checks that catch invalid assumptions.

The forecasting target is **recorded paid gross sales quantity for a product in a country on the next calendar day**. It is not net sales after returns or an estimate of all customer demand. Start an explanation with that distinction.

## 1. Follow the data through each layer

| Layer | What one row represents | What happens here |
|---|---|---|
| `raw.online_retail` | One ingested source row, including source duplicates. | Preserve source business values and attach filename, worksheet and load time. CSV and all workbook sheets use the same validated ingestion path. |
| `staging.stg_online_retail` | One retained source-business line after exact duplicate removal. | Normalize text, classify cancellation/non-stock lines, convert valid customer IDs, and disclose exclusions. |
| `marts.fct_daily_product_sales` | One product, calendar date and country with an eligible paid sale or return. | Aggregate gross paid quantity/revenue and sales invoice counts separately from absolute returned quantity/revenue and return invoice counts. |
| `marts.dim_product` | One product code. | Keep the latest nonempty description, first/last eligible dates and whole-period country count. |
| `features.product_daily_features` | One product-country-calendar-day from the pair's first eligible record through the shared dataset end. | Fill missing transaction days with zero recorded sales and derive prior-day/prior-week inputs. |
| Evaluation JSON/report | One evaluation of the currently loaded feature table. | Record training/test periods, coverage, model and comparator metrics, source metadata and limitations. |

A grain is the precise meaning of one row. It tells you which columns must uniquely identify it and which joins or aggregations are valid. Joining the product dimension to daily facts by product code is many-to-one; adding country to the dimension without changing the join could multiply facts and overstate sales.

## 2. Explain the cleaning decisions

The raw layer preserves the evidence. Staging excludes missing required invoice/product/country values, dates, quantities or prices, and excludes negative prices. It removes exact duplicates using the original business columns before normalizing text. The console audit reconciles raw rows, those exclusions and the resulting staging rows.

Customer IDs are optional for this product-level analysis. Valid positive integer IDs, including forms such as `12345.0`, become integers. Missing or malformed IDs remain null, with malformed nonempty values counted. The pipeline does not invent customers or drop otherwise usable sales solely because an ID is unavailable.

The cancellation flag comes from a case-insensitive `C` invoice prefix. It identifies a source convention; the mart also checks quantity and price before interpreting a line:

| Line | Mart treatment |
|---|---|
| Stock item, positive price, positive quantity, non-cancellation invoice | Paid gross sale. |
| Stock item, positive price, negative quantity, cancellation invoice | Return, stored as absolute returned units and amount. |
| Known non-stock code | Excluded from product sales/returns. |
| Zero-price stock line | Retained for inspection in staging, excluded from paid-sales marts; its business meaning is uncertain. |
| Non-cancellation negative quantity, zero quantity, or positive-quantity cancellation | Retained in staging, excluded from marts because the signs and invoice convention do not support a clear sale/return interpretation. |

Both fact and dimension use the same eligibility rules. Return-only product-days are retained with zero gross sales and positive returned quantities. Gross and returned amounts can be used to calculate a net measure explicitly; the forecast itself targets gross paid units.

The stock-code exclusion list is a documented rule, not a universal product classifier. The dataset may contain other ambiguous codes. Explain that limitation instead of claiming perfect classification.

## 3. Understand the forecast before explaining its scores

For target day `t`, the model receives exactly four values:

1. Quantity recorded on `t−1`.
2. Quantity recorded on `t−7`.
3. Mean quantity across `t−7` through `t−1`.
4. The known weekday of `t`, Monday `0` through Sunday `6`.

The first seven days of a newly observed product-country series lack the required history and are excluded from fitting and scoring. Missing transaction days inside a series contribute zero recorded quantity. Each series continues to the common observation end, even after its last sale. Stopping at its last sale would use knowledge unavailable at forecast time.

For example, if a product recorded seven units on 1 January and none on 2–7 January, its 8 January previous-day value is zero, seven-day lag is seven, and prior-week mean is one. The 8 January quantity has no role in those predictors. This is an illustration, not a result claimed for the source dataset.

The earliest 80% of eligible target dates train one pooled `LinearRegression`; the latest 20% evaluate it. Training dates are rounded down, with at least one date retained for training. All observations on the same date remain on the same side of the cutoff. Negative predictions are floored at zero.

The fitted model remains fixed during evaluation. A forecast for a later test day can use sales already observed on earlier test days. This is a sequence of one-day forecasts, not a multi-day forecast made without receiving new observations.

Two simple comparators forecast yesterday's quantity and the quantity seven days earlier. They are scored on exactly the same observations as the model:

- **MAE:** average absolute error, in units per product-country-day.
- **RMSE:** error in the same units, with greater weight on large misses.
- **R-squared:** improvement relative to variation around the observed mean; it can be negative. It is null for fewer than two observations or a constant observed target.

The JSON also discloses excluded history rows, new evaluation series, date boundaries, coefficients and labeled smallest/nearest-median/largest absolute-error examples. These help explain aggregate scores without selecting only flattering examples. Current full-run results belong in the matching evaluation evidence; no accuracy claim is needed to explain how the method works.

## 4. Explain what the repair changed

The earlier model used target-day revenue and other same-day quantities. Its rolling average included the target day and counted observed rows instead of consecutive calendar days. It also used whole-period country counts and could randomly split products when there was only one date. Those choices could produce an apparently useful score without testing a forecast made before the target day.

The repair makes the information boundary explicit: only prior observations and known calendar information enter the model. Calendar gaps are represented, the first week is excluded, and a single-day sample reports `not_evaluable` with null scores. A chronological split helps only when the predictors also respect that timing.

Database repairs matter as well. Ingestion now uses a COPY operation whose serialization matches its format. Failed replacement keeps the previous raw data. Fact and dimension refresh together in a transaction. Feature schema and data rebuild together, so an old table cannot silently preserve obsolete feature meanings after a code update.

## 5. Run a small demonstration

Follow the [README setup](../README.md) in a fresh, dedicated demonstration database with PostgreSQL 16 and the project dependencies installed. Run these commands from the repository root:

```bash
python -m src.ingestion.load_online_retail --sample
python -m src.staging.build_staging
python -m src.marts.build_marts
python -m src.quality.run_contracts
python -m src.features.build_features
python -m src.features.validate_registry
python -m src.model.train_baseline --output-dir data/sample --source-file data/sample_online_retail.csv
```

Stop if a step fails. The first command uses safe mode and refuses to load into a nonempty raw table. For an intentional rerun of this dedicated sample database, use `--sample --replace`; replacement changes its raw data. Keep the full-source database separate from demonstrations and integration tests.

Open `data/sample/model_evaluation.json` and `data/sample/model_report.md`. The 100-row sample should yield 89 feature rows, all excluded for insufficient history, with zero training/evaluation rows and null scores. That outcome is correct: one day's data cannot support this forecasting evaluation.

In a SQL client connected to the same sample database, ask: which product-country pairs have the most recorded paid units, and how many units were returned?

```sql
SELECT stock_code, country,
       SUM(total_quantity) AS paid_gross_units,
       SUM(return_quantity) AS returned_units
FROM marts.fct_daily_product_sales
GROUP BY stock_code, country
ORDER BY paid_gross_units DESC, stock_code, country
LIMIT 5;
```

Explain that this groups daily facts over the loaded period and keeps returns separate. It answers a historical sales question; it is not a forecast or an inventory recommendation.

Run unit tests and lint with:

```bash
python -m pytest tests/ -m "not integration"
python -m ruff check src/ tests/
```

Integration tests truncate tables. Use the separate test database and explicit environment settings described in the README; the test guard requires a database name ending in `_test`. Passing unit tests alone does not verify PostgreSQL behavior or the full workbook pipeline.

## 6. A five-minute walkthrough

| Time | What to show and explain |
|---|---|
| 0:00–0:40 | State the question: prepare reliable product-country sales history and evaluate next-day recorded paid units. Name the source and distinguish recorded sales from unconstrained demand. |
| 0:40–1:30 | Show the raw → staging → marts path. Explain one exclusion, optional customer IDs and the daily fact grain. |
| 1:30–2:15 | Run or show the grouped SQL result. Explain gross units versus returned units and why a correct join matters. |
| 2:15–3:15 | Show the four allowed predictors and the calendar-gap example. Explain why target-day revenue would leak information. |
| 3:15–4:15 | Show the actual full-run report, its date cutoff and the two naive comparators. If demonstrating only the sample, show `not_evaluable` and explain why no score is defensible. |
| 4:15–5:00 | Show one regression test and one transactional failure test. Close with missing stock availability, the fixed holdout period and one improvement that evidence would justify. |

Practise with the code or report visible first, then repeat without reading sentences from it. If a question exposes a gap, inspect the relevant layer and rehearse the explanation again.

## 7. Interview questions to rehearse

**1. Why use separate raw, staging and mart layers?**
Raw keeps the source evidence. Staging makes cleaning rules and exclusions inspectable. Marts define stable analytical grains. That separation makes it possible to trace a surprising total back to the source and decide whether the issue is input quality, a rule or an aggregation.

**2. Why not remove every row with a missing customer ID?**
The analysis is about products, countries and dates, so a customer ID is not required to count eligible paid sales. Removing those rows would discard otherwise usable activity. Missing IDs remain missing; the project makes no claim to complete customer-level analysis.

**3. Why keep returns separate from sales?**
A return is a different recorded event. Mixing negative returns into the gross-sales target changes what the model predicts and can hide substantial activity. The mart exposes both, allowing a net measure to be calculated when that is the actual question.

**4. What was wrong with the original model evaluation?**
Some inputs contained results from the target day or the full observation period. The rolling window also treated seven observed records as seven calendar days. The repair uses only earlier observations, a dense daily calendar and a chronological split; perturbation tests verify that target-day and future changes cannot alter earlier predictors.

**5. Why compare linear regression with such simple forecasts?**
A more complicated calculation is useful only if it adds something against a relevant baseline. Yesterday and the previous weekday are clear reference points. If the linear model loses on the same test observations, publish that result and inspect where it loses before adding complexity.

**6. What does a zero mean on a missing transaction day?**
It means no eligible paid sales were recorded in the available data for that product-country-date. It does not establish that nobody wanted the product or that it was in stock. That is why the target is described as recorded paid sales rather than unconstrained demand.

**7. How do you know the pipeline is reliable?**
Use several kinds of evidence: unit tests for validation, PostgreSQL integration tests for transformations and rollback, data contracts for table assumptions, and full-source reconciliations and repeat runs. Passing one layer does not replace the others. A constraint that rejects invalid input before a contract runs is useful protection, and should be described accurately.

**8. What would you improve next?**
Start with the observed error patterns and a concrete decision. Additional chronological backtests could test stability across periods; product/country error breakdowns could expose poor subgroup performance. Inventory or stockout data would be needed to reason about unmet demand. Do not claim those improvements are implemented or assume a new model will perform better.
