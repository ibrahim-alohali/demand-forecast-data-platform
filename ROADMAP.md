# Roadmap and current status

The project implements a local retail-data pipeline and one chronological evaluation of next-day recorded paid gross sales. Completion of a feature does not imply that every model assumption has been validated or that the project is ready for operational use.

## Implemented

| Area | Current behavior |
|---|---|
| Local setup | Python 3.11, PostgreSQL 16, Docker Compose, CLI commands and an isolated integration-test database. |
| Source ingestion | CSV and all Excel sheets; source metadata; explicit load modes; validated input; native COPY escaping and transactional replacement. |
| Staging | Exact business-column duplicate removal, text/customer-ID normalization and reconciled exclusions. |
| Analytical marts | Product-country daily gross paid sales and cancellations, plus a product dimension; both tables refresh atomically. |
| Quality checks | Eleven staging/mart contracts with positive and negative cases, including database-enforced constraints. |
| Calendar features | Observed product-country series expanded through the common end date, prior-day/prior-week predictors and documented warmup exclusions. |
| Evaluation | Fixed chronological split, a pooled linear regression, two simple comparators, explicit coverage and separate sample/full reports. |
| Documentation | Source attribution, analytical limits, reproducible commands, results and a project/interview walkthrough. |

The repair removed target-day and whole-period information from model inputs, corrected calendar windows, and replaced misleading single-day sample scoring with `not_evaluable`. It also fixed ingestion escaping and a mart refresh that could publish only one of its two tables.

## Evidence for the current implementation

- A clean editable install passed 142 tests and Ruff. CI now includes a PostgreSQL 16 service and runs the complete suite.
- The checked workbook contains 1,067,371 rows. Independent source reconstruction matched all 589,055 daily fact groups and the date/country summaries of 4,920 products.
- The full evaluation uses 3,754,801 later observations on the same dates for all three methods. Linear regression has lower RMSE but worse MAE than the previous-day and previous-weekday comparators.
- The sample demonstrates ingestion and transformations; its one-day span cannot establish forecast accuracy.

[VERIFICATION.md](docs/VERIFICATION.md) records the checked revisions, environments, repeatability and observed hosted-CI state. [DATA_SOURCE.md](DATA_SOURCE.md) records provenance and exclusions. Those records, not a blanket “all phases complete” statement, define what has been checked.

## Further work requires a concrete question

| Possible next step | Evidence or need that would justify it |
|---|---|
| More chronological backtests | Determine whether the current metric tradeoff holds across other time periods. |
| Product/country error breakdowns | Identify where aggregate scores conceal poor results or systematic bias. |
| Compare another forecasting method | Establish a specific weakness of the current model and keep the same evaluation/comparator discipline. |
| Reconsider duplicate or zero-price rules | Obtain source information that distinguishes repeated line items, free items and stock adjustments. |
| Estimate unmet demand or plan inventory | Obtain availability, stockout, lead-time and relevant business-cost data. The current source cannot support those claims. |
| Automate repeated runs | Have an actual recurring workload, defined failure handling and monitoring requirements. |

No new model, cloud deployment, scheduler, stock-risk feature or replenishment recommendation is promised by this roadmap.

## Before publishing another result

Keep the source identity and processing rules explicit, run the relevant tests and database checks, rebuild affected downstream layers, and record the evaluation dates, package versions, coverage and comparator results. Changes to the target or feature meaning require a new explanation and freshly generated evidence; old scores must not be presented as results of the changed implementation.
