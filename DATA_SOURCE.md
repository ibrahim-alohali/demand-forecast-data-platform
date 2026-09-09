# Data source and reconciliation

This project uses **Online Retail II**, a public record of transactions from a UK-based non-store retailer between **2009-12-01 and 2011-12-09**. Unit prices are in **pounds sterling (£)**. A C-prefixed invoice denotes a cancellation. The dataset does not provide inventory availability, stockouts or a complete measure of customer demand. [UCI dataset page](https://archive.ics.uci.edu/dataset/502/online+retail+ii)

## Attribution and license

Chen, D. (2012). *Online Retail II* [Dataset]. UCI Machine Learning Repository. [DOI: 10.24432/C5CG6D](https://doi.org/10.24432/C5CG6D).

UCI publishes the dataset under [Creative Commons Attribution 4.0 International](https://creativecommons.org/licenses/by/4.0/). This attribution concerns the dataset; it does not independently assign a license to the project's code.

## Checked input

| Item | Value |
|---|---|
| Original workbook | `online_retail_II.xlsx` |
| File size | 45,622,278 bytes |
| Worksheet `Year 2009-2010` | 525,461 rows |
| Worksheet `Year 2010-2011` | 541,910 rows |
| Total source rows | 1,067,371 |
| SHA-256 | `bcbe73b35f5b7babf197fb0cb983a11f5d9ff929078d4aa53d171b1f2df2e980` |

The original workbook is retained unchanged outside version control. The downloader retrieves the [official archive](https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip) and saves the workbook as `data/online_retail_ii.xlsx`; the canonical filename differs in capitalization from the original.

From the repository root:

~~~powershell
./.venv/Scripts/python.exe -m src.ingestion.download
Get-FileHash data/online_retail_ii.xlsx -Algorithm SHA256
~~~

An existing workbook is reused. Compare its checksum with the checked input above before treating a rerun as reproduction of the published results. If it differs, establish why and record the actual source identity with the new evaluation.

The included `data/sample_online_retail.csv` is a 100-row, single-day demonstration fixture. Its small size supports pipeline checks, not forecasting accuracy estimates. The published full-run metrics use the two-sheet workbook.

## Fields and preservation

| Workbook field | Meaning |
|---|---|
| `Invoice` | Recorded invoice identifier; C prefix indicates cancellation. |
| `StockCode` | Recorded item code; some codes describe non-product entries. |
| `Description` | Recorded item/entry description, sometimes missing. |
| `Quantity` | Recorded item quantity on the line. |
| `InvoiceDate` | Source timestamp; no timezone conversion is applied. |
| `Price` | Sterling unit price. |
| `Customer ID` | Optional recorded customer identifier. |
| `Country` | Recorded customer country. |

Raw ingestion preserves every input row and attaches filename, worksheet and load time. It maps fields into PostgreSQL types: integer quantities, timestamp dates and four-decimal numeric prices. Empty fields become null; labels such as `NA` remain literal text. It is a typed representation, not a byte-perfect copy of the workbook. The original file and checksum remain the source evidence.

The checked workbook's prices needed no rounding at the raw table's four-decimal scale. Independent line sums and sums after fact-group penny rounding also agreed exactly for this workbook. This observation does not promise lossless numeric representation for every possible future input.

## Full-workbook row reconciliation

The exclusion counts below are mutually exclusive within each step.

| Raw to staging | Rows |
|---|---:|
| Raw input | 1,067,371 |
| Missing required fields excluded | 0 |
| Negative prices excluded after the required-field check | 5 |
| Exact eligible business-column duplicates removed | 34,335 |
| Staging output | **1,033,031** |

Duplicates are compared on the original eight business fields before normalizing text, across both worksheets. Of the removed rows, 22,523 matched a row already seen in the other worksheet. Filename, sheet and load timestamp do not make an otherwise identical business line distinct.

This is a declared processing assumption. Without a source line identifier, the data cannot distinguish an accidental duplicate from a legitimate repeated line with identical fields. Raw retains all rows so the decision can be revisited.

Staging retains 235,146 rows with missing customer IDs. No nonempty customer IDs in this workbook required conversion to null as malformed values. Customer IDs are not required for the product-country analysis.

The five negative-price rows use code B and describe bad-debt adjustments. They are excluded from this sales analysis, while their original records remain in raw.

| Staging to marts | Lines |
|---|---:|
| Staging input | 1,033,031 |
| Non-stock entries excluded | 5,525 |
| Zero-price stock entries excluded | 5,989 |
| Remaining stock quantity/cancellation disagreements excluded | 0 |
| Eligible paid sale/cancellation lines | **1,021,517** |

The non-stock list is `POST`, `DOT`, `D`, `M`, `B`, `BANK CHARGES`, `PADS`, `AMAZONFEE`, `C2` and `CRUK`, compared without case differences. It is an explicit rule, not a universal guarantee that every other code is a physical product.

Eligible paid sales require positive price, positive quantity and a non-C invoice. Eligible cancellations require positive price, negative quantity and a C invoice. Quantity-sign disagreements and zero-price lines remain inspectable in staging; the pipeline does not guess their demand meaning.

The raw file contains 6,202 zero-price rows, 3,457 negative non-C quantities and one positive-quantity C line. These descriptive categories overlap. After non-stock and zero-price exclusions, no additional sign disagreement remained in the eligible stock population.

## Independently reconciled results

An independent streaming reconstruction using openpyxl, Python collections and Decimal, without importing the application transformations, matched all **589,055** fact groups' quantities, sterling amounts and per-group distinct-invoice counts. It also matched the date ranges and country counts of all **4,920** product rows. A separate read-only query confirmed that every fact grain was unique.

| Fact-table result | Value |
|---|---:|
| Recorded paid gross quantity | 11,188,217 units |
| Recorded paid gross amount | £19,655,609.62 |
| Cancellation quantity magnitude | 467,874 units |
| Cancellation amount magnitude | £724,708.68 |
| Return-only product-country-date groups | 7,033 |

The forecast target is **gross** recorded paid quantity. It does not subtract later or same-day cancellations. For example, stock code 23843 in the United Kingdom on 2011-12-09 has 80,995 gross units (£168,469.60) and the same quantities and amounts canceled on that date. Both events are retained; presenting the gross total as net retained sales or inventory need would change its meaning.

The feature calendar expands 28,353 observed product-country series to 12,286,085 daily rows. Dates without eligible transactions are zero recorded sales, not proof of zero demand or available inventory. The first seven days of each new series lack required history; 198,013 rows are excluded from model fitting and evaluation.

See [VERIFICATION.md](docs/VERIFICATION.md) for revision/environment and repeat-run evidence, [the full evaluation](docs/full_model_report.md) for model results, and [ARCHITECTURE.md](ARCHITECTURE.md) for the transformation rules.
