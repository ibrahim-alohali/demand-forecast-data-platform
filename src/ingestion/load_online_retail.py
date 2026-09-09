"""Load UCI Online Retail II data into raw.online_retail.

Reads an xlsx or csv file and bulk-inserts rows using psycopg COPY.

Usage:
    python -m src.ingestion.load_online_retail --file data/online_retail_ii.xlsx
    python -m src.ingestion.load_online_retail --sample
    python -m src.ingestion.load_online_retail --file <path> --replace
    python -m src.ingestion.load_online_retail --file <path> --append
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from src.config import DBConfig
from src.db import get_connection

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SQL_PATH = PROJECT_ROOT / "sql" / "raw_online_retail.sql"
SAMPLE_PATH = PROJECT_ROOT / "data" / "sample_online_retail.csv"

# Explicit mapping from source column names to raw table column names.
# This makes the rename visible and reviewable.
COLUMN_MAP = {
    "Invoice": "invoice",
    "StockCode": "stock_code",
    "Description": "description",
    "Quantity": "quantity",
    "InvoiceDate": "invoice_date",
    "Price": "price",
    "Customer ID": "customer_id",
    "Country": "country",
}

# Columns in the raw table that we INSERT into (excluding loaded_at DEFAULT).
RAW_COLUMNS = [
    "invoice",
    "stock_code",
    "description",
    "quantity",
    "invoice_date",
    "price",
    "customer_id",
    "country",
    "source_file",
    "source_sheet",
]


def _get_row_count(conn) -> int:
    """Return current row count of raw.online_retail."""
    result = conn.execute("SELECT COUNT(*) FROM raw.online_retail")
    return result.fetchone()[0]


def _create_table(conn) -> None:
    """Execute the raw table DDL."""
    sql = SQL_PATH.read_text()
    conn.execute(sql)


def _read_source(file_path: Path) -> list[pd.DataFrame]:
    """Read all source sheets, validate columns, and attach provenance.

    For xlsx: reads all sheets and tags each with its sheet name.
    For csv: reads as a single dataframe tagged with 'csv'.
    """
    suffix = file_path.suffix.lower()
    filename = file_path.name

    if suffix not in {".xlsx", ".csv"}:
        raise ValueError(f"Unsupported file format: {suffix}")
    # Only empty fields mean NULL. Labels such as 'NA' and '\\N' stay literal.
    read_options = {"dtype": str, "keep_default_na": False, "na_values": [""]}
    try:
        sheets = (
            pd.read_excel(file_path, sheet_name=None, **read_options)
            if suffix == ".xlsx"
            else {"csv": pd.read_csv(file_path, **read_options)}
        )
    except pd.errors.EmptyDataError as exc:
        raise ValueError("Source file is empty") from exc
    result = []
    for sheet_name, df in sheets.items():
        df = df.rename(columns=COLUMN_MAP)
        if df.columns.duplicated().any():
            raise ValueError(f"Duplicate source columns in sheet {sheet_name!r}")
        missing = set(COLUMN_MAP.values()) - set(df.columns)
        if missing:
            raise ValueError(
                f"Missing columns in sheet {sheet_name!r}: {sorted(missing)}"
            )
        unexpected = set(df.columns) - set(COLUMN_MAP.values())
        if unexpected:
            raise ValueError(
                f"Unexpected columns in sheet {sheet_name!r}: {sorted(unexpected)}"
            )
        if df.empty:
            raise ValueError(f"Source sheet {sheet_name!r} is empty")
        prices = pd.to_numeric(df["price"], errors="coerce")
        invalid_prices = df["price"].notna() & (
            prices.isna() | prices.isin([float("inf"), float("-inf")])
        )
        if invalid_prices.any():
            raise ValueError(
                f"Invalid/non-finite prices in sheet {sheet_name!r}: "
                f"{int(invalid_prices.sum())} rows"
            )
        df["source_file"] = filename
        df["source_sheet"] = sheet_name
        result.append(df)
    if not result:
        raise ValueError("Source contains no data sheets")
    return result


def _bulk_insert(conn, df: pd.DataFrame) -> int:
    """Insert a dataframe into raw.online_retail using COPY protocol."""
    copy_sql = (
        f"COPY raw.online_retail ({', '.join(RAW_COLUMNS)}) "
        "FROM STDIN"
    )
    with conn.cursor().copy(copy_sql) as copy:
        for row in df[RAW_COLUMNS].itertuples(index=False, name=None):
            copy.write_row(tuple(None if pd.isna(value) else value for value in row))

    return len(df)


def load_online_retail(
    file_path: Path,
    mode: str = "safe",
    config: DBConfig | None = None,
) -> int:
    """Load a source file into raw.online_retail.

    Args:
        file_path: Path to the xlsx or csv file.
        mode: One of 'safe', 'replace', 'append'.
            - 'safe': fail if the table already has rows.
            - 'replace': truncate the table before loading.
            - 'append': skip the check and add rows.
        config: Optional DB config override.

    Returns:
        Number of rows loaded.

    Raises:
        RuntimeError: If mode is 'safe' and the table already has rows.
    """
    if mode not in {"safe", "replace", "append"}:
        raise ValueError(f"Unsupported load mode: {mode!r}")
    dataframes = _read_source(Path(file_path))
    conn = get_connection(config)
    try:
        _create_table(conn)
        # Serialize competing loads so 'safe' cannot race another writer.
        conn.execute("LOCK TABLE raw.online_retail IN EXCLUSIVE MODE")
        existing_rows = _get_row_count(conn)

        if mode == "safe" and existing_rows > 0:
            raise RuntimeError(
                f"Table raw.online_retail already has {existing_rows:,} rows. "
                f"Use --replace to truncate and reload, or --append to add rows."
            )

        if mode == "replace" and existing_rows > 0:
            conn.execute("TRUNCATE raw.online_retail")
            print(f"Truncated raw.online_retail ({existing_rows:,} rows removed)")

        total_rows = 0
        for df in dataframes:
            sheet = df["source_sheet"].iloc[0]
            rows = _bulk_insert(conn, df)
            total_rows += rows
            print(f"  Loaded {rows:,} rows from sheet '{sheet}'")

        conn.commit()
        print(f"Total: {total_rows:,} rows loaded into raw.online_retail")
        return total_rows
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load Online Retail II data into raw.online_retail"
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--file",
        type=Path,
        help="Path to the xlsx or csv file to load",
    )
    source.add_argument(
        "--sample",
        action="store_true",
        help="Load the sample CSV from data/sample_online_retail.csv",
    )

    strategy = parser.add_mutually_exclusive_group()
    strategy.add_argument(
        "--replace",
        action="store_true",
        help="Truncate existing data before loading",
    )
    strategy.add_argument(
        "--append",
        action="store_true",
        help="Append rows even if the table already has data",
    )

    args = parser.parse_args()

    file_path = SAMPLE_PATH if args.sample else args.file
    if not file_path.exists():
        print(f"Error: file not found: {file_path}", file=sys.stderr)
        sys.exit(1)

    if args.replace:
        mode = "replace"
    elif args.append:
        mode = "append"
    else:
        mode = "safe"

    load_online_retail(file_path, mode=mode)


if __name__ == "__main__":
    main()
