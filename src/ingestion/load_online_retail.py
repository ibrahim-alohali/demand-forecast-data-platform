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
import io
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
    """Read source file and return list of (dataframe, sheet_name) pairs.

    For xlsx: reads all sheets and tags each with its sheet name.
    For csv: reads as a single dataframe tagged with 'csv'.
    """
    suffix = file_path.suffix.lower()
    filename = file_path.name

    if suffix == ".xlsx":
        sheets = pd.read_excel(file_path, sheet_name=None, dtype=str)
        result = []
        for sheet_name, df in sheets.items():
            df = df.rename(columns=COLUMN_MAP)
            df["source_file"] = filename
            df["source_sheet"] = sheet_name
            result.append(df)
        return result
    elif suffix == ".csv":
        df = pd.read_csv(file_path, dtype=str)
        # CSV columns may already be snake_case (sample file) or source-case
        if "Invoice" in df.columns:
            df = df.rename(columns=COLUMN_MAP)
        df["source_file"] = filename
        df["source_sheet"] = "csv"
        return [df]
    else:
        raise ValueError(f"Unsupported file format: {suffix}")


def _bulk_insert(conn, df: pd.DataFrame) -> int:
    """Insert a dataframe into raw.online_retail using COPY protocol."""
    # Build a CSV-like text buffer for COPY
    buffer = io.StringIO()
    df[RAW_COLUMNS].to_csv(buffer, index=False, header=False, sep="\t", na_rep="\\N")
    buffer.seek(0)

    copy_sql = (
        f"COPY raw.online_retail ({', '.join(RAW_COLUMNS)}) "
        f"FROM STDIN WITH (FORMAT text)"
    )
    with conn.cursor().copy(copy_sql) as copy:
        while data := buffer.read(8192):
            copy.write(data)

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
    conn = get_connection(config)
    try:
        _create_table(conn)
        conn.commit()

        existing_rows = _get_row_count(conn)

        if mode == "safe" and existing_rows > 0:
            raise RuntimeError(
                f"Table raw.online_retail already has {existing_rows:,} rows. "
                f"Use --replace to truncate and reload, or --append to add rows."
            )

        if mode == "replace" and existing_rows > 0:
            conn.execute("TRUNCATE raw.online_retail")
            print(f"Truncated raw.online_retail ({existing_rows:,} rows removed)")

        dataframes = _read_source(file_path)
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
