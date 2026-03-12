"""Unit tests for ingestion logic. No database required."""

import pandas as pd

from src.ingestion.load_online_retail import COLUMN_MAP, SAMPLE_PATH


def test_column_rename_mapping():
    """COLUMN_MAP covers all expected source columns."""
    expected_source_columns = {
        "Invoice",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "Price",
        "Customer ID",
        "Country",
    }
    assert set(COLUMN_MAP.keys()) == expected_source_columns


def test_sample_file_exists():
    """Sample CSV file exists at the expected path."""
    assert SAMPLE_PATH.exists(), f"Sample file not found: {SAMPLE_PATH}"


def test_sample_file_readable():
    """Sample CSV loads with expected columns and reasonable row count."""
    df = pd.read_csv(SAMPLE_PATH)
    expected_columns = {
        "invoice",
        "stock_code",
        "description",
        "quantity",
        "invoice_date",
        "price",
        "customer_id",
        "country",
    }
    assert expected_columns.issubset(set(df.columns)), (
        f"Missing columns: {expected_columns - set(df.columns)}"
    )
    assert len(df) >= 50, f"Sample too small: {len(df)} rows"
    assert len(df) <= 200, f"Sample too large: {len(df)} rows"


def test_sample_has_negative_quantity():
    """Sample contains at least one return (negative quantity)."""
    df = pd.read_csv(SAMPLE_PATH)
    assert (df["quantity"] < 0).any(), "No negative quantities found in sample"


def test_sample_has_null_customer():
    """Sample contains at least one null customer_id."""
    df = pd.read_csv(SAMPLE_PATH)
    assert df["customer_id"].isna().any(), "No null customer_ids found in sample"


def test_sample_has_multiple_countries():
    """Sample contains transactions from more than one country."""
    df = pd.read_csv(SAMPLE_PATH)
    assert df["country"].nunique() >= 2, "Sample should have multiple countries"
