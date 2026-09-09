"""Unit tests for ingestion logic. No database required."""

import pandas as pd
import pytest

from src.ingestion.load_online_retail import COLUMN_MAP, SAMPLE_PATH


@pytest.mark.parametrize("contents,suffix,message", [
    ("", ".csv", "empty"),
    ("invoice,quantity\na,1\n", ".csv", "Missing columns"),
    ("", ".txt", "Unsupported file format"),
])
def test_bad_source_rejected_before_connection(tmp_path, monkeypatch,
                                               contents, suffix, message):
    import importlib

    module = importlib.import_module("src.ingestion.load_online_retail")
    path = tmp_path / f"bad{suffix}"
    path.write_text(contents, encoding="utf-8")

    def unexpected_connection(*args):
        pytest.fail("Malformed input must be rejected before connecting")

    monkeypatch.setattr(module, "get_connection", unexpected_connection)
    with pytest.raises(ValueError, match=message):
        module.load_online_retail(path, mode="replace")


def test_header_only_and_ambiguous_columns_rejected(tmp_path):
    from src.ingestion.load_online_retail import _read_source

    frame = pd.read_csv(SAMPLE_PATH)
    path = tmp_path / "empty.csv"
    frame.iloc[:0].to_csv(path, index=False)
    with pytest.raises(ValueError, match="empty"):
        _read_source(path)
    frame["Invoice"] = frame["invoice"]
    frame.to_csv(path, index=False)
    with pytest.raises(ValueError, match="Duplicate"):
        _read_source(path)


@pytest.mark.parametrize("price", ["not a number", "NaN", "Infinity", "-Infinity"])
def test_invalid_price_rejected(tmp_path, price):
    from src.ingestion.load_online_retail import _read_source

    frame = pd.read_csv(SAMPLE_PATH, dtype=str).iloc[:1].copy()
    frame["price"] = price
    path = tmp_path / "price.csv"
    frame.to_csv(path, index=False)
    with pytest.raises(ValueError, match="Invalid/non-finite prices"):
        _read_source(path)


def test_all_excel_sheets_and_literal_null_labels(tmp_path):
    from src.ingestion.load_online_retail import _read_source

    path = tmp_path / "two_sheets.xlsx"
    frame = pd.read_csv(SAMPLE_PATH, dtype=str).iloc[:2].copy()
    frame["description"] = ["NA", "NULL"]
    frame = frame.rename(columns={value: key for key, value in COLUMN_MAP.items()})
    with pd.ExcelWriter(path) as writer:
        frame.to_excel(writer, sheet_name="First", index=False)
        frame.to_excel(writer, sheet_name="Second", index=False)
    sheets = _read_source(path)
    assert len(sheets) == 2
    for sheet, name in zip(sheets, ["First", "Second"], strict=True):
        assert sheet["source_sheet"].tolist() == [name, name]
        assert sheet["source_file"].tolist() == [path.name, path.name]
        assert sheet["description"].tolist() == ["NA", "NULL"]


def test_download_ignores_archive_paths(tmp_path, monkeypatch):
    import importlib
    import zipfile

    module = importlib.import_module("src.ingestion.download")

    def fake_download(url, destination):
        with zipfile.ZipFile(destination, "w") as archive:
            archive.writestr("../outside.xlsx", b"workbook bytes")

    monkeypatch.setattr(module, "urlretrieve", fake_download)
    result = module.download(tmp_path / "data")
    assert result == tmp_path / "data" / "online_retail_ii.xlsx"
    assert result.read_bytes() == b"workbook bytes"
    assert not (tmp_path / "outside.xlsx").exists()
    assert not result.with_suffix(".xlsx.part").exists()


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
