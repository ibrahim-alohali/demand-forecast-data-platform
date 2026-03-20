"""Unit tests for features layer. No database required."""

from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = PROJECT_ROOT / "sql"
REGISTRY_PATH = PROJECT_ROOT / "src" / "features" / "registry.yml"

DDL_FILE = SQL_DIR / "features_product_daily.sql"
INSERT_FILE = SQL_DIR / "features_product_daily_insert.sql"

REQUIRED_FIELDS = {
    "name", "description", "source_table",
    "grain", "transform", "leakage_risk",
}


# --- SQL file tests ---


def test_ddl_file_exists():
    assert DDL_FILE.exists()
    assert DDL_FILE.stat().st_size > 0


def test_insert_file_exists():
    assert INSERT_FILE.exists()
    assert INSERT_FILE.stat().st_size > 0


def test_insert_reads_from_marts():
    sql = INSERT_FILE.read_text()
    assert "marts.fct_daily_product_sales" in sql
    assert "marts.dim_product" in sql


def test_insert_writes_to_features():
    sql = INSERT_FILE.read_text()
    assert "features.product_daily_features" in sql


# --- registry.yml tests ---


def test_registry_is_valid_yaml():
    with open(REGISTRY_PATH) as f:
        data = yaml.safe_load(f)
    assert "features" in data
    assert isinstance(data["features"], list)


def test_registry_has_all_required_fields():
    with open(REGISTRY_PATH) as f:
        data = yaml.safe_load(f)
    for entry in data["features"]:
        missing = REQUIRED_FIELDS - set(entry.keys())
        name = entry.get("name", "?")
        assert not missing, f"Feature '{name}' missing: {missing}"


def test_registry_no_duplicate_names():
    with open(REGISTRY_PATH) as f:
        data = yaml.safe_load(f)
    names = [entry["name"] for entry in data["features"]]
    assert len(names) == len(set(names)), f"Duplicate feature names: {names}"


def test_registry_has_nine_features():
    with open(REGISTRY_PATH) as f:
        data = yaml.safe_load(f)
    assert len(data["features"]) == 9
