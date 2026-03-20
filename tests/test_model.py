"""Unit tests for baseline model. No database required."""

from src.model.train_baseline import FEATURE_COLUMNS, TARGET


def test_module_importable():
    from src.model import train_baseline  # noqa: F401


def test_feature_columns_defined():
    assert isinstance(FEATURE_COLUMNS, list)
    assert len(FEATURE_COLUMNS) > 0


def test_target_defined():
    assert isinstance(TARGET, str)
    assert TARGET == "total_quantity"


def test_feature_columns_are_strings():
    for col in FEATURE_COLUMNS:
        assert isinstance(col, str)


def test_target_not_in_features():
    assert TARGET not in FEATURE_COLUMNS
