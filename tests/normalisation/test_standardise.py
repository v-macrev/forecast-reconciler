from datetime import date

import polars as pl

from forecast_reconciler.config import ReconciliationConfig
from forecast_reconciler.exceptions import DataValidationError
from forecast_reconciler.normalisation.standardise import (
    standardise_granular_input,
    standardise_macro_input,
)


def test_standardise_macro_input_returns_canonical_dataset():
    config = ReconciliationConfig()
    df = pl.DataFrame(
        {
            "period": ["2026-01", "2026-02"],
            "market": ["SP", "RJ"],
            "channel": ["Retail", "Wholesale"],
            "macro_target_qty": ["100", 250],
        },
        strict=False,
    )

    result = standardise_macro_input(df=df, config=config)

    assert result.columns == ["period", "market", "channel", "macro_target_qty"]
    assert result.schema["period"] == pl.Date
    assert result.schema["macro_target_qty"] == pl.Float64
    assert result.get_column("period").to_list() == [
        date(2026, 1, 1),
        date(2026, 2, 1),
    ]
    assert result.get_column("macro_target_qty").to_list() == [100.0, 250.0]


def test_standardise_granular_input_returns_canonical_dataset():
    config = ReconciliationConfig()
    df = pl.DataFrame(
        {
            "period": ["2026/01/31", "15/02/2026"],
            "market": ["SP", "SP"],
            "channel": ["Retail", "Retail"],
            "sku": ["SKU-001", "SKU-002"],
            "baseline_qty": ["60", 40],
        },
        strict=False,
    )

    result = standardise_granular_input(df=df, config=config)

    assert result.columns == ["period", "market", "channel", "sku", "baseline_qty"]
    assert result.schema["period"] == pl.Date
    assert result.schema["baseline_qty"] == pl.Float64
    assert result.get_column("period").to_list() == [
        date(2026, 1, 1),
        date(2026, 2, 1),
    ]
    assert result.get_column("baseline_qty").to_list() == [60.0, 40.0]


def test_standardise_macro_input_rejects_duplicate_business_keys():
    config = ReconciliationConfig()
    df = pl.DataFrame(
        {
            "period": ["2026-01", "2026-01"],
            "market": ["SP", "SP"],
            "channel": ["Retail", "Retail"],
            "macro_target_qty": [100, 120],
        }
    )

    try:
        standardise_macro_input(df=df, config=config)
    except DataValidationError as exc:
        assert str(exc) == (
            "macro dataset contains duplicate business keys for columns: "
            "period, market, channel."
        )
    else:
        raise AssertionError("Expected DataValidationError for duplicate macro keys.")


def test_standardise_granular_input_rejects_duplicate_business_keys():
    config = ReconciliationConfig()
    df = pl.DataFrame(
        {
            "period": ["2026-01", "2026-01"],
            "market": ["SP", "SP"],
            "channel": ["Retail", "Retail"],
            "sku": ["SKU-001", "SKU-001"],
            "baseline_qty": [60, 40],
        }
    )

    try:
        standardise_granular_input(df=df, config=config)
    except DataValidationError as exc:
        assert str(exc) == (
            "granular dataset contains duplicate business keys for columns: "
            "period, market, channel, sku."
        )
    else:
        raise AssertionError("Expected DataValidationError for duplicate granular keys.")


def test_standardise_macro_input_rejects_null_quantity_values():
    config = ReconciliationConfig()
    df = pl.DataFrame(
        {
            "period": ["2026-01"],
            "market": ["SP"],
            "channel": ["Retail"],
            "macro_target_qty": [None],
        },
        strict=False,
    )

    try:
        standardise_macro_input(df=df, config=config)
    except DataValidationError as exc:
        assert str(exc) == (
            "macro dataset contains null values in quantity column 'macro_target_qty'."
        )
    else:
        raise AssertionError("Expected DataValidationError for null macro quantity.")


def test_standardise_granular_input_rejects_empty_string_quantity_values():
    config = ReconciliationConfig()
    df = pl.DataFrame(
        {
            "period": ["2026-01"],
            "market": ["SP"],
            "channel": ["Retail"],
            "sku": ["SKU-001"],
            "baseline_qty": ["   "],
        }
    )

    try:
        standardise_granular_input(df=df, config=config)
    except DataValidationError as exc:
        assert str(exc) == (
            "granular dataset contains empty string values in quantity column 'baseline_qty'."
        )
    else:
        raise AssertionError("Expected DataValidationError for empty granular quantity.")


def test_standardise_macro_input_rejects_non_numeric_quantity_values():
    config = ReconciliationConfig()
    df = pl.DataFrame(
        {
            "period": ["2026-01"],
            "market": ["SP"],
            "channel": ["Retail"],
            "macro_target_qty": ["abc"],
        }
    )

    try:
        standardise_macro_input(df=df, config=config)
    except DataValidationError as exc:
        assert str(exc) == (
            "macro dataset contains non-numeric value 'abc' in quantity column 'macro_target_qty'."
        )
    else:
        raise AssertionError("Expected DataValidationError for non-numeric macro quantity.")


def test_standardise_granular_input_accepts_thousands_separator_strings():
    config = ReconciliationConfig()
    df = pl.DataFrame(
        {
            "period": ["2026-01"],
            "market": ["SP"],
            "channel": ["Retail"],
            "sku": ["SKU-001"],
            "baseline_qty": ["1,250.5"],
        }
    )

    result = standardise_granular_input(df=df, config=config)

    assert result.get_column("baseline_qty").to_list() == [1250.5]


def test_standardise_respects_custom_group_keys_for_macro_and_granular():
    config = ReconciliationConfig(group_keys=("period", "market"))

    macro_df = pl.DataFrame(
        {
            "period": ["2026-01"],
            "market": ["SP"],
            "macro_target_qty": [100],
        }
    )
    granular_df = pl.DataFrame(
        {
            "period": ["2026-01"],
            "market": ["SP"],
            "sku": ["SKU-001"],
            "baseline_qty": [100],
        }
    )

    macro_result = standardise_macro_input(df=macro_df, config=config)
    granular_result = standardise_granular_input(df=granular_df, config=config)

    assert macro_result.columns == ["period", "market", "macro_target_qty"]
    assert granular_result.columns == ["period", "market", "sku", "baseline_qty"]


def test_standardise_rejects_boolean_quantity_values():
    config = ReconciliationConfig()
    df = pl.DataFrame(
        {
            "period": ["2026-01"],
            "market": ["SP"],
            "channel": ["Retail"],
            "macro_target_qty": [True],
        }
    )

    try:
        standardise_macro_input(df=df, config=config)
    except DataValidationError as exc:
        assert str(exc) == (
            "macro dataset contains non-numeric value 'True' in quantity column 'macro_target_qty'."
        )
    else:
        raise AssertionError("Expected DataValidationError for boolean quantity value.")