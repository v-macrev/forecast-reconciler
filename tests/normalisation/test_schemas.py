import polars as pl

from forecast_reconciler.config import ReconciliationConfig
from forecast_reconciler.exceptions import SchemaValidationError
from forecast_reconciler.normalisation.schemas import (
    SchemaValidationReport,
    validate_granular_schema,
    validate_macro_schema,
)


def test_validate_macro_schema_accepts_valid_dataset():
    config = ReconciliationConfig()
    df = pl.DataFrame(
        {
            "period": ["2026-01", "2026-01"],
            "market": ["SP", "RJ"],
            "channel": ["Retail", "Retail"],
            "macro_target_qty": [100, 200],
        }
    )

    result = validate_macro_schema(df=df, config=config)

    assert isinstance(result, SchemaValidationReport)
    assert result.dataset_name == "macro"
    assert result.row_count == 2
    assert result.column_count == 4
    assert result.required_columns == (
        "period",
        "market",
        "channel",
        "macro_target_qty",
    )


def test_validate_granular_schema_accepts_valid_dataset():
    config = ReconciliationConfig()
    df = pl.DataFrame(
        {
            "period": ["2026-01", "2026-01"],
            "market": ["SP", "SP"],
            "channel": ["Retail", "Retail"],
            "client": ["Client A", "Client B"],
            "sku": ["SKU-001", "SKU-002"],
            "baseline_qty": [60, 40],
        }
    )

    result = validate_granular_schema(df=df, config=config)

    assert isinstance(result, SchemaValidationReport)
    assert result.dataset_name == "granular"
    assert result.row_count == 2
    assert result.column_count == 6
    assert result.required_columns == (
        "period",
        "market",
        "channel",
        "client",
        "sku",
        "baseline_qty",
    )


def test_validate_macro_schema_rejects_missing_required_column():
    config = ReconciliationConfig()
    df = pl.DataFrame(
        {
            "period": ["2026-01"],
            "market": ["SP"],
            "macro_target_qty": [100],
        }
    )

    try:
        validate_macro_schema(df=df, config=config)
    except SchemaValidationError as exc:
        assert str(exc) == "macro dataset is missing required columns: channel."
    else:
        raise AssertionError("Expected SchemaValidationError for missing macro column.")


def test_validate_granular_schema_rejects_missing_required_column():
    config = ReconciliationConfig()
    df = pl.DataFrame(
        {
            "period": ["2026-01"],
            "market": ["SP"],
            "channel": ["Retail"],
            "sku": ["SKU-001"],
            "baseline_qty": [100],
        }
    )

    try:
        validate_granular_schema(df=df, config=config)
    except SchemaValidationError as exc:
        assert str(exc) == "granular dataset is missing required columns: client."
    else:
        raise AssertionError(
            "Expected SchemaValidationError for missing granular column."
        )


def test_validate_macro_schema_rejects_null_values_in_required_columns():
    config = ReconciliationConfig()
    df = pl.DataFrame(
        {
            "period": ["2026-01", None],
            "market": ["SP", "RJ"],
            "channel": ["Retail", "Retail"],
            "macro_target_qty": [100, 200],
        },
        strict=False,
    )

    try:
        validate_macro_schema(df=df, config=config)
    except SchemaValidationError as exc:
        assert (
            str(exc)
            == "macro dataset contains null values in required columns: period."
        )
    else:
        raise AssertionError("Expected SchemaValidationError for null macro key column.")


def test_validate_granular_schema_rejects_null_values_in_required_columns():
    config = ReconciliationConfig()
    df = pl.DataFrame(
        {
            "period": ["2026-01", "2026-01"],
            "market": ["SP", "SP"],
            "channel": ["Retail", "Retail"],
            "client": ["Client A", None],
            "sku": ["SKU-001", "SKU-002"],
            "baseline_qty": [60, 40],
        },
        strict=False,
    )

    try:
        validate_granular_schema(df=df, config=config)
    except SchemaValidationError as exc:
        assert (
            str(exc)
            == "granular dataset contains null values in required columns: client."
        )
    else:
        raise AssertionError(
            "Expected SchemaValidationError for null granular required column."
        )


def test_validate_schema_respects_custom_group_keys():
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
            "client": ["Client A"],
            "sku": ["SKU-001"],
            "baseline_qty": [100],
        }
    )

    macro_result = validate_macro_schema(df=macro_df, config=config)
    granular_result = validate_granular_schema(df=granular_df, config=config)

    assert macro_result.required_columns == ("period", "market", "macro_target_qty")
    assert granular_result.required_columns == (
        "period",
        "market",
        "client",
        "sku",
        "baseline_qty",
    )