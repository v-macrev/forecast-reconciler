from datetime import date

import polars as pl

from forecast_reconciler.config import ReconciliationConfig
from forecast_reconciler.exceptions import IntegrityCheckError
from forecast_reconciler.validation.integrity import (
    IntegrityValidationResult,
    validate_reconciliation_integrity,
)


def test_validate_reconciliation_integrity_accepts_valid_result():
    config = ReconciliationConfig(quantity_mode="integer", quantity_decimals=0)

    rounded_allocations_df = pl.DataFrame(
        {
            "period": [date(2026, 1, 1), date(2026, 1, 1)],
            "market": ["SP", "SP"],
            "channel": ["Retail", "Retail"],
            "sku": ["SKU-001", "SKU-002"],
            "macro_target_qty": [100.0, 100.0],
            "final_allocated_qty": [60.0, 40.0],
        }
    )

    result = validate_reconciliation_integrity(
        rounded_allocations_df=rounded_allocations_df,
        unmatched_macro_groups=pl.DataFrame(schema={"period": pl.Date, "market": pl.String, "channel": pl.String}),
        unmatched_granular_groups=pl.DataFrame(schema={"period": pl.Date, "market": pl.String, "channel": pl.String}),
        config=config,
    )

    assert isinstance(result, IntegrityValidationResult)
    assert result.is_valid is True
    assert result.summary.to_dicts() == [
        {
            "validated_group_count": 1,
            "groups_with_gap_count": 0,
            "negative_allocation_count": 0,
            "unmatched_macro_group_count": 0,
            "unmatched_granular_group_count": 0,
            "is_valid": True,
        }
    ]


def test_validate_reconciliation_integrity_raises_for_group_gap_when_enforced():
    config = ReconciliationConfig(quantity_mode="integer", quantity_decimals=0)

    rounded_allocations_df = pl.DataFrame(
        {
            "period": [date(2026, 1, 1), date(2026, 1, 1)],
            "market": ["SP", "SP"],
            "channel": ["Retail", "Retail"],
            "sku": ["SKU-001", "SKU-002"],
            "macro_target_qty": [100.0, 100.0],
            "final_allocated_qty": [60.0, 39.0],
        }
    )

    try:
        validate_reconciliation_integrity(
            rounded_allocations_df=rounded_allocations_df,
            unmatched_macro_groups=pl.DataFrame(schema={"period": pl.Date, "market": pl.String, "channel": pl.String}),
            unmatched_granular_groups=pl.DataFrame(schema={"period": pl.Date, "market": pl.String, "channel": pl.String}),
            config=config,
        )
    except IntegrityCheckError as exc:
        assert str(exc) == (
            "Reconciliation integrity validation failed: "
            "group control totals do not match macro targets."
        )
    else:
        raise AssertionError("Expected IntegrityCheckError for group total gap.")


def test_validate_reconciliation_integrity_raises_for_negative_allocations():
    config = ReconciliationConfig(
        quantity_mode="integer",
        quantity_decimals=0,
        allow_negative_allocations=False,
    )

    rounded_allocations_df = pl.DataFrame(
        {
            "period": [date(2026, 1, 1)],
            "market": ["SP"],
            "channel": ["Retail"],
            "sku": ["SKU-001"],
            "macro_target_qty": [100.0],
            "final_allocated_qty": [-5.0],
        }
    )

    try:
        validate_reconciliation_integrity(
            rounded_allocations_df=rounded_allocations_df,
            unmatched_macro_groups=pl.DataFrame(schema={"period": pl.Date, "market": pl.String, "channel": pl.String}),
            unmatched_granular_groups=pl.DataFrame(schema={"period": pl.Date, "market": pl.String, "channel": pl.String}),
            config=config,
        )
    except IntegrityCheckError as exc:
        assert str(exc) == (
            "Reconciliation integrity validation failed: "
            "group control totals do not match macro targets; negative final allocations were detected."
        )
    else:
        raise AssertionError("Expected IntegrityCheckError for negative allocation.")


def test_validate_reconciliation_integrity_allows_negative_allocations_when_configured():
    config = ReconciliationConfig(
        quantity_mode="integer",
        quantity_decimals=0,
        allow_negative_allocations=True,
        enforce_exact_totals=False,
    )

    rounded_allocations_df = pl.DataFrame(
        {
            "period": [date(2026, 1, 1), date(2026, 1, 1)],
            "market": ["SP", "SP"],
            "channel": ["Retail", "Retail"],
            "sku": ["SKU-001", "SKU-002"],
            "macro_target_qty": [5.0, 5.0],
            "final_allocated_qty": [10.0, -5.0],
        }
    )

    result = validate_reconciliation_integrity(
        rounded_allocations_df=rounded_allocations_df,
        unmatched_macro_groups=pl.DataFrame(schema={"period": pl.Date, "market": pl.String, "channel": pl.String}),
        unmatched_granular_groups=pl.DataFrame(schema={"period": pl.Date, "market": pl.String, "channel": pl.String}),
        config=config,
    )

    assert result.is_valid is True
    assert result.negative_allocations.height == 1
    assert result.group_checks.get_column("within_tolerance").to_list() == [True]


def test_validate_reconciliation_integrity_raises_for_unmatched_groups():
    config = ReconciliationConfig()

    rounded_allocations_df = pl.DataFrame(
        {
            "period": [date(2026, 1, 1)],
            "market": ["SP"],
            "channel": ["Retail"],
            "sku": ["SKU-001"],
            "macro_target_qty": [100.0],
            "final_allocated_qty": [100.0],
        }
    )

    unmatched_macro_groups = pl.DataFrame(
        {
            "period": [date(2026, 1, 1)],
            "market": ["RJ"],
            "channel": ["Retail"],
        }
    )

    unmatched_granular_groups = pl.DataFrame(
        {
            "period": [date(2026, 1, 1)],
            "market": ["MG"],
            "channel": ["Retail"],
        }
    )

    try:
        validate_reconciliation_integrity(
            rounded_allocations_df=rounded_allocations_df,
            unmatched_macro_groups=unmatched_macro_groups,
            unmatched_granular_groups=unmatched_granular_groups,
            config=config,
        )
    except IntegrityCheckError as exc:
        assert str(exc) == (
            "Reconciliation integrity validation failed: "
            "unmatched macro groups were detected; unmatched granular groups were detected."
        )
    else:
        raise AssertionError("Expected IntegrityCheckError for unmatched groups.")


def test_validate_reconciliation_integrity_returns_invalid_result_without_raising_when_not_enforced():
    config = ReconciliationConfig(enforce_exact_totals=False)

    rounded_allocations_df = pl.DataFrame(
        {
            "period": [date(2026, 1, 1), date(2026, 1, 1)],
            "market": ["SP", "SP"],
            "channel": ["Retail", "Retail"],
            "sku": ["SKU-001", "SKU-002"],
            "macro_target_qty": [100.0, 100.0],
            "final_allocated_qty": [60.0, 39.0],
        }
    )

    result = validate_reconciliation_integrity(
        rounded_allocations_df=rounded_allocations_df,
        unmatched_macro_groups=pl.DataFrame(schema={"period": pl.Date, "market": pl.String, "channel": pl.String}),
        unmatched_granular_groups=pl.DataFrame(schema={"period": pl.Date, "market": pl.String, "channel": pl.String}),
        config=config,
    )

    assert result.is_valid is False
    assert result.summary.to_dicts() == [
        {
            "validated_group_count": 1,
            "groups_with_gap_count": 1,
            "negative_allocation_count": 0,
            "unmatched_macro_group_count": 0,
            "unmatched_granular_group_count": 0,
            "is_valid": False,
        }
    ]


def test_validate_reconciliation_integrity_requires_final_allocated_qty_column():
    config = ReconciliationConfig()

    rounded_allocations_df = pl.DataFrame(
        {
            "period": [date(2026, 1, 1)],
            "market": ["SP"],
            "channel": ["Retail"],
            "sku": ["SKU-001"],
            "macro_target_qty": [100.0],
        }
    )

    try:
        validate_reconciliation_integrity(
            rounded_allocations_df=rounded_allocations_df,
            unmatched_macro_groups=pl.DataFrame(schema={"period": pl.Date, "market": pl.String, "channel": pl.String}),
            unmatched_granular_groups=pl.DataFrame(schema={"period": pl.Date, "market": pl.String, "channel": pl.String}),
            config=config,
        )
    except IntegrityCheckError as exc:
        assert str(exc) == (
            "Rounded allocation dataset is missing required columns for integrity validation: "
            "final_allocated_qty."
        )
    else:
        raise AssertionError("Expected IntegrityCheckError for missing final_allocated_qty column.")