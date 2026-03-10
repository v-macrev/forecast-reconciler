from datetime import date

import polars as pl

from forecast_reconciler.config import ReconciliationConfig
from forecast_reconciler.exceptions import IntegrityCheckError, ReconciliationError
from forecast_reconciler.reconciliation.rounding import (
    RoundingResult,
    apply_deterministic_rounding,
)


def test_apply_deterministic_rounding_preserves_integer_control_totals():
    config = ReconciliationConfig(quantity_mode="integer", quantity_decimals=0)

    allocation_df = pl.DataFrame(
        {
            "period": [date(2026, 1, 1)] * 3,
            "market": ["SP"] * 3,
            "channel": ["Retail"] * 3,
            "sku": ["SKU-001", "SKU-002", "SKU-003"],
            "baseline_qty": [1.0, 1.0, 1.0],
            "group_baseline_qty": [3.0, 3.0, 3.0],
            "weight": [1 / 3, 1 / 3, 1 / 3],
            "macro_target_qty": [100.0, 100.0, 100.0],
            "raw_allocated_qty": [33.333333, 33.333333, 33.333333],
        }
    )

    result = apply_deterministic_rounding(allocation_df=allocation_df, config=config)

    assert isinstance(result, RoundingResult)

    allocations = result.allocations.sort("sku")
    assert allocations.get_column("rounded_allocated_qty").to_list() == [33.0, 33.0, 33.0]
    assert allocations.get_column("final_allocated_qty").to_list() == [34.0, 33.0, 33.0]

    summary = result.group_summary
    assert summary.get_column("rounded_group_total").to_list() == [99.0]
    assert summary.get_column("final_group_total").to_list() == [100.0]
    assert summary.get_column("final_gap").to_list() == [0.0]


def test_apply_deterministic_rounding_uses_largest_remainder_order():
    config = ReconciliationConfig(quantity_mode="integer", quantity_decimals=0)

    allocation_df = pl.DataFrame(
        {
            "period": [date(2026, 1, 1)] * 3,
            "market": ["SP"] * 3,
            "channel": ["Retail"] * 3,
            "sku": ["SKU-001", "SKU-002", "SKU-003"],
            "baseline_qty": [0.0, 0.0, 0.0],
            "group_baseline_qty": [0.0, 0.0, 0.0],
            "weight": [0.0, 0.0, 0.0],
            "macro_target_qty": [10.0, 10.0, 10.0],
            "raw_allocated_qty": [3.8, 3.3, 2.9],
        }
    )

    result = apply_deterministic_rounding(allocation_df=allocation_df, config=config)
    allocations = result.allocations.sort("sku")

    assert allocations.get_column("rounded_allocated_qty").to_list() == [3.0, 3.0, 2.0]
    assert allocations.get_column("residual_adjustment").to_list() == [1.0, 0.0, 1.0]
    assert allocations.get_column("final_allocated_qty").to_list() == [4.0, 3.0, 3.0]


def test_apply_deterministic_rounding_supports_decimal_quantity_mode():
    config = ReconciliationConfig(quantity_mode="decimal", quantity_decimals=2)

    allocation_df = pl.DataFrame(
        {
            "period": [date(2026, 1, 1)] * 2,
            "market": ["SP"] * 2,
            "channel": ["Retail"] * 2,
            "sku": ["SKU-001", "SKU-002"],
            "baseline_qty": [0.0, 0.0],
            "group_baseline_qty": [0.0, 0.0],
            "weight": [0.0, 0.0],
            "macro_target_qty": [1.00, 1.00],
            "raw_allocated_qty": [0.333, 0.667],
        }
    )

    result = apply_deterministic_rounding(allocation_df=allocation_df, config=config)
    allocations = result.allocations.sort("sku")

    assert allocations.get_column("rounded_allocated_qty").to_list() == [0.33, 0.66]
    assert allocations.get_column("residual_adjustment").to_list() == [0.0, 0.01]
    assert allocations.get_column("final_allocated_qty").to_list() == [0.33, 0.67]

    summary = result.group_summary
    assert summary.get_column("final_group_total").to_list() == [1.0]
    assert summary.get_column("final_gap").to_list() == [0.0]


def test_apply_deterministic_rounding_handles_multiple_groups_independently():
    config = ReconciliationConfig(quantity_mode="integer", quantity_decimals=0)

    allocation_df = pl.DataFrame(
        {
            "period": [date(2026, 1, 1)] * 4,
            "market": ["RJ", "RJ", "SP", "SP"],
            "channel": ["Retail"] * 4,
            "sku": ["SKU-010", "SKU-011", "SKU-001", "SKU-002"],
            "baseline_qty": [0.0, 0.0, 0.0, 0.0],
            "group_baseline_qty": [0.0, 0.0, 0.0, 0.0],
            "weight": [0.0, 0.0, 0.0, 0.0],
            "macro_target_qty": [5.0, 5.0, 100.0, 100.0],
            "raw_allocated_qty": [1.6, 3.4, 33.333333, 66.666667],
        }
    )

    result = apply_deterministic_rounding(allocation_df=allocation_df, config=config)
    allocations = result.allocations.sort(["market", "sku"])

    assert allocations.get_column("final_allocated_qty").to_list() == [2.0, 3.0, 33.0, 67.0]

    summary = result.group_summary.sort("market")
    assert summary.get_column("final_group_total").to_list() == [5.0, 100.0]
    assert summary.get_column("final_gap").to_list() == [0.0, 0.0]


def test_apply_deterministic_rounding_requires_input_columns():
    config = ReconciliationConfig()

    allocation_df = pl.DataFrame(
        {
            "period": [date(2026, 1, 1)],
            "market": ["SP"],
            "channel": ["Retail"],
            "sku": ["SKU-001"],
        }
    )

    try:
        apply_deterministic_rounding(allocation_df=allocation_df, config=config)
    except ReconciliationError as exc:
        assert str(exc) == (
            "Allocation dataset is missing required columns for rounding: "
            "macro_target_qty, raw_allocated_qty."
        )
    else:
        raise AssertionError("Expected ReconciliationError for missing rounding columns.")


def test_apply_deterministic_rounding_rejects_empty_input():
    config = ReconciliationConfig()

    allocation_df = pl.DataFrame(
        schema={
            "period": pl.Date,
            "market": pl.String,
            "channel": pl.String,
            "sku": pl.String,
            "macro_target_qty": pl.Float64,
            "raw_allocated_qty": pl.Float64,
        }
    )

    try:
        apply_deterministic_rounding(allocation_df=allocation_df, config=config)
    except ReconciliationError as exc:
        assert str(exc) == "Allocation dataset is empty and cannot be rounded."
    else:
        raise AssertionError("Expected ReconciliationError for empty allocation dataset.")


def test_apply_deterministic_rounding_aligns_macro_target_to_configured_precision():
    config = ReconciliationConfig(quantity_mode="decimal", quantity_decimals=2)

    allocation_df = pl.DataFrame(
        {
            "period": [date(2026, 1, 1)],
            "market": ["SP"],
            "channel": ["Retail"],
            "sku": ["SKU-001"],
            "baseline_qty": [0.0],
            "group_baseline_qty": [0.0],
            "weight": [1.0],
            "macro_target_qty": [0.004],
            "raw_allocated_qty": [0.004],
        }
    )

    result = apply_deterministic_rounding(allocation_df=allocation_df, config=config)

    allocations = result.allocations
    assert allocations.get_column("rounded_allocated_qty").to_list() == [0.0]
    assert allocations.get_column("final_allocated_qty").to_list() == [0.0]

    summary = result.group_summary
    assert summary.get_column("macro_target_qty").to_list() == [0.004]
    assert summary.get_column("rounded_group_total").to_list() == [0.0]
    assert summary.get_column("final_group_total").to_list() == [0.0]
    assert summary.get_column("final_gap").to_list() == [0.004]