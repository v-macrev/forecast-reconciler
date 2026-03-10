from datetime import date

import polars as pl

from forecast_reconciler.config import ReconciliationConfig
from forecast_reconciler.exceptions import ZeroBaselineError
from forecast_reconciler.reconciliation.weights import (
    WeightCalculationResult,
    calculate_weights,
)


def test_calculate_weights_returns_group_totals_and_weights():
    config = ReconciliationConfig()
    granular_df = pl.DataFrame(
        {
            "period": [date(2026, 1, 1), date(2026, 1, 1)],
            "market": ["SP", "SP"],
            "channel": ["Retail", "Retail"],
            "sku": ["SKU-001", "SKU-002"],
            "baseline_qty": [60.0, 40.0],
        }
    )

    result = calculate_weights(granular_df=granular_df, config=config)

    assert isinstance(result, WeightCalculationResult)
    assert result.zero_baseline_groups.height == 0

    weights_df = result.weights
    assert weights_df.columns == [
        "period",
        "market",
        "channel",
        "sku",
        "baseline_qty",
        "group_baseline_qty",
        "weight",
    ]
    assert weights_df.get_column("group_baseline_qty").to_list() == [100.0, 100.0]
    assert weights_df.get_column("weight").to_list() == [0.6, 0.4]


def test_calculate_weights_handles_multiple_groups_independently():
    config = ReconciliationConfig()
    granular_df = pl.DataFrame(
        {
            "period": [
                date(2026, 1, 1),
                date(2026, 1, 1),
                date(2026, 1, 1),
                date(2026, 1, 1),
            ],
            "market": ["SP", "SP", "RJ", "RJ"],
            "channel": ["Retail", "Retail", "Retail", "Retail"],
            "sku": ["SKU-001", "SKU-002", "SKU-010", "SKU-011"],
            "baseline_qty": [60.0, 40.0, 30.0, 70.0],
        }
    )

    result = calculate_weights(granular_df=granular_df, config=config)
    weights_df = result.weights.sort(["market", "sku"])

    assert weights_df.get_column("group_baseline_qty").to_list() == [
        100.0,
        100.0,
        100.0,
        100.0,
    ]
    assert weights_df.get_column("weight").to_list() == [0.3, 0.7, 0.6, 0.4]


def test_calculate_weights_rejects_zero_baseline_groups_in_fail_mode():
    config = ReconciliationConfig(zero_baseline_mode="fail")
    granular_df = pl.DataFrame(
        {
            "period": [date(2026, 1, 1), date(2026, 1, 1)],
            "market": ["SP", "SP"],
            "channel": ["Retail", "Retail"],
            "sku": ["SKU-001", "SKU-002"],
            "baseline_qty": [0.0, 0.0],
        }
    )

    try:
        calculate_weights(granular_df=granular_df, config=config)
    except ZeroBaselineError as exc:
        assert str(exc) == (
            "One or more reconciliation groups have zero baseline quantity and "
            "cannot be weighted proportionally."
        )
    else:
        raise AssertionError("Expected ZeroBaselineError for zero-baseline group.")


def test_calculate_weights_equal_split_mode_assigns_uniform_weights():
    config = ReconciliationConfig(zero_baseline_mode="equal_split")
    granular_df = pl.DataFrame(
        {
            "period": [date(2026, 1, 1), date(2026, 1, 1), date(2026, 1, 1)],
            "market": ["SP", "SP", "SP"],
            "channel": ["Retail", "Retail", "Retail"],
            "sku": ["SKU-001", "SKU-002", "SKU-003"],
            "baseline_qty": [0.0, 0.0, 0.0],
        }
    )

    result = calculate_weights(granular_df=granular_df, config=config)

    assert result.zero_baseline_groups.height == 1
    assert result.zero_baseline_groups.columns == ["period", "market", "channel"]

    weights = result.weights.get_column("weight").to_list()
    assert weights == [1 / 3, 1 / 3, 1 / 3]


def test_calculate_weights_preserves_standard_proportions_for_non_zero_groups_in_equal_split_mode():
    config = ReconciliationConfig(zero_baseline_mode="equal_split")
    granular_df = pl.DataFrame(
        {
            "period": [
                date(2026, 1, 1),
                date(2026, 1, 1),
                date(2026, 1, 1),
                date(2026, 1, 1),
            ],
            "market": ["SP", "SP", "RJ", "RJ"],
            "channel": ["Retail", "Retail", "Retail", "Retail"],
            "sku": ["SKU-001", "SKU-002", "SKU-010", "SKU-011"],
            "baseline_qty": [60.0, 40.0, 0.0, 0.0],
        }
    )

    result = calculate_weights(granular_df=granular_df, config=config)
    weights_df = result.weights.sort(["market", "sku"])

    assert result.zero_baseline_groups.height == 1
    assert weights_df.get_column("weight").to_list() == [0.5, 0.5, 0.6, 0.4]


def test_calculate_weights_requires_sku_column():
    config = ReconciliationConfig()
    granular_df = pl.DataFrame(
        {
            "period": [date(2026, 1, 1)],
            "market": ["SP"],
            "channel": ["Retail"],
            "baseline_qty": [100.0],
        }
    )

    try:
        calculate_weights(granular_df=granular_df, config=config)
    except ZeroBaselineError as exc:
        assert str(exc) == (
            "Granular dataset must contain sku column 'sku' for weight calculation."
        )
    else:
        raise AssertionError("Expected ZeroBaselineError when sku column is missing.")