from datetime import date

import polars as pl

from forecast_reconciler.config import ReconciliationConfig
from forecast_reconciler.exceptions import DataValidationError
from forecast_reconciler.reporting.summaries import (
    ReportingResult,
    build_reporting_views,
)


def test_build_reporting_views_returns_group_and_sku_reports():
    config = ReconciliationConfig()

    rounded_allocations_df = pl.DataFrame(
        {
            "period": [date(2026, 1, 1), date(2026, 1, 1)],
            "market": ["SP", "SP"],
            "channel": ["Retail", "Retail"],
            "sku": ["SKU-001", "SKU-002"],
            "baseline_qty": [60.0, 40.0],
            "group_baseline_qty": [100.0, 100.0],
            "weight": [0.6, 0.4],
            "macro_target_qty": [120.0, 120.0],
            "raw_allocated_qty": [72.0, 48.0],
            "rounded_allocated_qty": [72.0, 48.0],
            "final_allocated_qty": [72.0, 48.0],
        }
    )

    result = build_reporting_views(
        rounded_allocations_df=rounded_allocations_df,
        config=config,
    )

    assert isinstance(result, ReportingResult)

    group_summary = result.group_summary
    assert group_summary.columns == [
        "period",
        "market",
        "channel",
        "macro_target_qty",
        "baseline_group_qty",
        "raw_allocated_group_qty",
        "rounded_group_qty",
        "final_allocated_group_qty",
        "sku_count",
        "group_delta_qty",
        "group_delta_pct",
        "final_gap_to_target",
    ]
    assert group_summary.to_dicts() == [
        {
            "period": date(2026, 1, 1),
            "market": "SP",
            "channel": "Retail",
            "macro_target_qty": 120.0,
            "baseline_group_qty": 100.0,
            "raw_allocated_group_qty": 120.0,
            "rounded_group_qty": 120.0,
            "final_allocated_group_qty": 120.0,
            "sku_count": 2,
            "group_delta_qty": 20.0,
            "group_delta_pct": 0.2,
            "final_gap_to_target": 0.0,
        }
    ]

    sku_variance = result.sku_variance
    assert sku_variance.columns == [
        "period",
        "market",
        "channel",
        "sku",
        "baseline_qty",
        "group_baseline_qty",
        "weight",
        "raw_allocated_qty",
        "rounded_allocated_qty",
        "final_allocated_qty",
        "baseline_weight",
        "final_weight",
        "sku_delta_qty",
        "sku_delta_pct",
        "weight_delta",
    ]
    assert sku_variance.to_dicts() == [
        {
            "period": date(2026, 1, 1),
            "market": "SP",
            "channel": "Retail",
            "sku": "SKU-001",
            "baseline_qty": 60.0,
            "group_baseline_qty": 100.0,
            "weight": 0.6,
            "raw_allocated_qty": 72.0,
            "rounded_allocated_qty": 72.0,
            "final_allocated_qty": 72.0,
            "baseline_weight": 0.6,
            "final_weight": 0.6,
            "sku_delta_qty": 12.0,
            "sku_delta_pct": 0.2,
            "weight_delta": 0.0,
        },
        {
            "period": date(2026, 1, 1),
            "market": "SP",
            "channel": "Retail",
            "sku": "SKU-002",
            "baseline_qty": 40.0,
            "group_baseline_qty": 100.0,
            "weight": 0.4,
            "raw_allocated_qty": 48.0,
            "rounded_allocated_qty": 48.0,
            "final_allocated_qty": 48.0,
            "baseline_weight": 0.4,
            "final_weight": 0.4,
            "sku_delta_qty": 8.0,
            "sku_delta_pct": 0.2,
            "weight_delta": 0.0,
        },
    ]


def test_build_reporting_views_handles_zero_baseline_group_in_percent_fields():
    config = ReconciliationConfig()

    rounded_allocations_df = pl.DataFrame(
        {
            "period": [date(2026, 1, 1), date(2026, 1, 1)],
            "market": ["RJ", "RJ"],
            "channel": ["Retail", "Retail"],
            "sku": ["SKU-010", "SKU-011"],
            "baseline_qty": [0.0, 0.0],
            "group_baseline_qty": [0.0, 0.0],
            "weight": [0.5, 0.5],
            "macro_target_qty": [10.0, 10.0],
            "raw_allocated_qty": [5.0, 5.0],
            "rounded_allocated_qty": [5.0, 5.0],
            "final_allocated_qty": [5.0, 5.0],
        }
    )

    result = build_reporting_views(
        rounded_allocations_df=rounded_allocations_df,
        config=config,
    )

    group_summary = result.group_summary
    assert group_summary.get_column("group_delta_pct").to_list() == [None]

    sku_variance = result.sku_variance.sort("sku")
    assert sku_variance.get_column("baseline_weight").to_list() == [None, None]
    assert sku_variance.get_column("sku_delta_pct").to_list() == [None, None]
    assert sku_variance.get_column("final_weight").to_list() == [0.5, 0.5]
    assert sku_variance.get_column("weight_delta").to_list() == [None, None]


def test_build_reporting_views_handles_final_zero_total_for_weight_calculation():
    config = ReconciliationConfig()

    rounded_allocations_df = pl.DataFrame(
        {
            "period": [date(2026, 1, 1), date(2026, 1, 1)],
            "market": ["MG", "MG"],
            "channel": ["Retail", "Retail"],
            "sku": ["SKU-020", "SKU-021"],
            "baseline_qty": [5.0, 5.0],
            "group_baseline_qty": [10.0, 10.0],
            "weight": [0.5, 0.5],
            "macro_target_qty": [0.0, 0.0],
            "raw_allocated_qty": [0.0, 0.0],
            "rounded_allocated_qty": [0.0, 0.0],
            "final_allocated_qty": [0.0, 0.0],
        }
    )

    result = build_reporting_views(
        rounded_allocations_df=rounded_allocations_df,
        config=config,
    )

    sku_variance = result.sku_variance.sort("sku")
    assert sku_variance.get_column("final_weight").to_list() == [None, None]
    assert sku_variance.get_column("weight_delta").to_list() == [None, None]


def test_build_reporting_views_requires_expected_columns():
    config = ReconciliationConfig()

    rounded_allocations_df = pl.DataFrame(
        {
            "period": [date(2026, 1, 1)],
            "market": ["SP"],
            "channel": ["Retail"],
            "sku": ["SKU-001"],
            "final_allocated_qty": [100.0],
        }
    )

    try:
        build_reporting_views(
            rounded_allocations_df=rounded_allocations_df,
            config=config,
        )
    except DataValidationError as exc:
        assert str(exc) == (
            "Rounded allocation dataset is missing required columns for reporting: "
            "baseline_qty, group_baseline_qty, macro_target_qty, raw_allocated_qty, "
            "rounded_allocated_qty, weight."
        )
    else:
        raise AssertionError("Expected DataValidationError for missing reporting columns.")