from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from forecast_reconciler.config import ReconciliationConfig
from forecast_reconciler.exceptions import DataValidationError


@dataclass(frozen=True, slots=True)
class ReportingResult:

    group_summary: pl.DataFrame
    sku_variance: pl.DataFrame


def build_reporting_views(
    rounded_allocations_df: pl.DataFrame,
    config: ReconciliationConfig,
) -> ReportingResult:

    _validate_reporting_input(rounded_allocations_df=rounded_allocations_df, config=config)

    group_summary = _build_group_summary(
        rounded_allocations_df=rounded_allocations_df,
        config=config,
    )

    sku_variance = _build_sku_variance(
        rounded_allocations_df=rounded_allocations_df,
        config=config,
    )

    return ReportingResult(
        group_summary=group_summary,
        sku_variance=sku_variance,
    )


def _build_group_summary(
    rounded_allocations_df: pl.DataFrame,
    config: ReconciliationConfig,
) -> pl.DataFrame:
    group_keys = list(config.group_keys)
    macro_col = config.columns.macro_target_qty_col

    return (
        rounded_allocations_df.group_by(group_keys)
        .agg(
            pl.first(macro_col).alias("macro_target_qty"),
            pl.col("baseline_qty").sum().alias("baseline_group_qty"),
            pl.col("raw_allocated_qty").sum().alias("raw_allocated_group_qty"),
            pl.col("rounded_allocated_qty").sum().alias("rounded_group_qty"),
            pl.col("final_allocated_qty").sum().alias("final_allocated_group_qty"),
            pl.len().alias("sku_count"),
        )
        .with_columns(
            (
                pl.col("final_allocated_group_qty") - pl.col("baseline_group_qty")
            ).alias("group_delta_qty"),
            pl.when(pl.col("baseline_group_qty") == 0)
            .then(None)
            .otherwise(
                (
                    pl.col("final_allocated_group_qty") - pl.col("baseline_group_qty")
                )
                / pl.col("baseline_group_qty")
            )
            .alias("group_delta_pct"),
            (
                pl.col("macro_target_qty") - pl.col("final_allocated_group_qty")
            ).alias("final_gap_to_target"),
        )
        .sort(group_keys)
    )


def _build_sku_variance(
    rounded_allocations_df: pl.DataFrame,
    config: ReconciliationConfig,
) -> pl.DataFrame:
    group_keys = list(config.group_keys)
    baseline_col = config.columns.baseline_qty_col
    sku_col = config.columns.sku_col

    return (
        rounded_allocations_df.with_columns(
            pl.when(pl.col("group_baseline_qty") == 0)
            .then(None)
            .otherwise(pl.col(baseline_col) / pl.col("group_baseline_qty"))
            .alias("baseline_weight"),
            pl.when(pl.col("final_allocated_qty").sum().over(group_keys) == 0)
            .then(None)
            .otherwise(
                pl.col("final_allocated_qty")
                / pl.col("final_allocated_qty").sum().over(group_keys)
            )
            .alias("final_weight"),
        )
        .with_columns(
            (pl.col("final_allocated_qty") - pl.col(baseline_col)).alias("sku_delta_qty"),
            pl.when(pl.col(baseline_col) == 0)
            .then(None)
            .otherwise(
                (pl.col("final_allocated_qty") - pl.col(baseline_col))
                / pl.col(baseline_col)
            )
            .alias("sku_delta_pct"),
            (pl.col("final_weight") - pl.col("baseline_weight")).alias("weight_delta"),
        )
        .select(
            group_keys
            + [
                sku_col,
                baseline_col,
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
        )
        .sort(group_keys + [sku_col])
    )


def _validate_reporting_input(
    rounded_allocations_df: pl.DataFrame,
    config: ReconciliationConfig,
) -> None:
    required_columns = set(config.group_keys) | {
        config.columns.sku_col,
        config.columns.baseline_qty_col,
        config.columns.macro_target_qty_col,
        "group_baseline_qty",
        "weight",
        "raw_allocated_qty",
        "rounded_allocated_qty",
        "final_allocated_qty",
    }

    missing = [col for col in sorted(required_columns) if col not in rounded_allocations_df.columns]
    if missing:
        raise DataValidationError(
            "Rounded allocation dataset is missing required columns for reporting: "
            f"{', '.join(missing)}."
        )