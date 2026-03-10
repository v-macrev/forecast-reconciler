from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from forecast_reconciler.config import ReconciliationConfig
from forecast_reconciler.exceptions import IntegrityCheckError, ReconciliationError


@dataclass(frozen=True, slots=True)
class RoundingResult:

    allocations: pl.DataFrame
    group_summary: pl.DataFrame


def apply_deterministic_rounding(
    allocation_df: pl.DataFrame,
    config: ReconciliationConfig,
) -> RoundingResult:

    _validate_allocation_input(allocation_df=allocation_df, config=config)

    group_keys = list(config.group_keys)
    macro_col = config.columns.macro_target_qty_col
    raw_col = "raw_allocated_qty"

    scale = 10**config.quantity_decimals
    unit = 1.0 / scale

    work = allocation_df.with_columns(
        (pl.col(raw_col) * scale).alias("_raw_scaled"),
        (pl.col(macro_col) * scale).round(0).alias("_macro_scaled"),
    )

    if config.quantity_mode == "integer":
        work = work.with_columns(
            pl.col("_raw_scaled").floor().alias("_rounded_scaled")
        )
    else:
        work = work.with_columns(
            pl.col("_raw_scaled").floor().alias("_rounded_scaled")
        )

    work = work.with_columns(
        (pl.col("_raw_scaled") - pl.col("_rounded_scaled")).alias("_remainder_scaled")
    )

    group_residuals = (
        work.group_by(group_keys)
        .agg(
            pl.first(macro_col).alias(macro_col),
            pl.first("_macro_scaled").alias("_macro_scaled"),
            pl.col(raw_col).sum().alias("raw_group_total"),
            pl.col("_rounded_scaled").sum().alias("_rounded_group_scaled_total"),
        )
        .with_columns(
            (
                pl.col("_macro_scaled") - pl.col("_rounded_group_scaled_total")
            ).alias("_residual_units")
        )
    )

    if group_residuals.filter(pl.col("_residual_units") < 0).height > 0:
        raise IntegrityCheckError(
            "Residual correction produced a negative residual, which indicates an invalid rounding state."
        )

    work = work.join(
        group_residuals.select(group_keys + ["_residual_units"]),
        on=group_keys,
        how="left",
    )

    sort_by = group_keys + ["_remainder_scaled", config.columns.sku_col]
    descending = [False] * len(group_keys) + [True, False]

    work = (
        work.sort(sort_by, descending=descending)
        .with_columns(
            (
                pl.int_range(0, pl.len()).over(group_keys) + 1
            ).alias("residual_rank")
        )
        .with_columns(
            pl.when(pl.col("residual_rank") <= pl.col("_residual_units"))
            .then(1.0)
            .otherwise(0.0)
            .alias("_residual_adjustment_scaled")
        )
    )

    work = work.with_columns(
        (pl.col("_rounded_scaled") / scale).alias("rounded_allocated_qty"),
        (pl.col("_remainder_scaled") / scale).alias("rounding_remainder"),
        (pl.col("_residual_adjustment_scaled") / scale).alias("residual_adjustment"),
        (
            (pl.col("_rounded_scaled") + pl.col("_residual_adjustment_scaled")) / scale
        ).alias("final_allocated_qty"),
    )

    group_summary = (
        work.group_by(group_keys)
        .agg(
            pl.first(macro_col).alias(macro_col),
            pl.col(raw_col).sum().alias("raw_group_total"),
            pl.col("rounded_allocated_qty").sum().alias("rounded_group_total"),
            pl.col("final_allocated_qty").sum().alias("final_group_total"),
            pl.col("residual_adjustment").sum().alias("residual_added_total"),
        )
        .with_columns(
            (pl.col(macro_col) - pl.col("rounded_group_total")).alias("rounded_gap"),
            (pl.col(macro_col) - pl.col("final_group_total")).alias("final_gap"),
        )
    )

    invalid_groups = group_summary.filter(pl.col("final_gap").abs() > (unit / 2))
    if config.enforce_exact_totals and invalid_groups.height > 0:
        raise IntegrityCheckError(
            "Final rounded allocations do not match macro targets for one or more groups."
        )

    final_allocations = work.select(
        allocation_df.columns
        + [
            "rounded_allocated_qty",
            "rounding_remainder",
            "residual_rank",
            "residual_adjustment",
            "final_allocated_qty",
        ]
    )

    return RoundingResult(
        allocations=final_allocations,
        group_summary=group_summary,
    )


def _validate_allocation_input(
    allocation_df: pl.DataFrame,
    config: ReconciliationConfig,
) -> None:
    required_columns = {
        config.columns.sku_col,
        config.columns.macro_target_qty_col,
        "raw_allocated_qty",
    }
    missing = [col for col in sorted(required_columns) if col not in allocation_df.columns]

    if missing:
        raise ReconciliationError(
            "Allocation dataset is missing required columns for rounding: "
            f"{', '.join(missing)}."
        )

    if allocation_df.height == 0:
        raise ReconciliationError(
            "Allocation dataset is empty and cannot be rounded."
        )