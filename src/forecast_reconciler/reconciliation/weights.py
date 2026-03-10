from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from forecast_reconciler.config import ReconciliationConfig
from forecast_reconciler.exceptions import ZeroBaselineError


@dataclass(frozen=True, slots=True)
class WeightCalculationResult:

    weights: pl.DataFrame
    zero_baseline_groups: pl.DataFrame


def calculate_weights(
    granular_df: pl.DataFrame,
    config: ReconciliationConfig,
) -> WeightCalculationResult:

    baseline_col = config.columns.baseline_qty_col
    sku_col = config.columns.sku_col
    group_keys = list(config.group_keys)

    if sku_col not in granular_df.columns:
        raise ZeroBaselineError(
            f"Granular dataset must contain sku column '{sku_col}' for weight calculation."
        )

    result = granular_df.with_columns(
        pl.col(baseline_col).sum().over(group_keys).alias("group_baseline_qty")
    )

    zero_baseline_groups = (
        result
        .filter(pl.col("group_baseline_qty") == 0)
        .select(group_keys)
        .unique(maintain_order=True)
    )

    if zero_baseline_groups.height > 0 and config.zero_baseline_mode == "fail":
        raise ZeroBaselineError(
            "One or more reconciliation groups have zero baseline quantity and "
            "cannot be weighted proportionally."
        )

    if config.zero_baseline_mode == "equal_split":
        group_row_count_expr = pl.len().over(group_keys).cast(pl.Float64)

        result = result.with_columns(
            pl.when(pl.col("group_baseline_qty") == 0)
            .then(1.0 / group_row_count_expr)
            .otherwise(pl.col(baseline_col) / pl.col("group_baseline_qty"))
            .alias("weight")
        )
    else:
        result = result.with_columns(
            (pl.col(baseline_col) / pl.col("group_baseline_qty")).alias("weight")
        )

    return WeightCalculationResult(
        weights=result,
        zero_baseline_groups=zero_baseline_groups,
    )