from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from forecast_reconciler.config import ReconciliationConfig
from forecast_reconciler.exceptions import ReconciliationError


@dataclass(frozen=True, slots=True)
class RedistributionResult:

    allocations: pl.DataFrame
    unmatched_macro_groups: pl.DataFrame
    unmatched_granular_groups: pl.DataFrame


def redistribute_macro_targets(
    macro_df: pl.DataFrame,
    weighted_granular_df: pl.DataFrame,
    config: ReconciliationConfig,
) -> RedistributionResult:

    group_keys = list(config.group_keys)
    macro_target_col = config.columns.macro_target_qty_col

    _validate_weighted_granular_input(
        weighted_granular_df=weighted_granular_df,
        macro_target_col=macro_target_col,
    )

    unmatched_macro_groups = _find_unmatched_macro_groups(
        macro_df=macro_df,
        weighted_granular_df=weighted_granular_df,
        group_keys=group_keys,
    )
    unmatched_granular_groups = _find_unmatched_granular_groups(
        macro_df=macro_df,
        weighted_granular_df=weighted_granular_df,
        group_keys=group_keys,
    )

    joined = weighted_granular_df.join(
        macro_df.select(group_keys + [macro_target_col]),
        on=group_keys,
        how="inner",
    )

    allocations = joined.with_columns(
        (pl.col("weight") * pl.col(macro_target_col)).alias("raw_allocated_qty")
    )

    return RedistributionResult(
        allocations=allocations,
        unmatched_macro_groups=unmatched_macro_groups,
        unmatched_granular_groups=unmatched_granular_groups,
    )


def _validate_weighted_granular_input(
    weighted_granular_df: pl.DataFrame,
    macro_target_col: str,
) -> None:
    required_columns = {"group_baseline_qty", "weight"}
    missing = [col for col in required_columns if col not in weighted_granular_df.columns]

    if missing:
        raise ReconciliationError(
            "Weighted granular dataset is missing required columns for redistribution: "
            f"{', '.join(sorted(missing))}."
        )

    if macro_target_col in weighted_granular_df.columns:
        raise ReconciliationError(
            f"Weighted granular dataset must not already contain macro target column "
            f"'{macro_target_col}' before redistribution."
        )


def _find_unmatched_macro_groups(
    macro_df: pl.DataFrame,
    weighted_granular_df: pl.DataFrame,
    group_keys: list[str],
) -> pl.DataFrame:
    return (
        macro_df.select(group_keys)
        .unique(maintain_order=True)
        .join(
            weighted_granular_df.select(group_keys).unique(maintain_order=True),
            on=group_keys,
            how="anti",
        )
    )


def _find_unmatched_granular_groups(
    macro_df: pl.DataFrame,
    weighted_granular_df: pl.DataFrame,
    group_keys: list[str],
) -> pl.DataFrame:
    return (
        weighted_granular_df.select(group_keys)
        .unique(maintain_order=True)
        .join(
            macro_df.select(group_keys).unique(maintain_order=True),
            on=group_keys,
            how="anti",
        )
    )