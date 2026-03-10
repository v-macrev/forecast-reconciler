from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from forecast_reconciler.config import ReconciliationConfig
from forecast_reconciler.exceptions import IntegrityCheckError


@dataclass(frozen=True, slots=True)
class IntegrityValidationResult:

    summary: pl.DataFrame
    group_checks: pl.DataFrame
    negative_allocations: pl.DataFrame
    unmatched_macro_groups: pl.DataFrame
    unmatched_granular_groups: pl.DataFrame
    is_valid: bool


def validate_reconciliation_integrity(
    rounded_allocations_df: pl.DataFrame,
    unmatched_macro_groups: pl.DataFrame,
    unmatched_granular_groups: pl.DataFrame,
    config: ReconciliationConfig,
) -> IntegrityValidationResult:

    _validate_input_columns(rounded_allocations_df=rounded_allocations_df, config=config)

    group_keys = list(config.group_keys)
    macro_col = config.columns.macro_target_qty_col
    final_col = "final_allocated_qty"
    tolerance = 0.5 / (10**config.quantity_decimals)

    group_checks = (
        rounded_allocations_df.group_by(group_keys)
        .agg(
            pl.first(macro_col).alias(macro_col),
            pl.col(final_col).sum().alias("final_allocated_total"),
            pl.len().alias("row_count"),
        )
        .with_columns(
            (pl.col(macro_col) - pl.col("final_allocated_total")).alias("allocation_gap"),
            (pl.col(macro_col) == pl.col("final_allocated_total")).alias("exact_match"),
            (pl.col(macro_col) - pl.col("final_allocated_total")).abs().alias("absolute_gap"),
        )
        .with_columns(
            (pl.col("absolute_gap") <= tolerance).alias("within_tolerance")
        )
    )

    negative_allocations = rounded_allocations_df.filter(pl.col(final_col) < 0)

    has_group_gap = group_checks.filter(~pl.col("within_tolerance")).height > 0
    has_negative_allocations = negative_allocations.height > 0
    has_unmatched_macro = unmatched_macro_groups.height > 0
    has_unmatched_granular = unmatched_granular_groups.height > 0

    negative_allocation_failure = (
        has_negative_allocations and not config.allow_negative_allocations
    )

    is_valid = not any(
        [
            has_group_gap,
            negative_allocation_failure,
            has_unmatched_macro,
            has_unmatched_granular,
        ]
    )

    summary = pl.DataFrame(
        {
            "validated_group_count": [group_checks.height],
            "groups_with_gap_count": [group_checks.filter(~pl.col("within_tolerance")).height],
            "negative_allocation_count": [negative_allocations.height],
            "unmatched_macro_group_count": [unmatched_macro_groups.height],
            "unmatched_granular_group_count": [unmatched_granular_groups.height],
            "is_valid": [is_valid],
        }
    )

    if config.enforce_exact_totals and not is_valid:
        failure_reasons: list[str] = []

        if has_group_gap:
            failure_reasons.append("group control totals do not match macro targets")
        if negative_allocation_failure:
            failure_reasons.append("negative final allocations were detected")
        if has_unmatched_macro:
            failure_reasons.append("unmatched macro groups were detected")
        if has_unmatched_granular:
            failure_reasons.append("unmatched granular groups were detected")

        raise IntegrityCheckError(
            "Reconciliation integrity validation failed: "
            + "; ".join(failure_reasons)
            + "."
        )

    return IntegrityValidationResult(
        summary=summary,
        group_checks=group_checks,
        negative_allocations=negative_allocations,
        unmatched_macro_groups=unmatched_macro_groups,
        unmatched_granular_groups=unmatched_granular_groups,
        is_valid=is_valid,
    )


def _validate_input_columns(
    rounded_allocations_df: pl.DataFrame,
    config: ReconciliationConfig,
) -> None:
    required_columns = set(config.group_keys) | {
        config.columns.macro_target_qty_col,
        "final_allocated_qty",
    }
    missing = [col for col in sorted(required_columns) if col not in rounded_allocations_df.columns]

    if missing:
        raise IntegrityCheckError(
            "Rounded allocation dataset is missing required columns for integrity validation: "
            f"{', '.join(missing)}."
        )