from __future__ import annotations

from typing import Literal

import polars as pl


ReconciliationBasis = Literal["units", "value"]

NULL_MARKET_SENTINEL = "[NULL_MERCADO]"


def normalise_market_column(df: pl.DataFrame) -> pl.DataFrame:
    """
    Normalise market naming and preserve null market rows.

    Supported input columns:
    - market
    - dc_mercado

    Output:
    - market (Utf8, nulls replaced with sentinel)
    """
    if "market" not in df.columns and "dc_mercado" in df.columns:
        df = df.rename({"dc_mercado": "market"})

    if "market" in df.columns:
        df = df.with_columns(
            pl.when(pl.col("market").is_null())
            .then(pl.lit(NULL_MARKET_SENTINEL))
            .otherwise(pl.col("market").cast(pl.Utf8))
            .alias("market")
        )

    return df


def canonicalise_period_strings(
    df: pl.DataFrame,
    period_col: str = "period",
) -> pl.DataFrame:
    """
    Keep a canonical YYYY-MM-DD string representation for UI joins and locking metadata.
    """
    if period_col not in df.columns:
        return df

    return df.with_columns(
        pl.col(period_col).cast(pl.Utf8).str.slice(0, 10).alias(period_col)
    )


def prepare_granular_reference(
    granular_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Prepare granular reference data with both qty and value plus unit price.
    """
    work = canonicalise_period_strings(normalise_market_column(granular_df))

    required = {
        "period",
        "market",
        "channel",
        "client",
        "sku",
        "baseline_qty",
        "baseline_value",
    }
    missing = [col for col in sorted(required) if col not in work.columns]
    if missing:
        raise ValueError(
            "Granular dataset is missing required columns: "
            f"{', '.join(missing)}."
        )

    return (
        work.with_columns(
            pl.col("client").cast(pl.Utf8),
            pl.col("sku").cast(pl.Utf8),
            pl.col("baseline_qty").cast(pl.Float64),
            pl.col("baseline_value").cast(pl.Float64),
        )
        .with_columns(
            pl.when(pl.col("baseline_qty") > 0)
            .then(pl.col("baseline_value") / pl.col("baseline_qty"))
            .otherwise(None)
            .alias("unit_price")
        )
        .select(
            [
                "period",
                "market",
                "channel",
                "client",
                "sku",
                "baseline_qty",
                "baseline_value",
                "unit_price",
            ]
        )
    )


def _prepare_lock_df(lock_df: pl.DataFrame | None) -> pl.DataFrame:
    if lock_df is None:
        return pl.DataFrame(
            schema={
                "period": pl.Utf8,
                "market": pl.Utf8,
                "channel": pl.Utf8,
                "is_locked": pl.Boolean,
                "locked_target": pl.Float64,
            }
        )

    return (
        canonicalise_period_strings(normalise_market_column(lock_df))
        .select(
            [
                "period",
                "market",
                "channel",
                pl.col("is_locked").cast(pl.Boolean),
                pl.col("locked_target").cast(pl.Float64),
            ]
        )
        .unique()
    )


def build_lock_template(
    granular_df: pl.DataFrame,
    basis: ReconciliationBasis,
    locked_periods: list[str],
) -> pl.DataFrame:
    """
    Build an editable lock template for selected periods using GRANULAR monthly totals.

    This is the correct business source of truth for month locking:
    - locked months must preserve their granular absolute totals
    - the lock editor is prefilled from the granular baseline aggregate

    Units basis:
    - locked_target = sum(baseline_qty) by period/market/channel

    Value basis:
    - locked_target = sum(baseline_value) by period/market/channel
    """
    if not locked_periods:
        return pl.DataFrame(
            schema={
                "period": pl.Utf8,
                "market": pl.Utf8,
                "channel": pl.Utf8,
                "is_locked": pl.Boolean,
                "locked_target": pl.Float64,
            }
        )

    granular_ref = prepare_granular_reference(granular_df)

    if basis == "units":
        target_expr = pl.col("baseline_qty").sum().alias("locked_target")
    else:
        target_expr = pl.col("baseline_value").sum().alias("locked_target")

    return (
        granular_ref.filter(pl.col("period").is_in(locked_periods))
        .group_by(["period", "market", "channel"])
        .agg(target_expr)
        .with_columns(
            pl.lit(True).alias("is_locked"),
            pl.col("locked_target").cast(pl.Float64),
        )
        .select(["period", "market", "channel", "is_locked", "locked_target"])
        .sort(["period", "market", "channel"])
    )


def prepare_direct_targets(
    macro_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Normalise direct macro targets and preserve both target columns when available.
    """
    work = canonicalise_period_strings(normalise_market_column(macro_df))

    if "market" not in work.columns:
        raise ValueError("Macro dataset must contain 'market' or 'dc_mercado'.")

    required = {"period", "market", "channel"}
    missing = [col for col in sorted(required) if col not in work.columns]
    if missing:
        raise ValueError(
            "Macro dataset is missing required key columns: "
            f"{', '.join(missing)}."
        )

    if "macro_target_qty" not in work.columns:
        work = work.with_columns(pl.lit(None).cast(pl.Float64).alias("macro_target_qty"))

    if "macro_target_value" not in work.columns:
        work = work.with_columns(pl.lit(None).cast(pl.Float64).alias("macro_target_value"))

    return (
        work.select(
            [
                "period",
                "market",
                "channel",
                pl.col("macro_target_qty").cast(pl.Float64),
                pl.col("macro_target_value").cast(pl.Float64),
            ]
        )
        .unique()
        .sort(["period", "market", "channel"])
    )


def build_share_based_targets(
    macro_df: pl.DataFrame,
    granular_df: pl.DataFrame,
    share_target: float,
    basis: ReconciliationBasis,
    lock_df: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """
    Build monthly macro targets from an ANNUAL market share target.

    Rules:
    - share_target is annual
    - annual target is calculated per year + market + channel
    - locked months are sourced from GRANULAR monthly totals
    - locked months keep their absolute granular value unchanged
    - only unlocked months absorb the remaining annual target
    - unlocked redistribution is weighted by monthly market totals
    - output ALWAYS contains both macro_target_qty and macro_target_value

    Required macro columns:
    - period
    - market or dc_mercado
    - channel
    - total_market_qty
    - total_market_value
    """
    required = {"period", "channel", "total_market_qty", "total_market_value"}
    missing = [col for col in sorted(required) if col not in macro_df.columns]
    if missing:
        raise ValueError(
            "Macro dataset is missing required columns for share-based target generation: "
            f"{', '.join(missing)}."
        )

    macro_work = canonicalise_period_strings(normalise_market_column(macro_df))
    if "market" not in macro_work.columns:
        raise ValueError("Macro dataset must contain 'market' or 'dc_mercado'.")

    granular_ref = prepare_granular_reference(granular_df)

    macro_work = macro_work.with_columns(
        pl.col("total_market_qty").cast(pl.Float64),
        pl.col("total_market_value").cast(pl.Float64),
        pl.col("period").str.slice(0, 4).alias("_year"),
    )

    if basis == "units":
        driver_total_col = "total_market_qty"
        locked_source_expr = pl.col("baseline_qty").sum().alias("_granular_locked_target")
    else:
        driver_total_col = "total_market_value"
        locked_source_expr = pl.col("baseline_value").sum().alias("_granular_locked_target")

    granular_monthly = (
        granular_ref.group_by(["period", "market", "channel"])
        .agg(
            pl.col("baseline_qty").sum().alias("_granular_month_qty"),
            pl.col("baseline_value").sum().alias("_granular_month_value"),
            locked_source_expr,
        )
    )

    lock_work = _prepare_lock_df(lock_df)

    macro_work = (
        macro_work.join(
            granular_monthly,
            on=["period", "market", "channel"],
            how="left",
        )
        .join(
            lock_work,
            on=["period", "market", "channel"],
            how="left",
        )
        .with_columns(
            pl.col("is_locked").fill_null(False),
            pl.col("locked_target").cast(pl.Float64),
            pl.col("_granular_locked_target").cast(pl.Float64),
        )
    )

    annual_keys = ["_year", "market", "channel"]

    macro_work = macro_work.with_columns(
        pl.when(
            pl.col("total_market_qty").is_not_null()
            & pl.col("total_market_value").is_not_null()
            & (pl.col("total_market_qty") > 0)
        )
        .then(pl.col("total_market_value") / pl.col("total_market_qty"))
        .otherwise(None)
        .alias("_market_unit_price"),
        (pl.col(driver_total_col).sum().over(annual_keys) * pl.lit(float(share_target))).alias(
            "_annual_driver_target"
        ),
    )

    macro_work = macro_work.with_columns(
        pl.when(pl.col("is_locked"))
        .then(
            pl.coalesce(
                [
                    pl.col("locked_target"),
                    pl.col("_granular_locked_target"),
                ]
            )
        )
        .otherwise(None)
        .alias("_effective_locked_driver_target")
    )

    missing_locked_source = macro_work.filter(
        pl.col("is_locked") & pl.col("_effective_locked_driver_target").is_null()
    )
    if missing_locked_source.height > 0:
        raise ValueError(
            "At least one locked month could not be sourced from the granular table."
        )

    macro_work = macro_work.with_columns(
        pl.col("_effective_locked_driver_target")
        .fill_null(0.0)
        .sum()
        .over(annual_keys)
        .alias("_locked_driver_total"),
        pl.when(~pl.col("is_locked"))
        .then(pl.col(driver_total_col))
        .otherwise(0.0)
        .sum()
        .over(annual_keys)
        .alias("_unlocked_driver_weight_total"),
    )

    macro_work = macro_work.with_columns(
        (pl.col("_annual_driver_target") - pl.col("_locked_driver_total")).alias(
            "_remaining_driver_target"
        )
    )

    infeasible_locked = macro_work.filter(pl.col("_remaining_driver_target") < -1e-9)
    if infeasible_locked.height > 0:
        raise ValueError(
            "Locked granular totals exceed the annual target for one or more market/channel/year groups."
        )

    invalid_unlocked = macro_work.filter(
        (~pl.col("is_locked"))
        & (pl.col("_remaining_driver_target") > 1e-9)
        & (pl.col("_unlocked_driver_weight_total") <= 0)
    )
    if invalid_unlocked.height > 0:
        raise ValueError(
            "At least one unlocked annual group has no positive market weight available for redistribution."
        )

    driver_target_expr = (
        pl.when(pl.col("is_locked"))
        .then(pl.col("_effective_locked_driver_target"))
        .otherwise(
            pl.when(pl.col("_unlocked_driver_weight_total") > 0)
            .then(
                pl.col("_remaining_driver_target")
                * pl.col(driver_total_col)
                / pl.col("_unlocked_driver_weight_total")
            )
            .otherwise(0.0)
        )
    )

    if basis == "units":
        macro_work = macro_work.with_columns(
            driver_target_expr.alias("macro_target_qty")
        ).with_columns(
            pl.when(
                pl.col("_market_unit_price").is_not_null()
                & (pl.col("_market_unit_price") > 0)
            )
            .then(pl.col("macro_target_qty") * pl.col("_market_unit_price"))
            .otherwise(None)
            .alias("macro_target_value")
        )
    else:
        macro_work = macro_work.with_columns(
            driver_target_expr.alias("macro_target_value")
        ).with_columns(
            pl.when(
                pl.col("_market_unit_price").is_not_null()
                & (pl.col("_market_unit_price") > 0)
            )
            .then(pl.col("macro_target_value") / pl.col("_market_unit_price"))
            .otherwise(None)
            .alias("macro_target_qty")
        )

    return (
        macro_work.select(
            [
                "period",
                "market",
                "channel",
                pl.col("macro_target_qty").cast(pl.Float64),
                pl.col("macro_target_value").cast(pl.Float64),
            ]
        )
        .unique()
        .sort(["period", "market", "channel"])
    )


def _cast_period_to_match(
    source_df: pl.DataFrame,
    target_df: pl.DataFrame,
    period_col: str = "period",
) -> pl.DataFrame:
    """
    Cast source period column to the same dtype as target period column.
    """
    if period_col not in source_df.columns or period_col not in target_df.columns:
        return source_df

    source_dtype = source_df.schema[period_col]
    target_dtype = target_df.schema[period_col]

    if source_dtype == target_dtype:
        return source_df

    return source_df.with_columns(pl.col(period_col).cast(target_dtype).alias(period_col))


def align_to_macro_groups(
    macro_df: pl.DataFrame,
    granular_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Keep rows only for existing macro groups and preserve null-market sentinel.

    Important:
    - preserve the dtype of the macro period column
    - do not blindly coerce standardised Date columns back to string
    """
    macro_work = normalise_market_column(macro_df)
    granular_work = normalise_market_column(granular_df)

    granular_work = _cast_period_to_match(granular_work, macro_work, period_col="period")

    valid_groups = macro_work.select(["period", "market", "channel"]).unique()

    return granular_work.join(
        valid_groups,
        on=["period", "market", "channel"],
        how="inner",
    )


def build_dual_metric_outputs(
    engine_allocations_df: pl.DataFrame,
    macro_targets_df: pl.DataFrame,
    granular_reference_df: pl.DataFrame,
    basis: ReconciliationBasis,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Convert engine outputs into final outputs with both qty and value.
    """
    keys = ["period", "market", "channel", "client", "sku"]
    group_keys = ["period", "market", "channel"]

    valid_groups = engine_allocations_df.select(group_keys).unique()

    granular_reference_work = _cast_period_to_match(
        granular_reference_df,
        valid_groups,
        period_col="period",
    )
    macro_targets_work = _cast_period_to_match(
        macro_targets_df,
        valid_groups,
        period_col="period",
    )

    ref = (
        granular_reference_work.join(valid_groups, on=group_keys, how="inner")
        .select(keys + ["baseline_qty", "baseline_value", "unit_price"])
    )

    macro_ref = (
        macro_targets_work.join(valid_groups, on=group_keys, how="inner")
        .select(group_keys + ["macro_target_qty", "macro_target_value"])
        .unique()
    )

    if basis == "units":
        enriched = (
            engine_allocations_df.join(
                ref.select(keys + ["baseline_value", "unit_price"]),
                on=keys,
                how="left",
            )
            .drop("macro_target_qty", strict=False)
            .join(
                macro_ref,
                on=group_keys,
                how="left",
            )
            .with_columns(
                pl.when(pl.col("unit_price").is_not_null() & (pl.col("unit_price") > 0))
                .then(pl.col("raw_allocated_qty") * pl.col("unit_price"))
                .otherwise(None)
                .alias("raw_allocated_value"),
                pl.when(pl.col("unit_price").is_not_null() & (pl.col("unit_price") > 0))
                .then(pl.col("rounded_allocated_qty") * pl.col("unit_price"))
                .otherwise(None)
                .alias("rounded_allocated_value"),
                pl.when(pl.col("unit_price").is_not_null() & (pl.col("unit_price") > 0))
                .then(pl.col("final_allocated_qty") * pl.col("unit_price"))
                .otherwise(None)
                .alias("final_allocated_value"),
            )
        )
    else:
        enriched = (
            engine_allocations_df.rename(
                {
                    "baseline_qty": "baseline_value",
                    "group_baseline_qty": "group_baseline_value",
                    "macro_target_qty": "_driver_macro_target_value",
                    "raw_allocated_qty": "raw_allocated_value",
                    "rounded_allocated_qty": "rounded_allocated_value",
                    "final_allocated_qty": "final_allocated_value",
                }
            )
            .join(
                ref.select(keys + ["baseline_qty", "unit_price"]),
                on=keys,
                how="left",
            )
            .join(
                macro_ref,
                on=group_keys,
                how="left",
            )
            .with_columns(
                pl.when(pl.col("unit_price").is_not_null() & (pl.col("unit_price") > 0))
                .then(pl.col("raw_allocated_value") / pl.col("unit_price"))
                .otherwise(None)
                .alias("raw_allocated_qty"),
                pl.when(pl.col("unit_price").is_not_null() & (pl.col("unit_price") > 0))
                .then(pl.col("rounded_allocated_value") / pl.col("unit_price"))
                .otherwise(None)
                .alias("rounded_allocated_qty"),
                pl.when(pl.col("unit_price").is_not_null() & (pl.col("unit_price") > 0))
                .then(pl.col("final_allocated_value") / pl.col("unit_price"))
                .otherwise(None)
                .alias("final_allocated_qty"),
            )
        )

    final_allocations = (
        enriched.select(
            group_keys
            + [
                "client",
                "sku",
                "baseline_qty",
                "baseline_value",
                "unit_price",
                "weight",
                "macro_target_qty",
                "macro_target_value",
                "raw_allocated_qty",
                "raw_allocated_value",
                "rounded_allocated_qty",
                "rounded_allocated_value",
                "final_allocated_qty",
                "final_allocated_value",
            ]
        )
        .sort(group_keys + ["client", "sku"])
    )

    group_summary = (
        final_allocations.group_by(group_keys)
        .agg(
            pl.first("macro_target_qty").alias("macro_target_qty"),
            pl.first("macro_target_value").alias("macro_target_value"),
            pl.col("baseline_qty").sum().alias("baseline_group_qty"),
            pl.col("baseline_value").sum().alias("baseline_group_value"),
            pl.col("final_allocated_qty").sum().alias("final_allocated_group_qty"),
            pl.col("final_allocated_value").sum().alias("final_allocated_group_value"),
            pl.len().alias("row_count"),
        )
        .with_columns(
            (pl.col("final_allocated_group_qty") - pl.col("baseline_group_qty")).alias(
                "group_delta_qty"
            ),
            (pl.col("final_allocated_group_value") - pl.col("baseline_group_value")).alias(
                "group_delta_value"
            ),
            pl.when(pl.col("baseline_group_qty") == 0)
            .then(None)
            .otherwise(
                (pl.col("final_allocated_group_qty") - pl.col("baseline_group_qty"))
                / pl.col("baseline_group_qty")
            )
            .alias("group_delta_qty_pct"),
            pl.when(pl.col("baseline_group_value") == 0)
            .then(None)
            .otherwise(
                (pl.col("final_allocated_group_value") - pl.col("baseline_group_value"))
                / pl.col("baseline_group_value")
            )
            .alias("group_delta_value_pct"),
            (pl.col("macro_target_qty") - pl.col("final_allocated_group_qty")).alias(
                "final_gap_to_target_qty"
            ),
            (pl.col("macro_target_value") - pl.col("final_allocated_group_value")).alias(
                "final_gap_to_target_value"
            ),
        )
        .sort(group_keys)
    )

    sku_variance = (
        final_allocations.with_columns(
            pl.when(pl.col("final_allocated_qty").sum().over(group_keys) == 0)
            .then(None)
            .otherwise(
                pl.col("final_allocated_qty")
                / pl.col("final_allocated_qty").sum().over(group_keys)
            )
            .alias("final_weight_qty"),
            pl.when(pl.col("final_allocated_value").sum().over(group_keys) == 0)
            .then(None)
            .otherwise(
                pl.col("final_allocated_value")
                / pl.col("final_allocated_value").sum().over(group_keys)
            )
            .alias("final_weight_value"),
        )
        .with_columns(
            (pl.col("final_allocated_qty") - pl.col("baseline_qty")).alias("sku_delta_qty"),
            (pl.col("final_allocated_value") - pl.col("baseline_value")).alias(
                "sku_delta_value"
            ),
            pl.when(pl.col("baseline_qty") == 0)
            .then(None)
            .otherwise(
                (pl.col("final_allocated_qty") - pl.col("baseline_qty"))
                / pl.col("baseline_qty")
            )
            .alias("sku_delta_qty_pct"),
            pl.when(pl.col("baseline_value") == 0)
            .then(None)
            .otherwise(
                (pl.col("final_allocated_value") - pl.col("baseline_value"))
                / pl.col("baseline_value")
            )
            .alias("sku_delta_value_pct"),
        )
        .select(
            group_keys
            + [
                "client",
                "sku",
                "baseline_qty",
                "baseline_value",
                "unit_price",
                "weight",
                "raw_allocated_qty",
                "raw_allocated_value",
                "rounded_allocated_qty",
                "rounded_allocated_value",
                "final_allocated_qty",
                "final_allocated_value",
                "final_weight_qty",
                "final_weight_value",
                "sku_delta_qty",
                "sku_delta_qty_pct",
                "sku_delta_value",
                "sku_delta_value_pct",
            ]
        )
        .sort(group_keys + ["client", "sku"])
    )

    return final_allocations, group_summary, sku_variance