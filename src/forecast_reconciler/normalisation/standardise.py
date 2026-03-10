from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Iterable

import polars as pl

from forecast_reconciler.config import ReconciliationConfig
from forecast_reconciler.exceptions import DataValidationError
from forecast_reconciler.normalisation.dates import normalise_period_column
from forecast_reconciler.normalisation.schemas import (
    validate_granular_schema,
    validate_macro_schema,
)
from forecast_reconciler.types import ColumnName


def standardise_macro_input(
    df: pl.DataFrame,
    config: ReconciliationConfig,
) -> pl.DataFrame:

    validate_macro_schema(df=df, config=config)

    result = normalise_period_column(df=df, period_col=config.columns.period_col)
    result = _coerce_numeric_column(
        df=result,
        column=config.columns.macro_target_qty_col,
        dataset_name="macro",
    )

    ordered_columns = list(config.group_keys) + [config.columns.macro_target_qty_col]
    result = result.select(ordered_columns)

    _raise_if_duplicate_business_keys(
        df=result,
        key_columns=config.group_keys,
        dataset_name="macro",
    )

    return result


def standardise_granular_input(
    df: pl.DataFrame,
    config: ReconciliationConfig,
) -> pl.DataFrame:

    validate_granular_schema(df=df, config=config)

    result = normalise_period_column(df=df, period_col=config.columns.period_col)
    result = _coerce_numeric_column(
        df=result,
        column=config.columns.baseline_qty_col,
        dataset_name="granular",
    )

    granular_key_columns = tuple(
        list(config.group_keys) + [config.columns.sku_col]
        if config.columns.sku_col not in config.group_keys
        else list(config.group_keys)
    )

    ordered_columns = list(granular_key_columns) + [config.columns.baseline_qty_col]
    result = result.select(ordered_columns)

    _raise_if_duplicate_business_keys(
        df=result,
        key_columns=granular_key_columns,
        dataset_name="granular",
    )

    return result


def _coerce_numeric_column(
    df: pl.DataFrame,
    column: ColumnName,
    dataset_name: str,
) -> pl.DataFrame:
    values = df.get_column(column).to_list()
    coerced_values = [
        _coerce_single_numeric_value(value=value, column=column, dataset_name=dataset_name)
        for value in values
    ]

    return df.with_columns(pl.Series(name=column, values=coerced_values, dtype=pl.Float64))


def _coerce_single_numeric_value(
    value: object,
    column: ColumnName,
    dataset_name: str,
) -> float:
    if value is None:
        raise DataValidationError(
            f"{dataset_name} dataset contains null values in quantity column '{column}'."
        )

    if isinstance(value, bool):
        raise DataValidationError(
            f"{dataset_name} dataset contains non-numeric value '{value}' in quantity column '{column}'."
        )

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            raise DataValidationError(
                f"{dataset_name} dataset contains empty string values in quantity column '{column}'."
            )

        normalised = stripped.replace(",", "")

        try:
            return float(Decimal(normalised))
        except (InvalidOperation, ValueError):
            raise DataValidationError(
                f"{dataset_name} dataset contains non-numeric value '{value}' in quantity column '{column}'."
            ) from None

    raise DataValidationError(
        f"{dataset_name} dataset contains unsupported value type '{type(value).__name__}' "
        f"in quantity column '{column}'."
    )


def _raise_if_duplicate_business_keys(
    df: pl.DataFrame,
    key_columns: Iterable[ColumnName],
    dataset_name: str,
) -> None:
    key_columns_tuple = tuple(key_columns)

    duplicates = (
        df.group_by(list(key_columns_tuple))
        .len()
        .filter(pl.col("len") > 1)
    )

    if duplicates.height == 0:
        return

    raise DataValidationError(
        f"{dataset_name} dataset contains duplicate business keys for columns: "
        f"{', '.join(key_columns_tuple)}."
    )