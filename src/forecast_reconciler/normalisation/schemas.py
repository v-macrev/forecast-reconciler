from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import polars as pl

from forecast_reconciler.config import ReconciliationConfig
from forecast_reconciler.exceptions import SchemaValidationError
from forecast_reconciler.types import ColumnName


@dataclass(frozen=True, slots=True)
class SchemaValidationReport:

    dataset_name: str
    row_count: int
    column_count: int
    required_columns: tuple[ColumnName, ...]
    actual_columns: tuple[ColumnName, ...]


def validate_macro_schema(
    df: pl.DataFrame,
    config: ReconciliationConfig,
) -> SchemaValidationReport:
    return _validate_dataset_schema(
        df=df,
        dataset_name="macro",
        required_columns=config.macro_required_columns,
        non_nullable_columns=config.group_keys,
    )


def validate_granular_schema(
    df: pl.DataFrame,
    config: ReconciliationConfig,
) -> SchemaValidationReport:
    granular_non_nullable_columns = (
        *config.group_keys,
        config.columns.client_col,
        config.columns.sku_col,
    )

    return _validate_dataset_schema(
        df=df,
        dataset_name="granular",
        required_columns=config.granular_required_columns,
        non_nullable_columns=granular_non_nullable_columns,
    )


def _validate_dataset_schema(
    df: pl.DataFrame,
    dataset_name: str,
    required_columns: Iterable[ColumnName],
    non_nullable_columns: Iterable[ColumnName],
) -> SchemaValidationReport:
    actual_columns = tuple(df.columns)
    required_columns_tuple = tuple(required_columns)
    non_nullable_columns_tuple = tuple(non_nullable_columns)

    duplicate_columns = _find_duplicate_columns(actual_columns)
    if duplicate_columns:
        duplicates_txt = ", ".join(duplicate_columns)
        raise SchemaValidationError(
            f"{dataset_name} dataset contains duplicate columns: {duplicates_txt}."
        )

    missing_columns = [
        column for column in required_columns_tuple if column not in actual_columns
    ]
    if missing_columns:
        missing_txt = ", ".join(missing_columns)
        raise SchemaValidationError(
            f"{dataset_name} dataset is missing required columns: {missing_txt}."
        )

    null_columns = [
        column
        for column in non_nullable_columns_tuple
        if df.get_column(column).null_count() > 0
    ]
    if null_columns:
        null_txt = ", ".join(null_columns)
        raise SchemaValidationError(
            f"{dataset_name} dataset contains null values in required columns: {null_txt}."
        )

    return SchemaValidationReport(
        dataset_name=dataset_name,
        row_count=df.height,
        column_count=df.width,
        required_columns=required_columns_tuple,
        actual_columns=actual_columns,
    )


def _find_duplicate_columns(columns: Iterable[ColumnName]) -> tuple[ColumnName, ...]:
    seen: set[ColumnName] = set()
    duplicates: list[ColumnName] = []

    for column in columns:
        if column in seen and column not in duplicates:
            duplicates.append(column)
        seen.add(column)

    return tuple(duplicates)