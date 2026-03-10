from __future__ import annotations

from datetime import date, datetime

import polars as pl

from forecast_reconciler.exceptions import DataValidationError
from forecast_reconciler.types import ColumnName

SUPPORTED_PERIOD_FORMATS: tuple[str, ...] = (
    "%Y-%m",
    "%Y-%m-%d",
    "%Y/%m",
    "%Y/%m/%d",
    "%d/%m/%Y",
)


def normalise_period_column(
    df: pl.DataFrame,
    period_col: ColumnName,
) -> pl.DataFrame:
    if period_col not in df.columns:
        raise DataValidationError(
            f"Period column '{period_col}' does not exist in the provided dataset."
        )

    normalised_values = [
        _normalise_single_period_value(value=value, period_col=period_col)
        for value in df.get_column(period_col).to_list()
    ]

    return df.with_columns(pl.Series(name=period_col, values=normalised_values))


def _normalise_single_period_value(
    value: object,
    period_col: ColumnName,
) -> date:
    if value is None:
        raise DataValidationError(
            f"Period column '{period_col}' contains null values, which are not allowed."
        )

    if isinstance(value, datetime):
        return date(value.year, value.month, 1)

    if isinstance(value, date):
        return date(value.year, value.month, 1)

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            raise DataValidationError(
                f"Period column '{period_col}' contains empty string values, which are not allowed."
            )

        for fmt in SUPPORTED_PERIOD_FORMATS:
            try:
                parsed = datetime.strptime(stripped, fmt)
                return date(parsed.year, parsed.month, 1)
            except ValueError:
                continue

        raise DataValidationError(
            f"Invalid period value '{value}' found in column '{period_col}'. "
            f"Supported formats are: {', '.join(SUPPORTED_PERIOD_FORMATS)}."
        )

    raise DataValidationError(
        f"Unsupported period value type '{type(value).__name__}' found in column '{period_col}'."
    )