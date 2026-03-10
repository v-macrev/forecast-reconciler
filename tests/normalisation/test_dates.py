from datetime import date, datetime

import polars as pl

from forecast_reconciler.exceptions import DataValidationError
from forecast_reconciler.normalisation.dates import normalise_period_column


def test_normalise_period_column_accepts_year_month_format():
    df = pl.DataFrame(
        {
            "period": ["2026-01", "2026-02"],
            "value": [10, 20],
        }
    )

    result = normalise_period_column(df=df, period_col="period")

    assert result.schema["period"] == pl.Date
    assert result.get_column("period").to_list() == [
        date(2026, 1, 1),
        date(2026, 2, 1),
    ]


def test_normalise_period_column_accepts_full_iso_date_format():
    df = pl.DataFrame(
        {
            "period": ["2026-01-31", "2026-02-15"],
            "value": [10, 20],
        }
    )

    result = normalise_period_column(df=df, period_col="period")

    assert result.get_column("period").to_list() == [
        date(2026, 1, 1),
        date(2026, 2, 1),
    ]


def test_normalise_period_column_accepts_slash_based_formats():
    df = pl.DataFrame(
        {
            "period": ["2026/01", "15/02/2026"],
            "value": [10, 20],
        }
    )

    result = normalise_period_column(df=df, period_col="period")

    assert result.get_column("period").to_list() == [
        date(2026, 1, 1),
        date(2026, 2, 1),
    ]


def test_normalise_period_column_accepts_python_date_values():
    df = pl.DataFrame(
        {
            "period": [date(2026, 1, 31), date(2026, 2, 28)],
            "value": [10, 20],
        }
    )

    result = normalise_period_column(df=df, period_col="period")

    assert result.get_column("period").to_list() == [
        date(2026, 1, 1),
        date(2026, 2, 1),
    ]


def test_normalise_period_column_accepts_python_datetime_values():
    df = pl.DataFrame(
        {
            "period": [datetime(2026, 1, 31, 10, 5), datetime(2026, 2, 28, 23, 59)],
            "value": [10, 20],
        }
    )

    result = normalise_period_column(df=df, period_col="period")

    assert result.get_column("period").to_list() == [
        date(2026, 1, 1),
        date(2026, 2, 1),
    ]


def test_normalise_period_column_rejects_missing_period_column():
    df = pl.DataFrame({"value": [10]})

    try:
        normalise_period_column(df=df, period_col="period")
    except DataValidationError as exc:
        assert (
            str(exc)
            == "Period column 'period' does not exist in the provided dataset."
        )
    else:
        raise AssertionError("Expected DataValidationError for missing period column.")


def test_normalise_period_column_rejects_null_values():
    df = pl.DataFrame(
        {
            "period": ["2026-01", None],
            "value": [10, 20],
        },
        strict=False,
    )

    try:
        normalise_period_column(df=df, period_col="period")
    except DataValidationError as exc:
        assert (
            str(exc)
            == "Period column 'period' contains null values, which are not allowed."
        )
    else:
        raise AssertionError("Expected DataValidationError for null period values.")


def test_normalise_period_column_rejects_empty_string_values():
    df = pl.DataFrame(
        {
            "period": ["2026-01", "   "],
            "value": [10, 20],
        }
    )

    try:
        normalise_period_column(df=df, period_col="period")
    except DataValidationError as exc:
        assert (
            str(exc)
            == "Period column 'period' contains empty string values, which are not allowed."
        )
    else:
        raise AssertionError("Expected DataValidationError for empty string period values.")


def test_normalise_period_column_rejects_invalid_string_format():
    df = pl.DataFrame(
        {
            "period": ["2026-13"],
            "value": [10],
        }
    )

    try:
        normalise_period_column(df=df, period_col="period")
    except DataValidationError as exc:
        assert str(exc) == (
            "Invalid period value '2026-13' found in column 'period'. "
            "Supported formats are: %Y-%m, %Y-%m-%d, %Y/%m, %Y/%m/%d, %d/%m/%Y."
        )
    else:
        raise AssertionError("Expected DataValidationError for invalid period format.")


def test_normalise_period_column_rejects_unsupported_types():
    df = pl.DataFrame(
        {
            "period": [202601],
            "value": [10],
        }
    )

    try:
        normalise_period_column(df=df, period_col="period")
    except DataValidationError as exc:
        assert (
            str(exc)
            == "Unsupported period value type 'int' found in column 'period'."
        )
    else:
        raise AssertionError("Expected DataValidationError for unsupported period type.")