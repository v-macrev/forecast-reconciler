from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl
from openpyxl import Workbook

from forecast_reconciler.exceptions import DataValidationError


@dataclass(frozen=True, slots=True)
class ExcelExportResult:

    output_path: Path
    sheet_names: tuple[str, ...]


def export_reconciliation_workbook(
    output_path: str | Path,
    final_allocations_df: pl.DataFrame,
    group_summary_df: pl.DataFrame,
    sku_variance_df: pl.DataFrame,
    integrity_summary_df: pl.DataFrame,
) -> ExcelExportResult:

    _validate_export_inputs(
        final_allocations_df=final_allocations_df,
        group_summary_df=group_summary_df,
        sku_variance_df=sku_variance_df,
        integrity_summary_df=integrity_summary_df,
    )

    path = Path(output_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)

    workbook = Workbook()
    default_sheet = workbook.active
    workbook.remove(default_sheet)

    sheet_payloads = [
        ("final_allocations", final_allocations_df),
        ("group_summary", group_summary_df),
        ("sku_variance", sku_variance_df),
        ("integrity_summary", integrity_summary_df),
    ]

    written_sheet_names: list[str] = []

    for sheet_name, df in sheet_payloads:
        worksheet = workbook.create_sheet(title=sheet_name)
        _write_dataframe_to_worksheet(worksheet=worksheet, df=df)
        written_sheet_names.append(sheet_name)

    workbook.save(path)

    return ExcelExportResult(
        output_path=path,
        sheet_names=tuple(written_sheet_names),
    )


def _validate_export_inputs(
    final_allocations_df: pl.DataFrame,
    group_summary_df: pl.DataFrame,
    sku_variance_df: pl.DataFrame,
    integrity_summary_df: pl.DataFrame,
) -> None:
    datasets = {
        "final_allocations": final_allocations_df,
        "group_summary": group_summary_df,
        "sku_variance": sku_variance_df,
        "integrity_summary": integrity_summary_df,
    }

    for dataset_name, df in datasets.items():
        if not isinstance(df, pl.DataFrame):
            raise DataValidationError(
                f"Export input '{dataset_name}' must be a Polars DataFrame."
            )

        if df.width == 0:
            raise DataValidationError(
                f"Export input '{dataset_name}' must contain at least one column."
            )


def _write_dataframe_to_worksheet(worksheet: Any, df: pl.DataFrame) -> None:

    worksheet.append(list(df.columns))

    for row in df.iter_rows(named=False):
        worksheet.append([_serialise_cell_value(value) for value in row])


def _serialise_cell_value(value: Any) -> Any:

    if isinstance(value, datetime):
        return value.replace(tzinfo=None)

    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)

    return value