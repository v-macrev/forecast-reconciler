from datetime import date
from io import BytesIO
from zipfile import ZipFile

import polars as pl

from forecast_reconciler.app.streamlit_app import (
    PipelineRunResult,
    build_workbook_filename,
    load_uploaded_table,
    run_reconciliation_pipeline,
)
from forecast_reconciler.config import ReconciliationConfig


def test_build_workbook_filename_uses_expected_pattern():
    filename = build_workbook_filename()

    assert filename.startswith("forecast_reconciliation_")
    assert filename.endswith(".xlsx")
    assert len(filename) == len("forecast_reconciliation_YYYYMMDD_HHMMSS.xlsx")


def test_load_uploaded_table_reads_csv_content():
    csv_bytes = BytesIO(
        b"period,market,channel,macro_target_qty\n2026-01,SP,Retail,100\n"
    )

    df = load_uploaded_table(csv_bytes, "macro.csv")

    assert df.columns == ["period", "market", "channel", "macro_target_qty"]
    assert df.to_dicts() == [
        {
            "period": "2026-01",
            "market": "SP",
            "channel": "Retail",
            "macro_target_qty": 100,
        }
    ]


def test_load_uploaded_table_rejects_unsupported_extension():
    csv_bytes = BytesIO(b"anything")

    try:
        load_uploaded_table(csv_bytes, "macro.txt")
    except ValueError as exc:
        assert str(exc) == (
            "Unsupported input file format '.txt'. Supported formats are .csv and .xlsx/.xlsm."
        )
    else:
        raise AssertionError("Expected ValueError for unsupported uploaded file type.")


def test_run_reconciliation_pipeline_returns_full_result_and_workbook_bytes():
    config = ReconciliationConfig(
        quantity_mode="integer",
        quantity_decimals=0,
        zero_baseline_mode="fail",
        allow_negative_allocations=False,
        enforce_exact_totals=True,
    )

    macro_df = pl.DataFrame(
        {
            "period": ["2026-01"],
            "market": ["SP"],
            "channel": ["Retail"],
            "macro_target_qty": [120],
        }
    )

    granular_df = pl.DataFrame(
        {
            "period": ["2026-01", "2026-01"],
            "market": ["SP", "SP"],
            "channel": ["Retail", "Retail"],
            "sku": ["SKU-001", "SKU-002"],
            "baseline_qty": [60, 40],
        }
    )

    result = run_reconciliation_pipeline(
        macro_df=macro_df,
        granular_df=granular_df,
        config=config,
    )

    assert isinstance(result, PipelineRunResult)

    final_allocations = result.final_allocations.sort("sku")
    assert final_allocations.select(
        ["period", "market", "channel", "sku", "final_allocated_qty"]
    ).to_dicts() == [
        {
            "period": date(2026, 1, 1),
            "market": "SP",
            "channel": "Retail",
            "sku": "SKU-001",
            "final_allocated_qty": 72.0,
        },
        {
            "period": date(2026, 1, 1),
            "market": "SP",
            "channel": "Retail",
            "sku": "SKU-002",
            "final_allocated_qty": 48.0,
        },
    ]

    assert result.group_summary.height == 1
    assert result.sku_variance.height == 2
    assert result.integrity_summary.to_dicts() == [
        {
            "validated_group_count": 1,
            "groups_with_gap_count": 0,
            "negative_allocation_count": 0,
            "unmatched_macro_group_count": 0,
            "unmatched_granular_group_count": 0,
            "is_valid": True,
        }
    ]

    assert result.workbook_name.startswith("forecast_reconciliation_")
    assert result.workbook_name.endswith(".xlsx")
    assert isinstance(result.workbook_bytes, bytes)
    assert len(result.workbook_bytes) > 0

    with ZipFile(BytesIO(result.workbook_bytes), "r") as workbook_zip:
        names = set(workbook_zip.namelist())
        assert "[Content_Types].xml" in names
        assert "xl/workbook.xml" in names