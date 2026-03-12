from io import BytesIO
from zipfile import ZipFile

import polars as pl

from forecast_reconciler.app.streamlit_app import (
    build_export_filename,
    load_uploaded_table,
    run_reconciliation_pipeline,
)
from forecast_reconciler.config import ReconciliationConfig


def test_build_export_filename_returns_xlsx_name():
    filename = build_export_filename("xlsx")
    assert filename.startswith("forecast_reconciliation_")
    assert filename.endswith(".xlsx")


def test_build_export_filename_returns_zip_name():
    filename = build_export_filename("zip_csv")
    assert filename.startswith("forecast_reconciliation_")
    assert filename.endswith(".zip")


def test_load_uploaded_table_reads_csv_with_string_sku():
    csv_bytes = BytesIO(
        b"period,market,channel,client,sku,baseline_qty,baseline_value\n"
        b"2026-01-01,GEN,Retail,Client A,EL-JOLE181020241202617,10,150\n"
    )

    df = load_uploaded_table(csv_bytes, "granular.csv")

    assert df.get_column("sku").to_list() == ["EL-JOLE181020241202617"]


def test_run_reconciliation_pipeline_units_mode_returns_both_qty_and_value():
    config = ReconciliationConfig(
        quantity_mode="integer",
        quantity_decimals=0,
        zero_baseline_mode="fail",
        allow_negative_allocations=False,
        enforce_exact_totals=True,
    )

    macro_df = pl.DataFrame(
        {
            "period": ["2026-01-01"],
            "market": ["GEN"],
            "channel": ["Retail"],
            "macro_target_qty": [120.0],
            "macro_target_value": [1200.0],
        }
    )

    granular_df = pl.DataFrame(
        {
            "period": ["2026-01-01", "2026-01-01"],
            "market": ["GEN", "GEN"],
            "channel": ["Retail", "Retail"],
            "client": ["Client A", "Client B"],
            "sku": ["SKU-001", "SKU-002"],
            "baseline_qty": [60.0, 40.0],
            "baseline_value": [600.0, 400.0],
        }
    )

    result = run_reconciliation_pipeline(
        macro_df=macro_df,
        granular_df=granular_df,
        config=config,
        output_format="zip_csv",
        basis="units",
        macro_input_mode="Direct Macro Targets",
        share_target=None,
        lock_df=None,
    )

    final_allocations = result.final_allocations.sort(["client", "sku"])
    assert final_allocations.select(
        ["client", "sku", "final_allocated_qty", "final_allocated_value"]
    ).to_dicts() == [
        {
            "client": "Client A",
            "sku": "SKU-001",
            "final_allocated_qty": 72.0,
            "final_allocated_value": 720.0,
        },
        {
            "client": "Client B",
            "sku": "SKU-002",
            "final_allocated_qty": 48.0,
            "final_allocated_value": 480.0,
        },
    ]

    with ZipFile(BytesIO(result.export_bytes), "r") as zip_file:
        assert "final_allocations.csv" in zip_file.namelist()


def test_run_reconciliation_pipeline_value_mode_returns_both_qty_and_value():
    config = ReconciliationConfig(
        quantity_mode="decimal",
        quantity_decimals=2,
        zero_baseline_mode="fail",
        allow_negative_allocations=False,
        enforce_exact_totals=True,
    )

    macro_df = pl.DataFrame(
        {
            "period": ["2026-01-01"],
            "market": ["GEN"],
            "channel": ["Retail"],
            "macro_target_qty": [33.0],
            "macro_target_value": [330.0],
        }
    )

    granular_df = pl.DataFrame(
        {
            "period": ["2026-01-01", "2026-01-01"],
            "market": ["GEN", "GEN"],
            "channel": ["Retail", "Retail"],
            "client": ["Client A", "Client B"],
            "sku": ["SKU-001", "SKU-002"],
            "baseline_qty": [10.0, 20.0],
            "baseline_value": [100.0, 200.0],
        }
    )

    result = run_reconciliation_pipeline(
        macro_df=macro_df,
        granular_df=granular_df,
        config=config,
        output_format="zip_csv",
        basis="value",
        macro_input_mode="Direct Macro Targets",
        share_target=None,
        lock_df=None,
    )

    final_allocations = result.final_allocations.sort(["client", "sku"])
    assert final_allocations.select(
        ["client", "sku", "final_allocated_qty", "final_allocated_value"]
    ).to_dicts() == [
        {
            "client": "Client A",
            "sku": "SKU-001",
            "final_allocated_qty": 11.0,
            "final_allocated_value": 110.0,
        },
        {
            "client": "Client B",
            "sku": "SKU-002",
            "final_allocated_qty": 22.0,
            "final_allocated_value": 220.0,
        },
    ]