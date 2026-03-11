from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import BinaryIO

import polars as pl

from forecast_reconciler.config import ReconciliationConfig
from forecast_reconciler.io.writers import export_reconciliation_workbook
from forecast_reconciler.normalisation.standardise import (
    standardise_granular_input,
    standardise_macro_input,
)
from forecast_reconciler.reconciliation.allocator import redistribute_macro_targets
from forecast_reconciler.reconciliation.rounding import apply_deterministic_rounding
from forecast_reconciler.reconciliation.weights import calculate_weights
from forecast_reconciler.reporting.summaries import build_reporting_views
from forecast_reconciler.validation.integrity import validate_reconciliation_integrity


@dataclass(frozen=True, slots=True)
class PipelineRunResult:

    final_allocations: pl.DataFrame
    group_summary: pl.DataFrame
    sku_variance: pl.DataFrame
    integrity_summary: pl.DataFrame
    workbook_bytes: bytes
    workbook_name: str


def load_uploaded_table(file_obj: BinaryIO, file_name: str) -> pl.DataFrame:
    """
    Load an uploaded CSV or XLSX file into a Polars DataFrame.
    """
    suffix = Path(file_name).suffix.lower()

    if suffix == ".csv":
        return pl.read_csv(file_obj)

    if suffix in {".xlsx", ".xlsm"}:
        return pl.read_excel(file_obj)

    raise ValueError(
        f"Unsupported input file format '{suffix}'. Supported formats are .csv and .xlsx/.xlsm."
    )


def build_workbook_filename() -> str:

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return f"forecast_reconciliation_{timestamp}.xlsx"


def run_reconciliation_pipeline(
    macro_df: pl.DataFrame,
    granular_df: pl.DataFrame,
    config: ReconciliationConfig,
) -> PipelineRunResult:

    macro_std = standardise_macro_input(df=macro_df, config=config)
    granular_std = standardise_granular_input(df=granular_df, config=config)

    weight_result = calculate_weights(granular_df=granular_std, config=config)

    redistribution_result = redistribute_macro_targets(
        macro_df=macro_std,
        weighted_granular_df=weight_result.weights,
        config=config,
    )

    rounding_result = apply_deterministic_rounding(
        allocation_df=redistribution_result.allocations,
        config=config,
    )

    integrity_result = validate_reconciliation_integrity(
        rounded_allocations_df=rounding_result.allocations,
        unmatched_macro_groups=redistribution_result.unmatched_macro_groups,
        unmatched_granular_groups=redistribution_result.unmatched_granular_groups,
        config=config,
    )

    reporting_result = build_reporting_views(
        rounded_allocations_df=rounding_result.allocations,
        config=config,
    )

    workbook_name = build_workbook_filename()
    workbook_bytes = _build_workbook_bytes(
        workbook_name=workbook_name,
        final_allocations_df=rounding_result.allocations,
        group_summary_df=reporting_result.group_summary,
        sku_variance_df=reporting_result.sku_variance,
        integrity_summary_df=integrity_result.summary,
    )

    return PipelineRunResult(
        final_allocations=rounding_result.allocations,
        group_summary=reporting_result.group_summary,
        sku_variance=reporting_result.sku_variance,
        integrity_summary=integrity_result.summary,
        workbook_bytes=workbook_bytes,
        workbook_name=workbook_name,
    )


def _build_workbook_bytes(
    workbook_name: str,
    final_allocations_df: pl.DataFrame,
    group_summary_df: pl.DataFrame,
    sku_variance_df: pl.DataFrame,
    integrity_summary_df: pl.DataFrame,
) -> bytes:
    with TemporaryDirectory() as tmp_dir:
        output_path = Path(tmp_dir) / workbook_name

        export_reconciliation_workbook(
            output_path=output_path,
            final_allocations_df=final_allocations_df,
            group_summary_df=group_summary_df,
            sku_variance_df=sku_variance_df,
            integrity_summary_df=integrity_summary_df,
        )

        return output_path.read_bytes()


def main() -> None:
    import streamlit as st

    st.set_page_config(
        page_title="Forecast Reconciler",
        page_icon="📊",
        layout="wide",
    )

    st.title("Forecast Reconciler")
    st.caption(
        "Align macro demand targets to granular SKU allocations while preserving proportional demand mix."
    )

    with st.sidebar:
        st.header("Run Configuration")

        quantity_mode = st.selectbox(
            "Quantity Mode",
            options=["integer", "decimal"],
            index=0,
        )

        quantity_decimals = st.number_input(
            "Quantity Decimals",
            min_value=0,
            max_value=6,
            value=0 if quantity_mode == "integer" else 2,
            step=1,
        )

        zero_baseline_mode = st.selectbox(
            "Zero Baseline Mode",
            options=["fail", "equal_split"],
            index=0,
        )

        enforce_exact_totals = st.checkbox(
            "Enforce Exact Totals",
            value=True,
        )

        allow_negative_allocations = st.checkbox(
            "Allow Negative Allocations",
            value=False,
        )

    macro_file = st.file_uploader(
        "Upload macro targets file",
        type=["csv", "xlsx", "xlsm"],
        key="macro_file",
    )

    granular_file = st.file_uploader(
        "Upload granular baseline file",
        type=["csv", "xlsx", "xlsm"],
        key="granular_file",
    )

    st.markdown(
        """
Expected canonical columns:

- Macro: `period`, `market`, `channel`, `macro_target_qty`
- Granular: `period`, `market`, `channel`, `sku`, `baseline_qty`
"""
    )

    if macro_file is None or granular_file is None:
        st.info("Upload both files to run the reconciliation pipeline.")
        return

    config = ReconciliationConfig(
        quantity_mode=quantity_mode,
        quantity_decimals=int(quantity_decimals),
        zero_baseline_mode=zero_baseline_mode,
        allow_negative_allocations=allow_negative_allocations,
        enforce_exact_totals=enforce_exact_totals,
    )

    try:
        macro_df = load_uploaded_table(macro_file, macro_file.name)
        granular_df = load_uploaded_table(granular_file, granular_file.name)
    except Exception as exc:
        st.error(f"Failed to read uploaded files: {exc}")
        return

    if st.button("Run Reconciliation", type="primary", use_container_width=True):
        try:
            result = run_reconciliation_pipeline(
                macro_df=macro_df,
                granular_df=granular_df,
                config=config,
            )
        except Exception as exc:
            st.error(f"Pipeline execution failed: {exc}")
            return

        is_valid = bool(result.integrity_summary.get_column("is_valid").item(0))
        if is_valid:
            st.success("Reconciliation completed successfully.")
        else:
            st.warning("Reconciliation completed with validation issues.")

        col1, col2, col3, col4 = st.columns(4)
        summary_row = result.integrity_summary.to_dicts()[0]

        col1.metric("Validated Groups", int(summary_row["validated_group_count"]))
        col2.metric("Groups With Gap", int(summary_row["groups_with_gap_count"]))
        col3.metric("Unmatched Macro Groups", int(summary_row["unmatched_macro_group_count"]))
        col4.metric(
            "Unmatched Granular Groups",
            int(summary_row["unmatched_granular_group_count"]),
        )

        tab1, tab2, tab3, tab4 = st.tabs(
            ["Final Allocations", "Group Summary", "SKU Variance", "Integrity Summary"]
        )

        with tab1:
            st.dataframe(result.final_allocations, use_container_width=True)

        with tab2:
            st.dataframe(result.group_summary, use_container_width=True)

        with tab3:
            st.dataframe(result.sku_variance, use_container_width=True)

        with tab4:
            st.dataframe(result.integrity_summary, use_container_width=True)

        st.download_button(
            label="Download Excel Workbook",
            data=BytesIO(result.workbook_bytes),
            file_name=result.workbook_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )


if __name__ == "__main__":
    main()