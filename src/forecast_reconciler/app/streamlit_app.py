from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import BinaryIO, Literal

import pandas as pd
import polars as pl

from forecast_reconciler.app.planning import (
    ReconciliationBasis,
    align_to_macro_groups,
    build_dual_metric_outputs,
    build_lock_template,
    build_share_based_targets,
    normalise_market_column,
    prepare_direct_targets,
    prepare_granular_reference,
)
from forecast_reconciler.config import ReconciliationConfig
from forecast_reconciler.io.writers import (
    export_reconciliation_csv_zip,
    export_reconciliation_workbook,
)
from forecast_reconciler.normalisation.standardise import (
    standardise_granular_input,
    standardise_macro_input,
)
from forecast_reconciler.reconciliation.allocator import redistribute_macro_targets
from forecast_reconciler.reconciliation.rounding import apply_deterministic_rounding
from forecast_reconciler.reconciliation.weights import calculate_weights
from forecast_reconciler.validation.integrity import validate_reconciliation_integrity


OutputFormat = Literal["xlsx", "zip_csv"]


@dataclass(frozen=True, slots=True)
class PipelineRunResult:
    final_allocations: pl.DataFrame
    group_summary: pl.DataFrame
    sku_variance: pl.DataFrame
    integrity_summary: pl.DataFrame
    export_bytes: bytes
    export_name: str
    export_mime_type: str


def load_uploaded_table(file_obj: BinaryIO, file_name: str) -> pl.DataFrame:
    """
    Load an uploaded CSV or XLSX file into a Polars DataFrame.

    sku is always forced to string to support alphanumeric values.
    """
    suffix = Path(file_name).suffix.lower()

    schema_overrides = {
        "period": pl.Utf8,
        "market": pl.Utf8,
        "dc_mercado": pl.Utf8,
        "channel": pl.Utf8,
        "client": pl.Utf8,
        "sku": pl.Utf8,
    }

    if suffix == ".csv":
        return pl.read_csv(
            file_obj,
            schema_overrides=schema_overrides,
            infer_schema_length=10000,
        )

    if suffix in {".xlsx", ".xlsm"}:
        return pl.read_excel(
            file_obj,
            schema_overrides=schema_overrides,
        )

    raise ValueError(
        f"Unsupported input file format '{suffix}'. Supported formats are .csv and .xlsx/.xlsm."
    )


def build_export_filename(output_format: OutputFormat) -> str:
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    if output_format == "xlsx":
        return f"forecast_reconciliation_{timestamp}.xlsx"

    return f"forecast_reconciliation_{timestamp}.zip"


def _build_export_bytes(
    export_name: str,
    output_format: OutputFormat,
    final_allocations_df: pl.DataFrame,
    group_summary_df: pl.DataFrame,
    sku_variance_df: pl.DataFrame,
    integrity_summary_df: pl.DataFrame,
    progress_callback=None,
) -> tuple[bytes, str]:
    with TemporaryDirectory() as tmp_dir:
        output_path = Path(tmp_dir) / export_name

        if output_format == "xlsx":
            export_reconciliation_workbook(
                output_path=output_path,
                final_allocations_df=final_allocations_df,
                group_summary_df=group_summary_df,
                sku_variance_df=sku_variance_df,
                integrity_summary_df=integrity_summary_df,
                progress_callback=progress_callback,
            )
            mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        else:
            export_reconciliation_csv_zip(
                output_path=output_path,
                final_allocations_df=final_allocations_df,
                group_summary_df=group_summary_df,
                sku_variance_df=sku_variance_df,
                integrity_summary_df=integrity_summary_df,
                progress_callback=progress_callback,
            )
            mime_type = "application/zip"

        return output_path.read_bytes(), mime_type


def _driver_macro_for_engine(
    macro_targets_df: pl.DataFrame,
    basis: ReconciliationBasis,
) -> pl.DataFrame:
    """
    Convert dual-metric macro targets into the canonical engine macro schema.

    The core reconciliation engine always expects:
    - period
    - market
    - channel
    - macro_target_qty

    In value basis, macro_target_value becomes the engine driver metric and is
    aliased into macro_target_qty for the engine contract.
    """
    required_keys = {"period", "market", "channel"}
    missing_keys = [col for col in sorted(required_keys) if col not in macro_targets_df.columns]
    if missing_keys:
        raise ValueError(
            "Macro targets are missing required key columns for engine conversion: "
            f"{', '.join(missing_keys)}."
        )

    if basis == "units":
        if "macro_target_qty" not in macro_targets_df.columns:
            raise ValueError("Macro targets must contain 'macro_target_qty' for Units mode.")

        engine_macro = macro_targets_df.select(
            ["period", "market", "channel", "macro_target_qty"]
        )
    else:
        if "macro_target_value" not in macro_targets_df.columns:
            raise ValueError("Macro targets must contain 'macro_target_value' for Value mode.")

        engine_macro = macro_targets_df.select(
            [
                "period",
                "market",
                "channel",
                pl.col("macro_target_value").alias("macro_target_qty"),
            ]
        )

    return standardise_macro_input(engine_macro, ReconciliationConfig())


def _driver_granular_for_engine(
    granular_reference_df: pl.DataFrame,
    basis: ReconciliationBasis,
) -> pl.DataFrame:
    """
    Convert dual-metric granular reference data into the canonical engine schema.

    The core reconciliation engine always expects:
    - period
    - market
    - channel
    - client
    - sku
    - baseline_qty

    In value basis, baseline_value becomes the engine driver metric and is
    aliased into baseline_qty for the engine contract.
    """
    required = {"period", "market", "channel", "client", "sku"}
    missing = [col for col in sorted(required) if col not in granular_reference_df.columns]
    if missing:
        raise ValueError(
            "Granular reference is missing required key columns for engine conversion: "
            f"{', '.join(missing)}."
        )

    if basis == "units":
        if "baseline_qty" not in granular_reference_df.columns:
            raise ValueError("Granular reference must contain 'baseline_qty' for Units mode.")

        engine_granular = granular_reference_df.select(
            ["period", "market", "channel", "client", "sku", "baseline_qty"]
        )
    else:
        if "baseline_value" not in granular_reference_df.columns:
            raise ValueError("Granular reference must contain 'baseline_value' for Value mode.")

        engine_granular = granular_reference_df.select(
            [
                "period",
                "market",
                "channel",
                "client",
                "sku",
                pl.col("baseline_value").alias("baseline_qty"),
            ]
        )

    return standardise_granular_input(engine_granular, ReconciliationConfig())


def _prepare_macro_targets(
    macro_df: pl.DataFrame,
    granular_df: pl.DataFrame,
    basis: ReconciliationBasis,
    macro_input_mode: str,
    share_target: float | None,
    lock_df: pl.DataFrame | None,
) -> pl.DataFrame:
    macro_df = normalise_market_column(macro_df)

    if macro_input_mode == "Share-Based Targets":
        if share_target is None:
            raise ValueError("Share-Based Targets mode requires a market share value.")

        return build_share_based_targets(
            macro_df=macro_df,
            granular_df=granular_df,
            share_target=share_target,
            basis=basis,
            lock_df=lock_df,
        )

    return prepare_direct_targets(macro_df)


def _run_pipeline_core(
    macro_df: pl.DataFrame,
    granular_df: pl.DataFrame,
    config: ReconciliationConfig,
    basis: ReconciliationBasis,
    macro_input_mode: str,
    share_target: float | None,
    lock_df: pl.DataFrame | None,
    step_callback=None,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Execute the reconciliation pipeline and return:
    - final_allocations_df
    - group_summary_df
    - sku_variance_df
    - integrity_summary_df
    """
    if step_callback:
        step_callback("Preparing macro and granular inputs")

    granular_reference_df = prepare_granular_reference(granular_df)

    macro_targets_df = _prepare_macro_targets(
        macro_df=macro_df,
        granular_df=granular_df,
        basis=basis,
        macro_input_mode=macro_input_mode,
        share_target=share_target,
        lock_df=lock_df,
    )

    if step_callback:
        step_callback("Standardising driver metric")

    engine_macro_df = _driver_macro_for_engine(
        macro_targets_df=macro_targets_df,
        basis=basis,
    )
    engine_granular_df = _driver_granular_for_engine(
        granular_reference_df=granular_reference_df,
        basis=basis,
    )

    if step_callback:
        step_callback("Aligning and weighting granular rows")

    engine_granular_df = align_to_macro_groups(
        macro_df=engine_macro_df,
        granular_df=engine_granular_df,
    )
    granular_reference_df = align_to_macro_groups(
        macro_df=engine_macro_df,
        granular_df=granular_reference_df,
    )

    weight_result = calculate_weights(
        granular_df=engine_granular_df,
        config=config,
    )

    if step_callback:
        step_callback("Redistributing and rounding")

    redistribution_result = redistribute_macro_targets(
        macro_df=engine_macro_df,
        weighted_granular_df=weight_result.weights,
        config=config,
    )

    rounding_result = apply_deterministic_rounding(
        allocation_df=redistribution_result.allocations,
        config=config,
    )

    if step_callback:
        step_callback("Validating and reconstructing both metrics")

    integrity_result = validate_reconciliation_integrity(
        rounded_allocations_df=rounding_result.allocations,
        unmatched_macro_groups=redistribution_result.unmatched_macro_groups,
        unmatched_granular_groups=redistribution_result.unmatched_granular_groups,
        config=config,
    )

    final_allocations_df, group_summary_df, sku_variance_df = build_dual_metric_outputs(
        engine_allocations_df=rounding_result.allocations,
        macro_targets_df=macro_targets_df,
        granular_reference_df=granular_reference_df,
        basis=basis,
    )

    return (
        final_allocations_df,
        group_summary_df,
        sku_variance_df,
        integrity_result.summary,
    )


def run_reconciliation_pipeline(
    macro_df: pl.DataFrame,
    granular_df: pl.DataFrame,
    config: ReconciliationConfig,
    output_format: OutputFormat,
    basis: ReconciliationBasis,
    macro_input_mode: str,
    share_target: float | None,
    lock_df: pl.DataFrame | None,
) -> PipelineRunResult:
    (
        final_allocations_df,
        group_summary_df,
        sku_variance_df,
        integrity_summary_df,
    ) = _run_pipeline_core(
        macro_df=macro_df,
        granular_df=granular_df,
        config=config,
        basis=basis,
        macro_input_mode=macro_input_mode,
        share_target=share_target,
        lock_df=lock_df,
    )

    export_name = build_export_filename(output_format)
    export_bytes, export_mime_type = _build_export_bytes(
        export_name=export_name,
        output_format=output_format,
        final_allocations_df=final_allocations_df,
        group_summary_df=group_summary_df,
        sku_variance_df=sku_variance_df,
        integrity_summary_df=integrity_summary_df,
    )

    return PipelineRunResult(
        final_allocations=final_allocations_df,
        group_summary=group_summary_df,
        sku_variance=sku_variance_df,
        integrity_summary=integrity_summary_df,
        export_bytes=export_bytes,
        export_name=export_name,
        export_mime_type=export_mime_type,
    )


def run_reconciliation_pipeline_with_status(
    macro_df: pl.DataFrame,
    granular_df: pl.DataFrame,
    config: ReconciliationConfig,
    status_container,
    output_format: OutputFormat,
    basis: ReconciliationBasis,
    macro_input_mode: str,
    share_target: float | None,
    lock_df: pl.DataFrame | None,
) -> PipelineRunResult:
    step_messages = [
        "Step 1/5 — Preparing macro and granular inputs",
        "Step 2/5 — Standardising driver metric",
        "Step 3/5 — Aligning and weighting granular rows",
        "Step 4/5 — Redistributing and rounding",
        "Step 5/5 — Validating and reconstructing both metrics",
    ]
    step_index = {"value": 0}

    def step_callback(_: str) -> None:
        current = step_messages[step_index["value"]]
        status_container.update(label=current, state="running")
        step_index["value"] += 1

    (
        final_allocations_df,
        group_summary_df,
        sku_variance_df,
        integrity_summary_df,
    ) = _run_pipeline_core(
        macro_df=macro_df,
        granular_df=granular_df,
        config=config,
        basis=basis,
        macro_input_mode=macro_input_mode,
        share_target=share_target,
        lock_df=lock_df,
        step_callback=step_callback,
    )

    def export_progress(message: str) -> None:
        status_container.update(label=f"Step 6/6 — {message}", state="running")

    export_name = build_export_filename(output_format)
    export_bytes, export_mime_type = _build_export_bytes(
        export_name=export_name,
        output_format=output_format,
        final_allocations_df=final_allocations_df,
        group_summary_df=group_summary_df,
        sku_variance_df=sku_variance_df,
        integrity_summary_df=integrity_summary_df,
        progress_callback=export_progress,
    )

    status_container.update(label="Pipeline completed successfully", state="complete")

    return PipelineRunResult(
        final_allocations=final_allocations_df,
        group_summary=group_summary_df,
        sku_variance=sku_variance_df,
        integrity_summary=integrity_summary_df,
        export_bytes=export_bytes,
        export_name=export_name,
        export_mime_type=export_mime_type,
    )


def main() -> None:
    import streamlit as st

    st.set_page_config(
        page_title="Forecast Reconciler",
        page_icon="📊",
        layout="wide",
    )

    st.title("Forecast Reconciler")
    st.caption(
        "Annual-share-aware reconciliation with month locking from granular totals, "
        "dual qty/value outputs, string SKU support, and null-market handling."
    )

    with st.sidebar:
        st.header("Run Configuration")

        basis_label = st.selectbox(
            "Reconciliation Basis",
            options=["Units", "Value"],
            index=0,
        )
        basis: ReconciliationBasis = "units" if basis_label == "Units" else "value"

        macro_input_mode = st.selectbox(
            "Macro Input Mode",
            options=["Direct Macro Targets", "Share-Based Targets"],
            index=0,
        )

        share_target = None
        if macro_input_mode == "Share-Based Targets":
            share_target = (
                st.number_input(
                    "Annual Market Share Target (%)",
                    min_value=0.0,
                    max_value=100.0,
                    value=9.2,
                    step=0.1,
                )
                / 100.0
            )

        output_format_label = st.radio(
            "Output Format",
            options=[
                "CSV / ZIP (recommended for large outputs)",
                "Excel (.xlsx)",
            ],
            index=0,
        )
        output_format: OutputFormat = (
            "zip_csv"
            if output_format_label == "CSV / ZIP (recommended for large outputs)"
            else "xlsx"
        )

        st.caption(
            "Excel export is much slower for large datasets. CSV/ZIP is recommended when "
            "final allocations or SKU variance are large."
        )

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
        "Upload macro file",
        type=["csv", "xlsx", "xlsm"],
        key="macro_file",
    )
    granular_file = st.file_uploader(
        "Upload granular file",
        type=["csv", "xlsx", "xlsm"],
        key="granular_file",
    )

    if macro_input_mode == "Direct Macro Targets":
        st.markdown(
            """
Expected macro columns:
- `period`
- `market` or `dc_mercado`
- `channel`
- `macro_target_qty`
- `macro_target_value`

Expected granular columns:
- `period`
- `market` or `dc_mercado`
- `channel`
- `client`
- `sku`
- `baseline_qty`
- `baseline_value`
"""
        )
    else:
        st.markdown(
            """
Expected macro columns:
- `period`
- `market` or `dc_mercado`
- `channel`
- `total_market_qty`
- `total_market_value`

Expected granular columns:
- `period`
- `market` or `dc_mercado`
- `channel`
- `client`
- `sku`
- `baseline_qty`
- `baseline_value`
"""
        )

    if basis == "units":
        st.caption(
            "Units mode enforces the unit target. Locked months are sourced from granular monthly totals. "
            "Value is reconstructed using `final_allocated_value = final_allocated_qty × unit_price`."
        )
    else:
        st.caption(
            "Value mode enforces the value target. Locked months are sourced from granular monthly totals. "
            "Units are reconstructed using `final_allocated_qty = final_allocated_value / unit_price`."
        )

    if macro_file is None or granular_file is None:
        st.info("Upload both files to continue.")
        return

    try:
        macro_df = load_uploaded_table(macro_file, macro_file.name)
        granular_df = load_uploaded_table(granular_file, granular_file.name)
    except Exception as exc:
        st.error(f"Failed to read uploaded files: {exc}")
        return

    lock_df_pl: pl.DataFrame | None = None

    if macro_input_mode == "Share-Based Targets":
        macro_preview = normalise_market_column(macro_df)
        available_periods = sorted(
            macro_preview.get_column("period").cast(pl.Utf8).str.slice(0, 10).unique().to_list()
        )

        locked_periods = st.multiselect(
            "Locked Months",
            options=available_periods,
            help=(
                "Locked months keep their absolute monthly totals sourced from the granular table. "
                "Only unlocked months absorb the remaining annual target."
            ),
        )

        if locked_periods:
            lock_template = build_lock_template(
                granular_df=granular_df,
                basis=basis,
                locked_periods=locked_periods,
            )

            st.markdown("### Locked Target Editor")
            st.caption(
                "These values are prefilled from the granular monthly totals. "
                "Editing them overrides the granular-derived lock for the selected month."
            )

            lock_editor_df = st.data_editor(
                lock_template.to_pandas(),
                num_rows="fixed",
                use_container_width=True,
                hide_index=True,
                column_config={
                    "is_locked": st.column_config.CheckboxColumn(required=True),
                    "locked_target": st.column_config.NumberColumn(required=False),
                },
                key="lock_editor",
            )

            lock_df_pl = pl.from_pandas(pd.DataFrame(lock_editor_df))

    config = ReconciliationConfig(
        quantity_mode=quantity_mode,
        quantity_decimals=int(quantity_decimals),
        zero_baseline_mode=zero_baseline_mode,
        allow_negative_allocations=allow_negative_allocations,
        enforce_exact_totals=enforce_exact_totals,
    )

    if st.button("Run Reconciliation", type="primary", use_container_width=True):
        try:
            with st.status("Starting reconciliation pipeline...", expanded=True) as status:
                result = run_reconciliation_pipeline_with_status(
                    macro_df=macro_df,
                    granular_df=granular_df,
                    config=config,
                    status_container=status,
                    output_format=output_format,
                    basis=basis,
                    macro_input_mode=macro_input_mode,
                    share_target=share_target,
                    lock_df=lock_df_pl,
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
        col4.metric("Unmatched Granular Groups", int(summary_row["unmatched_granular_group_count"]))

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
            label="Download Output",
            data=BytesIO(result.export_bytes),
            file_name=result.export_name,
            mime=result.export_mime_type,
            use_container_width=True,
        )


if __name__ == "__main__":
    main()