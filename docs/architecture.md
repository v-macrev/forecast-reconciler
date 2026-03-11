# Forecast Reconciler Architecture

## Purpose

Forecast Reconciler is a planning-oriented Python platform that aligns macro demand targets to granular SKU-level allocations while preserving proportional baseline mix and enforcing exact control totals at the configured output precision.

The platform is designed to model a realistic business planning workflow in which:

- macro targets are defined at a control grain such as `period + market + channel`
- operational planning occurs at a lower grain such as `period + market + channel + sku`
- the system must redistribute volume without breaking arithmetic integrity or mix realism

## Core Design Principles

The architecture follows five principles:

1. **Deterministic reconciliation**
   Given the same inputs and configuration, the platform produces the same output.

2. **Separation of concerns**
   Input validation, reconciliation logic, reporting, export, and UI orchestration are kept as separate layers.

3. **Auditability**
   The system preserves enough intermediate information to explain how outputs were produced.

4. **Fail-fast controls**
   Structural and semantic data issues are surfaced early.

5. **Business-usable outputs**
   Results are available as dataframes, integrity summaries, and Excel workbook artefacts.

## End-to-End Flow

The platform follows this sequence:

```text
Raw Macro Input
Raw Granular Input
        ↓
Schema Validation
        ↓
Date Harmonisation
        ↓
Input Standardisation
        ↓
Weight Calculation
        ↓
Macro Redistribution
        ↓
Deterministic Rounding + Residual Correction
        ↓
Integrity Validation
        ↓
Reporting Views
        ↓
Excel Export / Optional Streamlit UI
````

## Layer Breakdown

### 1. Configuration Layer

Core module(s):

* `forecast_reconciler.config`
* `forecast_reconciler.types`
* `forecast_reconciler.exceptions`

Responsibilities:

* define canonical column semantics
* define reconciliation behaviour
* expose typed configuration for quantity mode, zero-baseline handling, and control-grain logic
* provide domain-specific exceptions

### 2. Normalisation Layer

Core module(s):

* `forecast_reconciler.normalisation.schemas`
* `forecast_reconciler.normalisation.dates`
* `forecast_reconciler.normalisation.standardise`

Responsibilities:

* validate required columns
* enforce structural non-nullability for keys
* harmonise period values to canonical monthly dates
* coerce quantity columns into numeric form
* reject duplicate business keys

Output guarantee:

* standardised macro dataframe
* standardised granular dataframe

Both are suitable for reconciliation processing.

### 3. Reconciliation Layer

Core module(s):

* `forecast_reconciler.reconciliation.weights`
* `forecast_reconciler.reconciliation.allocator`
* `forecast_reconciler.reconciliation.rounding`

Responsibilities:

* compute group baseline totals
* compute proportional SKU weights
* join macro targets to weighted granular rows
* produce raw allocated quantities
* round allocations to the configured precision
* correct residual gaps deterministically

Output guarantee:

* final granular allocations match the macro target at the configured output precision

### 4. Validation Layer

Core module(s):

* `forecast_reconciler.validation.integrity`

Responsibilities:

* validate group-level control totals
* surface unmatched macro groups
* surface unmatched granular groups
* detect negative allocations
* produce run-level integrity summaries

Output guarantee:

* explicit validity state for the run

### 5. Reporting Layer

Core module(s):

* `forecast_reconciler.reporting.summaries`

Responsibilities:

* create group-level reconciliation summaries
* create SKU-level variance views
* expose planner-friendly comparison artefacts

Output guarantee:

* business-readable analytical views derived from final allocations

### 6. Export Layer

Core module(s):

* `forecast_reconciler.io.writers`

Responsibilities:

* write deterministic Excel workbook outputs
* preserve stable sheet naming
* serialise planning artefacts for business review

Workbook sheets:

* `final_allocations`
* `group_summary`
* `sku_variance`
* `integrity_summary`

### 7. UI Layer

Core module(s):

* `forecast_reconciler.app.streamlit_app`

Responsibilities:

* accept uploaded files
* trigger pipeline execution
* preview outputs
* provide workbook download

Architectural principle:

* the UI orchestrates the engine but does not reimplement business logic

## Control Grain

The default control grain is:

* `period`
* `market`
* `channel`

This is configurable through `ReconciliationConfig.group_keys`.

The control grain defines:

* how macro targets are grouped
* how granular baseline totals are computed
* where exact reconciliation equality is enforced

## Quantity Precision Model

The platform supports:

* `integer` mode
* `decimal` mode with configurable decimal places

Rounding control is enforced at the configured precision, not at arbitrary source precision. This means a source macro target may carry more decimal precision than the operational output can preserve.

## Zero-Baseline Behaviour

Two modes are supported:

* `fail`
* `equal_split`

In `fail` mode, any control group with zero baseline total causes reconciliation failure.

In `equal_split` mode, zero-baseline groups receive uniform weights across participating granular rows.

## Testing Strategy

The repository uses layered automated testing:

* unit tests for each processing component
* edge-case tests for zero baselines, precision, nulls, duplicates, and mismatches
* end-to-end integration testing across the full pipeline

## Repository Signal

This project is intentionally designed to signal strength in:

* forecasting systems design
* supply chain planning logic
* analytics engineering discipline
* deterministic business-rule implementation
* validation-driven development