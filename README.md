# Forecast Reconciler

Production-grade algorithmic forecast reconciler for aligning macro targets to granular SKU-level forecasts while preserving proportional demand mix and planning realism.

## Overview

This project models a common planning and supply chain analytics problem:

- a business defines **macro targets** at a higher aggregation level such as market, channel, or brand
- operational execution happens at a **granular level** such as SKU or presentation
- the system must redistribute the macro target downwards without breaking the underlying product mix logic

The reconciler is designed to:

- ingest macro and granular forecast inputs
- normalise planning data into canonical schemas
- calculate granular weights from baseline demand
- allocate macro targets proportionally to granular items
- preserve realistic seasonality and mix shape
- validate control totals exactly
- generate auditable business-facing outputs
- optionally expose a lightweight Streamlit interface

## Core Reconciliation Principle

For each planning control group, the reconciled granular allocation follows:

```text
New_Granular_Qty = Macro_Target × (Original_Granular_Qty / Total_Original_Market_Qty)
````

This ensures that the total output matches the macro target while preserving the relative contribution of each granular item inside the group.

## Target Use Cases

Typical users include:

* demand planners
* supply chain analysts
* S&OP support teams
* analytics engineers building planning workflows
* finance or commercial planning stakeholders requiring auditability

## Planned Capabilities

* strict input normalisation
* monthly period harmonisation
* group-based weighting and redistribution
* deterministic rounding with residual control
* validation and exception reporting
* Excel-oriented export outputs
* optional Streamlit execution layer

## Proposed Stack

* **Python** for core engineering and portability
* **Polars** for vectorised tabular processing
* **NumPy** for supporting numerical utilities
* **OpenPyXL** for Excel workbook generation
* **PyTest** for incremental automated testing
* **Streamlit** as an optional business-facing layer

## Repository Structure

```text
forecast-reconciler/
├─ data/
├─ docs/
├─ notebooks/
├─ src/
│  └─ forecast_reconciler/
├─ tests/
├─ pyproject.toml
├─ README.md
└─ LICENSE
```

## Development Setup

Create a virtual environment and install the project with development dependencies.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

On Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
```

## Running Tests

```bash
pytest
```