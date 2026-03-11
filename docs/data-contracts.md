# Data Contracts

## Overview

Forecast Reconciler operates on two primary input datasets:

- **macro targets**
- **granular baseline**

The current implementation assumes a monthly planning model and canonical column naming.

The normalisation layer can harmonise date representations, but the semantic contract of the columns must still be respected.

## Macro Input Contract

### Expected Grain

One row per reconciliation control group.

Default control group:

- `period`
- `market`
- `channel`

### Required Columns

| Column | Type | Description |
|---|---|---|
| `period` | string/date-like | Planning period. Normalised to first day of month internally. |
| `market` | string | Market or equivalent planning group. |
| `channel` | string | Channel or equivalent planning group. |
| `macro_target_qty` | numeric-like | Macro target quantity to be redistributed. |

### Rules

- required columns must exist
- grouping columns must not be null
- duplicate business keys are not allowed
- quantity must be coercible to numeric
- the dataset must represent one macro target per control group

### Example

| period | market | channel | macro_target_qty |
|---|---|---|---:|
| 2026-01 | SP | Retail | 120 |
| 2026-01 | RJ | Retail | 50 |

## Granular Input Contract

### Expected Grain

One row per granular item within a reconciliation control group.

Default grain:

- `period`
- `market`
- `channel`
- `sku`

### Required Columns

| Column | Type | Description |
|---|---|---|
| `period` | string/date-like | Planning period. Normalised to first day of month internally. |
| `market` | string | Market or equivalent planning group. |
| `channel` | string | Channel or equivalent planning group. |
| `sku` | string | Granular item identifier. |
| `baseline_qty` | numeric-like | Baseline granular quantity used to derive weights. |

### Rules

- required columns must exist
- grouping columns and SKU must not be null
- duplicate business keys are not allowed
- quantity must be coercible to numeric
- the dataset must represent one baseline value per granular row

### Example

| period | market | channel | sku | baseline_qty |
|---|---|---|---|---:|
| 2026-01 | SP | Retail | SKU-001 | 60 |
| 2026-01 | SP | Retail | SKU-002 | 40 |

## Period Handling

The platform accepts these input period formats:

- `%Y-%m`
- `%Y-%m-%d`
- `%Y/%m`
- `%Y/%m/%d`
- `%d/%m/%Y`

All accepted period values are normalised internally to:

- Polars `Date`
- first day of the month

Example:

- `2026-01`
- `2026-01-31`
- `15/01/2026`

All become:

- `2026-01-01`

## Numeric Quantity Handling

Quantity columns are standardised to `Float64`.

Accepted quantity forms include:

- integers
- floats
- numeric strings
- strings with comma thousands separators such as `1,250.5`

Rejected forms include:

- null quantity values
- empty strings
- boolean values
- non-numeric strings

## Duplicate Business Keys

The platform rejects duplicates at canonical business grain.

### Macro duplicate rule

Duplicates are evaluated on:

- `group_keys`

Default:

- `period + market + channel`

### Granular duplicate rule

Duplicates are evaluated on:

- `group_keys + sku`

unless `sku` is already included in `group_keys`.

## Output Contracts

The pipeline produces four principal output artefacts.

### 1. Final Allocations

Granular reconciliation output including:

- baseline quantities
- computed weights
- macro targets
- raw allocated quantities
- rounded allocations
- residual adjustments
- final allocated quantities

### 2. Group Summary

Control-group comparison including:

- macro target
- baseline total
- final allocated total
- group deltas
- target gap

### 3. SKU Variance

Granular comparison including:

- baseline quantity
- final quantity
- baseline weight
- final weight
- quantity deltas
- weight deltas

### 4. Integrity Summary

Run-level validation output including:

- validated group count
- groups with gap count
- negative allocation count
- unmatched macro group count
- unmatched granular group count
- final validity flag

## Configurable Contract Elements

The following behaviours are configurable through `ReconciliationConfig`:

- `group_keys`
- `quantity_mode`
- `quantity_decimals`
- `zero_baseline_mode`
- `allow_negative_allocations`
- `enforce_exact_totals`

These options alter behaviour without changing the core data model.