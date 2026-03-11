# Reconciliation Rules

## Purpose

This document defines the business and mathematical rules used by Forecast Reconciler to align macro targets to granular SKU-level outputs.

The goal is to preserve the baseline mix signal while guaranteeing exact control totals at the configured output precision.

## Core Allocation Rule

For each control group:

```text
New_Granular_Qty = Macro_Target × (Original_Granular_Qty / Total_Original_Group_Qty)
````

Where:

* `Macro_Target` is the group-level target quantity
* `Original_Granular_Qty` is the baseline quantity for the SKU
* `Total_Original_Group_Qty` is the sum of all baseline quantities in the control group

This creates a proportional redistribution of the macro target across granular rows.

## Weight Calculation Rule

The platform first computes a granular weight for each SKU:

```text
weight = baseline_qty / group_baseline_qty
```

Where:

* `baseline_qty` is the granular baseline quantity
* `group_baseline_qty` is the sum of baseline quantities inside the control group

The group baseline total is repeated on each row for auditability.

## Redistribution Rule

After weights are computed, the macro target is joined onto granular rows using the configured control grain.

The raw allocation becomes:

```text
raw_allocated_qty = macro_target_qty × weight
```

At this stage, the sum of raw allocations per group equals the macro target subject only to floating-point representation noise.

## Rounding Rule

Operational outputs usually require integer or fixed-decimal quantities. Therefore raw allocations must be rounded to the configured quantity precision.

The platform uses this rule:

1. scale quantities to the configured precision
2. floor each row-level allocation
3. compute the residual units still needed to match the rounded macro control total
4. assign residual units to the rows with the largest fractional remainders
5. break ties deterministically using SKU order

This is a largest-remainder correction strategy.

## Exact-Total Rule

The final allocated total must equal the macro control total at the configured output precision.

Examples:

* in integer mode, final SKU allocations must sum to the integer macro target
* in decimal mode with 2 decimals, final SKU allocations must sum to the cent-rounded macro target

This rule is enforced during rounding and verified again during integrity validation.

## Zero-Baseline Rule

A zero-baseline group occurs when:

```text
group_baseline_qty = 0
```

In this case, proportional weights are undefined.

Supported behaviours:

### `fail`

The run fails for the affected group because proportional redistribution cannot be computed safely.

### `equal_split`

The affected group receives equal weights across all granular rows in the group.

Example with 3 SKUs:

```text
weight = 1 / 3
```

This mode is useful as a controlled fallback but should be used intentionally.

## Unmatched Group Rule

Two mismatch cases are tracked.

### Unmatched macro groups

A macro target group exists without any corresponding granular support.

Effect:

* the group cannot be allocated
* the mismatch is surfaced in validation

### Unmatched granular groups

A granular group exists without any corresponding macro target.

Effect:

* the group is excluded from allocation
* the mismatch is surfaced in validation

## Negative Allocation Rule

By default, final allocated quantities must not be negative.

If `allow_negative_allocations=False`, any negative final allocation invalidates the run.

A configuration option exists to allow negative allocations in specialised scenarios, but the default planning assumption is non-negative output.

## Validation Rule

A run is valid only when all of the following hold:

* each group final total matches its macro target within configured precision tolerance
* no disallowed negative allocations exist
* no unmatched macro groups exist
* no unmatched granular groups exist

If `enforce_exact_totals=True`, validation failure raises an exception.

If `enforce_exact_totals=False`, validation returns an invalid result object without raising.

## Reporting Rule

The reporting layer exposes both group-level and SKU-level change views.

### Group-level reporting includes

* macro target
* baseline group quantity
* final allocated group quantity
* group delta quantity
* group delta percent
* final gap to target

### SKU-level reporting includes

* baseline quantity
* final allocated quantity
* baseline weight
* final weight
* SKU delta quantity
* SKU delta percent
* weight delta

This allows a reviewer to understand not only whether the reconciliation matched totals, but also how the mix changed.

## Practical Interpretation

The reconciler preserves **shape**, not absolute history.

That means:

* if the macro target increases, each SKU grows proportionally unless rounding changes the smallest details
* if the macro target decreases, each SKU shrinks proportionally unless rounding changes the smallest details
* if the group baseline is zero, proportional allocation is impossible and a fallback rule is required

In short: the platform preserves the baseline distribution logic while enforcing the new macro control total.