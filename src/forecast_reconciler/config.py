from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from forecast_reconciler.types import (
    DEFAULT_BASELINE_QTY_COL,
    DEFAULT_CHANNEL_COL,
    DEFAULT_MACRO_TARGET_QTY_COL,
    DEFAULT_MARKET_COL,
    DEFAULT_PERIOD_COL,
    DEFAULT_SKU_COL,
    ColumnName,
    GroupKey,
)

ZeroBaselineMode = Literal["fail", "equal_split"]
QuantityMode = Literal["integer", "decimal"]


@dataclass(frozen=True, slots=True)
class InputColumnConfig:
    period_col: ColumnName = DEFAULT_PERIOD_COL
    market_col: ColumnName = DEFAULT_MARKET_COL
    channel_col: ColumnName = DEFAULT_CHANNEL_COL
    sku_col: ColumnName = DEFAULT_SKU_COL
    macro_target_qty_col: ColumnName = DEFAULT_MACRO_TARGET_QTY_COL
    baseline_qty_col: ColumnName = DEFAULT_BASELINE_QTY_COL


@dataclass(frozen=True, slots=True)
class ReconciliationConfig:
    group_keys: tuple[GroupKey, ...] = field(
        default_factory=lambda: (
            DEFAULT_PERIOD_COL,
            DEFAULT_MARKET_COL,
            DEFAULT_CHANNEL_COL,
        )
    )
    quantity_mode: QuantityMode = "integer"
    quantity_decimals: int = 0
    zero_baseline_mode: ZeroBaselineMode = "fail"
    allow_negative_allocations: bool = False
    enforce_exact_totals: bool = True
    columns: InputColumnConfig = field(default_factory=InputColumnConfig)

    def __post_init__(self) -> None:
        if not self.group_keys:
            raise ValueError("group_keys must contain at least one grouping column.")

        if len(set(self.group_keys)) != len(self.group_keys):
            raise ValueError("group_keys must not contain duplicate columns.")

        if self.quantity_mode not in {"integer", "decimal"}:
            raise ValueError("quantity_mode must be either 'integer' or 'decimal'.")

        if self.quantity_decimals < 0:
            raise ValueError("quantity_decimals must be greater than or equal to zero.")

        if self.quantity_mode == "integer" and self.quantity_decimals != 0:
            raise ValueError(
                "quantity_decimals must be 0 when quantity_mode is 'integer'."
            )

        if self.zero_baseline_mode not in {"fail", "equal_split"}:
            raise ValueError(
                "zero_baseline_mode must be either 'fail' or 'equal_split'."
            )

    @property
    def macro_required_columns(self) -> tuple[ColumnName, ...]:
        return (*self.group_keys, self.columns.macro_target_qty_col)

    @property
    def granular_required_columns(self) -> tuple[ColumnName, ...]:
        granular_group_keys = tuple(
            key for key in self.group_keys if key != self.columns.sku_col
        )

        return (
            *granular_group_keys,
            self.columns.sku_col,
            self.columns.baseline_qty_col,
        )