from __future__ import annotations

from typing import TypeAlias

ColumnName: TypeAlias = str
GroupKey: TypeAlias = str
SkuId: TypeAlias = str
PeriodValue: TypeAlias = str
Quantity: TypeAlias = float


DEFAULT_PERIOD_COL: ColumnName = "period"
DEFAULT_MARKET_COL: ColumnName = "market"
DEFAULT_CHANNEL_COL: ColumnName = "channel"
DEFAULT_SKU_COL: ColumnName = "sku"
DEFAULT_MACRO_TARGET_QTY_COL: ColumnName = "macro_target_qty"
DEFAULT_BASELINE_QTY_COL: ColumnName = "baseline_qty"


CANONICAL_MACRO_COLUMNS: tuple[ColumnName, ...] = (
    DEFAULT_PERIOD_COL,
    DEFAULT_MARKET_COL,
    DEFAULT_CHANNEL_COL,
    DEFAULT_MACRO_TARGET_QTY_COL,
)

CANONICAL_GRANULAR_COLUMNS: tuple[ColumnName, ...] = (
    DEFAULT_PERIOD_COL,
    DEFAULT_MARKET_COL,
    DEFAULT_CHANNEL_COL,
    DEFAULT_SKU_COL,
    DEFAULT_BASELINE_QTY_COL,
)