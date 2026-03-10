from forecast_reconciler.types import (
    CANONICAL_GRANULAR_COLUMNS,
    CANONICAL_MACRO_COLUMNS,
    DEFAULT_BASELINE_QTY_COL,
    DEFAULT_CHANNEL_COL,
    DEFAULT_MACRO_TARGET_QTY_COL,
    DEFAULT_MARKET_COL,
    DEFAULT_PERIOD_COL,
    DEFAULT_SKU_COL,
)


def test_default_column_names_are_stable():
    assert DEFAULT_PERIOD_COL == "period"
    assert DEFAULT_MARKET_COL == "market"
    assert DEFAULT_CHANNEL_COL == "channel"
    assert DEFAULT_SKU_COL == "sku"
    assert DEFAULT_MACRO_TARGET_QTY_COL == "macro_target_qty"
    assert DEFAULT_BASELINE_QTY_COL == "baseline_qty"


def test_canonical_macro_columns_are_defined():
    assert CANONICAL_MACRO_COLUMNS == (
        "period",
        "market",
        "channel",
        "macro_target_qty",
    )


def test_canonical_granular_columns_are_defined():
    assert CANONICAL_GRANULAR_COLUMNS == (
        "period",
        "market",
        "channel",
        "sku",
        "baseline_qty",
    )