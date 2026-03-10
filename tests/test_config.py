from forecast_reconciler.config import InputColumnConfig, ReconciliationConfig


def test_default_reconciliation_config_is_valid():
    config = ReconciliationConfig()

    assert config.group_keys == ("period", "market", "channel")
    assert config.quantity_mode == "integer"
    assert config.quantity_decimals == 0
    assert config.zero_baseline_mode == "fail"
    assert config.allow_negative_allocations is False
    assert config.enforce_exact_totals is True


def test_custom_reconciliation_config_is_valid():
    columns = InputColumnConfig(
        period_col="plan_month",
        market_col="region",
        channel_col="segment",
        sku_col="product_code",
        macro_target_qty_col="target_units",
        baseline_qty_col="baseline_units",
    )

    config = ReconciliationConfig(
        group_keys=("plan_month", "region", "segment"),
        quantity_mode="decimal",
        quantity_decimals=2,
        zero_baseline_mode="equal_split",
        columns=columns,
    )

    assert config.group_keys == ("plan_month", "region", "segment")
    assert config.quantity_mode == "decimal"
    assert config.quantity_decimals == 2
    assert config.zero_baseline_mode == "equal_split"
    assert config.columns.sku_col == "product_code"
    assert config.columns.macro_target_qty_col == "target_units"


def test_config_rejects_empty_group_keys():
    try:
        ReconciliationConfig(group_keys=())
    except ValueError as exc:
        assert str(exc) == "group_keys must contain at least one grouping column."
    else:
        raise AssertionError("Expected ValueError for empty group_keys.")


def test_config_rejects_duplicate_group_keys():
    try:
        ReconciliationConfig(group_keys=("period", "market", "market"))
    except ValueError as exc:
        assert str(exc) == "group_keys must not contain duplicate columns."
    else:
        raise AssertionError("Expected ValueError for duplicate group_keys.")


def test_config_rejects_negative_quantity_decimals():
    try:
        ReconciliationConfig(quantity_mode="decimal", quantity_decimals=-1)
    except ValueError as exc:
        assert str(exc) == "quantity_decimals must be greater than or equal to zero."
    else:
        raise AssertionError("Expected ValueError for negative quantity_decimals.")


def test_config_rejects_integer_mode_with_non_zero_decimals():
    try:
        ReconciliationConfig(quantity_mode="integer", quantity_decimals=2)
    except ValueError as exc:
        assert str(exc) == "quantity_decimals must be 0 when quantity_mode is 'integer'."
    else:
        raise AssertionError(
            "Expected ValueError when integer quantity mode uses decimal places."
        )


def test_macro_required_columns_follow_group_keys():
    config = ReconciliationConfig()

    assert config.macro_required_columns == (
        "period",
        "market",
        "channel",
        "macro_target_qty",
    )


def test_granular_required_columns_include_sku_and_baseline():
    config = ReconciliationConfig()

    assert config.granular_required_columns == (
        "period",
        "market",
        "channel",
        "sku",
        "baseline_qty",
    )


def test_granular_required_columns_do_not_duplicate_sku_when_in_group_keys():
    config = ReconciliationConfig(group_keys=("period", "market", "sku"))

    assert config.granular_required_columns == (
        "period",
        "market",
        "sku",
        "baseline_qty",
    )