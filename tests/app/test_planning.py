import polars as pl

from forecast_reconciler.app.planning import (
    NULL_MARKET_SENTINEL,
    align_to_macro_groups,
    build_share_based_targets,
    normalise_market_column,
    prepare_direct_targets,
    prepare_granular_reference,
)


def test_normalise_market_column_supports_dc_mercado_and_nulls():
    df = pl.DataFrame(
        {
            "period": ["2026-01-01"],
            "dc_mercado": [None],
            "channel": ["Retail"],
        }
    )

    result = normalise_market_column(df)

    assert result.columns == ["period", "market", "channel"]
    assert result.to_dicts() == [
        {
            "period": "2026-01-01",
            "market": NULL_MARKET_SENTINEL,
            "channel": "Retail",
        }
    ]


def test_prepare_direct_targets_preserves_both_macro_target_columns():
    df = pl.DataFrame(
        {
            "period": ["2026-01-01"],
            "market": ["GEN"],
            "channel": ["Retail"],
            "macro_target_qty": [100.0],
            "macro_target_value": [1000.0],
        }
    )

    result = prepare_direct_targets(df)

    assert result.to_dicts() == [
        {
            "period": "2026-01-01",
            "market": "GEN",
            "channel": "Retail",
            "macro_target_qty": 100.0,
            "macro_target_value": 1000.0,
        }
    ]


def test_prepare_granular_reference_keeps_sku_as_string_and_builds_unit_price():
    df = pl.DataFrame(
        {
            "period": ["2026-01-01"],
            "market": ["GEN"],
            "channel": ["Retail"],
            "client": ["Client A"],
            "sku": ["EL-JOLE181020241202617"],
            "baseline_qty": [10.0],
            "baseline_value": [150.0],
        }
    )

    result = prepare_granular_reference(df)

    assert result.get_column("sku").to_list() == ["EL-JOLE181020241202617"]
    assert result.get_column("unit_price").to_list() == [15.0]


def test_build_share_based_targets_respects_locked_rows_and_annual_target():
    macro_df = pl.DataFrame(
        {
            "period": ["2026-01-01", "2026-02-01", "2026-03-01"],
            "market": ["GEN", "GEN", "GEN"],
            "channel": ["Retail", "Retail", "Retail"],
            "total_market_qty": [100.0, 100.0, 100.0],
            "total_market_value": [1000.0, 1000.0, 1000.0],
        }
    )

    lock_df = pl.DataFrame(
        {
            "period": ["2026-01-01"],
            "market": ["GEN"],
            "channel": ["Retail"],
            "is_locked": [True],
            "locked_target": [50.0],
        }
    )

    result = build_share_based_targets(
        macro_df=macro_df,
        share_target=0.20,
        basis="units",
        lock_df=lock_df,
    ).sort("period")

    # annual qty target = 300 * 20% = 60
    # Jan locked at 50, remaining 10 goes across Feb/Mar equally
    assert result.select(["period", "macro_target_qty"]).to_dicts() == [
        {"period": "2026-01-01", "macro_target_qty": 50.0},
        {"period": "2026-02-01", "macro_target_qty": 5.0},
        {"period": "2026-03-01", "macro_target_qty": 5.0},
    ]


def test_align_to_macro_groups_keeps_only_supported_period_market_channel():
    macro_df = pl.DataFrame(
        {
            "period": ["2026-01-01"],
            "market": ["GEN"],
            "channel": ["Retail"],
        }
    )

    granular_df = pl.DataFrame(
        {
            "period": ["2026-01-01", "2026-02-01"],
            "market": ["GEN", "GEN"],
            "channel": ["Retail", "Retail"],
            "client": ["A", "A"],
            "sku": ["1", "1"],
            "baseline_qty": [10.0, 20.0],
            "baseline_value": [100.0, 200.0],
        }
    )

    result = align_to_macro_groups(macro_df=macro_df, granular_df=granular_df)

    assert result.height == 1
    assert result.to_dicts()[0]["period"] == "2026-01-01"