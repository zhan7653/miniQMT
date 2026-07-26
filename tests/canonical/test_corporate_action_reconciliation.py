from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from fundlab.common.canonical import canonical_json
from fundlab.marketdata import (
    CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
    SnapshotNotReadyError,
    UniverseScope,
    reconcile_corporate_action_factors,
)


def _scope():
    return UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        date(2026, 7, 17),
        date(2020, 1, 1),
        date(2026, 7, 17),
        survivorship_bias=True,
        instrument_ids=("510230.SH",),
    )


def _instruments():
    return pd.DataFrame([{
        "instrument_id": "510230.SH",
        "listed_date": "2011-05-23",
        "delisted_date": None,
    }])


def _actions():
    common = {
        "instrument_id": "510230.SH",
        "pay_date": None,
        "cash_per_share": None,
        "share_ratio": None,
        "rights_price": None,
        "source_provider": "eastmoney-fund-public",
        "source_observation_id": "obs-actions",
        "source_payload": "action-payload",
    }
    return pd.DataFrame([
        {
            **common,
            "action_id": "cash",
            "action_type": "cash_dividend",
            "known_date": "2025-06-19",
            "record_date": "2025-06-23",
            "ex_date": "2025-06-24",
            "pay_date": "2025-06-27",
            "listing_date": None,
            "cash_per_share": 0.0668,
            "quantity_multiplier": None,
        },
        {
            **common,
            "action_id": "split",
            "action_type": "split",
            "known_date": "2020-08-05",
            "record_date": "2020-08-13",
            "ex_date": "2020-08-17",
            "listing_date": "2020-08-17",
            "quantity_multiplier": 5.0,
        },
    ])


def _factors():
    return pd.DataFrame([
        {
            "factor_id": "factor-split",
            "instrument_id": "510230.SH",
            "effective_date": "2020-08-17",
            "known_date": "2020-08-17",
            "price_multiplier": 0.2,
            "source_provider": "xtquant",
            "source_observation_id": "obs-factors",
            "field_lineage": None,
            "source_payload": canonical_json({
                "raw": {
                    "interest": 0,
                    "stockBonus": 4,
                    "stockGift": 0,
                    "allotNum": 0,
                    "allotPrice": 0,
                },
            }),
        },
        {
            "factor_id": "factor-cash",
            "instrument_id": "510230.SH",
            "effective_date": "2025-06-24",
            "known_date": "2025-06-24",
            "price_multiplier": 0.95,
            "source_provider": "xtquant",
            "source_observation_id": "obs-factors",
            "field_lineage": None,
            "source_payload": canonical_json({
                "raw": {
                    "interest": 0.0668,
                    "stockBonus": 0,
                    "stockGift": 0,
                    "allotNum": 0,
                    "allotPrice": 0,
                },
            }),
        },
    ])


def test_actions_and_factors_are_bidirectionally_complete_and_known_at_announcement():
    result = reconcile_corporate_action_factors(
        instruments=_instruments(),
        actions=_actions(),
        factors=_factors(),
        universe_scope=_scope(),
    )

    known = result.factors.set_index("effective_date")["known_date"].to_dict()
    assert known == {
        "2020-08-17": "2020-08-05",
        "2025-06-24": "2025-06-19",
    }
    assert result.report["action_count"] == 2
    assert len(result.evidence_hash) == 64


def test_factor_without_action_blocks_simulation_readiness():
    with pytest.raises(SnapshotNotReadyError, match="coverage mismatch"):
        reconcile_corporate_action_factors(
            instruments=_instruments(),
            actions=_actions().iloc[:1],
            factors=_factors(),
            universe_scope=_scope(),
        )


def test_non_position_factor_is_retained_without_becoming_a_holding_action():
    factors = pd.concat((
        _factors(),
        pd.DataFrame([{
            **_factors().iloc[0].to_dict(),
            "factor_id": pd.NA,
            "effective_date": "2024-01-02",
            "known_date": "2024-01-02",
            "price_multiplier": 1.0,
            "source_payload": canonical_json({
                "raw": {
                    "interest": 0,
                    "stockBonus": 0,
                    "stockGift": 0,
                    "allotNum": 0,
                    "allotPrice": 0,
                    "dr": 1,
                },
            }),
        }]),
    ), ignore_index=True)

    result = reconcile_corporate_action_factors(
        instruments=_instruments(),
        actions=_actions(),
        factors=factors,
        universe_scope=_scope(),
    )

    assert len(result.factors) == 3
    assert result.report["technical_factor_events"] == (
        ("510230.SH", "2024-01-02"),
    )
    assert len(result.evidence_hash) == 64


def test_material_factor_before_public_listing_is_outside_holding_lifecycle():
    instruments = _instruments().copy()
    instruments.loc[:, "listed_date"] = "2024-01-02"
    factor = _factors().iloc[[0]].copy()
    factor.loc[:, "effective_date"] = "2023-12-29"

    result = reconcile_corporate_action_factors(
        instruments=instruments,
        actions=_actions().iloc[:0],
        factors=factor,
        universe_scope=_scope(),
    )

    assert result.factors.empty
    assert result.report["out_of_lifecycle_factor_events"] == (
        ("510230.SH", "2023-12-29"),
    )


def test_unique_economic_factor_match_may_use_a_nearby_raw_date():
    factors = _factors().copy()
    factors.loc[
        factors["effective_date"].eq("2020-08-17"), "effective_date"
    ] = "2020-08-15"

    result = reconcile_corporate_action_factors(
        instruments=_instruments(),
        actions=_actions(),
        factors=factors,
        universe_scope=_scope(),
    )

    split = result.factors.loc[result.factors["factor_id"].eq("factor-split")].iloc[0]
    assert split["effective_date"] == "2020-08-17"
    assert result.report["verified_events"][0]["raw_factor_effective_date"] == (
        "2020-08-15"
    )


def test_unverified_restructuring_capitalization_is_not_applied_to_all_holders():
    action = _actions().loc[_actions()["action_type"].eq("split")].copy()
    action.loc[:, "action_type"] = "stock_dividend"
    action.loc[:, "share_ratio"] = 1.15
    action.loc[:, "quantity_multiplier"] = float("nan")
    action.loc[:, "source_payload"] = canonical_json({
        "raw": {"分红类型": "重整转增", "实施方案分红说明": "10转增11.5股"},
    })

    result = reconcile_corporate_action_factors(
        instruments=_instruments(),
        actions=action,
        factors=_factors().iloc[:0],
        universe_scope=_scope(),
    )

    assert result.actions.empty
    assert result.report["ignored_nonholder_restructuring_candidates"] == (
        ("510230.SH", "2020-08-17"),
    )


def test_rights_entitlement_may_exceed_market_wide_actual_allotment():
    action = pd.DataFrame([{
        "action_id": "rights",
        "instrument_id": "510230.SH",
        "action_type": "rights_issue",
        "known_date": "2022-01-01",
        "record_date": "2022-01-04",
        "ex_date": "2022-01-13",
        "pay_date": "2022-01-11",
        "listing_date": "2022-01-28",
        "cash_per_share": None,
        "share_ratio": 0.3,
        "rights_price": 3.2,
        "quantity_multiplier": None,
        "field_lineage": None,
        "source_provider": "cninfo-public",
        "source_observation_id": "obs-actions",
        "source_payload": "official-rights-entitlement",
    }])
    factor = pd.DataFrame([{
        **_factors().iloc[0].to_dict(),
        "factor_id": "factor-rights",
        "effective_date": "2022-01-13",
        "known_date": "2022-01-13",
        "source_payload": canonical_json({
            "raw": {
                "interest": 0,
                "stockBonus": 0,
                "stockGift": 0,
                "allotNum": 0.28462,
                "allotPrice": 3.2,
            },
        }),
    }])

    result = reconcile_corporate_action_factors(
        instruments=_instruments(),
        actions=action,
        factors=factor,
        universe_scope=_scope(),
    )

    assert result.actions.iloc[0]["share_ratio"] == pytest.approx(0.3)


def test_official_action_can_be_recovered_by_independent_price_factor():
    action = _actions().loc[_actions()["action_type"].eq("split")].copy()
    secondary = _factors().iloc[[0]].copy()
    secondary.loc[:, "source_provider"] = "baostock"
    secondary.loc[:, "source_observation_id"] = "obs-bao-factor"

    result = reconcile_corporate_action_factors(
        instruments=_instruments(),
        actions=action,
        factors=_factors().iloc[:0],
        corroborating_factors=secondary,
        universe_scope=_scope(),
    )

    assert len(result.actions) == len(result.factors) == 1
    assert result.factors.iloc[0]["source_provider"] == (
        "canonical-action-factor-r2"
    )
    assert len(result.report["corroboration"]["recovered_official_actions"]) == 1


def test_dual_factor_economics_can_recover_conservative_holding_action():
    instruments = _instruments().assign(asset_type="etf")
    primary = _factors().iloc[[0]].copy()
    secondary = primary.copy()
    secondary.loc[:, "source_provider"] = "tencent-public"
    secondary.loc[:, "source_observation_id"] = "obs-tencent-factor"

    result = reconcile_corporate_action_factors(
        instruments=instruments,
        actions=_actions().iloc[:0],
        factors=primary,
        corroborating_factors=secondary,
        universe_scope=_scope(),
    )

    action = result.actions.iloc[0]
    assert action["action_type"] == "split"
    assert action["quantity_multiplier"] == pytest.approx(5.0)
    assert action["known_date"] == action["ex_date"] == "2020-08-17"
    assert result.report["corroboration"]["synthesized_actions"] == (
        action["action_id"],
    )


def test_delayed_secondary_factor_keeps_primary_market_effect_date():
    primary = _factors().iloc[[0]].copy()
    secondary = primary.copy()
    secondary.loc[:, "effective_date"] = "2020-09-30"
    secondary.loc[:, "known_date"] = "2020-09-30"
    secondary.loc[:, "source_provider"] = "baostock"
    secondary.loc[:, "source_observation_id"] = "obs-bao-delayed-factor"

    empty_actions = _actions().iloc[:0].copy()
    empty_actions["field_lineage"] = pd.Series(dtype="string")
    result = reconcile_corporate_action_factors(
        instruments=_instruments().assign(asset_type="stock"),
        actions=empty_actions,
        factors=primary,
        corroborating_factors=secondary,
        universe_scope=_scope(),
    )

    action = result.actions.iloc[0]
    assert action["ex_date"] == primary.iloc[0]["effective_date"]
    assert "2020-09-30" in action["field_lineage"]


def test_official_share_ratio_wins_when_two_price_factors_agree_within_three_percent():
    action = _actions().loc[_actions()["action_type"].eq("split")].copy()
    action.loc[:, "action_type"] = "stock_dividend"
    action.loc[:, "share_ratio"] = 0.5
    action.loc[:, "quantity_multiplier"] = float("nan")
    action.loc[:, "source_payload"] = "official-10-transfer-5"
    primary = _factors().iloc[[0]].copy()
    primary.loc[:, "price_multiplier"] = 0.647379924
    primary.loc[:, "source_payload"] = canonical_json({
        "raw": {
            "interest": 0.0,
            "stockBonus": 0.0,
            "stockGift": 0.493253,
            "allotNum": 0.0,
            "allotPrice": 0.0,
        },
    })
    secondary = primary.copy()
    secondary.loc[:, "price_multiplier"] = 0.6484716341
    secondary.loc[:, "source_provider"] = "baostock"
    secondary.loc[:, "source_observation_id"] = "obs-bao-factor"

    result = reconcile_corporate_action_factors(
        instruments=_instruments(),
        actions=action,
        factors=primary,
        corroborating_factors=secondary,
        universe_scope=_scope(),
    )

    assert result.actions.iloc[0]["share_ratio"] == pytest.approx(0.5)
    assert result.factors.iloc[0]["price_multiplier"] == pytest.approx(0.6484716341)
    quarantined = result.report["corroboration"]["quarantined_primary_factors"]
    assert quarantined[0]["reason"] == (
        "conflicts_with_official_action_confirmed_by_secondary_factor"
    )


def test_audit_factor_recovers_official_cash_total_when_primary_is_strict_subset():
    action = _actions().loc[_actions()["action_type"].eq("cash_dividend")].copy()
    action.loc[:, "cash_per_share"] = 0.35
    primary = _factors().loc[_factors()["factor_id"].eq("factor-cash")].copy()
    primary.loc[:, "price_multiplier"] = 0.9811828748
    primary.loc[:, "source_payload"] = canonical_json({
        "raw": {
            "interest": 0.14,
            "stockBonus": 0,
            "stockGift": 0,
            "allotNum": 0,
            "allotPrice": 0,
        },
    })
    bao = primary.copy()
    bao.loc[:, "price_multiplier"] = 0.9532085182
    bao.loc[:, "source_provider"] = "baostock"
    bao.loc[:, "source_observation_id"] = "obs-bao"
    daily = pd.DataFrame([{
        "instrument_id": "510230.SH",
        "session_date": "2025-06-24",
        "previous_close": 7.13,
    }])

    result = reconcile_corporate_action_factors(
        instruments=_instruments().assign(asset_type="stock"),
        actions=action,
        factors=primary,
        corroborating_factors=bao,
        daily_bars=daily,
        universe_scope=_scope(),
    )

    assert result.actions.iloc[0]["cash_per_share"] == pytest.approx(0.35)
    assert result.factors.iloc[0]["price_multiplier"] == pytest.approx(
        0.9532085182
    )
    assert result.report["corroboration"]["recovered_official_actions"]


def test_complete_secondary_audit_quarantines_uncorroborated_material_factor():
    result = reconcile_corporate_action_factors(
        instruments=_instruments().assign(asset_type="etf"),
        actions=_actions().iloc[:0],
        factors=_factors().iloc[[0]],
        corroborating_factors=_factors().iloc[:0],
        universe_scope=_scope(),
    )

    assert result.actions.empty and result.factors.empty
    quarantined = result.report["corroboration"]["quarantined_primary_factors"]
    assert quarantined[0]["reason"] == (
        "material_primary_factor_not_independently_corroborated"
    )


def test_nonzero_factor_economics_cannot_hide_behind_a_different_action_type():
    actions = _actions().loc[_actions()["action_type"].eq("split")].copy()
    factors = _factors().loc[_factors()["effective_date"].eq("2020-08-17")].copy()
    payload = {
        "raw": {
            "interest": 0.12,
            "stockBonus": 4,
            "stockGift": 0,
            "allotNum": 0.3,
            "allotPrice": 2.0,
        },
    }
    factors.loc[:, "source_payload"] = canonical_json(payload)

    with pytest.raises(SnapshotNotReadyError, match="cash_per_share.*rights_ratio"):
        reconcile_corporate_action_factors(
            instruments=_instruments(),
            actions=actions,
            factors=factors,
            universe_scope=_scope(),
        )


def test_missing_noncritical_cash_pay_date_is_explicitly_completed_after_factor_check():
    actions = _actions().copy()
    actions.loc[actions["action_type"].eq("cash_dividend"), "pay_date"] = None

    result = reconcile_corporate_action_factors(
        instruments=_instruments(),
        actions=actions,
        factors=_factors(),
        universe_scope=_scope(),
    )

    cash = result.actions.loc[
        result.actions["action_type"].eq("cash_dividend")
    ].iloc[0]
    assert cash["pay_date"] == cash["ex_date"] == "2025-06-24"
    assert result.report["derived_cash_pay_date_action_ids"] == ("cash",)
    assert "cash_pay_date_completion_r2" in cash["field_lineage"]


def test_pre_ex_share_credit_date_is_completed_to_first_execution_safe_session():
    action = pd.DataFrame([{
        "action_id": "stock-distribution",
        "instrument_id": "510230.SH",
        "action_type": "stock_dividend",
        "known_date": "2024-05-01",
        "record_date": "2024-05-09",
        "ex_date": "2024-05-10",
        "pay_date": None,
        "listing_date": "2024-05-09",
        "cash_per_share": None,
        "share_ratio": 0.5,
        "rights_price": None,
        "quantity_multiplier": None,
        "field_lineage": None,
        "source_provider": "cninfo-public",
        "source_observation_id": "obs-actions",
        "source_payload": "official-credit-date-payload",
    }])
    factor = pd.DataFrame([{
        "factor_id": "factor-stock-distribution",
        "instrument_id": "510230.SH",
        "effective_date": "2024-05-10",
        "known_date": "2024-05-10",
        "price_multiplier": 2 / 3,
        "source_provider": "xtquant",
        "source_observation_id": "obs-factors",
        "field_lineage": None,
        "source_payload": canonical_json({
            "raw": {
                "interest": 0,
                "stockBonus": 0.5,
                "stockGift": 0,
                "allotNum": 0,
                "allotPrice": 0,
            },
        }),
    }])

    result = reconcile_corporate_action_factors(
        instruments=_instruments(),
        actions=action,
        factors=factor,
        universe_scope=_scope(),
    )

    normalized = result.actions.iloc[0]
    assert normalized["listing_date"] == "2024-05-10"
    assert result.report["derived_share_listing_date_action_ids"] == (
        "stock-distribution",
    )
    assert "share_listing_date_completion_r2" in normalized["field_lineage"]
