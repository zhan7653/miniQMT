from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from fundlab.marketdata import (
    TradeRuleError,
    apply_corroborated_historical_limit_exceptions,
    audit_provider_price_limits,
    materialize_daily_trade_rules,
)


def _instrument(
    instrument_id: str,
    *,
    asset_type: str = "stock",
    board: str = "main",
    listed_date: str = "2000-01-01",
) -> pd.DataFrame:
    local, exchange = instrument_id.split(".")
    return pd.DataFrame([{
        "instrument_id": instrument_id,
        "exchange": exchange,
        "local_code": local,
        "asset_type": asset_type,
        "board": board,
        "listed_date": listed_date,
        "buy_lot": 100,
        "price_tick": 0.001 if asset_type == "etf" else 0.01,
    }])


def _calendar(*days: str, exchange: str = "SH") -> pd.DataFrame:
    return pd.DataFrame([{
        "exchange": exchange, "session_date": day, "is_open": True,
    } for day in days])


def _bars(instrument_id: str, rows: tuple[tuple[str, bool, bool, float], ...]) -> pd.DataFrame:
    return pd.DataFrame([{
        "instrument_id": instrument_id,
        "session_date": day,
        "price_mode": "raw",
        "open": previous,
        "high": previous,
        "low": previous,
        "close": previous,
        "volume": 1000,
        "suspended": suspended,
        "is_st": is_st,
        "previous_close": previous,
        "field_lineage": "{}",
    } for day, suspended, is_st, previous in rows])


def test_main_board_st_rule_changes_on_2026_exchange_effective_date():
    days = ("2026-07-03", "2026-07-06")
    result = materialize_daily_trade_rules(
        _bars("600000.SH", tuple((day, False, True, 10.0) for day in days)),
        _instrument("600000.SH"),
        _calendar(*days),
    )

    assert list(result["price_limit_ratio"]) == [0.05, 0.10]
    assert list(result["limit_up"]) == [10.5, 11.0]
    assert list(result["trade_rule_id"]) == [
        "cn-stock-main-st-5pct-through-2026-07-05-v1",
        "cn-stock-main-st-10pct-from-2026-07-06-v1",
    ]
    assert result.iloc[1]["trade_rule_known_date"] == "2026-04-24"


def test_sse_unreformed_s_share_keeps_its_separate_five_percent_rule():
    days = ("2010-01-04", "2026-07-17")
    instrument = _instrument("600182.SH")
    instrument["name"] = "S佳通"
    result = materialize_daily_trade_rules(
        _bars("600182.SH", tuple((day, False, False, 13.3) for day in days)),
        instrument,
        _calendar(*days),
    )

    assert result["trade_rule_id"].tolist() == [
        "sse-unreformed-s-share-5pct-v1",
        "sse-unreformed-s-share-5pct-v1",
    ]
    assert result["limit_up"].tolist() == [13.97, 13.97]
    assert result["limit_down"].tolist() == [12.64, 12.64]


def test_provider_price_limit_audit_uses_two_independent_numeric_observations():
    day = "2026-07-17"
    derived = materialize_daily_trade_rules(
        _bars("600000.SH", ((day, False, False, 10.0),)),
        _instrument("600000.SH"),
        _calendar(day),
    )
    provider = pd.DataFrame([{
        "instrument_id": "600000.SH", "session_date": day, "price_mode": "raw",
        "high": 10.8, "low": 9.2, "limit_up": None, "limit_down": None,
        "_audit_observation_id": "obs-provider-a",
    }])

    audit = audit_provider_price_limits(
        derived,
        {"tickflow": provider, "xtquant": provider.copy()},
    )

    assert audit.minimum_provider_observations_per_session == 2
    assert audit.observed_price_bound_checks == 2
    assert audit.direct_limit_values == 0
    assert audit.source_observation_ids == ("obs-provider-a",)


def test_provider_price_limit_audit_blocks_an_upstream_price_outside_derived_rule():
    day = "2026-07-17"
    derived = materialize_daily_trade_rules(
        _bars("600000.SH", ((day, False, False, 10.0),)),
        _instrument("600000.SH"),
        _calendar(day),
    )
    valid = pd.DataFrame([{
        "instrument_id": "600000.SH", "session_date": day, "price_mode": "raw",
        "high": 10.8, "low": 9.2, "limit_up": None, "limit_down": None,
    }])
    invalid = valid.copy()
    invalid["high"] = 11.2

    with pytest.raises(TradeRuleError, match="needs two independent"):
        audit_provider_price_limits(
            derived,
            {"tickflow": valid, "xtquant": invalid},
        )

    audit = audit_provider_price_limits(
        derived,
        {"tickflow": valid, "xtquant": invalid, "baostock": valid.copy()},
    )
    assert audit.minimum_provider_observations_per_session == 2
    assert audit.conflicting_provider_rows == 1


def test_provider_price_limit_audit_requires_two_direct_latest_boundaries():
    day = "2026-07-17"
    derived = materialize_daily_trade_rules(
        _bars("600000.SH", ((day, False, False, 10.0),)),
        _instrument("600000.SH"),
        _calendar(day),
    )
    history = pd.DataFrame([{
        "instrument_id": "600000.SH", "session_date": day, "price_mode": "raw",
        "high": 10.8, "low": 9.2, "limit_up": None, "limit_down": None,
    }])
    direct = history.copy()
    direct[["high", "low"]] = None
    direct[["limit_up", "limit_down"]] = [11.0, 9.0]

    with pytest.raises(TradeRuleError, match="2 direct provider"):
        audit_provider_price_limits(
            derived,
            {"history-a": history, "history-b": history.copy(), "direct-a": direct},
            required_direct_limit_date=day,
        )

    single = audit_provider_price_limits(
        derived,
        {
            "history-a": history,
            "history-b": history.copy(),
            "direct-a": direct,
        },
        required_direct_limit_date=day,
        minimum_direct_limit_observations=1,
    )
    assert single.latest_direct_verified_sessions == 1
    assert single.minimum_direct_limit_observations_latest_session == 1

    audit = audit_provider_price_limits(
        derived,
        {
            "history-a": history,
            "history-b": history.copy(),
            "direct-a": direct,
            "direct-b": direct.copy(),
        },
        required_direct_limit_date=day,
    )
    assert audit.latest_bounded_sessions == 1
    assert audit.latest_direct_verified_sessions == 1
    assert audit.minimum_direct_limit_observations_latest_session == 2


def test_provider_price_limit_audit_reports_all_exact_local_impacts():
    days = ("2026-07-16", "2026-07-17")
    derived = materialize_daily_trade_rules(
        _bars("600000.SH", tuple((day, False, False, 10.0) for day in days)),
        _instrument("600000.SH"),
        _calendar(*days),
    )
    provider = pd.DataFrame([
        {
            "instrument_id": "600000.SH", "session_date": day,
            "price_mode": "raw", "high": 10.8, "low": 9.2,
            "limit_up": None, "limit_down": None,
        }
        for day in days
    ])

    with pytest.raises(TradeRuleError) as failure:
        audit_provider_price_limits(derived, {"only-one": provider})

    assert [(item.instrument_id, item.session_date) for item in failure.value.impacts] == [
        ("600000.SH", "2026-07-16"),
        ("600000.SH", "2026-07-17"),
    ]

    malformed = provider.drop(columns=["high"])
    with pytest.raises(TradeRuleError) as failure:
        audit_provider_price_limits(derived, {"malformed": malformed})
    assert failure.value.impacts == ()


def test_historical_limit_exception_requires_two_independent_boundary_observations():
    day = "2015-12-18"
    derived = materialize_daily_trade_rules(
        _bars("600000.SH", ((day, False, False, 2.5),)),
        _instrument("600000.SH"),
        _calendar(day),
    )
    exceptional = pd.DataFrame([{
        "instrument_id": "600000.SH", "session_date": day, "price_mode": "raw",
        "high": 20.0, "low": 16.0,
    }])

    unchanged, evidence = apply_corroborated_historical_limit_exceptions(
        derived, {"tickflow": exceptional},
    )
    assert unchanged.iloc[0]["price_limit_state"] == "bounded"
    assert evidence["exception_count"] == 0

    resolved, evidence = apply_corroborated_historical_limit_exceptions(
        derived, {"tickflow": exceptional, "xtquant": exceptional.copy()},
    )
    assert resolved.iloc[0]["price_limit_state"] == "unbounded"
    assert pd.isna(resolved.iloc[0]["limit_up"])
    assert evidence["exception_count"] == 1

    protected, evidence = apply_corroborated_historical_limit_exceptions(
        derived,
        {"tickflow": exceptional, "xtquant": exceptional.copy()},
        protected_dates=(day,),
    )
    assert protected.iloc[0]["price_limit_state"] == "bounded"
    assert evidence["exception_count"] == 0
    assert evidence["protected_dates"] == (day,)
    direct_exceptional = exceptional.assign(limit_up=2.75, limit_down=2.25)
    with pytest.raises(TradeRuleError, match="two independent provider observations"):
        audit_provider_price_limits(
            protected,
            {
                "tickflow": direct_exceptional,
                "xtquant": direct_exceptional.copy(),
            },
            required_direct_limit_date=day,
        )


def test_star_quantity_rule_separates_minimum_from_increment():
    result = materialize_daily_trade_rules(
        _bars("688001.SH", (("2026-07-17", False, False, 10.0),)),
        _instrument("688001.SH", board="star", listed_date="2019-07-22"),
        _calendar("2026-07-17"),
    )

    row = result.iloc[0]
    assert row["buy_lot"] == 200
    assert row["quantity_step"] == 1
    assert bool(row["odd_lot_sell_all"])


def test_registration_ipo_first_five_sessions_are_explicitly_unbounded():
    days = tuple(f"2023-04-{day:02d}" for day in range(10, 16))
    result = materialize_daily_trade_rules(
        _bars("600001.SH", tuple((day, False, False, 10.0) for day in days)),
        _instrument("600001.SH", listed_date="2023-04-10"),
        _calendar(*days),
    )

    assert list(result.iloc[:5]["price_limit_state"].unique()) == ["unbounded"]
    assert result.iloc[:5][["limit_up", "limit_down"]].isna().all().all()
    assert result.iloc[5]["price_limit_state"] == "bounded"
    assert result.iloc[5]["price_limit_ratio"] == 0.10


def test_suspended_session_has_state_but_never_a_fake_price():
    result = materialize_daily_trade_rules(
        _bars("600000.SH", (("2026-07-17", True, False, 10.0),)),
        _instrument("600000.SH"),
        _calendar("2026-07-17"),
    )

    assert result.iloc[0][["open", "high", "low", "close"]].isna().all()
    assert result.iloc[0]["volume"] == 0
    assert result.iloc[0]["trade_rule_id"] == "cn-stock-main-10pct-v1"


def test_initial_suspension_keeps_unknown_state_without_future_leakage():
    bars = _bars("600000.SH", (("2026-07-17", True, False, 10.0),))
    bars["is_st"] = pd.Series([pd.NA], dtype="boolean")
    bars["previous_close"] = pd.NA

    result = materialize_daily_trade_rules(
        bars,
        _instrument("600000.SH"),
        _calendar("2026-07-17"),
    )

    row = result.iloc[0]
    assert row["trade_rule_id"] == "cn-suspended-no-execution-v1"
    assert row["price_limit_state"] == "unknown"
    assert pd.isna(row["previous_close"])
    assert pd.isna(row["is_st"])
    assert row[["limit_up", "limit_down"]].isna().all()


def test_daily_rule_errors_expose_all_exact_execution_impacts():
    days = ("2026-07-16", "2026-07-17")
    bars = _bars("600000.SH", tuple((day, False, False, 10.0) for day in days))
    bars["is_st"] = pd.Series([pd.NA, pd.NA], dtype="boolean")

    with pytest.raises(TradeRuleError, match="ST state is unknown") as failure:
        materialize_daily_trade_rules(bars, _instrument("600000.SH"), _calendar(*days))

    assert [(item.instrument_id, item.session_date) for item in failure.value.impacts] == [
        ("600000.SH", "2026-07-16"),
        ("600000.SH", "2026-07-17"),
    ]

    bars = _bars("600000.SH", tuple((day, False, False, 10.0) for day in days))
    bars["previous_close"] = pd.NA
    with pytest.raises(TradeRuleError, match="Bounded rule has no previous_close") as failure:
        materialize_daily_trade_rules(bars, _instrument("600000.SH"), _calendar(*days))

    assert [(item.instrument_id, item.session_date) for item in failure.value.impacts] == [
        ("600000.SH", "2026-07-16"),
        ("600000.SH", "2026-07-17"),
    ]


def test_etf_dated_rule_gap_and_conflict_expose_exact_execution_impacts():
    days = ("2026-07-16", "2026-07-17")
    bars = _bars("513100.SH", tuple((day, False, False, 2.0) for day in days))
    instrument = _instrument("513100.SH", asset_type="etf", listed_date="2013-05-15")

    with pytest.raises(TradeRuleError, match="exactly one explicit dated rule") as failure:
        materialize_daily_trade_rules(bars, instrument, _calendar(*days))

    assert [(item.instrument_id, item.session_date) for item in failure.value.impacts] == [
        ("513100.SH", "2026-07-16"),
        ("513100.SH", "2026-07-17"),
    ]

    rules = pd.DataFrame([
        {
            "instrument_id": "513100.SH", "effective_from": date(2015, 1, 19),
            "effective_to": None, "known_date": date(2015, 1, 9),
            "price_limit_ratio": 0.10, "sell_delay_sessions": 0,
            "rule_id": "sse-cross-border-etf-t0-2015-v1", "evidence": "source-a",
        },
        {
            "instrument_id": "513100.SH", "effective_from": date(2020, 1, 1),
            "effective_to": None, "known_date": date(2019, 12, 20),
            "price_limit_ratio": 0.10, "sell_delay_sessions": 0,
            "rule_id": "sse-cross-border-etf-t0-2020-v1", "evidence": "source-b",
        },
    ])
    with pytest.raises(TradeRuleError, match="exactly one explicit dated rule") as failure:
        materialize_daily_trade_rules(bars, instrument, _calendar(*days), etf_rules=rules)

    assert [(item.instrument_id, item.session_date) for item in failure.value.impacts] == [
        ("513100.SH", "2026-07-16"),
        ("513100.SH", "2026-07-17"),
    ]


def test_untrusted_master_quantity_failure_remains_structural():
    instrument = _instrument("600000.SH")
    instrument["buy_lot"] = 0

    with pytest.raises(TradeRuleError, match="Invalid minimum buy quantity") as failure:
        materialize_daily_trade_rules(
            _bars("600000.SH", (("2026-07-17", False, False, 10.0),)),
            instrument,
            _calendar("2026-07-17"),
        )

    assert failure.value.impacts == ()
    assert failure.value.instrument_ids == ()

    with pytest.raises(TradeRuleError, match="Rule materialization has no instrument") as failure:
        materialize_daily_trade_rules(
            _bars("600000.SH", (("2026-07-17", False, False, 10.0),)),
            _instrument("600001.SH"),
            _calendar("2026-07-17"),
        )

    assert failure.value.impacts == ()
    assert failure.value.instrument_ids == ()


def test_unsupported_asset_type_remains_a_structural_trade_rule_failure():
    with pytest.raises(TradeRuleError, match="Unsupported rule asset type") as failure:
        materialize_daily_trade_rules(
            _bars("600000.SH", (("2026-07-17", False, False, 10.0),)),
            _instrument("600000.SH", asset_type="bond"),
            _calendar("2026-07-17"),
        )

    assert failure.value.impacts == ()
    assert failure.value.instrument_ids == ()


def test_etf_rule_is_never_inferred_from_name_or_code():
    bars = _bars("513100.SH", (("2026-07-17", False, False, 2.0),))
    instrument = _instrument("513100.SH", asset_type="etf", listed_date="2013-05-15")
    with pytest.raises(TradeRuleError, match="exactly one explicit dated rule"):
        materialize_daily_trade_rules(
            bars, instrument, _calendar("2026-07-17"),
        )

    rule = pd.DataFrame([{
        "instrument_id": "513100.SH",
        "effective_from": date(2015, 1, 19),
        "effective_to": None,
        "known_date": date(2015, 1, 9),
        "price_limit_ratio": 0.10,
        "sell_delay_sessions": 0,
        "rule_id": "sse-cross-border-etf-t0-2015-v1",
        "evidence": "SSE/SZSE cross-border ETF T+0 rule",
    }])
    result = materialize_daily_trade_rules(
        bars, instrument, _calendar("2026-07-17"), etf_rules=rule,
    )
    assert result.iloc[0]["sell_delay_sessions"] == 0
    assert result.iloc[0]["price_limit_ratio"] == 0.10
    assert result.iloc[0]["price_tick"] == 0.001
