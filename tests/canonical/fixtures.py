from __future__ import annotations

from datetime import date, datetime, timezone

import pandas as pd

from fundlab.marketdata import (
    CanonicalMarketData,
    CoverageClaim,
    MarketTable,
    ObservationPayload,
    ProviderCapability,
    ProviderRequest,
    MarketDataWarehouse,
    SnapshotPlan,
    SourceSlice,
    UniverseScope,
    CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
)


DAYS = (date(2026, 7, 13), date(2026, 7, 14), date(2026, 7, 15), date(2026, 7, 16))


def fixture_universe_scope() -> UniverseScope:
    return UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        DAYS[-1],
        DAYS[0],
        DAYS[-1],
        survivorship_bias=True,
        instrument_ids=("600000.SH",),
    )


def market_frames(
    *, close_shift: float = 0.0, suspended_on: date | None = None,
    close_values: tuple[float, ...] | None = None,
):
    instruments = pd.DataFrame([{
        "instrument_id": "600000.SH",
        "exchange": "SH",
        "local_code": "600000",
        "asset_type": "stock",
        "name": "Fixture Bank",
        "currency": "CNY",
        "listed_date": "1999-11-10",
        "delisted_date": None,
        "board": "main",
        "buy_lot": 100,
        "quantity_step": 100,
        "odd_lot_sell_all": True,
        "price_tick": 0.01,
        "sell_delay_sessions": 1,
        "price_limit_ratio": 0.10,
        "source_payload": None,
    }])
    calendar = pd.DataFrame([
        {"exchange": "SH", "session_date": day.isoformat(), "is_open": True, "source_payload": None}
        for day in DAYS
    ])
    rows = []
    for index, day in enumerate(DAYS):
        raw_close = (
            10.0 + index + close_shift if close_values is None else float(close_values[index])
        )
        raw_previous = (9.5 + index if close_values is None else (
            raw_close * 0.95 if index == 0 else float(close_values[index - 1])
        ))
        suspended = day == suspended_on
        for mode, factor in (("raw", 1.0), ("adjusted", 0.8)):
            close = raw_close * factor
            rows.append({
                "instrument_id": "600000.SH",
                "session_date": day.isoformat(),
                "price_mode": mode,
                "open": close if not suspended else None,
                "high": close * 1.02 if not suspended else None,
                "low": close * 0.98 if not suspended else None,
                "close": close if not suspended else None,
                "volume": 1_000_000 if not suspended else 0,
                "amount": 10_000_000 if not suspended else 0,
                "suspended": suspended,
                "is_st": False,
                "trade_rule_id": "fixture-main-10pct-v1",
                "trade_rule_known_date": DAYS[0].isoformat(),
                "buy_lot": 100,
                "quantity_step": 100,
                "odd_lot_sell_all": True,
                "price_tick": 0.01,
                "sell_delay_sessions": 1,
                "price_limit_state": "bounded",
                "previous_close": raw_previous * factor,
                "price_limit_ratio": 0.10,
                "limit_up": round(raw_previous * factor * 1.1, 2),
                "limit_down": round(raw_previous * factor * 0.9, 2),
                "source_payload": None,
            })
    actions = pd.DataFrame(columns=(
        "action_id", "instrument_id", "action_type", "known_date", "record_date", "ex_date", "pay_date",
        "listing_date", "cash_per_share", "share_ratio", "rights_price", "source_payload",
    ))
    factors = pd.DataFrame(columns=(
        "factor_id", "instrument_id", "effective_date", "known_date", "price_multiplier",
        "source_payload",
    ))
    return {
        MarketTable.INSTRUMENTS: instruments,
        MarketTable.CALENDAR: calendar,
        MarketTable.DAILY_BARS: pd.DataFrame(rows),
        MarketTable.CORPORATE_ACTIONS: actions,
        MarketTable.ADJUSTMENT_FACTORS: factors,
    }


def observation(
    *, provider="fixture", complete=True, close_shift=0.0, observed_second=0,
    close_values: tuple[float, ...] | None = None,
):
    frames = market_frames(close_shift=close_shift, close_values=close_values)
    claims = tuple(
        CoverageClaim(
            table,
            complete,
            DAYS[0] if table is not MarketTable.INSTRUMENTS else None,
            DAYS[-1] if table is not MarketTable.INSTRUMENTS else None,
            ("600000.SH",) if table in {MarketTable.DAILY_BARS, MarketTable.CORPORATE_ACTIONS} else (),
        )
        for table in MarketTable
    )
    return ObservationPayload(
        provider,
        datetime(2026, 7, 17, 1, 2, observed_second, tzinfo=timezone.utc),
        ProviderRequest(ProviderCapability.DAILY_BARS_RAW, DAYS[0], DAYS[-1], ("600000.SH",)),
        frames,
        claims,
        {
            "fixture": True,
            "kind": "field_level_reconciliation",
            "reconciliation_ready": complete,
        },
    )


def ready_market(path, *, close_values: tuple[float, ...] | None = None):
    warehouse = MarketDataWarehouse(path)
    manifest = warehouse.record_observation(observation(close_values=close_values))
    snapshot = warehouse.build_snapshot(SnapshotPlan(
        tuple(SourceSlice(
            manifest.observation_id, table, "deterministic test fixture",
        ) for table in MarketTable),
        "deterministic test fixture",
        universe_scope=fixture_universe_scope(),
    ))
    warehouse.publish(snapshot.snapshot_id)
    return CanonicalMarketData.open(path)
