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
    ReadinessProfile,
    SnapshotPlan,
    SourceSlice,
    StoredFile,
    UniverseScope,
    CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
)
from fundlab.marketdata.incremental import IncrementalCanonicalPublisher
from fundlab.marketdata.schema import (
    TABLE_DATE_COLUMNS,
    TABLE_INSTRUMENT_COLUMNS,
    validate_snapshot_tables,
)


DAYS = (date(2026, 7, 13), date(2026, 7, 14), date(2026, 7, 15), date(2026, 7, 16))
# Exchange-announced future sessions past the data head.  The canonical calendar
# carries them so a close-of-head intent can schedule its T+1 order; bars exist
# only for DAYS.
FUTURE_DAYS = (date(2026, 7, 17), date(2026, 7, 20))


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
        for day in (*DAYS, *FUTURE_DAYS)
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
            (
                None if table is MarketTable.INSTRUMENTS
                else FUTURE_DAYS[-1] if table is MarketTable.CALENDAR
                else DAYS[-1]
            ),
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


def commit_test_snapshot(
    warehouse: MarketDataWarehouse,
    plan: SnapshotPlan,
):
    """Create an immutable legacy fixture without reopening a production API."""

    tables: dict[MarketTable, pd.DataFrame] = {}
    files: list[StoredFile] = []
    seen: set[tuple[str, MarketTable]] = set()
    for table in MarketTable:
        selections = [item for item in plan.selections if item.table is table]
        if not selections:
            continue
        pieces = []
        for selection in selections:
            frame = warehouse.read_observation_table(selection.observation_id, table)
            instrument_column = TABLE_INSTRUMENT_COLUMNS[table]
            if selection.instrument_ids and instrument_column:
                frame = frame.loc[
                    frame[instrument_column].astype(str).isin(selection.instrument_ids)
                ]
            date_column = TABLE_DATE_COLUMNS[table]
            if selection.start_date is not None and date_column:
                frame = frame.loc[
                    frame[date_column].astype(str).between(
                        selection.start_date.isoformat(), selection.end_date.isoformat(),
                    )
                ]
            frame = frame.copy()
            frame["source_observation_id"] = selection.observation_id
            pieces.append(frame)
            key = (selection.observation_id, table)
            if key not in seen:
                manifest = warehouse.load_observation(selection.observation_id)
                stored = next(item for item in manifest.files if item.table is table)
                files.append(StoredFile(
                    table,
                    f"{selection.observation_id}/{stored.path}",
                    stored.sha256,
                    stored.row_count,
                ))
                seen.add(key)
        tables[table] = pd.concat(pieces, ignore_index=True)
    quality = validate_snapshot_tables(
        tables,
        profile=ReadinessProfile(plan.readiness),
        universe_scope=plan.universe_scope,
    )
    assert quality.ready, quality.errors
    return warehouse._commit_snapshot(plan, quality, tuple(files))


def ready_market(path, *, close_values: tuple[float, ...] | None = None):
    warehouse = MarketDataWarehouse(path)
    manifest = warehouse.record_observation(observation(close_values=close_values))
    legacy = commit_test_snapshot(warehouse, SnapshotPlan(
        tuple(SourceSlice(
            manifest.observation_id, table, "deterministic test fixture",
        ) for table in MarketTable),
        "deterministic test fixture",
        universe_scope=fixture_universe_scope(),
    ))
    snapshot = IncrementalCanonicalPublisher(warehouse).bootstrap(legacy.snapshot_id)
    warehouse._replace_current_pointer(snapshot)
    return CanonicalMarketData.open(path)
