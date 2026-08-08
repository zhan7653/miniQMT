from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_UP
import json
from typing import Any, Mapping

import pandas as pd

from fundlab.marketdata.contracts import (
    AssetType,
    CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
    CorporateActionType,
    DATA_GAP_QUARANTINE_RULE_ID,
    EXECUTION_EVIDENCE_GAP_RULE_ID,
    MarketTable,
    ObservationError,
    PriceLimitState,
    PriceMode,
    QualityReport,
    ReadinessProfile,
    SnapshotState,
    UniverseScope,
)


@dataclass(frozen=True)
class ColumnSpec:
    kind: str
    nullable: bool = False
    default: Any = None


BUSINESS_SCHEMAS: Mapping[MarketTable, Mapping[str, ColumnSpec]] = {
    MarketTable.INSTRUMENTS: {
        "instrument_id": ColumnSpec("string"),
        "exchange": ColumnSpec("string"),
        "local_code": ColumnSpec("string"),
        "asset_type": ColumnSpec("string"),
        "name": ColumnSpec("string"),
        "currency": ColumnSpec("string", default="CNY"),
        "listed_date": ColumnSpec("date", nullable=True),
        "delisted_date": ColumnSpec("date", nullable=True),
        "board": ColumnSpec("string", nullable=True),
        "exchange_product_class": ColumnSpec("string", nullable=True),
        # ``buy_lot`` is the minimum normal buy declaration, not necessarily the
        # quantity increment.  STAR shares use minimum=200 and step=1.
        "buy_lot": ColumnSpec("int"),
        "quantity_step": ColumnSpec("int", nullable=True),
        "odd_lot_sell_all": ColumnSpec("bool", nullable=True),
        "price_tick": ColumnSpec("float"),
        # Current-universe sources may not identify ETF T+0 subclasses.  Simulation
        # uses the mandatory dated daily value; this static field is descriptive only.
        "sell_delay_sessions": ColumnSpec("int", nullable=True),
        "price_limit_ratio": ColumnSpec("float", nullable=True),
        "field_lineage": ColumnSpec("string", nullable=True),
        "source_payload": ColumnSpec("string", nullable=True),
    },
    MarketTable.CALENDAR: {
        "exchange": ColumnSpec("string"),
        "session_date": ColumnSpec("date"),
        "is_open": ColumnSpec("bool"),
        "field_lineage": ColumnSpec("string", nullable=True),
        "source_payload": ColumnSpec("string", nullable=True),
    },
    MarketTable.DAILY_BARS: {
        "instrument_id": ColumnSpec("string"),
        "session_date": ColumnSpec("date"),
        "price_mode": ColumnSpec("string"),
        "open": ColumnSpec("float", nullable=True),
        "high": ColumnSpec("float", nullable=True),
        "low": ColumnSpec("float", nullable=True),
        "close": ColumnSpec("float", nullable=True),
        "volume": ColumnSpec("float"),
        "amount": ColumnSpec("float", nullable=True),
        "suspended": ColumnSpec("bool", nullable=True),
        "is_st": ColumnSpec("bool", nullable=True),
        "trade_rule_id": ColumnSpec("string", nullable=True),
        "trade_rule_known_date": ColumnSpec("date", nullable=True),
        "buy_lot": ColumnSpec("int", nullable=True),
        "quantity_step": ColumnSpec("int", nullable=True),
        "odd_lot_sell_all": ColumnSpec("bool", nullable=True),
        "price_tick": ColumnSpec("float", nullable=True),
        "sell_delay_sessions": ColumnSpec("int", nullable=True),
        "price_limit_state": ColumnSpec("string", nullable=True),
        "previous_close": ColumnSpec("float", nullable=True),
        "price_limit_ratio": ColumnSpec("float", nullable=True),
        "limit_up": ColumnSpec("float", nullable=True),
        "limit_down": ColumnSpec("float", nullable=True),
        "field_lineage": ColumnSpec("string", nullable=True),
        "source_payload": ColumnSpec("string", nullable=True),
    },
    MarketTable.CORPORATE_ACTIONS: {
        "action_id": ColumnSpec("string"),
        "instrument_id": ColumnSpec("string"),
        "action_type": ColumnSpec("string"),
        "known_date": ColumnSpec("date", nullable=True),
        "record_date": ColumnSpec("date", nullable=True),
        "ex_date": ColumnSpec("date"),
        "pay_date": ColumnSpec("date", nullable=True),
        "listing_date": ColumnSpec("date", nullable=True),
        "cash_per_share": ColumnSpec("float", nullable=True),
        "share_ratio": ColumnSpec("float", nullable=True),
        "rights_price": ColumnSpec("float", nullable=True),
        # Total post-event units divided by pre-event units.  Unlike a stock
        # dividend ratio this can be below one for an ETF consolidation.
        "quantity_multiplier": ColumnSpec("float", nullable=True),
        "field_lineage": ColumnSpec("string", nullable=True),
        "source_payload": ColumnSpec("string", nullable=True),
    },
    MarketTable.ADJUSTMENT_FACTORS: {
        "factor_id": ColumnSpec("string"),
        "instrument_id": ColumnSpec("string"),
        "effective_date": ColumnSpec("date"),
        "known_date": ColumnSpec("date"),
        # Multiply prices strictly before effective_date by this event ratio.
        "price_multiplier": ColumnSpec("float"),
        "field_lineage": ColumnSpec("string", nullable=True),
        "source_payload": ColumnSpec("string", nullable=True),
    },
}

LINEAGE_SCHEMA: Mapping[str, ColumnSpec] = {
    "source_provider": ColumnSpec("string"),
    "source_observation_id": ColumnSpec("string", nullable=True),
    "observed_at": ColumnSpec("datetime"),
}

TABLE_KEYS: Mapping[MarketTable, tuple[str, ...]] = {
    MarketTable.INSTRUMENTS: ("instrument_id",),
    MarketTable.CALENDAR: ("exchange", "session_date"),
    MarketTable.DAILY_BARS: ("instrument_id", "session_date", "price_mode"),
    # Provider-generated ids are lineage fields, not cross-provider semantic keys.
    MarketTable.CORPORATE_ACTIONS: ("instrument_id", "action_type", "ex_date"),
    MarketTable.ADJUSTMENT_FACTORS: ("instrument_id", "effective_date"),
}

TABLE_DATE_COLUMNS: Mapping[MarketTable, str | None] = {
    MarketTable.INSTRUMENTS: None,
    MarketTable.CALENDAR: "session_date",
    MarketTable.DAILY_BARS: "session_date",
    MarketTable.CORPORATE_ACTIONS: "ex_date",
    MarketTable.ADJUSTMENT_FACTORS: "effective_date",
}

TABLE_INSTRUMENT_COLUMNS: Mapping[MarketTable, str | None] = {
    MarketTable.INSTRUMENTS: "instrument_id",
    MarketTable.CALENDAR: None,
    MarketTable.DAILY_BARS: "instrument_id",
    MarketTable.CORPORATE_ACTIONS: "instrument_id",
    MarketTable.ADJUSTMENT_FACTORS: "instrument_id",
}


def empty_table(table: MarketTable, *, include_lineage: bool = False) -> pd.DataFrame:
    columns = list(BUSINESS_SCHEMAS[table])
    if include_lineage:
        columns.extend(LINEAGE_SCHEMA)
    return normalize_table(table, pd.DataFrame(columns=columns), require_observation_id=False)


def normalize_table(
    table: MarketTable,
    frame: pd.DataFrame,
    *,
    provider: str | None = None,
    observed_at: str | None = None,
    observation_id: str | None = None,
    require_observation_id: bool = False,
) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame):
        raise ObservationError(f"{table.value} must be a pandas DataFrame")
    result = frame.copy(deep=True)
    schema = BUSINESS_SCHEMAS[table]
    unknown = sorted(set(result.columns) - set(schema) - set(LINEAGE_SCHEMA))
    if unknown:
        raise ObservationError(f"{table.value} has unknown columns: {', '.join(unknown)}")
    for name, spec in schema.items():
        if name not in result:
            if spec.default is not None:
                result[name] = spec.default
            elif spec.nullable:
                result[name] = pd.NA
            else:
                raise ObservationError(f"{table.value} is missing required column: {name}")
        result[name] = _coerce(result[name], spec, table, name)

    if provider is not None:
        result["source_provider"] = provider
    if observed_at is not None:
        result["observed_at"] = observed_at
    if observation_id is not None:
        result["source_observation_id"] = observation_id
    for name, spec in LINEAGE_SCHEMA.items():
        if name not in result:
            if name == "source_observation_id" and not require_observation_id:
                result[name] = pd.NA
            else:
                raise ObservationError(f"{table.value} is missing lineage column: {name}")
        result[name] = _coerce(result[name], spec, table, name)

    if require_observation_id and result["source_observation_id"].isna().any():
        raise ObservationError(f"{table.value} has rows without source_observation_id")
    result = result[list(schema) + list(LINEAGE_SCHEMA)]
    result = result.sort_values(list(TABLE_KEYS[table]), kind="stable", na_position="last").reset_index(drop=True)
    return result


def validate_snapshot_tables(
    tables: Mapping[MarketTable, pd.DataFrame], *, coverage_errors: tuple[str, ...] = (),
    profile: ReadinessProfile = ReadinessProfile.SIMULATION,
    universe_scope: UniverseScope | None = None,
    allow_universe_subset: bool = False,
) -> QualityReport:
    profile = ReadinessProfile(profile)
    errors = list(coverage_errors)
    warnings: list[str] = []
    required = {
        ReadinessProfile.RESEARCH_PRICE: {
            MarketTable.INSTRUMENTS,
            MarketTable.DAILY_BARS,
        },
        ReadinessProfile.SIMULATION: set(MarketTable),
    }[profile]
    missing = sorted(item.value for item in required - set(tables))
    errors.extend(f"missing_table:{name}" for name in missing)
    if missing:
        return QualityReport(SnapshotState.INCOMPLETE, tuple(sorted(set(errors))), (), {})

    normalized: dict[MarketTable, pd.DataFrame] = {}
    for table in sorted(tables, key=lambda item: item.value):
        try:
            normalized[table] = normalize_table(table, tables[table], require_observation_id=True)
        except ObservationError as exc:
            errors.append(f"schema:{table.value}:{exc}")
            continue
        keys = list(TABLE_KEYS[table])
        if normalized[table].duplicated(keys).any():
            errors.append(f"duplicate_key:{table.value}")

    if not required <= set(normalized):
        return QualityReport(
            SnapshotState.INCOMPLETE,
            tuple(sorted(set(errors))),
            tuple(sorted(set(warnings))),
            {table.value: len(frame) for table, frame in normalized.items()},
        )

    instruments = normalized[MarketTable.INSTRUMENTS]
    bars = normalized[MarketTable.DAILY_BARS]

    if profile is ReadinessProfile.SIMULATION and universe_scope is None:
        errors.append("missing_universe_scope")
    if universe_scope is not None:
        actual_instruments = set(map(str, instruments["instrument_id"]))
        declared_instruments = set(universe_scope.instrument_ids)
        if allow_universe_subset:
            if not actual_instruments <= declared_instruments:
                errors.append("partition_contains_instrument_outside_universe")
        elif actual_instruments != declared_instruments:
            errors.append("snapshot_universe_instrument_mismatch")
        if universe_scope.definition == CURRENT_SH_SZ_STOCK_ETF_UNIVERSE:
            if not universe_scope.survivorship_bias:
                errors.append("current_universe_missing_survivorship_bias_declaration")
            if not set(instruments["exchange"].dropna()) <= {"SH", "SZ"}:
                errors.append("current_universe_contains_unsupported_exchange")
            if not set(instruments["asset_type"].dropna()) <= {
                AssetType.STOCK.value, AssetType.ETF.value,
            }:
                errors.append("current_universe_contains_unsupported_asset")
            as_of = universe_scope.as_of_date.isoformat()
            if instruments["listed_date"].dropna().gt(as_of).any():
                errors.append("current_universe_contains_future_listing")
            delisted = instruments["delisted_date"].dropna()
            if delisted.le(as_of).any():
                errors.append("current_universe_contains_delisted_instrument")

    allowed_assets = {item.value for item in AssetType}
    if not set(instruments["asset_type"].dropna()) <= allowed_assets:
        errors.append("unsupported_asset_type")
    if (instruments["buy_lot"] <= 0).any() or (instruments["price_tick"] <= 0).any():
        errors.append("invalid_instrument_trading_rule")
    if (instruments["quantity_step"].dropna() <= 0).any():
        errors.append("invalid_instrument_quantity_step")
    if (instruments["sell_delay_sessions"].dropna() < 0).any():
        errors.append("invalid_sell_delay")
    if instruments["listed_date"].isna().any():
        errors.append("missing_instrument_listed_date")
    dated = instruments.dropna(subset=["listed_date", "delisted_date"])
    if (dated["listed_date"] > dated["delisted_date"]).any():
        errors.append("invalid_instrument_lifetime")
    ratios = instruments["price_limit_ratio"].dropna()
    if ((ratios <= 0) | (ratios >= 1)).any():
        errors.append("invalid_price_limit_ratio")

    instrument_ids = set(instruments["instrument_id"])
    if not set(bars["instrument_id"]) <= instrument_ids:
        errors.append("bar_instrument_missing_from_master")

    allowed_modes = {item.value for item in PriceMode}
    if not set(bars["price_mode"].dropna()) <= allowed_modes:
        errors.append("invalid_price_mode")
    raw = bars.loc[bars["price_mode"] == PriceMode.RAW.value].copy()
    if raw.empty:
        errors.append("missing_raw_bars")
    if (bars["price_mode"] == PriceMode.ADJUSTED.value).any():
        warnings.append("provider_adjusted_bars_are_audit_only")
    quarantine = raw["trade_rule_id"].astype(str).eq(DATA_GAP_QUARANTINE_RULE_ID)
    execution_guard = raw["trade_rule_id"].astype(str).eq(
        EXECUTION_EVIDENCE_GAP_RULE_ID
    )
    active = raw.loc[
        ~raw["suspended"].fillna(False) & ~quarantine & ~execution_guard
    ]
    if active[["open", "high", "low", "close"]].isna().any().any():
        errors.append("active_bar_missing_ohlc")
    if not active.empty:
        invalid_ohlc = (
            (active[["open", "high", "low", "close"]].min(axis=1) <= 0)
            | (active["high"] < active[["open", "close", "low"]].max(axis=1))
            | (active["low"] > active[["open", "close", "high"]].min(axis=1))
        )
        if invalid_ohlc.any():
            errors.append("invalid_ohlc")
    guarded_prices = raw.loc[
        execution_guard
        & raw[["open", "high", "low", "close"]].notna().any(axis=1)
    ]
    if not guarded_prices.empty:
        if guarded_prices[["open", "high", "low", "close"]].isna().any().any():
            errors.append("execution_guard_bar_partial_ohlc")
        else:
            invalid_guarded_ohlc = (
                (guarded_prices[["open", "high", "low", "close"]].min(axis=1) <= 0)
                | (
                    guarded_prices["high"]
                    < guarded_prices[["open", "close", "low"]].max(axis=1)
                )
                | (
                    guarded_prices["low"]
                    > guarded_prices[["open", "close", "high"]].min(axis=1)
                )
            )
            if invalid_guarded_ohlc.any():
                errors.append("invalid_execution_guard_ohlc")
    if (raw["volume"] < 0).any():
        errors.append("negative_volume")
    if raw["amount"].isna().any():
        warnings.append("optional_amount_missing")
    if (raw["amount"].dropna() < 0).any():
        errors.append("negative_amount")
    limit_rows = raw.dropna(subset=["limit_up", "limit_down"])
    if (limit_rows["limit_up"] < limit_rows["limit_down"]).any():
        errors.append("invalid_price_limits")
    raw_keys = set(map(tuple, raw[["instrument_id", "session_date"]].to_numpy()))

    if profile is ReadinessProfile.SIMULATION:
        calendar = normalized[MarketTable.CALENDAR]
        actions = normalized[MarketTable.CORPORATE_ACTIONS]
        factors = normalized[MarketTable.ADJUSTMENT_FACTORS]
        if calendar.empty or not calendar["is_open"].any():
            errors.append("empty_trading_calendar")
        open_days = set(calendar.loc[calendar["is_open"], "session_date"])
        if not set(raw["session_date"]) <= open_days:
            errors.append("bar_on_closed_or_unknown_session")
        if (raw["suspended"].isna() & ~quarantine & ~execution_guard).any():
            errors.append("missing_tradability_state")
        suspended = raw.loc[raw["suspended"].fillna(False)]
        simulation_active = raw.loc[
            ~raw["suspended"].fillna(False) & ~quarantine & ~execution_guard
        ]
        quarantine_rows = raw.loc[quarantine]
        if not suspended.empty:
            if pd.to_numeric(suspended["volume"], errors="coerce").fillna(0).ne(0).any():
                errors.append("suspended_bar_nonzero_volume")
            if suspended[["open", "high", "low", "close"]].notna().any().any():
                errors.append("suspended_bar_has_ohlc")
        if not quarantine_rows.empty:
            if pd.to_numeric(
                quarantine_rows["volume"], errors="coerce",
            ).fillna(0).ne(0).any():
                errors.append("quarantine_bar_nonzero_volume")
            if quarantine_rows[["open", "high", "low", "close"]].notna().any().any():
                errors.append("quarantine_bar_has_ohlc")
        if simulation_active["is_st"].isna().any():
            errors.append("missing_st_state")
        if raw["trade_rule_id"].isna().any() or raw["trade_rule_id"].str.strip().eq("").fillna(True).any():
            errors.append("missing_trade_rule_identity")
        if raw["trade_rule_known_date"].isna().any():
            errors.append("missing_trade_rule_known_date")
        else:
            future_rules = raw["trade_rule_known_date"].astype(str) > raw["session_date"].astype(str)
            if future_rules.any():
                errors.append("future_trade_rule_knowledge")
        if raw[[
            "buy_lot", "quantity_step", "odd_lot_sell_all",
            "price_tick", "sell_delay_sessions",
        ]].isna().any().any():
            errors.append("missing_daily_trading_attributes")
        else:
            if (
                (raw["buy_lot"] <= 0).any()
                or (raw["quantity_step"] <= 0).any()
                or (raw["price_tick"] <= 0).any()
            ):
                errors.append("invalid_daily_trading_attributes")
            if (raw["sell_delay_sessions"] < 0).any():
                errors.append("invalid_daily_sell_delay")
        allowed_limit_states = {item.value for item in PriceLimitState}
        if not set(raw["price_limit_state"].dropna()) <= allowed_limit_states:
            errors.append("invalid_price_limit_state")
        if raw["price_limit_state"].isna().any() or (
            simulation_active["price_limit_state"] == PriceLimitState.UNKNOWN.value
        ).any():
            errors.append("unknown_price_limit_state")
        if simulation_active["previous_close"].isna().any():
            errors.append("missing_previous_close")
        bounded = simulation_active.loc[
            simulation_active["price_limit_state"] == PriceLimitState.BOUNDED.value
        ]
        if bounded[["previous_close", "limit_up", "limit_down"]].isna().any().any():
            errors.append("missing_price_limit_rule")
        unbounded = simulation_active.loc[
            simulation_active["price_limit_state"] == PriceLimitState.UNBOUNDED.value
        ]
        if unbounded[["limit_up", "limit_down"]].notna().any().any():
            errors.append("unbounded_session_has_price_limits")
        if not bounded.empty:
            symmetric = bounded.dropna(subset=["price_limit_ratio"])
            for (ratio, tick), group in symmetric.groupby(
                ["price_limit_ratio", "price_tick"], sort=False,
            ):
                expected_up = _round_limit_series(
                    group["previous_close"], Decimal("1") + Decimal(str(ratio)), tick,
                )
                expected_down = _round_limit_series(
                    group["previous_close"], Decimal("1") - Decimal(str(ratio)), tick,
                )
                tolerance = max(float(tick) / 100, 1e-9)
                if (
                    pd.to_numeric(group["limit_up"], errors="coerce")
                    .sub(expected_up).abs().gt(tolerance).any()
                    or pd.to_numeric(group["limit_down"], errors="coerce")
                    .sub(expected_down).abs().gt(tolerance).any()
                ):
                    errors.append("derived_price_limit_mismatch")
                    break

        scope_start = None if universe_scope is None else universe_scope.history_start.isoformat()
        scope_end = None if universe_scope is None else universe_scope.history_end.isoformat()
        scoped_calendar = calendar
        if scope_start is not None:
            target_exchanges = set(map(str, instruments["exchange"]))
            scoped_calendar = calendar.loc[
                calendar["session_date"].between(scope_start, scope_end)
                & calendar["exchange"].isin(target_exchanges)
            ]
            expected_calendar_keys = {
                (exchange, day.isoformat())
                for exchange in target_exchanges
                for day in _date_range(universe_scope.history_start, universe_scope.history_end)
            }
            actual_calendar_keys = set(map(
                tuple, scoped_calendar[["exchange", "session_date"]].to_numpy(),
            ))
            if actual_calendar_keys != expected_calendar_keys:
                errors.append("calendar_date_coverage_mismatch")
        sessions_by_exchange = {
            exchange: tuple(sorted(set(group.loc[group["is_open"], "session_date"])))
            for exchange, group in scoped_calendar.groupby("exchange")
        }
        expected_keys: set[tuple[str, str]] = set()
        for row in instruments.to_dict("records"):
            listed = row["listed_date"]
            if pd.isna(listed):
                continue
            delisted = row["delisted_date"]
            start = str(listed)
            end = "9999-12-31" if pd.isna(delisted) else str(delisted)
            if scope_start is not None:
                start = max(start, scope_start)
                end = min(end, scope_end)
            expected_keys.update(
                (row["instrument_id"], day)
                for day in sessions_by_exchange.get(row["exchange"], ())
                if start <= day <= end
            )
        if raw_keys != expected_keys:
            errors.append("daily_bar_calendar_coverage_mismatch")
        if not set(actions["instrument_id"]) <= instrument_ids:
            errors.append("action_instrument_missing_from_master")
        allowed_actions = {item.value for item in CorporateActionType}
        if not set(actions["action_type"].dropna()) <= allowed_actions:
            errors.append("unsupported_corporate_action")
        for row in actions.to_dict("records"):
            action_type = row["action_type"]
            if _missing(row.get("known_date")):
                errors.append(f"action_missing_known_date:{row['action_id']}")
            else:
                first_effective = (
                    row["ex_date"] if _missing(row.get("record_date"))
                    else row["record_date"]
                )
                if str(row["known_date"]) > str(first_effective):
                    errors.append(f"future_action_knowledge:{row['action_id']}")
            if _missing(row.get("record_date")):
                errors.append(f"action_missing_record_date:{row['action_id']}")
            elif str(row["record_date"]) > str(row["ex_date"]):
                errors.append(f"invalid_action_date_order:{row['action_id']}")
            if action_type == CorporateActionType.CASH_DIVIDEND.value:
                if (
                    _missing(row.get("pay_date"))
                    or str(row.get("pay_date")) < str(row.get("ex_date"))
                    or _nonpositive(row.get("cash_per_share"))
                ):
                    errors.append(f"invalid_cash_dividend:{row['action_id']}")
            elif action_type in {
                CorporateActionType.STOCK_DIVIDEND.value,
            }:
                if (
                    (
                        not _missing(row.get("listing_date"))
                        and str(row.get("listing_date")) < str(row.get("ex_date"))
                    )
                    or _nonpositive(row.get("share_ratio"))
                ):
                    errors.append(f"invalid_share_action:{row['action_id']}")
            elif action_type == CorporateActionType.SPLIT.value:
                if (
                    _missing(row.get("listing_date"))
                    or _nonpositive(row.get("quantity_multiplier"))
                    or str(row.get("listing_date")) != str(row.get("ex_date"))
                    or str(row.get("record_date")) > str(row.get("ex_date"))
                ):
                    errors.append(f"invalid_split:{row['action_id']}")
            elif action_type == CorporateActionType.RIGHTS_ISSUE.value:
                if (
                    _missing(row.get("pay_date"))
                    or _nonpositive(row.get("share_ratio"))
                    or _nonpositive(row.get("rights_price"))
                ):
                    errors.append(f"invalid_rights_issue:{row['action_id']}")

        if not set(factors["instrument_id"]) <= instrument_ids:
            errors.append("factor_instrument_missing_from_master")
        if (factors["price_multiplier"] <= 0).any():
            errors.append("invalid_adjustment_factor")
        if (
            factors["known_date"].astype(str)
            > factors["effective_date"].astype(str)
        ).any():
            errors.append("future_adjustment_factor_knowledge")
        factor_events = set(map(tuple, factors[["instrument_id", "effective_date"]].to_numpy()))
        position_factors = factors.loc[[
            not _is_non_position_adjustment_factor(row)
            for row in factors.to_dict("records")
        ]]
        position_factor_events = set(map(
            tuple,
            position_factors[["instrument_id", "effective_date"]].to_numpy(),
        ))
        action_events = set(map(tuple, actions[["instrument_id", "ex_date"]].to_numpy()))
        if not action_events <= position_factor_events:
            errors.append("corporate_action_missing_adjustment_factor")
        if not position_factor_events <= action_events:
            errors.append("adjustment_factor_missing_corporate_action")
        technical_count = len(factor_events - position_factor_events)
        if technical_count:
            warnings.append(f"non_position_adjustment_factors:{technical_count}")

        lifetimes = instruments.set_index("instrument_id")
        for instrument_id, event_date in action_events | factor_events:
            if instrument_id not in lifetimes.index:
                continue
            instrument = lifetimes.loc[instrument_id]
            start = max(str(instrument["listed_date"]), scope_start or "0001-01-01")
            delisted = instrument.get("delisted_date")
            end = scope_end or "9999-12-31"
            if not _missing(delisted):
                end = min(end, str(delisted))
            if not start <= str(event_date) <= end:
                errors.append("corporate_event_outside_instrument_lifecycle")
                break

    state = SnapshotState.READY if not errors else SnapshotState.INCOMPLETE
    return QualityReport(
        state,
        tuple(sorted(set(errors))),
        tuple(sorted(set(warnings))),
        {table.value: len(frame) for table, frame in normalized.items()},
    )


def _is_non_position_adjustment_factor(row: Mapping[str, Any]) -> bool:
    value = row.get("field_lineage")
    if value is None or pd.isna(value):
        return False
    try:
        payload = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return False
    return (
        isinstance(payload, Mapping)
        and payload.get("kind") == "non_position_adjustment_factor_r2"
    )


def _coerce(series: pd.Series, spec: ColumnSpec, table: MarketTable, name: str) -> pd.Series:
    try:
        if spec.kind == "string":
            converted = series.astype("string")
        elif spec.kind == "date":
            parsed = pd.to_datetime(series, errors="coerce")
            converted = parsed.dt.strftime("%Y-%m-%d").astype("string")
        elif spec.kind == "datetime":
            parsed = pd.to_datetime(series, errors="coerce", utc=True)
            converted = parsed.dt.strftime("%Y-%m-%dT%H:%M:%S.%f%z").astype("string")
        elif spec.kind == "int":
            converted = pd.to_numeric(series, errors="coerce").astype("Int64")
        elif spec.kind == "float":
            converted = pd.to_numeric(series, errors="coerce").astype("Float64")
        elif spec.kind == "bool":
            converted = series.astype("boolean")
        else:
            raise AssertionError(spec.kind)
    except (TypeError, ValueError) as exc:
        raise ObservationError(f"{table.value}.{name} cannot be converted to {spec.kind}") from exc
    if not spec.nullable and converted.isna().any():
        raise ObservationError(f"{table.value}.{name} contains null or invalid values")
    if spec.kind == "string" and not spec.nullable and converted.str.strip().eq("").any():
        raise ObservationError(f"{table.value}.{name} contains empty values")
    return converted


def _missing(value: Any) -> bool:
    return value is None or pd.isna(value)


def _nonpositive(value: Any) -> bool:
    return _missing(value) or float(value) <= 0


def _round_limit_series(
    previous_close: pd.Series,
    multiplier: Decimal,
    tick: Any,
) -> pd.Series:
    """Vectorized HALF_UP rounding with an exact fallback for unusual prices."""

    price_tick = Decimal(str(tick))
    tick_value = float(price_tick)
    prices = pd.to_numeric(previous_close, errors="coerce").astype(float)
    rounded = pd.Series(float("nan"), index=previous_close.index, dtype="float64")
    valid = prices.notna() & (tick_value > 0)
    if not valid.any():
        return rounded

    units_raw = prices.loc[valid] / tick_value
    units = units_raw.round().astype("int64")
    aligned = units_raw.sub(units).abs() <= 1e-6
    if aligned.any():
        numerator, denominator = multiplier.as_integer_ratio()
        aligned_units = units.loc[aligned]
        result_units = (
            2 * aligned_units * numerator + denominator
        ) // (2 * denominator)
        rounded.loc[aligned_units.index] = (
            result_units.astype(float) * tick_value
        ).round(8)

    if (~aligned).any():
        rounded.loc[aligned.index[~aligned]] = prices.loc[
            aligned.index[~aligned]
        ].map(
            lambda value: float(
                (
                    Decimal(str(value)) * multiplier / price_tick
                ).to_integral_value(rounding=ROUND_HALF_UP)
                * price_tick
            )
        )
    return rounded


def _round_limit(previous_close: Any, ratio: Any, tick: Any, *, upper: bool) -> float:
    previous = Decimal(str(previous_close))
    movement = Decimal(str(ratio))
    price_tick = Decimal(str(tick))
    sign = Decimal("1") if upper else Decimal("-1")
    value = previous * (Decimal("1") + sign * movement)
    return float((value / price_tick).to_integral_value(rounding=ROUND_HALF_UP) * price_tick)


def _date_range(start, end):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)
