from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable, Mapping

import pandas as pd

from fundlab.marketdata.adjustments import derive_ratio_adjusted_bars
from fundlab.marketdata.contracts import (
    AssetType,
    CorporateActionType,
    DATA_GAP_QUARANTINE_RULE_ID,
    EXECUTION_EVIDENCE_GAP_RULE_ID,
    MarketTable,
    PriceLimitState,
    PriceMode,
    ReadinessProfile,
    SnapshotNotReadyError,
)
from fundlab.marketdata.trade_rules import resolve_order_quantity_rule
from fundlab.marketdata.warehouse import MarketDataWarehouse


@dataclass(frozen=True)
class Instrument:
    instrument_id: str
    exchange: str
    local_code: str
    asset_type: AssetType
    name: str
    currency: str
    listed_date: date | None
    delisted_date: date | None
    board: str | None
    exchange_product_class: str | None
    buy_lot: int
    quantity_step: int
    odd_lot_sell_all: bool
    price_tick: float
    sell_delay_sessions: int | None
    price_limit_ratio: float | None


@dataclass(frozen=True)
class DailyBar:
    instrument_id: str
    session_date: date
    price_mode: PriceMode
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: float
    amount: float | None
    suspended: bool
    is_st: bool | None
    trade_rule_id: str
    trade_rule_known_date: date
    buy_lot: int
    quantity_step: int
    odd_lot_sell_all: bool
    price_tick: float
    sell_delay_sessions: int
    price_limit_state: PriceLimitState
    previous_close: float | None
    price_limit_ratio: float | None
    limit_up: float | None
    limit_down: float | None
    source_provider: str
    source_observation_id: str


@dataclass(frozen=True)
class CorporateAction:
    action_id: str
    instrument_id: str
    action_type: CorporateActionType
    known_date: date
    record_date: date | None
    ex_date: date
    pay_date: date | None
    listing_date: date | None
    cash_per_share: float | None
    share_ratio: float | None
    rights_price: float | None
    source_provider: str
    source_observation_id: str
    quantity_multiplier: float | None = None


@dataclass(frozen=True)
class MarketSession:
    session_date: date
    bars: Mapping[str, DailyBar]
    record_actions: tuple[CorporateAction, ...]
    ex_actions: tuple[CorporateAction, ...]
    pay_actions: tuple[CorporateAction, ...]
    listing_actions: tuple[CorporateAction, ...]


class CanonicalMarketData:
    """The only query surface for version-pinned canonical market data."""

    def __init__(
        self,
        warehouse: MarketDataWarehouse,
        snapshot_id: str,
        *,
        required_readiness: ReadinessProfile = ReadinessProfile.SIMULATION,
    ) -> None:
        self.warehouse = warehouse
        self.manifest = warehouse.load_snapshot(snapshot_id)
        required_readiness = ReadinessProfile(required_readiness)
        if required_readiness is ReadinessProfile.LEGACY_UNKNOWN:
            raise ValueError("legacy_unknown is an inspection state, not a query readiness")
        if self.manifest.plan.readiness is ReadinessProfile.LEGACY_UNKNOWN:
            raise SnapshotNotReadyError(
                f"Legacy snapshot {snapshot_id} has no v2 readiness evidence; inspect or rebuild it"
            )
        if (
            required_readiness is ReadinessProfile.SIMULATION
            and self.manifest.plan.readiness is not ReadinessProfile.SIMULATION
        ):
            raise SnapshotNotReadyError(
                f"Snapshot {snapshot_id} is only {self.manifest.plan.readiness.value}-ready"
            )
        self.snapshot_id = self.manifest.snapshot_id
        # ``load_snapshot`` above is this handle's trust boundary. Reuse the
        # verified manifest for later queries instead of re-hashing the whole
        # warehouse on every Agent feature request.
        self._instruments = warehouse.query_loaded_snapshot_table(
            self.manifest, MarketTable.INSTRUMENTS,
        )
        selected = {item.table for item in self.manifest.plan.selections}
        self._calendar = (
            warehouse.query_loaded_snapshot_table(self.manifest, MarketTable.CALENDAR)
            if MarketTable.CALENDAR in selected else pd.DataFrame()
        )
        self._actions = (
            warehouse.query_loaded_snapshot_table(
                self.manifest, MarketTable.CORPORATE_ACTIONS,
            )
            if MarketTable.CORPORATE_ACTIONS in selected else pd.DataFrame()
        )
        self._instrument_records = {
            row.instrument_id: _instrument(row._asdict())
            for row in self._instruments.itertuples(index=False)
        }
        self._action_records = tuple(
            _action(row._asdict()) for row in self._actions.itertuples(index=False)
        )

    @classmethod
    def open(
        cls,
        root: str | Path,
        snapshot_id: str | None = None,
        *,
        required_readiness: ReadinessProfile = ReadinessProfile.SIMULATION,
    ) -> "CanonicalMarketData":
        warehouse = MarketDataWarehouse(root)
        resolved = snapshot_id or warehouse.current_snapshot_id()
        return cls(warehouse, resolved, required_readiness=required_readiness)

    def instrument(self, instrument_id: str) -> Instrument:
        try:
            return self._instrument_records[instrument_id]
        except KeyError as exc:
            raise KeyError(f"Unknown instrument in snapshot {self.snapshot_id}: {instrument_id}") from exc

    def instruments(
        self,
        *,
        as_of: date | None = None,
        asset_types: Iterable[AssetType | str] | None = None,
    ) -> tuple[Instrument, ...]:
        allowed = None if asset_types is None else {AssetType(item) for item in asset_types}
        found = []
        for item in self._instrument_records.values():
            if allowed is not None and item.asset_type not in allowed:
                continue
            if as_of is not None:
                if item.listed_date is not None and item.listed_date > as_of:
                    continue
                if item.delisted_date is not None and item.delisted_date < as_of:
                    continue
            found.append(item)
        return tuple(sorted(found, key=lambda item: item.instrument_id))

    def trading_days(
        self,
        start_date: date,
        end_date: date,
        *,
        exchanges: Iterable[str] | None = None,
    ) -> tuple[date, ...]:
        if start_date > end_date:
            raise ValueError("start_date must not exceed end_date")
        frame = self._calendar
        selected = frame[frame["is_open"] & frame["session_date"].between(start_date.isoformat(), end_date.isoformat())]
        if exchanges is not None:
            selected = selected[selected["exchange"].isin(tuple(exchanges))]
        return tuple(date.fromisoformat(item) for item in sorted(set(selected["session_date"])))

    def next_trading_day(self, value: date) -> date | None:
        selected = self._calendar[self._calendar["is_open"] & (self._calendar["session_date"] > value.isoformat())]
        if selected.empty:
            return None
        return date.fromisoformat(str(selected["session_date"].min()))

    def all_trading_days(self) -> tuple[date, ...]:
        selected = self._calendar[self._calendar["is_open"]]
        return tuple(date.fromisoformat(item) for item in sorted(set(selected["session_date"])))

    def bars(
        self,
        instrument_ids: Iterable[str],
        start_date: date,
        end_date: date,
        *,
        price_mode: PriceMode,
        as_of: date,
    ) -> pd.DataFrame:
        if start_date > end_date:
            raise ValueError("start_date must not exceed end_date")
        if end_date > as_of:
            raise ValueError("Market-data query exceeds its point-in-time as_of boundary")
        symbols = tuple(sorted(set(instrument_ids)))
        if not symbols:
            return pd.DataFrame()
        if price_mode is PriceMode.ADJUSTED:
            return self.adjusted_history(symbols, start_date, end_date, as_of=as_of)
        return self.warehouse.query_loaded_snapshot_table(
            self.manifest,
            MarketTable.DAILY_BARS,
            instrument_ids=symbols,
            start_date=start_date,
            end_date=end_date,
            price_mode=price_mode.value,
        ).sort_values(
            ["session_date", "instrument_id"], kind="stable",
        ).reset_index(drop=True)

    def adjusted_history(
        self,
        instrument_ids: Iterable[str],
        start_date: date,
        end_date: date,
        *,
        as_of: date,
    ) -> pd.DataFrame:
        if start_date > end_date:
            raise ValueError("start_date must not exceed end_date")
        if end_date > as_of:
            raise ValueError("Market-data query exceeds its point-in-time as_of boundary")
        symbols = tuple(sorted(set(instrument_ids)))
        if not symbols:
            return pd.DataFrame()
        raw = self.warehouse.query_loaded_snapshot_table(
            self.manifest,
            MarketTable.DAILY_BARS,
            instrument_ids=symbols,
            start_date=start_date,
            end_date=end_date,
            price_mode=PriceMode.RAW.value,
        )
        selected = {item.table for item in self.manifest.plan.selections}
        if MarketTable.ADJUSTMENT_FACTORS not in selected:
            raise SnapshotNotReadyError(
                f"Snapshot {self.snapshot_id} has no adjustment-factor coverage"
            )
        factors = self.warehouse.query_loaded_snapshot_table(
            self.manifest,
            MarketTable.ADJUSTMENT_FACTORS,
            instrument_ids=symbols,
            start_date=start_date,
            end_date=as_of,
        )
        return derive_ratio_adjusted_bars(raw, factors, as_of=as_of)

    def corporate_actions(
        self,
        instrument_ids: Iterable[str],
        start_date: date,
        end_date: date,
        *,
        as_of: date,
    ) -> pd.DataFrame:
        """Return action history visible at the explicit point-in-time boundary.

        Selection is by ``ex_date``. A row whose implementation announcement
        (``known_date``) was later than ``as_of`` remains invisible even when
        the current immutable snapshot already contains it.
        """
        if start_date > end_date:
            raise ValueError("start_date must not exceed end_date")
        if end_date > as_of:
            raise ValueError("Market-data query exceeds its point-in-time as_of boundary")
        symbols = tuple(sorted(set(instrument_ids)))
        if not symbols:
            return pd.DataFrame()
        frame = self.warehouse.query_loaded_snapshot_table(
            self.manifest,
            MarketTable.CORPORATE_ACTIONS,
            instrument_ids=symbols,
            start_date=start_date,
            end_date=end_date,
        )
        if frame.empty:
            return frame
        visible = frame["known_date"].astype(str) <= as_of.isoformat()
        return frame[visible].sort_values(
            ["ex_date", "instrument_id"], kind="stable",
        ).reset_index(drop=True)

    def session(
        self,
        session_date: date,
        *,
        instrument_ids: Iterable[str] | None = None,
    ) -> MarketSession:
        symbols = (
            tuple(self._instrument_records)
            if instrument_ids is None
            else tuple(sorted(set(map(str, instrument_ids))))
        )
        unknown = set(symbols) - set(self._instrument_records)
        if unknown:
            raise KeyError(f"Unknown instruments in session scope: {sorted(unknown)}")
        frame = self.bars(
            symbols,
            session_date,
            session_date,
            price_mode=PriceMode.RAW,
            as_of=session_date,
        )
        bars = {
            row.instrument_id: _bar(
                row._asdict(), self._instrument_records[row.instrument_id],
            )
            for row in frame.itertuples(index=False)
        }
        symbol_set = set(symbols)
        visible_actions = tuple(
            item
            for item in self._action_records
            if item.known_date <= session_date and item.instrument_id in symbol_set
        )
        return MarketSession(
            session_date,
            bars,
            tuple(item for item in visible_actions if item.record_date == session_date),
            tuple(item for item in visible_actions if item.ex_date == session_date),
            tuple(item for item in visible_actions if item.pay_date == session_date),
            tuple(item for item in visible_actions if item.listing_date == session_date),
        )

    def session_range(
        self,
        session_dates: Iterable[date],
        *,
        instrument_ids: Iterable[str],
    ) -> Mapping[date, MarketSession]:
        """Materialize an exact fixed-universe clock without repeated partition reads."""

        days = tuple(sorted(set(session_dates)))
        if not days:
            return {}
        symbols = tuple(sorted(set(map(str, instrument_ids))))
        unknown = set(symbols) - set(self._instrument_records)
        if unknown:
            raise KeyError(f"Unknown instruments in session scope: {sorted(unknown)}")
        frame = self.bars(
            symbols,
            days[0],
            days[-1],
            price_mode=PriceMode.RAW,
            as_of=days[-1],
        )
        rows_by_day: dict[date, list[object]] = {day: [] for day in days}
        for row in frame.itertuples(index=False):
            row_day = date.fromisoformat(str(row.session_date))
            if row_day in rows_by_day:
                rows_by_day[row_day].append(row)
        symbol_set = set(symbols)
        actions = tuple(
            item for item in self._action_records if item.instrument_id in symbol_set
        )
        found: dict[date, MarketSession] = {}
        for day in days:
            bars = {
                row.instrument_id: _bar(
                    row._asdict(), self._instrument_records[row.instrument_id]
                )
                for row in rows_by_day[day]
            }
            visible = tuple(item for item in actions if item.known_date <= day)
            found[day] = MarketSession(
                day,
                bars,
                tuple(item for item in visible if item.record_date == day),
                tuple(item for item in visible if item.ex_date == day),
                tuple(item for item in visible if item.pay_date == day),
                tuple(item for item in visible if item.listing_date == day),
            )
        return found


@dataclass(frozen=True)
class PointInTimeMarketView:
    market_data: CanonicalMarketData
    as_of: date

    @property
    def snapshot_id(self) -> str:
        return self.market_data.snapshot_id

    def adjusted_history(
        self, instrument_ids: Iterable[str], start_date: date, end_date: date | None = None,
    ) -> pd.DataFrame:
        bounded_end = self.as_of if end_date is None else end_date
        return self.market_data.adjusted_history(
            instrument_ids, start_date, bounded_end, as_of=self.as_of,
        )


def _instrument(row: Mapping[str, object]) -> Instrument:
    quantity_rule = resolve_order_quantity_rule(row)
    return Instrument(
        str(row["instrument_id"]),
        str(row["exchange"]),
        str(row["local_code"]),
        AssetType(str(row["asset_type"])),
        str(row["name"]),
        str(row["currency"]),
        _optional_date(row.get("listed_date")),
        _optional_date(row.get("delisted_date")),
        _optional_str(row.get("board")),
        _optional_str(row.get("exchange_product_class")),
        quantity_rule.minimum_buy_quantity,
        quantity_rule.quantity_step,
        quantity_rule.odd_lot_sell_all,
        float(row["price_tick"]),
        _optional_int(row.get("sell_delay_sessions")),
        _optional_float(row.get("price_limit_ratio")),
    )


def _bar(row: Mapping[str, object], instrument: Instrument) -> DailyBar:
    has_explicit_quantity_rule = all(
        row.get(field) is not None and not pd.isna(row.get(field))
        for field in ("buy_lot", "quantity_step", "odd_lot_sell_all")
    )
    minimum = int(row["buy_lot"]) if has_explicit_quantity_rule else instrument.buy_lot
    step = int(row["quantity_step"]) if has_explicit_quantity_rule else instrument.quantity_step
    odd_lot_sell_all = (
        bool(row["odd_lot_sell_all"])
        if has_explicit_quantity_rule else instrument.odd_lot_sell_all
    )
    return DailyBar(
        str(row["instrument_id"]),
        date.fromisoformat(str(row["session_date"])),
        PriceMode(str(row["price_mode"])),
        _optional_float(row.get("open")),
        _optional_float(row.get("high")),
        _optional_float(row.get("low")),
        _optional_float(row.get("close")),
        float(row["volume"]),
        _optional_float(row.get("amount")),
        _bar_suspended(row),
        _optional_bool(row.get("is_st")),
        str(row["trade_rule_id"]),
        date.fromisoformat(str(row["trade_rule_known_date"])),
        minimum,
        step,
        odd_lot_sell_all,
        float(row["price_tick"]),
        int(row["sell_delay_sessions"]),
        PriceLimitState(str(row["price_limit_state"])),
        _optional_float(row.get("previous_close")),
        _optional_float(row.get("price_limit_ratio")),
        _optional_float(row.get("limit_up")),
        _optional_float(row.get("limit_down")),
        str(row["source_provider"]),
        str(row["source_observation_id"]),
    )


def _action(row: Mapping[str, object]) -> CorporateAction:
    return CorporateAction(
        str(row["action_id"]),
        str(row["instrument_id"]),
        CorporateActionType(str(row["action_type"])),
        date.fromisoformat(str(row["known_date"])),
        _optional_date(row.get("record_date")),
        date.fromisoformat(str(row["ex_date"])),
        _optional_date(row.get("pay_date")),
        _optional_date(row.get("listing_date")),
        _optional_float(row.get("cash_per_share")),
        _optional_float(row.get("share_ratio")),
        _optional_float(row.get("rights_price")),
        str(row["source_provider"]),
        str(row["source_observation_id"]),
        _optional_float(row.get("quantity_multiplier")),
    )


def _optional_date(value: object) -> date | None:
    return None if value is None or pd.isna(value) else date.fromisoformat(str(value))


def _optional_float(value: object) -> float | None:
    return None if value is None or pd.isna(value) else float(value)


def _optional_int(value: object) -> int | None:
    return None if value is None or pd.isna(value) else int(value)


def _optional_bool(value: object) -> bool | None:
    return None if value is None or pd.isna(value) else bool(value)


def _bar_suspended(row: Mapping[str, object]) -> bool:
    value = row.get("suspended")
    if value is not None and not pd.isna(value):
        return bool(value)
    if str(row.get("trade_rule_id")) in {
        DATA_GAP_QUARANTINE_RULE_ID,
        EXECUTION_EVIDENCE_GAP_RULE_ID,
    }:
        return False
    raise SnapshotNotReadyError(
        f"Daily bar has unknown suspension state: {row.get('instrument_id')}/"
        f"{row.get('session_date')}"
    )


def _optional_str(value: object) -> str | None:
    return None if value is None or pd.isna(value) else str(value)
