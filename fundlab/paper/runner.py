from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Protocol, Sequence

from fundlab.paper.repository import AccountRecord, PaperLedgerRepository, ReplayValidationError
from fundlab.paper.strategy_registry import StrategyRegistry
from fundlab.trading import (
    AccountStatus,
    DecisionSourceType,
    ExecutionProfile,
    OrderStatus,
    ResearchRiskProfile,
    is_rebalance_day,
)
from fundlab.trading.accounting import apply_fill
from fundlab.trading.decision import DecisionValidationContext, build_decision_envelope
from fundlab.trading.execution import execute_quantity, size_target_orders


class Portal(Protocol):
    data_version: str

    def get_trading_days(self, start_date: str | date, end_date: str | date) -> list[str]: ...
    def is_trading_day(self, value: str | date) -> bool: ...
    def next_trading_day(self, value: str | date) -> str | None: ...
    def get_universe(self, value: str | date) -> list[str]: ...
    def get_open_price_for_execution(self, symbol: str, value: str | date) -> float | None: ...
    def get_close_price_for_valuation(self, symbol: str, value: str | date) -> float | None: ...
    def get_features(self, symbols: Sequence[str], value: str | date, fields=None): ...


class PreflightError(RuntimeError):
    """A system-level error detected before account writes."""


@dataclass(frozen=True)
class AccountDayResult:
    account_id: str
    trade_date: str
    status: str
    reused: bool = False
    error: str | None = None


@dataclass(frozen=True)
class DailyRunResult:
    trade_date: str
    batch_id: str
    status: str
    data_version: str
    accounts: tuple[AccountDayResult, ...]
    reused: bool = False


@dataclass(frozen=True)
class ReplayRunResult:
    account_id: str
    ledger_version: int
    parent_version: int
    requested_start_date: str
    target_date: str
    rebuilt_start_date: str
    required_dates: tuple[str, ...]
    status: str
    activated: bool = False
    reused: bool = False
    error: str | None = None


class ReplayRunError(RuntimeError):
    def __init__(self, result: ReplayRunResult):
        super().__init__(result.error or "paper replay failed")
        self.result = result


class _BoundUniversePortal:
    def __init__(self, portal: Portal, symbols: tuple[str, ...]) -> None:
        self._portal = portal
        self._symbols = symbols

    def get_universe(self, value: str | date) -> list[str]:
        return list(self._symbols)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._portal, name)


class DailyPaperRunner:
    """Atomic multi-account T/T+1 daily runner over one pinned complete portal."""

    def __init__(
        self,
        repository: PaperLedgerRepository,
        portal_factory: Callable[[date], Portal] | Callable[[], Portal],
        strategy_registry: StrategyRegistry | None = None,
        system_finalize: Callable[[PaperLedgerRepository, str], None] | None = None,
    ) -> None:
        self.repository = repository
        self.portal_factory = portal_factory
        self.strategies = strategy_registry or StrategyRegistry()
        self.system_finalize = system_finalize

    def run_date(self, trade_date: date | str) -> DailyRunResult:
        day = _as_date(trade_date)
        try:
            portal = self._preflight(day)
        except PreflightError as exc:
            self._record_preflight_failure(day, exc)
            raise
        accounts = self.repository.list_accounts()
        eligible = tuple(a.account_id for a in accounts if a.status is not AccountStatus.CLOSED)
        selected_versions = {account.account_id: account.selected_ledger_version for account in accounts}
        prior_completed = {
            (row["account_id"], int(row["ledger_version"]))
            for row in self.repository.table_rows(
                "paper_daily_runs", where="trade_date=? AND status='complete'",
                parameters=(day.isoformat(),),
            )
        }
        fingerprint = _digest({
            "date": day.isoformat(),
            "data_version": portal.data_version,
            "accounts": [(a.account_id, a.selected_ledger_version, a.status.value) for a in accounts],
        })

        def process(repo: PaperLedgerRepository, account: AccountRecord, batch_id: str) -> None:
            self._process_account(repo, account, day, portal, batch_id)

        batch = self.repository.run_date_batch(
            trade_date=day,
            data_version=portal.data_version,
            config_fingerprint=fingerprint,
            account_ids=eligible,
            process_account=process,
            system_finalize=self.system_finalize,
            provenance={"component": "DailyPaperRunner", "entrypoint": "run_date"},
        )
        failed = batch.failed_accounts
        completed = set(batch.completed_accounts)
        skipped = set(batch.skipped_accounts)
        results = tuple(
            AccountDayResult(
                account_id=account_id,
                trade_date=day.isoformat(),
                status="failed" if account_id in failed else "complete",
                reused=(account_id in skipped
                        or (account_id, selected_versions[account_id]) in prior_completed),
                error=failed.get(account_id),
            )
            for account_id in eligible
            if account_id in completed or account_id in skipped or account_id in failed
        )
        result_status = "failed" if any(item.status == "failed" for item in results) else batch.status
        return DailyRunResult(
            day.isoformat(), batch.batch_id, result_status, portal.data_version, results,
            reused=bool(results) and all(item.reused for item in results),
        )

    def backfill(self, start_date: date | str, target_date: date | str) -> tuple[DailyRunResult, ...]:
        start, target = _as_date(start_date), _as_date(target_date)
        if target < start:
            raise ValueError("target_date must not precede start_date")
        try:
            portal = self._open_portal(target)
        except PreflightError as exc:
            self._record_preflight_failure(target, exc)
            raise
        days: list[date] = []
        current = start
        while current <= target:
            try:
                if portal.is_trading_day(current):
                    days.append(current)
            except Exception as exc:
                failure = PreflightError(
                    f"pinned calendar cannot classify {current.isoformat()}: {exc}"
                )
                self._record_preflight_failure(current, failure)
                raise failure from exc
            current += timedelta(days=1)
        return tuple(self.run_date(day) for day in days)

    def replay(
        self, account_id: str, *, start_date: date | str, target_date: date | str, reason: str,
    ) -> ReplayRunResult:
        requested_start, target = _as_date(start_date), _as_date(target_date)
        if target < requested_start:
            raise ValueError("target_date must not precede start_date")
        account = self.repository.get_account(account_id)
        parent_version = account.selected_ledger_version
        parent_dates = self.repository.table_rows(
            "paper_daily_runs",
            where="account_id=? AND ledger_version=? AND status='complete' AND trade_date<=?",
            parameters=(account_id, parent_version, target.isoformat()),
        )
        if not parent_dates:
            raise ValueError("replay requires at least one completed parent-ledger account date")
        rebuilt_start = min(_as_date(row["trade_date"]) for row in parent_dates)
        try:
            portal = self._open_portal(target)
            required = self._trading_dates(portal, rebuilt_start, target)
        except PreflightError as exc:
            self._record_preflight_failure(target, exc)
            raise
        candidates = self.repository.connection.execute(
            """SELECT ledger_version,parent_version FROM paper_ledger_versions
            WHERE account_id=? AND replay_reason=? AND replay_start_date=? AND replay_end_date=?
            ORDER BY ledger_version DESC""",
            (account_id, reason, requested_start.isoformat(), target.isoformat()),
        ).fetchall()
        existing = next(
            (row for row in candidates if int(row["ledger_version"]) == account.selected_ledger_version),
            next((row for row in candidates if int(row["parent_version"]) == parent_version), None),
        )
        if existing is None:
            with self.repository.transaction():
                version = self.repository.create_replay_version(
                    account_id, reason=reason, start_date=requested_start, end_date=target,
                    parent_version=parent_version,
                ).ledger_version
        else:
            version = int(existing["ledger_version"])
            parent_version = int(existing["parent_version"])
        base = dict(
            account_id=account_id, ledger_version=version, parent_version=parent_version,
            requested_start_date=requested_start.isoformat(), target_date=target.isoformat(),
            rebuilt_start_date=rebuilt_start.isoformat(),
            required_dates=tuple(day.isoformat() for day in required),
        )
        validation = self.repository.validate_replay_version(account_id, version, required)
        if account.selected_ledger_version == version and validation.complete:
            return ReplayRunResult(**base, status="complete", activated=True, reused=True)
        try:
            for day in required:
                day_portal = self._preflight(day)

                def process(repo: PaperLedgerRepository, effective: AccountRecord, batch_id: str) -> None:
                    self._process_account(repo, effective, day, day_portal, batch_id)

                batch = self.repository.run_date_batch(
                    trade_date=day, data_version=day_portal.data_version,
                    config_fingerprint=_digest({"replay": version, "date": day.isoformat()}),
                    account_ids=(account_id,), process_account=process,
                    system_finalize=self.system_finalize,
                    ledger_version_overrides={account_id: version},
                    provenance={
                        "component": "DailyPaperRunner", "entrypoint": "replay",
                        "reason": reason, "requested_start_date": requested_start.isoformat(),
                        "target_date": target.isoformat(), "rebuilt_start_date": rebuilt_start.isoformat(),
                    },
                )
                if account_id in batch.failed_accounts:
                    raise RuntimeError(batch.failed_accounts[account_id])
            validation = self.repository.validate_replay_version(account_id, version, required)
            if not validation.complete:
                raise ReplayValidationError(
                    f"missing runs={validation.missing_daily_runs}, snapshots={validation.missing_snapshots}"
                )
            self.repository.activate_replay_version(
                account_id, version, required_dates=required,
                validation=lambda _repo, result: result.complete,
            )
        except Exception as exc:
            with self.repository.transaction():
                self.repository.append_event(
                    account_id, version, target, "replay_failed", "ledger_version", str(version),
                    {"type": type(exc).__name__, "message": str(exc)},
                )
            result = ReplayRunResult(**base, status="failed", error=f"{type(exc).__name__}: {exc}")
            raise ReplayRunError(result) from exc
        return ReplayRunResult(**base, status="complete", activated=True, reused=existing is not None)

    @staticmethod
    def _trading_dates(portal: Portal, start: date, target: date) -> tuple[date, ...]:
        days: list[date] = []
        current = start
        while current <= target:
            try:
                if portal.is_trading_day(current):
                    days.append(current)
            except Exception as exc:
                raise PreflightError(
                    f"pinned calendar cannot classify {current.isoformat()}: {exc}"
                ) from exc
            current += timedelta(days=1)
        return tuple(days)

    def _preflight(self, day: date) -> Portal:
        portal = self._open_portal(day)
        if not portal.is_trading_day(day):
            raise PreflightError(f"target is not a covered trading date: {day.isoformat()}")
        return portal

    def _open_portal(self, day: date) -> Portal:
        try:
            try:
                portal = self.portal_factory(day)  # type: ignore[misc]
            except TypeError:
                portal = self.portal_factory()  # type: ignore[call-arg]
        except Exception as exc:
            raise PreflightError(f"complete published data is unavailable: {exc}") from exc
        if not getattr(portal, "data_version", None):
            raise PreflightError("portal is not pinned to a complete published data version")
        return portal

    def _record_preflight_failure(self, day: date, error: Exception) -> None:
        now = datetime.now(timezone.utc).isoformat()
        payload = json.dumps({"type": type(error).__name__, "message": str(error)},
                             sort_keys=True, separators=(",", ":"))
        scope_key = f"daily:preflight:{day.isoformat()}"
        provenance = json.dumps({
            "operation": "daily", "trade_date": day.isoformat(), "data_version": "unavailable",
            "config_fingerprint": "preflight", "accounts": [],
            "caller": {"component": "DailyPaperRunner", "entrypoint": "preflight"},
        }, sort_keys=True, separators=(",", ":"))
        with self.repository.transaction():
            self.repository.connection.execute(
                """INSERT INTO paper_date_batches
                (batch_id,scope_key,trade_date,operation,account_id,ledger_version,parent_ledger_version,
                 status,data_version,config_fingerprint,provenance_json,started_at,completed_at,error_json)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(scope_key) DO UPDATE SET status='failed',completed_at=excluded.completed_at,
                error_json=excluded.error_json WHERE paper_date_batches.status!='complete'""",
                (_stable_id("preflight", day.isoformat()), scope_key, day.isoformat(), "daily",
                 None, None, None, "failed", "unavailable", "preflight", provenance, now, now, payload),
            )

    def _process_account(
        self, repo: PaperLedgerRepository, account: AccountRecord, day: date,
        portal: Portal, batch_id: str,
    ) -> None:
        version = account.selected_ledger_version
        positions = {
            row["symbol"]: [int(row["quantity"]), float(row["average_cost"])]
            for row in repo.current_positions(account.account_id, version)
        }
        version_snapshots = repo.account_snapshots(account.account_id, version)
        cash = float(version_snapshots[-1]["cash"]) if version_snapshots else float(account.initial_cash)

        if account.status is AccountStatus.PAUSED:
            for order in repo.pending_orders(account.account_id, version):
                repo.update_order_outcome(
                    order["order_id"], status=OrderStatus.CANCELLED,
                    requested_quantity=order["requested_quantity"], actual_quantity=0,
                    outcome_reason="account_paused",
                )
                repo.append_event(account.account_id, version, day, "order_cancelled", "order",
                                  order["order_id"], {"reason": "account_paused"})
            self._value(repo, account, day, portal, cash, positions)
            return

        execution_profile = self._execution_profile(account)
        due = tuple(order for order in repo.pending_orders(account.account_id, version)
                    if order["execution_date"] == day.isoformat())
        due_by_decision: dict[str, list[dict[str, Any]]] = {}
        for order in due:
            due_by_decision.setdefault(order["decision_id"], []).append(order)
        for decision_id, pending in due_by_decision.items():
            decision_row = repo.connection.execute(
                "SELECT decision_date,original_json FROM paper_decisions WHERE decision_id=?",
                (decision_id,),
            ).fetchone()
            if decision_row is None:
                raise ValueError(f"missing decision for pending orders: {decision_id}")
            targets = json.loads(decision_row["original_json"])
            opens = {order["symbol"]: portal.get_open_price_for_execution(order["symbol"], day)
                     for order in pending}
            snapshots = repo.account_snapshots(account.account_id, version)
            total_asset = float(snapshots[-1]["total_asset"]) if snapshots else float(account.initial_cash)
            sized = size_target_orders(
                target_weights=targets,
                positions={symbol: quantity for symbol, (quantity, _) in positions.items()},
                total_asset=total_asset, raw_open_prices=opens, lot_size=execution_profile.lot_size,
            )
            sized_by_symbol = {item.symbol: item for item in sized}
            pending_by_symbol = {order["symbol"]: order for order in pending}
            for symbol in sorted(set(pending_by_symbol) - set(sized_by_symbol)):
                order = pending_by_symbol[symbol]
                missing_open = opens[symbol] is None
                repo.update_order_outcome(order["order_id"],
                                          status=OrderStatus.REJECTED if missing_open else OrderStatus.CANCELLED,
                                          requested_quantity=0, actual_quantity=0,
                                          outcome_reason="missing_raw_open" if missing_open else "no_rebalance_quantity")
            for item in sized:
                order = pending_by_symbol[item.symbol]
                symbol = item.symbol
                side = item.side
                requested = item.requested_quantity
                quantity, average_cost = positions.get(symbol, [0, 0.0])
                raw_open = portal.get_open_price_for_execution(symbol, day)
                frozen_amount = self._frozen_liquidity(portal, symbol, _as_date(decision_row[0]))
                result = execute_quantity(
                    symbol=symbol, side=side, requested_quantity=requested,
                    raw_open_price=raw_open, available_cash=cash, available_position=quantity,
                    frozen_average_amount=frozen_amount, profile=execution_profile,
                )
                repo.update_order_outcome(
                    order["order_id"], status=result.status, requested_quantity=result.requested_quantity,
                    actual_quantity=result.actual_quantity, outcome_reason=";".join(result.risk.codes) or None,
                )
                if result.actual_quantity:
                    applied = apply_fill(cash=cash, position_quantity=quantity, side=side,
                                         quantity=result.actual_quantity, amount=result.amount,
                                         commission=result.commission)
                    cash = applied.cash_after
                    if side == "buy":
                        old_amount = quantity * average_cost
                        average_cost = (old_amount + result.amount + result.commission) / applied.quantity_after
                    quantity = applied.quantity_after
                    if quantity:
                        positions[symbol] = [quantity, average_cost]
                    else:
                        positions.pop(symbol, None)
                    repo.record_fill(
                        fill_id=_stable_id("fill", order["order_id"]), order_id=order["order_id"],
                        account_id=account.account_id, ledger_version=version, fill_date=day,
                        price=float(result.fill_price), quantity=result.actual_quantity,
                        commission=result.commission, slippage=result.slippage,
                        data_version=portal.data_version,
                    )
                    repo.append_event(account.account_id, version, day, "order_filled", "order",
                                      order["order_id"], {"quantity": result.actual_quantity,
                                                           "price": result.fill_price})

        self._value(repo, account, day, portal, cash, positions)
        next_day = self._next_trading_day(portal, day)
        calendar = [day] if next_day is None else [day, next_day]
        if is_rebalance_day(day, calendar, account.schedule):
            self._decide(repo, account, day, portal, cash, positions, execution_profile, batch_id)

    def _value(self, repo, account, day, portal, cash, positions) -> None:
        valued: dict[str, tuple[int, float, float]] = {}
        for symbol, (quantity, average_cost) in positions.items():
            close = portal.get_close_price_for_valuation(symbol, day)
            if close is None:
                raise ValueError(f"missing raw close for {symbol} on {day}")
            valued[symbol] = (quantity, average_cost, close)
        repo.record_daily_state(account_id=account.account_id,
                                ledger_version=account.selected_ledger_version,
                                trade_date=day, cash=cash, positions=valued,
                                data_version=portal.data_version)

    def _decide(self, repo, account, day, portal, cash, positions, profile, batch_id) -> None:
        next_day = self._next_trading_day(portal, day)
        if next_day is None:
            return
        strategy = self.strategies.create(account.bindings.strategy_id,
                                          account.bindings.strategy_config_version)
        universe_metadata = self.strategies.get_universe_metadata(account.bindings.universe_version)
        universe = universe_metadata.symbols
        strategy_portal = _BoundUniversePortal(portal, universe)
        targets = strategy.on_rebalance(day.isoformat(), strategy_portal, {
            "account_id": account.account_id, "cash": cash,
            "positions": {symbol: quantity for symbol, (quantity, _) in positions.items()},
        })
        risk_profile = self._risk_profile(account)
        target_symbols = frozenset(symbol for symbol in targets if symbol != "cash")
        missing_symbols = frozenset(
            symbol for symbol in target_symbols
            if portal.get_close_price_for_valuation(symbol, day) is None
        )
        cross_border = frozenset(universe_metadata.cross_border_symbols & target_symbols)
        trusted_premium = self._trusted_premium_discount_symbols(portal, cross_border, day)
        context = DecisionValidationContext(
            frozenset(universe), missing_symbols=missing_symbols,
            cross_border_symbols=cross_border,
            trusted_premium_discount_symbols=trusted_premium,
        )
        observation = _digest({
            "date": day.isoformat(), "universe": universe, "targets": targets,
            "data_version": portal.data_version, "missing_symbols": sorted(missing_symbols),
            "cross_border_symbols": sorted(cross_border),
            "trusted_premium_discount_symbols": sorted(trusted_premium),
        })
        decision_id = _stable_id("decision", account.account_id,
                                 str(account.selected_ledger_version), day.isoformat())
        decision = build_decision_envelope(
            decision_id=decision_id, account_id=account.account_id,
            source_type=DecisionSourceType.RULE_STRATEGY,
            source_id=account.bindings.strategy_id,
            config_version=account.bindings.strategy_config_version,
            decision_date=day, target_weights=targets, reason="scheduled_rebalance",
            data_version=portal.data_version, observation_hash=observation,
            context=context, profile=risk_profile,
            source_metadata={"batch_id": batch_id},
        )
        repo.record_decision(decision, account.selected_ledger_version)
        if not decision.executable:
            return
        for symbol in sorted(set(positions) | {s for s in targets if s != "cash"}):
            order_id = _stable_id("order", decision_id, symbol)
            repo.record_order(
                order_id=order_id, decision_id=decision_id, account_id=account.account_id,
                ledger_version=account.selected_ledger_version, symbol=symbol,
                original_target_weight=float(targets.get(symbol, 0.0)), execution_date=next_day,
                requested_quantity=None, status=OrderStatus.PENDING,
            )

    def _execution_profile(self, account: AccountRecord) -> ExecutionProfile:
        row = self.repository.connection.execute(
            "SELECT payload_json FROM paper_execution_profiles WHERE profile_id=? AND version=?",
            (account.execution_profile_id, account.bindings.execution_profile_version),
        ).fetchone()
        if row is None:
            raise ValueError("bound execution profile is unavailable")
        return ExecutionProfile(**json.loads(row[0]))

    def _risk_profile(self, account: AccountRecord) -> ResearchRiskProfile:
        row = self.repository.connection.execute(
            "SELECT payload_json FROM paper_risk_profiles WHERE profile_id=? AND version=?",
            (account.risk_profile_id, account.bindings.risk_profile_version),
        ).fetchone()
        if row is None:
            raise ValueError("bound risk profile is unavailable")
        return ResearchRiskProfile(**json.loads(row[0]))

    @staticmethod
    def _frozen_liquidity(portal: Portal, symbol: str, decision_date: date) -> float | None:
        frame = portal.get_features([symbol], decision_date, fields=["amount_avg_20d"])
        if getattr(frame, "empty", True):
            return None
        if symbol in frame.index:
            value = frame.loc[symbol, "amount_avg_20d"]
        else:
            value = frame.iloc[0]["amount_avg_20d"]
        return None if value is None else float(value)

    @staticmethod
    def _trusted_premium_discount_symbols(
        portal: Portal, symbols: frozenset[str], decision_date: date,
    ) -> frozenset[str]:
        if not symbols:
            return frozenset()
        try:
            frame = portal.get_features(sorted(symbols), decision_date)
        except Exception:
            return frozenset()
        if getattr(frame, "empty", True) or "premium_discount" not in frame.columns:
            return frozenset()
        trusted: set[str] = set()
        for symbol in symbols:
            if symbol not in frame.index:
                continue
            value = frame.loc[symbol, "premium_discount"]
            if value is None or value != value:
                continue
            trusted.add(symbol)
        return frozenset(trusted)

    @staticmethod
    def _next_trading_day(portal: Portal, day: date) -> date | None:
        try:
            resolved = portal.next_trading_day(day)
        except (TypeError, ValueError):
            resolved = None
        else:
            return None if resolved is None else _as_date(resolved)
        candidate = day + timedelta(days=1)
        limit = day + timedelta(days=366)
        while candidate <= limit:
            if portal.is_trading_day(candidate):
                return candidate
            candidate += timedelta(days=1)
        return None


def _as_date(value: date | str) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str,
                                     separators=(",", ":")).encode()).hexdigest()


def _stable_id(*parts: str) -> str:
    return uuid.uuid5(uuid.NAMESPACE_URL, ":".join(parts)).hex
