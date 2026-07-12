from __future__ import annotations

import json
import hashlib
import sqlite3
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, is_dataclass, replace
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Mapping, Sequence, TypeVar

from fundlab.paper.schema import initialize_schema
from fundlab.trading import (
    AccountBindings,
    AccountStatus,
    DecisionEnvelope,
    ExecutionProfile,
    OrderStatus,
    RebalanceFrequency,
    ResearchRiskProfile,
)


T = TypeVar("T")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _date(value: date | str) -> str:
    return value.isoformat() if isinstance(value, date) else str(value)


def _json(value: Any) -> str:
    if is_dataclass(value):
        value = asdict(value)
    if isinstance(value, Mapping):
        value = {str(key): _json_value(item) for key, item in value.items()}
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _json_value(value: Any) -> Any:
    if is_dataclass(value):
        return _json_value(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _loaded(value: str | None) -> Any:
    return None if value is None else json.loads(value)


@dataclass(frozen=True)
class AccountRecord:
    account_id: str
    name: str
    status: AccountStatus
    initial_cash: float
    cash: float
    bindings: AccountBindings
    execution_profile_id: str
    risk_profile_id: str
    schedule: RebalanceFrequency
    selected_ledger_version: int
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class LedgerVersionRecord:
    account_id: str
    ledger_version: int
    parent_version: int | None
    replay_reason: str | None
    replay_start_date: str | None
    replay_end_date: str | None
    created_at: str


@dataclass(frozen=True)
class DateBatchResult:
    batch_id: str
    trade_date: str
    status: str
    completed_accounts: tuple[str, ...]
    failed_accounts: Mapping[str, str]
    skipped_accounts: tuple[str, ...]


@dataclass(frozen=True)
class ReplayValidationResult:
    account_id: str
    ledger_version: int
    required_dates: tuple[str, ...]
    missing_daily_runs: tuple[str, ...]
    missing_snapshots: tuple[str, ...]

    @property
    def complete(self) -> bool:
        return not self.missing_daily_runs and not self.missing_snapshots


class ImmutableHistoryError(RuntimeError):
    pass


class ReplayValidationError(RuntimeError):
    pass


class PaperLedgerRepository:
    """The sole SQLite authority for paper accounts and their versioned ledgers.

    The repository owns one connection.  Callers should not share an instance
    between threads; independent accounts are isolated with SQLite savepoints,
    not separate connections.
    """

    def __init__(self, database: str | Path | sqlite3.Connection):
        if isinstance(database, sqlite3.Connection):
            self.connection = database
            self.database_path: Path | None = None
        else:
            path = Path(database).expanduser().resolve()
            path.parent.mkdir(parents=True, exist_ok=True)
            self.database_path = path
            self.connection = sqlite3.connect(path, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA busy_timeout = 5000")
        initialize_schema(self.connection)
        self._install_immutability_guards()

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "PaperLedgerRepository":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _install_immutability_guards(self) -> None:
        self.connection.executescript("""
        CREATE TRIGGER IF NOT EXISTS paper_accounts_immutable_bindings
        BEFORE UPDATE OF strategy_id, strategy_config_version, universe_version,
          benchmark_symbol, execution_profile_id, execution_profile_version,
          risk_profile_id, risk_profile_version, schedule, initial_cash
        ON paper_accounts BEGIN
          SELECT RAISE(ABORT, 'account bindings are immutable');
        END;
        CREATE TRIGGER IF NOT EXISTS paper_execution_profiles_no_update
        BEFORE UPDATE ON paper_execution_profiles BEGIN SELECT RAISE(ABORT, 'profile is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS paper_execution_profiles_no_delete
        BEFORE DELETE ON paper_execution_profiles BEGIN SELECT RAISE(ABORT, 'profile is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS paper_risk_profiles_no_update
        BEFORE UPDATE ON paper_risk_profiles BEGIN SELECT RAISE(ABORT, 'profile is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS paper_risk_profiles_no_delete
        BEFORE DELETE ON paper_risk_profiles BEGIN SELECT RAISE(ABORT, 'profile is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS paper_fills_no_update
        BEFORE UPDATE ON paper_fills BEGIN SELECT RAISE(ABORT, 'fill history is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS paper_fills_no_delete
        BEFORE DELETE ON paper_fills BEGIN SELECT RAISE(ABORT, 'fill history is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS paper_events_no_update
        BEFORE UPDATE ON paper_event_ledger BEGIN SELECT RAISE(ABORT, 'event history is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS paper_events_no_delete
        BEFORE DELETE ON paper_event_ledger BEGIN SELECT RAISE(ABORT, 'event history is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS paper_snapshots_no_update
        BEFORE UPDATE ON paper_account_snapshots BEGIN SELECT RAISE(ABORT, 'snapshot history is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS paper_snapshots_no_delete
        BEFORE DELETE ON paper_account_snapshots BEGIN SELECT RAISE(ABORT, 'snapshot history is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS paper_daily_positions_no_update
        BEFORE UPDATE ON paper_daily_positions BEGIN SELECT RAISE(ABORT, 'position history is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS paper_daily_positions_no_delete
        BEFORE DELETE ON paper_daily_positions BEGIN SELECT RAISE(ABORT, 'position history is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS paper_decisions_no_update
        BEFORE UPDATE ON paper_decisions BEGIN SELECT RAISE(ABORT, 'decision history is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS paper_decisions_no_delete
        BEFORE DELETE ON paper_decisions BEGIN SELECT RAISE(ABORT, 'decision history is immutable'); END;
        """)

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[None]:
        if self.connection.in_transaction:
            raise RuntimeError("nested outer transactions are not supported; use savepoint()")
        self.connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
        try:
            yield
        except BaseException:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    @contextmanager
    def savepoint(self, name: str | None = None) -> Iterator[None]:
        token = name or f"sp_{uuid.uuid4().hex}"
        if not token.replace("_", "").isalnum():
            raise ValueError("savepoint name must be alphanumeric/underscore")
        self.connection.execute(f'SAVEPOINT "{token}"')
        try:
            yield
        except BaseException:
            self.connection.execute(f'ROLLBACK TO SAVEPOINT "{token}"')
            self.connection.execute(f'RELEASE SAVEPOINT "{token}"')
            raise
        else:
            self.connection.execute(f'RELEASE SAVEPOINT "{token}"')

    def register_execution_profile(self, profile: ExecutionProfile) -> bool:
        payload = _json(profile)
        row = self.connection.execute(
            "SELECT payload_json FROM paper_execution_profiles WHERE profile_id=? AND version=?",
            (profile.profile_id, profile.version),
        ).fetchone()
        if row:
            if row[0] != payload:
                raise ImmutableHistoryError("execution profile version already exists with different content")
            return False
        self.connection.execute(
            "INSERT INTO paper_execution_profiles VALUES (?,?,?,?)",
            (profile.profile_id, profile.version, payload, _utc_now()),
        )
        return True

    def register_risk_profile(self, profile: ResearchRiskProfile) -> bool:
        payload = _json(profile)
        row = self.connection.execute(
            "SELECT payload_json FROM paper_risk_profiles WHERE profile_id=? AND version=?",
            (profile.profile_id, profile.version),
        ).fetchone()
        if row:
            if row[0] != payload:
                raise ImmutableHistoryError("risk profile version already exists with different content")
            return False
        self.connection.execute(
            "INSERT INTO paper_risk_profiles VALUES (?,?,?,?)",
            (profile.profile_id, profile.version, payload, _utc_now()),
        )
        return True

    def create_account(
        self, *, account_id: str, name: str, initial_cash: float,
        bindings: AccountBindings, execution_profile_id: str, risk_profile_id: str,
        schedule: RebalanceFrequency | str = RebalanceFrequency.DAILY,
    ) -> AccountRecord:
        if not account_id or not name or initial_cash <= 0:
            raise ValueError("account identity must be non-empty and initial_cash positive")
        schedule_value = RebalanceFrequency(schedule).value
        now = _utc_now()
        with self.transaction():
            self.connection.execute(
                """INSERT INTO paper_accounts
                (account_id,name,status,initial_cash,cash,strategy_id,strategy_config_version,
                 universe_version,benchmark_symbol,execution_profile_id,execution_profile_version,
                 risk_profile_id,risk_profile_version,schedule,selected_ledger_version,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (account_id, name, AccountStatus.ACTIVE.value, initial_cash, initial_cash,
                 bindings.strategy_id, bindings.strategy_config_version, bindings.universe_version,
                 bindings.benchmark_symbol, execution_profile_id, bindings.execution_profile_version,
                 risk_profile_id, bindings.risk_profile_version, schedule_value, 1, now, now),
            )
            self.connection.execute(
                "INSERT INTO paper_ledger_versions VALUES (?,?,?,?,?,?,?)",
                (account_id, 1, None, None, None, None, now),
            )
            self.append_event(account_id, 1, date.today(), "account_created", "account", account_id,
                              {"initial_cash": initial_cash, "status": "active"})
        return self.get_account(account_id)

    def get_account(self, account_id: str) -> AccountRecord:
        row = self.connection.execute("SELECT * FROM paper_accounts WHERE account_id=?", (account_id,)).fetchone()
        if row is None:
            raise KeyError(f"unknown paper account: {account_id}")
        return self._account(row)

    def list_accounts(self, status: AccountStatus | str | None = None) -> tuple[AccountRecord, ...]:
        if status is None:
            rows = self.connection.execute("SELECT * FROM paper_accounts ORDER BY account_id").fetchall()
        else:
            rows = self.connection.execute(
                "SELECT * FROM paper_accounts WHERE status=? ORDER BY account_id",
                (AccountStatus(status).value,),
            ).fetchall()
        return tuple(self._account(row) for row in rows)

    @staticmethod
    def _account(row: sqlite3.Row) -> AccountRecord:
        return AccountRecord(
            account_id=row["account_id"], name=row["name"], status=AccountStatus(row["status"]),
            initial_cash=row["initial_cash"], cash=row["cash"],
            bindings=AccountBindings(
                row["strategy_id"], row["strategy_config_version"], row["universe_version"],
                row["benchmark_symbol"], row["execution_profile_version"], row["risk_profile_version"],
            ),
            execution_profile_id=row["execution_profile_id"], risk_profile_id=row["risk_profile_id"],
            schedule=RebalanceFrequency(row["schedule"]), selected_ledger_version=row["selected_ledger_version"],
            created_at=row["created_at"], updated_at=row["updated_at"],
        )

    def set_account_status(self, account_id: str, status: AccountStatus | str) -> AccountRecord:
        value = AccountStatus(status)
        current = self.get_account(account_id)
        allowed = {
            AccountStatus.ACTIVE: {AccountStatus.PAUSED, AccountStatus.CLOSED},
            AccountStatus.PAUSED: {AccountStatus.ACTIVE, AccountStatus.CLOSED},
            AccountStatus.CLOSED: set(),
        }
        if value is current.status:
            return current
        if value not in allowed[current.status]:
            raise ValueError(f"invalid lifecycle transition: {current.status.value} -> {value.value}")
        now = _utc_now()
        self.connection.execute(
            "UPDATE paper_accounts SET status=?,updated_at=? WHERE account_id=?",
            (value.value, now, account_id),
        )
        self.append_event(account_id, current.selected_ledger_version, date.today(),
                          f"account_{value.value}", "account", account_id,
                          {"from": current.status.value, "to": value.value})
        return self.get_account(account_id)

    def create_replay_version(
        self, account_id: str, *, reason: str, start_date: date | str, end_date: date | str,
        parent_version: int | None = None,
    ) -> LedgerVersionRecord:
        if not reason.strip():
            raise ValueError("replay reason is required")
        account = self.get_account(account_id)
        parent = parent_version or account.selected_ledger_version
        if not self.connection.execute(
            "SELECT 1 FROM paper_ledger_versions WHERE account_id=? AND ledger_version=?", (account_id, parent)
        ).fetchone():
            raise KeyError(f"unknown parent ledger version: {parent}")
        version = self.connection.execute(
            "SELECT COALESCE(MAX(ledger_version),0)+1 FROM paper_ledger_versions WHERE account_id=?", (account_id,)
        ).fetchone()[0]
        now = _utc_now()
        self.connection.execute(
            "INSERT INTO paper_ledger_versions VALUES (?,?,?,?,?,?,?)",
            (account_id, version, parent, reason, _date(start_date), _date(end_date), now),
        )
        return LedgerVersionRecord(account_id, version, parent, reason, _date(start_date), _date(end_date), now)

    def select_ledger_version(self, account_id: str, ledger_version: int) -> AccountRecord:
        version = self.connection.execute(
            "SELECT parent_version FROM paper_ledger_versions WHERE account_id=? AND ledger_version=?",
            (account_id, ledger_version),
        ).fetchone()
        if version is None:
            raise KeyError(f"unknown ledger version: {ledger_version}")
        if version["parent_version"] is not None:
            raise ReplayValidationError("replay versions must be selected with activate_replay_version()")
        return self._set_selected_ledger_version(account_id, ledger_version)

    def _set_selected_ledger_version(self, account_id: str, ledger_version: int) -> AccountRecord:
        latest = self.connection.execute(
            """SELECT cash FROM paper_account_snapshots
            WHERE account_id=? AND ledger_version=? ORDER BY trade_date DESC LIMIT 1""",
            (account_id, ledger_version),
        ).fetchone()
        if latest is None:
            self.connection.execute(
                "UPDATE paper_accounts SET selected_ledger_version=?,updated_at=? WHERE account_id=?",
                (ledger_version, _utc_now(), account_id),
            )
        else:
            self.connection.execute(
                "UPDATE paper_accounts SET selected_ledger_version=?,cash=?,updated_at=? WHERE account_id=?",
                (ledger_version, latest["cash"], _utc_now(), account_id),
            )
        return self.get_account(account_id)

    def validate_replay_version(
        self, account_id: str, ledger_version: int, required_dates: Iterable[date | str],
    ) -> ReplayValidationResult:
        version = self.connection.execute(
            "SELECT parent_version FROM paper_ledger_versions WHERE account_id=? AND ledger_version=?",
            (account_id, ledger_version),
        ).fetchone()
        if version is None:
            raise KeyError(f"unknown ledger version: {ledger_version}")
        if version["parent_version"] is None:
            raise ValueError("validation promotion is only for explicit replay versions")
        dates = tuple(dict.fromkeys(_date(value) for value in required_dates))
        missing_runs = tuple(day for day in dates if not self.connection.execute(
            """SELECT 1 FROM paper_daily_runs
            WHERE account_id=? AND ledger_version=? AND trade_date=? AND status='complete'""",
            (account_id, ledger_version, day),
        ).fetchone())
        missing_snapshots = tuple(day for day in dates if not self.connection.execute(
            "SELECT 1 FROM paper_account_snapshots WHERE account_id=? AND ledger_version=? AND trade_date=?",
            (account_id, ledger_version, day),
        ).fetchone())
        return ReplayValidationResult(account_id, ledger_version, dates, missing_runs, missing_snapshots)

    def activate_replay_version(
        self, account_id: str, ledger_version: int, *, required_dates: Iterable[date | str],
        validation: Callable[["PaperLedgerRepository", ReplayValidationResult], bool] | None = None,
    ) -> AccountRecord:
        dates = tuple(required_dates)
        failure: dict[str, Any] | None = None
        account: AccountRecord | None = None
        with self.transaction():
            result = self.validate_replay_version(account_id, ledger_version, dates)
            try:
                accepted = result.complete and (validation is None or bool(validation(self, result)))
            except Exception as exc:
                accepted = False
                failure = {"type": type(exc).__name__, "message": str(exc)}
            if not accepted:
                failure = failure or {
                    "type": "incomplete_replay",
                    "missing_daily_runs": result.missing_daily_runs,
                    "missing_snapshots": result.missing_snapshots,
                }
                self.append_event(
                    account_id, ledger_version, date.today(), "replay_validation_failed", "ledger_version",
                    str(ledger_version), failure,
                )
            else:
                account = self._set_selected_ledger_version(account_id, ledger_version)
                self.append_event(
                    account_id, ledger_version, result.required_dates[-1] if result.required_dates else date.today(),
                    "replay_activated", "ledger_version", str(ledger_version),
                    {"required_dates": result.required_dates},
                )
        if failure is not None:
            raise ReplayValidationError(f"replay version {ledger_version} failed validation: {failure}")
        assert account is not None
        return account

    def list_ledger_versions(self, account_id: str) -> tuple[LedgerVersionRecord, ...]:
        rows = self.connection.execute(
            "SELECT * FROM paper_ledger_versions WHERE account_id=? ORDER BY ledger_version", (account_id,)
        ).fetchall()
        return tuple(LedgerVersionRecord(**dict(row)) for row in rows)

    def append_event(
        self, account_id: str, ledger_version: int, trade_date: date | str, event_type: str,
        entity_type: str, entity_id: str, payload: Mapping[str, Any], *, event_id: str | None = None,
    ) -> str:
        sequence = self.connection.execute(
            "SELECT COALESCE(MAX(event_sequence),0)+1 FROM paper_event_ledger WHERE account_id=? AND ledger_version=?",
            (account_id, ledger_version),
        ).fetchone()[0]
        identity = event_id or uuid.uuid4().hex
        self.connection.execute(
            "INSERT INTO paper_event_ledger VALUES (?,?,?,?,?,?,?,?,?,?)",
            (identity, account_id, ledger_version, sequence, _date(trade_date), event_type,
             entity_type, entity_id, _json(payload), _utc_now()),
        )
        return identity

    def list_events(self, account_id: str, ledger_version: int | None = None) -> tuple[dict[str, Any], ...]:
        version = ledger_version or self.get_account(account_id).selected_ledger_version
        rows = self.connection.execute(
            "SELECT * FROM paper_event_ledger WHERE account_id=? AND ledger_version=? ORDER BY event_sequence",
            (account_id, version),
        ).fetchall()
        return tuple({**dict(row), "payload": _loaded(row["payload_json"])} for row in rows)

    def record_decision(self, decision: DecisionEnvelope, ledger_version: int) -> bool:
        values = (
            decision.decision_id, decision.account_id, ledger_version, decision.decision_date.isoformat(),
            decision.source_type.value, decision.source_id, decision.config_version,
            _json(decision.target_weights), decision.reason, decision.data_version,
            decision.observation_hash, _json(decision.validation), decision.original_json, _utc_now(),
        )
        try:
            self.connection.execute("INSERT INTO paper_decisions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", values)
        except sqlite3.IntegrityError:
            row = self.connection.execute(
                "SELECT * FROM paper_decisions WHERE account_id=? AND ledger_version=? AND decision_date=?",
                (decision.account_id, ledger_version, decision.decision_date.isoformat()),
            ).fetchone()
            if row and tuple(row[key] for key in (
                "decision_id", "account_id", "ledger_version", "decision_date", "source_type", "source_id",
                "config_version", "target_weights_json", "reason", "data_version", "observation_hash",
                "validation_json", "original_json",
            )) == values[:-1]:
                return False
            raise ImmutableHistoryError("a different decision already exists for account/date/version")
        return True

    def record_order(
        self, *, order_id: str, decision_id: str, account_id: str, ledger_version: int,
        symbol: str, original_target_weight: float, execution_date: date | str,
        requested_quantity: int | None = None, actual_quantity: int | None = None,
        status: OrderStatus | str = OrderStatus.PENDING, outcome_reason: str | None = None,
    ) -> bool:
        cursor = self.connection.execute(
            """INSERT OR IGNORE INTO paper_orders
            (order_id,decision_id,account_id,ledger_version,symbol,original_target_weight,
             requested_quantity,actual_quantity,status,outcome_reason,execution_date,created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (order_id, decision_id, account_id, ledger_version, symbol, original_target_weight,
             requested_quantity, actual_quantity, OrderStatus(status).value, outcome_reason,
             _date(execution_date), _utc_now()),
        )
        return cursor.rowcount == 1

    def update_order_outcome(
        self, order_id: str, *, status: OrderStatus | str, requested_quantity: int | None,
        actual_quantity: int | None, outcome_reason: str | None = None,
    ) -> None:
        current = self.connection.execute("SELECT status FROM paper_orders WHERE order_id=?", (order_id,)).fetchone()
        if current is None:
            raise KeyError(f"unknown order: {order_id}")
        if current[0] != OrderStatus.PENDING.value:
            raise ImmutableHistoryError("only pending orders can receive an execution outcome")
        self.connection.execute(
            "UPDATE paper_orders SET status=?,requested_quantity=?,actual_quantity=?,outcome_reason=? WHERE order_id=?",
            (OrderStatus(status).value, requested_quantity, actual_quantity, outcome_reason, order_id),
        )

    def pending_orders(self, account_id: str, ledger_version: int | None = None) -> tuple[dict[str, Any], ...]:
        version = ledger_version or self.get_account(account_id).selected_ledger_version
        rows = self.connection.execute(
            "SELECT * FROM paper_orders WHERE account_id=? AND ledger_version=? AND status='pending' ORDER BY created_at,order_id",
            (account_id, version),
        ).fetchall()
        return tuple(dict(row) for row in rows)

    def record_fill(
        self, *, fill_id: str, order_id: str, account_id: str, ledger_version: int,
        fill_date: date | str, price: float, quantity: int, commission: float,
        slippage: float, data_version: str,
    ) -> bool:
        cursor = self.connection.execute(
            "INSERT OR IGNORE INTO paper_fills VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (fill_id, order_id, account_id, ledger_version, _date(fill_date), price, quantity,
             commission, slippage, data_version, _utc_now()),
        )
        return cursor.rowcount == 1

    def replace_current_positions(
        self, account_id: str, ledger_version: int, positions: Mapping[str, tuple[int, float]],
    ) -> None:
        self.connection.execute(
            "DELETE FROM paper_current_positions WHERE account_id=? AND ledger_version=?", (account_id, ledger_version)
        )
        now = _utc_now()
        self.connection.executemany(
            "INSERT INTO paper_current_positions VALUES (?,?,?,?,?,?)",
            ((account_id, ledger_version, symbol, quantity, average_cost, now)
             for symbol, (quantity, average_cost) in positions.items() if quantity > 0),
        )

    def current_positions(self, account_id: str, ledger_version: int | None = None) -> tuple[dict[str, Any], ...]:
        version = ledger_version or self.get_account(account_id).selected_ledger_version
        return tuple(dict(row) for row in self.connection.execute(
            "SELECT * FROM paper_current_positions WHERE account_id=? AND ledger_version=? ORDER BY symbol",
            (account_id, version),
        ))

    def record_daily_state(
        self, *, account_id: str, ledger_version: int, trade_date: date | str, cash: float,
        positions: Mapping[str, tuple[int, float, float]], data_version: str,
        benchmark_value: float | None = None,
    ) -> bool:
        day = _date(trade_date)
        existing = self.connection.execute(
            "SELECT 1 FROM paper_account_snapshots WHERE account_id=? AND ledger_version=? AND trade_date=?",
            (account_id, ledger_version, day),
        ).fetchone()
        if existing:
            return False
        now = _utc_now()
        market_value = sum(quantity * close for quantity, _, close in positions.values())
        initial_cash = self.get_account(account_id).initial_cash
        total_asset = cash + market_value
        self.connection.executemany(
            "INSERT INTO paper_daily_positions VALUES (?,?,?,?,?,?,?,?)",
            ((account_id, ledger_version, day, symbol, quantity, average_cost, close,
              quantity * close) for symbol, (quantity, average_cost, close) in positions.items() if quantity > 0),
        )
        self.connection.execute(
            "INSERT INTO paper_account_snapshots VALUES (?,?,?,?,?,?,?,?,?,?)",
            (account_id, ledger_version, day, cash, market_value, total_asset,
             total_asset / initial_cash, benchmark_value, data_version, now),
        )
        self.replace_current_positions(
            account_id, ledger_version,
            {symbol: (quantity, average_cost) for symbol, (quantity, average_cost, _) in positions.items()},
        )
        self.connection.execute(
            """UPDATE paper_accounts SET cash=?,updated_at=?
            WHERE account_id=? AND selected_ledger_version=?""",
            (cash, now, account_id, ledger_version),
        )
        return True

    def account_snapshots(self, account_id: str, ledger_version: int | None = None) -> tuple[dict[str, Any], ...]:
        version = ledger_version or self.get_account(account_id).selected_ledger_version
        return tuple(dict(row) for row in self.connection.execute(
            "SELECT * FROM paper_account_snapshots WHERE account_id=? AND ledger_version=? ORDER BY trade_date",
            (account_id, version),
        ))

    def mark_daily_run(
        self, *, account_id: str, ledger_version: int, trade_date: date | str, batch_id: str,
        status: str, data_version: str, error: Mapping[str, Any] | None = None,
    ) -> bool:
        day = _date(trade_date)
        now = _utc_now()
        cursor = self.connection.execute(
            """INSERT INTO paper_daily_runs
            (account_id,ledger_version,trade_date,batch_id,status,data_version,started_at,completed_at,error_json)
            VALUES (?,?,?,?,?,?,?,?,?)""",
            (account_id, ledger_version, day, batch_id, status, data_version, now,
             now if status != "running" else None, _json(error) if error else None),
            ) if not self.connection.execute(
                "SELECT 1 FROM paper_daily_runs WHERE account_id=? AND ledger_version=? AND trade_date=?",
                (account_id, ledger_version, day),
            ).fetchone() else self.connection.execute(
                """UPDATE paper_daily_runs SET batch_id=?,status=?,data_version=?,started_at=?,completed_at=?,error_json=?
                WHERE account_id=? AND ledger_version=? AND trade_date=? AND status!='complete'""",
                (batch_id, status, data_version, now, now if status != "running" else None,
                 _json(error) if error else None, account_id, ledger_version, day),
            )
        return cursor.rowcount == 1

    def run_date_batch(
        self, *, trade_date: date | str, data_version: str, config_fingerprint: str,
        account_ids: Iterable[str], process_account: Callable[["PaperLedgerRepository", AccountRecord, str], T],
        system_finalize: Callable[["PaperLedgerRepository", str], None] | None = None,
        batch_id: str | None = None, ledger_version_overrides: Mapping[str, int] | None = None,
        scope_key: str | None = None, provenance: Mapping[str, Any] | None = None,
    ) -> DateBatchResult:
        """Run one atomic date batch with isolated account savepoints.

        ``process_account`` writes through this repository. Its exceptions are
        account failures: all writes in that savepoint are removed and one
        failed daily-run row is persisted. Exceptions outside an account
        callback (including ``system_finalize``) roll back the complete batch;
        the failed batch is then recorded in a separate transaction.
        """
        day = _date(trade_date)
        requested_accounts = tuple(account_ids)
        overrides = dict(ledger_version_overrides or {})
        unknown_overrides = set(overrides) - set(requested_accounts)
        if unknown_overrides:
            raise ValueError(f"ledger version overrides include unrequested accounts: {sorted(unknown_overrides)}")
        effective_versions: dict[str, int] = {}
        for account_id in requested_accounts:
            selected = self.get_account(account_id).selected_ledger_version
            version = overrides.get(account_id, selected)
            if not self.connection.execute(
                "SELECT 1 FROM paper_ledger_versions WHERE account_id=? AND ledger_version=?",
                (account_id, version),
            ).fetchone():
                raise KeyError(f"unknown ledger version for {account_id}: {version}")
            effective_versions[account_id] = version
        operation = "replay" if overrides else "daily"
        replay_account_id: str | None = None
        replay_version: int | None = None
        parent_version: int | None = None
        if operation == "replay":
            if len(requested_accounts) != 1 or set(overrides) != set(requested_accounts):
                raise ValueError("a replay date batch must target exactly one explicitly overridden account")
            replay_account_id = requested_accounts[0]
            replay_version = effective_versions[replay_account_id]
            version_row = self.connection.execute(
                "SELECT parent_version FROM paper_ledger_versions WHERE account_id=? AND ledger_version=?",
                (replay_account_id, replay_version),
            ).fetchone()
            parent_version = version_row["parent_version"]
            if parent_version is None:
                raise ValueError("replay batches require an explicit child ledger version")
        scope_input = {
            "operation": operation,
            "trade_date": day,
            "data_version": data_version,
            "config_fingerprint": config_fingerprint,
            "accounts": sorted(effective_versions.items()),
        }
        if scope_key is None:
            if operation == "replay":
                scope_key = f"replay:{replay_account_id}:{replay_version}:{day}"
            else:
                digest = hashlib.sha256(_json(scope_input).encode("utf-8")).hexdigest()
                scope_key = f"daily:{digest}"
        if not scope_key.strip():
            raise ValueError("scope_key cannot be empty")
        provenance_json = _json({**scope_input, "caller": dict(provenance or {})})
        prior = self.connection.execute(
            "SELECT * FROM paper_date_batches WHERE scope_key=?", (scope_key,)
        ).fetchone()
        if prior and (
            prior["operation"] != operation or prior["trade_date"] != day
            or prior["data_version"] != data_version
            or prior["config_fingerprint"] != config_fingerprint
            or prior["provenance_json"] != provenance_json
            or prior["account_id"] != replay_account_id
            or prior["ledger_version"] != replay_version
            or prior["parent_ledger_version"] != parent_version
        ):
            raise ImmutableHistoryError("batch scope already exists with different immutable provenance")
        if prior and batch_id is not None and prior["batch_id"] != batch_id:
            raise ImmutableHistoryError("batch scope already exists with a different batch_id")
        if prior and prior["status"] == "complete":
            for account_id in requested_accounts:
                canonical = self.connection.execute(
                    """SELECT status,data_version FROM paper_daily_runs
                    WHERE account_id=? AND ledger_version=? AND trade_date=?""",
                    (account_id, effective_versions[account_id], day),
                ).fetchone()
                if canonical is None or canonical["status"] not in {"complete", "skipped"}:
                    raise ImmutableHistoryError(
                        "completed batch is terminal but lacks the requested canonical account result"
                    )
                if canonical["data_version"] != data_version:
                    raise ImmutableHistoryError(
                        "completed batch canonical account result has a different data version"
                    )
            return DateBatchResult(
                prior["batch_id"], day, "complete", (), {}, tuple(requested_accounts)
            )
        identity = batch_id or (prior["batch_id"] if prior else uuid.uuid4().hex)
        completed: list[str] = []
        skipped: list[str] = []
        failed: dict[str, str] = {}
        try:
            with self.transaction():
                if prior:
                    self.connection.execute(
                        """UPDATE paper_date_batches SET status='running',started_at=?,completed_at=NULL,error_json=NULL
                        WHERE batch_id=? AND status!='complete'""",
                        (_utc_now(), identity),
                    )
                else:
                    self.connection.execute(
                        "INSERT INTO paper_date_batches VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (identity, scope_key, day, operation, replay_account_id, replay_version, parent_version,
                         "running", data_version, config_fingerprint, provenance_json, _utc_now(), None, None),
                    )
                for account_id in requested_accounts:
                    account = self.get_account(account_id)
                    version = effective_versions[account_id]
                    effective_account = replace(account, selected_ledger_version=version)
                    prior_run = self.connection.execute(
                        "SELECT status,data_version FROM paper_daily_runs WHERE account_id=? AND ledger_version=? AND trade_date=?",
                        (account_id, version, day),
                    ).fetchone()
                    if prior_run and prior_run[0] == "complete":
                        if prior_run["data_version"] != data_version:
                            raise ImmutableHistoryError(
                                "canonical account result has a different data version"
                            )
                        skipped.append(account_id)
                        continue
                    try:
                        with self.savepoint(f"account_{uuid.uuid4().hex}"):
                            process_account(self, effective_account, identity)
                            self.mark_daily_run(account_id=account_id, ledger_version=version, trade_date=day,
                                                batch_id=identity, status="complete", data_version=data_version)
                        completed.append(account_id)
                    except Exception as exc:
                        failed[account_id] = f"{type(exc).__name__}: {exc}"
                        self.mark_daily_run(account_id=account_id, ledger_version=version, trade_date=day,
                                            batch_id=identity, status="failed", data_version=data_version,
                                            error={"type": type(exc).__name__, "message": str(exc)})
                if system_finalize:
                    system_finalize(self, identity)
                terminal_status = "failed" if failed else "complete"
                self.connection.execute(
                    "UPDATE paper_date_batches SET status=?,completed_at=? WHERE batch_id=? AND status!='complete'",
                    (terminal_status, _utc_now(), identity),
                )
        except BaseException as exc:
            with self.transaction():
                self.connection.execute(
                    """INSERT INTO paper_date_batches
                    (batch_id,scope_key,trade_date,operation,account_id,ledger_version,parent_ledger_version,
                     status,data_version,config_fingerprint,provenance_json,started_at,completed_at,error_json)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(scope_key) DO UPDATE SET status='failed',completed_at=excluded.completed_at,
                    error_json=excluded.error_json WHERE paper_date_batches.status!='complete'""",
                    (identity, scope_key, day, operation, replay_account_id, replay_version, parent_version,
                     "failed", data_version, config_fingerprint, provenance_json, _utc_now(), _utc_now(),
                     _json({"type": type(exc).__name__, "message": str(exc)})),
                )
            raise
        return DateBatchResult(identity, day, "failed" if failed else "complete",
                               tuple(completed), failed, tuple(skipped))

    def table_rows(self, table: str, *, where: str = "", parameters: Sequence[Any] = ()) -> tuple[dict[str, Any], ...]:
        allowed = {row[0] for row in self.connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if table not in allowed or (where and any(token in where for token in (";", "--", "/*"))):
            raise ValueError("unsafe table query")
        sql = f'SELECT * FROM "{table}"' + (f" WHERE {where}" if where else "")
        return tuple(dict(row) for row in self.connection.execute(sql, tuple(parameters)))
