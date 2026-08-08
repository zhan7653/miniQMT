from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import StrEnum
import json
from pathlib import Path
import sqlite3
from typing import Any, Iterable, Iterator, Mapping

from fundlab.common.canonical import canonical_json, stable_digest
from fundlab.trading.state import (
    Entitlement,
    ExecutionStatus,
    LedgerEvent,
    Order,
    PortfolioState,
    PositionLot,
    Side,
    Valuation,
)


class AccountStatus(StrEnum):
    ACTIVE = "active"
    PAUSED = "paused"
    CLOSED = "closed"


class RunMode(StrEnum):
    HISTORICAL = "historical"
    DAILY = "daily"


class RunStatus(StrEnum):
    RUNNING = "running"
    COMPLETE = "complete"
    FAILED = "failed"


@dataclass(frozen=True)
class AccountRecord:
    account_id: str
    name: str
    status: AccountStatus
    initial_state: PortfolioState
    selected_run_id: str | None


@dataclass(frozen=True)
class RunBinding:
    account_id: str
    mode: RunMode
    snapshot_id: str
    strategy_id: str
    strategy_version: str
    strategy_config_hash: str
    execution_policy_hash: str
    risk_policy_hash: str
    fee_schedule_hash: str
    initial_state_hash: str
    start_date: date
    end_date: date
    seed: int
    parent_run_id: str | None = None

    def __post_init__(self) -> None:
        identities = (
            self.account_id, self.snapshot_id, self.strategy_id, self.strategy_version,
            self.strategy_config_hash, self.execution_policy_hash, self.risk_policy_hash,
            self.fee_schedule_hash, self.initial_state_hash,
        )
        if not all(item.strip() for item in identities):
            raise ValueError("Run binding identities and hashes cannot be empty")
        if self.start_date > self.end_date:
            raise ValueError("Run binding start_date must not exceed end_date")

    @property
    def binding_hash(self) -> str:
        return stable_digest(self)


@dataclass(frozen=True)
class RunRecord:
    run_id: str
    binding: RunBinding
    attempt: int
    status: RunStatus
    final_state_hash: str | None
    result_hash: str | None
    error: str | None


@dataclass(frozen=True)
class BeginRunResult:
    record: RunRecord
    created: bool


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS trading_accounts (
    account_id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK(status IN ('active','paused','closed')),
    initial_state_json TEXT NOT NULL,
    initial_state_hash TEXT NOT NULL,
    selected_run_id TEXT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(selected_run_id) REFERENCES simulation_runs(run_id)
);
CREATE TABLE IF NOT EXISTS simulation_runs (
    run_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    binding_hash TEXT NOT NULL,
    binding_json TEXT NOT NULL,
    attempt INTEGER NOT NULL CHECK(attempt >= 1),
    status TEXT NOT NULL CHECK(status IN ('running','complete','failed')),
    initial_state_json TEXT NOT NULL,
    initial_state_hash TEXT NOT NULL,
    final_state_json TEXT NULL,
    final_state_hash TEXT NULL,
    result_hash TEXT NULL,
    incomplete_reasons_json TEXT NULL,
    error TEXT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT NULL,
    UNIQUE(binding_hash, attempt),
    FOREIGN KEY(account_id) REFERENCES trading_accounts(account_id)
);
CREATE INDEX IF NOT EXISTS simulation_runs_account_idx ON simulation_runs(account_id, started_at);
CREATE TABLE IF NOT EXISTS trading_ledger_events (
    run_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK(sequence >= 1),
    event_id TEXT NOT NULL UNIQUE,
    session_date TEXT NOT NULL,
    event_type TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL,
    PRIMARY KEY(run_id, sequence),
    FOREIGN KEY(run_id) REFERENCES simulation_runs(run_id)
);
CREATE TABLE IF NOT EXISTS trading_checkpoints (
    run_id TEXT NOT NULL,
    session_date TEXT NOT NULL,
    state_json TEXT NOT NULL,
    state_hash TEXT NOT NULL,
    cash TEXT NOT NULL,
    market_value TEXT NOT NULL,
    total_equity TEXT NOT NULL,
    nav TEXT NOT NULL,
    stale_instruments_json TEXT NOT NULL,
    PRIMARY KEY(run_id, session_date),
    FOREIGN KEY(run_id) REFERENCES simulation_runs(run_id)
);
CREATE TRIGGER IF NOT EXISTS trading_events_no_update
BEFORE UPDATE ON trading_ledger_events BEGIN SELECT RAISE(ABORT, 'ledger events are immutable'); END;
CREATE TRIGGER IF NOT EXISTS trading_events_no_delete
BEFORE DELETE ON trading_ledger_events BEGIN SELECT RAISE(ABORT, 'ledger events are immutable'); END;
CREATE TRIGGER IF NOT EXISTS trading_checkpoints_no_update
BEFORE UPDATE ON trading_checkpoints BEGIN SELECT RAISE(ABORT, 'checkpoints are immutable'); END;
CREATE TRIGGER IF NOT EXISTS trading_checkpoints_no_delete
BEFORE DELETE ON trading_checkpoints BEGIN SELECT RAISE(ABORT, 'checkpoints are immutable'); END;
CREATE TRIGGER IF NOT EXISTS trading_runs_terminal_no_update
BEFORE UPDATE ON simulation_runs WHEN OLD.status IN ('complete','failed')
BEGIN SELECT RAISE(ABORT, 'terminal runs are immutable'); END;
CREATE TRIGGER IF NOT EXISTS trading_runs_no_delete
BEFORE DELETE ON simulation_runs BEGIN SELECT RAISE(ABORT, 'runs are immutable'); END;
CREATE TRIGGER IF NOT EXISTS trading_events_running_only
BEFORE INSERT ON trading_ledger_events
WHEN (SELECT status FROM simulation_runs WHERE run_id=NEW.run_id) != 'running'
BEGIN SELECT RAISE(ABORT, 'events require a running run'); END;
CREATE TRIGGER IF NOT EXISTS trading_checkpoints_running_only
BEFORE INSERT ON trading_checkpoints
WHEN (SELECT status FROM simulation_runs WHERE run_id=NEW.run_id) != 'running'
BEGIN SELECT RAISE(ABORT, 'checkpoints require a running run'); END;
"""


class TradingRepository:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.executescript(SCHEMA_SQL)

    def create_account(
        self, account_id: str, name: str, initial_state: PortfolioState,
    ) -> AccountRecord:
        if not account_id.strip() or not name.strip():
            raise ValueError("Account identity and name cannot be empty")
        now = _now()
        with self.transaction() as connection:
            connection.execute(
                """INSERT INTO trading_accounts(
                    account_id,name,status,initial_state_json,initial_state_hash,selected_run_id,created_at,updated_at
                ) VALUES (?,?,?,?,?,NULL,?,?)""",
                (account_id, name, AccountStatus.ACTIVE.value, _state_json(initial_state),
                 initial_state.state_hash, now, now),
            )
        return self.account(account_id)

    def account(self, account_id: str) -> AccountRecord:
        with self._connect(read_only=True) as connection:
            row = connection.execute(
                "SELECT * FROM trading_accounts WHERE account_id=?", (account_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"Unknown trading account: {account_id}")
        return AccountRecord(
            row["account_id"], row["name"], AccountStatus(row["status"]),
            _state(row["initial_state_json"]), row["selected_run_id"],
        )

    def set_account_status(self, account_id: str, status: AccountStatus) -> AccountRecord:
        with self.transaction() as connection:
            current = connection.execute(
                "SELECT status FROM trading_accounts WHERE account_id=?", (account_id,),
            ).fetchone()
            if current is None:
                raise KeyError(f"Unknown trading account: {account_id}")
            if current["status"] == AccountStatus.CLOSED.value and status is not AccountStatus.CLOSED:
                raise ValueError("Closed trading accounts cannot be reopened")
            connection.execute(
                "UPDATE trading_accounts SET status=?, updated_at=? WHERE account_id=?",
                (status.value, _now(), account_id),
            )
        return self.account(account_id)

    def abandon_pending_only_head(
        self,
        account_id: str,
        expected_run_id: str,
    ) -> AccountRecord:
        """Move an account head behind an unfilled-only run without deleting history.

        This is deliberately narrower than a general rollback: the selected run
        must start without pending orders, end with at least one, and have no
        other state change once pending orders and the close-price cache are
        removed. The immutable run remains available for audit or recovery.
        """

        account = self.account(account_id)
        if account.selected_run_id != expected_run_id:
            raise ValueError("Trading account head changed before pending-order abandonment")
        run = self.run(expected_run_id)
        if run.binding.account_id != account_id or run.status is not RunStatus.COMPLETE:
            raise ValueError("Pending-order abandonment requires the completed selected run")
        initial = self.initial_state(expected_run_id)
        final = self.final_state(expected_run_id)
        if initial.pending_orders or not final.pending_orders:
            raise ValueError("Selected run is not a new-pending-orders-only head")
        normalized_initial = replace(initial, pending_orders=(), last_prices={})
        normalized_final = replace(final, pending_orders=(), last_prices={})
        if normalized_initial != normalized_final:
            raise ValueError("Selected run contains state changes beyond unfilled orders")
        with self.transaction() as connection:
            current = connection.execute(
                "SELECT selected_run_id FROM trading_accounts WHERE account_id=?",
                (account_id,),
            ).fetchone()
            if current is None or current["selected_run_id"] != expected_run_id:
                raise ValueError("Trading account head changed before pending-order abandonment")
            connection.execute(
                "UPDATE trading_accounts SET selected_run_id=?,updated_at=? WHERE account_id=?",
                (run.binding.parent_run_id, _now(), account_id),
            )
        return self.account(account_id)

    def selected_state(self, account_id: str) -> tuple[PortfolioState, str | None]:
        account = self.account(account_id)
        if account.selected_run_id is None:
            return account.initial_state, None
        run = self.run(account.selected_run_id)
        if run.status is not RunStatus.COMPLETE:
            raise RuntimeError("Account selected_run_id does not point to a completed run")
        with self._connect(read_only=True) as connection:
            row = connection.execute(
                "SELECT final_state_json FROM simulation_runs WHERE run_id=?", (run.run_id,),
            ).fetchone()
        return _state(row["final_state_json"]), run.run_id

    def state_after(self, run_id: str | None, account_id: str) -> PortfolioState:
        if run_id is None:
            return self.account(account_id).initial_state
        run = self.run(run_id)
        if run.binding.account_id != account_id or run.status is not RunStatus.COMPLETE:
            raise ValueError("Parent run must be a completed run for the same account")
        with self._connect(read_only=True) as connection:
            row = connection.execute(
                "SELECT final_state_json FROM simulation_runs WHERE run_id=?", (run_id,),
            ).fetchone()
        return _state(row["final_state_json"])

    def begin_run(self, binding: RunBinding, initial_state: PortfolioState) -> BeginRunResult:
        account = self.account(binding.account_id)
        if account.status is not AccountStatus.ACTIVE:
            raise ValueError(f"Trading account is not active: {binding.account_id}")
        if initial_state.state_hash != binding.initial_state_hash:
            raise ValueError("Run binding initial_state_hash does not match initial state")
        with self.transaction() as connection:
            complete = connection.execute(
                "SELECT * FROM simulation_runs WHERE binding_hash=? AND status='complete' ORDER BY attempt LIMIT 1",
                (binding.binding_hash,),
            ).fetchone()
            if complete is not None:
                return BeginRunResult(_run_record(complete), False)
            attempt = int(connection.execute(
                "SELECT COALESCE(MAX(attempt),0)+1 FROM simulation_runs WHERE binding_hash=?",
                (binding.binding_hash,),
            ).fetchone()[0])
            run_id = f"run-{stable_digest({'binding_hash': binding.binding_hash, 'attempt': attempt})[:24]}"
            connection.execute(
                """INSERT INTO simulation_runs(
                    run_id,account_id,binding_hash,binding_json,attempt,status,initial_state_json,
                    initial_state_hash,started_at
                ) VALUES (?,?,?,?,?,'running',?,?,?)""",
                (run_id, binding.account_id, binding.binding_hash, canonical_json(binding), attempt,
                 _state_json(initial_state), initial_state.state_hash, _now()),
            )
            row = connection.execute("SELECT * FROM simulation_runs WHERE run_id=?", (run_id,)).fetchone()
        return BeginRunResult(_run_record(row), True)

    def append_session(
        self,
        run_id: str,
        events: Iterable[LedgerEvent],
        state: PortfolioState,
        valuation: Valuation,
    ) -> None:
        with self.transaction() as connection:
            run = connection.execute(
                "SELECT status,initial_state_hash FROM simulation_runs WHERE run_id=?", (run_id,),
            ).fetchone()
            if run is None or run["status"] != RunStatus.RUNNING.value:
                raise ValueError("Session can only be appended to a running run")
            prior = connection.execute(
                "SELECT sequence,event_hash FROM trading_ledger_events WHERE run_id=? ORDER BY sequence DESC LIMIT 1",
                (run_id,),
            ).fetchone()
            sequence = 1 if prior is None else int(prior["sequence"]) + 1
            previous_hash = run["initial_state_hash"] if prior is None else prior["event_hash"]
            for event in events:
                body = {
                    "run_id": run_id,
                    "sequence": sequence,
                    "session_date": event.session_date,
                    "event_type": event.event_type,
                    "entity_type": event.entity_type,
                    "entity_id": event.entity_id,
                    "payload": event.payload,
                    "previous_hash": previous_hash,
                }
                event_hash = stable_digest(body)
                event_id = f"event-{event_hash[:24]}"
                connection.execute(
                    "INSERT INTO trading_ledger_events VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (run_id, sequence, event_id, event.session_date.isoformat(), event.event_type,
                     event.entity_type, event.entity_id, canonical_json(event.payload), previous_hash, event_hash),
                )
                sequence += 1
                previous_hash = event_hash
            connection.execute(
                "INSERT INTO trading_checkpoints VALUES (?,?,?,?,?,?,?,?,?)",
                (run_id, valuation.session_date.isoformat(), _state_json(state), state.state_hash,
                 str(valuation.cash), str(valuation.market_value), str(valuation.total_equity),
                 str(valuation.nav), canonical_json(valuation.stale_instruments)),
            )

    def complete_run(self, run_id: str, final_state: PortfolioState, *, promote: bool) -> RunRecord:
        with self.transaction() as connection:
            row = connection.execute("SELECT * FROM simulation_runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None or row["status"] != RunStatus.RUNNING.value:
                raise ValueError("Only a running run can be completed")
            binding = _binding(row["binding_json"])
            tail = connection.execute(
                "SELECT event_hash FROM trading_ledger_events WHERE run_id=? ORDER BY sequence DESC LIMIT 1",
                (run_id,),
            ).fetchone()
            chain_head = binding.initial_state_hash if tail is None else tail["event_hash"]
            result_hash = stable_digest({
                "binding_hash": binding.binding_hash,
                "event_chain_head": chain_head,
                "final_state_hash": final_state.state_hash,
            })
            connection.execute(
                """UPDATE simulation_runs SET status='complete',final_state_json=?,final_state_hash=?,
                   result_hash=?,incomplete_reasons_json=?,completed_at=? WHERE run_id=?""",
                (_state_json(final_state), final_state.state_hash, result_hash,
                 canonical_json(final_state.incomplete_reasons), _now(), run_id),
            )
            if promote:
                account = connection.execute(
                    "SELECT selected_run_id FROM trading_accounts WHERE account_id=?", (binding.account_id,),
                ).fetchone()
                if account["selected_run_id"] != binding.parent_run_id:
                    raise RuntimeError("Account head changed while the run was executing")
                connection.execute(
                    "UPDATE trading_accounts SET selected_run_id=?,updated_at=? WHERE account_id=?",
                    (run_id, _now(), binding.account_id),
                )
            completed = connection.execute("SELECT * FROM simulation_runs WHERE run_id=?", (run_id,)).fetchone()
        return _run_record(completed)

    def fail_run(self, run_id: str, error: str) -> RunRecord:
        with self.transaction() as connection:
            row = connection.execute("SELECT status FROM simulation_runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None or row["status"] != RunStatus.RUNNING.value:
                raise ValueError("Only a running run can fail")
            connection.execute(
                "UPDATE simulation_runs SET status='failed',error=?,completed_at=? WHERE run_id=?",
                (error, _now(), run_id),
            )
            failed = connection.execute("SELECT * FROM simulation_runs WHERE run_id=?", (run_id,)).fetchone()
        return _run_record(failed)

    def run(self, run_id: str) -> RunRecord:
        with self._connect(read_only=True) as connection:
            row = connection.execute("SELECT * FROM simulation_runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            raise KeyError(f"Unknown simulation run: {run_id}")
        return _run_record(row)

    def final_state(self, run_id: str) -> PortfolioState:
        with self._connect(read_only=True) as connection:
            row = connection.execute(
                "SELECT status,final_state_json FROM simulation_runs WHERE run_id=?", (run_id,),
            ).fetchone()
        if row is None or row["status"] != RunStatus.COMPLETE.value:
            raise ValueError("Final state is available only for completed runs")
        return _state(row["final_state_json"])

    def initial_state(self, run_id: str) -> PortfolioState:
        with self._connect(read_only=True) as connection:
            row = connection.execute(
                "SELECT initial_state_json FROM simulation_runs WHERE run_id=?", (run_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"Unknown simulation run: {run_id}")
        return _state(row["initial_state_json"])

    def checkpoints(self, run_id: str) -> tuple[Mapping[str, Any], ...]:
        with self._connect(read_only=True) as connection:
            rows = connection.execute(
                "SELECT * FROM trading_checkpoints WHERE run_id=? ORDER BY session_date", (run_id,),
            ).fetchall()
        return tuple(dict(row) for row in rows)

    def events(self, run_id: str) -> tuple[Mapping[str, Any], ...]:
        with self._connect(read_only=True) as connection:
            rows = connection.execute(
                "SELECT * FROM trading_ledger_events WHERE run_id=? ORDER BY sequence", (run_id,),
            ).fetchall()
        return tuple(dict(row) | {"payload": json.loads(row["payload_json"])} for row in rows)

    def verify_run(self, run_id: str) -> str:
        run = self.run(run_id)
        with self._connect(read_only=True) as connection:
            rows = connection.execute(
                "SELECT * FROM trading_ledger_events WHERE run_id=? ORDER BY sequence", (run_id,),
            ).fetchall()
        previous = run.binding.initial_state_hash
        for expected_sequence, row in enumerate(rows, 1):
            if row["sequence"] != expected_sequence or row["previous_hash"] != previous:
                raise RuntimeError(f"Ledger chain discontinuity in {run_id} at sequence {expected_sequence}")
            body = {
                "run_id": run_id,
                "sequence": expected_sequence,
                "session_date": date.fromisoformat(row["session_date"]),
                "event_type": row["event_type"],
                "entity_type": row["entity_type"],
                "entity_id": row["entity_id"],
                "payload": json.loads(row["payload_json"]),
                "previous_hash": previous,
            }
            actual = stable_digest(body)
            if actual != row["event_hash"]:
                raise RuntimeError(f"Ledger event hash mismatch in {run_id} at sequence {expected_sequence}")
            previous = actual
        if run.status is RunStatus.COMPLETE:
            expected_result = stable_digest({
                "binding_hash": run.binding.binding_hash,
                "event_chain_head": previous,
                "final_state_hash": run.final_state_hash,
            })
            if expected_result != run.result_hash:
                raise RuntimeError(f"Run result hash mismatch: {run_id}")
        return previous

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _connect(self, *, read_only: bool = False) -> sqlite3.Connection:
        if read_only:
            connection = sqlite3.connect(f"file:{self.path.resolve().as_posix()}?mode=ro", uri=True)
        else:
            connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=5000")
        return connection


def _state_json(state: PortfolioState) -> str:
    return canonical_json(state)


def _state(payload: str) -> PortfolioState:
    value = json.loads(payload)
    return PortfolioState(
        Decimal(value["initial_cash"]),
        Decimal(value["cash"]),
        tuple(PositionLot(
            item["lot_id"], item["instrument_id"], int(item["quantity"]),
            date.fromisoformat(item["acquired_on"]), date.fromisoformat(item["sellable_on"]),
            Decimal(item["cost_amount"]),
        ) for item in value.get("lots", ())),
        tuple(Order(
            item["order_id"], item["intent_id"], item["instrument_id"], Side(item["side"]),
            date.fromisoformat(item["created_on"]), date.fromisoformat(item["execution_date"]),
            date.fromisoformat(item["expiry_date"]), int(item["requested_quantity"]),
            int(item["remaining_quantity"]), ExecutionStatus(item["status"]),
        ) for item in value.get("pending_orders", ())),
        tuple(Entitlement(
            item["action_id"], item["instrument_id"], int(item["quantity"]),
            date.fromisoformat(item["captured_on"]),
            int(item.get("distributed_quantity", 0)),
            Decimal(item.get("allocated_cost", "0")),
            None if item.get("adjusted_on") is None else date.fromisoformat(item["adjusted_on"]),
        ) for item in value.get("entitlements", ())),
        {symbol: Decimal(price) for symbol, price in value.get("last_prices", {}).items()},
        Decimal(value.get("realized_pnl", "0")),
        Decimal(value.get("fees_paid", "0")),
        Decimal(value.get("dividend_income", "0")),
        tuple(value.get("incomplete_reasons", ())),
    )


def _binding(payload: str) -> RunBinding:
    value = json.loads(payload)
    return RunBinding(
        value["account_id"], RunMode(value["mode"]), value["snapshot_id"], value["strategy_id"],
        value["strategy_version"], value["strategy_config_hash"], value["execution_policy_hash"],
        value["risk_policy_hash"], value["fee_schedule_hash"], value["initial_state_hash"],
        date.fromisoformat(value["start_date"]), date.fromisoformat(value["end_date"]),
        int(value["seed"]), value.get("parent_run_id"),
    )


def _run_record(row: sqlite3.Row) -> RunRecord:
    return RunRecord(
        row["run_id"], _binding(row["binding_json"]), int(row["attempt"]), RunStatus(row["status"]),
        row["final_state_hash"], row["result_hash"], row["error"],
    )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
