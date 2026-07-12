from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class PaperTable(StrEnum):
    ACCOUNTS = "paper_accounts"
    CURRENT_POSITIONS = "paper_current_positions"
    DAILY_POSITIONS = "paper_daily_positions"
    DECISIONS = "paper_decisions"
    ORDERS = "paper_orders"
    FILLS = "paper_fills"
    ACCOUNT_SNAPSHOTS = "paper_account_snapshots"
    EVENT_LEDGER = "paper_event_ledger"
    EXECUTION_PROFILES = "paper_execution_profiles"
    RISK_PROFILES = "paper_risk_profiles"
    DAILY_RUNS = "paper_daily_runs"
    LEDGER_VERSIONS = "paper_ledger_versions"
    DATE_BATCHES = "paper_date_batches"


@dataclass(frozen=True)
class TableSpec:
    name: PaperTable
    primary_key: tuple[str, ...]
    unique_constraints: tuple[tuple[str, ...], ...] = ()
    append_only: bool = False
    immutable_when_complete: bool = False
