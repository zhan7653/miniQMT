from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, time
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

import yaml

from fundlab.trading import ExecutionPolicy, FeeRule, FeeSchedule, RiskPolicy


@dataclass(frozen=True)
class FoundationPaths:
    market_data: Path
    trading_database: Path
    report_root: Path
    legacy_market_data: Path
    legacy_reports: Path
    protected_legacy_database: Path
    protected_legacy_bars: Path


@dataclass(frozen=True)
class DailyAccountSettings:
    account_id: str
    name: str
    initial_cash: Decimal
    strategy: str
    weights: Mapping[str, Decimal] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.strategy not in {"static", "agent-file"}:
            raise ValueError(f"Unknown daily strategy: {self.strategy}")
        if self.strategy == "static" and not self.weights:
            raise ValueError(f"Static daily account needs weights: {self.account_id}")


@dataclass(frozen=True)
class DailySettings:
    session_cutoff: time
    agent_decision_root: Path
    report_root: Path
    accounts: tuple[DailyAccountSettings, ...]
    source_pair: tuple[str, str] = ("tickflow", "xtquant")
    adjudicator: str = "baostock"
    batch_size: int = 100


@dataclass(frozen=True)
class FoundationSettings:
    paths: FoundationPaths
    execution_policy: ExecutionPolicy
    risk_policy: RiskPolicy
    fee_schedule: FeeSchedule
    daily: DailySettings


def load_foundation_settings(path: str | Path = "config/fundlab.yaml") -> FoundationSettings:
    config_path = Path(path).resolve()
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError("Foundation config root must be a mapping")
    base = config_path.parent
    raw_paths = _mapping(payload, "paths")
    paths = FoundationPaths(
        *(_resolve(base, raw_paths, key) for key in (
            "market_data", "trading_database", "report_root", "legacy_market_data",
            "legacy_reports", "protected_legacy_database", "protected_legacy_bars",
        ))
    )
    raw_execution = _mapping(payload, "execution")
    execution = ExecutionPolicy(
        str(raw_execution["policy_id"]),
        str(raw_execution["version"]),
        Decimal(str(raw_execution["maximum_participation"])),
        Decimal(str(raw_execution["base_slippage_bps"])),
        Decimal(str(raw_execution["impact_bps_at_max_participation"])),
        bool(raw_execution.get("block_at_price_limit", True)),
        bool(raw_execution.get("allow_partial_fills", True)),
        bool(raw_execution.get("subscribe_rights", False)),
    )
    raw_risk = _mapping(payload, "risk")
    risk = RiskPolicy(
        str(raw_risk["policy_id"]),
        str(raw_risk["version"]),
        Decimal(str(raw_risk.get("max_position_weight", 1))),
        Decimal(str(raw_risk.get("minimum_cash_weight", 0))),
        frozenset(map(str, raw_risk.get("allowed_asset_types", ("stock", "etf")))),
    )
    raw_fees = _mapping(payload, "fees")
    rules = tuple(_fee_rule(item) for item in raw_fees.get("rules", ()))
    fee_schedule = FeeSchedule(
        str(raw_fees["schedule_id"]),
        str(raw_fees["version"]),
        rules,
        bool(raw_fees.get("trusted_for_simulation", False)),
        str(raw_fees.get("verification_note", "")),
    )
    daily = _daily_settings(payload.get("daily"), base)
    return FoundationSettings(paths, execution, risk, fee_schedule, daily)


def _daily_settings(raw: Any, base: Path) -> DailySettings:
    raw = raw if isinstance(raw, dict) else {}
    accounts = []
    for item in raw.get("accounts", ()):
        if not isinstance(item, dict):
            raise ValueError("Daily account entries must be mappings")
        accounts.append(DailyAccountSettings(
            str(item["account_id"]),
            str(item.get("name", item["account_id"])),
            Decimal(str(item.get("initial_cash", "1000000"))),
            str(item.get("strategy", "static")),
            {
                str(symbol): Decimal(str(weight))
                for symbol, weight in (item.get("weights") or {}).items()
            },
        ))
    def _path(key: str, default: str) -> Path:
        value = Path(str(raw.get(key, default)))
        return value.resolve() if value.is_absolute() else (base / value).resolve()
    return DailySettings(
        session_cutoff=time.fromisoformat(str(raw.get("session_cutoff_local", "19:00"))),
        agent_decision_root=_path("agent_decision_dir", "../data/agent/decisions"),
        report_root=_path("report_dir", "../data/reports/daily"),
        accounts=tuple(accounts),
        source_pair=tuple(map(str, raw.get("source_pair", ("tickflow", "xtquant")))),
        adjudicator=str(raw.get("adjudicator", "baostock")),
        batch_size=int(raw.get("batch_size", 100)),
    )


def _fee_rule(payload: Mapping[str, Any]) -> FeeRule:
    return FeeRule(
        date.fromisoformat(str(payload["effective_from"])),
        None if payload.get("effective_to") is None else date.fromisoformat(str(payload["effective_to"])),
        frozenset(map(str, payload.get("asset_types", ()))),
        frozenset(map(str, payload.get("exchanges", ()))),
        Decimal(str(payload.get("broker_commission_rate", 0))),
        Decimal(str(payload.get("minimum_commission", 0))),
        Decimal(str(payload.get("stamp_duty_sell_rate", 0))),
        Decimal(str(payload.get("transfer_fee_rate", 0))),
        Decimal(str(payload.get("exchange_handling_rate", 0))),
        Decimal(str(payload.get("regulatory_levy_rate", 0))),
        int(payload.get("priority", 0)),
        str(payload.get("evidence", "")),
    )


def _mapping(payload: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"Foundation config section must be a mapping: {key}")
    return value


def _resolve(base: Path, payload: Mapping[str, Any], key: str) -> Path:
    value = Path(str(payload[key]))
    return value.resolve() if value.is_absolute() else (base / value).resolve()
