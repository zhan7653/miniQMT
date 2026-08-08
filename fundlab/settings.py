from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, time
from decimal import Decimal
from pathlib import Path
import os
import re
from typing import Any, Mapping
from urllib.parse import urlsplit

import yaml

from fundlab.trading import ExecutionPolicy, FeeRule, FeeSchedule, RiskPolicy


@dataclass(frozen=True)
class FoundationPaths:
    market_data: Path
    trading_database: Path
    report_root: Path


@dataclass(frozen=True)
class DailyAccountSettings:
    account_id: str
    name: str
    initial_cash: Decimal
    strategy: str
    weights: Mapping[str, Decimal] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.strategy not in {"static", "agent-file", "moving-average-grid"}:
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
    # How far past today the canonical calendar carries exchange-announced
    # future sessions, so a head-of-data intent can always schedule its T+1
    # order inside the published snapshot calendar.
    calendar_horizon_days: int = 60

    def __post_init__(self) -> None:
        if self.calendar_horizon_days < 1:
            raise ValueError("calendar_horizon_days must be at least 1")


@dataclass(frozen=True)
class AgentPolicySettings:
    """One account's declared decision policy: a kind plus opaque params.

    Parsing stays dumb on purpose — policy construction and validation live in
    ``fundlab.agent.policy.build_policy`` so a config typo fails there, loudly,
    instead of being half-interpreted here.
    """

    account_id: str
    kind: str
    params: Mapping[str, Any]
    scheduled: bool = True


@dataclass(frozen=True)
class AgentLLMSettings:
    """One OpenAI-compatible Responses endpoint used by local agents.

    The API key itself is deliberately absent: ``api_key_env`` only names the
    environment variable the runtime must read at call time.
    """

    base_url: str = ""
    model: str = "gpt-5.6-sol"
    api_key_env: str = "FUNDLAB_LLM_API_KEY"
    reasoning_effort: str = "medium"
    max_output_tokens: int = 32_768
    timeout_seconds: int = 300
    max_retries: int = 2
    store: bool = False

    def __post_init__(self) -> None:
        base_url = self.base_url.strip().rstrip("/")
        model = self.model.strip()
        api_key_env = self.api_key_env.strip()
        if base_url:
            parsed = urlsplit(base_url)
            if (
                parsed.scheme not in {"https", "http"}
                or not parsed.netloc
                or parsed.username is not None
                or parsed.password is not None
                or parsed.query
                or parsed.fragment
            ):
                raise ValueError(
                    "agent.llm.base_url must be an HTTP(S) base URL without credentials, "
                    "query, or fragment"
                )
        if not model:
            raise ValueError("agent.llm.model cannot be empty")
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", api_key_env) is None:
            raise ValueError("agent.llm.api_key_env must name an environment variable")
        if self.reasoning_effort not in {"none", "low", "medium", "high", "xhigh", "max"}:
            raise ValueError(f"Unsupported agent.llm.reasoning_effort: {self.reasoning_effort!r}")
        if self.max_output_tokens < 1:
            raise ValueError("agent.llm.max_output_tokens must be positive")
        if self.timeout_seconds < 1:
            raise ValueError("agent.llm.timeout_seconds must be positive")
        if not 0 <= self.max_retries <= 10:
            raise ValueError("agent.llm.max_retries must be between 0 and 10")
        if self.store:
            raise ValueError("agent.llm.store must remain false for this local Agent")
        object.__setattr__(self, "base_url", base_url)
        object.__setattr__(self, "model", model)
        object.__setattr__(self, "api_key_env", api_key_env)


@dataclass(frozen=True)
class AgentLibrarySettings:
    """Bounded, explicitly allow-listed local reading material."""

    root: Path = Path("data/agent/library")
    documents: tuple[str, ...] = ()
    max_document_chars: int = 40_000
    max_total_chars: int = 120_000

    def __post_init__(self) -> None:
        if self.max_document_chars < 1 or self.max_total_chars < 1:
            raise ValueError("agent.library character limits must be positive")
        if self.max_document_chars > self.max_total_chars:
            raise ValueError("agent.library.max_document_chars cannot exceed max_total_chars")
        object.__setattr__(self, "root", Path(self.root))
        object.__setattr__(self, "documents", tuple(map(str, self.documents)))


@dataclass(frozen=True)
class AgentMemorySettings:
    """Append-only local review memory exposed back to the model in a window."""

    root: Path = Path("data/agent/memory")
    recent_entries: int = 20
    max_entry_chars: int = 20_000
    max_total_chars: int = 120_000

    def __post_init__(self) -> None:
        if self.recent_entries < 0 or self.recent_entries > 1_000:
            raise ValueError("agent.memory.recent_entries must be between 0 and 1000")
        if self.max_entry_chars < 1 or self.max_total_chars < 1:
            raise ValueError("agent.memory character limits must be positive")
        if self.max_entry_chars > self.max_total_chars:
            raise ValueError("agent.memory.max_entry_chars cannot exceed max_total_chars")
        object.__setattr__(self, "root", Path(self.root))


@dataclass(frozen=True)
class AgentNotificationSettings:
    """Notification recipients only; SMTP credentials remain environment-only."""

    email_to: str | None = None


@dataclass(frozen=True)
class AgentSettings:
    """Per-account policies plus shared, bounded Agent infrastructure."""

    policies: Mapping[str, AgentPolicySettings] = field(default_factory=dict)
    llm: AgentLLMSettings = field(default_factory=AgentLLMSettings)
    library: AgentLibrarySettings = field(default_factory=AgentLibrarySettings)
    memory: AgentMemorySettings = field(default_factory=AgentMemorySettings)
    notify: AgentNotificationSettings = field(default_factory=AgentNotificationSettings)


@dataclass(frozen=True)
class FoundationSettings:
    paths: FoundationPaths
    execution_policy: ExecutionPolicy
    risk_policy: RiskPolicy
    fee_schedule: FeeSchedule
    daily: DailySettings
    agent: AgentSettings = field(default_factory=AgentSettings)


def load_foundation_settings(path: str | Path = "config/fundlab.yaml") -> FoundationSettings:
    config_path = Path(path).resolve()
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError("Foundation config root must be a mapping")
    base = config_path.parent
    raw_paths = _mapping(payload, "paths")
    paths = FoundationPaths(
        *(_resolve(base, raw_paths, key) for key in (
            "market_data", "trading_database", "report_root",
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
    agent = _agent_settings(payload.get("agent"), base)
    return FoundationSettings(paths, execution, risk, fee_schedule, daily, agent)


def _agent_settings(raw: Any, base: Path) -> AgentSettings:
    if raw is not None and not isinstance(raw, dict):
        raise ValueError("agent must be a mapping")
    raw = raw or {}
    policies_raw = raw.get("policies")
    policies: dict[str, AgentPolicySettings] = {}
    if policies_raw is not None:
        if not isinstance(policies_raw, dict):
            raise ValueError("agent.policies must be a mapping of account_id -> policy")
        for account_id, item in policies_raw.items():
            if not isinstance(item, dict) or "type" not in item:
                raise ValueError(f"agent.policies.{account_id} needs a mapping with a `type`")
            params = {
                key: value for key, value in item.items()
                if key not in {"type", "scheduled"}
            }
            if "charter" in params:
                charter = Path(str(params["charter"]))
                params["charter"] = str(
                    charter.resolve() if charter.is_absolute() else (base / charter).resolve()
                )
            scheduled = item.get("scheduled", True)
            if not isinstance(scheduled, bool):
                raise ValueError(f"agent.policies.{account_id}.scheduled must be true or false")
            policies[str(account_id)] = AgentPolicySettings(
                account_id=str(account_id),
                kind=str(item["type"]),
                params=params,
                scheduled=scheduled,
            )
    llm_raw = _optional_mapping(raw, "llm", "agent.llm")
    llm = AgentLLMSettings(
        base_url=str(llm_raw.get("base_url", "")),
        model=str(llm_raw.get("model", "gpt-5.6-sol")),
        api_key_env=str(llm_raw.get("api_key_env", "FUNDLAB_LLM_API_KEY")),
        reasoning_effort=str(llm_raw.get("reasoning_effort", "medium")),
        max_output_tokens=int(llm_raw.get("max_output_tokens", 32_768)),
        timeout_seconds=int(llm_raw.get("timeout_seconds", 300)),
        max_retries=int(llm_raw.get("max_retries", 2)),
        store=_strict_bool(llm_raw.get("store", False), "agent.llm.store"),
    )
    library_raw = _optional_mapping(raw, "library", "agent.library")
    documents_raw = library_raw.get("documents", ())
    if not isinstance(documents_raw, (list, tuple)):
        raise ValueError("agent.library.documents must be a list of file names")
    library_root = Path(str(library_raw.get("root", "../data/agent/library")))
    library = AgentLibrarySettings(
        root=library_root.resolve() if library_root.is_absolute() else (base / library_root).resolve(),
        documents=tuple(map(str, documents_raw)),
        max_document_chars=int(library_raw.get("max_document_chars", 40_000)),
        max_total_chars=int(library_raw.get("max_total_chars", 120_000)),
    )
    memory_raw = _optional_mapping(raw, "memory", "agent.memory")
    memory_root = Path(str(memory_raw.get("root", "../data/agent/memory")))
    memory = AgentMemorySettings(
        root=memory_root.resolve() if memory_root.is_absolute() else (base / memory_root).resolve(),
        recent_entries=int(memory_raw.get("recent_entries", 20)),
        max_entry_chars=int(memory_raw.get("max_entry_chars", 20_000)),
        max_total_chars=int(memory_raw.get("max_total_chars", 120_000)),
    )
    notify_raw = _optional_mapping(raw, "notify", "agent.notify")
    email_to = (
        os.environ.get("FUNDLAB_EMAIL_TO")
        or notify_raw.get("email_to")
        or os.environ.get("FUNDLAB_SMTP_USER")
    )
    notify = AgentNotificationSettings(
        email_to=None if email_to is None or not str(email_to).strip() else str(email_to).strip(),
    )
    return AgentSettings(
        policies=policies,
        llm=llm,
        library=library,
        memory=memory,
        notify=notify,
    )


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
        calendar_horizon_days=int(raw.get("calendar_horizon_days", 60)),
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


def _strict_bool(value: object, label: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{label} must be true or false")
    return value


def _optional_mapping(payload: Mapping[str, Any], key: str, label: str) -> Mapping[str, Any]:
    value = payload.get(key)
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a mapping")
    return value
