from __future__ import annotations

import argparse
from dataclasses import asdict, is_dataclass
from enum import Enum
import json
from pathlib import Path
import sys
from typing import Any, Mapping, Sequence

import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from fundlab.common.config import load_paper_trading_config, resolve_config_path
from fundlab.paper import (
    CreateAccountRequest,
    DailyPaperRunner,
    PaperAccountLifecycle,
    PaperLedgerRepository,
    ReplayRunError,
    StrategyRegistry,
)
from fundlab.trading import AccountBindings, RebalanceFrequency
from scripts.run_paper_daily import _portal, _registry


DEFAULT_CONFIG = REPOSITORY_ROOT / "config" / "paper_trading.yaml"


def _jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return _jsonable(asdict(value))
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def emit(payload: Mapping[str, Any]) -> None:
    print(json.dumps(_jsonable(payload), ensure_ascii=False, sort_keys=True))


class JsonArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        emit({"ok": False, "command": "parse", "error": {"type": "ArgumentError", "message": message}})
        self.exit(2)


def _raw_config(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as stream:
        value = yaml.safe_load(stream) or {}
    if not isinstance(value, dict):
        raise ValueError("paper trading config must be a mapping")
    return value


def _register_profiles(lifecycle: PaperAccountLifecycle, config) -> None:
    for profile in config.execution_profiles.values():
        for risk in config.risk_profiles.values():
            lifecycle.register_profiles(profile, risk)


def _configured_universes(raw: Mapping[str, Any]) -> dict[str, tuple[str, ...]]:
    value = raw.get("universes", {})
    if not isinstance(value, Mapping):
        raise ValueError("universes must be a version-to-symbols mapping")
    universes: dict[str, tuple[str, ...]] = {}
    for version, symbols in value.items():
        if not isinstance(symbols, (list, tuple)):
            raise ValueError(f"universe {version} must be a symbol list")
        normalized = tuple(dict.fromkeys(str(symbol) for symbol in symbols if str(symbol)))
        if not str(version) or not normalized:
            raise ValueError("configured universe versions and symbol lists cannot be empty")
        universes[str(version)] = normalized
    return universes


def _binding_registry(raw: Mapping[str, Any]) -> StrategyRegistry:
    registry = StrategyRegistry()
    for version, symbols in _configured_universes(raw).items():
        registry.register_universe(version, symbols)
    configs = raw.get("strategy_configs", [])
    if not isinstance(configs, list):
        raise ValueError("strategy_configs must be a list")
    for item in configs:
        if not isinstance(item, Mapping):
            raise ValueError("strategy config entries must be mappings")
        registry.register_config(str(item.get("strategy_id", "")), str(item.get("version", "")),
                                 item.get("parameters", {}))
    return registry


def _request(spec: Mapping[str, Any], *, default_cash: float, default_benchmark: str) -> CreateAccountRequest:
    return CreateAccountRequest(
        account_id=str(spec["account_id"]),
        name=str(spec.get("name", spec["account_id"])),
        bindings=AccountBindings(
            strategy_id=str(spec["strategy_id"]),
            strategy_config_version=str(spec["strategy_config_version"]),
            universe_version=str(spec["universe_version"]),
            benchmark_symbol=str(spec.get("benchmark_symbol", default_benchmark)),
            execution_profile_version=str(spec["execution_profile_version"]),
            risk_profile_version=str(spec["risk_profile_version"]),
        ),
        execution_profile_id=str(spec["execution_profile_id"]),
        risk_profile_id=str(spec["risk_profile_id"]),
        initial_cash=float(spec.get("initial_cash", default_cash)),
        schedule=RebalanceFrequency(str(spec.get("schedule", "daily"))),
    )


def _validated_request(spec: Mapping[str, Any], *, config, registry: StrategyRegistry) -> CreateAccountRequest:
    request = _request(spec, default_cash=config.default_initial_cash,
                       default_benchmark=config.default_benchmark)
    if not request.account_id or not request.name:
        raise ValueError("account identity must be non-empty")
    if request.initial_cash <= 0:
        raise ValueError("initial_cash must be positive")
    strategy_config = registry.get_config(
        request.bindings.strategy_id, request.bindings.strategy_config_version,
    )
    universe = frozenset(registry.get_universe(request.bindings.universe_version))
    declared = strategy_config.get("symbols")
    if declared is not None:
        if not isinstance(declared, (list, tuple)):
            raise ValueError("strategy config symbols must be a list")
        outside = sorted({str(symbol) for symbol in declared} - universe)
        if outside:
            raise ValueError(f"strategy symbols are outside bound universe: {outside}")
    if request.bindings.benchmark_symbol not in universe:
        raise ValueError("benchmark must belong to the bound universe")
    execution_exists = any(
        profile.profile_id == request.execution_profile_id
        and profile.version == request.bindings.execution_profile_version
        for profile in config.execution_profiles.values()
    )
    if not execution_exists:
        raise ValueError("unknown execution profile id/version binding")
    risk_exists = any(
        profile.profile_id == request.risk_profile_id
        and profile.version == request.bindings.risk_profile_version
        for profile in config.risk_profiles.values()
    )
    if not risk_exists:
        raise ValueError("unknown risk profile id/version binding")
    return request


def build_parser() -> argparse.ArgumentParser:
    parser = JsonArgumentParser(description="Manage durable FundLab paper accounts non-interactively.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--database", type=Path, help="Override the configured paper ledger path.")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("bootstrap", help="Register immutable profiles and create missing default accounts.")
    listing = commands.add_parser("list", help="List accounts.")
    listing.add_argument("--status", choices=("active", "paused", "closed"))
    show = commands.add_parser("show", help="Show an account, current positions, snapshots, and events.")
    show.add_argument("account_id")
    for name in ("pause", "resume", "close"):
        command = commands.add_parser(name)
        command.add_argument("account_id")
    create = commands.add_parser("create", help="Create an account with immutable bindings.")
    create.add_argument("--account-id", required=True)
    create.add_argument("--name")
    create.add_argument("--strategy-id", required=True)
    create.add_argument("--strategy-config-version", required=True)
    create.add_argument("--universe-version", required=True)
    create.add_argument("--benchmark")
    create.add_argument("--execution-profile", default="etf_default_v1")
    create.add_argument("--risk-profile", default="research_default_v1")
    create.add_argument("--initial-cash", type=float)
    create.add_argument("--schedule", choices=tuple(item.value for item in RebalanceFrequency), default="daily")
    replay = commands.add_parser("replay", help="Rebuild, validate, and atomically activate a replay version.")
    replay.add_argument("account_id")
    replay.add_argument("--start-date", required=True)
    replay.add_argument("--target-date", required=True)
    replay.add_argument("--reason", required=True)
    return parser


def execute(args: argparse.Namespace) -> Mapping[str, Any]:
    config_path = args.config.expanduser().resolve()
    config = load_paper_trading_config(config_path)
    raw = _raw_config(config_path)
    database = args.database.expanduser().resolve() if args.database else config.paths.database
    bootstrap_requests: tuple[CreateAccountRequest, ...] = ()
    create_request: CreateAccountRequest | None = None
    create_profiles = None
    replay_runtime = None
    if args.command == "bootstrap":
        registry = _binding_registry(raw)
        defaults = raw.get("default_accounts", [])
        if not isinstance(defaults, list):
            raise ValueError("default_accounts must be a list")
        bootstrap_requests = tuple(
            _validated_request(spec, config=config, registry=registry) for spec in defaults
        )
    elif args.command == "create":
        registry = _binding_registry(raw)
        execution = config.execution_profiles[args.execution_profile]
        risk = config.risk_profiles[args.risk_profile]
        spec = {
            "account_id": args.account_id, "name": args.name or args.account_id,
            "strategy_id": args.strategy_id,
            "strategy_config_version": args.strategy_config_version,
            "universe_version": args.universe_version,
            "benchmark_symbol": args.benchmark or config.default_benchmark,
            "execution_profile_id": execution.profile_id,
            "execution_profile_version": execution.version,
            "risk_profile_id": risk.profile_id, "risk_profile_version": risk.version,
            "initial_cash": (args.initial_cash if args.initial_cash is not None
                             else config.default_initial_cash),
            "schedule": args.schedule,
        }
        create_request = _validated_request(spec, config=config, registry=registry)
        create_profiles = (execution, risk)
    elif args.command == "replay":
        platform_path = resolve_config_path(
            {"_config_dir": config_path.parent}, raw.get("data_platform_config", "base.yaml")
        )
        replay_runtime = (_portal(platform_path), _registry(raw))
    with PaperLedgerRepository(database) as repository:
        lifecycle = PaperAccountLifecycle(repository)
        if args.command == "bootstrap":
            existing = {account.account_id: account for account in repository.list_accounts()}
            to_create, reused = [], []
            for request in bootstrap_requests:
                if request.account_id in existing:
                    current = existing[request.account_id]
                    immutable_match = (
                        current.bindings == request.bindings
                        and current.execution_profile_id == request.execution_profile_id
                        and current.risk_profile_id == request.risk_profile_id
                        and current.initial_cash == request.initial_cash
                        and current.schedule == request.schedule
                    )
                    if not immutable_match:
                        raise ValueError(f"default account {request.account_id} exists with different immutable bindings")
                    reused.append(request.account_id)
                else:
                    to_create.append(request)
            _register_profiles(lifecycle, config)
            for request in to_create:
                lifecycle.create(request)
            created = [request.account_id for request in to_create]
            return {"ok": True, "command": "bootstrap", "database": database,
                    "created": created, "reused": reused}
        if args.command == "list":
            return {"ok": True, "command": "list", "accounts": repository.list_accounts(args.status)}
        if args.command == "show":
            account = repository.get_account(args.account_id)
            return {"ok": True, "command": "show", "account": account,
                    "positions": repository.current_positions(args.account_id),
                    "snapshots": repository.account_snapshots(args.account_id),
                    "events": repository.list_events(args.account_id),
                    "ledger_versions": repository.list_ledger_versions(args.account_id)}
        if args.command in {"pause", "resume", "close"}:
            account = getattr(lifecycle, args.command)(args.account_id)
            return {"ok": True, "command": args.command, "account": account}
        if args.command == "create":
            assert create_request is not None and create_profiles is not None
            execution, risk = create_profiles
            lifecycle.register_profiles(execution, risk)
            account = lifecycle.create(create_request)
            return {"ok": True, "command": "create", "account": account}
        if args.command == "replay":
            assert replay_runtime is not None
            portal, registry = replay_runtime
            result = DailyPaperRunner(repository, lambda *_: portal, registry).replay(
                args.account_id, start_date=args.start_date,
                target_date=args.target_date, reason=args.reason,
            )
            return {"ok": True, "command": "replay", "replay": result}
    raise AssertionError("unreachable command")


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        emit(execute(args))
        return 0
    except ReplayRunError as exc:
        emit({"ok": False, "command": "replay", "replay": exc.result,
              "error": {"type": type(exc).__name__, "message": str(exc)}})
        return 3
    except Exception as exc:
        emit({"ok": False, "command": getattr(args, "command", None),
              "error": {"type": type(exc).__name__, "message": str(exc)}})
        return 1


if __name__ == "__main__":
    sys.exit(main())
