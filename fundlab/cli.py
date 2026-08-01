from __future__ import annotations

import argparse
from datetime import date
from decimal import Decimal
from pathlib import Path
import sys
from typing import Mapping, Sequence

from fundlab.common.canonical import canonical_json, to_primitive
from fundlab.common.local_env import load_local_environment
from fundlab.marketdata import (
    CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
    CanonicalMarketData,
    DEFAULT_HISTORY_START,
    EvidenceCollectionSpec,
    HistoryBuildSpec,
    HistoryDatabaseBuilder,
    MarketIngestionService,
    MarketDataWarehouse,
    MarketTable,
    ProviderCapability,
    ProviderRequest,
    ReadinessProfile,
    ReconciliationService,
    SnapshotPlan,
    SimulationSnapshotBuilder,
    SimulationEvidenceCollector,
    SimulationIncrementValidator,
    SimulationStatusCollector,
    StatusCollectionSpec,
    SourceSlice,
    UniverseScope,
    compose_history_snapshot,
    derive_current_research_snapshot,
    default_provider_registry,
    default_reconciliation_policy,
    source_statuses,
)
from fundlab.pipeline import DailyPipeline
from fundlab.settings import FoundationSettings, load_foundation_settings
from fundlab.strategies import StaticAllocationSource
from fundlab.trading import (
    PortfolioState,
    SimulationService,
    TradingRepository,
    build_simulation_feedback,
)


def _add_universe_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--universe-as-of", type=date.fromisoformat)
    parser.add_argument("--history-start", type=date.fromisoformat)
    parser.add_argument("--history-end", type=date.fromisoformat)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fundlab", description="Canonical local market-data and simulation system")
    parser.add_argument("--config", default="config/fundlab.yaml")
    commands = parser.add_subparsers(dest="command", required=True)

    data = commands.add_parser("data", help="Manage source observations and canonical snapshots")
    data_commands = data.add_subparsers(dest="data_command", required=True)
    snapshot = data_commands.add_parser("build-snapshot", help="Build a snapshot from one explicit observation")
    snapshot.add_argument("--observation-id", required=True)
    snapshot.add_argument("--description", required=True)
    snapshot.add_argument("--publish", action="store_true")
    _add_universe_arguments(snapshot)
    compose = data_commands.add_parser(
        "compose-history",
        help="Compose disjoint validated research-history partitions",
    )
    compose.add_argument("--observation-id", action="append", required=True)
    compose.add_argument("--description", required=True)
    compose.add_argument("--publish", action="store_true")
    current_research = data_commands.add_parser(
        "derive-current-research",
        help="Project an immutable research snapshot onto one exact current SH/SZ universe",
    )
    current_research.add_argument(
        "--source-snapshot-id",
        action="append",
        required=True,
        help="Primary research snapshot, repeated only for disjoint supplemental snapshots",
    )
    current_research.add_argument("--universe-observation-id", required=True)
    current_research.add_argument("--universe-as-of", type=date.fromisoformat, required=True)
    current_research.add_argument("--start-date", type=date.fromisoformat, required=True)
    current_research.add_argument("--end-date", type=date.fromisoformat, required=True)
    current_research.add_argument("--publish", action="store_true")
    status = data_commands.add_parser(
        "collect-status",
        help="Resumably collect exhaustive daily suspension/ST/previous-close facts",
    )
    status.add_argument("--source-snapshot-id", required=True)
    status.add_argument("--calendar-observation-id", required=True)
    status.add_argument("--provider", choices=("baostock", "xtquant"), default="baostock")
    status.add_argument("--batch-size", type=int, default=50)
    status.add_argument("--shard-count", type=int, default=1)
    status.add_argument("--shard-index", type=int, default=0)
    status.add_argument("--refresh", action="store_true")
    evidence = data_commands.add_parser(
        "collect-evidence",
        help="Resumably collect stock/ETF corporate actions or adjustment factors",
    )
    evidence.add_argument("--source-snapshot-id", required=True)
    evidence.add_argument(
        "--predecessor-snapshot-id",
        help="Required for incremental stock-actions collection",
    )
    evidence.add_argument(
        "--kind", choices=("stock-actions", "etf-actions", "factors"), required=True,
    )
    evidence.add_argument("--batch-size", type=int, default=100)
    evidence.add_argument("--refresh", action="store_true")
    validate_increment = data_commands.add_parser(
        "validate-simulation-increment",
        help="Assemble and validate one reconciled EOD partition",
    )
    validate_increment.add_argument("--candidate-observation-id", required=True)
    validate_increment.add_argument("--calendar-observation-id", required=True)
    validate_increment.add_argument("--universe-as-of", type=date.fromisoformat, required=True)
    validate_increment.add_argument("--start-date", type=date.fromisoformat, required=True)
    validate_increment.add_argument("--end-date", type=date.fromisoformat, required=True)
    validate_increment.add_argument("--description", required=True)
    eod = data_commands.add_parser(
        "extend-simulation",
        help="Validate a contiguous manual EOD increment and optionally publish atomically",
    )
    eod.add_argument("--predecessor-snapshot-id", required=True)
    eod.add_argument("--calendar-observation-id", required=True)
    eod.add_argument("--increment-observation-id", action="append", required=True)
    eod.add_argument("--universe-as-of", type=date.fromisoformat, required=True)
    eod.add_argument("--target-date", type=date.fromisoformat, required=True)
    eod.add_argument("--description", required=True)
    eod.add_argument("--publish", action="store_true")
    inspect = data_commands.add_parser("inspect", help="Inspect a pinned or current snapshot")
    inspect.add_argument("--snapshot-id")
    data_commands.add_parser("sources", help="List direct upstream channels and installed clients")
    collect = data_commands.add_parser("collect", help="Capture one named upstream observation")
    collect.add_argument("--provider", required=True)
    collect.add_argument(
        "--capability",
        required=True,
        choices=tuple(
            item.value for item in ProviderCapability
            if item is not ProviderCapability.CANONICAL_RECONCILIATION
        ),
    )
    collect.add_argument("--start-date", type=date.fromisoformat)
    collect.add_argument("--end-date", type=date.fromisoformat)
    collect.add_argument("--instrument", action="append", default=[])
    collect.add_argument(
        "--refresh", action="store_true",
        help="Force a new upstream observation even when this exact scope is complete locally",
    )
    reconcile = data_commands.add_parser(
        "reconcile", help="Create a field-level canonical observation and scoped snapshot",
    )
    reconcile.add_argument("--observation-id", action="append", required=True)
    reconcile.add_argument(
        "--readiness",
        choices=tuple(
            item.value for item in ReadinessProfile
            if item is not ReadinessProfile.LEGACY_UNKNOWN
        ),
        default=ReadinessProfile.RESEARCH_PRICE.value,
    )
    reconcile.add_argument("--description", required=True)
    reconcile.add_argument("--publish", action="store_true")
    _add_universe_arguments(reconcile)
    history = data_commands.add_parser(
        "build-history",
        help="Resumably build a scoped multi-source research-price history database",
    )
    history.add_argument("--start-date", type=date.fromisoformat, default=DEFAULT_HISTORY_START)
    history.add_argument("--end-date", type=date.fromisoformat, required=True)
    history.add_argument("--universe-as-of", type=date.fromisoformat)
    history.add_argument("--instrument", action="append", default=[])
    history.add_argument("--exchange", action="append", choices=("SH", "SZ"), default=[])
    history.add_argument("--asset-type", action="append", choices=("stock", "etf"), default=[])
    history.add_argument(
        "--source", action="append", choices=(
            "tickflow", "eastmoney-efinance", "exchange-public", "sina-etf", "baostock", "xtquant",
        ),
        required=True,
        help="Two baseline providers and an optional third conflict adjudicator",
    )
    history.add_argument("--batch-size", type=int, default=50)
    history.add_argument("--max-instruments", type=int)
    history.add_argument("--shard-count", type=int, default=1)
    history.add_argument("--shard-index", type=int, default=0)
    history.add_argument(
        "--assemble-only", action="store_true",
        help="Assemble completed partitions from all shards in the same cohort",
    )
    history.add_argument("--refresh", action="store_true")
    history.add_argument("--publish", action="store_true")
    snapshot.add_argument(
        "--readiness",
        choices=(ReadinessProfile.RESEARCH_PRICE.value,),
        default=ReadinessProfile.RESEARCH_PRICE.value,
    )

    account = commands.add_parser("account", help="Manage isolated persistent simulation accounts")
    account_commands = account.add_subparsers(dest="account_command", required=True)
    create = account_commands.add_parser("create")
    create.add_argument("--account-id", required=True)
    create.add_argument("--name", required=True)
    create.add_argument("--initial-cash", type=Decimal, required=True)
    show = account_commands.add_parser("show")
    show.add_argument("--account-id", required=True)

    web = commands.add_parser("web", help="Serve the local dashboard (accounts, runs, schedule, agent decisions)")
    web.add_argument("--host", default="127.0.0.1")
    web.add_argument("--port", type=int, default=8610)
    web.add_argument("--no-browser", action="store_true")

    daily = commands.add_parser("daily", help="Automated daily cycle: extend the snapshot, advance paper accounts")
    daily_commands = daily.add_subparsers(dest="daily_command", required=True)
    daily_run = daily_commands.add_parser("run", help="Run one idempotent daily cycle")
    daily_run.add_argument("--target-date", type=date.fromisoformat)
    daily_run.add_argument("--skip-data", action="store_true")
    daily_run.add_argument("--skip-accounts", action="store_true")
    daily_commands.add_parser("status", help="Show snapshot head, account heads, and configuration")

    simulate = commands.add_parser("simulate", help="Run the shared kernel with a static PortfolioIntent source")
    simulate.add_argument("--account-id", required=True)
    simulate.add_argument("--snapshot-id")
    simulate.add_argument("--weight", action="append", required=True, metavar="INSTRUMENT=WEIGHT")
    simulate.add_argument("--seed", type=int, default=0)
    simulate.add_argument("--allow-untrusted-fees", action="store_true")
    clock = simulate.add_mutually_exclusive_group(required=True)
    clock.add_argument("--date", type=date.fromisoformat)
    clock.add_argument("--start-date", type=date.fromisoformat)
    simulate.add_argument("--end-date", type=date.fromisoformat)
    simulate.add_argument("--promote-historical", action="store_true")

    agent = commands.add_parser(
        "agent", help="Local decision agents: bounded research -> policy -> decision file",
    )
    agent_commands = agent.add_subparsers(dest="agent_command", required=True)
    decide = agent_commands.add_parser(
        "decide", help="Compute the next decision for agent-file accounts and drop the file",
    )
    decide_scope = decide.add_mutually_exclusive_group(required=True)
    decide_scope.add_argument("--account-id")
    decide_scope.add_argument("--all", action="store_true")
    decide.add_argument("--target-date", type=date.fromisoformat)
    decide.add_argument("--overwrite", action="store_true")
    decide.add_argument("--dry-run", action="store_true")
    decide.add_argument(
        "--force-review",
        action="store_true",
        help="Run a review-cadence policy now (single account only)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        config_path = Path(args.config).resolve()
        config_root = (
            config_path.parent.parent
            if config_path.parent.name == "config" else config_path.parent
        )
        load_local_environment(config_root / ".env.local")
        settings = load_foundation_settings(config_path)
        if args.command == "data":
            return _data(args, settings)
        if args.command == "account":
            return _account(args, settings)
        if args.command == "simulate":
            return _simulate(args, settings)
        if args.command == "daily":
            return _daily(args, settings)
        if args.command == "web":
            return _web(args, settings)
        if args.command == "agent":
            return _agent(args, settings)
        raise AssertionError(args.command)
    except Exception as exc:
        print(canonical_json({
            "status": "error", "error_type": type(exc).__name__, "error": str(exc),
        }), file=sys.stderr)
        return 2


def _agent(args, settings: FoundationSettings) -> int:
    from fundlab.agent import AgentDecisionService

    service = AgentDecisionService(settings)
    if args.agent_command != "decide":
        raise AssertionError(args.agent_command)
    if args.all:
        if args.target_date is not None:
            raise ValueError("--target-date needs a single --account-id")
        if args.force_review:
            raise ValueError("--force-review needs a single --account-id")
        outcomes = service.decide_all(overwrite=args.overwrite, dry_run=args.dry_run)
    else:
        outcomes = [service.decide(
            args.account_id,
            target_date=args.target_date,
            overwrite=args.overwrite,
            dry_run=args.dry_run,
            force_review=args.force_review,
        )]
    failed = [item for item in outcomes if item.get("error")]
    print(canonical_json({
        "status": "ok" if not failed else "error",
        "decisions": outcomes,
    }))
    return 0 if not failed else 2


def _data(args, settings: FoundationSettings) -> int:
    warehouse = MarketDataWarehouse(settings.paths.market_data)
    if args.data_command == "sources":
        print(canonical_json({"status": "ok", "sources": source_statuses()}))
        return 0
    if args.data_command == "collect":
        registry = default_provider_registry()
        request = ProviderRequest(
            ProviderCapability(args.capability),
            args.start_date,
            args.end_date,
            tuple(args.instrument),
        )
        observed, reused = MarketIngestionService(registry, warehouse).capture_resumable(
            args.provider, request, refresh=args.refresh,
        )
        print(canonical_json({
            "status": "ok",
            "observation_id": observed.observation_id,
            "provider": observed.provider,
            "coverage": observed.coverage,
            "source_metadata": observed.source_metadata,
            "reused": reused,
        }))
        return 0
    if args.data_command == "build-history":
        sources = tuple(args.source)
        if len(sources) not in (2, 3):
            raise ValueError("build-history requires two or three distinct --source values")
        if len(set(sources)) != len(sources):
            raise ValueError("build-history source providers must be distinct")
        builder = HistoryDatabaseBuilder(
            warehouse,
            settings.paths.report_root,
            source_pair=sources[:2],
            adjudicator_provider=sources[2] if len(sources) == 3 else None,
        )
        spec = HistoryBuildSpec(
            start_date=args.start_date,
            end_date=args.end_date,
            universe_as_of=args.universe_as_of,
            instrument_ids=tuple(args.instrument),
            exchanges=tuple(args.exchange or ("SH", "SZ")),
            asset_types=tuple(args.asset_type or ("stock", "etf")),
            batch_size=args.batch_size,
            max_instruments=args.max_instruments,
            shard_count=args.shard_count,
            shard_index=args.shard_index,
            publish=args.publish and not args.assemble_only,
            refresh=args.refresh,
        )
        result = (
            builder.assemble(spec, publish=args.publish)
            if args.assemble_only else builder.build(spec)
        )
        print(canonical_json({"status": result.status, **result.to_dict()}))
        return 0 if result.status == "complete" else 2
    if args.data_command == "derive-current-research":
        result = derive_current_research_snapshot(
            warehouse,
            settings.paths.report_root,
            source_snapshot_id=args.source_snapshot_id[0],
            supplement_snapshot_ids=tuple(args.source_snapshot_id[1:]),
            universe_observation_id=args.universe_observation_id,
            universe_as_of=args.universe_as_of,
            start_date=args.start_date,
            end_date=args.end_date,
            publish=args.publish,
        )
        print(canonical_json({"status": result.status, **result.to_dict()}))
        return 0 if result.status == "complete" else 2
    if args.data_command == "collect-status":
        result = SimulationStatusCollector(
            warehouse, settings.paths.report_root,
        ).collect(StatusCollectionSpec(
            source_snapshot_id=args.source_snapshot_id,
            calendar_observation_id=args.calendar_observation_id,
            provider_name=args.provider,
            batch_size=args.batch_size,
            shard_count=args.shard_count,
            shard_index=args.shard_index,
            refresh=args.refresh,
        ))
        print(canonical_json({"status": result.status, **to_primitive(result)}))
        return 0 if result.status == "complete" else 2
    if args.data_command == "collect-evidence":
        result = SimulationEvidenceCollector(
            warehouse, settings.paths.report_root,
        ).collect(EvidenceCollectionSpec(
            source_snapshot_id=args.source_snapshot_id,
            kind=args.kind,
            batch_size=args.batch_size,
            refresh=args.refresh,
            predecessor_snapshot_id=args.predecessor_snapshot_id,
        ))
        print(canonical_json({"status": result.status, **to_primitive(result)}))
        return 0 if result.status == "complete" else 2
    if args.data_command == "validate-simulation-increment":
        candidate = warehouse.load_observation(args.candidate_observation_id)
        instruments = warehouse.read_observation_table(
            candidate.observation_id, MarketTable.INSTRUMENTS,
        )
        scope = UniverseScope(
            CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
            args.universe_as_of,
            args.start_date,
            args.end_date,
            survivorship_bias=True,
            instrument_ids=tuple(sorted(map(str, instruments["instrument_id"]))),
        )
        result = SimulationIncrementValidator(
            warehouse, settings.paths.report_root,
        ).validate_and_record(
            candidate_observation_id=candidate.observation_id,
            calendar_observation_id=args.calendar_observation_id,
            universe_scope=scope,
            description=args.description,
        )
        print(canonical_json({"status": "complete", **to_primitive(result)}))
        return 0
    if args.data_command == "extend-simulation":
        predecessor = warehouse.load_snapshot(args.predecessor_snapshot_id)
        previous_scope = predecessor.plan.universe_scope
        if previous_scope is None:
            raise ValueError("Predecessor snapshot has no universe scope")
        target_instrument_ids = set(previous_scope.instrument_ids)
        for observation_id in args.increment_observation_id:
            increment = warehouse.load_observation(observation_id)
            partition_quality = increment.source_metadata.get("partition_quality")
            if not isinstance(partition_quality, Mapping):
                raise ValueError(
                    f"Increment has no partition quality: {observation_id}"
                )
            target_instrument_ids.update(map(
                str, partition_quality.get("instrument_ids", ()),
            ))
        scope = UniverseScope(
            previous_scope.definition,
            args.universe_as_of,
            previous_scope.history_start,
            args.target_date,
            survivorship_bias=previous_scope.survivorship_bias,
            instrument_ids=tuple(sorted(target_instrument_ids)),
        )
        result = SimulationSnapshotBuilder(
            warehouse, settings.paths.report_root,
        ).extend(
            predecessor_snapshot_id=args.predecessor_snapshot_id,
            calendar_observation_id=args.calendar_observation_id,
            increment_observation_ids=tuple(args.increment_observation_id),
            universe_scope=scope,
            description=args.description,
            publish=args.publish,
        )
        print(canonical_json({"status": "complete", **to_primitive(result)}))
        return 0
    if args.data_command == "build-snapshot":
        observed = warehouse.load_observation(args.observation_id)
        readiness = ReadinessProfile(args.readiness)
        plan = SnapshotPlan(
            tuple(_scoped_source_slice(
                observed, item.table, "explicit CLI selection of one reviewed observation",
            ) for item in _files_for_readiness(observed, readiness)),
            args.description,
            readiness=readiness,
            universe_scope=_cli_universe_scope(args, warehouse, observed, readiness),
        )
        snapshot = warehouse.build_snapshot(plan)
        if args.publish:
            warehouse.publish(snapshot.snapshot_id)
        print(canonical_json({
            "status": "ok", "snapshot_id": snapshot.snapshot_id,
            "quality": snapshot.quality, "published": bool(args.publish),
        }))
        return 0
    if args.data_command == "compose-history":
        snapshot, policy_version = compose_history_snapshot(
            warehouse,
            tuple(args.observation_id),
            args.description,
            publish=args.publish,
        )
        published = bool(args.publish and snapshot.quality.ready)
        payload = {
            "decision_source": "https://github.com/zhan7653/miniQMT/issues/7",
            "kind": "history_partition_composition",
            "observation_ids": tuple(args.observation_id),
            "policy_version": policy_version,
            "snapshot_id": snapshot.snapshot_id,
            "quality": snapshot.quality,
            "published": published,
        }
        report_path = settings.paths.report_root / (
            f"history-composition-{snapshot.snapshot_id}.json"
        )
        _write_immutable_report(report_path, payload)
        print(canonical_json({
            "status": "ok" if snapshot.quality.ready else "incomplete",
            **payload,
            "report": report_path,
        }))
        return 0 if snapshot.quality.ready else 2
    if args.data_command == "reconcile":
        readiness = ReadinessProfile(args.readiness)
        service = ReconciliationService(
            warehouse,
            default_reconciliation_policy(readiness),
        )
        observed, report = service.reconcile_and_record(
            args.observation_id,
            readiness=readiness,
            description=args.description,
        )
        if readiness is ReadinessProfile.SIMULATION:
            if args.publish:
                raise ValueError(
                    "Simulation reconciliation cannot publish directly; validate the "
                    "EOD partition and use extend-simulation"
                )
            reconciliation_payload = {
                "decision_source": "https://github.com/zhan7653/miniQMT/issues/7",
                "canonical_observation_id": observed.observation_id,
                "snapshot_id": None,
                "readiness": readiness,
                "reconciliation": report,
                "published": False,
            }
            report_path = settings.paths.report_root / (
                f"reconciliation-{observed.observation_id}-candidate.json"
            )
            _write_immutable_report(report_path, reconciliation_payload)
            print(canonical_json({
                "status": "ok" if report.ready else "incomplete",
                **reconciliation_payload,
                "report": report_path,
            }))
            return 0 if report.ready else 2
        plan = SnapshotPlan(
            tuple(_scoped_source_slice(
                observed,
                item.table,
                f"field-level reconciliation policy {report.policy_version}",
            ) for item in _files_for_readiness(observed, readiness)),
            args.description,
            readiness=readiness,
            universe_scope=_cli_universe_scope(args, warehouse, observed, readiness),
        )
        snapshot = warehouse.build_snapshot(plan)
        published = False
        if args.publish and snapshot.quality.ready:
            warehouse.publish(snapshot.snapshot_id)
            published = True
        reconciliation_payload = {
            "decision_source": "https://github.com/zhan7653/miniQMT/issues/7",
            "canonical_observation_id": observed.observation_id,
            "snapshot_id": snapshot.snapshot_id,
            "readiness": readiness,
            "reconciliation": report,
            "quality": snapshot.quality,
            "published": published,
        }
        publication_state = "published" if published else "unpublished"
        report_path = settings.paths.report_root / (
            f"reconciliation-{observed.observation_id}-{publication_state}.json"
        )
        _write_immutable_report(report_path, reconciliation_payload)
        print(canonical_json({
            "status": "ok" if report.ready and snapshot.quality.ready else "incomplete",
            **reconciliation_payload,
            "report": report_path,
        }))
        return 0 if report.ready and snapshot.quality.ready else 2
    if args.data_command == "inspect":
        snapshot_id = args.snapshot_id or warehouse.current_snapshot_id()
        manifest = warehouse.load_snapshot(snapshot_id, require_ready=False)
        print(canonical_json({
            "status": "ok", "snapshot_id": snapshot_id, "quality": manifest.quality,
            "plan": manifest.plan, "files": manifest.files,
        }))
        return 0
    raise AssertionError(args.data_command)


def _web(args, settings: FoundationSettings) -> int:
    import threading
    import webbrowser

    import uvicorn

    from fundlab.web import create_app

    config_path = Path(args.config).resolve()
    app = create_app(
        settings,
        repo_root=config_path.parent.parent,
        config_path=config_path,
    )
    url = f"http://{args.host}:{args.port}"
    print(canonical_json({"status": "ok", "dashboard": url}))
    if not args.no_browser:
        threading.Timer(1.0, webbrowser.open, args=(url,)).start()
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    return 0


def _daily(args, settings: FoundationSettings) -> int:
    if args.daily_command == "run":
        result = DailyPipeline(settings).run(
            target_date=args.target_date,
            skip_data=args.skip_data,
            skip_accounts=args.skip_accounts,
        )
        print(canonical_json({
            "status": result.status,
            "target_date": result.target_date,
            "snapshot_id": result.snapshot_id,
            "stages": [to_primitive(item) for item in result.stages],
            "accounts": result.accounts,
            "report": result.report_path,
        }))
        return result.exit_code
    if args.daily_command == "status":
        warehouse = MarketDataWarehouse(settings.paths.market_data)
        snapshot = warehouse.load_snapshot(warehouse.current_snapshot_id())
        scope = snapshot.plan.universe_scope
        repository = TradingRepository(settings.paths.trading_database)
        accounts = []
        for item in settings.daily.accounts:
            entry: dict = {"account_id": item.account_id, "strategy": item.strategy}
            try:
                repository.account(item.account_id)
                _, selected_parent = repository.selected_state(item.account_id)
                entry["exists"] = True
                entry["head"] = (
                    None if selected_parent is None
                    else repository.run(selected_parent).binding.end_date
                )
            except Exception:
                entry["exists"] = False
            accounts.append(entry)
        print(canonical_json({
            "status": "ok",
            "snapshot_id": snapshot.snapshot_id,
            "published_end": None if scope is None else scope.history_end,
            "instruments": 0 if scope is None else len(scope.instrument_ids),
            "accounts": accounts,
            "agent_decision_dir": settings.daily.agent_decision_root,
            "session_cutoff_local": settings.daily.session_cutoff.isoformat(timespec="minutes"),
        }))
        return 0
    raise AssertionError(args.daily_command)


def _account(args, settings: FoundationSettings) -> int:
    repository = TradingRepository(settings.paths.trading_database)
    if args.account_command == "create":
        account = repository.create_account(
            args.account_id, args.name, PortfolioState.with_cash(args.initial_cash),
        )
    elif args.account_command == "show":
        account = repository.account(args.account_id)
    else:
        raise AssertionError(args.account_command)
    print(canonical_json({"status": "ok", "account": account}))
    return 0


def _simulate(args, settings: FoundationSettings) -> int:
    if (
        not settings.fee_schedule.trusted_for_simulation
        and not args.allow_untrusted_fees
    ):
        raise ValueError(
            "Fee schedule is not trusted for simulation; update config or pass --allow-untrusted-fees "
            "to produce an explicitly incomplete run"
        )
    weights = _weights(args.weight)
    market = CanonicalMarketData.open(settings.paths.market_data, args.snapshot_id)
    repository = TradingRepository(settings.paths.trading_database)
    source = StaticAllocationSource(weights)
    service = SimulationService(
        market_data=market,
        repository=repository,
        execution_policy=settings.execution_policy,
        risk_policy=settings.risk_policy,
        fee_schedule=settings.fee_schedule,
        seed=args.seed,
    )
    if args.date is not None:
        if args.end_date is not None:
            raise ValueError("--end-date cannot be used with --date")
        outcome = service.run_daily(args.account_id, args.date, source)
    else:
        if args.end_date is None:
            raise ValueError("Historical simulation requires --end-date")
        outcome = service.run_historical(
            args.account_id, args.start_date, args.end_date, source,
            promote=args.promote_historical,
        )
    feedback = build_simulation_feedback(repository, outcome.run.run_id)
    report = {
        "decision_source": "https://github.com/zhan7653/miniQMT/issues/6",
        "binding": outcome.run.binding,
        "feedback": feedback,
        "feedback_hash": feedback.feedback_hash,
    }
    report_path = settings.paths.report_root / f"simulation-{outcome.run.run_id}.json"
    _write_immutable_report(report_path, report)
    print(canonical_json({
        "status": "ok", "report": report_path, "run_id": feedback.run_id,
        "run_status": outcome.run.status, "result_hash": feedback.result_hash,
        "feedback_hash": feedback.feedback_hash, "quality": feedback.quality,
        "incomplete_reasons": feedback.incomplete_reasons, "reused": outcome.reused,
    }))
    return 0


def _cli_universe_scope(
    args,
    warehouse: MarketDataWarehouse,
    observed,
    readiness: ReadinessProfile,
) -> UniverseScope | None:
    supplied = (args.universe_as_of, args.history_start, args.history_end)
    if all(item is None for item in supplied) and readiness is ReadinessProfile.RESEARCH_PRICE:
        return None
    if any(item is None for item in supplied):
        raise ValueError(
            "simulation snapshots require --universe-as-of, --history-start, and --history-end"
        )
    instruments = warehouse.read_observation_table(
        observed.observation_id, MarketTable.INSTRUMENTS,
    )
    instrument_ids = tuple(sorted(set(map(str, instruments["instrument_id"]))))
    return UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        args.universe_as_of,
        args.history_start,
        args.history_end,
        survivorship_bias=True,
        instrument_ids=instrument_ids,
    )


def _weights(values: Sequence[str]) -> dict[str, Decimal]:
    found: dict[str, Decimal] = {}
    for value in values:
        symbol, separator, raw_weight = value.partition("=")
        if not separator or not symbol.strip():
            raise ValueError(f"Weight must use INSTRUMENT=WEIGHT: {value}")
        if symbol in found:
            raise ValueError(f"Duplicate target weight: {symbol}")
        found[symbol] = Decimal(raw_weight)
    return found


def _scoped_source_slice(observation, table: MarketTable, reason: str) -> SourceSlice:
    dated = {
        MarketTable.CALENDAR,
        MarketTable.DAILY_BARS,
        MarketTable.CORPORATE_ACTIONS,
        MarketTable.ADJUSTMENT_FACTORS,
    }
    instrument_scoped = {
        MarketTable.INSTRUMENTS,
        MarketTable.DAILY_BARS,
        MarketTable.CORPORATE_ACTIONS,
        MarketTable.ADJUSTMENT_FACTORS,
    }
    return SourceSlice(
        observation.observation_id,
        table,
        reason,
        instrument_ids=(
            observation.request.instrument_ids if table in instrument_scoped else ()
        ),
        start_date=(observation.request.start_date if table in dated else None),
        end_date=(observation.request.end_date if table in dated else None),
    )


def _files_for_readiness(observation, readiness: ReadinessProfile):
    required = {
        ReadinessProfile.RESEARCH_PRICE: {
            MarketTable.INSTRUMENTS,
            MarketTable.DAILY_BARS,
        },
        ReadinessProfile.SIMULATION: set(MarketTable),
    }[ReadinessProfile(readiness)]
    complete = {claim.table for claim in observation.coverage if claim.complete}
    return tuple(
        item for item in observation.files
        if item.table in required or item.table in complete
    )


def _write_immutable_report(path: Path, payload) -> None:
    content = canonical_json(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_text(encoding="utf-8") != content:
            raise RuntimeError(f"Completed simulation report is immutable: {path}")
        return
    path.write_text(content, encoding="utf-8", newline="\n")


if __name__ == "__main__":
    raise SystemExit(main())
