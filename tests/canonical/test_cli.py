from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

import fundlab.cli as cli_module
from fundlab.agent import build_policy
from fundlab.cli import build_parser, main
from fundlab.settings import load_foundation_settings
from tests.canonical.fixtures import DAYS, ready_market


def test_cli_exposes_only_the_componentized_simulation_publication_flow():
    parser = build_parser()

    web = parser.parse_args(["web", "--no-browser"])
    assert web.host == "127.0.0.1"
    assert web.port == 8610

    forced_review = parser.parse_args([
        "agent", "decide", "--account-id", "paper-dividend",
        "--force-review", "--dry-run",
    ])
    assert forced_review.force_review is True
    assert forced_review.dry_run is True

    with pytest.raises(SystemExit):
        parser.parse_args([
            "data", "build-snapshot",
            "--observation-id", "candidate",
            "--description", "must not parse",
            "--readiness", "simulation",
            "--publish",
        ])

    validated = parser.parse_args([
        "data", "validate-simulation-increment",
        "--candidate-observation-id", "candidate",
        "--calendar-observation-id", "calendar",
        "--universe-as-of", "2026-07-24",
        "--start-date", "2026-07-24",
        "--end-date", "2026-07-24",
        "--description", "validated increment",
    ])
    assert validated.data_command == "validate-simulation-increment"

    extend = parser.parse_args([
        "data", "extend-simulation",
        "--predecessor-snapshot-id", "predecessor",
        "--calendar-observation-id", "calendar",
        "--increment-observation-id", "validated-partition",
        "--universe-as-of", "2026-07-24",
        "--target-date", "2026-07-24",
        "--description", "atomic increment",
        "--publish",
    ])
    assert extend.data_command == "extend-simulation"
    assert extend.publish is True

    evidence = parser.parse_args([
        "data", "collect-evidence",
        "--source-snapshot-id", "research",
        "--predecessor-snapshot-id", "predecessor",
        "--kind", "stock-actions",
    ])
    assert evidence.predecessor_snapshot_id == "predecessor"


def test_cli_loads_only_repository_local_environment_for_custom_config(
    tmp_path, monkeypatch,
):
    loaded = []
    settings = object()
    monkeypatch.setattr(cli_module, "load_local_environment", loaded.append)
    monkeypatch.setattr(cli_module, "load_foundation_settings", lambda path: settings)
    monkeypatch.setattr(
        cli_module, "_data", lambda args, received: int(received is not settings),
    )

    result = main([
        "--config", str(tmp_path / "external" / "fundlab.yaml"),
        "data", "sources",
    ])

    assert result == 0
    assert loaded == [Path(cli_module.__file__).resolve().parents[1] / ".env.local"]


def test_committed_simulation_fee_schedule_has_dated_public_boundaries():
    settings = load_foundation_settings(
        Path(__file__).resolve().parents[2] / "config" / "fundlab.yaml"
    )
    schedule = settings.fee_schedule
    agent = settings.agent.policies["paper-agent"]
    policy = build_policy(agent.kind, agent.params)
    for declared in settings.agent.policies.values():
        build_policy(declared.kind, declared.params)

    assert policy.policy_id == "momentum-rotation"
    assert policy.risk_instrument == "510300.SH"
    assert policy.defensive_instrument == "511010.SH"
    assert {
        item.account_id for item in settings.daily.accounts
    } >= {
        "paper-dividend-rules",
        "paper-crash-aggressive",
        "paper-crash-cn-small",
        "paper-crash-cn-wide",
        "paper-crash-conservative",
        "paper-crash-fast-profit",
        "paper-crash-global",
        "paper-crash-semiconductor",
        "paper-crash-vol-control",
        "paper-dual-momentum",
        "paper-sector-momentum",
        "paper-inverse-vol",
        "paper-low-beta",
        "paper-ma-grid-510050",
        "paper-ma-grid-510300",
        "paper-ma-grid-510500",
        "paper-ma-grid-511380",
        "paper-risk-parity",
        "paper-st-momentum",
        "paper-st-removal",
        "paper-trend-vol",
    }
    assert build_policy(
        settings.agent.policies["paper-dividend-rules"].kind,
        settings.agent.policies["paper-dividend-rules"].params,
    ).policy_id == "dividend-rules"
    assert build_policy(
        settings.agent.policies["paper-dual-momentum"].kind,
        settings.agent.policies["paper-dual-momentum"].params,
    ).policy_id == "dual-momentum"
    sector_policy = build_policy(
        settings.agent.policies["paper-sector-momentum"].kind,
        settings.agent.policies["paper-sector-momentum"].params,
    )
    assert sector_policy.policy_id == "sector-momentum"
    assert sector_policy.whitelist_version == "cn-core-sector-etf-2026-08-08-v1"
    assert sector_policy.sector_mapping["通信设备"] == "515880.SH"
    grid_policy = build_policy(
        settings.agent.policies["paper-ma-grid-511380"].kind,
        settings.agent.policies["paper-ma-grid-511380"].params,
    )
    assert grid_policy.policy_id == "moving-average-grid"
    assert grid_policy.config.instrument == "511380.SH"
    assert grid_policy.config.max_weight == Decimal("0.85")
    daily_accounts = {item.account_id: item for item in settings.daily.accounts}
    assert {
        daily_accounts[account_id].strategy
        for account_id in (
            "paper-ma-grid-510050",
            "paper-ma-grid-510300",
            "paper-ma-grid-510500",
            "paper-ma-grid-511380",
        )
    } == {"moving-average-grid"}
    assert build_policy(
        settings.agent.policies["paper-inverse-vol"].kind,
        settings.agent.policies["paper-inverse-vol"].params,
    ).policy_id == "inverse-volatility"
    assert build_policy(
        settings.agent.policies["paper-risk-parity"].kind,
        settings.agent.policies["paper-risk-parity"].params,
    ).policy_id == "correlation-risk-parity"
    assert build_policy(
        settings.agent.policies["paper-trend-vol"].kind,
        settings.agent.policies["paper-trend-vol"].params,
    ).policy_id == "trend-volatility-target"
    assert build_policy(
        settings.agent.policies["paper-low-beta"].kind,
        settings.agent.policies["paper-low-beta"].params,
    ).policy_id == "low-beta-volatility"
    assert build_policy(
        settings.agent.policies["paper-st-removal"].kind,
        settings.agent.policies["paper-st-removal"].params,
    ).policy_id == "st-removal-momentum"
    assert build_policy(
        settings.agent.policies["paper-st-momentum"].kind,
        settings.agent.policies["paper-st-momentum"].params,
    ).policy_id == "st-active-momentum"
    assert build_policy(
        settings.agent.policies["paper-crash-global"].kind,
        settings.agent.policies["paper-crash-global"].params,
    ).policy_id == "crisis-drawdown"

    before_2022 = schedule.calculate(
        day=date(2022, 4, 28), asset_type="stock", exchange="SH",
        side="sell", amount=Decimal("100000"),
    )
    after_2022 = schedule.calculate(
        day=date(2022, 4, 29), asset_type="stock", exchange="SH",
        side="sell", amount=Decimal("100000"),
    )
    after_2023 = schedule.calculate(
        day=date(2023, 8, 28), asset_type="stock", exchange="SH",
        side="sell", amount=Decimal("100000"),
    )

    assert schedule.trusted_for_simulation
    assert before_2022.transfer_fee == Decimal("2.00")
    assert after_2022.transfer_fee == Decimal("1.00")
    assert before_2022.stamp_duty == after_2022.stamp_duty == Decimal("100.00")
    assert after_2023.stamp_duty == Decimal("50.00")
    with pytest.raises(LookupError, match="No fee rule"):
        schedule.calculate(
            day=date(2015, 7, 31), asset_type="stock", exchange="SH",
            side="buy", amount=Decimal("100000"),
        )


def test_canonical_cli_creates_account_and_runs_shared_kernel(tmp_path, capsys):
    market_root = tmp_path / "market"
    market = ready_market(market_root)
    config = tmp_path / "fundlab.yaml"
    config.write_text(yaml.safe_dump({
        "paths": {
            "market_data": str(market_root),
            "trading_database": str(tmp_path / "trading.sqlite3"),
            "report_root": str(tmp_path / "reports"),
        },
        "execution": {
            "policy_id": "test", "version": "1", "maximum_participation": "0.05",
            "base_slippage_bps": "0", "impact_bps_at_max_participation": "0",
        },
        "risk": {"policy_id": "test", "version": "1"},
        "agent": {"policies": {"paper-grid": {
            "type": "moving-average-grid",
            "instrument": "600000.SH",
            "activation_date": DAYS[0].isoformat(),
            "max_weight": "0.8",
            "minimum_grid_step": "0.01",
            "moving_average_days": 2,
            "trend_average_days": 3,
            "trend_slope_days": 1,
            "residual_window_days": 2,
            "defensive_confirm_days": 2,
            "defensive_brake_days": 3,
            "reset_confirm_days": 1,
            "maximum_cycle_days": 5,
            "pause_tier": 2,
            "brake_tier": 3,
            "startup_ramp_days": 1,
        }}},
        "fees": {
            "schedule_id": "test", "version": "1", "trusted_for_simulation": True,
            "verification_note": "test fixture", "rules": [{
                "effective_from": "2023-01-01", "asset_types": ["stock"], "exchanges": ["SH"],
                "broker_commission_rate": "0.0003", "minimum_commission": "5",
                "stamp_duty_sell_rate": "0.0005", "transfer_fee_rate": "0.00001",
            }],
        },
    }), encoding="utf-8")

    assert main(["--config", str(config), "data", "sources"]) == 0
    sources = json.loads(capsys.readouterr().out)["sources"]
    assert {item["provider"] for item in sources} == {
        "baostock", "cninfo-public", "eastmoney-efinance", "exchange-public",
        "eastmoney-fund-public", "sina-calendar", "sina-etf", "tickflow",
        "xtquant",
    }

    assert main([
        "--config", str(config), "account", "create", "--account-id", "cli",
        "--name", "CLI", "--initial-cash", "100000",
    ]) == 0
    capsys.readouterr()
    simulate_args = [
        "--config", str(config), "simulate", "--account-id", "cli",
        "--snapshot-id", market.snapshot_id, "--weight", "600000.SH=0.8",
        "--start-date", DAYS[0].isoformat(), "--end-date", DAYS[-1].isoformat(),
    ]
    assert main(simulate_args) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "ok" and payload["incomplete_reasons"] == []
    report = Path(payload["report"])
    assert report.is_file()
    stored = json.loads(report.read_text(encoding="utf-8"))
    assert stored["feedback"]["run_id"] == payload["run_id"]
    assert stored["feedback_hash"] == payload["feedback_hash"]
    assert main(simulate_args) == 0
    repeated = json.loads(capsys.readouterr().out)
    assert repeated["reused"] is True and repeated["report"] == payload["report"]

    assert main([
        "--config", str(config), "account", "create", "--account-id", "grid-cli",
        "--name", "Grid CLI", "--initial-cash", "100000",
    ]) == 0
    capsys.readouterr()
    assert main([
        "--config", str(config), "simulate", "--account-id", "grid-cli",
        "--snapshot-id", market.snapshot_id,
        "--policy-account-id", "paper-grid",
        "--policy-activation-date", DAYS[0].isoformat(),
        "--start-date", DAYS[0].isoformat(), "--end-date", DAYS[-1].isoformat(),
    ]) == 0
    grid_payload = json.loads(capsys.readouterr().out)
    grid_report = json.loads(Path(grid_payload["report"]).read_text(encoding="utf-8"))
    assert grid_report["decision_source"]["kind"] == "moving-average-grid"
    assert grid_report["decision_source"]["policy_account_id"] == "paper-grid"
    assert grid_report["feedback"]["orders"] == 1


def test_agent_cli_runs_the_configured_policy_in_dry_run(
    tmp_path, capsys, monkeypatch,
):
    from tests.canonical.test_agent_decision import agent_settings

    ready_market(tmp_path / "market", close_values=(10.0, 11.0, 12.0, 13.0))
    settings = agent_settings(tmp_path)
    monkeypatch.setattr(cli_module, "load_foundation_settings", lambda _: settings)

    code = main([
        "--config", "ignored.yaml", "agent", "decide",
        "--account-id", "paper-agent", "--dry-run",
    ])

    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "ok"
    assert payload["decisions"][0]["written"] is False
    assert payload["decisions"][0]["target_weights"] == {"600000.SH": "0.6"}
