from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from fundlab.cli import build_parser, main
from fundlab.settings import load_foundation_settings
from tests.canonical.fixtures import DAYS, ready_market


def test_cli_exposes_only_the_componentized_simulation_publication_flow():
    parser = build_parser()

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


def test_committed_simulation_fee_schedule_has_dated_public_boundaries():
    settings = load_foundation_settings(
        Path(__file__).resolve().parents[2] / "config" / "fundlab.yaml"
    )
    schedule = settings.fee_schedule

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
