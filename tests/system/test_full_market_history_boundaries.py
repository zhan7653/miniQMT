from __future__ import annotations

from datetime import date, datetime

import pytest

from fundlab.data.pipeline import FullMarketHistoryRunner, HistoryRunSpec
from fundlab.data.platform import CollectionPhase, DataCatalog, PreflightResult, ProviderHealth
from fundlab.data.storage import VersionedParquetStore
from scripts.bootstrap_fund_history import build_parser


class UnavailableProvider:
    name = "xtquant"

    def preflight(self):
        return PreflightResult(
            self.name, ProviderHealth.SERVICE_UNAVAILABLE, datetime.now().astimezone(), "fixture offline",
        )


def build_runner(tmp_path):
    catalog = DataCatalog(tmp_path / "v2" / "catalog.sqlite3"); catalog.initialize()
    return FullMarketHistoryRunner(
        provider=UnavailableProvider(), catalog=catalog,
        store=VersionedParquetStore(tmp_path / "v2", catalog), report_root=tmp_path / "reports",
        history_config={
            "partition_years": 1,
            "retry": {"max_attempts": 3, "backoff_seconds": [0, 0, 0]},
            "throttle": {
                "initial": {"workers": 1, "request_interval_seconds": 2, "cooldown_every_symbols": 20, "cooldown_seconds": 60},
                "maximum": {"workers": 4, "requests_per_second": 2, "cooldown_every_symbols": 100, "cooldown_seconds": 30},
            },
            "coverage": {"min_symbol_trading_day_ratio": 0.95},
        },
        config_hash="fixture", sleeper=lambda _: None,
    ), catalog


def test_phase_one_rejects_publication_before_any_pointer_change(tmp_path):
    history, catalog = build_runner(tmp_path)
    result = history.run(HistoryRunSpec(CollectionPhase.PUBLISH, date(2024, 1, 5), minimum_free_bytes=0))
    assert result.status == "blocked"
    assert "explicit publish=True" in result.error
    assert catalog.latest_complete() is None


def test_canary_spec_cannot_request_publication():
    with pytest.raises(ValueError, match="publication is forbidden"):
        HistoryRunSpec(CollectionPhase.CANARY, date(2024, 1, 5), publish=True)


def test_service_preflight_failure_stops_before_discovery_or_collection(tmp_path):
    history, catalog = build_runner(tmp_path)
    result = history.run(HistoryRunSpec(CollectionPhase.CANARY, date(2024, 1, 5), minimum_free_bytes=0))
    assert result.status == "failed"
    assert "Provider preflight failed" in result.error
    assert catalog.list_partitions() == ()


def test_disk_guard_stops_before_provider_access(tmp_path):
    history, catalog = build_runner(tmp_path)
    result = history.run(HistoryRunSpec(
        CollectionPhase.CANARY, date(2024, 1, 5), minimum_free_bytes=10**30,
    ))
    assert result.status == "failed"
    assert "Insufficient disk space" in result.error
    assert catalog.list_partitions() == ()


def test_canary_selection_is_capped_and_stably_sorted():
    master = [
        {"symbol": f"{510000 + index:06d}.SH", "exchange": "SH", "product_type": "ETF",
         "is_active": 1, "listed_date": "2020-01-01"}
        for index in range(30)
    ]
    first, gaps = FullMarketHistoryRunner.select_canary(master, 20)
    second, _ = FullMarketHistoryRunner.select_canary(list(reversed(master)), 20)
    assert len(first) == 20
    assert [row["symbol"] for row in first] == [row["symbol"] for row in second]
    assert "exchange=SZ" in gaps and "product_type=LOF" in gaps


def test_cli_accepts_noninteractive_phase_surface():
    parser = build_parser()
    for phase in ("preflight", "discover", "canary", "collect", "publish"):
        args = parser.parse_args(["--phase", phase, "--no-publish", "--json"])
        assert args.phase == phase and args.no_publish and args.json
