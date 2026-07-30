from datetime import timedelta
import json
import multiprocessing

import pytest

import fundlab.marketdata.warehouse as warehouse_module

from fundlab.common.canonical import canonical_json, stable_digest
from fundlab.marketdata import (
    CanonicalMarketData,
    MarketIngestionService,
    MarketDataWarehouse,
    MarketTable,
    PriceMode,
    ProviderCapability,
    ProviderRegistry,
    ProviderRequest,
    ProviderSelectionError,
    ReadinessProfile,
    SnapshotNotReadyError,
    SnapshotPlan,
    SourceConflictError,
    SourceSlice,
    UniverseScope,
    CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
)
from tests.canonical.fixtures import DAYS, FUTURE_DAYS, fixture_universe_scope, observation


def _publish_if_current_worker(root, expected, successor, start, results):
    warehouse = MarketDataWarehouse(root)
    start.wait()
    try:
        warehouse.publish_if_current(expected, successor)
        results.put(("published", successor))
    except Exception as exc:
        results.put(("rejected", str(exc)))


def test_provider_request_parameters_are_stable_across_json_sequence_shapes():
    left = ProviderRequest(
        ProviderCapability.INSTRUMENTS,
        parameters={"exchanges": ["SH", "SZ"], "nested": {"values": [1, 2]}},
    )
    right = ProviderRequest(
        ProviderCapability.INSTRUMENTS,
        parameters={"exchanges": ("SH", "SZ"), "nested": {"values": (1, 2)}},
    )

    assert left == right


def plan(observation_id, *, require_complete=True):
    return SnapshotPlan(
        tuple(SourceSlice(observation_id, table, "fixture is the explicit canonical source") for table in MarketTable),
        "complete deterministic fixture",
        require_complete,
        readiness=ReadinessProfile.RESEARCH_PRICE,
        universe_scope=fixture_universe_scope(),
    )


def test_observation_snapshot_and_point_in_time_portal_are_immutable(tmp_path):
    warehouse = MarketDataWarehouse(tmp_path / "market")
    observed = warehouse.record_observation(observation())
    snapshot = warehouse.build_snapshot(plan(observed.observation_id))
    assert snapshot.quality.ready
    assert list(warehouse.snapshot_path(snapshot.snapshot_id).iterdir()) == [
        warehouse.snapshot_path(snapshot.snapshot_id) / "manifest.json"
    ]
    assert snapshot.quality.row_counts == {
        "adjustment_factors": 0, "calendar": len(DAYS) + len(FUTURE_DAYS),
        "corporate_actions": 0, "daily_bars": 8, "instruments": 1,
    }
    warehouse.publish(snapshot.snapshot_id)
    portal = CanonicalMarketData.open(
        tmp_path / "market", required_readiness=ReadinessProfile.RESEARCH_PRICE,
    )
    assert portal.snapshot_id == snapshot.snapshot_id
    assert portal.trading_days(DAYS[0], DAYS[-1]) == DAYS
    bars = portal.bars(
        ["600000.SH"], DAYS[0], DAYS[1], price_mode=PriceMode.RAW, as_of=DAYS[1],
    )
    assert list(bars["source_provider"].unique()) == ["fixture"]
    assert list(bars["source_observation_id"].unique()) == [observed.observation_id]
    with pytest.raises(ValueError, match="point-in-time"):
        portal.adjusted_history(["600000.SH"], DAYS[0], DAYS[2], as_of=DAYS[1])


def test_observation_commit_retries_transient_windows_permission_error(
    tmp_path, monkeypatch,
):
    real_replace = warehouse_module.os.replace
    calls = 0

    def flaky_replace(source, target):
        nonlocal calls
        if str(source).find(".observation-") >= 0:
            calls += 1
            if calls == 1:
                raise PermissionError(5, "temporary scanner lock")
        return real_replace(source, target)

    monkeypatch.setattr(warehouse_module.os, "replace", flaky_replace)
    observed = MarketDataWarehouse(tmp_path / "market").record_observation(
        observation()
    )

    assert observed.observation_id.startswith("obs-")
    assert calls == 2


def test_loaded_snapshot_query_does_not_repeat_whole_snapshot_verification(
    tmp_path, monkeypatch,
):
    warehouse = MarketDataWarehouse(tmp_path / "market")
    observed = warehouse.record_observation(observation())
    snapshot = warehouse.load_snapshot(
        warehouse.build_snapshot(plan(observed.observation_id)).snapshot_id
    )

    def unexpected_reload(*_args, **_kwargs):
        raise AssertionError("the already verified snapshot must not be reloaded")

    monkeypatch.setattr(warehouse, "load_snapshot", unexpected_reload)
    bars = warehouse.query_loaded_snapshot_table(
        snapshot,
        MarketTable.DAILY_BARS,
        instrument_ids=("600000.SH",),
        start_date=DAYS[0],
        end_date=DAYS[0],
        price_mode="raw",
    )

    assert len(bars) == 1


def test_cross_source_overlap_requires_explicit_precedence(tmp_path):
    warehouse = MarketDataWarehouse(tmp_path / "market")
    first = warehouse.record_observation(observation())
    revised = warehouse.record_observation(observation(
        provider="revision", close_shift=1.0, observed_second=1,
    ))
    selections = [
        SourceSlice(first.observation_id, table, "base source") for table in MarketTable
    ]
    selections.append(SourceSlice(revised.observation_id, MarketTable.DAILY_BARS, "correction source"))
    with pytest.raises(SourceConflictError, match="Explicit precedence"):
        warehouse.build_snapshot(SnapshotPlan(
            tuple(selections),
            "ambiguous correction",
            readiness=ReadinessProfile.RESEARCH_PRICE,
        ))

    selections[-1] = SourceSlice(
        revised.observation_id, MarketTable.DAILY_BARS, "reviewed correction", priority=1,
    )
    snapshot = warehouse.build_snapshot(SnapshotPlan(
        tuple(selections),
        "explicit correction",
        readiness=ReadinessProfile.RESEARCH_PRICE,
        universe_scope=fixture_universe_scope(),
    ))
    assert snapshot.quality.ready
    warehouse.publish(snapshot.snapshot_id)
    portal = CanonicalMarketData.open(
        tmp_path / "market", required_readiness=ReadinessProfile.RESEARCH_PRICE,
    )
    bars = portal.bars(["600000.SH"], DAYS[0], DAYS[0], price_mode=PriceMode.RAW, as_of=DAYS[0])
    assert bars.iloc[0]["close"] == 11.0
    assert bars.iloc[0]["source_provider"] == "revision"


def test_incomplete_coverage_can_be_inspected_but_not_published(tmp_path):
    warehouse = MarketDataWarehouse(tmp_path / "market")
    observed = warehouse.record_observation(observation(complete=False))
    snapshot = warehouse.build_snapshot(plan(observed.observation_id))
    assert not snapshot.quality.ready
    assert any(item.startswith("coverage:") for item in snapshot.quality.errors)
    with pytest.raises(SnapshotNotReadyError):
        warehouse.publish(snapshot.snapshot_id)


def test_publish_if_current_is_atomic_across_processes(tmp_path):
    root = tmp_path / "market"
    warehouse = MarketDataWarehouse(root)
    observed = warehouse.record_observation(observation())
    base = plan(observed.observation_id)
    predecessor = warehouse.build_snapshot(base)
    contender_a = warehouse.build_snapshot(SnapshotPlan(
        base.selections,
        "concurrent successor A",
        readiness=base.readiness,
        universe_scope=base.universe_scope,
    ))
    contender_b = warehouse.build_snapshot(SnapshotPlan(
        base.selections,
        "concurrent successor B",
        readiness=base.readiness,
        universe_scope=base.universe_scope,
    ))
    warehouse.publish(predecessor.snapshot_id)

    context = multiprocessing.get_context("spawn")
    start = context.Event()
    results = context.Queue()
    processes = [
        context.Process(
            target=_publish_if_current_worker,
            args=(
                str(root), predecessor.snapshot_id, contender.snapshot_id,
                start, results,
            ),
        )
        for contender in (contender_a, contender_b)
    ]
    for process in processes:
        process.start()
    start.set()
    outcomes = [results.get(timeout=30) for _ in processes]
    for process in processes:
        process.join(timeout=30)
        assert process.exitcode == 0

    assert [item[0] for item in outcomes].count("published") == 1
    assert [item[0] for item in outcomes].count("rejected") == 1
    rejected = next(item[1] for item in outcomes if item[0] == "rejected")
    assert "Stale EOD predecessor" in rejected
    winner = next(item[1] for item in outcomes if item[0] == "published")
    assert warehouse.current_snapshot_id() == winner
    assert not tuple(root.glob(".current.*.tmp"))


def test_schema_v1_snapshot_identity_verifies_but_has_no_trusted_readiness(tmp_path):
    warehouse = MarketDataWarehouse(tmp_path / "market")
    observed = warehouse.record_observation(observation())
    current = warehouse.build_snapshot(plan(observed.observation_id))
    payload = json.loads(
        (warehouse.snapshot_path(current.snapshot_id) / "manifest.json").read_text(encoding="utf-8")
    )
    payload["plan"].pop("readiness")
    payload["plan"].pop("universe_scope")
    payload["schema_version"] = 1
    identity = {
        "plan": payload["plan"],
        "quality": payload["quality"],
        "files": payload["files"],
        "schema_version": 1,
    }
    legacy_id = f"snap-{stable_digest(identity)[:24]}"
    payload["snapshot_id"] = legacy_id
    legacy_path = warehouse.snapshot_path(legacy_id)
    legacy_path.mkdir()
    (legacy_path / "manifest.json").write_text(
        canonical_json(payload), encoding="utf-8", newline="\n",
    )

    legacy = warehouse.load_snapshot(legacy_id, require_ready=False)

    assert legacy.plan.readiness is ReadinessProfile.LEGACY_UNKNOWN
    with pytest.raises(SnapshotNotReadyError, match="no v2 readiness evidence"):
        CanonicalMarketData(warehouse, legacy_id)
    with pytest.raises(SnapshotNotReadyError, match="cannot be published"):
        warehouse.publish(legacy_id)


def test_provider_registry_never_falls_back():
    class Provider:
        name = "only"
        capabilities = frozenset({ProviderCapability.DAILY_BARS_RAW})

        def observe(self, request):
            raise AssertionError("not reached")

    registry = ProviderRegistry()
    registry.register(Provider())
    with pytest.raises(ProviderSelectionError, match="not registered"):
        registry.resolve("missing", ProviderCapability.DAILY_BARS_RAW)
    with pytest.raises(ProviderSelectionError, match="does not declare"):
        registry.resolve("only", ProviderCapability.CORPORATE_ACTIONS)


def test_exact_complete_collection_scope_is_resumable_without_another_source_call(tmp_path):
    class Provider:
        name = "resumable"
        capabilities = frozenset({ProviderCapability.DAILY_BARS_RAW})

        def __init__(self):
            self.calls = 0

        def observe(self, request):
            self.calls += 1
            return observation(provider=self.name)

    provider = Provider()
    registry = ProviderRegistry()
    registry.register(provider)
    service = MarketIngestionService(registry, MarketDataWarehouse(tmp_path / "market"))
    request = ProviderRequest(
        ProviderCapability.DAILY_BARS_RAW, DAYS[0], DAYS[-1], ("600000.SH",),
    )

    first, first_reused = service.capture_resumable(provider.name, request)
    second, second_reused = service.capture_resumable(provider.name, request)

    assert not first_reused and second_reused
    assert first.observation_id == second.observation_id
    assert provider.calls == 1
