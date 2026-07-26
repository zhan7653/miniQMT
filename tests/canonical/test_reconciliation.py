from __future__ import annotations

from datetime import datetime, timezone
import json

import pandas as pd
import pytest

from fundlab.marketdata import (
    CanonicalMarketData,
    CoverageClaim,
    MarketDataWarehouse,
    MarketTable,
    ObservationPayload,
    PriceMode,
    ProviderCapability,
    ProviderRequest,
    ReadinessProfile,
    ReconciliationService,
    SnapshotNotReadyError,
    SnapshotPlan,
    SourceSlice,
    default_reconciliation_policy,
)
from tests.canonical.fixtures import DAYS, fixture_universe_scope, market_frames, observation


def test_two_independent_backends_win_field_level_conflict_and_keep_lineage(tmp_path):
    warehouse = MarketDataWarehouse(tmp_path / "market")
    tickflow = warehouse.record_observation(observation(provider="tickflow"))
    eastmoney = warehouse.record_observation(observation(
        provider="eastmoney-efinance", observed_second=1,
    ))
    outlier = warehouse.record_observation(observation(
        provider="baostock", close_shift=5.0, observed_second=2,
    ))
    policy = default_reconciliation_policy(ReadinessProfile.RESEARCH_PRICE)
    service = ReconciliationService(warehouse, policy)

    reconciled, report = service.reconcile_and_record(
        (tickflow.observation_id, eastmoney.observation_id, outlier.observation_id),
        readiness=ReadinessProfile.RESEARCH_PRICE,
        description="three-source canary",
    )

    assert report.ready
    assert report.resolved_conflicts >= 4
    bars = warehouse.read_observation_table(reconciled.observation_id, MarketTable.DAILY_BARS)
    assert set(bars["price_mode"]) == {PriceMode.RAW.value}
    assert bars.iloc[0]["close"] == 10.0
    lineage = json.loads(bars.iloc[0]["field_lineage"])
    assert lineage["fields"]["close"]["selected"]["provider"] == "tickflow"
    assert set(lineage["fields"]["close"]["agreeing_backends"]) == {
        "eastmoney", "tickflow-unverified",
    }

    snapshot = warehouse.build_snapshot(SnapshotPlan(
        tuple(SourceSlice(
            reconciled.observation_id, item.table, "reconciled canary",
        ) for item in reconciled.files),
        "research-price canary",
        readiness=ReadinessProfile.RESEARCH_PRICE,
    ))
    assert snapshot.quality.ready


def test_volume_policy_accepts_one_lot_rounding_after_share_normalization():
    rule = default_reconciliation_policy(
        ReadinessProfile.RESEARCH_PRICE,
    ).rule(MarketTable.DAILY_BARS, "volume")

    assert rule.absolute_tolerance == 100.0
    assert rule.relative_tolerance == 1e-6


def test_price_policy_never_accepts_a_full_minimum_price_tick():
    policy = default_reconciliation_policy(ReadinessProfile.RESEARCH_PRICE)
    rule = policy.rule(MarketTable.DAILY_BARS, "close")

    assert policy.version == "a-share-daily-research_price-v4"
    assert rule.absolute_tolerance == 0.0005
    assert rule.relative_tolerance == 1e-8
    assert rule.absolute_tolerance + rule.relative_tolerance * 1_000 < 0.001


def test_two_clients_on_same_eastmoney_backend_do_not_count_as_independent(tmp_path):
    warehouse = MarketDataWarehouse(tmp_path / "market")
    efinance = warehouse.record_observation(observation(provider="eastmoney-efinance"))
    akshare = warehouse.record_observation(observation(
        provider="eastmoney-akshare", observed_second=1,
    ))
    service = ReconciliationService(
        warehouse,
        default_reconciliation_policy(ReadinessProfile.RESEARCH_PRICE),
    )

    result = service.reconcile(
        (efinance.observation_id, akshare.observation_id),
        readiness=ReadinessProfile.RESEARCH_PRICE,
    )

    assert not result.report.ready
    assert "insufficient_complete_backends:daily_bars:1/2" in result.report.blockers
    assert any(
        item.table is MarketTable.DAILY_BARS and item.field == "close"
        for item in result.report.unresolved_conflicts
    )


def test_incomplete_reconciliation_cannot_bypass_trust_gate_by_disabling_coverage_checks(tmp_path):
    warehouse = MarketDataWarehouse(tmp_path / "market")
    efinance = warehouse.record_observation(observation(provider="eastmoney-efinance"))
    akshare = warehouse.record_observation(observation(
        provider="eastmoney-akshare", observed_second=1,
    ))
    service = ReconciliationService(
        warehouse,
        default_reconciliation_policy(ReadinessProfile.RESEARCH_PRICE),
    )
    reconciled, report = service.reconcile_and_record(
        (efinance.observation_id, akshare.observation_id),
        readiness=ReadinessProfile.RESEARCH_PRICE,
        description="same-backend bypass regression",
    )
    assert not report.ready

    snapshot = warehouse.build_snapshot(SnapshotPlan(
        tuple(SourceSlice(
            reconciled.observation_id, item.table, "attempted bypass",
        ) for item in reconciled.files),
        "attempted bypass",
        require_complete_coverage=False,
        readiness=ReadinessProfile.RESEARCH_PRICE,
    ))

    assert not snapshot.quality.ready
    assert any(
        item.startswith("trust:reconciliation_not_ready:")
        for item in snapshot.quality.errors
    )
    with pytest.raises(SnapshotNotReadyError):
        warehouse.publish(snapshot.snapshot_id)


def test_tied_independent_consensus_clusters_are_blocked_instead_of_priority_forced(tmp_path):
    warehouse = MarketDataWarehouse(tmp_path / "market")
    manifests = (
        warehouse.record_observation(observation(provider="tickflow")),
        warehouse.record_observation(observation(provider="eastmoney-efinance", observed_second=1)),
        warehouse.record_observation(observation(provider="baostock", close_shift=5, observed_second=2)),
        warehouse.record_observation(observation(provider="xtquant-legacy-import", close_shift=5, observed_second=3)),
    )
    service = ReconciliationService(
        warehouse,
        default_reconciliation_policy(ReadinessProfile.RESEARCH_PRICE),
    )

    result = service.reconcile(
        tuple(item.observation_id for item in manifests),
        readiness=ReadinessProfile.RESEARCH_PRICE,
    )

    assert not result.report.ready
    assert any(
        item.table is MarketTable.DAILY_BARS and item.field == "close"
        for item in result.report.unresolved_conflicts
    )


def test_provider_factor_ids_do_not_duplicate_one_semantic_event(tmp_path):
    warehouse = MarketDataWarehouse(tmp_path / "market")
    manifests = []
    for second, (provider, factor_id) in enumerate((
        ("tickflow", "tick-factor"),
        ("baostock", "bao-factor"),
    )):
        frames = market_frames()
        frames[MarketTable.ADJUSTMENT_FACTORS] = pd.DataFrame([{
            "factor_id": factor_id,
            "instrument_id": "600000.SH",
            "effective_date": DAYS[2].isoformat(),
            "known_date": DAYS[2].isoformat(),
            "price_multiplier": 0.5,
            "source_payload": None,
        }])
        payload = ObservationPayload(
            provider,
            datetime(2026, 7, 17, 1, 0, second, tzinfo=timezone.utc),
            ProviderRequest(
                ProviderCapability.DAILY_BARS_RAW, DAYS[0], DAYS[-1], ("600000.SH",),
            ),
            frames,
            tuple(CoverageClaim(
                table,
                True,
                None if table is MarketTable.INSTRUMENTS else DAYS[0],
                None if table is MarketTable.INSTRUMENTS else DAYS[-1],
                ("600000.SH",) if "instrument_id" in frame.columns else (),
            ) for table, frame in frames.items()),
        )
        manifests.append(warehouse.record_observation(payload))
    service = ReconciliationService(
        warehouse,
        default_reconciliation_policy(ReadinessProfile.RESEARCH_PRICE),
    )

    reconciled, report = service.reconcile_and_record(
        tuple(item.observation_id for item in manifests),
        readiness=ReadinessProfile.RESEARCH_PRICE,
        description="semantic factor identity",
    )

    assert report.ready
    factors = warehouse.read_observation_table(
        reconciled.observation_id, MarketTable.ADJUSTMENT_FACTORS,
    )
    assert len(factors) == 1
    assert factors.iloc[0]["price_multiplier"] == 0.5


def test_adjusted_history_is_derived_only_from_factors_visible_as_of(tmp_path):
    frames = market_frames()
    frames[MarketTable.CORPORATE_ACTIONS] = pd.DataFrame([{
        "action_id": "action-1",
        "instrument_id": "600000.SH",
        "action_type": "cash_dividend",
        "known_date": DAYS[0].isoformat(),
        "record_date": DAYS[1].isoformat(),
        "ex_date": DAYS[2].isoformat(),
        "pay_date": DAYS[3].isoformat(),
        "listing_date": None,
        "cash_per_share": 0.5,
        "share_ratio": None,
        "rights_price": None,
        "quantity_multiplier": None,
        "source_payload": None,
    }])
    frames[MarketTable.ADJUSTMENT_FACTORS] = pd.DataFrame([{
        "factor_id": "factor-1",
        "instrument_id": "600000.SH",
        "effective_date": DAYS[2].isoformat(),
        "known_date": DAYS[2].isoformat(),
        "price_multiplier": 0.5,
        "source_payload": None,
    }])
    payload = ObservationPayload(
        "trusted-fixture",
        datetime(2026, 7, 17, tzinfo=timezone.utc),
        ProviderRequest(
            ProviderCapability.DAILY_BARS_RAW, DAYS[0], DAYS[-1], ("600000.SH",),
        ),
        frames,
        tuple(CoverageClaim(
            table,
            True,
            None if table is MarketTable.INSTRUMENTS else DAYS[0],
            None if table is MarketTable.INSTRUMENTS else DAYS[-1],
            ("600000.SH",) if "instrument_id" in frame.columns else (),
        ) for table, frame in frames.items()),
        {
            "kind": "field_level_reconciliation",
            "reconciliation_ready": True,
            "fixture": True,
        },
    )
    warehouse = MarketDataWarehouse(tmp_path / "market")
    observed = warehouse.record_observation(payload)
    snapshot = warehouse.build_snapshot(SnapshotPlan(
        tuple(SourceSlice(observed.observation_id, item.table, "trusted fixture") for item in observed.files),
        "point-in-time ratio fixture",
        universe_scope=fixture_universe_scope(),
    ))
    assert snapshot.quality.ready
    market = CanonicalMarketData(warehouse, snapshot.snapshot_id)

    before = market.adjusted_history(
        ("600000.SH",), DAYS[0], DAYS[1], as_of=DAYS[1],
    )
    after = market.adjusted_history(
        ("600000.SH",), DAYS[0], DAYS[1], as_of=DAYS[2],
    )

    assert before["close"].tolist() == [10.0, 11.0]
    assert after["close"].tolist() == [5.0, 5.5]
    assert after["volume"].tolist() == before["volume"].tolist()
    assert set(after["source_provider"]) == {"canonical-ratio-adjustment"}


def test_research_snapshot_can_publish_without_simulation_fields_but_cannot_simulate(tmp_path):
    frames = market_frames()
    research_tables = {
        MarketTable.INSTRUMENTS: frames[MarketTable.INSTRUMENTS],
        MarketTable.DAILY_BARS: frames[MarketTable.DAILY_BARS].query("price_mode == 'raw'"),
    }
    payload = ObservationPayload(
        "research-only",
        datetime(2026, 7, 17, tzinfo=timezone.utc),
        ProviderRequest(
            ProviderCapability.DAILY_BARS_RAW, DAYS[0], DAYS[-1], ("600000.SH",),
        ),
        research_tables,
        (
            CoverageClaim(MarketTable.INSTRUMENTS, True),
            CoverageClaim(
                MarketTable.DAILY_BARS, True, DAYS[0], DAYS[-1], ("600000.SH",),
            ),
        ),
        {
            "kind": "field_level_reconciliation",
            "reconciliation_ready": True,
            "fixture": True,
        },
    )
    warehouse = MarketDataWarehouse(tmp_path / "market")
    observed = warehouse.record_observation(payload)
    snapshot = warehouse.build_snapshot(SnapshotPlan(
        tuple(SourceSlice(observed.observation_id, item.table, "research-only fixture") for item in observed.files),
        "research-only fixture",
        readiness=ReadinessProfile.RESEARCH_PRICE,
    ))
    assert snapshot.quality.ready
    warehouse.publish(snapshot.snapshot_id)
    with pytest.raises(SnapshotNotReadyError, match="only research_price-ready"):
        CanonicalMarketData.open(tmp_path / "market")
    research = CanonicalMarketData.open(
        tmp_path / "market", required_readiness=ReadinessProfile.RESEARCH_PRICE,
    )
    assert len(research.bars(
        ("600000.SH",), DAYS[0], DAYS[0], price_mode=PriceMode.RAW, as_of=DAYS[0],
    )) == 1
    with pytest.raises(SnapshotNotReadyError, match="no adjustment-factor coverage"):
        research.adjusted_history(("600000.SH",), DAYS[0], DAYS[0], as_of=DAYS[0])
