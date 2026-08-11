from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from fundlab.common.atomic_files import publish_immutable_bytes
from fundlab.marketdata import (
    IntegrityError,
    CoverageClaim,
    CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
    MarketDataWarehouse,
    MarketTable,
    ObservationPayload,
    ProviderCapability,
    ProviderRequest,
    QualityReport,
    ReadinessProfile,
    SIMULATION_PARTITION_VALIDATOR_VERSION,
    SnapshotComponentSelection,
    SnapshotNotReadyError,
    SnapshotPlan,
    SnapshotState,
    SourceSlice,
    UniverseScope,
    SimulationIncrementValidator,
    SimulationSnapshotBuilder,
)
from fundlab.marketdata.components import ComponentKind, ComponentScope, ComponentStore
from fundlab.marketdata.incremental import (
    IncrementalCanonicalPublisher,
    _materialize_simulation_view,
)
from tests.canonical.fixtures import (
    DAYS,
    FUTURE_DAYS,
    commit_test_snapshot,
    fixture_universe_scope,
    market_frames,
    observation,
)


def _snapshot(warehouse: MarketDataWarehouse):
    observed = warehouse.record_observation(observation())
    return commit_test_snapshot(warehouse, SnapshotPlan(
        tuple(SourceSlice(
            observed.observation_id,
            table,
            "accepted canonical fixture",
        ) for table in MarketTable),
        "accepted canonical fixture",
        universe_scope=fixture_universe_scope(),
    ))


class _EodReportWarehouse:
    """Small verified-current double for report-publication fault injection."""

    predecessor_snapshot_id = "snap-" + "a" * 24
    candidate_snapshot_id = "snap-" + "b" * 24

    def __init__(self, *, cas_outcome: str) -> None:
        self.root = Path(".")
        self.current = self.predecessor_snapshot_id
        self.cas_outcome = cas_outcome

    def load_snapshot(self, snapshot_id: str):
        if snapshot_id not in {
            self.predecessor_snapshot_id, self.candidate_snapshot_id,
        }:
            raise ValueError(f"unverified test snapshot: {snapshot_id}")
        return SimpleNamespace(snapshot_id=snapshot_id)

    def current_snapshot_id(self) -> str:
        return self.current

    def publish_if_current(self, expected: str, successor: str) -> None:
        assert expected == self.predecessor_snapshot_id
        assert successor == self.candidate_snapshot_id
        if self.cas_outcome == "before_failure":
            raise RuntimeError("injected CAS failure before replace")
        self.current = successor
        if self.cas_outcome == "after_failure":
            raise RuntimeError("injected CAS failure after replace")


def _patch_eod_extension(monkeypatch):
    snapshot = SimpleNamespace(
        snapshot_id=_EodReportWarehouse.candidate_snapshot_id,
        quality=QualityReport(SnapshotState.READY),
    )

    def extend(_self, **_kwargs):
        return snapshot, {"audit": "verified"}

    monkeypatch.setattr(IncrementalCanonicalPublisher, "extend", extend)


def _run_eod_report_extension(
    builder: SimulationSnapshotBuilder,
    *,
    publish: bool = True,
):
    return builder.extend(
        predecessor_snapshot_id=_EodReportWarehouse.predecessor_snapshot_id,
        calendar_observation_id="obs-" + "c" * 24,
        increment_observation_ids=("obs-" + "d" * 24,),
        universe_scope=fixture_universe_scope(),
        description="fault-injected EOD report fixture",
        publish=publish,
    )


def test_eod_report_is_hidden_when_cas_fails_before_current(tmp_path, monkeypatch):
    _patch_eod_extension(monkeypatch)
    reports = tmp_path / "reports"
    warehouse = _EodReportWarehouse(cas_outcome="before_failure")

    with pytest.raises(RuntimeError, match="before replace"):
        _run_eod_report_extension(SimulationSnapshotBuilder(warehouse, reports))

    assert not tuple(reports.glob("simulation-eod-*.json"))
    assert len(tuple((reports / ".pending").glob("*.pending.json"))) == 1

    # A valid pending report whose candidate is not current is intentionally
    # retained; it must not block a later normal attempt or publish itself.
    warehouse.cas_outcome = "success"
    result = _run_eod_report_extension(SimulationSnapshotBuilder(warehouse, reports))
    assert result.published and result.report.is_file()


def test_eod_report_recovers_after_finalize_failure_idempotently(tmp_path, monkeypatch):
    _patch_eod_extension(monkeypatch)
    reports = tmp_path / "reports"
    warehouse = _EodReportWarehouse(cas_outcome="success")
    builder = SimulationSnapshotBuilder(warehouse, reports)
    from fundlab.marketdata import simulation_data

    original_write = simulation_data._write_immutable_json
    failed = False

    def fail_formal_once(path, payload):
        nonlocal failed
        if path.parent == reports and not failed:
            failed = True
            raise OSError("injected formal-report failure")
        original_write(path, payload)

    monkeypatch.setattr(simulation_data, "_write_immutable_json", fail_formal_once)
    with pytest.raises(OSError, match="formal-report"):
        _run_eod_report_extension(builder)
    assert not tuple(reports.glob("simulation-eod-*.json"))
    assert len(tuple((reports / ".pending").glob("*.pending.json"))) == 1

    result = _run_eod_report_extension(builder)
    assert result.published
    assert result.report.is_file()
    assert not tuple((reports / ".pending").glob("*.pending.json"))

    repeated = _run_eod_report_extension(builder)
    assert repeated.published and repeated.report == result.report
    assert len(tuple(reports.glob("simulation-eod-*.json"))) == 1


def test_eod_report_finalizes_when_cas_raises_after_current(tmp_path, monkeypatch):
    _patch_eod_extension(monkeypatch)
    reports = tmp_path / "reports"
    warehouse = _EodReportWarehouse(cas_outcome="after_failure")

    result = _run_eod_report_extension(SimulationSnapshotBuilder(warehouse, reports))

    assert result.published
    assert result.report.is_file()
    assert not tuple((reports / ".pending").glob("*.pending.json"))


def test_unpublished_eod_candidate_keeps_its_visible_report_without_pending(
    tmp_path, monkeypatch,
):
    _patch_eod_extension(monkeypatch)
    reports = tmp_path / "reports"
    warehouse = _EodReportWarehouse(cas_outcome="before_failure")

    result = _run_eod_report_extension(
        SimulationSnapshotBuilder(warehouse, reports), publish=False,
    )

    assert not result.published
    assert result.report.is_file()
    assert warehouse.current == warehouse.predecessor_snapshot_id
    assert not tuple((reports / ".pending").glob("*.pending.json"))
    assert result.report.read_text(encoding="utf-8").find('"published":false') >= 0


def test_immutable_publish_rejects_concurrent_winner_with_different_bytes(tmp_path):
    path = tmp_path / "report.json"
    def publish(payload: bytes) -> bytes | str:
        try:
            publish_immutable_bytes(path, payload)
        except ValueError:
            return "collision"
        return payload

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(publish, (b"winner", b"loser")))

    assert results.count("collision") == 1
    winning = next(item for item in results if item != "collision")
    assert path.read_bytes() == winning


def test_zero_copy_bootstrap_preserves_public_rows_and_separates_components(tmp_path):
    warehouse = MarketDataWarehouse(tmp_path / "market")
    source = _snapshot(warehouse)

    candidate = IncrementalCanonicalPublisher(warehouse).bootstrap(source.snapshot_id)
    parity = IncrementalCanonicalPublisher(warehouse).compare(
        source.snapshot_id, candidate.snapshot_id,
    )

    assert parity.equivalent
    assert candidate.snapshot_id != source.snapshot_id
    store = ComponentStore(warehouse.root / "components", warehouse)
    kinds = {
        store.load(item.component_id, verify_payload=False).kind
        for item in candidate.component_selections
    }
    assert kinds == set(ComponentKind)
    daily = [
        store.load(item.component_id, verify_payload=False)
        for item in candidate.component_selections
        if store.load(item.component_id, verify_payload=False).scope.table
        is MarketTable.DAILY_BARS
    ]
    assert any(item.kind is ComponentKind.MARKET_FACTS for item in daily)
    assert any(item.kind is ComponentKind.TRADING_RULEBOOK for item in daily)
    assert any(item.kind is ComponentKind.FIELD_ADJUDICATIONS for item in daily)
    assert any(
        item.kind is ComponentKind.SIMULATION_VIEW and not item.data_file
        for item in daily
    )


def test_increment_rejects_implicit_legacy_bootstrap(tmp_path):
    warehouse = MarketDataWarehouse(tmp_path / "market")
    source = _snapshot(warehouse)

    with pytest.raises(SnapshotNotReadyError, match="componentized predecessor"):
        IncrementalCanonicalPublisher(warehouse).extend(
            predecessor_snapshot_id=source.snapshot_id,
            calendar_observation_id="obs-" + "1" * 24,
            increment_observation_ids=("obs-" + "2" * 24,),
            universe_scope=fixture_universe_scope(),
            description="must not hide a migration bootstrap",
        )


def test_partitioned_snapshot_rejects_retired_full_simulation_path(tmp_path):
    warehouse = MarketDataWarehouse(tmp_path / "market")
    source = _snapshot(warehouse)

    with pytest.raises(ValueError, match="componentized incremental publisher"):
        warehouse.build_partitioned_snapshot(SnapshotPlan(
            source.plan.selections,
            "retired full simulation path",
            readiness=ReadinessProfile.SIMULATION,
            universe_scope=source.plan.universe_scope,
        ))


def test_all_non_componentized_simulation_publication_paths_fail_closed(tmp_path):
    warehouse = MarketDataWarehouse(tmp_path / "market")
    observed = warehouse.record_observation(observation())
    plan = SnapshotPlan(
        tuple(SourceSlice(
            observed.observation_id, table, "retired simulation fixture",
        ) for table in MarketTable),
        "retired simulation fixture",
        readiness=ReadinessProfile.SIMULATION,
        universe_scope=fixture_universe_scope(),
    )

    with pytest.raises(SnapshotNotReadyError, match="construction is retired"):
        warehouse.build_snapshot(plan)

    legacy = commit_test_snapshot(warehouse, plan)
    with pytest.raises(SnapshotNotReadyError, match="publication is retired"):
        warehouse.publish(legacy.snapshot_id)

    componentized = IncrementalCanonicalPublisher(warehouse).bootstrap(
        legacy.snapshot_id,
    )
    with pytest.raises(SnapshotNotReadyError, match="compare-and-swap"):
        warehouse.publish(componentized.snapshot_id)


def test_eod_validator_produces_partition_consumed_by_atomic_increment(tmp_path):
    warehouse = MarketDataWarehouse(tmp_path / "market")
    source = warehouse.record_observation(observation())
    previous_scope = UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        DAYS[-2], DAYS[0], DAYS[-2],
        survivorship_bias=True,
        instrument_ids=("600000.SH",),
    )
    predecessor = IncrementalCanonicalPublisher(warehouse).bootstrap(
        commit_test_snapshot(warehouse, SnapshotPlan(
            tuple(SourceSlice(
                source.observation_id,
                table,
                "accepted predecessor fixture",
                start_date=None if table is MarketTable.INSTRUMENTS else DAYS[0],
                end_date=None if table is MarketTable.INSTRUMENTS else DAYS[-2],
            ) for table in MarketTable),
            "accepted predecessor fixture",
            readiness=ReadinessProfile.SIMULATION,
            universe_scope=previous_scope,
        )).snapshot_id,
    )
    warehouse._replace_current_pointer(predecessor)

    frames = market_frames()
    last_raw = frames[MarketTable.DAILY_BARS].loc[
        frames[MarketTable.DAILY_BARS]["session_date"].eq(DAYS[-1].isoformat())
        & frames[MarketTable.DAILY_BARS]["price_mode"].eq("raw")
    ].reset_index(drop=True)
    upstream_ids = []
    for second, provider in enumerate(("baostock", "xtquant"), start=1):
        upstream = warehouse.record_observation(ObservationPayload(
            provider,
            datetime(2026, 7, 18, 1, 0, second, tzinfo=timezone.utc),
            ProviderRequest(
                ProviderCapability.DAILY_BARS_RAW,
                DAYS[-1], DAYS[-1], ("600000.SH",),
            ),
            {MarketTable.DAILY_BARS: last_raw},
            (CoverageClaim(
                MarketTable.DAILY_BARS,
                True,
                DAYS[-1], DAYS[-1], ("600000.SH",),
            ),),
        ))
        upstream_ids.append(upstream.observation_id)

    candidate = warehouse.record_observation(ObservationPayload(
        "canonical-reconciler-a-share-daily-simulation-v4",
        datetime(2026, 7, 18, 1, 1, tzinfo=timezone.utc),
        ProviderRequest(
            ProviderCapability.CANONICAL_RECONCILIATION,
            DAYS[-1], DAYS[-1], ("600000.SH",),
            {"input_observation_ids": tuple(upstream_ids)},
        ),
        {
            MarketTable.INSTRUMENTS: frames[MarketTable.INSTRUMENTS],
            MarketTable.DAILY_BARS: last_raw,
            MarketTable.CORPORATE_ACTIONS: frames[MarketTable.CORPORATE_ACTIONS],
            MarketTable.ADJUSTMENT_FACTORS: frames[MarketTable.ADJUSTMENT_FACTORS],
        },
        tuple(CoverageClaim(
            table,
            True,
            None if table is MarketTable.INSTRUMENTS else DAYS[-1],
            None if table is MarketTable.INSTRUMENTS else DAYS[-1],
            ("600000.SH",),
        ) for table in (
            MarketTable.INSTRUMENTS,
            MarketTable.DAILY_BARS,
            MarketTable.CORPORATE_ACTIONS,
            MarketTable.ADJUSTMENT_FACTORS,
        )),
        {
            "kind": "field_level_reconciliation",
            "reconciliation_ready": True,
            "report": {
                "blockers": (),
                "unresolved_conflicts": (),
                "input_observation_ids": tuple(upstream_ids),
            },
        },
    ))
    increment_scope = UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        DAYS[-1], DAYS[-1], DAYS[-1],
        survivorship_bias=True,
        instrument_ids=("600000.SH",),
    )
    validated = SimulationIncrementValidator(
        warehouse, tmp_path / "reports",
    ).validate_and_record(
        candidate_observation_id=candidate.observation_id,
        calendar_observation_id=source.observation_id,
        universe_scope=increment_scope,
        description="validated one-day EOD fixture",
    )
    validated_manifest = warehouse.load_observation(validated.observation_id)
    partition_quality = validated_manifest.source_metadata["partition_quality"]
    assert partition_quality["validated"] is True
    assert (
        partition_quality["validator_version"]
        == SIMULATION_PARTITION_VALIDATOR_VERSION
    )
    assert partition_quality["price_limit_audit"][
        "minimum_direct_limit_observations_latest_session"
    ] == 2

    target_scope = UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        DAYS[-1], DAYS[0], DAYS[-1],
        survivorship_bias=True,
        instrument_ids=("600000.SH",),
    )
    result = SimulationSnapshotBuilder(
        warehouse, tmp_path / "reports",
    ).extend(
        predecessor_snapshot_id=predecessor.snapshot_id,
        calendar_observation_id=source.observation_id,
        increment_observation_ids=(validated.observation_id,),
        universe_scope=target_scope,
        description="atomic validated EOD fixture",
        publish=True,
    )

    assert result.ready and result.published
    assert warehouse.current_snapshot_id() == result.snapshot_id
    row = warehouse.query_snapshot_table(
        result.snapshot_id,
        MarketTable.DAILY_BARS,
        instrument_ids=("600000.SH",),
        start_date=DAYS[-1],
        end_date=DAYS[-1],
        price_mode="raw",
    ).iloc[0]
    assert row["close"] == 13.0


def test_real_increment_audit_measures_every_prior_daily_open(tmp_path, monkeypatch):
    warehouse = MarketDataWarehouse(tmp_path / "market")
    source = warehouse.record_observation(observation())
    previous_scope = UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        DAYS[-2], DAYS[0], DAYS[-2],
        survivorship_bias=True,
        instrument_ids=("600000.SH",),
    )
    predecessor = commit_test_snapshot(warehouse, SnapshotPlan(
        tuple(SourceSlice(
            source.observation_id,
            table,
            "predecessor fixture",
            start_date=None if table is MarketTable.INSTRUMENTS else DAYS[0],
            end_date=None if table is MarketTable.INSTRUMENTS else DAYS[-2],
        ) for table in MarketTable),
        "predecessor fixture",
        readiness=ReadinessProfile.SIMULATION,
        universe_scope=previous_scope,
    ))
    componentized = IncrementalCanonicalPublisher(warehouse).bootstrap(
        predecessor.snapshot_id,
    )
    frames = market_frames()
    increment_tables = {
        MarketTable.INSTRUMENTS: frames[MarketTable.INSTRUMENTS],
        MarketTable.DAILY_BARS: frames[MarketTable.DAILY_BARS].loc[
            frames[MarketTable.DAILY_BARS]["session_date"].eq(DAYS[-1].isoformat())
        ],
        MarketTable.CORPORATE_ACTIONS: frames[MarketTable.CORPORATE_ACTIONS],
        MarketTable.ADJUSTMENT_FACTORS: frames[MarketTable.ADJUSTMENT_FACTORS],
    }
    calendar_observation = source.observation_id
    increment = warehouse.record_observation(ObservationPayload(
        "fixture-increment",
        datetime(2026, 7, 18, tzinfo=timezone.utc),
        ProviderRequest(
            ProviderCapability.DAILY_BARS_RAW,
            DAYS[-1], DAYS[-1], ("600000.SH",),
        ),
        increment_tables,
        tuple(CoverageClaim(
            table,
            True,
            None if table is MarketTable.INSTRUMENTS else DAYS[-1],
            None if table is MarketTable.INSTRUMENTS else DAYS[-1],
            ("600000.SH",) if table is not MarketTable.ADJUSTMENT_FACTORS else (),
        ) for table in increment_tables),
        {
            "kind": "field_level_reconciliation",
            "reconciliation_ready": True,
            "partition_quality": {
                "validated": True,
                "validator_version": SIMULATION_PARTITION_VALIDATOR_VERSION,
                "readiness": ReadinessProfile.SIMULATION.value,
                "calendar_observation_id": calendar_observation,
                "start_date": DAYS[-1].isoformat(),
                "end_date": DAYS[-1].isoformat(),
                "universe_as_of": DAYS[-1].isoformat(),
                "instrument_ids": ["600000.SH"],
            },
        },
    ))
    target_scope = UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        DAYS[-1], DAYS[0], DAYS[-1],
        survivorship_bias=True,
        instrument_ids=("600000.SH",),
    )

    publisher = IncrementalCanonicalPublisher(warehouse)
    candidate, audit = publisher.extend(
        predecessor_snapshot_id=componentized.snapshot_id,
        calendar_observation_id=calendar_observation,
        increment_observation_ids=(increment.observation_id,),
        universe_scope=target_scope,
        description="measured one-day increment",
    )

    assert audit.history_daily_reads == 0
    assert set(audit.reused_component_ids) == {
        item.component_id for item in componentized.component_selections
    }
    store = ComponentStore(warehouse.root / "components", warehouse)
    prior_daily_id = next(
        item.component_id
        for item in componentized.component_selections
        if (
            (manifest := store.load(item.component_id, verify_payload=False)).kind
            is ComponentKind.MARKET_FACTS
            and manifest.scope.table is MarketTable.DAILY_BARS
        )
    )
    # Open it before the measured build, then reopen the same ID during the build.
    # A cumulative set difference would miss the second I/O event.
    store.read(prior_daily_id, start_date=DAYS[0], end_date=DAYS[0])
    original = publisher._instrument_update_components

    def reopen_prior_daily(*args, **kwargs):
        publisher.components.read(
            prior_daily_id, start_date=DAYS[0], end_date=DAYS[0],
        )
        return original(*args, **kwargs)

    monkeypatch.setattr(publisher, "_instrument_update_components", reopen_prior_daily)
    _, repeated_audit = publisher.extend(
        predecessor_snapshot_id=componentized.snapshot_id,
        calendar_observation_id=calendar_observation,
        increment_observation_ids=(increment.observation_id,),
        universe_scope=target_scope,
        description="measured repeated one-day increment",
    )
    assert repeated_audit.opened_prior_daily_components == (prior_daily_id,)
    assert repeated_audit.history_daily_reads == 1
    rows = warehouse.query_snapshot_table(
        candidate.snapshot_id,
        MarketTable.DAILY_BARS,
        instrument_ids=("600000.SH",),
        start_date=DAYS[-1],
        end_date=DAYS[-1],
        price_mode="raw",
    )
    assert rows.iloc[0]["close"] == 13.0


def test_successive_increments_overlay_overlapping_future_calendar(tmp_path):
    """Two consecutive daily increments overlap in the future calendar region.

    The overlay must keep component keys unique, let the newest exchange
    announcement win for a revised future session, and keep the quality row
    count at the number of composed rows rather than double-counting overlap.
    """

    warehouse = MarketDataWarehouse(tmp_path / "market")
    source = warehouse.record_observation(observation())
    previous_scope = UniverseScope(
        CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
        DAYS[1], DAYS[0], DAYS[1],
        survivorship_bias=True,
        instrument_ids=("600000.SH",),
    )
    predecessor = IncrementalCanonicalPublisher(warehouse).bootstrap(
        commit_test_snapshot(warehouse, SnapshotPlan(
            tuple(SourceSlice(
                source.observation_id,
                table,
                "predecessor fixture",
                start_date=None if table is MarketTable.INSTRUMENTS else DAYS[0],
                end_date=None if table is MarketTable.INSTRUMENTS else DAYS[1],
            ) for table in MarketTable),
            "predecessor fixture",
            readiness=ReadinessProfile.SIMULATION,
            universe_scope=previous_scope,
        )).snapshot_id,
    )

    def increment_for(day, calendar_observation_id, second):
        frames = market_frames()
        tables = {
            MarketTable.INSTRUMENTS: frames[MarketTable.INSTRUMENTS],
            MarketTable.DAILY_BARS: frames[MarketTable.DAILY_BARS].loc[
                frames[MarketTable.DAILY_BARS]["session_date"].eq(day.isoformat())
            ],
            MarketTable.CORPORATE_ACTIONS: frames[MarketTable.CORPORATE_ACTIONS],
            MarketTable.ADJUSTMENT_FACTORS: frames[MarketTable.ADJUSTMENT_FACTORS],
        }
        return warehouse.record_observation(ObservationPayload(
            "fixture-increment",
            datetime(2026, 7, 18, 0, 0, second, tzinfo=timezone.utc),
            ProviderRequest(
                ProviderCapability.DAILY_BARS_RAW, day, day, ("600000.SH",),
            ),
            tables,
            tuple(CoverageClaim(
                table,
                True,
                None if table is MarketTable.INSTRUMENTS else day,
                None if table is MarketTable.INSTRUMENTS else day,
                ("600000.SH",) if table is not MarketTable.ADJUSTMENT_FACTORS else (),
            ) for table in tables),
            {
                "kind": "field_level_reconciliation",
                "reconciliation_ready": True,
                "partition_quality": {
                    "validated": True,
                    "validator_version": SIMULATION_PARTITION_VALIDATOR_VERSION,
                    "readiness": ReadinessProfile.SIMULATION.value,
                    "calendar_observation_id": calendar_observation_id,
                    "start_date": day.isoformat(),
                    "end_date": day.isoformat(),
                    "universe_as_of": day.isoformat(),
                    "instrument_ids": ["600000.SH"],
                },
            },
        ))

    publisher = IncrementalCanonicalPublisher(warehouse)
    first, _ = publisher.extend(
        predecessor_snapshot_id=predecessor.snapshot_id,
        calendar_observation_id=source.observation_id,
        increment_observation_ids=(
            increment_for(DAYS[2], source.observation_id, 1).observation_id,
        ),
        universe_scope=UniverseScope(
            CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
            DAYS[2], DAYS[0], DAYS[2],
            survivorship_bias=True,
            instrument_ids=("600000.SH",),
        ),
        description="first daily increment with future calendar",
    )

    # The next day the exchange revises its announcement: the furthest future
    # session is no longer open.  The newer overlapping window must win.
    revised_frames = market_frames()
    revised_calendar = revised_frames[MarketTable.CALENDAR]
    revised_calendar.loc[
        revised_calendar["session_date"].eq(FUTURE_DAYS[-1].isoformat()), "is_open",
    ] = False
    revised_source = warehouse.record_observation(ObservationPayload(
        "fixture-revised-calendar",
        datetime(2026, 7, 18, 2, 0, tzinfo=timezone.utc),
        ProviderRequest(
            ProviderCapability.TRADING_CALENDAR, DAYS[0], FUTURE_DAYS[-1],
        ),
        {MarketTable.CALENDAR: revised_calendar},
        (CoverageClaim(MarketTable.CALENDAR, True, DAYS[0], FUTURE_DAYS[-1]),),
        {"kind": "field_level_reconciliation", "reconciliation_ready": True},
    ))
    second, _ = publisher.extend(
        predecessor_snapshot_id=first.snapshot_id,
        calendar_observation_id=revised_source.observation_id,
        increment_observation_ids=(
            increment_for(DAYS[3], revised_source.observation_id, 2).observation_id,
        ),
        universe_scope=UniverseScope(
            CURRENT_SH_SZ_STOCK_ETF_UNIVERSE,
            DAYS[3], DAYS[0], DAYS[3],
            survivorship_bias=True,
            instrument_ids=("600000.SH",),
        ),
        description="second daily increment with revised future calendar",
    )

    calendar = warehouse.query_snapshot_table(second.snapshot_id, MarketTable.CALENDAR)
    keys = list(map(tuple, calendar[["exchange", "session_date"]].to_numpy()))
    assert len(keys) == len(set(keys)), "overlapping windows must compose to unique keys"
    assert set(calendar["session_date"]) == {
        day.isoformat() for day in (*DAYS, *FUTURE_DAYS)
    }
    revised_row = calendar.loc[
        calendar["session_date"].eq(FUTURE_DAYS[-1].isoformat())
    ]
    assert not revised_row["is_open"].any(), "the newest announcement must win"
    untouched = calendar.loc[
        calendar["session_date"] <= DAYS[-1].isoformat(), "is_open",
    ]
    assert untouched.all(), "published history must never be rewritten"
    assert int(second.quality.row_counts["calendar"]) == len(DAYS) + len(FUTURE_DAYS)


def test_scoped_rule_correction_reuses_facts_and_only_changes_declared_view(tmp_path):
    warehouse = MarketDataWarehouse(tmp_path / "market")
    componentized = IncrementalCanonicalPublisher(warehouse).bootstrap(
        _snapshot(warehouse).snapshot_id,
    )
    publisher = IncrementalCanonicalPublisher(warehouse)
    store = ComponentStore(warehouse.root / "components", warehouse)
    fact_ids = {
        item.component_id
        for item in componentized.component_selections
        if store.load(item.component_id, verify_payload=False).kind
        is ComponentKind.MARKET_FACTS
    }
    fields = (
        "instrument_id", "session_date", "price_mode", "price_limit_ratio",
    )
    scope = ComponentScope(
        MarketTable.DAILY_BARS,
        ("600000.SH",),
        DAYS[0],
        DAYS[0],
        fields,
    )
    correction = store.record_materialized(
        ComponentKind.TRADING_RULEBOOK,
        scope,
        pd.DataFrame([{
            "instrument_id": "600000.SH",
            "session_date": DAYS[0].isoformat(),
            "price_mode": "raw",
            "price_limit_ratio": 0.20,
        }]),
        builder_version="reviewed-rule-correction-r1",
    )

    corrected = publisher.apply_scoped_update(
        predecessor_snapshot_id=componentized.snapshot_id,
        component_ids=(correction.component_id,),
        declared_impacts=(scope,),
        description="one reviewed rule correction",
    )

    corrected_fact_ids = {
        item.component_id
        for item in corrected.component_selections
        if store.load(item.component_id, verify_payload=False).kind
        is ComponentKind.MARKET_FACTS
    }
    assert corrected_fact_ids == fact_ids
    row = warehouse.query_snapshot_table(
        corrected.snapshot_id,
        MarketTable.DAILY_BARS,
        instrument_ids=("600000.SH",),
        start_date=DAYS[0],
        end_date=DAYS[0],
        price_mode="raw",
    ).iloc[0]
    assert row["close"] == 10.0
    assert row["price_limit_ratio"] == 0.20
    assert row["limit_up"] == 11.4
    assert row["limit_down"] == 7.6


def test_unknown_correction_scope_fails_closed_instead_of_rebuilding(tmp_path):
    warehouse = MarketDataWarehouse(tmp_path / "market")
    componentized = IncrementalCanonicalPublisher(warehouse).bootstrap(
        _snapshot(warehouse).snapshot_id,
    )
    store = ComponentStore(warehouse.root / "components", warehouse)
    broad = ComponentScope(
        MarketTable.DAILY_BARS,
        fields=("instrument_id", "session_date", "price_mode", "volume"),
    )
    component = store.record_materialized(
        ComponentKind.MARKET_FACTS,
        broad,
        pd.DataFrame([{
            "instrument_id": "600000.SH",
            "session_date": DAYS[0].isoformat(),
            "price_mode": "raw",
            "volume": 1.0,
        }]),
        builder_version="ambiguous-correction-r1",
    )

    with pytest.raises(SnapshotNotReadyError, match="Unknown instrument impact scope"):
        IncrementalCanonicalPublisher(warehouse).apply_scoped_update(
            predecessor_snapshot_id=componentized.snapshot_id,
            component_ids=(component.component_id,),
            declared_impacts=(broad,),
            description="must not publish",
        )


def test_disposable_view_rebuilds_asymmetric_legacy_ipo_limits():
    rebuilt = _materialize_simulation_view(pd.DataFrame([{
        "instrument_id": "600001.SH",
        "session_date": date(2018, 1, 2).isoformat(),
        "price_mode": "raw",
        "suspended": False,
        "previous_close": 10.0,
        "price_tick": 0.01,
        "price_limit_state": "bounded",
        "price_limit_ratio": None,
        "trade_rule_id": "cn-stock-legacy-ipo-first-session-44up-36down-v1",
    }]))

    assert rebuilt.iloc[0]["limit_up"] == 14.4
    assert rebuilt.iloc[0]["limit_down"] == 6.4


def test_snapshot_rejects_an_unselected_view_dependency(tmp_path):
    warehouse = MarketDataWarehouse(tmp_path / "market")
    componentized = IncrementalCanonicalPublisher(warehouse).bootstrap(
        _snapshot(warehouse).snapshot_id,
    )
    store = ComponentStore(warehouse.root / "components", warehouse)
    view = next(
        store.load(item.component_id, verify_payload=False)
        for item in componentized.component_selections
        if store.load(item.component_id, verify_payload=False).kind
        is ComponentKind.SIMULATION_VIEW
    )
    omitted = view.dependency_ids[0]
    refs = tuple(
        item for item in componentized.component_selections
        if item.component_id != omitted
    )

    with pytest.raises(IntegrityError, match="omits direct component dependencies"):
        warehouse.build_component_snapshot(
            plan=componentized.plan,
            quality=componentized.quality,
            component_selections=refs,
        )


def test_query_fails_closed_for_an_unsupported_pinned_view_builder(tmp_path):
    warehouse = MarketDataWarehouse(tmp_path / "market")
    componentized = IncrementalCanonicalPublisher(warehouse).bootstrap(
        _snapshot(warehouse).snapshot_id,
    )
    store = ComponentStore(warehouse.root / "components", warehouse)
    base_view = next(
        store.load(item.component_id, verify_payload=False)
        for item in componentized.component_selections
        if store.load(item.component_id, verify_payload=False).kind
        is ComponentKind.SIMULATION_VIEW
    )
    unknown = store.record_view(
        base_view.scope,
        base_view.dependency_ids,
        builder_version="simulation-view-unknown",
    )
    ordinal = max(item.ordinal for item in componentized.component_selections) + 1
    candidate = warehouse.build_component_snapshot(
        plan=componentized.plan,
        quality=componentized.quality,
        component_selections=(*componentized.component_selections,
                              SnapshotComponentSelection(unknown.component_id, 100, ordinal)),
    )

    with pytest.raises(IntegrityError, match="Unsupported pinned simulation view builder"):
        warehouse.query_snapshot_table(candidate.snapshot_id, MarketTable.DAILY_BARS)


def test_warehouse_query_detects_materialized_component_tampering(tmp_path):
    warehouse = MarketDataWarehouse(tmp_path / "market")
    componentized = IncrementalCanonicalPublisher(warehouse).bootstrap(
        _snapshot(warehouse).snapshot_id,
    )
    store = ComponentStore(warehouse.root / "components", warehouse)
    scope = ComponentScope(
        MarketTable.DAILY_BARS,
        ("600000.SH",), DAYS[0], DAYS[0],
        ("instrument_id", "session_date", "price_mode", "price_limit_ratio"),
    )
    correction = store.record_materialized(
        ComponentKind.TRADING_RULEBOOK,
        scope,
        pd.DataFrame([{
            "instrument_id": "600000.SH",
            "session_date": DAYS[0].isoformat(),
            "price_mode": "raw",
            "price_limit_ratio": 0.20,
        }]),
        builder_version="tamper-test-r1",
    )
    corrected = IncrementalCanonicalPublisher(warehouse).apply_scoped_update(
        predecessor_snapshot_id=componentized.snapshot_id,
        component_ids=(correction.component_id,),
        declared_impacts=(scope,),
        description="tamper test",
    )
    (store.component_path(correction.component_id) / "data.parquet").write_bytes(b"tampered")

    reopened = MarketDataWarehouse(warehouse.root)
    with pytest.raises(IntegrityError, match="file mismatch"):
        reopened.query_snapshot_table(corrected.snapshot_id, MarketTable.DAILY_BARS)
