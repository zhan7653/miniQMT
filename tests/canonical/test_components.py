from __future__ import annotations

from datetime import date
import json

import pandas as pd
import pytest

from fundlab.marketdata.components import (
    ComponentKind,
    ComponentScope,
    ComponentStore,
    SourceProjection,
)
from fundlab.marketdata.contracts import IntegrityError, MarketTable
from fundlab.marketdata.warehouse import MarketDataWarehouse
from tests.canonical.fixtures import DAYS, observation


def _scope(*, start=DAYS[0], end=DAYS[1]):
    return ComponentScope(
        MarketTable.DAILY_BARS,
        ("600000.SH",),
        start,
        end,
        ("instrument_id", "session_date", "price_mode", "close"),
    )


def _frame(*, reverse=False):
    rows = [
        {"instrument_id": "600000.SH", "session_date": DAYS[0], "price_mode": "raw", "close": 10.0},
        {"instrument_id": "600000.SH", "session_date": DAYS[1], "price_mode": "raw", "close": 11.0},
    ]
    return pd.DataFrame(list(reversed(rows)) if reverse else rows, index=[9, 3])


def test_materialized_component_identity_ignores_index_and_input_row_order(tmp_path):
    store = ComponentStore(tmp_path / "components", MarketDataWarehouse(tmp_path / "market"))
    first = store.record_materialized(
        ComponentKind.MARKET_FACTS, _scope(), _frame(), builder_version="test-r1",
    )
    second = store.record_materialized(
        ComponentKind.MARKET_FACTS, _scope(), _frame(reverse=True), builder_version="test-r1",
    )

    assert first.component_id == second.component_id
    assert first.content_sha256 == second.content_sha256


def test_source_projection_identity_is_locator_independent():
    scope = _scope()
    left = SourceProjection("obs-first", MarketTable.DAILY_BARS, scope.fields, scope, "a" * 64)
    right = SourceProjection("obs-second", MarketTable.DAILY_BARS, scope.fields, scope, "a" * 64)

    assert left.content_identity() == right.content_identity()


def test_component_scope_overlap_and_containment():
    whole = _scope(start=DAYS[0], end=DAYS[2])
    intersecting = _scope(start=DAYS[1], end=DAYS[3])
    disjoint = _scope(start=date(2026, 7, 20), end=date(2026, 7, 21))

    assert whole.overlaps(intersecting)
    assert not whole.overlaps(disjoint)
    assert whole.contains(_scope(start=DAYS[1], end=DAYS[1]))
    assert not _scope(start=DAYS[1], end=DAYS[1]).contains(whole)


def test_virtual_view_identity_depends_only_on_direct_dependencies_scope_and_builder(tmp_path):
    store = ComponentStore(tmp_path / "components", MarketDataWarehouse(tmp_path / "market"))
    dependencies = ("cmp-" + "1" * 24, "cmp-" + "2" * 24)

    first = store.record_view(_scope(), dependencies, builder_version="view-r1")
    second = store.record_view(_scope(), reversed(dependencies), builder_version="view-r1")
    changed = store.record_view(_scope(), dependencies, builder_version="view-r2")

    assert first.component_id == second.component_id
    assert first.component_id != changed.component_id
    assert not (store.component_path(first.component_id) / "data.parquet").exists()


def test_materialized_component_detects_tampering(tmp_path):
    warehouse = MarketDataWarehouse(tmp_path / "market")
    store = ComponentStore(tmp_path / "components", warehouse)
    component = store.record_materialized(
        ComponentKind.FIELD_ADJUDICATIONS, _scope(), _frame(), builder_version="test-r1",
    )
    (store.component_path(component.component_id) / "data.parquet").write_bytes(b"tampered")

    with pytest.raises(IntegrityError, match="file mismatch"):
        ComponentStore(tmp_path / "components", MarketDataWarehouse(tmp_path / "market")).load(
            component.component_id,
        )


def test_source_backed_component_reads_the_exact_declared_slice(tmp_path):
    warehouse = MarketDataWarehouse(tmp_path / "market")
    observed = warehouse.record_observation(observation())
    store = ComponentStore(tmp_path / "components", warehouse)
    scope = _scope()
    projection = store.source_projection(observed.observation_id, scope)
    component = store.record_source(
        ComponentKind.MARKET_FACTS, scope, (projection,), builder_version="source-r1",
    )
    store.opened_component_ids.clear()

    frame = store.read(
        component.component_id,
        instrument_ids=("600000.SH",), start_date=DAYS[1], end_date=DAYS[1],
    )

    assert list(frame.columns) == list(scope.fields)
    assert frame.to_dict("records") == [
        {"instrument_id": "600000.SH", "session_date": DAYS[1].isoformat(), "price_mode": "adjusted", "close": 8.8},
        {"instrument_id": "600000.SH", "session_date": DAYS[1].isoformat(), "price_mode": "raw", "close": 11.0},
    ]
    assert component.component_id in store.opened_component_ids
    assert component.component_id in warehouse._opened_component_ids


def test_source_locator_is_bound_into_component_identity(tmp_path):
    warehouse = MarketDataWarehouse(tmp_path / "market")
    observed = warehouse.record_observation(observation())
    store = ComponentStore(tmp_path / "components", warehouse)
    scope = _scope()
    component = store.record_source(
        ComponentKind.MARKET_FACTS, scope,
        (store.source_projection(observed.observation_id, scope),),
        builder_version="source-r1",
    )
    manifest_path = store.component_path(component.component_id) / "manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["projections"][0]["observation_id"] = "obs-" + "f" * 24
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(IntegrityError, match="content identity mismatch"):
        ComponentStore(tmp_path / "components", MarketDataWarehouse(tmp_path / "market")).load(
            component.component_id,
        )


def test_first_component_verification_is_shared_by_new_stores(tmp_path, monkeypatch):
    warehouse = MarketDataWarehouse(tmp_path / "market")
    observed = warehouse.record_observation(observation())
    store = ComponentStore(tmp_path / "components", warehouse)
    scope = _scope()
    component = store.record_source(
        ComponentKind.MARKET_FACTS, scope,
        (store.source_projection(observed.observation_id, scope),),
        builder_version="source-r1",
    )
    warehouse._verified_component_manifests.clear()
    warehouse._verified_component_source_paths.clear()
    calls = 0
    original = warehouse.load_observation

    def counted(observation_id):
        nonlocal calls
        calls += 1
        return original(observation_id)

    monkeypatch.setattr(warehouse, "load_observation", counted)
    ComponentStore(tmp_path / "components", warehouse).load(
        component.component_id,
    )
    ComponentStore(tmp_path / "components", warehouse).load(
        component.component_id,
    )

    assert calls == 1


def test_reopening_a_previously_seen_component_appends_a_new_io_event(tmp_path):
    warehouse = MarketDataWarehouse(tmp_path / "market")
    observed = warehouse.record_observation(observation())
    scope = _scope()
    first = ComponentStore(tmp_path / "components", warehouse)
    component = first.record_source(
        ComponentKind.MARKET_FACTS,
        scope,
        (first.source_projection(observed.observation_id, scope),),
        builder_version="source-r1",
    )
    first.read(component.component_id)
    before = len(warehouse._component_open_events)

    ComponentStore(tmp_path / "components", warehouse).read(component.component_id)

    assert warehouse._component_open_events[before:] == [component.component_id]
