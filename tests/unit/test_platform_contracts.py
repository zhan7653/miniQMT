from datetime import date

import pytest

from fundlab.data.platform import (
    BatchStatus,
    PriceMode,
    ProviderCapability,
    ProviderRequest,
    QualityDisposition,
    TrustState,
    UniverseSnapshot,
    VersionStatus,
    load_universe_snapshot,
    stable_fingerprint,
)


def test_provider_and_trust_contracts_are_explicit():
    assert ProviderCapability.DAILY_BARS_RAW.value == "daily_bars_raw"
    assert PriceMode.RAW is not PriceMode.ADJUSTED
    assert set(BatchStatus) == {BatchStatus.PENDING, BatchStatus.RUNNING, BatchStatus.COMPLETE, BatchStatus.FAILED}
    assert set(VersionStatus) == {
        VersionStatus.BUILDING, VersionStatus.COMPLETE, VersionStatus.FAILED, VersionStatus.SUPERSEDED
    }
    assert QualityDisposition.BLOCK_BATCH.value == "block_batch"
    assert TrustState.QUARANTINED.value == "quarantined"


def test_provider_request_rejects_empty_or_reversed_scope():
    with pytest.raises(ValueError, match="symbols"):
        ProviderRequest((), date(2026, 1, 1), date(2026, 1, 2), ProviderCapability.TRADING_CALENDAR)
    with pytest.raises(ValueError, match="start_date"):
        ProviderRequest(("510300.SH",), date(2026, 1, 2), date(2026, 1, 1), ProviderCapability.DAILY_BARS_RAW)


def test_universe_snapshot_is_reviewed_versioned_and_deterministic():
    snapshot = UniverseSnapshot.create(
        version="reviewed-v1",
        effective_date=date(2026, 5, 7),
        symbols=("510500.SH", "510300.SH", "510300.SH"),
        benchmarks=("510300.SH",),
        configuration={"manual_include": ["510300.SH", "510500.SH"]},
    )
    assert snapshot.symbols == ("510300.SH", "510500.SH")
    assert snapshot.config_hash == stable_fingerprint({"manual_include": ["510300.SH", "510500.SH"]})


def test_universe_rejects_discovered_or_missing_benchmark_membership():
    with pytest.raises(ValueError, match="Benchmarks"):
        UniverseSnapshot("v1", date(2026, 5, 7), ("510500.SH",), ("510300.SH",), "hash")


def test_reviewed_universe_file_is_authoritative(tmp_path):
    path = tmp_path / "universe.yaml"
    path.write_text(
        "version: reviewed-v1\neffective_date: '2026-05-07'\nsymbols: [510300.SH]\nbenchmarks: [510300.SH]\n",
        encoding="utf-8",
    )
    assert load_universe_snapshot(path).symbols == ("510300.SH",)
