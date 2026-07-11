from datetime import date, datetime, timezone

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from fundlab.data.platform import (BatchRecord, BatchStatus, DataCatalog, ManifestIdentity, TrustState,
                                   VersionRecord, VersionStatus)
from fundlab.data.portal import DataPortal
from fundlab.data.storage import VersionedParquetStore
from fundlab.features import FeatureEngine


def _portal(tmp_path, *, adjusted=True):
    days = pd.bdate_range("2025-01-01", periods=150).strftime("%Y-%m-%d").tolist()
    catalog = DataCatalog(tmp_path / "catalog.sqlite")
    catalog.initialize()
    catalog.create_batch(BatchRecord("source-batch", "xtquant", date.fromisoformat(days[0]), date.fromisoformat(days[-1]),
                                     ("A",), "u1", "cfg", "request", BatchStatus.PENDING, 0, None))
    store = VersionedParquetStore(tmp_path / "v2", catalog)
    store.begin_version("source-v1")
    raw = pa.table({"date": days, "symbol": ["A"] * len(days), "amount": np.arange(len(days)) * 10.0 + 100.0})
    store.write_table("source-v1", "daily_bars_raw", raw)
    if adjusted:
        close = np.linspace(100.0, 249.0, len(days))
        store.write_table("source-v1", "daily_bars_adjusted",
                          pa.table({"date": days, "symbol": ["A"] * len(days), "close": close}))
    store.write_table("source-v1", "calendar", pa.table({"date": days, "is_trading_day": [True] * len(days)}))
    store.write_table("source-v1", "universe", pa.table({"symbol": ["A"], "effective_date": [days[0]]}))
    identity = ManifestIdentity("xtquant", "source-batch", "source-v1", datetime.now(timezone.utc), TrustState.TRUSTED,
                                "content", 1, {})
    manifest = store.prepare_manifest(identity, {})
    catalog.create_version(VersionRecord("source-v1", "source-batch", manifest.fingerprint, "content",
                                         VersionStatus.BUILDING, None, None, None))
    store.finalize_manifest(manifest)
    catalog.complete_version("source-v1")
    return DataPortal.open_latest_complete(store), days


def _engine(portal):
    return FeatureEngine(portal, provider="xtquant", batch_id="candidate-batch",
                         candidate_version_id="candidate-v2")


def test_features_use_120_day_adjusted_history_raw_amount_and_provenance(tmp_path):
    portal, days = _portal(tmp_path)
    result = _engine(portal).compute(days[120], days[-1])
    first = result.iloc[0]
    assert first["ret_120d"] == pytest.approx(1.2)
    assert first["amount_avg_20d"] == np.mean(np.arange(101, 121) * 10.0 + 100.0)
    assert first["provider"] == "xtquant"
    assert first["batch_id"] == "candidate-batch"
    assert first["data_version"] == "candidate-v2"
    assert first["source_data_version"] == "source-v1"
    assert first["price_mode"] == "adjusted"
    assert "valuation_score" not in result.columns
    assert "dividend_yield_12m" not in result.columns
    assert "premium_discount" not in result.columns


def test_missing_adjusted_data_publishes_no_features_and_never_uses_raw(tmp_path):
    portal, days = _portal(tmp_path, adjusted=False)
    assert _engine(portal).compute(days[120], days[-1]).empty


def test_missing_adjusted_row_excludes_only_that_feature_row(tmp_path):
    portal, days = _portal(tmp_path)
    original = portal.get_daily_bar

    def missing(symbols, start, end, fields=None, *, price_mode):
        frame = original(symbols, start, end, fields=fields, price_mode=price_mode)
        if price_mode.value == "adjusted":
            frame = frame.drop(index=(days[-1], "A"))
        return frame

    portal.get_daily_bar = missing
    result = _engine(portal).compute(days[120], days[-1])
    assert days[-1] not in set(result["date"])
