from __future__ import annotations

import pytest

from scripts.benchmark_data_v2 import run_benchmarks


pytestmark = pytest.mark.performance


def test_fixed_local_data_v2_thresholds(tmp_path):
    result = run_benchmarks(tmp_path)
    assert result["backtest_seconds"] <= 10.0
    assert result["snapshot_cold_seconds"] <= 1.0
    assert result["snapshot_repeat_seconds"] <= 0.1
    assert result["feature_publish_seconds"] <= 60.0
