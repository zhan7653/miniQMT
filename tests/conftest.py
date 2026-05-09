from __future__ import annotations

import shutil

import pytest
import yaml


@pytest.fixture(autouse=True)
def isolated_fundlab_config(tmp_path, monkeypatch):
    test_root = tmp_path / "fundlab_test"
    sqlite_db = test_root / "sqlite" / "fundlab.db"
    parquet_root = test_root / "parquet"
    config = {
        "paths": {
            "sqlite_db": sqlite_db.as_posix(),
            "parquet_root": parquet_root.as_posix(),
            "log_dir": (test_root / "logs").as_posix(),
        },
        "data": {
            "default_feature_version": "v1",
            "source": "test",
            "default_start_date": "2026-01-02",
            "default_end_date": "2026-02-12",
            "validation_symbols": ["510300.SH", "510500.SH", "518880.SH"],
        },
        "universe": {
            "include_product_types": ["ETF", "MONEY_ETF"],
            "include_management_types": [
                "passive_index",
                "smart_beta",
                "passive_bond",
                "passive_commodity",
                "passive_money_market",
                "cross_border_index",
            ],
        },
        "xtquant": {
            "calendar_market": "SH",
            "calendar_probe_symbol": "510300.SH",
        },
    }
    config_path = test_root / "config.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(yaml.safe_dump(config, sort_keys=False, allow_unicode=True), encoding="utf-8")
    monkeypatch.setenv("FUNDLAB_CONFIG_PATH", config_path.as_posix())
    yield
    shutil.rmtree(test_root, ignore_errors=True)
