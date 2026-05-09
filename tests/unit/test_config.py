import yaml

from fundlab.common.config import get_path, load_config
import scripts.update_real_data as update_real_data_module


def test_load_base_config():
    config = load_config("config/base.yaml")

    assert get_path(config, "sqlite_db").as_posix() == "data/warehouse/sqlite/fundlab.db"
    assert get_path(config, "parquet_root").as_posix() == "data/warehouse/parquet"


def test_update_real_data_forwards_config_path(tmp_path, monkeypatch):
    config_path = tmp_path / "real_data.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "paths": {
                    "sqlite_db": (tmp_path / "fundlab.db").as_posix(),
                    "parquet_root": (tmp_path / "parquet").as_posix(),
                },
                "data": {
                    "default_start_date": "2026-01-02",
                    "default_end_date": "2026-01-05",
                },
            }
        ),
        encoding="utf-8",
    )
    calls = {}

    def record_step(name):
        def step(*args, config_path=None, **kwargs):
            calls[name] = config_path
            return 0

        return step

    monkeypatch.setattr(update_real_data_module, "update_calendar", record_step("calendar"))
    monkeypatch.setattr(update_real_data_module, "update_universe", record_step("universe"))
    monkeypatch.setattr(update_real_data_module, "update_daily_bars", record_step("daily_bars"))
    monkeypatch.setattr(update_real_data_module, "update_nav", record_step("nav"))
    monkeypatch.setattr(update_real_data_module, "update_dividends", record_step("dividends"))
    monkeypatch.setattr(update_real_data_module, "update_index_valuation", record_step("index_valuation"))
    monkeypatch.setattr(update_real_data_module, "compute_features", record_step("features"))
    monkeypatch.setattr(update_real_data_module, "check_daily_bar_quality", record_step("quality"))

    update_real_data_module.update_real_data(config_path=config_path.as_posix())

    assert calls == {
        "calendar": config_path.as_posix(),
        "universe": config_path.as_posix(),
        "daily_bars": config_path.as_posix(),
        "nav": config_path.as_posix(),
        "dividends": config_path.as_posix(),
        "index_valuation": config_path.as_posix(),
        "features": config_path.as_posix(),
        "quality": config_path.as_posix(),
    }

