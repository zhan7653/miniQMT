from fundlab.common.config import get_path, load_config


def test_load_base_config():
    config = load_config("config/base.yaml")

    assert get_path(config, "sqlite_db").as_posix() == "data/warehouse/sqlite/fundlab.db"
    assert get_path(config, "parquet_root").as_posix() == "data/warehouse/parquet"

