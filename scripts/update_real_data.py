from __future__ import annotations

from fundlab.common.config import load_config
from scripts.check_data_quality import check_daily_bar_quality
from scripts.compute_features import compute_features
from scripts.update_calendar import update_calendar
from scripts.update_daily_bars import update_daily_bars
from scripts.update_dividends import update_dividends
from scripts.update_index_valuation import update_index_valuation
from scripts.update_nav import update_nav
from scripts.update_universe import update_universe


def update_real_data(config_path: str | None = None) -> dict[str, int | str]:
    config = load_config(config_path)
    start_date = config.get("data", {}).get("default_start_date", "2023-01-01")
    end_date = config.get("data", {}).get("default_end_date", "2026-05-07")
    result: dict[str, int | str] = {}
    result["calendar_rows"] = update_calendar(start_date, end_date, config_path=config_path)
    result["universe_rows"] = update_universe(config_path=config_path)
    result["daily_bar_rows"] = update_daily_bars(start_date, end_date, config_path=config_path)
    result["nav_rows"] = update_nav(start_date, end_date, config_path=config_path)
    result["dividend_rows"] = update_dividends(start_date, end_date, config_path=config_path)
    result["index_valuation_rows"] = update_index_valuation(start_date, end_date, config_path=config_path)
    result["feature_rows"] = compute_features(start_date, end_date, config_path=config_path)
    result["quality_report"] = str(check_daily_bar_quality(config_path=config_path))
    return result


def main() -> None:
    result = update_real_data()
    for key, value in result.items():
        print(f"{key}={value}")


if __name__ == "__main__":
    main()
