from __future__ import annotations

from pathlib import Path

from fundlab.common.config import get_path, load_config
from fundlab.data.processors import DailyBarQualityChecker
from fundlab.data.storage import ParquetStore, SQLiteStore


def check_daily_bar_quality(config_path: str | Path | None = None) -> Path:
    config = load_config(config_path)
    sqlite_store = SQLiteStore(get_path(config, "sqlite_db"))
    parquet_store = ParquetStore(get_path(config, "parquet_root"))

    calendar = sqlite_store.get_trading_days("1900-01-01", "2100-12-31")
    trading_days = calendar["date"].tolist()
    fund_master = sqlite_store.get_fund_master()
    symbols = fund_master["symbol"].tolist()
    listed_dates = fund_master.set_index("symbol")["listed_date"].where(fund_master.set_index("symbol")["listed_date"].notna(), None).to_dict()

    bars = parquet_store.read_daily_bar(symbols, "1900-01-01", "2100-12-31").reset_index()
    checker = DailyBarQualityChecker()
    issues = checker.check(bars, trading_days=trading_days, listed_dates=listed_dates)
    report = checker.to_frame(issues)

    output_dir = Path("data/reports/quality")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "daily_bar_quality.csv"
    report.to_csv(output_path, index=False, encoding="utf-8-sig")
    return output_path


def main() -> None:
    output_path = check_daily_bar_quality()
    print(f"Wrote daily bar quality report: {output_path}")


if __name__ == "__main__":
    main()
