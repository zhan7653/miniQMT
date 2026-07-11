from __future__ import annotations

import math

import pandas as pd

from fundlab.data.platform import PriceMode, QualityDisposition
from fundlab.data.portal import AdjustedDataUnavailable, DataPortal


class FeatureEngine:
    """Compute publishable v2 features from adjusted prices and raw liquidity only."""

    WARMUP_DAYS = 120
    SCHEMA_VERSION = 1

    def __init__(self, data_portal: DataPortal, *, provider: str, batch_id: str,
                 candidate_version_id: str, feature_version: str = "v2") -> None:
        if not provider or not batch_id or not candidate_version_id:
            raise ValueError("provider, batch_id and candidate_version_id are required provenance")
        self.data_portal = data_portal
        self.provider = provider
        self.batch_id = batch_id
        self.candidate_version_id = candidate_version_id
        self.feature_version = feature_version

    def compute(self, start_date: str, end_date: str, symbols: list[str] | None = None) -> pd.DataFrame:
        all_days = self.data_portal.get_trading_days("1900-01-01", end_date)
        output_days = [day for day in all_days if start_date <= day <= end_date]
        if not output_days:
            return self.empty_frame()
        first = all_days.index(output_days[0])
        history_start = all_days[max(0, first - self.WARMUP_DAYS)]
        universe = symbols or self.data_portal.get_universe(output_days[-1])
        if not universe:
            return self.empty_frame()

        try:
            adjusted = self.data_portal.get_daily_bar(universe, history_start, output_days[-1],
                                                       fields=["close"], price_mode=PriceMode.ADJUSTED).reset_index()
        except AdjustedDataUnavailable:
            return self.empty_frame()
        raw = self.data_portal.get_daily_bar(universe, history_start, output_days[-1],
                                             fields=["amount"], price_mode=PriceMode.RAW).reset_index()
        if adjusted.empty or raw.empty:
            return self.empty_frame()
        bars = adjusted[["date", "symbol", "close"]].merge(
            raw[["date", "symbol", "amount"]], on=["date", "symbol"], how="inner", validate="one_to_one"
        ).dropna(subset=["close", "amount"])

        frames: list[pd.DataFrame] = []
        for _, group in bars.groupby("symbol", sort=True):
            history = group.sort_values("date").copy()
            close, amount = history["close"].astype(float), history["amount"].astype(float)
            returns = close.pct_change()
            history["ret_1d"] = close.pct_change(1)
            history["ret_5d"] = close.pct_change(5)
            history["ret_20d"] = close.pct_change(20)
            history["ret_60d"] = close.pct_change(60)
            history["ret_120d"] = close.pct_change(120)
            history["volatility_20d"] = returns.rolling(20, min_periods=20).std() * math.sqrt(252)
            history["volatility_60d"] = returns.rolling(60, min_periods=60).std() * math.sqrt(252)
            rolling_max = close.rolling(60, min_periods=60).max()
            history["max_drawdown_60d"] = (close / rolling_max - 1).rolling(60, min_periods=60).min()
            history["amount_avg_20d"] = amount.rolling(20, min_periods=20).mean()
            history["amount_avg_60d"] = amount.rolling(60, min_periods=60).mean()
            frames.append(history[history["date"].isin(output_days)])

        if not frames:
            return self.empty_frame()
        result = pd.concat(frames, ignore_index=True)
        result["feature_version"] = self.feature_version
        result["schema_version"] = self.SCHEMA_VERSION
        result["provider"] = self.provider
        result["batch_id"] = self.batch_id
        result["data_version"] = self.candidate_version_id
        result["source_data_version"] = self.data_portal.data_version
        result["quality_disposition"] = QualityDisposition.PASS.value
        result["available_date"] = result["date"]
        result["price_mode"] = PriceMode.ADJUSTED.value
        return result[self.columns()].sort_values(["date", "symbol"]).reset_index(drop=True).where(lambda x: pd.notna(x), None)

    @classmethod
    def columns(cls) -> list[str]:
        return ["date", "symbol", "feature_version", "schema_version", "provider", "batch_id", "data_version",
                "source_data_version", "quality_disposition", "available_date", "price_mode", "ret_1d", "ret_5d",
                "ret_20d", "ret_60d", "ret_120d", "volatility_20d", "volatility_60d", "max_drawdown_60d",
                "amount_avg_20d", "amount_avg_60d"]

    @classmethod
    def empty_frame(cls) -> pd.DataFrame:
        return pd.DataFrame(columns=cls.columns())
