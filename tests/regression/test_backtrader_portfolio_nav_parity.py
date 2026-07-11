from __future__ import annotations

from dataclasses import dataclass
from math import floor
from pathlib import Path

import backtrader as bt
import pandas as pd
import pytest

from fundlab.backtest.broker import BacktestBroker
from fundlab.backtest.cost import CostModel
from fundlab.backtest.engine import BacktestEngine
from fundlab.backtest.models import Order
from fundlab.backtest.slippage import SlippageModel
from fundlab.data.platform import PriceMode
from fundlab.data.portal import DataPortal
from fundlab.risk import RiskEngine
from fundlab.strategies import (
    AssetAllocationStrategy,
    DividendValueStrategy,
    EqualWeightStrategy,
    MomentumRotationStrategy,
    ValueMomentumStrategy,
)
from fundlab.strategies.base import Strategy
from scripts.create_fake_data import create_fake_v2_portal


REPO_ROOT = Path(__file__).resolve().parents[2]
BASE_CONFIG_PATH = REPO_ROOT / "config" / "base.yaml"
MIN_USABLE_DAYS = 20
TOLERANCE = 1e-6
ASSUMPTIONS = {
    "initial_cash": 1_000_000.0,
    "target_weight": 1.0,
    "execution_price_rule": "first signal then next trading day open",
    "mark_price_rule": "daily close",
    "fees": 0.0,
    "slippage": 0.0,
    "lot_size": 100,
    "adjustment": "none",
    "dividends": "excluded",
    "tolerance": TOLERANCE,
}
STRATEGY_PARITY_START_DATE = "2026-01-02"
CLOCK_SYMBOL = "510300.SH"


@dataclass(frozen=True)
class BenchmarkRange:
    symbol: str
    start_date: str
    end_date: str
    length: int


class SingleAssetBuyAndHoldStrategy(Strategy):
    strategy_id = "backtrader_parity_single_asset_buy_and_hold"

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.rebalanced = False

    def on_rebalance(self, date: str, data_portal: DataPortal, context: dict) -> dict[str, float]:
        if self.rebalanced:
            return {}
        self.rebalanced = True
        return {self.symbol: 1.0, "cash": 0.0}


class BacktraderBuyAndHold(bt.Strategy):
    params = (("size", 0), ("initial_cash", 1.0), ("records", None))

    def __init__(self):
        self.ordered = False

    def next(self):
        if not self.ordered:
            self.buy(size=self.p.size)
            self.ordered = True
        self.p.records.append(
            {
                "date": self.datas[0].datetime.date(0).isoformat(),
                "nav": self.broker.getvalue() / self.p.initial_cash,
                "total_asset": self.broker.getvalue(),
            }
        )


class BacktraderOrderReplay(bt.Strategy):
    params = (("orders_by_signal_date", None), ("initial_cash", 1.0), ("records", None))

    def __init__(self):
        self.submitted_dates: set[str] = set()

    def next(self):
        current_date = self.datas[0].datetime.date(0).isoformat()
        if current_date not in self.submitted_dates:
            data_by_name = {data._name: data for data in self.datas}
            for order in self.p.orders_by_signal_date.get(current_date, []):
                data = data_by_name.get(order.symbol)
                if data is None:
                    pytest.fail(f"Backtrader replay missing data feed for {order.symbol}")
                if order.side == "buy":
                    self.buy(data=data, size=order.quantity)
                else:
                    self.sell(data=data, size=order.quantity)
            self.submitted_dates.add(current_date)
        self.p.records.append(
            {
                "date": current_date,
                "nav": self.broker.getvalue() / self.p.initial_cash,
                "total_asset": self.broker.getvalue(),
            }
        )


def test_single_etf_buy_and_hold_portfolio_nav_matches_backtrader_v2(tmp_path):
    portal = create_fake_v2_portal(tmp_path / "v2")
    benchmark_range, bars = select_benchmark_range(portal)

    fundlab_nav = run_fundlab_benchmark(portal, benchmark_range)
    backtrader_nav = run_backtrader_benchmark(bars)

    assert_nav_parity(benchmark_range, fundlab_nav, backtrader_nav)


@pytest.mark.parametrize(
    "strategy",
    [
        EqualWeightStrategy(["510300.SH", "510500.SH", "518880.SH"], cash_weight=0.02),
        MomentumRotationStrategy(),
        ValueMomentumStrategy(),
        DividendValueStrategy(),
        AssetAllocationStrategy(),
    ],
    ids=[
        "equal_weight",
        "momentum_rotation",
        "value_momentum",
        "dividend_value",
        "asset_allocation",
    ],
)
def test_rule_strategy_portfolio_nav_matches_backtrader_v2(strategy: Strategy, tmp_path):
    portal = create_fake_v2_portal(tmp_path / "v2")
    end_date = "2026-02-12"

    fundlab_nav, orders = run_fundlab_strategy_benchmark(portal, strategy, STRATEGY_PARITY_START_DATE, end_date)
    symbols = sorted({order.symbol for order in orders})
    bars = load_replay_bars(portal, symbols, STRATEGY_PARITY_START_DATE, end_date)
    backtrader_nav = run_backtrader_order_replay(bars, orders)

    assert_strategy_nav_parity(strategy.strategy_id, symbols, fundlab_nav, backtrader_nav, orders)


def resolve_repo_path(path: Path) -> Path:
    return path if path.is_absolute() else REPO_ROOT / path


def select_benchmark_range(portal: DataPortal) -> tuple[BenchmarkRange, pd.DataFrame]:
    candidates = get_candidate_symbols(portal)
    start_date = "2026-01-02"
    end_date = "2026-02-12"

    bars = portal.get_daily_bar(
        candidates,
        start_date=start_date,
        end_date=end_date,
        fields=["open", "high", "low", "close", "volume"],
        price_mode=PriceMode.RAW,
    )
    if bars.empty:
        pytest.fail(real_data_failure_message("no candidate ETF daily bars were found"))

    bars = bars.reset_index()
    valid_bars = bars[usable_bar_mask(bars)].copy()
    ranges = find_continuous_ranges(portal, valid_bars)
    eligible = [item for item in ranges if item.length >= MIN_USABLE_DAYS]
    if not eligible:
        pytest.fail(real_data_failure_message(f"required >= {MIN_USABLE_DAYS} continuous usable OHLCV daily bars"))

    selected = sorted(eligible, key=lambda item: (-item.length, item.start_date, item.end_date, item.symbol))[0]
    selected_bars = valid_bars[
        (valid_bars["symbol"] == selected.symbol)
        & (valid_bars["date"] >= selected.start_date)
        & (valid_bars["date"] <= selected.end_date)
    ].sort_values("date")
    return selected, selected_bars.reset_index(drop=True)


def get_candidate_symbols(portal: DataPortal) -> list[str]:
    symbols = portal.get_universe("2026-02-12")
    if not symbols:
        pytest.fail(real_data_failure_message("no ETF symbols were found in fund_master"))
    return symbols


def usable_bar_mask(bars: pd.DataFrame) -> pd.Series:
    price_columns = ["open", "high", "low", "close"]
    prices = bars[price_columns].apply(pd.to_numeric, errors="coerce")
    volume = pd.to_numeric(bars["volume"], errors="coerce")
    return (
        prices.notna().all(axis=1)
        & (prices > 0).all(axis=1)
        & volume.notna()
        & (volume >= 0)
        & (prices["high"] >= prices[["open", "close"]].max(axis=1))
        & (prices["low"] <= prices[["open", "close"]].min(axis=1))
    )


def find_continuous_ranges(portal: DataPortal, valid_bars: pd.DataFrame) -> list[BenchmarkRange]:
    ranges: list[BenchmarkRange] = []
    for symbol, group in valid_bars.groupby("symbol"):
        dates = sorted(group["date"].drop_duplicates().tolist())
        if not dates:
            continue
        trading_days = portal.get_trading_days(dates[0], dates[-1])
        if not trading_days:
            continue
        valid_dates = set(dates)
        current_start: str | None = None
        current_end: str | None = None
        current_length = 0
        for trading_day in trading_days:
            if trading_day in valid_dates:
                current_start = current_start or trading_day
                current_end = trading_day
                current_length += 1
            elif current_start is not None and current_end is not None:
                ranges.append(BenchmarkRange(str(symbol), current_start, current_end, current_length))
                current_start = None
                current_end = None
                current_length = 0
        if current_start is not None and current_end is not None:
            ranges.append(BenchmarkRange(str(symbol), current_start, current_end, current_length))
    return ranges


def run_fundlab_benchmark(portal: DataPortal, benchmark_range: BenchmarkRange) -> pd.DataFrame:
    engine = BacktestEngine(
        data_portal=portal,
        strategy=SingleAssetBuyAndHoldStrategy(benchmark_range.symbol),
        start_date=benchmark_range.start_date,
        end_date=benchmark_range.end_date,
        initial_cash=ASSUMPTIONS["initial_cash"],
        rebalance_frequency="monthly",
        risk_engine=RiskEngine([]),
    )
    engine.broker = BacktestBroker(
        cost_model=CostModel(commission_rate=0.0, min_commission=0.0),
        slippage_model=SlippageModel(base_bps=0.0),
    )
    recorder = engine.run()
    nav = recorder.account_daily_frame()[["date", "nav", "total_asset"]]
    if nav.empty:
        pytest.fail(f"FundLab produced no NAV rows for {benchmark_range}")
    return nav


def run_fundlab_strategy_benchmark(
    portal: DataPortal, strategy: Strategy, start_date: str, end_date: str
) -> tuple[pd.DataFrame, list[Order]]:
    engine = BacktestEngine(
        data_portal=portal,
        strategy=strategy,
        start_date=start_date,
        end_date=end_date,
        initial_cash=ASSUMPTIONS["initial_cash"],
        rebalance_frequency="monthly",
        risk_engine=RiskEngine([]),
    )
    engine.broker = BacktestBroker(
        cost_model=CostModel(commission_rate=0.0, min_commission=0.0),
        slippage_model=SlippageModel(base_bps=0.0),
    )
    recorder = engine.run()
    nav = recorder.account_daily_frame()[["date", "nav", "total_asset"]]
    if nav.empty:
        pytest.fail(f"FundLab produced no NAV rows for strategy {strategy.strategy_id}")
    orders = [order for order in recorder.orders if order.status != "rejected" and order.quantity > 0]
    if not orders:
        pytest.skip(f"FundLab strategy {strategy.strategy_id} produced no executable orders")
    return nav, orders


def load_replay_bars(portal: DataPortal, symbols: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    symbols_with_clock = sorted(set(symbols) | {CLOCK_SYMBOL})
    bars = portal.get_daily_bar(
        symbols_with_clock,
        start_date=start_date,
        end_date=end_date,
        fields=["open", "high", "low", "close", "volume"],
        price_mode=PriceMode.RAW,
    )
    if bars.empty:
        pytest.fail(f"No daily bars found for Backtrader replay symbols={symbols_with_clock}")
    bars = bars.reset_index()
    if CLOCK_SYMBOL not in set(bars["symbol"]):
        pytest.fail(f"Clock symbol {CLOCK_SYMBOL} has no bars for Backtrader strategy replay")
    return bars


def run_backtrader_order_replay(bars: pd.DataFrame, orders: list[Order]) -> pd.DataFrame:
    initial_cash = float(ASSUMPTIONS["initial_cash"])
    records: list[dict] = []
    cerebro = bt.Cerebro()
    symbols = sorted(set(bars["symbol"]))
    feed_order = [CLOCK_SYMBOL] + [symbol for symbol in symbols if symbol != CLOCK_SYMBOL]
    clock_dates = pd.to_datetime(bars[bars["symbol"] == CLOCK_SYMBOL]["date"].sort_values().drop_duplicates())
    for symbol in feed_order:
        feed_data = bars[bars["symbol"] == symbol][["date", "open", "high", "low", "close", "volume"]].copy()
        if feed_data.empty:
            continue
        feed_data["date"] = pd.to_datetime(feed_data["date"])
        feed_data["openinterest"] = 0
        feed_data = feed_data.set_index("date").sort_index()
        feed_data = feed_data.reindex(clock_dates)
        price_columns = ["open", "high", "low", "close"]
        feed_data[price_columns] = feed_data[price_columns].ffill().bfill()
        feed_data["volume"] = feed_data["volume"].fillna(0)
        feed_data["openinterest"] = 0
        cerebro.adddata(bt.feeds.PandasData(dataname=feed_data), name=symbol)

    orders_by_signal_date: dict[str, list[Order]] = {}
    for order in sorted(orders, key=lambda item: (item.signal_date, 0 if item.side == "sell" else 1, item.symbol)):
        orders_by_signal_date.setdefault(order.signal_date, []).append(order)

    cerebro.addstrategy(
        BacktraderOrderReplay,
        orders_by_signal_date=orders_by_signal_date,
        initial_cash=initial_cash,
        records=records,
    )
    cerebro.broker.setcash(initial_cash)
    cerebro.broker.setcommission(commission=0.0)
    cerebro.broker.set_slippage_perc(perc=0.0)
    cerebro.broker.set_checksubmit(False)
    cerebro.run()

    nav = pd.DataFrame(records)
    if nav.empty:
        pytest.fail("Backtrader strategy order replay produced no NAV rows")
    return nav[["date", "nav", "total_asset"]]


def run_backtrader_benchmark(bars: pd.DataFrame) -> pd.DataFrame:
    if len(bars) < 2:
        pytest.fail("Backtrader benchmark requires at least two bars to match FundLab next-open execution")

    initial_cash = float(ASSUMPTIONS["initial_cash"])
    execution_open = float(bars.iloc[1]["open"])
    quantity = floor(initial_cash / execution_open / ASSUMPTIONS["lot_size"]) * ASSUMPTIONS["lot_size"]
    if quantity <= 0:
        pytest.fail(f"Computed Backtrader buy quantity is zero from execution open {execution_open}")

    feed_data = bars[["date", "open", "high", "low", "close", "volume"]].copy()
    feed_data["date"] = pd.to_datetime(feed_data["date"])
    feed_data["openinterest"] = 0
    feed_data = feed_data.set_index("date")

    records: list[dict] = []
    cerebro = bt.Cerebro()
    cerebro.adddata(bt.feeds.PandasData(dataname=feed_data))
    cerebro.addstrategy(BacktraderBuyAndHold, size=quantity, initial_cash=initial_cash, records=records)
    cerebro.broker.setcash(initial_cash)
    cerebro.broker.setcommission(commission=0.0)
    cerebro.broker.set_slippage_perc(perc=0.0)
    cerebro.run()

    nav = pd.DataFrame(records)
    if nav.empty:
        pytest.fail("Backtrader produced no NAV rows")
    return nav[["date", "nav", "total_asset"]]


def assert_nav_parity(benchmark_range: BenchmarkRange, fundlab_nav: pd.DataFrame, backtrader_nav: pd.DataFrame) -> None:
    fundlab_dates = set(fundlab_nav["date"])
    backtrader_dates = set(backtrader_nav["date"])
    if fundlab_dates != backtrader_dates:
        pytest.fail(
            "Backtrader parity date alignment failed for "
            f"{benchmark_range.symbol}. Missing in FundLab: {sorted(backtrader_dates - fundlab_dates)[:5]}; "
            f"missing in Backtrader: {sorted(fundlab_dates - backtrader_dates)[:5]}; "
            f"assumptions={ASSUMPTIONS}"
        )

    comparison = fundlab_nav.merge(
        backtrader_nav,
        on="date",
        how="inner",
        suffixes=("_fundlab", "_backtrader"),
    ).sort_values("date")
    comparison["abs_diff"] = (comparison["nav_fundlab"] - comparison["nav_backtrader"]).abs()
    mismatches = comparison[comparison["abs_diff"] > TOLERANCE]
    if not mismatches.empty:
        first = mismatches.iloc[0]
        pytest.fail(
            f"Backtrader parity failed for {benchmark_range.symbol}. "
            f"Range: {benchmark_range.start_date} to {benchmark_range.end_date}. "
            f"Assumptions: {ASSUMPTIONS}. "
            f"First mismatch: {first['date']}. "
            f"FundLab NAV: {first['nav_fundlab']:.12f}. "
            f"Backtrader NAV: {first['nav_backtrader']:.12f}. "
            f"Abs diff: {first['abs_diff']:.12f}. "
            f"Tolerance: {TOLERANCE:.12f}."
        )


def assert_strategy_nav_parity(
    strategy_id: str,
    symbols: list[str],
    fundlab_nav: pd.DataFrame,
    backtrader_nav: pd.DataFrame,
    orders: list[Order],
) -> None:
    fundlab_dates = set(fundlab_nav["date"])
    backtrader_dates = set(backtrader_nav["date"])
    if fundlab_dates != backtrader_dates:
        pytest.fail(
            "Backtrader strategy parity date alignment failed for "
            f"{strategy_id}. Missing in FundLab: {sorted(backtrader_dates - fundlab_dates)[:5]}; "
            f"missing in Backtrader: {sorted(fundlab_dates - backtrader_dates)[:5]}; "
            f"symbols={symbols}; assumptions={ASSUMPTIONS}"
        )

    comparison = fundlab_nav.merge(
        backtrader_nav,
        on="date",
        how="inner",
        suffixes=("_fundlab", "_backtrader"),
    ).sort_values("date")
    comparison["abs_diff"] = (comparison["nav_fundlab"] - comparison["nav_backtrader"]).abs()
    mismatches = comparison[comparison["abs_diff"] > TOLERANCE]
    if not mismatches.empty:
        first = mismatches.iloc[0]
        pytest.fail(
            f"Backtrader strategy parity failed for {strategy_id}. "
            f"Symbols: {symbols}. Order count: {len(orders)}. "
            f"Assumptions: {ASSUMPTIONS}. "
            f"First mismatch: {first['date']}. "
            f"FundLab NAV: {first['nav_fundlab']:.12f}. "
            f"Backtrader NAV: {first['nav_backtrader']:.12f}. "
            f"Abs diff: {first['abs_diff']:.12f}. "
            f"Tolerance: {TOLERANCE:.12f}."
        )


def real_data_failure_message(reason: str) -> str:
    return f"No eligible isolated v2 ETF/range exists: {reason}."
