from fundlab.backtest.cost import CostModel
from fundlab.backtest.slippage import SlippageModel


def test_cost_model_etf_commission_only():
    assert CostModel(commission_rate=0.00003, min_commission=0).calculate(100_000) == 3


def test_slippage_model_adjusts_buy_and_sell():
    model = SlippageModel(base_bps=2)

    buy_price, buy_slippage = model.adjust_price(10, "buy")
    sell_price, sell_slippage = model.adjust_price(10, "sell")

    assert buy_price == 10.002
    assert sell_price == 9.998
    assert buy_slippage == sell_slippage == 0.002

