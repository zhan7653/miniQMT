from fundlab.backtest.execution import ExecutionPlanner
from fundlab.backtest.models import Account
from fundlab.risk import PremiumDiscountCheck, RiskEngine
from scripts.create_fake_data import create_fake_v2_portal


def test_untrusted_premium_discount_is_not_read_for_buy(tmp_path):
    portal = create_fake_v2_portal(tmp_path / "v2")
    account = Account.create("test", 1_000_000)
    intents = ExecutionPlanner().create_intents("test", "s", "2026-01-02", "2026-01-05", {"510500.SH": 0.5})
    orders = ExecutionPlanner().create_orders(intents, account, portal)

    checked = RiskEngine([PremiumDiscountCheck(max_premium_abs=0.03)]).check_orders(orders, account, "2026-01-05", portal)

    assert checked[0].status == "pending"
    assert checked[0].reject_reason is None


def test_sell_is_allowed_without_legacy_premium_data(tmp_path):
    portal = create_fake_v2_portal(tmp_path / "v2")
    account = Account.create("test", 1_000_000)
    intents = ExecutionPlanner().create_intents("test", "s", "2026-01-02", "2026-01-05", {"510500.SH": 0.0})
    account.positions["510500.SH"] = type(
        "PositionLike",
        (),
        {"quantity": 1000, "market_value": 6000, "avg_cost": 6, "market_price": 6},
    )()
    orders = ExecutionPlanner().create_orders(intents, account, portal)

    checked = RiskEngine([PremiumDiscountCheck(max_premium_abs=0.03)]).check_orders(orders, account, "2026-01-05", portal)

    assert checked[0].status == "pending"
    assert checked[0].side == "sell"
