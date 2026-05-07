from fundlab.backtest.execution import ExecutionPlanner
from fundlab.backtest.models import Account
from fundlab.common.config import get_path, load_config
from fundlab.data.portal import DataPortal
from fundlab.data.storage import ParquetStore, SQLiteStore
from fundlab.risk import PremiumDiscountCheck, RiskEngine
from scripts.create_fake_data import create_fake_data


def build_portal() -> DataPortal:
    create_fake_data()
    config = load_config()
    return DataPortal(SQLiteStore(get_path(config, "sqlite_db")), ParquetStore(get_path(config, "parquet_root")), config)


def test_cross_border_missing_premium_discount_rejects_buy():
    portal = build_portal()
    portal.sqlite_store.execute_many(
        "UPDATE fund_master SET asset_class = ? WHERE symbol = ?",
        [["cross_border", "510500.SH"]],
    )
    account = Account.create("test", 1_000_000)
    intents = ExecutionPlanner().create_intents("test", "s", "2026-01-02", "2026-01-05", {"510500.SH": 0.5})
    orders = ExecutionPlanner().create_orders(intents, account, portal)

    checked = RiskEngine([PremiumDiscountCheck(max_premium_abs=0.03)]).check_orders(orders, account, "2026-01-05", portal)

    assert checked[0].status == "rejected"
    assert checked[0].reject_reason == "missing_premium_discount"


def test_sell_is_allowed_even_when_premium_missing():
    portal = build_portal()
    portal.sqlite_store.execute_many(
        "UPDATE fund_master SET asset_class = ? WHERE symbol = ?",
        [["cross_border", "510500.SH"]],
    )
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

