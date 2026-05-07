import random
from xtquant import xtdata
from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
from xtquant import xtconstant

# ================= 配置区域 =================
# 替换为你的 QMT 安装目录下的 userdata_mini 路径 (注意路径中的斜杠需要转义或用 r 前缀)
mini_qmt_path = r'D:\software\国金QMT交易端模拟\userdata_mini'
# 替换为你的资金账号
account_id = '66633585' 
# 账号类型：通常 A 股选 STOCK，具体参考官方文档
account_type = 'STOCK'
# ==========================================

class MyTraderCallback(XtQuantTraderCallback):
    def on_disconnected(self):
        print("交易服务器连接断开")

    def on_stock_order(self, order):
        print(f"委托回报: {order.stock_code}, 状态: {order.order_status}, 数量: {order.order_volume}")

def main():
    print("正在连接 miniQMT...")
    
    # 1. 初始化交易对象
    # session_id 要求是一个整数，每次启动尽量不同
    session_id = int(random.randint(100000, 999999))
    xt_trader = XtQuantTrader(mini_qmt_path, session_id)
    
    # 注册回调类（用于接收订单状态更新等异步信息）
    callback = MyTraderCallback()
    xt_trader.register_callback(callback)
    
    # 2. 启动交易线程并连接
    xt_trader.start()
    connect_result = xt_trader.connect()
    
    if connect_result == 0:
        print("交易接口连接成功！")
    else:
        print(f"交易接口连接失败，错误码: {connect_result}")
        return

    # 3. 订阅账号
    from xtquant.xttype import StockAccount
    account = StockAccount(account_id, account_type)
    subscribe_result = xt_trader.subscribe(account)
    
    if subscribe_result == 0:
        print("账号订阅成功！")
    else:
        print(f"账号订阅失败，错误码: {subscribe_result}")
        
    # 4. 获取账户资产信息
    asset = xt_trader.query_stock_asset(account)
    if asset:
        print(f"账户总资产: {asset.m_dTotalAsset}")
        print(f"可用资金: {asset.m_dCash}")
        print(f"持仓市值: {asset.m_dMarketValue}")

    # 5. 获取一段测试行情数据 (例如浦发银行)
    test_stock = '600000.SH'
    print(f"\n正在获取 {test_stock} 的最新行情...")
    xtdata.subscribe_quote(test_stock, period='1d', start_time='', end_time='', count=1, callback=None)
    tick = xtdata.get_full_tick([test_stock])
    print(tick)

    # 阻塞主线程，保持程序运行
    # xt_trader.run_until_quit() 

if __name__ == '__main__':
    main()