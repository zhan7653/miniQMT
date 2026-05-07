import pandas as pd
import numpy as np
from xtquant import xtdata
import matplotlib.pyplot as plt
from matplotlib import gridspec

# 配置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

# ================= 配置区域 =================
START_DATE = '20140101'
ETF_POOL = {
    '513100.SH': '纳指ETF',
    '510300.SH': '沪深300ETF',
    '511010.SH': '5年期国债ETF',
    '518880.SH': '黄金ETF'
}
# ==========================================

def get_metrics(nav, benchmark_nav):
    """计算专业量化指标"""
    def calc_single(s_nav):
        rets = s_nav.pct_change().dropna()
        total_ret = s_nav.iloc[-1] - 1
        annual_ret = (s_nav.iloc[-1]) ** (252 / len(s_nav)) - 1
        annual_vol = rets.std() * np.sqrt(252)
        sharpe = (annual_ret - 0.02) / annual_vol
        # 最大回撤计算
        rolling_max = s_nav.cummax()
        drawdown = (s_nav - rolling_max) / rolling_max
        max_dd = drawdown.min()
        return total_ret, annual_ret, annual_vol, sharpe, max_dd

    s_res = calc_single(nav)
    b_res = calc_single(benchmark_nav)
    
    metrics = [
        ["策略收益", f"{s_res[0]:.2%}"],
        ["策略年化", f"{s_res[1]:.2%}"],
        ["基准收益", f"{b_res[0]:.2%}"],
        ["夏普比率", f"{s_res[3]:.3f}"],
        ["最大回撤", f"{s_res[4]:.2%}"],
        ["年化波动", f"{s_res[2]:.2%}"],
        ["胜率(月)", f"{(nav.resample('M').last().pct_change() > 0).mean():.2%}"]
    ]
    return metrics

def run_pro_backtest():
    # 1. 数据获取 (保持之前逻辑)
    codes = list(ETF_POOL.keys())
    for c in codes: xtdata.download_history_data(c, '1d', START_DATE)
    data = xtdata.get_market_data(['close'], codes, '1d', START_DATE, dividend_type='front')
    df = data['close'].T.dropna(how='all')
    df.index = pd.to_datetime(df.index)

    # 2. 策略逻辑 (双动量)
    mom = df / df.shift(252) - 1
    m_ends = df.groupby([df.index.year, df.index.month]).tail(1).index
    weights = pd.DataFrame(0.0, index=df.index, columns=df.columns)
    for d in m_ends:
        cur_mom = mom.loc[d].dropna()
        if not cur_mom.empty and cur_mom.max() > 0:
            weights.loc[d, cur_mom.idxmax()] = 1.0
    
    pos = weights.replace(0, np.nan).ffill().fillna(0).shift(1).fillna(0)
    s_rets = (pos * df.pct_change().fillna(0)).sum(axis=1)
    s_nav = (1 + s_rets).cumprod()
    b_nav = (1 + df.pct_change().fillna(0)['510300.SH']).cumprod()

    # 3. 绘图布局 (模仿你提供的图片)
    fig = plt.figure(figsize=(16, 8))
    gs = gridspec.GridSpec(1, 2, width_ratios=[1, 4])
    
    # --- 左侧：指标表格 ---
    ax_table = plt.subplot(gs[0])
    ax_table.axis('off')
    metrics_data = get_metrics(s_nav, b_nav)
    table = ax_table.table(cellText=metrics_data, colLabels=['指标', '数值'], 
                          loc='center', cellLoc='center', colColours=['#f2f2f2']*2)
    table.scale(1, 2.5)
    table.set_fontsize(12)

    # --- 右侧：净值与回撤图 ---
    ax_plot = plt.subplot(gs[1])
    ax_drawdown = ax_plot.twinx() # 共用X轴

    # 绘制回撤面积图 (置于底层)
    rolling_max = s_nav.cummax()
    dd = (s_nav - rolling_max) / rolling_max
    ax_drawdown.fill_between(dd.index, dd, 0, color='#1f77b4', alpha=0.2, label='策略回撤 (右轴)')
    ax_drawdown.set_ylim(-1, 0.1) # 限制回撤轴范围
    ax_drawdown.set_ylabel('回撤幅度')

    # 绘制净值曲线 (置于顶层)
    ax_plot.plot(s_nav, color='#f39c12', linewidth=2, label='策略收益 (左轴)')
    ax_plot.plot(b_nav, color='#c0392b', linewidth=1.5, alpha=0.7, label='基准收益 (左轴)')
    
    ax_plot.set_yscale('log') # 使用对数坐标
    ax_plot.set_title('ETF轮动策略回测报告', fontsize=16, pad=20)
    ax_plot.grid(True, linestyle='--', alpha=0.4)
    ax_plot.set_ylabel('累积净值 (对数坐标)')
    
    # 合并图例
    lines, labels = ax_plot.get_legend_handles_labels()
    lines2, labels2 = ax_drawdown.get_legend_handles_labels()
    ax_plot.legend(lines + lines2, labels + labels2, loc='upper left')

    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    run_pro_backtest()