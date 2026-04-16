"""
批量可视化失败交易信号的股票
为所有有失败信号的股票生成K线图表，标记BS点，区分盈利亏损
"""

import os
import sys
import pandas as pd
import json

# 添加项目根目录
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

import config
from data.fetcher import load_kline_cache
from backtest.visualizer import plot_stock_signals_chart


def get_stock_names():
    """从akshare获取股票名称映射"""
    try:
        import akshare as ak
        # 获取A股股票列表
        stock_list = ak.stock_zh_a_spot_em()
        stock_list['代码'] = stock_list['代码'].astype(str).str.zfill(6)
        name_map = dict(zip(stock_list['代码'], stock_list['名称']))
        print(f"获取股票名称: {len(name_map)} 条")
        return name_map
    except Exception as e:
        print(f"获取股票名称失败: {e}")
        return {}


def generate_failure_signal_charts():
    """生成所有有失败交易信号的股票可视化图表"""

    # 加载回测收益数据
    returns_file = os.path.join(config.REPORTS_DIR, 'backtest_returns_20260415_153228.csv')
    returns_df = pd.read_csv(returns_file, dtype={'股票代码': str})

    # 加载信号数据
    signals_file = os.path.join(config.REPORTS_DIR, 'signals_20260415_153228.json')
    with open(signals_file, 'r', encoding='utf-8') as f:
        signals = json.load(f)

    # 获取股票名称
    stock_names = get_stock_names()

    # 找出30日亏损的信号
    losers = returns_df[returns_df['30日收益%'] < 0]

    # 统计有失败信号的股票
    loser_codes = losers['股票代码'].unique()
    loser_codes = [str(c).zfill(6) for c in loser_codes]

    print("=" * 60)
    print("批量可视化失败交易信号的股票")
    print("=" * 60)
    print(f"30日亏损信号: {len(losers)}个")
    print(f"涉及股票: {len(loser_codes)}只")
    print("=" * 60)

    # 构建信号字典（按股票代码分组）
    signals_by_code = {}
    for s in signals:
        code = s['code']
        if code not in signals_by_code:
            signals_by_code[code] = []
        # 补充名称
        if not s.get('name'):
            s['name'] = stock_names.get(code, '')
        signals_by_code[code].append(s)

    # 批量生成图表
    success_count = 0
    fail_count = 0

    print("\n开始生成图表...")
    for code in sorted(loser_codes):
        # 获取该股票名称
        stock_name = stock_names.get(code, '')

        # 获取该股票的所有信号
        stock_signals = signals_by_code.get(code, [])

        if not stock_signals:
            continue

        # 加载K线数据
        kline = load_kline_cache(code)

        if kline.empty:
            print(f"  {code} {stock_name}: K线数据为空，跳过")
            fail_count += 1
            continue

        try:
            chart_path = plot_stock_signals_chart(kline, code, stock_name, stock_signals, returns_df)
            if chart_path:
                success_count += 1
            else:
                fail_count += 1
        except Exception as e:
            print(f"  {code} {stock_name}: 可视化失败 - {e}")
            fail_count += 1

    print("\n" + "=" * 60)
    print("生成完成!")
    print("=" * 60)
    print(f"成功: {success_count} 张")
    print(f"失败: {fail_count} 张")
    print(f"图表目录: {config.CHARTS_DIR}")
    print("=" * 60)


if __name__ == '__main__':
    generate_failure_signal_charts()