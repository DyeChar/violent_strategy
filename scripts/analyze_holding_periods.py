"""
分析不同持有期下有效信号vs无效信号的收益对比
"""

import os
import sys
import pandas as pd
import json
from datetime import datetime, timedelta

project_root = '/Users/dyechar/Documents/MEGA/AICode/violent_strategy'
sys.path.insert(0, project_root)

# 加载回测数据
returns_file = '/Users/dyechar/Documents/MEGA/AICode/violent_strategy/output/reports/backtest_returns_20260415_153228.csv'
returns_df = pd.read_csv(returns_file, dtype={'股票代码': str})

signals_file = '/Users/dyechar/Documents/MEGA/AICode/violent_strategy/output/reports/signals_20260415_153228.json'
with open(signals_file, 'r') as f:
    signals = json.load(f)

# 定义不同持有期
holding_periods = [1, 2, 3, 5, 7, 14, 20, 30]

print('=' * 80)
print('不同持有期下：有效信号 vs 无效信号 收益对比')
print('=' * 80)

results = []

for holding_period in holding_periods:
    # 按股票分组
    stock_signals = {}
    for s in signals:
        code = s['code']
        date = s['signal_date']
        if code not in stock_signals:
            stock_signals[code] = []
        stock_signals[code].append(date)

    # 分离有效/无效信号
    valid_dates = {}
    invalid_dates = {}

    for code, dates in stock_signals.items():
        dates_sorted = sorted(dates)
        valid_dates[code] = []
        invalid_dates[code] = []

        last_buy_date = None
        for date in dates_sorted:
            if last_buy_date is None:
                valid_dates[code].append(date)
                last_buy_date = pd.to_datetime(date)
            else:
                current_date = pd.to_datetime(date)
                days_diff = (current_date - last_buy_date).days

                if days_diff < holding_period:
                    invalid_dates[code].append(date)
                else:
                    valid_dates[code].append(date)
                    last_buy_date = current_date

    total_valid = sum(len(d) for d in valid_dates.values())
    total_invalid = sum(len(d) for d in invalid_dates.values())

    # 构建有效/无效标记
    valid_keys = set()
    invalid_keys = set()
    for code, dates in valid_dates.items():
        for d in dates:
            valid_keys.add(f"{code}_{d}")
    for code, dates in invalid_dates.items():
        for d in dates:
            invalid_keys.add(f"{code}_{d}")

    # 计算收益对比
    returns_df['股票代码'] = returns_df['股票代码'].astype(str).str.zfill(6)

    ret_col = f'{holding_period}日收益%'

    valid_returns = []
    invalid_returns = []

    for _, row in returns_df.iterrows():
        code = str(row['股票代码']).zfill(6)
        date = row['信号日期']
        ret = row.get(ret_col)

        if pd.isna(ret):
            continue

        key = f"{code}_{date}"
        if key in valid_keys:
            valid_returns.append(ret)
        elif key in invalid_keys:
            invalid_returns.append(ret)

    # 统计
    if valid_returns:
        valid_df = pd.Series(valid_returns)
        valid_avg = valid_df.mean()
        valid_win_rate = len(valid_df[valid_df > 0]) / len(valid_returns) * 100
    else:
        valid_avg = 0
        valid_win_rate = 0

    if invalid_returns:
        invalid_df = pd.Series(invalid_returns)
        invalid_avg = invalid_df.mean()
        invalid_win_rate = len(invalid_df[invalid_df > 0]) / len(invalid_returns) * 100
    else:
        invalid_avg = 0
        invalid_win_rate = 0

    results.append({
        'holding_period': holding_period,
        'total_valid': total_valid,
        'total_invalid': total_invalid,
        'invalid_ratio': total_invalid / (total_valid + total_invalid) * 100 if (total_valid + total_invalid) > 0 else 0,
        'valid_avg_return': valid_avg,
        'valid_win_rate': valid_win_rate,
        'invalid_avg_return': invalid_avg,
        'invalid_win_rate': invalid_win_rate,
        'valid_samples': len(valid_returns),
        'invalid_samples': len(invalid_returns)
    })

# 打印结果表
print()
print(f"{'持有期':^8} | {'无效信号数':^10} | {'无效占比':^10} | {'有效收益':^10} | {'有效胜率':^10} | {'无效收益':^10} | {'无效胜率':^10} | {'收益差':^10}")
print('-' * 90)

for r in results:
    diff = r['invalid_avg_return'] - r['valid_avg_return']
    diff_mark = '★' if diff > 0 else ''
    print(f"{r['holding_period']}日 | {r['total_invalid']:>10} | {r['invalid_ratio']:>10.1f}% | {r['valid_avg_return']:>10.2f}% | {r['valid_win_rate']:>10.1f}% | {r['invalid_avg_return']:>10.2f}% | {r['invalid_win_rate']:>10.1f}% | {diff:>10.2f}%{diff_mark}")

print()
print('=' * 80)
print('结论')
print('=' * 80)

# 找无效信号表现最好的持有期
best_invalid = max(results, key=lambda x: x['invalid_avg_return'])
print(f"无效信号表现最好的持有期: {best_invalid['holding_period']}日，收益{best_invalid['invalid_avg_return']:.2f}%")

# 找无效占比最高的持有期
highest_invalid_ratio = max(results, key=lambda x: x['invalid_ratio'])
print(f"无效信号占比最高的持有期: {highest_invalid_ratio['holding_period']}日，占比{highest_invalid_ratio['invalid_ratio']:.1f}%")

print()
print('核心发现:')
for r in results:
    if r['invalid_avg_return'] > r['valid_avg_return']:
        print(f"  {r['holding_period']}日持有期：无效信号收益更高（{r['invalid_avg_return']:.2f}% vs {r['valid_avg_return']:.2f}%）")