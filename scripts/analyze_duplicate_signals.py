"""
分析同一股票持有期内重复信号的问题
"""

import os
import sys
import pandas as pd
import json
from datetime import datetime, timedelta

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# 加载回测数据
returns_file = '/Users/dyechar/Documents/MEGA/AICode/violent_strategy/output/reports/backtest_returns_20260415_153228.csv'
returns_df = pd.read_csv(returns_file, dtype={'股票代码': str})

signals_file = '/Users/dyechar/Documents/MEGA/AICode/violent_strategy/output/reports/signals_20260415_153228.json'
with open(signals_file, 'r') as f:
    signals = json.load(f)

holding_period = 30

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

print('='*70)
print('同一股票持有期内重复信号分析')
print('='*70)
print(f'总信号数: {len(signals)}')
print(f'有效信号: {total_valid}')
print(f'无效信号（持有期内重复）: {total_invalid}')
print(f'无效信号比例: {total_invalid/len(signals)*100:.1f}%')

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

valid_returns = []
invalid_returns = []

for _, row in returns_df.iterrows():
    code = str(row['股票代码']).zfill(6)
    date = row['信号日期']
    ret_30 = row['30日收益%']

    key = f"{code}_{date}"
    if key in valid_keys:
        valid_returns.append(ret_30)
    elif key in invalid_keys:
        invalid_returns.append(ret_30)

print()
print('='*70)
print('有效信号 vs 无效信号 收益对比')
print('='*70)

if valid_returns:
    valid_df = pd.Series(valid_returns)
    wins = len(valid_df[valid_df > 0])
    print(f'有效信号: {len(valid_returns)}个')
    print(f'  平均收益: {valid_df.mean():.2f}%')
    print(f'  胜率: {wins/len(valid_returns)*100:.1f}%')
    print(f'  最大盈利: {valid_df.max():.2f}%')
    print(f'  最大亏损: {valid_df.min():.2f}%')

if invalid_returns:
    invalid_df = pd.Series(invalid_returns)
    wins = len(invalid_df[invalid_df > 0])
    print(f'\n无效信号: {len(invalid_returns)}个')
    print(f'  平均收益: {invalid_df.mean():.2f}%')
    print(f'  胜率: {wins/len(invalid_returns)*100:.1f}%')
    print(f'  最大盈利: {invalid_df.max():.2f}%')
    print(f'  最大亏损: {invalid_df.min():.2f}%')

# t检验
if len(valid_returns) > 10 and len(invalid_returns) > 10:
    from scipy import stats
    t_stat, p_value = stats.ttest_ind(valid_returns, invalid_returns)
    print()
    print(f't检验: t={t_stat:.3f}, p={p_value:.3f}')

print()
print('='*70)
print('建议')
print('='*70)
print('1. 实际交易中，同一股票持有期内不应重复买入')
print('2. 回测应过滤掉无效信号，才是真实可执行的收益')
print(f'3. 过滤后有效信号从232个减少到{total_valid}个')