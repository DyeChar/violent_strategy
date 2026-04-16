"""
追加信号策略分析
"""

import sys
import pandas as pd
import json

sys.path.insert(0, '/Users/dyechar/Documents/MEGA/AICode/violent_strategy')

# 加载回测数据
returns_df = pd.read_csv('/Users/dyechar/Documents/MEGA/AICode/violent_strategy/output/reports/backtest_returns_20260415_153228.csv', dtype={'股票代码': str})
signals_file = '/Users/dyechar/Documents/MEGA/AICode/violent_strategy/output/reports/signals_20260415_153228.json'
with open(signals_file, 'r') as f:
    signals = json.load(f)

print('='*80)
print('追加信号策略效果分析')
print('='*80)

# 按股票分组
stock_signals = {}
for s in signals:
    code = s['code']
    date = s['signal_date']
    if code not in stock_signals:
        stock_signals[code] = []
    stock_signals[code].append({
        'date': date,
        'close': s.get('close', 0),
        'fractal_low': s.get('fractal_low', 0)
    })

# 分类：单次信号 vs 多次信号（有追加）
single_signals_stocks = []  # 只有1次信号
multi_signals_stocks = []   # 有追加信号

for code, sigs in stock_signals.items():
    dates_sorted = sorted(sigs, key=lambda x: x['date'])
    if len(dates_sorted) == 1:
        single_signals_stocks.append(code)
    else:
        # 检查是否在30天内有多信号
        first_date = pd.to_datetime(dates_sorted[0]['date'])
        has_multi = False
        for sig in dates_sorted[1:]:
            second_date = pd.to_datetime(sig['date'])
            if (second_date - first_date).days < 30:
                has_multi = True
                break
        if has_multi:
            multi_signals_stocks.append(code)
        else:
            single_signals_stocks.append(code)

returns_df['股票代码'] = returns_df['股票代码'].astype(str).str.zfill(6)

# 获取每个股票的首次信号日期
first_signal_dates = {}
for code, sigs in stock_signals.items():
    dates_sorted = sorted([s['date'] for s in sigs])
    first_signal_dates[code] = dates_sorted[0]

# 统计单次信号 vs 多次信号的收益
single_returns = []
multi_returns = []

for _, row in returns_df.iterrows():
    code = str(row['股票代码']).zfill(6)
    date = row['信号日期']

    # 只取首次信号
    if code in first_signal_dates and date == first_signal_dates[code]:
        if code in single_signals_stocks:
            single_returns.append(row['30日收益%'])
        elif code in multi_signals_stocks:
            multi_returns.append(row['30日收益%'])

print()
print('单次信号 vs 多次信号股票（首次买入收益对比）')
print('-'*80)

if single_returns:
    single_df = pd.Series(single_returns)
    wins = len(single_df[single_df > 0])
    print(f'单次信号股票: {len(single_returns)}只')
    print(f'  平均收益: {single_df.mean():.2f}%')
    print(f'  胜率: {wins/len(single_returns)*100:.1f}%')

if multi_returns:
    multi_df = pd.Series(multi_returns)
    wins = len(multi_df[multi_df > 0])
    print(f'\n多次信号股票（有追加）: {len(multi_returns)}只')
    print(f'  平均收益: {multi_df.mean():.2f}%')
    print(f'  胜率: {wins/len(multi_returns)*100:.1f}%')

# 追加信号的收益（持有期内第二次信号）
holding_period = 30
valid_dates = {}
invalid_dates = {}

for code, sigs in stock_signals.items():
    dates_sorted = sorted([s['date'] for s in sigs])
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
                invalid_dates[code].append(date)  # 追加信号
            else:
                valid_dates[code].append(date)
                last_buy_date = current_date

# 构建标记
valid_keys = set()
invalid_keys = set()
for code, dates in valid_dates.items():
    for d in dates:
        valid_keys.add(f'{code}_{d}')
for code, dates in invalid_dates.items():
    for d in dates:
        invalid_keys.add(f'{code}_{d}')

# 追加信号的收益统计
addon_returns = []
for _, row in returns_df.iterrows():
    code = str(row['股票代码']).zfill(6)
    date = row['信号日期']
    key = f'{code}_{date}'
    if key in invalid_keys:
        addon_returns.append(row['30日收益%'])

print()
print('='*80)
print('追加信号策略效果')
print('='*80)

if addon_returns:
    addon_df = pd.Series(addon_returns)
    wins = len(addon_df[addon_df > 0])
    print(f'追加信号数量: {len(addon_returns)}个')
    print(f'  平均收益: {addon_df.mean():.2f}%')
    print(f'  胜率: {wins/len(addon_returns)*100:.1f}%')
    print(f'  最大盈利: {addon_df.max():.2f}%')
    print(f'  最大亏损: {addon_df.min():.2f}%')

# 模拟加仓策略效果
print()
print('='*80)
print('加仓策略模拟')
print('='*80)

# 假设首次买入1份，追加信号加仓0.5份
simulated_returns = []

for code in multi_signals_stocks:
    sigs = stock_signals[code]
    dates_sorted = sorted(sigs, key=lambda x: x['date'])

    # 找首次信号和追加信号的收益
    first_date = dates_sorted[0]['date']
    first_return = returns_df[(returns_df['股票代码'].astype(str).str.zfill(6) == code) & (returns_df['信号日期'] == first_date)]['30日收益%'].values
    first_ret = first_return[0] if len(first_return) > 0 else 0

    # 追加信号收益（取最早的追加）
    addon_dates = invalid_dates.get(code, [])
    if addon_dates:
        addon_date = addon_dates[0]
        addon_return = returns_df[(returns_df['股票代码'].astype(str).str.zfill(6) == code) & (returns_df['信号日期'] == addon_date)]['30日收益%'].values
        addon_ret = addon_return[0] if len(addon_return) > 0 else 0

        # 计算加仓后的综合收益（首次1份 + 追加0.5份）
        # 追加信号买入时，首次持仓已经有N天收益了
        days_diff = (pd.to_datetime(addon_date) - pd.to_datetime(first_date)).days

        # 首次持仓N天的收益 = 用对应持有期的收益
        first_n_days_col = f'{days_diff}日收益%'
        first_n_days_return = returns_df[(returns_df['股票代码'].astype(str).str.zfill(6) == code) & (returns_df['信号日期'] == first_date)][first_n_days_col].values
        first_n_ret = first_n_days_return[0] if len(first_n_days_return) > 0 else first_ret

        # 加仓后30天的综合收益
        # 首次仓位：(first_ret + 30天追加涨幅) * 1份
        # 追加仓位：addon_ret * 0.5份

        # 简化：假设加仓0.5份，30天后综合收益
        combined = (first_ret * 1 + addon_ret * 0.5) / 1.5
        simulated_returns.append(combined)

if simulated_returns:
    sim_df = pd.Series(simulated_returns)
    wins = len(sim_df[sim_df > 0])
    print(f'加仓策略股票数: {len(simulated_returns)}只')
    print(f'  平均收益: {sim_df.mean():.2f}%')
    print(f'  胜率: {wins/len(simulated_returns)*100:.1f}%')

print()
print('='*80)
print('策略建议')
print('='*80)
print('【追加信号 ≠ 无效信号，而是强势确认信号】')
print()
print('建议策略:')
print('  1. 追加信号发生时 → 加仓（0.5份或1份）')
print('  2. 追加信号发生时 → 放宽止损（强势确认）')
print('  3. 追加信号发生时 → 延长持有期')
print('  4. 有追加信号的股票优先关注（强势股特征）')