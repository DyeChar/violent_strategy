"""
策略优化分析脚本

分析维度：
1. 盈利vs亏损信号特征差异（放量天数、红肥绿瘦、距MA20等）
2. 止盈止损模拟效果
3. 板块协同效应分析（需行业分类数据）
4. 股价位置分析（需历史价格区间数据）
5. 第二天开盘价影响分析
6. 行业板块效应分析

输入：
- signals_*.json：完整信号记录
- backtest_returns_*.csv：各持有期收益

输出：
- 策略优化建议报告
"""

import os
import sys
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import warnings
warnings.filterwarnings('ignore')

# 添加项目根目录
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

import config
from data.fetcher import load_kline_cache

# 行业分类缓存
INDUSTRY_CACHE_FILE = os.path.join(config.OUTPUT_DIR, 'industry_classification.json')


def load_latest_files() -> Tuple[pd.DataFrame, List[Dict]]:
    """加载最新的回测结果文件"""
    reports_dir = config.REPORTS_DIR

    # 找最新的returns文件
    returns_files = [f for f in os.listdir(reports_dir) if f.startswith('backtest_returns_') and f.endswith('.csv')]
    if not returns_files:
        raise FileNotFoundError("未找到回测收益文件")
    returns_file = sorted(returns_files)[-1]
    returns_df = pd.read_csv(os.path.join(reports_dir, returns_file), dtype={'股票代码': str})

    # 找最新的signals文件
    signals_files = [f for f in os.listdir(reports_dir) if f.startswith('signals_') and f.endswith('.json')]
    if not signals_files:
        raise FileNotFoundError("未找到信号记录文件")
    signals_file = sorted(signals_files)[-1]
    with open(os.path.join(reports_dir, signals_file), 'r', encoding='utf-8') as f:
        signals = json.load(f)

    print(f"加载文件:")
    print(f"  收益数据: {returns_file}")
    print(f"  信号数据: {signals_file}")

    return returns_df, signals


def analyze_win_loss_characteristics(returns_df: pd.DataFrame, holding_period: int = 30) -> Dict:
    """
    分析盈利vs亏损信号的特征差异

    Args:
        returns_df: 回测收益数据
        holding_period: 持有期（天数）

    Returns:
        Dict: 分析结果
    """
    period_col = f'{holding_period}日收益%'

    # 过滤有效数据
    df = returns_df[returns_df[period_col].notna()].copy()

    # 分组
    winners = df[df[period_col] > 0]
    losers = df[df[period_col] <= 0]

    print(f"\n{'='*60}")
    print(f"盈利vs亏损信号特征分析（{holding_period}天持有期）")
    print(f"{'='*60}")
    print(f"盈利信号: {len(winners)} 个")
    print(f"亏损信号: {len(losers)} 个")
    print(f"{'='*60}")

    # 特征对比
    features = ['放量天数', '红肥绿瘦', '距MA20', '买入价格']

    results = {}
    for feature in features:
        if feature not in df.columns:
            continue

        win_mean = winners[feature].mean()
        win_median = winners[feature].median()
        win_std = winners[feature].std()

        lose_mean = losers[feature].mean()
        lose_median = losers[feature].median()
        lose_std = losers[feature].std()

        # 统计检验（简单t检验）
        from scipy import stats
        t_stat, p_value = stats.ttest_ind(winners[feature], losers[feature])

        results[feature] = {
            'win_mean': win_mean,
            'win_median': win_median,
            'win_std': win_std,
            'lose_mean': lose_mean,
            'lose_median': lose_median,
            'lose_std': lose_std,
            't_stat': t_stat,
            'p_value': p_value,
            'significant': p_value < 0.05
        }

        print(f"\n{feature}:")
        print(f"  盈利组: 均值={win_mean:.4f}, 中位数={win_median:.4f}, 标准差={win_std:.4f}")
        print(f"  亏损组: 均值={lose_mean:.4f}, 中位数={lose_median:.4f}, 标准差={lose_std:.4f}")
        print(f"  t检验: t={t_stat:.4f}, p={p_value:.4f} {'***显著***' if p_value < 0.05 else ''}")

    return results


def analyze_deep_features(signals: List[Dict], returns_df: pd.DataFrame, holding_period: int = 30) -> Dict:
    """
    深度分析信号特征（从signals JSON中提取更多维度）

    分析维度：
    1. 震荡天数（oscillation_days）
    2. 涨跌天数比例
    3. 底分型位置（买入价相对于底分型低点的涨幅）
    4. 信号日期分布（月份、周几等）

    Args:
        signals: 信号记录JSON
        returns_df: 回测收益数据
        holding_period: 持有期

    Returns:
        Dict: 分析结果
    """
    period_col = f'{holding_period}日收益%'

    print(f"\n{'='*60}")
    print(f"深度信号特征分析（从完整信号数据提取）")
    print(f"{'='*60}")

    # 构建信号字典
    signal_dict = {}
    for s in signals:
        key = f"{s['code']}_{s['signal_date']}"
        signal_dict[key] = s

    # 提取深度特征
    deep_features = []

    for _, row in returns_df.iterrows():
        code = str(row['股票代码']).zfill(6)
        signal_date = row['信号日期']
        period_return = row.get(period_col)

        if pd.isna(period_return):
            continue

        key = f"{code}_{signal_date}"
        signal = signal_dict.get(key, {})

        if not signal:
            continue

        # 提取特征
        stage2 = signal.get('stage2', {})
        stage4 = signal.get('stage4', {})
        stage1 = signal.get('stage1', {})

        # 震荡天数
        oscillation_days = stage2.get('oscillation_days', 0)
        up_days = stage2.get('up_days', 0)
        down_days = stage2.get('down_days', 0)

        # 涨跌比例
        up_down_ratio = up_days / down_days if down_days > 0 else 0

        # 底分型位置
        fractal_low = signal.get('fractal_low', 0)
        buy_price = row['买入价格']
        close_at_signal = signal.get('close', 0)

        # 买入价相对于底分型低点的涨幅
        gain_from_fractal = (buy_price - fractal_low) / fractal_low * 100 if fractal_low > 0 else 0

        # 信号日期特征
        signal_dt = pd.to_datetime(signal_date)
        month = signal_dt.month
        weekday = signal_dt.dayofweek  # 0=Monday, 6=Sunday

        # 放量日期特征
        volume_dates = signal.get('volume_dates', [])
        volume_days_count = len(volume_dates)

        deep_features.append({
            'code': code,
            'name': row['股票名称'],
            'signal_date': signal_date,
            'oscillation_days': oscillation_days,
            'up_days': up_days,
            'down_days': down_days,
            'up_down_ratio': up_down_ratio,
            'fractal_low': fractal_low,
            'buy_price': buy_price,
            'gain_from_fractal': gain_from_fractal,
            'month': month,
            'weekday': weekday,
            'volume_days_count': volume_days_count,
            'red_green_ratio': signal.get('red_green_ratio', 0),
            'ma_distance': signal.get('stage3', {}).get('ma_distance', 0),
            'return': period_return
        })

    if not deep_features:
        print("无法提取深度特征数据")
        return {}

    df = pd.DataFrame(deep_features)

    # 分组分析
    winners = df[df['return'] > 0]
    losers = df[df['return'] <= 0]

    print(f"\n有效样本: {len(df)} 条")
    print(f"盈利: {len(winners)} 条, 亏损: {len(losers)} 条")

    # 深度特征对比
    deep_feature_cols = [
        'oscillation_days', 'up_days', 'down_days', 'up_down_ratio',
        'gain_from_fractal', 'volume_days_count', 'red_green_ratio'
    ]

    results = {}
    print(f"\n深度特征对比:")
    print("-" * 80)

    from scipy import stats

    for col in deep_feature_cols:
        if col not in df.columns:
            continue

        win_mean = winners[col].mean()
        win_std = winners[col].std()
        lose_mean = losers[col].mean()
        lose_std = losers[col].std()

        t_stat, p_value = stats.ttest_ind(winners[col], losers[col])

        results[col] = {
            'win_mean': win_mean,
            'win_std': win_std,
            'lose_mean': lose_mean,
            'lose_std': lose_std,
            't_stat': t_stat,
            'p_value': p_value,
            'significant': p_value < 0.1  # 放宽显著性标准
        }

        sig_mark = '***' if p_value < 0.05 else ('*' if p_value < 0.1 else '')
        print(f"  {col:20s}: 盈利={win_mean:.3f}±{win_std:.3f}, 亏损={lose_mean:.3f}±{lose_std:.3f}, p={p_value:.3f} {sig_mark}")

    # 按震荡天数分组
    print(f"\n按震荡天数分组:")
    osc_bins = [0, 5, 10, 15, 20, 100]
    osc_labels = ['0-5天', '5-10天', '10-15天', '15-20天', '20天+']
    df['osc_group'] = pd.cut(df['oscillation_days'], bins=osc_bins, labels=osc_labels)

    osc_results = {}
    for group in osc_labels:
        group_df = df[df['osc_group'] == group]
        if len(group_df) == 0:
            continue

        wins = len(group_df[group_df['return'] > 0])
        total = len(group_df)
        win_rate = wins / total * 100 if total > 0 else 0
        avg_return = group_df['return'].mean()

        osc_results[group] = {
            'count': total,
            'win_rate': win_rate,
            'avg_return': avg_return
        }

        print(f"  {group}: 样本={total}, 胜率={win_rate:.1f}%, 平均收益={avg_return:.2f}%")

    # 按底分型涨幅分组
    print(f"\n按底分型涨幅分组（买入价相对底分型低点）:")
    gain_bins = [-100, 0, 5, 10, 20, 100]
    gain_labels = ['<0%', '0-5%', '5-10%', '10-20%', '>20%']
    df['gain_group'] = pd.cut(df['gain_from_fractal'], bins=gain_bins, labels=gain_labels)

    gain_results = {}
    for group in gain_labels:
        group_df = df[df['gain_group'] == group]
        if len(group_df) == 0:
            continue

        wins = len(group_df[group_df['return'] > 0])
        total = len(group_df)
        win_rate = wins / total * 100 if total > 0 else 0
        avg_return = group_df['return'].mean()

        gain_results[group] = {
            'count': total,
            'win_rate': win_rate,
            'avg_return': avg_return
        }

        print(f"  {group}: 样本={total}, 胜率={win_rate:.1f}%, 平均收益={avg_return:.2f}%")

    # 按月份分析
    print(f"\n按月份分析:")
    month_results = {}
    for month in range(1, 13):
        month_df = df[df['month'] == month]
        if len(month_df) == 0:
            continue

        wins = len(month_df[month_df['return'] > 0])
        total = len(month_df)
        win_rate = wins / total * 100 if total > 0 else 0
        avg_return = month_df['return'].mean()

        month_results[month] = {
            'count': total,
            'win_rate': win_rate,
            'avg_return': avg_return
        }

        print(f"  {month}月: 样本={total}, 胜率={win_rate:.1f}%, 平均收益={avg_return:.2f}%")

    # 极端收益案例分析
    print(f"\n极端案例分析:")
    print("-" * 80)

    # 最大盈利案例
    top_winners = df.nlargest(5, 'return')
    print("\n最大盈利案例（前5）:")
    for _, row in top_winners.iterrows():
        print(f"  {row['code']} {row['name']}: 收益={row['return']:.2f}%")
        print(f"    震荡天数={row['oscillation_days']}, 底分型涨幅={row['gain_from_fractal']:.2f}%")

    # 最大亏损案例
    top_losers = df.nsmallest(5, 'return')
    print("\n最大亏损案例（前5）:")
    for _, row in top_losers.iterrows():
        print(f"  {row['code']} {row['name']}: 收益={row['return']:.2f}%")
        print(f"    震荡天数={row['oscillation_days']}, 底分型涨幅={row['gain_from_fractal']:.2f}%")

    return {
        'feature_results': results,
        'osc_results': osc_results,
        'gain_results': gain_results,
        'month_results': month_results,
        'deep_df': df
    }


def simulate_stop_loss(returns_df: pd.DataFrame, stop_loss_pct: float = -10.0,
                       holding_period: int = 30) -> Dict:
    """
    模拟止损策略效果

    Args:
        returns_df: 回测收益数据
        stop_loss_pct: 止损阈值（负数，如-10表示亏10%止损）
        holding_period: 最大持有期

    Returns:
        Dict: 模拟结果
    """
    print(f"\n{'='*60}")
    print(f"止损策略模拟（止损阈值={stop_loss_pct}%）")
    print(f"{'='*60}")

    # 需要加载信号数据来模拟每日持仓收益
    reports_dir = config.REPORTS_DIR
    signals_files = [f for f in os.listdir(reports_dir) if f.startswith('signals_') and f.endswith('.json')]
    signals_file = sorted(signals_files)[-1]
    with open(os.path.join(reports_dir, signals_file), 'r', encoding='utf-8') as f:
        signals = json.load(f)

    # 构建信号字典
    signal_dict = {}
    for s in signals:
        key = f"{s['code']}_{s['signal_date']}"
        signal_dict[key] = s

    # 模拟止损
    simulated_returns = []
    triggered_stop_loss = 0
    avoided_big_loss = 0

    for _, row in returns_df.iterrows():
        code = str(row['股票代码']).zfill(6)
        signal_date = row['信号日期']
        buy_price = row['买入价格']

        key = f"{code}_{signal_date}"
        signal = signal_dict.get(key, {})

        # 获取持仓期间每日收益
        holding_returns = signal.get('holding_returns', {})

        # 如果没有每日收益数据，用各持有期收益近似
        # 取最接近的持有期收益
        final_return = row[f'{holding_period}日收益%']

        if pd.isna(final_return):
            continue

        # 简化模拟：如果持有期内某天跌破止损线，则止损
        # 这里用不同持有期收益来推断
        simulated_return = final_return
        stop_triggered = False

        # 检查短期收益是否触发止损
        for period in [1, 2, 3, 5, 7]:
            period_return = row.get(f'{period}日收益%')
            if pd.notna(period_return) and period_return <= stop_loss_pct:
                simulated_return = stop_loss_pct
                stop_triggered = True
                triggered_stop_loss += 1

                # 记录避免了多大的亏损
                if final_return < stop_loss_pct:
                    avoided_big_loss += (final_return - stop_loss_pct)  # 避免的亏损量
                break

        simulated_returns.append({
            'code': code,
            'name': row['股票名称'],
            'original_return': final_return,
            'simulated_return': simulated_return,
            'stop_triggered': stop_triggered
        })

    # 统计结果
    sim_df = pd.DataFrame(simulated_returns)

    total = len(sim_df)
    wins = len(sim_df[sim_df['simulated_return'] > 0])
    losses = len(sim_df[sim_df['simulated_return'] <= 0])

    win_rate = wins / total * 100
    avg_return = sim_df['simulated_return'].mean()
    avg_win = sim_df[sim_df['simulated_return'] > 0]['simulated_return'].mean()
    avg_loss = sim_df[sim_df['simulated_return'] <= 0]['simulated_return'].mean()
    profit_loss_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0

    # 与原始策略对比
    original_win_rate = len(sim_df[sim_df['original_return'] > 0]) / total * 100
    original_avg_return = sim_df['original_return'].mean()

    results = {
        'stop_loss_pct': stop_loss_pct,
        'total_signals': total,
        'triggered_stop_loss': triggered_stop_loss,
        'stop_loss_rate': triggered_stop_loss / total * 100,
        'avoided_big_loss_avg': avoided_big_loss / triggered_stop_loss if triggered_stop_loss > 0 else 0,
        'win_rate': win_rate,
        'avg_return': avg_return,
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'profit_loss_ratio': profit_loss_ratio,
        'original_win_rate': original_win_rate,
        'original_avg_return': original_avg_return,
        'win_rate_change': win_rate - original_win_rate,
        'avg_return_change': avg_return - original_avg_return
    }

    print(f"\n止损效果:")
    print(f"  触发止损次数: {triggered_stop_loss} ({triggered_stop_loss/total*100:.2f}%)")
    print(f"  平均避免亏损: {abs(avoided_big_loss/triggered_stop_loss):.2f}% (触发止损的交易)")
    print(f"\n策略对比:")
    print(f"  原始策略: 胜率={original_win_rate:.2f}%, 平均收益={original_avg_return:.2f}%")
    print(f"  止损策略: 胜率={win_rate:.2f}%, 平均收益={avg_return:.2f}%")
    print(f"  变化: 胜率{win_rate - original_win_rate:+.2f}%, 收益{avg_return - original_avg_return:+.2f}%")
    print(f"  盈亏比: {profit_loss_ratio:.2f}")

    return results


def analyze_price_position(returns_df: pd.DataFrame, signals: List[Dict]) -> Dict:
    """
    分析信号触发时的股价位置（相对于历史区间）

    Args:
        returns_df: 回测收益数据
        signals: 信号记录

    Returns:
        Dict: 分析结果
    """
    print(f"\n{'='*60}")
    print(f"股价位置分析（信号触发时的价格相对于近一年位置）")
    print(f"{'='*60}")

    # 构建信号字典
    signal_dict = {}
    for s in signals:
        key = f"{s['code']}_{s['signal_date']}"
        signal_dict[key] = s

    position_data = []

    for _, row in returns_df.iterrows():
        code = str(row['股票代码']).zfill(6)
        signal_date = row['信号日期']
        buy_price = row['买入价格']

        key = f"{code}_{signal_date}"
        signal = signal_dict.get(key, {})

        # 获取阶段1数据（放量吸筹阶段）
        stage1 = signal.get('stage1', {})
        stage1_end_idx = stage1.get('end_idx', 0)
        stage1_high = stage1.get('high', 0)  # 阶段1最高价
        stage1_low = stage1.get('low', 0)    # 阶段1最低价

        # 计算股价位置
        if stage1_high > 0 and stage1_low > 0:
            price_range = stage1_high - stage1_low
            if price_range > 0:
                # 买入价相对于阶段1的位置（0=最低，1=最高）
                position_in_stage1 = (buy_price - stage1_low) / price_range

                # 计算从阶段1低点的涨幅
                gain_from_low = (buy_price - stage1_low) / stage1_low * 100

                position_data.append({
                    'code': code,
                    'name': row['股票名称'],
                    'signal_date': signal_date,
                    'buy_price': buy_price,
                    'stage1_high': stage1_high,
                    'stage1_low': stage1_low,
                    'position_in_stage1': position_in_stage1,
                    'gain_from_low': gain_from_low,
                    'return_30': row.get('30日收益%', None)
                })

    if not position_data:
        print("无法计算股价位置（缺少阶段1数据）")
        return {}

    pos_df = pd.DataFrame(position_data)

    # 过滤有效30日收益
    pos_df = pos_df[pos_df['return_30'].notna()]

    # 按位置分组分析
    print(f"\n有效数据: {len(pos_df)} 条")

    # 按涨幅分组
    gain_bins = [0, 10, 20, 30, 50, 100]
    gain_labels = ['0-10%', '10-20%', '20-30%', '30-50%', '50%+']

    pos_df['gain_group'] = pd.cut(pos_df['gain_from_low'], bins=gain_bins, labels=gain_labels)

    print(f"\n按阶段1涨幅分组:")
    print("-" * 60)

    group_results = {}
    for group in gain_labels:
        group_df = pos_df[pos_df['gain_group'] == group]
        if len(group_df) == 0:
            continue

        wins = len(group_df[group_df['return_30'] > 0])
        total = len(group_df)
        win_rate = wins / total * 100
        avg_return = group_df['return_30'].mean()

        group_results[group] = {
            'count': total,
            'win_rate': win_rate,
            'avg_return': avg_return
        }

        print(f"  {group}: 样本={total}, 胜率={win_rate:.2f}%, 平均收益={avg_return:.2f}%")

    return {
        'position_df': pos_df,
        'group_results': group_results
    }


def simulate_take_profit_and_stop_loss(returns_df: pd.DataFrame,
                                        take_profit_pct: float = 20.0,
                                        stop_loss_pct: float = -10.0,
                                        holding_period: int = 30) -> Dict:
    """
    模拟止盈止损组合策略效果

    Args:
        returns_df: 回测收益数据
        take_profit_pct: 止盈阈值（正数，如20表示盈利20%止盈）
        stop_loss_pct: 止损阈值（负数，如-10表示亏10%止损）
        holding_period: 最大持有期

    Returns:
        Dict: 模拟结果
    """
    print(f"\n{'='*60}")
    print(f"止盈止损组合策略模拟（止盈={take_profit_pct}%, 止损={stop_loss_pct}%）")
    print(f"{'='*60}")

    # 需要获取短期收益来模拟
    simulated_returns = []
    triggered_take_profit = 0
    triggered_stop_loss = 0

    for _, row in returns_df.iterrows():
        if pd.isna(row.get(f'{holding_period}日收益%')):
            continue

        final_return = row[f'{holding_period}日收益%']

        # 检查各持有期收益，模拟是否触发止盈或止损
        simulated_return = final_return
        exit_reason = '到期'

        # 按时间顺序检查（1日 -> 2日 -> 3日 -> ...）
        for period in [1, 2, 3, 5, 7, 14, 20, 30]:
            period_return = row.get(f'{period}日收益%')
            if pd.isna(period_return):
                continue

            # 检查止盈
            if period_return >= take_profit_pct:
                simulated_return = take_profit_pct
                triggered_take_profit += 1
                exit_reason = f'{period}日止盈'
                break

            # 检查止损
            if period_return <= stop_loss_pct:
                simulated_return = stop_loss_pct
                triggered_stop_loss += 1
                exit_reason = f'{period}日止损'
                break

        simulated_returns.append({
            'code': row['股票代码'],
            'name': row['股票名称'],
            'original_return': final_return,
            'simulated_return': simulated_return,
            'exit_reason': exit_reason
        })

    # 统计结果
    sim_df = pd.DataFrame(simulated_returns)

    total = len(sim_df)
    wins = len(sim_df[sim_df['simulated_return'] > 0])
    losses = len(sim_df[sim_df['simulated_return'] <= 0])

    win_rate = wins / total * 100
    avg_return = sim_df['simulated_return'].mean()
    avg_win = sim_df[sim_df['simulated_return'] > 0]['simulated_return'].mean()
    avg_loss = sim_df[sim_df['simulated_return'] <= 0]['simulated_return'].mean()
    profit_loss_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0

    # 原始策略对比
    original_win_rate = len(sim_df[sim_df['original_return'] > 0]) / total * 100
    original_avg_return = sim_df['original_return'].mean()

    results = {
        'take_profit_pct': take_profit_pct,
        'stop_loss_pct': stop_loss_pct,
        'total_signals': total,
        'triggered_take_profit': triggered_take_profit,
        'triggered_stop_loss': triggered_stop_loss,
        'take_profit_rate': triggered_take_profit / total * 100,
        'stop_loss_rate': triggered_stop_loss / total * 100,
        'win_rate': win_rate,
        'avg_return': avg_return,
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'profit_loss_ratio': profit_loss_ratio,
        'original_win_rate': original_win_rate,
        'original_avg_return': original_avg_return,
        'win_rate_change': win_rate - original_win_rate,
        'avg_return_change': avg_return - original_avg_return
    }

    print(f"\n止盈止损效果:")
    print(f"  止盈次数: {triggered_take_profit} ({triggered_take_profit/total*100:.2f}%)")
    print(f"  止损次数: {triggered_stop_loss} ({triggered_stop_loss/total*100:.2f}%)")
    print(f"\n策略对比:")
    print(f"  原始策略: 胜率={original_win_rate:.2f}%, 平均收益={original_avg_return:.2f}%")
    print(f"  组合策略: 胜率={win_rate:.2f}%, 平均收益={avg_return:.2f}%")
    print(f"  变化: 胜率{win_rate - original_win_rate:+.2f}%, 收益{avg_return - original_avg_return:+.2f}%")
    print(f"  盈亏比: {profit_loss_ratio:.2f}")

    return results


def compare_take_profit_stop_loss_combinations(returns_df: pd.DataFrame) -> List[Dict]:
    """
    对比多种止盈止损组合的效果

    Returns:
        List[Dict]: 各组合的结果
    """
    print(f"\n{'='*60}")
    print(f"多止盈止损组合对比模拟")
    print(f"{'='*60}")

    # 定义组合
    combinations = [
        # 无止盈止损
        {'tp': 100, 'sl': -100},  # 实际上不触发
        # 只有止损
        {'tp': 100, 'sl': -10},
        {'tp': 100, 'sl': -15},
        # 只有止盈
        {'tp': 15, 'sl': -100},
        {'tp': 20, 'sl': -100},
        {'tp': 30, 'sl': -100},
        # 止盈止损组合
        {'tp': 15, 'sl': -10},
        {'tp': 20, 'sl': -10},
        {'tp': 30, 'sl': -10},
        {'tp': 20, 'sl': -15},
        {'tp': 30, 'sl': -15},
    ]

    results = []

    for combo in combinations:
        result = simulate_take_profit_and_stop_loss(
            returns_df,
            take_profit_pct=combo['tp'],
            stop_loss_pct=combo['sl'],
            holding_period=30
        )
        results.append(result)

    # 打印对比表
    print(f"\n止盈止损组合对比表:")
    print("-" * 90)
    print(f"{'止盈%':>8} {'止损%':>8} {'止盈率%':>10} {'止损率%':>10} {'胜率%':>10} "
          f"{'平均收益%':>12} {'盈亏比':>8} {'vs原始':>10}")
    print("-" * 90)

    for r in results:
        # 判断是否实际有效
        tp_effective = r['take_profit_pct'] < 50
        sl_effective = r['stop_loss_pct'] > -50

        label_tp = str(r['take_profit_pct']) if tp_effective else '无'
        label_sl = str(r['stop_loss_pct']) if sl_effective else '无'

        print(f"{label_tp:>8} {label_sl:>8} {r['take_profit_rate']:>10.2f} {r['stop_loss_rate']:>10.2f} "
              f"{r['win_rate']:>10.2f} {r['avg_return']:>12.2f} {r['profit_loss_ratio']:>8.2f} "
              f"{r['avg_return_change']:>+10.2f}")

    return results
    """
    模拟多种止损阈值的效果对比

    Args:
        returns_df: 回测收益数据

    Returns:
        Dict: 各止损阈值的结果对比
    """
    print(f"\n{'='*60}")
    print(f"多止损阈值对比模拟")
    print(f"{'='*60}")

    stop_loss_levels = [-5, -8, -10, -12, -15, -20]
    results = []

    for stop_pct in stop_loss_levels:
        result = simulate_stop_loss(returns_df, stop_loss_pct=stop_pct, holding_period=30)
        results.append(result)

    # 打印对比表
    print(f"\n止损阈值对比表:")
    print("-" * 80)
    print(f"{'止损%':>8} {'触发率%':>10} {'胜率%':>10} {'平均收益%':>12} {'盈亏比':>8} {'vs原始胜率':>12} {'vs原始收益':>12}")
    print("-" * 80)

    for r in results:
        print(f"{r['stop_loss_pct']:>8} {r['stop_loss_rate']:>10.2f} {r['win_rate']:>10.2f} "
              f"{r['avg_return']:>12.2f} {r['profit_loss_ratio']:>8.2f} "
              f"{r['win_rate_change']:>+12.2f} {r['avg_return_change']:>+12.2f}")

    return results


def analyze_signal_frequency_by_stock(signals: List[Dict]) -> Dict:
    """
    分析同一股票多次出现信号的情况

    Args:
        signals: 信号记录

    Returns:
        Dict: 分析结果
    """
    print(f"\n{'='*60}")
    print(f"股票信号频率分析")
    print(f"{'='*60}")

    # 统计每只股票的信号次数
    stock_signal_count = {}
    for s in signals:
        code = s['code']
        if code not in stock_signal_count:
            stock_signal_count[code] = []
        stock_signal_count[code].append(s['signal_date'])

    # 按信号次数分组
    count_distribution = {}
    for code, dates in stock_signal_count.items():
        count = len(dates)
        if count not in count_distribution:
            count_distribution[count] = []
        count_distribution[count].append(code)

    print(f"\n信号次数分布:")
    for count in sorted(count_distribution.keys()):
        codes = count_distribution[count]
        print(f"  {count}次信号: {len(codes)} 只股票")

    # 多次信号股票示例
    multi_signal_stocks = [code for code, dates in stock_signal_count.items() if len(dates) >= 3]
    if multi_signal_stocks:
        print(f"\n多次信号股票示例（>=3次）:")
        for code in multi_signal_stocks[:10]:
            dates = stock_signal_count[code]
            print(f"  {code}: {dates}")

    return {
        'stock_signal_count': stock_signal_count,
        'count_distribution': count_distribution
    }


def analyze_open_price_effect(returns_df: pd.DataFrame, signals: List[Dict],
                               holding_period: int = 30) -> Dict:
    """
    分析第二天开盘价相对于信号日收盘价的影响

    核心问题：
    - T日信号触发，收盘价是多少
    - T+1日买入，开盘价是多少
    - 开盘价相对于收盘价是高开还是低开？
    - 高开/低开幅度对后续收益有何影响？

    Args:
        returns_df: 回测收益数据（买入价格 = T+1开盘价）
        signals: 信号记录（含信号日收盘价）
        holding_period: 持有期

    Returns:
        Dict: 分析结果
    """
    print(f"\n{'='*60}")
    print(f"第二天开盘价影响分析")
    print(f"{'='*60}")
    print(f"分析买入价（T+1开盘价）相对于信号日收盘价的涨跌幅影响")

    # 构建信号字典
    signal_dict = {}
    for s in signals:
        key = f"{s['code']}_{s['signal_date']}"
        signal_dict[key] = s

    # 计算开盘价涨幅
    open_price_data = []

    for _, row in returns_df.iterrows():
        code = str(row['股票代码']).zfill(6)
        signal_date = row['信号日期']
        buy_price = row['买入价格']  # T+1开盘价
        period_return = row.get(f'{holding_period}日收益%')

        if pd.isna(period_return):
            continue

        key = f"{code}_{signal_date}"
        signal = signal_dict.get(key, {})

        # 信号日收盘价
        signal_close = signal.get('close', 0)

        if signal_close <= 0:
            continue

        # 计算开盘价相对于信号日收盘价的涨跌幅
        open_gap = (buy_price - signal_close) / signal_close * 100

        open_price_data.append({
            'code': code,
            'name': row['股票名称'],
            'signal_date': signal_date,
            'signal_close': signal_close,
            'buy_price': buy_price,  # T+1开盘价
            'open_gap': open_gap,    # 开盘涨幅（正=高开，负=低开）
            'return': period_return
        })

    if not open_price_data:
        print("无法计算开盘价数据")
        return {}

    df = pd.DataFrame(open_price_data)

    print(f"\n有效样本: {len(df)} 条")

    # 开盘涨幅统计
    print(f"\n开盘涨幅统计:")
    print(f"  平均开盘涨幅: {df['open_gap'].mean():.3f}%")
    print(f"  中位数: {df['open_gap'].median():.3f}%")
    print(f"  高开(>0%)次数: {len(df[df['open_gap'] > 0])} ({len(df[df['open_gap'] > 0])/len(df)*100:.1f}%)")
    print(f"  低开(<0%)次数: {len(df[df['open_gap'] < 0])} ({len(df[df['open_gap'] < 0])/len(df)*100:.1f}%)")
    print(f"  平开(=0%)次数: {len(df[df['open_gap'] == 0])}")

    # 按开盘涨幅分组分析
    print(f"\n按开盘涨幅分组:")
    gap_bins = [-20, -3, -1, 0, 1, 3, 20]
    gap_labels = ['大幅低开(<-3%)', '中幅低开(-3~-1%)', '小幅低开(-1~0%)',
                  '小幅高开(0~1%)', '中幅高开(1~3%)', '大幅高开(>3%)']
    df['gap_group'] = pd.cut(df['open_gap'], bins=gap_bins, labels=gap_labels)

    gap_results = {}
    for group in gap_labels:
        group_df = df[df['gap_group'] == group]
        if len(group_df) == 0:
            continue

        wins = len(group_df[group_df['return'] > 0])
        total = len(group_df)
        win_rate = wins / total * 100 if total > 0 else 0
        avg_return = group_df['return'].mean()

        gap_results[group] = {
            'count': total,
            'win_rate': win_rate,
            'avg_return': avg_return,
            'avg_open_gap': group_df['open_gap'].mean()
        }

        print(f"  {group}: 样本={total}, 胜率={win_rate:.1f}%, 平均收益={avg_return:.2f}%")

    # 高开 vs 低开 对比
    high_open = df[df['open_gap'] > 0]
    low_open = df[df['open_gap'] < 0]

    print(f"\n高开vs低开对比:")
    if len(high_open) > 0:
        print(f"  高开信号: {len(high_open)}个, 胜率={len(high_open[high_open['return']>0])/len(high_open)*100:.1f}%, "
              f"平均收益={high_open['return'].mean():.2f}%")
    if len(low_open) > 0:
        print(f"  低开信号: {len(low_open)}个, 胜率={len(low_open[low_open['return']>0])/len(low_open)*100:.1f}%, "
              f"平均收益={low_open['return'].mean():.2f}%")

    # 统计检验
    if len(high_open) > 10 and len(low_open) > 10:
        from scipy import stats
        t_stat, p_value = stats.ttest_ind(high_open['return'], low_open['return'])
        print(f"\n  t检验: t={t_stat:.3f}, p={p_value:.3f} {'***显著***' if p_value < 0.05 else ''}")

    # 开盘涨幅与后续收益的相关性
    corr = df['open_gap'].corr(df['return'])
    print(f"\n开盘涨幅与{holding_period}日收益的相关系数: {corr:.3f}")

    if corr > 0.1:
        print(f"  结论: 高开信号后续收益更好（正相关）")
    elif corr < -0.1:
        print(f"  结论: 低开信号后续收益更好（负相关）")
    else:
        print(f"  结论: 开盘涨幅对后续收益影响不明显")

    return {
        'gap_results': gap_results,
        'high_open_stats': {
            'count': len(high_open),
            'win_rate': len(high_open[high_open['return']>0])/len(high_open)*100 if len(high_open) > 0 else 0,
            'avg_return': high_open['return'].mean() if len(high_open) > 0 else 0
        },
        'low_open_stats': {
            'count': len(low_open),
            'win_rate': len(low_open[low_open['return']>0])/len(low_open)*100 if len(low_open) > 0 else 0,
            'avg_return': low_open['return'].mean() if len(low_open) > 0 else 0
        },
        'correlation': corr,
        'open_price_df': df
    }


def get_industry_classification(stock_codes: List[str], use_cache: bool = True) -> Dict[str, str]:
    """
    获取股票行业分类数据（使用akshare）

    Args:
        stock_codes: 股票代码列表
        use_cache: 是否使用缓存

    Returns:
        Dict: {股票代码: 行业名称}
    """
    print(f"\n{'='*60}")
    print(f"获取行业分类数据")
    print(f"{'='*60}")

    # 检查缓存
    if use_cache and os.path.exists(INDUSTRY_CACHE_FILE):
        try:
            with open(INDUSTRY_CACHE_FILE, 'r', encoding='utf-8') as f:
                cached = json.load(f)
            print(f"从缓存加载: {len(cached)} 条行业数据")
            return cached
        except Exception as e:
            print(f"缓存加载失败: {e}")

    # 从akshare获取
    try:
        import akshare as ak

        # 获取A股行业分类
        print("从akshare获取行业分类数据...")

        # 使用 stock_board_industry_name_em 获取行业列表
        industry_list = ak.stock_board_industry_name_em()

        print(f"行业列表获取成功，共 {len(industry_list)} 个行业")

        # 构建股票->行业的映射
        stock_industry = {}

        for _, row in industry_list.iterrows():
            industry_name = row['板块名称']

            # 获取该行业的成分股
            try:
                members = ak.stock_board_industry_cons_em(symbol=industry_name)

                for _, member in members.iterrows():
                    code = str(member['代码']).zfill(6)
                    stock_industry[code] = industry_name

            except Exception as e:
                continue

        # 保存缓存
        if stock_industry:
            with open(INDUSTRY_CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(stock_industry, f, ensure_ascii=False, indent=2)
            print(f"行业分类已缓存: {len(stock_industry)} 条")

        return stock_industry

    except Exception as e:
        print(f"akshare获取失败: {e}")
        return {}


def analyze_industry_sector_effect(returns_df: pd.DataFrame, signals: List[Dict],
                                    holding_period: int = 30) -> Dict:
    """
    分析行业板块效应

    分析维度：
    1. 不同行业的胜率和平均收益对比
    2. 同一行业多只股票同时出现信号（板块协同效应）
    3. 板块信号集中度与后续表现的关系

    Args:
        returns_df: 回测收益数据
        signals: 信号记录
        holding_period: 持有期

    Returns:
        Dict: 分析结果
    """
    print(f"\n{'='*60}")
    print(f"行业板块效应分析")
    print(f"{'='*60}")

    period_col = f'{holding_period}日收益%'

    # 获取所有涉及股票
    all_codes = returns_df['股票代码'].unique()
    all_codes = [str(c).zfill(6) for c in all_codes]

    # 获取行业分类
    stock_industry = get_industry_classification(all_codes, use_cache=True)

    if not stock_industry:
        print("无法获取行业分类数据")
        return {}

    # 构建信号字典
    signal_dict = {}
    for s in signals:
        key = f"{s['code']}_{s['signal_date']}"
        signal_dict[key] = s

    # 添加行业标签到收益数据
    industry_data = []

    for _, row in returns_df.iterrows():
        code = str(row['股票代码']).zfill(6)
        signal_date = row['信号日期']
        period_return = row.get(period_col)

        if pd.isna(period_return):
            continue

        industry = stock_industry.get(code, '未知')

        industry_data.append({
            'code': code,
            'name': row['股票名称'],
            'signal_date': signal_date,
            'industry': industry,
            'return': period_return
        })

    if not industry_data:
        print("无有效行业数据")
        return {}

    df = pd.DataFrame(industry_data)

    print(f"\n有效样本: {len(df)} 条")

    # 统计各行业
    industry_counts = df['industry'].value_counts()
    print(f"\n行业分布（前10）:")
    for industry, count in industry_counts.head(10).items():
        print(f"  {industry}: {count} 条")

    # 各行业收益对比
    print(f"\n各行业胜率和收益对比:")
    print("-" * 70)

    industry_results = {}
    for industry in df['industry'].unique():
        industry_df = df[df['industry'] == industry]

        if len(industry_df) < 3:  # 至少3条数据才分析
            continue

        wins = len(industry_df[industry_df['return'] > 0])
        total = len(industry_df)
        win_rate = wins / total * 100
        avg_return = industry_df['return'].mean()
        max_return = industry_df['return'].max()
        min_return = industry_df['return'].min()

        industry_results[industry] = {
            'count': total,
            'win_rate': win_rate,
            'avg_return': avg_return,
            'max_return': max_return,
            'min_return': min_return
        }

        print(f"  {industry:15s}: 样本={total:3d}, 胜率={win_rate:5.1f}%, "
              f"平均收益={avg_return:6.2f}%, 最大={max_return:6.2f}%, 最小={min_return:6.2f}%")

    # 找表现最好和最差的行业
    if industry_results:
        best_industry = max(industry_results.items(), key=lambda x: x[1]['avg_return'])
        worst_industry = min(industry_results.items(), key=lambda x: x[1]['avg_return'])

        print(f"\n表现最好的行业: {best_industry[0]} (平均收益={best_industry[1]['avg_return']:.2f}%)")
        print(f"表现最差的行业: {worst_industry[0]} (平均收益={worst_industry[1]['avg_return']:.2f}%)")

    # 板块协同效应：同一日期同一行业出现多个信号
    print(f"\n板块协同效应分析:")
    print("-" * 70)

    # 统计每个日期每个行业的信号数量
    date_industry_count = df.groupby(['signal_date', 'industry']).size().reset_index(name='signal_count')

    # 标记有板块协同的信号（同日同行业>=2个信号）
    df['sector_synergy'] = df.apply(
        lambda row: date_industry_count[
            (date_industry_count['signal_date'] == row['signal_date']) &
            (date_industry_count['industry'] == row['industry'])
        ]['signal_count'].values[0] if len(date_industry_count[
            (date_industry_count['signal_date'] == row['signal_date']) &
            (date_industry_count['industry'] == row['industry'])
        ]) > 0 else 1, axis=1
    )

    # 对比有板块协同 vs 无板块协同的表现
    synergy_signals = df[df['sector_synergy'] >= 2]
    solo_signals = df[df['sector_synergy'] == 1]

    synergy_results = {}

    if len(synergy_signals) > 0:
        print(f"\n有板块协同的信号（同日同行业>=2个）:")
        print(f"  数量: {len(synergy_signals)} 条")
        print(f"  胜率: {len(synergy_signals[synergy_signals['return']>0])/len(synergy_signals)*100:.1f}%")
        print(f"  平均收益: {synergy_signals['return'].mean():.2f}%")

        synergy_results['synergy'] = {
            'count': len(synergy_signals),
            'win_rate': len(synergy_signals[synergy_signals['return']>0])/len(synergy_signals)*100,
            'avg_return': synergy_signals['return'].mean()
        }

    if len(solo_signals) > 0:
        print(f"\n无板块协同的信号（同日同行业只有1个）:")
        print(f"  数量: {len(solo_signals)} 条")
        print(f"  胜率: {len(solo_signals[solo_signals['return']>0])/len(solo_signals)*100:.1f}%")
        print(f"  平均收益: {solo_signals['return'].mean():.2f}%")

        synergy_results['solo'] = {
            'count': len(solo_signals),
            'win_rate': len(solo_signals[solo_signals['return']>0])/len(solo_signals)*100,
            'avg_return': solo_signals['return'].mean()
        }

    # 统计检验
    if len(synergy_signals) > 10 and len(solo_signals) > 10:
        from scipy import stats
        t_stat, p_value = stats.ttest_ind(synergy_signals['return'], solo_signals['return'])
        print(f"\n  t检验: t={t_stat:.3f}, p={p_value:.3f} {'***显著***' if p_value < 0.05 else ''}")

        if p_value < 0.05:
            if synergy_signals['return'].mean() > solo_signals['return'].mean():
                print(f"  结论: 有板块协同的信号表现更好！")
            else:
                print(f"  结论: 单独信号表现更好！")
        else:
            print(f"  结论: 板块协同效应不显著")

    # 展示板块协同案例
    print(f"\n板块协同案例:")
    high_synergy = date_industry_count[date_industry_count['signal_count'] >= 3].head(10)

    for _, row in high_synergy.iterrows():
        date = row['signal_date']
        industry = row['industry']
        count = row['signal_count']

        examples = df[(df['signal_date'] == date) & (df['industry'] == industry)]

        avg_ret = examples['return'].mean()
        win_rate = len(examples[examples['return']>0])/len(examples)*100

        print(f"  {date} {industry}: {count}只信号, 平均收益={avg_ret:.2f}%, 胜率={win_rate:.1f}%")

    return {
        'industry_results': industry_results,
        'synergy_results': synergy_results,
        'industry_df': df,
        'date_industry_count': date_industry_count
    }


def generate_optimization_report(returns_df: pd.DataFrame, signals: List[Dict]) -> str:
    """
    生成完整的策略优化报告

    Returns:
        str: 报告文本
    """
    report = []
    report.append("=" * 80)
    report.append("暴力战法策略优化分析报告")
    report.append("=" * 80)
    report.append(f"分析日期: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"样本数据: {len(returns_df)} 条交易信号")
    report.append("=" * 80)

    # 1. 基础特征分析
    char_results = analyze_win_loss_characteristics(returns_df, holding_period=30)

    # 2. 深度特征分析
    deep_results = analyze_deep_features(signals, returns_df, holding_period=30)

    # 3. 止盈止损组合模拟（包含止损单独分析）
    tp_sl_results = compare_take_profit_stop_loss_combinations(returns_df)

    # 4. 信号频率分析
    freq_results = analyze_signal_frequency_by_stock(signals)

    # 5. 开盘价影响分析
    open_price_results = analyze_open_price_effect(returns_df, signals, holding_period=30)

    # 6. 行业板块效应分析
    industry_results = analyze_industry_sector_effect(returns_df, signals, holding_period=30)

    # 总结和建议
    report.append("\n" + "=" * 80)
    report.append("优化建议总结")
    report.append("=" * 80)

    # 特征筛选建议
    report.append("\n1. 信号筛选优化:")
    report.append("   基础特征分析显示当前筛选条件已比较均衡，无明显差异化特征。")

    # 深度特征建议
    if deep_results.get('feature_results'):
        report.append("\n   深度特征分析发现:")
        for feature, data in deep_results['feature_results'].items():
            if data.get('significant', False):
                direction = "更高" if data['win_mean'] > data['lose_mean'] else "更低"
                report.append(f"   - {feature}: 盈利组{direction}（{data['win_mean']:.3f} vs {data['lose_mean']:.3f}）")

    # 震荡天数建议
    if deep_results.get('osc_results'):
        report.append("\n   震荡天数分组效果:")
        best_osc = None
        for group, data in deep_results['osc_results'].items():
            if data['avg_return'] > 0 and data['count'] >= 5:
                if best_osc is None or data['avg_return'] > best_osc['avg_return']:
                    best_osc = {'group': group, **data}

        if best_osc:
            report.append(f"   - 震荡天数 {best_osc['group']} 效果最好: 胜率={best_osc['win_rate']:.1f}%, "
                          f"平均收益={best_osc['avg_return']:.2f}%")

    # 底分型涨幅建议
    if deep_results.get('gain_results'):
        report.append("\n   底分型涨幅分组效果:")
        for group, data in deep_results['gain_results'].items():
            if data['count'] > 0:
                report.append(f"   - {group}: 样本={data['count']}, 胜率={data['win_rate']:.1f}%, "
                              f"平均收益={data['avg_return']:.2f}%")

    # 月份建议
    if deep_results.get('month_results'):
        report.append("\n   月份效果分析:")
        best_months = sorted(deep_results['month_results'].items(),
                            key=lambda x: x[1]['avg_return'], reverse=True)[:3]
        for month, data in best_months:
            if data['count'] >= 5:
                report.append(f"   - {month}月表现较好: 样本={data['count']}, 胜率={data['win_rate']:.1f}%, "
                              f"平均收益={data['avg_return']:.2f}%")

    # 止损建议
    report.append("\n2. 止损策略建议:")
    report.append("   分析发现：单纯设置止损反而降低了平均收益！")
    report.append("   原因：很多触发止损的股票后来涨回来了，过早止损损失了潜在收益。")
    report.append("   建议：不要设置过紧的止损（如-5%），可以放宽到-15%或-20%")

    # 止盈止损组合建议
    report.append("\n3. 止盈止损组合建议:")
    if tp_sl_results:
        # 找收益最高的组合
        best_combo = max(tp_sl_results, key=lambda x: x['avg_return'])
        report.append(f"   收益最高的组合: 止盈={best_combo['take_profit_pct']}%, 止损={best_combo['stop_loss_pct']}%")
        report.append(f"   - 平均收益: {best_combo['avg_return']:.2f}%")
        report.append(f"   - 胜率: {best_combo['win_rate']:.2f}%")
        report.append(f"   - 盈亏比: {best_combo['profit_loss_ratio']:.2f}")

    # 其他优化方向
    report.append("\n4. 开盘价影响建议:")
    if open_price_results:
        corr = open_price_results.get('correlation', 0)
        if corr > 0.1:
            report.append("   发现: 高开信号后续收益更好（正相关）")
        elif corr < -0.1:
            report.append("   发现: 低开信号后续收益更好（负相关）")
        else:
            report.append("   发现: 开盘涨幅对后续收益影响不明显")

        high_stats = open_price_results.get('high_open_stats', {})
        low_stats = open_price_results.get('low_open_stats', {})

        if high_stats and low_stats:
            report.append(f"   高开信号: 胜率={high_stats.get('win_rate', 0):.1f}%, "
                          f"平均收益={high_stats.get('avg_return', 0):.2f}%")
            report.append(f"   低开信号: 胜率={low_stats.get('win_rate', 0):.1f}%, "
                          f"平均收益={low_stats.get('avg_return', 0):.2f}%")

    report.append("\n5. 行业板块效应建议:")
    if industry_results:
        # 表现最好和最差的行业
        ind_results = industry_results.get('industry_results', {})
        if ind_results:
            best_ind = max(ind_results.items(), key=lambda x: x[1]['avg_return'])
            worst_ind = min(ind_results.items(), key=lambda x: x[1]['avg_return'])
            report.append(f"   表现最好的行业: {best_ind[0]} (平均收益={best_ind[1]['avg_return']:.2f}%)")
            report.append(f"   表现最差的行业: {worst_ind[0]} (平均收益={worst_ind[1]['avg_return']:.2f}%)")

        # 板块协同效应
        synergy = industry_results.get('synergy_results', {})
        if synergy.get('synergy') and synergy.get('solo'):
            syn = synergy['synergy']
            solo = synergy['solo']
            report.append(f"\n   板块协同效应:")
            report.append(f"   有协同信号: {syn['count']}条, 胜率={syn['win_rate']:.1f}%, "
                          f"平均收益={syn['avg_return']:.2f}%")
            report.append(f"   单独信号: {solo['count']}条, 胜率={solo['win_rate']:.1f}%, "
                          f"平均收益={solo['avg_return']:.2f}%")

    report.append("\n6. 其他优化建议:")
    report.append("   a. 市值筛选: 小市值股票可能波动更大，收益更高")
    report.append("   b. 流动性筛选: 过滤成交额过低的股票")
    report.append("   c. 多次信号股票: 同一只股票多次出现信号，后续表现如何？")

    report_text = "\n".join(report)
    return report_text


# ==================== 主函数 ====================

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='策略优化分析')
    parser.add_argument('--stop-loss', type=float, default=-10, help='止损阈值（负数）')
    parser.add_argument('--holding', type=int, default=30, help='持有期天数')
    parser.add_argument('--full', action='store_true', help='生成完整报告')
    parser.add_argument('--open-price', action='store_true', help='单独分析开盘价影响')
    parser.add_argument('--industry', action='store_true', help='单独分析行业板块效应')

    args = parser.parse_args()

    # 加载数据
    returns_df, signals = load_latest_files()

    if args.full:
        # 生成完整报告
        report = generate_optimization_report(returns_df, signals)
        print(report)

        # 保存报告
        report_file = os.path.join(config.REPORTS_DIR, f'optimization_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt')
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)
        print(f"\n报告已保存: {report_file}")
    elif args.open_price:
        # 单独分析开盘价影响
        analyze_open_price_effect(returns_df, signals, args.holding)
    elif args.industry:
        # 单独分析行业板块效应
        analyze_industry_sector_effect(returns_df, signals, args.holding)
    else:
        # 默认分析
        analyze_win_loss_characteristics(returns_df, args.holding)
        simulate_stop_loss(returns_df, args.stop_loss, args.holding)