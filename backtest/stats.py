"""
统计分析模块
计算各持有期的统计指标（胜率、平均收益、盈亏比等）

统计指标：
- 样本数：有效信号数量
- 胜率：盈利信号占比
- 平均收益：所有信号的平均收益率
- 最大收益：单笔最大盈利
- 最大亏损：单笔最大亏损
- 盈亏比：平均盈利/平均亏损
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime
import os

import config


def calculate_period_stats(results: List[Dict], periods: List[int] = None) -> Dict:
    """
    计算各持有期的统计指标

    Args:
        results: 收益结果列表（calculate_batch_returns的结果）
        periods: 持有期列表

    Returns:
        Dict: {持有期: 统计指标dict}
    """
    periods = periods or config.HOLDING_PERIODS
    stats = {}

    for period in periods:
        key = f'{period}日收益%'

        # 提取该持有期的有效数据
        valid_returns = [r[key] for r in results if r[key] is not None]

        if not valid_returns:
            stats[key] = {
                '样本数': 0,
                '胜率': 0,
                '平均收益': 0,
                '最大收益': 0,
                '最大亏损': 0,
                '盈利次数': 0,
                '亏损次数': 0,
                '平均盈利': 0,
                '平均亏损': 0,
                '盈亏比': 0
            }
            continue

        # 计算统计指标
        profits = valid_returns
        win_count = len([p for p in profits if p > 0])
        loss_count = len([p for p in profits if p <= 0])

        win_profits = [p for p in profits if p > 0]
        loss_profits = [abs(p) for p in profits if p <= 0]

        avg_win = np.mean(win_profits) if win_profits else 0
        avg_loss = np.mean(loss_profits) if loss_profits else 0

        stats[key] = {
            '样本数': len(valid_returns),
            '胜率': round(win_count / len(valid_returns) * 100, 2),
            '平均收益': round(np.mean(profits), 2),
            '最大收益': round(max(profits), 2),
            '最大亏损': round(min(profits), 2),
            '盈利次数': win_count,
            '亏损次数': loss_count,
            '平均盈利': round(avg_win, 2),
            '平均亏损': round(avg_loss, 2),
            '盈亏比': round(avg_win / avg_loss, 2) if avg_loss > 0 else 0
        }

    return stats


def print_stats_table(stats: Dict):
    """
    打印统计表格

    Args:
        stats: 统计指标字典
    """
    print("\n" + "=" * 80)
    print("各持有期收益率统计")
    print("=" * 80)
    print(f"{'持有期':<10} {'样本数':<10} {'胜率':<10} {'平均收益':<12} {'最大收益':<12} {'最大亏损':<12} {'盈亏比':<10}")
    print("-" * 80)

    for period_key, s in stats.items():
        print(f"{period_key:<10} {s['样本数']:<10} {s['胜率']:.2f}%{'':<5} {s['平均收益']:.2f}%{'':<6} {s['最大收益']:.2f}%{'':<6} {s['最大亏损']:.2f}%{'':<6} {s['盈亏比']:<10}")

    print("=" * 80)


def save_stats_to_csv(stats: Dict, output_file: str = None) -> str:
    """
    保存统计结果到CSV

    Args:
        stats: 统计指标字典
        output_file: 输出文件路径

    Returns:
        str: 保存的文件路径
    """
    if not stats:
        return ''

    output_file = output_file or os.path.join(config.REPORTS_DIR, f'stats_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')

    # 转换为DataFrame
    df_data = []
    for period_key, s in stats.items():
        df_data.append({
            '持有期': period_key,
            '样本数': s['样本数'],
            '胜率%': s['胜率'],
            '平均收益%': s['平均收益'],
            '最大收益%': s['最大收益'],
            '最大亏损%': s['最大亏损'],
            '盈利次数': s['盈利次数'],
            '亏损次数': s['亏损次数'],
            '平均盈利%': s['平均盈利'],
            '平均亏损%': s['平均亏损'],
            '盈亏比': s['盈亏比']
        })

    df = pd.DataFrame(df_data)
    df.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f"统计结果已保存: {output_file}")
    return output_file


def generate_backtest_report(results: List[Dict], stats: Dict, output_dir: str = None) -> str:
    """
    生成完整回测报告（包含收益明细和统计汇总）

    Args:
        results: 收益结果列表
        stats: 统计指标
        output_dir: 输出目录

    Returns:
        str: 报告文件路径
    """
    output_dir = output_dir or config.REPORTS_DIR
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # 保存收益明细
    returns_file = os.path.join(output_dir, f'backtest_returns_{timestamp}.csv')
    if results:
        returns_df = pd.DataFrame(results)
        returns_df.to_csv(returns_file, index=False, encoding='utf-8-sig')
        print(f"收益明细已保存: {returns_file}")

    # 保存统计汇总
    stats_file = os.path.join(output_dir, f'backtest_stats_{timestamp}.csv')
    save_stats_to_csv(stats, stats_file)

    # 打印统计表格
    print_stats_table(stats)

    return returns_file


# ==================== 测试代码 ====================

if __name__ == '__main__':
    print("=" * 50)
    print("统计分析模块测试")
    print("=" * 50)

    # 创建模拟收益数据
    test_results = [
        {'股票代码': '300001', '1日收益%': 2.5, '2日收益%': 3.2, '3日收益%': -1.5, '5日收益%': 5.0},
        {'股票代码': '300002', '1日收益%': -1.0, '2日收益%': 1.5, '3日收益%': 2.0, '5日收益%': -2.0},
        {'股票代码': '300003', '1日收益%': 3.0, '2日收益%': -0.5, '3日收益%': 4.0, '5日收益%': 6.0},
        {'股票代码': '300004', '1日收益%': 1.5, '2日收益%': 2.0, '3日收益%': 1.0, '5日收益%': None},
        {'股票代码': '300005', '1日收益%': -2.0, '2日收益%': -1.0, '3日收益%': -3.0, '5日收益%': -4.0},
    ]

    # 计算统计
    stats = calculate_period_stats(test_results, periods=[1, 2, 3, 5])

    # 打印表格
    print_stats_table(stats)