"""
选股执行模块
批量筛选创业板股票，找出符合暴力战法信号的股票
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional
import time
import os
from datetime import datetime

import config
import data_fetcher
import strategy


def run_selection(stock_codes: List[str] = None,
                  start_date: str = None,
                  end_date: str = None,
                  verbose: bool = True) -> pd.DataFrame:
    """
    执行选股筛选，找出符合暴力战法信号的股票

    Args:
        stock_codes: 股票代码列表（默认为创业板全部）
        start_date: K线起始日期
        end_date: K线结束日期
        verbose: 是否打印详细信息

    Returns:
        DataFrame: 选股结果，包含股票代码、名称、信号日期、各阶段信息
    """
    start_date = start_date or config.START_DATE
    end_date = end_date or config.END_DATE

    # 获取股票列表
    if stock_codes is None:
        stock_codes = data_fetcher.get_chinext_stocks()

    print(f"\n{'=' * 60}")
    print(f"暴力战法选股")
    print(f"{'=' * 60}")
    print(f"股票池: 创业板 ({len(stock_codes)}只)")
    print(f"日期范围: {start_date} - {end_date}")
    print(f"策略参数:")
    print(f"  - 前期下跌阈值: {config.DRAWBACK_THRESHOLD:.0%}")
    print(f"  - 放量倍数阈值: {config.VOLUME_RATIO_THRESHOLD}倍")
    print(f"  - 连续放量天数: {config.VOLUME_CONTINUOUS_DAYS}天")
    print(f"  - 红肥绿瘦阈值: {config.RED_FAT_THRESHOLD}")
    print(f"  - 回踩容差: {config.MA_TOLERANCE:.0%}")
    print(f"{'=' * 60}\n")

    # 批量获取K线数据
    kline_data = data_fetcher.batch_fetch_klines(
        stock_codes,
        start_date=start_date,
        end_date=end_date
    )

    # 执行策略检测
    signals = []

    print(f"\n开始策略检测...")
    for code, kline in kline_data.items():
        if verbose:
            print(f"  检测 {code}...", end=' ')

        # 执行策略检测
        result = strategy.detect_strategy_signal(kline)

        if result['signal']:
            # 有信号，记录详细信息
            signal_info = {
                'code': code,
                'name': result['name'],
                'signal_date': result['signal_date'],
                'current_price': result['current_price'],
                'ma20': result['ma20'],
                'drawback': result['stage1'].get('drawback', 0),
                'volume_days': result['stage2'].get('volume_days', 0),
                'red_green_ratio': result['stage3'].get('ratio', 0),
                'ma_distance': result['stage4'].get('distance', 0),
                'high_price': result['high_point'][0] if result['high_point'] else 0,
                'high_date': result['high_point'][1] if result['high_point'] else '',
                'stage_desc': strategy.get_stage_description(result)
            }
            signals.append(signal_info)

            if verbose:
                print(f"✓ 发现信号！日期: {result['signal_date']}")
        else:
            if verbose:
                print(f"× {result['reason']}")

    # 转换为DataFrame
    if signals:
        result_df = pd.DataFrame(signals)
        # 按信号日期排序
        result_df = result_df.sort_values('signal_date', ascending=False)
    else:
        result_df = pd.DataFrame()

    print(f"\n{'=' * 60}")
    print(f"选股完成！")
    print(f"扫描股票: {len(kline_data)} 只")
    print(f"发现信号: {len(signals)} 只")
    print(f"{'=' * 60}")

    return result_df, kline_data


def save_signals(signals_df: pd.DataFrame, output_path: str = None) -> str:
    """
    保存选股信号结果

    Args:
        signals_df: 信号DataFrame
        output_path: 输出路径

    Returns:
        str: 保存的文件路径
    """
    if signals_df.empty:
        print("无信号结果，不保存")
        return ''

    # 确保输出目录存在
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)

    # 生成输出路径
    if output_path is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = os.path.join(config.OUTPUT_DIR, f'signals_{timestamp}.csv')

    # 保存
    signals_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"保存信号结果: {output_path}")

    return output_path


def print_summary(signals_df: pd.DataFrame):
    """
    打印选股结果摘要

    Args:
        signals_df: 信号DataFrame
    """
    if signals_df.empty:
        print("\n今日未发现暴力战法信号股票")
        return

    print(f"\n{'=' * 60}")
    print(f"暴力战法信号股票列表")
    print(f"{'=' * 60}")
    print(f"共 {len(signals_df)} 只股票")

    for i, row in signals_df.iterrows():
        print(f"\n[{i+1}] {row['code']} {row['name']}")
        print(f"    信号日期: {row['signal_date']}")
        print(f"    当前价格: {row['current_price']:.2f}元")
        print(f"    20日均线: {row['ma20']:.2f}元")
        print(f"    回调幅度: {row['drawback']:.1%} (高点: {row['high_price']:.2f}元 @ {row['high_date']})")
        print(f"    放量天数: {row['volume_days']}天")
        print(f"    红肥绿瘦比率: {row['red_green_ratio']:.2f}")
        print(f"    距均线距离: {row['ma_distance']:.1%}")

    print(f"\n{'=' * 60}")


# ==================== 测试代码 ====================

if __name__ == '__main__':
    print("=" * 50)
    print("选股执行模块测试")
    print("=" * 50)

    # 使用测试样本快速验证
    print("\n使用测试样本进行快速验证...")
    test_codes = config.TEST_SAMPLES[:5]

    result_df, kline_data = run_selection(
        stock_codes=test_codes,
        start_date='20240101',
        end_date='20260411',
        verbose=True
    )

    # 打印摘要
    print_summary(result_df)

    # 保存结果
    if not result_df.empty:
        save_signals(result_df)