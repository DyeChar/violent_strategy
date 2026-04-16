"""
固定持有期收益计算模块
计算信号触发后各固定持有期的收益率

买入规则：
- T日收盘后信号触发
- T+1日开盘价买入
- T日不可卖出（买入当日不能卖）

持有期计算：
- 1日：买入后第1个交易日的收盘价（相当于持有1天）
- 2日：买入后第2个交易日的收盘价
- ...以此类推

注意：持有期天数从买入日开始计算，不是从信号日开始
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime, timedelta

import config


def calculate_holding_returns(kline: pd.DataFrame, signal_idx: int, periods: List[int] = None) -> Dict:
    """
    计算固定持有期收益率

    Args:
        kline: K线数据
        signal_idx: T日（信号日）的索引位置
        periods: 持有期天数列表，默认使用config配置

    Returns:
        Dict: {
            'signal_date': str,  # 信号日期
            'buy_date': str,  # 买入日期（T+1）
            'buy_price': float,  # 买入价格（T+1开盘价）
            'returns': Dict[str, float],  # {'1日收益%': x, '2日收益%': y, ...}
            'reason': str
        }
    """
    periods = periods or config.HOLDING_PERIODS

    if kline.empty or signal_idx >= len(kline) - 1:
        return {
            'signal_date': '',
            'buy_date': '',
            'buy_price': 0,
            'returns': {},
            'reason': '数据不足（无T+1日数据）'
        }

    # T日信号
    signal_date = kline['date'].iloc[signal_idx]

    # T+1日买入（开盘价）
    buy_idx = signal_idx + 1

    if buy_idx >= len(kline):
        return {
            'signal_date': signal_date,
            'buy_date': '',
            'buy_price': 0,
            'returns': {},
            'reason': '无T+1日数据'
        }

    buy_date = kline['date'].iloc[buy_idx]
    buy_price = kline['open'].iloc[buy_idx]

    # 计算各持有期收益率
    returns = {}

    for period in periods:
        # 持有period天后的收盘价（从买入日开始计算）
        # period=1: 买入后第1个交易日（buy_idx + 0）
        # period=2: 买入后第2个交易日（buy_idx + 1）
        sell_idx = buy_idx + period - 1

        if sell_idx < len(kline):
            sell_price = kline['close'].iloc[sell_idx]
            return_pct = (sell_price - buy_price) / buy_price * 100
            returns[f'{period}日收益%'] = round(return_pct, 2)
        else:
            returns[f'{period}日收益%'] = None  # 数据不足

    return {
        'signal_date': signal_date,
        'buy_date': buy_date,
        'buy_price': round(buy_price, 2),
        'returns': returns,
        'reason': '成功计算'
    }


def calculate_batch_returns(klines: Dict[str, pd.DataFrame], signals: List[Dict], periods: List[int] = None) -> List[Dict]:
    """
    批量计算信号收益

    Args:
        klines: {股票代码: K线DataFrame}
        signals: 信号列表（detect_signal的结果）
        periods: 持有期列表

    Returns:
        List[Dict]: 收益结果列表
    """
    periods = periods or config.HOLDING_PERIODS
    results = []

    for signal in signals:
        # 股票代码已标准化为6位字符串
        code = str(signal['code'])
        kline = klines.get(code)

        if kline is None:
            continue

        # 从signal_date在完整K线中找到正确索引（不能用截断K线的索引）
        signal_date = signal.get('signal_date')
        if signal_date is None:
            continue

        # 查找signal_date在完整K线中的位置
        kline_dates = kline['date'].astype(str).str.strip()
        matching_idx = kline_dates[kline_dates == signal_date].index

        if len(matching_idx) == 0:
            continue

        signal_idx = kline.index.get_loc(matching_idx[0])

        # 计算收益
        returns_result = calculate_holding_returns(kline, signal_idx, periods)

        # 合合信号信息
        result = {
            '股票代码': code,
            '股票名称': signal.get('name', ''),
            '信号日期': returns_result['signal_date'],
            '买入日期': returns_result['buy_date'],
            '买入价格': returns_result['buy_price'],
            '放量天数': signal.get('volume_days', 0),
            '红肥绿瘦': signal.get('red_green_ratio', 0),
            '距MA20': signal.get('stage3', {}).get('ma_distance', 0),
        }

        # 添加各持有期收益
        for key, value in returns_result['returns'].items():
            result[key] = value

        results.append(result)

    return results


def save_returns_to_csv(results: List[Dict], output_file: str = None) -> str:
    """
    保存收益结果到CSV

    Args:
        results: 收益结果列表
        output_file: 输出文件路径

    Returns:
        str: 保存的文件路径
    """
    if not results:
        return ''

    output_file = output_file or os.path.join(config.REPORTS_DIR, f'returns_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')

    df = pd.DataFrame(results)
    df.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f"收益结果已保存: {output_file}")
    return output_file


# ==================== 测试代码 ====================

if __name__ == '__main__':
    import os

    print("=" * 50)
    print("固定持有期收益计算模块测试")
    print("=" * 50)

    # 创建测试K线数据
    import numpy as np

    dates = pd.date_range('2025-01-01', periods=70, freq='D')
    test_data = pd.DataFrame({
        'date': dates.strftime('%Y-%m-%d'),
        'open': np.random.uniform(10, 12, 70),
        'close': np.random.uniform(10, 12, 70),
        'high': np.random.uniform(12, 14, 70),
        'low': np.random.uniform(8, 10, 70),
        'volume': np.random.uniform(1000000, 3000000, 70)
    })

    # 模拟买入后上涨
    test_data.loc[51:60, 'close'] = 12  # 从buy_idx=51开始，逐渐上涨
    test_data.loc[51, 'open'] = 11  # 买入价格

    # 测试单股收益计算
    signal_idx = 50  # T日信号
    print(f"\n信号日: {test_data.iloc[signal_idx]['date']}")
    print(f"买入日: {test_data.iloc[signal_idx+1]['date']}")

    result = calculate_holding_returns(test_data, signal_idx)
    print(f"\n收益计算结果:")
    print(f"  买入价格: {result['buy_price']}")
    for key, value in result['returns'].items():
        print(f"  {key}: {value}%")