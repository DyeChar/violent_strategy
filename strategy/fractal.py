"""
分型检测模块
实现缠论分型检测（底分型、顶分型）

严格版本定义：
- 底分型：中间K线最低价最低 + 最高价低于左右两根
- 顶分型：中间K线最高价最高 + 最低价高于左右两根

重要：只使用T日及之前的数据，不含未来数据
"""

import pandas as pd
from typing import Optional, Dict


def check_bottom_fractal_strict(kline: pd.DataFrame, mid_idx: int) -> bool:
    """
    严格版底分型检测

    定义：三根K线中，中间K线最低价最低，且中间K线最高价低于左右两根

    Args:
        kline: K线数据
        mid_idx: 中间K线索引（T-1日）

    Returns:
        bool: 是否形成底分型

    示例：
        T-2日(左)    T-1日(中)    T日(右)
           │            │            │
           │      ┌────┴────┐       │
        ┌──┴──┐  │ 最低点   │   ┌──┴──┐
        │最高 │  │         │   │最高 │
        └─────┘  └─────────┘   └─────┘

    注意：
        mid_idx 必须在有效范围内（>=1 且 < len(kline)-1）
        这样才能取到 T-2日(mid_idx-1) 和 T日(mid_idx+1) 的数据
    """
    # 检查索引范围
    if mid_idx < 1 or mid_idx >= len(kline) - 1:
        return False

    # 取三根K线
    left = kline.iloc[mid_idx - 1]   # T-2日
    mid = kline.iloc[mid_idx]        # T-1日（中间点）
    right = kline.iloc[mid_idx + 1]  # T日

    # 条件1：中间K线最低价最低
    cond1 = mid['low'] < left['low'] and mid['low'] < right['low']

    # 条件2：中间K线最高价低于左右两根（严格版本新增）
    cond2 = mid['high'] < left['high'] and mid['high'] < right['high']

    return cond1 and cond2


def check_top_fractal_strict(kline: pd.DataFrame, mid_idx: int) -> bool:
    """
    严格版顶分型检测

    定义：三根K线中，中间K线最高价最高，且中间K线最低价高于左右两根

    Args:
        kline: K线数据
        mid_idx: 中间K线索引

    Returns:
        bool: 是否形成顶分型
    """
    # 检查索引范围
    if mid_idx < 1 or mid_idx >= len(kline) - 1:
        return False

    # 取三根K线
    left = kline.iloc[mid_idx - 1]
    mid = kline.iloc[mid_idx]
    right = kline.iloc[mid_idx + 1]

    # 条件1：中间K线最高价最高
    cond1 = mid['high'] > left['high'] and mid['high'] > right['high']

    # 条件2：中间K线最低价高于左右两根（严格版本）
    cond2 = mid['low'] > left['low'] and mid['low'] > right['low']

    return cond1 and cond2


def find_bottom_fractals(kline: pd.DataFrame) -> list:
    """
    找出K线中所有的底分型位置

    Args:
        kline: K线数据

    Returns:
        list: 底分型中间点索引列表
    """
    fractals = []

    for i in range(1, len(kline) - 1):
        if check_bottom_fractal_strict(kline, i):
            fractals.append(i)

    return fractals


def find_top_fractals(kline: pd.DataFrame) -> list:
    """
    找出K线中所有的顶分型位置

    Args:
        kline: K线数据

    Returns:
        list: 顶分型中间点索引列表
    """
    fractals = []

    for i in range(1, len(kline) - 1):
        if check_top_fractal_strict(kline, i):
            fractals.append(i)

    return fractals


def check_bottom_fractal_on_date(kline: pd.DataFrame, date_idx: int) -> Dict:
    """
    在T日收盘后检查T-1日是否形成底分型

    Args:
        kline: K线数据（截至T日）
        date_idx: T日的索引位置

    Returns:
        Dict: {
            'satisfied': bool,
            'fractal_idx': int,  # 底分型中间点索引（T-1日）
            'fractal_low': float,  # 底分型最低价
            'fractal_date': str  # 底分型日期
        }
    """
    # T日收盘后，检查T-1日是否是底分型中间点
    # 需要用T-2、T-1、T三天的数据
    mid_idx = date_idx - 1  # T-1日

    if check_bottom_fractal_strict(kline, mid_idx):
        mid = kline.iloc[mid_idx]
        return {
            'satisfied': True,
            'fractal_idx': mid_idx,
            'fractal_low': mid['low'],
            'fractal_date': mid['date'] if 'date' in kline.columns else ''
        }

    return {
        'satisfied': False,
        'fractal_idx': -1,
        'fractal_low': 0,
        'fractal_date': ''
    }


def check_top_fractal_on_date(kline: pd.DataFrame, date_idx: int) -> Dict:
    """
    在T日收盘后检查T-1日是否形成顶分型

    Args:
        kline: K线数据（截至T日）
        date_idx: T日的索引位置

    Returns:
        Dict: 顶分型检测结果
    """
    mid_idx = date_idx - 1  # T-1日

    if check_top_fractal_strict(kline, mid_idx):
        mid = kline.iloc[mid_idx]
        return {
            'satisfied': True,
            'fractal_idx': mid_idx,
            'fractal_high': mid['high'],
            'fractal_date': mid['date'] if 'date' in kline.columns else ''
        }

    return {
        'satisfied': False,
        'fractal_idx': -1,
        'fractal_high': 0,
        'fractal_date': ''
    }


# ==================== 测试代码 ====================

if __name__ == '__main__':
    print("=" * 50)
    print("分型检测模块测试")
    print("=" * 50)

    # 创建测试K线数据（模拟底分型）
    test_data = pd.DataFrame([
        {'date': '2026-01-01', 'open': 10, 'high': 12, 'low': 9, 'close': 11},
        {'date': '2026-01-02', 'open': 10, 'high': 10, 'low': 8, 'close': 9},   # 底分型中间点
        {'date': '2026-01-03', 'open': 9, 'high': 11, 'low': 9, 'close': 10},
        {'date': '2026-01-04', 'open': 10, 'high': 13, 'low': 10, 'close': 12},
        {'date': '2026-01-05', 'open': 12, 'high': 14, 'low': 11, 'close': 13},  # 顶分型中间点
        {'date': '2026-01-06', 'open': 12, 'high': 12, 'low': 10, 'close': 11},
    ])

    # 测试底分型检测
    print("\n测试底分型检测:")
    for i in range(1, len(test_data) - 1):
        result = check_bottom_fractal_strict(test_data, i)
        print(f"  {test_data.iloc[i]['date']}: {result}")

    # 测试顶分型检测
    print("\n测试顶分型检测:")
    for i in range(1, len(test_data) - 1):
        result = check_top_fractal_strict(test_data, i)
        print(f"  {test_data.iloc[i]['date']}: {result}")

    # 测试找出所有底分型
    print("\n找出所有底分型:")
    fractals = find_bottom_fractals(test_data)
    print(f"  底分型位置: {fractals}")