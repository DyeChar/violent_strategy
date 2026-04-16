"""
策略阶段检测模块
实现暴力战法的4阶段顺序检测逻辑

阶段顺序：
1. 放量吸筹：连续>=2天成交量>=2倍5日均量且上涨
2. 震荡回调红肥绿瘦：放量结束后，涨日均量/跌日均量>=1.2
3. 回踩MA20：收盘价距20日均线±3%
4. 底分型确认：T-1日形成底分型（严格版本）

重要：各阶段检测必须不含未来数据
- 阶段检测只使用截至信号检测日的数据
- T日信号只能用T日及之前的数据
"""

import pandas as pd
import numpy as np
from typing import Dict, Tuple, Optional, List

import config


def detect_stage1_volume(kline: pd.DataFrame, signal_idx: int = None) -> Dict:
    """
    阶段1：放量吸筹检测

    检测逻辑：
    - 从signal_idx往前回溯，找连续放量上涨的日期
    - 放量定义：成交量 >= 2倍5日均量 且 收盘价 > 开盘价（上涨）
    - 要求连续>=2天

    Args:
        kline: K线数据
        signal_idx: 信号检测日的索引（默认用最后一天）

    Returns:
        Dict: {
            'satisfied': bool,
            'volume_days': int,  # 连续放量天数
            'volume_dates': List[str],  # 放量日期列表
            'end_idx': int,  # 放量结束位置索引
            'reason': str
        }
    """
    if kline.empty or len(kline) < config.VOLUME_MA_PERIOD + 5:
        return {'satisfied': False, 'volume_days': 0, 'volume_dates': [], 'end_idx': -1, 'reason': '数据不足'}

    # 默认用最后一天作为检测日
    signal_idx = signal_idx or len(kline) - 1

    # 只使用signal_idx及之前的数据
    check_kline = kline.iloc[:signal_idx + 1].copy()

    # 计算5日均量
    check_kline['vol_ma5'] = check_kline['volume'].rolling(window=config.VOLUME_MA_PERIOD).mean()

    # 判断是否放量上涨
    check_kline['is_volume_breakout'] = check_kline['volume'] >= check_kline['vol_ma5'] * config.VOLUME_RATIO_THRESHOLD
    check_kline['is_up'] = check_kline['close'] > check_kline['open']
    check_kline['is_volume_up'] = check_kline['is_volume_breakout'] & check_kline['is_up']

    # 从signal_idx往前找连续放量上涨
    volume_dates = []
    consecutive_count = 0

    for i in range(signal_idx, max(signal_idx - 25, config.VOLUME_MA_PERIOD), -1):
        if check_kline['is_volume_up'].iloc[i]:
            consecutive_count += 1
            date = check_kline['date'].iloc[i] if 'date' in check_kline.columns else str(i)
            volume_dates.append(date)
        else:
            # 遇到不满足的，停止（只统计最近的连续天数）
            if consecutive_count >= config.VOLUME_CONTINUOUS_DAYS:
                break
            consecutive_count = 0
            volume_dates = []

    # 放量日期按时间顺序排列（从早到晚）
    volume_dates = volume_dates[::-1]

    # 判断是否满足
    satisfied = consecutive_count >= config.VOLUME_CONTINUOUS_DAYS

    # 找到最后一个放量日索引
    end_idx = -1
    if satisfied and volume_dates:
        last_vol_date = volume_dates[-1]
        for i in range(len(check_kline)):
            if check_kline['date'].iloc[i] == last_vol_date:
                end_idx = i
                break

    return {
        'satisfied': satisfied,
        'volume_days': consecutive_count,
        'volume_dates': volume_dates,
        'end_idx': end_idx,
        'reason': f'连续放量{consecutive_count}天' if satisfied else f'放量天数不足（{consecutive_count}<{config.VOLUME_CONTINUOUS_DAYS}）'
    }


def detect_stage2_oscillation(kline: pd.DataFrame, stage1_end_idx: int, signal_idx: int = None) -> Dict:
    """
    阶段2：震荡回调红肥绿瘦检测

    检测逻辑：
    - 从放量结束后（stage1_end_idx+1）开始检测
    - 到signal_idx为止（不含signal_idx之后的未来数据）
    - 计算上涨日平均成交量 / 下跌日平均成交量 >= 1.2

    Args:
        kline: K线数据
        stage1_end_idx: 阶段1放量结束位置索引
        signal_idx: 信号检测日索引（默认用最后一天）

    Returns:
        Dict: {
            'satisfied': bool,
            'red_green_ratio': float,
            'oscillation_days': int,
            'up_days': int,
            'down_days': int,
            'reason': str
        }
    """
    if kline.empty or stage1_end_idx < 0:
        return {'satisfied': False, 'red_green_ratio': 0, 'oscillation_days': 0, 'up_days': 0, 'down_days': 0, 'reason': '阶段1未满足'}

    signal_idx = signal_idx or len(kline) - 1

    # 震荡期：从放量结束后到signal_idx（不含未来数据）
    # 注意：signal_idx是T日，震荡期是放量后到T日
    start_idx = stage1_end_idx + 1
    end_idx = signal_idx + 1  # 包含signal_idx

    if start_idx >= end_idx:
        return {'satisfied': False, 'red_green_ratio': 0, 'oscillation_days': 0, 'up_days': 0, 'down_days': 0, 'reason': '无震荡期'}

    oscillation_kline = kline.iloc[start_idx:end_idx].copy()

    if oscillation_kline.empty or len(oscillation_kline) < config.OSCILLATION_MIN_DAYS:
        return {'satisfied': False, 'red_green_ratio': 0, 'oscillation_days': len(oscillation_kline), 'up_days': 0, 'down_days': 0, 'reason': '震荡期天数不足'}

    # 判断涨跌
    oscillation_kline['is_up'] = oscillation_kline['close'] > oscillation_kline['open']

    up_days = oscillation_kline[oscillation_kline['is_up']]
    down_days = oscillation_kline[~oscillation_kline['is_up']]

    # 需要同时有上涨日和下跌日
    if up_days.empty or down_days.empty:
        return {
            'satisfied': False,
            'red_green_ratio': 0,
            'oscillation_days': len(oscillation_kline),
            'up_days': len(up_days),
            'down_days': len(down_days),
            'reason': '缺少上涨日或下跌日'
        }

    # 计算红肥绿瘦比率
    up_avg_vol = up_days['volume'].mean()
    down_avg_vol = down_days['volume'].mean()

    red_green_ratio = up_avg_vol / down_avg_vol if down_avg_vol > 0 else 0

    satisfied = red_green_ratio >= config.RED_GREEN_RATIO_THRESHOLD

    return {
        'satisfied': satisfied,
        'red_green_ratio': round(red_green_ratio, 2),
        'oscillation_days': len(oscillation_kline),
        'up_days': len(up_days),
        'down_days': len(down_days),
        'reason': f'红肥绿瘦比率{red_green_ratio:.2f}' if satisfied else f'红肥绿瘦比率不足（{red_green_ratio:.2f}<{config.RED_GREEN_RATIO_THRESHOLD}）'
    }


def detect_stage3_ma20(kline: pd.DataFrame, signal_idx: int = None) -> Dict:
    """
    阶段3：回踩MA20检测

    检测逻辑：
    - T日收盘价距20日均线±3%
    - 只使用signal_idx及之前的数据计算均线

    Args:
        kline: K线数据
        signal_idx: 信号检测日索引（默认用最后一天）

    Returns:
        Dict: {
            'satisfied': bool,
            'ma20': float,
            'close': float,
            'ma_distance': float,  # 距离MA20的百分比
            'reason': str
        }
    """
    if kline.empty or len(kline) < config.MA20_PERIOD:
        return {'satisfied': False, 'ma20': 0, 'close': 0, 'ma_distance': 0, 'reason': '数据不足'}

    signal_idx = signal_idx or len(kline) - 1

    # 只用signal_idx及之前的数据计算MA20
    check_kline = kline.iloc[:signal_idx + 1]

    # 计算20日均线
    ma20 = check_kline['close'].rolling(window=config.MA20_PERIOD).mean().iloc[signal_idx]

    if pd.isna(ma20):
        return {'satisfied': False, 'ma20': 0, 'close': 0, 'ma_distance': 0, 'reason': 'MA20计算失败'}

    close = check_kline['close'].iloc[signal_idx]

    # 计算距离MA20的百分比
    ma_distance = abs(close - ma20) / ma20

    satisfied = ma_distance <= config.MA20_TOLERANCE

    return {
        'satisfied': satisfied,
        'ma20': round(ma20, 2),
        'close': round(close, 2),
        'ma_distance': round(ma_distance, 4),
        'reason': f'距MA20 {ma_distance:.2%}' if satisfied else f'距MA20超过容差（{ma_distance:.2%}>{config.MA20_TOLERANCE:.0%}）'
    }


def detect_stage4_fractal(kline: pd.DataFrame, signal_idx: int = None) -> Dict:
    """
    阶段4：底分型确认检测

    检测逻辑：
    - T日收盘后检查T-1日是否形成底分型
    - 使用严格版本：中间K线最低价最低 + 最高价低于左右两根

    Args:
        kline: K线数据
        signal_idx: T日的索引（默认用最后一天）

    Returns:
        Dict: {
            'satisfied': bool,
            'fractal_idx': int,  # 底分型中间点索引（T-1日）
            'fractal_low': float,
            'fractal_date': str,
            'reason': str
        }
    """
    from .fractal import check_bottom_fractal_on_date

    if kline.empty or len(kline) < 3:
        return {'satisfied': False, 'fractal_idx': -1, 'fractal_low': 0, 'fractal_date': '', 'reason': '数据不足'}

    signal_idx = signal_idx or len(kline) - 1

    # 检查T-1日是否形成底分型
    result = check_bottom_fractal_on_date(kline, signal_idx)

    if result['satisfied']:
        return {
            'satisfied': True,
            'fractal_idx': result['fractal_idx'],
            'fractal_low': result['fractal_low'],
            'fractal_date': result['fractal_date'],
            'reason': f'T-1日({result["fractal_date"]})形成底分型'
        }

    return {
        'satisfied': False,
        'fractal_idx': -1,
        'fractal_low': 0,
        'fractal_date': '',
        'reason': 'T-1日未形成底分型'
    }


# ==================== 测试代码 ====================

if __name__ == '__main__':
    print("=" * 50)
    print("策略阶段检测模块测试")
    print("=" * 50)

    # 创建测试K线数据
    import numpy as np

    dates = pd.date_range('2025-01-01', periods=50, freq='D')
    test_data = pd.DataFrame({
        'date': dates.strftime('%Y-%m-%d'),
        'open': np.random.uniform(10, 12, 50),
        'close': np.random.uniform(10, 12, 50),
        'high': np.random.uniform(12, 14, 50),
        'low': np.random.uniform(8, 10, 50),
        'volume': np.random.uniform(1000000, 3000000, 50)
    })

    # 模拟放量吸筹
    test_data.loc[25:26, 'volume'] = 5000000  # 放量
    test_data.loc[25:26, 'close'] = test_data.loc[25:26, 'open'] + 0.5  # 上涨

    # 模拟底分型
    test_data.loc[48, 'low'] = test_data.loc[47, 'low'] - 0.5
    test_data.loc[48, 'high'] = test_data.loc[47, 'high'] - 0.3
    test_data.loc[49, 'low'] = test_data.loc[48, 'low'] + 0.3
    test_data.loc[49, 'high'] = test_data.loc[48, 'high'] + 0.5

    # 测试各阶段检测
    signal_idx = 49

    print(f"\n检测信号日: {test_data.iloc[signal_idx]['date']}")

    print("\n阶段1检测:")
    stage1 = detect_stage1_volume(test_data, signal_idx)
    print(f"  {stage1}")

    print("\n阶段2检测:")
    if stage1['satisfied']:
        stage2 = detect_stage2_oscillation(test_data, stage1['end_idx'], signal_idx)
        print(f"  {stage2}")

    print("\n阶段3检测:")
    stage3 = detect_stage3_ma20(test_data, signal_idx)
    print(f"  {stage3}")

    print("\n阶段4检测:")
    stage4 = detect_stage4_fractal(test_data, signal_idx)
    print(f"  {stage4}")