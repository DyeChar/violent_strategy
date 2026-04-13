"""
暴力战法策略检测模块
实现4阶段选股逻辑：
1. 前期下跌：从高点下跌≥20%
2. 放量吸筹：量能放大≥2倍，持续≥2天
3. 红肥绿瘦：涨放量、跌缩量
4. 回踩20日均线：提示机会
"""

import pandas as pd
import numpy as np
from typing import Tuple, List, Dict, Optional
import config


# ==================== 阶段1: 前期下跌检测 ====================

def find_high_point(kline: pd.DataFrame, lookback: int = None) -> Tuple[float, str, int]:
    """
    在回溯周期内寻找前期高点

    Args:
        kline: K线数据
        lookback: 回溯天数

    Returns:
        Tuple: (最高价, 高点日期, 高点索引位置)
    """
    lookback = lookback or config.DRAWBACK_LOOKBACK

    if len(kline) < lookback:
        lookback = len(kline)

    # 取回溯周期内的数据
    recent_data = kline.tail(lookback)

    # 找最高价
    max_high_idx = recent_data['high'].idxmax()
    high_price = recent_data.loc[max_high_idx, 'high']
    high_date = recent_data.loc[max_high_idx, 'date']

    # 计算在原始kline中的位置
    pos = kline.index.get_loc(max_high_idx) if max_high_idx in kline.index else -1

    return high_price, high_date, pos


def check_drawback(kline: pd.DataFrame, lookback: int = None) -> Tuple[bool, float, str]:
    """
    阶段1检测：前期下跌检测
    从前期高点下跌至少20%

    Args:
        kline: K线数据
        lookback: 回溯天数

    Returns:
        Tuple: (是否满足, 回调幅度, 高点日期)
    """
    if kline.empty or len(kline) < 10:
        return False, 0.0, ''

    lookback = lookback or config.DRAWBACK_LOOKBACK

    # 寻找前期高点
    high_price, high_date, high_pos = find_high_point(kline, lookback)

    # 当前价格
    current_price = kline['close'].iloc[-1]

    # 计算回调幅度
    drawback_ratio = (high_price - current_price) / high_price

    # 判断是否满足阈值
    is_satisfied = drawback_ratio >= config.DRAWBACK_THRESHOLD

    return is_satisfied, drawback_ratio, high_date


# ==================== 阶段2: 放量吸筹检测 ====================

def check_volume_breakout(kline: pd.DataFrame) -> Tuple[bool, int, List[str]]:
    """
    阶段2检测：放量吸筹检测
    量能放大至前面的2倍以上，至少持续两天以上

    Args:
        kline: K线数据

    Returns:
        Tuple: (是否满足, 连续放量天数, 放量日期列表)
    """
    if kline.empty or len(kline) < config.VOLUME_MA_PERIOD + 10:
        return False, 0, []

    # 计算成交量均线（5日均量）
    kline['volume_ma'] = kline['volume'].rolling(window=config.VOLUME_MA_PERIOD).mean()

    # 判断是否放量：成交量 >= 均线 * 放量倍数
    kline['is_volume_breakout'] = kline['volume'] >= kline['volume_ma'] * config.VOLUME_RATIO_THRESHOLD

    # 判断是否上涨（收盘价 > 开盘价）
    kline['is_up'] = kline['close'] > kline['open']

    # 放量上涨
    kline['is_volume_up'] = kline['is_up'] & kline['is_volume_breakout']

    # 检查最近是否有连续放量上涨（从后往前找）
    volume_dates = []
    consecutive_count = 0

    # 从最后一天开始往前检查
    for i in range(len(kline) - 1, max(len(kline) - 20, 0), -1):
        if kline['is_volume_up'].iloc[i]:
            consecutive_count += 1
            volume_dates.append(kline['date'].iloc[i])
        else:
            # 遇到不满足的就停止（只统计最近的连续天数）
            break

    # 放量日期按时间顺序排列（从早到晚）
    volume_dates = volume_dates[::-1]

    # 判断是否满足条件
    is_satisfied = consecutive_count >= config.VOLUME_CONTINUOUS_DAYS

    return is_satisfied, consecutive_count, volume_dates


# ==================== 阶段3: 红肥绿瘦检测 ====================

def check_red_fat_green_thin(kline: pd.DataFrame,
                             start_idx: int = None,
                             period: int = None) -> Tuple[bool, float]:
    """
    阶段3检测：红肥绿瘦检测
    震荡回调期：涨放量、跌缩量

    Args:
        kline: K线数据
        start_idx: 开始检测的索引位置（放量吸筹结束后）
        period: 观察周期天数

    Returns:
        Tuple: (是否满足, 红肥绿瘦比率)
    """
    if kline.empty:
        return False, 0.0

    period = period or config.OSCILLATION_PERIOD

    # 确定检测范围
    if start_idx is None:
        # 默认检测最近period天
        check_data = kline.tail(period)
    else:
        # 从指定位置开始检测
        end_idx = min(start_idx + period, len(kline))
        check_data = kline.iloc[start_idx:end_idx]

    if check_data.empty:
        return False, 0.0

    # 判断涨跌（收盘价 > 开盘价为涨）
    check_data['is_up'] = check_data['close'] > check_data['open']

    # 分离上涨日和下跌日
    up_days = check_data[check_data['is_up']]
    down_days = check_data[~check_data['is_up']]

    # 需要同时有上涨日和下跌日才能计算比率
    if up_days.empty or down_days.empty:
        return False, 0.0

    # 计算上涨日平均成交量
    up_avg_volume = up_days['volume'].mean()

    # 计算下跌日平均成交量
    down_avg_volume = down_days['volume'].mean()

    # 红肥绿瘦比率（上涨日成交量 / 下跌日成交量）
    ratio = up_avg_volume / down_avg_volume if down_avg_volume > 0 else 0

    # 判断是否满足：涨放量 > 跌缩量
    # 比率 > 1 表示上涨日成交量大于下跌日
    is_satisfied = ratio > config.RED_FAT_THRESHOLD

    return is_satisfied, ratio


# ==================== 阶段4: 回踩均线检测 ====================

def calculate_ma(kline: pd.DataFrame, period: int = None) -> pd.Series:
    """
    计算移动平均线

    Args:
        kline: K线数据
        period: 均线周期

    Returns:
        Series: 均线数据
    """
    period = period or config.MA_PERIOD
    return kline['close'].rolling(window=period).mean()


def check_ma_touch(kline: pd.DataFrame) -> Tuple[bool, float, str]:
    """
    阶段4检测：回踩20日均线检测
    当前价格接近20日均线（±2%容差）

    Args:
        kline: K线数据

    Returns:
        Tuple: (是否满足, 距离均线幅度, 日期)
    """
    if kline.empty or len(kline) < config.MA_PERIOD:
        return False, 0.0, ''

    # 计算20日均线
    kline['ma20'] = calculate_ma(kline, config.MA_PERIOD)

    # 当前价格和均线
    current_price = kline['close'].iloc[-1]
    ma20 = kline['ma20'].iloc[-1]
    current_date = kline['date'].iloc[-1]

    # 计算距离均线的幅度（绝对值）
    distance_ratio = abs(current_price - ma20) / ma20

    # 判断是否在容差范围内
    is_satisfied = distance_ratio <= config.MA_TOLERANCE

    return is_satisfied, distance_ratio, current_date


# ==================== 综合检测：4阶段策略 ====================

def detect_strategy_signal(kline: pd.DataFrame) -> Dict:
    """
    综合检测4阶段策略，返回完整信号信息

    Args:
        kline: K线数据

    Returns:
        Dict: {
            'signal': bool,           # 是否出现买入机会
            'signal_date': str,       # 信号日期
            'code': str,              # 股票代码
            'name': str,              # 股票名称
            'stage1': dict,           # 阶段1检测结果
            'stage2': dict,           # 阶段2检测结果
            'stage3': dict,           # 阶段3检测结果
            'stage4': dict,           # 阶段4检测结果
            'high_point': tuple,      # 前期高点信息
            'volume_dates': list,     # 放量日期
            'current_price': float,   # 当前价格
            'ma20': float             # 20日均线值
        }
    """
    if kline.empty or len(kline) < config.MA_PERIOD + config.OSCILLATION_PERIOD + 10:
        return {
            'signal': False,
            'reason': '数据不足',
            'code': kline['code'].iloc[-1] if 'code' in kline.columns else '',
            'name': kline['name'].iloc[-1] if 'name' in kline.columns else ''
        }

    # 获取股票信息
    code = kline['code'].iloc[-1] if 'code' in kline.columns else ''
    name = kline['name'].iloc[-1] if 'name' in kline.columns else ''

    # 当前价格
    current_price = kline['close'].iloc[-1]

    # 计算20日均线
    kline['ma20'] = calculate_ma(kline, config.MA_PERIOD)
    ma20 = kline['ma20'].iloc[-1]

    result = {
        'signal': False,
        'signal_date': '',
        'code': code,
        'name': name,
        'current_price': current_price,
        'ma20': ma20,
        'stage1': {},
        'stage2': {},
        'stage3': {},
        'stage4': {},
        'high_point': (),
        'volume_dates': [],
        'reason': ''
    }

    # ========== 阶段1: 前期下跌检测 ==========
    stage1_ok, drawback, high_date = check_drawback(kline)
    result['stage1'] = {
        'satisfied': stage1_ok,
        'drawback': drawback,
        'high_date': high_date
    }

    if not stage1_ok:
        result['reason'] = f'阶段1不满足: 回调幅度{drawback:.1%} < {config.DRAWBACK_THRESHOLD:.0%}'
        return result

    # 记录前期高点信息
    high_price, _, high_pos = find_high_point(kline)
    result['high_point'] = (high_price, high_date)

    # ========== 阶段2: 放量吸筹检测 ==========
    stage2_ok, vol_days, vol_dates = check_volume_breakout(kline)
    result['stage2'] = {
        'satisfied': stage2_ok,
        'volume_days': vol_days,
        'volume_dates': vol_dates
    }
    result['volume_dates'] = vol_dates

    if not stage2_ok:
        result['reason'] = f'阶段2不满足: 连续放量天数{vol_days} < {config.VOLUME_CONTINUOUS_DAYS}'
        return result

    # ========== 阶段3: 红肥绿瘦检测 ==========
    # 放量吸筹结束后进入震荡期，从放量日期的最后一天之后开始检测
    if vol_dates:
        # 找到最后一个放量日期的索引
        last_vol_date = vol_dates[-1]
        last_vol_idx = kline[kline['date'] == last_vol_date].index[0] if last_vol_date in kline['date'].values else -1

        # 从放量结束后开始检测红肥绿瘦
        start_idx = last_vol_idx + 1 if last_vol_idx >= 0 else None
    else:
        start_idx = None

    stage3_ok, ratio = check_red_fat_green_thin(kline, start_idx)
    result['stage3'] = {
        'satisfied': stage3_ok,
        'ratio': ratio
    }

    if not stage3_ok:
        result['reason'] = f'阶段3不满足: 红肥绿瘦比率{ratio:.2f} < {config.RED_FAT_THRESHOLD}'
        return result

    # ========== 阶段4: 回踩均线检测 ==========
    stage4_ok, distance, signal_date = check_ma_touch(kline)
    result['stage4'] = {
        'satisfied': stage4_ok,
        'distance': distance,
        'signal_date': signal_date
    }

    if not stage4_ok:
        result['reason'] = f'阶段4不满足: 距均线距离{distance:.1%} > {config.MA_TOLERANCE:.0%}'
        return result

    # ========== 全部满足，产生信号 ==========
    result['signal'] = True
    result['signal_date'] = signal_date
    result['reason'] = '全部阶段满足，出现买入机会'

    return result


# ==================== 辅助函数 ====================

def get_stage_description(result: Dict) -> str:
    """
    获取当前阶段描述

    Args:
        result: 策略检测结果

    Returns:
        str: 阶段描述文字
    """
    if result['signal']:
        return '✓ 暴力战法信号：出现买入机会'

    # 检查各阶段状态
    stage1_ok = result['stage1'].get('satisfied', False)
    stage2_ok = result['stage2'].get('satisfied', False)
    stage3_ok = result['stage3'].get('satisfied', False)
    stage4_ok = result['stage4'].get('satisfied', False)

    if stage1_ok and stage2_ok and stage3_ok:
        return '阶段3完成，等待回踩均线'
    elif stage1_ok and stage2_ok:
        return '阶段2完成，等待红肥绿瘦'
    elif stage1_ok:
        return '阶段1完成，等待放量吸筹'
    else:
        return '不符合暴力战法条件'


# ==================== 测试代码 ====================

if __name__ == '__main__':
    print("=" * 50)
    print("策略检测模块测试")
    print("=" * 50)

    # 创建模拟数据测试
    import data_fetcher

    # 测试单只股票
    test_code = '300001'
    print(f"\n测试股票: {test_code}")

    kline = data_fetcher.get_stock_kline(test_code, start_date='20240101', end_date='20260411')
    print(f"获取K线: {len(kline)} 条")

    if not kline.empty:
        # 执行策略检测
        result = detect_strategy_signal(kline)
        print(f"\n检测结果:")
        print(f"  信号: {result['signal']}")
        print(f"  原因: {result['reason']}")
        print(f"  阶段1: {result['stage1']}")
        print(f"  阶段2: {result['stage2']}")
        print(f"  阶段3: {result['stage3']}")
        print(f"  阶段4: {result['stage4']}")