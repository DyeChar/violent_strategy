"""
信号检测模块
整合4阶段顺序检测，判断是否触发买入信号

检测顺序（严格顺序）：
1. 放量吸筹检测 → 满足才继续
2. 震荡回调红肥绿瘦检测 → 满足才继续
3. 回踩MA20检测 → 满足才继续
4. 底分型确认检测 → 全部满足则触发信号

重要：此函数是监控和回测共用的核心函数
- 检测某日信号只用该日及之前数据
- 绝不含未来数据
"""

import pandas as pd
from typing import Dict, Optional
from datetime import datetime

import config
from .stages import detect_stage1_volume, detect_stage2_oscillation, detect_stage3_ma20, detect_stage4_fractal


def detect_signal(kline: pd.DataFrame, date: str = None) -> Dict:
    """
    检测策略信号（监控/回测共用）

    Args:
        kline: K线数据（必须包含到检测日期为止的数据）
        date: 检测日期 YYYY-MM-DD（默认用最后一天）

    Returns:
        Dict: {
            'signal': bool,  # 是否触发信号
            'signal_date': str,  # 信号日期
            'code': str,  # 股票代码
            'name': str,  # 股票名称
            'close': float,  # 收盘价
            'ma20': float,  # MA20值
            'stage1': dict,  # 阶段1检测结果
            'stage2': dict,  # 阶段2检测结果
            'stage3': dict,  # 阶段3检测结果
            'stage4': dict,  # 阶段4检测结果
            'reason': str  # 检测结果说明
        }
    """
    if kline.empty or len(kline) < config.MA20_PERIOD + 10:
        return {
            'signal': False,
            'signal_date': date or '',
            'code': '',
            'name': '',
            'reason': '数据不足'
        }

    # 确定检测日期和索引
    if date:
        # 找到该日期对应的索引
        kline_dates = kline['date'].astype(str).str.strip()
        matching_idx = kline_dates[kline_dates == date].index

        if len(matching_idx) == 0:
            return {
                'signal': False,
                'signal_date': date,
                'code': kline['code'].iloc[-1] if 'code' in kline.columns else '',
                'name': kline['name'].iloc[-1] if 'name' in kline.columns else '',
                'reason': f'日期{date}不在K线数据中'
            }

        signal_idx = kline.index.get_loc(matching_idx[0])
    else:
        signal_idx = len(kline) - 1
        date = kline['date'].iloc[signal_idx]

    # 获取股票信息
    code = kline['code'].iloc[signal_idx] if 'code' in kline.columns else ''
    name = kline['name'].iloc[signal_idx] if 'name' in kline.columns else ''

    # 初始化结果
    result = {
        'signal': False,
        'signal_date': date,
        'signal_idx': signal_idx,
        'code': code,
        'name': name,
        'close': round(kline['close'].iloc[signal_idx], 2),
        'stage1': {},
        'stage2': {},
        'stage3': {},
        'stage4': {},
        'reason': ''
    }

    # ========== 阶段1：放量吸筹检测 ==========
    stage1 = detect_stage1_volume(kline, signal_idx)
    result['stage1'] = stage1

    if not stage1['satisfied']:
        result['reason'] = f'阶段1不满足: {stage1["reason"]}'
        return result

    # ========== 阶段2：震荡回调红肥绿瘦检测 ==========
    stage2 = detect_stage2_oscillation(kline, stage1['end_idx'], signal_idx)
    result['stage2'] = stage2

    if not stage2['satisfied']:
        result['reason'] = f'阶段2不满足: {stage2["reason"]}'
        return result

    # ========== 阶段3：回踩MA20检测 ==========
    stage3 = detect_stage3_ma20(kline, signal_idx)
    result['stage3'] = stage3
    result['ma20'] = stage3['ma20']

    if not stage3['satisfied']:
        result['reason'] = f'阶段3不满足: {stage3["reason"]}'
        return result

    # ========== 阶段4：底分型确认检测 ==========
    stage4 = detect_stage4_fractal(kline, signal_idx)
    result['stage4'] = stage4

    if not stage4['satisfied']:
        result['reason'] = f'阶段4不满足: {stage4["reason"]}'
        return result

    # ========== 全部满足，触发信号 ==========
    result['signal'] = True
    result['reason'] = '4阶段全部满足，触发买入信号'

    # 记录额外信息
    result['volume_days'] = stage1['volume_days']
    result['volume_dates'] = stage1['volume_dates']
    result['red_green_ratio'] = stage2['red_green_ratio']
    result['fractal_low'] = stage4['fractal_low']

    return result


def batch_detect_signals(klines: Dict[str, pd.DataFrame], date: str = None, verbose: bool = True) -> list:
    """
    批量检测信号（用于回测/监控）

    Args:
        klines: {股票代码: K线DataFrame}
        date: 检测日期（默认用各K线的最后一天）
        verbose: 是否打印进度

    Returns:
        List[Dict]: 信号列表
    """
    signals = []

    for code, kline in klines.items():
        # 获取股票名称
        name = kline['name'].iloc[-1] if 'name' in kline.columns and not kline.empty else ''
        display_name = f"{code} {name}" if name else code

        if verbose:
            print(f"  检测 {display_name}...", end=' ')

        result = detect_signal(kline, date)

        if result['signal']:
            # 补充名称（如果之前没有）
            if not result['name'] and name:
                result['name'] = name
            signals.append(result)
            if verbose:
                signal_name = result.get('name', name)
                signal_display = f"{result['code']} {signal_name}" if signal_name else result['code']
                print(f"✓ 发现信号! {signal_display} 日期: {result['signal_date']}")
        else:
            if verbose:
                print(f"× {result['reason']}")

    return signals


def get_stage_progress(result: Dict) -> str:
    """
    获取股票当前所处的阶段进度

    Args:
        result: detect_signal的结果

    Returns:
        str: 阶段进度描述
    """
    if result['signal']:
        return '✓ 触发信号'

    stage1_ok = result['stage1'].get('satisfied', False)
    stage2_ok = result['stage2'].get('satisfied', False)
    stage3_ok = result['stage3'].get('satisfied', False)
    stage4_ok = result['stage4'].get('satisfied', False)

    if stage1_ok and stage2_ok and stage3_ok:
        return '阶段3完成，等待底分型确认'
    elif stage1_ok and stage2_ok:
        return '阶段2完成，等待回踩MA20'
    elif stage1_ok:
        return '阶段1完成，等待震荡回调'
    else:
        return '不符合策略条件'


# ==================== 测试代码 ====================

if __name__ == '__main__':
    print("=" * 50)
    print("信号检测模块测试")
    print("=" * 50)

    # 创建模拟K线数据
    import numpy as np

    dates = pd.date_range('2025-01-01', periods=60, freq='D')
    test_data = pd.DataFrame({
        'date': dates.strftime('%Y-%m-%d'),
        'code': '300001',
        'name': '测试股票',
        'open': np.random.uniform(10, 12, 60),
        'close': np.random.uniform(10, 12, 60),
        'high': np.random.uniform(12, 14, 60),
        'low': np.random.uniform(8, 10, 60),
        'volume': np.random.uniform(1000000, 3000000, 60)
    })

    # 模拟放量吸筹
    test_data.loc[40:41, 'volume'] = 5000000
    test_data.loc[40:41, 'close'] = test_data.loc[40:41, 'open'] + 0.5

    # 模拟回踩MA20
    for i in range(20, 60):
        test_data.loc[i, 'close'] = 11 + np.random.uniform(-0.1, 0.1)

    # 模拟底分型
    test_data.loc[58, 'low'] = 10.5
    test_data.loc[58, 'high'] = 11.0
    test_data.loc[59, 'low'] = 10.3  # 最低
    test_data.loc[59, 'high'] = 10.8  # 最高也低
    test_data.loc[60-1, 'low'] = 10.5
    test_data.loc[60-1, 'high'] = 11.2

    # 测试信号检测
    print("\n检测最后一天信号:")
    result = detect_signal(test_data)
    print(f"信号: {result['signal']}")
    print(f"原因: {result['reason']}")
    print(f"阶段进度: {get_stage_progress(result)}")