"""
回测模块
执行暴力战法策略的历史回测
统计所有信号的交易结果和胜率等指标

买入条件（两种方案对比）：
- 方案B：接近20日线 + 底分型确认
- 方案C：接近20日线 + 底分型 + 站上5日线

卖出条件：
- 止盈20%
- 顶分型
- 放量阴线
- 跌破5日线（仅在站上5日线后生效）
"""

import pandas as pd
import numpy as np
import random
from typing import List, Dict, Tuple
from datetime import datetime, timedelta
import data_fetcher
import config
import stock_pool  # 导入预定义股票池
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
import os

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'SimHei', 'STHeiti']
plt.rcParams['axes.unicode_minus'] = False

# 回测参数
BUY_AMOUNT = 50000  # 每次买入金额
STOP_PROFIT = 0.20  # 止盈阈值20%
MIN_LOT_SIZE = 100  # 最小交易单位（一手）


def check_bottom_fractal_confirmed(kline: pd.DataFrame, today_idx: int) -> bool:
    """
    在T日收盘后，检查T-1日是否确认形成了底分型
    只使用T日及之前的数据（不含未来数据）

    底分型定义：三根K线中，中间K线最低价最低
    - T-2日（左）
    - T-1日（中）- 需要是最低的
    - T日（右）

    Args:
        kline: K线数据
        today_idx: T日的索引位置（用于确认T-1日的底分型）

    Returns:
        bool: T-1日是否是底分型的中间点
    """
    if today_idx < 2:
        return False

    # 使用T-2, T-1, T三天的数据
    low_left = kline['low'].iloc[today_idx - 2]   # T-2日最低价
    low_mid = kline['low'].iloc[today_idx - 1]    # T-1日最低价（中间点）
    low_right = kline['low'].iloc[today_idx]      # T日最低价

    # T-1日最低价是三者中最低，则确认T-1日是底分型中间点
    return low_mid < low_left and low_mid < low_right


def check_top_fractal_confirmed(kline: pd.DataFrame, today_idx: int) -> bool:
    """
    在T日收盘后，检查T-1日是否确认形成了顶分型
    只使用T日及之前的数据（不含未来数据）

    顶分型定义：三根K线中，中间K线最高价最高
    - T-2日（左）
    - T-1日（中）- 需要是最高的
    - T日（右）

    Args:
        kline: K线数据
        today_idx: T日的索引位置

    Returns:
        bool: T-1日是否是顶分型的中间点
    """
    if today_idx < 2:
        return False

    high_left = kline['high'].iloc[today_idx - 2]   # T-2日最高价
    high_mid = kline['high'].iloc[today_idx - 1]    # T-1日最高价（中间点）
    high_right = kline['high'].iloc[today_idx]      # T日最高价

    return high_mid > high_left and high_mid > high_right


def get_random_chinext_stocks(n: int = 20, seed: int = 42) -> List[str]:
    """
    从创业板股票池随机选取n只股票

    Args:
        n: 选取数量
        seed: 随机种子（便于复现）

    Returns:
        List[str]: 股票代码列表
    """
    all_stocks = stock_pool.创业板龙头池  # 使用预定义的创业板龙头池
    random.seed(seed)
    sample = random.sample(all_stocks, min(n, len(all_stocks)))
    return sample


def find_signals_plan_b(kline: pd.DataFrame) -> List[Dict]:
    """
    方案B：接近20日线 + 底分型确认买入
    【重要】只使用T日及之前的数据，不含未来数据

    在T日收盘后判断：
    - T-1日是否形成底分型（需要T-2, T-1, T三天的数据）
    - 其他条件使用T日及之前的数据

    Args:
        kline: K线数据

    Returns:
        List[Dict]: 信号列表，信号日期为T日（确认日）
    """
    if kline.empty or len(kline) < 30:
        return []

    # 计算辅助指标
    kline['ma5'] = kline['close'].rolling(5).mean()
    kline['ma20'] = kline['close'].rolling(20).mean()
    kline['vol_ma5'] = kline['volume'].rolling(5).mean()
    kline['is_up'] = kline['close'] > kline['open']
    kline['is_volume_breakout'] = kline['volume'] >= kline['vol_ma5'] * 2.0
    kline['is_volume_up'] = kline['is_up'] & kline['is_volume_breakout']

    signals = []

    # 从第30天开始（确保MA20已稳定），不再需要-3的偏移（因为不再用未来数据）
    for i in range(30, len(kline)):
        date = kline['date'].iloc[i]
        close = kline['close'].iloc[i]
        ma20 = kline['ma20'].iloc[i]

        if pd.isna(ma20):
            continue

        # 条件1: T日接近20日均线（±3%）
        ma_distance = abs(close - ma20) / ma20
        if ma_distance > 0.03:
            continue

        # 条件2: 检查T日之前（不含T日）是否有放量吸筹
        # 往前看25天到T-1日
        lookback_start = max(0, i - 25)
        lookback_end = i  # 不包含T日，只看之前的数据

        check_data = kline.iloc[lookback_start:lookback_end]

        # 找连续放量上涨天数（从最近往前看）
        consecutive_vol = 0
        for j in range(len(check_data) - 1, -1, -1):
            if check_data['is_volume_up'].iloc[j]:
                consecutive_vol += 1
            else:
                if consecutive_vol >= 2:
                    break
                consecutive_vol = 0

        if consecutive_vol < 2:
            continue

        # 条件3: 检查T-1日是否形成底分型（在T日确认）
        # 使用T-2, T-1, T三天的数据，不含未来数据
        if not check_bottom_fractal_confirmed(kline, i):
            continue

        # 条件4: 检查红肥绿瘦（放量后的震荡期）
        # 找到最后一次放量上涨的日期
        last_vol_idx = lookback_end - 1
        for j in range(lookback_end - 1, lookback_start - 1, -1):
            if kline['is_volume_up'].iloc[j]:
                last_vol_idx = j
                break

        # 震荡期：从最后一次放量到T-1日（底分型中间点）
        oscillation_data = kline.iloc[last_vol_idx + 1:i]  # 不含T日

        if len(oscillation_data) >= 2:
            up_days = oscillation_data[oscillation_data['is_up']]
            down_days = oscillation_data[~oscillation_data['is_up']]

            if len(up_days) > 0 and len(down_days) > 0:
                up_vol_avg = up_days['volume'].mean()
                down_vol_avg = down_days['volume'].mean()
                red_green_ratio = up_vol_avg / down_vol_avg if down_vol_avg > 0 else 0

                if red_green_ratio >= 1.2:  # 放宽到1.2
                    signals.append({
                        'signal_date': date,
                        'signal_idx': i,
                        'close': close,
                        'ma20': ma20,
                        'ma5': kline['ma5'].iloc[i],
                        'volume_days': consecutive_vol,
                        'red_green_ratio': red_green_ratio,
                        'plan': 'B'
                    })

    return signals


def find_signals_plan_c(kline: pd.DataFrame) -> List[Dict]:
    """
    方案C：接近20日线 + 底分型 + 站上5日线买入（最严格）
    【重要】只使用T日及之前的数据，不含未来数据

    在T日收盘后判断：
    - T-1日是否形成底分型（需要T-2, T-1, T三天的数据）
    - T日是否站上5日线
    - 其他条件使用T日及之前的数据

    Args:
        kline: K线数据

    Returns:
        List[Dict]: 信号列表，信号日期为T日（确认日）
    """
    if kline.empty or len(kline) < 30:
        return []

    # 计算辅助指标
    kline['ma5'] = kline['close'].rolling(5).mean()
    kline['ma20'] = kline['close'].rolling(20).mean()
    kline['vol_ma5'] = kline['volume'].rolling(5).mean()
    kline['is_up'] = kline['close'] > kline['open']
    kline['is_volume_breakout'] = kline['volume'] >= kline['vol_ma5'] * 2.0
    kline['is_volume_up'] = kline['is_up'] & kline['is_volume_breakout']

    signals = []

    # 从第30天开始，不再需要-3的偏移
    for i in range(30, len(kline)):
        date = kline['date'].iloc[i]
        close = kline['close'].iloc[i]
        ma5 = kline['ma5'].iloc[i]
        ma20 = kline['ma20'].iloc[i]

        if pd.isna(ma5) or pd.isna(ma20):
            continue

        # 条件1: T日接近20日均线（±3%）
        ma_distance = abs(close - ma20) / ma20
        if ma_distance > 0.03:
            continue

        # 条件2: T日站上5日线（收盘价 > 5日线）
        if close <= ma5:
            continue

        # 条件3: 检查T日之前是否有放量吸筹
        lookback_start = max(0, i - 25)
        lookback_end = i  # 不包含T日

        check_data = kline.iloc[lookback_start:lookback_end]

        consecutive_vol = 0
        for j in range(len(check_data) - 1, -1, -1):
            if check_data['is_volume_up'].iloc[j]:
                consecutive_vol += 1
            else:
                if consecutive_vol >= 2:
                    break
                consecutive_vol = 0

        if consecutive_vol < 2:
            continue

        # 条件4: 检查T-1日是否形成底分型（在T日确认）
        if not check_bottom_fractal_confirmed(kline, i):
            continue

        # 条件5: 检查红肥绿瘦
        last_vol_idx = lookback_end - 1
        for j in range(lookback_end - 1, lookback_start - 1, -1):
            if kline['is_volume_up'].iloc[j]:
                last_vol_idx = j
                break

        oscillation_data = kline.iloc[last_vol_idx + 1:i]  # 不含T日

        if len(oscillation_data) >= 2:
            up_days = oscillation_data[oscillation_data['is_up']]
            down_days = oscillation_data[~oscillation_data['is_up']]

            if len(up_days) > 0 and len(down_days) > 0:
                up_vol_avg = up_days['volume'].mean()
                down_vol_avg = down_days['volume'].mean()
                red_green_ratio = up_vol_avg / down_vol_avg if down_vol_avg > 0 else 0

                if red_green_ratio >= 1.2:
                    signals.append({
                        'signal_date': date,
                        'signal_idx': i,
                        'close': close,
                        'ma20': ma20,
                        'ma5': ma5,
                        'volume_days': consecutive_vol,
                        'red_green_ratio': red_green_ratio,
                        'plan': 'C'
                    })

    return signals


def find_all_signals(kline: pd.DataFrame) -> List[Dict]:
    """
    扫描K线历史数据，找出所有符合暴力战法信号的日期

    Args:
        kline: K线数据

    Returns:
        List[Dict]: 信号列表，每个信号包含日期、索引位置等信息
    """
    if kline.empty or len(kline) < 30:
        return []

    # 计算辅助指标
    kline['ma5'] = kline['close'].rolling(5).mean()
    kline['ma10'] = kline['close'].rolling(10).mean()
    kline['ma20'] = kline['close'].rolling(20).mean()
    kline['vol_ma5'] = kline['volume'].rolling(5).mean()
    kline['is_up'] = kline['close'] > kline['open']
    kline['change_pct'] = (kline['close'] - kline['close'].shift(1)) / kline['close'].shift(1)

    # 放量判断：成交量 >= 2倍5日均量
    kline['is_volume_breakout'] = kline['volume'] >= kline['vol_ma5'] * 2.0
    kline['is_volume_up'] = kline['is_up'] & kline['is_volume_breakout']

    signals = []

    for i in range(30, len(kline) - 5):  # 需要后续数据来模拟卖出
        date = kline['date'].iloc[i]
        close = kline['close'].iloc[i]
        ma20 = kline['ma20'].iloc[i]
        change_pct = kline['change_pct'].iloc[i]

        if pd.isna(ma20):
            continue

        # 条件1: 回踩20日均线（±2%）
        ma_distance = abs(close - ma20) / ma20
        if ma_distance > 0.02:
            continue

        # 条件2: 回踩当日应该是下跌或微涨
        if change_pct > 0.03:
            continue

        # 条件3: 检查之前15天内是否有连续放量上涨（>=2天）
        lookback_start = max(0, i - 20)
        lookback_end = i - 2

        check_data = kline.iloc[lookback_start:lookback_end]

        # 找连续放量上涨天数
        consecutive_vol = 0
        for j in range(len(check_data) - 1, -1, -1):
            if check_data['is_volume_up'].iloc[j]:
                consecutive_vol += 1
            else:
                if consecutive_vol >= 2:
                    break
                consecutive_vol = 0

        if consecutive_vol < 2:
            continue

        # 条件4: 检查红肥绿瘦（放量后的震荡期）
        # 找到最后一个放量日期的索引
        last_vol_idx = lookback_end
        for j in range(lookback_end - 1, lookback_start - 1, -1):
            if kline['is_volume_up'].iloc[j]:
                last_vol_idx = j
                break

        oscillation_data = kline.iloc[last_vol_idx + 1:i]

        if len(oscillation_data) >= 2:
            up_days = oscillation_data[oscillation_data['is_up']]
            down_days = oscillation_data[~oscillation_data['is_up']]

            if len(up_days) > 0 and len(down_days) > 0:
                up_vol_avg = up_days['volume'].mean()
                down_vol_avg = down_days['volume'].mean()
                red_green_ratio = up_vol_avg / down_vol_avg if down_vol_avg > 0 else 0

                if red_green_ratio >= 1.3:
                    signals.append({
                        'signal_date': date,
                        'signal_idx': i,
                        'close': close,
                        'ma20': ma20,
                        'ma_distance': ma_distance,
                        'volume_days': consecutive_vol,
                        'red_green_ratio': red_green_ratio
                    })

    return signals


def check_sell_signal(kline: pd.DataFrame, check_idx: int, buy_price: float, above_ma5: bool = False,
                      top_fractal_mode: str = 'original') -> Tuple[bool, str, bool]:
    """
    检查卖出信号

    Args:
        kline: K线数据
        check_idx: 检查的索引位置
        buy_price: 买入价格
        above_ma5: 是否已经站上5日线（只有站上后才判断跌破5日线）
        top_fractal_mode: 顶分型判断模式
            - 'original': 原始版本（三根K线中间最高）
            - 'strict_a': 收盘跌破前日收盘才触发
            - 'strict_b': 近5天最高点才触发
            - 'strict_c': 放量配合才触发
            - 'strict_ab': a+b两个条件
            - 'strict_ac': a+c两个条件
            - 'strict_bc': b+c两个条件
            - 'strict_abc': a+b+c三个条件
            - 'disabled': 禁用顶分型卖出

    Returns:
        Tuple[bool, str, bool]: (是否卖出, 卖出原因, 是否已站上5日线)
    """
    close = kline['close'].iloc[check_idx]
    open_price = kline['open'].iloc[check_idx]
    high = kline['high'].iloc[check_idx]
    volume = kline['volume'].iloc[check_idx]
    ma5 = kline['ma5'].iloc[check_idx]
    vol_ma5 = kline['vol_ma5'].iloc[check_idx]

    # 检查是否站上5日线（收盘价 > 5日线）
    now_above_ma5 = close > ma5

    # 收益率
    profit_ratio = (close - buy_price) / buy_price

    # 卖出条件1: 止盈
    if profit_ratio >= STOP_PROFIT:
        return True, '止盈', now_above_ma5

    # 卖出条件2: 跌破5日线（仅在已站上5日线后生效）
    if above_ma5 and close < ma5:
        return True, '跌破5日线', now_above_ma5

    # 卖出条件3: 顶分型（根据mode选择不同判断）
    if top_fractal_mode != 'disabled' and check_idx >= 5:
        high1 = kline['high'].iloc[check_idx - 2]
        high2 = kline['high'].iloc[check_idx - 1]
        high3 = kline['high'].iloc[check_idx]
        close1 = kline['close'].iloc[check_idx - 1]

        # 基础顶分型：中间K线高点最高
        if high2 > high1 and high2 > high3:
            should_sell = False

            # 条件a: 收盘价跌破前一日收盘
            cond_a = close < close1

            # 条件b: 高点是近5天最高（阶段性顶部）
            recent_highs = kline['high'].iloc[check_idx - 5:check_idx + 1]
            cond_b = high2 >= recent_highs.max() * 0.98

            # 条件c: 放量（成交量>=1.5倍均量）
            cond_c = volume >= vol_ma5 * 1.5

            # 根据mode判断是否触发
            if top_fractal_mode == 'original':
                should_sell = True  # 原始版本，只要顶分型就卖
            elif top_fractal_mode == 'strict_a':
                should_sell = cond_a
            elif top_fractal_mode == 'strict_b':
                should_sell = cond_b
            elif top_fractal_mode == 'strict_c':
                should_sell = cond_c
            elif top_fractal_mode == 'strict_ab':
                should_sell = cond_a and cond_b
            elif top_fractal_mode == 'strict_ac':
                should_sell = cond_a and cond_c
            elif top_fractal_mode == 'strict_bc':
                should_sell = cond_b and cond_c
            elif top_fractal_mode == 'strict_abc':
                should_sell = cond_a and cond_b and cond_c

            if should_sell:
                return True, '顶分型', now_above_ma5

    # 卖出条件4: 放量阴线
    if close < open_price and volume >= vol_ma5 * 2.0:
        return True, '放量阴线', now_above_ma5

    return False, '', now_above_ma5


def simulate_trade(kline: pd.DataFrame, signal: Dict, code: str, name: str,
                   top_fractal_mode: str = 'original') -> Dict:
    """
    模拟单次交易

    Args:
        kline: K线数据
        signal: 信号信息
        code: 股票代码
        name: 股票名称
        top_fractal_mode: 顶分型判断模式

    Returns:
        Dict: 交易结果
    """
    signal_idx = signal['signal_idx']
    signal_date = signal['signal_date']

    # T+1日买入
    buy_idx = signal_idx + 1
    if buy_idx >= len(kline):
        return {'valid': False, 'reason': '无后续交易日'}

    buy_date = kline['date'].iloc[buy_idx]
    buy_price = kline['open'].iloc[buy_idx]  # 开盘价买入

    # 检查买入金额是否足够一手
    shares = int(BUY_AMOUNT / buy_price)
    if shares < MIN_LOT_SIZE:
        return {'valid': False, 'reason': f'买入金额不足一手（{shares}股）'}

    # 实际买入金额
    actual_buy_amount = shares * buy_price

    # 方案C买入时已经站上5日线，可以直接判断跌破5日线
    # 方案B买入时可能未站上5日线，需要先等待站上5日线
    above_ma5 = signal.get('plan', 'B') == 'C'  # 方案C默认已站上5日线

    # 从买入后开始检查卖出信号
    for check_idx in range(buy_idx + 1, len(kline)):
        should_sell, sell_reason, now_above_ma5 = check_sell_signal(
            kline, check_idx, buy_price, above_ma5, top_fractal_mode
        )

        # 更新是否已站上5日线
        if not above_ma5 and now_above_ma5:
            above_ma5 = True

        if should_sell:
            sell_date = kline['date'].iloc[check_idx]
            sell_price = kline['close'].iloc[check_idx]  # 收盘价卖出
            actual_sell_amount = shares * sell_price

            profit_ratio = (sell_price - buy_price) / buy_price
            holding_days = check_idx - buy_idx

            return {
                'valid': True,
                'code': code,
                'name': name,
                'signal_date': signal_date,
                'buy_date': buy_date,
                'buy_price': buy_price,
                'sell_date': sell_date,
                'sell_price': sell_price,
                'sell_reason': sell_reason,
                'shares': shares,
                'holding_days': holding_days,
                'profit_ratio': profit_ratio,
                'buy_amount': actual_buy_amount,
                'sell_amount': actual_sell_amount,
                'profit_amount': actual_sell_amount - actual_buy_amount
            }

    # 没有触发卖出信号，按最后一天收盘价卖出
    last_idx = len(kline) - 1
    sell_date = kline['date'].iloc[last_idx]
    sell_price = kline['close'].iloc[last_idx]
    actual_sell_amount = shares * sell_price

    profit_ratio = (sell_price - buy_price) / buy_price
    holding_days = last_idx - buy_idx

    return {
        'valid': True,
        'code': code,
        'name': name,
        'signal_date': signal_date,
        'buy_date': buy_date,
        'buy_price': buy_price,
        'sell_date': sell_date,
        'sell_price': sell_price,
        'sell_reason': '回测结束',
        'shares': shares,
        'holding_days': holding_days,
        'profit_ratio': profit_ratio,
        'buy_amount': actual_buy_amount,
        'sell_amount': actual_sell_amount,
        'profit_amount': actual_sell_amount - actual_buy_amount
    }


def calculate_stats(trades: List[Dict]) -> Dict:
    """
    计算回测统计指标

    Args:
        trades: 交易列表

    Returns:
        Dict: 统计结果
    """
    if not trades:
        return {
            'total_trades': 0,
            'win_rate': 0,
            'avg_profit': 0,
            'avg_holding_days': 0,
            'profit_loss_ratio': 0,
            'max_profit': 0,
            'max_loss': 0,
            'total_profit_amount': 0,
            'total_invested': 0,
            'total_return': 0
        }

    valid_trades = [t for t in trades if t['valid']]
    profits = [t['profit_ratio'] for t in valid_trades]
    holding_days = [t['holding_days'] for t in valid_trades]
    profit_amounts = [t['profit_amount'] for t in valid_trades]
    buy_amounts = [t['buy_amount'] for t in valid_trades]

    # 胜率
    win_trades = [p for p in profits if p > 0]
    win_rate = len(win_trades) / len(profits) if profits else 0

    # 平均收益率
    avg_profit = np.mean(profits) if profits else 0

    # 平均持股天数
    avg_holding_days = np.mean(holding_days) if holding_days else 0

    # 盈亏比（平均盈利/平均亏损）
    avg_win = np.mean([p for p in profits if p > 0]) if win_trades else 0
    avg_loss = abs(np.mean([p for p in profits if p < 0])) if any(p < 0 for p in profits) else 0
    profit_loss_ratio = avg_win / avg_loss if avg_loss > 0 else float('inf') if avg_win > 0 else 0

    # 最大单笔盈亏
    max_profit = max(profits) if profits else 0
    max_loss = min(profits) if profits else 0

    # 累计收益
    total_profit_amount = sum(profit_amounts)
    total_invested = sum(buy_amounts)
    total_return = total_profit_amount / total_invested if total_invested > 0 else 0

    return {
        'total_trades': len(valid_trades),
        'win_rate': win_rate,
        'avg_profit': avg_profit,
        'avg_holding_days': avg_holding_days,
        'profit_loss_ratio': profit_loss_ratio,
        'max_profit': max_profit,
        'max_loss': max_loss,
        'total_profit_amount': total_profit_amount,
        'total_invested': total_invested,
        'total_return': total_return
    }


def plot_stock_signals(kline: pd.DataFrame, trades: List[Dict], code: str, name: str,
                       plan: str, save_dir: str = None) -> str:
    """
    绘制股票K线图并标注买入/卖出信号

    Args:
        kline: K线数据
        trades: 交易记录列表
        code: 股票代码
        name: 股票名称
        plan: 方案名称 (B 或 C)
        save_dir: 保存目录

    Returns:
        str: 保存的图片路径
    """
    # 过滤该股票的交易记录
    stock_trades = [t for t in trades if t['valid'] and t['code'] == code]

    if not stock_trades:
        return None

    # 计算均线
    kline['ma5'] = kline['close'].rolling(5).mean()
    kline['ma20'] = kline['close'].rolling(20).mean()

    # 创建图形
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10), height_ratios=[3, 1])

    # 准备数据
    dates = range(len(kline))
    closes = kline['close'].values
    opens = kline['open'].values
    highs = kline['high'].values
    lows = kline['low'].values
    volumes = kline['volume'].values

    # 绘制K线
    for i in range(len(kline)):
        color = 'red' if closes[i] >= opens[i] else 'green'
        # 绘制实体
        height = abs(closes[i] - opens[i])
        bottom = min(closes[i], opens[i])
        ax1.add_patch(Rectangle((i - 0.3, bottom), 0.6, height, facecolor=color, edgecolor=color))
        # 绘制上下影线
        ax1.plot([i, i], [lows[i], min(closes[i], opens[i])], color=color, linewidth=1)
        ax1.plot([i, i], [max(closes[i], opens[i]), highs[i]], color=color, linewidth=1)

    # 绘制均线
    ax1.plot(dates, kline['ma5'].values, 'b-', linewidth=1.5, label='MA5', alpha=0.8)
    ax1.plot(dates, kline['ma20'].values, 'orange', linewidth=1.5, label='MA20', alpha=0.8)

    # 标注买入/卖出点
    for trade in stock_trades:
        # 找到买入日期的索引
        buy_idx = None
        sell_idx = None
        for i, d in enumerate(kline['date'].values):
            if d == trade['buy_date']:
                buy_idx = i
            if d == trade['sell_date']:
                sell_idx = i

        if buy_idx is not None:
            # 买入点（蓝色向上箭头）
            ax1.annotate('买', (buy_idx, trade['buy_price']),
                        xytext=(buy_idx, trade['buy_price'] * 0.97),
                        fontsize=10, color='blue', ha='center',
                        arrowprops=dict(arrowstyle='->', color='blue'))

        if sell_idx is not None:
            # 卖出点（根据盈亏选择颜色）
            sell_color = 'red' if trade['profit_ratio'] > 0 else 'green'
            sell_text = f"卖({trade['sell_reason']})"
            ax1.annotate(sell_text, (sell_idx, trade['sell_price']),
                        xytext=(sell_idx, trade['sell_price'] * 1.03),
                        fontsize=9, color=sell_color, ha='center',
                        arrowprops=dict(arrowstyle='->', color=sell_color))

    # 设置标题和标签
    ax1.set_title(f'{code} {name} - 方案{plan} 信号可视化', fontsize=14)
    ax1.set_ylabel('价格', fontsize=12)
    ax1.legend(loc='upper left')
    ax1.grid(True, alpha=0.3)

    # 设置x轴范围
    ax1.set_xlim(-5, len(kline) + 5)

    # 绘制成交量
    colors = ['red' if closes[i] >= opens[i] else 'green' for i in range(len(kline))]
    ax2.bar(dates, volumes, color=colors, width=0.6, alpha=0.7)
    ax2.set_ylabel('成交量', fontsize=12)
    ax2.grid(True, alpha=0.3)
    ax2.set_xlim(-5, len(kline) + 5)

    # 添加交易信息文本框
    info_text = ""
    for trade in stock_trades:
        profit_pct = trade['profit_ratio'] * 100
        info_text += f"{trade['buy_date']}→{trade['sell_date']} | {trade['sell_reason']} | {profit_pct:.1f}%\n"

    ax1.text(0.02, 0.98, info_text.strip(), transform=ax1.transAxes,
            fontsize=10, verticalalignment='top',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

    plt.tight_layout()

    # 保存图片
    if save_dir is None:
        save_dir = os.path.join(config.OUTPUT_DIR, 'charts', f'plan_{plan}')

    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, f'{code}_{plan}.png')
    plt.savefig(save_path, dpi=150, bbox_inches='tight')
    plt.close()

    return save_path


def plot_stock_signals_combined(kline: pd.DataFrame, trades_b: List[Dict], trades_c: List[Dict],
                                 code: str, name: str, save_dir: str = None) -> str:
    """
    绘制股票K线图，同时标注方案B和方案C的信号

    Args:
        kline: K线数据
        trades_b: 方案B交易记录
        trades_c: 方案C交易记录
        code: 股票代码
        name: 股票名称
        save_dir: 保存目录

    Returns:
        str: 保存的图片路径
    """
    # 过滤该股票的交易记录
    stock_trades_b = [t for t in trades_b if t['valid'] and t['code'] == code]
    stock_trades_c = [t for t in trades_c if t['valid'] and t['code'] == code]

    if not stock_trades_b and not stock_trades_c:
        return None

    # 计算均线
    kline['ma5'] = kline['close'].rolling(5).mean()
    kline['ma20'] = kline['close'].rolling(20).mean()

    # 创建图形
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10), height_ratios=[3, 1])

    # 准备数据
    dates = range(len(kline))
    closes = kline['close'].values
    opens = kline['open'].values
    highs = kline['high'].values
    lows = kline['low'].values
    volumes = kline['volume'].values

    # 绘制K线
    for i in range(len(kline)):
        color = 'red' if closes[i] >= opens[i] else 'green'
        height = abs(closes[i] - opens[i])
        bottom = min(closes[i], opens[i])
        ax1.add_patch(Rectangle((i - 0.3, bottom), 0.6, height, facecolor=color, edgecolor=color))
        ax1.plot([i, i], [lows[i], min(closes[i], opens[i])], color=color, linewidth=1)
        ax1.plot([i, i], [max(closes[i], opens[i]), highs[i]], color=color, linewidth=1)

    # 绘制均线
    ax1.plot(dates, kline['ma5'].values, 'b-', linewidth=1.5, label='MA5', alpha=0.8)
    ax1.plot(dates, kline['ma20'].values, 'orange', linewidth=1.5, label='MA20', alpha=0.8)

    # 标注方案B信号（蓝色）
    for trade in stock_trades_b:
        buy_idx = sell_idx = None
        for i, d in enumerate(kline['date'].values):
            if d == trade['buy_date']:
                buy_idx = i
            if d == trade['sell_date']:
                sell_idx = i

        if buy_idx is not None:
            ax1.scatter([buy_idx], [trade['buy_price']], marker='^', s=100, c='blue', zorder=5, label='_nolegend_')
            ax1.annotate(f'B买', (buy_idx, trade['buy_price']),
                        xytext=(buy_idx - 2, trade['buy_price'] * 0.95),
                        fontsize=9, color='blue', ha='center')

        if sell_idx is not None:
            ax1.scatter([sell_idx], [trade['sell_price']], marker='v', s=100, c='purple', zorder=5)
            profit_pct = trade['profit_ratio'] * 100
            ax1.annotate(f'B卖({profit_pct:.1f}%)', (sell_idx, trade['sell_price']),
                        xytext=(sell_idx + 2, trade['sell_price'] * 1.05),
                        fontsize=9, color='purple', ha='center')

    # 标注方案C信号（红色）
    for trade in stock_trades_c:
        buy_idx = sell_idx = None
        for i, d in enumerate(kline['date'].values):
            if d == trade['buy_date']:
                buy_idx = i
            if d == trade['sell_date']:
                sell_idx = i

        if buy_idx is not None:
            ax1.scatter([buy_idx], [trade['buy_price']], marker='^', s=150, c='red', zorder=6)
            ax1.annotate(f'C买', (buy_idx, trade['buy_price']),
                        xytext=(buy_idx + 2, trade['buy_price'] * 0.95),
                        fontsize=9, color='red', ha='center')

        if sell_idx is not None:
            ax1.scatter([sell_idx], [trade['sell_price']], marker='v', s=150, c='darkred', zorder=6)
            profit_pct = trade['profit_ratio'] * 100
            ax1.annotate(f'C卖({profit_pct:.1f}%)', (sell_idx, trade['sell_price']),
                        xytext=(sell_idx - 2, trade['sell_price'] * 1.05),
                        fontsize=9, color='darkred', ha='center')

    # 设置标题
    ax1.set_title(f'{code} {name} - 方案B({len(stock_trades_b)}次) vs 方案C({len(stock_trades_c)}次)', fontsize=14)
    ax1.set_ylabel('价格', fontsize=12)
    ax1.legend(['MA5', 'MA20'], loc='upper left')
    ax1.grid(True, alpha=0.3)
    ax1.set_xlim(-5, len(kline) + 5)

    # 绘制成交量
    colors = ['red' if closes[i] >= opens[i] else 'green' for i in range(len(kline))]
    ax2.bar(dates, volumes, color=colors, width=0.6, alpha=0.7)
    ax2.set_ylabel('成交量', fontsize=12)
    ax2.grid(True, alpha=0.3)
    ax2.set_xlim(-5, len(kline) + 5)

    # 添加交易汇总
    info_lines = []
    if stock_trades_b:
        info_lines.append(f"【方案B】{len(stock_trades_b)}次交易")
        for t in stock_trades_b:
            info_lines.append(f"  {t['buy_date']}→{t['sell_date']} | {t['sell_reason']} | {t['profit_ratio']*100:.1f}%")
    if stock_trades_c:
        info_lines.append(f"【方案C】{len(stock_trades_c)}次交易")
        for t in stock_trades_c:
            info_lines.append(f"  {t['buy_date']}→{t['sell_date']} | {t['sell_reason']} | {t['profit_ratio']*100:.1f}%")

    ax1.text(0.02, 0.98, '\n'.join(info_lines), transform=ax1.transAxes,
            fontsize=9, verticalalignment='top',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

    plt.tight_layout()

    # 保存图片
    if save_dir is None:
        save_dir = os.path.join(config.OUTPUT_DIR, 'charts', 'comparison')

    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, f'{code}_comparison.png')
    plt.savefig(save_path, dpi=150, bbox_inches='tight')
    plt.close()

    return save_path


def print_trades_table(trades: List[Dict]):
    """
    打印交易明细表格

    Args:
        trades: 交易列表
    """
    valid_trades = [t for t in trades if t['valid']]

    if not valid_trades:
        print("无有效交易")
        return

    print("\n" + "=" * 120)
    print("交易明细")
    print("=" * 120)

    header = f"{'信号日期':^12}{'股票代码':^10}{'股票名称':^12}{'买入日期':^12}{'买入价格':^10}{'卖出日期':^12}{'卖出价格':^10}{'卖出原因':^12}{'持有天数':^10}{'盈亏比例':^10}"
    print(header)
    print("-" * 120)

    for t in valid_trades:
        row = f"{t['signal_date']:^12}{t['code']:^10}{t['name']:^12}{t['buy_date']:^12}{t['buy_price']:^10.2f}{t['sell_date']:^12}{t['sell_price']:^10.2f}{t['sell_reason']:^12}{t['holding_days']:^10}{t['profit_ratio']:^10.1%}"
        print(row)

    print("=" * 120)


def print_stats_summary(stats: Dict):
    """
    打印统计汇总

    Args:
        stats: 统计结果
    """
    print("\n" + "=" * 60)
    print("回测统计汇总")
    print("=" * 60)

    print(f"出手次数: {stats['total_trades']} 次")
    print(f"胜率: {stats['win_rate']:.1%}")
    print(f"平均收益率: {stats['avg_profit']:.1%}")
    print(f"平均持股天数: {stats['avg_holding_days']:.1f} 天")
    print(f"盈亏比: {stats['profit_loss_ratio']:.2f}")
    print(f"最大单笔盈利: {stats['max_profit']:.1%}")
    print(f"最大单笔亏损: {stats['max_loss']:.1%}")
    print(f"累计投入资金: {stats['total_invested']:,.0f} 元")
    print(f"累计盈利金额: {stats['total_profit_amount']:,.0f} 元")
    print(f"累计收益率: {stats['total_return']:.1%}")

    print("=" * 60)


def run_backtest(stock_codes: List[str] = None,
                 sample_size: int = 20,
                 start_date: str = '20250411',
                 end_date: str = '20260411',
                 seed: int = 42,
                 plan: str = 'B') -> Tuple[List[Dict], Dict]:
    """
    执行回测

    Args:
        stock_codes: 股票代码列表（默认随机选取）
        sample_size: 随机选取数量
        start_date: 起始日期
        end_date: 结束日期
        seed: 随机种子
        plan: 策略方案 ('B' 或 'C')
            - B: 接近20日线 + 底分型确认
            - C: 接近20日线 + 底分型 + 站上5日线

    Returns:
        Tuple[List[Dict], Dict]: (交易列表, 统计结果)
    """
    # 获取股票池
    if stock_codes is None:
        stock_codes = get_random_chinext_stocks(sample_size, seed)

    # 选择信号检测函数
    if plan == 'C':
        find_signals_func = find_signals_plan_c
        plan_desc = "方案C: 接近20日线 + 底分型 + 站上5日线"
    else:
        find_signals_func = find_signals_plan_b
        plan_desc = "方案B: 接近20日线 + 底分型确认"

    print("\n" + "=" * 60)
    print("暴力战法回测系统")
    print("=" * 60)
    print(f"策略方案: {plan_desc}")
    print(f"股票池: 创业板随机{len(stock_codes)}只")
    print(f"回测时间: {start_date} - {end_date}")
    print(f"买入金额: {BUY_AMOUNT}元/次")
    print(f"止盈阈值: {STOP_PROFIT:.0%}")
    print("=" * 60)

    # 执行回测
    all_trades = []

    print(f"\n开始获取K线数据...")
    kline_data = data_fetcher.batch_fetch_klines(
        stock_codes,
        start_date=start_date,
        end_date=end_date,
        delay=0.3
    )

    print(f"\n开始扫描信号和模拟交易...")
    for code, kline in kline_data.items():
        name = kline['name'].iloc[-1] if 'name' in kline.columns else f'股票{code}'

        # 找信号（使用指定方案）
        signals = find_signals_func(kline)

        if signals:
            print(f"  {code} {name}: 发现 {len(signals)} 个信号")
        else:
            print(f"  {code} {name}: 无信号")

        # 模拟交易
        for signal in signals:
            trade = simulate_trade(kline, signal, code, name)
            all_trades.append(trade)

    # 计算统计
    stats = calculate_stats(all_trades)
    stats['plan'] = plan

    # 打印结果
    print_trades_table(all_trades)
    print_stats_summary(stats)

    return all_trades, stats


def compare_plans(stock_codes: List[str] = None,
                  sample_size: int = 20,
                  start_date: str = '20250411',
                  end_date: str = '20260411',
                  seed: int = 42,
                  save_charts: bool = True) -> Dict:
    """
    对比两种策略方案的回测效果

    Args:
        stock_codes: 股票代码列表
        sample_size: 随机选取数量
        start_date: 起始日期
        end_date: 结束日期
        seed: 随机种子
        save_charts: 是否保存可视化图表

    Returns:
        Dict: 包含两种方案的对比结果
    """
    # 获取股票池
    if stock_codes is None:
        stock_codes = get_random_chinext_stocks(sample_size, seed)

    print("\n" + "=" * 70)
    print("暴力战法策略对比回测")
    print("=" * 70)
    print(f"股票池: 创业板{len(stock_codes)}只")
    print(f"回测时间: {start_date} - {end_date}")
    print("=" * 70)

    # 先获取所有K线数据（避免重复请求）
    print(f"\n获取K线数据...")
    kline_data = data_fetcher.batch_fetch_klines(
        stock_codes,
        start_date=start_date,
        end_date=end_date,
        delay=0.5
    )

    results = {}

    # 方案B回测
    print("\n" + "=" * 60)
    print("方案B回测")
    print("=" * 60)

    trades_b = []
    stocks_with_signals_b = []  # 记录有信号的股票
    for code, kline in kline_data.items():
        name = kline['name'].iloc[-1] if 'name' in kline.columns else f'股票{code}'
        signals = find_signals_plan_b(kline)
        if signals:
            print(f"  {code} {name}: {len(signals)} 个信号")
            stocks_with_signals_b.append((code, name, kline))
        for signal in signals:
            trade = simulate_trade(kline, signal, code, name)
            trades_b.append(trade)

    stats_b = calculate_stats(trades_b)
    stats_b['plan'] = 'B'
    results['B'] = {'trades': trades_b, 'stats': stats_b}

    # 打印方案B详细交易记录
    print_trades_table(trades_b)
    print_stats_summary(stats_b)

    # 方案C回测
    print("\n" + "=" * 60)
    print("方案C回测")
    print("=" * 60)

    trades_c = []
    stocks_with_signals_c = []
    for code, kline in kline_data.items():
        name = kline['name'].iloc[-1] if 'name' in kline.columns else f'股票{code}'
        signals = find_signals_plan_c(kline)
        if signals:
            print(f"  {code} {name}: {len(signals)} 个信号")
            stocks_with_signals_c.append((code, name, kline))
        for signal in signals:
            trade = simulate_trade(kline, signal, code, name)
            trades_c.append(trade)

    stats_c = calculate_stats(trades_c)
    stats_c['plan'] = 'C'
    results['C'] = {'trades': trades_c, 'stats': stats_c}

    # 打印方案C详细交易记录
    print_trades_table(trades_c)
    print_stats_summary(stats_c)

    # 对比汇总
    print("\n" + "=" * 70)
    print("策略对比汇总")
    print("=" * 70)
    print(f"{'指标':<20} {'方案B':>20} {'方案C':>20}")
    print("-" * 70)
    print(f"{'出手次数':<20} {stats_b['total_trades']:>20} {stats_c['total_trades']:>20}")
    print(f"{'胜率':<20} {stats_b['win_rate']:>20.1%} {stats_c['win_rate']:>20.1%}")
    print(f"{'平均收益率':<20} {stats_b['avg_profit']:>20.1%} {stats_c['avg_profit']:>20.1%}")
    print(f"{'平均持股天数':<20} {stats_b['avg_holding_days']:>20.1f} {stats_c['avg_holding_days']:>20.1f}")
    print(f"{'盈亏比':<20} {stats_b['profit_loss_ratio']:>20.2f} {stats_c['profit_loss_ratio']:>20.2f}")
    print(f"{'最大盈利':<20} {stats_b['max_profit']:>20.1%} {stats_c['max_profit']:>20.1%}")
    print(f"{'最大亏损':<20} {stats_b['max_loss']:>20.1%} {stats_c['max_loss']:>20.1%}")
    print(f"{'累计收益率':<20} {stats_b['total_return']:>20.1%} {stats_c['total_return']:>20.1%}")
    print("=" * 70)

    # 可视化有信号的股票
    if save_charts:
        print("\n" + "=" * 60)
        print("生成可视化图表")
        print("=" * 60)

        # 找出所有有信号的股票（B或C任一）
        all_signal_stocks = set()
        for t in trades_b:
            if t['valid']:
                all_signal_stocks.add(t['code'])
        for t in trades_c:
            if t['valid']:
                all_signal_stocks.add(t['code'])

        chart_paths = []
        for code in all_signal_stocks:
            if code in kline_data:
                kline = kline_data[code]
                name = kline['name'].iloc[-1] if 'name' in kline.columns else f'股票{code}'

                # 绘制对比图
                chart_path = plot_stock_signals_combined(kline, trades_b, trades_c, code, name)
                if chart_path:
                    chart_paths.append(chart_path)
                    print(f"  保存图表: {chart_path}")

        print(f"\n共生成 {len(chart_paths)} 个图表")
        results['chart_paths'] = chart_paths

    return results


def compare_top_fractal_modes(stock_codes: List[str] = None,
                               sample_size: int = 50,
                               start_date: str = '20250101',
                               end_date: str = '20260411',
                               seed: int = 42) -> Dict:
    """
    对比不同顶分型判断模式的效果

    Args:
        stock_codes: 股票代码列表
        sample_size: 随机选取数量
        start_date: 起始日期
        end_date: 结束日期
        seed: 随机种子

    Returns:
        Dict: 各模式的对比结果
    """
    # 定义要对比的模式
    modes = [
        ('original', '原始版本（三根K线中间最高）'),
        ('strict_a', '严格a：收盘跌破前日'),
        ('strict_b', '严格b：近5天最高点'),
        ('strict_c', '严格c：放量配合'),
        ('strict_ab', '严格ab：收盘跌破+5天最高'),
        ('strict_ac', '严格ac：收盘跌破+放量'),
        ('strict_bc', '严格bc：5天最高+放量'),
        ('disabled', '禁用顶分型')
    ]

    # 获取股票池
    if stock_codes is None:
        stock_codes = get_random_chinext_stocks(sample_size, seed)

    print("\n" + "=" * 80)
    print("顶分型判断模式对比回测")
    print("=" * 80)
    print(f"股票池: 创业板{len(stock_codes)}只")
    print(f"回测时间: {start_date} - {end_date}")
    print("=" * 80)

    # 先获取所有K线数据
    print(f"\n获取K线数据...")
    kline_data = data_fetcher.batch_fetch_klines(
        stock_codes,
        start_date=start_date,
        end_date=end_date,
        delay=0.5
    )

    results = {}

    # 对每个模式进行回测
    for mode, mode_desc in modes:
        print(f"\n{'='*60}")
        print(f"模式: {mode_desc}")
        print("=" * 60)

        trades = []
        sell_reasons = {}

        for code, kline in kline_data.items():
            name = kline['name'].iloc[-1] if 'name' in kline.columns else f'股票{code}'
            signals = find_signals_plan_b(kline)  # 使用方案B信号

            for signal in signals:
                trade = simulate_trade(kline, signal, code, name, top_fractal_mode=mode)
                trades.append(trade)

                if trade['valid']:
                    reason = trade['sell_reason']
                    sell_reasons[reason] = sell_reasons.get(reason, 0) + 1

        stats = calculate_stats(trades)
        stats['mode'] = mode
        stats['sell_reasons'] = sell_reasons
        results[mode] = {'trades': trades, 'stats': stats}

        # 打印简要结果
        print(f"出手次数: {stats['total_trades']} 次")
        print(f"胜率: {stats['win_rate']:.1%}")
        print(f"平均收益率: {stats['avg_profit']:.1%}")
        print(f"平均持股天数: {stats['avg_holding_days']:.1f} 天")
        print(f"盈亏比: {stats['profit_loss_ratio']:.2f}")
        print(f"卖出原因分布: {sell_reasons}")

    # 打印对比汇总表
    print("\n" + "=" * 100)
    print("顶分型模式对比汇总")
    print("=" * 100)
    print(f"{'模式':<12} {'出手':>8} {'胜率':>8} {'平均收益':>10} {'持股天数':>10} {'盈亏比':>8} {'最大盈利':>8} {'最大亏损':>8} {'顶分型卖出次数':>12}")
    print("-" * 100)

    for mode, mode_desc in modes:
        stats = results[mode]['stats']
        top_fractal_count = stats['sell_reasons'].get('顶分型', 0)
        print(f"{mode:<12} {stats['total_trades']:>8} {stats['win_rate']:>8.1%} {stats['avg_profit']:>10.1%} {stats['avg_holding_days']:>10.1f} {stats['profit_loss_ratio']:>8.2f} {stats['max_profit']:>8.1%} {stats['max_loss']:>8.1%} {top_fractal_count:>12}")

    print("=" * 100)

    # 分析结论
    print("\n" + "=" * 80)
    print("分析结论")
    print("=" * 80)

    # 找出效果最好的模式
    best_profit = max(results.items(), key=lambda x: x[1]['stats']['avg_profit'])
    best_winrate = max(results.items(), key=lambda x: x[1]['stats']['win_rate'])
    least_top_fractal = min(results.items(), key=lambda x: x[1]['stats']['sell_reasons'].get('顶分型', 0))

    print(f"平均收益最高: {best_profit[0]} ({best_profit[1]['stats']['avg_profit']:.1%})")
    print(f"胜率最高: {best_winrate[0]} ({best_winrate[1]['stats']['win_rate']:.1%})")
    print(f"顶分型卖出最少: {least_top_fractal[0]} ({least_top_fractal[1]['stats']['sell_reasons'].get('顶分型', 0)}次)")

    return results


if __name__ == '__main__':
    # 测试回测
    print("回测模块测试")

    # 先用少量股票测试
    test_codes = ['300166', '300001', '300002', '300003', '300004']
    trades, stats = run_backtest(
        stock_codes=test_codes,
        start_date='20250411',
        end_date='20260411'
    )