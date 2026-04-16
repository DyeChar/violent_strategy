"""
涨停池管理模块
扫描涨停股票，维护涨停历史表，生成候选池

命名规范：使用 limit_up（不用zt）

数据存储：
- 涨停历史表：output/data/limit_up_history.csv（持久化，只记录涨停股票）
- 候选池：不持久化，实时从涨停历史计算

数据格式：
- limit_up_history.csv: 股票代码,日期,涨跌幅%,收盘价
- 股票代码统一为6位纯数字字符串格式（如 '000001', '300001'）
"""

import os
import time
import pandas as pd
import baostock as bs
from datetime import datetime, timedelta
from typing import List, Dict
import warnings
warnings.filterwarnings('ignore')

import config
from .stock_pool import get_stock_pool


def normalize_stock_code(code) -> str:
    """
    标准化股票代码为6位纯数字字符串格式

    Args:
        code: 原始代码（可以是int、str、带前缀的格式）

    Returns:
        str: 6位纯数字字符串（如 '000001', '300001'）

    Examples:
        normalize_stock_code(1) -> '000001'
        normalize_stock_code('300001') -> '300001'
        normalize_stock_code('sz.300001') -> '300001'
        normalize_stock_code(300001) -> '300001'
    """
    # 转为字符串
    s = str(code)

    # 去除前缀（如 sz. 或 sh.）
    if '.' in s:
        s = s.split('.')[-1]

    # 去除特殊字符（如括号）
    s = s.replace('(', '').replace(')', '')

    # 补齐到6位
    return s.zfill(6)


def get_trade_dates(start_date: str, end_date: str) -> List[str]:
    """
    获取交易日列表

    Args:
        start_date: 起始日期 YYYY-MM-DD
        end_date: 结束日期 YYYY-MM-DD

    Returns:
        List[str]: 交易日列表 YYYY-MM-DD
    """
    lg = bs.login()
    if lg.error_code != '0':
        print(f"baostock登录失败: {lg.error_msg}")
        return []

    try:
        rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)

        trade_dates = []
        while rs.next():
            row = rs.get_row_data()
            if row[1] == '1':  # 是交易日
                trade_dates.append(row[0])

        bs.logout()
        return trade_dates

    except Exception as e:
        print(f"获取交易日列表异常: {e}")
        bs.logout()
        return []


def load_limit_up_history() -> pd.DataFrame:
    """
    加载涨停历史表

    Returns:
        DataFrame: 涨停历史，空DataFrame表示无历史
    """
    history_file = os.path.join(config.DATA_DIR, 'limit_up_history.csv')

    if not os.path.exists(history_file):
        return pd.DataFrame(columns=['股票代码', '日期', '涨跌幅%', '收盘价'])

    try:
        # 指定股票代码为字符串类型，避免pandas自动转int导致前导零丢失
        history = pd.read_csv(history_file, dtype={'股票代码': str})
        return history
    except Exception as e:
        print(f"加载涨停历史失败: {e}")
        return pd.DataFrame(columns=['股票代码', '日期', '涨跌幅%', '收盘价'])


def save_limit_up_history(history: pd.DataFrame):
    """
    保存涨停历史表

    Args:
        history: 涨停历史DataFrame
    """
    history_file = os.path.join(config.DATA_DIR, 'limit_up_history.csv')
    os.makedirs(os.path.dirname(history_file), exist_ok=True)
    history.to_csv(history_file, index=False, encoding='utf-8-sig')


def batch_fetch_change_pct(codes: List[str], date: str) -> List[Dict]:
    """
    批量获取股票涨跌幅（单次登录，效率更高）

    Args:
        codes: 股票代码列表
        date: 日期 YYYY-MM-DD

    Returns:
        List[Dict]: [{'code': xxx, 'change_pct': xxx, 'close': xxx}, ...]
    """
    lg = bs.login()
    if lg.error_code != '0':
        print(f"baostock登录失败: {lg.error_msg}")
        return []

    results = []

    try:
        for code in codes:
            try:
                market = 'sz' if code.startswith(('0', '3')) else 'sh'
                bs_code = f"{market}.{code}"

                rs = bs.query_history_k_data_plus(
                    bs_code,
                    "date,close,preClose",
                    start_date=date,
                    end_date=date,
                    frequency="d"
                )

                if rs.error_code != '0':
                    continue

                data = []
                while rs.next():
                    data.append(rs.get_row_data())

                if data:
                    close = float(data[0][1])
                    preclose = float(data[0][2])
                    if preclose > 0:
                        change_pct = (close - preclose) / preclose * 100
                        results.append({
                            'code': normalize_stock_code(code),  # 标准化代码格式
                            'change_pct': round(change_pct, 2),
                            'close': round(close, 2)
                        })

            except Exception:
                continue

        bs.logout()
        return results

    except Exception as e:
        print(f"批量查询异常: {e}")
        bs.logout()
        return []


def scan_limit_up_akshare(date: str, threshold: float = None) -> List[Dict]:
    """
    使用akshare扫描当日涨停股票（实时数据，适合今日扫描）

    Args:
        date: 日期 YYYY-MM-DD
        threshold: 涨停阈值，默认使用config配置

    Returns:
        List[Dict]: 涨停股票列表
    """
    threshold = threshold or config.LIMIT_UP_THRESHOLD

    print(f"使用akshare扫描 {date} 涨停股票（涨幅 >= {threshold}%）...")

    try:
        import akshare as ak

        # 获取A股实时行情
        df = ak.stock_zh_a_spot_em()

        limit_ups = []
        for _, row in df.iterrows():
            code = str(row['代码']).zfill(6)
            name = row['名称']
            change_pct = row['涨跌幅']
            close = row['最新价']

            # 过滤：涨幅>=阈值，且是有效股票代码
            if change_pct >= threshold:
                # 检查代码是否在有效范围内
                if code.startswith(('0', '3', '6')):
                    limit_ups.append({
                        '股票代码': normalize_stock_code(code),
                        '日期': date,
                        '涨跌幅%': round(change_pct, 2),
                        '收盘价': round(close, 2)
                    })
                    print(f"  涨停: {code} {name} +{change_pct:.2f}%")

        print(f"akshare扫描完成，发现 {len(limit_ups)} 只涨停股")
        return limit_ups

    except Exception as e:
        print(f"akshare扫描失败: {e}")
        return []


def scan_limit_up(date: str, threshold: float = None) -> List[Dict]:
    """
    扫描当日涨停股票（涨幅 >= threshold%）

    自动选择数据源：
    - 今日数据：使用akshare（实时）
    - 历史数据：使用baostock

    Args:
        date: 日期 YYYY-MM-DD
        threshold: 涨停阈值，默认使用config配置

    Returns:
        List[Dict]: 涨停股票列表 [{'股票代码': xxx, '日期': xxx, '涨跌幅%': xxx, '收盘价': xxx}, ...]
    """
    threshold = threshold or config.LIMIT_UP_THRESHOLD

    from datetime import datetime
    today = datetime.now().strftime('%Y-%m-%d')

    # 今日扫描用akshare（实时数据）
    if date == today:
        return scan_limit_up_akshare(date, threshold)

    # 历史数据用baostock
    print(f"扫描 {date} 涨停股票（涨幅 >= {threshold}%）...")

    stock_pool = get_stock_pool()

    if not stock_pool:
        print("股票池为空")
        return []

    print(f"待扫描股票: {len(stock_pool)} 只")

    change_data = batch_fetch_change_pct(stock_pool, date)

    limit_ups = []
    for item in change_data:
        if item['change_pct'] >= threshold:
            limit_ups.append({
                '股票代码': item['code'],
                '日期': date,
                '涨跌幅%': item['change_pct'],
                '收盘价': item['close']
            })
            print(f"  涨停: {item['code']} +{item['change_pct']:.2f}%")

    print(f"扫描完成，发现 {len(limit_ups)} 只涨停股")
    return limit_ups


def update_limit_up_history(date: str = None, threshold: float = None) -> pd.DataFrame:
    """
    增量更新涨停历史表（追加当日涨停记录）

    Args:
        date: 日期，默认今天
        threshold: 涨停阈值

    Returns:
        DataFrame: 更新后的涨停历史表
    """
    date = date or datetime.now().strftime('%Y-%m-%d')
    threshold = threshold or config.LIMIT_UP_THRESHOLD

    history = load_limit_up_history()

    if not history.empty and date in history['日期'].values:
        print(f"  {date} 数据已存在，跳过")
        return history

    limit_ups = scan_limit_up(date, threshold)

    if not limit_ups:
        print(f"  {date}: 无涨停股")
        return history

    new_rows = pd.DataFrame(limit_ups)
    history = pd.concat([history, new_rows], ignore_index=True)
    save_limit_up_history(history)

    print(f"  {date}: 新增 {len(limit_ups)} 条涨停记录")
    return history


def backfill_limit_up_history(start_date: str, end_date: str):
    """
    补齐缺失日期的涨停历史

    Args:
        start_date: 起始日期 YYYY-MM-DD
        end_date: 结束日期 YYYY-MM-DD
    """
    print(f"\n补齐涨停历史: {start_date} → {end_date}")

    history = load_limit_up_history()
    existing_dates = set(history['日期'].unique()) if not history.empty else set()

    trade_dates = get_trade_dates(start_date, end_date)

    for date in trade_dates:
        if date not in existing_dates:
            print(f"补齐 {date}...")
            update_limit_up_history(date)
            time.sleep(1)

    print("补齐完成")


def get_candidate_pool(date: str, lookback_days: int = None) -> List[str]:
    """
    获取某日候选股池（近N天有过涨停的股票）

    此函数是监控和回测共用的核心函数。

    Args:
        date: 日期 YYYY-MM-DD
        lookback_days: 回看天数，默认30天

    Returns:
        List[str]: 候选股代码列表
    """
    lookback_days = lookback_days or config.LIMIT_UP_POOL_LOOKBACK

    history = load_limit_up_history()

    if history.empty:
        print(f"  {date}: 涨停历史为空，候选池为空")
        return []

    lookback_start = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

    recent_limit_up = history[
        (history['日期'] >= lookback_start) &
        (history['日期'] <= date)
    ]

    candidate_codes = recent_limit_up['股票代码'].unique().tolist()
    # 确保为字符串格式（数据源已标准化为6位，只需转str）
    candidate_codes = [str(c) for c in candidate_codes]

    print(f"  {date} 候选池: {len(candidate_codes)} 只（回看{lookback_days}天）")
    return candidate_codes


def ensure_limit_up_history_up_to_date():
    """
    确保涨停历史是最新的（启动时调用）
    """
    history = load_limit_up_history()

    if history.empty:
        last_date = config.BACKTEST_START_DATE
    else:
        last_date = history['日期'].max()

    today = datetime.now().strftime('%Y-%m-%d')

    if last_date >= today:
        print(f"涨停历史已是最新（{last_date}）")
        return

    print(f"涨停历史需要补齐: {last_date} → {today}")
    backfill_limit_up_history(last_date, today)


# ==================== 测试代码 ====================

if __name__ == '__main__':
    print("=" * 50)
    print("涨停池管理模块测试")
    print("=" * 50)

    # 测试加载涨停历史
    print("\n测试加载涨停历史...")
    history = load_limit_up_history()
    print(f"涨停历史记录数: {len(history)}")
    print(f"最新日期: {history['日期'].max()}")

    # 测试候选池
    test_date = '2026-04-14'
    print(f"\n测试获取候选池 {test_date}...")
    candidates = get_candidate_pool(test_date)
    print(f"候选股数量: {len(candidates)}")
    print(f"前10只: {candidates[:10]}")