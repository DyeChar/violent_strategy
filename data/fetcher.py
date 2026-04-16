"""
K线数据获取模块
数据源优先级：本地缓存 > baostock > akshare

增量更新机制：
- 每股一个CSV文件：output/cache/{code}.csv
- 只追加新日期数据，不重复获取历史
"""

import os
import time
import pandas as pd
import numpy as np
import baostock as bs
import akshare as ak
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import warnings
warnings.filterwarnings('ignore')

import config


def normalize_stock_code(code) -> str:
    """
    标准化股票代码为6位纯数字字符串格式

    Args:
        code: 原始代码（可以是int、str、带前缀的格式）

    Returns:
        str: 6位纯数字字符串（如 '000001', '300001')

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


def get_cache_path(code: str) -> str:
    """获取K线缓存文件路径"""
    return os.path.join(config.CACHE_DIR, f'{code}.csv')


def load_kline_cache(code: str) -> pd.DataFrame:
    """
    从缓存加载K线数据

    Args:
        code: 股票代码（如 '300001'）

    Returns:
        DataFrame: K线数据，空DataFrame表示无缓存
    """
    cache_file = get_cache_path(normalize_stock_code(code))

    if os.path.exists(cache_file):
        try:
            # 指定code列为字符串类型，避免pandas自动转int导致前导零丢失
            kline = pd.read_csv(cache_file, dtype={'code': str})
            return kline
        except Exception as e:
            print(f"  加载缓存失败 {code}: {e}")

    return pd.DataFrame()


def save_kline_cache(kline: pd.DataFrame, code: str):
    """
    保存K线数据到缓存（自动标准化code格式）

    Args:
        kline: K线数据
        code: 股票代码
    """
    if kline.empty:
        return

    # 标准化code列格式
    if 'code' in kline.columns:
        kline['code'] = kline['code'].apply(normalize_stock_code)

    cache_file = get_cache_path(normalize_stock_code(code))
    kline.to_csv(cache_file, index=False)


def fetch_kline_baostock(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    从baostock获取K线数据（稳定免费数据源）

    Args:
        code: 股票代码（如 '300001'）
        start_date: 起始日期 YYYY-MM-DD
        end_date: 结束日期 YYYY-MM-DD

    Returns:
        DataFrame: K线数据
    """
    lg = bs.login()
    if lg.error_code != '0':
        print(f"baostock登录失败: {lg.error_msg}")
        return pd.DataFrame()

    try:
        # baostock代码格式：sz.300001 或 sh.600000
        market = 'sz' if code.startswith(('0', '3')) else 'sh'
        bs_code = f"{market}.{code}"

        rs = bs.query_history_k_data_plus(
            bs_code,
            "date,code,open,high,low,close,volume,amount",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="2"  # 不复权
        )

        if rs.error_code != '0':
            bs.logout()
            return pd.DataFrame()

        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())

        bs.logout()

        if not data_list:
            return pd.DataFrame()

        kline = pd.DataFrame(data_list, columns=rs.fields)

        # 数据类型转换
        for col in ['open', 'high', 'low', 'close', 'volume', 'amount']:
            kline[col] = pd.to_numeric(kline[col], errors='coerce')

        # 标准化code格式（6位纯数字字符串）
        kline['code'] = kline['code'].apply(normalize_stock_code)

        return kline

    except Exception as e:
        bs.logout()
        print(f"baostock获取 {code} 异常: {e}")
        return pd.DataFrame()


def fetch_kline_akshare(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    从akshare获取K线数据（备用数据源）

    Args:
        code: 股票代码
        start_date: 起始日期 YYYYMMDD（akshare格式）
        end_date: 结束日期 YYYYMMDD

    Returns:
        DataFrame: K线数据
    """
    try:
        # akshare日期格式：YYYYMMDD
        ak_start = start_date.replace('-', '')
        ak_end = end_date.replace('-', '')

        kline = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=ak_start,
            end_date=ak_end,
            adjust="qfq"  # 前复权
        )

        if kline.empty:
            return pd.DataFrame()

        # 标准化列名
        column_map = {
            '日期': 'date',
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'amount'
        }
        rename_cols = {k: v for k, v in column_map.items() if k in kline.columns}
        kline = kline.rename(columns=rename_cols)

        # 格式化日期
        kline['date'] = pd.to_datetime(kline['date']).dt.strftime('%Y-%m-%d')

        # 标准化code格式（6位纯数字字符串）
        kline['code'] = normalize_stock_code(code)

        return kline[['date', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount']]

    except Exception as e:
        print(f"akshare获取 {code} 异常: {e}")
        return pd.DataFrame()


def get_kline(code: str, use_cache: bool = True) -> pd.DataFrame:
    """
    获取单股K线数据（监控/回测共用）

    Args:
        code: 股票代码
        use_cache: 是否使用缓存

    Returns:
        DataFrame: K线数据
    """
    end_date = datetime.now().strftime('%Y-%m-%d')

    # 1. 加载缓存
    if use_cache:
        cached = load_kline_cache(code)
        if not cached.empty:
            last_date = cached['date'].max()
            # 缓存数据足够（覆盖到最近），直接返回
            # 回测不需要"今天"的数据，只要缓存有足够历史数据即可
            if len(cached) >= config.MA20_PERIOD + 30:  # 至少有50条数据
                return cached  # 直接返回缓存，不尝试增量获取

            # 数据不足才尝试增量获取
            start_date = (datetime.strptime(last_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            cached = pd.DataFrame()
            start_date = config.KLINE_START_DATE
    else:
        cached = pd.DataFrame()
        start_date = config.KLINE_START_DATE

    # 2. 从baostock获取
    new_kline = fetch_kline_baostock(code, start_date, end_date)

    # 3. baostock失败则用akshare备用
    if new_kline.empty:
        new_kline = fetch_kline_akshare(code, start_date, end_date)

    # 4. 合并并保存
    if not new_kline.empty:
        kline = pd.concat([cached, new_kline], ignore_index=True)
        kline = kline.drop_duplicates(subset=['date'])
        kline = kline.sort_values('date').reset_index(drop=True)
        save_kline_cache(kline, code)
        return kline

    # 增量获取失败，返回缓存数据
    return cached


def batch_fetch_klines(codes: List[str], use_cache: bool = True) -> Dict[str, pd.DataFrame]:
    """
    批量获取K线数据（监控/回测共用）

    Args:
        codes: 股票代码列表
        use_cache: 是否使用缓存

    Returns:
        Dict: {股票代码: K线DataFrame}
    """
    results = {}
    total = len(codes)

    print(f"开始获取K线数据（{total}只股票）...")

    for i, code in enumerate(codes):
        kline = get_kline(code, use_cache=use_cache)

        if not kline.empty and len(kline) >= config.MA20_PERIOD:
            results[code] = kline

        if (i + 1) % 20 == 0:
            print(f"  已获取 {i+1}/{total}...")

        time.sleep(config.REQUEST_DELAY)

    print(f"成功获取 {len(results)} 只股票K线数据")
    return results


def update_kline_cache(codes: List[str]) -> Dict[str, pd.DataFrame]:
    """
    更新指定股票的K线缓存（增量更新）

    Args:
        codes: 股票代码列表

    Returns:
        Dict: {股票代码: K线DataFrame}
    """
    return batch_fetch_klines(codes, use_cache=True)


def get_kline_until_date(code: str, date: str, use_cache: bool = True) -> pd.DataFrame:
    """
    获取截至某日期的K线数据（用于回测，不含未来数据）

    Args:
        code: 股票代码
        date: 截止日期 YYYY-MM-DD
        use_cache: 是否使用缓存

    Returns:
        DataFrame: 截至该日期的K线数据
    """
    kline = get_kline(code, use_cache=use_cache)

    if kline.empty:
        return kline

    # 只保留date及之前的数据
    kline = kline[kline['date'] <= date].copy()

    return kline.reset_index(drop=True)


# ==================== 测试代码 ====================

if __name__ == '__main__':
    print("=" * 50)
    print("K线获取模块测试")
    print("=" * 50)

    # 测试单股获取
    test_code = '300001'
    print(f"\n测试获取 {test_code}...")
    kline = get_kline(test_code)
    print(f"获取到 {len(kline)} 条记录")
    print(kline.tail(3))

    # 测试批量获取
    test_codes = ['300001', '300002', '300003']
    print(f"\n测试批量获取 {len(test_codes)} 只...")
    results = batch_fetch_klines(test_codes)
    print(f"成功: {list(results.keys())}")