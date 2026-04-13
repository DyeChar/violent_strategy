"""
数据获取模块
基于efinance和akshare获取创业板股票列表和K线数据
优先使用本地缓存，其次baostock（稳定免费），最后efinance/akshare
"""

import efinance as ef
import akshare as ak  # 备用数据源
import baostock as bs  # 更稳定的免费数据源
import pandas as pd
import numpy as np
from typing import List, Dict, Optional
from datetime import datetime
import time
import os
import json
import warnings
warnings.filterwarnings('ignore')

import config

# 本地缓存目录
LOCAL_CACHE_DIR = os.path.join(config.OUTPUT_DIR, 'local_cache')


def get_local_cache_path(code: str, start_date: str, end_date: str) -> str:
    """
    获取本地缓存文件路径

    Args:
        code: 股票代码
        start_date: 起始日期
        end_date: 结束日期

    Returns:
        str: 缓存文件路径
    """
    os.makedirs(LOCAL_CACHE_DIR, exist_ok=True)
    return os.path.join(LOCAL_CACHE_DIR, f'{code}_{start_date}_{end_date}.csv')


def load_local_cache(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    从本地缓存加载K线数据

    Args:
        code: 股票代码
        start_date: 起始日期
        end_date: 结束日期

    Returns:
        DataFrame: K线数据，如果不存在返回空DataFrame
    """
    cache_path = get_local_cache_path(code, start_date, end_date)

    if os.path.exists(cache_path):
        try:
            kline = pd.read_csv(cache_path)
            print(f"  从本地缓存加载 {code}")
            return kline
        except Exception as e:
            print(f"  加载缓存失败: {e}")

    return pd.DataFrame()


def save_local_cache(kline: pd.DataFrame, code: str, start_date: str, end_date: str):
    """
    保存K线数据到本地缓存

    Args:
        kline: K线数据
        code: 股票代码
        start_date: 起始日期
        end_date: 结束日期
    """
    if kline.empty:
        return

    cache_path = get_local_cache_path(code, start_date, end_date)
    kline.to_csv(cache_path, index=False, encoding='utf-8-sig')
    print(f"  已缓存到本地: {code}")


def get_all_stocks() -> pd.DataFrame:
    """
    获取全部A股股票列表

    Returns:
        DataFrame: 包含股票代码、名称等信息
    """
    try:
        # 使用efinance获取股票列表
        # 方法1: 使用quote_history获取基础信息
        stocks = ef.stock.get_quote_history('000001', beg='20260101', end='20260411')
        # 从返回的数据中提取股票名称作为示例

        # 方法2: 使用efinance的股票列表API
        stock_df = ef.stock.get_realtime_quotes()
        return stock_df
    except Exception as e:
        print(f"获取股票列表失败: {e}")
        return pd.DataFrame()


def get_chinext_stocks(use_cache: bool = True) -> List[str]:
    """
    获取创业板股票列表（代码以300开头）

    Args:
        use_cache: 是否使用缓存（缓存有效期7天）

    Returns:
        List[str]: 股票代码列表
    """
    cache_file = os.path.join(config.PROJECT_DIR, 'chinext_cache.json')

    # 检查缓存
    if use_cache and os.path.exists(cache_file):
        try:
            cache_data = json.load(open(cache_file, 'r'))
            cache_date = cache_data.get('date', '')
            # 检查缓存是否过期（7天）
            from datetime import datetime
            cache_dt = datetime.strptime(cache_date, '%Y%m%d')
            if (datetime.now() - cache_dt).days < 7:
                print(f"使用缓存的创业板股票列表（{len(cache_data['codes'])}只）")
                return cache_data['codes']
        except Exception:
            pass

    # 从API获取
    print("正在获取创业板股票列表...")
    all_stocks = get_all_stocks()

    if all_stocks.empty:
        print("获取失败，使用测试样本")
        return config.TEST_SAMPLES

    # 筛选创业板股票（300开头）
    # 注意：efinance返回的列名可能是中文
    code_col = '股票代码' if '股票代码' in all_stocks.columns else 'code'
    chinext_stocks = all_stocks[all_stocks[code_col].astype(str).str.startswith(config.CHINEXT_PREFIX)]
    codes = chinext_stocks[code_col].astype(str).tolist()

    print(f"获取到创业板股票 {len(codes)} 只")

    # 保存缓存
    cache_data = {
        'date': time.strftime('%Y%m%d'),
        'codes': codes
    }
    with open(cache_file, 'w') as f:
        json.dump(cache_data, f)

    return codes


def get_stock_kline(code: str,
                    start_date: str = None,
                    end_date: str = None,
                    use_backup: bool = True,
                    use_local_cache: bool = True) -> pd.DataFrame:
    """
    获取个股历史K线数据
    优先级：本地缓存 > baostock（稳定） > efinance > akshare

    Args:
        code: 股票代码（如 '300001'）
        start_date: 起始日期（如 '20240101'）
        end_date: 结束日期（如 '20260411'）
        use_backup: 是否使用备用数据源
        use_local_cache: 是否优先使用本地缓存

    Returns:
        DataFrame: K线数据，包含 date, open, close, high, low, volume 列
    """
    start_date = start_date or config.START_DATE
    end_date = end_date or config.END_DATE

    # 优先从本地缓存加载
    if use_local_cache:
        cached_kline = load_local_cache(code, start_date, end_date)
        if not cached_kline.empty:
            return cached_kline

    # 优先使用baostock（更稳定，无IP限制）
    try:
        kline = get_stock_kline_baostock(code, start_date, end_date)
        if not kline.empty:
            if use_local_cache:
                save_local_cache(kline, code, start_date, end_date)
            return kline
    except Exception as e:
        print(f"baostock获取 {code} 失败: {e}")

    # 再尝试efinance
    try:
        kline = ef.stock.get_quote_history(
            code,
            beg=start_date,
            end=end_date
        )

        if kline.empty:
            if use_backup:
                return get_stock_kline_akshare(code, start_date, end_date)
            return pd.DataFrame()

        # 标准化列名（中文→英文）
        column_mapping = {
            '日期': 'date',
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'amount',
            '振幅': 'amplitude',
            '涨跌幅': 'change_pct',
            '涨跌额': 'change',
            '换手率': 'turnover',
            '股票代码': 'code',
            '股票名称': 'name'
        }

        # 只重命名存在的列
        rename_cols = {k: v for k, v in column_mapping.items() if k in kline.columns}
        kline = kline.rename(columns=rename_cols)

        # 格式化日期
        if 'date' in kline.columns:
            kline['date'] = pd.to_datetime(kline['date']).dt.strftime('%Y%m%d')

        # 确保必要列存在
        required_cols = ['date', 'open', 'close', 'high', 'low', 'volume']
        if not all(col in kline.columns for col in required_cols):
            print(f"股票 {code} efinance数据列不完整，尝试akshare...")
            if use_backup:
                return get_stock_kline_akshare(code, start_date, end_date)
            return pd.DataFrame()

        # 保存到本地缓存
        if use_local_cache:
            save_local_cache(kline, code, start_date, end_date)

        return kline

    except Exception as e:
        print(f"efinance获取 {code} 失败: {e}")
        # 切换到akshare备用
        if use_backup:
            print(f"  切换到akshare备用数据源...")
            return get_stock_kline_akshare(code, start_date, end_date, use_local_cache)
        return pd.DataFrame()


def get_stock_kline_baostock(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    使用baostock获取个股历史K线数据（推荐数据源，稳定无限制）

    Args:
        code: 股票代码（如 '300001'）
        start_date: 起始日期（如 '20240101'）
        end_date: 结束日期（如 '20260411'）

    Returns:
        DataFrame: K线数据，包含 date, open, close, high, low, volume 列
    """
    # 登录baostock系统
    lg = bs.login()
    if lg.error_code != '0':
        print(f"baostock登录失败: {lg.error_msg}")
        return pd.DataFrame()

    try:
        # baostock代码格式：sz.300001
        bs_code = f"sz.{code}" if code.startswith('0') or code.startswith('3') else f"sh.{code}"

        # 转换日期格式：YYYYMMDD -> YYYY-MM-DD
        bs_start = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
        bs_end = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"

        # 获取K线数据
        rs = bs.query_history_k_data_plus(
            bs_code,
            "date,code,open,high,low,close,volume,amount,turn",
            start_date=bs_start,
            end_date=bs_end,
            frequency="d",
            adjustflag="2"  # 不复权
        )

        if rs.error_code != '0':
            print(f"baostock查询失败: {rs.error_msg}")
            return pd.DataFrame()

        # 转换为DataFrame
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())

        if not data_list:
            return pd.DataFrame()

        kline = pd.DataFrame(data_list, columns=rs.fields)

        # 数据类型转换
        for col in ['open', 'high', 'low', 'close', 'volume', 'amount']:
            if col in kline.columns:
                kline[col] = pd.to_numeric(kline[col], errors='coerce')

        # 格式化日期
        kline['date'] = kline['date'].astype(str)

        # 确保必要列存在
        required_cols = ['date', 'open', 'close', 'high', 'low', 'volume']
        if not all(col in kline.columns for col in required_cols):
            print(f"baostock获取 {code} 数据列不完整")
            return pd.DataFrame()

        return kline

    finally:
        # 登出baostock系统
        bs.logout()


def get_stock_kline_akshare(code: str, start_date: str, end_date: str, use_local_cache: bool = True) -> pd.DataFrame:
    """
    使用akshare获取个股历史K线数据（备用数据源）

    Args:
        code: 股票代码（如 '300001'）
        start_date: 起始日期（如 '20240101'）
        end_date: 结束日期（如 '20260411'）
        use_local_cache: 是否保存到本地缓存

    Returns:
        DataFrame: K线数据，包含 date, open, close, high, low, volume 列
    """
    try:
        # akshare获取日线数据（前复权）
        kline = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq"  # 前复权
        )

        if kline.empty:
            return pd.DataFrame()

        # akshare列名标准化
        column_mapping = {
            '日期': 'date',
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'amount',
            '振幅': 'amplitude',
            '涨跌幅': 'change_pct',
            '涨跌额': 'change',
            '换手率': 'turnover'
        }

        rename_cols = {k: v for k, v in column_mapping.items() if k in kline.columns}
        kline = kline.rename(columns=rename_cols)

        # 格式化日期
        if 'date' in kline.columns:
            kline['date'] = pd.to_datetime(kline['date']).dt.strftime('%Y%m%d')

        # 添加股票代码和名称
        kline['code'] = code

        # 确保必要列存在
        required_cols = ['date', 'open', 'close', 'high', 'low', 'volume']
        if not all(col in kline.columns for col in required_cols):
            print(f"akshare获取 {code} 数据列不完整")
            return pd.DataFrame()

        # 保存到本地缓存
        if use_local_cache:
            save_local_cache(kline, code, start_date, end_date)

        return kline

    except Exception as e:
        print(f"akshare获取 {code} 也失败: {e}")
        return pd.DataFrame()


def batch_fetch_klines(stock_codes: List[str],
                       start_date: str = None,
                       end_date: str = None,
                       delay: float = None,
                       save_cache: bool = True) -> Dict[str, pd.DataFrame]:
    """
    批量获取多只股票的K线数据，并可选保存到缓存

    Args:
        stock_codes: 股票代码列表
        start_date: 起始日期
        end_date: 结束日期
        delay: 每次请求间隔（秒）
        save_cache: 是否保存到缓存文件

    Returns:
        Dict: {股票代码: K线DataFrame}
    """
    result = {}
    total = len(stock_codes)
    delay = delay or config.REQUEST_DELAY

    print(f"开始批量获取K线数据（共{total}只股票）...")

    for i, code in enumerate(stock_codes):
        print(f"  获取 {code} ({i+1}/{total})...")
        kline = get_stock_kline(code, start_date=start_date, end_date=end_date)

        if not kline.empty:
            result[code] = kline
            # 提取股票名称
            if 'name' in kline.columns:
                result[code]['name'] = kline['name'].iloc[-1]

        # 延时避免API封禁
        if delay > 0 and i < total - 1:
            time.sleep(delay)

    print(f"成功获取 {len(result)} 只股票数据")

    # 保存到缓存文件
    if save_cache and result:
        cache_dir = os.path.join(config.OUTPUT_DIR, 'kline_cache')
        os.makedirs(cache_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        cache_file = os.path.join(cache_dir, f'klines_{timestamp}.pkl')

        # 保存为pickle格式（保留完整数据结构）
        import pickle
        with open(cache_file, 'wb') as f:
            pickle.dump(result, f)
        print(f"K线数据已缓存: {cache_file}")

    return result


def load_kline_cache(cache_file: str = None) -> Dict[str, pd.DataFrame]:
    """
    从缓存文件加载K线数据

    Args:
        cache_file: 缓存文件路径（默认加载最新的缓存）

    Returns:
        Dict: {股票代码: K线DataFrame}
    """
    import pickle

    cache_dir = os.path.join(config.OUTPUT_DIR, 'kline_cache')

    if cache_file is None:
        # 找最新的缓存文件
        if not os.path.exists(cache_dir):
            return {}

        cache_files = [f for f in os.listdir(cache_dir) if f.startswith('klines_') and f.endswith('.pkl')]
        if not cache_files:
            return {}

        cache_files.sort(reverse=True)  # 按时间倒序
        cache_file = os.path.join(cache_dir, cache_files[0])

    if not os.path.exists(cache_file):
        return {}

    try:
        with open(cache_file, 'rb') as f:
            result = pickle.load(f)
        print(f"从缓存加载K线数据: {cache_file}")
        return result
    except Exception as e:
        print(f"加载缓存失败: {e}")
        return {}


def get_stock_name(code: str, kline: pd.DataFrame = None) -> str:
    """
    获取股票名称

    Args:
        code: 股票代码
        kline: K线数据（如果已获取）

    Returns:
        str: 股票名称
    """
    if kline is not None and 'name' in kline.columns:
        return kline['name'].iloc[-1]

    try:
        # 尝试从K线数据获取名称
        df = get_stock_kline(code)
        if 'name' in df.columns:
            return df['name'].iloc[-1]
    except Exception:
        pass

    return f"股票{code}"


# ==================== 测试代码 ====================

if __name__ == '__main__':
    print("=" * 50)
    print("数据获取模块测试")
    print("=" * 50)

    # 测试创业板股票列表获取
    print("\n1. 测试获取创业板股票列表...")
    chinext_codes = get_chinext_stocks()
    print(f"创业板股票数量: {len(chinext_codes)}")
    print(f"前10只: {chinext_codes[:10]}")

    # 测试K线获取
    print("\n2. 测试获取K线数据...")
    test_code = '300001'
    kline = get_stock_kline(test_code, start_date='20250101', end_date='20260411')
    print(f"获取 {test_code} K线: {len(kline)} 条")
    if not kline.empty:
        print(kline.tail(3))

    # 测试批量获取
    print("\n3. 测试批量获取...")
    test_codes = ['300001', '300002', '300003']
    data = batch_fetch_klines(test_codes, start_date='20250101', end_date='20260411')
    print(f"成功获取: {list(data.keys())}")