"""
股票池管理模块
获取有效A股股票池（主板+创业板，排除ST/北交所/B股/退市股）

股票范围：
- 主板：600/601/603/605/000/001/002/003
- 创业板：300
- 排除：科创板(688)、北交所(8开头/4开头)、B股(900/200)、ST、退市股
"""

import os
import json
import baostock as bs
from datetime import datetime
from typing import List, Tuple, Optional
import pandas as pd

import config


# ==================== 预定义股票池 ====================

# 创业板龙头池（约100只，用于测试和备用）
创业板龙头池 = [
    '300001', '300002', '300003', '300004', '300005',
    '300006', '300007', '300008', '300009', '300010',
    '300014', '300015', '300017', '300020', '300024',
    '300026', '300033', '300037', '300059', '300066',
    '300072', '300073', '300075', '300077', '300080',
    '300088', '300098', '300101', '300124', '300133',
    '300142', '300144', '300146', '300147', '300153',
    '300159', '300166', '300170', '300182', '300202',
    '300212', '300223', '300229', '300232', '300244',
    '300251', '300257', '300274', '300287', '300315',
    '300327', '300333', '300347', '300357', '300367',
    '300377', '300382', '300383', '300394', '300408',
    '300413', '300431', '300433', '300450', '300457',
    '300459', '300460', '300463', '300474', '300476',
    '300482', '300496', '300498', '300502', '300508',
    '300529', '300532', '300550', '300558', '300562',
    '300567', '300576', '300582', '300585', '300597',
    '300601', '300607', '300618', '300628', '300630',
    '300633', '300634', '300641', '300647', '300655',
    '300661', '300666', '300669', '300672', '300676',
    '300687', '300699', '300724', '300725', '300726',
    '300735', '300745', '300750',
]

# 主板龙头池（约50只，用于测试和备用）
主板龙头池 = [
    '600000', '600036', '600519', '600887', '601318',
    '000001', '000002', '000333', '000651', '002415',
]


def is_valid_stock_code(code: str) -> bool:
    """
    判断股票代码是否有效（符合前缀规则）

    Args:
        code: 股票代码

    Returns:
        bool: 是否是有效代码
    """
    # 排除无效前缀
    for prefix in config.EXCLUDE_PREFIXES:
        if code.startswith(prefix):
            return False

    # 必须是有效前缀
    for prefix in config.VALID_PREFIXES:
        if code.startswith(prefix):
            return True

    return False


def is_st_stock(name: str) -> bool:
    """
    判断是否是ST股票

    Args:
        name: 股票名称

    Returns:
        bool: 是否是ST股票
    """
    if not name:
        return False

    # ST标识：ST、*ST、SST等
    st_keywords = ['ST', '*ST', 'SST', 'S*ST']
    return any(keyword in name for keyword in st_keywords)


def is_delisted(status: str) -> bool:
    """
    判断是否已退市

    Args:
        status: 股票状态

    Returns:
        bool: 是否已退市
    """
    if not status:
        return False

    # 退市标识
    delisted_keywords = ['退市', '终止上市', '暂停上市']
    return any(keyword in status for keyword in delisted_keywords)


def fetch_all_stocks_from_baostock() -> List[Tuple[str, str]]:
    """
    从baostock获取全A股股票列表

    Returns:
        List[Tuple]: [(股票代码, 股票名称), ...]
    """
    lg = bs.login()
    if lg.error_code != '0':
        print(f"baostock登录失败: {lg.error_msg}")
        return []

    try:
        today = datetime.now().strftime('%Y-%m-%d')
        rs = bs.query_all_stock(day=today)

        if rs.error_code != '0':
            print(f"获取股票列表失败: {rs.error_msg}")
            bs.logout()
            return []

        stocks = []
        while rs.next():
            row = rs.get_row_data()
            # row格式：[code, tradeStatus, code_name, ...]
            bs_code = row[0]
            name = row[2] if len(row) > 2 else ''

            # 提取股票代码（去除sz./sh.前缀）
            code = bs_code.split('.')[-1] if '.' in bs_code else bs_code

            stocks.append((code, name))

        bs.logout()
        return stocks

    except Exception as e:
        print(f"获取股票列表异常: {e}")
        bs.logout()
        return []


def filter_valid_stocks(stocks: List[Tuple[str, str]]) -> List[str]:
    """
    筛选有效股票（排除ST/退市/北交所/B股）

    Args:
        stocks: [(股票代码, 股票名称), ...]

    Returns:
        List[str]: 有效股票代码列表
    """
    valid_codes = []

    for code, name in stocks:
        # 1. 检查代码前缀
        if not is_valid_stock_code(code):
            continue

        # 2. 检查是否ST
        if is_st_stock(name):
            continue

        # 3. 检查是否退市（通过名称判断）
        if is_delisted(name):
            continue

        valid_codes.append(code)

    return valid_codes


def load_stock_pool_cache() -> List[str]:
    """
    加载股票池缓存

    Returns:
        List[str]: 股票代码列表，空列表表示无缓存
    """
    if not os.path.exists(config.STOCK_POOL_FILE):
        return []

    try:
        cache = json.load(open(config.STOCK_POOL_FILE, 'r'))
        cache_date = cache.get('date', '')

        # 检查缓存是否过期
        cache_dt = datetime.strptime(cache_date, '%Y-%m-%d')
        if (datetime.now() - cache_dt).days >= config.STOCK_POOL_CACHE_DAYS:
            print("股票池缓存已过期")
            return []

        return cache.get('codes', [])

    except Exception as e:
        print(f"加载股票池缓存失败: {e}")
        return []


def save_stock_pool_cache(codes: List[str]):
    """
    保存股票池缓存

    Args:
        codes: 股票代码列表
    """
    cache = {
        'date': datetime.now().strftime('%Y-%m-%d'),
        'codes': codes,
        'count': len(codes)
    }

    os.makedirs(os.path.dirname(config.STOCK_POOL_FILE), exist_ok=True)
    json.dump(cache, open(config.STOCK_POOL_FILE, 'w'))


def get_stock_pool(use_cache: bool = True, pool_type: str = 'all') -> List[str]:
    """
    获取股票池（监控/回测共用）

    Args:
        use_cache: 是否使用缓存
        pool_type: 股票池类型
            - 'all': 全A股有效股票
            - 'chinext': 创业板龙头池（测试用）
            - 'main': 主板龙头池（测试用）

    Returns:
        List[str]: 股票代码列表
    """
    # 测试用预定义池
    if pool_type == 'chinext':
        return 创业板龙头池
    elif pool_type == 'main':
        return 主板龙头池

    # 使用缓存
    if use_cache:
        cached = load_stock_pool_cache()
        if cached:
            print(f"使用股票池缓存（{len(cached)}只）")
            return cached

    # 从API获取
    print("从baostock获取股票池...")
    stocks = fetch_all_stocks_from_baostock()

    if not stocks:
        print("获取失败，使用预定义池")
        return 创业板龙头池 + 主板龙头池

    # 筛选有效股票
    valid_codes = filter_valid_stocks(stocks)

    print(f"获取到有效股票 {len(valid_codes)} 只")

    # 保存缓存
    save_stock_pool_cache(valid_codes)

    return valid_codes


def is_valid_stock(code: str, name: str = None) -> bool:
    """
    判断股票是否有效（综合检查）

    Args:
        code: 股票代码
        name: 股票名称（可选）

    Returns:
        bool: 是否是有效股票
    """
    # 检查代码前缀
    if not is_valid_stock_code(code):
        return False

    # 检查名称（如果提供）
    if name:
        if is_st_stock(name) or is_delisted(name):
            return False

    return True


# ==================== 测试代码 ====================

if __name__ == '__main__':
    print("=" * 50)
    print("股票池模块测试")
    print("=" * 50)

    # 测试预定义池
    print(f"\n创业板龙头池: {len(创业板龙头池)} 只")
    print(f"主板龙头池: {len(主板龙头池)} 只")

    # 测试股票代码判断
    print("\n测试股票代码判断:")
    test_codes = ['300001', '688001', '800001', '900001', 'ST某某']
    for code in test_codes:
        print(f"  {code}: {is_valid_stock_code(code)}")

    # 测试获取股票池
    print("\n测试获取股票池（chinext模式）...")
    codes = get_stock_pool(pool_type='chinext')
    print(f"获取到 {len(codes)} 只")
    print(f"前5只: {codes[:5]}")