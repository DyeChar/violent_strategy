"""
K线数据补齐脚本
分批补齐所有股票的K线数据，从2024-12-01到昨天

特性：
- 增量更新：只获取缺失日期的数据
- 分批处理：每批100只股票
- 速率限制：批次间延迟30秒，请求间延迟0.3秒
- 断点续传：保存进度checkpoint
"""

import os
import sys
import time

# 添加项目根目录到path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

import pandas as pd
import baostock as bs
from datetime import datetime, timedelta
from typing import List, Dict
import warnings
warnings.filterwarnings('ignore')

import config
from data.fetcher import (
    normalize_stock_code, get_cache_path, load_kline_cache,
    save_kline_cache, fetch_kline_baostock
)


# 补齐参数
START_DATE = '2024-12-01'  # 起始日期
END_DATE = datetime.now().strftime('%Y-%m-%d')  # 昨天（当天可能还没收盘）
BATCH_SIZE = 100  # 每批处理数量
BATCH_DELAY = 30  # 批次间延迟（秒）
REQUEST_DELAY = 0.3  # 请求间延迟（秒）
CHECKPOINT_FILE = os.path.join(config.OUTPUT_DIR, 'kline_backfill_checkpoint.json')


def get_all_cached_codes() -> List[str]:
    """获取所有已缓存的股票代码"""
    cache_dir = config.CACHE_DIR
    codes = []
    for f in os.listdir(cache_dir):
        if f.endswith('.csv'):
            code = f.replace('.csv', '')
            codes.append(code)
    return sorted(codes)


def check_data_gap(code: str) -> Dict:
    """
    检查单只股票的数据缺失情况

    Returns:
        Dict: {
            'code': str,
            'has_cache': bool,
            'cache_start': str,
            'cache_end': str,
            'cache_rows': int,
            'need_backfill': bool,
            'backfill_start': str,
            'backfill_end': str
        }
    """
    cache_file = get_cache_path(code)

    if not os.path.exists(cache_file):
        return {
            'code': code,
            'has_cache': False,
            'cache_start': '',
            'cache_end': '',
            'cache_rows': 0,
            'need_backfill': True,
            'backfill_start': START_DATE,
            'backfill_end': END_DATE
        }

    kline = load_kline_cache(code)

    if kline.empty:
        return {
            'code': code,
            'has_cache': False,
            'cache_start': '',
            'cache_end': '',
            'cache_rows': 0,
            'need_backfill': True,
            'backfill_start': START_DATE,
            'backfill_end': END_DATE
        }

    cache_start = kline['date'].min()
    cache_end = kline['date'].max()
    cache_rows = len(kline)

    # 检查是否需要补齐
    need_backfill = False
    backfill_start = ''
    backfill_end = ''

    # 前端缺失（开始日期晚于目标）
    if cache_start > START_DATE:
        need_backfill = True
        backfill_start = START_DATE
        backfill_end = (datetime.strptime(cache_start, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

    # 后端缺失（结束日期早于目标）
    if cache_end < END_DATE:
        need_backfill = True
        if not backfill_start:  # 前端不缺失
            backfill_start = (datetime.strptime(cache_end, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        backfill_end = END_DATE

    return {
        'code': code,
        'has_cache': True,
        'cache_start': cache_start,
        'cache_end': cache_end,
        'cache_rows': cache_rows,
        'need_backfill': need_backfill,
        'backfill_start': backfill_start,
        'backfill_end': backfill_end
    }


def backfill_kline(code: str, start_date: str, end_date: str) -> bool:
    """
    补齐单只股票的K线数据（增量追加）

    Returns:
        bool: 是否成功
    """
    # 加载现有缓存
    cached = load_kline_cache(code)

    # 获取新数据
    new_kline = fetch_kline_baostock(code, start_date, end_date)

    if new_kline.empty:
        return False

    # 合并数据
    if not cached.empty:
        kline = pd.concat([cached, new_kline], ignore_index=True)
        kline = kline.drop_duplicates(subset=['date'])
        kline = kline.sort_values('date').reset_index(drop=True)
    else:
        kline = new_kline

    # 保存缓存
    save_kline_cache(kline, code)

    return True


def save_checkpoint(processed_codes: List[str], total: int):
    """保存进度checkpoint"""
    import json
    data = {
        'processed_codes': processed_codes,
        'total': total,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(data, f)
    print(f"  [Checkpoint已保存: {len(processed_codes)}/{total}]")


def load_checkpoint() -> List[str]:
    """加载checkpoint"""
    import json
    if not os.path.exists(CHECKPOINT_FILE):
        return []

    try:
        with open(CHECKPOINT_FILE, 'r') as f:
            data = json.load(f)
        return data.get('processed_codes', [])
    except:
        return []


def clear_checkpoint():
    """清除checkpoint"""
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print("Checkpoint已清除")


def run_backfill(batch_size: int = BATCH_SIZE, resume: bool = True):
    """
    执行分批补齐

    Args:
        batch_size: 每批处理数量
        resume: 是否从checkpoint恢复
    """
    print("=" * 60)
    print("K线数据补齐")
    print("=" * 60)
    print(f"目标时间范围: {START_DATE} ~ {END_DATE}")
    print(f"批次大小: {batch_size}")
    print(f"批次间延迟: {BATCH_DELAY}秒")
    print(f"请求间延迟: {REQUEST_DELAY}秒")
    print("=" * 60)

    # 获取所有股票代码
    codes = get_all_cached_codes()
    total = len(codes)
    print(f"\n待检查股票: {total} 只")

    # 加载checkpoint
    processed_codes = load_checkpoint() if resume else []
    if processed_codes:
        print(f"从checkpoint恢复: 已处理 {len(processed_codes)} 只")

    # 分析数据缺失情况
    print("\n[Step 1] 分析数据缺失情况...")
    need_backfill_list = []

    for i, code in enumerate(codes):
        if code in processed_codes:
            continue

        gap_info = check_data_gap(code)

        if gap_info['need_backfill']:
            need_backfill_list.append(gap_info)

        if (i + 1) % 500 == 0:
            print(f"  已检查 {i+1}/{total}...")

    print(f"\n需要补齐的股票: {len(need_backfill_list)} 只")

    if not need_backfill_list:
        print("所有数据已完整，无需补齐")
        clear_checkpoint()
        return

    # 分批补齐
    print("\n[Step 2] 开始分批补齐...")
    success_count = 0
    fail_count = 0

    batches = [need_backfill_list[i:i+batch_size] for i in range(0, len(need_backfill_list), batch_size)]

    for batch_idx, batch in enumerate(batches):
        print(f"\n--- 批次 {batch_idx+1}/{len(batches)} ({len(batch)}只) ---")

        batch_success = 0
        batch_fail = 0

        for item in batch:
            code = item['code']
            start = item['backfill_start']
            end = item['backfill_end']

            print(f"  补齐 {code}: {start} ~ {end}...", end=' ')

            success = backfill_kline(code, start, end)

            if success:
                batch_success += 1
                print("✓")
            else:
                batch_fail += 1
                print("×")

            time.sleep(REQUEST_DELAY)

        success_count += batch_success
        fail_count += batch_fail

        # 保存checkpoint
        processed_codes.extend([item['code'] for item in batch])
        save_checkpoint(processed_codes, len(need_backfill_list))

        print(f"  批次结果: 成功 {batch_success}, 失败 {batch_fail}")

        # 批次间延迟（最后一批不需要）
        if batch_idx < len(batches) - 1:
            print(f"  等待 {BATCH_DELAY}秒...")
            time.sleep(BATCH_DELAY)

    # 补齐完成
    print("\n" + "=" * 60)
    print("补齐完成!")
    print("=" * 60)
    print(f"总计: 成功 {success_count}, 失败 {fail_count}")
    print("=" * 60)

    clear_checkpoint()


def analyze_data_coverage():
    """
    分析当前数据覆盖情况（不执行补齐）
    """
    print("=" * 60)
    print("K线数据覆盖分析")
    print("=" * 60)

    codes = get_all_cached_codes()
    total = len(codes)
    print(f"缓存股票数: {total} 只")

    # 统计
    stats = {
        'full_coverage': 0,  # 完整覆盖
        'front_gap': 0,      # 前端缺失
        'back_gap': 0,       # 后端缺失
        'both_gap': 0,       # 两端缺失
        'no_cache': 0        # 无缓存
    }

    gap_details = []

    for i, code in enumerate(codes):
        gap_info = check_data_gap(code)

        if not gap_info['has_cache']:
            stats['no_cache'] += 1
        elif not gap_info['need_backfill']:
            stats['full_coverage'] += 1
        else:
            # 判断缺失类型
            front_missing = gap_info['cache_start'] > START_DATE
            back_missing = gap_info['cache_end'] < END_DATE

            if front_missing and back_missing:
                stats['both_gap'] += 1
            elif front_missing:
                stats['front_gap'] += 1
            else:
                stats['back_gap'] += 1

            gap_details.append(gap_info)

        if (i + 1) % 500 == 0:
            print(f"  已分析 {i+1}/{total}...")

    print("\n" + "-" * 60)
    print("覆盖统计:")
    print(f"  完整覆盖: {stats['full_coverage']} 只")
    print(f"  前端缺失: {stats['front_gap']} 只")
    print(f"  后端缺失: {stats['back_gap']} 只")
    print(f"  两端缺失: {stats['both_gap']} 只")
    print(f"  无缓存: {stats['no_cache']} 只")
    print("-" * 60)

    if gap_details:
        print(f"\n需要补齐: {len(gap_details)} 只")
        print("示例（前10只）:")
        for item in gap_details[:10]:
            print(f"  {item['code']}: {item['cache_start']}~{item['cache_end']} → 需补齐 {item['backfill_start']}~{item['backfill_end']}")

    return stats, gap_details


# ==================== CLI ====================

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='K线数据补齐工具')
    parser.add_argument('--analyze', action='store_true', help='只分析不补齐')
    parser.add_argument('--run', action='store_true', help='执行补齐')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE, help='批次大小')
    parser.add_argument('--no-resume', action='store_true', help='不使用checkpoint恢复')

    args = parser.parse_args()

    if args.analyze:
        analyze_data_coverage()
    elif args.run:
        run_backfill(batch_size=args.batch_size, resume=not args.no_resume)
    else:
        # 默认：先分析，再确认是否执行
        stats, gap_details = analyze_data_coverage()

        if gap_details:
            print("\n是否执行补齐？输入 'y' 确认:")
            confirm = input().strip().lower()
            if confirm == 'y':
                run_backfill(batch_size=args.batch_size)
        else:
            print("\n数据已完整，无需补齐")