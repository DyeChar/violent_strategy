"""
回测执行器模块（支持断点续测）
整合数据层、策略层、收益计算、统计分析，执行完整回测流程

回测流程：
1. 获取交易日列表
2. 确定股票池（涨停池候选）
3. 遍历每个交易日检测信号
4. 计算各持有期收益
5. 统计分析
6. 保存报告和信号记录

特性：
- 断点续测：自动保存checkpoint，中断后可恢复
- 信号记录：保存完整信号JSON，供后续策略优化分析
- 无未来数据：T日信号只用T日及之前数据

checkpoint文件：output/backtest_checkpoint.json
信号记录文件：output/reports/signals_{timestamp}.json
"""

import os
import json
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

import config
from data.limit_up_pool import get_trade_dates, get_candidate_pool
from data.fetcher import batch_fetch_klines, load_kline_cache
from data.stock_pool import get_stock_pool
from strategy.detector import detect_signal
from .returns import calculate_batch_returns, save_returns_to_csv
from .stats import calculate_period_stats, print_stats_table, save_stats_to_csv
from .visualizer import plot_signal_chart


# checkpoint文件路径
CHECKPOINT_FILE = os.path.join(config.OUTPUT_DIR, 'backtest_checkpoint.json')


def _convert_numpy(obj):
    """转换numpy类型为Python原生类型（用于JSON序列化）"""
    if isinstance(obj, dict):
        return {k: _convert_numpy(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_convert_numpy(v) for v in obj]
    elif isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    elif isinstance(obj, (np.bool_, bool)):
        return bool(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj


def save_checkpoint(data: Dict):
    """保存checkpoint"""
    converted = _convert_numpy(data)
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        json.dump(converted, f, ensure_ascii=False, indent=2)
    print(f"  [Checkpoint已保存: 步骤{data.get('step', 0)}]")


def load_checkpoint() -> Optional[Dict]:
    """加载checkpoint"""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"加载checkpoint失败: {e}")
    return None


def clear_checkpoint():
    """清除checkpoint"""
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print("Checkpoint已清除")


def save_signals_json(signals: List[Dict], output_file: str = None) -> str:
    """
    保存完整信号记录到JSON（供后续策略优化分析）

    Args:
        signals: 信号列表（detect_signal的完整结果）
        output_file: 输出文件路径

    Returns:
        str: 保存的文件路径
    """
    if not signals:
        return ''

    output_file = output_file or os.path.join(
        config.REPORTS_DIR,
        f'signals_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    )

    # 转换numpy类型
    converted = _convert_numpy(signals)

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(converted, f, ensure_ascii=False, indent=2)

    print(f"信号记录已保存: {output_file}")
    return output_file


def run_backtest(
    start_date: str = None,
    end_date: str = None,
    periods: List[int] = None,
    use_cache: bool = True,
    generate_charts: bool = False,
    verbose: bool = True,
    use_stock_pool: str = None,
    resume: bool = True,
    save_signals: bool = True
) -> Tuple[List[Dict], Dict, str]:
    """
    执行完整回测流程（支持断点续测）

    Args:
        start_date: 回测起始日期 YYYY-MM-DD
        end_date: 回测结束日期 YYYY-MM-DD
        periods: 持有期列表
        use_cache: 是否使用K线缓存
        generate_charts: 是否生成可视化图表
        verbose: 是否打印详细信息
        use_stock_pool: 使用预定义股票池替代涨停池
        resume: 是否从checkpoint恢复
        save_signals: 是否保存完整信号记录JSON

    Returns:
        Tuple: (收益结果列表, 统计指标字典, 信号JSON文件路径)
    """
    start_date = start_date or config.BACKTEST_START_DATE
    end_date = end_date or datetime.now().strftime('%Y-%m-%d')
    periods = periods or config.HOLDING_PERIODS

    print("=" * 60)
    print("暴力战法回测系统")
    print("=" * 60)
    print("暴力战法策略回测（支持断点续测）")
    print("=" * 60)
    print(f"回测区间: {start_date} ~ {end_date}")
    print(f"持有期: {periods}")
    if use_stock_pool:
        print(f"股票池: 预定义池({use_stock_pool})")
    else:
        print(f"涨停阈值: {config.LIMIT_UP_THRESHOLD}%")
        print(f"涨停池回看: {config.LIMIT_UP_POOL_LOOKBACK}天")
    print("=" * 60)

    # 尝试加载checkpoint
    checkpoint = None
    if resume:
        checkpoint = load_checkpoint()
        if checkpoint:
            print(f"\n发现checkpoint: 步骤{checkpoint.get('step', 0)}")
            # 检查参数是否匹配
            if checkpoint.get('start_date') != start_date or checkpoint.get('end_date') != end_date:
                print("参数不匹配，重新开始")
                checkpoint = None
                clear_checkpoint()
            elif checkpoint.get('use_stock_pool') != use_stock_pool:
                print("股票池不匹配，重新开始")
                checkpoint = None
                clear_checkpoint()

    # ========== Step 1: 获取交易日列表 ==========
    if checkpoint and checkpoint.get('step', 0) >= 1:
        trade_dates = checkpoint.get('trade_dates', [])
        print(f"\n[Step 1] 从checkpoint恢复: 交易日 {len(trade_dates)} 个")
    else:
        print("\n[Step 1] 获取交易日列表...")
        trade_dates = get_trade_dates(start_date, end_date)
        print(f"交易日数量: {len(trade_dates)}")

        if not trade_dates:
            print("无法获取交易日列表，回测终止")
            return [], {}, ''

        # 初始化checkpoint
        checkpoint = {
            'start_date': start_date,
            'end_date': end_date,
            'use_stock_pool': use_stock_pool,
            'step': 1,
            'trade_dates': trade_dates,
            'periods': periods
        }
        save_checkpoint(checkpoint)

    # ========== Step 2: 确定股票池 ==========
    klines = {}
    all_candidates = []
    fixed_pool = []

    if checkpoint and checkpoint.get('step', 0) >= 2:
        all_candidates = checkpoint.get('all_candidates', [])
        fixed_pool = checkpoint.get('fixed_pool', [])
        klines_loaded = checkpoint.get('klines_loaded', False)

        print(f"\n[Step 2] 从checkpoint恢复:")
        if use_stock_pool:
            print(f"  固定股票池: {len(fixed_pool)} 只")
        else:
            print(f"  候选池股票: {len(all_candidates)} 只")

        if klines_loaded:
            print("  K线已从缓存加载（跳过API获取）")
            # 从缓存加载K线
            codes_to_load = fixed_pool if use_stock_pool else all_candidates
            for code in codes_to_load:
                kline = load_kline_cache(code)
                if not kline.empty and len(kline) >= config.MA20_PERIOD:
                    klines[code] = kline
            print(f"  成功加载K线: {len(klines)} 只")
    else:
        print("\n[Step 2] 确定股票池...")

        if use_stock_pool:
            fixed_pool = get_stock_pool(pool_type=use_stock_pool)
            print(f"使用预定义股票池: {len(fixed_pool)} 只")
            all_candidates = fixed_pool
        else:
            # 收集所有需要K线的股票
            all_candidates = set()
            for date in trade_dates:
                candidates = get_candidate_pool(date)
                all_candidates.update(candidates)
            all_candidates = list(all_candidates)
            print(f"候选池涉及股票: {len(all_candidates)} 只")

        if not all_candidates:
            print("候选池为空，回测终止")
            clear_checkpoint()
            return [], {}, ''

        # 批量获取K线
        print(f"开始获取K线数据（{len(all_candidates)}只股票）...")
        klines = batch_fetch_klines(all_candidates, use_cache=use_cache)

        # 更新checkpoint
        checkpoint['step'] = 2
        checkpoint['all_candidates'] = all_candidates
        checkpoint['fixed_pool'] = fixed_pool
        checkpoint['klines_loaded'] = True
        save_checkpoint(checkpoint)

    # ========== Step 3: 遍历交易日检测信号 ==========
    all_signals = checkpoint.get('signals', []) if checkpoint else []
    processed_idx = checkpoint.get('processed_idx', 0) if checkpoint else 0

    print(f"\n[Step 3] 遍历交易日检测信号...")
    if processed_idx > 0:
        print(f"  从断点恢复: 已处理 {processed_idx}/{len(trade_dates)} 个交易日")

    for i in range(processed_idx, len(trade_dates)):
        trade_date = trade_dates[i]

        if use_stock_pool:
            candidates = fixed_pool
        else:
            candidates = get_candidate_pool(trade_date)

        if not candidates:
            continue

        date_signals = []
        for code in candidates:
            kline = klines.get(code)
            if kline is None or kline.empty:
                continue

            kline_until = kline[kline['date'] <= trade_date].copy()
            if len(kline_until) < config.MA20_PERIOD + 10:
                continue

            result = detect_signal(kline_until, trade_date)
            if result['signal']:
                # 补充股票名称
                if 'name' in kline.columns and not result['name']:
                    result['name'] = kline['name'].iloc[-1]
                date_signals.append(result)

                if verbose:
                    signal_display = f"{result['code']} {result['name']}" if result['name'] else result['code']
                    print(f"    ✓ 发现信号: {signal_display} @ {trade_date}")

        all_signals.extend(date_signals)

        # 每20个交易日保存checkpoint
        if (i + 1) % 20 == 0:
            checkpoint['step'] = 3
            checkpoint['signals'] = all_signals
            checkpoint['processed_idx'] = i + 1
            save_checkpoint(checkpoint)

            if verbose:
                print(f"  已处理 {i+1}/{len(trade_dates)} 个交易日, "
                      f"候选池: {len(candidates)}只, 信号: {len(date_signals)}个, "
                      f"累计信号: {len(all_signals)}个")

    print(f"\n回测期间共发现 {len(all_signals)} 个信号")

    # 更新checkpoint（信号检测完成）
    checkpoint['step'] = 4
    checkpoint['signals'] = all_signals
    checkpoint['processed_idx'] = len(trade_dates)
    save_checkpoint(checkpoint)

    if not all_signals:
        print("无信号，回测结束")
        clear_checkpoint()
        return [], {}, ''

    # ========== Step 4: 计算固定持有期收益 ==========
    print("\n[Step 4] 计算固定持有期收益...")
    results = calculate_batch_returns(klines, all_signals, periods)

    # ========== Step 5: 统计分析 ==========
    print("\n[Step 5] 统计分析...")
    stats = calculate_period_stats(results, periods)
    print_stats_table(stats)

    # ========== Step 6: 保存报告 ==========
    print("\n[Step 6] 保存回测报告...")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # 保存收益明细CSV
    returns_file = os.path.join(config.REPORTS_DIR, f'backtest_returns_{timestamp}.csv')
    save_returns_to_csv(results, returns_file)

    # 保存统计汇总CSV
    stats_file = os.path.join(config.REPORTS_DIR, f'backtest_stats_{timestamp}.csv')
    save_stats_to_csv(stats, stats_file)

    # 保存完整信号记录JSON（供后续策略优化）
    signals_file = ''
    if save_signals:
        signals_file = save_signals_json(all_signals)

    # ========== Step 7: 生成可视化（可选） ==========
    if generate_charts and all_signals:
        print("\n[Step 7] 生成信号可视化...")
        chart_count = 0

        for signal in all_signals[:20]:
            code = signal['code']
            kline = klines.get(code)

            if kline is None:
                continue

            try:
                chart_path = plot_signal_chart(kline, signal)
                if chart_path:
                    chart_count += 1
            except Exception as e:
                if verbose:
                    signal_display = f"{signal['code']} {signal['name']}" if signal['name'] else signal['code']
                    print(f"  {signal_display} 可视化失败: {e}")

        print(f"生成 {chart_count} 张可视化图表")

    # 清除checkpoint（回测完成）
    clear_checkpoint()

    print("\n" + "=" * 60)
    print("回测完成!")
    print("=" * 60)
    print(f"收益明细: {returns_file}")
    print(f"统计汇总: {stats_file}")
    if signals_file:
        print(f"信号记录: {signals_file}")
    print("=" * 60)

    return results, stats, signals_file


# ==================== 测试代码 ====================

if __name__ == '__main__':
    print("=" * 50)
    print("回测执行器测试")
    print("=" * 50)

    # 运行回测
    results, stats, signals_file = run_backtest(
        start_date='2025-03-01',
        end_date='2025-04-30',
        generate_charts=False,
        verbose=True
    )

    if signals_file:
        print(f"\n信号文件: {signals_file}")