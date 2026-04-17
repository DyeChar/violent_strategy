"""
每日信号监控模块
每日收盘后扫描涨停池，检测信号，发送微信推送

流程：
1. 更新涨停池（扫描今日涨停）
2. 获取候选池（近30天涨停股）
3. 更新候选池股票的K线
4. 在候选池中检测今日信号
5. 保存信号记录
6. 推送微信通知

与回测共用核心函数：get_candidate_pool, detect_signal
"""

import os
import json
import time
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import config
from data.limit_up_pool import update_limit_up_history, get_candidate_pool
from data.fetcher import batch_fetch_klines
from strategy.detector import detect_signal
from backtest.visualizer import plot_signal_chart
from .push import ServerChanPusher, send_test_message


class DailyMonitor:
    """每日信号监控器"""

    def __init__(self, send_key: str = None):
        """
        Args:
            send_key: Server酱SendKey
        """
        self.send_key = send_key or os.environ.get(config.SERVERCHAN_KEY_ENV, '')
        self.pusher = ServerChanPusher(self.send_key)
        self.today = datetime.now().strftime('%Y-%m-%d')
        # 实际检测日期（根据K线数据最新日期确定）
        self.signal_date = None

    def update_pool(self) -> Dict:
        """
        更新涨停池

        Returns:
            Dict: 涨停池更新信息
        """
        print("\n[Step 1] 更新涨停池...")
        history = update_limit_up_history(self.today)

        # 统计今日涨停数
        today_limit_ups = history[history['日期'] == self.today]
        limit_up_count = len(today_limit_ups)

        print(f"今日涨停股: {limit_up_count} 只")

        return {
            'limit_up_count': limit_up_count,
            'date': self.today
        }

    def get_candidates(self) -> List[str]:
        """
        获取今日候选池

        Returns:
            List[str]: 候选股代码列表
        """
        print("\n[Step 2] 获取候选池...")
        candidates = get_candidate_pool(self.today)

        return candidates

    def scan_signals(self, candidates: List[str]) -> List[Dict]:
        """
        扫描候选池检测信号（与回测共用detect_signal函数）

        每个股票用自己的K线最新日期检测信号

        Args:
            candidates: 候选股代码列表

        Returns:
            List[Dict]: 信号列表
        """
        print("\n[Step 3] 更新候选池K线...")
        klines = batch_fetch_klines(candidates)

        print("\n[Step 4] 检测信号（使用各股票K线最新日期）...")
        signals = []

        for code in candidates:
            kline = klines.get(code)

            if kline is None or kline.empty or len(kline) < config.MA20_PERIOD + 10:
                name = kline['name'].iloc[-1] if kline is not None and 'name' in kline.columns else ''
                display = f"{code} {name}" if name else code
                print(f"  {display}: 数据不足")
                continue

            # 获取股票名称和最新日期
            name = kline['name'].iloc[-1] if 'name' in kline.columns else ''
            latest_date = kline['date'].max()
            display = f"{code} {name}" if name else code

            # 用该股票的K线最新日期检测信号
            result = detect_signal(kline, latest_date)

            if result['signal']:
                signals.append(result)
                signal_name = result.get('name', name)
                signal_display = f"{result['code']} {signal_name}" if signal_name else result['code']
                print(f"  {signal_display}: ✓ 发现信号! (日期: {latest_date})")
            else:
                print(f"  {display}: × {result['reason']}")

        print(f"\n发现 {len(signals)} 个信号")
        return signals

    def save_signals(self, signals: List[Dict]) -> str:
        """
        保存信号记录

        Args:
            signals: 信号列表

        Returns:
            str: 保存的文件路径
        """
        if not signals:
            return ''

        os.makedirs(config.REPORTS_DIR, exist_ok=True)
        signal_file = os.path.join(config.REPORTS_DIR, f'signals_{self.today.replace("-", "")}.json')

        with open(signal_file, 'w', encoding='utf-8') as f:
            json.dump(signals, f, ensure_ascii=False, indent=2)

        print(f"信号已保存: {signal_file}")
        return signal_file

    def generate_charts(self, signals: List[Dict], klines: Dict[str, pd.DataFrame]) -> List[str]:
        """
        生成信号可视化图表

        Args:
            signals: 信号列表
            klines: K线数据

        Returns:
            List[str]: 图表路径列表
        """
        if not signals:
            return []

        print("\n[Step 5] 生成信号可视化...")
        chart_paths = []

        for signal in signals[:10]:  # 最多生成10张
            code = signal['code']
            kline = klines.get(code)

            if kline is None:
                continue

            try:
                path = plot_signal_chart(kline, signal)
                if path:
                    chart_paths.append(path)
            except Exception as e:
                signal_display = f"{signal['code']} {signal['name']}" if signal['name'] else signal['code']
                print(f"  {signal_display} 可视化失败: {e}")

        print(f"生成 {len(chart_paths)} 张图表")
        return chart_paths

    def push_notification(self, signals: List[Dict], pool_info: Dict) -> bool:
        """
        推送微信通知

        Args:
            signals: 信号列表
            pool_info: 候选池信息

        Returns:
            bool: 是否推送成功
        """
        print("\n[Step 6] 推送微信通知...")
        title, content = self.pusher.format_signal_message(signals, pool_info)

        return self.pusher.push(title, content)

    def run(self, push: bool = True, generate_charts: bool = True) -> List[Dict]:
        """
        执行每日监控流程

        Args:
            push: 是否推送微信
            generate_charts: 是否生成可视化图表

        Returns:
            List[Dict]: 今日信号列表
        """
        print("=" * 60)
        print("暴力战法每日信号监控")
        print("=" * 60)
        print(f"当前日期: {self.today}")
        print(f"运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

        # 1. 更新涨停池
        pool_result = self.update_pool()

        # 2. 获取候选池
        candidates = self.get_candidates()

        if not candidates:
            print("\n候选池为空，今日无监控任务")

            # 推送涨停池更新消息
            if push:
                title, content = self.pusher.format_pool_update_message(
                    pool_result['limit_up_count'],
                    self.today
                )
                self.pusher.push(title, content)

            return []

        # 3. 扫描信号
        signals = self.scan_signals(candidates)

        # 4. 保存信号
        self.save_signals(signals)

        # 5. 生成可视化（可选）
        if generate_charts and signals:
            # 重新获取K线（scan_signals已经获取过，这里用缓存）
            klines = batch_fetch_klines([s['code'] for s in signals])
            self.generate_charts(signals, klines)

        # 6. 推送通知
        pool_info = {
            'source': '涨停池（近30天涨停）',
            'size': len(candidates)
        }

        if push:
            self.push_notification(signals, pool_info)

        print("\n" + "=" * 60)
        print("每日监控完成!")
        if self.signal_date:
            print(f"检测日期: {self.signal_date}")
        print("=" * 60)

        return signals


def run_daily_monitor(push: bool = True, send_key: str = None) -> List[Dict]:
    """
    运行每日监控（简化入口）

    Args:
        push: 是否推送微信
        send_key: Server酱SendKey

    Returns:
        List[Dict]: 今日信号列表
    """
    monitor = DailyMonitor(send_key)
    return monitor.run(push=push)


# ==================== 测试代码 ====================

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='暴力战法每日监控')
    parser.add_argument('--push', action='store_true', help='发送微信推送')
    parser.add_argument('--test', action='store_true', help='发送测试消息')
    parser.add_argument('--key', type=str, help='Server酱SendKey')

    args = parser.parse_args()

    # 测试模式
    if args.test:
        send_test_message(args.key)
    else:
        # 正常监控
        run_daily_monitor(push=args.push, send_key=args.key)