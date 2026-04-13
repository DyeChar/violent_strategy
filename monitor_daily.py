"""
每日信号监控模块
扫描创业板股票，检测方案B信号，并发送微信推送（Server酱）

使用方式：
    python monitor_daily.py [--push] [--test]

参数：
    --push: 发送微信推送（需要Server酱 SendKey）
    --test: 测试模式，发送测试消息

Server酱注册：https://sct.ftqq.com 微信扫码获取SendKey
"""

import os
import sys
import json
import time
import requests
from datetime import datetime, timedelta
from typing import List, Dict
import pandas as pd
import numpy as np

# 导入策略模块
import data_fetcher
import backtest
import stock_pool
import config


class SignalMonitor:
    """信号监控器"""

    def __init__(self, send_key: str = None):
        """
        Args:
            send_key: Server酱 SendKey（从 sct.ftqq.com 获取）
        """
        self.send_key = send_key or os.environ.get('SERVERCHAN_KEY', '')
        self.today = datetime.now().strftime('%Y%m%d')

    def scan_all_stocks(self, stock_codes: List[str] = None) -> List[Dict]:
        """
        扫描股票池，检测所有信号

        Args:
            stock_codes: 股票代码列表，默认使用创业板龙头池

        Returns:
            List[Dict]: 信号列表
        """
        if stock_codes is None:
            stock_codes = stock_pool.创业板龙头池

        print(f"开始扫描 {len(stock_codes)} 只股票...")
        print(f"今日日期: {self.today}")

        # 获取最近3个月的K线数据
        start_date = (datetime.now() - timedelta(days=90)).strftime('%Y%m%d')
        end_date = self.today

        signals = []
        scanned = 0

        for code in stock_codes:
            try:
                # 获取K线数据
                kline = data_fetcher.get_stock_kline(code, start_date=start_date, end_date=end_date)

                if kline.empty or len(kline) < 30:
                    continue

                # 获取股票名称
                name = kline['name'].iloc[-1] if 'name' in kline.columns else f'股票{code}'

                # 检测方案B信号
                detected = backtest.find_signals_plan_b(kline)

                # 只保留今日信号（最新一天）
                for signal in detected:
                    # 检查是否是最新一天的信号
                    latest_date = kline['date'].iloc[-1]
                    signal_date = signal['signal_date']

                    # 兼容不同日期格式
                    if isinstance(latest_date, str):
                        latest_date_str = latest_date.replace('-', '')
                    else:
                        latest_date_str = latest_date.strftime('%Y%m%d')

                    if isinstance(signal_date, str):
                        signal_date_str = signal_date.replace('-', '')
                    else:
                        signal_date_str = signal_date.strftime('%Y%m%d')

                    # 只保留今天的信号
                    if signal_date_str == self.today or signal_date_str == latest_date_str:
                        signal['code'] = code
                        signal['name'] = name
                        signal['today_close'] = kline['close'].iloc[-1]
                        signal['today_date'] = self.today
                        signals.append(signal)

                scanned += 1
                if scanned % 20 == 0:
                    print(f"已扫描 {scanned}/{len(stock_codes)} 只...")

                # 延迟避免API限制
                time.sleep(0.3)

            except Exception as e:
                print(f"扫描 {code} 失败: {e}")
                continue

        print(f"扫描完成，发现 {len(signals)} 个信号")
        return signals

    def format_message(self, signals: List[Dict]) -> tuple:
        """
        格式化推送消息（Server酱格式）

        Args:
            signals: 信号列表

        Returns:
            tuple: (title, content) 标题和内容
        """
        if not signals:
            title = "暴力战法监控-无信号"
            content = """📊 今日扫描：无信号

今日创业板龙头池未发现符合条件的信号。

💡 策略条件：
• 放量吸筹：连续≥2天成交量≥2倍均量
• 回踩MA20：收盘价距离20日线±3%
• 底分型：T-1日最低点确认反转
• 红肥绿瘦：上涨日成交量/下跌日成交量≥1.2

⏰ 下次扫描：明日15:30收盘后"""
            return title, content

        # 有信号
        title = f"暴力战法信号({len(signals)}只)"

        content_lines = [
            f"📊 **今日信号：{self.today}**",
            f"股票池：创业板龙头池（{len(stock_pool.创业板龙头池)}只）",
            "",
            "---",
            f"### 🎯 信号详情（共{len(signals)}只）",
            ""
        ]

        for i, sig in enumerate(signals, 1):
            code = sig.get('code', 'N/A')
            name = sig.get('name', 'N/A')
            close = sig.get('today_close', sig.get('close', 0))
            ma20 = sig.get('ma20', 0)
            ma5 = sig.get('ma5', 0)

            # 计算距离MA20
            ma_distance = abs(close - ma20) / ma20 * 100 if ma20 > 0 else 0

            # 放量天数
            vol_days = sig.get('volume_days', 0)

            # 红肥绿瘦比例
            red_green = sig.get('red_green_ratio', 0)

            content_lines.extend([
                f"**{i}. {code} {name}**",
                f"- 收盘价：{close:.2f}",
                f"- MA20：{ma20:.2f}（距离{ma_distance:.1f}%）",
                f"- MA5：{ma5:.2f}",
                f"- 底分型：T-1日确认 ✓",
                f"- 放量吸筹：连续{vol_days}天 ✓",
                f"- 红肥绿瘦：{red_green:.1f}倍 ✓",
                f"- **建议：明日开盘价买入**",
                ""
            ])

        content_lines.extend([
            "---",
            "### 💡 操作提示",
            "- T日信号 → T+1日开盘买入",
            "- 每次投入5万元",
            "- 止盈20%自动卖出",
            "- 跌破5日线止损",
            "",
            "⏰ 下次扫描：明日15:30"
        ])

        return title, '\n'.join(content_lines)

    def push_to_wechat(self, title: str, content: str) -> bool:
        """
        发送微信推送（通过Server酱Turbo版）

        Args:
            title: 消息标题
            content: 消息内容（支持Markdown）

        Returns:
            bool: 是否发送成功
        """
        if not self.send_key:
            print("未配置Server酱 SendKey，跳过推送")
            print("获取SendKey: https://sct.ftqq.com 微信扫码注册")
            return False

        url = f"https://sctapi.ftqq.com/{self.send_key}.send"
        data = {
            "title": title,
            "desp": content  # Server酱支持Markdown格式
        }

        try:
            response = requests.post(url, data=data, timeout=10)
            result = response.json()

            # Server酱返回格式：{"code": 0, "message": "success", "data": {...}}
            if result.get('code') == 0 or result.get('message') == 'success':
                print(f"✅ 推送成功！请检查微信")
                return True
            else:
                print(f"❌ 推送失败: {result.get('message', '未知错误')}")
                return False

        except Exception as e:
            print(f"❌ 推送异常: {e}")
            return False

    def save_signals(self, signals: List[Dict], output_file: str = None):
        """
        保存信号到文件

        Args:
            signals: 信号列表
            output_file: 输出文件路径
        """
        if output_file is None:
            output_file = os.path.join(config.OUTPUT_DIR, f'signals_{self.today}.json')

        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(signals, f, ensure_ascii=False, indent=2)

        print(f"信号已保存: {output_file}")

    def run(self, push: bool = True, save: bool = True):
        """
        执行监控流程

        Args:
            push: 是否推送微信
            save: 是否保存信号文件
        """
        print("=" * 50)
        print("暴力战法每日信号监控")
        print("=" * 50)

        # 扫描信号
        signals = self.scan_all_stocks()

        # 格式化消息
        title, content = self.format_message(signals)
        print("\n" + content)

        # 保存信号
        if save and signals:
            self.save_signals(signals)

        # 推送微信
        if push:
            self.push_to_wechat(title, content)


def send_test_message(send_key: str = None):
    """
    发送测试消息，验证推送配置
    """
    key = send_key or os.environ.get('SERVERCHAN_KEY', '')

    if not key:
        print("❌ 未配置Server酱 SendKey")
        print("请设置环境变量 SERVERCHAN_KEY 或在代码中传入send_key")
        print("获取SendKey: https://sct.ftqq.com 微信扫码注册")
        return False

    test_title = "暴力战法-测试消息"
    test_content = """✅ **推送配置成功！**

这是暴力战法监控系统的测试消息。

如果您收到此消息，说明Server酱推送配置正确！

---

**免费额度**：每天5条消息
**注册方式**：微信扫码即可，无需实名认证

⏰ 每日15:30自动扫描信号"""

    url = f"https://sctapi.ftqq.com/{key}.send"
    data = {
        "title": test_title,
        "desp": test_content
    }

    try:
        response = requests.post(url, data=data, timeout=10)
        result = response.json()

        if result.get('code') == 0 or result.get('message') == 'success':
            print("✅ 测试消息发送成功！请检查微信是否收到")
            return True
        else:
            print(f"❌ 发送失败: {result.get('message')}")
            return False

    except Exception as e:
        print(f"❌ 发送异常: {e}")
        return False


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='暴力战法每日信号监控')
    parser.add_argument('--push', action='store_true', help='发送微信推送')
    parser.add_argument('--test', action='store_true', help='发送测试消息')
    parser.add_argument('--key', type=str, help='Server酱 SendKey')

    args = parser.parse_args()

    # 测试模式
    if args.test:
        send_test_message(args.key)
        sys.exit(0)

    # 正常监控
    monitor = SignalMonitor(send_key=args.key)
    monitor.run(push=args.push)