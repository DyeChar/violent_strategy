"""
Server酱推送模块
通过Server酱Turbo版发送微信推送

API文档：https://sct.ftqq.com/
每日免费额度：5条消息
"""

import os
import requests
from typing import Tuple, List, Dict, Optional
from datetime import datetime

import config


class ServerChanPusher:
    """Server酱推送器"""

    def __init__(self, send_key: str = None):
        """
        Args:
            send_key: Server酱SendKey（从 sct.ftqq.com 获取）
        """
        self.send_key = send_key or os.environ.get(config.SERVERCHAN_KEY_ENV, '')

    def push(self, title: str, content: str) -> bool:
        """
        发送微信推送

        Args:
            title: 消息标题
            content: 消息内容（支持Markdown格式）

        Returns:
            bool: 是否发送成功
        """
        if not self.send_key:
            print("未配置Server酱SendKey，跳过推送")
            print("获取SendKey: https://sct.ftqq.com 微信扫码注册")
            return False

        url = config.SERVERCHAN_API_URL.format(send_key=self.send_key)
        data = {
            "title": title,
            "desp": content
        }

        try:
            response = requests.post(url, data=data, timeout=10)
            result = response.json()

            if result.get('code') == 0 or result.get('message') == 'success':
                print("推送成功！请检查微信")
                return True
            else:
                print(f"推送失败: {result.get('message', '未知错误')}")
                return False

        except Exception as e:
            print(f"推送异常: {e}")
            return False

    def format_signal_message(self, signals: List[Dict], pool_info: Dict = None) -> Tuple[str, str]:
        """
        格式化信号推送消息（保留原格式）

        Args:
            signals: 信号列表
            pool_info: 候选池信息 {'source': str, 'size': int}

        Returns:
            Tuple: (标题, 内容)
        """
        today = datetime.now().strftime('%Y-%m-%d')
        pool_source = pool_info.get('source', '涨停池')
        pool_size = pool_info.get('size', 0)

        if not signals:
            title = "暴力战法监控-无信号"
            content = f"""**今日扫描：无信号**

候选池：{pool_source}（{pool_size}只）未发现符合条件的信号。

**策略条件：**
- 放量吸筹：连续≥2天成交量≥2倍均量
- 回踩MA20：收盘价距离20日线±3%
- 底分型：T-1日最低点确认反转
- 红肥绿瘦：上涨日成交量/下跌日成交量≥1.2

下次扫描：明日15:30收盘后"""
            return title, content

        # 有信号
        title = f"暴力战法信号({len(signals)}只)"

        content_lines = [
            f"**今日信号：{today}**",
            f"候选池：{pool_source}（{pool_size}只）",
            "",
            "---",
            f"### 信号详情（共{len(signals)}只）",
            ""
        ]

        for i, sig in enumerate(signals, 1):
            code = sig.get('code', 'N/A')
            name = sig.get('name', 'N/A')
            close = sig.get('close', 0)
            ma20 = sig.get('ma20', 0)
            ma5 = sig.get('ma5', close)  # 可选字段

            ma_distance = abs(close - ma20) / ma20 * 100 if ma20 > 0 else 0
            vol_days = sig.get('volume_days', 0)
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
            "### 操作提示",
            "- T日信号 → T+1日开盘买入",
            "- 每次投入5万元",
            "- 止盈20%自动卖出",
            "- 跌破5日线止损",
            "",
            "下次扫描：明日15:30"
        ])

        return title, '\n'.join(content_lines)

    def format_pool_update_message(self, limit_up_count: int, date: str) -> Tuple[str, str]:
        """
        格式化涨停池更新消息

        Args:
            limit_up_count: 今日涨停股数量
            date: 日期

        Returns:
            Tuple: (标题, 内容)
        """
        title = f"涨停池更新({limit_up_count}只)"

        content = f"""**涨停池更新 - {date}**

今日涨停股：{limit_up_count}只

近30天涨停池已更新
下次信号检测将只扫描涨停池股票

涨停池文件已保存"""

        return title, content


def send_test_message(send_key: str = None) -> bool:
    """
    发送测试消息，验证推送配置

    Args:
        send_key: Server酱SendKey

    Returns:
        bool: 是否发送成功
    """
    key = send_key or os.environ.get(config.SERVERCHAN_KEY_ENV, '')

    if not key:
        print("未配置Server酱SendKey")
        print("请设置环境变量 SERVERCHAN_KEY 或在代码中传入send_key")
        print("获取SendKey: https://sct.ftqq.com 微信扫码注册")
        return False

    pusher = ServerChanPusher(key)

    test_title = "暴力战法-测试消息"
    test_content = """**推送配置成功！**

这是暴力战法监控系统的测试消息。

如果您收到此消息，说明Server酱推送配置正确！

---

**免费额度**：每天5条消息
**注册方式**：微信扫码即可

下次扫描：每日15:30"""

    return pusher.push(test_title, test_content)


# ==================== 测试代码 ====================

if __name__ == '__main__':
    print("=" * 50)
    print("推送模块测试")
    print("=" * 50)

    # 测试格式化消息
    pusher = ServerChanPusher()

    # 无信号消息
    title, content = pusher.format_signal_message([], {'source': '涨停池', 'size': 150})
    print(f"\n无信号消息:\n标题: {title}")
    print(f"内容:\n{content}")

    # 有信号消息
    test_signals = [
        {
            'code': '300001',
            'name': '特锐德',
            'close': 25.68,
            'ma20': 25.50,
            'ma5': 25.60,
            'volume_days': 3,
            'red_green_ratio': 1.5
        }
    ]

    title, content = pusher.format_signal_message(test_signals, {'source': '涨停池', 'size': 150})
    print(f"\n有信号消息:\n标题: {title}")
    print(f"内容:\n{content}")