"""
K线可视化模块
绘制带信号标记的K线图（类似同花顺界面）

图表内容：
- 蜡烛图（红涨绿跌）
- 成交量柱状图
- MA20均线
- 信号标记（箭头）
- 策略信息摘要
- BS点标记（盈利/亏损区分）

文件命名：{code}_{name}.png（新图覆盖旧图）
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import mplfinance as mpf
from typing import Dict, Optional, List
from datetime import datetime
import json

import config


# 中文字体配置（macOS）
CHINESE_FONT_PATH = '/System/Library/Fonts/Supplemental/Arial Unicode.ttf'
CHINESE_FONT = fm.FontProperties(fname=CHINESE_FONT_PATH)

plt.rcParams['axes.unicode_minus'] = False


def prepare_mpf_data(kline: pd.DataFrame) -> pd.DataFrame:
    """
    准备mplfinance所需的数据格式

    Args:
        kline: K线数据

    Returns:
        DataFrame: mplfinance格式数据
    """
    df = kline.copy()

    # 转换日期为datetime并设为索引
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)

    # 重命名列（mplfinance要求大写）
    rename_map = {
        'open': 'Open',
        'high': 'High',
        'low': 'Low',
        'close': 'Close',
        'volume': 'Volume'
    }
    df = df.rename(columns=rename_map)

    # 确保必要列存在
    required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    df = df[required_cols]

    return df


def create_chinese_style():
    """
    创建中国股市风格配色（红涨绿跌）

    Returns:
        mplfinance style对象
    """
    mc = mpf.make_marketcolors(
        up='red',
        down='green',
        edge='inherit',
        wick='inherit',
        volume={'up': 'red', 'down': 'green'}
    )

    style = mpf.make_mpf_style(
        marketcolors=mc,
        gridstyle=':',
        gridcolor='gray',
        figcolor='white',
        facecolor='white'
    )

    return style


def plot_stock_signals_chart(kline: pd.DataFrame, stock_code: str, stock_name: str,
                              stock_signals: List[Dict], returns_data: pd.DataFrame,
                              show: bool = False) -> Optional[str]:
    """
    绘制单只股票的所有信号图表（标记BS点，区分盈利亏损）

    Args:
        kline: K线数据
        stock_code: 股票代码
        stock_name: 股票名称
        stock_signals: 该股票的所有信号列表
        returns_data: 收益数据DataFrame
        show: 是否显示图表

    Returns:
        str: 图片保存路径
    """
    if kline.empty:
        return None

    # 准备mplfinance数据
    df = prepare_mpf_data(kline)

    # 计算MA20
    df['MA20'] = df['Close'].rolling(window=config.MA20_PERIOD).mean()

    # 创建中国风格
    style = create_chinese_style()

    # 创建附加图层（均线）
    apds = [
        mpf.make_addplot(
            df['MA20'],
            color='#FFD700',  # 黄色
            width=1.5,
            linestyle='--'
        )
    ]

    # 绘制图表
    fig, axes = mpf.plot(
        df,
        type='candle',
        style=style,
        volume=True,
        addplot=apds,
        figsize=(config.CHART_WIDTH, config.CHART_HEIGHT),
        returnfig=True,
        tight_layout=True
    )

    ax_price = axes[0]
    ax_volume = axes[2]

    # 设置标题
    ax_price.set_title(f'{stock_code} {stock_name} - 交易信号全景', fontproperties=CHINESE_FONT, fontsize=14)
    ax_price.set_ylabel('价格', fontproperties=CHINESE_FONT)
    ax_volume.set_ylabel('成交量', fontproperties=CHINESE_FONT)

    # 统计信息
    win_count = 0
    loss_count = 0
    total_return = 0

    # 交易详情列表（用于底部信息区）
    trade_details = []

    # 标记每个信号点
    for signal in stock_signals:
        signal_date = signal.get('signal_date', '')

        # 从returns_data找到对应的收益
        signal_returns = returns_data[
            (returns_data['股票代码'].astype(str).str.zfill(6) == stock_code) &
            (returns_data['信号日期'] == signal_date)
        ]

        if len(signal_returns) == 0:
            continue

        ret_30 = signal_returns['30日收益%'].values[0]
        buy_price = signal_returns['买入价格'].values[0]
        buy_date = signal_returns['买入日期'].values[0]

        total_return += ret_30
        if ret_30 > 0:
            win_count += 1
        else:
            loss_count += 1

        # 记录交易详情
        trade_details.append({
            'date': signal_date,
            'buy_date': buy_date,
            'buy_price': buy_price,
            'return': ret_30,
            'is_win': ret_30 > 0
        })

        try:
            # 标记买入点（B点）
            signal_dt = pd.to_datetime(signal_date)
            signal_idx = df.index.get_loc(signal_dt)

            # 买入点标记（信号日收盘价位置）
            signal_close = df['Close'].iloc[signal_idx]

            # 区分盈利亏损颜色
            marker_color = '#00AA00' if ret_30 > 0 else '#FF0000'  # 绿色盈利，红色亏损

            # 标记B点（买入信号）- 大圆点
            ax_price.scatter(
                signal_idx,
                signal_close,
                s=150,
                color=marker_color,
                marker='o',
                zorder=10,
                edgecolors='white',
                linewidths=1
            )

            # 添加日期标注（上方）
            date_text = signal_date[-5:]  # 只显示月-日
            ax_price.text(
                signal_idx,
                signal_close * 1.05,
                date_text,
                fontsize=9,
                ha='center',
                va='bottom',
                color=marker_color,
                fontproperties=CHINESE_FONT,
                zorder=10
            )

            # 添加收益标注（下方）
            ret_text = f'{ret_30:+.1f}%'
            ax_price.text(
                signal_idx,
                signal_close * 0.95,
                ret_text,
                fontsize=10,
                fontweight='bold',
                ha='center',
                va='top',
                color=marker_color,
                fontproperties=CHINESE_FONT,
                zorder=10
            )

        except (KeyError, ValueError):
            pass

    # 添加统计摘要
    avg_return = total_return / len(stock_signals) if stock_signals else 0

    # 交易明细文本
    details_text = "交易明细:\n"
    for t in trade_details:
        status = "盈利" if t['is_win'] else "亏损"
        color_mark = "●" if t['is_win'] else "●"
        details_text += f"  {t['date']} 买入{t['buy_price']:.2f} → {status}{t['return']:+.1f}%\n"

    info_text = f"""
统计信息：
  总信号数: {len(stock_signals)}
  盈利: {win_count}个  亏损: {loss_count}个
  平均收益: {avg_return:.2f}%

{details_text}"""

    fig.text(
        0.02, 0.02,
        info_text,
        fontsize=9,
        va='bottom',
        ha='left',
        fontproperties=CHINESE_FONT,
        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8)
    )

    # 添加颜色说明
    legend_text = "● 绿色=盈利  ● 红色=亏损"
    fig.text(
        0.98, 0.02,
        legend_text,
        fontsize=10,
        va='bottom',
        ha='right',
        fontproperties=CHINESE_FONT,
        color='gray'
    )

    # 保存图表（文件名包含代码+名称）
    os.makedirs(config.CHARTS_DIR, exist_ok=True)
    chart_path = os.path.join(config.CHARTS_DIR, f'{stock_code}_{stock_name}_signals.png')

    fig.savefig(chart_path, dpi=config.CHART_DPI, bbox_inches='tight')
    print(f"  保存: {stock_code}_{stock_name}_signals.png")

    if show:
        plt.show()
    else:
        plt.close(fig)

    return chart_path


def plot_signal_chart(kline: pd.DataFrame, signal: Dict, show: bool = False) -> Optional[str]:
    """
    绘制带信号标记的K线图

    Args:
        kline: K线数据
        signal: 信号信息（detect_signal的结果）
        show: 是否显示图表

    Returns:
        str: 图片保存路径
    """
    if kline.empty:
        print("K线数据为空，无法绘制")
        return None

    # 获取股票信息
    code = signal.get('code', '')
    name = signal.get('name', '')
    signal_date = signal.get('signal_date', '')

    # 准备mplfinance数据
    df = prepare_mpf_data(kline)

    # 计算MA20
    df['MA20'] = df['Close'].rolling(window=config.MA20_PERIOD).mean()

    # 创建中国风格
    style = create_chinese_style()

    # 创建附加图层（均线）
    apds = [
        mpf.make_addplot(
            df['MA20'],
            color='#FFD700',  # 黄色
            width=1.5,
            linestyle='--'
        )
    ]

    # 绘制图表
    fig, axes = mpf.plot(
        df,
        type='candle',
        style=style,
        volume=True,
        addplot=apds,
        figsize=(config.CHART_WIDTH, config.CHART_HEIGHT),
        returnfig=True,
        tight_layout=True
    )

    ax_price = axes[0]
    ax_volume = axes[2]

    # 设置中文标题和标签
    ax_price.set_title(f'{code} {name} - 暴力战法信号', fontproperties=CHINESE_FONT, fontsize=14)
    ax_price.set_ylabel('价格', fontproperties=CHINESE_FONT)
    ax_volume.set_ylabel('成交量', fontproperties=CHINESE_FONT)

    # 标记信号点（黄色箭头）
    if signal_date:
        try:
            signal_dt = pd.to_datetime(signal_date)
            signal_idx = df.index.get_loc(signal_dt)

            signal_low = df['Low'].iloc[signal_idx]

            # 向上箭头
            ax_price.annotate(
                '',
                xy=(signal_idx, signal_low),
                xytext=(signal_idx, signal_low * 0.95),
                arrowprops=dict(
                    arrowstyle='-|>',
                    color='#FFD700',
                    lw=3,
                    mutation_scale=20
                ),
                zorder=10
            )

            # 文字标注
            ax_price.text(
                signal_idx,
                signal_low * 0.93,
                '机会',
                color='#FFD700',
                fontsize=12,
                ha='center',
                va='top',
                weight='bold',
                fontproperties=CHINESE_FONT,
                zorder=10
            )

        except (KeyError, ValueError):
            pass

    # 添加策略信息摘要
    info_text = f"""
策略信息：
  信号日期: {signal_date}
  收盘价: {signal.get('close', 0):.2f}
  MA20: {signal.get('ma20', 0):.2f}
  放量天数: {signal.get('volume_days', 0)}
  红肥绿瘦: {signal.get('red_green_ratio', 0):.2f}
"""

    fig.text(
        0.02, 0.02,
        info_text,
        fontsize=10,
        va='bottom',
        ha='left',
        fontproperties=CHINESE_FONT,
        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5)
    )

    # 保存图表（命名：{code}_{name}.png，覆盖旧图）
    os.makedirs(config.CHARTS_DIR, exist_ok=True)
    chart_path = os.path.join(config.CHARTS_DIR, f'{code}_{name}.png')

    fig.savefig(chart_path, dpi=config.CHART_DPI, bbox_inches='tight')
    print(f"保存图表: {chart_path}")

    if show:
        plt.show()
    else:
        plt.close(fig)

    return chart_path


def batch_plot_charts(klines: Dict[str, pd.DataFrame], signals: List[Dict], max_count: int = 20) -> List[str]:
    """
    批量生成信号可视化图表

    Args:
        klines: {股票代码: K线DataFrame}
        signals: 信号列表
        max_count: 最大生成数量（避免过多图表）

    Returns:
        List[str]: 图表路径列表
    """
    chart_paths = []

    for signal in signals[:max_count]:
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


# ==================== 测试代码 ====================

if __name__ == '__main__':
    print("=" * 50)
    print("可视化模块测试")
    print("=" * 50)

    # 创建测试K线数据
    import numpy as np

    dates = pd.date_range('2025-01-01', periods=60, freq='D')
    test_data = pd.DataFrame({
        'date': dates.strftime('%Y-%m-%d'),
        'code': '300001',
        'name': '特锐德',
        'open': np.random.uniform(10, 12, 60),
        'close': np.random.uniform(10, 12, 60),
        'high': np.random.uniform(12, 14, 60),
        'low': np.random.uniform(8, 10, 60),
        'volume': np.random.uniform(1000000, 3000000, 60)
    })

    # 创建测试信号
    test_signal = {
        'code': '300001',
        'name': '特锐德',
        'signal_date': '2025-02-20',
        'close': 11.5,
        'ma20': 11.2,
        'volume_days': 3,
        'red_green_ratio': 1.5,
        'signal': True
    }

    # 绘制图表
    path = plot_signal_chart(test_data, test_signal, show=True)
    print(f"图表路径: {path}")