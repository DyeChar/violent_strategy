"""
可视化模块
绘制类似同花顺的K线蜡烛图+成交量图，用箭头标记机会点
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import FancyArrowPatch
import mplfinance as mpf
from typing import Dict, Optional, List
import os
from datetime import datetime

import config


# 设置中文字体（使用字体文件路径，最可靠）
import matplotlib.font_manager as fm
# 使用Arial Unicode MS字体（支持中文）
CHINESE_FONT_PATH = '/System/Library/Fonts/Supplemental/Arial Unicode.ttf'
CHINESE_FONT = fm.FontProperties(fname=CHINESE_FONT_PATH)

plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['font.family'] = 'sans-serif'


def prepare_mpf_data(kline: pd.DataFrame) -> pd.DataFrame:
    """
    准备mplfinance所需的数据格式

    Args:
        kline: K线数据

    Returns:
        DataFrame: 符合mplfinance格式的数据
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
    创建中国股市风格的配色（红涨绿跌）

    Returns:
        mplfinance style对象
    """
    # 市场颜色配置
    mc = mpf.make_marketcolors(
        up='red',        # 上涨：红色
        down='green',    # 下跌：绿色
        edge='inherit',
        wick='inherit',
        volume={'up': 'red', 'down': 'green'}
    )

    # 创建样式
    style = mpf.make_mpf_style(
        marketcolors=mc,
        gridstyle=':',
        gridcolor='gray',
        figcolor='white',
        facecolor='white'
    )

    return style


def plot_kline_with_signal(kline: pd.DataFrame,
                           signal_info: Dict,
                           save_path: str = None,
                           show: bool = False) -> str:
    """
    绘制带信号标记的K线图（类似同花顺界面）

    包含：
    - 蜡烛图（红涨绿跌）
    - 成交量柱状图
    - 20日均线（黄色虚线）
    - 机会点箭头标记（黄色向上箭头）

    Args:
        kline: K线数据
        signal_info: 信号信息字典
        save_path: 保存路径
        show: 是否显示图表

    Returns:
        str: 图片保存路径
    """
    if kline.empty:
        print("K线数据为空，无法绘制")
        return ''

    # 准备数据
    df = prepare_mpf_data(kline)

    # 计算20日均线
    df['MA20'] = df['Close'].rolling(window=config.MA_PERIOD).mean()

    # 获取股票信息
    code = signal_info.get('code', '')
    name = signal_info.get('name', '')

    # 创建中国风格
    style = create_chinese_style()

    # 创建附加图层（均线）
    apds = [
        mpf.make_addplot(
            df['MA20'],
            color='#FFD700',       # 黄色
            width=1.5,
            linestyle='--',
            label='MA20'
        )
    ]

    # 创建图表标题
    title_text = f'{code} {name} - 暴力战法信号图'

    # 绘制前设置全局字体（解决标题中文问题）
    import matplotlib
    matplotlib.rcParams['font.family'] = 'sans-serif'
    # 临时添加字体路径
    from matplotlib.font_manager import fontManager
    fontManager.addfont(CHINESE_FONT_PATH)
    matplotlib.rcParams['font.sans-serif'] = ['Arial Unicode MS']

    # 绘制图表（返回fig对象以便添加标记）- 不使用中文标签参数
    fig, axes = mpf.plot(
        df,
        type='candle',
        style=style,
        volume=True,
        addplot=apds,
        figsize=(config.CHART_WIDTH, config.CHART_HEIGHT),
        # title=title_text,  # 不通过mplfinance设置标题
        # ylabel='价格',  # 不通过mplfinance设置，手动设置
        # ylabel_lower='成交量',  # 不通过mplfinance设置，手动设置
        returnfig=True,
        tight_layout=True
    )

    # 添加箭头标记（在matplotlib层处理）
    ax_price = axes[0]  # 价格图axes
    ax_volume = axes[2]  # 成交量图axes

    # 手动设置所有中文标签（使用中文字体）
    ax_price.set_title(title_text, fontproperties=CHINESE_FONT, fontsize=14, pad=10)
    ax_price.set_ylabel('价格', fontproperties=CHINESE_FONT)
    ax_volume.set_ylabel('成交量', fontproperties=CHINESE_FONT)

    # 设置所有axes的字体
    for ax in axes:
        # 设置所有文本
        for text in ax.texts:
            try:
                text.set_fontproperties(CHINESE_FONT)
            except:
                pass
        # 设置所有刻度标签
        for label in ax.get_xticklabels() + ax.get_yticklabels():
            try:
                label.set_fontproperties(CHINESE_FONT)
            except:
                pass

    # ========== 标记信号点（黄色箭头） ==========
    if signal_info.get('signal'):
        signal_date = pd.to_datetime(signal_info['signal_date'])

        # 找到信号日期在数据中的位置
        try:
            signal_idx = df.index.get_loc(signal_date)

            # 获取当天的最低价
            signal_low = df['Low'].iloc[signal_idx]

            # 绘制向上箭头（从下方指向信号点）
            arrow_y_start = signal_low * 0.95
            arrow_y_end = signal_low

            ax_price.annotate(
                '',
                xy=(signal_idx, arrow_y_end),
                xytext=(signal_idx, arrow_y_start),
                arrowprops=dict(
                    arrowstyle='-|>',
                    color='#FFD700',
                    lw=3,
                    mutation_scale=20
                ),
                zorder=10
            )

            # 在箭头下方添加文字标注
            ax_price.text(
                signal_idx,
                arrow_y_start * 0.98,
                '机会',
                color='#FFD700',
                fontsize=12,
                ha='center',
                va='top',
                weight='bold',
                fontproperties=CHINESE_FONT,
                zorder=10
            )

        except KeyError:
            # 信号日期不在数据中
            print(f"信号日期 {signal_info['signal_date']} 不在数据中")

    # ========== 标记前期高点（红色向下箭头） ==========
    high_point = signal_info.get('high_point', ())
    if high_point and len(high_point) >= 2:
        high_price = high_point[0]
        high_date_str = high_point[1]

        try:
            high_date = pd.to_datetime(high_date_str)
            high_idx = df.index.get_loc(high_date)

            # 获取当天的最高价
            signal_high = df['High'].iloc[high_idx]

            # 绘制向下箭头（标记高点）
            ax_price.annotate(
                '',
                xy=(high_idx, signal_high),
                xytext=(high_idx, signal_high * 1.02),
                arrowprops=dict(
                    arrowstyle='-|>',
                    color='blue',
                    lw=2,
                    mutation_scale=15
                ),
                zorder=10
            )

            # 添加文字标注
            ax_price.text(
                high_idx,
                signal_high * 1.03,
                f'高点\n{high_price:.2f}',
                color='blue',
                fontsize=9,
                ha='center',
                va='bottom',
                fontproperties=CHINESE_FONT,
                zorder=10
            )

        except (KeyError, ValueError):
            pass

    # ========== 标记放量日期（圆圈标记） ==========
    volume_dates = signal_info.get('volume_dates', [])
    if volume_dates:
        for vol_date_str in volume_dates:
            try:
                vol_date = pd.to_datetime(vol_date_str)
                vol_idx = df.index.get_loc(vol_date)

                # 在成交量图上标记放量日
                vol_value = df['Volume'].iloc[vol_idx]
                ax_volume.plot(
                    vol_idx,
                    vol_value,
                    'o',
                    color='orange',
                    markersize=8,
                    markeredgecolor='black',
                    zorder=10
                )

            except KeyError:
                pass

    # ========== 添加策略信息摘要 ==========
    # 在图表底部添加信息文本
    info_text = f"""
策略信息摘要：
  前期高点: {high_point[0] if high_point else 'N/A':.2f}元 ({high_point[1] if high_point else 'N/A'})
  回调幅度: {signal_info.get('stage1', {}).get('drawback', 0):.1%}
  放量天数: {signal_info.get('stage2', {}).get('volume_days', 0)}天
  红肥绿瘦比率: {signal_info.get('stage3', {}).get('ratio', 0):.2f}
  信号日期: {signal_info.get('signal_date', 'N/A')}
  当前价格: {signal_info.get('current_price', 0):.2f}元
  20日均线: {signal_info.get('ma20', 0):.2f}元
"""

    # 使用fig.text在图表下方添加信息（使用中文字体）
    fig.text(
        0.02, 0.02,
        info_text,
        fontsize=10,
        va='bottom',
        ha='left',
        fontproperties=CHINESE_FONT,
        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5)
    )

    # 保存图表
    if save_path is None:
        # 确保输出目录存在
        os.makedirs(config.CHARTS_DIR, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        save_path = os.path.join(
            config.CHARTS_DIR,
            f'{code}_{timestamp}.png'
        )

    fig.savefig(save_path, dpi=config.CHART_DPI, bbox_inches='tight')
    print(f"保存图表: {save_path}")

    if show:
        plt.show()
    else:
        plt.close(fig)

    return save_path


def batch_generate_charts(signals_df: pd.DataFrame,
                          kline_data: Dict,
                          show: bool = False) -> List[str]:
    """
    批量生成图表

    Args:
        signals_df: 信号DataFrame
        kline_data: K线数据字典
        show: 是否显示图表

    Returns:
        List[str]: 图片路径列表
    """
    chart_paths = []

    if signals_df.empty:
        print("无信号，不生成图表")
        return chart_paths

    print(f"\n开始生成可视化图表...")
    os.makedirs(config.CHARTS_DIR, exist_ok=True)

    for i, row in signals_df.iterrows():
        code = row['code']

        # 获取K线数据
        kline = kline_data.get(code)
        if kline is None or kline.empty:
            print(f"  {code}: 无K线数据")
            continue

        # 构建信号信息
        signal_info = {
            'signal': True,
            'code': code,
            'name': row['name'],
            'signal_date': row['signal_date'],
            'current_price': row['current_price'],
            'ma20': row['ma20'],
            'high_point': (row['high_price'], row['high_date']),
            'volume_dates': row.get('volume_dates', []),
            'stage1': {'drawback': row['drawback']},
            'stage2': {'volume_days': row['volume_days']},
            'stage3': {'ratio': row['red_green_ratio']}
        }

        # 生成图表
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        save_path = os.path.join(
            config.CHARTS_DIR,
            f'{code}_{row["signal_date"]}_{timestamp}.png'
        )

        try:
            path = plot_kline_with_signal(
                kline,
                signal_info,
                save_path=save_path,
                show=show
            )
            chart_paths.append(path)
            print(f"  {code} {row['name']}: ✓ 图表已生成")
        except Exception as e:
            print(f"  {code} {row['name']}: × 图表生成失败 - {e}")

    print(f"\n共生成 {len(chart_paths)} 张图表")
    return chart_paths


# ==================== 测试代码 ====================

if __name__ == '__main__':
    print("=" * 50)
    print("可视化模块测试")
    print("=" * 50)

    # 导入必要模块
    import data_fetcher
    import strategy
    import selector

    # 使用测试样本
    test_codes = config.TEST_SAMPLES[:3]

    # 获取数据并执行选股
    result_df, kline_data = selector.run_selection(
        stock_codes=test_codes,
        start_date='20240101',
        end_date='20260411',
        verbose=True
    )

    # 如果有信号，生成图表
    if not result_df.empty:
        chart_paths = batch_generate_charts(result_df, kline_data, show=False)
        print(f"\n图表路径: {chart_paths}")
    else:
        print("\n无信号，尝试手动测试可视化...")

        # 使用第一只股票的K线数据测试可视化
        test_code = test_codes[0]
        if test_code in kline_data:
            kline = kline_data[test_code]

            # 创建测试信号信息
            test_signal = {
                'signal': True,
                'code': test_code,
                'name': '测试股票',
                'signal_date': kline['date'].iloc[-1],
                'current_price': kline['close'].iloc[-1],
                'ma20': kline['close'].rolling(20).mean().iloc[-1],
                'high_point': (kline['high'].max(), kline.loc[kline['high'].idxmax(), 'date']),
                'volume_dates': [],
                'stage1': {'drawback': 0.25},
                'stage2': {'volume_days': 3},
                'stage3': {'ratio': 1.5}
            }

            # 绘制测试图表
            path = plot_kline_with_signal(kline, test_signal, show=True)
            print(f"测试图表已生成: {path}")