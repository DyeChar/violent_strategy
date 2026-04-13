"""
暴力战法选股系统主入口
命令行运行：python main.py

模式：
1. 选股模式（默认）：扫描当前符合条件的股票
2. 回测模式（--backtest）：历史回测验证策略效果

策略说明：
1. 放量吸筹：量能放大≥2倍，连续≥2天
2. 红肥绿瘦：涨放量、跌缩量
3. 回踩20日均线：提示机会

输出：
- 选股模式：信号结果CSV + 可视化图表PNG
- 回测模式：交易明细表格 + 统计汇总
"""

import argparse
import sys
import os
from datetime import datetime
import pandas as pd

import config
import selector
import visualizer
import backtest


def parse_args():
    """
    解析命令行参数
    """
    parser = argparse.ArgumentParser(
        description='暴力战法选股系统 - 主力资金战法'
    )

    # 运行模式
    parser.add_argument(
        '--backtest', '-b',
        action='store_true',
        help='运行回测模式（历史验证）'
    )

    # 日期范围
    parser.add_argument(
        '--start', '-s',
        type=str,
        default=config.START_DATE,
        help=f'起始日期 (默认: {config.START_DATE})'
    )

    parser.add_argument(
        '--end', '-e',
        type=str,
        default=config.END_DATE,
        help=f'结束日期 (默认: {config.END_DATE})'
    )

    # 股票范围
    parser.add_argument(
        '--codes', '-c',
        type=str,
        nargs='+',
        default=None,
        help='指定股票代码列表（如: 300001 300002），默认为全部创业板'
    )

    parser.add_argument(
        '--test', '-t',
        action='store_true',
        help='使用测试样本（快速验证）'
    )

    # 回测参数
    parser.add_argument(
        '--sample', '-n',
        type=int,
        default=20,
        help='回测随机选取股票数量 (默认: 20)'
    )

    parser.add_argument(
        '--seed',
        type=int,
        default=42,
        help='随机种子 (默认: 42，便于复现)'
    )

    # 输出控制
    parser.add_argument(
        '--no-chart',
        action='store_true',
        help='不生成可视化图表'
    )

    parser.add_argument(
        '--show',
        action='store_true',
        help='显示图表（不保存）'
    )

    return parser.parse_args()


def format_date(date_str: str) -> str:
    """
    格式化日期字符串为 YYYYMMDD 格式

    Args:
        date_str: 输入日期字符串

    Returns:
        str: 格式化后的日期
    """
    if date_str is None:
        return None
    return date_str.replace('-', '')


def main():
    """
    主函数
    """
    args = parse_args()

    # 格式化日期
    start_date = format_date(args.start)
    end_date = format_date(args.end)

    # 回测模式
    if args.backtest:
        run_backtest_mode(args, start_date, end_date)
        return

    # 选股模式
    run_select_mode(args, start_date, end_date)


def run_backtest_mode(args, start_date: str, end_date: str):
    """
    运行回测模式
    """
    print("\n" + "=" * 60)
    print("暴力战法回测系统")
    print("=" * 60)
    print("回测说明:")
    print("  买入: T日信号 → T+1开盘价买入 → 5万/次")
    print("  卖出: 跌破5日线 OR 顶分型 OR 放量阴线 OR 20%止盈")
    print("=" * 60)
    print(f"运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    try:
        trades, stats = backtest.run_backtest(
            stock_codes=args.codes,
            sample_size=args.sample,
            start_date=start_date,
            end_date=end_date,
            seed=args.seed
        )

        # 保存回测结果
        if trades:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = os.path.join(config.OUTPUT_DIR, f'backtest_{timestamp}.csv')

            # 转换为DataFrame保存
            valid_trades = [t for t in trades if t['valid']]
            if valid_trades:
                df = pd.DataFrame(valid_trades)
                df.to_csv(output_file, index=False, encoding='utf-8-sig')
                print(f"\n回测结果已保存: {output_file}")

        print("\n" + "=" * 60)
        print("回测完成！")
        print("=" * 60)

    except KeyboardInterrupt:
        print("\n程序被用户中断")
        sys.exit(0)
    except Exception as e:
        print(f"\n回测运行出错: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def run_select_mode(args, start_date: str, end_date: str):
    """
    运行选股模式
    """
    # 确定股票范围
    if args.test:
        stock_codes = config.TEST_SAMPLES
        print(f"使用测试样本: {stock_codes}")
    elif args.codes:
        stock_codes = args.codes
        print(f"使用指定股票: {stock_codes}")
    else:
        stock_codes = None  # 使用全部创业板

    print("\n" + "=" * 60)
    print("暴力战法选股系统")
    print("主力资金战法")
    print("=" * 60)
    print("策略说明:")
    print("  阶段1: 放量吸筹 - 成交量≥2倍均量，连续≥2天")
    print("  阶段2: 红肥绿瘦 - 涨放量、跌缩量")
    print("  阶段3: 回踩均线 - 股价接近20日均线±2%，提示机会")
    print("=" * 60)
    print(f"运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    try:
        # 执行选股
        result_df, kline_data = selector.run_selection(
            stock_codes=stock_codes,
            start_date=start_date,
            end_date=end_date,
            verbose=True
        )

        # 打印摘要
        selector.print_summary(result_df)

        # 保存信号结果
        if not result_df.empty:
            signals_path = selector.save_signals(result_df)

            # 生成可视化图表
            if not args.no_chart:
                chart_paths = visualizer.batch_generate_charts(
                    result_df,
                    kline_data,
                    show=args.show
                )

                print("\n" + "=" * 40)
                print("输出文件:")
                print("=" * 40)
                print(f"信号结果: {signals_path}")
                if chart_paths:
                    print(f"图表目录: {config.CHARTS_DIR}")
                    print(f"图表数量: {len(chart_paths)} 张")
            else:
                print(f"\n信号结果已保存: {signals_path}")

        else:
            print("\n本次扫描未发现符合条件的股票")

        print("\n" + "=" * 60)
        print("选股完成！")
        print("=" * 60)

    except KeyboardInterrupt:
        print("\n程序被用户中断")
        sys.exit(0)
    except Exception as e:
        print(f"\n程序运行出错: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()