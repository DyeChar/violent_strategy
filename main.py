"""
暴力战法选股系统主入口

命令行运行：python main.py

模式：
1. 回测模式（--backtest）：历史回测验证策略效果
2. 监控模式（--monitor）：每日信号扫描与推送
3. 更新模式（--update-pool）：更新涨停池

策略说明：
- 阶段1：放量吸筹（连续>=2天成交量>=2倍均量且上涨）
- 阶段2：震荡回调红肥绿瘦（涨日均量/跌日均量>=1.2）
- 阶段3：回踩MA20（收盘价距20日线±3%）
- 阶段4：底分型确认（严格版本）
"""

import argparse
import sys
from datetime import datetime

import config
from backtest.runner import run_backtest
from monitor.daily import run_daily_monitor
from monitor.push import send_test_message
from data.limit_up_pool import update_limit_up_history, backfill_limit_up_history, get_candidate_pool


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='暴力战法选股系统 - 主力资金战法'
    )

    # 运行模式
    parser.add_argument(
        '--backtest', '-b',
        action='store_true',
        help='运行回测模式'
    )

    parser.add_argument(
        '--monitor', '-m',
        action='store_true',
        help='运行每日监控模式'
    )

    parser.add_argument(
        '--update-pool',
        action='store_true',
        help='更新涨停池（扫描今日涨停）'
    )

    parser.add_argument(
        '--test-push',
        action='store_true',
        help='发送测试推送消息'
    )

    # 回测参数
    parser.add_argument(
        '--start', '-s',
        type=str,
        default=config.BACKTEST_START_DATE,
        help=f'回测起始日期 (默认: {config.BACKTEST_START_DATE})'
    )

    parser.add_argument(
        '--end', '-e',
        type=str,
        default=datetime.now().strftime('%Y-%m-%d'),
        help='回测结束日期 (默认: 今天)'
    )

    parser.add_argument(
        '--no-cache',
        action='store_true',
        help='不使用K线缓存，重新获取'
    )

    parser.add_argument(
        '--no-chart',
        action='store_true',
        help='不生成可视化图表'
    )

    parser.add_argument(
        '--pool',
        type=str,
        choices=['chinext', 'main', 'limit_up'],
        default=None,
        help='股票池类型: chinext(创业板龙头), main(主板龙头), limit_up(涨停池)'
    )

    # 监控参数
    parser.add_argument(
        '--push',
        action='store_true',
        help='发送微信推送'
    )

    parser.add_argument(
        '--key',
        type=str,
        help='Server酱SendKey（也可通过环境变量SERVERCHAN_KEY配置）'
    )

    # 补齐涨停历史
    parser.add_argument(
        '--backfill',
        nargs=2,
        metavar=('START', 'END'),
        help='补齐涨停历史（指定起始和结束日期）'
    )

    return parser.parse_args()


def run_backtest_mode(args):
    """运行回测模式"""
    print("\n" + "=" * 60)
    print("暴力战法回测系统")
    print("=" * 60)

    # 确定股票池类型
    use_stock_pool = None
    if args.pool:
        if args.pool == 'limit_up':
            use_stock_pool = None  # 使用涨停池
        else:
            use_stock_pool = args.pool  # 使用预定义池

    try:
        results, stats, signals_file = run_backtest(
            start_date=args.start,
            end_date=args.end,
            use_cache=not args.no_cache,
            generate_charts=not args.no_chart,
            verbose=True,
            use_stock_pool=use_stock_pool,
            resume=True  # 支持断点续测
        )

        print("\n回测完成!")

    except KeyboardInterrupt:
        print("\n程序被用户中断")
        sys.exit(0)
    except Exception as e:
        print(f"\n回测运行出错: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def run_monitor_mode(args):
    """运行监控模式"""
    print("\n" + "=" * 60)
    print("暴力战法每日监控")
    print("=" * 60)

    try:
        signals = run_daily_monitor(
            push=args.push,
            send_key=args.key
        )

        print(f"\n今日发现 {len(signals)} 个信号")

    except KeyboardInterrupt:
        print("\n程序被用户中断")
        sys.exit(0)
    except Exception as e:
        print(f"\n监控运行出错: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def run_update_pool_mode(args):
    """运行更新涨停池模式"""
    print("\n" + "=" * 60)
    print("涨停池更新")
    print("=" * 60)

    try:
        today = datetime.now().strftime('%Y-%m-%d')
        history = update_limit_up_history(today)

        today_count = len(history[history['日期'] == today])
        total_count = len(history)

        print(f"\n今日涨停: {today_count} 只")
        print(f"历史总计: {total_count} 条记录")

        # 显示候选池
        candidates = get_candidate_pool(today)
        print(f"候选池（近30天涨停）: {len(candidates)} 只")

    except Exception as e:
        print(f"\n更新涨停池出错: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def run_backfill_mode(args):
    """运行补齐涨停历史模式"""
    start_date = args.backfill[0]
    end_date = args.backfill[1]

    print("\n" + "=" * 60)
    print("补齐涨停历史")
    print("=" * 60)
    print(f"补齐范围: {start_date} ~ {end_date}")

    try:
        backfill_limit_up_history(start_date, end_date)
        print("\n补齐完成!")

    except Exception as e:
        print(f"\n补齐出错: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main():
    """主函数"""
    args = parse_args()

    # 测试推送
    if args.test_push:
        send_test_message(args.key)
        return

    # 回测模式
    if args.backtest:
        run_backtest_mode(args)
        return

    # 监控模式
    if args.monitor:
        run_monitor_mode(args)
        return

    # 更新涨停池
    if args.update_pool:
        run_update_pool_mode(args)
        return

    # 补齐涨停历史
    if args.backfill:
        run_backfill_mode(args)
        return

    # 无指定模式，显示帮助
    print("\n请指定运行模式:")
    print("  --backtest     运行回测")
    print("  --monitor      运行每日监控")
    print("  --update-pool  更新涨停池")
    print("  --test-push    发送测试推送")
    print("  --backfill     补齐涨停历史")
    print("\n回测股票池选项:")
    print("  --pool chinext   使用创业板龙头池（快速测试）")
    print("  --pool main      使用主板龙头池")
    print("  --pool limit_up  使用涨停池（默认，需初始化）")
    print("\n示例:")
    print("  python main.py --backtest --pool chinext --start 2025-01-01 --end 2025-04-30")
    print("  python main.py --backtest --pool limit_up --start 2025-01-01 --end 2025-04-30")
    print("  python main.py --monitor --push")
    print("  python main.py --update-pool")
    print("  python main.py --test-push")


if __name__ == '__main__':
    main()