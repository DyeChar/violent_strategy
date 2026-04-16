"""
暴力战法选股系统配置文件
包含所有策略参数和数据路径设置

命名规范：
- 涨停相关使用 limit_up（不用zt）
- 日期格式统一使用 YYYY-MM-DD
"""

import os
from datetime import datetime

# ==================== 项目路径 ====================

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(PROJECT_DIR, 'output')
DATA_DIR = os.path.join(OUTPUT_DIR, 'data')
CACHE_DIR = os.path.join(OUTPUT_DIR, 'cache')
CHARTS_DIR = os.path.join(OUTPUT_DIR, 'charts')
REPORTS_DIR = os.path.join(OUTPUT_DIR, 'reports')

# 数据文件路径
LIMIT_UP_HISTORY_FILE = os.path.join(DATA_DIR, 'limit_up_history.csv')
STOCK_POOL_FILE = os.path.join(DATA_DIR, 'stock_pool.json')

# ==================== 涨停池参数 ====================

LIMIT_UP_THRESHOLD = 9.0           # 涨停阈值（涨幅 >= 9%）
LIMIT_UP_POOL_LOOKBACK = 30        # 涨停池回看天数（近30天有过涨停）

# ==================== 策略参数 ====================

# 阶段1：放量吸筹
VOLUME_RATIO_THRESHOLD = 2.0      # 放量倍数（成交量 >= 2倍5日均量）
VOLUME_CONTINUOUS_DAYS = 2         # 连续放量天数要求
VOLUME_MA_PERIOD = 5               # 前期均量计算周期

# 阶段2：震荡回调红肥绿瘦
OSCILLATION_MIN_DAYS = 2           # 震荡期最少天数
RED_GREEN_RATIO_THRESHOLD = 1.2    # 红肥绿瘦比率阈值（上涨日均量/下跌日均量）

# 阶段3：回踩MA20
MA20_PERIOD = 20                   # MA20周期
MA20_TOLERANCE = 0.03               # 距MA20容差（±3%）

# 阶段4：底分型确认（严格版本）
# 定义：中间K线最低价最低 + 最高价低于左右两根

# ==================== 回测参数 ====================

# 固定持有期天数
HOLDING_PERIODS = [1, 2, 3, 5, 7, 14, 20, 30]

# 默认回测区间
BACKTEST_START_DATE = '2025-01-01'
BACKTEST_END_DATE = datetime.now().strftime('%Y-%m-%d')

# ==================== 股票范围 ====================

# 有效股票代码前缀
VALID_PREFIXES = ['600', '601', '603', '605', '000', '001', '002', '003', '300']

# 排除的股票代码前缀
EXCLUDE_PREFIXES = ['688', '8', '4', '900', '200']  # 科创板、北交所、B股

# ==================== 数据获取参数 ====================

# 数据源优先级：baostock > akshare
REQUEST_DELAY = 0.3                # API请求间隔（秒），避免封禁

# 缓存有效期
STOCK_POOL_CACHE_DAYS = 30         # 股票池缓存有效期（天）

# 默认K线起始日期（用于首次获取）
KLINE_START_DATE = '2024-01-01'

# ==================== 可视化参数 ====================

CHART_DPI = 150                    # 图表分辨率
CHART_WIDTH = 14                   # 图表宽度（英寸）
CHART_HEIGHT = 8                   # 图表高度（英寸）

# ==================== 推送参数 ====================

# Server酱API
SERVERCHAN_API_URL = 'https://sctapi.ftqq.com/{send_key}.send'

# 环境变量名
SERVERCHAN_KEY_ENV = 'SERVERCHAN_KEY'

# ==================== 初始化 ====================

def ensure_dirs():
    """确保所有目录存在"""
    for dir_path in [OUTPUT_DIR, DATA_DIR, CACHE_DIR, CHARTS_DIR, REPORTS_DIR]:
        os.makedirs(dir_path, exist_ok=True)

# 启动时自动创建目录
ensure_dirs()