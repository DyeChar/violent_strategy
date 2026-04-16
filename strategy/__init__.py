"""策略层模块"""
from .fractal import check_bottom_fractal_strict, check_top_fractal_strict
from .stages import detect_stage1_volume, detect_stage2_oscillation, detect_stage3_ma20, detect_stage4_fractal
from .detector import detect_signal