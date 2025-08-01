# app/services/chromasky_calculator.py
import logging
import numpy as np

logger = logging.getLogger(__name__)

def score_local_clouds(high_cloud: float | None, medium_cloud: float | None) -> float:
    """因子A: 本地云况 (The Canvas)"""
    # 处理 None 值，如果任一云量数据缺失，则认为画布条件不满足
    if high_cloud is None or medium_cloud is None:
        return 0.0
        
    canvas_cloud_cover = high_cloud + medium_cloud
    if canvas_cloud_cover < 20.0: # 云量太少
        return 0.1
    return 1.0

def score_light_path(avg_tcc_along_path: float | None) -> float:
    """因子B: 光照路径 (The Window)"""
    if avg_tcc_along_path is None:
        return 0.0
        
    clarity = (100.0 - avg_tcc_along_path) / 100.0
    return clarity ** 2

def score_cloud_altitude(cloud_base_meters: float | None) -> float:
    """因子D: 云层高度 (The Scale)"""
    if cloud_base_meters is None or np.isnan(cloud_base_meters): # 没有云或数据缺失
        return 0.0
    if cloud_base_meters > 6000: # 高云
        return 1.0
    if cloud_base_meters > 2500: # 中云
        return 0.7
    return 0.3 # 低云