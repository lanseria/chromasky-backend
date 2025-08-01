# app/services/chromasky_calculator.py
import logging
import numpy as np
from typing import Dict, Any

logger = logging.getLogger(__name__)

def score_local_clouds(high_cloud: float | None, medium_cloud: float | None) -> float:
    """因子A: 本地云况 (The Canvas)"""
    if high_cloud is None or medium_cloud is None: return 0.0
    canvas_cloud_cover = high_cloud + medium_cloud
    if canvas_cloud_cover < 20.0: return 0.1
    return 1.0

def score_light_path(avg_tcc_along_path: float | None) -> float:
    """因子B: 光照路径 (The Window)"""
    if avg_tcc_along_path is None: return 0.0
    clarity = (100.0 - avg_tcc_along_path) / 100.0
    return clarity ** 2

def score_air_quality(aod: float | None) -> float:
    """因子C: 空气质量 (The Filter)"""
    if aod is None or np.isnan(aod): return 0.5
    if aod < 0.2: return 1.0
    if aod > 0.8: return 0.0
    return 1.0 - ((aod - 0.2) / 0.6)

def score_cloud_altitude(cloud_base_meters: float | None) -> float:
    """因子D: 云层高度 (The Scale)"""
    if cloud_base_meters is None or np.isnan(cloud_base_meters): return 0.0
    if cloud_base_meters > 6000: return 1.0
    if cloud_base_meters > 2500: return 0.7
    return 0.3

# --- 新增的主计算函数 ---
def calculate_final_score(
    raw_gfs_data: Dict[str, Any],
    aod_value: float | None,
    avg_cloud_path: float | None
) -> Dict[str, Any]:
    """
    接收所有输入数据，计算所有因子得分和最终指数。
    """
    # 提取输入值
    hcc = raw_gfs_data.get("high_cloud_cover")
    mcc = raw_gfs_data.get("medium_cloud_cover")
    cloud_base = raw_gfs_data.get("cloud_base_height_meters")
    
    # 计算各个因子的得分
    factor_a = score_local_clouds(hcc, mcc)
    factor_b = score_light_path(avg_cloud_path)
    factor_c = score_air_quality(aod_value)
    factor_d = score_cloud_altitude(cloud_base)
    
    # 计算最终总分
    final_score = factor_a * factor_b * factor_c * factor_d * 10
    
    # 准备 breakdown (分项得分)
    breakdown = {
        "factor_A_local_clouds": {
            "score": round(factor_a, 2),
            "details": f"High({hcc}%) + Medium({mcc}%)"
        },
        "factor_B_light_path": {
            "score": round(factor_b, 2),
            "details": f"Avg cloudiness on path: {round(avg_cloud_path, 1) if avg_cloud_path is not None else 'N/A'}%"
        },
        "factor_C_air_quality": {
            "score": round(factor_c, 2),
            "details": f"AOD: {round(aod_value, 3) if aod_value is not None else 'N/A'}"
        },
        "factor_D_cloud_altitude": {
            "score": round(factor_d, 2),
            "details": f"Cloud base: {round(cloud_base) if cloud_base is not None else 'N/A'} m"
        }
    }
    
    return {
        "chromasky_score": round(final_score, 1),
        "breakdown": breakdown
    }