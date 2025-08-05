# app/services/chromasky_calculator.py
import logging
import numpy as np
from typing import Dict, Any
from enum import Enum
import concurrent.futures
import os

from .data_fetcher import DataFetcher, EventType

logger = logging.getLogger(__name__)

class MapDensity(str, Enum):
    low = "low"
    medium = "medium"
    high = "high"
def score_local_clouds(high_cloud: float | None, medium_cloud: float | None) -> float:
    """
    因子A：本地“画布”云评分（20%阈值递增模型）
    - 中高云总量达到 20% 或以上，即为最佳状态，得满分 1.0。
    - 低于 20% 时，分数从一个基础分开始，随云量线性增长至 1.0。
    """
    if high_cloud is None or medium_cloud is None:
        return 0.0

    # 中高云总覆盖度，单位是百分比 (0-100)
    canvas_cover = high_cloud + medium_cloud
    
    # --- 定义新规则的边界和得分 ---
    
    # 阈值，达到此值即为最佳
    optimal_threshold = 20.0

    # 基础分，适用于晴空无云(0%)的情况
    base_score = 0

    # 1. 如果云量达到或超过阈值，直接返回满分
    if canvas_cover >= optimal_threshold:
        return 1.0
    
    # 2. 如果云量低于阈值，进行线性插值
    #    分数将在 [0, optimal_threshold] 的范围内，从 base_score 增长到 1.0
    else:
        # 计算当前进度的百分比 (0 到 1)
        progress = canvas_cover / optimal_threshold
        
        # 在 base_score 和 1.0 之间进行线性插值
        return base_score + (1.0 - base_score) * progress

def score_light_path(avg_tcc_along_path: float | None) -> float:
    if avg_tcc_along_path is None: return 0.0
    return ((100.0 - avg_tcc_along_path) / 100.0) ** 1.2

def score_air_quality(aod: float | None) -> float:
    if aod is None or np.isnan(aod): return 0.5
    if aod < 0.2: return 1.0
    if aod > 0.8: return 0.0
    return 1.0 - ((aod - 0.2) / 0.6)

def score_cloud_altitude(cloud_base_meters: float | None) -> float:
    if cloud_base_meters is None or np.isnan(cloud_base_meters): return 0.0
    if cloud_base_meters > 6000: return 1.0
    if cloud_base_meters > 2500: return 0.7
    return 0.3

def _calculate_point_for_map(lat: float, lon: float, event: str) -> Dict[str, Any] | None:
    data_fetcher = DataFetcher()
    raw_gfs_data = data_fetcher.get_all_variables_for_point(lat, lon, event)
    if not raw_gfs_data or "error" in raw_gfs_data: return None

    avg_cloud_path = data_fetcher.get_light_path_avg_cloudiness(lat, lon, event)
    factor_a = score_local_clouds(raw_gfs_data.get("high_cloud_cover"), raw_gfs_data.get("medium_cloud_cover"))
    factor_b = score_light_path(avg_cloud_path)
    factor_c = score_air_quality(raw_gfs_data.get("aod"))
    factor_d = score_cloud_altitude(raw_gfs_data.get("cloud_base_height_meters"))
    
    final_score = factor_a * factor_b * factor_c * factor_d * 10
    return {"lat": lat, "lon": lon, "score": round(final_score, 1)}


class ChromaSkyCalculator:
    def __init__(self):
        self.data_fetcher = DataFetcher()

    def calculate_for_point(self, lat: float, lon: float, event: str) -> Dict[str, Any] | None:
        raw_gfs_data = self.data_fetcher.get_all_variables_for_point(lat, lon, event)
        if not raw_gfs_data or "error" in raw_gfs_data:
            return None

        avg_cloud_path = self.data_fetcher.get_light_path_avg_cloudiness(lat, lon, event)
        factor_a = score_local_clouds(raw_gfs_data.get("high_cloud_cover"), raw_gfs_data.get("medium_cloud_cover"))
        factor_b = score_light_path(avg_cloud_path)
        factor_c = score_air_quality(raw_gfs_data.get("aod"))
        factor_d = score_cloud_altitude(raw_gfs_data.get("cloud_base_height_meters"))
        
        final_score = factor_a * factor_b * factor_c * factor_d * 10
        
        return {
            "score": round(final_score, 1),
            "breakdown": {
                "factor_A_local_clouds": {"score": round(factor_a, 2), "details": f"High({raw_gfs_data.get('high_cloud_cover')}%) + Medium({raw_gfs_data.get('medium_cloud_cover')}%)"},
                "factor_B_light_path": {"score": round(factor_b, 2), "details": f"Avg cloud path: {round(avg_cloud_path, 1) if avg_cloud_path is not None else 'N/A'}%"},
                "factor_C_air_quality": {"score": round(factor_c, 2), "details": f"AOD: {raw_gfs_data.get('aod')}"},
                "factor_D_cloud_altitude": {"score": round(factor_d, 2), "details": f"Cloud base: {round(raw_gfs_data.get('cloud_base_height_meters')) if raw_gfs_data.get('cloud_base_height_meters') is not None else 'N/A'} m"}
            }
        }

    def generate_map_data(self, event: str, density: MapDensity = MapDensity.medium) -> dict:
        if event not in self.data_fetcher.gfs_datasets:
            return {"error": f"事件 '{event}' 的 GFS 数据不可用。"}
        
        density_to_step = {MapDensity.low: 3, MapDensity.medium: 2, MapDensity.high: 1}
        step = density_to_step[density]
        lats = self.data_fetcher.gfs_datasets[event].latitude.values[::step]
        lons = self.data_fetcher.gfs_datasets[event].longitude.values[::step]
        
        points_to_process = [(lat, lon) for lat in lats for lon in lons]
        total_points = len(points_to_process)
        logger.info(f"开始为 {total_points} 个格点 (密度: {density.value}) 生成事件 '{event}' 的地图数据...")

        features = []
        max_workers = (os.cpu_count() or 1) -1 if (os.cpu_count() or 1) > 1 else 1
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_point = {
                executor.submit(_calculate_point_for_map, lat, lon, event): (lat, lon)
                for lat, lon in points_to_process
            }
            
            for future in concurrent.futures.as_completed(future_to_point):
                try:
                    result = future.result()
                    if result and 'score' in result:
                        lon_180 = result['lon'] if result['lon'] <= 180 else result['lon'] - 360
                        features.append({
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [lon_180, result['lat']]},
                            "properties": {"score": result["score"]}
                        })
                except Exception as exc:
                    point = future_to_point[future]
                    logger.error(f"格点 {point} 的计算生成了异常: {exc}", exc_info=True)

        logger.info(f"地图数据生成完成，共包含 {len(features)} 个有效特征点。")
        return {"type": "FeatureCollection", "features": features}