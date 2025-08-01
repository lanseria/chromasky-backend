# app/services/chromasky_calculator.py
import logging
import numpy as np
from typing import Dict, Any

logger = logging.getLogger(__name__)

# --- 评分函数 (因子计算) ---

def score_local_clouds(high_cloud: float | None, medium_cloud: float | None) -> float:
    """因子A: 本地云况 (The Canvas)"""
    if high_cloud is None or medium_cloud is None:
        logger.warning("本地云况数据缺失，因子A得分为 0。")
        return 0.0
        
    canvas_cloud_cover = high_cloud + medium_cloud
    if canvas_cloud_cover < 20.0:
        return 0.1
    return 1.0

def score_light_path(avg_tcc_along_path: float | None) -> float:
    """因子B: 光照路径 (The Window)"""
    if avg_tcc_along_path is None:
        logger.warning("光路云量数据缺失，因子B得分为 0。")
        return 0.0
        
    clarity = (100.0 - avg_tcc_along_path) / 100.0
    return clarity ** 2

def score_air_quality(aod: float | None) -> float:
    """因子C: 空气质量 (The Filter)"""
    if aod is None or np.isnan(aod):
        logger.warning("AOD 数据缺失，因子C得分为 0.5 (中性分)。")
        return 0.5 
    
    if aod < 0.2:
        return 1.0
    if aod > 0.8:
        return 0.0
    return 1.0 - ((aod - 0.2) / 0.6)

def score_cloud_altitude(cloud_base_meters: float | None) -> float:
    """因子D: 云层高度 (The Scale)"""
    if cloud_base_meters is None or np.isnan(cloud_base_meters):
        logger.warning("云底高度数据缺失，因子D得分为 0。")
        return 0.0
    if cloud_base_meters > 6000:
        return 1.0
    if cloud_base_meters > 2500:
        return 0.7
    return 0.3

# --- 主计算类 ---

class ChromaSkyCalculator:
    # 我们在构造函数中接收 data_fetcher 的实例，这是一种依赖注入的好实践
    def __init__(self, data_fetcher):
        self.data_fetcher = data_fetcher

    def calculate_for_point(self, lat: float, lon: float, event: str) -> Dict[str, Any] | None:
        """
        为单个点计算所有因子和最终指数。
        返回包含分数和分项的字典，如果失败则返回 None。
        """
        # 1. 获取本地 GFS 数据
        raw_gfs_data = self.data_fetcher.get_all_variables_for_point(lat, lon, event)
        if not raw_gfs_data or "error" in raw_gfs_data:
            logger.warning(f"无法获取 GFS 数据 @({lat:.2f},{lon:.2f}) for {event}: {raw_gfs_data.get('error', '未知错误')}")
            return None

        # 2. 获取 AOD 数据
        aod_value = self.data_fetcher.get_aod_for_event(lat, lon, event)
        
        # 3. 计算光路平均云量
        avg_cloud_path = self.data_fetcher.get_light_path_avg_cloudiness(lat, lon, event)

        # 4. 提取计算所需的输入值
        hcc = raw_gfs_data.get("high_cloud_cover")
        mcc = raw_gfs_data.get("medium_cloud_cover")
        cloud_base = raw_gfs_data.get("cloud_base_height_meters")

        # 5. 调用评分函数计算各个因子的得分
        factor_a = score_local_clouds(hcc, mcc)
        factor_b = score_light_path(avg_cloud_path)
        factor_c = score_air_quality(aod_value)
        factor_d = score_cloud_altitude(cloud_base)
        
        # 6. 计算最终总分
        final_score = factor_a * factor_b * factor_c * factor_d * 10
        
        # 7. 组织并返回结果
        return {
            "score": round(final_score, 1),
            "breakdown": {
                "factor_A_local_clouds": {
                    "score": round(factor_a, 2),
                    "details": f"High({hcc}%) + Medium({mcc}%)"
                },
                "factor_B_light_path": {
                    "score": round(factor_b, 2),
                    "details": f"Avg cloud path: {round(avg_cloud_path, 1) if avg_cloud_path is not None else 'N/A'}%"
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
        }

    def generate_map_data(self, event: str) -> dict:
        """
        为整个区域的格点计算指数，并生成 GeoJSON。
        """
        dataset = self.data_fetcher.gfs_datasets.get(event)
        if dataset is None:
            return {"error": f"事件 '{event}' 的 GFS 数据不可用。"}

        features = []
        lats = dataset.latitude.values
        lons = dataset.longitude.values

        logger.info(f"开始为 {len(lats) * len(lons)} 个格点生成事件 '{event}' 的地图数据...")
        
        # 为了提高性能，可以考虑使用多进程或异步任务来并行计算
        for lat in lats[::4]:  # 降采样：每隔4个纬度点取一个
            for lon in lons[::4]: # 降采样：每隔4个经度点取一个
                result = self.calculate_for_point(lat, lon, event)
                
                if result and 'score' in result:
                    lon_180 = lon if lon <= 180 else lon - 360
                    
                    feature = {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [lon_180, lat]},
                        "properties": {"score": result["score"]}
                    }
                    features.append(feature)

        logger.info(f"地图数据生成完成，共包含 {len(features)} 个有效特征点。")
        return {
            "type": "FeatureCollection",
            "features": features
        }