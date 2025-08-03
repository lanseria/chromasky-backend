# app/tasks/worker_tasks.py
import numpy as np
import logging
import math
import ephem
from datetime import datetime

# 从 calculator 导入评分函数
from app.services.chromasky_calculator import score_local_clouds, score_light_path, score_air_quality, score_cloud_altitude

logger = logging.getLogger("WorkerTask")

# --- Worker 进程的全局变量 ---
# 这些变量将在每个 worker 初始化时被设置一次
worker_gfs_data = {}
worker_gfs_coords = {}
worker_aod_data = {}
worker_aod_coords = {}
worker_gfs_time_meta = {}

def init_worker(gfs_data, gfs_coords, aod_data, aod_coords, gfs_time_meta):
    """
    每个 Worker 进程的初始化函数。
    接收主进程传递的 NumPy 数组和元数据，并将其存为全局变量。
    """
    global worker_gfs_data, worker_gfs_coords, worker_aod_data, worker_aod_coords, worker_gfs_time_meta
    worker_gfs_data = gfs_data
    worker_gfs_coords = gfs_coords
    worker_aod_data = aod_data
    worker_aod_coords = aod_coords
    worker_gfs_time_meta = gfs_time_meta
    # logger.info(f"Worker {os.getpid()} initialized with data.")

def _find_nearest_idx(array: np.ndarray, value: float) -> int:
    """在 NumPy 数组中找到最接近给定值的索引。"""
    # 确保数组已排序，这对于坐标轴通常是成立的
    idx = np.searchsorted(array, value, side="left")
    if idx > 0 and (idx == len(array) or math.fabs(value - array[idx-1]) < math.fabs(value - array[idx])):
        return idx - 1
    else:
        return idx

def _get_sun_azimuth(lat: float, lon: float, event_time_utc: datetime) -> float:
    """轻量级的天文计算，与 DataFetcher 中的版本相同。"""
    observer = ephem.Observer()
    observer.lat, observer.lon, observer.date, observer.pressure = str(lat), str(lon), event_time_utc, 0
    sun = ephem.Sun(observer)
    return math.degrees(sun.az)

def _get_point_along_path(lat1: float, lon1: float, azimuth_deg: float, distance_km: float) -> tuple[float, float]:
    """轻量级的天文计算，与 DataFetcher 中的版本相同。"""
    R = 6371.0
    lat1_rad, lon1_rad, azimuth_rad = math.radians(lat1), math.radians(lon1), math.radians(azimuth_deg)
    d_div_R = distance_km / R
    lat2_rad = math.asin(math.sin(lat1_rad) * math.cos(d_div_R) + math.cos(lat1_rad) * math.sin(d_div_R) * math.cos(azimuth_rad))
    lon2_rad = lon1_rad + math.atan2(math.sin(azimuth_rad) * math.sin(d_div_R) * math.cos(lat1_rad), math.cos(d_div_R) - math.sin(lat1_rad) * math.sin(lat2_rad))
    return math.degrees(lat2_rad), math.degrees(lon2_rad)


def _worker_extract_and_calculate_task(lat: float, lon: float, event_name: str) -> dict | None:
    """
    最终的 Worker 任务函数。
    它只接收 (lat, lon, event_name)，然后使用全局数据进行所有提取和计算。
    """
    try:
        # --- 1. 数据提取 (纯 NumPy 操作) ---
        lon_360 = lon + 360 if lon < 0 else lon
        lat_idx = _find_nearest_idx(worker_gfs_coords['latitude'], lat)
        lon_idx = _find_nearest_idx(worker_gfs_coords['longitude'], lon_360)

        # 提取 GFS 数据
        raw_gfs_data = {
            "high_cloud_cover": worker_gfs_data[event_name]['hcdc'][lat_idx, lon_idx],
            "medium_cloud_cover": worker_gfs_data[event_name]['mcdc'][lat_idx, lon_idx],
            "cloud_base_height_meters": worker_gfs_data[event_name]['hgt'][lat_idx, lon_idx],
        }

        # 提取 AOD 数据
        event_time_utc = datetime.fromisoformat(worker_gfs_time_meta[event_name]["forecast_time_utc"])
        aod_base_time_utc = worker_aod_coords['base_time']
        target_timedelta = event_time_utc - aod_base_time_utc
        target_step_hours = target_timedelta.total_seconds() / 3600.0
        
        aod_lat_idx = _find_nearest_idx(worker_aod_coords['latitude'], lat)
        aod_lon_idx = _find_nearest_idx(worker_aod_coords['longitude'], lon_360)
        aod_step_idx = _find_nearest_idx(worker_aod_coords['step'], target_step_hours)
        
        raw_gfs_data["aod"] = worker_aod_data['aod550'][aod_step_idx, aod_lat_idx, aod_lon_idx]

        # 计算光路平均云量
        sun_azimuth = _get_sun_azimuth(lat, lon, event_time_utc)
        path_cloudiness = []
        num_samples = 5
        scan_distance_km = 400
        for i in range(1, num_samples + 1):
            distance = (i / num_samples) * scan_distance_km
            sample_lat, sample_lon = _get_point_along_path(lat, lon, sun_azimuth, distance)
            sample_lon_360 = sample_lon + 360 if sample_lon < 0 else sample_lon
            
            s_lat_idx = _find_nearest_idx(worker_gfs_coords['latitude'], sample_lat)
            s_lon_idx = _find_nearest_idx(worker_gfs_coords['longitude'], sample_lon_360)
            
            tcc_val = worker_gfs_data[event_name]['tcdc'][s_lat_idx, s_lon_idx]
            if not np.isnan(tcc_val):
                path_cloudiness.append(tcc_val)
        
        avg_cloud_path = np.mean(path_cloudiness) if path_cloudiness else None

        # --- 2. 指数计算 ---
        factor_a = score_local_clouds(raw_gfs_data.get("high_cloud_cover"), raw_gfs_data.get("medium_cloud_cover"))
        factor_b = score_light_path(avg_cloud_path)
        factor_c = score_air_quality(raw_gfs_data.get("aod"))
        factor_d = score_cloud_altitude(raw_gfs_data.get("cloud_base_height_meters"))
        
        final_score = factor_a * factor_b * factor_c * factor_d * 10
        
        return {"lat": lat, "lon": lon, "score": round(final_score, 1)}

    except Exception as e:
        logger.error(f"Worker在处理点 ({lat}, {lon}) 时出错: {e}", exc_info=True)
        return None