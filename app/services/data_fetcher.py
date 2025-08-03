# app/services/data_fetcher.py
import xarray as xr
import numpy as np
import logging
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Literal
from zoneinfo import ZoneInfo
import ephem
import math
import pandas as pd
import threading

from .grib_downloader import grib_downloader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def to_python_float(value) -> float:
    if hasattr(value, 'item'):
        return value.item()
    return float(value)

EventType = Literal["today_sunrise", "today_sunset", "tomorrow_sunrise", "tomorrow_sunset"]

class DataFetcher:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs): # 允许传递参数
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    # --- 修改此方法 ---
    def __init__(self, force_reload=False):
        # 检查是否需要重新加载
        if hasattr(self, '_initialized') and not force_reload:
            return
            
        with self._lock:
            # 双重检查锁，确保在等待锁期间没有其他线程完成初始化
            if hasattr(self, '_initialized') and not force_reload:
                return
            
            logger.info("首次初始化 DataFetcher 或强制重新加载...")
            self.gfs_datasets: Dict[EventType, xr.Dataset] = {}
            self.gfs_time_metadata: Dict[EventType, dict] = {}
            self.aod_dataset: xr.Dataset | None = None
            self.aod_time_metadata: dict = {}
            
            self._load_all_data_from_disk()
            
            self._initialized = True
            logger.info("DataFetcher 数据加载完成。")

    def _find_latest_manifest(self, pattern: str, search_dir: Path | None = None) -> Path | None:
        base_dir = search_dir if search_dir else grib_downloader.download_dir
        if not base_dir.exists(): return None
        manifest_files = sorted(base_dir.rglob(pattern), reverse=True)
        return manifest_files[0] if manifest_files else None

    def _load_all_data_from_disk(self):
        # 1. 加载 GFS 数据
        latest_gfs_manifest_path = self._find_latest_manifest("manifest_*_[0-9][0-9].json")
        if latest_gfs_manifest_path:
            logger.info(f"[GFS] 正在从 GFS 清单加载: {latest_gfs_manifest_path.name}")
            with open(latest_gfs_manifest_path, 'r') as f:
                gfs_manifest = json.load(f)
            for event_name, data in gfs_manifest.items():
                self.gfs_time_metadata[event_name] = data["time_meta"]
                file_paths = data["file_paths"]
                datasets_to_merge = []
                for block_name, path_str in file_paths.items():
                    path = Path(path_str)
                    if path.exists():
                        try:
                            # --- START OF CHANGE: 添加 backend_kwargs 来处理冲突 ---
                            backend_kwargs = {'filter_by_keys': {'stepType': 'instant'}}
                            ds = xr.open_dataset(
                                path, 
                                engine="cfgrib", 
                                decode_timedelta=False,
                                backend_kwargs=backend_kwargs
                            )
                            # --- END OF CHANGE ---
                            datasets_to_merge.append(ds)
                            # 日志移到 try 块的末尾，确保成功加载才打印
                            logger.info(f"[GFS]   > 成功加载文件: {path.name} (for event: {event_name})")
                        except Exception as e:
                            # 保持详细的错误日志
                            logger.error(f"[GFS]   > 加载文件 {path.name} (for event: {event_name}) 时出错: {e}", exc_info=True)
                    else:
                        logger.warning(f"[GFS]   > 文件未找到，已跳过: {path}")
                
                if datasets_to_merge:
                    self.gfs_datasets[event_name] = xr.merge(datasets_to_merge)
                    logger.info(f"[GFS] ==> 事件 '{event_name}' 的数据集已成功加载并缓存。")
                else:
                    # 如果一个事件的所有文件都加载失败，也需要记录
                    logger.error(f"[GFS] 事件 '{event_name}' 没有成功加载任何数据文件。")
        else:
            logger.error("[GFS] 未找到任何 GFS 数据清单。")

        # 2. 加载 AOD 数据
        aod_base_dir = grib_downloader.download_dir / "cams_aod"
        latest_aod_manifest_path = self._find_latest_manifest("manifest_aod.json", search_dir=aod_base_dir)
        if latest_aod_manifest_path:
            logger.info(f"[CAMS_AOD] 正在从 AOD 清单加载: {latest_aod_manifest_path.name}")
            with open(latest_aod_manifest_path, 'r') as f:
                aod_manifest = json.load(f)
            self.aod_time_metadata = aod_manifest
            aod_file_path = Path(aod_manifest["file_path"])
            if aod_file_path.exists():
                try:
                    self.aod_dataset = xr.open_dataset(aod_file_path, engine="cfgrib", decode_timedelta=False)
                    logger.info("[CAMS_AOD] ==> AOD 数据集已成功加载并缓存。")
                except Exception as e:
                    logger.error(f"[CAMS_AOD] 加载 AOD 文件失败: {e}")
            else:
                logger.error(f"[CAMS_AOD] 清单中指定的 AOD 文件未找到: {aod_file_path}")
        else:
            logger.warning("[CAMS_AOD] 未找到任何 AOD 数据清单。")

    def get_light_path_avg_cloudiness(self, lat: float, lon: float, event: EventType) -> float | None:
        dataset = self.gfs_datasets.get(event)
        time_meta = self.gfs_time_metadata.get(event)
        if dataset is None or time_meta is None:
            return None
        try:
            event_time_utc = datetime.fromisoformat(time_meta["forecast_time_utc"])
            sun_azimuth = self._get_sun_azimuth(lat, lon, event_time_utc)
            path_cloudiness = []
            num_samples = 5
            scan_distance_km = 400
            for i in range(1, num_samples + 1):
                distance = (i / num_samples) * scan_distance_km
                sample_lat, sample_lon = self._get_point_along_path(lat, lon, sun_azimuth, distance)
                lon_360 = sample_lon + 360 if sample_lon < 0 else sample_lon
                point_data = dataset.sel(latitude=sample_lat, longitude=lon_360, method="nearest")
                tcc_vars = ['tcc', 'tcdc']
                tcc_val = np.nan
                for var in tcc_vars:
                    if var in point_data:
                        tcc_val = to_python_float(point_data[var])
                        break
                if not np.isnan(tcc_val):
                    path_cloudiness.append(tcc_val)
            if not path_cloudiness:
                return None
            return np.mean(path_cloudiness)
        except Exception as e:
            logger.error(f"计算光路云量时出错: {e}", exc_info=True)
            return None
    
    def get_aod_for_event(self, lat: float, lon: float, event: EventType) -> float | None:
        if self.aod_dataset is None: return None
        gfs_meta = self.gfs_time_metadata.get(event)
        if not gfs_meta: return None
        target_time_utc = datetime.fromisoformat(gfs_meta["forecast_time_utc"])
        try:
            if 'time' in self.aod_dataset.coords:
                aod_base_time_utc = pd.to_datetime(self.aod_dataset.time.values).to_pydatetime().replace(tzinfo=timezone.utc)
            else:
                return None
            target_timedelta = target_time_utc - aod_base_time_utc
            target_step_hours = target_timedelta.total_seconds() / 3600.0
            lon_360 = lon + 360 if lon < 0 else lon
            aod_point_data = self.aod_dataset.sel(latitude=lat, longitude=lon_360, step=target_step_hours, method="nearest")
            aod_value = to_python_float(aod_point_data.get("aod550", np.nan))
            return aod_value if not np.isnan(aod_value) else None
        except Exception as e:
            logger.error(f"为事件 '{event}' 提取 AOD 时发生未知错误: {e}", exc_info=True)
            return None
        
    def get_all_variables_for_point(self, lat: float, lon: float, event: EventType):
        dataset = self.gfs_datasets.get(event)
        if dataset is None: return {"error": f"事件 '{event}' 的 GFS 数据不可用。"}
        try:
            lon_360 = lon + 360 if lon < 0 else lon
            point_data = dataset.sel(latitude=lat, longitude=lon_360, method="nearest")
            
            data = {}
            var_map = {
                "total_cloud_cover": ['tcc', 'tcdc'],
                "high_cloud_cover": ['hcc', 'hcdc'],
                "medium_cloud_cover": ['mcc', 'mcdc'],
                "low_cloud_cover": ['lcc', 'lcdc'],
                "cloud_base_height_meters": ['gh', 'hgt']
            }
            for key, names in var_map.items():
                val = np.nan
                for name in names:
                    if name in point_data:
                        val = to_python_float(point_data[name])
                        break
                data[key] = round(val, 2) if not np.isnan(val) else None
            
            data["aod"] = round(self.get_aod_for_event(lat, lon, event), 3) if self.get_aod_for_event(lat, lon, event) is not None else None
            return data
        except Exception as e:
            logger.error(f"为事件 '{event}' 在 ({lat}, {lon}) 提取数据时出错: {e}", exc_info=True)
            return {"error": "在服务器端提取数据时发生内部错误。"}

    def _get_sun_azimuth(self, lat: float, lon: float, event_time_utc: datetime) -> float:
        observer = ephem.Observer()
        observer.lat, observer.lon, observer.date, observer.pressure = str(lat), str(lon), event_time_utc, 0
        sun = ephem.Sun(observer)
        return math.degrees(sun.az)

    def _get_point_along_path(self, lat1: float, lon1: float, azimuth_deg: float, distance_km: float) -> tuple[float, float]:
        R = 6371.0
        lat1_rad, lon1_rad, azimuth_rad = math.radians(lat1), math.radians(lon1), math.radians(azimuth_deg)
        d_div_R = distance_km / R
        lat2_rad = math.asin(math.sin(lat1_rad) * math.cos(d_div_R) + math.cos(lat1_rad) * math.sin(d_div_R) * math.cos(azimuth_rad))
        lon2_rad = lon1_rad + math.atan2(math.sin(azimuth_rad) * math.sin(d_div_R) * math.cos(lat1_rad), math.cos(d_div_R) - math.sin(lat1_rad) * math.sin(lat2_rad))
        return math.degrees(lat2_rad), math.degrees(lon2_rad)