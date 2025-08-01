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

# 确保 grib_downloader 被导入，以便我们能使用它的 DOWNLOAD_DIR
from .grib_downloader import grib_downloader

# --- 辅助函数 ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def to_python_float(value) -> float:
    if hasattr(value, 'item'):
        return value.item()
    return float(value)

# 定义类型别名
EventType = Literal["today_sunrise", "today_sunset", "tomorrow_sunrise", "tomorrow_sunset"]

# --- DataFetcher 类 ---
class DataFetcher:
    def __init__(self, load_data: bool = True):
        self.gfs_datasets: Dict[EventType, xr.Dataset] = {}
        self.gfs_time_metadata: Dict[EventType, dict] = {}
        self.aod_dataset: xr.Dataset | None = None
        self.aod_time_metadata: dict = {}
        
        if load_data:
            self._load_all_data_from_disk()

    def _find_latest_manifest(self, pattern: str) -> Path | None:
        manifest_dir = grib_downloader.download_dir
        if not manifest_dir.exists(): return None
        manifest_files = sorted(manifest_dir.rglob(pattern), reverse=True)
        return manifest_files[0] if manifest_files else None

    def _calculate_target_times(self) -> Dict[EventType, datetime]:
        """计算出所有四个目标事件的UTC时间。"""
        shanghai_tz = ZoneInfo("Asia/Shanghai")
        now_shanghai = datetime.now(shanghai_tz)
        
        today = now_shanghai.date()
        tomorrow = today + timedelta(days=1)
        
        target_times_shanghai = {
            "today_sunrise": datetime.combine(today, datetime.min.time(), tzinfo=shanghai_tz).replace(hour=4),
            "today_sunset": datetime.combine(today, datetime.min.time(), tzinfo=shanghai_tz).replace(hour=18),
            "tomorrow_sunrise": datetime.combine(tomorrow, datetime.min.time(), tzinfo=shanghai_tz).replace(hour=4),
            "tomorrow_sunset": datetime.combine(tomorrow, datetime.min.time(), tzinfo=shanghai_tz).replace(hour=18),
        }
        
        return {name: dt.astimezone(timezone.utc) for name, dt in target_times_shanghai.items()}

    def _load_all_data_from_disk(self):
        """
        在应用启动时，加载最新的 GFS 和 AOD 数据。
        """
        # 1. 加载 GFS 数据
        latest_gfs_manifest_path = self._find_latest_manifest("manifest_*.json")
        if latest_gfs_manifest_path:
            logger.info(f"正在从 GFS 清单加载: {latest_gfs_manifest_path.name}")
            with open(latest_gfs_manifest_path, 'r') as f:
                gfs_manifest = json.load(f)
            for event_name, data in gfs_manifest.items():
                self.gfs_time_metadata[event_name] = data["time_meta"]
                file_paths = data["file_paths"]
                
                datasets_to_merge = []
                
                # 遍历该事件所需的所有数据块文件
                for block_name, path_str in file_paths.items():
                    path = Path(path_str)
                    if path.exists():
                        try:
                            # 定义通用的加载参数，以消除警告
                            open_kwargs = {"engine": "cfgrib", "decode_timedelta": False}
                            # 定义额外的过滤条件，以处理 stepType 冲突
                            backend_kwargs = {'filter_by_keys': {'stepType': 'instant'}}
                            
                            try:
                                ds = xr.open_dataset(path, **open_kwargs, backend_kwargs=backend_kwargs)
                            except (ValueError, KeyError):
                                # 如果按 stepType='instant' 过滤失败，回退到不带过滤的加载方式
                                logger.warning(f"文件 {path} 按 stepType='instant' 加载失败，尝试无 stepType 加载...")
                                ds = xr.open_dataset(path, **open_kwargs)
                                
                            datasets_to_merge.append(ds)
                            logger.info(f"  > 成功加载文件: {path.name}")

                        except Exception as e:
                            logger.error(f"  > 加载文件 {path.name} (用于事件 {event_name}) 时发生严重错误: {e}", exc_info=True)
                    else:
                        logger.warning(f"  > 文件未找到，已跳过: {path}")
                
                if datasets_to_merge:
                    # 使用 xr.merge 合并该事件的所有数据块
                    self.gfs_datasets[event_name] = xr.merge(datasets_to_merge)
                    logger.info(f"==> 事件 '{event_name}' 的数据集已成功加载并缓存。")
                else:
                    logger.error(f"事件 '{event_name}' 没有可加载的数据文件。")
        else:
            logger.error("未找到 GFS 数据清单。")

        # 2. 加载 AOD 数据
        latest_aod_manifest_path = self._find_latest_manifest("cams_aod/*/manifest_aod.json")
        if latest_aod_manifest_path:
            logger.info(f"正在从 AOD 清单加载: {latest_aod_manifest_path.name}")
            with open(latest_aod_manifest_path, 'r') as f:
                aod_manifest = json.load(f)
            self.aod_time_metadata = aod_manifest
            aod_file_path = Path(aod_manifest["file_path"])
            if aod_file_path.exists():
                try:
                    self.aod_dataset = xr.open_dataset(aod_file_path, engine="cfgrib", decode_timedelta=False)
                    logger.info("==> AOD 数据集已成功加载并缓存。")
                except Exception as e:
                    logger.error(f"加载 AOD 文件失败: {e}")
        else:
            logger.warning("未找到 AOD 数据清单。")
        
    def _get_sun_azimuth(self, lat: float, lon: float, event_time_utc: datetime) -> float:
        """使用 ephem 计算给定地点和时间的太阳方位角。"""
        observer = ephem.Observer()
        observer.lat = str(lat)
        observer.lon = str(lon)
        observer.date = event_time_utc
        observer.pressure = 0 # 忽略大气折射效应
        observer.epoch = ephem.J2000

        sun = ephem.Sun(observer)
        
        # 方位角 (Azimuth) 以度为单位，从北点顺时针测量
        # 我们需要的是光路方向，对于日落是方位角+180度，对于日出是方位角本身
        # 但为了简化，我们统一使用方位角，因为光路是双向的
        # 为了回溯，我们需要的是太阳来的方向，即方位角 + 180度
        # 但通常方位角指的是太阳在天空的位置，光线从那里来
        # 日落时，太阳在西方 (约270度)，光线从270度方向来
        # 我们要扫描的是270度方向，所以直接用azimuth
        return math.degrees(sun.az)

    def _get_point_along_path(self, lat1: float, lon1: float, azimuth_deg: float, distance_km: float) -> tuple[float, float]:
        """计算从一个点沿着指定方位角移动一定距离后的新点坐标。"""
        R = 6371.0  # 地球平均半径 (km)
        lat1_rad = math.radians(lat1)
        lon1_rad = math.radians(lon1)
        azimuth_rad = math.radians(azimuth_deg)

        d_div_R = distance_km / R

        lat2_rad = math.asin(
            math.sin(lat1_rad) * math.cos(d_div_R) +
            math.cos(lat1_rad) * math.sin(d_div_R) * math.cos(azimuth_rad)
        )
        lon2_rad = lon1_rad + math.atan2(
            math.sin(azimuth_rad) * math.sin(d_div_R) * math.cos(lat1_rad),
            math.cos(d_div_R) - math.sin(lat1_rad) * math.sin(lat2_rad)
        )
        
        return math.degrees(lat2_rad), math.degrees(lon2_rad)

    def get_light_path_avg_cloudiness(self, lat: float, lon: float, event: EventType) -> float | None:
        """
        计算光照路径上的平均总云量。
        """
        dataset = self.gfs_datasets.get(event)
        time_meta = self.gfs_time_metadata.get(event)
        if dataset is None or time_meta is None:
            return None

        try:
            event_time_utc = datetime.fromisoformat(time_meta["forecast_time_utc"])
            sun_azimuth = self._get_sun_azimuth(lat, lon, event_time_utc)
            
            # 沿着光路回溯，采样 N 个点
            path_cloudiness = []
            num_samples = 5
            scan_distance_km = 400

            for i in range(1, num_samples + 1):
                distance = (i / num_samples) * scan_distance_km
                sample_lat, sample_lon = self._get_point_along_path(lat, lon, sun_azimuth, distance)
                
                # 从数据集中提取该采样点的总云量
                lon_360 = sample_lon + 360 if sample_lon < 0 else sample_lon
                point_data = dataset.sel(latitude=sample_lat, longitude=lon_360, method="nearest")
                tcc = to_python_float(point_data.get("tcc", np.nan))
                
                if not np.isnan(tcc):
                    path_cloudiness.append(tcc)
            
            if not path_cloudiness:
                return None # 所有采样点都无数据

            return np.mean(path_cloudiness)
            
        except Exception as e:
            logger.error(f"计算光路云量时出错: {e}", exc_info=True)
            return None
    
    def get_aod_for_event(self, lat: float, lon: float, event: EventType) -> float | None:
        if self.aod_dataset is None: return None
        gfs_meta = self.gfs_time_metadata.get(event)
        if not gfs_meta: return None
        target_time = datetime.fromisoformat(gfs_meta["forecast_time_utc"])
        try:
            lon_360 = lon + 360 if lon < 0 else lon
            aod_point_data = self.aod_dataset.sel(latitude=lat, longitude=lon_360, time=target_time, method="nearest")
            return to_python_float(aod_point_data.get("aod550", np.nan))
        except Exception as e:
            logger.error(f"提取 AOD 时出错: {e}")
            return None
        
    def get_all_variables_for_point(self, lat: float, lon: float, event: EventType):
        """
        负责提取单点数据
        """
        dataset = self.gfs_datasets.get(event)
        if dataset is None: return {"error": f"事件 '{event}' 的 GFS 数据不可用。"}

        try:
            lon_360 = lon + 360 if lon < 0 else lon
            point_data = dataset.sel(latitude=lat, longitude=lon_360, method="nearest")
            
            total_cloud_cover = to_python_float(point_data.get("tcc", np.nan))
            
            high_cloud_cover = to_python_float(point_data.get("hcc", np.nan))
            medium_cloud_cover = to_python_float(point_data.get("mcc", np.nan))
            low_cloud_cover = to_python_float(point_data.get("lcc", np.nan))

            cloud_base_height_meters = to_python_float(point_data.get("gh", np.nan))

            real_aod = self.get_aod_for_event(lat, lon, event)
            
            return {
                "total_cloud_cover": round(total_cloud_cover, 2) if not np.isnan(total_cloud_cover) else None,
                "high_cloud_cover": round(high_cloud_cover, 2) if not np.isnan(high_cloud_cover) else None,
                "medium_cloud_cover": round(medium_cloud_cover, 2) if not np.isnan(medium_cloud_cover) else None,
                "low_cloud_cover": round(low_cloud_cover, 2) if not np.isnan(low_cloud_cover) else None,
                "cloud_base_height_meters": round(cloud_base_height_meters, 2) if not np.isnan(cloud_base_height_meters) else None,
                "aod": round(real_aod, 3) if real_aod is not None else None,
            }

        except Exception as e:
            logger.error(f"为事件 '{event}' 在 ({lat}, {lon}) 提取数据时出错: {e}", exc_info=True)
            return {"error": "在服务器端提取数据时发生内部错误。"}

# 创建单例
data_fetcher = DataFetcher()