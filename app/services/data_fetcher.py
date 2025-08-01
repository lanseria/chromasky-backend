# app/services/data_fetcher.py
import xarray as xr
import numpy as np
import logging
from datetime import datetime, timezone, timedelta
from .grib_downloader import grib_downloader
from zoneinfo import ZoneInfo
from typing import Dict, Literal

# --- 辅助函数 (保持不变) ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def to_python_float(value) -> float:
    if hasattr(value, 'item'):
        return value.item()
    return float(value)

# 定义一个类型别名，让代码更清晰
EventType = Literal["today_sunrise", "today_sunset", "tomorrow_sunrise", "tomorrow_sunset"]

# --- DataFetcher 类 (核心重构) ---
class DataFetcher:
    """
    负责在启动时一次性下载并缓存未来四个关键事件（今明两天的日出日落）的数据。
    """
    def __init__(self):
        # --- START OF CHANGES ---
        # self.dataset -> self.datasets: 一个字典，用事件名作为键
        self.datasets: Dict[EventType, xr.Dataset] = {}
        self.time_metadata: Dict[EventType, dict] = {}
        # --- END OF CHANGES ---
        self._initialize_all_data()

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
        
        # 将所有本地时间转换为UTC时间
        return {name: dt.astimezone(timezone.utc) for name, dt in target_times_shanghai.items()}

    def _initialize_all_data(self):
        """
        在应用启动时，循环下载并加载所有四个目标时间点的数据。
        """
        target_times_utc = self._calculate_target_times()

        for event_name, target_time in target_times_utc.items():
            logger.info(f"--- 开始处理事件: {event_name} ({target_time.isoformat()}) ---")
            
            time_meta, file_paths = grib_downloader.get_gfs_data_for_time(target_time)
            
            if not time_meta or not isinstance(file_paths, dict):
                logger.error(f"事件 '{event_name}' 的数据下载失败。")
                continue # 继续处理下一个事件

            self.time_metadata[event_name] = time_meta
            
            datasets_to_merge = []
            for block_name, path in file_paths.items():
                if path and path.exists():
                    try:
                        # ... (加载逻辑不变) ...
                        open_kwargs = {"engine": "cfgrib", "decode_timedelta": False}
                        backend_kwargs = {'filter_by_keys': {'stepType': 'instant'}}
                        try:
                            ds = xr.open_dataset(path, **open_kwargs, backend_kwargs=backend_kwargs)
                        except ValueError:
                            ds = xr.open_dataset(path, **open_kwargs)
                        datasets_to_merge.append(ds)
                    except Exception as e:
                        logger.error(f"加载文件 {path} (用于事件 {event_name}) 失败: {e}")
                else:
                    logger.warning(f"数据块 '{block_name}' (用于事件 {event_name}) 未能下载。")
            
            if datasets_to_merge:
                self.datasets[event_name] = xr.merge(datasets_to_merge)
                logger.info(f"==> 事件 '{event_name}' 的数据集已成功加载并缓存。")
            else:
                logger.error(f"事件 '{event_name}' 没有可加载的数据文件。")

    def get_all_variables_for_point(self, lat: float, lon: float, event: EventType):
        """
        (这个方法基本可以保持不变，因为数据结构现在变得清晰了)
        """
        
        # 从字典中选择对应事件的数据集
        dataset = self.datasets.get(event)
        if dataset is None:
            return {"error": f"事件 '{event}' 的数据尚未加载或加载失败。"}

        try:
            lon_360 = lon + 360 if lon < 0 else lon
            point_data = dataset.sel(latitude=lat, longitude=lon_360, method="nearest")
            
            # 现在变量名和层级都应该是正确的
            total_cloud_cover = to_python_float(point_data.get("tcdc", np.nan))
            high_cloud_cover = to_python_float(point_data.get("hcc", np.nan))
            medium_cloud_cover = to_python_float(point_data.get("mcc", np.nan))
            low_cloud_cover = to_python_float(point_data.get("lcc", np.nan))
            visibility = to_python_float(point_data.get("vis", np.nan))
            approximated_aod = 0.2
            cloud_base_height_meters = 3500.0 # 假设用回模拟值

            return {
                "total_cloud_cover": round(total_cloud_cover, 2) if not np.isnan(total_cloud_cover) else None,
                "high_cloud_cover": round(high_cloud_cover, 2) if not np.isnan(high_cloud_cover) else None,
                "medium_cloud_cover": round(medium_cloud_cover, 2) if not np.isnan(medium_cloud_cover) else None,
                "low_cloud_cover": round(low_cloud_cover, 2) if not np.isnan(low_cloud_cover) else None,
                "approximated_aod": round(approximated_aod, 2),
                "cloud_base_height_meters": cloud_base_height_meters,
            }

        except Exception as e:
            logger.error(f"在经纬度({lat}, {lon})提取数据时出错: {e}", exc_info=True)
            return {"error": "在服务器端提取数据时发生内部错误。"}

# 创建单例
data_fetcher = DataFetcher()