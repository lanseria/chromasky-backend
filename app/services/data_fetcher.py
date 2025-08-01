# app/services/data_fetcher.py
import xarray as xr
import numpy as np
import logging
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Literal
from zoneinfo import ZoneInfo

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
    """
    负责从磁盘加载由 scheduler.py 准备好的、多个事件的预报数据。
    """
    def __init__(self, load_data: bool = True):
        self.datasets: Dict[EventType, xr.Dataset] = {}
        self.time_metadata: Dict[EventType, dict] = {}
        
        if load_data:
            self._load_all_data_from_disk()

    def _find_latest_manifest(self) -> Path | None:
        """在grib_data目录中查找最新的 manifest.json 文件。"""
        manifest_dir = grib_downloader.download_dir
        if not manifest_dir.exists():
            return None
        
        # 使用 glob 查找所有清单文件，并按名称排序（名称中包含了日期和小时）
        manifest_files = sorted(manifest_dir.glob("manifest_*.json"), reverse=True)
        
        if not manifest_files:
            return None
        
        return manifest_files[0] # 返回最新的一个

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
        在应用启动时，查找最新的清单文件，并加载其中描述的所有GRIB数据。
        """
        latest_manifest_path = self._find_latest_manifest()
        
        if not latest_manifest_path:
            logger.error(f"在 {grib_downloader.download_dir} 中未找到任何数据清单文件。请先运行 scheduler.py。")
            return

        logger.info(f"正在从最新的清单文件加载数据: {latest_manifest_path.name}")
        with open(latest_manifest_path, 'r') as f:
            try:
                manifest = json.load(f)
            except json.JSONDecodeError:
                logger.error(f"无法解析数据清单文件: {self.manifest_path}。文件可能已损坏或为空。")
                return
            
        for event_name, data in manifest.items():
            logger.info(f"--- 正在为事件 '{event_name}' 加载数据 ---")
            
            # 确保 manifest 结构正确
            if "time_meta" not in data or "file_paths" not in data:
                logger.warning(f"清单中事件 '{event_name}' 的条目格式不正确，已跳过。")
                continue

            self.time_metadata[event_name] = data["time_meta"]
            file_paths = data["file_paths"]
            
            datasets_to_merge = []
            
            # --- START OF MISSING CODE ---
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
            # --- END OF MISSING CODE ---
            
            if datasets_to_merge:
                # 使用 xr.merge 合并该事件的所有数据块
                self.datasets[event_name] = xr.merge(datasets_to_merge)
                logger.info(f"==> 事件 '{event_name}' 的数据集已成功加载并缓存。")
            else:
                logger.error(f"事件 '{event_name}' 没有可加载的数据文件。")

    def get_all_variables_for_point(self, lat: float, lon: float, event: EventType):
        """
        为给定的经纬度和事件，从缓存中提取变量。
        """
        dataset = self.datasets.get(event)
        if dataset is None:
            return {"error": f"事件 '{event}' 的数据尚未加载或加载失败。"}

        try:
            lon_360 = lon + 360 if lon < 0 else lon
            point_data = dataset.sel(latitude=lat, longitude=lon_360, method="nearest")
            
            # --- 这里的数据提取逻辑需要与 grib_downloader.py 中的 DATA_BLOCKS 保持同步 ---
            total_cloud_cover = to_python_float(point_data.get("tcdc", np.nan))
            high_cloud_cover = to_python_float(point_data.get("hcc", np.nan))
            medium_cloud_cover = to_python_float(point_data.get("mcc", np.nan))
            low_cloud_cover = to_python_float(point_data.get("lcc", np.nan))
            visibility = to_python_float(point_data.get("vis", np.nan))
            
            # AOD 估算
            approximated_aod = 0.2 # 默认值
            if not np.isnan(visibility):
                vis_km = visibility / 1000
                approximated_aod = max(0.1, min(1.0, -0.04 * vis_km + 1.0))
            else:
                logger.warning(f"事件 '{event}' 的能见度数据不可用，AOD使用默认值0.2")

            # 云底高度估算 (需要确保 geopotential_height 数据块已下载)
            gh_data = point_data.get("gh")
            cloud_base_height_meters = np.nan
            if gh_data is not None:
                # 这里的逻辑需要根据您下载的GH数据结构来定
                # 假设您下载了所有等压面
                # ... (这里可以添加我们之前讨论的估算逻辑) ...
                pass # 暂时留空
            else:
                cloud_base_height_meters = 3500.0 # 如果没有GH数据，使用模拟值

            return {
                "total_cloud_cover": round(total_cloud_cover, 2) if not np.isnan(total_cloud_cover) else None,
                "high_cloud_cover": round(high_cloud_cover, 2) if not np.isnan(high_cloud_cover) else None,
                "medium_cloud_cover": round(medium_cloud_cover, 2) if not np.isnan(medium_cloud_cover) else None,
                "low_cloud_cover": round(low_cloud_cover, 2) if not np.isnan(low_cloud_cover) else None,
                "approximated_aod": round(approximated_aod, 2),
                "cloud_base_height_meters": round(cloud_base_height_meters, 2) if not np.isnan(cloud_base_height_meters) else None,
            }

        except Exception as e:
            logger.error(f"为事件 '{event}' 在 ({lat}, {lon}) 提取数据时出错: {e}", exc_info=True)
            return {"error": "在服务器端提取数据时发生内部错误。"}

# 创建单例
data_fetcher = DataFetcher()