# app/services/data_fetcher.py
import xarray as xr
import numpy as np
import logging
from datetime import datetime, timezone, timedelta
from .grib_downloader import grib_downloader

# --- 辅助函数 (保持不变) ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def to_python_float(value) -> float:
    if hasattr(value, 'item'):
        return value.item()
    return float(value)

# --- DataFetcher 类 (核心修改) ---
class DataFetcher:
    """
    负责加载由 GribDownloader 下载的、结构清晰的 GRIB 数据。
    """
    def __init__(self):
        self.dataset = None
        # 添加一个新属性来存储时间元数据
        self.time_metadata = {}
        self._initialize_data()

    def _initialize_data(self):
        """
        在应用启动时，触发下载并加载特定时间点的数据。
        (已更新，对下载器的返回值进行健壮性检查)
        """
        target_time_utc = datetime.now(timezone.utc) + timedelta(hours=8)
        logger.info(f"正在为目标时间 {target_time_utc.isoformat()} 触发 GRIB 数据下载...")
        
        # 调用下载器，它总是返回一个2元素的元组
        time_meta, file_paths = grib_downloader.get_gfs_data_for_time(target_time_utc)

        # 关键的健壮性检查
        if not time_meta or not isinstance(file_paths, dict):
            logger.error("下载器未能返回有效的数据路径，DataFetcher 初始化失败。")
            # 确保 self.dataset 保持为 None
            self.dataset = None
            return # 提前退出函数
        
        # 存储时间元数据
        self.time_metadata = time_meta
        logger.info(f"已获取时间元数据: {self.time_metadata}")
        
        datasets_to_merge = []
        # 现在我们可以安全地调用 .items()，因为我们已经确认了 file_paths 是一个字典
        for block_name, path in file_paths.items():
            if path and path.exists():
                try:
                    open_kwargs = {"engine": "cfgrib", "decode_timedelta": False}
                    backend_kwargs = {'filter_by_keys': {'stepType': 'instant'}}
                    
                    try:
                        ds = xr.open_dataset(path, **open_kwargs, backend_kwargs=backend_kwargs)
                    except ValueError:
                        logger.warning(f"文件 {path} 按 stepType='instant' 加载失败，尝试无 stepType 加载...")
                        ds = xr.open_dataset(path, **open_kwargs)
                        
                    datasets_to_merge.append(ds)
                    logger.info(f"成功加载文件: {path}")

                except Exception as e:
                    logger.error(f"加载文件 {path} 时发生严重错误: {e}", exc_info=True)
            else:
                logger.warning(f"数据块 '{block_name}' 未能下载，将跳过。")
        
        if not datasets_to_merge:
            logger.error("没有任何数据文件被成功加载，数据集为空。")
            self.dataset = None
            return
            
        self.dataset = xr.merge(datasets_to_merge)
        logger.info("所有GRIB数据块已成功合并。")
        logger.info(f"最终可用的变量: {list(self.dataset.variables)}")

    def get_all_variables_for_point(self, lat: float, lon: float):
        """
        (这个方法基本可以保持不变，因为数据结构现在变得清晰了)
        """
        if self.dataset is None:
            return {"error": "数据集未加载或加载失败。"}

        try:
            lon_360 = lon + 360 if lon < 0 else lon
            point_data = self.dataset.sel(latitude=lat, longitude=lon_360, method="nearest")
            
            # 现在变量名和层级都应该是正确的
            total_cloud_cover = to_python_float(point_data.get("tcdc", np.nan))
            high_cloud_cover = to_python_float(point_data.get("hcc", np.nan))
            medium_cloud_cover = to_python_float(point_data.get("mcc", np.nan))
            low_cloud_cover = to_python_float(point_data.get("lcc", np.nan))
            visibility = to_python_float(point_data.get("vis", np.nan))

            approximated_aod = np.nan
            if not np.isnan(visibility):
                vis_km = visibility / 1000
                approximated_aod = max(0.1, min(1.0, -0.04 * vis_km + 1.0))
            else:
                # 如果能见度下载失败，我们回退到硬编码值
                approximated_aod = 0.2
                logger.warning("能见度数据不可用，AOD使用默认值0.2")

            return {
                "total_cloud_cover": round(total_cloud_cover, 2) if not np.isnan(total_cloud_cover) else None,
                "high_cloud_cover": round(high_cloud_cover, 2) if not np.isnan(high_cloud_cover) else None,
                "medium_cloud_cover": round(medium_cloud_cover, 2) if not np.isnan(medium_cloud_cover) else None,
                "low_cloud_cover": round(low_cloud_cover, 2) if not np.isnan(low_cloud_cover) else None,
                "approximated_aod": round(approximated_aod, 2),
            }

        except Exception as e:
            logger.error(f"在经纬度({lat}, {lon})提取数据时出错: {e}", exc_info=True)
            return {"error": "在服务器端提取数据时发生内部错误。"}

# 创建单例
data_fetcher = DataFetcher()