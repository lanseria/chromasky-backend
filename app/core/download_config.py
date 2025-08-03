# app/core/download_config.py
from pathlib import Path
from enum import Enum # Import Enum

# 限制天文计算和指数计算的纬度范围（不影响数据下载）
CALCULATION_LAT_TOP = 60.0
CALCULATION_LAT_BOTTOM = 0.0

# 定义计算密度
class MapDensity(str, Enum):
    low = "low"      # 最稀疏，计算最快
    medium = "medium"  # 中等密度
    high = "high"    # 最密集，计算最慢

# 设置任务调度器使用的默认计算密度
CALCULATION_DENSITY = MapDensity.low


# --- 新增：天文事件窗口配置 ---
# 预报的日出事件的中心时间 (本地时间, 24小时制)
SUNRISE_CENTER_TIME = "05:00" 
# 预报的日落事件的中心时间 (本地时间, 24小时制)
SUNSET_CENTER_TIME = "19:00"
# 时间窗口的总分钟数 (例如, 60 表示中心时间前后各30分钟)
WINDOW_MINUTES = 60


# --- 修改：下载区域配置 ---
# 将 SUBREGION_PARAMS 的 subregion 字段留空，以告知下载器下载全球数据。
# 下载器将忽略其他经纬度字段。
SUBREGION_PARAMS = {
    "subregion": "", # 留空代表全球
    "toplat": 33,
    "leftlon": 118,
    "rightlon": 124,
    "bottomlat": 28
}

# 下载文件的根目录
DOWNLOAD_DIR = Path("grib_data")

# --- GFS 特定配置 (保持不变) ---
GFS_BASE_URL = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
GFS_DATA_BLOCKS = {
    "total_cloud": {
        "vars": ["tcdc"],
        "levels": ["entire_atmosphere"]
    },
    "cloud_layers": {
        "vars": ["lcdc", "mcdc", "hcdc"],
        "levels": ["low_cloud_layer", "middle_cloud_layer", "high_cloud_layer"]
    },
    "cloud_base": {
        "vars": ["hgt"],
        "levels": ["cloud_ceiling"]
    },
}

# --- CAMS AOD 特定配置 (保持不变) ---
CAMS_DATASET_NAME = 'cams-global-atmospheric-composition-forecasts'
CAMS_DATA_BLOCK = {
    'variable': 'total_aerosol_optical_depth_550nm',
}