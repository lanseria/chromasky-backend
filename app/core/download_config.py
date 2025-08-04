# app/core/download_config.py
from pathlib import Path
from enum import Enum

# 本地时区
LOCAL_TZ = "Asia/Shanghai"
LOCAL_LAT = 29.910
LOCAL_LON = 122.190

# 限制天文计算和指数计算的纬度范围（不影响数据下载）
CALCULATION_LAT_TOP = 55.0
CALCULATION_LAT_BOTTOM = 0.0

# 定义计算密度
class MapDensity(str, Enum):
    low = "low"      # 最稀疏，计算最快
    medium = "medium"  # 中等密度
    high = "high"    # 最密集，计算最慢

# 设置任务调度器使用的默认计算密度
CALCULATION_DENSITY = MapDensity.low

# --- 天文事件窗口配置 ---
# 预报的日出事件的中心时间 (本地时间, 24小时制)
SUNRISE_CENTER_TIME = "05:00" 
# 预报的日落事件的中心时间 (本地时间, 24小时制)
SUNSET_CENTER_TIME = "19:00"
# 时间窗口的总分钟数 (例如, 60 表示中心时间前后各30分钟)
WINDOW_MINUTES = 60

# 下载文件的根目录
DOWNLOAD_DIR = Path("grib_data")

# --- GFS 特定配置 ---
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

# --- CAMS AOD 特定配置 ---
CAMS_DATASET_NAME = 'cams-global-atmospheric-composition-forecasts'
CAMS_DATA_BLOCK = {
    'variable': 'total_aerosol_optical_depth_550nm',
}