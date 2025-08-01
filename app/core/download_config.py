# app/core/download_config.py
from pathlib import Path

# --- 共享配置 ---
# 这个字典现在是所有下载器的公共配置
SUBREGION_PARAMS = {
    "subregion": "",
    "toplat": 55,
    "leftlon": 100,
    "rightlon": 135,
    "bottomlat": 15
}

# 下载文件的根目录
DOWNLOAD_DIR = Path("grib_data")

# --- GFS 特定配置 ---
GFS_BASE_URL = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
GFS_DATA_BLOCKS = {
    "total_cloud": {"vars": ["tcc"], "levels": ["entire_atmosphere"]},
    "cloud_layers": {"vars": ["lcc", "mcc", "hcc"], "levels": ["lowCloudLayer", "middleCloudLayer", "highCloudLayer"]},
    "cloud_base": {"vars": ["gh"], "levels": ["cloudCeiling"]},
    # "visibility": {"vars": ["vis"], "levels": ["surface"]}
}

# --- CAMS AOD 特定配置 ---
CAMS_DATASET_NAME = 'cams-global-atmospheric-composition-forecasts'
CAMS_DATA_BLOCK = {
    'variable': 'total_aerosol_optical_depth_550nm',
}
