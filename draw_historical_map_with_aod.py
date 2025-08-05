# draw_historical_map_with_aod_v2.py
import argparse
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
import sys

import numpy as np
import pandas as pd
import xarray as xr
from shapely import contains, points
from shapely.geometry import Polygon
import cfgrib

# --- 项目根目录设置 (保持不变) ---
FILE = Path(__file__).resolve()
ROOT = FILE.parent.parent
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

try:
    from tools.map_drawer import generate_map_from_grid
    from app.core.download_config import (
        LOCAL_TZ, SUNRISE_EVENT_TIMES, SUNSET_EVENT_TIMES,
        WINDOW_MINUTES, CALCULATION_LAT_BOTTOM, CALCULATION_LAT_TOP
    )
    from app.services.astronomy_service import AstronomyService
    from shapely import union_all
    from app.services.chromasky_calculator import (
        score_local_clouds, score_cloud_altitude
    )
except ImportError as e:
    print(f"❌ 关键模块导入失败: {e}")
    print("请确保你从项目的根目录运行此脚本，并且所有依赖已安装。")
    exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("HistoricalMapDrawerWithAOD_V2")

# --- 评分函数 (保持不变) ---
def score_sun_altitude(altitude_deg: np.ndarray) -> np.ndarray:
    alt_points = [-8, -5, -2, 0, 5]
    score_points = [0.5, 1.0, 1.0, 0.8, 0.4]
    return np.interp(altitude_deg, alt_points, score_points)

def score_aerosol_optical_depth(aod: np.ndarray) -> np.ndarray:
    aod_points = [0.0, 0.1, 0.3, 0.8, 2.0]
    score_points = [0.6, 1.0, 1.0, 0.5, 0.1]
    return np.interp(aod, aod_points, score_points)

def print_stats(da: xr.DataArray, name: str):
    # ... (此函数保持不变) ...
    if da is None or da.size == 0: logger.info(f"  > Stats for {name}: DataArray is empty or None"); return
    try:
        values = da.values
        if np.all(np.isnan(values)): logger.info(f"  > Stats for {name}: All values are NaN.")
        else: logger.info(f"  > Stats for {name}: Min={np.nanmin(values):.3f}, Max={np.nanmax(values):.3f}, Mean={np.nanmean(values):.3f}")
    except Exception as e: logger.error(f"    Could not print stats for {name}: {e}")

class HistoricalDataFetcher:
    def __init__(self, target_date: date, source: str):
        self.target_date, self.source = target_date, source
        self.data_dir = Path("historical_data") / target_date.strftime('%Y-%m-%d') / source
        self.dataset = None
        self.time_coord_name = None
        if not self.data_dir.exists(): raise FileNotFoundError(f"未找到历史数据目录: {self.data_dir}")
        self._load_data()

    def _load_data(self):
        netcdf_file = self.data_dir / "era5_data.nc"
        if not netcdf_file.exists(): raise FileNotFoundError(f"在 {self.data_dir} 中未找到ERA5数据文件 (era5_data.nc)。")
        logger.info(f"正在加载 [ERA5] 数据: {netcdf_file.name}")
        ds = xr.open_dataset(netcdf_file)
        
        # 动态检测时间坐标
        time_coord_name = None
        possible_time_names = ['valid_time', 'time', 't']
        for name in possible_time_names:
            if name in ds.coords:
                time_coord_name = name
                logger.info(f"[ERA5] 成功检测到时间坐标为: '{name}'")
                break
        if time_coord_name is None: raise ValueError(f"ERA5数据中未找到任何已知的时间坐标。")
        
        # 统一重命名
        if time_coord_name != 'time':
            logger.info(f"[ERA5] 将时间坐标从 '{time_coord_name}' 重命名为 'time'。")
            ds = ds.rename({time_coord_name: 'time'})
        
        # --- 终极修复 Part 1：确保时间坐标是一维的 ---
        if ds.time.ndim > 1:
            logger.warning(f"[ERA5] 时间坐标维度为 {ds.time.ndim}，超过1维。正在将其展平...")
            # 获取原始多维时间值，展平，然后重新赋给坐标
            ds = ds.assign_coords(time=ds.time.values.flatten())
            logger.info("[ERA5] 时间坐标已成功展平为1维。")
            
        self.dataset = ds.load()
        expected_vars = ['hcc', 'mcc', 'cbh']
        if not all(var in self.dataset for var in expected_vars): raise ValueError("ERA5 NetCDF 文件不完整。")
        logger.info("[ERA5] 数据加载成功。")

    def get_data_for_time(self, target_time_utc: datetime) -> xr.Dataset | None:
        if self.dataset is None: return None
        try:
            target_np_time = np.datetime64(target_time_utc.replace(tzinfo=None))
            available_times = self.dataset.time.values
            time_diffs = np.abs(available_times - target_np_time)
            nearest_index = time_diffs.argmin()
            min_diff = time_diffs[nearest_index]
            tolerance = np.timedelta64(2, 'h')
            if min_diff <= tolerance:
                return self.dataset.isel(time=nearest_index)
            else:
                logger.warning(f"在ERA5数据集中找到的最近时间点与目标 {target_time_utc.isoformat()} 的差距 ({min_diff}) 超过了容差 ({tolerance})。")
                return None
        except Exception as e:
            logger.error(f"为 {target_time_utc.isoformat()} 选择ERA5数据时发生未知错误: {e}", exc_info=True)
            return None

# --- AODDataFetcher 类保持不变 ---
class AODDataFetcher:
    def __init__(self, target_date: date):
        self.target_date = target_date
        self.data_dir = Path("forecast_data") / target_date.strftime('%Y-%m-%d') / "cams"
        self.dataset = None
        if not self.data_dir.exists(): raise FileNotFoundError(f"未找到AOD数据目录: {self.data_dir}")
        self._load_data()

    def _load_data(self):
        netcdf_file = self.data_dir / "cams_aod_data.nc"
        if not netcdf_file.exists(): raise FileNotFoundError(f"在 {self.data_dir} 中未找到AOD数据文件。")
        logger.info(f"正在加载 [CAMS AOD] 数据: {netcdf_file.name}")
        ds = xr.open_dataset(netcdf_file)
        
        time_coord_name = 'valid_time'
        if time_coord_name not in ds.coords: raise ValueError("AOD数据中未找到'valid_time'坐标。")
        
        logger.info(f"[CAMS AOD] 将时间坐标从 '{time_coord_name}' 重命名为 'time'。")
        ds = ds.rename({time_coord_name: 'time'})
        
        # --- 终极修复 Part 2：确保时间坐标是一维的 ---
        if ds.time.ndim > 1:
            logger.warning(f"[CAMS AOD] 时间坐标维度为 {ds.time.ndim}，超过1维。正在将其展平...")
            ds = ds.assign_coords(time=ds.time.values.flatten())
            logger.info("[CAMS AOD] 时间坐标已成功展平为1维。")
            
        self.dataset = ds.load()
        if 'aod550' not in self.dataset: raise ValueError("AOD NetCDF 文件不完整。")
        logger.info("[CAMS AOD] 数据加载成功。")

    def get_data_for_time(self, target_time_utc: datetime) -> xr.Dataset | None:
        if self.dataset is None: return None
        try:
            target_np_time = np.datetime64(target_time_utc.replace(tzinfo=None))
            available_times = self.dataset.time.values
            time_diffs = np.abs(available_times - target_np_time)
            nearest_index = time_diffs.argmin()
            min_diff = time_diffs[nearest_index]
            tolerance = np.timedelta64(90, 'm')
            if min_diff <= tolerance:
                return self.dataset.isel(time=nearest_index)
            else:
                logger.warning(f"在AOD数据集中找到的最近时间点与目标 {target_time_utc.isoformat()} 的差距 ({min_diff}) 超过了容差 ({tolerance})。")
                return None
        except Exception as e:
            logger.error(f"为 {target_time_utc.isoformat()} 选择AOD数据时发生未知错误: {e}", exc_info=True)
            return None

def get_event_polygon_for_batch_historical(event_type_prefix: str, time_list: list[str], target_date_override: date) -> Polygon | None:
    # ... (代码不变)
    logger.info(f"--- [天象计算] 开始为事件 '{event_type_prefix}' 在日期 {target_date_override} 批处理计算地理区域 ---")
    astronomy_service = AstronomyService()
    event_type = "sunrise" if "sunrise" in event_type_prefix else "sunset"
    all_polygons = []
    for center_time_str in time_list:
        area_geojson = astronomy_service.generate_event_area_geojson(
            event=event_type, target_date=target_date_override, center_time_str=center_time_str,
            window_minutes=WINDOW_MINUTES, local_tz_str=LOCAL_TZ,
            lat_range=(CALCULATION_LAT_BOTTOM, CALCULATION_LAT_TOP)
        )
        if "error" not in area_geojson and area_geojson["features"]:
            all_polygons.append(Polygon(area_geojson["features"][0]["geometry"]["coordinates"][0]))
    if not all_polygons: return None
    merged_polygon = union_all(all_polygons)
    logger.info(f"成功合并地理区域，总面积: {merged_polygon.area:.2f} (平方度)。")
    return merged_polygon

def calculate_historical_composite_score(
    target_date: date, event_type: str, time_suffixes: list, 
    era5_fetcher: HistoricalDataFetcher, cams_fetcher: AODDataFetcher
) -> xr.DataArray | None:
    """
    核心计算函数，通过强制提取最后两个维度来确保所有数据都是二维的。
    """
    all_scores, debug_printed = [], False
    local_tz = ZoneInfo(LOCAL_TZ)
    astro_service = AstronomyService()

    for suffix in time_suffixes:
        time_str = f"{suffix[:2]}:{suffix[2:]}"
        center_time_local = datetime.combine(target_date, datetime.strptime(time_str, '%H:%M').time(), tzinfo=local_tz)
        target_time_utc = center_time_local.astimezone(timezone.utc)
        logger.info(f"===== 开始处理子事件: {time_str} (UTC: {target_time_utc.isoformat()}) =====")
        
        ds_era5 = era5_fetcher.get_data_for_time(target_time_utc)
        ds_aod = cams_fetcher.get_data_for_time(target_time_utc)
        
        if ds_era5 is None or ds_aod is None:
            logger.warning(f"无法获取 {time_str} 的ERA5或AOD数据，跳过。")
            continue
            
        # --- 终极修复：无论输入是多少维，直接提取最后两个维度的二维切片 ---
        
        # 1. 定义目标网格坐标（以高分辨率的ERA5为准）
        target_coords = {'latitude': ds_era5.latitude.values, 'longitude': ds_era5.longitude.values}
        target_dims = ['latitude', 'longitude']

        # 2. 从任意维度的 NumPy 数组中提取二维空间数据
        # array.values[..., -H:, -W:] 这种语法确保我们总是得到最后两个维度
        hcc_values = ds_era5.hcc.values
        mcc_values = ds_era5.mcc.values
        cbh_values = ds_era5.cbh.values
        aod_values = ds_aod.aod550.values

        # 使用负索引来保证我们总是取到最后两个维度
        high_cloud_2d = hcc_values.reshape(-1, hcc_values.shape[-2], hcc_values.shape[-1])[0]
        medium_cloud_2d = mcc_values.reshape(-1, mcc_values.shape[-2], mcc_values.shape[-1])[0]
        cloud_base_height_2d = cbh_values.reshape(-1, cbh_values.shape[-2], cbh_values.shape[-1])[0]
        aod_2d_raw = aod_values.reshape(-1, aod_values.shape[-2], aod_values.shape[-1])[0]

        # 3. 用干净的二维数据重构 DataArray
        high_cloud = xr.DataArray(high_cloud_2d, coords=target_coords, dims=target_dims)
        medium_cloud = xr.DataArray(medium_cloud_2d, coords=target_coords, dims=target_dims)
        cloud_base_height = xr.DataArray(cloud_base_height_2d, coords=target_coords, dims=target_dims)
        
        aod_raw_coords = {'latitude': ds_aod.latitude.values, 'longitude': ds_aod.longitude.values}
        aod_raw = xr.DataArray(aod_2d_raw, coords=aod_raw_coords, dims=target_dims)

        # 4. 在干净的二维 aod_raw 上进行重采样
        logger.info("  > 正在将AOD数据重采样(regridding)到ERA5网格...")
        aod = aod_raw.interp_like(high_cloud, method="linear")

        # --- 数据准备 ---
        if high_cloud.max() <= 1.0: high_cloud *= 100
        if medium_cloud.max() <= 1.0: medium_cloud *= 100
        
        # --- 因子计算 (现在所有输入都保证是二维的) ---
        factor_a = xr.apply_ufunc(score_local_clouds, high_cloud, medium_cloud, vectorize=True)
        # ... (后续计算逻辑不变) ...
        sun_altitude_grid = astro_service.get_sun_altitude_grid(ds_era5.latitude, ds_era5.longitude, target_time_utc)
        factor_b = xr.apply_ufunc(score_sun_altitude, sun_altitude_grid, vectorize=True)
        factor_c = xr.apply_ufunc(score_aerosol_optical_depth, aod, vectorize=True)
        
        if np.all(np.isnan(cloud_base_height.values)):
            factor_d = xr.full_like(cloud_base_height, 0.7, dtype=float)
        else:
            factor_d = xr.apply_ufunc(score_cloud_altitude, cloud_base_height, vectorize=True)
        
        score = factor_a * factor_b * factor_c * factor_d * 10
        
        if not debug_printed:
            # ... (Debug 日志部分不变) ...
            debug_printed = True
        
        all_scores.append(score)
        
        all_scores.append(score)
        
    if not all_scores: return None
    composite_da = xr.concat(all_scores, dim='time_batch').fillna(0)
    final_score = composite_da.max(dim='time_batch')
    logger.info(f"已将 {len(all_scores)} 个子事件的分数合并（取最大值）。")
    
    # 这里的 final_score 现在保证是二维的了
    time_list_for_poly = [f"{s[:2]}:{s[2:]}" for s in time_suffixes]
    poly_event_prefix = f"today_{event_type}"
    event_polygon = get_event_polygon_for_batch_historical(poly_event_prefix, time_list_for_poly, target_date_override=target_date)
    
    if event_polygon:
        mask = xr.full_like(final_score, fill_value=False, dtype=bool)
        lons, lats = np.meshgrid(final_score.longitude, final_score.latitude)
        lons_180 = np.where(lons > 180, lons - 360, lons)
        is_inside = contains(event_polygon, points(lons_180, lats))
        # 现在这里的赋值是安全的
        mask.values = is_inside
        final_score = final_score.where(mask)
        logger.info("已使用天象事件地理区域对分数进行裁剪。")
    
    final_score = final_score.fillna(0)
    final_score.name = "chromasky_score_historical_full"
    return final_score

def main():
    parser = argparse.ArgumentParser(description="为指定的过去日期生成包含AOD的火烧云指数地图。")
    parser.add_argument("date", type=str, help="目标日期 (YYYY-MM-DD)")
    parser.add_argument("event", type=str, choices=["sunrise", "sunset"], help="事件类型")
    args = parser.parse_args()
    
    try:
        target_d = date.fromisoformat(args.date)
    except ValueError:
        logger.error(f"日期格式无效: '{args.date}'。"); return

    event_type_str = args.event
    
    era5_dir = Path("historical_data") / target_d.strftime('%Y-%m-%d') / "era5"
    cams_dir = Path("forecast_data") / target_d.strftime('%Y-%m-%d') / "cams"
    if not era5_dir.exists() or not cams_dir.exists():
        logger.error(f"日期 {target_d} 缺少必要的数据。请确保 ERA5 和 CAMS AOD 数据都已下载。")
        return

    time_suffixes_to_run = [t.replace(":", "") for t in (SUNRISE_EVENT_TIMES if event_type_str == "sunrise" else SUNSET_EVENT_TIMES)]
    
    logger.info(f"\n===== 开始为 {args.date} {args.event} 生成完整版地图 =====\n")
    try:
        era5_fetcher = HistoricalDataFetcher(target_d, "era5")
        cams_fetcher = AODDataFetcher(target_d)
        
        score_grid = calculate_historical_composite_score(
            target_d, event_type_str, time_suffixes_to_run, era5_fetcher, cams_fetcher
        )
        
        if score_grid is not None:
            output_dir = Path("historical_maps_full"); output_dir.mkdir(exist_ok=True)
            filename = f"{target_d.strftime('%Y%m%d')}_{event_type_str}_full_factors.png"
            output_file_path = output_dir / filename
            
            map_title = f"Full Score (A+B+C+D): {event_type_str.title()}\nDate: {target_d.isoformat()}"
            
            generate_map_from_grid(
                score_grid=score_grid,
                title=map_title,
                output_path=output_file_path,
            )
        else:
            logger.error(f"无法计算完整分数网格，跳过制图。")
    except Exception as e:
        logger.error(f"处理数据时发生严重错误: {e}", exc_info=True)

if __name__ == "__main__":
    main()