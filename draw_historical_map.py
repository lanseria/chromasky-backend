# draw_historical_map.py (v19 - Use Centralized Map Drawer)
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

# --- 关键修复：确保项目根目录在 Python 搜索路径中 ---
FILE = Path(__file__).resolve()
ROOT = FILE.parent.parent # 该脚本在根目录，所以父目录是根
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))
# --- 修复结束 ---

try:
    # --- 关键修改：从 tools.map_drawer 导入绘图函数 ---
    from tools.map_drawer import generate_map_from_grid
    
    # 导入其他需要的模块
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
logger = logging.getLogger("HistoricalMapDrawer")

def print_stats(da: xr.DataArray, name: str):
    if da is None or da.size == 0: logger.info(f"  > Stats for {name}: DataArray is empty or None"); return
    try:
        values = da.values
        if np.all(np.isnan(values)): logger.info(f"  > Stats for {name}: All values are NaN.")
        else: logger.info(f"  > Stats for {name}: Min={np.nanmin(values):.2f}, Max={np.nanmax(values):.2f}, Mean={np.nanmean(values):.2f}")
    except Exception as e: logger.error(f"    Could not print stats for {name}: {e}")

class HistoricalDataFetcher:
    def __init__(self, target_date: date, source: str):
        self.target_date, self.source = target_date, source
        self.data_dir = Path("historical_data") / target_date.strftime('%Y-%m-%d') / source
        self.dataset = None
        self.time_coord_name = None
        if not self.data_dir.exists(): raise FileNotFoundError(f"未找到数据目录: {self.data_dir}")
        self._load_data()

    def _load_data(self):
        netcdf_file = self.data_dir / "era5_data.nc"
        if not netcdf_file.exists():
            raise FileNotFoundError(f"在 {self.data_dir} 中未找到优化的数据文件 (era5_data.nc)。请先运行 download_historical_data.py。")

        try:
            logger.info(f"正在从优化的 NetCDF 文件加载数据: {netcdf_file.name}")
            ds = xr.open_dataset(netcdf_file)
            
            possible_time_names = ['valid_time', 'time', 't']
            for name in possible_time_names:
                if name in ds.coords:
                    self.time_coord_name = name
                    logger.info(f"成功检测到时间坐标为: '{name}'")
                    break
            
            if self.time_coord_name is None:
                raise ValueError(f"无法在数据集中找到任何已知的时间坐标 (已尝试: {possible_time_names})。")

            if self.time_coord_name != 'time':
                logger.info(f"将时间坐标从 '{self.time_coord_name}' 重命名为 'time' 以实现兼容性。")
                ds = ds.rename({self.time_coord_name: 'time'})
                self.time_coord_name = 'time'

            self.dataset = ds.load()
            
            expected_vars = ['hcc', 'mcc', 'tcc', 'cbh']
            if not all(var in self.dataset for var in expected_vars):
                 raise ValueError(f"NetCDF 文件不完整，缺少变量。")
            
            logger.info("所有数据已成功加载。")
            time_range = (self.dataset.time.min().values, self.dataset.time.max().values)
            logger.info(f"  > 数据集的UTC时间范围: {pd.to_datetime(str(time_range[0]))} to {pd.to_datetime(str(time_range[1]))}")
        
        except Exception as e: 
            logger.error(f"加载 NetCDF 文件 '{netcdf_file.name}' 时发生严重错误: {e}", exc_info=True)
            raise

    def get_data_for_time(self, target_time_utc: datetime) -> xr.Dataset | None:
        if self.dataset is None: return None
        try:
            naive_target_time = target_time_utc.replace(tzinfo=None)
            return self.dataset.sel(time=naive_target_time, method="nearest", tolerance=np.timedelta64(2, 'h'))
        except KeyError: 
            logger.warning(f"在容差范围内未找到目标时间 {target_time_utc.isoformat()} 的数据。"); return None
        except Exception as e: 
            logger.error(f"在为 {target_time_utc.isoformat()} 选择数据时出错: {e}", exc_info=True); return None

def get_event_polygon_for_batch_historical(event_type_prefix: str, time_list: list[str], target_date_override: date) -> Polygon | None:
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

def calculate_historical_composite_score(target_date: date, event_type: str, time_suffixes: list, data_fetcher: HistoricalDataFetcher) -> xr.DataArray | None:
    all_scores, debug_printed = [], False
    local_tz = ZoneInfo(LOCAL_TZ)
    for suffix in time_suffixes:
        time_str = f"{suffix[:2]}:{suffix[2:]}"
        center_time_local = datetime.combine(target_date, datetime.strptime(time_str, '%H:%M').time(), tzinfo=local_tz)
        target_time_utc = center_time_local.astimezone(timezone.utc)
        logger.info(f"===== 开始处理子事件: {time_str} (UTC: {target_time_utc.isoformat()}) =====")
        ds_at_time = data_fetcher.get_data_for_time(target_time_utc)
        if ds_at_time is None: logger.warning(f"无法获取 {time_str} 的数据，跳过。"); continue
        
        high_cloud, medium_cloud, cloud_base_height = ds_at_time.get('hcc'), ds_at_time.get('mcc'), ds_at_time.get('cbh')
        
        if high_cloud is None or medium_cloud is None or cloud_base_height is None:
            logger.error(f"数据在时间点 {time_str} 不完整，跳过。")
            continue
        
        if high_cloud.max() <= 1.0: high_cloud *= 100
        if medium_cloud.max() <= 1.0: medium_cloud *= 100
            
        factor_a = xr.apply_ufunc(score_local_clouds, high_cloud, medium_cloud, vectorize=True)
        
        if np.all(np.isnan(cloud_base_height.values)):
            logger.warning(f"  > 在时间点 {time_str}，Cloud Base Height (cbh) 数据全部为NaN。将云高因子设为中性值0.7。")
            factor_d = xr.full_like(cloud_base_height, 0.7, dtype=float)
        else:
            factor_d = xr.apply_ufunc(score_cloud_altitude, cloud_base_height, vectorize=True)

        score = factor_a * factor_d * 10
        
        if not debug_printed:
            logger.info("--- [DEBUG] Inspecting first valid time step's data ---")
            print_stats(high_cloud, "High Cloud Cover (%)")
            print_stats(medium_cloud, "Medium Cloud Cover (%)")
            print_stats(cloud_base_height, "Cloud Base Height (m)")
            logger.info("---"); print_stats(factor_a, "Factor A"); print_stats(factor_d, "Factor D"); logger.info("---"); print_stats(score, "Final Score (single step)"); logger.info("-------------------------------------------------")
            debug_printed = True
        all_scores.append(score)
        
    if not all_scores: return None
    composite_da = xr.concat(all_scores, dim='time_batch')
    final_score = composite_da.max(dim='time_batch')
    logger.info(f"已将 {len(all_scores)} 个子事件的分数合并（取最大值）。")
    time_list_for_poly = [f"{s[:2]}:{s[2:]}" for s in time_suffixes]
    poly_event_prefix = f"today_{event_type}"
    event_polygon = get_event_polygon_for_batch_historical(poly_event_prefix, time_list_for_poly, target_date_override=target_date)
    if event_polygon:
        mask = xr.full_like(final_score, fill_value=False, dtype=bool)
        lons, lats = np.meshgrid(final_score.longitude, final_score.latitude)
        lons_180 = np.where(lons > 180, lons - 360, lons)
        is_inside = contains(event_polygon, points(lons_180, lats))
        mask.values = is_inside
        final_score = final_score.where(mask)
        logger.info("已使用天象事件地理区域对分数进行裁剪。")
    final_score = final_score.fillna(0)
    final_score.name = "chromasky_score_historical"
    return final_score

def main():
    parser = argparse.ArgumentParser(description="为指定的过去日期生成火烧云指数地图。")
    parser.add_argument("date", type=str, help="目标日期 (YYYY-MM-DD)")
    parser.add_argument("event", type=str, choices=["sunrise", "sunset"], help="事件类型")
    args = parser.parse_args()
    try: target_d = date.fromisoformat(args.date)
    except ValueError: logger.error(f"日期格式无效: '{args.date}'。"); return
    event_type_str = args.event
    available_sources = [src for src in ["era5"] if (Path("historical_data") / target_d.strftime('%Y-%m-%d') / src).exists()]
    if not available_sources: logger.error(f"未找到 {target_d.strftime('%Y-%m-%d')} 的任何已下载数据。"); return
    time_suffixes_to_run = [t.replace(":", "") for t in (SUNRISE_EVENT_TIMES if event_type_str == "sunrise" else SUNSET_EVENT_TIMES)]
    for source_name in available_sources:
        logger.info(f"\n===== 开始使用 '{source_name.upper()}' 数据为 {args.date} {args.event} 生成地图 =====\n")
        try:
            fetcher = HistoricalDataFetcher(target_d, source_name)
            score_grid = calculate_historical_composite_score(target_d, event_type_str, time_suffixes_to_run, fetcher)
            if score_grid is not None:
                output_dir = Path("historical_maps"); output_dir.mkdir(exist_ok=True)
                filename = f"{target_d.strftime('%Y%m%d')}_{event_type_str}_{source_name}.png"
                output_file_path = output_dir / filename
                
                # 为地图构建一个有意义的标题
                map_title = f"Historical Score: {event_type_str.title()} ({source_name.upper()})\nDate: {target_d.isoformat()}"
                
                # --- 关键修改：调用新的绘图函数 ---
                generate_map_from_grid(
                    score_grid=score_grid,
                    title=map_title,
                    output_path=output_file_path,
                )
            else:
                logger.error(f"无法为数据源 '{source_name}' 计算分数网格，跳过制图。")
        except Exception as e:
            logger.error(f"处理数据源 '{source_name}' 时发生严重错误: {e}", exc_info=True)

if __name__ == "__main__":
    main()