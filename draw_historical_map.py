# draw_historical_map.py (v16 - Final Logic and Merge Fix)
import argparse
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import xarray as xr
from shapely import contains, points
from shapely.geometry import Polygon
import cfgrib

try:
    from draw_score_map import draw_map, WINDOW_MINUTES, CALCULATION_LAT_BOTTOM, CALCULATION_LAT_TOP
    from app.services.astronomy_service import AstronomyService
    from shapely import union_all
except ImportError:
    print("错误: 无法导入 'draw_score_map' 或 'app' 模块。")
    exit(1)

try:
    from app.services.chromasky_calculator import (
        score_local_clouds, score_cloud_altitude
    )
    from app.core.download_config import (
        LOCAL_TZ, SUNRISE_EVENT_TIMES, SUNSET_EVENT_TIMES
    )
except ImportError:
    print("错误: 无法从 'app' 目录导入模块。")
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
        if not self.data_dir.exists(): raise FileNotFoundError(f"未找到数据目录: {self.data_dir}")
        self._load_data()

    def _load_data(self):
        grib_file_path_list = sorted(list(self.data_dir.glob("*.grib")))
        if not grib_file_path_list: raise FileNotFoundError(f"在 {self.data_dir} 中未找到任何GRIB文件。")
        try:
            logger.info(f"正在从 {len(grib_file_path_list)} 个GRIB文件中加载数据...")
            
            final_ds = None
            expected_vars = ['hcc', 'mcc', 'tcc', 'cbh']

            for var_name in expected_vars:
                logger.info(f"--- 正在为变量 '{var_name}' 加载所有GRIB文件 ---")
                var_ds_list = []
                for grib_path in grib_file_path_list:
                    try:
                        ds_var = xr.open_dataset(grib_path, engine="cfgrib", backend_kwargs={'filter_by_keys': {'shortName': var_name}})
                        coords_to_keep = ['time', 'latitude', 'longitude']
                        coords_to_drop = [coord for coord in ds_var.coords if coord not in coords_to_keep]
                        if coords_to_drop:
                            ds_var = ds_var.drop_vars(coords_to_drop)
                        var_ds_list.append(ds_var)
                    except Exception: 
                        logger.warning(f"  > 在文件 {grib_path.name} 中未找到变量 '{var_name}' 或加载失败，已跳过。")
                
                if not var_ds_list: 
                    logger.error(f"  > 错误: 在任何文件中都未能加载变量 '{var_name}'。"); continue
                
                full_var_ds = xr.concat(var_ds_list, dim="time")
                _, index = np.unique(full_var_ds['time'], return_index=True)
                full_var_ds = full_var_ds.isel(time=index)

                # --- 关键修复：迭代式合并 ---
                if final_ds is None:
                    final_ds = full_var_ds
                else:
                    final_ds = final_ds.merge(full_var_ds)

            if final_ds is None or not all(var in final_ds for var in expected_vars):
                raise ValueError("GRIB 文件加载或合并后不完整。")
            
            self.dataset = final_ds.sortby('time').load()
            logger.info("所有GRIB文件已成功加载并合并。")
            time_range = (self.dataset.time.min().values, self.dataset.time.max().values)
            logger.info(f"  > 最终数据集的UTC时间范围: {pd.to_datetime(str(time_range[0]))} to {pd.to_datetime(str(time_range[1]))}")
        except Exception as e: 
            logger.error(f"加载或处理 GRIB 文件时发生严重错误: {e}", exc_info=True); raise

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
    # ... (same as before)
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
        
        # --- 关键逻辑修复 ---
        if high_cloud is None or medium_cloud is None or cloud_base_height is None:
            logger.error(f"数据在时间点 {time_str} 不完整，跳过。")
            continue
        
        if high_cloud.max() <= 1.0: high_cloud *= 100
        if medium_cloud.max() <= 1.0: medium_cloud *= 100
            
        factor_a = xr.apply_ufunc(score_local_clouds, high_cloud, medium_cloud, vectorize=True)
        
        # 如果 cbh 全部是 NaN，说明数据缺失，此时云高因子应为中性值
        if np.all(np.isnan(cloud_base_height.values)):
            logger.warning(f"  > 在时间点 {time_str}，Cloud Base Height (cbh) 数据全部为NaN。将云高因子设为中性值0.7。")
            factor_d = 0.7 
        else:
            # 否则，正常计算，但要处理单个的NaN值
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
                mid_time_str = time_suffixes_to_run[len(time_suffixes_to_run)//2]
                rep_dt = datetime.fromisoformat(f"{target_d.isoformat()}T{mid_time_str[:2]}:{mid_time_str[2:]}:00")
                score_grid = score_grid.assign_coords(datetime_for_title=pd.to_datetime(rep_dt))
                map_title = f"Historical - {event_type_str.title()} ({source_name.upper()})"
                draw_map(score_grid, map_title, output_file_path)
            else:
                logger.error(f"无法为数据源 '{source_name}' 计算分数网格，跳过制图。")
        except Exception as e:
            logger.error(f"处理数据源 '{source_name}' 时发生严重错误: {e}", exc_info=True)

if __name__ == "__main__":
    main()