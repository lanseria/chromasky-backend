# draw_score_map.py
import logging
import xarray as xr
import numpy as np
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from shapely.geometry import Polygon
from datetime import date, timedelta
from scipy.ndimage import gaussian_filter
from typing import List

# (其他导入保持不变)
from app.services.data_fetcher import DataFetcher, EventType
from app.services.astronomy_service import AstronomyService
from app.services.chromasky_calculator import (
    score_local_clouds, score_light_path, score_air_quality, score_cloud_altitude
)
from app.core.download_config import (
    SUNRISE_EVENT_TIMES, SUNSET_EVENT_TIMES, WINDOW_MINUTES, LOCAL_TZ,
    CALCULATION_LAT_TOP, CALCULATION_LAT_BOTTOM
)

# --- 关键修复：导入 shapely 2.0+ 的正确合并函数 ---
from shapely import union_all

# (clean_dataset_coords 函数保持不变)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MapDrawer")

def clean_dataset_coords(ds: xr.Dataset, name: str) -> xr.Dataset:
    """A helper function to clean coordinates of a dataset."""
    ds_cleaned = ds.copy()
    for dim in ['latitude', 'longitude']:
        if dim in ds_cleaned.coords:
            if not (pd.Series(ds_cleaned[dim].values).is_monotonic_increasing or 
                    pd.Series(ds_cleaned[dim].values).is_monotonic_decreasing):
                logger.info(f"[修复] {name} '{dim}' 坐标不是单调的，正在排序...")
                ds_cleaned = ds_cleaned.sortby(dim)
            
            _, index = np.unique(ds_cleaned[dim], return_index=True)
            if len(index) < len(ds_cleaned[dim]):
                logger.info(f"[修复] 检测到 {name} '{dim}' 坐标有重复值，正在使用索引强制去重...")
                ds_cleaned = ds_cleaned.isel({dim: index})
    return ds_cleaned

def get_event_polygon_for_batch(event_type_prefix: str, time_list: List[str]) -> Polygon | None:
    """为一批时间点计算合并后的地理区域"""
    logger.info(f"--- [天象计算] 开始为事件 '{event_type_prefix}' 批处理计算地理区域 ---")
    astronomy_service = AstronomyService()
    
    today = date.today()
    tomorrow = today + timedelta(days=1)
    
    event_type = "sunrise" if "sunrise" in event_type_prefix else "sunset"
    target_d = tomorrow if "tomorrow" in event_type_prefix else today
    
    all_polygons = []
    for center_time_str in time_list:
        area_geojson = astronomy_service.generate_event_area_geojson(
            event=event_type, target_date=target_d, center_time_str=center_time_str,
            window_minutes=WINDOW_MINUTES, local_tz_str=LOCAL_TZ, 
            lat_range=(CALCULATION_LAT_BOTTOM, CALCULATION_LAT_TOP)
        )
        if "error" not in area_geojson and area_geojson["features"]:
            all_polygons.append(Polygon(area_geojson["features"][0]["geometry"]["coordinates"][0]))
    
    if not all_polygons:
        logger.error(f"无法为事件 '{event_type_prefix}' 的任何时间点计算地理区域。")
        return None
    
    # --- 关键修复：使用 shapely.union_all() 代替旧的 cascaded_union ---
    merged_polygon = union_all(all_polygons)
    logger.info(f"成功合并地理区域，总面积: {merged_polygon.area:.2f} (平方度)。")
    return merged_polygon

# (calculate_composite_score_grid, draw_map, 和 __main__ 部分保持不变)
# ...
def calculate_composite_score_grid(
    event_type_prefix: str, 
    time_suffixes: List[str], 
    use_aod: bool = True
) -> xr.DataArray | None:
    """
    为一批事件计算并叠加分数，生成一个综合的得分网格。
    """
    all_scores = []
    
    # 遍历这一批次的所有具体事件
    for suffix in time_suffixes:
        event_name = f"{event_type_prefix}_{suffix}"
        logger.info(f"===== 开始处理子事件: {event_name} =====")
        # (这里是单个事件的计算逻辑，基本和之前一样)
        df = DataFetcher(force_reload=True)
        if event_name not in df.gfs_datasets:
            logger.warning(f"跳过子事件: {event_name}，数据未找到。")
            continue
        
        gfs_ds = clean_dataset_coords(df.gfs_datasets[event_name], "GFS")
        lat_slice = slice(CALCULATION_LAT_TOP, CALCULATION_LAT_BOTTOM)
        gfs_ds = gfs_ds.sel(latitude=lat_slice)
        
        high_cloud = gfs_ds.get('hcdc', gfs_ds.get('hcc', xr.DataArray(0)))
        medium_cloud = gfs_ds.get('mcdc', gfs_ds.get('mcc', xr.DataArray(0)))
        total_cloud = gfs_ds.get('tcdc', gfs_ds.get('tcc', xr.DataArray(0)))
        cloud_base = gfs_ds.get('gh', gfs_ds.get('hgt'))
        if cloud_base is None: continue

        factor_a = xr.apply_ufunc(score_local_clouds, high_cloud, medium_cloud, vectorize=True)
        factor_b = xr.apply_ufunc(score_light_path, total_cloud, vectorize=True)
        factor_d = xr.apply_ufunc(score_cloud_altitude, cloud_base, vectorize=True)
        
        factor_c = 1.0 # 默认值
        if use_aod and df.aod_dataset and df.aod_base_time:
            aod_ds_original = clean_dataset_coords(df.aod_dataset, "AOD")
            if 'step' in aod_ds_original.coords:
                if 'time' in aod_ds_original.coords: aod_ds_original = aod_ds_original.drop_vars('time')
                time_deltas = pd.to_timedelta(aod_ds_original['step'].values, unit='h')
                absolute_times = [df.aod_base_time + td for td in time_deltas]
                unix_times = [t.timestamp() for t in absolute_times]
                aod_ds_processed = aod_ds_original.assign_coords(step=unix_times).rename({'step': 'time'})
                
                gfs_time_unix = pd.to_datetime(gfs_ds.time.values).timestamp()
                aod_interpolated = aod_ds_processed.interp(
                    time=gfs_time_unix, latitude=gfs_ds.latitude, longitude=gfs_ds.longitude,
                    method="linear", kwargs={"fill_value": "extrapolate"}
                )
                aod = aod_interpolated.get('aod550')
                factor_c = xr.apply_ufunc(score_air_quality, aod, vectorize=True)
            else:
                logger.warning("AOD 数据集缺少 'step' 坐标，跳过空气质量因子。")
        elif use_aod:
            logger.info("请求使用AOD但数据不可用，跳过空气质量因子。")


        score = factor_a * factor_b * factor_c * factor_d * 10
        all_scores.append(score)

    if not all_scores:
        logger.error(f"未能为事件 '{event_type_prefix}' 计算任何子事件的分数。")
        return None

    # --- 关键步骤：合并所有分数矩阵 ---
    # 将所有 DataArray 拼接成一个新的、带有 'time_batch' 维度的 DataArray
    composite_da = xr.concat(all_scores, dim='time_batch')
    # 沿着 'time_batch' 维度取最大值，得到最终的综合分数
    final_score = composite_da.max(dim='time_batch')
    logger.info(f"已将 {len(all_scores)} 个子事件的分数合并（取最大值）。")

    # --- 裁剪逻辑现在使用合并后的大区域 ---
    time_list_for_poly = [f"{s[:2]}:{s[2:]}" for s in time_suffixes]
    event_polygon = get_event_polygon_for_batch(event_type_prefix, time_list_for_poly)
    
    if event_polygon:
        mask = xr.full_like(final_score, fill_value=False, dtype=bool)
        lons, lats = np.meshgrid(final_score.longitude, final_score.latitude)
        lons_180 = np.where(lons > 180, lons - 360, lons)
        from shapely import contains, points
        is_inside = contains(event_polygon, points(lons_180, lats))
        mask.values = is_inside
        final_score = final_score.where(mask)
        logger.info("已使用合并后的天象事件地理区域对分数进行裁剪。")

    final_score = final_score.fillna(0)
    # 为标题找一个代表性的时间
    first_event_name = f"{event_type_prefix}_{time_suffixes[0]}"
    if first_event_name in df.gfs_datasets:
        representative_time = pd.to_datetime(df.gfs_datasets[first_event_name].time.values)
        final_score = final_score.assign_coords(datetime_for_title=representative_time)
        
    final_score.name = "chromasky_score"
    logger.info(f"--- [计算] 综合指数格网计算完成 ---")
    return final_score

def draw_map(score_grid: xr.DataArray, event_name: EventType, output_path: Path):
    """
    使用 Cartopy 和 Matplotlib 将计算出的指数格网绘制成地图图片（视觉增强版）。
    """
    logger.info(f"--- [绘图] 开始为事件 '{event_name}' 绘制地图 ---")
    
    scores_for_smoothing = score_grid.fillna(0).values
    smoothed_scores = gaussian_filter(scores_for_smoothing, sigma=1.5)
    
    interp_factor = 4
    orig_lats = score_grid.latitude.values
    orig_lons = score_grid.longitude.values
    
    new_lats = np.linspace(orig_lats.min(), orig_lats.max(), len(orig_lats) * interp_factor)
    new_lons = np.linspace(orig_lons.min(), orig_lons.max(), len(orig_lons) * interp_factor)
    
    high_res_grid = xr.DataArray(
        smoothed_scores,
        coords=[orig_lats, orig_lons],
        dims=['latitude', 'longitude']
    ).interp(latitude=new_lats, longitude=new_lons, method='cubic') 

    lats = high_res_grid.latitude.values
    lons = high_res_grid.longitude.values
    scores = high_res_grid.values
    
    scores[scores < 2] = np.nan

    logger.info(f"绘图前最终得分统计 (平滑后): Min={np.nanmin(scores):.2f}, Max={np.nanmax(scores):.2f}, Mean={np.nanmean(scores):.2f}")

    proj = ccrs.PlateCarree(central_longitude=180)
    fig = plt.figure(figsize=(15, 8), facecolor='black')
    ax = fig.add_subplot(1, 1, 1, projection=proj)
    ax.set_facecolor('black')

    ax.set_extent([70, 140, CALCULATION_LAT_BOTTOM, CALCULATION_LAT_TOP], crs=ccrs.PlateCarree())
    
    colors = ["#3b82f6", "#fde047", "#f97316", "#ef4444", "#ec4899"]
    nodes = [0.0, 0.5, 0.7, 0.85, 1.0]
    chromasky_cmap = mcolors.LinearSegmentedColormap.from_list("chromasky", list(zip(nodes, colors)))
    
    levels = np.linspace(2, 10, 100)
    
    contour_fill = ax.contourf(lons, lats, scores, levels=levels, cmap=chromasky_cmap, 
                               transform=ccrs.PlateCarree(), extend='max', zorder=1)
    
    contour_lines = ax.contour(lons, lats, scores, 
                               levels=[4, 6, 8, 9],
                               colors='white', 
                               linewidths=[0.5, 0.8, 1.2, 1.5],
                               alpha=0.6,
                               transform=ccrs.PlateCarree(), zorder=2)
    ax.clabel(contour_lines, inline=True, fontsize=8, fmt='%1.0f', colors='white')

    ax.add_feature(cfeature.OCEAN.with_scale('50m'), facecolor='#0c0a09', zorder=-1)
    ax.add_feature(cfeature.LAND.with_scale('50m'), facecolor='#1c1917', edgecolor='none', zorder=0)
    ax.add_feature(cfeature.COASTLINE.with_scale('50m'), edgecolor='#a8a29e', linewidth=0.5, zorder=3)
    ax.add_feature(cfeature.BORDERS.with_scale('50m'), linestyle=':', edgecolor='#78716c', zorder=3)

    gl = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=True,
                      linewidth=0.5, color='#44403c', alpha=0.8, linestyle='--')
    gl.top_labels = False
    gl.right_labels = False
    gl.xlabel_style = {'color': 'white', 'size': 8}
    gl.ylabel_style = {'color': 'white', 'size': 8}

    forecast_time_utc_str = "N/A"
    if 'datetime_for_title' in score_grid.coords:
       ts = pd.to_datetime(score_grid.datetime_for_title.values)
       forecast_time_utc_str = ts.strftime('%Y-%m-%d %H:%M UTC')

    ax.set_title(f"ChromaSky Index Forecast - {event_name.replace('_', ' ').title()}\nValid for {forecast_time_utc_str}",
                 fontsize=16, color='white')
    
    cbar = fig.colorbar(contour_fill, ax=ax, orientation='vertical', pad=0.02, shrink=0.8,
                        ticks=[2, 4, 6, 8, 10])
    cbar.set_label('ChromaSky Score (0-10)', color='white')
    cbar.ax.yaxis.set_tick_params(color='white')
    plt.setp(plt.getp(cbar.ax.axes, 'yticklabels'), color='white')

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=200, bbox_inches='tight', pad_inches=0.1, transparent=True, facecolor=fig.get_facecolor())
    plt.close(fig)
    
    logger.info(f"--- [绘图] 地图已成功保存到: {output_path} ---")


if __name__ == "__main__":
    # --- 新增：设置命令行参数解析 ---
    import argparse
    parser = argparse.ArgumentParser(description="生成火烧云指数预报图。")
    
    # 定义要接收的命令行参数
    parser.add_argument(
        "event_type",  # 参数名
        type=str,
        nargs='?',     # '?' 表示这个参数是可选的
        default="today_sunset", # 如果不提供参数，则使用此默认值
        choices=["today_sunrise", "today_sunset", "tomorrow_sunrise", "tomorrow_sunset"], # 限制可选值
        help="要生成的事件类型 (例如: today_sunset, tomorrow_sunrise)。"
    )
    
    parser.add_argument(
        "--no-aod", # 定义一个开关参数
        action="store_true", # 如果出现 --no-aod，则其值为 True
        help="计算时不使用AOD（空气质量）因子。"
    )

    args = parser.parse_args()

    # --- 使用解析后的参数 ---
    target_event_type = args.event_type
    use_aod_factor = not args.no_aod # 如果 --no-aod 被设置，则 use_aod_factor 为 False

    logger.info(f"===== 开始生成任务: event_type='{target_event_type}', use_aod={use_aod_factor} =====")
    
    # 从配置中动态获取对应的时间点后缀
    if "sunrise" in target_event_type:
        time_suffixes = [t.replace(":", "") for t in SUNRISE_EVENT_TIMES]
    else:
        time_suffixes = [t.replace(":", "") for t in SUNSET_EVENT_TIMES]
        
    # 调用新的综合计算函数
    score_data_array = calculate_composite_score_grid(
        target_event_type, 
        time_suffixes, 
        use_aod=use_aod_factor
    )
    
    if score_data_array is not None:
        output_dir = Path("map_images")
        df = DataFetcher()
        run_key = "latest"
        if df.gfs_time_metadata:
            gfs_manifest_path = df._find_latest_manifest("manifest_*_[0-9][0-9].json")
            if gfs_manifest_path:
                parts = gfs_manifest_path.name.split('_')
                run_key = f"{parts[1]}_t{parts[2][:2]}z"
        
        aod_tag = "full" if use_aod_factor else "no_aod"
        filename = f"{run_key}_{target_event_type}_composite_{aod_tag}.png"
        output_file_path = output_dir / filename
        
        draw_map(score_data_array, target_event_type, output_file_path)
    else:
        logger.error(f"因数据计算失败，无法为事件 '{target_event_type}' 生成地图。")