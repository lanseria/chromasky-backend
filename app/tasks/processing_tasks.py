# app/tasks/processing_tasks.py
import logging
import json
from pathlib import Path
from typing import Dict, Any # 新增 Any
import threading
from datetime import date, timedelta
from zoneinfo import ZoneInfo
import os
import concurrent.futures
import numpy as np # 新增 numpy

from shapely.geometry import Point, Polygon

from app.services.data_fetcher import DataFetcher
# --- 注意：这里我们不再从 chromasky_calculator 导入 _calculate_point_for_map ---
from app.services.chromasky_calculator import score_local_clouds, score_light_path, score_air_quality, score_cloud_altitude
from app.services.astronomy_service import AstronomyService
from app.core.download_config import (
    SUNRISE_CENTER_TIME, SUNSET_CENTER_TIME, WINDOW_MINUTES,
    CALCULATION_LAT_TOP, CALCULATION_LAT_BOTTOM, MapDensity, CALCULATION_DENSITY
)

logger = logging.getLogger("ProcessingTask")

MAIN_MANIFEST_PATH = Path("frontend/gfs/gfs_data_manifest.json")
_manifest_lock = threading.Lock()


# --- 新增：Worker 初始化函数 ---
# 这个全局变量将在每个 worker 进程中被设置
worker_data_fetcher_instance = None

def init_worker():
    """
    此函数在每个 worker 进程启动时运行一次。
    """
    global worker_data_fetcher_instance
    logger.info(f"Worker process {os.getpid()} initializing DataFetcher...")
    # 每个 worker 创建自己的 DataFetcher 实例，并让它在自己的生命周期内保持单例
    worker_data_fetcher_instance = DataFetcher(force_reload=True)
    logger.info(f"Worker process {os.getpid()} initialization complete.")


# --- 新增：为并行化量身定做的任务函数 ---
# 我们将计算逻辑直接移到这里，以使用 worker 的全局 DataFetcher 实例
def _calculate_point_for_map_task(lat: float, lon: float, event: str) -> Dict[str, Any] | None:
    """
    这是一个专为 ProcessPoolExecutor 设计的任务函数。
    它使用在 worker 初始化时创建的全局 DataFetcher 实例。
    """
    if worker_data_fetcher_instance is None:
        logger.error("DataFetcher not initialized in worker process!")
        return None

    # 从 chromasky_calculator._calculate_point_for_map 复制并修改逻辑
    raw_gfs_data = worker_data_fetcher_instance.get_all_variables_for_point(lat, lon, event)
    if not raw_gfs_data or "error" in raw_gfs_data:
        return None

    avg_cloud_path = worker_data_fetcher_instance.get_light_path_avg_cloudiness(lat, lon, event)
    
    factor_a = score_local_clouds(raw_gfs_data.get("high_cloud_cover"), raw_gfs_data.get("medium_cloud_cover"))
    factor_b = score_light_path(avg_cloud_path)
    factor_c = score_air_quality(raw_gfs_data.get("aod"))
    factor_d = score_cloud_altitude(raw_gfs_data.get("cloud_base_height_meters"))
    
    final_score = factor_a * factor_b * factor_c * factor_d * 10
    
    # 返回原始经度（0-360），以便在主进程中统一处理
    return {"lat": lat, "lon": lon, "score": round(final_score, 1)}

# update_gfs_main_manifest 函数保持不变，我们继续使用它
def update_gfs_main_manifest(run_key: str, event_geojson_paths: Dict[str, str], metadata: Dict):
    """
    更新前端使用的Gfs主数据清单，并包含新的元数据。
    """
    logger.info(f"正在更新主数据清单 '{MAIN_MANIFEST_PATH}'...")
    with _manifest_lock:
        try:
            if MAIN_MANIFEST_PATH.exists():
                with open(MAIN_MANIFEST_PATH, 'r') as f:
                    main_manifest = json.load(f)
            else:
                main_manifest = {"latest_run": None, "runs": {}}
            
            main_manifest["runs"][run_key] = {
                "metadata": metadata,
                "events": event_geojson_paths
            }
            main_manifest["latest_run"] = run_key
            
            MAIN_MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(MAIN_MANIFEST_PATH, 'w') as f:
                json.dump(main_manifest, f, indent=2)
            
            logger.info(f"主数据清单已成功更新，最新运行周期为: {run_key}")

        except Exception as e:
            logger.error(f"更新主数据清单时发生错误: {e}", exc_info=True)


# --- 修改 run_geojson_generation_task 函数 ---
def run_geojson_generation_task(manifest_path: Path, run_date: str, run_hour: str) -> None:
    """
    新流程：
    1. 计算天文事件区域 (Polygon)。
    2. 在该区域内，为 GFS 格点计算火烧云指数。
    3. 生成包含指数点 (Point) 的 GeoJSON。
    """
    logger.info("--- [GeoJSON Point-in-Area] 任务启动 ---")
    
    try:
        # 主进程不再需要加载数据，所以我们不再调用 DataFetcher(force_reload=True)
        astronomy_service = AstronomyService()
        shanghai_tz = "Asia/Shanghai"
        today = date.today()
        tomorrow = today + timedelta(days=1)

        
        # --- 定义纬度范围和密度步长 ---
        lat_range = (CALCULATION_LAT_BOTTOM, CALCULATION_LAT_TOP)
        density_to_step = {MapDensity.low: 4, MapDensity.medium: 2, MapDensity.high: 1}
        step = density_to_step[CALCULATION_DENSITY]
        logger.info(f"计算配置 - 纬度范围: {lat_range}, 密度: {CALCULATION_DENSITY.value} (步长: {step})")

        with open(manifest_path, 'r') as f:
            manifest_content = json.load(f)

        run_key = f"{run_date}_t{run_hour}z"
        output_base_dir = Path("frontend/gfs") / run_key
        output_base_dir.mkdir(parents=True, exist_ok=True)
        
        generated_files = {}

        for event_name in manifest_content.keys():
            logger.info(f"--- 开始处理事件: {event_name} ---")

            # --- 步骤 A: 计算天文事件区域 ---
            if "sunrise" in event_name:
                event_type, center_time = "sunrise", SUNRISE_CENTER_TIME
            elif "sunset" in event_name:
                event_type, center_time = "sunset", SUNSET_CENTER_TIME
            else:
                continue
            target_d = tomorrow if "tomorrow" in event_name else today
            logger.info(f"[GeoJSON] 正在计算事件 '{event_type}' on {target_d} 的地理区域...")
            area_geojson = astronomy_service.generate_event_area_geojson(
                event=event_type,
                target_date=target_d,
                center_time_str=center_time,
                window_minutes=WINDOW_MINUTES,
                local_tz_str=shanghai_tz,
                lat_range=lat_range # 传入新的纬度范围
            )
            if "error" in area_geojson or not area_geojson["features"]:
                logger.error(f"无法计算事件 '{event_name}' 的地理区域，跳过。")
                continue
            poly_coords = area_geojson["features"][0]["geometry"]["coordinates"][0]
            event_polygon = Polygon(poly_coords)
            logger.info(f"成功计算地理区域，面积: {event_polygon.area:.2f} (平方度)。")

            # --- 步骤 B: 筛选格点 ---
            # 为了获取格点，我们仍然需要一个 DataFetcher 实例，但这次只在主进程中创建一次。
            # 这个实例仅用于获取坐标，不会在 worker 中使用。
            main_df = DataFetcher(force_reload=True) # 在主进程中加载一次数据
            if event_name not in main_df.gfs_datasets:
                logger.warning(f"事件 '{event_name}' 的 GFS 数据未加载，跳过。")
                continue
             
            gfs_ds = main_df.gfs_datasets[event_name]
            lats_all = gfs_ds.latitude.values
            lons_all = gfs_ds.longitude.values

            # --- 高效筛选格点 ---
            # 1. 先按纬度范围裁剪
            lats_clipped = lats_all[(lats_all >= CALCULATION_LAT_BOTTOM) & (lats_all <= CALCULATION_LAT_TOP)]
            # 2. 再按密度(步长)采样
            lats_sampled = lats_clipped[::step]
            lons_sampled = lons_all[::step]

            logger.info(f"筛选 GFS 格点: 从 {len(lats_all)}x{len(lons_all)} 裁剪采样到 {len(lats_sampled)}x{len(lons_sampled)}")

            # 3. 筛选出在多边形内的点
            points_to_process = []
            for lat in lats_sampled:
                for lon in lons_sampled:
                    lon_180 = lon if lon <= 180 else lon - 360
                    if event_polygon.contains(Point(lon_180, lat)):
                        points_to_process.append((lat, lon))
            
            total_points = len(points_to_process)
            if not total_points:
                logger.warning(f"在计算出的地理区域内没有找到任何GFS格点，跳过事件 '{event_name}'。")
                continue
            
            logger.info(f"在区域内筛选出 {total_points} 个格点，开始并行计算指数...")

            # --- 步骤 C: 使用 initializer 进行并行计算 ---
            features = []
            max_workers = (os.cpu_count() or 1) -1 if (os.cpu_count() or 1) > 1 else 1
            # 在这里使用 initializer
            with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers, initializer=init_worker) as executor:
                future_to_point = {
                    # 调用我们新的、为 worker 设计的任务函数
                    executor.submit(_calculate_point_for_map_task, lat, lon, event_name): (lat, lon)
                    for lat, lon in points_to_process
                }
                
                for future in concurrent.futures.as_completed(future_to_point):
                    try:
                        result = future.result()
                        if result and 'score' in result:
                            lon_180 = result['lon'] if result['lon'] <= 180 else result['lon'] - 360
                            features.append({
                                "type": "Feature",
                                "geometry": {"type": "Point", "coordinates": [lon_180, result['lat']]},
                                "properties": {"score": result["score"]}
                            })
                    except Exception as exc:
                        point = future_to_point[future]
                        logger.error(f"格点 {point} 的计算生成了异常: {exc}", exc_info=True)
            
            # ... (后续的 GeoJSON 生成和保存逻辑保持不变) ...
            logger.info(f"指数计算完成，共生成 {len(features)} 个有效特征点。")
            final_geojson = {
                "type": "FeatureCollection", 
                "features": features,
                "properties": {
                    "event_name": event_name,
                    "center_time_local": center_time,
                    "window_minutes": WINDOW_MINUTES,
                    "density": CALCULATION_DENSITY.value, # 新增
                    "latitude_range": list(lat_range) # 新增
                }
            }
            filename = f"{event_name}.geojson"
            output_path = output_base_dir / filename
            with open(output_path, 'w') as f:
                json.dump(final_geojson, f)
            logger.info(f"成功为事件 '{event_name}' 生成并保存文件: {output_path}")
            relative_path = (Path("gfs") / run_key / filename).as_posix()
            generated_files[event_name] = relative_path
        
        if generated_files:
            metadata = {
                "sunrise_center_time": SUNRISE_CENTER_TIME,
                "sunset_center_time": SUNSET_CENTER_TIME,
                "window_minutes": WINDOW_MINUTES,
                "calculation_lat_top": CALCULATION_LAT_TOP, # 新增
                "calculation_lat_bottom": CALCULATION_LAT_BOTTOM, # 新增
                "density": CALCULATION_DENSITY.value # 新增
            }
            update_gfs_main_manifest(run_key, generated_files, metadata)
        else:
            logger.warning("[GeoJSON] 未生成任何有效的 GeoJSON 文件，跳过主清单更新。")

    except Exception as e:
        logger.error(f"[GeoJSON] 生成地图数据时发生严重错误: {e}", exc_info=True)
    
    logger.info("--- [GeoJSON Point-in-Area] 任务完成 ---")