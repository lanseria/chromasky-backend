# app/tasks/processing_tasks.py
import logging
import json
from pathlib import Path
from typing import Dict, Any, Tuple
import threading
from datetime import date, timedelta
from zoneinfo import ZoneInfo
import os
import concurrent.futures

from shapely.geometry import Point, Polygon

from app.services.data_fetcher import DataFetcher
# --- 优化：直接从 calculator 导入评分函数 ---
from app.services.chromasky_calculator import score_local_clouds, score_light_path, score_air_quality, score_cloud_altitude
from app.services.astronomy_service import AstronomyService
from app.core.download_config import (
    SUNRISE_CENTER_TIME, SUNSET_CENTER_TIME, WINDOW_MINUTES,
    CALCULATION_LAT_TOP, CALCULATION_LAT_BOTTOM, MapDensity, CALCULATION_DENSITY
)

logger = logging.getLogger("ProcessingTask")

MAIN_MANIFEST_PATH = Path("frontend/gfs/gfs_data_manifest.json")
_manifest_lock = threading.Lock()


# --- 优化第一步：创建一个轻量级的、纯计算的 worker 函数 ---
# 这个函数在模块的顶层定义，以便被子进程正确地序列化（pickle）
# 它只接收纯数据，不进行任何 I/O 或调用 DataFetcher
def _worker_calculate_score(
    lat: float, 
    lon: float, 
    raw_gfs_data: Dict[str, Any], 
    avg_cloud_path: float | None
) -> Dict[str, Any] | None:
    """
    一个纯粹的计算工作函数，专为并行化设计。
    它接收预先提取好的数据，并返回计算结果。
    """
    if not raw_gfs_data or "error" in raw_gfs_data:
        return None

    try:
        factor_a = score_local_clouds(raw_gfs_data.get("high_cloud_cover"), raw_gfs_data.get("medium_cloud_cover"))
        factor_b = score_light_path(avg_cloud_path)
        factor_c = score_air_quality(raw_gfs_data.get("aod"))
        factor_d = score_cloud_altitude(raw_gfs_data.get("cloud_base_height_meters"))
        
        final_score = factor_a * factor_b * factor_c * factor_d * 10
        
        return {"lat": lat, "lon": lon, "score": round(final_score, 1)}
    except Exception as e:
        logger.error(f"Worker在计算点 ({lat}, {lon}) 时出错: {e}", exc_info=True)
        return None


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


# --- 优化第二步：修改主任务函数 ---
def run_geojson_generation_task(manifest_path: Path, run_date: str, run_hour: str) -> None:
    """
    新流程（已优化）：
    1. 在主进程中加载一次所有数据。
    2. 计算天文事件区域 (Polygon)。
    3. 在该区域内，筛选出 GFS 格点。
    4. 在主进程中为每个格点提取所需原始数据。
    5. 将轻量级的原始数据和坐标发送到工作进程进行纯计算。
    6. 收集结果并生成 GeoJSON。
    """
    logger.info("--- [GeoJSON Point-in-Area] 任务启动 ---")
    
    try:
        # --- 优化：在主进程中加载一次数据 ---
        logger.info("[GeoJSON] 正在主进程中加载所有数据...")
        main_df = DataFetcher(force_reload=True)
        astronomy_service = AstronomyService()
        logger.info("[GeoJSON] 数据加载完成。")
        
        shanghai_tz = "Asia/Shanghai"
        today = date.today()
        tomorrow = today + timedelta(days=1)
        
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

            if event_name not in main_df.gfs_datasets:
                logger.warning(f"事件 '{event_name}' 的 GFS 数据未在DataFetcher中加载，跳过。")
                continue

            # 步骤 A: 计算天文事件区域 (逻辑不变)
            if "sunrise" in event_name:
                event_type, center_time = "sunrise", SUNRISE_CENTER_TIME
            elif "sunset" in event_name:
                event_type, center_time = "sunset", SUNSET_CENTER_TIME
            else: continue
            target_d = tomorrow if "tomorrow" in event_name else today
            area_geojson = astronomy_service.generate_event_area_geojson(
                event=event_type, target_date=target_d, center_time_str=center_time,
                window_minutes=WINDOW_MINUTES, local_tz_str=shanghai_tz, lat_range=lat_range
            )
            if "error" in area_geojson or not area_geojson["features"]:
                logger.error(f"无法计算事件 '{event_name}' 的地理区域，跳过。")
                continue
            event_polygon = Polygon(area_geojson["features"][0]["geometry"]["coordinates"][0])
            logger.info(f"成功计算地理区域，面积: {event_polygon.area:.2f} (平方度)。")

            # 步骤 B & C: 筛选格点并准备计算任务 (逻辑不变)
            gfs_ds = main_df.gfs_datasets[event_name]
            lats_all, lons_all = gfs_ds.latitude.values, gfs_ds.longitude.values
            lats_clipped = lats_all[(lats_all >= CALCULATION_LAT_BOTTOM) & (lats_all <= CALCULATION_LAT_TOP)]
            lats_sampled, lons_sampled = lats_clipped[::step], lons_all[::step]
            
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
            
            # --- 优化第三步：在主进程中准备好所有 worker 的输入数据 ---
            logger.info(f"在区域内筛选出 {total_points} 个格点，正在准备计算任务...")
            tasks_for_workers: list[Tuple] = []
            for lat, lon in points_to_process:
                # 在主进程中执行所有数据提取操作
                raw_data = main_df.get_all_variables_for_point(lat, lon, event_name)
                avg_cloud = main_df.get_light_path_avg_cloudiness(lat, lon, event_name)
                tasks_for_workers.append((lat, lon, raw_data, avg_cloud))

            # --- 优化第四步：使用新的并行计算模型 ---
            logger.info("任务准备完毕，开始并行计算指数...")
            features = []
            max_workers = (os.cpu_count() or 1) -1 if (os.cpu_count() or 1) > 1 else 1
            # 移除 initializer，因为 worker 不再需要初始化任何东西
            with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
                # 将准备好的轻量级任务提交给 worker
                future_to_point = {
                    executor.submit(_worker_calculate_score, *task_args): task_args[:2] # 映射到 (lat, lon)
                    for task_args in tasks_for_workers
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
                        logger.error(f"格点 {point} 的计算在主进程收集结果时生成了异常: {exc}", exc_info=True)
            
            # (后续的 GeoJSON 生成和保存逻辑保持不变)
            logger.info(f"指数计算完成，共生成 {len(features)} 个有效特征点。")
            final_geojson = {
                "type": "FeatureCollection", 
                "features": features,
                "properties": { "event_name": event_name, "center_time_local": center_time, "window_minutes": WINDOW_MINUTES, "density": CALCULATION_DENSITY.value, "latitude_range": list(lat_range) }
            }
            filename = f"{event_name}.geojson"
            output_path = output_base_dir / filename
            with open(output_path, 'w') as f:
                json.dump(final_geojson, f)
            logger.info(f"成功为事件 '{event_name}' 生成并保存文件: {output_path}")
            relative_path = (Path("gfs") / run_key / filename).as_posix()
            generated_files[event_name] = relative_path
        
        if generated_files:
            metadata = { "sunrise_center_time": SUNRISE_CENTER_TIME, "sunset_center_time": SUNSET_CENTER_TIME, "window_minutes": WINDOW_MINUTES, "calculation_lat_top": CALCULATION_LAT_TOP, "calculation_lat_bottom": CALCULATION_LAT_BOTTOM, "density": CALCULATION_DENSITY.value }
            update_gfs_main_manifest(run_key, generated_files, metadata)
        else:
            logger.warning("[GeoJSON] 未生成任何有效的 GeoJSON 文件，跳过主清单更新。")

    except Exception as e:
        logger.error(f"[GeoJSON] 生成地图数据时发生严重错误: {e}", exc_info=True)
    
    logger.info("--- [GeoJSON Point-in-Area] 任务完成 ---")