# app/tasks/gfs_tasks.py
import logging
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Dict, Tuple

from app.services.grib_downloader import grib_downloader
from .processing_tasks import run_geojson_generation_task # 相对导入

logger = logging.getLogger("GFSTask")

# 这些函数是 gfs_tasks 的内部实现细节，所以用下划线开头
def _get_target_event_times() -> Dict[str, datetime]:
    """计算所有目标事件的UTC时间（今天/明天的日出/日落）。"""
    shanghai_tz = ZoneInfo("Asia/Shanghai")
    now_shanghai = datetime.now(shanghai_tz)
    
    today = now_shanghai.date()
    tomorrow = today + timedelta(days=1)

    all_events = {
        "today_sunrise": datetime.combine(today, datetime.min.time(), tzinfo=shanghai_tz).replace(hour=6),
        "today_sunset": datetime.combine(today, datetime.min.time(), tzinfo=shanghai_tz).replace(hour=18),
        "tomorrow_sunrise": datetime.combine(tomorrow, datetime.min.time(), tzinfo=shanghai_tz).replace(hour=6),
        "tomorrow_sunset": datetime.combine(tomorrow, datetime.min.time(), tzinfo=shanghai_tz).replace(hour=18),
    }
    
    return {name: dt.astimezone(timezone.utc) for name, dt in all_events.items()}

def _find_latest_available_gfs_run() -> Tuple[str, str]:
    """智能判断当前可用的最新 GFS 运行周期。"""
    now_utc = datetime.now(timezone.utc)
    safe_margin = timedelta(hours=5)
    
    potential_runs = [
        (now_utc.date(), "18"),
        (now_utc.date(), "12"),
        (now_utc.date(), "06"),
        (now_utc.date(), "00"),
        (now_utc.date() - timedelta(days=1), "18")
    ]
    
    for run_date, run_hour in potential_runs:
        run_time_utc = datetime.strptime(f"{run_date.strftime('%Y%m%d')}{run_hour}", "%Y%m%d%H").replace(tzinfo=timezone.utc)
        if (now_utc - run_time_utc) >= safe_margin:
            logger.info(f"找到最新的可用运行周期: {run_date.strftime('%Y%m%d')} {run_hour}z")
            return run_date.strftime('%Y%m%d'), run_hour
            
    fallback_run = potential_runs[-1]
    logger.warning(f"未能通过标准逻辑找到运行周期，回退到: {fallback_run[0].strftime('%Y%m%d')} {fallback_run[1]}z")
    return fallback_run[0].strftime('%Y%m%d'), fallback_run[1]


def run_gfs_download_task() -> bool:
    """
    负责下载 GFS 数据。
    如果对应运行周期的清单已存在，则跳过下载。
    下载成功后，触发 GeoJSON 生成任务。
    """
    logger.info("--- [GFS] 任务启动 ---")
    
    run_date_utc, run_hour_utc = _find_latest_available_gfs_run()
    
    manifest_path = grib_downloader.download_dir / f"manifest_{run_date_utc}_{run_hour_utc}.json"
    if manifest_path.exists():
        logger.info(f"[GFS] 清单文件 '{manifest_path.name}' 已存在，跳过该运行周期的下载。")
        geojson_output_dir = Path("frontend/gfs") / f"{run_date_utc}_t{run_hour_utc}z"
        if not geojson_output_dir.exists() or not any(geojson_output_dir.iterdir()):
            logger.info(f"[GeoJSON] 检测到清单存在但GeoJSON文件缺失，开始补生成...")
            run_geojson_generation_task(manifest_path, run_date_utc, run_hour_utc)
        else:
            logger.info("[GeoJSON] 对应的GeoJSON文件已存在，跳过生成。")
        logger.info("--- [GFS] 任务完成 ---")
        return True

    run_info = {"date": run_date_utc, "run_hour": run_hour_utc}
    target_events_utc = _get_target_event_times()
    logger.info(f"[GFS] 将为以下事件下载数据: {list(target_events_utc.keys())}")
    
    manifest_content = {}
    for event_name, target_time in target_events_utc.items():
        logger.info(f"[GFS] 开始为事件 '{event_name}' ({target_time.isoformat()}) 下载数据")
        
        time_meta, file_paths = grib_downloader.get_gfs_data_for_time(run_info, target_time, event_name)
        
        if time_meta and file_paths and all(file_paths.values()):
            str_file_paths = {k: str(v) for k, v in file_paths.items()}
            manifest_content[event_name] = {
                "time_meta": time_meta,
                "file_paths": str_file_paths
            }
    
    if not manifest_content:
        logger.warning("[GFS] 未能成功为任何事件下载数据，不生成清单和GeoJSON文件。")
        return False
        
    with open(manifest_path, 'w') as f:
        json.dump(manifest_content, f, indent=4)
    logger.info(f"[GFS] GFS 数据清单已成功写入: {manifest_path}")

    # 触发 GeoJSON 生成任务
    run_geojson_generation_task(manifest_path, run_date_utc, run_hour_utc)

    logger.info("--- [GFS] 任务完成 ---")
    return True