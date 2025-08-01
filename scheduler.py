# scheduler.py
import logging
import sys
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Dict, Literal, Tuple

# 确保 app 目录在 Python 路径中
sys.path.append('app')

# --- 导入所有需要的服务和配置 ---
from app.services.grib_downloader import grib_downloader
import cdsapi
from app.core.download_config import SUBREGION_PARAMS, DOWNLOAD_DIR, CAMS_DATASET_NAME, CAMS_DATA_BLOCK

# --- 日志配置 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MasterScheduler")

# 定义类型别名
EventType = Literal["today_sunrise", "today_sunset", "tomorrow_sunrise", "tomorrow_sunset"]

def _get_target_event_times() -> Dict[EventType, datetime]:
    """
    计算所有目标事件的UTC时间（今天/明天的日出/日落）。
    不再根据当前时间进行过滤，总是获取全部4个事件。
    """
    shanghai_tz = ZoneInfo("Asia/Shanghai")
    now_shanghai = datetime.now(shanghai_tz)
    
    today = now_shanghai.date()
    tomorrow = today + timedelta(days=1)

    # 定义所有需要下载数据的事件时间（上海时区）
    all_events = {
        "today_sunrise": datetime.combine(today, datetime.min.time(), tzinfo=shanghai_tz).replace(hour=6),
        "today_sunset": datetime.combine(today, datetime.min.time(), tzinfo=shanghai_tz).replace(hour=18),
        "tomorrow_sunrise": datetime.combine(tomorrow, datetime.min.time(), tzinfo=shanghai_tz).replace(hour=6),
        "tomorrow_sunset": datetime.combine(tomorrow, datetime.min.time(), tzinfo=shanghai_tz).replace(hour=18),
    }
    
    # 将结果转换为 UTC 时间
    return {name: dt.astimezone(timezone.utc) for name, dt in all_events.items()}

def _get_future_event_times() -> Dict[EventType, datetime]:
    """
    计算所有未来事件的UTC时间。
    如果一个事件在当前时间之前，则不包含在内。
    """
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

    future_events = {name: dt for name, dt in all_events.items() if dt > now_shanghai}
    return {name: dt.astimezone(timezone.utc) for name, dt in future_events.items()}


def _find_latest_available_gfs_run() -> Tuple[str, str]:
    """
    智能判断当前可用的最新 GFS 运行周期。
    """
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
            logger.info(f"[GFS] 找到最新的可用运行周期: {run_date.strftime('%Y%m%d')} {run_hour}z")
            return run_date.strftime('%Y%m%d'), run_hour
            
    fallback_run = potential_runs[-1]
    logger.warning(f"[GFS] 未能通过标准逻辑找到运行周期，回退到: {fallback_run[0].strftime('%Y%m%d')} {fallback_run[1]}z")
    return fallback_run[0].strftime('%Y%m%d'), fallback_run[1]

# ==============================================================================
# --- GFS 下载任务 ---
# ==============================================================================
def run_gfs_download_task() -> bool:
    """
    负责下载 GFS 数据。
    如果对应运行周期的清单已存在，则跳过下载。
    """
    logger.info("--- [GFS] 任务启动 ---")
    
    run_date_utc, run_hour_utc = _find_latest_available_gfs_run()
    
    manifest_path = grib_downloader.download_dir / f"manifest_{run_date_utc}_{run_hour_utc}.json"
    if manifest_path.exists():
        logger.info(f"[GFS] 清单文件 '{manifest_path.name}' 已存在，跳过该运行周期的下载。")
        logger.info("--- [GFS] 任务完成 ---")
        return True

    run_info = {"date": run_date_utc, "run_hour": run_hour_utc}
    
    # --- START OF CHANGE ---
    # 调用新的、不过滤的函数
    target_events_utc = _get_target_event_times()
    # --- END OF CHANGE ---
    
    logger.info(f"[GFS] 将为以下事件下载数据: {list(target_events_utc.keys())}")
    
    manifest_content = {}
    
    # --- START OF CHANGE ---
    # 循环变量名修改以提高可读性
    for event_name, target_time in target_events_utc.items():
    # --- END OF CHANGE ---
        logger.info(f"[GFS] 开始为事件 '{event_name}' ({target_time.isoformat()}) 下载数据")
        
        time_meta, file_paths = grib_downloader.get_gfs_data_for_time(run_info, target_time, event_name)
        
        if time_meta and file_paths and all(file_paths.values()):
            str_file_paths = {k: str(v) for k, v in file_paths.items()}
            manifest_content[event_name] = {
                "time_meta": time_meta,
                "file_paths": str_file_paths
            }
    
    if not manifest_content:
        logger.warning("[GFS] 未能成功为任何事件下载数据，不生成清单文件。")
        return False
        
    with open(manifest_path, 'w') as f:
        json.dump(manifest_content, f, indent=4)
    logger.info(f"[GFS] GFS 数据清单已成功写入: {manifest_path}")
    logger.info("--- [GFS] 任务完成 ---")
    return True

# ==============================================================================
# --- CAMS AOD 下载任务 ---
# ==============================================================================
def run_cams_aod_download_task() -> bool:
    """
    负责下载 CAMS 全球气溶胶预报数据。
    如果对应日期的清单和数据文件已存在，则跳过下载。
    """
    logger.info("--- [CAMS_AOD] 任务启动 ---")
    
    now_utc = datetime.now(timezone.utc)
    logger.info(f"[CAMS_AOD] 当前系统 UTC 时间: {now_utc.isoformat()}")
    
    if now_utc.hour < 8:
        run_date = now_utc - timedelta(days=1)
        logger.info("[CAMS_AOD] 当前时间早于 UTC 08:00，将请求前一天的 CAMS 00z 数据。")
    else:
        run_date = now_utc
        logger.info("[CAMS_AOD] 当前时间晚于 UTC 08:00，将请求当天的 CAMS 00z 数据。")
        
    run_date_str = run_date.strftime('%Y-%m-%d')
    run_hour_str = "00:00"

    # --- START OF CHANGE: 检查是否已下载 ---
    output_dir_name = f"{run_date_str.replace('-', '')}_t{run_hour_str[:2]}z"
    output_dir = DOWNLOAD_DIR / "cams_aod" / output_dir_name
    manifest_path = output_dir / "manifest_aod.json"
    output_path = output_dir / "aod_forecast.grib"

    if manifest_path.exists() and output_path.exists():
        logger.info(f"[CAMS_AOD] 数据和清单文件已在 '{output_dir}' 存在，跳过下载。")
        logger.info("--- [CAMS_AOD] 任务完成 ---")
        return True
    # --- END OF CHANGE ---

    base_run_time = datetime.strptime(f"{run_date_str} {run_hour_str}", "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
    logger.info(f"[CAMS_AOD] 最终确定的目标 CAMS 运行周期: {run_date_str} {run_hour_str} UTC")
    
    leadtime_hours_list = [str(h) for h in range(0, 49, 3)]
    
    try:
        c = cdsapi.Client(timeout=600, quiet=False)
        output_dir.mkdir(parents=True, exist_ok=True) # 确保目录存在
        
        request_params = {
            'date': f"{run_date_str}/{run_date_str}",
            'time': run_hour_str,
            'format': 'grib',
            'variable': CAMS_DATA_BLOCK['variable'],
            'leadtime_hour': leadtime_hours_list,
            'area': [
                SUBREGION_PARAMS["toplat"], SUBREGION_PARAMS["leftlon"],
                SUBREGION_PARAMS["bottomlat"], SUBREGION_PARAMS["rightlon"],
            ],
            'type': 'forecast',
        }
        
        logger.info("[CAMS_AOD] 正在向 Copernicus ADS 发送请求...")
        c.retrieve(CAMS_DATASET_NAME, request_params, output_path)
        logger.info(f"[CAMS_AOD] CAMS AOD 数据已成功下载到: {output_path}")
        
        manifest_content = {
            "base_time_utc": base_run_time.isoformat(),
            "file_path": str(output_path),
            "available_forecast_hours": [int(h) for h in leadtime_hours_list]
        }
        with open(manifest_path, 'w') as f:
            json.dump(manifest_content, f, indent=4)
        logger.info(f"[CAMS_AOD] AOD 数据清单已成功写入: {manifest_path}")
        logger.info("--- [CAMS_AOD] 任务完成 ---")
        return True

    except Exception as e:
        logger.error(f"[CAMS_AOD] 下载数据时发生严重错误: {e}", exc_info=True)
        logger.info("--- [CAMS_AOD] 任务失败 ---")
        return False

# ==============================================================================
# --- 主执行函数 (保持不变) ---
# ==============================================================================
def main():
    """
    主调度函数，按顺序执行所有数据下载任务。
    """
    logger.info("====== 主调度任务开始 ======")
    
    try:
        run_gfs_download_task()
    except Exception as e:
        logger.error(f"执行 GFS 下载任务时发生未捕获的异常: {e}", exc_info=True)
    
    try:
        run_cams_aod_download_task()
    except Exception as e:
        logger.error(f"执行 CAMS AOD 下载任务时发生未捕AOD的异常: {e}", exc_info=True)
        
    logger.info("====== 主调度任务结束 ======")

if __name__ == "__main__":
    main()