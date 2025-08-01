# scheduler.py
import logging
import sys
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Dict, Literal

# 确保 app 目录在 Python 路径中
sys.path.append('app')

# --- 导入所有需要的服务和配置 ---
# GFS 相关
from app.services.grib_downloader import grib_downloader
# AOD 相关
import cdsapi
from app.core.download_config import SUBREGION_PARAMS, DOWNLOAD_DIR, CAMS_DATASET_NAME, CAMS_DATA_BLOCK

# --- 日志配置 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MasterScheduler")

# 定义类型别名
EventType = Literal["today_sunrise", "today_sunset", "tomorrow_sunrise", "tomorrow_sunset"]


def _calculate_target_times() -> Dict[EventType, datetime]:
    """
    计算出所有四个目标事件的UTC时间。
    这个函数从 data_fetcher.py 移到此处，因为它是一个独立的逻辑。
    """
    shanghai_tz = ZoneInfo("Asia/Shanghai")
    now_shanghai = datetime.now(shanghai_tz)

    today = now_shanghai.date()
    tomorrow = today + timedelta(days=1)

    # 使用一个近似的时间，因为精确的日出日落时间依赖于具体经纬度
    # 这里的目标是确定一个大致的预报时间点 (UTC)
    target_times_shanghai = {
        "today_sunrise": datetime.combine(today, datetime.min.time(), tzinfo=shanghai_tz).replace(hour=6),  # 对应傍晚的霞光
        "today_sunset": datetime.combine(today, datetime.min.time(), tzinfo=shanghai_tz).replace(hour=18),
        "tomorrow_sunrise": datetime.combine(tomorrow, datetime.min.time(), tzinfo=shanghai_tz).replace(hour=6),
        "tomorrow_sunset": datetime.combine(tomorrow, datetime.min.time(), tzinfo=shanghai_tz).replace(hour=18),
    }

    return {name: dt.astimezone(timezone.utc) for name, dt in target_times_shanghai.items()}


# ==============================================================================
# --- GFS 下载任务 ---
# ==============================================================================
def run_gfs_download_task() -> bool:
    """
    负责下载 GFS 数据。
    基于上海时间决定下载哪个运行周期，并为未来的4个事件获取数据。
    返回 True 表示成功执行，False 表示当前时间不在调度窗口内。
    """
    logger.info("--- [GFS] 任务启动 ---")
    shanghai_tz = ZoneInfo("Asia/Shanghai")
    now_shanghai = datetime.now(shanghai_tz)
    
    run_date_utc = None
    run_hour_utc = None
    
    logger.info(f"[GFS] 当前上海时间: {now_shanghai.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 根据调度规则决定目标运行周期
    if 6 <= now_shanghai.hour < 12:
        logger.info("[GFS] 调度窗口：上午，目标为前一天的 18z GFS 运行。")
        target_run_time_shanghai = now_shanghai.replace(hour=6, minute=0, second=0, microsecond=0)
        run_date_utc = (target_run_time_shanghai.astimezone(timezone.utc) - timedelta(days=1)).strftime('%Y%m%d')
        run_hour_utc = "18"
    elif 12 <= now_shanghai.hour < 18:
        logger.info("[GFS] 调度窗口：中午，目标为当天的 00z GFS 运行。")
        target_run_time_shanghai = now_shanghai.replace(hour=12, minute=0, second=0, microsecond=0)
        run_date_utc = target_run_time_shanghai.astimezone(timezone.utc).strftime('%Y%m%d')
        run_hour_utc = "00"
    else:
        logger.info("[GFS] 当前时间不在有效的调度窗口内，任务跳过。")
        return False
    
    target_times_utc = _calculate_target_times()
    
    manifest = {}
    
    for event_name, target_time in target_times_utc.items():
        logger.info(f"[GFS] 开始为事件 '{event_name}' ({target_time.isoformat()}) 下载数据")
        
        run_info = {"date": run_date_utc, "run_hour": run_hour_utc}
        time_meta, file_paths = grib_downloader.get_gfs_data_for_time(run_info, target_time, event_name)
        
        if time_meta and file_paths:
            str_file_paths = {k: str(v) for k, v in file_paths.items() if v}
            manifest[event_name] = {
                "time_meta": time_meta,
                "file_paths": str_file_paths
            }
    
    manifest_path = grib_downloader.download_dir / f"manifest_{run_date_utc}_{run_hour_utc}.json"
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=4)
    logger.info(f"[GFS] GFS 数据清单已成功写入: {manifest_path}")
    logger.info("--- [GFS] 任务完成 ---")
    return True

# ==============================================================================
# --- CAMS AOD 下载任务 (命名更加清晰) ---
# ==============================================================================
def run_cams_aod_download_task() -> bool:
    """
    负责下载 CAMS 全球气溶胶预报数据，覆盖未来48小时。
    返回 True 表示成功，False 表示失败。
    """
    logger.info("--- [CAMS_AOD] 任务启动 ---")
    
    # 1. 确定最新的、服务器上已存在的 CAMS 00z 运行日期和时间
    now_utc = datetime.now(timezone.utc)
    logger.info(f"[CAMS_AOD] 当前系统 UTC 时间: {now_utc.isoformat()}")
    
    # CAMS 00z 数据在 UTC 早上 8 点后才稳定可用。
    if now_utc.hour < 8:
        run_date = now_utc - timedelta(days=1)
        logger.info("[CAMS_AOD] 当前时间早于 UTC 08:00，将请求前一天的 CAMS 00z 数据。")
    else:
        run_date = now_utc
        logger.info("[CAMS_AOD] 当前时间晚于 UTC 08:00，将请求当天的 CAMS 00z 数据。")
        
    run_date_str = run_date.strftime('%Y-%m-%d')
    run_hour_str = "00:00"
    base_run_time = datetime.strptime(f"{run_date_str} {run_hour_str}", "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
    logger.info(f"[CAMS_AOD] 最终确定的目标 CAMS 运行周期: {run_date_str} {run_hour_str} UTC")
    
    # 2. 定义需要下载的预报时效
    leadtime_hours_list = [str(h) for h in range(0, 49, 3)]
    
    # 3. 调用 CDS API
    try:
        c = cdsapi.Client(timeout=600, quiet=False)
        
        output_dir_name = f"{run_date_str.replace('-', '')}_t{run_hour_str[:2]}z"
        output_dir = DOWNLOAD_DIR / "cams_aod" / output_dir_name
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "aod_forecast.grib"
        
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
        
        # 4. 生成清单文件
        manifest = {
            "base_time_utc": base_run_time.isoformat(),
            "file_path": str(output_path),
            "available_forecast_hours": [int(h) for h in leadtime_hours_list]
        }
        manifest_path = output_dir / "manifest_aod.json"
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=4)
        logger.info(f"[CAMS_AOD] AOD 数据清单已成功写入: {manifest_path}")
        logger.info("--- [CAMS_AOD] 任务完成 ---")
        return True

    except Exception as e:
        logger.error(f"[CAMS_AOD] 下载数据时发生严重错误: {e}", exc_info=True)
        logger.info("--- [CAMS_AOD] 任务失败 ---")
        return False

# ==============================================================================
# --- 主执行函数 ---
# ==============================================================================
def main():
    """
    主调度函数，按顺序执行所有数据下载任务。
    """
    logger.info("====== 主调度任务开始 ======")
    
    # 执行 GFS 下载
    try:
        gfs_success = run_gfs_download_task()
        if not gfs_success:
            logger.warning("GFS 下载任务因时间不匹配而被跳过。")
    except Exception as e:
        logger.error(f"执行 GFS 下载任务时发生未捕获的异常: {e}", exc_info=True)
    
    # 执行 CAMS AOD 下载
    try:
        run_cams_aod_download_task()
    except Exception as e:
        logger.error(f"执行 CAMS AOD 下载任务时发生未捕获的异常: {e}", exc_info=True)
        
    logger.info("====== 主调度任务结束 ======")

if __name__ == "__main__":
    main()