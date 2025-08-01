# scheduler.py
import logging
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import json

from app.services.grib_downloader import grib_downloader
from app.services.data_fetcher import DataFetcher

# 确保 app 目录在 Python 路径中
sys.path.append('app')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Scheduler")

def run_download_task():
    """
    这个函数将被定时任务（如 cron）每天调用两次。
    它会根据当前上海时间，决定要下载哪个GFS运行周期的数据。
    """
    shanghai_tz = ZoneInfo("Asia/Shanghai")
    now_shanghai = datetime.now(shanghai_tz)
    
    run_date_utc = None
    run_hour_utc = None
    
    logger.info(f"任务启动，当前上海时间: {now_shanghai.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 根据调度规则决定目标运行周期
    if 6 <= now_shanghai.hour < 12:
        # 在上海时间 06:00 - 12:00 之间运行，目标是前一天的 18z
        logger.info("调度窗口：上午，目标为前一天的 18z GFS 运行。")
        target_run_time_shanghai = now_shanghai.replace(hour=6, minute=0, second=0, microsecond=0)
        run_date_utc = (target_run_time_shanghai.astimezone(timezone.utc) - timedelta(days=1)).strftime('%Y%m%d')
        run_hour_utc = "18"
    elif 12 <= now_shanghai.hour < 18:
        # 在上海时间 12:00 - 18:00 之间运行，目标是当天的 00z
        logger.info("调度窗口：中午，目标为当天的 00z GFS 运行。")
        target_run_time_shanghai = now_shanghai.replace(hour=12, minute=0, second=0, microsecond=0)
        run_date_utc = target_run_time_shanghai.astimezone(timezone.utc).strftime('%Y%m%d')
        run_hour_utc = "00"
    else:
        logger.info("当前时间不在有效的调度窗口内，任务退出。")
        return
    
    temp_fetcher = DataFetcher(load_data=False)
    target_times_utc = temp_fetcher._calculate_target_times()
    
    manifest = {}
    
    for event_name, target_time in target_times_utc.items():
        logger.info(f"--- 开始为事件 '{event_name}' ({target_time.isoformat()}) 下载数据 ---")
        
        # 1. 构造 run_info (与之前版本可能略有不同，确保其正确)
        run_info = {"date": run_date_utc, "run_hour": run_hour_utc}
        
        # 2. 将 event_name 传递给下载器
        time_meta, file_paths = grib_downloader.get_gfs_data_for_time(run_info, target_time, event_name)
        
        if time_meta and file_paths:
            str_file_paths = {k: str(v) for k, v in file_paths.items() if v}
            manifest[event_name] = {
                "time_meta": time_meta,
                "file_paths": str_file_paths
            }
    
    # 将清单写入文件 (现在保存在根 grib_data 目录下)
    manifest_path = grib_downloader.download_dir / f"manifest_{run_date_utc}_{run_hour_utc}.json"
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=4)
    logger.info(f"数据清单已成功写入: {manifest_path}")

if __name__ == "__main__":
    run_download_task()