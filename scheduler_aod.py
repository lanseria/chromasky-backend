# scheduler_aod.py
import cdsapi
import logging
import sys
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.append('app')

from app.core.download_config import SUBREGION_PARAMS, DOWNLOAD_DIR, CAMS_DATASET_NAME, CAMS_DATA_BLOCK

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AOD_Scheduler")

def find_latest_cams_run_date_and_time() -> tuple[str, str]:
    """
    确定最新的、服务器上已存在的 CAMS 00z 运行日期和时间。
    这是获取日期的唯一来源。
    """
    now_utc = datetime.now(timezone.utc)
    logger.info(f"当前系统 UTC 时间: {now_utc.isoformat()}")
    
    # CAMS 00z 数据在 UTC 早上 6-7 点后才稳定可用。
    # 我们设置一个安全的边界，例如 8 点。
    if now_utc.hour < 24:
        # 如果当前时间在 UTC 08:00 之前，那么今天 00z 的数据很可能还没准备好。
        # 因此，我们必须请求前一天的 00z 数据。
        run_date = now_utc - timedelta(days=1)
        logger.info("当前时间早于 UTC 08:00，将请求前一天的 CAMS 00z 数据。")
    else:
        # 如果当前时间在 UTC 08:00 之后，今天 00z 的数据应该是可用的。
        run_date = now_utc
        logger.info("当前时间晚于 UTC 08:00，将请求当天的 CAMS 00z 数据。")
        
    run_date_str = run_date.strftime('%Y-%m-%d')
    run_hour_str = "00:00"
    
    return run_date_str, run_hour_str


def run_aod_download_task():
    """
    下载 CAMS 全球气溶胶预报数据，覆盖未来48小时。
    """
    logger.info("AOD 下载任务启动...")
    
    # 1. 使用单一、可靠的函数获取运行日期和时间
    run_date_str, run_hour_str = find_latest_cams_run_date_and_time()
    
    base_run_time = datetime.strptime(f"{run_date_str} {run_hour_str}", "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
    logger.info(f"最终确定的目标 CAMS 运行周期: {run_date_str} {run_hour_str} UTC")
    
    # 2. 定义需要下载的预报时效
    leadtime_hours_list = [str(h) for h in range(0, 49, 3)]
    logger.info(f"将下载以下预报时效 (小时): {leadtime_hours_list}")
    
    # 3. 调用 CDS API
    try:
        c = cdsapi.Client(timeout=600, quiet=False)
        
        output_dir_name = f"{run_date_str.replace('-', '')}_t{run_hour_str[:2]}z"
        output_dir = DOWNLOAD_DIR / "cams_aod" / output_dir_name
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "aod_forecast.grib"
        
        request_params = {
            'date': f"{run_date_str}/{run_date_str}",
            'time': [run_hour_str],
            'format': 'grib',
            'variable': [CAMS_DATA_BLOCK['variable']],
            'leadtime_hour': leadtime_hours_list,
            'area': [
                SUBREGION_PARAMS["toplat"], SUBREGION_PARAMS["leftlon"],
                SUBREGION_PARAMS["bottomlat"], SUBREGION_PARAMS["rightlon"],
            ],
            'type': 'forecast', # 'type' 参数需要是一个字符串，而不是列表
        }
        
        logger.info("正在向 Copernicus ADS 发送修正后的请求...")
        logger.debug(f"请求参数: {json.dumps(request_params, indent=4)}")

        c.retrieve(CAMS_DATASET_NAME, request_params, output_path)
        
        logger.info(f"CAMS AOD 数据已成功下载到: {output_path}")
        
        # 4. 生成清单文件
        manifest = {
            "base_time_utc": base_run_time.isoformat(),
            "file_path": str(output_path),
            "available_forecast_hours": [int(h) for h in leadtime_hours_list]
        }
        manifest_path = output_dir / "manifest_aod.json"
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=4)
        logger.info(f"AOD 数据清单已成功写入: {manifest_path}")

    except Exception as e:
        logger.error(f"下载 CAMS AOD 数据时发生严重错误: {e}", exc_info=True)

if __name__ == "__main__":
    run_aod_download_task()