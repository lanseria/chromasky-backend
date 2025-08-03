# app/tasks/cams_tasks.py
import logging
import json
import cdsapi
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.core.download_config import DOWNLOAD_DIR, CAMS_DATASET_NAME, CAMS_DATA_BLOCK

logger = logging.getLogger("CAMSTask")

def run_cams_aod_download_task() -> bool:
    """
    负责下载 CAMS 全球气溶胶预报数据。
    如果对应日期的清单和数据文件已存在，则跳过下载。
    此版本已更新为始终下载全球数据。
    """
    logger.info("--- [CAMS_AOD] 任务启动 ---")
    
    now_utc = datetime.now(timezone.utc)
    logger.info(f"[CAMS_AOD] 当前系统 UTC 时间: {now_utc.isoformat()}")
    
    # 决定请求哪个运行周期的逻辑保持不变
    if now_utc.hour < 8:
        run_date = now_utc - timedelta(days=1)
        logger.info("[CAMS_AOD] 当前时间早于 UTC 08:00，将请求前一天的 CAMS 00z 数据。")
    else:
        run_date = now_utc
        logger.info("[CAMS_AOD] 当前时间晚于 UTC 08:00，将请求当天的 CAMS 00z 数据。")
        
    run_date_str = run_date.strftime('%Y-%m-%d')
    run_hour_str = "00:00"

    # 文件和目录结构逻辑保持不变
    output_dir_name = f"{run_date_str.replace('-', '')}_t{run_hour_str[:2]}z"
    output_dir = DOWNLOAD_DIR / "cams_aod" / output_dir_name
    manifest_path = output_dir / "manifest_aod.json"
    output_path = output_dir / "aod_forecast.grib"

    # 检查文件是否已存在，跳过下载的逻辑保持不变
    if manifest_path.exists() and output_path.exists():
        logger.info(f"[CAMS_AOD] 全球数据和清单文件已在 '{output_dir}' 存在，跳过下载。")
        logger.info("--- [CAMS_AOD] 任务完成 ---")
        return True

    base_run_time = datetime.strptime(f"{run_date_str} {run_hour_str}", "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
    logger.info(f"[CAMS_AOD] 最终确定的目标 CAMS 运行周期: {run_date_str} {run_hour_str} UTC")
    
    leadtime_hours_list = [str(h) for h in range(0, 49, 3)]
    
    try:
        c = cdsapi.Client(timeout=600, quiet=False)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # --- START OF CHANGE ---
        # 构建请求参数，不再包含 'area' 键
        request_params = {
            'date': f"{run_date_str}/{run_date_str}",
            'time': run_hour_str,
            'format': 'grib',
            'variable': CAMS_DATA_BLOCK['variable'],
            'leadtime_hour': leadtime_hours_list,
            'type': 'forecast',
        }

        # 移除原来根据 SUBREGION_PARAMS 添加 area 的逻辑
        logger.info("[CAMS_AOD] 配置为下载全球数据。")
        # --- END OF CHANGE ---

        logger.info("[CAMS_AOD] 正在向 Copernicus ADS 发送请求以下载全球数据...")
        c.retrieve(CAMS_DATASET_NAME, request_params, str(output_path))
        logger.info(f"[CAMS_AOD] CAMS AOD 全球数据已成功下载到: {output_path}")
        
        # 清单文件写入逻辑保持不变
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