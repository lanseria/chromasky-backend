# app/tasks/cams_tasks.py
import logging
import json
import cdsapi
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Tuple, List

# --- 关键改动：从新的共享工具模块导入函数 ---
from .task_utils import get_target_event_times

from app.core.download_config import DOWNLOAD_DIR, CAMS_DATASET_NAME, CAMS_DATA_BLOCK

logger = logging.getLogger("CAMSTask")

def _find_latest_available_cams_run() -> Tuple[datetime.date, str]:
    """
    智能判断当前可用的最新 CAMS 运行周期 (00z 或 12z)。
    CAMS 数据通常有较长的延迟，我们设置一个安全边际。
    """
    now_utc = datetime.now(timezone.utc)
    # CAMS 数据延迟通常比 GFS 长，设置一个 8 小时的安全边际
    safe_margin = timedelta(hours=8)
    
    # 按时间倒序检查可能的运行周期
    potential_runs = [
        (now_utc.date(), "12:00"),
        (now_utc.date(), "00:00"),
        (now_utc.date() - timedelta(days=1), "12:00"),
        (now_utc.date() - timedelta(days=1), "00:00"),
    ]

    for run_date, run_hour in potential_runs:
        run_time_utc = datetime.strptime(f"{run_date.strftime('%Y-%m-%d')} {run_hour}", "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        if (now_utc - run_time_utc) >= safe_margin:
            logger.info(f"[CAMS_AOD] 找到最新的可用运行周期: {run_date.strftime('%Y-%m-%d')} {run_hour} UTC")
            return run_date, run_hour
            
    # 如果都找不到，作为回退，选择最后一个（最旧但最保险的）
    fallback_run = potential_runs[-1]
    logger.warning(f"[CAMS_AOD] 未能通过标准逻辑找到运行周期，回退到: {fallback_run[0].strftime('%Y-%m-%d')} {fallback_run[1]} UTC")
    return fallback_run[0], fallback_run[1]


def run_cams_aod_download_task() -> bool:
    """
    负责下载 CAMS 全球气溶胶预报数据。
    - 自动选择最新的可用运行周期 (00z 或 12z)。
    - 动态计算并只下载覆盖未来日出/日落事件所需的预报时效。
    """
    logger.info("--- [CAMS_AOD] 任务启动 (已优化) ---")
    
    # 1. 智能判断最新的运行周期
    run_date_obj, run_hour_str = _find_latest_available_cams_run()
    run_date_str = run_date_obj.strftime('%Y-%m-%d')
    
    # 构建输出目录和文件路径
    output_dir_name = f"{run_date_obj.strftime('%Y%m%d')}_t{run_hour_str[:2]}z"
    output_dir = DOWNLOAD_DIR / "cams_aod" / output_dir_name
    manifest_path = output_dir / "manifest_aod.json"
    output_path = output_dir / "aod_forecast.grib"

    # 检查文件是否已存在，如果存在则跳过
    if manifest_path.exists() and output_path.exists():
        logger.info(f"[CAMS_AOD] 全球数据和清单文件已在 '{output_dir}' 存在，跳过下载。")
        logger.info("--- [CAMS_AOD] 任务完成 ---")
        return True

    # 2. 动态计算需要的预报时效
    base_run_time = datetime.strptime(f"{run_date_str} {run_hour_str}", "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
    
    # 从共享的工具函数获取目标事件的 UTC 时间
    target_events_utc = get_target_event_times()
    
    leadtime_hours_set = set()
    for event_name, target_time in target_events_utc.items():
        # 只处理未来的事件
        if target_time > base_run_time:
            time_diff = target_time - base_run_time
            # CAMS 预报是按小时的，所以将差异四舍五入到最近的小时
            forecast_hour = round(time_diff.total_seconds() / 3600)
            leadtime_hours_set.add(forecast_hour)

    if not leadtime_hours_set:
        logger.warning("[CAMS_AOD] 计算后没有需要下载的未来预报时效，任务结束。")
        return False

    # 将小时数集合转换为有序的字符串列表，用于 API 请求
    leadtime_hours_list = sorted([str(h) for h in leadtime_hours_set])
    logger.info(f"[CAMS_AOD] 将为 CAMS 运行周期 {run_date_str} {run_hour_str} UTC 下载 {len(leadtime_hours_list)} 个特定预报时效的数据: {leadtime_hours_list}")

    try:
        c = cdsapi.Client(timeout=600, quiet=False)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        request_params = {
            'date': f"{run_date_str}/{run_date_str}",
            'time': run_hour_str,
            'format': 'grib',
            'variable': CAMS_DATA_BLOCK['variable'],
            'leadtime_hour': leadtime_hours_list, # 使用动态计算出的时效列表
            'type': 'forecast',
        }

        logger.info("[CAMS_AOD] 正在向 Copernicus ADS 发送请求以下载全球数据...")
        c.retrieve(CAMS_DATASET_NAME, request_params, str(output_path))
        logger.info(f"[CAMS_AOD] CAMS AOD 全球数据已成功下载到: {output_path}")
        
        # 创建清单文件
        manifest_content = {
            "base_time_utc": base_run_time.isoformat(),
            "file_path": str(output_path),
            "available_forecast_hours": sorted([int(h) for h in leadtime_hours_list]) # 存储实际下载的小时数
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