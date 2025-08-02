# scheduler.py
import logging
import sys

# 确保 app 目录在 Python 路径中，以便能找到 app.tasks
sys.path.append('app')

# --- 从新的任务模块中导入主任务函数 ---
from app.tasks import run_gfs_download_task, run_cams_aod_download_task

# --- 日志配置 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MasterScheduler")

def main():
    """
    主调度函数，按顺序执行所有数据下载和处理任务。
    """
    logger.info("====== 主调度任务开始 ======")
    
    # 执行 CAMS AOD 下载任务
    try:
        run_cams_aod_download_task()
    except Exception as e:
        logger.error(f"执行 CAMS AOD 下载任务时发生未捕获的异常: {e}", exc_info=True)
    
    # 执行 GFS 下载和后续的 GeoJSON 生成任务
    try:
        run_gfs_download_task()
    except Exception as e:
        logger.error(f"执行 GFS 任务流时发生未捕获的异常: {e}", exc_info=True)
    
        
    logger.info("====== 主调度任务结束 ======")

if __name__ == "__main__":
    main()