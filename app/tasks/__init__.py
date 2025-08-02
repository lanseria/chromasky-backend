# app/tasks/__init__.py

# 让主调度器可以方便地导入核心任务
from .gfs_tasks import run_gfs_download_task
from .cams_tasks import run_cams_aod_download_task