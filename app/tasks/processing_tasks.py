# app/tasks/processing_tasks.py
import logging
import json
from pathlib import Path
from typing import Dict
import threading

from app.services.data_fetcher import DataFetcher
from app.services.chromasky_calculator import ChromaSkyCalculator, MapDensity

logger = logging.getLogger("ProcessingTask")
# --- START OF CHANGE: 添加主清单文件路径和锁 ---
MAIN_MANIFEST_PATH = Path("frontend/gfs/gfs_data_manifest.json")
_manifest_lock = threading.Lock()
# --- END OF CHANGE ---


# --- START OF CHANGE: 新增主清单更新函数 ---
def update_gfs_main_manifest(run_key: str, event_geojson_paths: Dict[str, str]):
    """
    更新前端使用的Gfs主数据清单。

    Args:
        run_key (str): 当前运行周期的标识符, e.g., "20250802_t00z".
        event_geojson_paths (Dict[str, str]): 事件名到其geojson文件相对路径的映射.
    """
    logger.info(f"正在更新主数据清单 '{MAIN_MANIFEST_PATH}'...")
    with _manifest_lock:
        try:
            # 1. 读取现有清单，如果不存在则创建一个空结构
            if MAIN_MANIFEST_PATH.exists():
                with open(MAIN_MANIFEST_PATH, 'r') as f:
                    main_manifest = json.load(f)
            else:
                main_manifest = {"latest_run": None, "runs": {}}
            
            # 2. 更新或添加当前运行周期的数据
            main_manifest["runs"][run_key] = event_geojson_paths
            
            # 3. 将 'latest_run' 指向最新的运行周期
            main_manifest["latest_run"] = run_key
            
            # 4. 确保目录存在并写回更新后的清单
            MAIN_MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(MAIN_MANIFEST_PATH, 'w') as f:
                json.dump(main_manifest, f, indent=2) # 使用indent=2，方便调试
            
            logger.info(f"主数据清单已成功更新，最新运行周期为: {run_key}")

        except Exception as e:
            logger.error(f"更新主数据清单时发生错误: {e}", exc_info=True)
# --- END OF CHANGE ---


def run_geojson_generation_task(manifest_path: Path, run_date: str, run_hour: str) -> None:
    """
    根据给定的GFS清单文件，为所有事件生成高密度的GeoJSON地图数据，并更新主清单。
    """
    logger.info("--- [GeoJSON] 任务启动 ---")
    logger.info(f"使用清单 '{manifest_path.name}' 生成地图数据。")

    try:
        # 1. 强制重新加载 DataFetcher
        logger.info("[GeoJSON] 正在强制重新加载 DataFetcher...")
        DataFetcher()
        calculator = ChromaSkyCalculator()
        logger.info("[GeoJSON] DataFetcher 和 ChromaSkyCalculator 已准备就绪。")
        
        # 2. 读取清单内容
        with open(manifest_path, 'r') as f:
            manifest_content = json.load(f)

        # 3. 创建输出目录
        run_key = f"{run_date}_t{run_hour}z"
        output_base_dir = Path("frontend/gfs") / run_key
        output_base_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"[GeoJSON] 数据将被保存到: {output_base_dir}")

        # --- START OF CHANGE: 收集生成的GeoJSON路径 ---
        generated_files = {}
        # --- END OF CHANGE ---

        # 4. 遍历清单中的每个事件并生成数据
        for event_name, event_data in manifest_content.items():
            logger.info(f"[GeoJSON] 开始为事件 '{event_name}' 生成地图数据 (high density)...")
            
            if not calculator.data_fetcher.gfs_datasets.get(event_name):
                logger.warning(f"[GeoJSON] 事件 '{event_name}' 的数据在 DataFetcher 中未找到，跳过生成。")
                continue

            geojson_data = calculator.generate_map_data(event=event_name, density=MapDensity.high)
            
            if "error" in geojson_data:
                logger.error(f"[GeoJSON] 为事件 '{event_name}' 生成数据时出错: {geojson_data['error']}")
                continue

            if gfs_info := event_data.get("time_meta"):
                geojson_data["properties"] = {
                    "event": event_name,
                    "density": MapDensity.high.value,
                    **gfs_info
                }
            
            # 5. 保存 GeoJSON 文件
            filename = f"{event_name}.geojson"
            output_path = output_base_dir / filename
            with open(output_path, 'w') as f:
                json.dump(geojson_data, f)
            logger.info(f"[GeoJSON] 成功为事件 '{event_name}' 生成并保存文件: {output_path}")

            # --- START OF CHANGE: 记录文件相对路径 ---
            # 使用 forward slashes for web paths
            relative_path = (Path("gfs") / run_key / filename).as_posix()
            generated_files[event_name] = relative_path
            # --- END OF CHANGE ---

        # --- START OF CHANGE: 如果生成了任何文件，则更新主清单 ---
        if generated_files:
            update_gfs_main_manifest(run_key, generated_files)
        else:
            logger.warning("[GeoJSON] 未生成任何有效的 GeoJSON 文件，跳过主清单更新。")
        # --- END OF CHANGE ---

    except Exception as e:
        logger.error(f"[GeoJSON] 生成地图数据时发生严重错误: {e}", exc_info=True)
    
    logger.info("--- [GeoJSON] 任务完成 ---")