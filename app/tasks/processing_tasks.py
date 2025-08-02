# app/tasks/processing_tasks.py
import logging
import json
from pathlib import Path

from app.services.data_fetcher import DataFetcher
from app.services.chromasky_calculator import ChromaSkyCalculator, MapDensity

logger = logging.getLogger("ProcessingTask")

def run_geojson_generation_task(manifest_path: Path, run_date: str, run_hour: str) -> None:
    """
    根据给定的GFS清单文件，为所有事件生成高密度的GeoJSON地图数据。
    """
    logger.info("--- [GeoJSON] 任务启动 ---")
    logger.info(f"使用清单 '{manifest_path.name}' 生成地图数据。")

    try:
        # 1. 强制重新加载 DataFetcher 以确保它使用我们刚刚下载的最新数据
        logger.info("[GeoJSON] 正在强制重新加载 DataFetcher...")
        DataFetcher(force_reload=True)
        calculator = ChromaSkyCalculator()
        logger.info("[GeoJSON] DataFetcher 和 ChromaSkyCalculator 已准备就绪。")
        
        # 2. 读取清单内容
        with open(manifest_path, 'r') as f:
            manifest_content = json.load(f)

        # 3. 创建输出目录
        output_base_dir = Path("frontend/gfs") / f"{run_date}_t{run_hour}z"
        output_base_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"[GeoJSON] 数据将被保存到: {output_base_dir}")

        # 4. 遍历清单中的每个事件并生成数据
        for event_name, event_data in manifest_content.items():
            logger.info(f"[GeoJSON] 开始为事件 '{event_name}' 生成地图数据 (high density)...")
            
            # 检查数据是否真的可用，避免计算器出错
            if not calculator.data_fetcher.gfs_datasets.get(event_name):
                logger.warning(f"[GeoJSON] 事件 '{event_name}' 的数据在 DataFetcher 中未找到，跳过生成。")
                continue

            geojson_data = calculator.generate_map_data(event=event_name, density=MapDensity.high)
            
            if "error" in geojson_data:
                logger.error(f"[GeoJSON] 为事件 '{event_name}' 生成数据时出错: {geojson_data['error']}")
                continue

            # 添加元数据到 GeoJSON 的 properties 字段
            if gfs_info := event_data.get("time_meta"):
                geojson_data["properties"] = {
                    "event": event_name,
                    "density": MapDensity.high.value,
                    **gfs_info
                }
            
            # 5. 保存 GeoJSON 文件
            output_path = output_base_dir / f"{event_name}.geojson"
            with open(output_path, 'w') as f:
                json.dump(geojson_data, f) # 不使用 indent 以减小文件大小
            logger.info(f"[GeoJSON] 成功为事件 '{event_name}' 生成并保存文件: {output_path}")

    except Exception as e:
        logger.error(f"[GeoJSON] 生成地图数据时发生严重错误: {e}", exc_info=True)
    
    logger.info("--- [GeoJSON] 任务完成 ---")