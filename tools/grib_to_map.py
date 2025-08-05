# tools/grib_to_map.py (v5 - Final Context Manager Fix)
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

import cfgrib
import numpy as np
import xarray as xr

try:
    from tools.map_drawer import generate_map_from_grid
except ImportError:
    print("错误: 无法从 'tools.map_drawer' 导入。请确保脚本结构正确。")
    exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("GribToMap")


def get_metadata_for_message(grib_path: Path, msg_num: int) -> Dict[str, Any] | None:
    """
    快速扫描GRIB文件，获取指定消息编号的元数据。
    """
    try:
        # --- 关键修复：移除 with 语句 ---
        stream = cfgrib.FileStream(str(grib_path))
        msg_index = msg_num - 1
        if msg_index < 0 or msg_index >= len(stream):
            logger.error(f"错误: 消息编号 {msg_num} 超出范围。文件总共有 {len(stream)} 条消息 (1-{len(stream)})。")
            return None
        
        message = stream[msg_index]
        
        valid_time = datetime.strptime(
            f"{message.get('dataDate', '19000101')}{message.get('dataTime', 0):04d}",
            "%Y%m%d%H%M",
        )

        metadata = {
            "shortName": message.get("shortName", "unknown"),
            "paramId": message.get("paramId", "N/A"),
            "typeOfLevel": message.get("typeOfLevel", "N/A"),
            "level": message.get("level", ""),
            "valid_time": valid_time,
        }
        return metadata
    except Exception as e:
        logger.error(f"获取消息 #{msg_num} 的元数据时出错: {e}")
        return None


def load_full_grib_dataset(grib_path: Path) -> xr.Dataset | None:
    """
    使用最稳健的方法（逐变量加载再合并）加载整个GRIB文件。
    """
    try:
        # --- 关键修复：移除 with 语句 ---
        stream = cfgrib.FileStream(str(grib_path))
        available_vars = sorted(list(set(msg['shortName'] for _, msg in stream.items())))
        
        logger.info(f"在文件中找到的变量: {available_vars}")
        
        datasets_to_merge = []
        for var_name in available_vars:
            try:
                ds_var = xr.open_dataset(
                    grib_path, engine="cfgrib",
                    backend_kwargs={'filter_by_keys': {'shortName': var_name}}
                )
                coords_to_keep = ['time', 'latitude', 'longitude']
                coords_to_drop = [coord for coord in ds_var.coords if coord not in coords_to_keep]
                if coords_to_drop:
                    ds_var = ds_var.drop_vars(coords_to_drop)
                datasets_to_merge.append(ds_var)
            except Exception as e:
                logger.warning(f"加载变量 '{var_name}' 时跳过，原因: {e}")

        if not datasets_to_merge: return None
        
        final_ds = xr.merge(datasets_to_merge)
        return final_ds.load()
    except Exception as e:
        logger.error(f"加载完整GRIB数据集时出错: {e}", exc_info=True)
        return None


def extract_and_draw(grib_path: Path, message_numbers: list[int], output_dir: Path):
    if not grib_path.exists():
        logger.error(f"错误: GRIB文件未找到 -> {grib_path}")
        return

    logger.info(f"===== 开始处理 GRIB 文件: {grib_path.name} =====")
    
    full_dataset = load_full_grib_dataset(grib_path)
    if full_dataset is None:
        logger.error("无法加载GRIB数据集，任务终止。")
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    for msg_num in message_numbers:
        logger.info(f"\n--- 正在处理消息 #{msg_num} ---")
        
        metadata = get_metadata_for_message(grib_path, msg_num)
        if metadata is None:
            continue

        var_name = metadata["shortName"]
        target_time = metadata["valid_time"]

        if var_name not in full_dataset:
            logger.error(f"错误: 变量 '{var_name}' 未能成功加载到数据集中。")
            continue

        try:
            naive_target_time = target_time.replace(tzinfo=None)
            data_to_plot = full_dataset[var_name].sel(time=naive_target_time, method="nearest")
        except Exception as e:
            logger.error(f"无法从数据集中选择数据 (var={var_name}, time={target_time}): {e}")
            continue
            
        map_title = (
            f"Variable: {var_name.upper()} (ID: {metadata['paramId']})\n"
            f"Level: {metadata['level']} {metadata['typeOfLevel']} | Valid Time: {target_time.strftime('%Y-%m-%d %H:%M UTC')}"
        )
        filename = f"{grib_path.stem}_msg{msg_num}_{var_name}.png"
        output_path = output_dir / filename

        if var_name in ['hcc', 'mcc', 'lcc', 'tcc']:
            logger.info("检测到云量数据，将数值乘以100进行可视化。")
            data_to_plot *= 10
            
        generate_map_from_grid(
            score_grid=data_to_plot,
            title=map_title,
            output_path=output_path,
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="将GRIB文件中特定消息可视化为地图的工具。")
    parser.add_argument("filepath", type=str, help="GRIB文件的路径。")
    parser.add_argument("message_numbers", type=int, nargs="+", help="消息编号(从1开始)，可提供多个。")
    parser.add_argument("--output-dir", type=str, default="debug_maps", help="保存地图的目录。")
    args = parser.parse_args()
    print(args.message_numbers)
    extract_and_draw(
        grib_path=Path(args.filepath),
        message_numbers=args.message_numbers,
        output_dir=Path(args.output_dir)
    )