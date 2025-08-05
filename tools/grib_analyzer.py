# tools/grib_analyzer.py
import argparse
import logging
from datetime import datetime
from pathlib import Path

import cfgrib
import pandas as pd

# --- 日志配置 ---
# 我们将日志级别设为WARNING，以避免cfgrib的普通INFO消息干扰我们的输出
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("GribAnalyzer")


def analyze_grib_file(file_path: Path):
    """
    对指定的GRIB文件进行深入分析，并以清晰的格式打印报告。
    """
    if not file_path.exists():
        print(f"❌ 错误: 文件未找到 -> {file_path}")
        return

    print(f"\n===== 🔍 GRIB 文件分析报告: {file_path.name} =====")

    all_messages_info = []
    try:
        # 使用 cfgrib.FileStream 手动迭代，这是最稳健的方法
        stream = cfgrib.FileStream(str(file_path))
        
        for i, msg in enumerate(stream.items()):
            # msg 是一个 (index, message) 的元组
            message = msg[1]
            
            # 提取关键元数据
            info = {
                "msg_#": i + 1,
                "shortName": message.get("shortName", "N/A"),
                "paramId": message.get("paramId", "N/A"),
                "name": message.get("name", "N/A"),
                "units": message.get("units", "N/A"),
                "level": message.get("level", "N/A"),
                "typeOfLevel": message.get("typeOfLevel", "N/A"),
                "step": f"{message.get('step', 0)}h",
                "time": datetime.strptime(
                    f"{message.get('dataDate', '19000101')}"
                    f"{message.get('dataTime', 0):04d}",
                    "%Y%m%d%H%M",
                ),
            }
            all_messages_info.append(info)

    except Exception as e:
        logger.error(f"分析GRIB文件时发生错误: {e}", exc_info=True)
        print(f"❌ 无法完整分析文件 {file_path.name}。文件可能已损坏或格式不支持。")
        return

    if not all_messages_info:
        print("该文件中未找到任何可识别的GRIB消息。")
        return

    # --- 1. 打印概要信息 ---
    df = pd.DataFrame(all_messages_info)
    file_size_mb = file_path.stat().st_size / (1024 * 1024)
    unique_vars = df["shortName"].unique().tolist()
    unique_levels = df["typeOfLevel"].unique().tolist()
    time_range = (df["time"].min().strftime("%Y-%m-%d %H:%M"), df["time"].max().strftime("%Y-%m-%d %H:%M"))

    print("\n--- 概要信息 ---")
    print(f"  路径       : {file_path.resolve()}")
    print(f"  大小       : {file_size_mb:.2f} MB")
    print(f"  消息总数   : {len(df)}")
    print(f"  包含变量   : {unique_vars}")
    print(f"  层级类型   : {unique_levels}")
    print(f"  时间范围(UTC): {time_range[0]} -> {time_range[1]}")
    print("--------------------")

    # --- 2. 打印详细信息表格 ---
    print("\n--- 详细消息列表 ---")
    # 使用 to_string() 确保所有行和列都被完整打印
    with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 200):
        print(df)
    print("==========================================\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="一个用于分析GRIB文件内容的命令行工具。"
    )
    parser.add_argument(
        "filepath",
        type=str,
        help="要分析的GRIB文件的路径。",
    )

    args = parser.parse_args()
    grib_file = Path(args.filepath)
    
    analyze_grib_file(grib_file)