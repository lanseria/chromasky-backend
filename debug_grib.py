# debug_grib.py
import xarray as xr
import pandas as pd
import argparse
import sys
from pathlib import Path

def analyze_grib_file(file_path: Path):
    """
    分析单个GRIB文件的内容，并以清晰的表格形式打印出其结构。
    """
    if not file_path.exists():
        print(f"错误: 文件未找到 -> {file_path}")
        return

    print(f"--- 正在探查 GRIB 文件: {file_path.name} ---")

    all_vars_info = []

    try:
        # 尝试一次性加载整个文件，以捕获 multiple keys 错误
        # backend_kwargs={} 确保我们不应用任何默认过滤器
        xr.open_dataset(file_path, engine="cfgrib", backend_kwargs={})
        
        # 如果没有错误，说明文件结构很简单，可以直接分析
        print("文件结构简单，直接分析...")
        ds = xr.open_dataset(file_path, engine="cfgrib")
        for var_name in ds.data_vars:
            info = {
                "variable": var_name,
                "dims": list(ds[var_name].dims),
                "coords": list(ds[var_name].coords.keys()),
            }
            # 尝试获取层级类型
            if 'typeOfLevel' in ds[var_name].attrs:
                info['level_type'] = ds[var_name].attrs['typeOfLevel']
            else:
                # 检查坐标中是否有层级信息
                for coord in ['typeOfLevel', 'stepType', 'isobaricInhPa']:
                    if coord in ds[var_name].coords:
                        info.setdefault('coords_info', []).append(coord)

            all_vars_info.append(info)

    except ValueError as e:
        # 捕获 "multiple values for unique key" 错误
        if "multiple values for unique key" in str(e):
            print("文件包含多个层级，正在逐层分析...")
            
            # 从错误信息中智能提取可能的 key 和 values
            # 这是一个简化的解析，适用于 cfgrib 的典型错误消息
            try:
                key_to_filter = str(e).split("filter_by_keys={'")[1].split("':")[0]
                print(f"检测到冲突键: '{key_to_filter}'。将按此键进行分组。")
                
                # 我们需要一种方法来获取所有可能的值，这有点棘手
                # 暂时使用一个常见的列表
                possible_values = [
                    'surface', 'heightAboveGround', 'atmosphere', 'entireAtmosphere',
                    'isobaricInhPa', 'lowCloudLayer', 'middleCloudLayer', 'highCloudLayer',
                    'cloudCeiling', 'instant', 'avg'
                ]

                for value in possible_values:
                    try:
                        ds = xr.open_dataset(
                            file_path,
                            engine="cfgrib",
                            backend_kwargs={'filter_by_keys': {key_to_filter: value}}
                        )
                        for var_name in ds.data_vars:
                            all_vars_info.append({
                                'variable': var_name,
                                'level_type/key': f"{key_to_filter} = {value}"
                            })
                    except (ValueError, KeyError, OSError):
                        # 忽略加载失败的层级
                        continue
            except IndexError:
                print("无法从错误消息中自动解析冲突键。")

        else:
            print(f"\n读取GRIB文件时发生未知错误: {e}")
            return
            
    except Exception as e:
        print(f"\n读取GRIB文件时发生未知错误: {e}")
        return

    # 打印最终的总结表格
    print("\n\n--- GRIB 文件内容总结 ---")
    if all_vars_info:
        df = pd.DataFrame(all_vars_info).fillna('') # 用空字符串填充 NaN
        print("下表展示了文件中的变量及其层级/坐标信息:")
        print(df.to_string())
    else:
        print("未能从文件中解析出任何变量。文件可能为空或已损坏。")


if __name__ == "__main__":
    # --- 设置命令行参数解析 ---
    parser = argparse.ArgumentParser(description="分析GRIB文件的内容和结构。")
    parser.add_argument(
        "filepath",
        type=str,
        help="要分析的GRIB文件的路径。例如: grib_data/20231027_t12z/today_sunset_f006/total_cloud.grib2"
    )
    
    # 检查是否提供了参数
    if len(sys.argv) < 2:
        print("错误: 请提供要分析的GRIB文件的路径。")
        print("用法: python debug_grib.py <文件路径>")
        # 打印一个示例用法，引导用户
        example_path = Path("grib_data/").rglob("*.grib2")
        try:
            first_grib = next(example_path)
            print(f"\n示例: python debug_grib.py {first_grib}")
        except StopIteration:
            print("\n未在 'grib_data/' 目录下找到任何 .grib2 文件。")
        sys.exit(1)
        
    args = parser.parse_args()
    
    grib_file = Path(args.filepath)
    analyze_grib_file(grib_file)