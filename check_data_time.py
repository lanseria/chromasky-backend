import xarray as xr
from pathlib import Path
import argparse

def inspect_netcdf_time(file_path: Path):
    if not file_path.exists():
        print(f"❌ 文件不存在: {file_path}")
        return
        
    try:
        print(f"\n--- 正在检查文件: {file_path.name} ---")
        ds = xr.open_dataset(file_path)
        
        # 动态寻找时间坐标
        time_coord_name = None
        possible_time_names = ['time', 'valid_time', 't']
        for name in possible_time_names:
            if name in ds.coords:
                time_coord_name = name
                break

        if time_coord_name:
            print(f"✅ 找到时间坐标: '{time_coord_name}'")
            print("文件中包含的UTC时间点:")
            # 打印所有时间点
            for t in ds[time_coord_name].values:
                print(f"  - {t}")
        else:
            print(f"⚠️ 未能在此文件中找到任何已知的时间坐标。")
            print(f"   文件包含的坐标有: {list(ds.coords)}")

    except Exception as e:
        print(f"读取文件时出错: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="检查NetCDF文件中的时间坐标。")
    parser.add_argument("date", type=str, help="目标日期 (YYYY-MM-DD)")
    args = parser.parse_args()

    # 检查 ERA5 数据
    era5_file = Path("historical_data") / args.date / "era5" / "era5_data.nc"
    inspect_netcdf_time(era5_file)

    # 检查 CAMS AOD 数据
    # cams_file = Path("forecast_data") / args.date / "cams" / "cams_aod_data.nc"
    # inspect_netcdf_time(cams_file)