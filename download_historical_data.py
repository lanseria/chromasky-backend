# download_historical_data.py (v8 - Robust Time Coord)
import cdsapi
import logging
import argparse
import xarray as xr
from pathlib import Path
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Dict, Set

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("HistoricalDownloader")

from app.core.download_config import LOCAL_TZ, SUNRISE_EVENT_TIMES, SUNSET_EVENT_TIMES, CDS_AREA_EXTRACTION

DATA_DIR = Path("historical_data")

def get_required_utc_dates_and_hours(target_local_date: datetime.date) -> Dict[str, Set[int]]:
    # ... (此函数保持不变) ...
    local_tz = ZoneInfo(LOCAL_TZ)
    all_event_times = SUNRISE_EVENT_TIMES + SUNSET_EVENT_TIMES
    utc_date_hours: Dict[str, Set[int]] = {}

    for time_str in all_event_times:
        local_dt = datetime.combine(target_local_date, datetime.strptime(time_str, '%H:%M').time(), tzinfo=local_tz)
        utc_dt = local_dt.astimezone(timezone.utc)
        
        utc_date_str = utc_dt.strftime('%Y-%m-%d')
        if utc_date_str not in utc_date_hours:
            utc_date_hours[utc_date_str] = set()
        utc_date_hours[utc_date_str].add(utc_dt.hour)
    return utc_date_hours

def download_era5_data(target_local_date: datetime.date):
    # ... (此函数的主体下载逻辑保持不变) ...
    output_dir = DATA_DIR / target_local_date.strftime('%Y-%m-%d') / "era5"
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"--- [ERA5] 开始为本地日期 {target_local_date} 下载数据到 {output_dir} ---")

    final_output_file = output_dir / "era5_data.nc"
    if final_output_file.exists():
        logger.info(f"最终的 NetCDF 文件 '{final_output_file.name}' 已存在，跳过下载。")
        return

    required_utc_info = get_required_utc_dates_and_hours(target_local_date)
    if not required_utc_info:
        logger.warning("未能计算出任何需要下载的UTC日期和小时。")
        return

    years, months, days, hours = set(), set(), set(), set()
    for utc_date_str, hours_set in required_utc_info.items():
        dt_obj = datetime.strptime(utc_date_str, '%Y-%m-%d')
        years.add(str(dt_obj.year))
        months.add(f"{dt_obj.month:02d}")
        days.add(f"{dt_obj.day:02d}")
        for h in hours_set:
            hours.add(f"{h:02}:00")
            
    sorted_years = sorted(list(years))
    sorted_months = sorted(list(months))
    sorted_days = sorted(list(days))
    sorted_hours = sorted(list(hours))
    
    logger.info(f"将为以下参数发起单次下载请求:")
    logger.info(f"  > 年份: {sorted_years}")
    logger.info(f"  > 月份: {sorted_months}")
    logger.info(f"  > 日期: {sorted_days}")
    logger.info(f"  > 小时: {sorted_hours}")

    c = cdsapi.Client(timeout=600, quiet=False, url="https://cds.climate.copernicus.eu/api")
    area_bounds = [CDS_AREA_EXTRACTION[k] for k in ["north", "west", "south", "east"]]
    
    try:
        c.retrieve(
            'reanalysis-era5-single-levels',
            {
                'product_type': 'reanalysis',
                'format': 'netcdf',
                'variable': ["cloud_base_height", "high_cloud_cover", "low_cloud_cover", "medium_cloud_cover", "total_cloud_cover"],
                'year': sorted_years,
                'month': sorted_months,
                'day': sorted_days,
                'time': sorted_hours,
                'area': area_bounds,
            },
            str(final_output_file)
        )
        logger.info(f"✅ 数据已成功下载并保存为 NetCDF 文件: {final_output_file}")
        
        # --- 关键修改：增强验证逻辑 ---
        logger.info("正在验证下载的NetCDF文件...")
        with xr.open_dataset(final_output_file) as ds:
            logger.info("文件可以被 xarray 成功打开。以下是文件结构：")
            # 打印文件结构，这是非常有用的调试信息
            print(ds)

            # 智能查找时间坐标
            time_coord = None
            possible_time_names = ['time', 'valid_time', 't'] # 常见的时间坐标名列表
            for name in possible_time_names:
                if name in ds.coords:
                    time_coord = ds[name]
                    logger.info(f"成功检测到时间坐标为: '{name}'")
                    break
            
            if time_coord is None:
                raise ValueError(f"无法在数据集中找到任何已知的时间坐标 (已尝试: {possible_time_names})。请检查上面打印的文件结构。")

            logger.info(f"包含的变量: {list(ds.data_vars)}")
            # 使用找到的时间坐标来打印时间范围
            logger.info(f"时间范围: {time_coord.min().values} to {time_coord.max().values}")

    except Exception as e:
        logger.error(f"直接下载 NetCDF 时发生错误: {e}", exc_info=True)
        if final_output_file.exists():
            final_output_file.unlink()

def download_gfs_data(target_date: datetime.date):
    pass

if __name__ == "__main__":
    # ... (此部分保持不变) ...
    parser = argparse.ArgumentParser(description="下载指定过去日期的天气再分析数据（直接下载为NetCDF）。")
    parser.add_argument("date", type=str, help="要下载数据的本地日期，格式为 YYYY-MM-DD。")
    args = parser.parse_args()
    try:
        target_date_obj = datetime.strptime(args.date, "%Y-%m-%d").date()
    except ValueError:
        logger.error("日期格式不正确。"); exit(1)
    
    logger.info(f"===== 开始为本地日期 {args.date} 下载所有相关历史数据 =====")
    download_era5_data(target_date_obj)
    download_gfs_data(target_date_obj)
    logger.info(f"===== {args.date} 的历史数据下载任务完成 =====")