# download_historical_data.py (v5 - Correct Cross-Day Downloading)
import cdsapi
import logging
import argparse
from pathlib import Path
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Dict, Set

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("HistoricalDownloader")

try:
    from app.core.download_config import LOCAL_TZ, SUNRISE_EVENT_TIMES, SUNSET_EVENT_TIMES, CDS_AREA_EXTRACTION
except ImportError:
    # Fallback for standalone execution
    LOCAL_TZ, SUNRISE_EVENT_TIMES, SUNSET_EVENT_TIMES = "Asia/Shanghai", ["04:00", "05:00", "06:00", "07:00", "08:00"], ["19:00", "20:00", "21:00", "22:00"]
    CDS_AREA_EXTRACTION = {"north": 54.0, "south": 0.0, "west": 70.0, "east": 135.0}

DATA_DIR = Path("historical_data")

def get_required_utc_dates_and_hours(target_local_date: datetime.date) -> Dict[str, Set[int]]:
    """
    计算所有需要下载的UTC日期及其对应的小时集合。
    """
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
    """
    为本地日期下载所有相关的ERA5数据，能处理跨UTC天的情况。
    """
    output_dir = DATA_DIR / target_local_date.strftime('%Y-%m-%d') / "era5"
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"--- [ERA5] 开始为本地日期 {target_local_date} 下载数据到 {output_dir} ---")

    required_utc_info = get_required_utc_dates_and_hours(target_local_date)
    if not required_utc_info:
        logger.warning("未能计算出任何需要下载的UTC日期和小时。")
        return

    c = cdsapi.Client(timeout=600, quiet=True)
    area_bounds = [CDS_AREA_EXTRACTION[k] for k in ["north", "west", "south", "east"]]

    # 为每个需要的UTC日期发起下载
    for utc_date_str, hours_set in required_utc_info.items():
        dt_obj = datetime.strptime(utc_date_str, '%Y-%m-%d')
        hours_str_list = sorted([f'{h:02}:00' for h in hours_set])
        
        # 将文件命名为 YYYY-MM-DD.grib
        output_file = output_dir / f"{utc_date_str}.grib"
        
        if output_file.exists():
            logger.info(f"UTC日期 {utc_date_str} 的数据文件已存在，跳过下载。")
            continue
            
        logger.info(f"正在为UTC日期 {utc_date_str} 下载小时: {hours_str_list}")
        c.retrieve(
            'reanalysis-era5-single-levels',
            {
                'product_type': 'reanalysis', 'format': 'grib',
                'variable': ["cloud_base_height", "high_cloud_cover", "low_cloud_cover", "medium_cloud_cover", "total_cloud_cover"],
                'year': str(dt_obj.year), 'month': f"{dt_obj.month:02d}", 'day': f"{dt_obj.day:02d}",
                'time': hours_str_list, 'area': area_bounds,
            },
            str(output_file)
        )
        logger.info(f"数据已保存到: {output_file}")

# download_gfs_data 和 main 函数保持不变，但 main 的调用对象是 target_date_obj
def download_gfs_data(target_date: datetime.date):
    pass # Placeholder

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="下载指定过去日期的天气再分析数据（跨天修复版）。")
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