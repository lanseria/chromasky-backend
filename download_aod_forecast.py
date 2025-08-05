import cdsapi
import logging
import argparse
import xarray as xr
from pathlib import Path
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Set, List

# --- 基本配置 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("AOD_Forecast_Downloader_Final")

# --- 从外部配置文件导入 ---
try:
    from app.core.download_config import LOCAL_TZ, SUNRISE_EVENT_TIMES, SUNSET_EVENT_TIMES, CDS_AREA_EXTRACTION
except ImportError:
    logger.error("无法导入配置文件 'app.core.download_config'。请确保该文件存在且路径正确。")
    exit(1)

# --- 数据存储目录 ---
DATA_DIR = Path("forecast_data")

def get_required_utc_dates_and_hours(target_local_date: datetime.date) -> Dict[str, Set[int]]:
    """
    根据本地日期和关心的本地时间点，计算出需要下载的UTC日期和小时。
    (此函数保持不变)
    """
    local_tz = ZoneInfo(LOCAL_TZ)
    all_event_times = SUNRISE_EVENT_TIMES + SUNSET_EVENT_TIMES
    utc_date_hours: Dict[str, Set[int]] = {}

    for time_str in all_event_times:
        try:
            event_time = datetime.strptime(time_str, '%H:%M').time()
        except ValueError:
            logger.warning(f"跳过无效的时间格式: {time_str}")
            continue
            
        local_dt = datetime.combine(target_local_date, event_time, tzinfo=local_tz)
        utc_dt = local_dt.astimezone(timezone.utc)
        
        utc_date_str = utc_dt.strftime('%Y-%m-%d')
        if utc_date_str not in utc_date_hours:
            utc_date_hours[utc_date_str] = set()
        utc_date_hours[utc_date_str].add(utc_dt.hour)
        
    return utc_date_hours

def download_cams_aod_data(target_local_date: datetime.date):
    """
    为指定的本地日期，通过单次API请求下载所有需要的CAMS AOD预报数据。
    """
    output_dir = DATA_DIR / target_local_date.strftime('%Y-%m-%d') / "cams"
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"--- [CAMS AOD] 开始为本地日期 {target_local_date} 下载数据到 {output_dir} ---")

    final_output_file = output_dir / "cams_aod_data.nc"
    if final_output_file.exists():
        logger.info(f"最终的 NetCDF 文件 '{final_output_file.name}' 已存在，跳过下载。")
        return

    # 1. 计算所有需要的UTC时刻
    required_utc_info = get_required_utc_dates_and_hours(target_local_date)
    if not required_utc_info:
        logger.warning("未能计算出任何需要下载的UTC日期和小时。")
        return

    all_required_utc_dts = []
    for utc_date_str, hours_set in required_utc_info.items():
        for hour in hours_set:
            dt = datetime.fromisoformat(f"{utc_date_str}T{hour:02d}:00:00").replace(tzinfo=timezone.utc)
            all_required_utc_dts.append(dt)

    if not all_required_utc_dts:
        return

    # 2. 确定唯一的、最佳的起报时间，并计算所有预报时效
    # 策略：选择能覆盖所有需要时刻的、最近的 00:00 UTC 起报。
    # 这通常是第一个需要时刻的前一天的 00:00 UTC。
    first_required_dt = min(all_required_utc_dts)
    init_dt_base = (first_required_dt.replace(hour=0, minute=0, second=0, microsecond=0)) - timedelta(days=1)
    init_date = init_dt_base.strftime('%Y-%m-%d')
    init_time = "00:00"

    # 计算所有需要的预报时效
    lead_times = set()
    for dt in all_required_utc_dts:
        leadtime_hours = int((dt - init_dt_base).total_seconds() / 3600)
        lead_times.add(leadtime_hours)
    
    sorted_lead_times_str = [str(lt) for lt in sorted(list(lead_times))]

    logger.info(f"将使用单一最优起报点 {init_date} {init_time} UTC 发起一次性下载请求:")
    logger.info(f"  > 预报时效 (小时): {', '.join(sorted_lead_times_str)}")

    # 3. 执行单次下载请求
    c = cdsapi.Client(timeout=600, quiet=False, url="https://ads.atmosphere.copernicus.eu/api")
    area_bounds = [CDS_AREA_EXTRACTION[k] for k in ["north", "west", "south", "east"]]
    
    try:
        request_params = {
            'variable': ['total_aerosol_optical_depth_550nm'],
            'date': [init_date],
            'time': [init_time],
            'leadtime_hour': sorted_lead_times_str,
            'type': ['forecast'],
            'format': 'netcdf',
            'area': area_bounds,
        }
        c.retrieve('cams-global-atmospheric-composition-forecasts', request_params, str(final_output_file))
        logger.info(f"✅ 数据已成功下载并保存为: {final_output_file}")

        # 4. 验证下载的文件
        logger.info("正在验证下载的NetCDF文件...")
        with xr.open_dataset(final_output_file) as ds:
            logger.info("文件可以被 xarray 成功打开。以下是文件结构：")
            print(ds)
            
            # 使用正确的坐标名称 'valid_time'
            time_coord_name = 'valid_time'
            if time_coord_name in ds.coords:
                logger.info(f"时间范围: {ds[time_coord_name].min().values} to {ds[time_coord_name].max().values}")
            else:
                logger.warning(f"最终文件中未找到名为 '{time_coord_name}' 的坐标。")
    except Exception as e:
        logger.error(f"下载或验证数据时发生错误: {e}", exc_info=True)
        if final_output_file.exists():
            final_output_file.unlink() # 如果失败则删除不完整的文件

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="通过单次请求，下载指定本地日期的CAMS AOD预报数据。")
    parser.add_argument("date", type=str, help="要下载数据的本地日期，格式为 YYYY-MM-DD。")
    args = parser.parse_args()
    
    try:
        target_date_obj = datetime.strptime(args.date, "%Y-%m-%d").date()
    except ValueError:
        logger.error("日期格式不正确，应为 YYYY-MM-DD。")
        exit(1)
    
    logger.info(f"===== 开始为本地日期 {args.date} 下载CAMS AOD预报数据 =====")
    download_cams_aod_data(target_date_obj)
    logger.info(f"===== {args.date} 的CAMS AOD预报数据下载任务完成 =====")