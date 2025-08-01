# app/services/grib_downloader.py
import requests
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- 配置区域 ---
# 从您的URL中提取的固定配置
BASE_URL = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
SUBREGION_PARAMS = {
    "subregion": "",
    "toplat": 55,
    "leftlon": 100,
    "rightlon": 135,
    "bottomlat": 15
}
# 定义我们需要的每个数据块
# https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?dir=%2Fgfs.20250731%2F00%2Fatmos&file=gfs.t00z.pgrb2.0p25.f009&var_HCDC=on&var_LCDC=on&var_MCDC=on&lev_high_cloud_layer=on&lev_low_cloud_layer=on&lev_middle_cloud_layer=on&subregion=&toplat=55&leftlon=100&rightlon=135&bottomlat=15
# https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?dir=%2Fgfs.20250731%2F18%2Fatmos&file=gfs.t18z.pgrb2.0p25.f018&subregion=&toplat=55&leftlon=100&rightlon=135&bottomlat=15&var_LCC=on&var_MCC=on&var_HCC=on&lev_lowCloudLayer=on&lev_middleCloudLayer=on&lev_highCloudLayer=on
DATA_BLOCKS = {
    "total_cloud": {"vars": ["TCDC"], "levels": ["entire_atmosphere"]},
    "cloud_layers": {"vars": ["LCDC", "MCDC", "HCDC"], "levels": ["low_cloud_layer", "middle_cloud_layer", "high_cloud_layer"]},
    "visibility": {"vars": ["VIS"], "levels": ["surface"]}
}
# 数据将保存到这个目录
DOWNLOAD_DIR = Path("grib_data")
class GribDownloader:
    """
    负责从 NOAA NOMADS 自动下载 GFS GRIB 数据。
    (已更新为健壮的最新周期查找逻辑)
    """
    def __init__(self, download_dir: Path = DOWNLOAD_DIR):
        self.download_dir = download_dir
        self.download_dir.mkdir(parents=True, exist_ok=True)

    def _get_latest_available_run(self) -> dict | None:
        """
        向后查找并验证最新的可用GFS运行周期。
        返回包含'date'和'run_hour'的字典，如果找不到则返回None。
        """
        now_utc = datetime.now(timezone.utc)
        possible_run_hours = ["18", "12", "06", "00"]
        
        # 从今天开始，最多向后查找2天
        for days_ago in range(3):
            check_date = now_utc - timedelta(days=days_ago)
            date_str = check_date.strftime('%Y%m%d')
            
            for run_hour in possible_run_hours:
                # GFS运行时间通常比当前时间早
                run_time_utc = datetime.strptime(f"{date_str}{run_hour}", "%Y%m%d%H").replace(tzinfo=timezone.utc)
                if run_time_utc > now_utc:
                    continue # 这个运行周期在未来，跳过

                # 关键：检查该周期的目录是否存在于服务器上
                # 我们通过请求一个小的索引文件来验证
                dir_path = f"/gfs.{date_str}/{run_hour}/atmos"
                # NOMADS提供了一个小文件来列出目录内容
                inventory_url = f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.{date_str}/{run_hour}/atmos/gfs.t{run_hour}z.pgrb2.0p25.f000.idx"
                
                logger.info(f"正在检查周期: {dir_path} ...")
                try:
                    # 使用 HEAD 请求，我们只关心状态码，不需要下载内容
                    response = requests.head(inventory_url, timeout=10)
                    if response.status_code == 200:
                        logger.info(f"==> 找到最新的可用周期: {dir_path} <==")
                        return {"date": date_str, "run_hour": run_hour}
                except requests.exceptions.RequestException:
                    logger.warning(f"检查 {inventory_url} 时网络错误，跳过。")
                    continue
        
        logger.error("在过去3天内未找到任何可用的 GFS 运行周期。")
        return None

    def _build_url(self, run_info: dict, forecast_hour: int, block_config: dict) -> str:
        """根据已验证的运行周期和预报时效构建URL。"""
        dir_path = f"/gfs.{run_info['date']}/{run_info['run_hour']}/atmos"
        file_name = f"gfs.t{run_info['run_hour']}z.pgrb2.0p25.f{forecast_hour:03d}"
        
        params = { "dir": dir_path, "file": file_name, **SUBREGION_PARAMS }
        for var in block_config["vars"]:
            params[f"var_{var.upper()}"] = "on" # 变量名通常大写
        for level in block_config["levels"]:
            params[f"lev_{level}"] = "on"
        
        req = requests.models.PreparedRequest()
        req.prepare_url(BASE_URL, params)
        return req.url

    def get_gfs_data_for_time(self, target_time_utc: datetime):
        """
        为给定的目标UTC时间，下载所有定义的数据块。
        """
        latest_run = self._get_latest_available_run()
        if not latest_run:
            # 如果找不到运行周期，明确返回一个包含None和空字典的元组
            return None, {}

        run_time_utc = datetime.strptime(
            f"{latest_run['date']}{latest_run['run_hour']}", "%Y%m%d%H"
        ).replace(tzinfo=timezone.utc)
        
        time_diff_hours = (target_time_utc - run_time_utc).total_seconds() / 3600
        forecast_hour = round(time_diff_hours)
        
        forecast_time_utc = run_time_utc + timedelta(hours=forecast_hour)
        time_metadata = {
            "base_time_utc": run_time_utc.isoformat(),
            "forecast_time_utc": forecast_time_utc.isoformat(),
            "forecast_hour": forecast_hour
        }

        if forecast_hour < 0:
            logger.error("目标时间早于最新的可用运行周期，无法进行预报。")
            # 在这种失败情况下，也明确返回一个包含2个元素的元组
            return None, {}
            
        logger.info(
            f"目标时间: {target_time_utc.isoformat()}. "
            f"基于运行周期: {run_time_utc.isoformat()}. "
            f"计算出的预报时效: {forecast_hour} 小时."
        )
        
        downloaded_paths = {}
        
        for block_name, config in DATA_BLOCKS.items():
            url = self._build_url(latest_run, forecast_hour, config)
            output_path = self.download_dir / f"{block_name}_f{forecast_hour:03d}.grib2"
            
            logger.info(f"正在下载 {block_name} 数据 (f{forecast_hour:03d})...")
            try:
                # ... (下载逻辑不变) ...
                response = requests.get(url, stream=True, timeout=300)
                response.raise_for_status()
                with open(output_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                logger.info(f"成功保存到: {output_path}")
                downloaded_paths[block_name] = output_path
            except requests.exceptions.RequestException as e:
                logger.error(f"下载 {block_name} 失败: {e}")
                downloaded_paths[block_name] = None
        
        # 在成功的情况下，返回包含2个元素的元组
        return time_metadata, downloaded_paths

# 单例保持不变
grib_downloader = GribDownloader()