# app/services/grib_downloader.py
import requests
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from app.core.download_config import GFS_BASE_URL, GFS_DATA_BLOCKS, DOWNLOAD_DIR

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GribDownloader:
    """
    负责从 NOAA NOMADS 自动下载 GFS GRIB 数据。
    """
    def __init__(self, download_dir: Path = DOWNLOAD_DIR):
        self.download_dir = download_dir
        self.download_dir.mkdir(parents=True, exist_ok=True)

    def _build_url(self, run_info: dict, forecast_hour: int, block_config: dict) -> str:
        """根据已验证的运行周期和预报时效构建URL。"""
        dir_path = f"/gfs.{run_info['date']}/{run_info['run_hour']}/atmos"
        file_name = f"gfs.t{run_info['run_hour']}z.pgrb2.0p25.f{forecast_hour:03d}"
        
        params = { "dir": dir_path, "file": file_name }
        
        logger.info("将下载全球数据。")
            
        for var in block_config["vars"]:
            params[f"var_{var.upper()}"] = "on"
        for level in block_config["levels"]:
            params[f"lev_{level}"] = "on"
        
        req = requests.models.PreparedRequest()
        req.prepare_url(GFS_BASE_URL, params)
        return req.url

    def get_gfs_data_for_time(self, run_info: dict, target_time_utc: datetime, event_name: str):
        """
        为给定的目标UTC时间，从一个指定的运行周期下载所有数据块。
        (已更新，按事件组织文件结构)
        """
        run_time_utc = datetime.strptime(
            f"{run_info['date']}{run_info['run_hour']}", "%Y%m%d%H"
        ).replace(tzinfo=timezone.utc)
        
        time_diff_hours = (target_time_utc - run_time_utc).total_seconds() / 3600
        forecast_hour = round(time_diff_hours)
        
        if forecast_hour < 0:
            logger.error(f"目标时间 {target_time_utc} 早于运行周期 {run_time_utc}，无法预报。")
            return None, {}
            
        forecast_time_utc = run_time_utc + timedelta(hours=forecast_hour)
        time_metadata = {
            "base_time_utc": run_time_utc.isoformat(),
            "forecast_time_utc": forecast_time_utc.isoformat(),
            "forecast_hour": forecast_hour
        }
        
        logger.info(
            f"目标时间: {target_time_utc.isoformat()}. "
            f"基于运行周期: {run_time_utc.isoformat()}. "
            f"计算出的预报时效: {forecast_hour} 小时."
        )
        
        run_dir_name = f"{run_info['date']}_t{run_info['run_hour']}z"
        event_dir_name = f"{event_name}_f{forecast_hour:03d}"
        output_dir = self.download_dir / run_dir_name / event_dir_name
        output_dir.mkdir(parents=True, exist_ok=True)

        downloaded_paths = {}
        
        for block_name, config in GFS_DATA_BLOCKS.items():
            url = self._build_url(run_info, forecast_hour, config)
            output_path = output_dir / f"{block_name}.grib2"
            
            logger.info(f"正在下载 {block_name} 数据 (f{forecast_hour:03d})...")
            # 打印最终URL用于调试
            # logger.debug(f"Requesting URL: {url}")
            try:
                response = requests.get(url, stream=True, timeout=300)
                response.raise_for_status()
                with open(output_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                logger.info(f"成功保存到: {output_path}")
                downloaded_paths[block_name] = output_path
            except requests.exceptions.RequestException as e:
                # 打印失败的URL以帮助诊断
                logger.error(f"下载 {block_name} 失败 (URL: {url}): {e}")
                downloaded_paths[block_name] = None
        
        return time_metadata, downloaded_paths

# 单例保持不变
grib_downloader = GribDownloader()