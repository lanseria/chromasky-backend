# tools/download_map_data.py
import logging
import shutil
import tempfile
import urllib.request
import zipfile
from pathlib import Path

# --- 日志配置 ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("MapDataDownloader")

# --- 配置 ---
DATA_URL = "https://github.com/dongli/china-shapefiles/archive/refs/heads/master.zip"
# 确定项目根目录 (该脚本位于 tools/ 下，所以根目录是父目录的父目录)
PROJECT_ROOT = Path(__file__).parent.parent
TARGET_DIR = PROJECT_ROOT / "map_data"


def download_and_setup_map_data():
    """
    自动下载并设置中国地图shapefile数据。
    """
    logger.info("===== 开始下载和设置中国地图数据 =====")

    # 1. 确保目标目录存在
    TARGET_DIR.mkdir(exist_ok=True)
    logger.info(f"地图数据将安装到: {TARGET_DIR.resolve()}")

    # 使用临时目录进行下载和解压，保持项目目录干净
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        zip_path = tmp_path / "china-shapefiles.zip"

        # 2. 下载ZIP文件
        try:
            logger.info(f"正在从 {DATA_URL} 下载数据...")
            urllib.request.urlretrieve(DATA_URL, zip_path)
            logger.info(f"数据已成功下载到: {zip_path}")
        except Exception as e:
            logger.error(f"下载失败: {e}")
            return

        # 3. 解压ZIP文件
        extract_path = tmp_path / "extracted_data"
        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(extract_path)
            logger.info(f"文件已成功解压到: {extract_path}")
        except Exception as e:
            logger.error(f"解压失败: {e}")
            return

        # 4. 找到源 shapefiles 目录
        # 解压后的顶层目录名通常是 "china-shapefiles-master"
        source_shapefiles_dir = extract_path / "china-shapefiles-master" / "shapefiles"
        if not source_shapefiles_dir.exists():
            logger.error(f"在解压目录中未找到预期的 'shapefiles' 文件夹。")
            logger.error(f"请检查解压后的结构: {list(extract_path.glob('*'))}")
            return

        # 5. 将所有文件从源目录移动到目标目录
        logger.info(f"正在从 {source_shapefiles_dir} 移动文件到 {TARGET_DIR}...")
        files_moved = 0
        for file_path in source_shapefiles_dir.glob("*"):
            if file_path.is_file():
                destination_path = TARGET_DIR / file_path.name
                shutil.move(str(file_path), str(destination_path))
                logger.debug(f"  > 已移动: {file_path.name}")
                files_moved += 1
        
        if files_moved > 0:
            logger.info(f"成功移动 {files_moved} 个文件。")
        else:
            logger.warning("在源目录中没有找到可移动的文件。")

    logger.info("===== 地图数据设置完成！ =====")


if __name__ == "__main__":
    download_and_setup_map_data()