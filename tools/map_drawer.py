# tools/map_drawer.py (v13 - Organized Output Path)
import argparse
import logging
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.io import shapereader
import numpy as np
import xarray as xr
import pandas as pd
from scipy.ndimage import gaussian_filter

# --- 定位项目根目录 ---
PROJECT_ROOT = Path(__file__).parent.parent

try:
    from app.core.download_config import CDS_AREA_EXTRACTION
except ImportError:
    CDS_AREA_EXTRACTION = {"north": 54.0, "south": 0.0, "west": 70.0, "east": 135.0}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("MapDrawerTool")

try:
    plt.rcParams['font.sans-serif'] = ['SimHei'] 
    plt.rcParams['axes.unicode_minus'] = False
    CHINESE_FONT_FOUND = True
    logger.info("已设置中文字体 'SimHei'。")
except Exception:
    CHINESE_FONT_FOUND = False
    logger.warning("未找到中文字体 'SimHei'，城市名称将以降级模式（英文）显示。")


def generate_map_from_grid(score_grid: xr.DataArray, title: str, output_path: Path):
    # ... (此函数内容保持不变) ...
    logger.info(f"--- [绘图] 开始生成地图: {output_path.name} ---")

    scores_for_smoothing = score_grid.fillna(0).values
    smoothed_scores = gaussian_filter(scores_for_smoothing, sigma=1.5)
    smoothed_grid = xr.DataArray(smoothed_scores, coords=score_grid.coords, dims=score_grid.dims)
    interp_factor = 4
    orig_lats, orig_lons = smoothed_grid.latitude.values, smoothed_grid.longitude.values
    new_lats = np.linspace(orig_lats.min(), orig_lats.max(), len(orig_lats) * interp_factor)
    new_lons = np.linspace(orig_lons.min(), orig_lons.max(), len(orig_lons) * interp_factor)
    high_res_grid = smoothed_grid.interp(latitude=new_lats, longitude=new_lons, method='cubic')
    lats, lons, scores = high_res_grid.latitude.values, high_res_grid.longitude.values, high_res_grid.values
    scores[scores < 2] = np.nan
    logger.info(f"绘图前最终得分统计 (平滑后): Min={np.nanmin(scores):.2f}, Max={np.nanmax(scores):.2f}, Mean={np.nanmean(scores):.2f}")

    proj = ccrs.PlateCarree()
    fig = plt.figure(figsize=(12, 10), facecolor='black')
    ax = fig.add_subplot(1, 1, 1, projection=proj)
    ax.set_facecolor('black')
    area_bounds = [CDS_AREA_EXTRACTION[k] for k in ["west", "east", "south", "north"]]
    ax.set_extent(area_bounds, crs=ccrs.PlateCarree())

    ax.add_feature(cfeature.OCEAN.with_scale('50m'), facecolor='#0c0a09', zorder=0)
    ax.add_feature(cfeature.LAND.with_scale('50m'), facecolor='#1c1917', edgecolor='none', zorder=0)

    colors = ["#3b82f6", "#fde047", "#f97316", "#ef4444", "#ec4899"]
    nodes = [0.0, 0.5, 0.7, 0.85, 1.0]
    chromasky_cmap = mcolors.LinearSegmentedColormap.from_list("chromasky", list(zip(nodes, colors)))
    levels = np.linspace(2, 10, 100)
    
    contour_fill = ax.contourf(lons, lats, scores, levels=levels, cmap=chromasky_cmap, transform=ccrs.PlateCarree(), extend='max', zorder=1)

    china_map_dir = PROJECT_ROOT / "map_data"
    china_full_shp = china_map_dir / "china.shp"
    nine_dash_line_shp = china_map_dir / "china_nine_dotted_line.shp"

    if not all([p.exists() for p in [china_full_shp, nine_dash_line_shp]]):
        logger.error(f"地图数据文件未在 '{china_map_dir}' 目录中找到。请运行 tools/download_map_data.py")
        return

    china_geometries = shapereader.Reader(str(china_full_shp)).geometries()
    ax.add_feature(cfeature.ShapelyFeature(china_geometries, ccrs.PlateCarree()),
                   facecolor='none', edgecolor='#a8a29e', linewidth=0.5, zorder=2)
                   
    nine_dash_geometries = shapereader.Reader(str(nine_dash_line_shp)).geometries()
    ax.add_feature(cfeature.ShapelyFeature(nine_dash_geometries, ccrs.PlateCarree()),
                   facecolor='none', edgecolor='#a8a29e', linewidth=1.0, zorder=2)

    ax.add_feature(cfeature.COASTLINE.with_scale('50m'), edgecolor='#78716c', linewidth=0.5, zorder=2)

    contour_lines = ax.contour(lons, lats, scores, levels=[4, 6, 8, 9], colors='white', linewidths=[0.5, 0.8, 1.2, 1.5], alpha=0.6, transform=ccrs.PlateCarree(), zorder=3)
    ax.clabel(contour_lines, inline=True, fontsize=8, fmt='%1.0f', colors='white')
    
    cities_file = PROJECT_ROOT / "app" / "assets" / "china_cities.csv"
    if cities_file.exists():
        df_cities = pd.read_csv(cities_file)
        ax.plot(df_cities['lon'], df_cities['lat'], 'o', color='white',
                markersize=2, alpha=0.7, transform=ccrs.PlateCarree(), zorder=4)
        
        for _, city in df_cities.iterrows():
            display_name = city['name'] if CHINESE_FONT_FOUND else city['name_en']
            ax.text(city['lon'] + 0.1, city['lat'] + 0.1, display_name,
                    color='white', fontsize=8, alpha=0.8,
                    transform=ccrs.PlateCarree(), zorder=4)
        logger.info(f"已成功在地图上绘制 {len(df_cities)} 个主要城市。")
    else:
        logger.warning(f"未找到城市数据文件: {cities_file}，跳过城市绘制。")

    gl = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=True, linewidth=0.5, color='#44403c', alpha=0.8, linestyle='--')
    gl.top_labels, gl.right_labels = False, False
    gl.xlabel_style, gl.ylabel_style = {'color': 'white', 'size': 10}, {'color': 'white', 'size': 10}

    ax.set_title(title, fontsize=18, color='white', pad=20)
    cbar = fig.colorbar(contour_fill, ax=ax, orientation='vertical', pad=0.02, shrink=0.8, ticks=[2, 4, 6, 8, 10])
    cbar.set_label('ChromaSky Score (0-10)', color='white')
    cbar.ax.yaxis.set_tick_params(color='white')
    plt.setp(plt.getp(cbar.ax.axes, 'yticklabels'), color='white')

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=200, bbox_inches='tight', pad_inches=0.1, transparent=True, facecolor=fig.get_facecolor())
    plt.close(fig)
    logger.info(f"--- [绘图] 地图已成功保存到: {output_path} ---")


def create_dummy_score_grid() -> xr.DataArray:
    # ... (此函数内容保持不变) ...
    logger.info("正在创建用于验证的模拟分数网格...")
    lats = np.arange(CDS_AREA_EXTRACTION["south"], CDS_AREA_EXTRACTION["north"], 0.25)
    lons = np.arange(CDS_AREA_EXTRACTION["west"], CDS_AREA_EXTRACTION["east"], 0.25)
    lon_grid, lat_grid = np.meshgrid(lons, lats)
    center_lon, center_lat = 115, 30
    sigma_lon, sigma_lat = 10, 8
    exponent = -((lon_grid - center_lon)**2 / (2 * sigma_lon**2) + (lat_grid - center_lat)**2 / (2 * sigma_lat**2))
    scores = 10 * np.exp(exponent)
    return xr.DataArray(scores, coords={'latitude': lats, 'longitude': lons}, dims=['latitude', 'longitude'])


if __name__ == "__main__":
    # --- 修改开始 ---
    # 1. 定义一个默认的输出目录
    OUTPUT_DIR = PROJECT_ROOT / "debug_maps"

    parser = argparse.ArgumentParser(description="一个简单的地图绘制工具，用于验证绘图功能。")
    parser.add_argument(
        "-o", "--output", 
        type=str, 
        default="validation_map_china_with_cities.png",
        help=f"输出图片的文件名 (将保存在 '{OUTPUT_DIR.name}/' 目录下)。"
    )
    parser.add_argument(
        "-t", "--title", 
        type=str, 
        default="ChromaSky Validation Map (China)"
    )
    args = parser.parse_args()

    # 2. 创建模拟数据
    sample_grid = create_dummy_score_grid()

    # 3. 构建完整的输出路径
    # 这确保了目录会被自动创建（因为 generate_map_from_grid 内部有 mkdir)
    output_file_path = OUTPUT_DIR / args.output

    # 4. 调用绘图函数
    generate_map_from_grid(
        score_grid=sample_grid, 
        title=args.title, 
        output_path=output_file_path
    )
    
    print(f"\n✅ 验证地图生成成功！图片已保存到: {output_file_path.resolve()}")
    # --- 修改结束 ---