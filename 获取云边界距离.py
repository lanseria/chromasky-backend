import numpy as np
from geopy.distance import geodesic
# 假设你有一个函数可以计算方位角和沿路径移动
# from your_geo_utils import get_sun_azimuth, get_point_along_path

def find_cloud_edge_distance(
    dataset,                # GFS 数据集 (xarray.Dataset)
    obs_lat: float,         # 观测点纬度
    obs_lon: float,         # 观测点经度
    event_time_utc,         # 事件时间 (datetime)
    cloud_type: str = 'high', # 'high' or 'medium'
    max_scan_distance_km: int = 800, # 最大扫描距离
    step_km: int = 10,       # 扫描步长
    cloud_threshold_percent: float = 15.0 # “有云”的阈值
) -> float | None:
    """
    计算沿太阳方位角方向，云层边界的距离。
    返回最后一个“有云”点的距离，如果没有找到云则返回 None。
    """
    
    # 步骤 1: 获取太阳方位角
    sun_azimuth = get_sun_azimuth(obs_lat, obs_lon, event_time_utc)

    # 选择要分析的云层变量
    if cloud_type == 'high':
        cloud_var = 'hcdc'
    elif cloud_type == 'medium':
        cloud_var = 'mcdc'
    else:
        raise ValueError("cloud_type must be 'high' or 'medium'")
        
    if cloud_var not in dataset:
        print(f"变量 {cloud_var} 不在数据集中")
        return None

    last_cloud_distance = None

    # 步骤 3: 沿路径采样和搜索
    for distance in range(0, max_scan_distance_km + 1, step_km):
        
        # 计算当前采样点的经纬度
        sample_lat, sample_lon = get_point_along_path(obs_lat, obs_lon, sun_azimuth, distance)
        
        # GFS 经度是 0-360，需要转换
        lon_360 = sample_lon + 360 if sample_lon < 0 else sample_lon

        # 从 GFS 数据集中获取最近点的数据
        try:
            point_data = dataset.sel(latitude=sample_lat, longitude=lon_360, method="nearest")
            cloudiness = float(point_data[cloud_var].values)
        except Exception as e:
            # 如果采样点超出 GFS 数据范围，则停止
            print(f"在距离 {distance}km 处采样失败: {e}")
            break

        # 步骤 4: 识别边界
        # 如果当前点的云量超过阈值，就更新“最后一个有云点”的距离
        if cloudiness >= cloud_threshold_percent:
            last_cloud_distance = distance
            
    # 循环结束后，last_cloud_distance 存储的就是路径上最后一个有云点的距离
    # 这就是我们定义的“云边界”
    return last_cloud_distance