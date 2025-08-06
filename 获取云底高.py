def find_cloud_layers_from_rh(
    dataset,                         # GFS xarray.Dataset
    lat: float, lon: float,
    rh_threshold_percent: float = 85.0
) -> list[dict]:
    """
    通过分析相对湿度垂直剖面，找到云层的位置。
    返回一个包含云层信息的列表，例如 [{'base_hpa': 700, 'top_hpa': 650}, ...]
    """
    
    # GFS 提供的标准等压面层级 (从高空到地面排序)
    pressure_levels_hpa = [200, 250, 300, 400, 500, 600, 700, 850, 925, 1000] 
    
    # 提取该点的垂直湿度数据
    try:
        point_data = dataset.sel(latitude=lat, longitude=lon, method="nearest")
        rh_profile = {p: float(point_data['r'].sel(isobaricInhPa=p).values) for p in pressure_levels_hpa if 'isobaricInhPa' in point_data['r'].coords and p in point_data['r']['isobaricInhPa']}
    except Exception:
        return []

    cloud_layers = []
    in_cloud = False
    current_cloud_layer = {}

    # 从高空向地面扫描 (也可以反向)
    for p_level in sorted(rh_profile.keys()): # 从 200hPa 开始
        rh_value = rh_profile[p_level]
        
        # 从晴空进入云层 -> 找到了云顶
        if not in_cloud and rh_value >= rh_threshold_percent:
            in_cloud = True
            current_cloud_layer['top_hpa'] = p_level
        
        # 从云层进入晴空 -> 找到了云底，一个云层识别完毕
        elif in_cloud and rh_value < rh_threshold_percent:
            in_cloud = False
            current_cloud_layer['base_hpa'] = p_level_previous # 上一层是云底
            cloud_layers.append(current_cloud_layer)
            current_cloud_layer = {}
            
        p_level_previous = p_level

    # 如果扫描到地面仍然在云中，处理最后一个云层
    if in_cloud:
        current_cloud_layer['base_hpa'] = p_level_previous
        cloud_layers.append(current_cloud_layer)
        
    # 我们通常关心云底高，所以反转一下顺序，从低到高
    # 返回的 cloud_layers 类似 [{'base_hpa': 850, 'top_hpa': 700}, {'base_hpa': 400, 'top_hpa': 300}]
    return sorted(cloud_layers, key=lambda x: x['base_hpa'], reverse=True)