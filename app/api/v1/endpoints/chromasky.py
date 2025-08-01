# app/api/v1/endpoints/chromasky.py
from fastapi import APIRouter, HTTPException, Query
from app.services.data_fetcher import data_fetcher, EventType
from app.services import chromasky_calculator as calculator

router = APIRouter()

@router.get("/data_check")
def check_data_for_point(
    event: EventType = Query(
        default="today_sunset",
        description="选择要查询的预报事件: 'today_sunrise', 'today_sunset', 'tomorrow_sunrise', 'tomorrow_sunset'"
    ),
    lat: float = Query(
        default=29.800,  # 这是参数的默认值
        description="纬度 (Latitude)",
        ge=-90,         # ge = Greater than or equal to
        le=90           # le = Less than or equal to
    ),
    lon: float = Query(
        default=121.740, # 这是参数的默认值
        description="经度 (Longitude)",
        ge=-180,
        le=360          # 允许 0-360 和 -180-180 两种范围
    )
):
    """
    一个用于调试的端点，返回原始数据以及计算出的因子得分。
    """
    # 1. 获取本地的原始数据
    raw_data = data_fetcher.get_all_variables_for_point(lat=lat, lon=lon, event=event)
    if "error" in raw_data:
        raise HTTPException(status_code=404, detail=raw_data["error"])
        
    # 2. 计算光路上的平均云量 (因子B的输入)
    avg_cloud_path = data_fetcher.get_light_path_avg_cloudiness(lat=lat, lon=lon, event=event)

    # 3. 计算各个因子的得分
    factor_a_score = calculator.score_local_clouds(
        raw_data.get("high_cloud_cover"), raw_data.get("medium_cloud_cover")
    )
    factor_b_score = calculator.score_light_path(avg_cloud_path)
    factor_d_score = calculator.score_cloud_altitude(raw_data.get("cloud_base_height_meters"))
    
    # 从 data_fetcher 实例中获取对应事件的时间元数据
    time_info = data_fetcher.time_metadata.get(event)
    
    return {
        "message": f"成功获取事件 '{event}' 的原始数据及因子得分",
        "time_info": time_info,
        "location": {"lat": lat, "lon": lon},
        "raw_data": raw_data,
        "calculated_factors": {
            "factor_A_local_clouds": {
                "score": round(factor_a_score, 2),
                "input_hcc": raw_data.get("high_cloud_cover"),
                "input_mcc": raw_data.get("medium_cloud_cover"),
            },
            "factor_B_light_path": {
                "score": round(factor_b_score, 2),
                "input_avg_tcc_along_path": round(avg_cloud_path, 2) if avg_cloud_path is not None else None,
            },
            "factor_D_cloud_altitude": {
                "score": round(factor_d_score, 2),
                "input_cloud_base_meters": raw_data.get("cloud_base_height_meters"),
            }
        }
    }

@router.get("/")
def get_chromasky_index(
    lat: float = Query(
        default=31.23,  # 这是参数的默认值
        description="纬度 (Latitude)",
        ge=-90,         # ge = Greater than or equal to
        le=90           # le = Less than or equal to
    ),
    lon: float = Query(
        default=121.47, # 这是参数的默认值
        description="经度 (Longitude)",
        ge=-180,
        le=360          # 允许 0-360 和 -180-180 两种范围
    )
):
    """
    获取指定经纬度的ChromaSky指数。
    (这是V1的模拟实现)
    """
    # TODO: 在 app/services 中实现真实的计算逻辑
    # 真实的逻辑会调用服务来计算得分
    
    # 模拟返回数据
    score = 8.5
    breakdown = {
        "local_clouds": 0.9,  # 因子A
        "light_path": 0.95,   # 因子B
        "air_quality": 1.0,   # 因子C
        "cloud_altitude": 0.7 # 因子D
    }
    
    return {
        "location": {"lat": lat, "lon": lon},
        "chromasky_score": score,
        "breakdown": breakdown,
        "recommendation": "Excellent potential for a spectacular sunset!"
    }