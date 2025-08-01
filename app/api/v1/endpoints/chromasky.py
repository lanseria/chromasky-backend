# app/api/v1/endpoints/chromasky.py
from fastapi import APIRouter, HTTPException, Query
from app.services.data_fetcher import data_fetcher, EventType  # 导入我们的数据获取器单例

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
    一个用于调试的端点，检查能为指定经纬度获取到哪些原始数据。
    """
    # 调用服务来获取数据
    variables = data_fetcher.get_all_variables_for_point(lat=lat, lon=lon, event=event)
    
    if "error" in variables:
        raise HTTPException(status_code=404, detail=variables["error"])
        
    # 从 data_fetcher 实例中获取对应事件的时间元数据
    time_info = data_fetcher.time_metadata.get(event)
    
    return {
        "message": f"成功获取事件 '{event}' 的原始数据",
        "time_info": time_info,
        "location": {"lat": lat, "lon": lon},
        "data": variables
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