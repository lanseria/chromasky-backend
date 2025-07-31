# app/api/v1/endpoints/chromasky.py
from fastapi import APIRouter

router = APIRouter()

@router.get("/")
def get_chromasky_index(lat: float, lon: float):
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