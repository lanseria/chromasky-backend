# app/api/v1/endpoints/chromasky.py
from fastapi import APIRouter, HTTPException, Query
from typing import Dict, Any
from datetime import datetime, timezone

# 导入核心服务
from app.services.data_fetcher import data_fetcher, EventType
from app.services.chromasky_calculator import ChromaSkyCalculator

# --- 初始化 ---
router = APIRouter()

# 创建一个全局的计算器实例，并将 data_fetcher 实例注入
# 这确保了在整个应用的生命周期中，计算器只被创建一次
calculator = ChromaSkyCalculator(data_fetcher)

# --- 辅助函数，用于检查事件是否有效 ---
def is_event_valid(event: EventType) -> bool:
    """检查请求的事件是否是未来的事件"""
    event_time_utc_str = data_fetcher.gfs_time_metadata.get(event, {}).get("forecast_time_utc")
    if not event_time_utc_str:
        return False # 如果事件数据不存在，视为无效
    
    event_time_utc = datetime.fromisoformat(event_time_utc_str)
    return event_time_utc > datetime.now(timezone.utc)

# --- API 端点定义 ---

@router.get("/", 
            summary="获取单点火烧云指数",
            description="为指定的经纬度和事件，计算并返回详细的 ChromaSky 指数、分项得分和原始数据。")
def get_chromasky_index(
    event: EventType = Query(
        default="today_sunset",
        description="选择要查询的预报事件: 'today_sunrise', 'today_sunset', 'tomorrow_sunrise', 'tomorrow_sunset'"
    ),
    lat: float = Query(default=31.23, description="纬度 (Latitude)", ge=-90, le=90),
    lon: float = Query(default=121.47, description="经度 (Longitude)", ge=-180, le=360)
):
    """
    获取指定经纬度和事件的ChromaSky指数和详细分项得分。
    """
    # --- START OF CHANGE: 增加事件有效性检查 ---
    if not is_event_valid(event):
        raise HTTPException(
            status_code=404, 
            detail=f"事件 '{event}' 已过去或数据不可用。请查询未来的事件。"
        )
    # --- END OF CHANGE ---

    calculation_result = calculator.calculate_for_point(lat=lat, lon=lon, event=event)
    
    if calculation_result is None:
        raise HTTPException(
            status_code=404, 
            detail=f"无法为事件 '{event}' 在指定地点计算指数。可能是数据不完整。"
        )
        
    gfs_time_info = data_fetcher.gfs_time_metadata.get(event)
    aod_time_info = data_fetcher.aod_time_metadata
    
    return {
        "location": {"lat": lat, "lon": lon},
        "event": event,
        "time_info": {"gfs_forecast": gfs_time_info, "aod_forecast": aod_time_info},
        **calculation_result
    }


@router.get("/map_data", 
            summary="获取地图数据",
            description="为指定事件生成整个区域的火烧云指数地图数据 (GeoJSON格式)。注意：此请求可能耗时较长。")
def get_map_data(
    event: EventType = Query(default="today_sunset", description="选择要查询的预报事件")
):
    """
    为指定事件生成整个区域的火烧云指数地图数据 (GeoJSON格式)。
    """
    # --- START OF CHANGE: 增加事件有效性检查 ---
    if not is_event_valid(event):
        raise HTTPException(
            status_code=404, 
            detail=f"事件 '{event}' 已过去或数据不可用。请查询未来的事件。"
        )
    # --- END OF CHANGE ---
        
    geojson_data = calculator.generate_map_data(event=event)
    
    if "error" in geojson_data:
        raise HTTPException(status_code=404, detail=geojson_data["error"])
        
    gfs_time_info = data_fetcher.gfs_time_metadata.get(event)
    if gfs_time_info:
        geojson_data["properties"] = {
            "event": event,
            "gfs_base_time_utc": gfs_time_info.get("base_time_utc"),
            "forecast_time_utc": gfs_time_info.get("forecast_time_utc"),
        }
        
    return geojson_data


@router.get("/data_check",
            summary="调试接口：检查单点原始数据",
            description="一个用于调试的端点，返回计算指数所需的全部原始数据以及中间计算的因子得分。")
def check_data_for_point(
    event: EventType = Query(
        default="today_sunset",
        description="选择要查询的预报事件"
    ),
    lat: float = Query(
        default=29.800,
        description="纬度 (Latitude)",
        ge=-90,
        le=90
    ),
    lon: float = Query(
        default=121.740,
        description="经度 (Longitude)",
        ge=-180,
        le=360
    )
):
    """
    一个用于调试的端点，返回原始数据以及计算出的因子得分。
    """
    raw_gfs_data = data_fetcher.get_all_variables_for_point(lat=lat, lon=lon, event=event)
    if "error" in raw_gfs_data:
        raise HTTPException(status_code=404, detail=raw_gfs_data["error"])

    aod_value = data_fetcher.get_aod_for_event(lat=lat, lon=lon, event=event)
    avg_cloud_path = data_fetcher.get_light_path_avg_cloudiness(lat=lat, lon=lon, event=event)
        
    # 重新计算因子得分（与 / 路由中的逻辑一致）
    breakdown = calculator.calculate_for_point(lat, lon, event)
    
    gfs_time_info = data_fetcher.gfs_time_metadata.get(event)
    aod_time_info = data_fetcher.aod_time_metadata
    
    return {
        "message": f"成功获取事件 '{event}' 的原始数据及因子得分",
        "time_info": {
            "gfs_forecast": gfs_time_info,
            "aod_forecast": aod_time_info
        },
        "location": {"lat": lat, "lon": lon},
        "raw_data": {
            **raw_gfs_data,
            "aod": round(aod_value, 3) if aod_value is not None else None,
            "avg_tcc_along_path": round(avg_cloud_path, 2) if avg_cloud_path is not None else None,
        },
        "calculated_factors": breakdown.get("breakdown") if breakdown else None
    }