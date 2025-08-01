# app/api/v1/endpoints/chromasky.py
from fastapi import APIRouter, HTTPException, Query
from typing import Any
from datetime import datetime, timezone

from app.services.data_fetcher import DataFetcher, EventType
from app.services.chromasky_calculator import ChromaSkyCalculator, MapDensity

router = APIRouter()
calculator = ChromaSkyCalculator()

def is_event_valid(event: EventType) -> bool:
    data_fetcher = DataFetcher()
    event_time_utc_str = data_fetcher.gfs_time_metadata.get(event, {}).get("forecast_time_utc")
    if not event_time_utc_str: return False
    return datetime.fromisoformat(event_time_utc_str) > datetime.now(timezone.utc)

@router.get("/", summary="获取单点火烧云指数")
def get_chromasky_index(
    event: EventType = Query("today_sunset"),
    lat: float = Query(31.23, ge=-90, le=90),
    lon: float = Query(121.47, ge=-180, le=360)
):
    if not is_event_valid(event):
        raise HTTPException(status_code=404, detail=f"事件 '{event}' 已过去或数据不可用。")
    
    result = calculator.calculate_for_point(lat=lat, lon=lon, event=event)
    if result is None:
        raise HTTPException(status_code=404, detail="无法计算指数，数据不完整。")
    
    data_fetcher = DataFetcher()
    return {
        "location": {"lat": lat, "lon": lon},
        "event": event,
        "time_info": {
            "gfs_forecast": data_fetcher.gfs_time_metadata.get(event),
            "aod_forecast": data_fetcher.aod_time_metadata
        },
        **result
    }

@router.get("/map_data", summary="获取地图数据")
def get_map_data(
    event: EventType = Query("today_sunset"),
    density: MapDensity = Query(MapDensity.medium)
):
    if not is_event_valid(event):
        raise HTTPException(status_code=404, detail=f"事件 '{event}' 已过去或数据不可用。")
    
    geojson_data = calculator.generate_map_data(event=event, density=density)
    if "error" in geojson_data:
        raise HTTPException(status_code=404, detail=geojson_data["error"])
    
    data_fetcher = DataFetcher()
    if gfs_info := data_fetcher.gfs_time_metadata.get(event):
        geojson_data["properties"] = {
            "event": event,
            "density": density.value,
            **gfs_info
        }
    return geojson_data

@router.get("/data_check", summary="调试接口：检查单点原始数据")
def check_data_for_point(
    event: EventType = Query("today_sunset"),
    lat: float = Query(29.800, ge=-90, le=90),
    lon: float = Query(121.740, ge=-180, le=360)
):
    data_fetcher = DataFetcher()
    raw_gfs_data = data_fetcher.get_all_variables_for_point(lat=lat, lon=lon, event=event)
    if "error" in raw_gfs_data:
        raise HTTPException(status_code=404, detail=raw_gfs_data["error"])

    result = calculator.calculate_for_point(lat, lon, event)
    
    return {
        "message": "成功获取原始数据及因子得分",
        "time_info": {
            "gfs_forecast": data_fetcher.gfs_time_metadata.get(event),
            "aod_forecast": data_fetcher.aod_time_metadata
        },
        "location": {"lat": lat, "lon": lon},
        "raw_data": raw_gfs_data,
        "calculated_factors": result.get("breakdown") if result else None
    }