# app/api/v1/endpoints/chromasky.py
from fastapi import APIRouter, HTTPException, Query
from typing import Any
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from enum import Enum

from app.services.data_fetcher import DataFetcher, EventType
from app.services.chromasky_calculator import ChromaSkyCalculator, MapDensity
from app.services.astronomy_service import AstronomyService
from app.models.sun_events import SunEventsResponse
from app.core.download_config import (
    CALCULATION_LAT_TOP, CALCULATION_LAT_BOTTOM, LOCAL_TZ, LOCAL_LAT, LOCAL_LON)


class SunEventType(str, Enum):
    sunrise = "sunrise"
    sunset = "sunset"
    first_light = "first_light"
    last_light = "last_light"

router = APIRouter()
calculator = ChromaSkyCalculator()
astronomy_service = AstronomyService()


def is_event_valid(event: EventType) -> bool:
    data_fetcher = DataFetcher()
    event_time_utc_str = data_fetcher.gfs_time_metadata.get(event, {}).get("forecast_time_utc")
    if not event_time_utc_str: return False
    return datetime.fromisoformat(event_time_utc_str) > datetime.now(timezone.utc)

@router.get("/", summary="获取单点火烧云指数")
def get_chromasky_index(
    event: EventType = Query("today_sunset"),
    lat: float = Query(LOCAL_LAT, ge=-90, le=90),
    lon: float = Query(LOCAL_LON, ge=-180, le=360)
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


@router.get(
    "/event_area",
    summary="获取指定时间窗口内发生某太阳事件的地理区域",
    response_model=dict  # GeoJSON结构复杂，直接用dict作为响应模型
)
def get_event_area_geojson(
    event: SunEventType = Query(SunEventType.sunrise, description="要计算的太阳事件类型"),
    center_time: str = Query(
        "05:00",
        description="时间窗口的中心时间，格式为 HH:MM",
        regex=r"^([01]\d|2[0-3]):([0-5]\d)$" # 正则表达式验证格式
    ),
    window_minutes: int = Query(
        60,
        description="时间窗口的总分钟数 (例如, 60 表示中心时间前后各30分钟)",
        ge=1,
        le=240 # 限制最大窗口，防止无效计算
    ),
    target_date_str: str = Query(
        default_factory=lambda: date.today().isoformat(),
        description="目标日期，格式为 YYYY-MM-DD",
        alias="date"
    ),
    tz: str = Query(LOCAL_TZ, description="目标时区，例如 'Asia/Shanghai' 或 'UTC'")
):
    """
    计算并返回一个 GeoJSON Polygon，该多边形覆盖了在指定日期和时间窗口内发生特定太阳事件的所有地理区域。
    
    例如，要查找今天在上海时间早上4:30到5:30之间发生日出的区域:
    - `event=sunrise`
    - `center_time=05:00`
    - `window_minutes=60`
    """
    try:
        target_date = date.fromisoformat(target_date_str)
    except ValueError:
        raise HTTPException(status_code=400, detail="日期格式无效，请使用 'YYYY-MM-DD' 格式。")

    geojson_data = astronomy_service.generate_event_area_geojson(
        event=event.value,
        target_date=target_date,
        center_time_str=center_time,
        window_minutes=window_minutes,
        local_tz_str=tz,
        lat_range=(CALCULATION_LAT_BOTTOM, CALCULATION_LAT_TOP)
    )

    if "error" in geojson_data:
        raise HTTPException(status_code=404, detail=geojson_data["error"])

    return geojson_data


@router.get(
    "/sun_events",
    summary="获取指定日期的太阳事件时间",
    response_model=SunEventsResponse
)
def get_sun_events(
    lat: float = Query(LOCAL_LAT, description="纬度", ge=-90, le=90),
    lon: float = Query(LOCAL_LON, description="经度", ge=-180, le=360),
    target_date_str: str = Query(
        default_factory=lambda: date.today().isoformat(),
        description="目标日期，格式为 YYYY-MM-DD",
        alias="date" # 允许用户使用 'date' 作为查询参数
    ),
    tz: str = Query(LOCAL_TZ, description="目标时区，例如 'Asia/Shanghai' 或 'UTC'")
):
    """
    根据给定的经纬度、日期和时区，计算四个关键的太阳事件时间：
    - **first_light**: 民用晨光始 (太阳在地平线下6度)
    - **sunrise**: 标准日出时间
    - **sunset**: 标准日落时间
    - **last_light**: 民用昏影终 (太阳在地平线下6度)
    """
    try:
        target_date = date.fromisoformat(target_date_str)
    except ValueError:
        raise HTTPException(status_code=400, detail="日期格式无效，请使用 'YYYY-MM-DD' 格式。")

    try:
        # 验证时区是否有效
        ZoneInfo(tz)
    except ZoneInfoNotFoundError:
        raise HTTPException(status_code=400, detail=f"时区 '{tz}' 无效。")

    event_times = astronomy_service.calculate_sun_events(lat, lon, target_date, tz)

    return SunEventsResponse(
        location={"lat": lat, "lon": lon},
        date=target_date.isoformat(),
        timezone=tz,
        events=event_times
    )

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