# app/services/astronomy_service.py
import ephem
import logging
from datetime import datetime, date, time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from typing import Dict, Optional, List, Tuple, Any

logger = logging.getLogger(__name__)

# 定义不同事件对应的太阳地平高度
EVENT_HORIZONS = {
    "sunrise": "-0.833",  # 标准日出/日落（考虑大气折射和太阳视角半径）
    "sunset": "-0.833",
    "first_light": "-6",  # 民用曙暮光
    "last_light": "-6",
}

class AstronomyService:
    """
    提供基于地理坐标和日期的天文事件计算服务。
    """

    def calculate_sun_events(
        self,
        lat: float,
        lon: float,
        target_date: date,
        local_tz_str: str = "Asia/Shanghai"
    ) -> Dict[str, Optional[str]]:
        # ... 此方法保持不变，此处省略以保持简洁 ...
        observer = ephem.Observer()
        observer.lat = str(lat)
        observer.lon = str(lon)
        observer.elevation = 0
        local_tz = ZoneInfo(local_tz_str)
        observer.date = datetime.combine(target_date, time(12, 0), tzinfo=local_tz)
        sun = ephem.Sun()
        results = {}
        try:
            def to_local_iso(ephem_date) -> str:
                utc_naive_dt = ephem_date.datetime()
                utc_aware_dt = utc_naive_dt.replace(tzinfo=timezone.utc)
                local_dt = utc_aware_dt.astimezone(local_tz)
                return local_dt.isoformat()
            observer.horizon = EVENT_HORIZONS["sunrise"]
            results["sunrise"] = to_local_iso(observer.previous_rising(sun))
            results["sunset"] = to_local_iso(observer.next_setting(sun))
            observer.horizon = EVENT_HORIZONS["first_light"]
            results["first_light"] = to_local_iso(observer.previous_rising(sun, use_center=True))
            results["last_light"] = to_local_iso(observer.next_setting(sun, use_center=True))
        except ephem.AlwaysUpError:
            logger.warning(f"在 ({lat}, {lon}) on {target_date} 太阳永不落下 (极昼)。")
            results = {k: "always_up" for k in ["first_light", "sunrise", "sunset", "last_light"]}
        except ephem.NeverUpError:
            logger.warning(f"在 ({lat}, {lon}) on {target_date} 太阳永不升起 (极夜)。")
            results = {k: "never_up" for k in ["first_light", "sunrise", "sunset", "last_light"]}
        except Exception as e:
            logger.error(f"计算天文事件时出错: {e}", exc_info=True)
            results = {key: None for key in ["first_light", "sunrise", "sunset", "last_light"]}
        return results


    def _calculate_event_isochrone(
        self,
        target_utc_time: datetime,
        event: str,
        lat_range: Tuple[float, float] = (-75, 75),
        step: float = 2.0
    ) -> List[Tuple[float, float]]:
        """
        计算在给定UTC时间点，指定事件发生的地理轨迹线（等时线）。
        这是一个内部方法，是功能的核心。
        """
        points = []
        observer = ephem.Observer()
        sun = ephem.Sun()
        
        # 确保 target_utc_time 是 aware datetime
        if target_utc_time.tzinfo is None:
            target_utc_time = target_utc_time.replace(tzinfo=timezone.utc)
            
        observer.date = target_utc_time
        target_horizon = ephem.degrees(EVENT_HORIZONS[event])

        for lat in [x * step for x in range(int(lat_range[0]/step), int(lat_range[1]/step) + 1)]:
            observer.lat = str(lat)
            
            # 使用二分查找来寻找经度
            low_lon, high_lon = -180.0, 180.0
            found_lon = None
            
            for _ in range(20): # 20次迭代足够达到高精度
                mid_lon = (low_lon + high_lon) / 2
                observer.lon = str(mid_lon)
                sun.compute(observer)
                
                # 日出线通常在太阳东升时，经度较小
                if sun.alt > target_horizon:
                    high_lon = mid_lon
                else:
                    low_lon = mid_lon
            
            # 检查找到的点是否合理（避免极昼/极夜区域的无效点）
            sun.compute(observer)
            if abs(sun.alt - target_horizon) < ephem.degrees('1'): # 容忍1度误差
                found_lon = (low_lon + high_lon) / 2
                points.append((round(found_lon, 4), lat))

        return points

    def generate_event_area_geojson(
        self,
        event: str,
        target_date: date,
        center_time_str: str,
        window_minutes: int,
        local_tz_str: str
    ) -> Dict[str, Any]:
        """
        生成一个GeoJSON，表示在指定时间窗口内发生某事件的区域。
        """
        try:
            local_tz = ZoneInfo(local_tz_str)
            center_time = time.fromisoformat(center_time_str)
            center_dt_local = datetime.combine(target_date, center_time, tzinfo=local_tz)
            
            start_dt_local = center_dt_local - timedelta(minutes=window_minutes / 2)
            end_dt_local = center_dt_local + timedelta(minutes=window_minutes / 2)

            start_utc = start_dt_local.astimezone(timezone.utc)
            end_utc = end_dt_local.astimezone(timezone.utc)

        except (ValueError, ZoneInfoNotFoundError) as e:
            return {"error": f"时间和时区参数无效: {e}"}

        logger.info(f"正在计算事件 '{event}' 的区域...")
        logger.info(f"时间窗口 (UTC): {start_utc.isoformat()} to {end_utc.isoformat()}")

        # 计算两条等时线
        line1 = self._calculate_event_isochrone(start_utc, event)
        line2 = self._calculate_event_isochrone(end_utc, event)

        if not line1 or not line2:
            return {"error": "无法在此时间窗口内计算事件区域，可能处于极昼或极夜。"}

        # 将两条线合并成一个多边形环
        # 顺序：line1从南到北，line2从北到南（反转），然后闭合
        polygon_ring = line1 + line2[::-1] + [line1[0]]

        geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [polygon_ring]
                    },
                    "properties": {
                        "event": event,
                        "date": target_date.isoformat(),
                        "time_window_local": f"{start_dt_local.time().isoformat()} - {end_dt_local.time().isoformat()}",
                        "timezone": local_tz_str,
                    }
                }
            ]
        }
        return geojson