# app/tasks/task_utils.py
from datetime import datetime, timedelta, timezone, time
from zoneinfo import ZoneInfo
from typing import Dict

# --- 从新的配置中导入列表 ---
from app.core.download_config import SUNRISE_EVENT_TIMES, SUNSET_EVENT_TIMES, LOCAL_TZ

def get_target_event_times() -> Dict[str, datetime]:
    """
    根据配置中的时间列表，计算所有目标事件的中心UTC时间。
    (已更新为处理多个时间点)
    """
    shanghai_tz = ZoneInfo(LOCAL_TZ)
    now_shanghai = datetime.now(shanghai_tz)
    
    today = now_shanghai.date()
    tomorrow = today + timedelta(days=1)

    all_events = {}

    # 遍历日出时间列表
    for t_str in SUNRISE_EVENT_TIMES:
        event_time = time.fromisoformat(t_str)
        time_suffix = t_str.replace(":", "") # e.g., "0500"
        all_events[f"today_sunrise_{time_suffix}"] = datetime.combine(today, event_time, tzinfo=shanghai_tz)
        all_events[f"tomorrow_sunrise_{time_suffix}"] = datetime.combine(tomorrow, event_time, tzinfo=shanghai_tz)

    # 遍历日落时间列表
    for t_str in SUNSET_EVENT_TIMES:
        event_time = time.fromisoformat(t_str)
        time_suffix = t_str.replace(":", "") # e.g., "1700"
        all_events[f"today_sunset_{time_suffix}"] = datetime.combine(today, event_time, tzinfo=shanghai_tz)
        all_events[f"tomorrow_sunset_{time_suffix}"] = datetime.combine(tomorrow, event_time, tzinfo=shanghai_tz)
    
    # 将所有本地时间转换为UTC时间
    return {name: dt.astimezone(timezone.utc) for name, dt in all_events.items()}