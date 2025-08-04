# app/tasks/task_utils.py
from datetime import datetime, timedelta, timezone, time
from zoneinfo import ZoneInfo
from typing import Dict

from app.core.download_config import SUNRISE_CENTER_TIME, SUNSET_CENTER_TIME, LOCAL_TZ

def get_target_event_times() -> Dict[str, datetime]:
    """
    根据配置计算所有目标事件（日出/日落）的中心UTC时间。
    这是一个共享的工具函数，供所有需要目标时间的任务使用。
    """
    shanghai_tz = ZoneInfo(LOCAL_TZ)
    now_shanghai = datetime.now(shanghai_tz)
    
    today = now_shanghai.date()
    tomorrow = today + timedelta(days=1)

    # 从字符串配置创建 time 对象
    sunrise_t = time.fromisoformat(SUNRISE_CENTER_TIME)
    sunset_t = time.fromisoformat(SUNSET_CENTER_TIME)

    all_events = {
        # 注意：现在我们只关心这四个核心事件的中心时间点，用于下载数据
        "today_sunrise": datetime.combine(today, sunrise_t, tzinfo=shanghai_tz),
        "today_sunset": datetime.combine(today, sunset_t, tzinfo=shanghai_tz),
        "tomorrow_sunrise": datetime.combine(tomorrow, sunrise_t, tzinfo=shanghai_tz),
        "tomorrow_sunset": datetime.combine(tomorrow, sunset_t, tzinfo=shanghai_tz),
    }
    
    # 将所有本地时间转换为UTC时间，用于计算预报时效
    return {name: dt.astimezone(timezone.utc) for name, dt in all_events.items()}