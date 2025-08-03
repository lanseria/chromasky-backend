# app/models/sun_events.py
from pydantic import BaseModel
from typing import Optional, Dict

class SunEventsResponse(BaseModel):
    """
    API响应模型，用于返回太阳事件的时间。
    时间值是ISO 8601格式的字符串，或者在特殊情况下（如极昼/极夜）为特殊字符串。
    """
    location: Dict[str, float]
    date: str
    timezone: str
    events: Dict[str, Optional[str]]