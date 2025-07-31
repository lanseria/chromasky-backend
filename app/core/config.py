# app/core/config.py
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    PROJECT_NAME: str = "ChromaSky API"
    API_V1_STR: str = "/api/v1"
    # 在这里可以添加更多配置，例如数据库URL、API密钥等
    # 例如： GFS_DATA_SOURCE: str = "https://example.com/gfs"

    class Config:
        case_sensitive = True

settings = Settings()