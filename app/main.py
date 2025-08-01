# app/main.py
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from contextlib import asynccontextmanager
import logging

from app.core.config import settings
from app.api.v1.api import api_router
from app.services.data_fetcher import DataFetcher

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 应用启动时
    logger.info("应用启动，开始预加载数据...")
    DataFetcher() # 调用构造函数，触发单例的首次（也是唯一一次）加载
    logger.info("数据预加载完成。")
    yield
    # 应用关闭时
    logger.info("应用关闭。")

app = FastAPI(title=settings.PROJECT_NAME, lifespan=lifespan)

app.mount("/static", StaticFiles(directory="frontend"), name="static")

@app.get("/", include_in_schema=False)
async def read_index():
    return FileResponse('frontend/index.html')

app.include_router(api_router, prefix=settings.API_V1_STR)