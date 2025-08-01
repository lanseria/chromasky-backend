# app/main.py
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from app.core.config import settings
from app.api.v1.api import api_router

app = FastAPI(title=settings.PROJECT_NAME)

app.mount("/static", StaticFiles(directory="frontend"), name="static")

app.include_router(api_router, prefix=settings.API_V1_STR)

# 2. 创建一个根路径路由，返回我们的 HTML 文件
@app.get("/", include_in_schema=False) # include_in_schema=False 避免在 /docs 中显示
async def read_index():
    # FileResponse 会直接读取文件内容并返回
    return FileResponse('frontend/index.html')

# API 路由保持不变，但现在它的路径与根路径分开了
app.include_router(api_router, prefix=settings.API_V1_STR)