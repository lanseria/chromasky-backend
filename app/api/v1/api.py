# app/api/v1/api.py
from fastapi import APIRouter
from app.api.v1.endpoints import chromasky

api_router = APIRouter()
api_router.include_router(chromasky.router, prefix="/chromasky", tags=["ChromaSky"])

# 如果未来有更多v1的端点，在这里继续添加
# api_router.include_router(other_router, prefix="/other", tags=["Other"])