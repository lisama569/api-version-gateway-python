"""本地启动入口：uvicorn main:app --reload"""
from app.main import app

__all__ = ["app"]
