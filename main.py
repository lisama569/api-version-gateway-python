"""本地启动入口：uv run uvicorn main:app --reload"""

from gateway.app import app

__all__ = ["app"]
