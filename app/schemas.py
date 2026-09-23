"""请求/响应 Pydantic 模型：各版本字段与语义一致。"""
from __future__ import annotations

from pydantic import BaseModel, Field


class ResourceCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    data: dict = Field(default_factory=dict)


class ResourceOut(BaseModel):
    id: str
    name: str
    data: dict
    api_version: str


class ErrorBody(BaseModel):
    error: str
    reason: str
    message: str
    details: dict = Field(default_factory=dict)
