"""统一错误码、错误响应模型与领域异常。"""
from __future__ import annotations

from enum import Enum


class ErrorCode(str, Enum):
    # 版本协商：未声明（缺少请求头）/ 未知（无法识别）/ 已下线（曾发布但 retired）
    VERSION_UNDECLARED = "VERSION_UNDECLARED"
    VERSION_UNKNOWN = "VERSION_UNKNOWN"
    VERSION_RETIRED = "VERSION_RETIRED"
    IDEMPOTENCY_KEY_MISSING = "IDEMPOTENCY_KEY_MISSING"
    IDEMPOTENCY_IN_PROGRESS = "IDEMPOTENCY_IN_PROGRESS"
    IDEMPOTENCY_KEY_CONFLICT = "IDEMPOTENCY_KEY_CONFLICT"
    QUOTA_EXCEEDED = "QUOTA_EXCEEDED"
    RESOURCE_NOT_FOUND = "RESOURCE_NOT_FOUND"


class APIError(Exception):
    """可向调用方解释的业务错误。"""

    def __init__(
        self,
        code: ErrorCode,
        message: str,
        *,
        status_code: int = 400,
        headers: dict[str, str] | None = None,
        details: dict[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.headers = headers or {}
        self.details = details or {}
