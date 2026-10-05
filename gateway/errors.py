"""统一错误类型：每种拒绝原因对应独立的错误码。"""


class GatewayError(Exception):
    """网关错误基类。"""

    status_code: int = 400
    error_code: str = "gateway_error"

    def __init__(self, message: str, *, details: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}


class VersionUndeclaredError(GatewayError):
    """请求未声明任何 API 版本。"""

    status_code = 400
    error_code = "version_undeclared"


class VersionUnknownError(GatewayError):
    """声明的版本不在注册表中。"""

    status_code = 404
    error_code = "version_unknown"


class VersionSunsetError(GatewayError):
    """声明的版本已下线。"""

    status_code = 410
    error_code = "version_sunset"


class IdempotencyKeyMissingError(GatewayError):
    """写请求缺少幂等键。"""

    status_code = 400
    error_code = "idempotency_key_missing"


class IdempotencyConflictError(GatewayError):
    """同一幂等键被用于不同的请求体。"""

    status_code = 409
    error_code = "idempotency_conflict"


class QuotaExceededError(GatewayError):
    """调用方超出时间窗口配额。"""

    status_code = 429
    error_code = "quota_exceeded"


class BatchTooLargeError(GatewayError):
    """单批条数超过整段窗口的配额上限：无论等待多久都放不下，必须拆批。

    与“窗口内额度暂时不足”（429 quota_exceeded + 可重试时间）刻意区分：
    本错误不携带 Retry-After，调用方应缩小单批条数而不是立刻重试。
    """

    status_code = 422
    error_code = "batch_too_large"


class ResourceNotFoundError(GatewayError):
    """资源不存在。"""

    status_code = 404
    error_code = "resource_not_found"
