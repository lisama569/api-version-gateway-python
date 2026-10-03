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


class BatchValidationError(GatewayError):
    """批量请求中某一条目不合法（整批拒绝，不写入任何一条）。"""

    status_code = 400
    error_code = "batch_item_invalid"


class QuotaExceededError(GatewayError):
    """调用方超出时间窗口配额。"""

    status_code = 429
    error_code = "quota_exceeded"


class ResourceNotFoundError(GatewayError):
    """资源不存在。"""

    status_code = 404
    error_code = "resource_not_found"
