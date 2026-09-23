"""API 版本注册表与版本协商。

拒绝原因必须可区分：
- VERSION_UNDECLARED：请求根本没有声明版本（缺少 X-API-Version 头）；
- VERSION_UNKNOWN   ：声明了一个系统从未发布、无法识别的版本；
- VERSION_RETIRED   ：声明的是曾发布但已下线（retired）的版本。

已弃用（deprecated）版本仍可调用，但会通过 Deprecation/Sunset 等响应头
与响应体字段给出明确的弃用提示与停止支持时间。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from email.utils import format_datetime

from app.errors import APIError, ErrorCode

CURRENT = "current"
DEPRECATED = "deprecated"
RETIRED = "retired"


@dataclass(frozen=True)
class VersionInfo:
    version: str
    status: str  # current | deprecated | retired
    sunset: date | None = None
    successor: str | None = None
    note: str = ""


# 已发布版本注册表（含已下线版本，以便给出“已下线”而非“未知”的判定）
VERSIONS: dict[str, VersionInfo] = {
    "2023-01-01": VersionInfo(
        version="2023-01-01",
        status=RETIRED,
        sunset=date(2024, 6, 1),
        successor="2024-01-01",
        note="该版本已于 2024-06-01 停止支持。",
    ),
    "2024-01-01": VersionInfo(
        version="2024-01-01",
        status=DEPRECATED,
        sunset=date(2026, 12, 31),
        successor="2025-06-01",
        note="该版本已弃用，请尽快迁移到 2025-06-01。",
    ),
    "2025-06-01": VersionInfo(
        version="2025-06-01",
        status=CURRENT,
    ),
}

SUPPORTED_VERSIONS = [v for v in VERSIONS if VERSIONS[v].status != RETIRED]


def _http_date(day: date) -> str:
    """把日期渲染为 RFC 7231 HTTP-date（Sunset 头要求该格式）。"""
    moment = datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=timezone.utc)
    return format_datetime(moment, usegmt=True)


def resolve_version(requested: str | None) -> VersionInfo:
    """按请求头协商版本；非法情况抛 APIError（原因可区分）。"""
    if requested is None or not requested.strip():
        raise APIError(
            ErrorCode.VERSION_UNDECLARED,
            "请求未声明 API 版本：请通过 X-API-Version 请求头指定版本。",
            status_code=400,
            details={"supported_versions": SUPPORTED_VERSIONS},
        )

    requested = requested.strip()
    info = VERSIONS.get(requested)

    if info is None:
        raise APIError(
            ErrorCode.VERSION_UNKNOWN,
            f"未知的 API 版本：{requested!r}，系统从未发布该版本。",
            status_code=404,
            details={
                "requested_version": requested,
                "supported_versions": SUPPORTED_VERSIONS,
            },
        )

    if info.status == RETIRED:
        raise APIError(
            ErrorCode.VERSION_RETIRED,
            f"API 版本 {requested} 已下线，不再接受请求。",
            status_code=410,
            details={
                "requested_version": requested,
                "sunset": info.sunset.isoformat() if info.sunset else None,
                "successor": info.successor,
                "supported_versions": SUPPORTED_VERSIONS,
            },
        )

    return info


def deprecation_headers(info: VersionInfo) -> dict[str, str]:
    """为已弃用版本生成明确的弃用提示响应头。"""
    if info.status != DEPRECATED:
        return {}
    headers: dict[str, str] = {
        "Deprecation": "true",
        "X-Api-Deprecation": "true",
        # HTTP 头只允许 Latin-1，故头部提示用 ASCII；完整说明见错误响应体/日志
        "X-Api-Deprecation-Message": "This API version is deprecated; see Sunset and X-Api-Successor-Version.",
    }
    if info.sunset is not None:
        headers["Sunset"] = _http_date(info.sunset)
        headers["X-Api-Sunset"] = info.sunset.isoformat()
    if info.successor is not None:
        headers["Link"] = f'</>; rel="successor-version"; version="{info.successor}"'
        headers["X-Api-Successor-Version"] = info.successor
    return headers
