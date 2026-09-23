"""版本协商：解析请求版本并判定其生命周期状态。"""

from __future__ import annotations

from dataclasses import dataclass

from gateway.config import Settings, VersionInfo, VersionStatus
from gateway.errors import (
    VersionSunsetError,
    VersionUndeclaredError,
    VersionUnknownError,
)

API_VERSION_HEADER = "X-API-Version"


@dataclass(frozen=True)
class VersionDecision:
    """版本协商结论。"""

    requested: str
    version: VersionInfo


def negotiate_version(requested: str | None, settings: Settings) -> VersionDecision:
    """根据请求头协商版本。

    - 未声明（缺头或空串）→ VersionUndeclaredError(400 version_undeclared)
    - 未知版本            → VersionUnknownError(404 version_unknown)
    - 已下线版本          → VersionSunsetError(410 version_sunset)
    - 已弃用版本          → 正常放行，由调用方附加弃用提示头
    - 受支持版本          → 放行
    """
    if requested is None or not requested.strip():
        raise VersionUndeclaredError(
            "请求缺少 API 版本声明，请在 X-API-Version 头中指定版本。",
            details={"header": API_VERSION_HEADER, "available": sorted(settings.versions)},
        )
    version = requested.strip()
    info = settings.versions.get(version)
    if info is None:
        raise VersionUnknownError(
            f"未知的 API 版本：{version}",
            details={
                "requested_version": version,
                "supported": [
                    v.version for v in settings.versions.values()
                    if v.status is VersionStatus.SUPPORTED
                ],
            },
        )
    if info.status is VersionStatus.SUNSET:
        raise VersionSunsetError(
            f"API 版本 {version} 已下线，停止支持时间：{info.sunset_at}。",
            details={
                "requested_version": version,
                "sunset_at": info.sunset_at,
                "successor": info.successor,
            },
        )
    return VersionDecision(requested=version, version=info)
