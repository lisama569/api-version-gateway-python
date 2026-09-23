"""版本注册表与网关配置（纯数据，无外部服务依赖）。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping


class VersionStatus(str, Enum):
    """版本生命周期状态。"""

    SUPPORTED = "supported"
    DEPRECATED = "deprecated"
    SUNSET = "sunset"


@dataclass(frozen=True)
class VersionInfo:
    """单个 API 版本的元数据。"""

    version: str
    status: VersionStatus
    sunset_at: str | None = None
    successor: str | None = None
    notice: str | None = None


@dataclass(frozen=True)
class QuotaRule:
    """单个调用方的窗口配额。"""

    limit: int
    window_seconds: float


@dataclass(frozen=True)
class Settings:
    """网关运行配置。"""

    versions: Mapping[str, VersionInfo]
    quotas: Mapping[str, QuotaRule]
    default_quota: QuotaRule
    anonymous_quota: QuotaRule
    idempotency_ttl_seconds: float = 86400.0


def default_settings() -> Settings:
    """构造默认配置（纯内存数据，不依赖任何外部服务）。"""
    versions: dict[str, VersionInfo] = {
        "2024-01-01": VersionInfo(
            version="2024-01-01",
            status=VersionStatus.SUPPORTED,
            notice="当前推荐版本。",
        ),
        "2023-01-01": VersionInfo(
            version="2023-01-01",
            status=VersionStatus.DEPRECATED,
            sunset_at="2027-06-30T00:00:00Z",
            successor="2024-01-01",
            notice="该版本已弃用，请尽快迁移到 2024-01-01。",
        ),
        "2022-01-01": VersionInfo(
            version="2022-01-01",
            status=VersionStatus.SUNSET,
            sunset_at="2025-12-31T00:00:00Z",
            successor="2024-01-01",
            notice="该版本已下线，请升级到 2024-01-01。",
        ),
    }
    quotas = {
        "premium": QuotaRule(limit=1000, window_seconds=60.0),
        "trial": QuotaRule(limit=10, window_seconds=60.0),
    }
    return Settings(
        versions=versions,
        quotas=quotas,
        default_quota=QuotaRule(limit=100, window_seconds=60.0),
        anonymous_quota=QuotaRule(limit=20, window_seconds=60.0),
        idempotency_ttl_seconds=86400.0,
    )
