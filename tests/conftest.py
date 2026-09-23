"""pytest 公共夹具：虚拟时钟 + 全新应用实例。"""

from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from gateway.app import create_app
from gateway.config import QuotaRule, Settings, VersionInfo, VersionStatus


class FakeClock:
    """可手动推进的单调时钟。"""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def make_settings(**overrides) -> Settings:
    versions = {
        "2024-01-01": VersionInfo("2024-01-01", VersionStatus.SUPPORTED),
        "2023-01-01": VersionInfo(
            "2023-01-01",
            VersionStatus.DEPRECATED,
            sunset_at="2027-06-30T00:00:00Z",
            successor="2024-01-01",
        ),
        "2022-01-01": VersionInfo(
            "2022-01-01",
            VersionStatus.SUNSET,
            sunset_at="2025-12-31T00:00:00Z",
            successor="2024-01-01",
        ),
    }
    return Settings(
        versions=versions,
        quotas={"premium": QuotaRule(1000, 60.0), "trial": QuotaRule(3, 60.0)},
        default_quota=QuotaRule(5, 60.0),
        anonymous_quota=QuotaRule(2, 60.0),
        idempotency_ttl_seconds=3600.0,
        **overrides,
    )


@pytest.fixture
def settings() -> Settings:
    return make_settings()


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def client(settings: Settings, clock: FakeClock):
    app = create_app(settings, clock=clock)
    # 避免 StreamHandler 在测试运行时重复打印；caplog 仍可经传播捕获记录
    logging.getLogger("api_gateway").handlers[:] = []
    return TestClient(app)
