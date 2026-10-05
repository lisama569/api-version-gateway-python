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


# ---- 用例级结果日志：跑单测时能看出每个用例覆盖的业务与结果 ----


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item, call):
    """把各阶段报告挂到 item 上，供 log_case_result 夹具读取最终结果。"""
    report = (yield)
    setattr(item, f"rep_{report.when}", report)
    return report


@pytest.fixture(autouse=True)
def log_case_result(request):
    """每个用例结束都打印一条结构化日志：用例名 + 结果（passed/failed/skipped）。

    配合 `uv run pytest -v --log-cli-level=INFO` 可在测试输出中直接看到
    每个用例覆盖了什么业务、结论如何；业务判定细节由网关自身的
    version/quota/idempotency/batch 决策日志体现（可被 caplog 断言）。
    """
    from gateway.app import log_event

    yield
    report = getattr(request.node, "rep_call", None)
    result = report.outcome if report is not None else "unknown"
    log_event("test_case", case=request.node.name, result=result)
