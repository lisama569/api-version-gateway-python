"""pytest 公共夹具：虚拟时钟 + 全新应用实例。

另含轻量终端输出增强：跑测时按“业务模块”分组打印用例结果，
便于一眼看出本次覆盖了哪些业务、每个用例是通过还是失败。
"""

from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from gateway.app import create_app
from gateway.config import QuotaRule, Settings, VersionInfo, VersionStatus

# 测试文件 → 覆盖的业务模块（输出分组标题用）
_BUSINESS_GROUPS = {
    "test_versioning.py": "业务① 版本协商（未声明/未知/已下线/已弃用）",
    "test_idempotency.py": "业务② 幂等写入（去重/并发占位/失败回放/可重试失败）",
    "test_ratelimit.py": "业务③ 配额限流（滑动窗口/按条计费/并发不透支/超限标志）",
    "test_batch.py": "业务④ 批量下单（原子性/非法条目/同键重试/超限拆批/并发/隔离）",
    "test_api.py": "业务⑤ 端到端管道（错误分类/管道顺序/判定日志）",
}

def pytest_report_header(config):  # noqa: ARG001
    return ["批量下单正确性回归测试：版本协商 / 幂等 / 限流 / 批量 / 端到端"]


_shown_groups: set[str] = set()


def pytest_runtest_logstart(nodeid, location):
    """每个业务模块的第一个用例开始前，打印一次业务覆盖分组标题。"""
    path, _, _ = location
    module = path.split("/")[-1]
    group = _BUSINESS_GROUPS.get(module)
    if group is not None and module not in _shown_groups:
        _shown_groups.add(module)
        print(f"\n▶ 覆盖业务: {group}")


def pytest_terminal_summary(terminalreporter, exitstatus, config):  # noqa: ARG001
    tr = terminalreporter
    stats = tr.stats
    passed = len(stats.get("passed", []))
    failed = len(stats.get("failed", []))
    skipped = len(stats.get("skipped", []))
    line = f"业务覆盖汇总: 通过 {passed} / 失败 {failed} / 跳过 {skipped}"
    tr.write_sep("=", line)
    # 逐业务模块列出用例数，明确覆盖边界（stats 中可能混入 WarningReport，需过滤）
    for module, title in _BUSINESS_GROUPS.items():
        count = sum(
            1
            for reports in stats.values()
            for r in reports
            if getattr(r, "when", None) == "call" and module in getattr(r, "nodeid", "")
        )
        if count:
            tr.write_line(f"  {title}: {count} 个用例")


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
