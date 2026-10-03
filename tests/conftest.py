"""pytest 公共夹具：虚拟时钟 + 全新应用实例。

另在测试会话结束时打印「业务覆盖」分组，把每个用例按业务域归类并展示结果，
便于一眼看出本次运行覆盖了哪些业务行为。
"""

from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from gateway.app import create_app
from gateway.config import QuotaRule, Settings, VersionInfo, VersionStatus

# 测试模块 -> 业务域（用于终端「业务覆盖」分组输出）
BUSINESS_AREAS = {
    "test_versioning": "版本协商",
    "test_ratelimit": "配额限流（滑动窗口 / 批量预留）",
    "test_idempotency": "幂等写入（含并发去重）",
    "test_api": "端到端管道 / 错误归类 / 判定日志",
    "test_batch": "批量下单（原子性 / 幂等 / 配额 / 隔离 / 窗口边缘）",
}

_RESULT_MARK = {"passed": "✓", "failed": "✗", "skipped": "⊘", "error": "!"}


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


# ---------------------------------------------------------------------------
# 业务覆盖分组输出：按业务域归类每个用例及其结果
# ---------------------------------------------------------------------------

def pytest_terminal_summary(terminalreporter, exitstatus, config):
    lines: list[str] = ["", "===================== 业务覆盖 ====================="]
    total = passed = failed = skipped = 0
    for module, area in BUSINESS_AREAS.items():
        reports = [
            r for r in _all_reports(terminalreporter)
            if r.location[0].replace("\\", "/").split("/")[-1][:-3] == module
        ]
        if not reports:
            continue
        # 同一用例在 setup/call/teardown 有多条 report，按 nodeid 归并取最差结果
        rank = {"passed": 0, "skipped": 1, "failed": 2, "error": 2}
        outcomes: dict[str, str] = {}
        for r in reports:
            nodeid = r.nodeid
            when = getattr(r, "when", "call")
            if r.passed:
                result = "passed"
            elif r.skipped:
                result = "skipped"
            else:
                result = "failed" if when == "call" else "error"
            if rank[result] > rank.get(outcomes.get(nodeid), -1):
                outcomes[nodeid] = result
        lines.append(f"[{area}] {len(outcomes)} 个用例")
        for nodeid in sorted(outcomes):
            result = outcomes[nodeid]
            name = nodeid.split("::", 1)[1] if "::" in nodeid else nodeid
            lines.append(f"    {_RESULT_MARK.get(result, '?')} {result.upper():7} {name}")
        total += len(outcomes)
        passed += sum(1 for v in outcomes.values() if v == "passed")
        failed += sum(1 for v in outcomes.values() if v in ("failed", "error"))
        skipped += sum(1 for v in outcomes.values() if v == "skipped")
    lines.append(
        f"合计 {total} 个用例：{passed} 通过 / {failed} 失败 / {skipped} 跳过"
    )
    lines.append("====================================================")
    terminalreporter.write_line("\n".join(lines))


def _all_reports(terminalreporter):
    for reports in terminalreporter.stats.values():
        for r in reports:
            # 只取真正的测试阶段报告（setup/call/teardown），排除 WarningReport 等
            if hasattr(r, "when") and hasattr(r, "location"):
                yield r
