"""配额限流：边界计数、窗口滚动、重试信息、并发不漏计、按调用方隔离。"""

from __future__ import annotations

import threading

from gateway.config import QuotaRule
from gateway.ratelimit import RateLimiter

VH = {"X-API-Version": "2024-01-01", "X-Caller-Id": "trial"}  # limit=3


# ---- 限流器单元测试 ----

def test_boundary_allows_exactly_limit_then_denies():
    t = 0.0
    rl = RateLimiter(clock=lambda: t)
    rule = QuotaRule(limit=3, window_seconds=10.0)
    decisions = [rl.check("c", rule) for _ in range(4)]
    assert [d.allowed for d in decisions] == [True, True, True, False]
    denied = decisions[-1]
    assert denied.remaining == 0
    assert denied.retry_after_seconds == 10.0
    assert denied.used == 4


def test_window_rollover_no_gap():
    """切换瞬间旧窗口立即失效、新窗口从 1 开始计，不存在可反复利用的空档。"""
    t = 0.0
    rl = RateLimiter(clock=lambda: t)
    rule = QuotaRule(limit=2, window_seconds=10.0)
    assert rl.check("c", rule).allowed
    assert rl.check("c", rule).allowed
    assert not rl.check("c", rule).allowed
    t = 9.99
    assert not rl.check("c", rule).allowed  # 仍在旧窗口
    t = 10.0
    d = rl.check("c", rule)               # 新窗口第一个请求负责滚动
    assert d.allowed and d.remaining == 1
    assert d.window_started_at == 10.0
    assert rl.current_usage("c") == 1


def test_rejected_requests_still_counted():
    t = 0.0
    rl = RateLimiter(clock=lambda: t)
    rule = QuotaRule(limit=1, window_seconds=10.0)
    assert rl.check("c", rule).allowed
    for _ in range(5):
        assert not rl.check("c", rule).allowed
    assert rl.current_usage("c") == 6  # 超限请求不能刷出“额外名额”


def test_callers_isolated():
    t = 0.0
    rl = RateLimiter(clock=lambda: t)
    rule = QuotaRule(limit=1, window_seconds=10.0)
    assert rl.check("a", rule).allowed
    assert rl.check("b", rule).allowed
    assert not rl.check("a", rule).allowed
    assert not rl.check("b", rule).allowed


def test_concurrent_counting_no_undercount():
    t = 0.0
    rl = RateLimiter(clock=lambda: t)
    rule = QuotaRule(limit=50, window_seconds=60.0)
    n = 100
    barrier = threading.Barrier(n)
    allowed: list[bool] = []
    lock = threading.Lock()

    def worker() -> None:
        barrier.wait()
        d = rl.check("hot", rule)
        with lock:
            allowed.append(d.allowed)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for x in threads:
        x.start()
    for x in threads:
        x.join()
    assert sum(allowed) == 50
    assert rl.current_usage("hot") == 100


# ---- HTTP 层测试 ----

def test_http_quota_boundary_and_retry_after(client, clock):
    codes = [client.get(f"/orders/id{i}", headers=VH).status_code for i in range(5)]
    assert codes[:3] == [404, 404, 404]          # 配额内：业务错误，不是限流
    assert codes[3] == 429                        # 第 4 次超限
    body = client.get("/orders/x", headers=VH).json()
    assert body["error"]["code"] == "quota_exceeded"
    details = body["error"]["details"]
    assert details["caller"] == "trial"
    assert details["limit"] == 3
    assert details["window_seconds"] == 60.0
    assert details["retry_after_seconds"] == 60
    resp = client.get("/orders/x", headers=VH)
    assert resp.headers["Retry-After"] == "60"
    assert resp.headers["X-RateLimit-Remaining"] == "0"


def test_http_window_reset_restores_quota(client, clock):
    for i in range(3):
        assert client.get(f"/orders/a{i}", headers=VH).status_code == 404
    assert client.get("/orders/a", headers=VH).status_code == 429
    clock.advance(60.0)
    assert client.get("/orders/a", headers=VH).status_code == 404  # 新窗口恢复


def test_http_anonymous_has_separate_quota(client):
    h = {"X-API-Version": "2024-01-01"}  # 无 X-Caller-Id → anonymous, limit=2
    assert client.get("/orders/a", headers=h).status_code == 404
    assert client.get("/orders/b", headers=h).status_code == 404
    r = client.get("/orders/c", headers=h)
    assert r.status_code == 429
    assert r.json()["error"]["details"]["caller"] == "anonymous"


def test_http_quota_enforced_before_idempotency(client, clock):
    """配额耗尽后即便携带幂等键重放也会被拒，且不产生副作用。"""
    state = client.app.state.gateway
    h = {**VH, "Idempotency-Key": "ik"}
    assert client.post("/orders", json={"name": "x"}, headers=h).status_code == 201
    # trial 已用 1 次
    client.get("/orders/used2", headers=VH)
    client.get("/orders/used3", headers=VH)
    # 第 4 次：重放幂等键仍被限流
    r = client.post("/orders", json={"name": "x"}, headers=h)
    assert r.status_code == 429
    assert state.orders.side_effects == 1
