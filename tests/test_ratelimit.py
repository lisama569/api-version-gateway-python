"""配额限流：边界计数、窗口滚动、重试信息、并发不漏计、按调用方隔离。"""

from __future__ import annotations

import threading

from gateway.config import QuotaRule
from gateway.ratelimit import QuotaDecision, RateLimiter

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


def test_window_edge_no_burst_over_sliding_window():
    """滑动窗口：切换瞬间旧占用仍然计数，不存在可打满两份配额的边缘空档。

    旧的固定窗口实现允许"旧窗口打满 + 新窗口立刻再打满"（瞬时 2x 流量）；
    新语义要求任意长度为 window_seconds 的时间区间内占用合计都不超过上限，
    因此落在窗口边缘的请求仍与上一窗口的占用合并判定。
    """
    t = 0.0
    rl = RateLimiter(clock=lambda: t)
    rule = QuotaRule(limit=2, window_seconds=10.0)
    assert rl.check("c", rule).allowed
    assert rl.check("c", rule).allowed
    assert not rl.check("c", rule).allowed
    t = 9.99
    assert not rl.check("c", rule).allowed  # 仍在旧占用的滑动窗口内
    t = 10.0
    d = rl.check("c", rule)               # t=0 的三条（含拒绝）过期，释放出名额
    assert d.allowed and d.remaining == 0
    # 窗口内最早事件是 t=9.99 的拒绝记账 + t=10 的本次占用，合计 2
    assert d.window_started_at == 9.99
    assert rl.current_usage("c") == 2
    # 再来一条仍超限（当前窗口内为 t=9.99 与 t=10 的记账，合计 2）
    assert not rl.check("c", rule).allowed
    # 直到所有占用都滑出窗口后才恢复完整配额
    t = 20.0
    d2 = rl.check("c", rule)
    assert d2.allowed and d2.remaining == 1
    assert rl.current_usage("c") == 1


def test_reserve_whole_batch_or_nothing():
    """批量预留：余额覆盖不了整批时整笔拒绝，不留下任何占用。"""
    t = 0.0
    rl = RateLimiter(clock=lambda: t)
    rule = QuotaRule(limit=5, window_seconds=10.0)
    d = rl.reserve("c", rule, cost=3)
    assert d.allowed and d.reservation_id is not None and d.remaining == 2
    # 只剩 2，申请 3：整笔拒绝
    d2 = rl.reserve("c", rule, cost=3)
    assert not d2.allowed
    assert d2.used == 3 and d2.remaining == 2
    assert rl.current_usage("c") == 3  # 被拒的整批没有占用任何额度
    # 申请 2：恰好容纳
    d3 = rl.reserve("c", rule, cost=2)
    assert d3.allowed and d3.remaining == 0
    # 退还预留后额度恢复
    rl.refund("c", d3.reservation_id)
    assert rl.current_usage("c") == 3


def test_reserve_retry_after_points_to_earliest_fit():
    """整批被拒时给出最早可重试时刻：恰为最早占用过期、额度足够的那一刻。"""
    t = 0.0
    rl = RateLimiter(clock=lambda: t)
    rule = QuotaRule(limit=3, window_seconds=10.0)
    rl.reserve("c", rule, cost=2)       # t=0 占用 2
    t = 6.0
    rl.reserve("c", rule, cost=1)       # t=6 占用 1，窗口内合计 3
    t = 7.0
    d = rl.reserve("c", rule, cost=2)   # 需要 2，最早要等 t=0 的 2 个名额过期
    assert not d.allowed
    assert d.earliest_fit_at == 10.0
    assert d.retry_after_seconds == 3
    t = 10.0
    d2 = rl.reserve("c", rule, cost=2)  # t=0 的占用刚滑出，t=6 的 1 个仍在
    assert d2.allowed and d2.remaining == 0


def test_reserve_cost_over_limit_unretryable():
    """单笔成本超过上限：任何窗口都无法容纳，earliest_fit_at 为 None。"""
    rl = RateLimiter(clock=lambda: 0.0)
    rule = QuotaRule(limit=3, window_seconds=10.0)
    d = rl.reserve("c", rule, cost=4)
    assert not d.allowed and d.earliest_fit_at is None


def test_concurrent_reserve_never_overcommits():
    """并发批量预留：成功批次的成本合计恰好等于配额，绝不超卖。"""
    rl = RateLimiter(clock=lambda: 0.0)
    rule = QuotaRule(limit=50, window_seconds=60.0)
    n = 40
    barrier = threading.Barrier(n)
    decisions: list[QuotaDecision] = []
    lock = threading.Lock()

    def worker() -> None:
        barrier.wait()
        d = rl.reserve("hot", rule, cost=2)
        with lock:
            decisions.append(d)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for x in threads:
        x.start()
    for x in threads:
        x.join()
    accepted = [d for d in decisions if d.allowed]
    assert sum(d.requested_cost for d in accepted) == 50
    assert len(accepted) == 25
    assert rl.current_usage("hot") == 50


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
