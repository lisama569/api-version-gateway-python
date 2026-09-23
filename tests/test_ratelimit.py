"""配额限流：边界拒绝 + 可重试信息；并发不漏计；窗口滑动无空档。"""
from __future__ import annotations

import threading


def _post(client, caller="alice", key="k"):
    return client.post(
        "/resources",
        json={"name": "n"},
        headers={"X-API-Version": "2025-06-01", "X-Caller-Id": caller, "Idempotency-Key": key},
    )


def test_quota_boundary_rejected_with_retry_info(client):
    r1 = _post(client, key="k1")
    r2 = _post(client, key="k2")
    r3 = _post(client, key="k3")
    assert [r.status_code for r in (r1, r2, r3)] == [201, 201, 201]
    # 第 4 次必须被拒绝，且原因/重试信息齐备
    r4 = _post(client, key="k4")
    assert r4.status_code == 429
    err = r4.json()["error"]
    assert err["code"] == "QUOTA_EXCEEDED"
    assert err["details"]["limit"] == 3
    assert int(r4.headers["Retry-After"]) >= 1
    assert r4.headers["X-RateLimit-Limit"] == "3"
    assert r4.headers["X-RateLimit-Remaining"] == "0"


def test_quota_is_per_caller(client):
    assert _post(client, caller="a", key="k").status_code == 201
    assert _post(client, caller="b", key="k").status_code == 201


def test_caller_override_quota(client):
    # vip 配额 100：连续 10 次都应放行（普通用户早已超限）
    for i in range(10):
        assert _post(client, caller="vip", key=f"v{i}").status_code == 201


def test_window_sliding_no_reset_gap(client, clock):
    # t0 占用 3 个；t+9.9 仍拒绝；t+10.0 窗口滑过后放行
    for i in range(3):
        assert _post(client, key=f"a{i}").status_code == 201
    assert _post(client, key="a4").status_code == 429

    clock.advance(9.9)
    r = _post(client, key="a5")
    assert r.status_code == 429  # 尚未滑出，无“提前重置”空档

    clock.advance(0.1)  # t+10.0
    r = _post(client, key="a6")
    assert r.status_code == 201
    # 旧的 3 条都已滑出，此刻窗口内只有 a6
    assert r.headers["X-RateLimit-Remaining"] == "2"


def test_retry_after_decreases_with_window(client, clock):
    for i in range(3):
        _post(client, key=f"b{i}")
    clock.advance(4.0)
    r = _post(client, key="b4")
    assert r.status_code == 429
    # 最早一条记录在 t0 占用，t+4 时还剩 6 秒滑出
    assert int(r.headers["Retry-After"]) == 6


def test_concurrent_requests_not_undercounted(client):
    """50 个并发请求 / 限额 100（vip）：直接测限流器单元确保不超发。"""
    from app.ratelimit import RateLimiter

    limiter = RateLimiter(default_limit=10, window_seconds=10.0)
    decisions: list[bool] = []
    barrier = threading.Barrier(50)
    lock = threading.Lock()

    def hit() -> None:
        barrier.wait()
        d = limiter.check_and_consume("hot")
        with lock:
            decisions.append(d.allowed)

    threads = [threading.Thread(target=hit) for _ in range(50)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert sum(decisions) == 10  # 精确放行 10 个，无漏计/超发


def test_replay_does_not_consume_quota(client):
    # 占满 3 个配额
    for i in range(3):
        _post(client, key=f"c{i}")
    assert _post(client, key="c4").status_code == 429
    # 配额已满时，重复提交既有键仍应重放成功
    r = _post(client, key="c0")
    assert r.status_code == 201
    assert r.headers["Idempotency-Replayed"] == "true"
