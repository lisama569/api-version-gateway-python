"""批量下单：整批原子生效、按条数计配额、并发不透支、调用方隔离、窗口边缘不超额。

覆盖的新增业务行为：
- 原子性：一批多条要么全部生效、要么一条不生效；非法条目整批拒绝并定位到具体条目；
- 幂等：同键同批重放复用首次结果（不重复写、不重复扣额度），同键不同内容 409；
- 配额：按批内条数扣减；余额不足整批拒绝并给出 needed/remaining/retry_after；
- 并发：多批量请求并发提交不透支额度；
- 隔离：不同调用方同幂等键各自独立，配额互不影响；
- 窗口：滑动窗口下窗口边缘的密集请求也不能超额。
"""

from __future__ import annotations

import logging
import threading

H = {"X-API-Version": "2024-01-01", "X-Caller-Id": "premium"}      # 配额 1000/60s
TRIAL = {"X-API-Version": "2024-01-01", "X-Caller-Id": "trial"}    # 配额 3/60s
BATCH = {"orders": [{"name": "a", "quantity": 2, "amount": 10}, {"name": "b"}, {"name": "c"}]}


def batch_headers(base: dict, key: str) -> dict:
    return {**base, "Idempotency-Key": key}


# ---- 原子性 ----

def test_batch_create_applies_all_atomically(client):
    """一批 3 条全部生效，返回体能看出实际生效条数。"""
    state = client.app.state.gateway
    r = client.post("/orders/batch", json=BATCH, headers=batch_headers(H, "b-ok"))
    assert r.status_code == 201
    body = r.json()
    assert body["applied"] == 3
    assert len(body["orders"]) == 3
    assert body["orders"][0]["quantity"] == 2
    assert r.headers["Idempotent-Replay"] == "false"
    assert state.orders.side_effects == 3
    for order in body["orders"]:  # 每条都可查询
        assert client.get(f"/orders/{order['id']}", headers=H).status_code == 200


def test_batch_with_invalid_item_rejected_entirely(client):
    """第 2 条不合法 → 整批 400，一条都不写，错误体能定位到具体条目。"""
    state = client.app.state.gateway
    bad = {"orders": [{"name": "ok"}, {"name": 123}, {"name": "also-ok"}]}
    r = client.post("/orders/batch", json=bad, headers=batch_headers(H, "b-bad"))
    assert r.status_code == 400
    err = r.json()["error"]
    assert err["code"] == "invalid_request"
    locs = [e["loc"] for e in err["details"]["errors"]]
    assert any(list(loc[:2]) == ["orders", 1] for loc in locs)  # 指出是第 2 条（下标 1）
    assert state.orders.side_effects == 0  # 不存在只写一半的中态


def test_empty_batch_rejected(client):
    r = client.post("/orders/batch", json={"orders": []}, headers=batch_headers(H, "b-empty"))
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "invalid_request"


def test_batch_missing_idempotency_key_rejected(client):
    r = client.post("/orders/batch", json=BATCH, headers=H)
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "idempotency_key_missing"


# ---- 幂等 ----

def test_batch_replay_reuses_first_result(client):
    """同键同批重复提交：复用首次结果，不重复写、不重复扣额度。"""
    state = client.app.state.gateway
    h = batch_headers(H, "b-replay")
    r1 = client.post("/orders/batch", json=BATCH, headers=h)
    assert r1.status_code == 201
    ids1 = [o["id"] for o in r1.json()["orders"]]
    usage_after_first = state.limiter.current_usage("premium")

    r2 = client.post("/orders/batch", json=BATCH, headers=h)
    assert r2.status_code == 201
    assert r2.headers["Idempotent-Replay"] == "true"
    assert r2.json()["applied"] == 3
    assert [o["id"] for o in r2.json()["orders"]] == ids1  # 同一批订单
    assert state.orders.side_effects == 3                  # 没有重复写
    assert state.limiter.current_usage("premium") == usage_after_first  # 重放不再扣额度


def test_batch_same_key_different_content_conflicts(client):
    state = client.app.state.gateway
    h = batch_headers(H, "b-conflict")
    assert client.post("/orders/batch", json=BATCH, headers=h).status_code == 201
    r = client.post("/orders/batch", json={"orders": [{"name": "other"}]}, headers=h)
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "idempotency_conflict"
    assert state.orders.side_effects == 3


# ---- 配额按条数计量 ----

def test_batch_quota_charged_per_item(client):
    """一批 2 条占用 2 条额度（而不是按请求次数算 1 次）。"""
    state = client.app.state.gateway
    r = client.post(
        "/orders/batch",
        json={"orders": [{"name": "x"}, {"name": "y"}]},
        headers=batch_headers(TRIAL, "q-per-item"),
    )
    assert r.status_code == 201
    assert state.limiter.current_usage("trial") == 2
    assert r.headers["X-RateLimit-Remaining"] == "1"


def test_batch_rejected_when_quota_insufficient(client):
    """剩余额度不足整批：整批 429，一条不写，并说明需要多少/剩多少/何时重试。"""
    state = client.app.state.gateway
    # trial 配额 3 条：先用掉 2 条
    assert client.post(
        "/orders/batch",
        json={"orders": [{"name": "a"}, {"name": "b"}]},
        headers=batch_headers(TRIAL, "q-first"),
    ).status_code == 201
    # 再要 2 条，只剩 1 条 → 整批拒绝
    r = client.post(
        "/orders/batch",
        json={"orders": [{"name": "c"}, {"name": "d"}]},
        headers=batch_headers(TRIAL, "q-second"),
    )
    assert r.status_code == 429
    err = r.json()["error"]
    assert err["code"] == "quota_exceeded"
    details = err["details"]
    assert details["needed"] == 2
    assert details["remaining"] == 1
    assert details["limit"] == 3
    assert details["retry_after_seconds"] > 0
    assert r.headers["Retry-After"]
    assert state.orders.side_effects == 2          # 一条都没多写
    assert state.limiter.current_usage("trial") == 2  # 拒绝不扣额度


def test_concurrent_batches_never_exceed_quota(client):
    """8 个并发批量请求（每批 2 条）对 5 条配额：成功占用合计不超过配额。"""
    state = client.app.state.gateway
    headers = {"X-API-Version": "2024-01-01", "X-Caller-Id": "batchy"}  # 默认配额 5/60s
    barrier = threading.Barrier(8)
    results: list = []
    lock = threading.Lock()

    def worker(i: int) -> None:
        barrier.wait()
        r = client.post(
            "/orders/batch",
            json={"orders": [{"name": f"n{i}a"}, {"name": f"n{i}b"}]},
            headers=batch_headers(headers, f"cb-{i}"),
        )
        with lock:
            results.append(r)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    applied = [r for r in results if r.status_code == 201]
    rejected = [r for r in results if r.status_code == 429]
    assert len(applied) == 2    # 5 条额度只够两批（各 2 条）
    assert len(rejected) == 6
    assert state.orders.side_effects == 4
    assert state.limiter.current_usage("batchy") == 4  # ≤ 5，未透支
    for r in rejected:
        assert r.json()["error"]["details"]["needed"] == 2


# ---- 调用方隔离 ----

def test_same_key_different_callers_independent(client):
    """两个调用方碰巧用同一幂等键：各自独立生效，结果互不串扰。"""
    state = client.app.state.gateway
    ha = batch_headers({"X-API-Version": "2024-01-01", "X-Caller-Id": "caller-a"}, "shared-key")
    hb = batch_headers({"X-API-Version": "2024-01-01", "X-Caller-Id": "caller-b"}, "shared-key")
    ra = client.post("/orders/batch", json={"orders": [{"name": "from-a"}]}, headers=ha)
    rb = client.post("/orders/batch", json={"orders": [{"name": "from-b"}]}, headers=hb)
    assert ra.status_code == 201 and rb.status_code == 201
    order_a, order_b = ra.json()["orders"][0], rb.json()["orders"][0]
    assert order_a["id"] != order_b["id"]
    assert order_a["created_by"] == "caller-a"
    assert order_b["created_by"] == "caller-b"
    assert state.orders.side_effects == 2
    # 各自重放拿到各自的结果
    ra2 = client.post("/orders/batch", json={"orders": [{"name": "from-a"}]}, headers=ha)
    assert ra2.headers["Idempotent-Replay"] == "true"
    assert ra2.json()["orders"][0]["id"] == order_a["id"]


def test_quota_isolated_per_caller(client):
    """一个调用方超限被拒，不影响其他调用方。"""
    assert client.post(
        "/orders/batch",
        json={"orders": [{"name": "a1"}, {"name": "a2"}, {"name": "a3"}]},
        headers=batch_headers(TRIAL, "iso-fill"),
    ).status_code == 201  # trial 用满 3 条
    r = client.post(
        "/orders/batch",
        json={"orders": [{"name": "a4"}]},
        headers=batch_headers(TRIAL, "iso-over"),
    )
    assert r.status_code == 429  # trial 自己超限
    other = {"X-API-Version": "2024-01-01", "X-Caller-Id": "trial-b"}
    r2 = client.post(
        "/orders/batch",
        json={"orders": [{"name": "b1"}]},
        headers=batch_headers(other, "iso-other"),
    )
    assert r2.status_code == 201  # 其他调用方不受连累


# ---- 窗口边缘密集请求 ----

def test_window_edge_burst_cannot_exceed_quota(client, clock):
    """窗口边缘的密集请求也不能超额：任意 60s 内成功占用合计 ≤ 配额。"""
    state = client.app.state.gateway

    def post(ikey: str, name: str):
        return client.post(
            "/orders/batch",
            json={"orders": [{"name": name}]},
            headers=batch_headers(TRIAL, ikey),
        )

    assert post("e1", "n1").status_code == 201      # t=1000
    clock.advance(59.9)                             # t=1059.9：窗口“末尾”
    assert post("e2", "n2").status_code == 201
    assert post("e3", "n3").status_code == 201      # 打满 3 条
    clock.advance(0.1)                              # t=1060：越过原固定窗口边界
    assert post("e4", "n4").status_code == 201      # 仅 t=1000 的占用滑出，释放 1 条
    assert post("e5", "n5").status_code == 429      # 不能借切窗翻倍
    assert state.limiter.current_usage("trial") == 3
    clock.advance(59.9)                             # t=1119.9：t=1059.9 的占用滑出
    assert post("e6", "n6").status_code == 201


# ---- 日志可观测 ----

def test_batch_decisions_logged(client, caplog):
    """批量判定（生效条数、按条计费、重放）都有结构化日志可查。"""
    with caplog.at_level(logging.INFO, logger="api_gateway"):
        client.post("/orders/batch", json=BATCH, headers=batch_headers(H, "log-b1"))
        client.post("/orders/batch", json=BATCH, headers=batch_headers(H, "log-b1"))
    records = [(getattr(r, "event", None), r.getMessage()) for r in caplog.records]
    batch = [m for e, m in records if e == "batch_decision"]
    assert any("result=applied" in m and "applied=3" in m for m in batch)
    quota = [m for e, m in records if e == "quota_decision"]
    assert any("cost=3" in m and "allowed=true" in m for m in quota)
    idem = [m for e, m in records if e == "idempotency_decision"]
    assert any("batch_size=3" in m for m in idem)
    assert any("outcome=replay" in m for m in idem)


def test_batch_quota_rejection_logged(client, caplog):
    """批量因配额被拒：日志能看出需要多少、剩多少、多久后可重试。"""
    client.post(
        "/orders/batch",
        json={"orders": [{"name": "a"}, {"name": "b"}, {"name": "c"}]},
        headers=batch_headers(TRIAL, "log-fill"),
    )
    with caplog.at_level(logging.INFO, logger="api_gateway"):
        client.post(
            "/orders/batch",
            json={"orders": [{"name": "d"}]},
            headers=batch_headers(TRIAL, "log-denied"),
        )
    batch = [
        r.getMessage() for r in caplog.records
        if getattr(r, "event", None) == "batch_decision"
    ]
    assert any(
        "result=rejected" in m and "reason=quota_exceeded" in m
        and "needed=1" in m and "remaining=0" in m
        for m in batch
    )
