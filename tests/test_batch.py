"""批量下单：原子性、按条数计量配额、并发不超卖、调用方隔离、窗口边缘密集请求。

业务覆盖（配合 pytest 输出中的「业务覆盖」分组查看）：
- 批量提交的原子性：整批生效 / 非法条目整批拒绝并指出下标 / 空批拒绝；
- 幂等：同键同体重放复用首批结果零重复写入；配额拒绝后同键可重新占位；
- 配额按实际占用条数计量：余额不足整批拒绝（不先写一部分），
  返回需要多少、剩余多少、何时可重试；并发批量不透支；
- 调用方隔离：不同调用方同键各自独立；配额互不连累；
- 滑动窗口边缘：任意窗口长度内占用合计不超额。
"""

from __future__ import annotations

import logging
import threading

H_PREMIUM = {"X-API-Version": "2024-01-01", "X-Caller-Id": "premium"}
H_TRIAL = {"X-API-Version": "2024-01-01", "X-Caller-Id": "trial"}  # limit=3 / 60s


def _orders(names):
    return {"orders": [{"name": n} for n in names]}


# ---------------------------------------------------------------- 原子性

def test_batch_success_all_or_nothing_count_in_response(client):
    """整批生效：返回里能看出实际生效条数，side_effects 按条数增长。"""
    state = client.app.state.gateway
    r = client.post("/orders/batch", json=_orders(["a", "b", "c"]),
                    headers={**H_PREMIUM, "Idempotency-Key": "k1"})
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["created_count"] == 3
    assert body["batch_size"] == 3
    assert len(body["orders"]) == 3
    assert {o["name"] for o in body["orders"]} == {"a", "b", "c"}
    assert all(o["created_by"] == "premium" for o in body["orders"])
    assert r.headers["Idempotent-Replay"] == "false"
    assert state.orders.side_effects == 3


def test_batch_invalid_item_rejects_whole_batch_with_index(client):
    """某条不合法：整批拒绝且指出是哪一条，一条都不写入。"""
    state = client.app.state.gateway
    payload = {"orders": [
        {"name": "ok", "quantity": 1},
        {"name": "bad", "quantity": 0},
        {"name": "also-ok"},
    ]}
    r = client.post("/orders/batch", json=payload,
                    headers={**H_PREMIUM, "Idempotency-Key": "k2"})
    assert r.status_code == 400
    err = r.json()["error"]
    assert err["code"] == "batch_item_invalid"
    assert err["details"]["index"] == 1
    assert err["details"]["field"] == "quantity"
    assert state.orders.side_effects == 0  # 整批未写入
    # 非法下标可以是第 0 条（空 name）
    r2 = client.post("/orders/batch", json=_orders(["", "x"]),
                     headers={**H_PREMIUM, "Idempotency-Key": "k2b"})
    assert r2.status_code == 400
    assert r2.json()["error"]["details"]["index"] == 0
    assert state.orders.side_effects == 0


def test_batch_empty_rejected(client):
    state = client.app.state.gateway
    r = client.post("/orders/batch", json={"orders": []},
                    headers={**H_PREMIUM, "Idempotency-Key": "k3"})
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "batch_item_invalid"
    assert state.orders.side_effects == 0


def test_batch_malformed_body_is_invalid_request(client):
    r = client.post("/orders/batch", content=b"{bad",
                    headers={**H_PREMIUM, "Idempotency-Key": "k4",
                             "Content-Type": "application/json"})
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "invalid_request"


def test_batch_requires_idempotency_key(client):
    r = client.post("/orders/batch", json=_orders(["a"]), headers=H_PREMIUM)
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "idempotency_key_missing"


# ---------------------------------------------------------------- 幂等

def test_batch_replay_reuses_first_result_with_zero_new_writes(client):
    """同键同体重放：复用首批订单 ID，不再写任何一条。"""
    state = client.app.state.gateway
    headers = {**H_PREMIUM, "Idempotency-Key": "idem-1"}
    r1 = client.post("/orders/batch", json=_orders(["x", "y"]), headers=headers)
    ids1 = [o["id"] for o in r1.json()["orders"]]
    r2 = client.post("/orders/batch", json=_orders(["x", "y"]), headers=headers)
    assert r2.status_code == 201
    assert r2.headers["Idempotent-Replay"] == "true"
    assert r2.json()["created_count"] == 2
    assert [o["id"] for o in r2.json()["orders"]] == ids1
    assert state.orders.side_effects == 2  # 没有新增写入


def test_batch_same_key_different_body_conflicts(client):
    headers = {**H_PREMIUM, "Idempotency-Key": "idem-2"}
    assert client.post("/orders/batch", json=_orders(["a"]), headers=headers).status_code == 201
    r = client.post("/orders/batch", json=_orders(["b"]), headers=headers)
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "idempotency_conflict"
    assert client.app.state.gateway.orders.side_effects == 1


def test_batch_quota_rejection_does_not_pin_key(client, clock):
    """因配额不足整批被拒（无副作用）后，同键在窗口恢复时可成功，而非被永久占住。"""
    # trial limit=3：先占 2，再申请 2 被拒
    client.post("/orders/batch", json=_orders(["a", "b"]),
                headers={**H_TRIAL, "Idempotency-Key": "qk-1"})
    rejected = client.post("/orders/batch", json=_orders(["c", "d"]),
                           headers={**H_TRIAL, "Idempotency-Key": "qk-2"})
    assert rejected.status_code == 429
    assert client.app.state.gateway.orders.side_effects == 2
    # 窗口滑过后，仍用同一个键、同一批内容 → 应能成功
    clock.advance(60.0)
    r = client.post("/orders/batch", json=_orders(["c", "d"]),
                    headers={**H_TRIAL, "Idempotency-Key": "qk-2"})
    assert r.status_code == 201, r.text
    assert r.headers["Idempotent-Replay"] == "false"
    assert r.json()["created_count"] == 2
    assert client.app.state.gateway.orders.side_effects == 4


# ------------------------------------------------- 配额：按条数计量 / 不部分写

def test_quota_charged_per_item_not_per_request(client):
    """批量按实际占用条数扣额，而不是按请求次数。"""
    # trial limit=3：一条 2 + 一条 1 正好打满
    r1 = client.post("/orders/batch", json=_orders(["a", "b"]),
                     headers={**H_TRIAL, "Idempotency-Key": "charge-1"})
    assert r1.status_code == 201
    assert r1.headers["X-RateLimit-Remaining"] == "1"
    r2 = client.post("/orders/batch", json=_orders(["c"]),
                     headers={**H_TRIAL, "Idempotency-Key": "charge-2"})
    assert r2.status_code == 201
    assert r2.headers["X-RateLimit-Remaining"] == "0"
    # 只多要 1 条都不行
    r3 = client.post("/orders/batch", json=_orders(["d"]),
                     headers={**H_TRIAL, "Idempotency-Key": "charge-3"})
    assert r3.status_code == 429


def test_insufficient_quota_rejects_whole_batch_with_requirements(client):
    """余额不足：整批拒绝、不写一部分，details 说清需要/剩余/重试时间。"""
    state = client.app.state.gateway
    client.post("/orders/batch", json=_orders(["a", "b"]),
                headers={**H_TRIAL, "Idempotency-Key": "need-1"})
    before = state.orders.side_effects
    r = client.post("/orders/batch", json=_orders(["c", "d", "e"]),
                    headers={**H_TRIAL, "Idempotency-Key": "need-2"})
    assert r.status_code == 429
    details = r.json()["error"]["details"]
    assert details["required"] == 3
    assert details["remaining"] == 1
    assert details["used"] == 2
    assert details["retry_after_seconds"] == 60
    assert r.headers["Retry-After"] == "60"
    assert state.orders.side_effects == before  # 一条都没写进去
    # 被拒批次不占用额度：紧接着一个 1 条的小批仍可成功
    ok = client.post("/orders/batch", json=_orders(["z"]),
                     headers={**H_TRIAL, "Idempotency-Key": "need-3"})
    assert ok.status_code == 201
    assert ok.json()["created_count"] == 1


def test_batch_larger_than_limit_is_unretryable(client):
    """批次条数超过窗口上限：说明拆批，不给 Retry-After。"""
    r = client.post("/orders/batch", json=_orders([str(i) for i in range(4)]),
                    headers={**H_TRIAL, "Idempotency-Key": "too-big"})
    assert r.status_code == 429
    details = r.json()["error"]["details"]
    assert details["required"] == 4 and details["limit"] == 3
    assert details["retry_after_seconds"] is None
    assert "Retry-After" not in r.headers


def test_concurrent_batches_never_overspend_quota(client):
    """多个批量并发提交：成功批次的条数合计绝不超过配额，且每个成功批次完整生效。"""
    state = client.app.state.gateway
    n = 12
    barrier = threading.Barrier(n)
    results: list = []
    lock = threading.Lock()

    def worker(i: int) -> None:
        barrier.wait()
        # 每个请求 2 条，trial 窗口上限 3：最多一个批次成功
        resp = client.post(
            "/orders/batch",
            json=_orders([f"o{i}-{j}" for j in range(2)]),
            headers={**H_TRIAL, "Idempotency-Key": f"race-{i}"},
        )
        with lock:
            results.append(resp)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    accepted = [r for r in results if r.status_code == 201]
    rejected = [r for r in results if r.status_code == 429]
    assert len(accepted) == 1, [r.status_code for r in results]
    assert len(rejected) == n - 1
    assert sum(r.json()["created_count"] for r in accepted) <= 3
    assert state.orders.side_effects == 2  # 只有一个 2 条批次真正写入


def test_concurrent_duplicate_batches_single_effect(client):
    """同键同体的并发批量重复：恰好一个首执行，其余回放同一批结果。"""
    state = client.app.state.gateway
    n = 8
    barrier = threading.Barrier(n)
    results: list = []
    lock = threading.Lock()

    def worker() -> None:
        barrier.wait()
        resp = client.post("/orders/batch", json=_orders(["x", "y", "z"]),
                           headers={**H_PREMIUM, "Idempotency-Key": "dup-race"})
        with lock:
            results.append(resp)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    primaries = [r for r in results if r.headers["Idempotent-Replay"] == "false"]
    replays = [r for r in results if r.headers["Idempotent-Replay"] == "true"]
    assert len(primaries) == 1 and len(replays) == n - 1
    id_sets = {tuple(o["id"] for o in r.json()["orders"]) for r in results}
    assert len(id_sets) == 1
    assert state.orders.side_effects == 3


# ----------------------------------------------------------- 调用方隔离

def test_distinct_callers_same_key_are_independent(client):
    """两个调用方同键同体：各自首执行、各自结果，不串用。"""
    state = client.app.state.gateway
    h_a = {"X-API-Version": "2024-01-01", "X-Caller-Id": "alice"}
    h_b = {"X-API-Version": "2024-01-01", "X-Caller-Id": "bob"}
    payload = _orders(["shared-name"])
    ra1 = client.post("/orders/batch", json=payload,
                      headers={**h_a, "Idempotency-Key": "SAME"})
    rb1 = client.post("/orders/batch", json=payload,
                      headers={**h_b, "Idempotency-Key": "SAME"})
    assert ra1.status_code == 201 and rb1.status_code == 201
    assert ra1.headers["Idempotent-Replay"] == "false"
    assert rb1.headers["Idempotent-Replay"] == "false"
    assert ra1.json()["orders"][0]["id"] != rb1.json()["orders"][0]["id"]
    assert ra1.json()["orders"][0]["created_by"] == "alice"
    assert rb1.json()["orders"][0]["created_by"] == "bob"
    # 各自重放只回放自己的结果
    ra2 = client.post("/orders/batch", json=payload,
                      headers={**h_a, "Idempotency-Key": "SAME"})
    rb2 = client.post("/orders/batch", json=payload,
                      headers={**h_b, "Idempotency-Key": "SAME"})
    assert ra2.json()["orders"][0]["id"] == ra1.json()["orders"][0]["id"]
    assert rb2.json()["orders"][0]["id"] == rb1.json()["orders"][0]["id"]
    assert state.orders.side_effects == 2  # 每个调用方各写一次


def test_one_callers_quota_exhaustion_does_not_block_another(client, clock):
    """一个调用方超限不连累其他调用方（配额按调用方独立核算）。"""
    # trial 打满 3 条
    r = client.post("/orders/batch", json=_orders(["a", "b", "c"]),
                    headers={**H_TRIAL, "Idempotency-Key": "iso-1"})
    assert r.status_code == 201
    blocked = client.post("/orders/batch", json=_orders(["d"]),
                          headers={**H_TRIAL, "Idempotency-Key": "iso-2"})
    assert blocked.status_code == 429
    # premium 与一个新具名调用方都不受影响
    ok1 = client.post("/orders/batch", json=_orders(["p1"]),
                      headers={**H_PREMIUM, "Idempotency-Key": "iso-3"})
    ok2 = client.post(
        "/orders/batch", json=_orders(["n1"]),
        headers={"X-API-Version": "2024-01-01", "X-Caller-Id": "newcomer",
                 "Idempotency-Key": "iso-4"},
    )
    assert ok1.status_code == 201 and ok2.status_code == 201
    # 窗口滑过只恢复自己的额度，不影响他人
    clock.advance(60.0)
    retry = client.post("/orders/batch", json=_orders(["d"]),
                        headers={**H_TRIAL, "Idempotency-Key": "iso-2"})
    assert retry.status_code == 201


# ------------------------------------------------------ 滑动窗口边缘密集请求

def test_dense_requests_on_window_edge_do_not_exceed_quota(client, clock):
    """窗口边缘的密集请求：跨越固定窗口边界也不能获得双倍配额。

    旧固定窗口在边界两侧各允许一份配额（边界瞬间 2x 突发）；滑动窗口语义
    要求任意 60s 区间内成功占用合计不超过 3，且额度随占用逐条滑出而恢复。
    """
    # t=0：占用 2 条
    assert client.post("/orders/batch", json=_orders(["a", "b"]),
                       headers={**H_TRIAL, "Idempotency-Key": "edge-1"}).status_code == 201
    clock.advance(59.99)
    # t=59.99：窗口边缘再占 1 条成功（窗口内合计 3，恰好用满）
    assert client.post("/orders/batch", json=_orders(["c"]),
                       headers={**H_TRIAL, "Idempotency-Key": "edge-2"}).status_code == 201
    clock.advance(0.01)  # t=60.0：固定窗口视角已进入"新窗口"
    # 固定窗口这里会放出一整份新配额；滑动窗口下 t=59.99 的 1 条仍在窗口内，
    # 但 t=0 的 2 条恰好滑出 → 只剩 2 个名额：要 3 条必拒、要 2 条可成
    r3 = client.post("/orders/batch", json=_orders(["d", "e", "f"]),
                     headers={**H_TRIAL, "Idempotency-Key": "edge-3"})
    assert r3.status_code == 429
    assert r3.json()["error"]["details"]["remaining"] == 2
    r4 = client.post("/orders/batch", json=_orders(["d", "e"]),
                     headers={**H_TRIAL, "Idempotency-Key": "edge-4"})
    assert r4.status_code == 201, r4.text
    assert r4.json()["created_count"] == 2
    # 此刻窗口内为 t=59.99(1) + t=60.0(2) = 3，再来 1 条仍被拒
    r5 = client.post("/orders/batch", json=_orders(["g"]),
                     headers={**H_TRIAL, "Idempotency-Key": "edge-5"})
    assert r5.status_code == 429
    # 全区间总账：成功占用的条数从未超过 3
    assert client.app.state.gateway.orders.side_effects == 5


def test_window_recovers_gradually_as_occupancies_slide_out(client, clock):
    """额度随占用逐条滑出而逐步恢复（不是整点一次性全部恢复）。"""
    client.post("/orders/batch", json=_orders(["a"]),
                headers={**H_TRIAL, "Idempotency-Key": "w1"})
    clock.advance(10)
    client.post("/orders/batch", json=_orders(["b"]),
                headers={**H_TRIAL, "Idempotency-Key": "w2"})
    clock.advance(10)
    client.post("/orders/batch", json=_orders(["c"]),
                headers={**H_TRIAL, "Idempotency-Key": "w3"})  # 打满
    clock.advance(41)  # t=61：第一条（t=0）刚滑出，恢复 1 个名额
    r = client.post("/orders/batch", json=_orders(["d"]),
                    headers={**H_TRIAL, "Idempotency-Key": "w4"})
    assert r.status_code == 201
    assert r.headers["X-RateLimit-Remaining"] == "0"
    # 想要 2 条仍然不够（t=10、20、61 的占用同窗）
    r2 = client.post("/orders/batch", json=_orders(["e", "f"]),
                     headers={**H_TRIAL, "Idempotency-Key": "w5"})
    assert r2.status_code == 429
    assert r2.json()["error"]["details"]["remaining"] == 0


# ---------------------------------------------------------------- 日志

def test_batch_decisions_are_logged(client, caplog):
    """日志能看出批量业务的幂等与配额判定依据。"""
    with caplog.at_level(logging.INFO, logger="api_gateway"):
        client.post("/orders/batch", json=_orders(["log-a", "log-b"]),
                    headers={**H_PREMIUM, "Idempotency-Key": "log-batch"})
        client.post("/orders/batch", json=_orders(["log-a", "log-b"]),
                    headers={**H_PREMIUM, "Idempotency-Key": "log-batch"})
    idem = [r.getMessage() for r in caplog.records
            if getattr(r, "event", None) == "batch_idempotency_decision"]
    assert any("outcome=completed" in m and "created_count=2" in m
               and "caller=premium" in m for m in idem)
    assert any("outcome=replay" in m for m in idem)
    quota = [r.getMessage() for r in caplog.records
             if getattr(r, "event", None) == "batch_quota_decision"]
    # 重放不再次计费：只有首批一条 allowed=true 的配额日志
    assert sum("allowed=true" in m for m in quota) == 1
    assert any("required=2" in m and "remaining=998" in m for m in quota)
