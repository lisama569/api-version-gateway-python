"""批量拒绝场景的修复回归：

- 配额不足被拒（429）是同键可重试的：不落永久失败记录，窗口恢复后同键同内容
  重试会重新走一遍判定——额度够就生效（201），仍不够才继续 429；
- 非法条目（空名字/纯空白名字、数量为 0 或负数）整批 400、一条不写，
  错误体定位到具体条目；且拒绝发生在幂等占位之前，修正内容后同键可重新提交；
- 单批条数超过整段窗口配额（400 batch_exceeds_quota_limit）与"暂时不够"（429）
  明确区分：不给 Retry-After，指引拆批而不是立刻反复重试。
"""

from __future__ import annotations

import logging

TRIAL = {"X-API-Version": "2024-01-01", "X-Caller-Id": "trial"}  # 配额 3 条/60s


def h(key: str) -> dict:
    return {**TRIAL, "Idempotency-Key": key}


def batch(*names: str) -> dict:
    return {"orders": [{"name": n} for n in names]}


# ---- 配额拒绝后的同键重试语义 ----

def test_quota_rejection_is_not_memoized_same_key_retries_after_window(client, clock):
    """429 被拒 → 窗口恢复 → 同键同内容重试真正重新判定并生效（201），不是回放旧拒绝。"""
    state = client.app.state.gateway
    # 先占满 trial 的 3 条额度
    assert client.post("/orders/batch", json=batch("f1", "f2", "f3"), headers=h("fill")).status_code == 201
    # 额度不足：整批 429，一条不写
    r1 = client.post("/orders/batch", json=batch("a", "b"), headers=h("retry-me"))
    assert r1.status_code == 429
    assert r1.json()["error"]["code"] == "quota_exceeded"
    assert state.orders.side_effects == 3

    # 窗口未恢复时同键重试：仍是 429（重新判定的结论，不是回放），仍不写
    r2 = client.post("/orders/batch", json=batch("a", "b"), headers=h("retry-me"))
    assert r2.status_code == 429
    assert r2.json()["error"]["code"] == "quota_exceeded"  # 不是 idempotent_replay_quota_exceeded
    assert state.orders.side_effects == 3

    # 窗口恢复后同键同内容重试：重新判定，额度够了就正常生效
    clock.advance(61)
    r3 = client.post("/orders/batch", json=batch("a", "b"), headers=h("retry-me"))
    assert r3.status_code == 201
    assert r3.headers["Idempotent-Replay"] == "false"  # 真实执行，不是回放
    assert r3.json()["applied"] == 2
    assert state.orders.side_effects == 5
    assert state.limiter.current_usage("trial") == 2

    # 生效后同键重放才走幂等回放：同一批订单、不再重复写
    r4 = client.post("/orders/batch", json=batch("a", "b"), headers=h("retry-me"))
    assert r4.status_code == 201
    assert r4.headers["Idempotent-Replay"] == "true"
    assert [o["id"] for o in r4.json()["orders"]] == [o["id"] for o in r3.json()["orders"]]
    assert state.orders.side_effects == 5


def test_quota_rejection_does_not_charge_quota_on_retry(client, clock):
    """被拒与同键重试期间都不消耗额度：恢复后额度完整可用。"""
    state = client.app.state.gateway
    assert client.post("/orders/batch", json=batch("x", "y", "z"), headers=h("fill2")).status_code == 201
    for _ in range(3):  # 反复重试被拒，不应刷穿或污染额度
        assert client.post("/orders/batch", json=batch("w"), headers=h("q-retry")).status_code == 429
    assert state.limiter.current_usage("trial") == 3
    clock.advance(61)
    assert client.post("/orders/batch", json=batch("w"), headers=h("q-retry")).status_code == 201
    assert state.limiter.current_usage("trial") == 1


# ---- 非法条目：整批拒绝、一条不写、定位到条目 ----

def test_empty_name_item_rejects_entire_batch(client):
    """空名字条目 → 整批 400，一条不写，错误体指出是第几条。"""
    state = client.app.state.gateway
    bad = {"orders": [{"name": "ok"}, {"name": ""}, {"name": "also-ok"}]}
    r = client.post("/orders/batch", json=bad, headers=h("bad-empty-name"))
    assert r.status_code == 400
    err = r.json()["error"]
    assert err["code"] == "invalid_request"
    locs = [e["loc"] for e in err["details"]["errors"]]
    assert any(list(loc[:2]) == ["orders", 1] for loc in locs)  # 第 2 条（下标 1）
    assert state.orders.side_effects == 0


def test_blank_name_item_rejects_entire_batch(client):
    """纯空白名字同样非法。"""
    state = client.app.state.gateway
    r = client.post("/orders/batch", json={"orders": [{"name": "   "}]}, headers=h("bad-blank"))
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "invalid_request"
    assert state.orders.side_effects == 0


def test_non_positive_quantity_rejects_entire_batch(client):
    """数量为 0 或负数 → 整批 400，一条不写，定位到具体条目。"""
    state = client.app.state.gateway
    for i, bad_qty in enumerate([0, -2]):
        bad = {"orders": [{"name": "fine"}, {"name": "bad-qty", "quantity": bad_qty}]}
        r = client.post("/orders/batch", json=bad, headers=h(f"bad-qty-{i}"))
        assert r.status_code == 400
        err = r.json()["error"]
        assert err["code"] == "invalid_request"
        locs = [e["loc"] for e in err["details"]["errors"]]
        assert any(list(loc[:2]) == ["orders", 1] for loc in locs)
    assert state.orders.side_effects == 0


def test_invalid_batch_leaves_no_idempotency_record(client):
    """非法批次在幂等占位之前被拒：修正内容后用同一幂等键可正常提交。"""
    state = client.app.state.gateway
    bad = {"orders": [{"name": "", "quantity": 0}]}
    assert client.post("/orders/batch", json=bad, headers=h("fix-me")).status_code == 400
    fixed = {"orders": [{"name": "fixed", "quantity": 1}]}
    r = client.post("/orders/batch", json=fixed, headers=h("fix-me"))
    assert r.status_code == 201  # 不是 409 idempotency_conflict
    assert r.json()["applied"] == 1
    assert state.orders.side_effects == 1


def test_single_order_also_validates_name_and_quantity(client):
    """单笔下单走同一模型，空名字/非正数量同样 400。"""
    state = client.app.state.gateway
    assert client.post("/orders", json={"name": ""}, headers=h("s-1")).status_code == 400
    assert client.post(
        "/orders", json={"name": "x", "quantity": 0}, headers=h("s-2")
    ).status_code == 400
    assert state.orders.side_effects == 0


# ---- 单批超过整段窗口额度：与"暂时不够"明确区分 ----

def test_batch_larger_than_window_quota_gets_split_guidance(client):
    """单批 4 条 > 整段配额 3 条：400 batch_exceeds_quota_limit，指引拆批，不给 Retry-After。"""
    state = client.app.state.gateway
    r = client.post("/orders/batch", json=batch("a", "b", "c", "d"), headers=h("too-big"))
    assert r.status_code == 400
    err = r.json()["error"]
    assert err["code"] == "batch_exceeds_quota_limit"
    details = err["details"]
    assert details["needed"] == 4
    assert details["limit"] == 3
    assert details["retryable"] is False
    assert details["action"] == "split_batch"
    assert "Retry-After" not in r.headers  # 等多久都没用，不引导立刻重试
    assert state.orders.side_effects == 0
    assert state.limiter.current_usage("trial") in (None, 0)  # 不扣额度


def test_oversized_batch_distinct_from_temporary_shortage(client, clock):
    """永远放不下（400 拆批）与暂时不够（429 稍后重试）是两种可区分的结论。"""
    # 先用掉 2 条，剩 1 条
    assert client.post("/orders/batch", json=batch("u", "v"), headers=h("pre")).status_code == 201
    # 要 2 条 > 剩 1 条但 ≤ 整段配额 3 → 429，可稍后重试
    r_wait = client.post("/orders/batch", json=batch("m", "n"), headers=h("waitable"))
    assert r_wait.status_code == 429
    assert r_wait.json()["error"]["code"] == "quota_exceeded"
    assert r_wait.json()["error"]["details"]["retryable"] is True
    assert "Retry-After" in r_wait.headers
    # 要 4 条 > 整段配额 3 → 400，必须拆批
    r_never = client.post("/orders/batch", json=batch("p", "q", "r", "s"), headers=h("never-fits"))
    assert r_never.status_code == 400
    assert r_never.json()["error"]["code"] == "batch_exceeds_quota_limit"
    # 窗口恢复后，"暂时不够"的那批同键重试可以成功；"永远放不下"的仍是 400
    clock.advance(61)
    assert client.post("/orders/batch", json=batch("m", "n"), headers=h("waitable")).status_code == 201
    assert client.post("/orders/batch", json=batch("p", "q", "r", "s"), headers=h("never-fits")).status_code == 400


def test_oversized_batch_same_key_can_be_resubmitted_after_split(client):
    """超批被拒不占幂等键：拆成小批后用同一幂等键可正常提交。"""
    state = client.app.state.gateway
    assert client.post("/orders/batch", json=batch("a", "b", "c", "d"), headers=h("split")).status_code == 400
    r = client.post("/orders/batch", json=batch("a", "b"), headers=h("split"))
    assert r.status_code == 201  # 键未被占用，不是 409
    assert r.json()["applied"] == 2
    assert state.orders.side_effects == 2


# ---- 日志可观测 ----

def test_rejection_and_retry_logged(client, clock, caplog):
    """配额拒绝（release 语义）与窗口恢复后同键重试生效，都有结构化日志可查。"""
    client.post("/orders/batch", json=batch("f1", "f2", "f3"), headers=h("log-fill"))
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="api_gateway"):
        client.post("/orders/batch", json=batch("g1"), headers=h("log-denied"))
        clock.advance(61)
        client.post("/orders/batch", json=batch("g1"), headers=h("log-denied"))
    batch_logs = [
        r.getMessage() for r in caplog.records
        if getattr(r, "event", None) == "batch_decision"
    ]
    assert any("result=rejected" in m and "reason=quota_exceeded" in m for m in batch_logs)
    assert any("result=applied" in m and "applied=1" in m for m in batch_logs)
    idem_logs = [
        r.getMessage() for r in caplog.records
        if getattr(r, "event", None) == "idempotency_decision"
    ]
    # 拒绝后同键重试是重新占位（acquired），而不是回放（replay）
    assert sum("outcome=acquired" in m for m in idem_logs) == 2
    assert not any("outcome=replay" in m for m in idem_logs)


def test_oversized_batch_rejection_logged(client, caplog):
    """超批拒绝的日志与普通额度不足可区分。"""
    with caplog.at_level(logging.INFO, logger="api_gateway"):
        client.post("/orders/batch", json=batch("a", "b", "c", "d"), headers=h("log-big"))
    batch_logs = [
        r.getMessage() for r in caplog.records
        if getattr(r, "event", None) == "batch_decision"
    ]
    assert any(
        "result=rejected" in m and "reason=batch_exceeds_quota_limit" in m
        and "needed=4" in m and "limit=3" in m
        for m in batch_logs
    )
