"""幂等写入：重复提交零重复副作用；并发重复最多一个生效，其余可解释。"""

from __future__ import annotations

import threading

import pytest

from gateway.idempotency import (
    ACQUIRED,
    COMPLETED,
    FAILED,
    INFLIGHT,
    IdempotencyStore,
    fingerprint_payload,
)
from gateway.errors import IdempotencyConflictError

H = {"X-API-Version": "2024-01-01", "X-Caller-Id": "premium"}
PAYLOAD = {"name": "book", "quantity": 1, "amount": 10}


# ---- 存储单元测试 ----

def test_acquire_states_and_replay():
    store = IdempotencyStore(ttl_seconds=60)
    fp = fingerprint_payload(PAYLOAD)
    assert store.acquire("k", fp) == ACQUIRED
    assert store.acquire("k", fp) == INFLIGHT
    store.complete("k", {"order": {"id": "o1"}}, 201)
    assert store.acquire("k", fp) == COMPLETED
    entry = store.get_result("k")
    assert entry.response["order"]["id"] == "o1"
    assert entry.status_code == 201


def test_different_payload_same_key_conflicts():
    store = IdempotencyStore(ttl_seconds=60)
    store.acquire("k", fingerprint_payload(PAYLOAD))
    with pytest.raises(IdempotencyConflictError):
        store.acquire("k", fingerprint_payload({"name": "other"}))


def test_failed_first_request_is_replayable_and_creates_no_side_effect():
    store = IdempotencyStore(ttl_seconds=60)
    fp = fingerprint_payload(PAYLOAD)
    assert store.acquire("k", fp) == ACQUIRED
    store.fail("k", "boom", 500)
    assert store.acquire("k", fp) == FAILED
    entry = store.get_result("k")
    assert entry.error_code == "boom" and entry.status_code == 500


def test_concurrent_duplicates_exactly_one_wins():
    """N 个线程同时提交同一幂等键：恰好 1 个占位成功，其余等待后拿到完成结论。"""
    store = IdempotencyStore(ttl_seconds=60)
    fp = fingerprint_payload(PAYLOAD)
    n = 16
    barrier = threading.Barrier(n)
    outcomes: list[str] = []
    lock = threading.Lock()

    def worker() -> None:
        barrier.wait()
        outcome = store.acquire("k", fp)
        if outcome == ACQUIRED:
            store.complete("k", {"order": {"id": "o"}}, 201)
        elif outcome == INFLIGHT:
            outcome = store.wait_for_completion("k", timeout=5).status
        with lock:
            outcomes.append(outcome)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert outcomes.count(ACQUIRED) == 1
    assert outcomes.count(COMPLETED) == n - 1


# ---- HTTP 层测试 ----

def test_http_repeated_submission_single_side_effect(client):
    state = client.app.state.gateway
    headers = {**H, "Idempotency-Key": "order-1"}
    r1 = client.post("/orders", json=PAYLOAD, headers=headers)
    assert r1.status_code == 201
    assert r1.headers["Idempotent-Replay"] == "false"
    order_id = r1.json()["order"]["id"]

    r2 = client.post("/orders", json=PAYLOAD, headers=headers)
    assert r2.status_code == 201
    assert r2.headers["Idempotent-Replay"] == "true"
    assert r2.json()["order"]["id"] == order_id
    # 真实写入只发生一次
    assert state.orders.side_effects == 1


def test_http_missing_idempotency_key_rejected(client):
    r = client.post("/orders", json=PAYLOAD, headers=H)
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "idempotency_key_missing"


def test_http_same_key_different_body_conflicts(client):
    r1 = client.post("/orders", json=PAYLOAD, headers={**H, "Idempotency-Key": "k9"})
    assert r1.status_code == 201
    r2 = client.post(
        "/orders", json={"name": "different"}, headers={**H, "Idempotency-Key": "k9"}
    )
    assert r2.status_code == 409
    assert r2.json()["error"]["code"] == "idempotency_conflict"
    # 冲突请求未产生副作用
    assert client.app.state.gateway.orders.side_effects == 1


def test_http_concurrent_duplicates_one_effect(client):
    """经 HTTP 的并发重复提交：只有一个 201 首执行，其余为 201 重放，资源同一。"""
    state = client.app.state.gateway
    barrier = threading.Barrier(8)
    results: list = []
    lock = threading.Lock()

    def worker() -> None:
        barrier.wait()
        resp = client.post(
            "/orders", json=PAYLOAD, headers={**H, "Idempotency-Key": "race-key"}
        )
        with lock:
            results.append(resp)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    replays = [r for r in results if r.headers["Idempotent-Replay"] == "true"]
    primaries = [r for r in results if r.headers["Idempotent-Replay"] == "false"]
    assert len(primaries) == 1
    assert len(replays) == 7
    ids = {r.json()["order"]["id"] for r in results}
    assert len(ids) == 1
    assert state.orders.side_effects == 1


def test_http_distinct_keys_create_distinct_orders(client):
    state = client.app.state.gateway
    r1 = client.post("/orders", json=PAYLOAD, headers={**H, "Idempotency-Key": "a"})
    r2 = client.post("/orders", json=PAYLOAD, headers={**H, "Idempotency-Key": "b"})
    assert r1.json()["order"]["id"] != r2.json()["order"]["id"]
    assert state.orders.side_effects == 2
